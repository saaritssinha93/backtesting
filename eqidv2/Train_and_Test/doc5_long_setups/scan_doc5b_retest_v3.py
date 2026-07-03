from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve()
REPO_ROOT = HERE
for _ in range(10):
    if (REPO_ROOT / "research_v11_tier123_new_setups.py").exists():
        break
    REPO_ROOT = REPO_ROOT.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(HERE.parent) not in sys.path:
    sys.path.insert(0, str(HERE.parent))

import research_v11_tier123_new_setups as rv  # noqa: E402
import scan_doc5b_rs_breadth_v2 as base  # noqa: E402


SETUP = "DOC5B_MOMO_BREAKOUT_LONG"


def ref_type(or_high: float, rh20: float, pdh: float, ref: float) -> str:
    vals = {"ORH": or_high, "RH20": rh20, "PDH": pdh}
    for name, val in vals.items():
        if np.isfinite(val) and abs(float(val) - float(ref)) < 1e-9:
            return name
    return "MIXED"


def scan_ticker(ticker: str, df: pd.DataFrame, xsec: dict, market_ctx: dict, args) -> list[dict]:
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 24:
            continue
        or_high, _or_low = rv.v2._opening_range(g)
        prev_day_high = float(g.get("prev_day_high", pd.Series(np.nan, index=g.index)).iloc[0])
        prev_high20 = g["high"].shift(1).rolling(20, min_periods=8).max()
        breakouts: list[dict] = []

        for i in range(max(base.SLOPE_LOOKBACK, 8), len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = rv._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute

            close = float(row["close"])
            open_px = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            rng = float(row.get("range", np.nan))
            if not (base.finite(atr, vwap, vol_ratio, close_loc, vwap_dist) and atr > 0):
                continue
            if close < base.MIN_CLOSE or float(row.get("day_value_so_far_rs", 0.0)) < base.MIN_DAY_VALUE_RS:
                continue

            x = xsec.get((ticker, ts))
            if not x:
                continue
            rs_rank = float(x["rs_rank"])
            breadth_above = float(x["breadth_above_vwap"])
            breadth_pos = float(x["breadth_pos_ret"])
            stock_ret = float(x["stock_ret_pct"])
            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            rs_pct = stock_ret - market_ret
            slope_n = base.vwap_slope_atr(g, i, atr)

            above_vwap = close > vwap
            long_struct = close > open_px and close_loc >= args.min_confirm_close_loc
            market_ok = regime != "BEAR" and market_ret >= args.min_market_ret
            breadth_ok = breadth_above >= args.min_breadth_above_vwap and breadth_pos >= args.min_breadth_pos_ret

            # First evaluate retests of earlier breakout states.
            if args.min_retest_minute <= minute <= args.max_retest_minute and market_ok and breadth_ok:
                still_strong = rs_rank >= args.min_retest_rs_rank and rs_pct >= args.min_retest_rs_pct
                for b in breakouts[-3:]:
                    age = i - int(b["idx"])
                    if age < args.min_retest_bars or age > args.max_retest_bars:
                        continue
                    ref = float(b["ref"])
                    win = g.iloc[int(b["idx"]) + 1:i + 1]
                    min_low = float(win["low"].min())
                    max_high = float(win["high"].max())
                    retest_depth = (ref - min_low) / atr
                    pullback_from_high = (float(b["break_high"]) - min_low) / atr
                    close_reclaim = (close - ref) / atr
                    tagged_level = min_low <= ref + args.retest_tag_atr * atr
                    held_level = min_low >= ref - args.max_under_ref_atr * atr
                    not_overextended = vwap_dist <= args.max_retest_vwap_dist_atr
                    real_pullback = pullback_from_high >= args.min_pullback_from_high_atr
                    if (
                        tagged_level
                        and held_level
                        and real_pullback
                        and still_strong
                        and above_vwap
                        and long_struct
                        and close > ref
                        and close > float(prev["high"])
                        and close_reclaim >= args.min_close_reclaim_atr
                        and vol_ratio >= args.min_retest_vol_ratio
                        and not_overextended
                    ):
                        cand = rv._candidate(
                            str(ticker).upper(),
                            SETUP,
                            "LONG",
                            row,
                            rs_pct,
                            market_ret,
                            regime,
                            "rs_breadth_breakout_retest_hold_long",
                        )
                        score_boost = (
                            35.0 * max(rs_rank - args.min_retest_rs_rank, 0.0)
                            + 20.0 * max(min(retest_depth, 1.0), 0.0)
                            + 12.0 * max(breadth_above - 0.50, 0.0)
                            + 10.0 * max(close_reclaim, 0.0)
                        )
                        cand["quality_score"] = float(cand["quality_score"]) + score_boost
                        cand["ranker_score"] = cand["quality_score"]
                        cand["score"] = cand["quality_score"]
                        cand.update({
                            "candidate_family": "doc5b_retest_v3",
                            "selection_mode": "doc5b_retest_v3",
                            "v7_signal_sl_pct": 0.85,
                            "v7_signal_target_pct": 1.50,
                            "signal_minute": int(minute),
                            "stock_ret_pct": round(stock_ret, 6),
                            "rs_rank": round(rs_rank, 6),
                            "breadth_above_vwap": round(breadth_above, 6),
                            "breadth_pos_ret": round(breadth_pos, 6),
                            "breadth_count": int(x["breadth_count"]),
                            "breakout_ref": round(ref, 4),
                            "breakout_ref_type": str(b["ref_type"]),
                            "breakout_age_bars": int(age),
                            "breakout_strength_atr": round(float(b["break_strength"]), 6),
                            "retest_depth_atr": round(float(retest_depth), 6),
                            "pullback_from_breakout_high_atr": round(float(pullback_from_high), 6),
                            "retest_close_reclaim_atr": round(float(close_reclaim), 6),
                            "retest_max_high_since_breakout_atr": round(float((max_high - ref) / atr), 6),
                            "vwap_slope_atr": round(float(slope_n), 6) if np.isfinite(slope_n) else np.nan,
                        })
                        rows.append(cand)
                        # One candidate per breakout state is enough; later duplicate day/ticker
                        # candidates are also reduced by the repo probe gate.
                        b["used"] = True

            breakouts = [b for b in breakouts if not b.get("used") and i - int(b["idx"]) <= args.max_retest_bars]

            # Then mark fresh breakout states for future retests.
            if not (args.min_breakout_minute <= minute <= args.max_breakout_minute):
                continue
            if np.isfinite(rng) and rng > args.max_breakout_range_atr * atr:
                continue
            rh20 = float(prev_high20.iloc[i]) if np.isfinite(prev_high20.iloc[i]) else np.nan
            refs = [x for x in (or_high, rh20, prev_day_high) if np.isfinite(x)]
            if not refs:
                continue
            ref = max(refs)
            break_strength = (close - ref) / atr
            fresh_breakout = (
                close > ref
                and high > ref
                and break_strength >= args.min_breakout_strength_atr
                and close > open_px
                and close_loc >= args.min_breakout_close_loc
                and close > vwap
                and np.isfinite(slope_n)
                and slope_n >= args.min_vwap_slope_atr
                and vol_ratio >= args.min_breakout_vol_ratio
                and vwap_dist <= args.max_breakout_vwap_dist_atr
                and rs_rank >= args.min_breakout_rs_rank
                and rs_pct >= args.min_breakout_rs_pct
                and breadth_ok
                and market_ok
            )
            if fresh_breakout:
                breakouts.append({
                    "idx": int(i),
                    "ref": float(ref),
                    "ref_type": ref_type(or_high, rh20, prev_day_high, ref),
                    "break_high": float(high),
                    "break_strength": float(break_strength),
                })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parent / "pool_retest_v3"))
    ap.add_argument("--start", default="2026-05-15")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--max_tickers", type=int, default=0)
    ap.add_argument("--min_breakout_minute", type=int, default=585)
    ap.add_argument("--max_breakout_minute", type=int, default=810)
    ap.add_argument("--min_retest_minute", type=int, default=600)
    ap.add_argument("--max_retest_minute", type=int, default=840)
    ap.add_argument("--min_breakout_rs_rank", type=float, default=0.65)
    ap.add_argument("--min_retest_rs_rank", type=float, default=0.55)
    ap.add_argument("--min_breakout_rs_pct", type=float, default=0.10)
    ap.add_argument("--min_retest_rs_pct", type=float, default=0.00)
    ap.add_argument("--min_breadth_above_vwap", type=float, default=0.40)
    ap.add_argument("--min_breadth_pos_ret", type=float, default=0.38)
    ap.add_argument("--min_market_ret", type=float, default=-0.35)
    ap.add_argument("--min_vwap_slope_atr", type=float, default=0.03)
    ap.add_argument("--min_breakout_vol_ratio", type=float, default=1.25)
    ap.add_argument("--min_retest_vol_ratio", type=float, default=0.80)
    ap.add_argument("--min_breakout_close_loc", type=float, default=0.62)
    ap.add_argument("--min_confirm_close_loc", type=float, default=0.58)
    ap.add_argument("--max_breakout_vwap_dist_atr", type=float, default=3.2)
    ap.add_argument("--max_retest_vwap_dist_atr", type=float, default=2.8)
    ap.add_argument("--min_breakout_strength_atr", type=float, default=0.05)
    ap.add_argument("--max_breakout_range_atr", type=float, default=2.75)
    ap.add_argument("--min_retest_bars", type=int, default=1)
    ap.add_argument("--max_retest_bars", type=int, default=8)
    ap.add_argument("--retest_tag_atr", type=float, default=0.45)
    ap.add_argument("--max_under_ref_atr", type=float, default=0.85)
    ap.add_argument("--min_pullback_from_high_atr", type=float, default=0.25)
    ap.add_argument("--min_close_reclaim_atr", type=float, default=0.00)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    rv.START_DATE = pd.Timestamp(args.start)
    rv.END_DATE = pd.Timestamp(args.end)
    rv.SETUP_TIERS[SETUP] = "doc5b_retest_v3"
    rv.PROBE_EXIT_RULES[SETUP] = (0.85, 1.50)

    out_dir = Path(args.out)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    market_ctx = rv._market_context()
    if not market_ctx:
        print("[doc5b-retest-v3] FATAL: no market context")
        return 1

    universe = base.load_universe(args.max_tickers)
    print(f"[doc5b-retest-v3] universe={len(universe)} window={args.start}..{args.end}", flush=True)
    t0 = time.time()
    frames = base.load_frames(universe)
    print(f"[doc5b-retest-v3] loaded_frames={len(frames)} elapsed={time.time() - t0:.1f}s", flush=True)
    xsec = base.build_xsec(frames)
    print(f"[doc5b-retest-v3] xsec_records={len(xsec)} elapsed={time.time() - t0:.1f}s", flush=True)

    rows: list[dict] = []
    for idx, (ticker, df) in enumerate(frames.items(), 1):
        rows.extend(scan_ticker(ticker, df, xsec, market_ctx, args))
        if idx % 25 == 0 or idx == len(frames):
            print(
                f"[doc5b-retest-v3] {idx}/{len(frames)} last={ticker} rows={len(rows)} elapsed={time.time() - t0:.1f}s",
                flush=True,
            )

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(out_csv, index=False)
    print(f"[doc5b-retest-v3] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        print(raw["setup"].value_counts().to_string(), flush=True)
        print(
            raw[[
                "rs_rank",
                "breadth_above_vwap",
                "retest_depth_atr",
                "pullback_from_breakout_high_atr",
                "breakout_age_bars",
            ]].describe().to_string(),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
