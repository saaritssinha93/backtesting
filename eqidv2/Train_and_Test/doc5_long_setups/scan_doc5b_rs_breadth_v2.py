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

import research_v11_tier123_new_setups as rv  # noqa: E402


SETUP = "DOC5B_MOMO_BREAKOUT_LONG"
MIN_CLOSE = 80.0
MIN_DAY_VALUE_RS = 20_000_000.0
SLOPE_LOOKBACK = 5
SLOPE_MIN = 0.05


def finite(*xs) -> bool:
    return all(np.isfinite(x) for x in xs)


def vwap_slope_atr(g: pd.DataFrame, i: int, atr: float) -> float:
    if i - SLOPE_LOOKBACK < 0 or not (np.isfinite(atr) and atr > 0):
        return np.nan
    v_now = float(g["VWAP"].iloc[i])
    v_prev = float(g["VWAP"].iloc[i - SLOPE_LOOKBACK])
    if not finite(v_now, v_prev):
        return np.nan
    return (v_now - v_prev) / atr


def load_universe(max_tickers: int) -> list[str]:
    universe = [
        str(x).upper()
        for x in rv._load_probe_universe()
        if not str(x).upper().startswith("NIFTY")
        and not str(x).upper().endswith("BEES")
        and (rv.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    if max_tickers > 0:
        universe = universe[:max_tickers]
    return universe


def load_frames(universe: list[str]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for ticker in universe:
        df = rv._read_5m(ticker)
        if df is None or df.empty:
            continue
        df = rv._prev_day_levels(df)
        frames[ticker] = df
    return frames


def build_xsec(frames: dict[str, pd.DataFrame]) -> dict[tuple[str, pd.Timestamp], dict]:
    rows = []
    for ticker, df in frames.items():
        work = df[["date", "date_only", "open", "close", "VWAP"]].copy()
        work["ticker"] = ticker
        day_open = work.groupby("date_only")["open"].transform("first").replace(0, np.nan)
        work["stock_ret_pct"] = (work["close"] / day_open - 1.0) * 100.0
        work["above_vwap"] = work["close"] > work["VWAP"]
        work["pos_ret"] = work["stock_ret_pct"] > 0
        rows.append(work[["ticker", "date", "stock_ret_pct", "above_vwap", "pos_ret"]])
    if not rows:
        return {}
    all_bars = pd.concat(rows, ignore_index=True)
    all_bars = all_bars.dropna(subset=["date", "stock_ret_pct"])
    all_bars["rs_rank"] = all_bars.groupby("date")["stock_ret_pct"].rank(method="average", pct=True)
    breadth = all_bars.groupby("date").agg(
        breadth_above_vwap=("above_vwap", "mean"),
        breadth_pos_ret=("pos_ret", "mean"),
        breadth_count=("ticker", "nunique"),
    )
    all_bars = all_bars.join(breadth, on="date")
    return (
        all_bars.set_index(["ticker", "date"])[
            ["stock_ret_pct", "rs_rank", "breadth_above_vwap", "breadth_pos_ret", "breadth_count"]
        ]
        .to_dict("index")
    )


def scan_ticker(ticker: str, df: pd.DataFrame, xsec: dict, market_ctx: dict, args) -> list[dict]:
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 20:
            continue
        day_open = float(g["open"].iloc[0])
        or_high, _or_low = rv.v2._opening_range(g)
        prev_day_high = float(g.get("prev_day_high", pd.Series(np.nan, index=g.index)).iloc[0])
        prev_high20 = g["high"].shift(1).rolling(20, min_periods=8).max()

        for i in range(max(SLOPE_LOOKBACK, 6), len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = rv._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if not (585 <= minute <= 810):
                continue

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
            if not (finite(atr, vwap, vol_ratio, close_loc, vwap_dist) and atr > 0):
                continue
            if close < MIN_CLOSE or float(row.get("day_value_so_far_rs", 0.0)) < MIN_DAY_VALUE_RS:
                continue
            if np.isfinite(rng) and rng > args.max_range_atr * atr:
                continue

            x = xsec.get((ticker, ts))
            if not x:
                continue
            rs_rank = float(x["rs_rank"])
            breadth_above = float(x["breadth_above_vwap"])
            breadth_pos = float(x["breadth_pos_ret"])
            stock_ret = float(x["stock_ret_pct"])
            if rs_rank < args.min_rs_rank:
                continue
            if breadth_above < args.min_breadth_above_vwap or breadth_pos < args.min_breadth_pos_ret:
                continue

            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            if regime == "BEAR" or market_ret < args.min_market_ret:
                continue
            rs_pct = stock_ret - market_ret

            slope_n = vwap_slope_atr(g, i, atr)
            if not (np.isfinite(slope_n) and slope_n >= args.min_vwap_slope_atr):
                continue
            if not (close > vwap and close > open_px and close > float(prev["close"])):
                continue
            if close_loc < args.min_close_loc or vol_ratio < args.min_vol_ratio:
                continue
            if vwap_dist > args.max_vwap_dist_atr:
                continue

            rh20 = float(prev_high20.iloc[i]) if np.isfinite(prev_high20.iloc[i]) else np.nan
            refs = [x for x in (or_high, rh20, prev_day_high) if np.isfinite(x)]
            if not refs:
                continue
            break_ref = max(refs)
            breakout_strength = (close - break_ref) / atr
            if close <= break_ref or breakout_strength < args.min_breakout_strength_atr:
                continue
            if high <= break_ref:
                continue
            # Avoid a pure upper-wick poke through the reference.
            if low > break_ref and close_loc < max(args.min_close_loc, 0.70):
                continue

            cand = rv._candidate(
                str(ticker).upper(),
                SETUP,
                "LONG",
                row,
                rs_pct,
                market_ret,
                regime,
                "rs_rank_breadth_momentum_breakout_long",
            )
            score_boost = (
                45.0 * max(rs_rank - args.min_rs_rank, 0.0)
                + 18.0 * max(breadth_above - 0.50, 0.0)
                + 12.0 * max(breadth_pos - 0.50, 0.0)
                + 8.0 * min(max(breakout_strength, 0.0), 2.0)
            )
            cand["quality_score"] = float(cand["quality_score"]) + score_boost
            cand["ranker_score"] = cand["quality_score"]
            cand["score"] = cand["quality_score"]
            cand.update({
                "candidate_family": "doc5_long_setups_rs_breadth_v2",
                "selection_mode": "doc5b_rs_breadth_v2",
                "v7_signal_sl_pct": 0.85,
                "v7_signal_target_pct": 1.50,
                "signal_minute": int(minute),
                "stock_ret_pct": round(stock_ret, 6),
                "rs_rank": round(rs_rank, 6),
                "breadth_above_vwap": round(breadth_above, 6),
                "breadth_pos_ret": round(breadth_pos, 6),
                "breadth_count": int(x["breadth_count"]),
                "vwap_slope_atr": round(float(slope_n), 6),
                "breakout_strength_atr": round(float(breakout_strength), 6),
                "breakout_ref": round(float(break_ref), 4),
                "orh_dist_atr": round(float((close - or_high) / atr), 6) if np.isfinite(or_high) else np.nan,
                "pdh_dist_atr": round(float((close - prev_day_high) / atr), 6) if np.isfinite(prev_day_high) else np.nan,
                "prev20_dist_atr": round(float((close - rh20) / atr), 6) if np.isfinite(rh20) else np.nan,
            })
            rows.append(cand)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parent / "pool_rs_breadth_v2"))
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--max_tickers", type=int, default=0)
    ap.add_argument("--min_rs_rank", type=float, default=0.65)
    ap.add_argument("--min_breadth_above_vwap", type=float, default=0.42)
    ap.add_argument("--min_breadth_pos_ret", type=float, default=0.40)
    ap.add_argument("--min_market_ret", type=float, default=-0.35)
    ap.add_argument("--min_vwap_slope_atr", type=float, default=SLOPE_MIN)
    ap.add_argument("--min_vol_ratio", type=float, default=1.25)
    ap.add_argument("--min_close_loc", type=float, default=0.60)
    ap.add_argument("--max_vwap_dist_atr", type=float, default=3.0)
    ap.add_argument("--min_breakout_strength_atr", type=float, default=0.0)
    ap.add_argument("--max_range_atr", type=float, default=2.75)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    rv.START_DATE = pd.Timestamp(args.start)
    rv.END_DATE = pd.Timestamp(args.end)
    rv.SETUP_TIERS[SETUP] = "doc5_long_setups_rs_breadth_v2"
    rv.PROBE_EXIT_RULES[SETUP] = (0.85, 1.50)

    out_dir = Path(args.out)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    market_ctx = rv._market_context()
    if not market_ctx:
        print("[doc5b-v2] FATAL: no market context")
        return 1

    universe = load_universe(args.max_tickers)
    print(f"[doc5b-v2] universe={len(universe)} window={args.start}..{args.end}", flush=True)
    t0 = time.time()
    frames = load_frames(universe)
    print(f"[doc5b-v2] loaded_frames={len(frames)} elapsed={time.time() - t0:.1f}s", flush=True)
    xsec = build_xsec(frames)
    print(f"[doc5b-v2] xsec_records={len(xsec)} elapsed={time.time() - t0:.1f}s", flush=True)

    rows: list[dict] = []
    for idx, (ticker, df) in enumerate(frames.items(), 1):
        rows.extend(scan_ticker(ticker, df, xsec, market_ctx, args))
        if idx % 25 == 0 or idx == len(frames):
            print(
                f"[doc5b-v2] {idx}/{len(frames)} last={ticker} rows={len(rows)} elapsed={time.time() - t0:.1f}s",
                flush=True,
            )

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(out_csv, index=False)
    print(f"[doc5b-v2] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        print(raw["setup"].value_counts().to_string(), flush=True)
        print(raw[["rs_rank", "breadth_above_vwap", "breadth_pos_ret", "breakout_strength_atr"]].describe().to_string(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
