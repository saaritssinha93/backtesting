r"""reinvent_doc5d_scan.py — REDESIGNED AVWAP-reclaim LONG detector, MULTI-VARIANT.
============================================================================
Research-only. Standalone raw-5min-parquet scan. NO live v2/v11 engine edits,
NO conf edits, NO live trades. Emits, in ONE pass over the full parquet
universe (parquet I/O dominates; extra rule-packs per bar are ~free), a graded
family of reclaim detectors so we can screen count vs quality before spending a
full Optuna PF-band run on the winner.

Idea (unchanged from the doc's Setup D): a stock below session VWAP reclaims it
= catch the turn into an uptrend. The raw doc rule fires on the first up-bar back
through VWAP and is a 21%-win, 65%-stop loser on the recent window. These graded
variants demand progressively more CONFIRMATION (held reclaim, strong body,
momentum thrust, leadership, supportive regime) to lift the base win-rate.

Variants (loose -> strict); each emitted under setup = f"DOC5D_RECLAIM_{V}":
  vA  held reclaim + light trend (close>EMA20) + modest volume/leader
  vB  vA + stronger close/body + near-value + rising VWAP slope
  vC  vB + volume 1.5 + prior-bar-high thrust (or clear reclaim) + EMA stack
  vD  vC + BULL/TREND regime lean + strong leader + RSI band (highest quality)
The winning variant is later renamed to DOC5D_AVWAP_RECLAIM_LONG for the loop.

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/scripts/reinvent_doc5d_scan.py \
     --start 2026-05-01 --end 2026-06-30
"""
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

SLOPE_LOOKBACK = 5
MIN_CLOSE = 80.0
MIN_DAY_VALUE_RS = 20_000_000.0
VARIANTS = ["vA", "vB", "vC", "vD"]
# default (doc-suggested) exit; the loop re-tunes SL/Tgt regardless
DEFAULT_EXIT = (1.0, 1.5)


def _finite(*xs) -> bool:
    return all(np.isfinite(x) for x in xs)


def _slope_atr(g: pd.DataFrame, col: str, i: int, atr: float, n: int) -> float:
    if i - n < 0 or not (np.isfinite(atr) and atr > 0) or col not in g.columns:
        return np.nan
    a = float(g[col].iloc[i]); b = float(g[col].iloc[i - n])
    if not _finite(a, b):
        return np.nan
    return (a - b) / atr


def _matches(f: dict) -> list[str]:
    """Return the list of variants bar `f` satisfies (nested loose->strict)."""
    out = []
    base = (f["fresh_reclaim"] and f["up_close"] and f["close"] > f["open_px"]
            and 585 <= f["minute"] <= 780
            and f["regime"] != "BEAR")
    if not base:
        return out
    # vA — held reclaim + light trend
    vA = (f["close_loc"] >= 0.58 and f["vol_ratio"] >= 1.35 and f["rs_pct"] > 0.05
          and np.isfinite(f["atr"]) and f["low"] >= f["vwap"] - 0.45 * f["atr"]
          and f["market_ret"] >= -0.30 and np.isfinite(f["rng"]) and f["rng"] <= 2.3 * f["atr"]
          and np.isfinite(f["ema20"]) and f["close"] > f["ema20"])
    if not vA:
        return out
    out.append("vA")
    # vB — stronger close/body + near value + rising slope
    vB = (f["close_loc"] >= 0.62 and np.isfinite(f["body_pct"]) and f["body_pct"] >= 0.35
          and np.isfinite(f["vwap_dist"]) and f["vwap_dist"] <= 1.2
          and np.isfinite(f["slope_n"]) and f["slope_n"] >= 0.0)
    if not vB:
        return out
    out.append("vB")
    # vC — volume 1.5 + thrust/clear-reclaim + EMA stack
    reclaim_atr = (f["close"] - f["vwap"]) / f["atr"] if (np.isfinite(f["atr"]) and f["atr"] > 0) else np.nan
    vC = (f["vol_ratio"] >= 1.5
          and (f["close"] > f["prev_high"] or (np.isfinite(reclaim_atr) and reclaim_atr >= 0.05))
          and np.isfinite(f["ema50"]) and f["ema20"] >= 0.995 * f["ema50"]
          and f["market_ret"] >= -0.15 and np.isfinite(f["rng"]) and f["rng"] <= 2.0 * f["atr"])
    if not vC:
        return out
    out.append("vC")
    # vD — regime lean + strong leader + RSI band
    vD = (f["regime"] in ("BULL", "TREND") and f["rs_pct"] > 0.20
          and np.isfinite(f["rsi"]) and 50.0 <= f["rsi"] <= 72.0
          and np.isfinite(f["vwap_dist"]) and f["vwap_dist"] <= 0.85)
    if vD:
        out.append("vD")
    return out


def _scan_ticker(ticker: str, market_ctx: dict) -> list[dict]:
    df = rv._read_5m(ticker)
    if df is None or df.empty:
        return []
    df = rv._prev_day_levels(df)
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 20:
            continue
        day_open = float(g["open"].iloc[0])
        for i in range(max(SLOPE_LOOKBACK, 6), len(g) - 1):
            row = g.iloc[i]; prev = g.iloc[i - 1]
            ts = rv._normalise_ts(row["date"]); minute = ts.hour * 60 + ts.minute
            if minute < 585 or minute > 780:
                continue
            close = float(row["close"]); vwap = float(row.get("VWAP", np.nan))
            atr = float(row.get("ATR", np.nan))
            if not (_finite(atr) and atr > 0 and np.isfinite(vwap)):
                continue
            if close < MIN_CLOSE or float(row.get("day_value_so_far_rs", 0.0)) < MIN_DAY_VALUE_RS:
                continue
            prev_close = float(prev["close"]); prev_vwap = float(prev.get("VWAP", np.nan))
            if not np.isfinite(prev_vwap):
                continue
            fresh_reclaim = prev_close <= prev_vwap and close > vwap
            up_close = close > prev_close
            if not (fresh_reclaim and up_close):
                continue
            vol_ratio = float(row.get("vol_ratio", np.nan))
            if not np.isfinite(vol_ratio):
                continue
            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            f = {
                "close": close, "open_px": float(row["open"]), "high": float(row["high"]),
                "low": float(row["low"]), "prev_high": float(prev["high"]), "vwap": vwap,
                "atr": atr, "ema20": float(row.get("EMA_20", np.nan)),
                "ema50": float(row.get("EMA_50", np.nan)), "adx": float(row.get("ADX", np.nan)),
                "rsi": float(row.get("RSI", np.nan)), "vol_ratio": vol_ratio,
                "body_pct": float(row.get("body_pct", np.nan)),
                "close_loc": float(row.get("close_loc", np.nan)),
                "vwap_dist": float(row.get("vwap_dist_atr", np.nan)),
                "rng": float(row.get("range", np.nan)),
                "rs_pct": stock_ret - market_ret, "market_ret": market_ret, "regime": regime,
                "minute": minute, "fresh_reclaim": fresh_reclaim, "up_close": up_close,
                "slope_n": _slope_atr(g, "VWAP", i, atr, SLOPE_LOOKBACK),
                "ema20_slope": _slope_atr(g, "EMA_20", i, atr, 3),
            }
            matched = _matches(f)
            if not matched:
                continue
            lw = float(row.get("lower_wick_pct", np.nan)); uw = float(row.get("upper_wick_pct", np.nan))
            for V in matched:
                setup = f"DOC5D_RECLAIM_{V}"
                cand = rv._candidate(str(ticker).upper(), setup, "LONG", row, f["rs_pct"],
                                     market_ret, regime, f"reinvented_confirmed_vwap_reclaim_long_{V}")
                cand.update({
                    "candidate_family": "doc5_reinvent", "reclaim_variant": V,
                    "v7_signal_sl_pct": DEFAULT_EXIT[0], "v7_signal_target_pct": DEFAULT_EXIT[1],
                    "signal_minute": int(minute),
                    "upper_wick_pct": round(uw, 6) if np.isfinite(uw) else np.nan,
                    "lower_wick_pct": round(lw, 6) if np.isfinite(lw) else np.nan,
                    "signal_range_pct": round(float((f["high"] - f["low"]) / close * 100.0), 6),
                    "wick_skew_pct": round(float(lw - uw), 6) if _finite(lw, uw) else np.nan,
                    "rsi_sig": round(f["rsi"], 4) if np.isfinite(f["rsi"]) else np.nan,
                    "adx_sig": round(f["adx"], 4) if np.isfinite(f["adx"]) else np.nan,
                    "ema20_slope_atr": round(f["ema20_slope"], 6) if np.isfinite(f["ema20_slope"]) else np.nan,
                    "vwap_slope_atr": round(f["slope_n"], 6) if np.isfinite(f["slope_n"]) else np.nan,
                    "reclaim_atr": round(float((close - vwap) / atr), 6),
                })
                rows.append(cand)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-05-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--max_tickers", type=int, default=0)
    ap.add_argument("--out", default=str(HERE.parent.parent / "pool_reinvent"))
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    rv.START_DATE = pd.Timestamp(args.start); rv.END_DATE = pd.Timestamp(args.end)
    for V in VARIANTS:
        rv.SETUP_TIERS[f"DOC5D_RECLAIM_{V}"] = "doc5_reinvent"
        rv.PROBE_EXIT_RULES[f"DOC5D_RECLAIM_{V}"] = DEFAULT_EXIT

    market_ctx = rv._market_context()
    if not market_ctx:
        print("[reinvent] FATAL: no NIFTYBEES market context; abort."); return 1

    universe = sorted({
        p.name.replace("_stocks_indicators_5min.parquet", "").upper()
        for p in rv.DATA_ROOT.glob("*_stocks_indicators_5min.parquet")
    })
    universe = [t for t in universe if not t.startswith("NIFTY") and not t.endswith("BEES")]
    if args.max_tickers > 0:
        universe = universe[: args.max_tickers]
    print(f"[reinvent] MULTI-VARIANT {VARIANTS} universe={len(universe)} "
          f"window={args.start}..{args.end}", flush=True)

    t0 = time.time(); rows: list[dict] = []
    for idx, ticker in enumerate(universe, 1):
        rows.extend(_scan_ticker(ticker, market_ctx))
        if idx % 100 == 0 or idx == len(universe):
            print(f"[reinvent] {idx}/{len(universe)} last={ticker} rows={len(rows)} "
                  f"elapsed={time.time() - t0:.1f}s", flush=True)

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)
    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    raw.to_csv(out_csv, index=False)
    print(f"[reinvent] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        d = raw.copy(); d["day"] = pd.to_datetime(d["signal_time_ist"], errors="coerce").dt.date
        d["dstr"] = d["day"].astype(str)
        print("\n-- per variant: TRAIN(05-18..06-19) / TEST(06-20..) rows & sessions --", flush=True)
        for V in VARIANTS:
            s = f"DOC5D_RECLAIM_{V}"; dv = d[d["setup"] == s]
            tr = dv[dv["dstr"].between("2026-05-18", "2026-06-19")]
            te = dv[dv["dstr"] >= "2026-06-20"]
            print(f"  {s:18s} total={len(dv):4d}  TRAIN={len(tr):3d}/{tr['day'].nunique():2d}sess  "
                  f"TEST={len(te):3d}/{te['day'].nunique():2d}sess", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
