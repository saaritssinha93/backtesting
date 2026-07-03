r"""scan_doc5c_reinvent.py — REINVENT DOC5C from a breakout-chase into enter-at-value gap longs.
============================================================================
Research-only standalone raw-5min scan (NO edits to live v2/v11 engine, NO conf
edits, NO live trades). Emits a per-setup pool in the exact
`historical_all_available_pre_dedupe_live_candidates.csv` format that
setup_train_test.load_pool()/pf_band_fitval_loop.py consume.

WHY REINVENT
------------
The original DOC5C_ORB_GAP_GO_LONG buys the ORB gap breakout CONTINUATION. On a
5-min-only next-open fill that means entering ~5 min INTO an already-extended move,
which reverts (raw TRAIN PF 0.20 / TEST 0.14; see PARAMETER_SWEEP_SUMMARY.md). The
fix is structural: keep the "controlled gap-up" DNA but change the TRIGGER so the
next-open fill lands at VALUE, not deep in the move.

Three reinvented LONG variants (each a NEW distinct name):
  R1  DOC5C_GAP_RETEST_HOLD_LONG   gap-up that BROKE the ORH earlier, pulls back to
                                   retest the ORH level, and HOLDS above it (buy the
                                   hold, not the break). Enters near ORH = at value.
  R2  DOC5C_GAP_RECLAIM_LONG       gap-up that faded UNDER session VWAP (shakeout),
                                   then RECLAIMS VWAP on an up-bar (buy the reclaim).
  R3  DOC5C_GAP_PULLBACK_HOLD_LONG gap-up in an EMA-stacked uptrend that pulls back to
                                   VWAP/EMA20 and turns up (Setup-A pullback on gappers).

All three fill at the next 1-min open after the signal bar (the repo's honest
`next_open`), and the downstream loop tunes exits/masks/guards + validates TRAIN/TEST.

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5C_ORB_GAP_GO_LONG/scripts/scan_doc5c_reinvent.py --start 2026-04-01 --end 2026-06-30
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
for _ in range(12):
    if (REPO_ROOT / "research_v11_tier123_new_setups.py").exists():
        break
    REPO_ROOT = REPO_ROOT.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import research_v11_tier123_new_setups as rv  # noqa: E402

OUTDIR = HERE.parent.parent / "reinvent_pool"

SETUPS = {
    "DOC5C_GAP_RETEST_HOLD_LONG": (0.70, 1.25),
    "DOC5C_GAP_RECLAIM_LONG": (0.70, 1.25),
    "DOC5C_GAP_PULLBACK_HOLD_LONG": (0.70, 1.25),
}

MIN_CLOSE = 80.0
MIN_DAY_VALUE_RS = 20_000_000.0     # Rs 2cr traded-so-far liquidity floor
GAP_MIN, GAP_MAX = 0.5, 4.0         # controlled gap band (doc gap_min/gap_max), NOT exhaustion
SLOPE_LB = 5


def _f(*xs) -> bool:
    return all(np.isfinite(x) for x in xs)


def _vwap_slope(g: pd.DataFrame, i: int, atr: float) -> float:
    if i - SLOPE_LB < 0 or not (np.isfinite(atr) and atr > 0):
        return np.nan
    a, b = float(g["VWAP"].iloc[i]), float(g["VWAP"].iloc[i - SLOPE_LB])
    return (a - b) / atr if _f(a, b) else np.nan


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
        or_high, or_low = rv.v2._opening_range(g)
        pdc = float(g.get("Prev_Day_Close", pd.Series(np.nan, index=g.index)).iloc[0])
        if not np.isfinite(pdc):
            pdc = float(g.get("prev_day_close_calc", pd.Series(np.nan, index=g.index)).iloc[0])
        gap_pct = (day_open / pdc - 1.0) * 100.0 if (np.isfinite(pdc) and pdc > 0) else np.nan
        if not (np.isfinite(gap_pct) and GAP_MIN <= gap_pct <= GAP_MAX):
            continue  # controlled-gap universe only (all three variants)
        # cumulative max high BEFORE the current bar -> "already broke the ORH earlier"
        prior_high_max = g["high"].shift(1).cummax()

        for i in range(max(SLOPE_LB, 6), len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = rv._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute

            close = float(row["close"]); open_px = float(row["open"])
            atr = float(row.get("ATR", np.nan)); vwap = float(row.get("VWAP", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan)); close_loc = float(row.get("close_loc", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            ema20 = float(row.get("EMA_20", np.nan)); ema50 = float(row.get("EMA_50", np.nan))
            ema20_prev3 = float(g["EMA_20"].iloc[i - 3]) if i >= 3 and "EMA_20" in g.columns else np.nan
            adx = float(row.get("ADX", np.nan)); rsi = float(row.get("RSI", np.nan))
            rng = float(row.get("range", np.nan))
            low_i = float(row["low"])

            if not (_f(atr) and atr > 0 and _f(vwap, close_loc, vol_ratio)):
                continue
            if close < MIN_CLOSE or float(row.get("day_value_so_far_rs", 0.0)) < MIN_DAY_VALUE_RS:
                continue
            if np.isfinite(rng) and rng > 2.75 * atr:      # skip climax bars for all variants
                continue

            prev_close = float(prev["close"]); prev_vwap = float(prev.get("VWAP", np.nan))
            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            if regime == "BEAR" or market_ret < -0.35:
                continue
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            slope_n = _vwap_slope(g, i, atr)
            broke_orh = np.isfinite(or_high) and np.isfinite(prior_high_max.iloc[i]) and prior_high_max.iloc[i] > or_high
            orh_dist = (close - or_high) / atr if np.isfinite(or_high) else np.nan
            retest_depth = (or_high - low_i) / atr if np.isfinite(or_high) else np.nan
            above_vwap = close > vwap
            up_close = close > prev_close
            long_body = close > open_px
            ema_stack = _f(ema20, ema50) and close > ema20 and ema20 >= ema50
            ema_slope = (ema20 - ema20_prev3) if _f(ema20, ema20_prev3) else np.nan
            lo3 = float(g["low"].iloc[max(0, i - 3):i + 1].min())
            dipped_value = np.isfinite(lo3) and lo3 <= vwap + 0.15 * atr

            fired: list[tuple[str, str]] = []

            # R1 — ORH retest-and-hold (buy the hold near value, not the break)
            if 585 <= minute <= 750:
                if (broke_orh and above_vwap and long_body and close > or_high
                        and low_i <= or_high + 0.25 * atr           # pulled back to retest the level
                        and np.isfinite(orh_dist) and orh_dist <= 1.25   # not extended above ORH
                        and close_loc >= 0.55 and vol_ratio >= 1.1 and rs_pct > 0.0):
                    fired.append(("DOC5C_GAP_RETEST_HOLD_LONG", "gap_orh_retest_then_hold_long"))

            # R2 — gap VWAP reclaim after shakeout (buy the reclaim)
            if 585 <= minute <= 780:
                fresh_reclaim = np.isfinite(prev_vwap) and prev_close < prev_vwap and above_vwap
                if (fresh_reclaim and long_body and up_close and close_loc >= 0.60
                        and stock_ret > 0.0 and vol_ratio >= 1.3 and rs_pct > 0.0):
                    fired.append(("DOC5C_GAP_RECLAIM_LONG", "gap_vwap_reclaim_from_below_long"))

            # R3 — gap pullback-to-value hold in an EMA-stacked uptrend (Setup-A on gappers)
            if 585 <= minute <= 780:
                if (ema_stack and above_vwap and long_body and up_close and dipped_value
                        and close_loc >= 0.60 and np.isfinite(vwap_dist) and vwap_dist <= 1.5
                        and vol_ratio >= 1.1 and rs_pct > 0.2):
                    fired.append(("DOC5C_GAP_PULLBACK_HOLD_LONG", "gap_pullback_to_value_hold_long"))

            for setup, reason in fired:
                cand = rv._candidate(str(ticker).upper(), setup, "LONG", row, rs_pct, market_ret, regime, reason)
                ex = SETUPS[setup]
                cand.update({
                    "candidate_family": "doc5c_reinvent",
                    "v7_signal_sl_pct": ex[0],
                    "v7_signal_target_pct": ex[1],
                    "signal_minute": int(minute),
                    "gap_pct": round(float(gap_pct), 4),
                    "orh_dist_atr": round(float(orh_dist), 4) if np.isfinite(orh_dist) else np.nan,
                    "retest_depth_atr": round(float(retest_depth), 4) if np.isfinite(retest_depth) else np.nan,
                    "vwap_slope_atr": round(float(slope_n), 6) if np.isfinite(slope_n) else np.nan,
                    "adx": round(float(adx), 4) if np.isfinite(adx) else np.nan,
                    "rsi": round(float(rsi), 4) if np.isfinite(rsi) else np.nan,
                    "ema20_slope_3bar": round(float(ema_slope), 6) if np.isfinite(ema_slope) else np.nan,
                    "stock_ret_pct": round(float(stock_ret), 4),
                })
                rows.append(cand)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(OUTDIR))
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--max_tickers", type=int, default=0)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    rv.START_DATE = pd.Timestamp(args.start)
    rv.END_DATE = pd.Timestamp(args.end)
    for s, ex in SETUPS.items():
        rv.SETUP_TIERS[s] = "doc5c_reinvent"
        rv.PROBE_EXIT_RULES[s] = ex

    out_dir = Path(args.out)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"

    market_ctx = rv._market_context()
    if not market_ctx:
        print("[reinvent-scan] FATAL: no NIFTYBEES market context; abort.")
        return 1

    universe = [
        str(x).upper() for x in rv._load_probe_universe()
        if not str(x).upper().startswith("NIFTY") and not str(x).upper().endswith("BEES")
        and (rv.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    if args.max_tickers > 0:
        universe = universe[: args.max_tickers]
    print(f"[reinvent-scan] universe={len(universe)} window={args.start}..{args.end}", flush=True)

    t0 = time.time()
    rows: list[dict] = []
    for idx, ticker in enumerate(universe, 1):
        rows.extend(_scan_ticker(ticker, market_ctx))
        if idx % 25 == 0 or idx == len(universe):
            print(f"[reinvent-scan] {idx}/{len(universe)} last={ticker} rows={len(rows)} "
                  f"elapsed={time.time()-t0:.1f}s", flush=True)

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(out_csv, index=False)
    print(f"[reinvent-scan] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        print("\n-- rows per setup --", flush=True)
        print(raw["setup"].value_counts().to_string(), flush=True)
        m = pd.to_datetime(raw["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m")
        print("\n-- rows per setup x month --", flush=True)
        print(raw.assign(_m=m).groupby(["setup", "_m"]).size().to_string(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
