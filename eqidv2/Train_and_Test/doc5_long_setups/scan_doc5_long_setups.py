r"""scan_doc5_long_setups.py — mine the four LONG archetypes from 5min_long_setups.md
============================================================================
Research-only. Standalone raw-5min-parquet scan (NO edits to the live v2/v11
engine, NO conf edits, NO live trades). Emits a per-setup pool in the exact
`historical_all_available_pre_dedupe_live_candidates.csv` format that
setup_train_test.load_pool() / pf_band_fitval_loop.py consume, so each of the
four setups can then be run through the honest TRAIN/TEST approval loop.

The four archetypes (each given a NEW distinct name so they never collide with
the existing catalog detectors they resemble):

  DOC5A_AVWAP_PULLBACK_LONG   Setup A — buy a controlled dip back to value in an
                              established, rising-VWAP uptrend (best R:R workhorse)
  DOC5B_MOMO_BREAKOUT_LONG    Setup B — join the strongest movers breaking a new
                              intraday / prior-day high on volume expansion
  DOC5C_ORB_GAP_GO_LONG       Setup C — controlled gap-up that holds and breaks the
                              opening range (early-session trend-day signature)
  DOC5D_AVWAP_RECLAIM_LONG    Setup D — stock reclaims session VWAP from below,
                              catching the turn (earlier, looser RS)

FAITHFULNESS NOTES (read before trusting the pool):
  * RS: the doc uses a cross-sectional `rs_rank` percentile (0..1) across ~1200
    names. This scan uses the repo's `rs_pct` = stock_ret - index_ret (return vs
    NIFTYBEES) as the RS proxy — the same approximation the production engine uses.
    A true per-timestamp percentile rank would need a two-pass cross-sectional
    scan (TODO / v2).
  * BREADTH: the doc's `breadth_t > 0.50` gate is NOT modelled. We use the
    NIFTYBEES VWAP `regime` (BULL/BEAR/TREND/NEUTRAL) + `market_ret` as the
    regime proxy (regime != BEAR, market_ret not deeply negative).
  * EXEC MODE: the downstream loop fills at the next 1-min open after the 5-min
    signal + slippage. That IS the doc's honest `next_open` default. The doc's
    `stop_confirm` / `limit_better` conditional fills are NOT modelled here.
  * BAND: the doc's anchored-VWAP std-dev band `lower_b` is approximated by
    "recent low dipped back to session VWAP" for the pullback trigger.

Run from repo root, e.g.:
  py -3.12 Train_and_Test/doc5_long_setups/scan_doc5_long_setups.py \
      --start 2026-04-01 --end 2026-06-30
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

# --- setup names + default exit brackets (doc-suggested; the loop re-tunes exits) ---
SETUPS = {
    "DOC5A_AVWAP_PULLBACK_LONG": (0.70, 1.25),
    "DOC5B_MOMO_BREAKOUT_LONG": (0.85, 1.50),
    "DOC5C_ORB_GAP_GO_LONG": (0.85, 1.50),
    "DOC5D_AVWAP_RECLAIM_LONG": (0.70, 1.25),
}

# shared gates (tuned once, globally — doc §2)
MIN_CLOSE = 80.0
MIN_DAY_VALUE_RS = 20_000_000.0     # Rs 2cr traded-so-far liquidity floor
SLOPE_LOOKBACK = 5                  # bars for the ATR-normalised VWAP slope
SLOPE_MIN = 0.05                    # min rising-VWAP slope in ATR units (doc slope_min)


def _finite(*xs) -> bool:
    return all(np.isfinite(x) for x in xs)


def _vwap_slope_atr(g: pd.DataFrame, i: int, atr: float) -> float:
    """ATR-normalised session-VWAP slope over SLOPE_LOOKBACK bars (doc slope_n)."""
    if i - SLOPE_LOOKBACK < 0 or not (np.isfinite(atr) and atr > 0):
        return np.nan
    v_now = float(g["VWAP"].iloc[i])
    v_prev = float(g["VWAP"].iloc[i - SLOPE_LOOKBACK])
    if not _finite(v_now, v_prev):
        return np.nan
    return (v_now - v_prev) / atr


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
        prev_day_close = float(g.get("Prev_Day_Close", pd.Series(np.nan, index=g.index)).iloc[0])
        if not np.isfinite(prev_day_close):
            prev_day_close = float(g.get("prev_day_close_calc", pd.Series(np.nan, index=g.index)).iloc[0])
        prev_day_high = float(g.get("prev_day_high", pd.Series(np.nan, index=g.index)).iloc[0])
        gap_pct = (day_open / prev_day_close - 1.0) * 100.0 if (np.isfinite(prev_day_close) and prev_day_close > 0) else np.nan

        # rolling 20-bar prior high/low (known at bar i, uses bars < i)
        prev_high20 = g["high"].shift(1).rolling(20, min_periods=8).max()

        for i in range(max(SLOPE_LOOKBACK, 6), len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = rv._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute

            close = float(row["close"])
            open_px = float(row["open"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            ema20 = float(row.get("EMA_20", np.nan))
            ema50 = float(row.get("EMA_50", np.nan))
            rng = float(row.get("range", np.nan))

            if not (_finite(atr) and atr > 0 and _finite(vwap, close_loc, vol_ratio)):
                continue
            # shared liquidity gate
            if close < MIN_CLOSE or float(row.get("day_value_so_far_rs", 0.0)) < MIN_DAY_VALUE_RS:
                continue

            prev_close = float(prev["close"])
            prev_vwap = float(prev.get("VWAP", np.nan))
            prev_bar_high = float(prev["high"])
            rh20 = float(prev_high20.iloc[i]) if np.isfinite(prev_high20.iloc[i]) else np.nan

            # shared regime proxy (doc GATE_regime, breadth not modelled)
            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            if regime == "BEAR" or market_ret < -0.35:
                continue
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret

            above_vwap = close > vwap
            up_close = close > prev_close
            long_struct = close > open_px and close_loc >= 0.60
            slope_n = _vwap_slope_atr(g, i, atr)
            rising_vwap = np.isfinite(slope_n) and slope_n >= SLOPE_MIN
            uptrend = np.isfinite(ema20) and np.isfinite(ema50) and close > ema20 and ema20 >= ema50
            not_extended = np.isfinite(vwap_dist) and vwap_dist <= 1.5

            # recent dip back to value (doc pullback into band): a low in the last
            # 4 bars tagged session VWAP while the trend held above it.
            lo_window = g["low"].iloc[max(0, i - 4):i + 1]
            dipped_to_value = float(lo_window.min()) <= vwap + 0.10 * atr

            fired: list[tuple[str, str]] = []

            # ---- Setup A — AVWAP trend pullback (09:45–14:00) ----
            if 585 <= minute <= 840:
                if (uptrend and rising_vwap and above_vwap and up_close and long_struct
                        and dipped_to_value and not_extended
                        and rs_pct > 0.30 and vol_ratio >= 1.2):
                    fired.append(("DOC5A_AVWAP_PULLBACK_LONG", "avwap_trend_pullback_hold_break_long"))

            # ---- Setup B — momentum breakout (09:45–13:30) ----
            if 585 <= minute <= 810:
                new_high = (np.isfinite(or_high) and close > or_high) and (
                    (np.isfinite(rh20) and close > rh20) or (np.isfinite(prev_day_high) and close > prev_day_high)
                )
                if (above_vwap and rising_vwap and long_struct and new_high
                        and rs_pct > 0.50 and vol_ratio >= 1.5 and close_loc >= 0.65
                        and np.isfinite(vwap_dist) and vwap_dist <= 3.0):
                    fired.append(("DOC5B_MOMO_BREAKOUT_LONG", "momentum_break_new_high_volume_long"))

            # ---- Setup C — opening-range gap-and-go (09:45–11:00) ----
            if 585 <= minute <= 660:
                if (np.isfinite(gap_pct) and 0.5 <= gap_pct <= 4.0
                        and np.isfinite(or_high) and close > or_high
                        and close > open_px and close_loc >= 0.60
                        and rs_pct > 0.40 and vol_ratio >= 1.5):
                    fired.append(("DOC5C_ORB_GAP_GO_LONG", "controlled_gap_orb_continuation_long"))

            # ---- Setup D — AVWAP reclaim from below (09:45–13:00) ----
            if 585 <= minute <= 780:
                fresh_reclaim = np.isfinite(prev_vwap) and prev_close <= prev_vwap and above_vwap
                if (fresh_reclaim and up_close and long_struct
                        and rs_pct > 0.15 and vol_ratio >= 1.3
                        and (rising_vwap or slope_n != slope_n)):  # slope turning up (NaN early = allow)
                    fired.append(("DOC5D_AVWAP_RECLAIM_LONG", "reclaim_session_vwap_from_below_long"))

            if np.isfinite(rng) and rng > 2.75 * atr:   # skip climax bars for all setups
                continue

            for setup, reason in fired:
                cand = rv._candidate(str(ticker).upper(), setup, "LONG", row, rs_pct, market_ret, regime, reason)
                ex = SETUPS[setup]
                cand.update({
                    "candidate_family": "doc5_long_setups",
                    "v7_signal_sl_pct": ex[0],
                    "v7_signal_target_pct": ex[1],
                    "signal_minute": int(minute),
                    "gap_pct": round(float(gap_pct), 4) if np.isfinite(gap_pct) else np.nan,
                    "vwap_slope_atr": round(float(slope_n), 6) if np.isfinite(slope_n) else np.nan,
                    "orh_dist_atr": round(float((close - or_high) / atr), 4) if np.isfinite(or_high) else np.nan,
                })
                rows.append(cand)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parent / "pool"))
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
        rv.SETUP_TIERS[s] = "doc5_long_setups"
        rv.PROBE_EXIT_RULES[s] = ex

    out_dir = Path(args.out)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"

    market_ctx = rv._market_context()
    if not market_ctx:
        print("[doc5-scan] FATAL: no NIFTYBEES market context; abort.")
        return 1

    universe = [
        str(x).upper()
        for x in rv._load_probe_universe()
        if not str(x).upper().startswith("NIFTY")
        and not str(x).upper().endswith("BEES")
        and (rv.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    if args.max_tickers > 0:
        universe = universe[: args.max_tickers]
    print(f"[doc5-scan] universe={len(universe)} tickers  window={args.start}..{args.end}", flush=True)

    t0 = time.time()
    rows: list[dict] = []
    for idx, ticker in enumerate(universe, 1):
        rows.extend(_scan_ticker(ticker, market_ctx))
        if idx % 25 == 0 or idx == len(universe):
            print(f"[doc5-scan] {idx}/{len(universe)} last={ticker} total_rows={len(rows)} "
                  f"elapsed={time.time() - t0:.1f}s", flush=True)

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(out_csv, index=False)
    print(f"[doc5-scan] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        print("\n-- rows per setup --", flush=True)
        print(raw["setup"].value_counts().to_string(), flush=True)
        print("\n-- rows per setup x month --", flush=True)
        m = pd.to_datetime(raw["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m")
        print(raw.assign(_m=m).groupby(["setup", "_m"]).size().to_string(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
