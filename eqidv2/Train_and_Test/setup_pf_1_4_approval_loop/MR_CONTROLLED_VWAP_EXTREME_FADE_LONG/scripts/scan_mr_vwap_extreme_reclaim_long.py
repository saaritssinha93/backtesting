from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE
for _ in range(8):
    if (_REPO / "research_v11_tier123_new_setups.py").exists():
        break
    _REPO = _REPO.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import research_v11_tier123_new_setups as rv  # noqa: E402


SETUP = "MR_VWAP_EXTREME_RECLAIM_LONG"
POOL_DIR = Path(r"C:\TradingData\eqidv2\setup_pools_2026_06_29") / SETUP
OUT_CSV = POOL_DIR / "historical_all_available_pre_dedupe_live_candidates.csv"


def _scan_ticker(ticker: str, market_ctx: dict[str, dict[pd.Timestamp, dict]]) -> list[dict]:
    df = rv._read_5m(ticker)
    if df is None or df.empty:
        return []
    df = rv._prev_day_levels(df)
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 15:
            continue
        day_open = float(g["open"].iloc[0])
        for i in range(6, len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = rv._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 660 or minute > 840:
                continue

            close = float(row["close"])
            open_px = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            body_pct = float(row.get("body_pct", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            upper_wick = float(row.get("upper_wick_pct", np.nan))
            lower_wick = float(row.get("lower_wick_pct", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            rng = float(row.get("range", np.nan))
            rsi = float(row.get("RSI", np.nan))
            stoch = float(row.get("Stoch_%K", np.nan))

            if not np.isfinite(atr) or atr <= 0 or not np.isfinite(vwap):
                continue
            if not np.isfinite(vol_ratio) or vol_ratio < 1.00:
                continue
            if close < 25 or float(row.get("day_value_so_far_rs", 0.0)) < 20_000_000:
                continue
            if np.isfinite(rng) and rng > 3.5 * atr:
                continue

            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            if regime == "BEAR":
                continue
            if market_ret < -0.45:
                continue

            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            if rs_pct < -0.10:
                continue

            recent = g.iloc[max(0, i - 6):i].copy()
            recent_vwap = pd.to_numeric(recent.get("vwap_dist_atr"), errors="coerce")
            recent_lower_wick = pd.to_numeric(recent.get("lower_wick_pct"), errors="coerce")
            washout_min = float(recent_vwap.min()) if len(recent_vwap.dropna()) else np.nan
            washout_wick_max = float(recent_lower_wick.max()) if len(recent_lower_wick.dropna()) else np.nan
            if not np.isfinite(washout_min) or washout_min > -1.75:
                continue
            if not np.isfinite(washout_wick_max) or washout_wick_max < 0.25:
                continue

            # This is the actual improvement over the old fade: wait for the bounce
            # to reclaim meaningfully toward VWAP, then require a strong close.
            if not (-1.50 <= vwap_dist <= 0.75):
                continue
            if not (close > open_px and close > float(prev["high"])):
                continue
            if not (np.isfinite(close_loc) and close_loc >= 0.65):
                continue
            if not (np.isfinite(upper_wick) and upper_wick <= 0.25):
                continue
            if not (np.isfinite(body_pct) and body_pct >= 0.12):
                continue
            if np.isfinite(rsi) and rsi > 55:
                continue
            if np.isfinite(stoch) and stoch > 75:
                continue

            cand = rv._candidate(
                str(ticker).upper(),
                SETUP,
                "LONG",
                row,
                rs_pct,
                market_ret,
                regime,
                "prior_vwap_extreme_washout_then_reclaim_long",
            )
            cand.update(
                {
                    "candidate_family": "MR research variant",
                    "v7_signal_sl_pct": 0.70,
                    "v7_signal_target_pct": 1.00,
                    "signal_minute": int(minute),
                    "recent_min_vwap_dist_atr": round(washout_min, 6),
                    "recent_max_lower_wick_pct": round(washout_wick_max, 6),
                    "reclaim_vwap_dist_atr": round(float(vwap_dist), 6),
                    "signal_upper_wick_pct": round(float(upper_wick), 6),
                    "signal_lower_wick_pct": round(float(lower_wick), 6) if np.isfinite(lower_wick) else np.nan,
                }
            )
            rows.append(cand)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_tickers", type=int, default=0)
    args = ap.parse_args()

    rv.SETUP_TIERS[SETUP] = "Research variant"
    rv.PROBE_EXIT_RULES[SETUP] = (0.70, 1.00)

    market_ctx = rv._market_context()
    universe = rv._load_probe_universe()
    tickers = [
        str(x).upper()
        for x in universe
        if not str(x).upper().startswith("NIFTY")
        and not str(x).upper().endswith("BEES")
        and (rv.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    if args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]

    t0 = time.time()
    rows: list[dict] = []
    for i, ticker in enumerate(tickers, 1):
        ticker_rows = _scan_ticker(ticker, market_ctx)
        rows.extend(ticker_rows)
        if i % 10 == 0 or i == len(tickers):
            print(f"[scan-variant] {i}/{len(tickers)} last={ticker} ticker_rows={len(ticker_rows)} total={len(rows)} elapsed={time.time() - t0:.1f}s", flush=True)

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)

    POOL_DIR.mkdir(parents=True, exist_ok=True)
    raw.to_csv(OUT_CSV, index=False)
    print(f"[scan-variant] wrote {OUT_CSV} rows={len(raw)}", flush=True)
    if not raw.empty:
        by_month = pd.to_datetime(raw["signal_time_ist"], errors="coerce").dt.to_period("M").value_counts().sort_index()
        print(by_month.to_string(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
