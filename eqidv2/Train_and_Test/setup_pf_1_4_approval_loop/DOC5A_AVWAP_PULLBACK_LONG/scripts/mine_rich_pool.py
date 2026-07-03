r"""mine_rich_pool.py — re-mine DOC5A with a LOOSER base entry + RICH structural
columns, so a downstream structural sweep can tune the *detector* (not just
re-filter). Research-only; raw-5min scan; no live-engine edits, no conf edits.

Why: the first DOC5A pool had only generic mask columns, so the pf-band search
could re-filter but never change the entry definition. Here we emit the doc's
own structural knobs as columns:
  vwap_slope_atr        rising-VWAP slope in ATR units over 5 bars (doc slope_n)
  established_bars      consecutive prior bars with close>VWAP (doc 'estb')
  pullback_depth_atr    (VWAP - min(low last 5 bars)) / ATR  (how deep the dip)
  orh_dist_atr          (close - ORH) / ATR
  ema20_dist_atr        (close - EMA20) / ATR
  adx, rsi              trend/strength at signal (if present in parquet)
plus the standard rs_pct / vol_ratio / close_loc / body_pct / atr_pct / vwap_dist_atr.

Output: <SETUP dir>/variant_pool/historical_all_available_pre_dedupe_live_candidates.csv

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5A_AVWAP_PULLBACK_LONG/scripts/mine_rich_pool.py \
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
REPO = HERE
for _ in range(12):
    if (REPO / "research_v11_tier123_new_setups.py").exists():
        break
    REPO = REPO.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import research_v11_tier123_new_setups as rv  # noqa: E402

SETUP = "DOC5A_AVWAP_PULLBACK_LONG"
DIP_LOOKBACK = 5


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
        or_high, _ = rv.v2._opening_range(g)
        vwap_arr = g["VWAP"].to_numpy(dtype=float)
        close_arr = g["close"].to_numpy(dtype=float)
        low_arr = g["low"].to_numpy(dtype=float)

        for i in range(6, len(g) - 1):
            row = g.iloc[i]
            ts = rv._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if not (585 <= minute <= 840):     # 09:45–14:00 (doc window for A)
                continue

            close = float(row["close"]); open_px = float(row["open"])
            atr = float(row.get("ATR", np.nan)); vwap = float(row.get("VWAP", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan)); close_loc = float(row.get("close_loc", np.nan))
            ema20 = float(row.get("EMA_20", np.nan)); rng = float(row.get("range", np.nan))
            if not (np.isfinite(atr) and atr > 0 and np.isfinite(vwap) and np.isfinite(close_loc)):
                continue
            if close < rv_min_close or float(row.get("day_value_so_far_rs", 0.0)) < rv_min_value:
                continue

            prev_close = float(close_arr[i - 1])
            above_vwap = close > vwap
            up_close = close > prev_close
            # LOOSE base gate (sweep tightens later): uptrend-ish + reclaim of value + up close
            base = (above_vwap and up_close and close > open_px and close_loc >= 0.50
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.0
                    and np.isfinite(ema20) and close > ema20)
            if not base:
                continue
            # recent dip back to value within DIP_LOOKBACK bars
            lo_win = low_arr[max(0, i - DIP_LOOKBACK):i + 1]
            if float(lo_win.min()) > vwap + 0.10 * atr:
                continue
            if np.isfinite(rng) and rng > 3.0 * atr:  # skip only extreme climax bars
                continue

            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            if regime == "BEAR" or market_ret < -0.35:
                continue
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            if rs_pct <= 0.0:
                continue

            # structural columns
            v_prev5 = float(vwap_arr[i - 5]) if i - 5 >= 0 else np.nan
            vwap_slope_atr = (vwap - v_prev5) / atr if np.isfinite(v_prev5) else np.nan
            est = 0
            for k in range(i - 1, -1, -1):
                if close_arr[k] > vwap_arr[k]:
                    est += 1
                else:
                    break
            pullback_depth_atr = (vwap - float(lo_win.min())) / atr
            orh_dist_atr = (close - or_high) / atr if np.isfinite(or_high) else np.nan
            ema20_dist_atr = (close - ema20) / atr

            cand = rv._candidate(str(ticker).upper(), SETUP, "LONG", row, rs_pct, market_ret, regime,
                                 "avwap_trend_pullback_rich")
            cand.update({
                "candidate_family": "doc5_rich_variant",
                "v7_signal_sl_pct": 0.70, "v7_signal_target_pct": 1.25,
                "signal_minute": int(minute),
                "vwap_slope_atr": round(float(vwap_slope_atr), 6) if np.isfinite(vwap_slope_atr) else np.nan,
                "established_bars": int(est),
                "pullback_depth_atr": round(float(pullback_depth_atr), 6),
                "orh_dist_atr": round(float(orh_dist_atr), 6) if np.isfinite(orh_dist_atr) else np.nan,
                "ema20_dist_atr": round(float(ema20_dist_atr), 6),
                "adx_sig": float(row.get("ADX", np.nan)),
                "rsi_sig": float(row.get("RSI", np.nan)),
            })
            rows.append(cand)
    return rows


rv_min_close = 80.0
rv_min_value = 20_000_000.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parent.parent / "variant_pool"))
    ap.add_argument("--start", default="2026-04-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--max_tickers", type=int, default=0)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    rv.START_DATE = pd.Timestamp(args.start); rv.END_DATE = pd.Timestamp(args.end)
    rv.SETUP_TIERS[SETUP] = "doc5_rich_variant"; rv.PROBE_EXIT_RULES[SETUP] = (0.70, 1.25)
    rv.MAX_RAW_PER_SETUP = 100000  # do not cap — the sweep needs the full superset

    market_ctx = rv._market_context()
    universe = [str(x).upper() for x in rv._load_probe_universe()
                if not str(x).upper().startswith("NIFTY") and not str(x).upper().endswith("BEES")
                and (rv.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()]
    if args.max_tickers > 0:
        universe = universe[: args.max_tickers]
    print(f"[rich-mine] universe={len(universe)} window={args.start}..{args.end}", flush=True)

    t0 = time.time(); rows: list[dict] = []
    for idx, tk in enumerate(universe, 1):
        rows.extend(_scan_ticker(tk, market_ctx))
        if idx % 25 == 0 or idx == len(universe):
            print(f"[rich-mine] {idx}/{len(universe)} last={tk} rows={len(rows)} {time.time()-t0:.0f}s", flush=True)

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = raw.sort_values(["signal_time_ist", "ticker"]).reset_index(drop=True)
    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    raw.to_csv(out_csv, index=False)
    print(f"[rich-mine] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        m = pd.to_datetime(raw["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m")
        print(raw.assign(_m=m).groupby("_m").size().to_string(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
