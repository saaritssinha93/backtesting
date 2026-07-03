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


SETUPS = {
    "B_AVWAP_CONFIRMED_RECLAIM_LONG": "confirmed_vwap_reclaim_hold_break_long",
    "B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG": "vwap_reclaim_pullback_break_long",
    "B_AVWAP_RECLAIM_RS_MOMENTUM_LONG": "vwap_reclaim_rs_momentum_break_long",
    "B_AVWAP_RECLAIM_RANGE_BREAK_LONG": "vwap_reclaim_near_vwap_range_break_long",
}


def _finite(*vals: float) -> bool:
    return all(np.isfinite(v) for v in vals)


def _emit(ticker: str, setup: str, row: pd.Series, rs_pct: float, market_ret: float, regime: str, extra: dict) -> dict:
    cand = rv._candidate(str(ticker).upper(), setup, "LONG", row, rs_pct, market_ret, regime, SETUPS[setup])
    cand.update(
        {
            "candidate_family": "B VWAP reclaim research family",
            "v7_signal_sl_pct": 0.70,
            "v7_signal_target_pct": 1.50,
            "signal_minute": int(rv._normalise_ts(row["date"]).hour * 60 + rv._normalise_ts(row["date"]).minute),
        }
    )
    cand.update(extra)
    return cand


def _scan_ticker(ticker: str, market_ctx: dict[str, dict[pd.Timestamp, dict]]) -> list[dict]:
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
        reclaim_seen = False
        reclaim_idx = -1
        reclaim_high = np.nan

        for i in range(6, len(g) - 1):
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
            body_pct = float(row.get("body_pct", np.nan))
            upper_wick = float(row.get("upper_wick_pct", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            rng = float(row.get("range", np.nan))

            prev_close = float(prev["close"])
            prev_high = float(prev["high"])
            prev_low = float(prev["low"])
            prev_vwap = float(prev.get("VWAP", np.nan))
            prev_vwap_dist = float(prev.get("vwap_dist_atr", np.nan))
            prev_close_loc = float(prev.get("close_loc", np.nan))

            if not _finite(atr, vwap) or atr <= 0:
                continue

            above_vwap = close > vwap
            prev_below_vwap = np.isfinite(prev_vwap) and prev_close < prev_vwap
            reclaim_bar = (
                prev_below_vwap
                and above_vwap
                and close > open_px
                and np.isfinite(close_loc)
                and close_loc >= 0.60
                and np.isfinite(vol_ratio)
                and vol_ratio >= 1.20
            )
            if reclaim_bar:
                reclaim_seen = True
                reclaim_idx = i
                reclaim_high = high

            if minute < 600 or minute > 840:
                continue
            if not reclaim_seen or i <= reclaim_idx:
                continue
            bars_since = i - reclaim_idx
            if bars_since > 8:
                reclaim_seen = False
                reclaim_idx = -1
                reclaim_high = np.nan
                continue
            if close < 80 or float(row.get("day_value_so_far_rs", 0.0)) < 20_000_000:
                continue
            if np.isfinite(rng) and rng > 3.0 * atr:
                continue

            market_ret, regime = rv._bar_context(market_ctx, str(day), ts)
            if regime == "BEAR" or market_ret < -0.35:
                continue
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            if rs_pct < -0.05:
                continue

            prev_held_vwap = (
                np.isfinite(prev_vwap)
                and prev_low <= prev_vwap + 0.45 * atr
                and prev_close >= prev_vwap - 0.15 * atr
                and np.isfinite(prev_vwap_dist)
                and -0.25 <= prev_vwap_dist <= 0.90
                and np.isfinite(prev_close_loc)
                and prev_close_loc >= 0.40
            )
            confirmation_bar = (
                close > max(prev_high, vwap)
                and close > open_px
                and np.isfinite(close_loc)
                and close_loc >= 0.62
                and np.isfinite(body_pct)
                and body_pct >= 0.12
                and np.isfinite(upper_wick)
                and upper_wick <= 0.40
                and np.isfinite(vwap_dist)
                and 0.0 <= vwap_dist <= 1.50
                and np.isfinite(vol_ratio)
                and 1.15 <= vol_ratio <= 7.0
            )
            if prev_held_vwap and confirmation_bar and bars_since <= 6:
                rows.append(
                    _emit(
                        ticker,
                        "B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG",
                        row,
                        rs_pct,
                        market_ret,
                        regime,
                        {
                            "reclaim_bars_ago": int(bars_since),
                            "prev_vwap_dist_atr": round(prev_vwap_dist, 6),
                            "confirm_vwap_dist_atr": round(vwap_dist, 6),
                        },
                    )
                )

            strict_confirmation = (
                prev_held_vwap
                and confirmation_bar
                and bars_since <= 6
                and rs_pct >= 0.0
                and vwap_dist <= 1.25
                and body_pct >= 0.18
                and upper_wick <= 0.35
            )
            if strict_confirmation:
                rows.append(
                    _emit(
                        ticker,
                        "B_AVWAP_CONFIRMED_RECLAIM_LONG",
                        row,
                        rs_pct,
                        market_ret,
                        regime,
                        {
                            "reclaim_bars_ago": int(bars_since),
                            "prev_vwap_dist_atr": round(prev_vwap_dist, 6),
                            "confirm_vwap_dist_atr": round(vwap_dist, 6),
                        },
                    )
                )

            if (
                bars_since <= 5
                and close > max(prev_high, reclaim_high, vwap)
                and close > open_px
                and np.isfinite(close_loc)
                and close_loc >= 0.70
                and np.isfinite(vol_ratio)
                and vol_ratio >= 1.40
                and np.isfinite(vwap_dist)
                and 0.0 <= vwap_dist <= 1.80
                and rs_pct >= 0.50
            ):
                rows.append(
                    _emit(
                        ticker,
                        "B_AVWAP_RECLAIM_RS_MOMENTUM_LONG",
                        row,
                        rs_pct,
                        market_ret,
                        regime,
                        {"reclaim_bars_ago": int(bars_since), "confirm_vwap_dist_atr": round(vwap_dist, 6)},
                    )
                )

            if i >= 3 and bars_since >= 2:
                comp = g.iloc[max(reclaim_idx, i - 3):i]
                comp_high = float(comp["high"].max())
                comp_low = float(comp["low"].min())
                comp_width = comp_high - comp_low
                comp_vwap_dist = pd.to_numeric(comp.get("vwap_dist_atr"), errors="coerce")
                comp_near = bool((comp_vwap_dist.between(-0.20, 0.95)).all())
                if (
                    comp_near
                    and comp_width <= 1.35 * atr
                    and close > max(comp_high, vwap)
                    and close > open_px
                    and np.isfinite(close_loc)
                    and close_loc >= 0.62
                    and np.isfinite(vol_ratio)
                    and vol_ratio >= 1.10
                    and rs_pct >= 0.0
                    and np.isfinite(vwap_dist)
                    and 0.0 <= vwap_dist <= 1.50
                ):
                    rows.append(
                        _emit(
                            ticker,
                            "B_AVWAP_RECLAIM_RANGE_BREAK_LONG",
                            row,
                            rs_pct,
                            market_ret,
                            regime,
                            {
                                "reclaim_bars_ago": int(bars_since),
                                "range_break_width_atr": round(float(comp_width / atr), 6),
                                "confirm_vwap_dist_atr": round(vwap_dist, 6),
                            },
                        )
                    )

    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(HERE.parents[1] / "family_pool"))
    ap.add_argument("--start", default="2026-05-18")
    ap.add_argument("--end", default="2026-06-24")
    ap.add_argument("--max_tickers", type=int, default=0)
    args = ap.parse_args()

    rv.START_DATE = pd.Timestamp(args.start)
    rv.END_DATE = pd.Timestamp(args.end)
    for setup in SETUPS:
        rv.SETUP_TIERS[setup] = "B VWAP reclaim research family"
        rv.PROBE_EXIT_RULES[setup] = (0.70, 1.50)

    out_dir = Path(args.out)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"

    market_ctx = rv._market_context()
    universe = [
        str(x).upper()
        for x in rv._load_probe_universe()
        if not str(x).upper().startswith("NIFTY")
        and not str(x).upper().endswith("BEES")
        and (rv.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()
    ]
    if args.max_tickers > 0:
        universe = universe[: args.max_tickers]

    t0 = time.time()
    rows: list[dict] = []
    for idx, ticker in enumerate(universe, 1):
        ticker_rows = _scan_ticker(ticker, market_ctx)
        rows.extend(ticker_rows)
        if idx % 20 == 0 or idx == len(universe):
            print(
                f"[scan-vwap-family] {idx}/{len(universe)} last={ticker} "
                f"ticker_rows={len(ticker_rows)} total={len(rows)} elapsed={time.time() - t0:.1f}s",
                flush=True,
            )

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = rv._probe_gate(raw)
        raw = raw.sort_values(["setup", "signal_time_ist", "ticker"]).reset_index(drop=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(out_csv, index=False)
    print(f"[scan-vwap-family] wrote {out_csv} rows={len(raw)}", flush=True)
    if not raw.empty:
        print(raw.groupby("setup").size().to_string(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
