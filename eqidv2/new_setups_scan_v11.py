"""
new_setups_scan_v11.py — scanner for four research candidate setups:
  - L_RS_LEADER_VWAP_HOLD   (LONG)  : RS-leader VWAP-test-and-hold continuation
  - S_UPTHRUST_TRAP_FADE    (SHORT) : failed-high upthrust / bull-trap distribution
  - N_HIGH_RS_EMA_BOUNCE_LONG (LONG): high-RS overlay on D_EMA20_BOUNCE
  - N_MORNING_ZERO_WICK_SHORT (SHORT): morning zero-lower-wick overlay on four short families

Reuses the PROVEN tier123 scaffolding (research_v11_tier123_new_setups: data read, market
context, prev-day levels, candidate builder, futures universe) and adds only the two new
detection functions. Writes candidates in the SAME schema as the tier123 probe so the existing
robustness-first search engine (T_iterate-style) can be pointed straight at it.

*** NOT to be run during market hours (heavy scan starves the live v7 feed). Run AFTER 15:30 IST. ***
Usage (later):
  py -3.12 new_setups_scan_v11.py --dry-run            # 3 tickers, print fire-counts, verify detections
  py -3.12 new_setups_scan_v11.py --limit 40           # first 40 tickers
  py -3.12 new_setups_scan_v11.py                       # full futures universe -> candidates CSV
Then run the robustness-first search/validation on the output (reuse T_iterate, source = the CSV below).
"""
from __future__ import annotations
import argparse
import time
from pathlib import Path
import numpy as np
import pandas as pd

import research_v11_tier123_new_setups as r123   # reuse: _read_5m, _market_context, _bar_context,
import avwap_5min_ID_v2_backtesting as v2          #        _prev_day_levels, _normalise_ts, _candidate,
                                                   #        _load_probe_universe, DATA_ROOT

OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_new_setups_probe")
OUT_DIR.mkdir(parents=True, exist_ok=True)
CAND_CSV = OUT_DIR / "new_setups_standalone_trades.csv"   # tier123-schema candidates

LONG_SETUP = "L_RS_LEADER_VWAP_HOLD"
SHORT_SETUP = "S_UPTHRUST_TRAP_FADE"
HIGH_RS_EMA_LONG_SETUP = "N_HIGH_RS_EMA_BOUNCE_LONG"
MORNING_ZERO_WICK_SHORT_SETUP = "N_MORNING_ZERO_WICK_SHORT"

ALL_SETUPS = (
    LONG_SETUP,
    SHORT_SETUP,
    HIGH_RS_EMA_LONG_SETUP,
    MORNING_ZERO_WICK_SHORT_SETUP,
)


def _source_quality(
    side: str,
    source_setup: str,
    row: pd.Series,
    rs_pct: float,
    regime: str,
) -> float:
    return v2._score(
        side=side,
        setup=source_setup,
        rs_pct=rs_pct,
        vol_ratio=float(row.get("vol_ratio", np.nan)),
        close_loc=float(row.get("close_loc", np.nan)),
        vwap_dist_atr=float(row.get("vwap_dist_atr", np.nan)),
        atr_pct=float(row.get("atr_pct", np.nan)),
        regime=regime,
    )


def _early_vol_ratio(group: pd.DataFrame, index: int) -> float:
    row_ratio = float(group.iloc[index].get("vol_ratio", np.nan))
    if np.isfinite(row_ratio) and row_ratio > 0:
        return row_ratio
    prior = pd.to_numeric(group["volume"].iloc[max(0, index - 4):index], errors="coerce").dropna()
    base = float(prior.mean()) if not prior.empty else np.nan
    volume = float(group.iloc[index].get("volume", np.nan))
    return float(volume / base) if np.isfinite(base) and base > 0 and np.isfinite(volume) else np.nan


def _early_orb_quality(row: pd.Series, rs_pct: float, early_vol_ratio: float) -> float:
    close_loc = float(row.get("close_loc", 0.5))
    body_pct = float(row.get("body_pct", 0.0))
    vwap_dist_atr = float(row.get("vwap_dist_atr", 0.0))
    return float(
        30.0
        + 16.0 * max(abs(rs_pct), 0.0)
        + 8.0 * max(early_vol_ratio, 0.0)
        + 18.0 * max(body_pct, 0.0)
        + 16.0 * max(1.0 - close_loc, 0.0)
        - 5.0 * max(0.0, abs(vwap_dist_atr) - 1.5)
        + 10.0
    )


def _overlay_candidate(
    ticker: str,
    setup: str,
    side: str,
    source_setup: str,
    row: pd.Series,
    rs_pct: float,
    market_ret: float,
    regime: str,
    source_quality: float,
    reason: str,
    extra: dict,
) -> dict:
    candidate = r123._candidate(ticker, setup, side, row, rs_pct, market_ret, regime, reason)
    candidate.update(extra)
    candidate.update(
        {
            "source_setup": source_setup,
            "source_quality_score": float(source_quality),
            "quality_score": float(source_quality),
            "ranker_score": float(source_quality),
            "score": float(source_quality),
            "candidate_family": "new_overlay_forward_test",
            "selection_mode": "new_overlay_forward_test",
            "status": "RESEARCH_ONLY",
        }
    )
    return candidate


def _scan_ticker(ticker: str, market_ctx) -> list[dict]:
    df = r123._read_5m(ticker)
    if df is None or df.empty:
        return []
    df = r123._prev_day_levels(df)
    out: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        g = group.reset_index(drop=True)
        if len(g) < 15:
            continue
        day_open = float(g["open"].iloc[0])
        if "Upper_Band" in g.columns and "Lower_Band" in g.columns:
            g["bb_width_pct"] = (
                pd.to_numeric(g["Upper_Band"], errors="coerce")
                - pd.to_numeric(g["Lower_Band"], errors="coerce")
            ) / pd.to_numeric(g["close"], errors="coerce").replace(0, np.nan)
            g["bb_width_mean100"] = g["bb_width_pct"].shift(1).rolling(100, min_periods=20).mean()
        else:
            g["bb_width_pct"] = np.nan
            g["bb_width_mean100"] = np.nan
        opening_cutoff = r123._normalise_ts(g["date"].iloc[0]) + pd.Timedelta(minutes=15)
        opening_dates = pd.to_datetime(g["date"], errors="coerce")
        opening = g.loc[opening_dates < opening_cutoff]
        or_low = float(pd.to_numeric(opening["low"], errors="coerce").min()) if not opening.empty else np.nan
        overlay_long: list[dict] = []
        overlay_short: list[dict] = []
        # per-day precompute: prior 10-bar high (excl. current), RSI 3-bar max
        h10_prior = g["high"].rolling(10, min_periods=10).max().shift(1).to_numpy()
        rsi_s = g["RSI"] if "RSI" in g.columns else pd.Series(np.nan, index=g.index)
        rsi3max = rsi_s.rolling(3, min_periods=2).max().to_numpy()
        ema20_arr = (g["EMA_20"] if "EMA_20" in g.columns else pd.Series(np.nan, index=g.index)).to_numpy()
        mh = (g["MACD_Hist"] if "MACD_Hist" in g.columns else pd.Series(np.nan, index=g.index)).to_numpy()

        for i in range(10, len(g) - 1):
            row = g.iloc[i]
            ts = r123._normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 570 or minute > 900:
                continue
            close = float(row["close"]); open_px = float(row["open"])
            high = float(row["high"]); low = float(row["low"])
            atr = float(row.get("ATR", np.nan)); vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan)); ema50 = float(row.get("EMA_50", np.nan))
            adx = float(row.get("ADX", np.nan)); rsi = float(row.get("RSI", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            close_loc = float(row.get("close_loc", np.nan)); upper_wick = float(row.get("upper_wick_pct", np.nan))
            body_pct = float(row.get("body_pct", np.nan))
            atr_pct = float(row.get("atr_pct", np.nan))
            vwap_dist_atr = float(row.get("vwap_dist_atr", np.nan))
            prev_high = float(g["high"].iloc[i - 1])
            if not (np.isfinite(atr) and atr > 0 and np.isfinite(vwap)):
                continue
            market_ret, regime = r123._bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            ema20_slope = ema20 - float(ema20_arr[i - 3]) if (i >= 3 and np.isfinite(ema20_arr[i - 3])) else np.nan

            # extra features enriched onto candidate rows so the robustness-first search can mine
            # them as tunable gates (RSI rollover, MACD decel, VWAP, wicks, EMA slope, etc.)
            rsi3 = float(rsi3max[i]) if np.isfinite(rsi3max[i]) else np.nan
            mh_now = float(mh[i]) if np.isfinite(mh[i]) else np.nan
            mh_prev = float(mh[i - 1]) if np.isfinite(mh[i - 1]) else np.nan
            macd_hist_delta = (mh_now - mh_prev) if (np.isfinite(mh_now) and np.isfinite(mh_prev)) else np.nan
            extra = {"rsi": rsi, "rsi3max": rsi3, "adx": adx, "upper_wick_pct": upper_wick,
                     "lower_wick_pct": float(row.get("lower_wick_pct", np.nan)),
                     "macd_hist": mh_now, "macd_hist_delta": macd_hist_delta,
                     "ema20_slope": ema20_slope, "stock_ret": stock_ret}

            # ---------- LONG: RS-leader VWAP test-and-hold continuation ----------
            if (585 <= minute <= 840
                    and rs_pct >= 0.75 and stock_ret >= 0.30                       # leadership (non-ind)
                    and np.isfinite(ema20) and np.isfinite(ema50) and close > ema20 and ema20 >= ema50
                    and np.isfinite(ema20_slope) and ema20_slope > 0                # uptrend stack
                    and low <= vwap + 0.30 * atr and close > vwap                   # VWAP test + hold
                    and close > open_px and close_loc >= 0.60 and close > prev_high  # resumption
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.3                 # participation
                    and np.isfinite(adx) and adx >= 20 and np.isfinite(rsi) and 50 <= rsi <= 72
                    and regime in {"BULL", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, LONG_SETUP, "LONG", row, rs_pct, market_ret, regime,
                                    "rs_leader_vwap_test_hold_continuation")
                c.update(extra); out.append(c)

            # ---------- SHORT: failed-high upthrust / bull-trap distribution ----------
            # Detection = structural SKELETON (fires a workable population). The softer
            # discriminators (RSI rollover, MACD decel, VWAP loss, exact wick) are enriched
            # above for the robustness-first search to tune as gates.
            level = float(h10_prior[i]) if np.isfinite(h10_prior[i]) else np.nan
            if (585 <= minute <= 870
                    and np.isfinite(level) and high >= level and close < level           # failed 10-bar-high breakout
                    and np.isfinite(upper_wick) and upper_wick >= 0.30                    # rejection wick
                    and np.isfinite(close_loc) and close_loc <= 0.45 and close < open_px  # weak red close
                    and np.isfinite(vol_ratio) and vol_ratio >= 1.5                       # participation / distribution
                    and rs_pct <= 0.50                                                    # not a strong leader
                    and regime in {"BEAR", "TREND", "NEUTRAL"}):
                c = r123._candidate(ticker, SHORT_SETUP, "SHORT", row, rs_pct, market_ret, regime,
                                    "failed_high_upthrust_trap_distribution")
                c.update(extra); out.append(c)

            # Research overlay: D_EMA20_BOUNCE + directional body + very high RS.
            generic_source_bar = (
                i >= max(v2.VWAP_LOOKBACK, v2.RS_LOOKBACK_BARS)
                and v2._passes_common(row)
            )
            if generic_source_bar:
                near_ema20 = abs(close - ema20) <= 0.35 * atr if np.isfinite(ema20) else False
                d_ema20_bounce = (
                    near_ema20
                    and close > open_px
                    and close_loc >= v2.CLOSE_LOC_LONG_MIN
                    and close > ema20
                    and ema20 >= ema50
                    and rs_pct > -0.05
                    and vol_ratio >= 1.3
                    and regime != "BEAR"
                )
                if d_ema20_bounce and body_pct >= 0.60 and rs_pct >= 4.0:
                    source_quality = _source_quality("LONG", "D_EMA20_BOUNCE", row, rs_pct, regime)
                    overlay_long.append(
                        _overlay_candidate(
                            ticker,
                            HIGH_RS_EMA_LONG_SETUP,
                            "LONG",
                            "D_EMA20_BOUNCE",
                            row,
                            rs_pct,
                            market_ret,
                            regime,
                            source_quality,
                            "high_relative_strength_ema20_bounce_with_directional_body",
                            extra,
                        )
                    )

            # Research overlay: four source short families, morning window, near-zero lower wick.
            lower_body = min(open_px, close)
            lower_wick_price_pct = ((lower_body - low) / close * 100.0) if close > 0 else np.nan
            if 601 <= minute <= 690 and lower_wick_price_pct <= 0.01:
                short_sources: list[tuple[str, float]] = []

                if generic_source_bar:
                    short_struct = close < open_px and close_loc <= v2.CLOSE_LOC_SHORT_MAX
                    below_vwap = close < vwap
                    near_ema20 = abs(close - ema20) <= 0.35 * atr if np.isfinite(ema20) else False
                    downtrend_stack = (
                        np.isfinite(ema20)
                        and np.isfinite(ema50)
                        and close < ema20
                        and ema20 <= ema50
                    )
                    upper_band = float(row.get("Upper_Band", np.nan))
                    lower_band = float(row.get("Lower_Band", np.nan))
                    bb_width = float(row.get("bb_width_pct", np.nan))
                    bb_width_mean = float(row.get("bb_width_mean100", np.nan))
                    squeeze = (
                        np.isfinite(bb_width)
                        and np.isfinite(bb_width_mean)
                        and bb_width_mean > 0
                        and bb_width <= 0.55 * bb_width_mean
                    )
                    source_conditions = {
                        "D_EMA20_REJECTION": (
                            near_ema20
                            and short_struct
                            and downtrend_stack
                            and close < ema20
                            and rs_pct < 0.10
                            and vol_ratio >= 1.3
                            and regime != "BULL"
                        ),
                        "E_VWAP_BAND_FADE": (
                            short_struct
                            and below_vwap
                            and np.isfinite(upper_band)
                            and high >= upper_band
                            and upper_wick >= 0.20
                            and vol_ratio >= 1.5
                        ),
                        "S_BB_SQUEEZE_SHORT": (
                            squeeze
                            and short_struct
                            and np.isfinite(lower_band)
                            and close < lower_band * 0.997
                            and vol_ratio >= 2.0
                            and body_pct >= 0.65
                        ),
                    }
                    for source_setup, source_fired in source_conditions.items():
                        if source_fired:
                            short_sources.append(
                                (
                                    source_setup,
                                    _source_quality("SHORT", source_setup, row, rs_pct, regime),
                                )
                            )

                # E_ORB_BREAKOUT_SHORT comes from the separate v7 early scanner.
                early_vol = _early_vol_ratio(g, i)
                traded_value = close * float(row.get("volume", 0.0))
                candle_range = high - low
                early_orb_short = (
                    minute <= 660
                    and np.isfinite(or_low)
                    and close >= v2.MIN_PRICE
                    and traded_value >= 1_000_000.0
                    and candle_range <= 3.8 * atr
                    and np.isfinite(early_vol)
                    and body_pct >= 0.82
                    and early_vol >= 1.10
                    and regime != "BULL"
                    and -2.80 <= vwap_dist_atr <= 0.0
                    and close < or_low
                    and close < vwap
                    and close_loc <= 0.30
                    and -1.50 <= rs_pct <= -0.20
                    and np.isfinite(atr_pct)
                    and atr_pct <= 0.0065
                )
                if early_orb_short:
                    short_sources.append(
                        ("E_ORB_BREAKOUT_SHORT", _early_orb_quality(row, rs_pct, early_vol))
                    )

                for source_setup, source_quality in short_sources:
                    if source_quality <= 100.0:
                        overlay_extra = dict(extra)
                        overlay_extra["lower_wick_price_pct"] = lower_wick_price_pct
                        overlay_short.append(
                            _overlay_candidate(
                                ticker,
                                MORNING_ZERO_WICK_SHORT_SETUP,
                                "SHORT",
                                source_setup,
                                row,
                                rs_pct,
                                market_ret,
                                regime,
                                source_quality,
                                "morning_bearish_signal_closes_at_low_without_lower_wick",
                                overlay_extra,
                            )
                        )

        # Preserve the researched rule: one highest-score overlay per ticker/day.
        if overlay_long:
            out.append(max(overlay_long, key=lambda candidate: float(candidate.get("ranker_score", 0.0))))
        if overlay_short:
            out.append(max(overlay_short, key=lambda candidate: float(candidate.get("ranker_score", 0.0))))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="scan 3 tickers, print fire counts, no full output")
    ap.add_argument("--limit", type=int, default=0, help="scan only the first N tickers")
    args = ap.parse_args()

    print(f"[new_setups_scan] data_root={r123.DATA_ROOT}", flush=True)
    market_ctx = r123._market_context()
    if not market_ctx:
        print("!! market context empty (NIFTY 5m not found) — verify DATA_ROOT before full run.")
    universe = r123._load_probe_universe()
    universe = [x for x in universe if not str(x).upper().startswith("NIFTY")
                and (r123.DATA_ROOT / f"{str(x).upper()}_stocks_indicators_5min.parquet").exists()]
    if args.dry_run:
        universe = universe[:3]
    elif args.limit:
        universe = universe[:args.limit]
    print(f"[new_setups_scan] universe={len(universe)} (dry_run={args.dry_run})", flush=True)

    rows: list[dict] = []
    t0 = time.time()
    for i, tk in enumerate(universe, 1):
        try:
            rows.extend(_scan_ticker(str(tk).upper(), market_ctx))
        except Exception as exc:
            print(f"  [skip {i}/{len(universe)}] {tk}: {exc!r}", flush=True)
        if i % 25 == 0 or i == len(universe):
            print(f"  [{i}/{len(universe)}] raw_rows={len(rows)} elapsed={time.time()-t0:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    if len(df):
        vc = df.groupby(["setup", "side"]).size().to_dict()
        print("\n[new_setups_scan] fire counts:", vc)
        d = pd.to_datetime(df["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
        for s in ALL_SETUPS:
            sub = df[df["setup"] == s]
            if len(sub):
                ds = pd.to_datetime(sub["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
                print(f"   {s}: n={len(sub)} train(<=2026-04-30)={(ds<='2026-04-30').sum()} test(>=2026-05-01)={(ds>='2026-05-01').sum()}")
    else:
        print("\n[new_setups_scan] NO candidates fired — loosen thresholds or verify features on dry-run.")

    if not args.dry_run:
        df.to_csv(CAND_CSV, index=False)
        print(f"\n[new_setups_scan] wrote {len(df)} candidates -> {CAND_CSV}")
        print("NEXT: run the robustness-first search/validation on this CSV (reuse T_iterate pattern, "
              "source=new_setups_standalone_trades.csv) — build paths, tune gate+exit, anti-overfit battery.")
    else:
        print("\n[dry-run] detections verified. Inspect fire counts above before the full scan.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
