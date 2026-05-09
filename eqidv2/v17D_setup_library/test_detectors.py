"""Synthetic-data smoke tests for all 20 v17D detectors.

Builds small OHLCV+indicator DataFrames designed to trigger each setup,
then asserts the detector emits at least one Signal of the expected side.

Usage:
    python3 -m eqidv2.v17D_setup_library.test_detectors

No external data required; runs in seconds.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from eqidv2.v17D_setup_library import ALL_DETECTORS, scan_all
from eqidv2.v17D_setup_library import (
    family_breakout, family_mean_reversion, family_pattern,
    family_timeofday, family_trend_continuation, family_volume,
)

IST = "Asia/Kolkata"


def _bars_index(n: int, day: str = "2026-05-08", start: str = "09:15"):
    """Build a 5-min IST-aware DatetimeIndex starting at 09:15 of `day`."""
    base = pd.Timestamp(f"{day} {start}", tz=IST)
    return pd.DatetimeIndex([base + pd.Timedelta(minutes=5 * i) for i in range(n)])


def _bare_df(n: int, base_close: float = 500.0) -> pd.DataFrame:
    """Build a flat OHLCV df then layer indicators on top per test."""
    idx = _bars_index(n)
    df = pd.DataFrame(
        {
            "open": np.full(n, base_close),
            "high": np.full(n, base_close + 1),
            "low": np.full(n, base_close - 1),
            "close": np.full(n, base_close),
            "volume": np.full(n, 100_000.0),
        }, index=idx,
    )
    return df


def _add_indicators(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Default indicator stack; per-test overrides via kwargs."""
    n = len(df)
    df = df.copy()
    df["EMA_20"] = kwargs.get("EMA_20", df["close"].rolling(5, min_periods=1).mean())
    df["EMA_50"] = kwargs.get("EMA_50", df["close"].rolling(10, min_periods=1).mean())
    df["EMA_200"] = kwargs.get("EMA_200", df["close"])
    df["ATR"] = kwargs.get("ATR", np.full(n, 5.0))
    df["RSI"] = kwargs.get("RSI", np.full(n, 50.0))
    df["ADX"] = kwargs.get("ADX", np.full(n, 25.0))
    df["Stoch_K"] = kwargs.get("Stoch_K", np.full(n, 50.0))
    df["Upper_Band"] = kwargs.get("Upper_Band", df["close"] + 5)
    df["Lower_Band"] = kwargs.get("Lower_Band", df["close"] - 5)
    df["MFI"] = kwargs.get("MFI", np.full(n, 50.0))
    df["OBV"] = kwargs.get("OBV", np.cumsum(df["volume"].values))
    df["CCI"] = kwargs.get("CCI", np.full(n, 0.0))
    df["MACD_Hist"] = kwargs.get("MACD_Hist", np.full(n, 0.0))
    df["VWAP"] = kwargs.get("VWAP", df["close"])
    df["Volume_SMA20"] = kwargs.get("Volume_SMA20", df["volume"].rolling(20, min_periods=1).mean())
    df["Prev_Day_Close"] = kwargs.get("Prev_Day_Close", df["close"].iloc[0])
    return df


# -----------------------------------------------------------------------------
# Per-detector synthetic tests
# -----------------------------------------------------------------------------

def test_tc_1_pullback_ema20():
    n = 30
    df = _bare_df(n, 500.0)
    closes = np.linspace(490, 510, n)        # uptrend
    closes[10] = 495                          # pullback to EMA20
    closes[11] = 502                          # bullish reversal
    df["close"] = closes
    df["open"] = closes - 0.5
    df["high"] = closes + 1.0
    df["low"] = np.where(np.arange(n) == 10, 493, closes - 1.0)
    df = _add_indicators(
        df,
        EMA_20=pd.Series(np.linspace(488, 508, n), index=df.index),
        EMA_50=pd.Series(np.linspace(480, 500, n), index=df.index),
        ADX=np.full(n, 28.0),
    )
    sigs = family_trend_continuation.detect_tc_1_pullback_ema20(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"TC_1: expected LONG, got {sigs}"


def test_tc_2_pullback_ema50():
    n = 30
    df = _bare_df(n, 500.0)
    closes = np.linspace(490, 530, n)
    closes[15] = 510
    closes[16] = 520
    df["close"] = closes
    df["open"] = closes - 1.0
    df["high"] = closes + 1.5
    df["low"] = np.where(np.arange(n) == 15, 505, closes - 1.5)
    df = _add_indicators(
        df,
        EMA_50=pd.Series(np.linspace(500, 520, n), index=df.index),
        EMA_200=pd.Series(np.linspace(485, 505, n), index=df.index),
        ADX=np.full(n, 28.0),
    )
    sigs = family_trend_continuation.detect_tc_2_pullback_ema50(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"TC_2: expected LONG, got {sigs}"


def test_tc_3_hh_hl_momentum():
    n = 40
    closes = [500.0] * n
    # noise highs/lows must be BETWEEN swings and not equal the local min/max,
    # so the swing points are the unique extrema in their windows.
    swings_h = [505, 510, 515]
    swings_l = [495, 500, 505]
    sh_idx = [10, 20, 30]
    sl_idx = [5, 15, 25]
    highs = np.full(n, 503.0)        # noise floor between swing lows + 1
    lows = np.full(n, 506.0)         # noise above all swing lows
    # but close noise should be inside high/low: simplify by setting all noise
    highs = np.full(n, 502.0)
    lows = np.full(n, 506.0)         # noise floor ABOVE all swing-low picks
    for k, i in enumerate(sh_idx):
        highs[i] = swings_h[k]
    for k, i in enumerate(sl_idx):
        lows[i] = swings_l[k]
    # ensure index 39 (break bar) has high >= close
    highs[-1] = 521
    lows[-1] = 510
    closes[-1] = 520
    df = pd.DataFrame({
        "open": closes, "high": highs, "low": lows, "close": closes,
        "volume": np.full(n, 250_000.0),
    }, index=_bars_index(n))
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_trend_continuation.detect_tc_3_hh_hl_momentum(df, "TEST")
    assert sigs, "TC_3: expected at least one signal"


def test_tc_4_trendday_vwap_pullback():
    n = 30
    closes = np.linspace(500, 510, n)
    closes[15] = 506.5  # pullback to VWAP
    closes[16] = 508.0  # bullish reversal
    df = _bare_df(n, 500.0)
    df["close"] = closes
    df["open"] = closes - 0.5
    df["high"] = closes + 0.5
    df["low"] = np.where(np.arange(n) == 15, 505.5, closes - 0.5)
    vwap = np.full(n, 506.0)
    df = _add_indicators(df, VWAP=pd.Series(vwap, index=df.index), ADX=np.full(n, 32.0))
    sigs = family_trend_continuation.detect_tc_4_trendday_vwap_pullback(df, "TEST")
    assert sigs, "TC_4: expected at least one signal"


def test_mr_1_bb_rsi_extreme():
    n = 20
    df = _bare_df(n, 500.0)
    df["low"] = np.where(np.arange(n) == 10, 489, 498)
    df["close"] = np.where(np.arange(n) == 10, 495, 500.0)
    df["open"] = np.where(np.arange(n) == 10, 491, 500.0)
    lower_band = np.full(n, 490.0)
    rsi = np.where(np.arange(n) == 10, 18, 50.0)
    df = _add_indicators(df,
        Lower_Band=pd.Series(lower_band, index=df.index),
        RSI=pd.Series(rsi, index=df.index),
        ADX=np.full(n, 20.0),
    )
    sigs = family_mean_reversion.detect_mr_1_bb_rsi_extreme(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"MR_1: expected LONG, got {sigs}"


def test_mr_2_vwap_fade():
    n = 15
    df = _bare_df(n, 500.0)
    df["open"] = np.where(np.arange(n) == 5, 485, 500.0)
    df["close"] = np.where(np.arange(n) == 5, 488, 500.0)
    df["low"] = np.where(np.arange(n) == 5, 484, 499.0)
    df["high"] = np.where(np.arange(n) == 5, 489, 501.0)
    df = _add_indicators(df, ATR=np.full(n, 5.0), VWAP=np.full(n, 500.0), ADX=np.full(n, 22.0))
    sigs = family_mean_reversion.detect_mr_2_vwap_fade(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"MR_2: expected LONG, got {sigs}"


def test_mr_3_overext_ema_reversion():
    n = 15
    df = _bare_df(n, 500.0)
    df["close"] = np.where(np.arange(n) == 5, 480.0, 500.0)  # prev gap -4 ATR
    df["open"] = np.where(np.arange(n) == 6, 480, 500)
    df["close"] = np.where(np.arange(n) == 6, 485, df["close"])
    df["high"] = np.where(np.arange(n) == 6, 486, 501)
    df["low"] = np.where(np.arange(n) == 6, 479, 499)
    df = _add_indicators(df, ATR=np.full(n, 5.0), EMA_20=np.full(n, 500.0))
    sigs = family_mean_reversion.detect_mr_3_overext_ema_reversion(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"MR_3: expected LONG, got {sigs}"


def test_mr_4_three_bar_drive():
    n = 10
    df = _bare_df(n, 500.0)
    # Bars 0-2: red drive down; bar 3: bullish reversal
    df["open"]  = np.array([502, 500, 498, 495, 500, 500, 500, 500, 500, 500], dtype=float)
    df["close"] = np.array([500, 498, 495, 499, 500, 500, 500, 500, 500, 500], dtype=float)
    df["high"]  = np.array([503, 501, 499, 500, 501, 501, 501, 501, 501, 501], dtype=float)
    df["low"]   = np.array([499, 497, 494, 494, 499, 499, 499, 499, 499, 499], dtype=float)
    df["volume"] = np.array([100, 110, 150, 120, 100, 100, 100, 100, 100, 100], dtype=float) * 1000
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_mean_reversion.detect_mr_4_three_bar_drive(df, "TEST")
    assert any(s.side == "LONG" and s.signal_time_ist == df.index[3] for s in sigs), \
        f"MR_4: expected LONG at bar 3, got {sigs}"


def test_bo_1_donchian_break():
    n = 25
    df = _bare_df(n, 500.0)
    df["high"] = 501.0
    df["close"] = 500.0
    df.iloc[24, df.columns.get_loc("high")] = 510.0
    df.iloc[24, df.columns.get_loc("close")] = 509.0
    df["volume"] = 200_000.0
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_breakout.detect_bo_1_donchian_break(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"BO_1: expected LONG, got {sigs}"


def test_bo_2_squeeze_release():
    n = 25
    df = _bare_df(n, 500.0)
    # Inside KC for 12 bars then break above
    upper = np.full(n, 501.5)
    lower = np.full(n, 498.5)
    upper[20] = 510  # break bar
    df["close"] = np.where(np.arange(n) == 20, 510, 500.0)
    df["volume"] = np.where(np.arange(n) == 20, 200_000, 100_000)
    df = _add_indicators(
        df, Upper_Band=pd.Series(upper, index=df.index),
        Lower_Band=pd.Series(lower, index=df.index),
        EMA_20=np.full(n, 500.0), ATR=np.full(n, 5.0),
        Volume_SMA20=np.full(n, 100_000.0),
    )
    sigs = family_breakout.detect_bo_2_squeeze_release(df, "TEST")
    assert sigs, f"BO_2: expected signal, got {sigs}"


def test_bo_3_htf_level_break():
    # Two days; today breaks yesterday's high
    n_per_day = 10
    idx_d1 = _bars_index(n_per_day, day="2026-05-07")
    idx_d2 = _bars_index(n_per_day, day="2026-05-08")
    closes_d1 = np.linspace(490, 500, n_per_day)
    closes_d2 = np.full(n_per_day, 495.0)
    closes_d2[5] = 510  # break bar
    df = pd.DataFrame({
        "open": np.concatenate([closes_d1, closes_d2]),
        "high": np.concatenate([closes_d1 + 1, closes_d2 + 1]),
        "low":  np.concatenate([closes_d1 - 1, closes_d2 - 1]),
        "close": np.concatenate([closes_d1, closes_d2]),
        "volume": np.concatenate([np.full(n_per_day, 100_000), np.full(n_per_day, 200_000)]),
    }, index=idx_d1.append(idx_d2))
    df = _add_indicators(df, Volume_SMA20=np.full(2 * n_per_day, 100_000.0), ATR=np.full(2 * n_per_day, 5.0))
    sigs = family_breakout.detect_bo_3_htf_level_break(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"BO_3: expected LONG, got {sigs}"


def test_bo_4_or15_break():
    n = 20
    df = _bare_df(n, 500.0)
    # OR (first 3 bars 09:15-09:30): high=502, low=498
    df.iloc[0:3, df.columns.get_loc("high")] = 502.0
    df.iloc[0:3, df.columns.get_loc("low")] = 498.0
    df.iloc[5, df.columns.get_loc("close")] = 510  # break above OR
    df.iloc[5, df.columns.get_loc("open")] = 504
    df.iloc[5, df.columns.get_loc("high")] = 511
    df.iloc[5, df.columns.get_loc("low")] = 503
    df.iloc[5, df.columns.get_loc("volume")] = 200_000
    df = _add_indicators(df, ADX=np.full(n, 25.0), Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_breakout.detect_bo_4_or15_break(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"BO_4: expected LONG, got {sigs}"


def test_pt_1_gap_and_go():
    n = 5
    df = _bare_df(n, 500.0)
    df.iloc[0] = [510, 512, 509, 511, 250_000]
    df = _add_indicators(df, Prev_Day_Close=pd.Series(np.full(n, 500.0), index=df.index),
                         Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_pattern.detect_pt_1_gap_and_go(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"PT_1: expected LONG, got {sigs}"


def test_pt_2_gap_fill_fade():
    n = 12
    df = _bare_df(n, 500.0)
    # Gap up 3% from prior close 500 -> open 515
    df.iloc[0] = [515, 516, 514, 516, 100_000]
    # Bar 5 reaches 50% fill ((515+500)/2 = 507.5) and bearish reversal
    df.iloc[5, df.columns.get_loc("open")] = 510
    df.iloc[5, df.columns.get_loc("close")] = 506
    df.iloc[5, df.columns.get_loc("low")] = 505
    df.iloc[5, df.columns.get_loc("high")] = 511
    df = _add_indicators(df, Prev_Day_Close=pd.Series(np.full(n, 500.0), index=df.index),
                         ADX=np.full(n, 22.0))
    sigs = family_pattern.detect_pt_2_gap_fill_fade(df, "TEST")
    assert any(s.side == "SHORT" for s in sigs), f"PT_2: expected SHORT, got {sigs}"


def test_pt_3_inside_bar_break():
    n = 5
    df = _bare_df(n, 500.0)
    df.iloc[0] = [495, 505, 490, 500, 100_000]   # parent
    df.iloc[1] = [499, 504, 496, 501, 100_000]   # inside bar
    df.iloc[2] = [501, 510, 500, 509, 200_000]   # break up
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_pattern.detect_pt_3_inside_bar_break(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"PT_3: expected LONG, got {sigs}"


def test_vo_1_climax_reversal():
    n = 15
    df = _bare_df(n, 500.0)
    df["close"] = np.linspace(500, 510, n)
    df.iloc[10, df.columns.get_loc("open")] = 511
    df.iloc[10, df.columns.get_loc("close")] = 505     # bearish
    df.iloc[10, df.columns.get_loc("high")] = 520      # long upper wick
    df.iloc[10, df.columns.get_loc("low")] = 504
    df.iloc[10, df.columns.get_loc("volume")] = 500_000  # 5x climax
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0), ADX=np.full(n, 25.0))
    sigs = family_volume.detect_vo_1_climax_reversal(df, "TEST")
    assert any(s.side == "SHORT" for s in sigs), f"VO_1: expected SHORT, got {sigs}"


def test_vo_2_low_drift_accel():
    n = 12
    df = _bare_df(n, 500.0)
    df["volume"] = 50_000.0  # low drift
    df.iloc[10, df.columns.get_loc("volume")] = 200_000  # 2x accel
    df.iloc[10, df.columns.get_loc("open")] = 500
    df.iloc[10, df.columns.get_loc("close")] = 505
    df.iloc[10, df.columns.get_loc("high")] = 506
    df.iloc[10, df.columns.get_loc("low")] = 499.5
    df["close"] = np.linspace(498, 502, n)
    df.iloc[10, df.columns.get_loc("close")] = 505
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_volume.detect_vo_2_low_drift_accel(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"VO_2: expected LONG, got {sigs}"


def test_vo_3_obv_divergence():
    n = 15
    df = _bare_df(n, 500.0)
    # Price makes new low at bar 12; OBV doesn't (rising)
    df["low"] = np.where(np.arange(n) == 12, 480, 498.0)
    df.iloc[12, df.columns.get_loc("close")] = 495
    df.iloc[12, df.columns.get_loc("open")] = 482
    df.iloc[12, df.columns.get_loc("high")] = 496
    obv = np.linspace(100_000, 200_000, n)  # rising despite price LL
    df = _add_indicators(df, OBV=pd.Series(obv, index=df.index), ADX=np.full(n, 22.0))
    sigs = family_volume.detect_vo_3_obv_divergence(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"VO_3: expected LONG, got {sigs}"


def test_td_1_opening_drive():
    n = 5
    df = _bare_df(n, 500.0)
    df.iloc[0] = [500, 510, 499, 509, 200_000]
    df.iloc[1] = [509, 514, 508, 513, 200_000]
    df = _add_indicators(df, ATR=np.full(n, 5.0), Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_timeofday.detect_td_1_opening_drive(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"TD_1: expected LONG, got {sigs}"


def test_td_2_late_day_reversal():
    # bars from 09:15 to 14:30 (5min increments) = ~63 bars
    n = 63
    df = _bare_df(n, 500.0)
    closes = np.linspace(500, 488, n)  # 2.4% downtrend (so move_pct < -1.0%)
    closes[60] = 492    # reversal bar after 14:00 (still well below day_open)
    df["close"] = closes
    df["open"] = np.where(np.arange(n) == 60, 489, closes - 0.2)
    # explicitly anchor day_open
    df.iloc[0, df.columns.get_loc("open")] = 500.0
    df["high"] = np.where(np.arange(n) == 60, 493, closes + 0.5)
    df["low"] = np.where(np.arange(n) == 60, 488, closes - 0.5)
    df["volume"] = np.where(np.arange(n) == 60, 200_000, 100_000)
    df = _add_indicators(df, Volume_SMA20=np.full(n, 100_000.0))
    sigs = family_timeofday.detect_td_2_late_day_reversal(df, "TEST")
    assert any(s.side == "LONG" for s in sigs), f"TD_2: expected LONG, got {sigs}"


# -----------------------------------------------------------------------------
# Aggregate runner
# -----------------------------------------------------------------------------

ALL_TESTS = [
    test_tc_1_pullback_ema20, test_tc_2_pullback_ema50,
    test_tc_3_hh_hl_momentum, test_tc_4_trendday_vwap_pullback,
    test_mr_1_bb_rsi_extreme, test_mr_2_vwap_fade,
    test_mr_3_overext_ema_reversion, test_mr_4_three_bar_drive,
    test_bo_1_donchian_break, test_bo_2_squeeze_release,
    test_bo_3_htf_level_break, test_bo_4_or15_break,
    test_pt_1_gap_and_go, test_pt_2_gap_fill_fade,
    test_pt_3_inside_bar_break,
    test_vo_1_climax_reversal, test_vo_2_low_drift_accel,
    test_vo_3_obv_divergence,
    test_td_1_opening_drive, test_td_2_late_day_reversal,
]


def main():
    passed = 0
    failed = []
    for t in ALL_TESTS:
        try:
            t()
            passed += 1
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed.append((t.__name__, str(e)))
            print(f"  FAIL  {t.__name__}: {e}")
        except Exception as e:
            failed.append((t.__name__, f"{type(e).__name__}: {e}"))
            print(f"  ERR   {t.__name__}: {type(e).__name__}: {e}")
    total = len(ALL_TESTS)
    print(f"\n{passed}/{total} tests passed; {len(failed)} failed")
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
