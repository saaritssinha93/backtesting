"""
Signal-only candidate scanner for "Signal discovery v7 5mins ID".

This module scans the completed 5-minute signal candle and returns candidate
tickers only. It deliberately does not emit entry_ts, entry_price, SL, target,
or trade signal CSV rows. Entry is a separate 1-minute module.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_backtesting as v7


IST_TZ = "Asia/Kolkata"
LIVE_5M_DIR = Path(
    os.getenv(
        "EQIDV2_ID5MIN_V7_LIVE_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live",
    )
)

# v8 backtesting filters candidates through v7 exclusions and v6
# setup-specific exits before resolving trades. Keep live discovery on the
# same setup universe by default while still writing signal-only candidates.
EXCLUDED_SETUPS = set(v7.EXCLUDED_SETUPS)
ALLOWED_SETUPS = set(v6.SETUP_EXIT_RULES)
SELECTION_MODE = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE", "v8_setup_compatible")
FILTER_TO_V8_EXIT_SETUPS = SELECTION_MODE.strip().lower() in {
    "v8",
    "v8_setup_compatible",
    "v8_compatible",
}

DEFAULT_SCAN_WORKERS = max(1, int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_WORKERS", "8")))

EARLY_MODE_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MODE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
EARLY_START = pd.Timestamp("09:30").time()
EARLY_END = pd.Timestamp("11:00").time()
EARLY_OR_MINUTES = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_OR_MINUTES", "15"))
EARLY_MIN_5M_TRADED_VALUE_RS = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_5M_TRADED_VALUE_RS", "1000000"))
EARLY_MAX_VWAP_DIST_ATR = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_VWAP_DIST_ATR", "2.80"))
EARLY_MAX_CANDLE_RANGE_ATR = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_CANDLE_RANGE_ATR", "3.80"))
EARLY_MIN_BODY_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_BODY_PCT", "0.42"))
EARLY_MIN_VOL_RATIO = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_VOL_RATIO", "1.10"))
EARLY_SELECTION_MODE = "early_v1"
EARLY_TIGHT_FILTERS_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_TIGHT_FILTERS", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
EARLY_BLOCKED_SETUPS_DEFAULT = ",".join(
    [
        "E_RS_FIRST_HOUR_BREAK_LONG",
        "E_RS_FIRST_HOUR_BREAK_SHORT",
        "E_VWAP_RECLAIM_EARLY_LONG",
        "E_FAILED_OR_BREAKOUT_TRAP_SHORT",
        "E_ORB_RETEST_HOLD_SHORT",
        "E_ORB_RETEST_HOLD_LONG",
        "E_FAILED_OR_BREAKDOWN_TRAP_LONG",
        "E_GAP_HOLD_CONTINUATION_LONG",
        "E_GAP_HOLD_CONTINUATION_SHORT",
        "E_OPENING_DRIVE_CONTINUATION_LONG",
        "E_OPENING_DRIVE_CONTINUATION_SHORT",
    ]
)
EARLY_BLOCKED_SETUPS = {
    x.strip().upper()
    for x in os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_BLOCKED_SETUPS", EARLY_BLOCKED_SETUPS_DEFAULT).split(",")
    if x.strip()
}
EARLY_ORB_LONG_MAX_VOL_RATIO = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VOL_RATIO", "2.00"))
EARLY_ORB_LONG_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MIN_RS_PCT", "4.00"))
EARLY_ORB_LONG_MAX_VWAP_DIST_ATR = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VWAP_DIST_ATR", "1.80"))
EARLY_GAP_LONG_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_RS_PCT", "3.00"))
EARLY_GAP_LONG_MIN_QUALITY = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_QUALITY", "160.00"))
EARLY_ORB_SHORT_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_RS_PCT", "-1.50"))
EARLY_ORB_SHORT_MAX_ATR_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MAX_ATR_PCT", "0.0065"))
EARLY_ORB_SHORT_MIN_BODY_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_BODY_PCT", "0.82"))
EARLY_VWAP_SHORT_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_RS_PCT", "-1.20"))
EARLY_VWAP_SHORT_MIN_CLOSE_LOC = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_CLOSE_LOC", "0.08"))
EARLY_VWAP_SHORT_MAX_ATR_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MAX_ATR_PCT", "0.008"))

RESEARCH_SHADOW_VERSION = "v7_research_2026_06_03"
RESEARCH_PROBATION_SETUPS = {
    x.strip().upper()
    for x in os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_PROBATION_SETUPS",
        "T_TREND_DAY_EMA_STAIR_SHORT,C_OR_BREAKDOWN,L_TREND_PULLBACK",
    ).split(",")
    if x.strip()
}
RESEARCH_ANTI_CHASE_LONG_CLOSE_LOC_MIN = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN", "0.88")
)
RESEARCH_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN", "0.52")
)


def _ensure_ist_ts(ts: Any) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if out.tz is None:
        out = out.tz_localize(IST_TZ)
    else:
        out = out.tz_convert(IST_TZ)
    return out


def _read_one(fp: Path) -> Optional[pd.DataFrame]:
    if not fp.exists():
        return None
    try:
        df = v2._read_ohlcv(fp)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df


def _load_live_5m(ticker: str) -> Optional[pd.DataFrame]:
    fp = LIVE_5M_DIR / f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    df = _read_one(fp)
    if df is None or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
    return (
        df.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


def _append_synthetic_successor(prepared_day: pd.DataFrame, slot_ts: pd.Timestamp) -> pd.DataFrame:
    """Let v2._scan_day evaluate the latest signal candle without using entry.

    v2._scan_day loops to len(df)-1 because its normal trade candidate needs
    next_row for entry. For signal discovery, entry is intentionally absent.
    We append one synthetic successor after the completed signal candle only so
    v2 can evaluate the candle at slot_ts. All successor/entry fields are
    discarded from output.
    """
    if prepared_day.empty:
        return prepared_day
    last = prepared_day.iloc[-1].copy()
    next_ts = slot_ts + pd.Timedelta(minutes=5)
    last["date"] = next_ts
    if "date_only" in last.index:
        last["date_only"] = next_ts.date()
    close = float(last.get("close", np.nan))
    if np.isfinite(close):
        for col in ("open", "high", "low", "close"):
            if col in last.index:
                last[col] = close
    if "volume" in last.index:
        last["volume"] = 0
    return pd.concat([prepared_day, pd.DataFrame([last])], ignore_index=True)


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _early_signal_window(ts: pd.Timestamp) -> bool:
    t = _ensure_ist_ts(ts).time()
    return EARLY_START <= t <= EARLY_END


def _early_atr(day_df: pd.DataFrame, idx: int) -> float:
    row_atr = _safe_float(day_df.iloc[idx].get("ATR"))
    if np.isfinite(row_atr) and row_atr > 0:
        return row_atr
    work = day_df.iloc[: idx + 1].copy()
    prev_close = pd.to_numeric(work["close"], errors="coerce").shift(1)
    high = pd.to_numeric(work["high"], errors="coerce")
    low = pd.to_numeric(work["low"], errors="coerce")
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.tail(6).mean()
    return float(atr) if np.isfinite(atr) and atr > 0 else float("nan")


def _early_vol_ratio(day_df: pd.DataFrame, idx: int) -> float:
    row_ratio = _safe_float(day_df.iloc[idx].get("vol_ratio"))
    if np.isfinite(row_ratio) and row_ratio > 0:
        return row_ratio
    if idx <= 0:
        return float("nan")
    prev = pd.to_numeric(day_df["volume"].iloc[max(0, idx - 4):idx], errors="coerce").dropna()
    base = float(prev.mean()) if not prev.empty else float("nan")
    vol = _safe_float(day_df.iloc[idx].get("volume"))
    if np.isfinite(base) and base > 0 and np.isfinite(vol):
        return float(vol / base)
    return float("nan")


def _early_opening_range(day_df: pd.DataFrame) -> tuple[float, float, float, float]:
    start = _ensure_ist_ts(day_df["date"].iloc[0])
    cutoff = start + pd.Timedelta(minutes=EARLY_OR_MINUTES)
    dates = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST_TZ)
    else:
        dates = dates.dt.tz_convert(IST_TZ)
    opening = day_df.loc[dates < cutoff]
    if opening.empty:
        return float("nan"), float("nan"), float("nan"), float("nan")
    high = float(pd.to_numeric(opening["high"], errors="coerce").max())
    low = float(pd.to_numeric(opening["low"], errors="coerce").min())
    open_px = _safe_float(opening.iloc[0].get("open"))
    close_px = _safe_float(opening.iloc[-1].get("close"))
    return high, low, open_px, close_px


def _bar_market_context(market_ctx: Dict[str, Dict[str, Any]], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    try:
        return v2._bar_context(market_ctx, day, ts)
    except Exception:
        return 0.0, "NEUTRAL"


def _early_candidate(
    ticker: str,
    day: str,
    setup: str,
    side: str,
    row: pd.Series,
    next_row: pd.Series,
    *,
    rs_pct: float,
    market_ret: float,
    regime: str,
    reason: str,
    early_vol_ratio: float,
    atr: float,
    vwap_dist_atr: float,
    score_boost: float = 0.0,
) -> "v2.Candidate":
    close = _safe_float(row.get("close"))
    body_pct = _safe_float(row.get("body_pct"), 0.0)
    close_loc = _safe_float(row.get("close_loc"), 0.5)
    signal_volume = _safe_float(row.get("volume"), 0.0)
    day_value = _safe_float(row.get("day_value_so_far_rs"), close * signal_volume if np.isfinite(close) else 0.0)
    side_u = str(side).upper()
    loc_score = close_loc if side_u == "LONG" else 1.0 - close_loc
    score = (
        30.0
        + 16.0 * max(abs(rs_pct), 0.0)
        + 8.0 * max(early_vol_ratio, 0.0)
        + 18.0 * max(body_pct, 0.0)
        + 16.0 * max(loc_score, 0.0)
        - 5.0 * max(0.0, abs(vwap_dist_atr) - 1.5)
        + score_boost
    )
    entry_px = _safe_float(next_row.get("open"), close)
    return v2.Candidate(
        ticker=str(ticker).upper(),
        date=day,
        setup=setup,
        side=side_u,
        signal_ts=pd.Timestamp(row["date"]),
        signal_close=close,
        entry_ts=pd.Timestamp(next_row.get("date", row["date"])),
        entry_px=entry_px,
        target_px=entry_px,
        sl_px=entry_px,
        quality_score=float(score),
        rs_pct=float(rs_pct),
        market_ret_pct=float(market_ret),
        regime=regime,
        vol_ratio=float(early_vol_ratio),
        atr_pct=float(atr / close) if np.isfinite(atr) and np.isfinite(close) and close > 0 else float("nan"),
        close_loc=float(close_loc),
        body_pct=float(body_pct),
        vwap_dist_atr=float(vwap_dist_atr),
        day_value_so_far_rs=float(day_value),
        reason=reason,
    )


def _early_tight_filter(
    candidate: "v2.Candidate",
    *,
    setup: str,
    rs_pct: float,
    early_vol_ratio: float,
    atr: float,
    close: float,
    body_pct: float,
    close_loc: float,
    vwap_dist_atr: float,
) -> bool:
    if not EARLY_TIGHT_FILTERS_ENABLE:
        return True

    setup_u = str(setup).upper().strip()
    if setup_u in EARLY_BLOCKED_SETUPS:
        return False

    atr_pct = atr / close if np.isfinite(atr) and np.isfinite(close) and close > 0 else float("nan")
    quality = _safe_float(getattr(candidate, "quality_score", np.nan))

    if setup_u == "E_ORB_BREAKOUT_LONG":
        return (
            early_vol_ratio <= EARLY_ORB_LONG_MAX_VOL_RATIO
            and rs_pct >= EARLY_ORB_LONG_MIN_RS_PCT
            and vwap_dist_atr <= EARLY_ORB_LONG_MAX_VWAP_DIST_ATR
        )

    if setup_u == "E_GAP_HOLD_CONTINUATION_LONG":
        return rs_pct >= EARLY_GAP_LONG_MIN_RS_PCT and quality >= EARLY_GAP_LONG_MIN_QUALITY

    if setup_u == "E_ORB_BREAKOUT_SHORT":
        return (
            rs_pct >= EARLY_ORB_SHORT_MIN_RS_PCT
            and np.isfinite(atr_pct)
            and atr_pct <= EARLY_ORB_SHORT_MAX_ATR_PCT
            and body_pct >= EARLY_ORB_SHORT_MIN_BODY_PCT
        )

    if setup_u == "E_VWAP_LOSE_EARLY_SHORT":
        return (
            rs_pct >= EARLY_VWAP_SHORT_MIN_RS_PCT
            and close_loc >= EARLY_VWAP_SHORT_MIN_CLOSE_LOC
            and np.isfinite(atr_pct)
            and atr_pct <= EARLY_VWAP_SHORT_MAX_ATR_PCT
        )

    return True


def _scan_early_slot_candidates(
    day_df: pd.DataFrame,
    ticker: str,
    slot_ts: pd.Timestamp,
    market_ctx: Dict[str, Dict[str, Any]],
) -> List["v2.Candidate"]:
    if not EARLY_MODE_ENABLE or day_df.empty or not _early_signal_window(slot_ts):
        return []

    dates = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST_TZ)
    else:
        dates = dates.dt.tz_convert(IST_TZ)
    idxs = np.where(dates.dt.floor("min").eq(slot_ts.floor("min")))[0]
    if len(idxs) == 0:
        return []
    idx = int(idxs[-1])
    if idx < 3:
        return []

    row = day_df.iloc[idx]
    next_row = day_df.iloc[idx + 1] if idx + 1 < len(day_df) else row
    day = str(row.get("date_only", slot_ts.date()))
    close = _safe_float(row.get("close"))
    open_px = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    volume = _safe_float(row.get("volume"), 0.0)
    traded_value = close * volume if np.isfinite(close) else 0.0
    if not np.isfinite(close) or close < v2.MIN_PRICE or traded_value < EARLY_MIN_5M_TRADED_VALUE_RS:
        return []

    atr = _early_atr(day_df, idx)
    rng = high - low if np.isfinite(high) and np.isfinite(low) else float("nan")
    if np.isfinite(atr) and atr > 0 and np.isfinite(rng) and rng > EARLY_MAX_CANDLE_RANGE_ATR * atr:
        return []

    vwap = _safe_float(row.get("VWAP"))
    if not np.isfinite(vwap) or not np.isfinite(atr) or atr <= 0:
        return []
    vwap_dist_atr = (close - vwap) / atr
    close_loc = _safe_float(row.get("close_loc"), 0.5)
    body_pct = _safe_float(row.get("body_pct"), 0.0)
    early_vol = _early_vol_ratio(day_df, idx)
    if not np.isfinite(early_vol):
        return []

    day_open = _safe_float(day_df.iloc[0].get("open"))
    stock_ret = (close / day_open - 1.0) * 100.0 if np.isfinite(day_open) and day_open > 0 else 0.0
    market_ret, regime = _bar_market_context(market_ctx, day, slot_ts)
    rs_pct = stock_ret - market_ret
    or_high, or_low, or_open, or_close = _early_opening_range(day_df)
    if not np.isfinite(or_high) or not np.isfinite(or_low):
        return []

    prev = day_df.iloc[idx - 1]
    prev_close = _safe_float(prev.get("close"))
    prev_high = _safe_float(prev.get("high"))
    prev_low = _safe_float(prev.get("low"))
    prev_vwap = _safe_float(prev.get("VWAP"), vwap)
    prior = day_df.iloc[3:idx]
    prior_broke_high = (not prior.empty) and bool((pd.to_numeric(prior["high"], errors="coerce") > or_high).any())
    prior_broke_low = (not prior.empty) and bool((pd.to_numeric(prior["low"], errors="coerce") < or_low).any())
    first15_ret = (or_close / or_open - 1.0) * 100.0 if np.isfinite(or_open) and or_open > 0 and np.isfinite(or_close) else 0.0
    prev_day_close = _safe_float(row.get("Prev_Day_Close"))
    gap_pct = (day_open / prev_day_close - 1.0) * 100.0 if np.isfinite(prev_day_close) and prev_day_close > 0 else float("nan")
    opening_low = float(pd.to_numeric(day_df["low"].iloc[:3], errors="coerce").min())
    opening_high = float(pd.to_numeric(day_df["high"].iloc[:3], errors="coerce").max())
    upper_wick = _safe_float(row.get("upper_wick_pct"), 0.0)
    lower_wick = _safe_float(row.get("lower_wick_pct"), 0.0)

    above_vwap = close > vwap
    below_vwap = close < vwap
    out: List["v2.Candidate"] = []

    def add(setup: str, side: str, condition: bool, reason: str, boost: float = 0.0) -> None:
        if condition:
            candidate = _early_candidate(
                ticker,
                day,
                setup,
                side,
                row,
                next_row,
                rs_pct=rs_pct,
                market_ret=market_ret,
                regime=regime,
                reason=reason,
                early_vol_ratio=early_vol,
                atr=atr,
                vwap_dist_atr=vwap_dist_atr,
                score_boost=boost,
            )
            if _early_tight_filter(
                candidate,
                setup=setup,
                rs_pct=rs_pct,
                early_vol_ratio=early_vol,
                atr=atr,
                close=close,
                body_pct=body_pct,
                close_loc=close_loc,
                vwap_dist_atr=vwap_dist_atr,
            ):
                out.append(candidate)

    common_long = (
        body_pct >= EARLY_MIN_BODY_PCT
        and early_vol >= EARLY_MIN_VOL_RATIO
        and regime != "BEAR"
        and 0.0 <= vwap_dist_atr <= EARLY_MAX_VWAP_DIST_ATR
    )
    common_short = (
        body_pct >= EARLY_MIN_BODY_PCT
        and early_vol >= EARLY_MIN_VOL_RATIO
        and regime != "BULL"
        and -EARLY_MAX_VWAP_DIST_ATR <= vwap_dist_atr <= 0.0
    )

    add(
        "E_ORB_BREAKOUT_LONG",
        "LONG",
        common_long and close > or_high and above_vwap and close_loc >= 0.70 and rs_pct >= 0.20,
        "early_opening_range_breakout_long",
        10.0,
    )
    add(
        "E_ORB_BREAKOUT_SHORT",
        "SHORT",
        common_short and close < or_low and below_vwap and close_loc <= 0.30 and rs_pct <= -0.20,
        "early_opening_range_breakout_short",
        10.0,
    )
    add(
        "E_ORB_RETEST_HOLD_LONG",
        "LONG",
        common_long and prior_broke_high and low <= or_high + 0.35 * atr and close > or_high and above_vwap and close_loc >= 0.55 and rs_pct >= 0.0,
        "early_orb_retest_hold_long",
        7.0,
    )
    add(
        "E_ORB_RETEST_HOLD_SHORT",
        "SHORT",
        common_short and prior_broke_low and high >= or_low - 0.35 * atr and close < or_low and below_vwap and close_loc <= 0.45 and rs_pct <= 0.0,
        "early_orb_retest_hold_short",
        7.0,
    )
    add(
        "E_VWAP_RECLAIM_EARLY_LONG",
        "LONG",
        common_long and prev_close <= prev_vwap and close > vwap and close > prev_high and close_loc >= 0.65 and rs_pct >= 0.10 and vwap_dist_atr <= 1.80,
        "early_vwap_reclaim_break_prev_high",
        6.0,
    )
    add(
        "E_VWAP_LOSE_EARLY_SHORT",
        "SHORT",
        common_short and prev_close >= prev_vwap and close < vwap and close < prev_low and close_loc <= 0.35 and rs_pct <= -0.10 and vwap_dist_atr >= -1.80,
        "early_vwap_lose_break_prev_low",
        6.0,
    )
    add(
        "E_GAP_HOLD_CONTINUATION_LONG",
        "LONG",
        common_long and np.isfinite(gap_pct) and gap_pct >= 0.50 and opening_low >= prev_day_close * 0.997 and close > or_high and above_vwap and rs_pct >= 0.20,
        "early_gap_up_hold_continuation",
        8.0,
    )
    add(
        "E_GAP_HOLD_CONTINUATION_SHORT",
        "SHORT",
        common_short and np.isfinite(gap_pct) and gap_pct <= -0.50 and opening_high <= prev_day_close * 1.003 and close < or_low and below_vwap and rs_pct <= -0.20,
        "early_gap_down_hold_continuation",
        8.0,
    )
    add(
        "E_RS_FIRST_HOUR_BREAK_LONG",
        "LONG",
        common_long and stock_ret >= 1.0 and rs_pct >= 0.50 and close > max(or_high, prev_high) and above_vwap,
        "early_relative_strength_first_hour_break",
        9.0,
    )
    add(
        "E_RS_FIRST_HOUR_BREAK_SHORT",
        "SHORT",
        common_short and stock_ret <= -1.0 and rs_pct <= -0.50 and close < min(or_low, prev_low) and below_vwap,
        "early_relative_weakness_first_hour_break",
        9.0,
    )
    add(
        "E_OPENING_DRIVE_CONTINUATION_LONG",
        "LONG",
        common_long and first15_ret >= 0.50 and close > prev_high and close > or_high and rs_pct >= 0.30,
        "early_opening_drive_continuation_long",
        5.0,
    )
    add(
        "E_OPENING_DRIVE_CONTINUATION_SHORT",
        "SHORT",
        common_short and first15_ret <= -0.50 and close < prev_low and close < or_low and rs_pct <= -0.30,
        "early_opening_drive_continuation_short",
        5.0,
    )
    add(
        "E_FAILED_OR_BREAKOUT_TRAP_SHORT",
        "SHORT",
        early_vol >= 1.0 and regime != "BULL" and high > or_high and close < or_high and (below_vwap or close_loc <= 0.35) and upper_wick >= 0.30 and rs_pct <= 0.10,
        "early_failed_or_breakout_trap_short",
        4.0,
    )
    add(
        "E_FAILED_OR_BREAKDOWN_TRAP_LONG",
        "LONG",
        early_vol >= 1.0 and regime != "BEAR" and low < or_low and close > or_low and (above_vwap or close_loc >= 0.65) and lower_wick >= 0.30 and rs_pct >= -0.10,
        "early_failed_or_breakdown_trap_long",
        4.0,
    )
    return out


def scan_ticker_signal_candle(
    ticker: str,
    slot_ist: Any,
    market_ctx: Dict[str, Dict[str, Any]],
) -> List[Tuple["v2.Candidate", Dict[str, Any]]]:
    df = _load_live_5m(ticker)
    if df is None or df.empty:
        return []

    slot_ts = _ensure_ist_ts(slot_ist).floor("min")
    df = df[df["date"] <= slot_ts].copy()
    if df.empty:
        return []

    try:
        prepared = v2._prepare_5m(df)
    except Exception:
        return []
    if "date_only" not in prepared.columns:
        prepared["date_only"] = prepared["date"].dt.tz_convert(IST_TZ).dt.date

    day_df = prepared[prepared["date_only"] == slot_ts.date()].copy().reset_index(drop=True)
    if day_df.empty:
        return []
    day_df["date"] = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(day_df["date"].dt, "tz", None) is None:
        day_df["date"] = day_df["date"].dt.tz_localize(IST_TZ)
    else:
        day_df["date"] = day_df["date"].dt.tz_convert(IST_TZ)

    signal_rows = day_df[day_df["date"].dt.floor("min") == slot_ts]
    if signal_rows.empty:
        return []

    scan_df = _append_synthetic_successor(day_df, slot_ts)
    try:
        candidates = v2._scan_day(scan_df, str(ticker).upper(), market_ctx)
    except Exception:
        candidates = []
    candidates = list(candidates or [])
    try:
        candidates.extend(_scan_early_slot_candidates(scan_df, str(ticker).upper(), slot_ts, market_ctx))
    except Exception:
        pass

    signal_row = signal_rows.iloc[-1].to_dict()
    out: List[Tuple["v2.Candidate", Dict[str, Any]]] = []
    for c in candidates:
        c_ts = _ensure_ist_ts(c.signal_ts).floor("min")
        if c_ts != slot_ts:
            continue
        if str(c.setup) in EXCLUDED_SETUPS:
            continue
        if FILTER_TO_V8_EXIT_SETUPS and str(c.setup) not in ALLOWED_SETUPS:
            continue
        out.append((c, signal_row))
    return out


def _fmt_ist(ts: Any) -> str:
    t = _ensure_ist_ts(ts)
    offset = t.strftime("%z")
    return f"{t.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _finite_or_blank(x: Any) -> Any:
    try:
        v = float(x)
    except Exception:
        return ""
    return v if np.isfinite(v) else ""


def _research_shadow_metadata(side: str, setup: str, close_loc: Any, vwap_dist_atr: Any) -> Dict[str, str]:
    side_u = str(side).upper().strip()
    setup_u = str(setup).upper().strip()
    reasons: List[str] = []
    actions: List[str] = []
    status = ""
    try:
        close_loc_f = float(close_loc)
    except Exception:
        close_loc_f = np.nan
    try:
        vwap_dist_f = float(vwap_dist_atr)
    except Exception:
        vwap_dist_f = np.nan

    if setup_u in RESEARCH_PROBATION_SETUPS:
        status = "PROBATION"
        reasons.append("weak_setup_from_v7_live_research")
        actions.append("scanner_shadow_only_no_block")

    if (
        side_u == "LONG"
        and np.isfinite(close_loc_f)
        and np.isfinite(vwap_dist_f)
        and close_loc_f > RESEARCH_ANTI_CHASE_LONG_CLOSE_LOC_MIN
        and vwap_dist_f > RESEARCH_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN
    ):
        status = status or "PAPER_EXPERIMENT"
        reasons.append(
            f"anti_chase_long close_loc>{RESEARCH_ANTI_CHASE_LONG_CLOSE_LOC_MIN:.2f} "
            f"vwap_dist_atr>{RESEARCH_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN:.2f}"
        )
        actions.append("paper_gate_active_scanner_shadow_only")

    return {
        "research_shadow_status": status,
        "research_shadow_reason": ";".join(reasons),
        "research_shadow_action": ";".join(actions),
        "research_shadow_version": RESEARCH_SHADOW_VERSION if reasons else "",
    }


def candidates_to_dataframe(
    rows_in: Iterable[Tuple["v2.Candidate", Dict[str, Any]]],
    scan_slot_ist: Any,
    *,
    dedupe: bool = True,
) -> pd.DataFrame:
    scan_slot = _ensure_ist_ts(scan_slot_ist)
    created_at = pd.Timestamp.now(tz=IST_TZ)
    rows: List[Dict[str, Any]] = []
    for c, signal_row in rows_in:
        signal_ts = _ensure_ist_ts(c.signal_ts)
        ticker = str(c.ticker).upper().strip()
        side = str(c.side).upper().strip()
        setup = str(c.setup)
        selection_mode = EARLY_SELECTION_MODE if setup.startswith("E_") else SELECTION_MODE
        signal_time = _fmt_ist(signal_ts)
        candidate_id = f"{ticker}|{side}|{setup}|{signal_time}"
        diag = {
            "reason": str(c.reason),
            "day_value_so_far_rs": _finite_or_blank(c.day_value_so_far_rs),
            "market_ret_pct": _finite_or_blank(c.market_ret_pct),
            "rs_pct": _finite_or_blank(c.rs_pct),
            "regime": str(c.regime),
        }
        rows.append({
            "candidate_id": candidate_id,
            "scan_session": "Signal discovery v7 5mins ID",
            "selection_mode": selection_mode,
            "candidate_family": "EARLY" if setup.startswith("E_") else "V7_STANDARD",
            "scan_slot_ist": _fmt_ist(scan_slot),
            "signal_time_ist": signal_time,
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "signal_open": _finite_or_blank(signal_row.get("open")),
            "signal_high": _finite_or_blank(signal_row.get("high")),
            "signal_low": _finite_or_blank(signal_row.get("low")),
            "signal_close": _finite_or_blank(c.signal_close),
            "signal_volume": _finite_or_blank(signal_row.get("volume")),
            "quality_score": _finite_or_blank(c.quality_score),
            "rs_pct": _finite_or_blank(c.rs_pct),
            "market_ret_pct": _finite_or_blank(c.market_ret_pct),
            "regime": str(c.regime),
            "vol_ratio": _finite_or_blank(c.vol_ratio),
            "atr_pct": _finite_or_blank(c.atr_pct),
            "body_pct": _finite_or_blank(c.body_pct),
            "close_loc": _finite_or_blank(c.close_loc),
            "vwap_dist_atr": _finite_or_blank(c.vwap_dist_atr),
            "reason": str(c.reason),
            "status": "CANDIDATE",
            "created_at_ist": _fmt_ist(created_at),
            "diagnostics_json": json.dumps(diag, default=str),
            **_research_shadow_metadata(side, setup, c.close_loc, c.vwap_dist_atr),
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    if dedupe:
        return _dedupe_candidate_frame(out)
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    return (
        out.sort_values(["signal_time_ist", "ticker", "quality_score", "setup"], ascending=[True, True, False, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .reset_index(drop=True)
    )


def _dedupe_candidate_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the single best signal candidate per ticker per signal candle.

    Multiple setup labels can fire on the same 5-minute candle for the same
    ticker. Candidate discovery is signal-only, so downstream entry should see
    only the strongest ticker candidate, not one row per setup label.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["signal_time_ist"] = out["signal_time_ist"].astype(str)
    out = (
        out.sort_values(["quality_score", "ticker", "setup"], ascending=[False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )
    return out


_MARKET_CTX_CACHE: Dict[str, Dict[str, Any]] = {}


def build_market_context_once() -> Dict[str, Dict[str, Any]]:
    global _MARKET_CTX_CACHE
    if not _MARKET_CTX_CACHE:
        v2.DATA_ROOT_5M = LIVE_5M_DIR
        v2._init_worker({
            "ENABLE_NOISY_ADVANCED_SHORTS": True,
            "ENABLE_NATIVE_V2_MINED_FILTER": False,
        })
        _MARKET_CTX_CACHE = v2._load_market_context()
    return _MARKET_CTX_CACHE


_WORKER_MARKET_CTX: Optional[Dict[str, Dict[str, Any]]] = None


def _worker_init() -> None:
    global _WORKER_MARKET_CTX
    v2.DATA_ROOT_5M = LIVE_5M_DIR
    v2._init_worker({
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    })
    try:
        _WORKER_MARKET_CTX = v2._load_market_context()
    except Exception:
        _WORKER_MARKET_CTX = {}


def _worker_scan(payload: Tuple[str, str] | Tuple[str, str, bool]) -> List[Dict[str, Any]]:
    if len(payload) >= 3:
        ticker, slot_iso, dedupe = payload
    else:
        ticker, slot_iso = payload
        dedupe = True
    global _WORKER_MARKET_CTX
    if _WORKER_MARKET_CTX is None:
        _worker_init()
    try:
        out = scan_ticker_signal_candle(ticker, pd.Timestamp(slot_iso), _WORKER_MARKET_CTX or {})
    except Exception:
        return []
    if not out:
        return []
    return candidates_to_dataframe(out, pd.Timestamp(slot_iso), dedupe=bool(dedupe)).to_dict("records")


def scan_slot_candidates(
    slot_ist: Any,
    tickers: Iterable[str],
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    max_workers: Optional[int] = None,
    *,
    dedupe: bool = True,
) -> pd.DataFrame:
    slot_ts = _ensure_ist_ts(slot_ist)
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)
    rows: List[Dict[str, Any]] = []

    if workers <= 1:
        if market_ctx is None:
            market_ctx = build_market_context_once()
        for ticker in tickers:
            try:
                found = scan_ticker_signal_candle(ticker, slot_ts, market_ctx)
            except Exception:
                found = []
            if found:
                rows.extend(candidates_to_dataframe(found, slot_ts, dedupe=dedupe).to_dict("records"))
    else:
        slot_iso = slot_ts.isoformat()
        payloads = [(ticker, slot_iso, bool(dedupe)) for ticker in tickers]
        with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init) as ex:
            for result in ex.map(_worker_scan, payloads, chunksize=24):
                if result:
                    rows.extend(result)

    if not rows:
        return pd.DataFrame()
    df = _dedupe_candidate_frame(pd.DataFrame(rows)) if dedupe else pd.DataFrame(rows).drop_duplicates(subset=["candidate_id"], keep="first")
    if df.empty:
        return pd.DataFrame()
    return df.sort_values(["quality_score", "ticker"], ascending=[False, True]).reset_index(drop=True)
