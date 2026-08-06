"""Isolated V12 replay for the hourly-prefilter two-bar LONG impulse.

This module is deliberately research-only.  It does not modify the approved V12
setup book or the live scanner.  It builds candidates from the complete K300
hourly prefilter, applies the causal ``slot+5 .. slot+60`` membership contract,
detects two consecutive close-to-close five-minute gains, and delegates entry
price discovery to V12's historical one-minute entry engine.

Primary signal
--------------
* hourly row is complete, non-stale, and ``primary_side == LONG``;
* two adjacent close-to-close 5-minute returns observed while membership is
  continuously active are each in [0.50%, 1.50%];
* one signal per uninterrupted qualifying streak;
* loose context score >= 2 with no hard AVWAP/ADX/stochastic requirement;
* V12 next-available one-minute-open entry and statutory NSE costs.

Boundary contract
-----------------
A 09:20 membership is active at 09:25..10:20 inclusive.  The 10:20
membership becomes active at 10:25.  Thus a valid 10:20 signal is owned by the
09:20 list and cannot be retroactively removed by the 10:20 refresh.  Detector
state carries through the boundary only when eligibility remains contiguous.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import avwap_5min_ID_v12_backtesting as v12
import nse_intraday_costs as nse
import research_v12_prefilter_train_test_optimizer as optimizer


IST = "Asia/Kolkata"
SETUP = "L_HOURLY_TWO_BAR_5M_MOMENTUM"
PRODUCTION_APPROVED = False

DEFAULT_START = "2026-06-05"
DEFAULT_END = "2026-08-04"
DEFAULT_PREFILTER = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_six_month_replay_20260205_20260804_k300"
    r"\hourly_candidates_20260205_20260804_k300.csv"
)
DEFAULT_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_OUT = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_hourly_two_bar_long_20260605_20260804"
)

ENTRY_START_MINUTE = 9 * 60 + 25
ENTRY_END_MINUTE = 14 * 60 + 30
TRIGGER_MIN_PCT = 0.50
TRIGGER_MAX_PCT = 1.50
MIN_PRICE_RS = 80.0
MIN_5M_TRADED_VALUE_RS = 1_000_000.0
MAX_RANGE_ATR = 3.5
MIN_CONTEXT_SCORE = 2
MAX_VWAP_EXTENSION_ATR = 2.0
MAX_ENTRY_GAP_ATR = 0.20
MAX_STOP_DISTANCE_ATR = 1.25
STOP_BUFFER_ATR = 0.10
MAX_ORDER_PARTICIPATION = 0.02
EXPECTED_ONE_MINUTE_VOLUME_DIVISOR = 5.0
PRIMARY_TARGET_R = 1.50
TIME_STOP_MINUTES = 15
TIME_STOP_MIN_MFE_R = 0.50
TRAIL_TRIGGER_R = 1.00

BAR_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ATR",
    "ADX",
    "Stoch_%K",
    "Stoch_%D",
    "gap_filled",
    "opening_snapshot",
)

ONE_MINUTE_COLUMNS = ("date", "open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class ExitPolicy:
    name: str
    target_r: float = PRIMARY_TARGET_R
    conditional_time_stop: bool = False
    two_bar_low_trail: bool = False


PRIMARY_POLICY = ExitPolicy(
    "full_config",
    target_r=PRIMARY_TARGET_R,
    conditional_time_stop=True,
    two_bar_low_trail=True,
)
FIXED_POLICY = ExitPolicy("fixed_structure_1p5r", target_r=PRIMARY_TARGET_R)


def _normalise_ist(values: pd.Series) -> pd.Series:
    out = pd.to_datetime(values, errors="coerce")
    if out.dt.tz is None:
        return out.dt.tz_localize(IST)
    return out.dt.tz_convert(IST)


def _timestamp_ist(value: object) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        return pd.NaT
    return stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _profit_factor(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(pnl.loc[pnl > 0].sum())
    losses = float(-pnl.loc[pnl < 0].sum())
    if losses <= 0:
        return float("inf") if gains > 0 else 0.0
    return gains / losses


def load_long_memberships(
    path: Path,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = [
        "slot_ist",
        "ticker",
        "selection_rank",
        "selection_bucket",
        "primary_side",
        "primary_family",
        "selection_reason",
        "overall_score",
        "long_score",
        "activity_score",
        "date",
        "staleness_seconds",
    ]
    source = pd.read_csv(path, usecols=columns)
    source["slot_ist"] = _normalise_ist(source["slot_ist"])
    source["date"] = _normalise_ist(source["date"])
    source["ticker"] = source["ticker"].astype(str).str.upper().str.strip()
    source["trade_date"] = source["slot_ist"].dt.strftime("%Y-%m-%d")
    source = source.loc[source["trade_date"].between(start_date, end_date)].copy()
    if source.empty:
        raise RuntimeError("no hourly prefilter rows in requested window")
    if source[["slot_ist", "ticker"]].duplicated().any():
        raise RuntimeError("duplicate ticker in hourly prefilter slot")
    stale = pd.to_numeric(source["staleness_seconds"], errors="coerce")
    completed = source["date"].eq(source["slot_ist"]) & stale.eq(0.0)
    slot_minute = source["slot_ist"].dt.hour * 60 + source["slot_ist"].dt.minute
    entry_relevant_slot = slot_minute.add(5).le(ENTRY_END_MINUTE)
    if (~completed & entry_relevant_slot).any():
        rejected = source.loc[
            ~completed & entry_relevant_slot,
            ["slot_ist", "ticker", "date", "staleness_seconds"],
        ]
        raise RuntimeError(
            "strict completed-hourly contract rejected an entry-relevant snapshot:\n"
            f"{rejected.head(20)}"
        )

    session_rows: list[dict[str, Any]] = []
    for trade_date, day in source.groupby("trade_date", sort=True):
        slots = sorted(day["slot_ist"].dt.strftime("%H:%M").unique())
        expected_entry_slots = [f"{hour:02d}:20" for hour in range(9, 15)]
        allowed_slots = expected_entry_slots + ["15:20"]
        actionable_slots = [slot for slot in slots if slot in expected_entry_slots]
        unexpected_slots = [slot for slot in slots if slot not in allowed_slots]
        if actionable_slots != expected_entry_slots or unexpected_slots:
            raise RuntimeError(
                f"actionable hourly schedule mismatch {trade_date}: {slots}"
            )
        for slot, group in day.groupby("slot_ist", sort=True):
            slot_minute_value = int(slot.hour) * 60 + int(slot.minute)
            slot_is_entry_relevant = slot_minute_value + 5 <= ENTRY_END_MINUTE
            if not slot_is_entry_relevant:
                continue
            ranks = pd.to_numeric(group["selection_rank"], errors="coerce")
            if len(group) != 300 or group["ticker"].nunique() != 300:
                raise RuntimeError(f"K300 count mismatch at {slot}: {len(group)}")
            if set(ranks.astype(int)) != set(range(1, 301)):
                raise RuntimeError(f"K300 rank mismatch at {slot}")
        session_rows.append(
            {
                "trade_date": trade_date,
                "slots": int(day["slot_ist"].nunique()),
                "rows": int(len(day)),
                "actionable_slots": int(len(actionable_slots)),
                "terminal_1520_rows": int(
                    day["slot_ist"].dt.strftime("%H:%M").eq("15:20").sum()
                ),
                "long_rows": int(day["primary_side"].astype(str).str.upper().eq("LONG").sum()),
            }
        )

    long_mask = source["primary_side"].astype(str).str.upper().eq("LONG")
    # The 15:20 list first becomes usable at 15:25, after the entry cutoff.  It
    # remains part of the source integrity audit but is never an eligibility
    # source; this also prevents an irrelevant degraded closing snapshot from
    # weakening the strict contract applied to every actionable hourly list.
    memberships = source.loc[long_mask & completed & entry_relevant_slot].copy()
    ranks = pd.to_numeric(memberships["selection_rank"], errors="coerce")
    memberships = memberships.loc[ranks.between(1, 300, inclusive="both")].copy()
    memberships = memberships.sort_values(
        ["ticker", "slot_ist", "selection_rank"], kind="mergesort"
    ).reset_index(drop=True)
    audit = {
        "source_rows": int(len(source)),
        "sessions": int(source["trade_date"].nunique()),
        "slots": int(source["slot_ist"].nunique()),
        "completed_entry_relevant_rows": int((completed & entry_relevant_slot).sum()),
        "rejected_entry_relevant_rows": int((~completed & entry_relevant_slot).sum()),
        "inactive_noncompleted_rows": int((~completed & ~entry_relevant_slot).sum()),
        "long_memberships": int(len(memberships)),
        "unique_long_tickers": int(memberships["ticker"].nunique()),
        "session_rows": session_rows,
    }
    return memberships, audit


def expand_membership_schedule(memberships: pd.DataFrame) -> pd.DataFrame:
    """Expand each hourly membership to slot+5..slot+60, inclusive."""
    if memberships.empty:
        return memberships.copy()
    offsets = np.arange(5, 61, 5, dtype=int)
    repeated = memberships.loc[memberships.index.repeat(len(offsets))].copy()
    repeated["membership_offset_minutes"] = np.tile(offsets, len(memberships))
    repeated["signal_time_ist"] = repeated["slot_ist"] + pd.to_timedelta(
        repeated["membership_offset_minutes"], unit="m"
    )
    minute = repeated["signal_time_ist"].dt.hour * 60 + repeated["signal_time_ist"].dt.minute
    repeated = repeated.loc[minute.between(ENTRY_START_MINUTE, ENTRY_END_MINUTE)].copy()
    repeated["signal_minute"] = (
        repeated["signal_time_ist"].dt.hour * 60 + repeated["signal_time_ist"].dt.minute
    )
    repeated["trade_date"] = repeated["signal_time_ist"].dt.strftime("%Y-%m-%d")
    if repeated[["ticker", "signal_time_ist"]].duplicated().any():
        duplicates = repeated.loc[
            repeated[["ticker", "signal_time_ist"]].duplicated(False),
            ["ticker", "slot_ist", "signal_time_ist"],
        ]
        raise RuntimeError(f"overlapping hourly membership schedule:\n{duplicates.head(20)}")
    return repeated.sort_values(
        ["ticker", "signal_time_ist"], kind="mergesort"
    ).reset_index(drop=True)


def _read_parquet_window(path: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        available = set(pq.ParquetFile(path).schema_arrow.names)
    except Exception:
        return None
    columns = [column for column in BAR_COLUMNS if column in available]
    if "date" not in columns:
        return None
    try:
        frame = pd.read_parquet(
            path,
            columns=columns,
            filters=[("date", ">=", start), ("date", "<", end)],
        )
    except Exception:
        try:
            frame = pd.read_parquet(path, columns=columns)
        except Exception:
            return None
    if frame is None or frame.empty:
        return None
    frame["date"] = _normalise_ist(frame["date"])
    return frame.loc[frame["date"].between(start, end, inclusive="left")].copy()


def _read_one_minute_window(
    path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        available = set(pq.ParquetFile(path).schema_arrow.names)
    except Exception:
        return None
    columns = [column for column in ONE_MINUTE_COLUMNS if column in available]
    if set(ONE_MINUTE_COLUMNS) - set(columns):
        return None
    try:
        frame = pd.read_parquet(
            path,
            columns=columns,
            filters=[("date", ">=", start), ("date", "<", end)],
        )
    except Exception:
        try:
            frame = pd.read_parquet(path, columns=columns)
        except Exception:
            return None
    if frame is None or frame.empty:
        return None
    frame["date"] = _normalise_ist(frame["date"])
    frame = frame.loc[frame["date"].between(start, end, inclusive="left")].copy()
    return frame if not frame.empty else None


def _aggregate_one_minute_bars(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy().dropna(subset=["date"])
    for column in ONE_MINUTE_COLUMNS[1:]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.dropna(subset=["open", "high", "low", "close", "volume"])
    work = work.sort_values("date", kind="mergesort").drop_duplicates("date", keep="last")
    clock = work["date"].dt.hour * 60 + work["date"].dt.minute
    work = work.loc[clock.between(9 * 60 + 16, 15 * 60 + 30)].copy()
    work["completion_time"] = work["date"].dt.ceil("5min")
    grouped = work.groupby("completion_time", sort=True, as_index=False).agg(
        one_open=("open", "first"),
        one_high=("high", "max"),
        one_low=("low", "min"),
        one_close=("close", "last"),
        one_volume=("volume", "sum"),
        one_count=("date", "size"),
    )
    return grouped.loc[grouped["one_count"].eq(5)].set_index("completion_time")


def _alignment_distance(
    five: pd.DataFrame,
    aggregate: pd.DataFrame,
) -> pd.Series:
    scale = pd.to_numeric(five["close"], errors="coerce").abs().clip(lower=1.0)
    distance = pd.Series(0.0, index=five.index, dtype=float)
    for five_column, one_column in (
        ("open", "one_open"),
        ("high", "one_high"),
        ("low", "one_low"),
        ("close", "one_close"),
    ):
        distance += (
            pd.to_numeric(five[five_column], errors="coerce")
            - pd.to_numeric(aggregate[one_column], errors="coerce")
        ).abs() / scale
    volume = pd.to_numeric(five["volume"], errors="coerce").abs().clip(lower=1.0)
    distance += 0.25 * (
        pd.to_numeric(five["volume"], errors="coerce")
        - pd.to_numeric(aggregate["one_volume"], errors="coerce")
    ).abs() / volume
    valid = pd.to_numeric(aggregate["one_count"], errors="coerce").eq(5)
    return distance.where(valid, np.inf)


def _day_scaled_alignment_distance(
    five: pd.DataFrame,
    aggregate: pd.DataFrame,
    trade_dates: pd.Series,
) -> pd.Series:
    """OHLCV distance robust to split-adjustment differences between stores."""
    output = pd.Series(np.inf, index=five.index, dtype=float)
    for trade_date, day_index in trade_dates.groupby(trade_dates, sort=False).groups.items():
        day_five = five.loc[day_index]
        day_one = aggregate.loc[day_index].copy()
        valid = (
            pd.to_numeric(day_five["close"], errors="coerce").gt(0.0)
            & pd.to_numeric(day_one["one_close"], errors="coerce").gt(0.0)
            & pd.to_numeric(day_one["one_count"], errors="coerce").eq(5)
        )
        if not valid.any():
            continue
        price_ratio = (
            pd.to_numeric(day_five.loc[valid, "close"], errors="coerce")
            / pd.to_numeric(day_one.loc[valid, "one_close"], errors="coerce")
        ).replace([np.inf, -np.inf], np.nan).dropna().median()
        volume_ratio = (
            pd.to_numeric(day_five.loc[valid, "volume"], errors="coerce")
            / pd.to_numeric(day_one.loc[valid, "one_volume"], errors="coerce").replace(0.0, np.nan)
        ).replace([np.inf, -np.inf], np.nan).dropna().median()
        if not (pd.notna(price_ratio) and float(price_ratio) > 0.0):
            continue
        for column in ("one_open", "one_high", "one_low", "one_close"):
            day_one[column] = pd.to_numeric(day_one[column], errors="coerce") * float(price_ratio)
        if pd.notna(volume_ratio) and float(volume_ratio) > 0.0:
            day_one["one_volume"] = (
                pd.to_numeric(day_one["one_volume"], errors="coerce") * float(volume_ratio)
            )
        output.loc[day_index] = _alignment_distance(day_five, day_one)
    return output


def align_mixed_five_minute_completion_times(
    five_minute: pd.DataFrame,
    one_minute: pd.DataFrame,
    required_trade_dates: Iterable[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Repair the store's mixed native/+5-minute historical stamp regimes.

    The source has ticker-days where a row stamped ``t`` exactly aggregates the
    five end-stamped one-minute candles through ``t-5``.  Other ticker-days are
    correctly stamped at ``t``.  Classification is made independently per day
    from completed midday OHLCV bars, then only rows at 09:25 or later are
    shifted.  This preserves the 09:15 opening snapshot and de-duplicates the
    first real 09:20 candle.
    """
    work = five_minute.copy().dropna(subset=["date"])
    work = work.sort_values("date", kind="mergesort").drop_duplicates("date", keep="last")
    work["source_5m_date"] = work["date"]
    work["source_trade_date"] = work["date"].dt.strftime("%Y-%m-%d")
    aggregates = _aggregate_one_minute_bars(one_minute)
    if aggregates.empty:
        raise RuntimeError("one-minute bars cannot classify five-minute timestamps")

    same = aggregates.reindex(pd.DatetimeIndex(work["date"])).reset_index(drop=True)
    minus5 = aggregates.reindex(
        pd.DatetimeIndex(work["date"] - pd.Timedelta(minutes=5))
    ).reset_index(drop=True)
    same.index = work.index
    minus5.index = work.index
    same_distance = _day_scaled_alignment_distance(
        work, same, work["source_trade_date"]
    )
    minus5_distance = _day_scaled_alignment_distance(
        work, minus5, work["source_trade_date"]
    )
    clock = work["date"].dt.hour * 60 + work["date"].dt.minute
    voting = clock.between(9 * 60 + 30, 14 * 60 + 30)
    if "gap_filled" in work:
        voting &= pd.to_numeric(work["gap_filled"], errors="coerce").fillna(0.0).lt(0.5)
    if "opening_snapshot" in work:
        voting &= ~work["opening_snapshot"].fillna(False).astype(bool)
    separation = (same_distance - minus5_distance).abs()
    confident = voting & pd.concat([same_distance, minus5_distance], axis=1).min(axis=1).lt(0.02)
    confident &= separation.gt(0.001)
    work["_same_vote"] = confident & same_distance.lt(minus5_distance)
    work["_minus5_vote"] = confident & minus5_distance.lt(same_distance)
    work["_same_distance"] = same_distance
    work["_minus5_distance"] = minus5_distance

    required = sorted({str(value) for value in required_trade_dates})
    shifts: dict[str, int] = {}
    unclassified: list[str] = []
    same_votes = 0
    minus5_votes = 0
    for trade_date in required:
        day = work.loc[work["source_trade_date"].eq(trade_date) & voting]
        current_votes = int(day["_same_vote"].sum())
        delayed_votes = int(day["_minus5_vote"].sum())
        same_votes += current_votes
        minus5_votes += delayed_votes
        if current_votes > delayed_votes:
            shifts[trade_date] = 0
            continue
        if delayed_votes > current_votes:
            shifts[trade_date] = -5
            continue
        finite = day.loc[
            np.isfinite(day["_same_distance"]) & np.isfinite(day["_minus5_distance"])
        ]
        if not finite.empty:
            same_median = float(finite["_same_distance"].median())
            delayed_median = float(finite["_minus5_distance"].median())
            if abs(same_median - delayed_median) > 0.001 and min(same_median, delayed_median) < 0.02:
                shifts[trade_date] = 0 if same_median < delayed_median else -5
                continue
        unclassified.append(trade_date)

    if unclassified:
        raise RuntimeError(
            "unable to causally classify mixed five-minute timestamps for "
            + ",".join(unclassified[:20])
        )

    adjustment = work["source_trade_date"].map(shifts).fillna(0).astype(int)
    shiftable = clock.ge(9 * 60 + 25) & adjustment.eq(-5)
    work["completion_time_adjustment_minutes"] = np.where(shiftable, -5, 0)
    work.loc[shiftable, "date"] = work.loc[shiftable, "date"] - pd.Timedelta(minutes=5)
    work = (
        work.sort_values(["date", "source_5m_date"], kind="mergesort")
        .drop_duplicates("date", keep="last")
        .drop(
            columns=[
                "source_trade_date",
                "_same_vote",
                "_minus5_vote",
                "_same_distance",
                "_minus5_distance",
            ],
            errors="ignore",
        )
        .reset_index(drop=True)
    )
    audit = {
        "relevant_days": len(required),
        "shifted_days": int(sum(value == -5 for value in shifts.values())),
        "native_days": int(sum(value == 0 for value in shifts.values())),
        "same_votes": same_votes,
        "minus5_votes": minus5_votes,
        "rows_shifted": int(shiftable.sum()),
        "unclassified_days": unclassified,
    }
    return work, audit


def _wilder_dmi(frame: pd.DataFrame, period: int = 14) -> tuple[pd.Series, pd.Series]:
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=frame.index)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [(high - low).abs(), (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    alpha = 1.0 / float(period)
    smoothed_tr = true_range.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    smoothed_plus = plus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    smoothed_minus = minus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    plus_di = 100.0 * smoothed_plus / smoothed_tr.replace(0.0, np.nan)
    minus_di = 100.0 * smoothed_minus / smoothed_tr.replace(0.0, np.nan)
    return plus_di, minus_di


def _prepare_ticker_bars(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    for column in BAR_COLUMNS:
        if column not in work.columns:
            work[column] = np.nan
    for column in BAR_COLUMNS:
        if column not in {"date"}:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.dropna(subset=["date"]).sort_values("date", kind="mergesort")
    work = work.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    clock = work["date"].dt.hour * 60 + work["date"].dt.minute
    work = work.loc[clock.between(9 * 60 + 15, 15 * 60 + 20)].copy().reset_index(drop=True)
    work["trade_date"] = work["date"].dt.strftime("%Y-%m-%d")
    group = work.groupby("trade_date", sort=False, group_keys=False)

    open_price = pd.to_numeric(work["open"], errors="coerce")
    high = pd.to_numeric(work["high"], errors="coerce")
    low = pd.to_numeric(work["low"], errors="coerce")
    close = pd.to_numeric(work["close"], errors="coerce")
    volume = pd.to_numeric(work["volume"], errors="coerce")
    atr = pd.to_numeric(work["ATR"], errors="coerce")
    gap = pd.to_numeric(work["gap_filled"], errors="coerce").fillna(0.0)
    real = open_price.gt(0) & high.gt(0) & low.gt(0) & close.gt(0) & gap.lt(0.5)

    previous_close = group["close"].shift(1)
    previous_time = group["date"].shift(1)
    previous_real = real.groupby(work["trade_date"], sort=False).shift(1).astype("boolean").fillna(False)
    contiguous = (work["date"] - previous_time).dt.total_seconds().eq(300.0)
    work["return_5m_close_pct"] = np.where(
        real & previous_real & contiguous,
        (close / pd.to_numeric(previous_close, errors="coerce") - 1.0) * 100.0,
        np.nan,
    )
    work["previous_return_5m_close_pct"] = work.groupby(
        "trade_date", sort=False
    )["return_5m_close_pct"].shift(1)
    work["return_pair_exact"] = (
        work["return_5m_close_pct"].notna()
        & work["previous_return_5m_close_pct"].notna()
    )

    completed = (work["date"].dt.hour * 60 + work["date"].dt.minute).ge(9 * 60 + 20)
    typical = (high + low + close) / 3.0
    weighted = (typical * volume).where(completed, 0.0)
    completed_volume = volume.where(completed, 0.0)
    cumulative_value = weighted.groupby(work["trade_date"]).cumsum()
    cumulative_volume = completed_volume.groupby(work["trade_date"]).cumsum()
    work["session_vwap_causal"] = cumulative_value / cumulative_volume.replace(0.0, np.nan)
    work["vwap_dist_atr"] = (close - work["session_vwap_causal"]) / atr.replace(0.0, np.nan)
    vwap_lag3 = work.groupby("trade_date", sort=False)["session_vwap_causal"].shift(3)
    work["vwap_slope_3"] = work["session_vwap_causal"] - vwap_lag3

    complete_volume = volume.where(real)
    previous_volume_median = complete_volume.groupby(work["trade_date"], sort=False).transform(
        lambda values: values.shift(1).rolling(20, min_periods=8).median()
    )
    work["volume_ratio20"] = volume / previous_volume_median.replace(0.0, np.nan)
    work["traded_value_rs"] = close * volume
    candle_range = high - low
    work["close_location"] = (close - low) / candle_range.replace(0.0, np.nan)
    body_top = pd.concat([open_price, close], axis=1).max(axis=1)
    work["upper_wick_fraction"] = (high - body_top) / candle_range.replace(0.0, np.nan)
    work["range_atr"] = candle_range / atr.replace(0.0, np.nan)

    complete_bars = work.loc[real].copy()
    plus_di, minus_di = _wilder_dmi(complete_bars)
    work["plus_di"] = np.nan
    work["minus_di"] = np.nan
    work.loc[complete_bars.index, "plus_di"] = plus_di
    work.loc[complete_bars.index, "minus_di"] = minus_di
    adx = pd.to_numeric(work["ADX"], errors="coerce")
    work["adx_rising_3"] = adx > adx.shift(3)
    stoch_k = pd.to_numeric(work["Stoch_%K"], errors="coerce")
    stoch_d = pd.to_numeric(work["Stoch_%D"], errors="coerce")
    work["stoch_rising"] = stoch_k > group["Stoch_%K"].shift(1)
    work["stoch_bullish"] = (stoch_k > stoch_d) | work["stoch_rising"]
    return work


def load_nifty_context(five_minute_dir: Path, start_date: str, end_date: str) -> tuple[pd.DataFrame, list[str]]:
    start = pd.Timestamp(start_date, tz=IST) - pd.Timedelta(days=3)
    end = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    path = five_minute_dir / "NIFTY_stocks_indicators_5min.parquet"
    frame = _read_parquet_window(path, start, end)
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["signal_time_ist", "nifty_aligned"]), []
    prepared = _prepare_ticker_bars(frame)
    close = pd.to_numeric(prepared["close"], errors="coerce")
    group = prepared.groupby("trade_date", sort=False)
    ret_10m = (close / group["close"].shift(2) - 1.0) * 100.0
    prepared["nifty_aligned"] = (
        (close >= prepared["session_vwap_causal"]) | ret_10m.ge(-0.30)
    )
    expected_sessions = sorted(
        prepared.loc[
            prepared["trade_date"].between(start_date, end_date), "trade_date"
        ].unique()
    )
    return (
        prepared.loc[
            prepared["trade_date"].between(start_date, end_date),
            ["date", "nifty_aligned"],
        ]
        .rename(columns={"date": "signal_time_ist"})
        .drop_duplicates("signal_time_ist", keep="last"),
        expected_sessions,
    )


def _load_one_ticker(
    ticker: str,
    eligibility: pd.DataFrame,
    five_minute_dir: Path,
    one_minute_dir: Path,
    start_date: str,
    end_date: str,
    nifty: pd.DataFrame,
    correct_mixed_stamps: bool,
) -> tuple[str, pd.DataFrame | None, str, dict[str, Any]]:
    path = five_minute_dir / f"{ticker}_stocks_indicators_5min.parquet"
    start = pd.Timestamp(start_date, tz=IST) - pd.Timedelta(days=35)
    end = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    frame = _read_parquet_window(path, start, end)
    if frame is None or frame.empty:
        return ticker, None, "missing_5m", {}
    alignment_audit: dict[str, Any] = {
        "relevant_days": int(eligibility["trade_date"].nunique()),
        "shifted_days": 0,
        "native_days": int(eligibility["trade_date"].nunique()),
        "same_votes": 0,
        "minus5_votes": 0,
        "rows_shifted": 0,
        "unclassified_days": [],
    }
    if correct_mixed_stamps:
        one_path = one_minute_dir / f"{ticker}_stocks_indicators_1min.parquet"
        one = _read_one_minute_window(
            one_path,
            pd.Timestamp(start_date, tz=IST),
            end,
        )
        if one is None or one.empty:
            return ticker, None, "missing_1m_for_time_alignment", {}
        try:
            frame, alignment_audit = align_mixed_five_minute_completion_times(
                frame,
                one,
                eligibility["trade_date"].unique(),
            )
        except Exception as exc:
            return ticker, None, f"time_alignment:{type(exc).__name__}:{exc}", {}
    else:
        frame["source_5m_date"] = frame["date"]
        frame["completion_time_adjustment_minutes"] = 0
    bars = _prepare_ticker_bars(frame)
    bars = bars.loc[bars["trade_date"].between(start_date, end_date)].copy()
    feature_columns = [
        "date",
        "source_5m_date",
        "completion_time_adjustment_minutes",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ATR",
        "ADX",
        "Stoch_%K",
        "Stoch_%D",
        "gap_filled",
        "return_5m_close_pct",
        "previous_return_5m_close_pct",
        "return_pair_exact",
        "session_vwap_causal",
        "vwap_dist_atr",
        "vwap_slope_3",
        "volume_ratio20",
        "traded_value_rs",
        "close_location",
        "upper_wick_fraction",
        "range_atr",
        "plus_di",
        "minus_di",
        "adx_rising_3",
        "stoch_rising",
        "stoch_bullish",
    ]
    features = bars[feature_columns].rename(
        columns={
            "date": "signal_time_ist",
            "open": "signal_open",
            "high": "signal_high",
            "low": "signal_low",
            "close": "signal_close",
            "volume": "signal_volume",
            "ATR": "signal_atr",
            "ADX": "signal_adx",
            "Stoch_%K": "stoch_k",
            "Stoch_%D": "stoch_d",
        }
    )
    merged = eligibility.merge(features, on="signal_time_ist", how="left", validate="one_to_one")
    merged = merged.merge(nifty, on="signal_time_ist", how="left", validate="many_to_one")
    merged["nifty_aligned"] = merged["nifty_aligned"].astype("boolean").fillna(False)
    return ticker, merged, "", alignment_audit


def load_eligible_bar_states(
    eligibility: pd.DataFrame,
    five_minute_dir: Path,
    one_minute_dir: Path,
    start_date: str,
    end_date: str,
    nifty: pd.DataFrame,
    workers: int,
    correct_mixed_stamps: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    groups = {ticker: group.copy() for ticker, group in eligibility.groupby("ticker", sort=False)}
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    alignment_records: list[dict[str, Any]] = []
    started = time.time()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                _load_one_ticker,
                ticker,
                group,
                five_minute_dir,
                one_minute_dir,
                start_date,
                end_date,
                nifty,
                correct_mixed_stamps,
            ): ticker
            for ticker, group in groups.items()
        }
        for done, future in enumerate(as_completed(futures), 1):
            ticker, frame, error, alignment_audit = future.result()
            if frame is not None:
                frames.append(frame)
                alignment_records.append({"ticker": ticker, **alignment_audit})
            else:
                errors.append(f"{ticker}:{error}")
            if done % 200 == 0 or done == len(futures):
                print(
                    f"[two-bar 5m] {done:,}/{len(futures):,} tickers "
                    f"loaded={len(frames):,} errors={len(errors):,} elapsed={time.time()-started:.1f}s",
                    flush=True,
                )
    if errors:
        raise RuntimeError(f"incomplete 5m feature load: {errors[:20]}")
    merged = pd.concat(frames, ignore_index=True, copy=False)
    merged = merged.sort_values(["ticker", "signal_time_ist"], kind="mergesort").reset_index(drop=True)
    return merged, {
        "requested_tickers": len(groups),
        "loaded_tickers": len(frames),
        "errors": errors,
        "eligible_rows": int(len(eligibility)),
        "merged_rows": int(len(merged)),
        "elapsed_seconds": time.time() - started,
        "mixed_timestamp_correction_enabled": bool(correct_mixed_stamps),
        "alignment_relevant_ticker_days": int(
            sum(record.get("relevant_days", 0) for record in alignment_records)
        ),
        "alignment_shifted_ticker_days": int(
            sum(record.get("shifted_days", 0) for record in alignment_records)
        ),
        "alignment_native_ticker_days": int(
            sum(record.get("native_days", 0) for record in alignment_records)
        ),
        "alignment_rows_shifted": int(
            sum(record.get("rows_shifted", 0) for record in alignment_records)
        ),
        "alignment_same_votes": int(
            sum(record.get("same_votes", 0) for record in alignment_records)
        ),
        "alignment_minus5_votes": int(
            sum(record.get("minus5_votes", 0) for record in alignment_records)
        ),
        "_alignment_records": alignment_records,
    }


def mark_first_two_bar_trigger(
    frame: pd.DataFrame,
    *,
    minimum: float = TRIGGER_MIN_PCT,
    maximum: float = TRIGGER_MAX_PCT,
) -> pd.Series:
    """Return one trigger per qualifying streak inside each eligibility spell.

    Both returns must be observed on rows in the active membership spell.  A
    bar immediately before a ticker first becomes eligible (or is reselected
    after a gap) cannot seed the two-bar count.
    """
    if frame.empty:
        return pd.Series(dtype=bool, index=frame.index)
    work = frame.sort_values(["ticker", "trade_date", "signal_time_ist"], kind="mergesort")
    output = pd.Series(False, index=work.index, dtype=bool)
    for _, group in work.groupby(["ticker", "trade_date"], sort=False):
        previous_time: pd.Timestamp | None = None
        run = 0
        latched = False
        for row in group.itertuples():
            current_time = _timestamp_ist(row.signal_time_ist)
            contiguous_eligibility = (
                previous_time is not None
                and (current_time - previous_time).total_seconds() == 300.0
            )
            current_return = float(row.return_5m_close_pct) if pd.notna(row.return_5m_close_pct) else math.nan
            current_qualifies = math.isfinite(current_return) and minimum <= current_return <= maximum
            if not contiguous_eligibility:
                run = 0
                latched = False
            if not current_qualifies:
                run = 0
                latched = False
            else:
                run += 1
                if run >= 2 and not latched:
                    output.at[row.Index] = True
                    latched = True
            previous_time = current_time
    return output.reindex(frame.index).fillna(False).astype(bool)


def add_signal_rules(states: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = states.copy()
    work["raw_two_bar_trigger"] = mark_first_two_bar_trigger(work)
    adx = pd.to_numeric(work["signal_adx"], errors="coerce")
    stoch_k = pd.to_numeric(work["stoch_k"], errors="coerce")
    stoch_d = pd.to_numeric(work["stoch_d"], errors="coerce")
    work["score_avwap_price"] = pd.to_numeric(work["vwap_dist_atr"], errors="coerce").ge(0.0)
    work["score_avwap_slope"] = pd.to_numeric(work["vwap_slope_3"], errors="coerce").ge(0.0)
    work["score_dmi"] = pd.to_numeric(work["plus_di"], errors="coerce") > pd.to_numeric(work["minus_di"], errors="coerce")
    work["score_adx"] = adx.ge(25.0) | (adx.ge(20.0) & work["adx_rising_3"].astype(bool))
    work["score_stochastic"] = (stoch_k > stoch_d) | work["stoch_rising"].astype(bool)
    work["score_relative_volume"] = pd.to_numeric(work["volume_ratio20"], errors="coerce").ge(1.20)
    work["score_candle_quality"] = pd.to_numeric(work["close_location"], errors="coerce").ge(0.60)
    work["score_market"] = work["nifty_aligned"].astype(bool)
    score_columns = [column for column in work.columns if column.startswith("score_")]
    work["context_score"] = work[score_columns].fillna(False).astype(int).sum(axis=1)

    signal_atr = pd.to_numeric(work["signal_atr"], errors="coerce")
    signal_close = pd.to_numeric(work["signal_close"], errors="coerce")
    work["common_gate_pass"] = (
        work["return_pair_exact"].astype("boolean").fillna(False)
        & signal_close.ge(MIN_PRICE_RS)
        & pd.to_numeric(work["traded_value_rs"], errors="coerce").ge(MIN_5M_TRADED_VALUE_RS)
        & signal_atr.gt(0.0)
        & pd.to_numeric(work["range_atr"], errors="coerce").le(MAX_RANGE_ATR)
    )
    extension = pd.to_numeric(work["vwap_dist_atr"], errors="coerce").gt(MAX_VWAP_EXTENSION_ATR)
    below_falling = (
        pd.to_numeric(work["vwap_dist_atr"], errors="coerce").lt(-0.50)
        & pd.to_numeric(work["vwap_slope_3"], errors="coerce").lt(0.0)
    )
    opposing_dmi = (
        adx.ge(25.0)
        & work["adx_rising_3"].astype(bool)
        & (pd.to_numeric(work["minus_di"], errors="coerce") > pd.to_numeric(work["plus_di"], errors="coerce"))
    )
    rejection = (
        pd.to_numeric(work["upper_wick_fraction"], errors="coerce").ge(0.60)
        & pd.to_numeric(work["close_location"], errors="coerce").le(0.50)
    )
    work["veto_extension"] = extension
    work["veto_below_falling_vwap"] = below_falling
    work["veto_opposing_dmi"] = opposing_dmi
    work["veto_rejection_candle"] = rejection
    work["hard_veto_pass"] = ~(extension | below_falling | opposing_dmi | rejection)
    work["primary_signal"] = (
        work["raw_two_bar_trigger"]
        & work["common_gate_pass"]
        & work["context_score"].ge(MIN_CONTEXT_SCORE)
    )

    trigger = work["raw_two_bar_trigger"]
    common = trigger & work["common_gate_pass"]
    loose = common & work["context_score"].ge(MIN_CONTEXT_SCORE)
    guarded = loose & work["hard_veto_pass"]
    funnel = [
        {"stage": "eligible_hourly_long_5m_states", "before": len(work), "after": len(work), "removed": 0},
        {"stage": "first_two_bar_trigger_per_streak", "before": len(work), "after": int(trigger.sum()), "removed": int(len(work) - trigger.sum())},
        {"stage": "common_liquidity_and_data_gate", "before": int(trigger.sum()), "after": int(common.sum()), "removed": int(trigger.sum() - common.sum())},
        {"stage": f"primary_loose_context_score_gte_{MIN_CONTEXT_SCORE}", "before": int(common.sum()), "after": int(loose.sum()), "removed": int(common.sum() - loose.sum())},
        {"stage": "optional_guarded_variant_hard_vetoes", "before": int(loose.sum()), "after": int(guarded.sum()), "removed": int(loose.sum() - guarded.sum())},
    ]
    return work, funnel


def candidate_frame(signals: pd.DataFrame) -> pd.DataFrame:
    out = signals.copy().reset_index(drop=True)
    out["setup"] = SETUP
    out["side"] = "LONG"
    out["bar_time_ist"] = out["signal_time_ist"]
    out["decision_ready_at_ist"] = out["signal_time_ist"]
    out["decision_ready_source"] = "completed_second_5minute_return"
    out["quality_score"] = (
        pd.to_numeric(out["context_score"], errors="coerce").fillna(0.0) * 100.0
        + (301.0 - pd.to_numeric(out["selection_rank"], errors="coerce").fillna(300.0)) / 10.0
    )
    out["score"] = out["quality_score"]
    out["_optimizer_row_id"] = np.arange(len(out), dtype=int)
    return out


def install_v12_entry(candidates: pd.DataFrame, start_date: str, end_date: str, workers: int) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    loader = optimizer.install_windowed_1m_loader(v12, start_date=start_date, end_date=end_date)
    prewarm = optimizer.prewarm_windowed_1m_loader(loader, candidates["ticker"], workers=workers)
    optimizer.install_day_1m_adapter(v12, loader)
    v12._V11_EXACT_LIVE_PARITY = False
    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0

    old_exit = v12.v6.SETUP_EXIT_RULES.get(SETUP)
    v12.v6.SETUP_EXIT_RULES[SETUP] = (1.0, 1.5)  # entry-engine placeholder only
    try:
        raw, rejects = v12._v7_entry_engine_raw_rows(candidates)
    finally:
        if old_exit is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_exit
    if rejects is None or (rejects.empty and len(rejects.columns) == 0):
        rejects = pd.DataFrame(
            columns=["ticker", "setup", "signal_time_ist", "reject_reason"]
        )
    return raw, rejects, prewarm


def add_execution_guards(raw: pd.DataFrame) -> pd.DataFrame:
    work = raw.copy()
    raw_entry = pd.to_numeric(work["v7_signal_entry_price"], errors="coerce")
    atr = pd.to_numeric(work["signal_atr"], errors="coerce")
    signal_close = pd.to_numeric(work["signal_close"], errors="coerce")
    work["entry_gap_atr"] = (raw_entry - signal_close) / atr.replace(0.0, np.nan)
    work["entry_price_with_slippage"] = (raw_entry * (1.0 + v12.V7_PAPER_SLIPPAGE_PCT)).round(2)
    work["structure_stop_price"] = (
        pd.to_numeric(work["signal_low"], errors="coerce") - STOP_BUFFER_ATR * atr
    ).round(2)
    work["structure_risk_per_share"] = work["entry_price_with_slippage"] - work["structure_stop_price"]
    work["structure_stop_distance_atr"] = work["structure_risk_per_share"] / atr.replace(0.0, np.nan)

    # Preserve the entry-engine placeholders for audit, then make every public
    # execution field below describe the structural trade actually simulated.
    # In particular, selected-entry CSVs must not advertise V12's temporary
    # 1%/1.5% stop/target or its unconstrained risk-sized quantity.
    placeholder_fields = {
        "v7_signal_entry_price": "entry_engine_raw_entry_price",
        "v7_signal_stop_price": "entry_engine_placeholder_stop_price",
        "v7_signal_target_price": "entry_engine_placeholder_target_price",
        "v7_signal_sl_pct": "entry_engine_placeholder_sl_pct",
        "v7_signal_target_pct": "entry_engine_placeholder_target_pct",
        "quantity": "entry_engine_placeholder_quantity",
        "v7_signal_notional_rs": "entry_engine_placeholder_notional_rs",
    }
    for source, audit_name in placeholder_fields.items():
        if source in work.columns:
            work[audit_name] = work[source]

    risk_quantities = []
    for row in work.itertuples():
        entry = float(row.entry_price_with_slippage)
        stop = float(row.structure_stop_price)
        risk_quantities.append(v12._risk_based_qty(entry, stop) if entry > stop > 0 else 0)
    work["risk_based_quantity"] = pd.Series(risk_quantities, index=work.index, dtype="int64")

    # This capacity estimate is deliberately causal and conservative.  At the
    # decision timestamp only the completed signal 5-minute volume is known, so
    # expected next-minute volume is that completed value divided by five.
    signal_volume = pd.to_numeric(work["signal_volume"], errors="coerce")
    work["expected_1m_volume"] = signal_volume.where(signal_volume.ge(0.0)) / EXPECTED_ONE_MINUTE_VOLUME_DIVISOR
    capacity = np.floor(
        work["expected_1m_volume"].fillna(0.0) * MAX_ORDER_PARTICIPATION
    ).clip(lower=0.0)
    work["causal_capacity_quantity"] = capacity.astype("int64")
    work["quantity"] = np.minimum(
        work["risk_based_quantity"], work["causal_capacity_quantity"]
    ).astype("int64")
    work["order_participation"] = (
        pd.to_numeric(work["quantity"], errors="coerce")
        / pd.to_numeric(work["expected_1m_volume"], errors="coerce").replace(0.0, np.nan)
    )

    work["structure_target_price"] = (
        work["entry_price_with_slippage"]
        + PRIMARY_TARGET_R * work["structure_risk_per_share"]
    ).round(2)
    work["actual_initial_risk_rs"] = (
        work["structure_risk_per_share"] * work["quantity"]
    )

    # Neutralise the stale V12 placeholder execution fields.  The original
    # values remain available in entry_engine_placeholder_* audit columns.
    work["v7_signal_entry_price"] = work["entry_price_with_slippage"]
    work["v7_signal_stop_price"] = work["structure_stop_price"]
    work["v7_signal_target_price"] = work["structure_target_price"]
    work["v7_signal_sl_pct"] = (
        work["structure_risk_per_share"]
        / work["entry_price_with_slippage"].replace(0.0, np.nan)
        * 100.0
    )
    work["v7_signal_target_pct"] = (
        (work["structure_target_price"] - work["entry_price_with_slippage"])
        / work["entry_price_with_slippage"].replace(0.0, np.nan)
        * 100.0
    )
    work["v7_signal_notional_rs"] = work["entry_price_with_slippage"] * work["quantity"]
    work["execution_guard_pass"] = (
        work["entry_gap_atr"].le(MAX_ENTRY_GAP_ATR)
        & work["structure_risk_per_share"].gt(0.0)
        & work["structure_stop_distance_atr"].le(MAX_STOP_DISTANCE_ATR)
        & pd.to_numeric(work["quantity"], errors="coerce").gt(0)
        & work["order_participation"].le(MAX_ORDER_PARTICIPATION)
    )
    return work


def _result_record(
    row: pd.Series,
    policy: ExitPolicy,
    *,
    outcome: str,
    exit_price: float,
    exit_time: pd.Timestamp,
    bars_held: int,
    best_high: float,
    worst_low: float,
) -> dict[str, Any]:
    entry = float(row["entry_price_with_slippage"])
    stop = float(row["structure_stop_price"])
    quantity = int(row["quantity"])
    risk_per_share = entry - stop
    costs = nse.intraday_equity_costs(entry, float(exit_price), quantity, "LONG")
    gross = v12._price_pnl_rs("LONG", entry, float(exit_price), quantity)
    gross_risk = risk_per_share * quantity
    return {
        "_optimizer_row_id": int(row["_optimizer_row_id"]),
        "ticker": str(row["ticker"]),
        "setup": SETUP,
        "side": "LONG",
        "trade_date": str(_timestamp_ist(row["v7_signal_entry_time_ist"]).date()),
        "signal_time_ist": str(row["signal_time_ist"]),
        "membership_slot_ist": str(row["slot_ist"]),
        "selection_rank": int(row["selection_rank"]),
        "context_score": int(row["context_score"]),
        "entry_time_ist": _timestamp_ist(row["v7_signal_entry_time_ist"]),
        "entry_price": entry,
        "initial_stop_price": stop,
        "target_r": float(policy.target_r),
        "target_price": entry + float(policy.target_r) * risk_per_share,
        "quantity": quantity,
        "risk_based_quantity": int(row.get("risk_based_quantity", quantity)),
        "causal_capacity_quantity": int(row.get("causal_capacity_quantity", quantity)),
        "expected_1m_volume": float(row.get("expected_1m_volume", np.nan)),
        "entry_notional_rs": entry * quantity,
        "initial_risk_rs": gross_risk,
        "outcome": outcome,
        "exit_time_ist": exit_time,
        "exit_price": float(exit_price),
        "bars_held": int(bars_held),
        "mfe_r": (best_high - entry) / risk_per_share if risk_per_share > 0 else np.nan,
        "mae_r": (entry - worst_low) / risk_per_share if risk_per_share > 0 else np.nan,
        "gross_risk_rs": gross_risk,
        "gross_pnl_rs": gross,
        "cost_rs": float(costs.total_cost),
        "net_pnl_rs": float(costs.net_pnl),
        "net_r": float(costs.net_pnl / gross_risk) if gross_risk > 0 else np.nan,
        "exit_policy": policy.name,
        "cost_rates_as_of": str(nse.CostConfig().rates_as_of),
    }


def resolve_structural_trade(row: pd.Series, policy: ExitPolicy) -> dict[str, Any] | None:
    entry_time = _timestamp_ist(row["v7_signal_entry_time_ist"])
    if pd.isna(entry_time):
        return None
    day_loader = getattr(v12, "_optimizer_load_1m_day", None)
    bars = day_loader(str(row["ticker"]), str(entry_time.date())) if callable(day_loader) else None
    if bars is None or bars.empty:
        return None
    cutoff = entry_time.normalize() + pd.Timedelta(hours=15, minutes=20)
    sub = bars.loc[(bars.index >= entry_time) & (bars.index <= cutoff)].copy()
    if sub.empty:
        return None

    entry = float(row["entry_price_with_slippage"])
    initial_stop = float(row["structure_stop_price"])
    risk = entry - initial_stop
    if not (entry > initial_stop > 0 and risk > 0):
        return None
    target = entry + float(policy.target_r) * risk
    active_stop = initial_stop
    stop_outcome = "SL"
    best_high = entry
    observed_post_entry_high = -math.inf
    worst_low = entry
    time_checkpoint_done = False
    pending_time_exit = False
    completed_bucket_lows: list[float] = []
    current_bucket = None
    bucket_low = math.inf

    indexed = list(sub.iterrows())
    for i, (bar_time, bar) in enumerate(indexed):
        op = float(bar.get("open", bar.get("close")))
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])

        # A time-stop decision can only be made after its checkpoint bar has
        # completed.  Its first causal fill is therefore this next bar's open,
        # before any intrabar stop/target outcome from this bar is considered.
        if pending_time_exit:
            return _result_record(
                row, policy, outcome="TIME_NO_FOLLOW_THROUGH", exit_price=op,
                exit_time=bar_time, bars_held=i, best_high=best_high,
                worst_low=worst_low,
            )

        bucket = bar_time.ceil("5min")
        if current_bucket is None:
            current_bucket = bucket
        elif bucket != current_bucket:
            # The preceding bucket was committed at the end of its final row so
            # that its low can trail the very first minute of this new bucket.
            current_bucket = bucket
            bucket_low = math.inf
        bucket_low = min(bucket_low, low)

        stop_hit = low <= active_stop
        target_hit = high >= target
        stop_fill = min(active_stop, op)
        if stop_hit and target_hit:
            return _result_record(
                row, policy, outcome=stop_outcome, exit_price=stop_fill,
                exit_time=bar_time, bars_held=i + 1, best_high=best_high,
                worst_low=min(worst_low, low),
            )
        if stop_hit:
            return _result_record(
                row, policy, outcome=stop_outcome, exit_price=stop_fill,
                exit_time=bar_time, bars_held=i + 1, best_high=best_high,
                worst_low=min(worst_low, low),
            )
        if target_hit:
            return _result_record(
                row, policy, outcome="TARGET", exit_price=target,
                exit_time=bar_time, bars_held=i + 1, best_high=max(best_high, high),
                worst_low=min(worst_low, low),
            )

        best_high = max(best_high, high)
        observed_post_entry_high = max(observed_post_entry_high, high)
        worst_low = min(worst_low, low)
        held_minutes = (bar_time - entry_time).total_seconds() / 60.0
        if policy.conditional_time_stop and not time_checkpoint_done and held_minutes >= TIME_STOP_MINUTES:
            # Use only market highs actually observed after entry.  Initialising
            # best_high at a slipped execution price used to make a trade look
            # like it had broken the signal high without any such print.
            new_high = observed_post_entry_high > float(row["signal_high"])
            enough_mfe = observed_post_entry_high >= entry + TIME_STOP_MIN_MFE_R * risk
            time_checkpoint_done = True
            if not (new_high or enough_mfe):
                pending_time_exit = True

        next_bucket = indexed[i + 1][0].ceil("5min") if i + 1 < len(indexed) else None
        if next_bucket != current_bucket:
            if math.isfinite(bucket_low):
                completed_bucket_lows.append(bucket_low)
            bucket_low = math.inf
            if (
                policy.two_bar_low_trail
                and best_high >= entry + TRAIL_TRIGGER_R * risk
                and len(completed_bucket_lows) >= 2
            ):
                trail = min(completed_bucket_lows[-2:])
                if trail > active_stop:
                    active_stop = trail
                    stop_outcome = "TRAIL_TWO_BAR_LOW"

    last_time, last_bar = indexed[-1]
    return _result_record(
        row, policy, outcome="EOD", exit_price=float(last_bar["close"]),
        exit_time=last_time, bars_held=len(indexed), best_high=best_high,
        worst_low=worst_low,
    )


def resolve_policy(selected: pd.DataFrame, policy: ExitPolicy, label: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    total = len(selected)
    for done, (_, row) in enumerate(selected.iterrows(), 1):
        record = resolve_structural_trade(row, policy)
        if record is not None:
            rows.append(record)
        if done % 250 == 0 or done == total:
            print(f"[two-bar {label}] exits {done:,}/{total:,} resolved={len(rows):,}", flush=True)
    return pd.DataFrame(rows)


def session_metrics(trades: pd.DataFrame, sessions: Iterable[str]) -> tuple[dict[str, Any], pd.DataFrame]:
    sessions = [str(session) for session in sessions]
    daily = pd.DataFrame(
        {"trade_date": pd.Series(sessions, dtype="string")}
    )
    if trades.empty:
        grouped = pd.DataFrame(
            {
                "trade_date": pd.Series(dtype="string"),
                "trades": pd.Series(dtype="int64"),
                "gross_pnl_rs": pd.Series(dtype="float64"),
                "cost_rs": pd.Series(dtype="float64"),
                "net_pnl_rs": pd.Series(dtype="float64"),
            }
        )
    else:
        grouped = trades.groupby("trade_date", as_index=False).agg(
            trades=("ticker", "size"),
            gross_pnl_rs=("gross_pnl_rs", "sum"),
            cost_rs=("cost_rs", "sum"),
            net_pnl_rs=("net_pnl_rs", "sum"),
        )
        grouped["trade_date"] = grouped["trade_date"].astype("string")
    daily = daily.merge(grouped, on="trade_date", how="left").fillna(0.0)
    daily["trades"] = daily["trades"].astype(int)
    daily["cum_pnl_rs"] = daily["net_pnl_rs"].cumsum()
    daily["drawdown_rs"] = daily["cum_pnl_rs"] - daily["cum_pnl_rs"].cummax().clip(lower=0.0)
    pnl = pd.to_numeric(trades.get("net_pnl_rs", pd.Series(dtype=float)), errors="coerce")
    positive_total = float(daily.loc[daily["net_pnl_rs"] > 0, "net_pnl_rs"].sum())
    top_day = float(daily["net_pnl_rs"].max()) if len(daily) else 0.0
    metrics = {
        "sessions": len(sessions),
        "trades": int(len(trades)),
        "trades_per_session": float(len(trades) / len(sessions)) if sessions else 0.0,
        "zero_trade_sessions": int(daily["trades"].eq(0).sum()),
        "positive_sessions": int(daily["net_pnl_rs"].gt(0).sum()),
        "gross_pnl_rs": float(trades.get("gross_pnl_rs", pd.Series(dtype=float)).sum()),
        "cost_rs": float(trades.get("cost_rs", pd.Series(dtype=float)).sum()),
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": _profit_factor(pnl),
        "win_rate_pct": float(pnl.gt(0).mean() * 100.0) if len(pnl) else 0.0,
        "mean_net_pnl_trade_rs": float(pnl.mean()) if len(pnl) else 0.0,
        "median_net_pnl_trade_rs": float(pnl.median()) if len(pnl) else 0.0,
        "mean_net_r": float(pd.to_numeric(trades.get("net_r", pd.Series(dtype=float)), errors="coerce").mean()) if len(trades) else 0.0,
        "max_drawdown_rs": float(daily["drawdown_rs"].min()) if len(daily) else 0.0,
        "best_day_rs": top_day,
        "worst_day_rs": float(daily["net_pnl_rs"].min()) if len(daily) else 0.0,
        "top_positive_day_share_pct": float(top_day / positive_total * 100.0) if positive_total > 0 else 0.0,
    }
    return metrics, daily


def event_study(selected: pd.DataFrame) -> pd.DataFrame:
    horizons = (5, 10, 15, 30, 60)
    rows: list[dict[str, Any]] = []
    day_loader = getattr(v12, "_optimizer_load_1m_day", None)
    for _, row in selected.iterrows():
        entry_time = _timestamp_ist(row["v7_signal_entry_time_ist"])
        bars = day_loader(str(row["ticker"]), str(entry_time.date())) if callable(day_loader) else None
        if bars is None or bars.empty:
            continue
        entry = float(row["entry_price_with_slippage"])
        record = {
            "_optimizer_row_id": int(row["_optimizer_row_id"]),
            "ticker": str(row["ticker"]),
            "trade_date": str(entry_time.date()),
            "entry_time_ist": entry_time,
            "entry_price": entry,
        }
        for horizon in horizons:
            end = entry_time + pd.Timedelta(minutes=horizon)
            sub = bars.loc[(bars.index >= entry_time) & (bars.index <= end)]
            record[f"return_{horizon}m_pct"] = (
                (float(sub.iloc[-1]["close"]) / entry - 1.0) * 100.0 if not sub.empty else np.nan
            )
        sub60 = bars.loc[(bars.index >= entry_time) & (bars.index <= entry_time + pd.Timedelta(minutes=60))]
        record["mfe_60m_pct"] = (
            (float(sub60["high"].max()) / entry - 1.0) * 100.0 if not sub60.empty else np.nan
        )
        record["mae_60m_pct"] = (
            (float(sub60["low"].min()) / entry - 1.0) * 100.0 if not sub60.empty else np.nan
        )
        rows.append(record)
    return pd.DataFrame(rows)


def _summarise_groups(trades: pd.DataFrame, column: str) -> pd.DataFrame:
    rows = []
    if trades.empty or column not in trades:
        return pd.DataFrame()
    for value, group in trades.groupby(
        column, sort=True, dropna=False, observed=False
    ):
        rows.append(
            {
                column: value,
                "trades": int(len(group)),
                "net_pnl_rs": float(group["net_pnl_rs"].sum()),
                "profit_factor": _profit_factor(group["net_pnl_rs"]),
                "win_rate_pct": float(group["net_pnl_rs"].gt(0).mean() * 100.0),
                "mean_net_r": float(group["net_r"].mean()),
            }
        )
    return pd.DataFrame(rows)


def write_report(summary: dict[str, Any], output: Path) -> None:
    p = summary["primary_results"]
    guarded = summary.get("variant_results", {}).get("guarded_hard_veto", {})
    missing = summary["data_window"]["missing_prefilter_sessions"]
    verdict = "positive" if p["net_pnl_rs"] > 0 and (p["profit_factor"] or 0) > 1 else "negative"
    if summary.get("canonical_five_minute_source"):
        timing_note = (
            "Five-minute bars were canonically rebuilt from complete V12 "
            "one-minute buckets and use their actual causal completion time."
        )
    elif summary.get("mixed_5m_timestamp_correction"):
        timing_note = (
            "Mixed historical 5-minute timestamp regimes were classified against "
            "the one-minute OHLCV files per ticker-day and late-stamped rows were "
            "moved to their actual causal completion time."
        )
    else:
        timing_note = "Native V12 five-minute timestamps were used without repair."
    text = f"""# V12 hourly two-bar LONG replay

## Result

The requested loose-indicator replay was **{verdict}** over {p['sessions']} reconstructed strict-prefilter sessions: **{p['trades']} trades**, net **Rs {p['net_pnl_rs']:,.2f}**, PF **{p['profit_factor'] or 0:.3f}**, win rate **{p['win_rate_pct']:.2f}%**, mean net R **{p['mean_net_r']:.3f}**, and max daily-equity drawdown **Rs {p['max_drawdown_rs']:,.2f}**.

The rolling window is {summary['data_window']['start']} through {summary['data_window']['end']}. The market calendar and strict shadow hourly-prefilter replay each contain {summary['data_window']['expected_market_sessions']} sessions; missing reconstructed prefilter sessions: {', '.join(missing) if missing else 'none'}.

{timing_note}

The optional guarded comparison (extreme VWAP/DMI/rejection vetoes) produced **{guarded.get('trades', 0)} trades**, net **Rs {guarded.get('net_pnl_rs', 0):,.2f}**, and PF **{guarded.get('profit_factor') or 0:.3f}**. It is not the primary loose-indicator configuration.

## Tested contract

- all K300 hourly rows whose completed prefilter side was LONG (ranks 1..300);
- 09:20 list active 09:25..10:20 inclusive; the 10:20 list starts at 10:25;
- two adjacent **close-to-close** five-minute gains observed during continuous active LONG membership, each 0.50%..1.50%; one trigger per streak;
- loose score >= 2 across session VWAP, DMI/ADX, stochastic, relative volume, candle quality and NIFTY context; no individual indicator is mandatory;
- hard safeguards only for price/value/data quality, extreme range, entry gap, structural-stop width and order participation; VWAP-extension, opposing-DMI and rejection-candle vetoes are comparison-only;
- V12 next-available one-minute-open entry, 5 bps adverse entry fill, quantity capped at the lesser of V12 risk sizing and 2% of causal expected one-minute volume, one ticker entry per day, statutory NSE intraday costs;
- structural stop below trigger-bar low by 0.10 ATR, maximum stop distance 1.25 ATR, 1.50R target, conditional 15-minute no-follow-through decision filled at the next one-minute open, and completed-two-5m-bar-low trail after +1R.

The K300 lists were reconstructed in shadow research from exact completed canonical bars; archived slot markers are audit metadata, not authoritative snapshots. Sector confirmation was not applied because the configured historical sector files are absent. Historical spread was unavailable; traded-value and order-participation gates were used instead.

## Interpretation

This is a historical research replay, not a fresh holdout and not a production promotion. `PRODUCTION_APPROVED = False`; no approved setup file or live process was changed.
"""
    output.write_text(text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="V12 hourly-prefilter two-bar LONG replay")
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument("--prefilter", type=Path, default=DEFAULT_PREFILTER)
    parser.add_argument("--five-minute-dir", type=Path, default=DEFAULT_5M_DIR)
    parser.add_argument(
        "--nifty-five-minute-dir",
        type=Path,
        help="optional separate directory containing NIFTY_stocks_indicators_5min.parquet",
    )
    parser.add_argument("--one-minute-dir", type=Path, default=DEFAULT_1M_DIR)
    parser.add_argument(
        "--correct-mixed-5m-stamps",
        action="store_true",
        help=(
            "classify each ticker-day against one-minute OHLCV and repair "
            "historical five-minute rows stamped five minutes late"
        ),
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    started = time.time()
    canonical_five_minute_source = (
        args.five_minute_dir / "canonical_build_summary.json"
    ).exists()

    memberships, membership_audit = load_long_memberships(
        args.prefilter, args.start_date, args.end_date
    )
    eligibility = expand_membership_schedule(memberships)
    nifty, expected_sessions = load_nifty_context(
        args.nifty_five_minute_dir or args.five_minute_dir,
        args.start_date,
        args.end_date,
    )
    states, feature_audit = load_eligible_bar_states(
        eligibility,
        args.five_minute_dir,
        args.one_minute_dir,
        args.start_date,
        args.end_date,
        nifty,
        args.workers,
        args.correct_mixed_5m_stamps,
    )
    alignment_records = feature_audit.pop("_alignment_records", [])
    states, funnel = add_signal_rules(states)
    raw_trigger_states = states.loc[states["raw_two_bar_trigger"] & states["common_gate_pass"]].copy()
    candidates = candidate_frame(raw_trigger_states)
    if candidates.empty:
        raise RuntimeError("no two-bar candidates")

    raw_entries, entry_rejects, prewarm = install_v12_entry(
        candidates, args.start_date, args.end_date, args.workers
    )
    raw_entries = add_execution_guards(raw_entries)
    loose_score_mask = raw_entries["context_score"].ge(MIN_CONTEXT_SCORE)
    hard_veto_mask = raw_entries["hard_veto_pass"].astype(bool)
    adx25_mask = pd.to_numeric(raw_entries["signal_adx"], errors="coerce").ge(25)
    variant_masks = {
        "loose_context_score2": loose_score_mask,
        "guarded_hard_veto": loose_score_mask & hard_veto_mask,
        "adx25_only": loose_score_mask & adx25_mask,
        "adx25_guarded": loose_score_mask & hard_veto_mask & adx25_mask,
    }
    variant_summaries: dict[str, Any] = {}
    variant_trade_frames: dict[str, pd.DataFrame] = {}
    saved_sessions = sorted(memberships["trade_date"].unique())
    for name, mask in variant_masks.items():
        executable = raw_entries.loc[mask & raw_entries["execution_guard_pass"]].copy()
        selected = v12._select_v7_entry_engine_signals(executable)
        trades = resolve_policy(selected, PRIMARY_POLICY, name)
        metrics, _ = session_metrics(trades, saved_sessions)
        metrics.update(
            {
                "pre_execution_candidates": int(mask.sum()),
                "execution_guard_pass": int(len(executable)),
                "v12_selected_first_ticker_day": int(len(selected)),
            }
        )
        variant_summaries[name] = metrics
        variant_trade_frames[name] = trades

    primary_executable = raw_entries.loc[
        variant_masks["loose_context_score2"] & raw_entries["execution_guard_pass"]
    ].copy()
    primary_selected = v12._select_v7_entry_engine_signals(primary_executable)
    primary_trades = variant_trade_frames["loose_context_score2"]
    fixed_trades = resolve_policy(primary_selected, FIXED_POLICY, "fixed_structure_1p5r")
    fixed_metrics, _ = session_metrics(fixed_trades, saved_sessions)

    target_grid_rows = []
    target_grid_trades = []
    for target_r in (0.75, 1.00, 1.25, 1.50, 2.00, 2.50):
        policy = ExitPolicy(f"fixed_structure_{target_r:.2f}r", target_r=target_r)
        trades = resolve_policy(primary_selected, policy, f"target-{target_r:.2f}r")
        metrics, _ = session_metrics(trades, saved_sessions)
        metrics["target_r"] = target_r
        target_grid_rows.append(metrics)
        target_grid_trades.append(trades)

    primary_metrics, daily = session_metrics(primary_trades, saved_sessions)
    event = event_study(primary_selected)
    boundary_mask = primary_selected["signal_time_ist"].map(_timestamp_ist).map(lambda value: value.minute == 20)
    boundary = primary_selected.loc[boundary_mask].copy()
    boundary["boundary_owned_by_previous_list"] = (
        pd.to_datetime(boundary["signal_time_ist"], utc=True)
        - pd.to_datetime(boundary["slot_ist"], utc=True)
    ).dt.total_seconds().eq(3600.0)
    expected_set = set(expected_sessions)
    saved_set = set(saved_sessions)
    missing_sessions = sorted(expected_set - saved_set)
    limitations: list[str] = []
    if missing_sessions:
        limitations.append(
            "market sessions without an authoritative hourly-prefilter list: "
            + ",".join(missing_sessions)
        )
    limitations.extend(
        [
            "hourly K300 lists reconstructed in shadow research from canonical completed bars; archived slot markers are audit-only",
            "sector confirmation disabled because sector history is unavailable",
            "historical quoted spread unavailable; traded-value and participation proxies used",
            "V12 first-ticker-entry-per-day selector suppresses later same-day re-entry",
            "no portfolio-overlap capital constraint was applied",
            "static current 1,237-symbol universe used; point-in-time universe history is unavailable",
        ]
    )

    monthly = primary_trades.copy()
    if not monthly.empty:
        monthly["month"] = monthly["trade_date"].str[:7]
    hourly = primary_trades.copy()
    if not hourly.empty:
        hourly["signal_hour"] = pd.to_datetime(hourly["signal_time_ist"], utc=True).dt.tz_convert(IST).dt.strftime("%H:00")
        hourly["rank_bucket"] = pd.cut(
            hourly["selection_rank"], bins=[0, 50, 100, 150, 200, 250, 300],
            labels=["1-50", "51-100", "101-150", "151-200", "201-250", "251-300"],
        )

    summary = {
        "production_approved": PRODUCTION_APPROVED,
        "setup": SETUP,
        "primary_variant": "loose_context_score2",
        "canonical_five_minute_source": canonical_five_minute_source,
        "mixed_5m_timestamp_correction": bool(args.correct_mixed_5m_stamps),
        "data_window": {
            "start": args.start_date,
            "end": args.end_date,
            "saved_prefilter_sessions": len(saved_sessions),
            "expected_market_sessions": len(expected_sessions),
            "missing_prefilter_sessions": missing_sessions,
        },
        "membership_audit": membership_audit,
        "feature_audit": feature_audit,
        "candidate_funnel": funnel,
        "entry_engine": {
            "candidates": int(len(candidates)),
            "raw_executable_entries": int(len(raw_entries)),
            "rejects": int(len(entry_rejects)),
            "reject_reasons": entry_rejects.get("reject_reason", pd.Series(dtype=str)).value_counts().to_dict(),
            "prewarm": prewarm,
        },
        "boundary_audit": {
            "primary_selected_boundary_signals": int(len(boundary)),
            "all_owned_by_previous_hourly_list": bool(boundary["boundary_owned_by_previous_list"].all()) if len(boundary) else True,
        },
        "primary_results": primary_metrics,
        "fixed_structure_1p5r_results": fixed_metrics,
        "variant_results": variant_summaries,
        "target_grid_best_historical_net": max(target_grid_rows, key=lambda row: row["net_pnl_rs"]),
        "runtime_seconds": time.time() - started,
        "limitations": limitations,
    }

    pd.DataFrame(funnel).to_csv(args.out / "candidate_funnel.csv", index=False)
    pd.DataFrame(alignment_records).to_csv(args.out / "timestamp_alignment_audit.csv", index=False)
    states.loc[states["raw_two_bar_trigger"]].to_csv(args.out / "raw_trigger_states.csv", index=False)
    candidates.to_csv(args.out / "v12_input_candidates.csv", index=False)
    entry_rejects.to_csv(args.out / "entry_engine_rejects.csv", index=False)
    raw_entries.to_csv(args.out / "entry_engine_raw_entries.csv", index=False)
    primary_selected.to_csv(args.out / "primary_selected_entries.csv", index=False)
    primary_trades.to_csv(args.out / "primary_trades.csv", index=False)
    fixed_trades.to_csv(args.out / "fixed_structure_1p5r_trades.csv", index=False)
    daily.to_csv(args.out / "daily_summary.csv", index=False)
    event.to_csv(args.out / "event_study.csv", index=False)
    boundary.to_csv(args.out / "boundary_audit.csv", index=False)
    pd.DataFrame(target_grid_rows).to_csv(args.out / "target_grid_summary.csv", index=False)
    if target_grid_trades:
        pd.concat(target_grid_trades, ignore_index=True).to_csv(args.out / "target_grid_trades.csv", index=False)
    pd.DataFrame(
        [{"variant": name, **metrics} for name, metrics in variant_summaries.items()]
    ).to_csv(args.out / "variant_summary.csv", index=False)
    if not monthly.empty:
        _summarise_groups(monthly, "month").to_csv(args.out / "monthly_summary.csv", index=False)
    if not hourly.empty:
        _summarise_groups(hourly, "signal_hour").to_csv(args.out / "hourly_summary.csv", index=False)
        _summarise_groups(hourly, "rank_bucket").to_csv(args.out / "rank_bucket_summary.csv", index=False)

    contract = {
        "setup": SETUP,
        "research_only": True,
        "production_approved": False,
        "canonical_five_minute_source": canonical_five_minute_source,
        "primary_variant": "loose_context_score2",
        "trigger": {
            "return_basis": "close_to_previous_completed_5m_close",
            "minimum_each_pct": TRIGGER_MIN_PCT,
            "maximum_each_pct": TRIGGER_MAX_PCT,
            "consecutive_bars": 2,
            "both_bars_require_continuous_active_membership": True,
            "one_signal_per_streak": True,
        },
        "context": {
            "minimum_soft_score": MIN_CONTEXT_SCORE,
            "components": [
                "session_vwap_price",
                "session_vwap_slope",
                "dmi_direction",
                "adx_level_or_rise",
                "stochastic_direction",
                "relative_volume",
                "candle_quality",
                "nifty_direction",
            ],
            "individual_indicator_hard_requirements": False,
            "extreme_indicator_vetoes_primary": False,
        },
        "hourly_membership": {
            "primary_side": "LONG",
            "rank_min": 1,
            "rank_max": 300,
            "active_from": "slot+5m",
            "active_through": "slot+60m inclusive",
            "boundary_owner": "previous active list",
        },
        "primary_exit": {
            "stop": "second signal bar low - 0.10 ATR",
            "maximum_stop_distance_atr": MAX_STOP_DISTANCE_ATR,
            "target_r": PRIMARY_TARGET_R,
            "conditional_time_stop_minutes": TIME_STOP_MINUTES,
            "time_stop_requires_mfe_r_or_new_high": TIME_STOP_MIN_MFE_R,
            "time_stop_decision_source": "completed one-minute checkpoint bar",
            "time_stop_fill": "next one-minute bar open before its intrabar stop/target logic",
            "new_high_source": "observed post-entry market highs only",
            "trail": "completed two-5m-bar low after +1R",
        },
        "v12_execution": {
            "entry": "next available 1m open after completed 5m signal",
            "canonical_five_minute_source": canonical_five_minute_source,
            "mixed_5m_timestamp_correction": bool(args.correct_mixed_5m_stamps),
            "entry_slippage_pct": v12.V7_PAPER_SLIPPAGE_PCT,
            "cost_model": "NSE statutory intraday equity",
            "risk_equity_rs": v12.RISK_EQUITY_RS,
            "risk_pct_per_trade": v12.RISK_PCT_PER_TRADE,
            "quantity": "min(risk_based_quantity, causal_capacity_quantity)",
            "causal_expected_1m_volume": "completed signal 5m volume / 5",
            "maximum_expected_1m_participation": MAX_ORDER_PARTICIPATION,
            "reject_quantity_below": 1,
            "one_ticker_entry_per_day": True,
        },
    }
    (args.out / "filter_contract.json").write_text(json.dumps(_json_value(contract), indent=2), encoding="utf-8")
    (args.out / "summary.json").write_text(json.dumps(_json_value(summary), indent=2), encoding="utf-8")
    write_report(summary, args.out / "RESEARCH_REPORT.md")

    artifacts = []
    for path in sorted(args.out.iterdir()):
        if path.is_file() and path.name != "integrity_manifest.json":
            artifacts.append({"file": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    canonical_summary_path = args.five_minute_dir / "canonical_build_summary.json"
    strict_prefilter_contract_path = args.prefilter.parent / "strict_contract.json"
    nifty_source_path = (
        args.nifty_five_minute_dir or args.five_minute_dir
    ) / "NIFTY_stocks_indicators_5min.parquet"
    manifest_inputs = {
        "prefilter": str(args.prefilter.resolve()),
        "prefilter_sha256": _sha256(args.prefilter),
        "five_minute_dir": str(args.five_minute_dir.resolve()),
        "nifty_five_minute_dir": str(
            (args.nifty_five_minute_dir or args.five_minute_dir).resolve()
        ),
        "one_minute_dir": str(args.one_minute_dir.resolve()),
        "canonical_five_minute_source": canonical_five_minute_source,
        "mixed_5m_timestamp_correction": bool(args.correct_mixed_5m_stamps),
        "v12_source": str(Path(v12.__file__).resolve()),
        "v12_source_sha256": _sha256(Path(v12.__file__)),
        "adapter_source": str(Path(__file__).resolve()),
        "adapter_source_sha256": _sha256(Path(__file__)),
    }
    if canonical_summary_path.is_file():
        manifest_inputs.update(
            {
                "canonical_build_summary": str(canonical_summary_path.resolve()),
                "canonical_build_summary_sha256": _sha256(canonical_summary_path),
            }
        )
    if strict_prefilter_contract_path.is_file():
        manifest_inputs.update(
            {
                "strict_prefilter_contract": str(strict_prefilter_contract_path.resolve()),
                "strict_prefilter_contract_sha256": _sha256(
                    strict_prefilter_contract_path
                ),
            }
        )
    if nifty_source_path.is_file():
        manifest_inputs.update(
            {
                "nifty_five_minute_source": str(nifty_source_path.resolve()),
                "nifty_five_minute_source_sha256": _sha256(nifty_source_path),
            }
        )
    manifest = {
        "production_approved": False,
        "inputs": manifest_inputs,
        "artifacts": artifacts,
    }
    (args.out / "integrity_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(_json_value(summary), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
