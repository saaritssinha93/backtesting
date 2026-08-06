"""Research-only LONG setup discovery on six months of hourly prefilter data.

The study is deliberately causal:

* only inclusive prefilter ranks 200..300 with ``primary_side == LONG``;
* a pool becomes active five minutes after its hourly snapshot, matching the V12
  replay adapter; a completed 5-minute bar may trigger only while that pool is active;
* entry is the following end-stamped 5-minute bar's open, executed at the signal
  bar's end boundary;
* at most one entry per ticker/trading day, always the earliest rule match;
* the +5% label uses only highs after entry through the same session;
* one-minute data is used to timestamp the final daily maximum when available,
  with the independently computed 5-minute maximum retained as a fallback.

Nothing in this module modifies or enables a production setup.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pprint
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


IST = "Asia/Kolkata"
START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
TARGET_RETURN_PCT = 5.0
SESSION_FIRST_5M_END = "09:20"
SESSION_LAST_END = "15:30"
LAST_SIGNAL_END = "15:25"
CACHE_SCHEMA_VERSION = "prefilter_long_gt5_causal_v2"
MIN_TRAIN_LIFT = 1.20
MIN_VALIDATION_LIFT = 1.20
MIN_HOLDOUT_LIFT = 1.10

DEFAULT_PREFILTER = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_six_month_replay_20260205_20260804_k300"
    r"\hourly_candidates_20260205_20260804_rank200_300.csv"
)
DEFAULT_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_OUT = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_long_5m_gt5pct_20260205_20260804"
)

PREFILTER_COLUMNS = (
    "selection_rank",
    "selection_bucket",
    "primary_side",
    "primary_family",
    "selection_reason",
    "overall_score",
    "long_score",
    "short_score",
    "activity_score",
    "staleness_seconds",
)

FIVE_MINUTE_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "RSI",
    "ATR",
    "EMA_20",
    "EMA_50",
    "EMA_200",
    "20_SMA",
    "VWAP",
    "CCI",
    "MFI",
    "OBV",
    "MACD",
    "MACD_Signal",
    "MACD_Hist",
    "Upper_Band",
    "Lower_Band",
    "ADX",
    "Recent_High",
    "Recent_Low",
    "Prev_Day_Close",
    "Stoch_%K",
    "Stoch_%D",
    "gap_filled",
    "opening_snapshot",
)

MODEL_FEATURES = (
    "selection_rank",
    "overall_score",
    "long_score",
    "activity_score",
    "staleness_seconds",
    "signal_minute",
    "RSI",
    "ADX",
    "CCI",
    "MFI",
    "Stoch_%K",
    "Stoch_%D",
    "atr_pct",
    "vwap_dist_atr",
    "ema20_dist_atr",
    "ema50_dist_atr",
    "ema200_dist_atr",
    "macd_hist_atr",
    "bb_width_atr",
    "ret_5m_pct",
    "ret_15m_pct",
    "ret_30m_pct",
    "ret_60m_pct",
    "session_return_so_far_pct",
    "gap_pct",
    "range_pct",
    "body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "close_position_in_bar",
    "volume_ratio20",
    "traded_value_rs",
    "distance_from_running_session_high_atr",
    "distance_from_session_high_pct",
    "rebound_from_session_low_pct",
    "ema_long_stack",
)

CATEGORICAL_FEATURES = (
    "primary_family",
    "selection_bucket",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if value is pd.NA:
        return None
    return value


def write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(jsonable(value), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def normalise_ist(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(IST)
    return parsed.dt.tz_convert(IST)


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    den = pd.to_numeric(denominator, errors="coerce").replace(0.0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / den


def bars_reaching_target(frame: pd.DataFrame, target_price: float) -> pd.DataFrame:
    """Compare in float64 so float32 Series do not round the target downward."""
    if frame.empty:
        return frame.copy()
    highs = pd.to_numeric(frame["high"], errors="coerce").astype("float64")
    return frame.loc[highs.ge(float(target_price))]


def load_memberships(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = pd.read_csv(path, low_memory=False)
    required = {"slot_ist", "date", "ticker", *PREFILTER_COLUMNS}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"prefilter input missing columns: {missing}")
    frame = frame.copy()
    frame["membership_slot_ist"] = normalise_ist(frame["slot_ist"])
    frame["prefilter_data_time_ist"] = normalise_ist(frame["date"])
    frame["ticker"] = frame["ticker"].astype(str).str.upper().str.strip()
    frame["selection_rank"] = pd.to_numeric(frame["selection_rank"], errors="coerce")
    frame = frame.loc[
        frame["primary_side"].astype(str).str.upper().eq("LONG")
        & frame["selection_rank"].between(200, 300, inclusive="both")
        & frame["membership_slot_ist"].notna()
        & frame["ticker"].ne("")
    ].copy()
    if frame.duplicated(["membership_slot_ist", "ticker"]).any():
        raise RuntimeError("duplicate LONG ticker within a prefilter slot")
    frame["trade_date"] = frame["membership_slot_ist"].dt.strftime("%Y-%m-%d")
    frame["membership_hour"] = frame["membership_slot_ist"].dt.strftime("%H:%M")
    frame = frame.loc[frame["trade_date"].between(START_DATE, END_DATE)].copy()
    staleness = pd.to_numeric(frame["staleness_seconds"], errors="coerce")
    stale = staleness.ne(0.0) | staleness.isna()
    timestamp_mismatch = frame["prefilter_data_time_ist"].ne(frame["membership_slot_ist"])
    frame["membership_rejection_reason"] = ""
    frame.loc[stale, "membership_rejection_reason"] = "stale_prefilter_snapshot"
    frame.loc[timestamp_mismatch, "membership_rejection_reason"] = np.where(
        frame.loc[timestamp_mismatch, "membership_rejection_reason"].eq(""),
        "prefilter_timestamp_mismatch",
        frame.loc[timestamp_mismatch, "membership_rejection_reason"]
        + ";prefilter_timestamp_mismatch",
    )
    rejected = frame.loc[frame["membership_rejection_reason"].ne("")].copy()
    accepted = frame.loc[frame["membership_rejection_reason"].eq("")].copy()
    accepted = accepted.sort_values(
        ["ticker", "membership_slot_ist", "selection_rank"], kind="mergesort"
    ).reset_index(drop=True)
    rejected = rejected.sort_values(
        ["ticker", "membership_slot_ist", "selection_rank"], kind="mergesort"
    ).reset_index(drop=True)
    return accepted, rejected


def read_parquet_window(path: Path, columns: Sequence[str]) -> pd.DataFrame | None:
    if not path.exists():
        return None
    start = pd.Timestamp(START_DATE, tz=IST)
    end = pd.Timestamp(END_DATE, tz=IST) + pd.Timedelta(days=1)
    available_columns = list(columns)
    try:
        frame = pd.read_parquet(
            path,
            columns=available_columns,
            filters=[("date", ">=", start), ("date", "<", end)],
        )
    except Exception:
        try:
            frame = pd.read_parquet(path, columns=available_columns)
        except Exception:
            frame = pd.read_parquet(path)
    if frame is None or frame.empty or "date" not in frame:
        return None
    frame = frame.copy()
    timestamp = pd.to_datetime(frame["date"], errors="coerce")
    if getattr(timestamp.dt, "tz", None) is None:
        timestamp = timestamp.dt.tz_localize(IST)
    else:
        timestamp = timestamp.dt.tz_convert(IST)
    frame["date"] = timestamp
    frame = frame.loc[(frame["date"] >= start) & (frame["date"] < end)].copy()
    return frame if not frame.empty else None


def filter_end_stamped_session(
    frame: pd.DataFrame,
    *,
    first_label: str,
    last_label: str = SESSION_LAST_END,
) -> pd.DataFrame:
    """Keep only canonical same-session end-stamped bars."""
    if frame.empty:
        return frame.copy()
    clock = frame["date"].dt.strftime("%H:%M")
    return frame.loc[clock.between(first_label, last_label, inclusive="both")].copy()


def add_causal_features(bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.copy()
    for column in work.columns:
        if column not in {"date", "opening_snapshot"}:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    work = (
        work.dropna(subset=["date"])
        .sort_values("date", kind="mergesort")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    work["trade_date"] = work["date"].dt.strftime("%Y-%m-%d")
    grouped = work.groupby("trade_date", sort=False, group_keys=False)
    close = work["close"]
    open_px = work["open"]
    high = work["high"]
    low = work["low"]
    atr = work.get("ATR", pd.Series(np.nan, index=work.index))

    work["signal_minute"] = work["date"].dt.hour * 60 + work["date"].dt.minute
    work["ret_5m_pct"] = (safe_div(close, open_px) - 1.0) * 100.0
    for bars_back, label in ((3, "ret_15m_pct"), (6, "ret_30m_pct"), (12, "ret_60m_pct")):
        prior = grouped["close"].shift(bars_back)
        work[label] = (safe_div(close, prior) - 1.0) * 100.0
    work["range_pct"] = safe_div(high - low, close) * 100.0
    work["body_pct"] = safe_div(close - open_px, open_px) * 100.0
    body_top = pd.concat([open_px, close], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close], axis=1).min(axis=1)
    work["upper_wick_pct"] = safe_div(high - body_top, close) * 100.0
    work["lower_wick_pct"] = safe_div(body_bottom - low, close) * 100.0
    work["close_position_in_bar"] = safe_div(close - low, high - low)
    work["atr_pct"] = safe_div(atr, close) * 100.0
    for source, label in (
        ("VWAP", "vwap_dist_atr"),
        ("EMA_20", "ema20_dist_atr"),
        ("EMA_50", "ema50_dist_atr"),
        ("EMA_200", "ema200_dist_atr"),
    ):
        values = work.get(source, pd.Series(np.nan, index=work.index))
        work[label] = safe_div(close - values, atr)
    work["macd_hist_atr"] = safe_div(
        work.get("MACD_Hist", pd.Series(np.nan, index=work.index)), atr
    )
    work["bb_width_atr"] = safe_div(
        work.get("Upper_Band", pd.Series(np.nan, index=work.index))
        - work.get("Lower_Band", pd.Series(np.nan, index=work.index)),
        atr,
    )
    previous_volume_median = grouped["volume"].transform(
        lambda values: values.shift(1).rolling(20, min_periods=8).median()
    )
    work["volume_ratio20"] = safe_div(work["volume"], previous_volume_median)
    work["traded_value_rs"] = close * work["volume"]

    # Stored VWAP fields in older indicator files are not consistently populated.
    # Recompute a causal session VWAP from completed bars only.
    typical_price = (high + low + close) / 3.0
    cumulative_value = (typical_price * work["volume"]).groupby(work["trade_date"]).cumsum()
    cumulative_volume = work["volume"].groupby(work["trade_date"]).cumsum()
    work["session_vwap_causal"] = safe_div(cumulative_value, cumulative_volume)
    work["vwap_dist_atr"] = safe_div(close - work["session_vwap_causal"], atr)

    day_open = grouped["open"].transform("first")
    work["session_return_so_far_pct"] = (safe_div(close, day_open) - 1.0) * 100.0
    previous_close = work.get("Prev_Day_Close", pd.Series(np.nan, index=work.index))
    work["gap_pct"] = (safe_div(day_open, previous_close) - 1.0) * 100.0
    running_high = grouped["high"].cummax()
    running_low = grouped["low"].cummin()
    prior_session_high = running_high.groupby(work["trade_date"]).shift(1)
    prior_session_low = running_low.groupby(work["trade_date"]).shift(1)
    work["distance_from_running_session_high_atr"] = safe_div(
        close - prior_session_high, atr
    )
    work["distance_from_session_high_pct"] = (
        safe_div(close, prior_session_high) - 1.0
    ) * 100.0
    work["rebound_from_session_low_pct"] = (
        safe_div(close, prior_session_low) - 1.0
    ) * 100.0
    work["ema_long_stack"] = (
        (close >= work.get("EMA_20", np.nan))
        & (work.get("EMA_20", np.nan) >= work.get("EMA_50", np.nan))
        & (work.get("EMA_50", np.nan) >= work.get("EMA_200", np.nan))
    ).astype(float)
    return work


def add_forward_five_minute_outcomes(bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.copy()
    open_px = pd.to_numeric(work["open"], errors="coerce")
    high_px = pd.to_numeric(work["high"], errors="coerce")
    low_px = pd.to_numeric(work["low"], errors="coerce")
    close_px = pd.to_numeric(work["close"], errors="coerce")
    valid = (
        open_px.gt(0.0)
        & high_px.gt(0.0)
        & low_px.gt(0.0)
        & close_px.gt(0.0)
        & high_px.ge(pd.concat([open_px, low_px, close_px], axis=1).max(axis=1))
        & low_px.le(pd.concat([open_px, high_px, close_px], axis=1).min(axis=1))
        & pd.to_numeric(work.get("gap_filled", 0.0), errors="coerce").fillna(0.0).lt(0.5)
    )
    work["forward_max_high_5m"] = np.nan
    work["forward_max_time_5m"] = pd.Series(
        pd.NaT, index=work.index, dtype=f"datetime64[ns, {IST}]"
    )
    work["eod_close_5m"] = np.nan
    work["five_minute_day_complete"] = False
    work["forward_real_count_5m"] = 0
    work["forward_exact_grid_5m"] = False
    for _, positions in work.groupby("trade_date", sort=False).groups.items():
        pos = np.asarray(list(positions), dtype=int)
        highs = pd.to_numeric(work.loc[pos, "high"], errors="coerce").to_numpy(float)
        valids = valid.loc[pos].to_numpy(bool)
        timestamps = work.loc[pos, "date"].tolist()
        running_high = float("nan")
        running_time = pd.NaT
        output_high = np.full(len(pos), np.nan, dtype=float)
        output_time: list[pd.Timestamp | pd.NaT] = [pd.NaT] * len(pos)
        output_count = np.zeros(len(pos), dtype=int)
        output_exact_grid = np.zeros(len(pos), dtype=bool)
        running_count = 0
        for local in range(len(pos) - 1, -1, -1):
            value = highs[local]
            if valids[local] and math.isfinite(value):
                running_count += 1
                if not math.isfinite(running_high) or value >= running_high - 1e-12:
                    running_high = float(value)
                    running_time = timestamps[local]
            output_high[local] = running_high
            output_time[local] = running_time
            output_count[local] = running_count
        if len(pos):
            eod_expected = pd.Timestamp(
                f"{work.loc[pos[0], 'trade_date']} {SESSION_LAST_END}", tz=IST
            )
            for local in range(len(pos) - 1, -1, -1):
                if not valids[local]:
                    output_exact_grid[local] = False
                elif local == len(pos) - 1:
                    output_exact_grid[local] = pd.Timestamp(timestamps[local]) == eod_expected
                else:
                    output_exact_grid[local] = bool(
                        output_exact_grid[local + 1]
                        and pd.Timestamp(timestamps[local + 1])
                        - pd.Timestamp(timestamps[local])
                        == pd.Timedelta(minutes=5)
                    )
        actual_close = pd.to_numeric(
            work.loc[pos[valids], "close"], errors="coerce"
        ).dropna()
        eod = float(actual_close.iloc[-1]) if len(actual_close) else float("nan")
        valid_times = work.loc[pos[valids], "date"]
        day_complete = bool(
            len(valid_times)
            and valid_times.max()
            >= pd.Timestamp(f"{work.loc[pos[0], 'trade_date']} {SESSION_LAST_END}", tz=IST)
        )
        work.loc[pos, "forward_max_high_5m"] = output_high
        work.loc[pos, "forward_max_time_5m"] = output_time
        work.loc[pos, "eod_close_5m"] = eod
        work.loc[pos, "five_minute_day_complete"] = day_complete
        work.loc[pos, "forward_real_count_5m"] = output_count
        work.loc[pos, "forward_exact_grid_5m"] = output_exact_grid
    return work


def signal_schedule(slot: pd.Timestamp) -> list[pd.Timestamp]:
    session_last_signal = pd.Timestamp(
        f"{slot.strftime('%Y-%m-%d')} {LAST_SIGNAL_END}", tz=IST
    )
    start = slot + pd.Timedelta(minutes=5)
    end = min(slot + pd.Timedelta(minutes=60), session_last_signal)
    if start > end:
        return []
    return list(pd.date_range(start, end, freq="5min"))


def add_primary_one_minute_outcomes(
    opportunities: pd.DataFrame,
    ticker: str,
    one_minute_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach complete-path 1m maxima, with a complete 5m-only fallback."""
    work = opportunities.copy()
    timestamp_dtype = f"datetime64[ns, {IST}]"
    work["daily_max_price"] = np.nan
    work["daily_max_bar_end_ist"] = pd.Series(pd.NaT, index=work.index, dtype=timestamp_dtype)
    work["daily_max_interval_start_ist"] = pd.Series(
        pd.NaT, index=work.index, dtype=timestamp_dtype
    )
    work["daily_max_interval_end_ist"] = pd.Series(
        pd.NaT, index=work.index, dtype=timestamp_dtype
    )
    work["daily_max_time_ist"] = pd.Series(pd.NaT, index=work.index, dtype=timestamp_dtype)
    work["daily_max_time_source"] = ""
    work["max_time_resolution"] = ""
    work["max_window_complete"] = False
    work["eod_close"] = np.nan
    work["one_minute_rows_after_entry"] = 0
    work["five_minute_rows_after_entry"] = pd.to_numeric(
        work.get("forward_real_count_5m", 0), errors="coerce"
    ).fillna(0).astype(int)
    work["cross_tf_presearch_max_abs_diff"] = np.nan
    work["cross_tf_presearch_max_diff_bps"] = np.nan
    work["cross_tf_presearch_parity_within_tolerance"] = pd.Series(
        pd.NA, index=work.index, dtype="boolean"
    )
    work["cross_tf_target_agreement"] = pd.Series(
        pd.NA, index=work.index, dtype="boolean"
    )

    one = read_one_minute(
        one_minute_dir / f"{ticker}_stocks_indicators_1min.parquet"
    )
    one_dates = set(one["trade_date"].astype(str)) if one is not None else set()
    one_complete_rows = 0
    fallback_rows = 0
    incomplete_rows = 0

    for day, positions in work.groupby("trade_date", sort=False).groups.items():
        pos = np.asarray(list(positions), dtype=int)
        eod = pd.Timestamp(f"{day} {SESSION_LAST_END}", tz=IST)
        one_day = (
            one.loc[one["trade_date"].eq(str(day))].copy()
            if one is not None and str(day) in one_dates
            else pd.DataFrame()
        )
        if not one_day.empty:
            one_day = filter_end_stamped_session(one_day, first_label="09:16")
            integrity = (
                one_day["high"].ge(one_day[["open", "low", "close"]].max(axis=1))
                & one_day["low"].le(one_day[["open", "high", "close"]].min(axis=1))
            )
            one_day = one_day.loc[integrity].sort_values("date", kind="mergesort")
            one_day = one_day.reset_index(drop=True)
        if not one_day.empty:
            times = one_day["date"].array
            time_ns = one_day["date"].astype("int64").to_numpy()
            highs = one_day["high"].to_numpy(float)
            suffix_high = np.full(len(one_day), np.nan, dtype=float)
            suffix_time_ns = np.full(len(one_day), np.iinfo(np.int64).min, dtype=np.int64)
            suffix_exact_grid = np.zeros(len(one_day), dtype=bool)
            running_high = float("nan")
            running_time_ns = np.iinfo(np.int64).min
            for local in range(len(one_day) - 1, -1, -1):
                value = highs[local]
                if math.isfinite(value) and (
                    not math.isfinite(running_high) or value >= running_high - 1e-12
                ):
                    running_high = float(value)
                    running_time_ns = int(time_ns[local])
                suffix_high[local] = running_high
                suffix_time_ns[local] = running_time_ns
            for local in range(len(one_day) - 1, -1, -1):
                if local == len(one_day) - 1:
                    suffix_exact_grid[local] = pd.Timestamp(times[local]) == eod
                else:
                    suffix_exact_grid[local] = bool(
                        suffix_exact_grid[local + 1]
                        and int(time_ns[local + 1]) - int(time_ns[local])
                        == int(pd.Timedelta(minutes=1).value)
                    )
        else:
            time_ns = np.asarray([], dtype=np.int64)
            suffix_high = np.asarray([], dtype=float)
            suffix_time_ns = np.asarray([], dtype=np.int64)
            suffix_exact_grid = np.asarray([], dtype=bool)

        for row_index in pos:
            execution = pd.Timestamp(work.at[row_index, "entry_execution_time_ist"])
            execution_ns = int(execution.value)
            first_eligible = execution + pd.Timedelta(minutes=1)
            expected_1m = max(0, int((eod - execution) / pd.Timedelta(minutes=1)))
            start_index = int(np.searchsorted(time_ns, execution_ns, side="right"))
            available_1m = len(time_ns) - start_index
            one_path_complete = bool(
                expected_1m > 0
                and available_1m == expected_1m
                and start_index < len(time_ns)
                and pd.Timestamp(times[start_index]) == first_eligible
                and pd.Timestamp(times[-1]) == eod
                and bool(suffix_exact_grid[start_index])
            )
            work.at[row_index, "one_minute_rows_after_entry"] = available_1m
            expected_5m = max(0, int((eod - execution) / pd.Timedelta(minutes=5)))
            available_5m = int(work.at[row_index, "five_minute_rows_after_entry"])
            five_path_complete = bool(
                expected_5m > 0
                and available_5m == expected_5m
                and bool(work.at[row_index, "five_minute_day_complete"])
                and bool(work.at[row_index, "forward_exact_grid_5m"])
            )

            if one_path_complete:
                max_price = float(suffix_high[start_index])
                max_bar_end = pd.Timestamp(int(suffix_time_ns[start_index]), tz="UTC").tz_convert(IST)
                interval_start = max_bar_end - pd.Timedelta(minutes=1)
                eod_close = float(one_day.iloc[-1]["close"])
                source = "1min"
                resolution = "1min"
                one_complete_rows += 1
            elif five_path_complete:
                max_price = float(work.at[row_index, "forward_max_high_5m"])
                max_bar_end = pd.Timestamp(work.at[row_index, "forward_max_time_5m"])
                interval_start = max_bar_end - pd.Timedelta(minutes=5)
                eod_close = float(work.at[row_index, "eod_close_5m"])
                source = "5min_fallback"
                resolution = "5min"
                fallback_rows += 1
            else:
                incomplete_rows += 1
                continue

            work.at[row_index, "daily_max_price"] = max_price
            work.at[row_index, "daily_max_bar_end_ist"] = max_bar_end
            work.at[row_index, "daily_max_interval_start_ist"] = interval_start
            work.at[row_index, "daily_max_interval_end_ist"] = max_bar_end
            work.at[row_index, "daily_max_time_ist"] = interval_start
            work.at[row_index, "daily_max_time_source"] = source
            work.at[row_index, "max_time_resolution"] = resolution
            work.at[row_index, "max_window_complete"] = True
            work.at[row_index, "eod_close"] = eod_close
            if one_path_complete and five_path_complete:
                five_max = float(work.at[row_index, "forward_max_high_5m"])
                tolerance = max(0.02, abs(max_price) * 1e-6)
                difference = abs(max_price - five_max)
                work.at[row_index, "cross_tf_presearch_max_abs_diff"] = difference
                work.at[row_index, "cross_tf_presearch_max_diff_bps"] = (
                    difference / max(abs(max_price), 1e-12) * 10000.0
                )
                work.at[
                    row_index, "cross_tf_presearch_parity_within_tolerance"
                ] = difference <= tolerance
                target_price = float(work.at[row_index, "entry_price"]) * (
                    1.0 + TARGET_RETURN_PCT / 100.0
                )
                work.at[row_index, "cross_tf_target_agreement"] = bool(
                    (max_price >= target_price - 1e-9)
                    == (five_max >= target_price - 1e-9)
                )

    work["max_forward_return_pct"] = (
        safe_div(work["daily_max_price"], work["entry_price"]) - 1.0
    ) * 100.0
    work["hit_5pct"] = (
        work["max_window_complete"]
        & work["max_forward_return_pct"].ge(TARGET_RETURN_PCT)
    )
    work["eod_return_pct"] = (
        safe_div(work["eod_close"], work["entry_price"]) - 1.0
    ) * 100.0
    work["future_extreme_review_flag"] = work["max_forward_return_pct"].gt(40.0)
    return work, {
        "one_minute_complete_opportunities": one_complete_rows,
        "five_minute_fallback_opportunities": fallback_rows,
        "incomplete_opportunities": incomplete_rows,
        "one_minute_file_available": one is not None and not one.empty,
    }


def build_ticker_opportunities(
    ticker: str,
    memberships: pd.DataFrame,
    five_minute_dir: Path,
    one_minute_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    input_membership_rows = len(memberships)
    path = five_minute_dir / f"{ticker}_stocks_indicators_5min.parquet"
    try:
        bars = read_parquet_window(path, FIVE_MINUTE_COLUMNS)
    except Exception as exc:
        return pd.DataFrame(), {
            "ticker": ticker,
            "status": "read_error",
            "error": f"{type(exc).__name__}: {exc}",
            "membership_rows": input_membership_rows,
            "opportunity_rows": 0,
        }
    if bars is None or bars.empty:
        return pd.DataFrame(), {
            "ticker": ticker,
            "status": "missing_or_empty_5m",
            "error": "",
            "membership_rows": input_membership_rows,
            "opportunity_rows": 0,
        }
    for column in FIVE_MINUTE_COLUMNS:
        if column not in bars:
            bars[column] = np.nan
    bars = filter_end_stamped_session(bars, first_label=SESSION_FIRST_5M_END)
    relevant_dates = set(memberships["trade_date"].astype(str))
    bars = bars.loc[bars["date"].dt.strftime("%Y-%m-%d").isin(relevant_dates)].copy()
    if bars.empty:
        return pd.DataFrame(), {
            "ticker": ticker,
            "status": "missing_relevant_5m_days",
            "error": "",
            "membership_rows": len(memberships),
            "opportunity_rows": 0,
        }
    bar_gap = pd.to_numeric(bars["gap_filled"], errors="coerce").fillna(0.0)
    bar_real = (
        pd.to_numeric(bars["open"], errors="coerce").gt(0.0)
        & pd.to_numeric(bars["close"], errors="coerce").gt(0.0)
        & bar_gap.lt(0.5)
    )
    valid_membership_times = set(bars.loc[bar_real, "date"])
    source_membership_valid = memberships["membership_slot_ist"].isin(valid_membership_times)
    source_rejected_memberships = memberships.loc[~source_membership_valid].copy()
    memberships = memberships.loc[source_membership_valid].copy()
    if memberships.empty:
        return pd.DataFrame(), {
            "ticker": ticker,
            "status": "no_real_membership_source_bar",
            "error": "",
            "membership_rows": input_membership_rows,
            "source_memberships_rejected": int(len(source_rejected_memberships)),
            "source_rejected_membership_slots": "|".join(
                source_rejected_memberships["membership_slot_ist"].astype(str)
            ),
            "opportunity_rows": 0,
        }
    bars = add_forward_five_minute_outcomes(add_causal_features(bars))
    features = [
        "date",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "gap_filled",
        *[feature for feature in MODEL_FEATURES if feature in bars.columns],
        "forward_max_high_5m",
        "forward_max_time_5m",
        "eod_close_5m",
        "five_minute_day_complete",
        "forward_real_count_5m",
        "forward_exact_grid_5m",
    ]
    features = list(dict.fromkeys(features))
    signal_frame = bars[features].copy().rename(
        columns={
            "date": "signal_time_ist",
            "open": "signal_open",
            "high": "signal_high",
            "low": "signal_low",
            "close": "signal_close",
            "volume": "signal_volume",
            "gap_filled": "signal_gap_filled",
        }
    )
    entry_frame = bars[
        [
            "date",
            "open",
            "gap_filled",
            "forward_max_high_5m",
            "forward_max_time_5m",
            "eod_close_5m",
            "five_minute_day_complete",
            "forward_real_count_5m",
            "forward_exact_grid_5m",
        ]
    ].copy().rename(
        columns={
            "date": "entry_price_source_bar_end_ist",
            "open": "entry_price",
            "gap_filled": "entry_gap_filled",
        }
    )
    expanded_rows: list[dict[str, Any]] = []
    for _, membership in memberships.iterrows():
        base = {
            "ticker": ticker,
            "membership_slot_ist": membership["membership_slot_ist"],
            "membership_hour": membership["membership_hour"],
            **{column: membership.get(column) for column in PREFILTER_COLUMNS},
        }
        for signal_time in signal_schedule(membership["membership_slot_ist"]):
            expanded_rows.append({**base, "signal_time_ist": signal_time})
    expanded = pd.DataFrame(expanded_rows)
    merged = expanded.merge(signal_frame, on="signal_time_ist", how="left")
    merged["entry_price_source_bar_end_ist"] = merged["signal_time_ist"] + pd.Timedelta(minutes=5)
    merged = merged.merge(
        entry_frame,
        on="entry_price_source_bar_end_ist",
        how="left",
        suffixes=("", "_entry"),
    )
    # The outcome columns must come from the entry bar, not the signal bar.
    for column in (
        "forward_max_high_5m",
        "forward_max_time_5m",
        "eod_close_5m",
        "five_minute_day_complete",
        "forward_real_count_5m",
        "forward_exact_grid_5m",
    ):
        entry_column = f"{column}_entry"
        if entry_column in merged:
            merged[column] = merged[entry_column]
            merged = merged.drop(columns=[entry_column])
    merged["trade_date"] = merged["signal_time_ist"].dt.strftime("%Y-%m-%d")
    merged["entry_execution_time_ist"] = merged["signal_time_ist"]
    merged["first_eligible_1m_bar_end_ist"] = (
        merged["entry_execution_time_ist"] + pd.Timedelta(minutes=1)
    )
    merged["max_forward_return_pct_5m"] = (
        safe_div(merged["forward_max_high_5m"], merged["entry_price"]) - 1.0
    ) * 100.0
    merged["eod_return_pct_5m"] = (
        safe_div(merged["eod_close_5m"], merged["entry_price"]) - 1.0
    ) * 100.0
    merged["hit_5pct_5m"] = merged["max_forward_return_pct_5m"].ge(TARGET_RETURN_PCT)
    signal_ohlc_valid = (
        merged[["signal_open", "signal_high", "signal_low", "signal_close"]]
        .notna()
        .all(axis=1)
        & merged[["signal_open", "signal_high", "signal_low", "signal_close"]]
        .gt(0.0)
        .all(axis=1)
    )
    merged["pre_entry_data_invalid"] = (
        ~signal_ohlc_valid
        | pd.to_numeric(merged["entry_price"], errors="coerce").le(0.0)
        | pd.to_numeric(merged["entry_price"], errors="coerce").isna()
        | pd.to_numeric(merged["signal_gap_filled"], errors="coerce").fillna(0.0).ge(0.5)
        | pd.to_numeric(merged["entry_gap_filled"], errors="coerce").fillna(0.0).ge(0.5)
    )
    merged["large_gap_review_flag"] = pd.to_numeric(
        merged["gap_pct"], errors="coerce"
    ).abs().gt(30.0)
    valid = merged.loc[
        merged["entry_price"].notna()
        & merged["forward_max_high_5m"].notna()
        & ~merged["pre_entry_data_invalid"]
    ].copy()
    valid, primary_audit = add_primary_one_minute_outcomes(
        valid, ticker, one_minute_dir
    )
    return valid, {
        "ticker": ticker,
        "status": "ok",
        "error": "",
        "membership_rows": input_membership_rows,
        "opportunity_rows": len(valid),
        "raw_expanded_rows": len(merged),
        "invalid_or_missing_rows": int(len(merged) - len(valid)),
        "source_memberships_rejected": int(len(source_rejected_memberships)),
        "source_rejected_membership_slots": "|".join(
            source_rejected_memberships["membership_slot_ist"].astype(str)
        ),
        "five_minute_min": bars["date"].min(),
        "five_minute_max": bars["date"].max(),
        **primary_audit,
    }


def build_opportunity_dataset(
    memberships: pd.DataFrame,
    five_minute_dir: Path,
    one_minute_dir: Path,
    workers: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    grouped = {
        ticker: group.copy()
        for ticker, group in memberships.groupby("ticker", sort=True)
    }
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, int(workers))) as executor:
        futures = {
            executor.submit(
                build_ticker_opportunities,
                ticker,
                group,
                five_minute_dir,
                one_minute_dir,
            ): ticker
            for ticker, group in grouped.items()
        }
        for completed, future in enumerate(as_completed(futures), 1):
            frame, audit = future.result()
            if not frame.empty:
                frames.append(frame)
            audits.append(audit)
            if completed % 100 == 0 or completed == len(futures):
                print(
                    f"[gt5 extraction] tickers={completed:,}/{len(futures):,} "
                    f"opportunities={sum(len(item) for item in frames):,}",
                    flush=True,
                )
    if not frames:
        raise RuntimeError("no valid 5-minute entry opportunities were built")
    opportunities = pd.concat(frames, ignore_index=True, sort=False)
    opportunities = opportunities.sort_values(
        ["trade_date", "ticker", "entry_execution_time_ist", "membership_slot_ist"],
        kind="mergesort",
    ).reset_index(drop=True)
    opportunities["ticker_day_id"] = (
        opportunities["trade_date"].astype(str)
        + "|"
        + opportunities["ticker"].astype(str)
    )
    return opportunities, pd.DataFrame(audits).sort_values("ticker").reset_index(drop=True)


def assign_splits(
    frame: pd.DataFrame,
    authoritative_dates: Sequence[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = frame.copy()
    dates = sorted({str(value) for value in authoritative_dates})
    if len(dates) < 20:
        raise RuntimeError(f"too few trading dates for chronological validation: {len(dates)}")
    train_end_index = max(1, int(math.floor(len(dates) * 0.60)))
    validation_end_index = max(train_end_index + 1, int(math.floor(len(dates) * 0.80)))
    train_dates = dates[:train_end_index]
    validation_dates = dates[train_end_index:validation_end_index]
    holdout_dates = dates[validation_end_index:]
    mapping = {day: "TRAIN" for day in train_dates}
    mapping.update({day: "VALIDATION" for day in validation_dates})
    mapping.update({day: "HOLDOUT" for day in holdout_dates})
    work["split"] = work["trade_date"].map(mapping)
    unknown = sorted(work.loc[work["split"].isna(), "trade_date"].astype(str).unique())
    if unknown:
        raise RuntimeError(f"opportunities outside authoritative session calendar: {unknown}")
    contract = {
        "dates": len(dates),
        "train": [train_dates[0], train_dates[-1], len(train_dates)],
        "validation": [validation_dates[0], validation_dates[-1], len(validation_dates)],
        "holdout": [holdout_dates[0], holdout_dates[-1], len(holdout_dates)],
        "sessions_with_eligible_opportunities": {
            split: int(work.loc[work["split"].eq(split), "trade_date"].nunique())
            for split in ("TRAIN", "VALIDATION", "HOLDOUT")
        },
    }
    return work, contract


def condition_id(condition: Mapping[str, Any]) -> str:
    if condition["op"] == "==":
        return f"{condition['feature']} == {condition['value']}"
    return f"{condition['feature']} {condition['op']} {float(condition['value']):.8g}"


def apply_condition(frame: pd.DataFrame, condition: Mapping[str, Any]) -> np.ndarray:
    feature = str(condition["feature"])
    if feature not in frame:
        return np.zeros(len(frame), dtype=bool)
    if condition["op"] == "==":
        return frame[feature].astype(str).eq(str(condition["value"])).to_numpy(bool)
    values = pd.to_numeric(frame[feature], errors="coerce").to_numpy(float)
    threshold = float(condition["value"])
    if condition["op"] == ">=":
        return np.isfinite(values) & (values >= threshold)
    if condition["op"] == "<=":
        return np.isfinite(values) & (values <= threshold)
    raise ValueError(f"unknown condition operator: {condition['op']}")


def apply_rule(frame: pd.DataFrame, conditions: Sequence[Mapping[str, Any]]) -> np.ndarray:
    mask = np.ones(len(frame), dtype=bool)
    for condition in conditions:
        mask &= apply_condition(frame, condition)
    return mask


def first_entries(frame: pd.DataFrame, mask: np.ndarray) -> pd.DataFrame:
    selected = frame.loc[mask].copy()
    if selected.empty:
        return selected
    return selected.drop_duplicates(["trade_date", "ticker"], keep="first").reset_index(drop=True)


def wilson_lower(hits: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = hits / total
    denominator = 1.0 + z * z / total
    centre = p + z * z / (2.0 * total)
    spread = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
    return max(0.0, (centre - spread) / denominator)


def performance_metrics(
    frame: pd.DataFrame,
    mask: np.ndarray,
    achievable_positive_ticker_days: int,
) -> dict[str, Any]:
    metric_columns = [
        "trade_date",
        "ticker",
        "hit_5pct",
        "max_forward_return_pct",
        "eod_return_pct",
    ]
    entries = frame.loc[mask, metric_columns].drop_duplicates(
        ["trade_date", "ticker"], keep="first"
    )
    total = len(entries)
    hits = int(entries.get("hit_5pct", pd.Series(dtype=bool)).fillna(False).sum())
    hit_rate = hits / total if total else 0.0
    daily = entries.groupby("trade_date").size() if total else pd.Series(dtype=int)
    monthly = (
        entries.assign(month=entries["trade_date"].astype(str).str[:7])
        .groupby("month")["hit_5pct"]
        .agg(["size", "sum"])
        if total
        else pd.DataFrame(columns=["size", "sum"])
    )
    positive_months = int((monthly["sum"] > 0).sum()) if len(monthly) else 0
    return {
        "entries": total,
        "hits_5pct": hits,
        "hit_rate": hit_rate,
        "wilson_lower_95": wilson_lower(hits, total),
        "capture_of_achievable": (
            hits / achievable_positive_ticker_days if achievable_positive_ticker_days else 0.0
        ),
        "active_days": int(entries["trade_date"].nunique()) if total else 0,
        "max_entries_per_day": int(daily.max()) if len(daily) else 0,
        "median_entries_per_day": float(daily.median()) if len(daily) else 0.0,
        "positive_months": positive_months,
        "months": int(len(monthly)),
        "median_max_forward_return_pct": float(
            pd.to_numeric(entries.get("max_forward_return_pct"), errors="coerce").median()
        ) if total else 0.0,
        "median_eod_return_pct": float(
            pd.to_numeric(entries.get("eod_return_pct"), errors="coerce").median()
        ) if total else 0.0,
    }


def generate_conditions(train: pd.DataFrame) -> list[dict[str, Any]]:
    conditions: dict[str, dict[str, Any]] = {}
    quantiles = (0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90)
    for feature in MODEL_FEATURES:
        if feature not in train:
            continue
        numeric = pd.to_numeric(train[feature], errors="coerce").dropna()
        if len(numeric) < 500 or numeric.nunique() < 4:
            continue
        for value in numeric.quantile(quantiles).tolist():
            if not math.isfinite(float(value)):
                continue
            for operator in (">=", "<="):
                condition = {"feature": feature, "op": operator, "value": float(value)}
                conditions[condition_id(condition)] = condition
    for feature in CATEGORICAL_FEATURES:
        if feature not in train:
            continue
        values = train[feature]
        values = values.loc[values.notna()].astype(str).str.strip()
        values = values.loc[~values.str.lower().isin({"", "nan", "none", "null"})]
        counts = values.value_counts()
        for value, count in counts.items():
            if count >= max(100, int(len(train) * 0.02)):
                condition = {"feature": feature, "op": "==", "value": str(value)}
                conditions[condition_id(condition)] = condition
    return list(conditions.values())


def achievable_count(frame: pd.DataFrame) -> int:
    return int(
        frame.groupby(["trade_date", "ticker"], sort=False)["hit_5pct"]
        .max()
        .fillna(False)
        .sum()
    )


def selection_score(
    metrics: Mapping[str, Any],
    baseline_rate: float,
    *,
    split: str,
) -> float:
    if split == "TRAIN":
        minimums = {"entries": 150, "hits_5pct": 20, "active_days": 24, "positive_months": 3}
        minimum_lift = MIN_TRAIN_LIFT
    elif split == "VALIDATION":
        minimums = {"entries": 50, "hits_5pct": 5, "active_days": 8, "positive_months": 2}
        minimum_lift = MIN_VALIDATION_LIFT
    else:
        raise ValueError(f"unsupported selection split: {split}")
    if any(int(metrics[key]) < value for key, value in minimums.items()):
        return float("-inf")
    lift = float(metrics["hit_rate"]) / max(float(baseline_rate), 1e-12)
    if lift < minimum_lift:
        return float("-inf")
    if float(metrics["wilson_lower_95"]) <= float(baseline_rate):
        return float("-inf")
    return (
        float(metrics["wilson_lower_95"]) * 1000.0
        + lift * 2.0
        + math.sqrt(float(metrics["entries"])) * 0.02
    )


def rule_search(
    opportunities: pd.DataFrame,
    max_conditions: int = 4,
) -> tuple[list[dict[str, Any]], pd.DataFrame, dict[str, dict[str, Any]]]:
    train = opportunities.loc[opportunities["split"].eq("TRAIN")].copy()
    validation = opportunities.loc[opportunities["split"].eq("VALIDATION")].copy()
    holdout = opportunities.loc[opportunities["split"].eq("HOLDOUT")].copy()
    train_achievable = achievable_count(train)
    validation_achievable = achievable_count(validation)
    holdout_achievable = achievable_count(holdout)
    base_train = performance_metrics(train, np.ones(len(train), bool), train_achievable)
    base_validation = performance_metrics(
        validation, np.ones(len(validation), bool), validation_achievable
    )
    base_holdout = performance_metrics(holdout, np.ones(len(holdout), bool), holdout_achievable)
    baselines = {
        "TRAIN": base_train,
        "VALIDATION": base_validation,
        "HOLDOUT": base_holdout,
    }
    condition_pool = generate_conditions(train)
    selected: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    train_prefixes: list[tuple[list[dict[str, Any]], dict[str, Any], float]] = []

    for depth in range(1, max_conditions + 1):
        depth_candidates: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        used_features = {item["feature"] for item in selected}
        for condition in condition_pool:
            # Allow a second bound on the same feature only after another feature
            # has entered the rule; this keeps early searches broad and legible.
            same_feature_count = sum(
                1 for item in selected if item["feature"] == condition["feature"]
            )
            if same_feature_count >= 2 or (
                same_feature_count == 1 and len(used_features) <= 1
            ):
                continue
            trial = [*selected, condition]
            train_metrics = performance_metrics(
                train, apply_rule(train, trial), train_achievable
            )
            score = selection_score(
                train_metrics,
                float(base_train["hit_rate"]),
                split="TRAIN",
            )
            depth_candidates.append((score, condition, train_metrics))
        depth_candidates.sort(key=lambda item: item[0], reverse=True)
        for rank, (score, condition, train_metrics) in enumerate(
            depth_candidates[:50], 1
        ):
            audit_rows.append(
                {
                    "phase": "TRAIN_GREEDY_SEARCH",
                    "depth": depth,
                    "rank_at_depth": rank,
                    "candidate_condition": condition_id(condition),
                    "candidate_score": score,
                    "selected_prefix": " AND ".join(condition_id(item) for item in selected),
                    **{f"train_{key}": value for key, value in train_metrics.items()},
                }
            )
        if not depth_candidates or not math.isfinite(depth_candidates[0][0]):
            break
        top_score, top_condition, top_train_metrics = depth_candidates[0]
        selected.append(top_condition)
        train_prefixes.append(
            ([dict(item) for item in selected], dict(top_train_metrics), float(top_score))
        )

    validation_shortlist: list[
        tuple[float, list[dict[str, Any]], dict[str, Any], dict[str, Any]]
    ] = []
    for prefix, train_metrics, train_score in train_prefixes:
        validation_metrics = performance_metrics(
            validation, apply_rule(validation, prefix), validation_achievable
        )
        score = selection_score(
            validation_metrics,
            float(base_validation["hit_rate"]),
            split="VALIDATION",
        )
        validation_shortlist.append((score, prefix, train_metrics, validation_metrics))
        audit_rows.append(
            {
                "phase": "VALIDATION_PREFIX_SELECTION",
                "depth": len(prefix),
                "rank_at_depth": 1,
                "candidate_condition": " AND ".join(condition_id(item) for item in prefix),
                "candidate_score": score,
                "selected_prefix": "",
                "train_search_score": train_score,
                **{f"train_{key}": value for key, value in train_metrics.items()},
                **{f"validation_{key}": value for key, value in validation_metrics.items()},
            }
        )
    validation_shortlist.sort(key=lambda item: item[0], reverse=True)
    final = (
        [dict(item) for item in validation_shortlist[0][1]]
        if validation_shortlist and math.isfinite(validation_shortlist[0][0])
        else []
    )
    metrics_by_split: dict[str, dict[str, Any]] = {}
    for split, frame, achievable in (
        ("TRAIN", train, train_achievable),
        ("VALIDATION", validation, validation_achievable),
        ("HOLDOUT", holdout, holdout_achievable),
    ):
        metrics = performance_metrics(frame, apply_rule(frame, final), achievable)
        baseline = baselines[split]
        metrics["baseline_entries"] = baseline["entries"]
        metrics["baseline_hits_5pct"] = baseline["hits_5pct"]
        metrics["baseline_hit_rate"] = baseline["hit_rate"]
        metrics["lift_vs_baseline"] = (
            float(metrics["hit_rate"]) / max(float(baseline["hit_rate"]), 1e-12)
        )
        metrics_by_split[split] = metrics
    return final, pd.DataFrame(audit_rows), metrics_by_split


def cross_tf_sensitivity_metrics(
    opportunities: pd.DataFrame,
    conditions: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Re-evaluate the frozen rule after removing only cross-TF label disagreements."""
    agreement = opportunities["cross_tf_target_agreement"]
    clean = opportunities.loc[agreement.isna() | agreement.fillna(False)].copy()
    output: dict[str, dict[str, Any]] = {}
    for split in ("TRAIN", "VALIDATION", "HOLDOUT"):
        frame = clean.loc[clean["split"].eq(split)].copy()
        full_split = opportunities.loc[opportunities["split"].eq(split)]
        achievable = achievable_count(frame)
        baseline = performance_metrics(frame, np.ones(len(frame), bool), achievable)
        metrics = performance_metrics(frame, apply_rule(frame, conditions), achievable)
        metrics["baseline_entries"] = baseline["entries"]
        metrics["baseline_hits_5pct"] = baseline["hits_5pct"]
        metrics["baseline_hit_rate"] = baseline["hit_rate"]
        metrics["lift_vs_baseline"] = (
            float(metrics["hit_rate"]) / max(float(baseline["hit_rate"]), 1e-12)
        )
        metrics["excluded_cross_tf_label_disagreement_opportunities"] = int(
            full_split["cross_tf_target_agreement"].eq(False).sum()
        )
        output[split] = metrics
    return output


def indicator_range_report(opportunities: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split in ("TRAIN", "VALIDATION", "HOLDOUT"):
        split_frame = opportunities.loc[opportunities["split"].eq(split)].copy()
        baseline_entries = first_entries(split_frame, np.ones(len(split_frame), bool))
        for feature in MODEL_FEATURES:
            if feature not in baseline_entries:
                continue
            all_values = pd.to_numeric(baseline_entries[feature], errors="coerce")
            winner_values = all_values.loc[baseline_entries["hit_5pct"].fillna(False)]
            loser_values = all_values.loc[~baseline_entries["hit_5pct"].fillna(False)]
            if all_values.notna().sum() == 0:
                continue
            row: dict[str, Any] = {
                "split": split,
                "use": "DISCOVERY" if split == "TRAIN" else "POST_FREEZE_VERIFICATION",
                "feature": feature,
                "all_count": int(all_values.notna().sum()),
                "winner_count": int(winner_values.notna().sum()),
                "missing_pct": float(all_values.isna().mean() * 100.0),
                "all_median": float(all_values.median()),
                "loser_median": float(loser_values.median()) if loser_values.notna().any() else np.nan,
            }
            for quantile, label in (
                (0.10, "winner_p10"),
                (0.25, "winner_p25"),
                (0.50, "winner_median"),
                (0.75, "winner_p75"),
                (0.90, "winner_p90"),
            ):
                row[label] = (
                    float(winner_values.quantile(quantile))
                    if winner_values.notna().any()
                    else np.nan
                )
            rows.append(row)
    return pd.DataFrame(rows)


def read_one_minute(path: Path) -> pd.DataFrame | None:
    try:
        bars = read_parquet_window(path, ("date", "open", "high", "low", "close"))
    except Exception:
        return None
    if bars is None or bars.empty:
        return None
    for column in ("open", "high", "low", "close"):
        bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars = bars.loc[
        bars["date"].notna()
        & bars["high"].gt(0.0)
        & bars["close"].gt(0.0)
    ].copy()
    bars = bars.sort_values("date", kind="mergesort").reset_index(drop=True)
    bars["trade_date"] = bars["date"].dt.strftime("%Y-%m-%d")
    return bars.sort_values("date", kind="mergesort").reset_index(drop=True)


def resolve_ticker_entry_maxima(
    ticker: str,
    entries: pd.DataFrame,
    one_minute_dir: Path,
    five_minute_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    one = read_one_minute(
        one_minute_dir / f"{ticker}_stocks_indicators_1min.parquet"
    )
    five = read_parquet_window(
        five_minute_dir / f"{ticker}_stocks_indicators_5min.parquet",
        ("date", "open", "high", "low", "close", "gap_filled"),
    )
    if one is not None and not one.empty:
        one = filter_end_stamped_session(one, first_label="09:16")
        one = one.loc[
            one["high"].ge(one[["open", "low", "close"]].max(axis=1))
            & one["low"].le(one[["open", "high", "close"]].min(axis=1))
        ].copy()
    if five is not None and not five.empty:
        for column in ("open", "high", "low", "close", "gap_filled"):
            if column not in five:
                five[column] = 0.0 if column == "gap_filled" else np.nan
            five[column] = pd.to_numeric(five[column], errors="coerce")
        five = filter_end_stamped_session(five, first_label=SESSION_FIRST_5M_END)
        five = five.loc[
            five["date"].notna()
            & five["high"].gt(0.0)
            & five["close"].gt(0.0)
            & five["gap_filled"].fillna(0.0).lt(0.5)
        ].copy()
        five["trade_date"] = five["date"].dt.strftime("%Y-%m-%d")
    output_rows: list[dict[str, Any]] = []
    one_dates = set(one["trade_date"]) if one is not None else set()
    five_dates = set(five["trade_date"]) if five is not None else set()
    for _, entry in entries.iterrows():
        day = str(entry["trade_date"])
        execution = pd.Timestamp(entry["entry_execution_time_ist"])
        session_end = pd.Timestamp(f"{day} {SESSION_LAST_END}", tz=IST)
        one_day = (
            one.loc[
                one["trade_date"].eq(day)
                & one["date"].gt(execution)
                & one["date"].le(session_end)
            ].copy()
            if one is not None and day in one_dates
            else pd.DataFrame()
        )
        five_day = (
            five.loc[
                five["trade_date"].eq(day)
                & five["date"].ge(pd.Timestamp(entry["entry_price_source_bar_end_ist"]))
                & five["date"].le(session_end)
            ].copy()
            if five is not None and day in five_dates
            else pd.DataFrame()
        )
        one_max = float(one_day["high"].max()) if not one_day.empty else float("nan")
        five_max = float(five_day["high"].max()) if not five_day.empty else float("nan")
        max_price = float(entry["daily_max_price"])
        max_source = str(entry["daily_max_time_source"])
        chosen = one_day if max_source == "1min" else five_day
        tolerance = max(0.02, abs(max_price) * 1e-6)
        max_rows = (
            chosen.loc[np.isclose(chosen["high"], max_price, atol=tolerance, rtol=0.0)]
            if not chosen.empty and math.isfinite(max_price)
            else pd.DataFrame()
        )
        target_price = float(entry["entry_price"]) * (1.0 + TARGET_RETURN_PCT / 100.0)
        hit_rows = bars_reaching_target(chosen, target_price)
        hit_bar_end = hit_rows.iloc[0]["date"] if not hit_rows.empty else pd.NaT
        interval_minutes = 1 if max_source == "1min" else 5
        hit_interval_start = (
            hit_bar_end - pd.Timedelta(minutes=interval_minutes)
            if pd.notna(hit_bar_end)
            else pd.NaT
        )
        cross_tf_abs_diff = (
            abs(one_max - five_max)
            if math.isfinite(one_max) and math.isfinite(five_max)
            else np.nan
        )
        cross_tf_bps = (
            cross_tf_abs_diff / max(one_max, 1e-12) * 10000.0
            if math.isfinite(cross_tf_abs_diff)
            else np.nan
        )
        row = entry.to_dict()
        row.update(
            {
                "one_minute_rows_after_entry": int(len(one_day)),
                "one_minute_last_time_ist": one_day["date"].max() if not one_day.empty else pd.NaT,
                "five_minute_rows_after_entry": int(len(five_day)),
                "daily_max_tie_count": int(len(max_rows)),
                "daily_max_last_bar_end_ist": (
                    max_rows.iloc[-1]["date"] if not max_rows.empty else pd.NaT
                ),
                "first_hit_5pct_time_ist": hit_interval_start,
                "first_hit_5pct_bar_end_ist": hit_bar_end,
                "first_hit_5pct_interval_start_ist": hit_interval_start,
                "first_hit_5pct_interval_end_ist": hit_bar_end,
                "first_hit_5pct_time_source": max_source if pd.notna(hit_bar_end) else "",
                "one_minute_comparison_max": one_max,
                "five_minute_comparison_max": five_max,
                "cross_tf_max_abs_diff": cross_tf_abs_diff,
                "cross_tf_max_diff_bps": cross_tf_bps,
                "cross_tf_parity_within_tolerance": bool(
                    math.isfinite(cross_tf_abs_diff) and cross_tf_abs_diff <= tolerance
                ),
            }
        )
        output_rows.append(row)
    return pd.DataFrame(output_rows), {
        "ticker": ticker,
        "entries": len(entries),
        "one_minute_file_available": one is not None and not one.empty,
        "one_minute_dates_used": int(len(set(entries["trade_date"].astype(str)) & one_dates)),
        "five_minute_dates_used": int(
            len(set(entries["trade_date"].astype(str)) & five_dates)
        ),
    }


def resolve_entry_maxima(
    entries: pd.DataFrame,
    one_minute_dir: Path,
    five_minute_dir: Path,
    workers: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    audits: list[dict[str, Any]] = []
    groups = {
        ticker: group.copy()
        for ticker, group in entries.groupby("ticker", sort=True)
    }
    with ProcessPoolExecutor(max_workers=max(1, int(workers))) as executor:
        futures = {
            executor.submit(
                resolve_ticker_entry_maxima,
                ticker,
                group,
                one_minute_dir,
                five_minute_dir,
            ): ticker
            for ticker, group in groups.items()
        }
        for completed, future in enumerate(as_completed(futures), 1):
            frame, audit = future.result()
            frames.append(frame)
            audits.append(audit)
            if completed % 100 == 0 or completed == len(futures):
                print(
                    f"[gt5 max timing] tickers={completed:,}/{len(futures):,} "
                    f"entries={sum(len(item) for item in frames):,}",
                    flush=True,
                )
    resolved = pd.concat(frames, ignore_index=True, sort=False).sort_values(
        ["trade_date", "entry_execution_time_ist", "ticker"], kind="mergesort"
    ).reset_index(drop=True)
    return resolved, pd.DataFrame(audits).sort_values("ticker").reset_index(drop=True)


def summary_tables(entries: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    hour = (
        entries.groupby(["split", "membership_hour"], sort=True)
        .agg(
            entries=("ticker", "size"),
            unique_tickers=("ticker", "nunique"),
            hits_5pct=("hit_5pct", "sum"),
            hit_rate=("hit_5pct", "mean"),
            median_max_return_pct=("max_forward_return_pct", "median"),
            median_eod_return_pct=("eod_return_pct", "median"),
        )
        .reset_index()
    )
    month = entries.assign(month=entries["trade_date"].astype(str).str[:7])
    month = (
        month.groupby(["split", "month"], sort=True)
        .agg(
            entries=("ticker", "size"),
            unique_tickers=("ticker", "nunique"),
            active_days=("trade_date", "nunique"),
            hits_5pct=("hit_5pct", "sum"),
            hit_rate=("hit_5pct", "mean"),
            median_max_return_pct=("max_forward_return_pct", "median"),
            median_eod_return_pct=("eod_return_pct", "median"),
        )
        .reset_index()
    )
    return hour, month


def daily_summary(entries: pd.DataFrame) -> pd.DataFrame:
    return (
        entries.groupby(["split", "trade_date"], sort=True)
        .agg(
            entries=("ticker", "size"),
            unique_tickers=("ticker", "nunique"),
            hits_5pct=("hit_5pct", "sum"),
            hit_rate=("hit_5pct", "mean"),
            median_max_return_pct=("max_forward_return_pct", "median"),
            median_eod_return_pct=("eod_return_pct", "median"),
        )
        .reset_index()
    )


def evaluate_setup_acceptance(
    conditions: Sequence[Mapping[str, Any]],
    metrics_by_split: Mapping[str, Mapping[str, Any]],
    sensitivity_by_split: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    reasons: list[str] = []
    if not conditions:
        reasons.append("no TRAIN-discovered prefix passed selection-validation gates")
    holdout = metrics_by_split.get("HOLDOUT", {})
    holdout_requirements = {
        "entries": 50,
        "hits_5pct": 5,
        "active_days": 8,
        "positive_months": 2,
    }
    for key, minimum in holdout_requirements.items():
        if int(holdout.get(key, 0)) < minimum:
            reasons.append(f"HOLDOUT {key} {holdout.get(key, 0)} < {minimum}")
    if float(holdout.get("lift_vs_baseline", 0.0)) < MIN_HOLDOUT_LIFT:
        reasons.append(
            f"HOLDOUT lift {float(holdout.get('lift_vs_baseline', 0.0)):.4f} "
            f"< {MIN_HOLDOUT_LIFT:.2f}"
        )
    if float(holdout.get("wilson_lower_95", 0.0)) <= float(
        holdout.get("baseline_hit_rate", 0.0)
    ):
        reasons.append("HOLDOUT Wilson lower bound does not exceed baseline hit rate")
    for split, minimum_lift in (
        ("VALIDATION", MIN_VALIDATION_LIFT),
        ("HOLDOUT", MIN_HOLDOUT_LIFT),
    ):
        metrics = sensitivity_by_split.get(split, {})
        if int(metrics.get("entries", 0)) < 50:
            reasons.append(f"{split} cross-TF sensitivity entries < 50")
        if int(metrics.get("hits_5pct", 0)) < 5:
            reasons.append(f"{split} cross-TF sensitivity hits_5pct < 5")
        if int(metrics.get("active_days", 0)) < 8:
            reasons.append(f"{split} cross-TF sensitivity active_days < 8")
        if float(metrics.get("lift_vs_baseline", 0.0)) < minimum_lift:
            reasons.append(
                f"{split} cross-TF sensitivity lift "
                f"{float(metrics.get('lift_vs_baseline', 0.0)):.4f} < {minimum_lift:.2f}"
            )
        if float(metrics.get("wilson_lower_95", 0.0)) <= float(
            metrics.get("baseline_hit_rate", 0.0)
        ):
            reasons.append(
                f"{split} cross-TF sensitivity Wilson lower bound does not exceed baseline"
            )
    return {
        "passed": not reasons,
        "reasons": reasons,
        "predeclared_gates": {
            "train_min_lift": MIN_TRAIN_LIFT,
            "validation_min_lift": MIN_VALIDATION_LIFT,
            "holdout_min_lift": MIN_HOLDOUT_LIFT,
            "holdout_min_entries": 50,
            "holdout_min_hits_5pct": 5,
            "holdout_min_active_days": 8,
            "holdout_min_positive_months": 2,
            "validation_and_holdout_wilson_lower_must_exceed_baseline": True,
            "cross_tf_label_disagreement_sensitivity_must_pass": True,
        },
    }


def render_config(
    *,
    conditions: Sequence[Mapping[str, Any]],
    split_contract: Mapping[str, Any],
    metrics_by_split: Mapping[str, Mapping[str, Any]],
    sensitivity_by_split: Mapping[str, Mapping[str, Any]],
    source_path: Path,
    source_sha256: str,
    setup_status: str,
    acceptance: Mapping[str, Any],
) -> str:
    conditions_repr = pprint.pformat(jsonable(list(conditions)), width=100, sort_dicts=True)
    split_repr = pprint.pformat(jsonable(dict(split_contract)), width=100, sort_dicts=True)
    metrics_repr = pprint.pformat(jsonable(dict(metrics_by_split)), width=120, sort_dicts=True)
    sensitivity_repr = pprint.pformat(
        jsonable(dict(sensitivity_by_split)), width=120, sort_dicts=True
    )
    acceptance_repr = pprint.pformat(jsonable(dict(acceptance)), width=120, sort_dicts=True)
    return f'''"""Research-only LONG setup discovered on frozen six-month prefilter data.

This module is not production approved and is not imported by any live process.
"""

import math

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
SETUP_STATUS = {setup_status!r}
SETUP_NAME = "PREFILTER_LONG_5M_GT5_COHERENCE"

SOURCE_PREFILTER_FILE = {str(source_path)!r}
SOURCE_PREFILTER_SHA256 = {source_sha256!r}
RESEARCH_WINDOW = [{START_DATE!r}, {END_DATE!r}]
CHRONOLOGICAL_SPLIT = {split_repr}

PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_RANK_MIN = 200
PREFILTER_RANK_MAX = 300
ENTRY_TIMEFRAME_MINUTES = 5
ENTRY_SIGNAL_USES_COMPLETED_BAR = True
PREFILTER_POOL_ACTIVATION_DELAY_MINUTES = 5
ENTRY_PRICE_MODEL = "open_of_following_end_stamped_5min_bar"
ENTRY_EXECUTION_BOUNDARY_IS_SIGNAL_BAR_END = True
FIRST_ELIGIBLE_HISTORICAL_1M_LABEL_OFFSET_MINUTES = 1
ENTRY_MUST_BE_WITHIN_ACTIVE_PREFILTER_HOUR = True
ONE_ENTRY_PER_TICKER_PER_DAY = True
TARGET_FORWARD_RETURN_PCT = {TARGET_RETURN_PCT!r}
TARGET_OBSERVATION_END_IST = "15:30"
MAX_TIME_SOURCE_POLICY = "complete_1min_path_else_complete_5min_path_else_exclude"
HOLDOUT_CONSUMED = True

FILTERS = {conditions_repr}
VALIDATION_METRICS = {metrics_repr}
CROSS_TF_LABEL_SENSITIVITY_METRICS = {sensitivity_repr}
ACCEPTANCE = {acceptance_repr}


def matches(features):
    """Fail-closed evaluation of the frozen interpretable filter list."""
    if not FILTERS or not ACCEPTANCE.get("passed", False):
        return False
    for condition in FILTERS:
        feature = condition["feature"]
        if feature not in features or features[feature] is None:
            return False
        value = features[feature]
        operator = condition["op"]
        threshold = condition["value"]
        if operator in {{">=", "<="}}:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return False
            if not math.isfinite(numeric):
                return False
            if operator == ">=" and not (numeric >= float(threshold)):
                return False
            if operator == "<=" and not (numeric <= float(threshold)):
                return False
        if operator == "==" and (
            str(value).strip().lower() in {{"", "nan", "none", "null"}}
            or str(value) != str(threshold)
        ):
            return False
    return True
'''


def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    source_path = Path(args.prefilter).resolve()
    five_minute_dir = Path(args.five_minute_dir).resolve()
    one_minute_dir = Path(args.one_minute_dir).resolve()
    source_sha = sha256_file(source_path)
    memberships, rejected_memberships = load_memberships(source_path)
    rejected_memberships.to_csv(out_dir / "rejected_prefilter_memberships.csv", index=False)
    (
        memberships.groupby(["trade_date", "membership_hour"], sort=True)
        .agg(selected_long=("ticker", "size"), unique_tickers=("ticker", "nunique"))
        .reset_index()
        .to_csv(out_dir / "prefilter_long_selected_by_hour.csv", index=False)
    )
    (
        memberships.groupby("trade_date", sort=True)
        .agg(
            long_memberships=("ticker", "size"),
            unique_long_tickers=("ticker", "nunique"),
            hourly_slots=("membership_hour", "nunique"),
        )
        .reset_index()
        .to_csv(out_dir / "prefilter_long_selected_by_day.csv", index=False)
    )
    print(
        f"[gt5] LONG memberships={len(memberships):,} "
        f"rejected_prefilter_memberships={len(rejected_memberships):,} "
        f"ticker_days={memberships.drop_duplicates(['trade_date','ticker']).shape[0]:,} "
        f"tickers={memberships['ticker'].nunique():,}",
        flush=True,
    )

    cache_path = out_dir / "causal_entry_opportunities_v2.parquet"
    extraction_audit_path = out_dir / "five_minute_extraction_audit.csv"
    cache_manifest_path = out_dir / "causal_entry_opportunities_v2_manifest.json"
    expected_cache_manifest = {
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "source_prefilter_sha256": source_sha,
        "script_sha256": sha256_file(Path(__file__).resolve()),
        "five_minute_dir": str(five_minute_dir),
        "one_minute_dir": str(one_minute_dir),
        "accepted_memberships": len(memberships),
        "rejected_prefilter_memberships": len(rejected_memberships),
        "window": [START_DATE, END_DATE],
    }
    cache_matches = False
    if cache_manifest_path.exists():
        try:
            cache_matches = json.loads(cache_manifest_path.read_text(encoding="utf-8")) == expected_cache_manifest
        except Exception:
            cache_matches = False
    if (
        args.resume_opportunities
        and cache_path.exists()
        and extraction_audit_path.exists()
        and cache_matches
    ):
        opportunities = pd.read_parquet(cache_path)
        extraction_audit = pd.read_csv(extraction_audit_path, low_memory=False)
        for column in (
            "membership_slot_ist",
            "signal_time_ist",
            "entry_execution_time_ist",
            "entry_price_source_bar_end_ist",
            "first_eligible_1m_bar_end_ist",
            "forward_max_time_5m",
            "daily_max_bar_end_ist",
            "daily_max_interval_start_ist",
            "daily_max_interval_end_ist",
            "daily_max_time_ist",
        ):
            if column in opportunities:
                opportunities[column] = normalise_ist(opportunities[column])
        print(f"[gt5] resumed opportunities={len(opportunities):,}", flush=True)
    else:
        opportunities, extraction_audit = build_opportunity_dataset(
            memberships,
            five_minute_dir,
            one_minute_dir,
            int(args.workers),
        )
        if source_path == DEFAULT_PREFILTER.resolve() and len(opportunities) != 440_837:
            raise RuntimeError(
                "strict causal opportunity count mismatch: "
                f"expected 440,837, observed {len(opportunities):,}"
            )
        opportunities.to_parquet(cache_path, index=False)
        extraction_audit.to_csv(extraction_audit_path, index=False)
        write_json(cache_manifest_path, expected_cache_manifest)

    incomplete_opportunities = opportunities.loc[~opportunities["max_window_complete"]].copy()
    incomplete_opportunities.to_csv(
        out_dir / "incomplete_exit_path_opportunities.csv", index=False
    )
    opportunities = opportunities.loc[opportunities["max_window_complete"]].copy()
    authoritative_dates = sorted(memberships["trade_date"].astype(str).unique())
    opportunities, split_contract = assign_splits(opportunities, authoritative_dates)
    conditions, search_audit, metrics_by_split = rule_search(
        opportunities, max_conditions=int(args.max_conditions)
    )
    search_audit.to_csv(out_dir / "rule_search_audit.csv", index=False)
    ranges = indicator_range_report(opportunities)
    ranges.to_csv(out_dir / "indicator_ranges.csv", index=False)

    sensitivity_by_split = cross_tf_sensitivity_metrics(opportunities, conditions)
    acceptance = evaluate_setup_acceptance(
        conditions, metrics_by_split, sensitivity_by_split
    )
    if not conditions:
        setup_status = "NO_VALIDATED_FILTER_BASELINE_ONLY"
    else:
        setup_status = (
            "VALIDATED_HOLDOUT_RESEARCH_CANDIDATE"
            if acceptance["passed"]
            else "HOLDOUT_NOT_CONFIRMED_RESEARCH_ONLY"
        )

    full_entries_5m = first_entries(opportunities, np.ones(len(opportunities), bool))
    full_entries_5m["output_scope"] = "ALL_LONG_REFERENCE"
    if conditions:
        setup_entries_5m = first_entries(opportunities, apply_rule(opportunities, conditions))
    else:
        setup_entries_5m = opportunities.iloc[0:0].copy()
    setup_entries_5m["output_scope"] = "FROZEN_SETUP"
    combined_entries = pd.concat(
        [full_entries_5m, setup_entries_5m], ignore_index=True, sort=False
    )
    resolved_combined, max_timing_audit = resolve_entry_maxima(
        combined_entries,
        one_minute_dir,
        five_minute_dir,
        int(args.workers),
    )
    resolved_full = resolved_combined.loc[
        resolved_combined["output_scope"].eq("ALL_LONG_REFERENCE")
    ].copy()
    resolved_entries = resolved_combined.loc[
        resolved_combined["output_scope"].eq("FROZEN_SETUP")
    ].copy()
    resolved_full.to_csv(out_dir / "all_long_prefilter_entries_with_daily_max.csv", index=False)
    resolved_entries.to_csv(out_dir / "setup_entries_with_daily_max.csv", index=False)
    movers = resolved_full.loc[resolved_full["hit_5pct"].fillna(False)].copy()
    movers.to_csv(out_dir / "gt5pct_movers_full_list.csv", index=False)
    setup_movers = resolved_entries.loc[resolved_entries["hit_5pct"].fillna(False)].copy()
    setup_movers.to_csv(out_dir / "setup_gt5pct_movers.csv", index=False)
    review_rows = resolved_full.loc[
        resolved_full["future_extreme_review_flag"].fillna(False)
        | resolved_full["large_gap_review_flag"].fillna(False)
        | ~resolved_full["cross_tf_parity_within_tolerance"].fillna(False)
    ].copy()
    review_rows.to_csv(out_dir / "data_quality_review_entries.csv", index=False)
    max_timing_audit.to_csv(out_dir / "max_timing_source_audit.csv", index=False)
    hourly, monthly = summary_tables(resolved_entries)
    daily = daily_summary(resolved_entries)
    hourly.to_csv(out_dir / "hourly_summary.csv", index=False)
    daily.to_csv(out_dir / "daily_summary.csv", index=False)
    monthly.to_csv(out_dir / "monthly_summary.csv", index=False)
    full_hourly, full_monthly = summary_tables(resolved_full)
    full_daily = daily_summary(resolved_full)
    full_hourly.to_csv(out_dir / "all_long_hourly_summary.csv", index=False)
    full_daily.to_csv(out_dir / "all_long_daily_summary.csv", index=False)
    full_monthly.to_csv(out_dir / "all_long_monthly_summary.csv", index=False)

    conf_path = out_dir / "prefilter_long_5m_gt5pct_conf.py"
    conf_path.write_text(
        render_config(
            conditions=conditions,
            split_contract=split_contract,
            metrics_by_split=metrics_by_split,
            sensitivity_by_split=sensitivity_by_split,
            source_path=source_path,
            source_sha256=source_sha,
            setup_status=setup_status,
            acceptance=acceptance,
        ),
        encoding="utf-8",
    )
    compile(conf_path.read_text(encoding="utf-8"), str(conf_path), "exec")

    summary = {
        "status": setup_status,
        "production_approved": False,
        "source_prefilter": str(source_path),
        "source_prefilter_sha256": source_sha,
        "window": [START_DATE, END_DATE],
        "cache_schema_version": CACHE_SCHEMA_VERSION,
        "long_memberships": len(memberships),
        "rejected_prefilter_memberships": len(rejected_memberships),
        "long_membership_ticker_days": memberships.drop_duplicates(
            ["trade_date", "ticker"]
        ).shape[0],
        "strict_usable_signal_entry_opportunities": len(opportunities) + len(incomplete_opportunities),
        "complete_exit_path_opportunities": len(opportunities),
        "incomplete_exit_path_opportunities": len(incomplete_opportunities),
        "split_contract": split_contract,
        "filters": conditions,
        "acceptance": acceptance,
        "metrics_by_split": metrics_by_split,
        "cross_tf_label_sensitivity_metrics": sensitivity_by_split,
        "setup_entries": len(resolved_entries),
        "setup_gt5pct_movers": len(setup_movers),
        "all_long_reference_entries": len(resolved_full),
        "gt5pct_movers_full_list": len(movers),
        "one_minute_max_timestamps": int(
            resolved_full["daily_max_time_source"].eq("1min").sum()
        ),
        "five_minute_max_fallbacks": int(
            resolved_full["daily_max_time_source"].eq("5min_fallback").sum()
        ),
        "artifacts": {
            "config": str(conf_path),
            "all_long_entries": str(out_dir / "all_long_prefilter_entries_with_daily_max.csv"),
            "full_movers": str(out_dir / "gt5pct_movers_full_list.csv"),
            "setup_entries": str(out_dir / "setup_entries_with_daily_max.csv"),
            "setup_movers": str(out_dir / "setup_gt5pct_movers.csv"),
            "indicator_ranges": str(out_dir / "indicator_ranges.csv"),
            "hourly_summary": str(out_dir / "hourly_summary.csv"),
            "daily_summary": str(out_dir / "daily_summary.csv"),
            "monthly_summary": str(out_dir / "monthly_summary.csv"),
            "prefilter_selected_by_hour": str(out_dir / "prefilter_long_selected_by_hour.csv"),
            "prefilter_selected_by_day": str(out_dir / "prefilter_long_selected_by_day.csv"),
        },
    }
    write_json(out_dir / "summary.json", summary)
    print(json.dumps(jsonable(summary), indent=2, sort_keys=True), flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefilter", default=str(DEFAULT_PREFILTER))
    parser.add_argument("--five-minute-dir", default=str(DEFAULT_5M_DIR))
    parser.add_argument("--one-minute-dir", default=str(DEFAULT_1M_DIR))
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-conditions", type=int, default=4)
    parser.add_argument(
        "--resume-opportunities",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
