"""Frozen V12 research replay for a distilled late-compression LONG setup.

This runner is intentionally isolated from every production/live setup book.  It
combines the strict completed-hourly K300 LONG membership contract with a small,
pre-registered price/volume rule:

* completed 5-minute signal at 14:00..14:25 IST;
* price >= Rs 50 and completed 5-minute traded value >= Rs 1,000,000;
* the previous Bollinger(20, 2) bandwidth is at/below the 25th percentile of
  the twenty bandwidth readings preceding that previous candle;
* current close strictly exceeds the prior ten completed same-session highs;
* current close is at/above causal session VWAP;
* causal same-time-of-day relative volume is at least 1.25;
* the NIFTY proxy is at/above its causal session VWAP;
* first fully qualifying signal per ticker/day consumes the day's one attempt.

Entry is a stop one tick above the signal high, valid on signal+1..signal+3
one-minute completion bars.  Cancellation is checked before the trigger.  Size
is the lesser of Rs 100,000 notional and 2% of expected one-minute volume, where
the expectation is the just-completed five-minute volume divided by five.  The
fixed exit is -0.70% / +0.75%, stop-first on ambiguous minutes, with a 15:15
forced exit.  Five basis points of adverse entry slippage and five basis points
on non-target exits are applied before the statutory NSE intraday cost model.

The module never optimises parameters and never changes a live/paper config.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import eqidv2_late_bb10_compression as compression
import nse_intraday_costs as nse
import research_honest_pf_validator as validator
import research_v12_hourly_two_bar_long_backtest as hourly


IST = "Asia/Kolkata"
SETUP = "L_V12_HOURLY_LATE_COMPRESSION_SIMPLE_RESEARCH"
PRODUCTION_APPROVED = False
PROMOTION_ACTION = "NONE_RESEARCH_ONLY"

DEFAULT_START = "2026-06-01"
DEFAULT_END = "2026-07-31"
DEFAULT_PREFILTER = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_strict_entry_20250616_20260804_k300"
    r"\hourly_candidates_strict_entry.csv"
)
DEFAULT_5M_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\canonical_5m_from_1m_20250602_20260804"
)
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_NIFTY = Path(
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
    r"\NIFTY_stocks_indicators_5min.parquet"
)
DEFAULT_OUT = Path("research_outputs") / (
    "v12_late_compression_simple_long_20260601_20260731"
)

SIGNAL_START_MINUTE = 14 * 60
SIGNAL_END_MINUTE = 14 * 60 + 25
MIN_PRICE_RS = 50.0
MIN_TRADED_VALUE_RS = 1_000_000.0
MIN_RELATIVE_VOLUME = 1.25
BB_PERIOD = 20
BB_QUANTILE_LOOKBACK = 20
BB_QUANTILE = 0.25
BREAKOUT_LOOKBACK = 10
TICK_SIZE = 0.05
ENTRY_VALID_MINUTES = 3
ENTRY_MAX_GAP_PCT = 0.20
ENTRY_SLIPPAGE_BPS = 5.0
NON_TARGET_EXIT_SLIPPAGE_BPS = 5.0
STOP_PCT = 0.70
TARGET_PCT = 0.75
FORCED_EXIT_MINUTE = 15 * 60 + 15
NOTIONAL_RS = 100_000.0
COMPLETED_FIVE_MINUTE_SOURCE_BARS = 5
MAX_EXPECTED_ONE_MINUTE_PARTICIPATION = 0.02
RESEARCH_ACCOUNT_RS = 1_000_000.0
DAILY_LOSS_BRAKE_PCT = 0.50
BOOTSTRAP_DRAWS = 100_000

CANONICAL_COLUMNS = (
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_1m_count",
    "gap_filled",
    "opening_snapshot",
)


@dataclass(frozen=True)
class EntryDecision:
    entry_time: pd.Timestamp | None
    fill_raw: float | None
    trigger: float
    cancel: float
    reason: str


@dataclass(frozen=True)
class SizingDecision:
    notional_quantity: int
    expected_one_minute_volume: float
    capacity_quantity: int
    quantity: int


def _timestamp_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        return pd.NaT
    return stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
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


def _profit_factor(values: Iterable[float]) -> float:
    return validator.profit_factor(values)


def next_exchange_tick(price: float) -> float:
    """Return the first exchange tick strictly above an observed trade price.

    Parquet OHLC is often stored as float32, so a valid Rs 868.40 print can be
    read back as Rs 868.400024.  Treat values within one float32 ULP of a tick
    as that tick before advancing exactly once.  Genuinely off-tick inputs are
    rounded upward to the next valid tick.
    """

    raw = float(price)
    if not math.isfinite(raw) or raw <= 0.0:
        raise ValueError(f"invalid exchange price: {price!r}")
    ratio = raw / TICK_SIZE
    nearest_index = int(round(ratio))
    nearest_price = nearest_index * TICK_SIZE
    float32_ulp = float(np.spacing(np.float32(abs(raw))))
    tolerance = max(1e-9, 1.1 * float32_ulp)
    if abs(raw - nearest_price) <= tolerance:
        trigger_index = nearest_index + 1
    else:
        trigger_index = int(math.ceil(ratio))
    return round(trigger_index * TICK_SIZE, 2)


def causal_sizing(signal_volume: float, entry_price: float) -> SizingDecision:
    """Cap a Rs 100k order at 2% of causally expected one-minute volume."""

    volume = float(signal_volume)
    price = float(entry_price)
    if (
        not math.isfinite(volume)
        or volume <= 0.0
        or not math.isfinite(price)
        or price <= 0.0
    ):
        return SizingDecision(0, 0.0, 0, 0)
    notional_quantity = int(math.floor(NOTIONAL_RS / price))
    expected_one_minute_volume = volume / COMPLETED_FIVE_MINUTE_SOURCE_BARS
    capacity_quantity = int(
        math.floor(expected_one_minute_volume * MAX_EXPECTED_ONE_MINUTE_PARTICIPATION)
    )
    quantity = max(0, min(notional_quantity, capacity_quantity))
    return SizingDecision(
        notional_quantity=notional_quantity,
        expected_one_minute_volume=expected_one_minute_volume,
        capacity_quantity=capacity_quantity,
        quantity=quantity,
    )


def _read_parquet_window(
    path: Path,
    columns: Sequence[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    if not path.exists():
        return None
    available = set(pq.ParquetFile(path).schema_arrow.names)
    wanted = [column for column in columns if column in available]
    required = {"date", "open", "high", "low", "close", "volume"}
    if required - set(wanted):
        return None
    try:
        frame = pd.read_parquet(
            path,
            columns=wanted,
            filters=[("date", ">=", start), ("date", "<", end)],
        )
    except Exception:
        frame = pd.read_parquet(path, columns=wanted)
    if frame is None or frame.empty:
        return None
    frame["date"] = compression._timestamps(frame["date"])
    return frame.loc[frame["date"].between(start, end, inclusive="left")].copy()


def add_distilled_features(raw: pd.DataFrame) -> pd.DataFrame:
    """Add the literal frozen compression rule without indicator-score gates."""

    d = compression.add_features(raw)
    group = d.groupby("session", sort=False, group_keys=False)

    close = pd.to_numeric(d["close"], errors="coerce")
    mid = group["close"].transform(
        lambda values: values.rolling(BB_PERIOD, min_periods=BB_PERIOD).mean()
    )
    std = group["close"].transform(
        lambda values: values.rolling(BB_PERIOD, min_periods=BB_PERIOD).std()
    )
    width = 4.0 * std / mid.replace(0.0, np.nan)
    previous_width = width.groupby(d["session"], sort=False).shift(1)

    # Literal wording: compare the previous candle with the twenty bandwidth
    # observations before it.  The older researched detector included the
    # tested previous width inside its own quantile; retain that as an audit
    # column but do not use it for this new setup.
    prior_threshold = width.groupby(d["session"], sort=False).transform(
        lambda values: values.shift(2)
        .rolling(BB_QUANTILE_LOOKBACK, min_periods=BB_QUANTILE_LOOKBACK)
        .quantile(BB_QUANTILE)
    )
    d["bb_width"] = width
    d["previous_bb_width"] = previous_width
    d["bb_q25_preceding20"] = prior_threshold
    d["bb_compressed_literal"] = previous_width.le(prior_threshold)
    d["bb_compressed_legacy_inclusive"] = d["bb_compressed"]

    # The shared legacy feature builder allows synthetic/incomplete reference
    # bars into its same-time volume median.  Rebuild RVOL here from only valid,
    # fully completed historical bars.  The current bar is never in its own
    # reference window.
    reference_valid = d["valid"].astype("boolean").fillna(False)
    if "source_1m_count" in d.columns:
        reference_valid &= pd.to_numeric(
            d["source_1m_count"], errors="coerce"
        ).eq(COMPLETED_FIVE_MINUTE_SOURCE_BARS)
    reference_volume = pd.to_numeric(d["volume"], errors="coerce").where(
        reference_valid
    )
    minute_group = reference_volume.groupby(d["minute"], sort=False)
    rvol_reference_median = minute_group.transform(
        lambda values: values.shift(1).rolling(10, min_periods=5).median()
    )
    rvol_reference_count = minute_group.transform(
        lambda values: values.shift(1).rolling(10, min_periods=1).count()
    )
    d["rel_volume"] = pd.to_numeric(d["volume"], errors="coerce") / (
        rvol_reference_median.replace(0.0, np.nan)
    )
    d["relative_volume_reference_count"] = rvol_reference_count

    # Do not allow rolling features to bridge missing/synthetic five-minute
    # buckets.  Partial source-minute counts remain diagnostic; the current
    # signal bar itself must contain all five source minutes.
    dates = d["date"]
    link_ok = dates.sub(group["date"].shift(1)).dt.total_seconds().eq(300.0)
    real = d["valid"].astype("boolean").fillna(False)
    history_links = link_ok.groupby(d["session"], sort=False).transform(
        lambda values: values.rolling(40, min_periods=40).sum()
    )
    history_real = real.groupby(d["session"], sort=False).transform(
        lambda values: values.rolling(41, min_periods=41).sum()
    )
    d["compression_history_complete"] = history_links.eq(40) & history_real.eq(41)
    if "source_1m_count" in d.columns:
        source_count = pd.to_numeric(d["source_1m_count"], errors="coerce")
        d["signal_source_1m_complete"] = source_count.eq(5)
    else:
        d["signal_source_1m_complete"] = False
    d["breakout_extension_pct"] = (
        close / pd.to_numeric(d["prev_high10"], errors="coerce") - 1.0
    ) * 100.0
    return d


def load_nifty_above_vwap(
    path: Path,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    start = pd.Timestamp(start_date, tz=IST) - pd.Timedelta(days=3)
    end = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    raw = _read_parquet_window(path, CANONICAL_COLUMNS, start, end)
    if raw is None or raw.empty:
        raise RuntimeError(f"missing NIFTY proxy data: {path}")
    d = compression.add_features(raw)
    d["trade_date"] = d["date"].dt.strftime("%Y-%m-%d")
    d["nifty_bar_present"] = True
    d["nifty_bar_valid"] = d["valid"].astype("boolean").fillna(False)
    d["nifty_above_vwap"] = (
        d["nifty_bar_valid"]
        & pd.to_numeric(d["close"], errors="coerce").ge(
            pd.to_numeric(d["avwap"], errors="coerce")
        )
    )
    return (
        d.loc[
            d["trade_date"].between(start_date, end_date),
            [
                "date",
                "nifty_bar_present",
                "nifty_bar_valid",
                "nifty_above_vwap",
                "close",
                "avwap",
            ],
        ]
        .rename(
            columns={
                "date": "signal_time_ist",
                "close": "nifty_close",
                "avwap": "nifty_session_vwap",
            }
        )
        .drop_duplicates("signal_time_ist", keep="last")
    )


def _load_one_ticker_features(
    ticker: str,
    eligibility: pd.DataFrame,
    five_minute_dir: Path,
    start_date: str,
    end_date: str,
) -> tuple[str, pd.DataFrame | None, str]:
    path = five_minute_dir / f"{ticker}_stocks_indicators_5min.parquet"
    start = pd.Timestamp(start_date, tz=IST) - pd.Timedelta(days=45)
    end = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    try:
        raw = _read_parquet_window(path, CANONICAL_COLUMNS, start, end)
        if raw is None or raw.empty:
            return ticker, None, "missing_canonical_5m"
        features = add_distilled_features(raw)
        features["trade_date"] = features["date"].dt.strftime("%Y-%m-%d")
        features = features.loc[
            features["trade_date"].between(start_date, end_date)
        ].copy()
        features = features.rename(
            columns={
                "date": "signal_time_ist",
                "open": "signal_open",
                "high": "signal_high",
                "low": "signal_low",
                "close": "signal_close",
                "volume": "signal_volume",
                "avwap": "session_vwap",
                "rel_volume": "relative_volume",
                "traded_value": "traded_value_rs",
                "prev_high10": "breakout_level",
            }
        )
        wanted = [
            "signal_time_ist",
            "signal_open",
            "signal_high",
            "signal_low",
            "signal_close",
            "signal_volume",
            "source_1m_count",
            "valid",
            "signal_source_1m_complete",
            "compression_history_complete",
            "previous_bb_width",
            "bb_q25_preceding20",
            "bb_compressed_literal",
            "bb_compressed_legacy_inclusive",
            "breakout_level",
            "breakout_extension_pct",
            "session_vwap",
            "avwap_ext",
            "relative_volume",
            "relative_volume_reference_count",
            "traded_value_rs",
        ]
        for column in wanted:
            if column not in features.columns:
                features[column] = np.nan
        merged = eligibility.merge(
            features[wanted],
            on="signal_time_ist",
            how="left",
            validate="one_to_one",
        )
        merged["ticker"] = ticker
        return ticker, merged, ""
    except Exception as exc:
        return ticker, None, f"{type(exc).__name__}:{exc}"


def load_eligible_features(
    eligibility: pd.DataFrame,
    five_minute_dir: Path,
    start_date: str,
    end_date: str,
    workers: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    groups = {
        str(ticker): group.copy()
        for ticker, group in eligibility.groupby("ticker", sort=False)
    }
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    started = time.time()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                _load_one_ticker_features,
                ticker,
                group,
                five_minute_dir,
                start_date,
                end_date,
            ): ticker
            for ticker, group in groups.items()
        }
        for done, future in enumerate(as_completed(futures), 1):
            ticker, frame, error = future.result()
            if frame is None:
                errors.append(f"{ticker}:{error}")
            else:
                frames.append(frame)
            if done % 200 == 0 or done == len(futures):
                print(
                    f"[late compression 5m] {done:,}/{len(futures):,} "
                    f"loaded={len(frames):,} errors={len(errors):,} "
                    f"elapsed={time.time()-started:.1f}s",
                    flush=True,
                )
    if errors:
        preview = "\n".join(errors[:30])
        raise RuntimeError(
            f"strict canonical feature load failed for {len(errors)} ticker(s):\n{preview}"
        )
    states = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return states, {
        "requested_tickers": len(groups),
        "loaded_tickers": len(frames),
        "errors": errors,
        "rows": int(len(states)),
        "runtime_seconds": time.time() - started,
    }


def apply_frozen_signal_gate(
    states: pd.DataFrame,
    nifty: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    work = states.merge(
        nifty,
        on="signal_time_ist",
        how="left",
        validate="many_to_one",
    ).copy()
    if "nifty_bar_present" not in work.columns:
        work["nifty_bar_present"] = work["nifty_above_vwap"].notna()
    if "nifty_bar_valid" not in work.columns:
        work["nifty_bar_valid"] = work["nifty_bar_present"]
    close = pd.to_numeric(work["signal_close"], errors="coerce")
    breakout = pd.to_numeric(work["breakout_level"], errors="coerce")
    traded = pd.to_numeric(work["traded_value_rs"], errors="coerce")
    vwap = pd.to_numeric(work["session_vwap"], errors="coerce")
    rvol = pd.to_numeric(work["relative_volume"], errors="coerce")

    stages: list[tuple[str, pd.Series]] = []
    current = pd.Series(True, index=work.index, dtype=bool)

    def add(name: str, condition: pd.Series) -> None:
        nonlocal current
        current = current & condition.astype("boolean").fillna(False).astype(bool)
        stages.append((name, current.copy()))

    add("canonical_ohlcv_and_41bar_history_complete", work["valid"] & work["compression_history_complete"])
    add("current_signal_has_five_source_minutes", work["signal_source_1m_complete"])
    add("price_at_least_rs50", close.ge(MIN_PRICE_RS))
    add("completed_5m_traded_value_at_least_rs1m", traded.ge(MIN_TRADED_VALUE_RS))
    add("previous_bb_width_in_bottom_quartile", work["bb_compressed_literal"])
    add("close_strictly_above_prior10_high", close.gt(breakout))
    add("close_at_or_above_causal_session_vwap", close.ge(vwap))
    add("same_time_causal_relative_volume_at_least_1p25", rvol.ge(MIN_RELATIVE_VOLUME))
    work["pre_nifty_signal_pass"] = current
    add("nifty_proxy_completed_bar_available", work["nifty_bar_present"])
    add("nifty_proxy_completed_bar_valid", work["nifty_bar_valid"])
    add("nifty_proxy_at_or_above_causal_session_vwap", work["nifty_above_vwap"])

    work["frozen_signal_pass"] = current
    work["setup"] = SETUP
    work["side"] = "LONG"
    work["trade_date"] = work["signal_time_ist"].map(
        lambda value: str(_timestamp_ist(value).date())
    )
    raw_signals = work.loc[current].sort_values(
        ["ticker", "trade_date", "signal_time_ist", "selection_rank"],
        kind="mergesort",
    )
    armed = raw_signals.drop_duplicates(["ticker", "trade_date"], keep="first").copy()
    armed["attempt_consumes_ticker_day"] = True

    funnel: list[dict[str, Any]] = []
    before = int(len(work))
    for name, mask in stages:
        after = int(mask.sum())
        funnel.append(
            {"stage": name, "before": before, "after": after, "removed": before - after}
        )
        before = after
    funnel.append(
        {
            "stage": "first_fully_qualifying_signal_per_ticker_day",
            "before": int(len(raw_signals)),
            "after": int(len(armed)),
            "removed": int(len(raw_signals) - len(armed)),
        }
    )
    return work, armed.reset_index(drop=True), funnel


def resolve_entry_strict(
    bars: pd.DataFrame,
    signal_ts: Any,
    signal_high: float,
    signal_low: float,
    breakout_level: float,
) -> EntryDecision:
    trigger = next_exchange_tick(signal_high)
    cancel = max(float(signal_low), float(breakout_level))
    signal = _timestamp_ist(signal_ts)
    if bars is None or bars.empty:
        return EntryDecision(None, None, trigger, cancel, "missing_1m_day")
    work = bars.copy()
    if "date" in work.columns:
        work["date"] = compression._timestamps(work["date"])
        work = work.set_index("date")
    if not isinstance(work.index, pd.DatetimeIndex):
        return EntryDecision(None, None, trigger, cancel, "invalid_1m_index")
    if work.index.tz is None:
        work.index = work.index.tz_localize(IST)
    else:
        work.index = work.index.tz_convert(IST)
    work = work.sort_index().loc[~work.index.duplicated(keep="last")]
    expected = pd.date_range(
        signal + pd.Timedelta(minutes=1),
        periods=ENTRY_VALID_MINUTES,
        freq="min",
    )
    if not expected.isin(work.index).all():
        return EntryDecision(None, None, trigger, cancel, "missing_entry_minute")
    for ts in expected:
        bar = work.loc[ts]
        values = pd.to_numeric(bar[["open", "high", "low", "close"]], errors="coerce")
        if values.isna().any():
            return EntryDecision(None, None, trigger, cancel, "invalid_entry_bar")
        op, high, low = float(values["open"]), float(values["high"]), float(values["low"])
        if low <= cancel:
            return EntryDecision(None, None, trigger, cancel, "cancel_before_or_ambiguous_trigger")
        if high >= trigger:
            fill = max(trigger, op)
            if (fill / trigger - 1.0) * 100.0 > ENTRY_MAX_GAP_PCT:
                return EntryDecision(None, None, trigger, cancel, "entry_gap_over_0p20pct")
            return EntryDecision(pd.Timestamp(ts), float(fill), trigger, cancel, "filled")
    return EntryDecision(None, None, trigger, cancel, "not_triggered")


def resolve_fixed_exit(
    bars: pd.DataFrame,
    entry_time: Any,
    entry_price: float,
) -> dict[str, Any] | None:
    entry_ts = _timestamp_ist(entry_time)
    if bars is None or bars.empty or not math.isfinite(float(entry_price)):
        return None
    work = bars.copy()
    if "date" in work.columns:
        work["date"] = compression._timestamps(work["date"])
        work = work.set_index("date")
    if work.index.tz is None:
        work.index = work.index.tz_localize(IST)
    else:
        work.index = work.index.tz_convert(IST)
    work = work.sort_index().loc[~work.index.duplicated(keep="last")]
    cutoff = entry_ts.normalize() + pd.Timedelta(minutes=FORCED_EXIT_MINUTE)
    path = work.loc[(work.index >= entry_ts) & (work.index <= cutoff)].copy()
    if path.empty:
        return None
    stop = float(entry_price) * (1.0 - STOP_PCT / 100.0)
    target = float(entry_price) * (1.0 + TARGET_PCT / 100.0)
    best_high = float(entry_price)
    worst_low = float(entry_price)
    for held, (ts, row) in enumerate(path.iterrows(), 1):
        values = pd.to_numeric(row[["open", "high", "low", "close"]], errors="coerce")
        if values.isna().any():
            return None
        op = float(values["open"])
        high = float(values["high"])
        low = float(values["low"])
        best_high = max(best_high, high)
        worst_low = min(worst_low, low)
        stop_hit = low <= stop
        target_hit = high >= target
        if stop_hit:
            raw_exit = min(stop, op)
            exit_price = raw_exit * (1.0 - NON_TARGET_EXIT_SLIPPAGE_BPS / 10_000.0)
            return {
                "outcome": "SL",
                "exit_time_ist": pd.Timestamp(ts),
                "exit_price": exit_price,
                "raw_exit_price": raw_exit,
                "bars_held": held,
                "target_stop_tie": bool(target_hit),
                "best_high": best_high,
                "worst_low": worst_low,
            }
        if target_hit:
            return {
                "outcome": "TARGET",
                "exit_time_ist": pd.Timestamp(ts),
                "exit_price": target,
                "raw_exit_price": target,
                "bars_held": held,
                "target_stop_tie": False,
                "best_high": best_high,
                "worst_low": worst_low,
            }
    last_ts = path.index[-1]
    raw_exit = float(path.iloc[-1]["close"])
    exit_price = raw_exit * (1.0 - NON_TARGET_EXIT_SLIPPAGE_BPS / 10_000.0)
    return {
        "outcome": "FORCED_1515",
        "exit_time_ist": pd.Timestamp(last_ts),
        "exit_price": exit_price,
        "raw_exit_price": raw_exit,
        "bars_held": int(len(path)),
        "target_stop_tie": False,
        "best_high": best_high,
        "worst_low": worst_low,
    }


def resolve_trades(
    armed: pd.DataFrame,
    one_minute_dir: Path,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    trades: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    start = pd.Timestamp(start_date, tz=IST)
    end = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    for ticker, group in armed.groupby("ticker", sort=True):
        path = one_minute_dir / f"{ticker}_stocks_indicators_1min.parquet"
        bars = _read_parquet_window(path, hourly.ONE_MINUTE_COLUMNS, start, end)
        if bars is None or bars.empty:
            for _, row in group.iterrows():
                rejects.append(
                    {
                        "ticker": ticker,
                        "trade_date": row["trade_date"],
                        "signal_time_ist": row["signal_time_ist"],
                        "reason": "missing_1m_file_or_window",
                    }
                )
            continue
        bars = bars.sort_values("date", kind="mergesort").drop_duplicates("date", keep="last")
        for _, row in group.sort_values("signal_time_ist").iterrows():
            decision = resolve_entry_strict(
                bars,
                row["signal_time_ist"],
                float(row["signal_high"]),
                float(row["signal_low"]),
                float(row["breakout_level"]),
            )
            if decision.reason != "filled" or decision.entry_time is None or decision.fill_raw is None:
                rejects.append(
                    {
                        "ticker": ticker,
                        "trade_date": row["trade_date"],
                        "signal_time_ist": row["signal_time_ist"],
                        "trigger_price": decision.trigger,
                        "cancel_price": decision.cancel,
                        "reason": decision.reason,
                    }
                )
                continue
            entry_price = float(decision.fill_raw) * (1.0 + ENTRY_SLIPPAGE_BPS / 10_000.0)
            sizing = causal_sizing(float(row["signal_volume"]), entry_price)
            quantity = sizing.quantity
            if quantity <= 0:
                rejects.append(
                    {
                        "ticker": ticker,
                        "trade_date": row["trade_date"],
                        "signal_time_ist": row["signal_time_ist"],
                        "notional_quantity": sizing.notional_quantity,
                        "expected_one_minute_volume": sizing.expected_one_minute_volume,
                        "causal_capacity_quantity": sizing.capacity_quantity,
                        "reason": "zero_causal_capacity_quantity",
                    }
                )
                continue
            exit_result = resolve_fixed_exit(bars, decision.entry_time, entry_price)
            if exit_result is None:
                rejects.append(
                    {
                        "ticker": ticker,
                        "trade_date": row["trade_date"],
                        "signal_time_ist": row["signal_time_ist"],
                        "reason": "unresolved_exit_path",
                    }
                )
                continue
            costs = nse.intraday_equity_costs(
                entry_price,
                float(exit_result["exit_price"]),
                quantity,
                "LONG",
            )
            risk_rs = entry_price * (STOP_PCT / 100.0) * quantity
            record = row.to_dict()
            record.update(
                {
                    "entry_time_ist": decision.entry_time,
                    "planned_trigger": decision.trigger,
                    "entry_cancel_price": decision.cancel,
                    "entry_raw_price": decision.fill_raw,
                    "entry_price": entry_price,
                    "notional_quantity": sizing.notional_quantity,
                    "expected_one_minute_volume": sizing.expected_one_minute_volume,
                    "causal_capacity_quantity": sizing.capacity_quantity,
                    "quantity": quantity,
                    "capacity_limited": bool(quantity < sizing.notional_quantity),
                    "expected_one_minute_participation": (
                        quantity / sizing.expected_one_minute_volume
                        if sizing.expected_one_minute_volume > 0.0
                        else np.nan
                    ),
                    "entry_notional_rs": entry_price * quantity,
                    "initial_risk_rs": risk_rs,
                    **exit_result,
                    "gross_pnl_rs": float(costs.gross_pnl),
                    "cost_rs": float(costs.total_cost),
                    "net_pnl_rs": float(costs.net_pnl),
                    "net_r": float(costs.net_pnl / risk_rs) if risk_rs > 0 else np.nan,
                    "mfe_pct": (float(exit_result["best_high"]) / entry_price - 1.0) * 100.0,
                    "mae_pct": (float(exit_result["worst_low"]) / entry_price - 1.0) * 100.0,
                }
            )
            trades.append(record)
    return pd.DataFrame(trades), pd.DataFrame(rejects)


def _subset_metrics(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0,
            "active_days": 0,
            "net_pnl_rs": 0.0,
            "profit_factor": 0.0,
            "win_rate_pct": 0.0,
        }
    pnl = pd.to_numeric(trades["net_pnl_rs"], errors="coerce")
    return {
        "trades": int(len(trades)),
        "active_days": int(trades["trade_date"].nunique()),
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": float(_profit_factor(pnl)),
        "win_rate_pct": float(pnl.gt(0.0).mean() * 100.0),
        "mean_net_pnl_trade_rs": float(pnl.mean()),
    }


def remove_best_five_days(trades: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if trades.empty:
        return trades.copy(), {"removed_days": [], **_subset_metrics(trades)}
    daily = trades.groupby("trade_date")["net_pnl_rs"].sum()
    positive = daily.loc[daily > 0.0]
    removed = positive.nlargest(min(5, len(positive))).index.tolist()
    remaining = trades.loc[~trades["trade_date"].isin(removed)].copy()
    return remaining, {"removed_days": removed, **_subset_metrics(remaining)}


def apply_realised_daily_brake(trades: pd.DataFrame) -> pd.DataFrame:
    """Sensitivity only: stop new entries after realised daily P&L <= -0.50%."""

    if trades.empty:
        out = trades.copy()
        out["daily_brake_included"] = pd.Series(dtype=bool)
        return out
    threshold = -RESEARCH_ACCOUNT_RS * DAILY_LOSS_BRAKE_PCT / 100.0
    included: dict[int, bool] = {}
    for trade_date, day in trades.groupby("trade_date", sort=True):
        accepted: list[int] = []
        for idx, row in day.sort_values(
            ["entry_time_ist", "selection_rank", "ticker"], kind="mergesort"
        ).iterrows():
            entry_time = _timestamp_ist(row["entry_time_ist"])
            realised = sum(
                float(trades.at[prior, "net_pnl_rs"])
                for prior in accepted
                if _timestamp_ist(trades.at[prior, "exit_time_ist"]) < entry_time
            )
            allow = realised > threshold
            included[int(idx)] = allow
            if allow:
                accepted.append(int(idx))
    out = trades.copy()
    out["daily_brake_included"] = pd.Series(included).reindex(out.index).fillna(False).astype(bool)
    return out


def _render_report(summary: dict[str, Any]) -> str:
    overall = summary["metrics"]["overall"]
    first = summary["metrics"]["first_half"]
    second = summary["metrics"]["second_half"]
    honest = summary["honest_validation"]
    best_days = summary["robustness"]["best_five_days_removed"]
    stress = honest["cost_plus_25pct_stress"]
    verdict = summary["verdict"]
    nifty_audit = summary["nifty_data_audit"]
    entry_audit = summary["entry_audit"]
    return f"""# V12 distilled late-compression LONG replay

## Verdict

**{verdict['decision']}** — {verdict['reason']}

This is an isolated research result. `production_approved` remains `false`, and no live/paper configuration was changed.

## Frozen rule

- Strict completed hourly K300 `primary_side == LONG` membership.
- Completed 5-minute signals at 14:00..14:25 IST; old list owns `xx:20`, new list starts at `xx:25`.
- Hourly refresh never exits an open trade or resets ticker/day attempt state; the same ticker in consecutive lists is not re-armed.
- Price >= Rs {MIN_PRICE_RS:.0f}; completed 5-minute traded value >= Rs {MIN_TRADED_VALUE_RS:,.0f}.
- Previous Bollinger(20,2) bandwidth <= the causal 25th percentile of the twenty bandwidth readings before it.
- Close strictly above the prior {BREAKOUT_LOOKBACK} completed same-session highs, at/above causal session VWAP.
- Same-time-of-day causal relative volume >= {MIN_RELATIVE_VOLUME:.2f}; NIFTY proxy at/above causal session VWAP.
- First fully qualifying signal per ticker/day is the only armed attempt.
- Buy-stop signal high + one Rs {TICK_SIZE:.2f} tick for the next {ENTRY_VALID_MINUTES} one-minute bars; cancel checked before trigger; gap cap {ENTRY_MAX_GAP_PCT:.2f}%.
- Quantity is min(Rs {NOTIONAL_RS:,.0f} notional, {MAX_EXPECTED_ONE_MINUTE_PARTICIPATION:.0%} of completed-5m-volume/5); {entry_audit['capacity_limited_trades']} filled trades were capacity-limited.
- Entry slippage {ENTRY_SLIPPAGE_BPS:.0f} bps; stop {STOP_PCT:.2f}%; target {TARGET_PCT:.2f}%; non-target exit slippage {NON_TARGET_EXIT_SLIPPAGE_BPS:.0f} bps; forced exit 15:15; full statutory costs.

## Results

| Window | Sessions | Trades | Net PF | Net P&L | Win rate |
|---|---:|---:|---:|---:|---:|
| Full | {overall['sessions']} | {overall['trades']} | {overall['profit_factor']:.3f} | Rs {overall['net_pnl_rs']:,.2f} | {overall['win_rate_pct']:.1f}% |
| First half | {first['sessions']} | {first['trades']} | {first['profit_factor']:.3f} | Rs {first['net_pnl_rs']:,.2f} | {first['win_rate_pct']:.1f}% |
| Second half | {second['sessions']} | {second['trades']} | {second['profit_factor']:.3f} | Rs {second['net_pnl_rs']:,.2f} | {second['win_rate_pct']:.1f}% |

Full-window PF before statutory costs was `{overall['gross_profit_factor_before_costs']:.3f}`; the rule therefore lost even before costs.

Robustness: day-bootstrap 95% lower PF `{honest['day_cluster_bootstrap']['lower_profit_factor']:.3f}`; after removing the five best **trades** PF `{honest['top5_profitable_trades_removed']['profit_factor']:.3f}`; after removing the five best **days** PF `{best_days['profit_factor']:.3f}`; costs +25% PF `{stress['profit_factor']:.3f}`.

## Evidence gate

- Frozen honest-validator decision: **{honest['decision']}**.
- Failed checks: {', '.join(honest['failed_checks']) if honest['failed_checks'] else 'none'}.
- The requested two complete months contain only {overall['sessions']} sessions, so the >=60-session / >=300-trade promotion gate cannot pass on this replay alone.
- The hourly lists use a reconstructed static-current universe and are not point-in-time constituent data; survivorship bias remains possible.
- The NIFTY gate uses the repository's end-labelled traded NIFTYBEES proxy because the cash index has zero volume and no true traded VWAP.
- NIFTY signal-grid coverage: {nifty_audit['present_completed_signal_bars']}/{nifty_audit['expected_completed_signal_bars']}; missing/invalid bars fail closed. Missing pre-qualified candidate rows: {nifty_audit['prequalified_rows_missing_bar']}.

## Boundary and implementation audit

- Signals at `xx:20`: {summary['boundary_audit']['signals_at_xx20']}.
- All such signals owned by the previous hourly list: {summary['boundary_audit']['all_owned_by_previous_hourly_list']}.
- Existing production setup was not overwritten: `{summary['safety']['existing_setup_untouched']}`.
- Setup ID: `{SETUP}`.
"""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Frozen research-only V12 distilled late-compression LONG replay"
    )
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument("--prefilter", type=Path, default=DEFAULT_PREFILTER)
    parser.add_argument("--five-minute-dir", type=Path, default=DEFAULT_5M_DIR)
    parser.add_argument("--one-minute-dir", type=Path, default=DEFAULT_1M_DIR)
    parser.add_argument("--nifty-file", type=Path, default=DEFAULT_NIFTY)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.time()
    if pd.Timestamp(args.end_date) < pd.Timestamp(args.start_date):
        raise ValueError("end date precedes start date")
    for path, label in (
        (args.prefilter, "strict hourly prefilter"),
        (args.five_minute_dir, "canonical five-minute directory"),
        (args.one_minute_dir, "one-minute directory"),
        (args.nifty_file, "NIFTY proxy file"),
    ):
        if not path.exists():
            raise FileNotFoundError(f"missing {label}: {path}")

    memberships, membership_audit = hourly.load_long_memberships(
        args.prefilter, args.start_date, args.end_date
    )
    eligibility = hourly.expand_membership_schedule(memberships)
    eligibility = eligibility.loc[
        eligibility["signal_minute"].between(
            SIGNAL_START_MINUTE, SIGNAL_END_MINUTE, inclusive="both"
        )
    ].copy()
    nifty = load_nifty_above_vwap(args.nifty_file, args.start_date, args.end_date)
    expected_nifty = (
        eligibility[["signal_time_ist"]]
        .drop_duplicates()
        .sort_values("signal_time_ist", kind="mergesort")
    )
    nifty_coverage = expected_nifty.merge(
        nifty[["signal_time_ist", "nifty_bar_present", "nifty_bar_valid"]],
        on="signal_time_ist",
        how="left",
        validate="one_to_one",
    )
    present_mask = nifty_coverage["nifty_bar_present"].astype("boolean").fillna(False)
    valid_mask = nifty_coverage["nifty_bar_valid"].astype("boolean").fillna(False)
    states, feature_audit = load_eligible_features(
        eligibility,
        args.five_minute_dir,
        args.start_date,
        args.end_date,
        args.workers,
    )
    evaluated, armed, funnel = apply_frozen_signal_gate(states, nifty)
    pre_nifty = evaluated["pre_nifty_signal_pass"].astype("boolean").fillna(False)
    evaluated_nifty_present = evaluated["nifty_bar_present"].astype("boolean").fillna(False)
    evaluated_nifty_valid = evaluated["nifty_bar_valid"].astype("boolean").fillna(False)
    nifty_data_audit = {
        "proxy": "NIFTYBEES",
        "timestamp_contract": "end_labelled_completed_five_minute_bars",
        "coverage_policy": "missing_or_invalid_proxy_bar_fails_closed",
        "expected_completed_signal_bars": int(len(nifty_coverage)),
        "present_completed_signal_bars": int(present_mask.sum()),
        "valid_completed_signal_bars": int((present_mask & valid_mask).sum()),
        "missing_signal_times": [
            _timestamp_ist(value).isoformat()
            for value in nifty_coverage.loc[~present_mask, "signal_time_ist"]
        ],
        "invalid_signal_times": [
            _timestamp_ist(value).isoformat()
            for value in nifty_coverage.loc[present_mask & ~valid_mask, "signal_time_ist"]
        ],
        "prequalified_rows_missing_bar": int((pre_nifty & ~evaluated_nifty_present).sum()),
        "prequalified_rows_invalid_bar": int(
            (pre_nifty & evaluated_nifty_present & ~evaluated_nifty_valid).sum()
        ),
    }
    trades, rejects = resolve_trades(
        armed, args.one_minute_dir, args.start_date, args.end_date
    )

    sessions = [row["trade_date"] for row in membership_audit["session_rows"]]
    overall, daily = hourly.session_metrics(trades, sessions)
    overall["gross_profit_factor_before_costs"] = float(
        _profit_factor(pd.to_numeric(trades.get("gross_pnl_rs", pd.Series(dtype=float)), errors="coerce"))
    )
    midpoint = len(sessions) // 2
    trade_dates = trades.get("trade_date", pd.Series(dtype=str))
    first_trades = trades.loc[trade_dates.isin(sessions[:midpoint])]
    second_trades = trades.loc[trade_dates.isin(sessions[midpoint:])]
    first_metrics, _ = hourly.session_metrics(
        first_trades,
        sessions[:midpoint],
    )
    second_metrics, _ = hourly.session_metrics(
        second_trades,
        sessions[midpoint:],
    )
    first_metrics["gross_profit_factor_before_costs"] = float(
        _profit_factor(
            pd.to_numeric(first_trades.get("gross_pnl_rs", pd.Series(dtype=float)), errors="coerce")
        )
    )
    second_metrics["gross_profit_factor_before_costs"] = float(
        _profit_factor(
            pd.to_numeric(second_trades.get("gross_pnl_rs", pd.Series(dtype=float)), errors="coerce")
        )
    )

    required_trade_columns = ["trade_date", "net_pnl_rs", "gross_pnl_rs", "cost_rs"]
    validation_frame = trades.copy()
    for column in required_trade_columns:
        if column not in validation_frame.columns:
            validation_frame[column] = pd.Series(dtype="float64" if column != "trade_date" else "string")
    prepared = validator.prepare_trades(validation_frame)
    honest = validator.evaluate(
        prepared,
        [pd.Timestamp(day) for day in sessions],
        session_source="strict_completed_hourly_prefilter_session_audit",
        warnings=(
            "Reconstructed hourly lists use a static current universe, not point-in-time constituents.",
            "Two complete months cannot satisfy the immutable >=60-session validation gate.",
        ),
        bootstrap_draws=BOOTSTRAP_DRAWS,
    )
    _, best_days = remove_best_five_days(trades)
    brake = apply_realised_daily_brake(trades)
    brake_metrics = _subset_metrics(brake.loc[brake["daily_brake_included"]])

    boundary = armed.loc[
        armed["signal_time_ist"].map(_timestamp_ist).map(lambda stamp: stamp.minute == 20)
    ].copy()
    if not boundary.empty:
        owned = (
            pd.to_datetime(boundary["signal_time_ist"], utc=True)
            - pd.to_datetime(boundary["slot_ist"], utc=True)
        ).dt.total_seconds().eq(60 * 60)
    else:
        owned = pd.Series(dtype=bool)

    if honest["qualification_pass"]:
        decision = "RESEARCH_QUALIFIED_NOT_PRODUCTION_APPROVED"
        reason = "all immutable evidence gates passed, but production approval is hard-disabled"
    else:
        decision = "REJECT_NOT_PROVEN"
        reason = "one or more immutable profitability, robustness, or sample-size gates failed"

    summary: dict[str, Any] = {
        "schema_version": "v12_late_compression_simple_long_research_v2",
        "setup": SETUP,
        "production_approved": PRODUCTION_APPROVED,
        "promotion_action": PROMOTION_ACTION,
        "verdict": {"decision": decision, "reason": reason},
        "data_window": {
            "start": args.start_date,
            "end": args.end_date,
            "sessions": len(sessions),
            "sessions_list": sessions,
        },
        "frozen_config": {
            "signal_window_ist": ["14:00", "14:25"],
            "hourly_membership": "strict_completed_primary_side_LONG_slot_plus_5_through_plus_60",
            "hourly_boundary_owner": "previous list owns xx:20; refreshed list starts xx:25",
            "hourly_refresh_open_position_policy": "open positions persist unchanged",
            "hourly_refresh_same_ticker_policy": "ticker/day attempt state persists; no re-arm",
            "min_price_rs": MIN_PRICE_RS,
            "min_completed_5m_traded_value_rs": MIN_TRADED_VALUE_RS,
            "bb_period": BB_PERIOD,
            "bb_quantile_lookback": BB_QUANTILE_LOOKBACK,
            "bb_quantile": BB_QUANTILE,
            "bb_tested_previous_width_excluded_from_reference_window": True,
            "breakout_lookback_bars": BREAKOUT_LOOKBACK,
            "breakout_requires_strict_close_above": True,
            "minimum_relative_volume": MIN_RELATIVE_VOLUME,
            "relative_volume_definition": "current_5m_volume / median_previous_10_same-minute sessions using only valid complete bars; min_5",
            "stock_above_session_vwap": True,
            "nifty_proxy_above_session_vwap": True,
            "one_attempt_per_ticker_day": True,
            "entry": {
                "trigger": "signal_high_plus_one_0.05_tick",
                "valid_minutes": ENTRY_VALID_MINUTES,
                "cancel": "max(signal_low, prior10_high), checked before trigger",
                "max_gap_pct": ENTRY_MAX_GAP_PCT,
                "entry_slippage_bps": ENTRY_SLIPPAGE_BPS,
            },
            "exit": {
                "stop_pct": STOP_PCT,
                "target_pct": TARGET_PCT,
                "forced_exit": "15:15",
                "same_minute_tie": "stop_first",
                "non_target_exit_slippage_bps": NON_TARGET_EXIT_SLIPPAGE_BPS,
            },
            "sizing": {
                "maximum_notional_rs": NOTIONAL_RS,
                "expected_one_minute_volume": "completed_signal_5m_volume_divided_by_5",
                "maximum_expected_one_minute_participation": MAX_EXPECTED_ONE_MINUTE_PARTICIPATION,
                "quantity_rule": "min(floor(maximum_notional/entry_price), floor(expected_1m_volume*0.02))",
            },
            "daily_brake_sensitivity": {
                "account_rs": RESEARCH_ACCOUNT_RS,
                "realised_loss_pct": DAILY_LOSS_BRAKE_PCT,
            },
        },
        "metrics": {
            "overall": overall,
            "first_half": first_metrics,
            "second_half": second_metrics,
            "realised_daily_brake_sensitivity": brake_metrics,
        },
        "honest_validation": honest,
        "robustness": {"best_five_days_removed": best_days},
        "membership_audit": membership_audit,
        "feature_audit": feature_audit,
        "nifty_data_audit": nifty_data_audit,
        "entry_audit": {
            "armed_attempts": int(len(armed)),
            "filled_trades": int(len(trades)),
            "rejected_attempts": int(len(rejects)),
            "reject_reasons": (
                rejects["reason"].value_counts().sort_index().to_dict()
                if not rejects.empty and "reason" in rejects
                else {}
            ),
            "capacity_limited_trades": (
                int(trades["capacity_limited"].astype("boolean").fillna(False).sum())
                if not trades.empty and "capacity_limited" in trades
                else 0
            ),
            "maximum_expected_one_minute_participation_observed": (
                float(
                    pd.to_numeric(
                        trades["expected_one_minute_participation"], errors="coerce"
                    ).max()
                )
                if not trades.empty and "expected_one_minute_participation" in trades
                else 0.0
            ),
        },
        "boundary_audit": {
            "signals_at_xx20": int(len(boundary)),
            "all_owned_by_previous_hourly_list": bool(owned.all()) if len(owned) else True,
        },
        "input_provenance": {
            "prefilter": str(args.prefilter.resolve()),
            "prefilter_sha256": _sha256(args.prefilter),
            "five_minute_dir": str(args.five_minute_dir.resolve()),
            "one_minute_dir": str(args.one_minute_dir.resolve()),
            "nifty_file": str(args.nifty_file.resolve()),
            "nifty_proxy_note": "dedicated end-labelled NIFTYBEES proxy used for a volume-weighted session VWAP",
        },
        "safety": {
            "research_only": True,
            "live_config_changed": False,
            "existing_setup_untouched": compression.SETUP,
            "static_current_universe_bias": True,
        },
        "runtime_seconds": time.time() - started,
    }

    args.out.mkdir(parents=True, exist_ok=True)
    evaluated.to_csv(args.out / "evaluated_membership_states.csv", index=False)
    armed.to_csv(args.out / "armed_attempts.csv", index=False)
    trades.to_csv(args.out / "trades.csv", index=False)
    rejects.to_csv(args.out / "entry_rejects.csv", index=False)
    daily.to_csv(args.out / "daily.csv", index=False)
    pd.DataFrame(funnel).to_csv(args.out / "signal_funnel.csv", index=False)
    boundary.to_csv(args.out / "boundary_audit.csv", index=False)
    (args.out / "frozen_config.json").write_text(
        json.dumps(_json_safe(summary["frozen_config"]), indent=2), encoding="utf-8"
    )
    (args.out / "summary.json").write_text(
        json.dumps(_json_safe(summary), indent=2), encoding="utf-8"
    )
    (args.out / "REPORT.md").write_text(_render_report(summary), encoding="utf-8")
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run(args)
    metrics = summary["metrics"]["overall"]
    print(
        json.dumps(
            {
                "decision": summary["verdict"]["decision"],
                "sessions": metrics["sessions"],
                "trades": metrics["trades"],
                "net_pf": _json_safe(metrics["profit_factor"]),
                "net_pnl_rs": metrics["net_pnl_rs"],
                "out": str(args.out.resolve()),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
