"""Causal, feature-only market context for the EQID V7/V11/V12 pipelines.

The engine consumes completed 5-minute OHLCV bars and produces two small
tables:

* ``market``: one market-wide feature row per completed bar;
* ``sectors``: one row per completed bar and sector.

It deliberately has no order, side, entry, stop, target, sizing, or trade
selection API.  Candidate rows can be enriched with :func:`attach_context_asof`,
which performs a backward point-in-time join on ``available_at``.

Input timestamps are assumed to be bar-end timestamps.  For the repository's
hybrid 5-minute files, rows explicitly marked ``opening_snapshot=True`` are
excluded because the 09:15 row is not a completed candle.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
import warnings

import numpy as np
import pandas as pd


FEATURE_VERSION = "eqidv2_market_context_v1"
IST = "Asia/Kolkata"

# A feature engine must never acquire execution semantics.  This is also
# checked on every result returned by ``compute``.
FORBIDDEN_OUTPUT_COLUMNS = frozenset(
    {
        "side",
        "signal",
        "trade",
        "entry",
        "entry_price",
        "exit",
        "exit_price",
        "stop",
        "stop_loss",
        "sl_price",
        "target",
        "target_price",
        "quantity",
        "position",
        "order",
        "order_type",
    }
)


@dataclass(frozen=True)
class MarketContextConfig:
    """Configuration whose defaults match the EQID 5-minute stores."""

    timestamp_col: str = "date"
    ticker_col: str = "ticker"
    sector_col: str = "sector"
    timezone: str = IST
    bar_minutes: int = 5
    session_open_hhmm: tuple[int, int] = (9, 15)
    session_close_hhmm: tuple[int, int] = (15, 30)
    publish_delay_seconds: int = 0

    ema_fast_span: int = 20
    ema_slow_span: int = 50
    atr_span: int = 14
    trend_short_bars: int = 3
    trend_long_bars: int = 12
    trend_volatility_bars: int = 24
    sector_momentum_bars: int = 6
    rotation_lookback_bars: int = 3

    relative_volume_sessions: int = 20
    relative_volume_min_sessions: int = 5
    regime_baseline_sessions: int = 60
    regime_min_sessions: int = 10

    min_sector_members: int = 5
    expected_universe_size: int | None = None
    min_market_coverage: float = 0.70
    min_sector_coverage: float = 0.70
    max_context_staleness_minutes: int = 7

    trend_regime_threshold: float = 20.0
    volatility_z_threshold: float = 0.75
    rotation_score_threshold: float = 60.0
    broad_trend_breadth_threshold: float = 0.35

    index_aliases: Mapping[str, tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        positive = {
            "bar_minutes": self.bar_minutes,
            "ema_fast_span": self.ema_fast_span,
            "ema_slow_span": self.ema_slow_span,
            "atr_span": self.atr_span,
            "trend_short_bars": self.trend_short_bars,
            "trend_long_bars": self.trend_long_bars,
            "trend_volatility_bars": self.trend_volatility_bars,
            "sector_momentum_bars": self.sector_momentum_bars,
            "rotation_lookback_bars": self.rotation_lookback_bars,
            "relative_volume_sessions": self.relative_volume_sessions,
            "relative_volume_min_sessions": self.relative_volume_min_sessions,
            "regime_baseline_sessions": self.regime_baseline_sessions,
            "regime_min_sessions": self.regime_min_sessions,
            "min_sector_members": self.min_sector_members,
            "max_context_staleness_minutes": self.max_context_staleness_minutes,
        }
        invalid = [name for name, value in positive.items() if int(value) <= 0]
        if invalid:
            raise ValueError(f"configuration values must be positive: {invalid}")
        if float(self.publish_delay_seconds) < 0:
            raise ValueError("publish_delay_seconds must be non-negative")
        open_minute = self.session_open_hhmm[0] * 60 + self.session_open_hhmm[1]
        close_minute = self.session_close_hhmm[0] * 60 + self.session_close_hhmm[1]
        if not (0 <= open_minute < close_minute < 24 * 60):
            raise ValueError("session_open_hhmm/session_close_hhmm are invalid")
        if self.expected_universe_size is not None and int(self.expected_universe_size) <= 0:
            raise ValueError("expected_universe_size must be positive when provided")
        if self.relative_volume_min_sessions > self.relative_volume_sessions:
            raise ValueError(
                "relative_volume_min_sessions cannot exceed relative_volume_sessions"
            )
        if self.regime_min_sessions > self.regime_baseline_sessions:
            raise ValueError("regime_min_sessions cannot exceed regime_baseline_sessions")
        for name, value in (
            ("min_market_coverage", self.min_market_coverage),
            ("min_sector_coverage", self.min_sector_coverage),
        ):
            if not 0.0 <= float(value) <= 1.0:
                raise ValueError(f"{name} must be in [0, 1]")

    def aliases(self) -> Mapping[str, tuple[str, ...]]:
        if self.index_aliases is not None:
            return self.index_aliases
        return {
            "nifty": (
                "NIFTY50_INDEX",
                "NIFTY50",
                "NIFTY_50",
                "NIFTY",
                "NIFTYBEES",
            ),
            "bank_nifty": (
                "BANKNIFTY_INDEX",
                "BANKNIFTY",
                "NIFTYBANK",
                "NIFTY_BANK",
                "BANKBEES",
            ),
            "midcap": (
                "NIFTYMIDCAP150_INDEX",
                "NIFTYMIDCAP150",
                "NIFTYMIDCAP100",
                "NIFTYMIDCAP50",
                "NIFTY_MIDCAP_150",
                "MIDCAPETF",
            ),
        }


@dataclass(frozen=True)
class MarketContextResult:
    market: pd.DataFrame
    sectors: pd.DataFrame


def load_sector_map(path: str | Path) -> dict[str, str]:
    """Load the repository's ``sector_etf_map.json`` stock membership.

    If a ticker occurs in more than one sector, the later (usually more
    specific) declaration wins and a warning is emitted.  For production
    research this file should be replaced by point-in-time membership.
    """

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    sectors = payload.get("sectors", payload)
    mapping: dict[str, str] = {}
    duplicates: list[str] = []
    for sector, definition in sectors.items():
        stocks = definition.get("stocks", []) if isinstance(definition, dict) else definition
        for ticker in stocks:
            key = str(ticker).strip().upper()
            if not key:
                continue
            if key in mapping and mapping[key] != str(sector):
                duplicates.append(key)
            mapping[key] = str(sector).strip().upper()
    if duplicates:
        warnings.warn(
            "duplicate sector membership; later declaration used for "
            + ", ".join(sorted(set(duplicates))),
            RuntimeWarning,
            stacklevel=2,
        )
    return mapping


def _to_timezone(values: pd.Series, timezone: str) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(timezone, ambiguous="NaT", nonexistent="shift_forward")
    return parsed.dt.tz_convert(timezone)


def _normalise_symbol(value: Any) -> str:
    return "".join(ch for ch in str(value).upper().strip() if ch.isalnum())


def _finite_weighted_average(
    values: Sequence[np.ndarray | pd.Series],
    weights: Sequence[float],
) -> np.ndarray:
    matrix = np.column_stack([np.asarray(v, dtype="float64") for v in values])
    weight_array = np.asarray(weights, dtype="float64")
    valid = np.isfinite(matrix)
    numerator = np.nansum(matrix * weight_array, axis=1)
    denominator = np.sum(valid * weight_array, axis=1)
    return np.divide(
        numerator,
        denominator,
        out=np.full(len(matrix), np.nan, dtype="float64"),
        where=denominator > 0,
    )


def _safe_cross_section_z(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    mean = numeric.mean()
    std = numeric.std(ddof=0)
    if not np.isfinite(std) or std <= 1e-12:
        return pd.Series(0.0, index=values.index).where(numeric.notna())
    return (numeric - mean) / std


def _causal_same_slot_zscore(
    values: pd.Series,
    slots: pd.Series,
    window: int,
    min_periods: int,
) -> pd.Series:
    """Z-score against prior sessions at the same time-of-day only."""

    current = pd.to_numeric(values, errors="coerce").astype("float64")
    prior = current.groupby(slots, sort=False, observed=True).shift(1)
    mean = prior.groupby(slots, sort=False, observed=True).transform(
        lambda s: s.rolling(window, min_periods=min_periods).mean()
    )
    std = prior.groupby(slots, sort=False, observed=True).transform(
        lambda s: s.rolling(window, min_periods=min_periods).std(ddof=0)
    )
    z = (current - mean).div(std.replace(0.0, np.nan))
    # Insufficient history and a zero-variance baseline are unavailable, not
    # neutral.  Keeping them missing lets downstream composites reweight only
    # over genuinely observed components.
    return z.replace([np.inf, -np.inf], np.nan).where(current.notna())


def _causal_same_slot_prior_count(values: pd.Series, slots: pd.Series) -> pd.Series:
    valid = pd.to_numeric(values, errors="coerce").notna().astype("int32")
    return valid.groupby(slots, sort=False, observed=True).cumsum() - valid


def _session_fields(frame: pd.DataFrame, config: MarketContextConfig) -> pd.DataFrame:
    out = frame.copy()
    ts = out["_timestamp"]
    out["_session"] = ts.dt.strftime("%Y-%m-%d")
    minute = ts.dt.hour * 60 + ts.dt.minute
    open_minute = config.session_open_hhmm[0] * 60 + config.session_open_hhmm[1]
    out["_bar_slot"] = ((minute - open_minute) // config.bar_minutes).astype("int32")
    return out


def _normalise_bars(
    bars: pd.DataFrame,
    config: MarketContextConfig,
    *,
    require_ticker: bool = True,
) -> pd.DataFrame:
    if bars is None or bars.empty:
        return pd.DataFrame()
    required = {
        config.timestamp_col,
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    if require_ticker:
        required.add(config.ticker_col)
    missing = required - set(bars.columns)
    if missing:
        raise ValueError(f"bars missing required columns: {sorted(missing)}")

    out = bars.copy()
    out["_timestamp"] = _to_timezone(out[config.timestamp_col], config.timezone)
    if require_ticker:
        out["_ticker"] = out[config.ticker_col].astype("string").str.upper().str.strip()
        out["_ticker"] = out["_ticker"].mask(
            out["_ticker"].isin({"", "<NA>", "NAN", "NONE", "NULL"})
        )
    for column in ("open", "high", "low", "close", "volume"):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    required_observations = ["_timestamp", "close"]
    if require_ticker:
        required_observations.append("_ticker")
    out = out.dropna(subset=required_observations)
    if require_ticker:
        out = out.sort_values(["_ticker", "_timestamp"])
        out = out.drop_duplicates(["_ticker", "_timestamp"], keep="last")
    else:
        out = out.sort_values("_timestamp").drop_duplicates("_timestamp", keep="last")
    minute = out["_timestamp"].dt.hour * 60 + out["_timestamp"].dt.minute
    open_minute = config.session_open_hhmm[0] * 60 + config.session_open_hhmm[1]
    close_minute = config.session_close_hhmm[0] * 60 + config.session_close_hhmm[1]
    out = out.loc[minute.between(open_minute, close_minute, inclusive="both")].copy()
    opening_snapshot = (
        out["opening_snapshot"].fillna(False).astype(bool)
        if "opening_snapshot" in out
        else pd.Series(False, index=out.index)
    )
    # A marked 09:15 row is an opening quote snapshot in the EQID hybrid
    # store, not a completed candle.  Removing it (rather than just excluding
    # it from breadth denominators) also prevents EMA/momentum contamination.
    out = out.loc[~opening_snapshot].copy()
    out = _session_fields(out, config)
    explicit_eligible = (
        out["is_eligible"].fillna(False).astype(bool)
        if "is_eligible" in out
        else pd.Series(True, index=out.index)
    )
    # Explicitly invalid rows are not observations and must not update any
    # rolling state used by later bars.
    out = out.loc[explicit_eligible].copy()
    finite_ohlc = pd.Series(
        np.isfinite(out[["open", "high", "low", "close"]].to_numpy(dtype="float64")).all(axis=1),
        index=out.index,
    )
    valid_price = (
        finite_ohlc
        & out["close"].gt(0)
        & out["high"].ge(out["low"])
    )
    gap_filled = (
        pd.to_numeric(out["gap_filled"], errors="coerce").fillna(0).ne(0)
        if "gap_filled" in out
        else pd.Series(False, index=out.index)
    )
    complete_source = (
        pd.to_numeric(out["source_1m_count"], errors="coerce").ge(config.bar_minutes)
        if "source_1m_count" in out
        else pd.Series(True, index=out.index)
    )
    out["_price_eligible"] = valid_price
    out["_fresh_price_eligible"] = valid_price & ~gap_filled & complete_source
    out["_price_eligible"] = valid_price & complete_source
    out["_flow_eligible"] = (
        out["_fresh_price_eligible"]
        & out["volume"].notna()
        & np.isfinite(out["volume"])
        & out["volume"].ge(0)
    )
    return out.reset_index(drop=True)


def _prepare_stock_panel(
    stock_bars: pd.DataFrame,
    sector_map: Mapping[str, str] | None,
    config: MarketContextConfig,
) -> pd.DataFrame:
    out = _normalise_bars(stock_bars, config, require_ticker=True)
    if out.empty:
        return out

    if config.sector_col in out:
        sector = out[config.sector_col].astype("string").str.upper().str.strip()
        sector = sector.mask(sector.isin({"", "<NA>", "NAN", "NONE", "NULL"}))
    else:
        sector = pd.Series(np.nan, index=out.index, dtype="object")
    if sector_map:
        clean_sector_map: dict[str, str] = {}
        for key, value in sector_map.items():
            if pd.isna(key) or pd.isna(value):
                continue
            clean_key = str(key).upper().strip()
            clean_value = str(value).upper().strip()
            if clean_key in {"", "<NA>", "NAN", "NONE", "NULL"}:
                continue
            if clean_value in {"", "<NA>", "NAN", "NONE", "NULL"}:
                continue
            clean_sector_map[clean_key] = clean_value
        mapped = out["_ticker"].map(clean_sector_map)
        sector = sector.fillna(mapped)
    out["sector"] = sector.fillna("UNMAPPED")
    out["_sector_mapped"] = out["sector"].ne("UNMAPPED")

    keys = [out["_ticker"], out["_session"]]
    typical = (out["high"] + out["low"] + out["close"]) / 3.0
    usable_volume = out["volume"].where(out["_flow_eligible"], 0.0).clip(lower=0.0)
    cumulative_pv = (typical.fillna(0.0) * usable_volume).groupby(keys, sort=False).cumsum()
    cumulative_volume = usable_volume.groupby(keys, sort=False).cumsum()
    out["_session_vwap"] = cumulative_pv.div(cumulative_volume.replace(0.0, np.nan))

    for span, output_name, source_names in (
        (config.ema_fast_span, "_ema_fast", ("EMA_20", "ema20", "ema_20")),
        (config.ema_slow_span, "_ema_slow", ("EMA_50", "ema50", "ema_50")),
    ):
        source = next((name for name in source_names if name in out), None)
        supplied = pd.to_numeric(out[source], errors="coerce") if source is not None else None
        if supplied is not None and supplied.notna().all():
            out[output_name] = supplied
            continue
        # Synthetic gap-filled rows carry forward an old price.  They are useful
        # as an LTP observation, but must not advance indicator state used by
        # later completed bars.
        causal_close = out["close"].where(out["_fresh_price_eligible"])
        computed = causal_close.groupby(out["_ticker"], sort=False).transform(
            lambda s: s.ewm(
                span=span, adjust=False, min_periods=span, ignore_na=True
            ).mean()
        )
        out[output_name] = computed if supplied is None else supplied.fillna(computed)

    daily_close = (
        out.loc[out["_fresh_price_eligible"]]
        .groupby(["_ticker", "_session"], sort=True, observed=True)["close"]
        .last()
        .rename("_daily_close")
        .reset_index()
    )
    daily_close["_previous_close"] = daily_close.groupby("_ticker", sort=False)[
        "_daily_close"
    ].shift(1)
    out = out.merge(
        daily_close[["_ticker", "_session", "_previous_close"]],
        on=["_ticker", "_session"],
        how="left",
        validate="many_to_one",
    )

    eligible_high = out["high"].where(out["_fresh_price_eligible"])
    eligible_low = out["low"].where(out["_fresh_price_eligible"])
    intraday_keys = [out["_ticker"], out["_session"]]
    previous_high = eligible_high.groupby(intraday_keys, sort=False).transform(
        lambda s: s.shift(1).cummax()
    )
    previous_low = eligible_low.groupby(intraday_keys, sort=False).transform(
        lambda s: s.shift(1).cummin()
    )
    out["_new_intraday_high"] = out["high"].gt(previous_high).where(
        out["_fresh_price_eligible"] & previous_high.notna()
    )
    out["_new_intraday_low"] = out["low"].lt(previous_low).where(
        out["_fresh_price_eligible"] & previous_low.notna()
    )
    lookup_index = pd.MultiIndex.from_arrays([out["_ticker"], out["_timestamp"]])
    close_lookup = pd.Series(
        out["close"].where(out["_fresh_price_eligible"]).to_numpy(), index=lookup_index
    )
    lag_index = pd.MultiIndex.from_arrays(
        [
            out["_ticker"],
            out["_timestamp"]
            - pd.Timedelta(minutes=config.sector_momentum_bars * config.bar_minutes),
        ]
    )
    lagged = pd.Series(close_lookup.reindex(lag_index).to_numpy(), index=out.index)
    out["_stock_momentum"] = np.log(out["close"].div(lagged.where(lagged.gt(0))))
    out["_stock_day_return"] = out["close"].div(out["_previous_close"]).sub(1.0)
    out["_stock_momentum"] = out["_stock_momentum"].where(out["_fresh_price_eligible"])
    out["_stock_day_return"] = out["_stock_day_return"].where(out["_fresh_price_eligible"])

    supplied_rvol = None
    for name in ("stock_relative_volume", "relative_volume", "vol_ratio"):
        if name in out:
            supplied_rvol = pd.to_numeric(out[name], errors="coerce")
            break
    if supplied_rvol is not None and supplied_rvol.notna().all():
        out["_stock_rvol"] = supplied_rvol
    else:
        eligible_volume = out["volume"].where(out["_flow_eligible"])
        baseline = eligible_volume.groupby(
            [out["_ticker"], out["_bar_slot"]], sort=False, observed=True
        ).transform(
            lambda s: s.shift(1)
            .rolling(
                config.relative_volume_sessions,
                min_periods=config.relative_volume_min_sessions,
            )
            .median()
        )
        computed_rvol = out["volume"].div(baseline.replace(0.0, np.nan))
        out["_stock_rvol"] = (
            computed_rvol if supplied_rvol is None else supplied_rvol.fillna(computed_rvol)
        )
    out.loc[~out["_flow_eligible"], "_stock_rvol"] = np.nan
    return out.sort_values(["_timestamp", "_ticker"]).reset_index(drop=True)


def _aggregate_market(panel: pd.DataFrame, config: MarketContextConfig) -> pd.DataFrame:
    work = panel.copy()
    # Breadth is a cross-section of newly completed observations.  A
    # gap-filled row is only a carried-forward LTP and must not vote in the
    # advance/decline or technical-participation denominators.
    valid_previous_close = work["_fresh_price_eligible"] & work["_previous_close"].gt(0)
    work["_advance"] = work["close"].gt(work["_previous_close"]).where(valid_previous_close)
    work["_decline"] = work["close"].lt(work["_previous_close"]).where(valid_previous_close)
    work["_unchanged"] = work["close"].eq(work["_previous_close"]).where(valid_previous_close)
    work["_above_vwap"] = work["close"].gt(work["_session_vwap"]).where(
        work["_fresh_price_eligible"] & work["_session_vwap"].notna()
    )
    work["_above_ema20"] = work["close"].gt(work["_ema_fast"]).where(
        work["_fresh_price_eligible"] & work["_ema_fast"].notna()
    )
    work["_above_ema50"] = work["close"].gt(work["_ema_slow"]).where(
        work["_fresh_price_eligible"] & work["_ema_slow"].notna()
    )
    work["_new_high_flag"] = work["_new_intraday_high"].where(work["_fresh_price_eligible"])
    work["_new_low_flag"] = work["_new_intraday_low"].where(work["_fresh_price_eligible"])
    work["_up_volume"] = work["volume"].where(work["_flow_eligible"] & work["_advance"].eq(True), 0.0)
    work["_down_volume"] = work["volume"].where(work["_flow_eligible"] & work["_decline"].eq(True), 0.0)
    work["_previous_close_valid"] = valid_previous_close
    work["_mapped_fresh_price_eligible"] = (
        work["_fresh_price_eligible"] & work["_sector_mapped"]
    )

    grouped = work.groupby("_timestamp", sort=True, observed=True)
    market = grouped.agg(
        universe_observed=("_ticker", "nunique"),
        price_eligible_count=("_price_eligible", "sum"),
        fresh_price_eligible_count=("_fresh_price_eligible", "sum"),
        flow_eligible_count=("_flow_eligible", "sum"),
        previous_close_valid_count=("_previous_close_valid", "sum"),
        sector_mapped_count=("_mapped_fresh_price_eligible", "sum"),
        advance_count=("_advance", "sum"),
        decline_count=("_decline", "sum"),
        unchanged_count=("_unchanged", "sum"),
        above_vwap_valid_count=("_above_vwap", "count"),
        above_ema20_valid_count=("_above_ema20", "count"),
        above_ema50_valid_count=("_above_ema50", "count"),
        new_high_low_valid_count=("_new_high_flag", "count"),
        fraction_above_vwap=("_above_vwap", "mean"),
        fraction_above_ema20=("_above_ema20", "mean"),
        fraction_above_ema50=("_above_ema50", "mean"),
        fraction_new_intraday_highs=("_new_high_flag", "mean"),
        fraction_new_intraday_lows=("_new_low_flag", "mean"),
        up_volume=("_up_volume", "sum"),
        down_volume=("_down_volume", "sum"),
        cross_sectional_return_dispersion=("_stock_day_return", "std"),
    ).reset_index()

    numeric_aggregates = (
        "universe_observed",
        "price_eligible_count",
        "fresh_price_eligible_count",
        "flow_eligible_count",
        "previous_close_valid_count",
        "sector_mapped_count",
        "advance_count",
        "decline_count",
        "unchanged_count",
        "above_vwap_valid_count",
        "above_ema20_valid_count",
        "above_ema50_valid_count",
        "new_high_low_valid_count",
        "fraction_above_vwap",
        "fraction_above_ema20",
        "fraction_above_ema50",
        "fraction_new_intraday_highs",
        "fraction_new_intraday_lows",
        "up_volume",
        "down_volume",
        "cross_sectional_return_dispersion",
    )
    for column in numeric_aggregates:
        market[column] = pd.to_numeric(market[column], errors="coerce").astype("float64")

    if "universe_expected" in panel:
        expected_by_timestamp = (
            panel.groupby("_timestamp", sort=True, observed=True)["universe_expected"]
            .max()
            .pipe(pd.to_numeric, errors="coerce")
        )
        expected = market["_timestamp"].map(expected_by_timestamp)
        expected = expected.fillna(market["universe_observed"].cummax()).clip(lower=1.0)
    elif config.expected_universe_size is not None:
        expected = pd.Series(float(config.expected_universe_size), index=market.index)
    else:
        # Causal fallback only.  A future-listed ticker must not revise past
        # coverage; production should pass the feed manifest's exact universe.
        expected = market["universe_observed"].cummax().clip(lower=1.0)
    market["universe_expected"] = expected
    market["market_ltp_coverage"] = market["price_eligible_count"].div(expected)
    market["market_coverage"] = market["fresh_price_eligible_count"].div(expected)
    market["advance_decline_coverage"] = market["previous_close_valid_count"].div(expected)
    market["vwap_breadth_coverage"] = market["above_vwap_valid_count"].div(expected)
    market["ema20_breadth_coverage"] = market["above_ema20_valid_count"].div(expected)
    market["ema50_breadth_coverage"] = market["above_ema50_valid_count"].div(expected)
    market["new_high_low_coverage"] = market["new_high_low_valid_count"].div(expected)
    market["sector_mapping_coverage"] = market["sector_mapped_count"].div(
        market["fresh_price_eligible_count"].replace(0.0, np.nan)
    )
    # Jeffreys smoothing prevents infinities while converging to A/D rapidly.
    market["advance_decline_ratio"] = (market["advance_count"] + 0.5).div(
        market["decline_count"] + 0.5
    ).where(market["previous_close_valid_count"].gt(0))
    market["advance_decline_log_ratio"] = np.log(market["advance_decline_ratio"])
    ad_denominator = (
        market["advance_count"] + market["decline_count"] + market["unchanged_count"]
    )
    market["advance_decline_net"] = (market["advance_count"] - market["decline_count"]).div(
        ad_denominator.replace(0.0, np.nan)
    )
    volume_denominator = market["up_volume"] + market["down_volume"]
    market["up_volume_fraction"] = market["up_volume"].div(volume_denominator.replace(0.0, np.nan))

    fraction_columns = (
        "fraction_above_vwap",
        "fraction_above_ema20",
        "fraction_above_ema50",
        "fraction_new_intraday_highs",
        "fraction_new_intraday_lows",
    )
    for column in fraction_columns:
        market[column.replace("fraction_", "pct_")] = 100.0 * market[column]

    high_low_net = market["fraction_new_intraday_highs"] - market["fraction_new_intraday_lows"]
    market["market_breadth"] = _finite_weighted_average(
        [
            market["advance_decline_net"],
            2.0 * market["fraction_above_vwap"] - 1.0,
            2.0 * market["fraction_above_ema20"] - 1.0,
            2.0 * market["fraction_above_ema50"] - 1.0,
            high_low_net,
        ],
        [0.30, 0.25, 0.20, 0.15, 0.10],
    )
    session = market["_timestamp"].dt.strftime("%Y-%m-%d")
    market["breadth_thrust_15m"] = market["market_breadth"] - market.groupby(
        session, sort=False
    )["market_breadth"].shift(3)
    market["cross_sectional_return_dispersion"] *= 100.0
    market = market.rename(columns={"_timestamp": "timestamp"})
    return market


def _aggregate_sectors(panel: pd.DataFrame, config: MarketContextConfig) -> pd.DataFrame:
    mapped = panel.loc[panel["sector"].ne("UNMAPPED")].copy()
    if mapped.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "sector",
                "sector_strength_rank",
                "sector_strength_score",
                "sector_momentum_30m_pct",
                "sector_relative_volume",
            ]
        )
    mapped["_above_vwap"] = mapped["close"].gt(mapped["_session_vwap"]).where(
        mapped["_fresh_price_eligible"] & mapped["_session_vwap"].notna()
    )
    grouped = mapped.groupby(["_timestamp", "sector"], sort=True, observed=True)
    sectors = grouped.agg(
        sector_member_count=("_ticker", "nunique"),
        sector_price_eligible_count=("_price_eligible", "sum"),
        sector_fresh_price_eligible_count=("_fresh_price_eligible", "sum"),
        sector_momentum_30m=("_stock_momentum", "median"),
        sector_intraday_return=("_stock_day_return", "median"),
        sector_fraction_above_vwap=("_above_vwap", "mean"),
        sector_relative_volume=("_stock_rvol", "median"),
    ).reset_index()
    sectors["sector_is_reliable"] = sectors["sector_fresh_price_eligible_count"].ge(
        config.min_sector_members
    )
    sectors["sector_momentum_30m_pct"] = 100.0 * sectors["sector_momentum_30m"]
    sectors["sector_intraday_return_pct"] = 100.0 * sectors["sector_intraday_return"]
    sectors["sector_pct_above_vwap"] = 100.0 * sectors["sector_fraction_above_vwap"]

    market_momentum = panel.groupby("_timestamp", sort=True, observed=True)[
        "_stock_momentum"
    ].median()
    sectors["sector_relative_momentum_pct"] = 100.0 * (
        sectors["sector_momentum_30m"]
        - sectors["_timestamp"].map(market_momentum)
    )
    reliable = sectors["sector_is_reliable"]
    for source, output in (
        ("sector_relative_momentum_pct", "_momentum_z"),
        ("sector_fraction_above_vwap", "_breadth_z"),
    ):
        sectors[output] = np.nan
        sectors.loc[reliable, output] = sectors.loc[reliable].groupby(
            "_timestamp", sort=False, observed=True
        )[source].transform(_safe_cross_section_z)
    sectors["_log_rvol"] = np.log(sectors["sector_relative_volume"].clip(lower=1e-6))
    sectors["_rvol_z"] = np.nan
    sectors.loc[reliable, "_rvol_z"] = sectors.loc[reliable].groupby(
        "_timestamp", sort=False, observed=True
    )["_log_rvol"].transform(_safe_cross_section_z)

    raw_strength = _finite_weighted_average(
        [sectors["_momentum_z"], sectors["_breadth_z"], sectors["_rvol_z"]],
        [0.55, 0.25, 0.20],
    )
    sectors["sector_strength_score"] = 100.0 * np.tanh(raw_strength / 2.0)
    sectors.loc[~reliable, "sector_strength_score"] = np.nan
    sectors["sector_strength_rank"] = sectors.groupby(
        "_timestamp", sort=False, observed=True
    )["sector_strength_score"].rank(method="min", ascending=False)
    sector_count = sectors.groupby("_timestamp", sort=False, observed=True)[
        "sector_strength_score"
    ].transform("count")
    sectors["sector_strength_percentile"] = np.where(
        sector_count.gt(1),
        1.0 - (sectors["sector_strength_rank"] - 1.0) / (sector_count - 1.0),
        np.where(sector_count.eq(1), 0.5, np.nan),
    )
    sectors = sectors.sort_values(["sector", "_timestamp"])
    sector_session = sectors["_timestamp"].dt.strftime("%Y-%m-%d")
    sectors["sector_rank_turnover"] = (
        sectors["sector_strength_percentile"]
        - sectors.groupby(
            [sectors["sector"], sector_session], sort=False, observed=True
        )["sector_strength_percentile"].shift(config.rotation_lookback_bars)
    ).abs()
    sectors = sectors.sort_values(["_timestamp", "sector"])
    sectors = sectors.rename(columns={"_timestamp": "timestamp"})
    return sectors.drop(
        columns=[
            "sector_momentum_30m",
            "sector_intraday_return",
            "sector_fraction_above_vwap",
            "_momentum_z",
            "_breadth_z",
            "_log_rvol",
            "_rvol_z",
        ],
        errors="ignore",
    ).reset_index(drop=True)


def _canonical_index_name(value: Any, config: MarketContextConfig) -> str | None:
    normalised = _normalise_symbol(value)
    for canonical, aliases in config.aliases().items():
        if normalised in {_normalise_symbol(alias) for alias in aliases}:
            return canonical
    return None


def _prepare_one_index(frame: pd.DataFrame, config: MarketContextConfig) -> pd.DataFrame:
    out = frame.loc[frame["_fresh_price_eligible"]].sort_values("_timestamp").copy()
    if out.empty:
        return pd.DataFrame(
            columns=[
                "_timestamp",
                "trend_score",
                "realized_volatility_60m_bps",
                "source_ready",
            ]
        )
    session_group = out.groupby("_session", sort=False, observed=True)
    close = out["close"]
    typical = (out["high"] + out["low"] + close) / 3.0
    usable_volume = out["volume"].where(out["_flow_eligible"], 0.0).clip(lower=0.0)
    cum_volume = usable_volume.groupby(out["_session"], sort=False).cumsum()
    vwap = (typical.fillna(0.0) * usable_volume).groupby(out["_session"], sort=False).cumsum().div(
        cum_volume.replace(0.0, np.nan)
    )
    twap = typical.groupby(out["_session"], sort=False).expanding().mean().reset_index(level=0, drop=True)
    out["_session_anchor"] = vwap.fillna(twap)

    ema_fast_computed = close.ewm(span=config.ema_fast_span, adjust=False, min_periods=1).mean()
    ema_slow_computed = close.ewm(span=config.ema_slow_span, adjust=False, min_periods=1).mean()
    out["_ema_fast"] = (
        pd.to_numeric(out["EMA_20"], errors="coerce").fillna(ema_fast_computed)
        if "EMA_20" in out
        else ema_fast_computed
    )
    out["_ema_slow"] = (
        pd.to_numeric(out["EMA_50"], errors="coerce").fillna(ema_slow_computed)
        if "EMA_50" in out
        else ema_slow_computed
    )

    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - previous_close).abs(),
            (out["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    computed_atr = true_range.ewm(alpha=1.0 / config.atr_span, adjust=False, min_periods=1).mean()
    out["_atr"] = (
        pd.to_numeric(out["ATR"], errors="coerce").fillna(computed_atr)
        if "ATR" in out
        else computed_atr
    ).replace(0.0, np.nan)

    log_return = np.log(close.div(previous_close.where(previous_close.gt(0))))
    # Intraday rolling volatility does not include the overnight jump.
    log_return = log_return.where(out["_session"].eq(out["_session"].shift(1)))
    out["_realized_volatility"] = log_return.groupby(out["_session"], sort=False).transform(
        lambda s: s.pow(2)
        .rolling(config.trend_long_bars, min_periods=config.trend_long_bars)
        .sum()
        .pow(0.5)
    )
    scale = log_return.groupby(out["_session"], sort=False).transform(
        lambda s: s.rolling(config.trend_volatility_bars, min_periods=4).std(ddof=0)
    ).replace(0.0, np.nan)
    short_lag = session_group["close"].shift(config.trend_short_bars)
    long_lag = session_group["close"].shift(config.trend_long_bars)
    short_return = np.log(close.div(short_lag.where(short_lag.gt(0))))
    long_return = np.log(close.div(long_lag.where(long_lag.gt(0))))
    short_z = short_return.div(scale * np.sqrt(config.trend_short_bars))
    long_z = long_return.div(scale * np.sqrt(config.trend_long_bars))
    anchor_distance = (close - out["_session_anchor"]).div(out["_atr"])
    ema_spread = (out["_ema_fast"] - out["_ema_slow"]).div(out["_atr"])
    absolute_path = log_return.abs().groupby(out["_session"], sort=False).transform(
        lambda s: s.rolling(config.trend_long_bars, min_periods=3).sum()
    )
    efficiency = long_return.div(absolute_path.replace(0.0, np.nan)).clip(-1.0, 1.0)
    out["trend_score"] = 100.0 * _finite_weighted_average(
        [
            np.tanh(short_z.clip(-4.0, 4.0)),
            np.tanh(long_z.clip(-4.0, 4.0)),
            np.tanh(anchor_distance.clip(-4.0, 4.0)),
            np.tanh(ema_spread.clip(-4.0, 4.0)),
            efficiency,
        ],
        [0.30, 0.25, 0.20, 0.15, 0.10],
    )
    out["realized_volatility_60m_bps"] = 10_000.0 * out["_realized_volatility"]
    observations_this_session = out.groupby("_session", sort=False, observed=True).cumcount() + 1
    out["source_ready"] = observations_this_session.ge(config.trend_long_bars + 1)
    return out[
        ["_timestamp", "trend_score", "realized_volatility_60m_bps", "source_ready"]
    ]


def _prepare_index_features(
    index_bars: pd.DataFrame | Mapping[str, pd.DataFrame] | None,
    config: MarketContextConfig,
) -> dict[str, pd.DataFrame]:
    if index_bars is None:
        return {}
    if isinstance(index_bars, Mapping):
        pieces: list[pd.DataFrame] = []
        for name, frame in index_bars.items():
            if frame is None or frame.empty:
                continue
            one = frame.copy()
            one[config.ticker_col] = str(name)
            pieces.append(one)
        raw = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    else:
        raw = index_bars.copy()
    if raw.empty:
        return {}
    normalised = _normalise_bars(raw, config, require_ticker=True)
    normalised["_canonical"] = normalised["_ticker"].map(
        lambda value: _canonical_index_name(value, config)
    )
    normalised = normalised.dropna(subset=["_canonical"])
    outputs: dict[str, pd.DataFrame] = {}
    for canonical, aliases in config.aliases().items():
        candidates = normalised.loc[normalised["_canonical"].eq(canonical)]
        if candidates.empty:
            continue
        prepared_sources: list[pd.DataFrame] = []
        alias_list = list(aliases)
        for source, source_frame in candidates.groupby("_ticker", sort=True, observed=True):
            exact_priorities = [
                i for i, alias in enumerate(alias_list)
                if str(source).upper().strip() == str(alias).upper().strip()
            ]
            normalised_priorities = [
                i for i, alias in enumerate(alias_list)
                if _normalise_symbol(source) == _normalise_symbol(alias)
            ]
            priorities = exact_priorities or normalised_priorities
            if not priorities:
                continue
            priority = min(priorities)
            prepared = _prepare_one_index(source_frame, config)
            if prepared.empty:
                continue
            prepared["source"] = str(source)
            prepared["_source_priority"] = priority
            prepared_sources.append(prepared)
        if not prepared_sources:
            continue
        # Pick the best available source independently at each timestamp.  Each
        # alias is prepared on its own price scale, so an ETF/index hand-off
        # cannot inject a discontinuity into EMA or return history.  A preferred
        # alias that starts in the future also cannot erase older observations.
        outputs[canonical] = (
            pd.concat(prepared_sources, ignore_index=True)
            .sort_values(
                ["_timestamp", "source_ready", "_source_priority"],
                ascending=[True, False, True],
            )
            .drop_duplicates("_timestamp", keep="first")
            .drop(columns="_source_priority")
            .reset_index(drop=True)
        )
    return outputs


def _merge_index_features(
    market: pd.DataFrame,
    index_features: Mapping[str, pd.DataFrame],
    config: MarketContextConfig,
) -> pd.DataFrame:
    out = market.sort_values("timestamp").copy()
    tolerance = pd.Timedelta(minutes=config.max_context_staleness_minutes)
    for canonical in ("nifty", "bank_nifty", "midcap"):
        feature_frame = index_features.get(canonical)
        if feature_frame is None or feature_frame.empty:
            out[f"{canonical}_trend_score"] = np.nan
            out[f"{canonical}_realized_volatility_60m_bps"] = np.nan
            out[f"{canonical}_source"] = pd.NA
            out[f"{canonical}_source_ready"] = False
            out[f"{canonical}_source_timestamp"] = pd.NaT
            continue
        right = feature_frame.rename(
            columns={
                "_timestamp": f"{canonical}_source_timestamp",
                "trend_score": f"{canonical}_trend_score",
                "realized_volatility_60m_bps": f"{canonical}_realized_volatility_60m_bps",
                "source": f"{canonical}_source",
                "source_ready": f"{canonical}_source_ready",
            }
        ).sort_values(f"{canonical}_source_timestamp")
        out = pd.merge_asof(
            out.sort_values("timestamp"),
            right,
            left_on="timestamp",
            right_on=f"{canonical}_source_timestamp",
            direction="backward",
            tolerance=tolerance,
            allow_exact_matches=True,
        )
    return out


def _sector_market_summary(sectors: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "timestamp",
        "sector_count",
        "sector_positive_share",
        "sector_momentum_dispersion",
        "sector_rank_turnover_mean",
        "sector_leader_strength",
        "sector_laggard_strength",
        "leading_sector",
        "lagging_sector",
    ]
    if sectors.empty:
        return pd.DataFrame(columns=columns)

    def leader_name(group: pd.DataFrame) -> Any:
        valid = group.dropna(subset=["sector_strength_rank"])
        return valid.sort_values(["sector_strength_rank", "sector"]).iloc[0]["sector"] if not valid.empty else pd.NA

    def laggard_name(group: pd.DataFrame) -> Any:
        valid = group.dropna(subset=["sector_strength_rank"])
        return valid.sort_values(["sector_strength_rank", "sector"], ascending=[False, True]).iloc[0]["sector"] if not valid.empty else pd.NA

    reliable = sectors.loc[sectors["sector_is_reliable"]].copy()
    if reliable.empty:
        return pd.DataFrame(columns=columns)
    summary = reliable.groupby("timestamp", sort=True, observed=True).agg(
        sector_count=("sector", "nunique"),
        sector_positive_share=(
            "sector_momentum_30m_pct",
            lambda s: s.dropna().gt(0).mean() if s.notna().any() else np.nan,
        ),
        sector_momentum_dispersion=("sector_momentum_30m_pct", lambda s: s.std(ddof=0)),
        sector_rank_turnover_mean=("sector_rank_turnover", "mean"),
        sector_leader_strength=("sector_strength_score", "max"),
        sector_laggard_strength=("sector_strength_score", "min"),
    ).reset_index()
    names = reliable.groupby("timestamp", sort=True, observed=True).apply(
        lambda g: pd.Series(
            {"leading_sector": leader_name(g), "lagging_sector": laggard_name(g)}
        ),
        include_groups=False,
    ).reset_index()
    return summary.merge(names, on="timestamp", how="left", validate="one_to_one")


def _add_regimes(
    market: pd.DataFrame,
    sectors: pd.DataFrame,
    config: MarketContextConfig,
) -> pd.DataFrame:
    out = market.sort_values("timestamp").reset_index(drop=True).copy()
    out = _session_fields(out.rename(columns={"timestamp": "_timestamp"}), config).rename(
        columns={"_timestamp": "timestamp"}
    )
    sector_summary = _sector_market_summary(sectors)
    out = out.merge(sector_summary, on="timestamp", how="left", validate="one_to_one")

    index_scores = [
        out["nifty_trend_score"],
        out["bank_nifty_trend_score"],
        out["midcap_trend_score"],
    ]
    out["combined_index_trend_score"] = _finite_weighted_average(
        index_scores, [0.50, 0.30, 0.20]
    )
    signs = np.column_stack(
        [np.sign(pd.to_numeric(series, errors="coerce")) for series in index_scores]
    )
    valid_sign = np.isfinite(signs)
    out["index_trend_agreement"] = np.divide(
        np.abs(np.nansum(signs, axis=1)),
        valid_sign.sum(axis=1),
        out=np.full(len(out), np.nan),
        where=valid_sign.sum(axis=1) > 0,
    )
    out["index_context_count"] = valid_sign.sum(axis=1)
    ready_columns = [
        pd.to_numeric(out[f"{name}_source_ready"], errors="coerce").fillna(0).astype(bool)
        for name in ("nifty", "bank_nifty", "midcap")
    ]
    out["index_context_ready_count"] = np.column_stack(ready_columns).sum(axis=1)
    threshold = config.trend_regime_threshold
    trend_up = out["combined_index_trend_score"].ge(threshold) & out["index_trend_agreement"].ge(0.5)
    trend_down = out["combined_index_trend_score"].le(-threshold) & out["index_trend_agreement"].ge(0.5)
    trend_available = (
        out["combined_index_trend_score"].notna()
        & out["index_context_ready_count"].ge(2)
    )
    out["trend_regime_code"] = pd.Series(
        np.select([trend_up, trend_down], [1.0, -1.0], default=0.0), index=out.index
    ).where(trend_available)
    out["trend_regime"] = np.select(
        [~trend_available, trend_up, trend_down],
        ["UNKNOWN", "UPTREND", "DOWNTREND"],
        default="RANGE_MIXED",
    )

    out["intraday_volatility_z"] = _causal_same_slot_zscore(
        out["nifty_realized_volatility_60m_bps"],
        out["_bar_slot"],
        config.regime_baseline_sessions,
        config.regime_min_sessions,
    )
    out["volatility_baseline_observations"] = _causal_same_slot_prior_count(
        out["nifty_realized_volatility_60m_bps"], out["_bar_slot"]
    )
    out["volatility_baseline_ready"] = (
        out["nifty_realized_volatility_60m_bps"].notna()
        & out["intraday_volatility_z"].notna()
        & out["volatility_baseline_observations"].ge(config.regime_min_sessions)
    )
    vol_high = out["intraday_volatility_z"].ge(config.volatility_z_threshold)
    vol_low = out["intraday_volatility_z"].le(-config.volatility_z_threshold)
    vol_available = out["nifty_realized_volatility_60m_bps"].notna()
    out["intraday_volatility_regime_code"] = pd.Series(
        np.select([vol_high, vol_low], [1.0, -1.0], default=0.0), index=out.index
    ).where(out["volatility_baseline_ready"])
    out["intraday_volatility_regime"] = np.select(
        [
            ~vol_available,
            ~out["volatility_baseline_ready"],
            vol_high,
            vol_low,
        ],
        ["UNKNOWN", "WARMUP", "HIGH", "LOW"],
        default="NORMAL",
    )

    out["sector_dispersion_z"] = _causal_same_slot_zscore(
        out["sector_momentum_dispersion"],
        out["_bar_slot"],
        config.regime_baseline_sessions,
        config.regime_min_sessions,
    )
    out["rotation_baseline_observations"] = _causal_same_slot_prior_count(
        out["sector_momentum_dispersion"], out["_bar_slot"]
    )
    out["rotation_baseline_ready"] = (
        out["sector_momentum_dispersion"].notna()
        & out["sector_dispersion_z"].notna()
        & out["rotation_baseline_observations"].ge(config.regime_min_sessions)
    )
    dispersion_component = 1.0 / (1.0 + np.exp(-1.5 * out["sector_dispersion_z"].clip(-6, 6)))
    turnover_component = pd.to_numeric(
        out["sector_rank_turnover_mean"], errors="coerce"
    ).clip(0.0, 1.0)
    low_coherence_component = 1.0 - out["market_breadth"].abs().clip(0.0, 1.0)
    out["rotation_score"] = 100.0 * _finite_weighted_average(
        [dispersion_component, turnover_component, low_coherence_component],
        [0.45, 0.35, 0.20],
    )
    high_rotation = out["rotation_score"].ge(config.rotation_score_threshold)
    broad_trend = (
        out["market_breadth"].abs().ge(config.broad_trend_breadth_threshold)
        & out["sector_dispersion_z"].le(0.5)
    )
    rotation_available = (
        out["sector_momentum_dispersion"].notna()
        & pd.to_numeric(out["sector_count"], errors="coerce").fillna(0).ge(3)
    )
    out["rotation_regime_code"] = pd.Series(
        np.select([high_rotation, broad_trend], [1.0, -1.0], default=0.0), index=out.index
    ).where(rotation_available & out["rotation_baseline_ready"])
    out["rotation_regime"] = np.select(
        [
            ~rotation_available,
            ~out["rotation_baseline_ready"],
            high_rotation,
            broad_trend,
        ],
        ["UNKNOWN", "WARMUP", "HIGH_ROTATION", "BROAD_TREND"],
        default="MIXED",
    )

    midcap_relative = np.tanh(
        (out["midcap_trend_score"] - out["nifty_trend_score"]) / 40.0
    )
    volatility_risk = -np.tanh(out["intraday_volatility_z"] / 2.0)
    risk_terms = [
        out["combined_index_trend_score"] / 100.0,
        out["market_breadth"],
        2.0 * out["sector_positive_share"] - 1.0,
        midcap_relative,
        volatility_risk,
    ]
    out["risk_context_component_count"] = np.isfinite(
        np.column_stack([np.asarray(term, dtype="float64") for term in risk_terms])
    ).sum(axis=1)
    out["risk_on_off_score"] = 100.0 * _finite_weighted_average(
        risk_terms,
        [0.30, 0.30, 0.15, 0.15, 0.10],
    )
    risk_available = out["risk_on_off_score"].notna() & out["risk_context_component_count"].ge(3)
    out["risk_on_off_regime_code"] = pd.Series(
        np.select(
            [out["risk_on_off_score"].ge(20.0), out["risk_on_off_score"].le(-20.0)],
            [1.0, -1.0],
            default=0.0,
        ),
        index=out.index,
    ).where(risk_available)
    out["risk_on_off_regime"] = np.select(
        [
            ~risk_available,
            out["risk_on_off_score"].ge(20.0),
            out["risk_on_off_score"].le(-20.0),
        ],
        ["UNKNOWN", "RISK_ON", "RISK_OFF"],
        default="NEUTRAL",
    )

    out["context_complete"] = (
        out["index_context_count"].eq(3)
        & out["index_context_ready_count"].eq(3)
        & out["market_coverage"].ge(config.min_market_coverage)
        & out["sector_mapping_coverage"].ge(config.min_sector_coverage)
        & pd.to_numeric(out["sector_count"], errors="coerce").fillna(0).ge(3)
        & out["volatility_baseline_ready"]
        & out["rotation_baseline_ready"]
    )
    out["available_at"] = out["timestamp"] + pd.Timedelta(
        seconds=config.publish_delay_seconds
    )
    out["feature_version"] = FEATURE_VERSION
    return out.drop(columns=["_session", "_bar_slot"], errors="ignore")


def _assert_feature_only(frame: pd.DataFrame) -> None:
    forbidden = FORBIDDEN_OUTPUT_COLUMNS & {str(column).lower() for column in frame.columns}
    if forbidden:
        raise AssertionError(f"market context leaked execution fields: {sorted(forbidden)}")


def _backward_asof_preserve_missing(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_on: str,
    right_on: str,
    tolerance: pd.Timedelta,
) -> pd.DataFrame:
    """``merge_asof`` while retaining rows whose decision timestamp is NaT."""

    valid_mask = left[left_on].notna()
    pieces: list[pd.DataFrame] = []
    if valid_mask.any():
        pieces.append(
            pd.merge_asof(
                left.loc[valid_mask].sort_values(left_on),
                right.sort_values(right_on),
                left_on=left_on,
                right_on=right_on,
                direction="backward",
                tolerance=tolerance,
                allow_exact_matches=True,
            )
        )
    if (~valid_mask).any():
        missing = left.loc[~valid_mask].copy()
        for column in right.columns:
            if column in missing:
                continue
            if pd.api.types.is_datetime64_any_dtype(right[column]):
                missing[column] = pd.Series(pd.NaT, index=missing.index, dtype=right[column].dtype)
            else:
                missing[column] = np.nan
        pieces.append(missing)
    return pd.concat(pieces, ignore_index=True, sort=False) if pieces else left.iloc[0:0].copy()


class MarketContextEngine:
    """Vectorised batch engine; partition long histories by session if needed."""

    def __init__(
        self,
        config: MarketContextConfig | None = None,
        *,
        sector_map: Mapping[str, str] | None = None,
    ) -> None:
        self.config = config or MarketContextConfig()
        self.sector_map = dict(sector_map or {})

    def compute(
        self,
        stock_bars: pd.DataFrame,
        index_bars: pd.DataFrame | Mapping[str, pd.DataFrame] | None = None,
    ) -> MarketContextResult:
        """Compute causal market and sector features from completed bars."""

        panel = _prepare_stock_panel(stock_bars, self.sector_map, self.config)
        if panel.empty:
            empty = pd.DataFrame(columns=["timestamp", "available_at", "feature_version"])
            return MarketContextResult(market=empty, sectors=pd.DataFrame())
        market = _aggregate_market(panel, self.config)
        sectors = _aggregate_sectors(panel, self.config)
        indexes = _prepare_index_features(index_bars, self.config)
        market = _merge_index_features(market, indexes, self.config)
        market = _add_regimes(market, sectors, self.config)
        sectors = sectors.copy()
        sectors["available_at"] = sectors["timestamp"] + pd.Timedelta(
            seconds=self.config.publish_delay_seconds
        )
        sectors["feature_version"] = FEATURE_VERSION
        _assert_feature_only(market)
        _assert_feature_only(sectors)
        return MarketContextResult(
            market=market.sort_values("timestamp").reset_index(drop=True),
            sectors=sectors.sort_values(["timestamp", "sector"]).reset_index(drop=True),
        )

    def latest(
        self,
        stock_bars: pd.DataFrame,
        index_bars: pd.DataFrame | Mapping[str, pd.DataFrame] | None = None,
        *,
        asof: Any | None = None,
    ) -> MarketContextResult:
        """Return only the most recent published market/sector snapshot."""

        result = self.compute(stock_bars, index_bars)
        if result.market.empty:
            return result
        cutoff = (
            pd.Timestamp.now(tz=self.config.timezone)
            if asof is None
            else _to_timezone(pd.Series([asof]), self.config.timezone).iloc[0]
        )
        eligible = result.market.loc[result.market["available_at"].le(cutoff)]
        if eligible.empty:
            return MarketContextResult(result.market.iloc[0:0], result.sectors.iloc[0:0])
        timestamp = eligible.iloc[-1]["timestamp"]
        return MarketContextResult(
            market=eligible.tail(1).reset_index(drop=True),
            sectors=result.sectors.loc[result.sectors["timestamp"].eq(timestamp)].reset_index(drop=True),
        )


def attach_context_asof(
    candidates: pd.DataFrame,
    context: MarketContextResult,
    *,
    candidate_time_col: str = "signal_time_ist",
    ticker_col: str = "ticker",
    sector_col: str = "sector",
    sector_map: Mapping[str, str] | None = None,
    prefix: str = "mce_",
    timezone: str = IST,
    max_staleness_minutes: int = 7,
) -> pd.DataFrame:
    """Attach only context that was published at or before each candidate.

    Context columns are prefixed by default so existing V7/V11/V12 fields are
    never silently overwritten.  This function does not filter candidates.
    """

    if candidates is None or candidates.empty:
        return pd.DataFrame() if candidates is None else candidates.copy()
    if candidate_time_col not in candidates:
        raise ValueError(f"candidates missing {candidate_time_col!r}")
    if context.market.empty:
        return candidates.copy()
    existing_context_columns = [column for column in candidates.columns if str(column).startswith(prefix)]
    if existing_context_columns:
        raise ValueError(
            f"candidates already contain {prefix!r} context columns: "
            f"{existing_context_columns[:5]}"
        )
    reserved_columns = [column for column in candidates.columns if str(column).startswith("_mce_")]
    if reserved_columns:
        raise ValueError(f"candidates use reserved MCE columns: {reserved_columns[:5]}")

    left = candidates.copy()
    left["_mce_row_order"] = np.arange(len(left))
    left["_mce_original_index"] = list(left.index)
    left["_mce_decision_time"] = _to_timezone(left[candidate_time_col], timezone)
    right = context.market.copy()
    right["available_at"] = _to_timezone(right["available_at"], timezone)
    rename = {
        column: f"{prefix}{column}"
        for column in right.columns
        if column != "available_at"
    }
    right = right.rename(columns=rename)
    right = right.rename(columns={"available_at": f"{prefix}available_at"})
    tolerance = pd.Timedelta(minutes=max_staleness_minutes)
    out = _backward_asof_preserve_missing(
        left,
        right,
        left_on="_mce_decision_time",
        right_on=f"{prefix}available_at",
        tolerance=tolerance,
    )
    out[f"{prefix}age_seconds"] = (
        out["_mce_decision_time"] - out[f"{prefix}available_at"]
    ).dt.total_seconds()

    if not context.sectors.empty and ticker_col in out:
        if sector_col in out:
            candidate_sector = out[sector_col].astype("string").str.upper().str.strip()
            candidate_sector = candidate_sector.mask(candidate_sector.isin({"", "NAN", "NONE"}))
        else:
            candidate_sector = pd.Series(np.nan, index=out.index, dtype="object")
        if sector_map:
            normalised_map = {
                str(k).upper().strip(): str(v).upper().strip() for k, v in sector_map.items()
            }
            candidate_sector = candidate_sector.fillna(
                out[ticker_col].astype(str).str.upper().str.strip().map(normalised_map)
            )
        out["_mce_sector"] = candidate_sector
        sector_right = context.sectors.copy()
        sector_right["available_at"] = _to_timezone(sector_right["available_at"], timezone)
        sector_columns = [
            column
            for column in sector_right.columns
            if column not in {"timestamp", "available_at", "feature_version"}
        ]
        # Grouped as-of joins are small (candidate rows only) and avoid pandas'
        # global-sort edge cases when ``by=sector`` is used with many groups.
        joined_parts: list[pd.DataFrame] = []
        for sector, group in out.groupby("_mce_sector", sort=False, dropna=False):
            one = group.copy()
            if pd.isna(sector):
                for column in sector_columns:
                    if column != "sector":
                        one[f"{prefix}{column}"] = np.nan
                joined_parts.append(one)
                continue
            available = sector_right.loc[sector_right["sector"].eq(sector)].copy()
            if available.empty:
                for column in sector_columns:
                    if column != "sector":
                        one[f"{prefix}{column}"] = np.nan
                joined_parts.append(one)
                continue
            keep = ["available_at"] + [c for c in sector_columns if c != "sector"]
            available = available[keep].rename(
                columns={c: f"{prefix}{c}" for c in keep if c != "available_at"}
            )
            available = available.rename(columns={"available_at": "_mce_sector_available_at"})
            one = _backward_asof_preserve_missing(
                one,
                available,
                left_on="_mce_decision_time",
                right_on="_mce_sector_available_at",
                tolerance=tolerance,
            )
            joined_parts.append(one)
        out = pd.concat(joined_parts, ignore_index=True) if joined_parts else out

    out = out.sort_values("_mce_row_order")
    original_index = pd.Index(out.pop("_mce_original_index"), name=candidates.index.name)
    out = out.drop(
        columns=["_mce_row_order", "_mce_decision_time", "_mce_sector", "_mce_sector_available_at"],
        errors="ignore",
    )
    out.index = original_index
    return out


def context_feature_columns(
    context: MarketContextResult,
    *,
    include_quality_metadata: bool = False,
) -> list[str]:
    """Return numeric ML-ready names, excluding timestamps and labels."""

    excluded = {
        "timestamp",
        "available_at",
        "feature_version",
        "trend_regime",
        "intraday_volatility_regime",
        "rotation_regime",
        "risk_on_off_regime",
        "leading_sector",
        "lagging_sector",
    }
    if not include_quality_metadata:
        excluded |= {
            "universe_observed",
            "universe_expected",
            "price_eligible_count",
            "fresh_price_eligible_count",
            "flow_eligible_count",
            "previous_close_valid_count",
            "sector_mapped_count",
            "above_vwap_valid_count",
            "above_ema20_valid_count",
            "above_ema50_valid_count",
            "new_high_low_valid_count",
            "market_coverage",
            "market_ltp_coverage",
            "advance_decline_coverage",
            "vwap_breadth_coverage",
            "ema20_breadth_coverage",
            "ema50_breadth_coverage",
            "new_high_low_coverage",
            "sector_mapping_coverage",
            "context_complete",
            "index_context_count",
            "index_context_ready_count",
            "sector_count",
            "volatility_baseline_observations",
            "volatility_baseline_ready",
            "rotation_baseline_observations",
            "rotation_baseline_ready",
            "risk_context_component_count",
            "nifty_source_ready",
            "bank_nifty_source_ready",
            "midcap_source_ready",
        }
    return [
        column
        for column in context.market.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(context.market[column])
    ]


__all__ = [
    "FEATURE_VERSION",
    "FORBIDDEN_OUTPUT_COLUMNS",
    "MarketContextConfig",
    "MarketContextEngine",
    "MarketContextResult",
    "attach_context_asof",
    "context_feature_columns",
    "load_sector_map",
]
