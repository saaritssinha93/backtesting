"""Causal, numerical Sector Intelligence for EQID intraday models.

The module consumes completed five-minute stock bars and produces two feature
tables:

* ``sectors`` -- one row per timestamp and sector;
* ``stocks`` -- one row per timestamp and stock, measured relative to its
  point-in-time sector.

Identifiers and publication metadata are retained as join keys.  Every other
output is numeric.  There is deliberately no side, signal, order, entry, exit,
stop, target, sizing, portfolio, or trade-selection API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

try:  # Package import.
    from .market_context_engine import (
        IST,
        MarketContextConfig,
        _prepare_stock_panel,
    )
except ImportError:  # Direct module import used by the repository's tests/jobs.
    from market_context_engine import (  # type: ignore
        IST,
        MarketContextConfig,
        _prepare_stock_panel,
    )


SECTOR_INTELLIGENCE_VERSION = "eqidv2_sector_intelligence_v1"

IDENTIFIER_COLUMNS = frozenset(
    {"timestamp", "available_at", "feature_version", "ticker", "sector"}
)

FORBIDDEN_FEATURE_TOKENS = frozenset(
    {
        "buy",
        "sell",
        "side",
        "signal",
        "trade",
        "entry",
        "exit",
        "stop",
        "target",
        "quantity",
        "position",
        "order",
        "sizing",
    }
)

# Immutable ordered schemas.  Training and live inference must never infer a
# model schema from whichever sectors happened to be present in one batch.
SECTOR_FEATURE_COLUMNS = (
    "sector_member_count",
    "sector_price_eligible_count",
    "sector_fresh_price_eligible_count",
    "sector_flow_eligible_count",
    "sector_momentum_valid_count",
    "sector_rvol_valid_count",
    "sector_return_5m_valid_count",
    "sector_above_vwap_valid_count",
    "sector_above_ema20_valid_count",
    "sector_above_ema50_valid_count",
    "sector_new_high_low_valid_count",
    "sector_advance_count",
    "sector_decline_count",
    "sector_unchanged_count",
    "sector_previous_close_valid_count",
    "sector_relative_volume",
    "sector_relative_turnover",
    "sector_relative_turnover_valid_count",
    "sector_amihud_impact",
    "sector_amihud_valid_count",
    "sector_active_volume_count",
    "sector_expected_member_count",
    "sector_expected_member_source_code",
    "sector_expected_members_valid_flag",
    "market_expected_member_count",
    "market_momentum_valid_count",
    "market_momentum_coverage_pct",
    "market_momentum_ready_flag",
    "cross_sector_expected_count",
    "cross_sector_reliable_count",
    "cross_sector_reliable_coverage_pct",
    "cross_sector_ready_flag",
    "sector_data_coverage_pct",
    "sector_momentum_coverage_pct",
    "sector_rvol_coverage_pct",
    "sector_flow_coverage_pct",
    "sector_relative_turnover_coverage_pct",
    "sector_amihud_coverage_pct",
    "sector_return_5m_coverage_pct",
    "sector_reliable_flag",
    "sector_momentum_ready_flag",
    "sector_rvol_ready_flag",
    "sector_flow_ready_flag",
    "sector_relative_turnover_ready_flag",
    "sector_amihud_ready_flag",
    "sector_return_5m_ready_flag",
    "sector_previous_close_coverage_pct",
    "sector_previous_close_ready_flag",
    "sector_above_vwap_coverage_pct",
    "sector_above_vwap_ready_flag",
    "sector_above_ema20_coverage_pct",
    "sector_above_ema20_ready_flag",
    "sector_above_ema50_coverage_pct",
    "sector_above_ema50_ready_flag",
    "sector_new_high_low_coverage_pct",
    "sector_new_high_low_ready_flag",
    "sector_momentum_30m_pct",
    "sector_intraday_return_pct",
    "sector_return_dispersion_5m_bps",
    "sector_pct_above_vwap",
    "sector_pct_above_ema20",
    "sector_pct_above_ema50",
    "sector_pct_new_intraday_highs",
    "sector_pct_new_intraday_lows",
    "sector_total_turnover_crore",
    "sector_median_turnover_lakh",
    "sector_advance_decline_net",
    "sector_breadth",
    "sector_breadth_component_count",
    "sector_trend_score",
    "sector_trend_component_count",
    "sector_trend_return_component_count",
    "sector_trend_ready_flag",
    "sector_volatility_60m_bps",
    "sector_acceleration_30m_pct",
    "sector_relative_momentum_pct",
    "sector_relative_strength_score",
    "sector_acceleration_score",
    "sector_liquidity_component_count",
    "sector_liquidity_ready_flag",
    "sector_liquidity_score",
    "sector_liquidity_percentile",
    "sector_strength_component_count",
    "sector_strength_ready_flag",
    "sector_strength_score",
    "sector_strength_rank",
    "sector_strength_percentile",
    "sector_strength_percentile_turnover_pct_points",
    "sector_participation_pct",
    "sector_expected_member_support_pct",
    "sector_active_aligned_support_pct",
    "sector_signed_participation_pct",
    "sector_leader_stock_count",
    "sector_weakest_stock_count",
    "sector_leader_score_mean",
    "sector_weakest_score_mean",
    "sector_leader_momentum_30m_pct",
    "sector_weakest_momentum_30m_pct",
    "sector_leadership_spread_30m_pct",
)

STOCK_FEATURE_COLUMNS = (
    "stock_fresh_price_flag",
    "stock_sector_reliable_flag",
    "stock_sector_momentum_ready_flag",
    "stock_peer_momentum_valid_count",
    "stock_peer_intraday_valid_count",
    "stock_peer_vwap_valid_count",
    "stock_peer_rvol_valid_count",
    "stock_outperformance_component_count",
    "stock_leadership_component_count",
    "stock_sector_percentile",
    "stock_distance_from_sector_average_pct",
    "stock_relative_momentum_30m_pct",
    "stock_outperformance_score",
    "stock_sector_leadership_score",
    "stock_sector_leadership_percentile",
    "stock_is_sector_leader",
    "stock_is_sector_weakest",
    "stock_sector_mapped_flag",
)

# Diagnostics needed to decide whether a row is trainable/live-ready.  They are
# part of the immutable output schema, but are excluded from the default model
# feature list so that coverage artifacts and feed health do not become hidden
# alpha inputs.  Requested numerical summaries such as leader/weakest counts
# intentionally remain model candidates.
QUALITY_FEATURE_COLUMNS = frozenset(
    {
        "sector_member_count",
        "sector_price_eligible_count",
        "sector_fresh_price_eligible_count",
        "sector_flow_eligible_count",
        "sector_momentum_valid_count",
        "sector_rvol_valid_count",
        "sector_return_5m_valid_count",
        "sector_above_vwap_valid_count",
        "sector_above_ema20_valid_count",
        "sector_above_ema50_valid_count",
        "sector_new_high_low_valid_count",
        "sector_advance_count",
        "sector_decline_count",
        "sector_unchanged_count",
        "sector_previous_close_valid_count",
        "sector_relative_turnover_valid_count",
        "sector_amihud_valid_count",
        "sector_active_volume_count",
        "sector_expected_member_count",
        "sector_expected_member_source_code",
        "sector_expected_members_valid_flag",
        "market_expected_member_count",
        "market_momentum_valid_count",
        "market_momentum_coverage_pct",
        "market_momentum_ready_flag",
        "cross_sector_expected_count",
        "cross_sector_reliable_count",
        "cross_sector_reliable_coverage_pct",
        "cross_sector_ready_flag",
        "sector_data_coverage_pct",
        "sector_momentum_coverage_pct",
        "sector_rvol_coverage_pct",
        "sector_flow_coverage_pct",
        "sector_relative_turnover_coverage_pct",
        "sector_amihud_coverage_pct",
        "sector_return_5m_coverage_pct",
        "sector_reliable_flag",
        "sector_momentum_ready_flag",
        "sector_rvol_ready_flag",
        "sector_flow_ready_flag",
        "sector_relative_turnover_ready_flag",
        "sector_amihud_ready_flag",
        "sector_return_5m_ready_flag",
        "sector_previous_close_coverage_pct",
        "sector_previous_close_ready_flag",
        "sector_above_vwap_coverage_pct",
        "sector_above_vwap_ready_flag",
        "sector_above_ema20_coverage_pct",
        "sector_above_ema20_ready_flag",
        "sector_above_ema50_coverage_pct",
        "sector_above_ema50_ready_flag",
        "sector_new_high_low_coverage_pct",
        "sector_new_high_low_ready_flag",
        "sector_breadth_component_count",
        "sector_trend_component_count",
        "sector_trend_return_component_count",
        "sector_trend_ready_flag",
        "sector_liquidity_component_count",
        "sector_liquidity_ready_flag",
        "sector_strength_component_count",
        "sector_strength_ready_flag",
        "stock_fresh_price_flag",
        "stock_sector_reliable_flag",
        "stock_sector_momentum_ready_flag",
        "stock_peer_momentum_valid_count",
        "stock_peer_intraday_valid_count",
        "stock_peer_vwap_valid_count",
        "stock_peer_rvol_valid_count",
        "stock_outperformance_component_count",
        "stock_leadership_component_count",
        "stock_sector_mapped_flag",
    }
)


@dataclass(frozen=True)
class SectorIntelligenceConfig(MarketContextConfig):
    """Sector-specific settings layered on the shared causal bar contract."""

    sector_volatility_bars: int = 12
    sector_acceleration_bars: int = 6
    min_sector_data_coverage: float = 0.70
    expected_sector_count: int | None = None
    min_cross_sector_coverage: float = 0.70
    leader_percentile_threshold: float = 0.90

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.bar_minutes != 5:
            raise ValueError("Sector Intelligence requires completed 5-minute bars")
        if self.ema_fast_span != 20 or self.ema_slow_span != 50:
            raise ValueError("Sector Intelligence EMA spans must remain 20 and 50")
        if int(self.sector_volatility_bars) <= 0:
            raise ValueError("sector_volatility_bars must be positive")
        if int(self.sector_acceleration_bars) <= 0:
            raise ValueError("sector_acceleration_bars must be positive")
        if self.sector_volatility_bars * self.bar_minutes != 60:
            raise ValueError("sector_volatility_bars must span exactly 60 minutes")
        if self.sector_acceleration_bars * self.bar_minutes != 30:
            raise ValueError("sector_acceleration_bars must span exactly 30 minutes")
        if self.sector_momentum_bars * self.bar_minutes != 30:
            raise ValueError("sector_momentum_bars must span exactly 30 minutes")
        if not 0.0 <= float(self.min_sector_data_coverage) <= 1.0:
            raise ValueError("min_sector_data_coverage must be in [0, 1]")
        if self.expected_sector_count is not None and int(self.expected_sector_count) <= 0:
            raise ValueError("expected_sector_count must be positive when provided")
        if not 0.0 <= float(self.min_cross_sector_coverage) <= 1.0:
            raise ValueError("min_cross_sector_coverage must be in [0, 1]")
        if not 0.5 < float(self.leader_percentile_threshold) <= 1.0:
            raise ValueError("leader_percentile_threshold must be in (0.5, 1]")


@dataclass(frozen=True)
class SectorIntelligenceResult:
    sectors: pd.DataFrame
    stocks: pd.DataFrame


def _to_timezone(values: pd.Series, timezone: str) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if getattr(parsed.dt, "tz", None) is None:
        return parsed.dt.tz_localize(
            timezone, ambiguous="NaT", nonexistent="shift_forward"
        )
    return parsed.dt.tz_convert(timezone)


def _finite_weighted_average(
    values: Sequence[np.ndarray | pd.Series],
    weights: Sequence[float],
) -> np.ndarray:
    matrix = np.column_stack([np.asarray(value, dtype="float64") for value in values])
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


def _safe_zscore(values: pd.Series) -> pd.Series:
    """Robust within-group score with a standard-deviation fallback.

    A constant observed cross-section is neutral (zero); an unavailable value
    remains missing.  The fallback retains information when a discrete cross-
    section has zero MAD but non-zero standard deviation.
    """

    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(np.nan, index=values.index, dtype="float64")
    median = valid.median()
    mad_scale = 1.4826 * (valid - median).abs().median()
    if np.isfinite(mad_scale) and mad_scale > 1e-12:
        z = (numeric - median) / mad_scale
    else:
        mean = valid.mean()
        std = valid.std(ddof=0)
        if not np.isfinite(std) or std <= 1e-12:
            return pd.Series(0.0, index=values.index).where(numeric.notna())
        z = (numeric - mean) / std
    return z.clip(-5.0, 5.0).where(numeric.notna())


def _scaled_mad(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return np.nan
    median = numeric.median()
    return float(1.4826 * (numeric - median).abs().median())


def _numeric_sum_min_count(values: pd.Series) -> float:
    return float(pd.to_numeric(values, errors="coerce").sum(min_count=1))


def _neutral_midrank_percentile(values: pd.Series) -> pd.Series:
    """Ascending average-rank percentile with neutral handling for ties."""

    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    valid_count = int(numeric.notna().sum())
    result = pd.Series(np.nan, index=values.index, dtype="float64")
    if valid_count == 0:
        return result
    valid = numeric.dropna()
    if valid_count == 1 or float(valid.max() - valid.min()) <= 1e-12:
        result.loc[valid.index] = 0.5
        return result
    ranks = numeric.rank(method="average", ascending=True)
    result.loc[valid.index] = (ranks.loc[valid.index] - 1.0) / (valid_count - 1.0)
    return result


def _grouped_safe_zscore(
    values: pd.Series,
    groups: list[pd.Series],
) -> pd.Series:
    """Vectorised robust z-score for many small cross-sectional groups."""

    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    grouped = numeric.groupby(groups, sort=False, observed=True)
    median = grouped.transform("median")
    absolute_deviation = (numeric - median).abs()
    mad_scale = 1.4826 * absolute_deviation.groupby(
        groups, sort=False, observed=True
    ).transform("median")
    mean = grouped.transform("mean")
    std = grouped.transform("std", ddof=0)
    use_mad = mad_scale.gt(1e-12) & np.isfinite(mad_scale)
    use_std = ~use_mad & std.gt(1e-12) & np.isfinite(std)
    center = median.where(use_mad, mean)
    scale = mad_scale.where(use_mad, std.where(use_std))
    z = (numeric - center).div(scale)
    constant = numeric.notna() & ~use_mad & ~use_std
    z = z.where(~constant, 0.0)
    return z.clip(-5.0, 5.0).where(numeric.notna())


def _grouped_midrank_percentile(
    values: pd.Series,
    groups: list[pd.Series],
) -> pd.Series:
    """Vectorised ascending average-rank percentile with neutral ties."""

    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    grouped = numeric.groupby(groups, sort=False, observed=True)
    count = grouped.transform("count")
    ranks = grouped.rank(method="average", ascending=True)
    spread = grouped.transform("max") - grouped.transform("min")
    percentile = (ranks - 1.0).div(count - 1.0)
    neutral = numeric.notna() & (count.eq(1) | spread.le(1e-12))
    return percentile.where(~neutral, 0.5).where(numeric.notna())


def _grouped_valid_count(
    values: pd.Series,
    groups: list[pd.Series],
) -> pd.Series:
    valid = pd.to_numeric(values, errors="coerce").notna().astype("int32")
    return valid.groupby(groups, sort=False, observed=True).transform("sum")


def _exact_log_return(
    panel: pd.DataFrame,
    bars: int,
    bar_minutes: int,
) -> pd.Series:
    """Log return from the exact prior timestamp, never a row-count lag."""

    lookup_index = pd.MultiIndex.from_arrays([panel["_ticker"], panel["_timestamp"]])
    lookup = pd.Series(
        panel["close"].where(panel["_price_eligible"]).to_numpy(),
        index=lookup_index,
    )
    lag_index = pd.MultiIndex.from_arrays(
        [
            panel["_ticker"],
            panel["_timestamp"] - pd.Timedelta(minutes=bars * bar_minutes),
        ]
    )
    lagged = pd.Series(lookup.reindex(lag_index).to_numpy(), index=panel.index)
    result = np.log(panel["close"].div(lagged.where(lagged.gt(0))))
    return result.where(panel["_fresh_price_eligible"])


def _leave_one_out_mean(values: pd.Series, groups: list[pd.Series]) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    grouped = numeric.groupby(groups, sort=False, observed=True)
    total = grouped.transform("sum")
    count = grouped.transform("count")
    return (total - numeric).div(count - 1.0).where(numeric.notna() & count.gt(1))


def _rolling_exact_sum(
    frame: pd.DataFrame,
    value_col: str,
    bars: int,
    bar_minutes: int,
) -> pd.Series:
    keys = [frame["sector"], frame["_session"]]
    value = pd.to_numeric(frame[value_col], errors="coerce")
    rolled = value.groupby(keys, sort=False, observed=True).transform(
        lambda series: series.rolling(bars, min_periods=bars).sum()
    )
    if bars == 1:
        return rolled
    prior_timestamp = frame["timestamp"].groupby(keys, sort=False, observed=True).shift(
        bars - 1
    )
    exact = frame["timestamp"].sub(prior_timestamp).eq(
        pd.Timedelta(minutes=(bars - 1) * bar_minutes)
    )
    return rolled.where(exact)


def _exact_group_shift(
    frame: pd.DataFrame,
    value_col: str,
    bars: int,
    bar_minutes: int,
) -> pd.Series:
    keys = [frame["sector"], frame["_session"]]
    lagged = frame[value_col].groupby(keys, sort=False, observed=True).shift(bars)
    prior_timestamp = frame["timestamp"].groupby(keys, sort=False, observed=True).shift(
        bars
    )
    exact = frame["timestamp"].sub(prior_timestamp).eq(
        pd.Timedelta(minutes=bars * bar_minutes)
    )
    return pd.to_numeric(lagged, errors="coerce").where(exact)


def _assert_numeric_feature_contract(frame: pd.DataFrame) -> None:
    feature_columns = [column for column in frame.columns if column not in IDENTIFIER_COLUMNS]
    forbidden = sorted(
        column
        for column in feature_columns
        if any(token in str(column).lower() for token in FORBIDDEN_FEATURE_TOKENS)
    )
    if forbidden:
        raise AssertionError(f"sector intelligence acquired execution semantics: {forbidden}")
    non_numeric = [
        column
        for column in feature_columns
        if not pd.api.types.is_numeric_dtype(frame[column])
    ]
    if non_numeric:
        raise AssertionError(f"non-numeric sector intelligence features: {non_numeric}")
    if feature_columns:
        numeric = frame[feature_columns].apply(pd.to_numeric, errors="coerce")
        if np.isinf(numeric.to_numpy(dtype="float64")).any():
            raise AssertionError("sector intelligence contains infinite feature values")


def _empty_feature_frame(
    key_columns: Sequence[str],
    feature_columns: Sequence[str],
) -> pd.DataFrame:
    data: dict[str, pd.Series] = {
        "timestamp": pd.Series(dtype=f"datetime64[ns, {IST}]"),
    }
    for column in key_columns:
        if column != "timestamp":
            data[column] = pd.Series(dtype="string")
    for column in feature_columns:
        data[column] = pd.Series(dtype="float64")
    data["available_at"] = pd.Series(dtype=f"datetime64[ns, {IST}]")
    data["feature_version"] = pd.Series(dtype="string")
    return pd.DataFrame(data)


def _empty_result() -> SectorIntelligenceResult:
    return SectorIntelligenceResult(
        sectors=_empty_feature_frame(
            ("timestamp", "sector"), SECTOR_FEATURE_COLUMNS
        ),
        stocks=_empty_feature_frame(
            ("timestamp", "ticker", "sector"), STOCK_FEATURE_COLUMNS
        ),
    )


def _finalize_feature_frame(
    frame: pd.DataFrame,
    *,
    key_columns: Sequence[str],
    feature_columns: Sequence[str],
    config: SectorIntelligenceConfig,
) -> pd.DataFrame:
    out = frame.copy()
    missing_keys = set(key_columns) - set(out.columns)
    if missing_keys:
        raise AssertionError(f"sector intelligence missing keys: {sorted(missing_keys)}")
    allowed = set(key_columns) | set(feature_columns) | {"available_at", "feature_version"}
    unexpected = [column for column in out.columns if column not in allowed]
    if unexpected:
        raise AssertionError(f"unexpected sector intelligence columns: {unexpected}")
    for column in feature_columns:
        if column not in out:
            out[column] = np.nan
        out[column] = pd.to_numeric(out[column], errors="coerce").replace(
            [np.inf, -np.inf], np.nan
        )
    out["available_at"] = out["timestamp"] + pd.Timedelta(
        seconds=config.publish_delay_seconds
    )
    out["feature_version"] = SECTOR_INTELLIGENCE_VERSION
    ordered = [*key_columns, *feature_columns, "available_at", "feature_version"]
    out = out[ordered]
    _assert_numeric_feature_contract(out)
    return out


def _finalize_result(
    sectors: pd.DataFrame,
    stocks: pd.DataFrame,
    config: SectorIntelligenceConfig,
) -> SectorIntelligenceResult:
    return SectorIntelligenceResult(
        sectors=_finalize_feature_frame(
            sectors,
            key_columns=("timestamp", "sector"),
            feature_columns=SECTOR_FEATURE_COLUMNS,
            config=config,
        ).sort_values(["timestamp", "sector"]).reset_index(drop=True),
        stocks=_finalize_feature_frame(
            stocks,
            key_columns=("timestamp", "ticker", "sector"),
            feature_columns=STOCK_FEATURE_COLUMNS,
            config=config,
        ).sort_values(["timestamp", "ticker"]).reset_index(drop=True),
    )


def _compute_sector_intelligence_from_panel(
    panel: pd.DataFrame,
    config: SectorIntelligenceConfig,
) -> SectorIntelligenceResult:
    if panel is None or panel.empty:
        return _empty_result()

    work = panel.sort_values(["_timestamp", "_ticker"]).copy()
    for column in ("_ema_fast", "_ema_slow"):
        numeric = pd.to_numeric(work[column], errors="coerce").replace(
            [np.inf, -np.inf], np.nan
        )
        work[column] = numeric.where(numeric.gt(0))
    cleaned_rvol = pd.to_numeric(work["_stock_rvol"], errors="coerce").replace(
        [np.inf, -np.inf], np.nan
    )
    work["_stock_rvol"] = cleaned_rvol.where(cleaned_rvol.ge(0))
    work["_stock_return_5m"] = _exact_log_return(work, 1, config.bar_minutes)
    work["_stock_return_15m"] = _exact_log_return(work, 3, config.bar_minutes)
    work["_stock_momentum_30m"] = pd.to_numeric(
        work["_stock_momentum"], errors="coerce"
    ).where(work["_fresh_price_eligible"])
    work["_stock_intraday_return"] = pd.to_numeric(
        work["_stock_day_return"], errors="coerce"
    ).where(work["_fresh_price_eligible"])

    valid_previous_close = work["_fresh_price_eligible"] & work["_previous_close"].gt(0)
    work["_advance"] = work["close"].gt(work["_previous_close"]).where(valid_previous_close)
    work["_decline"] = work["close"].lt(work["_previous_close"]).where(valid_previous_close)
    work["_unchanged"] = work["close"].eq(work["_previous_close"]).where(
        valid_previous_close
    )
    work["_above_vwap"] = work["close"].gt(work["_session_vwap"]).where(
        work["_fresh_price_eligible"] & work["_session_vwap"].notna()
    )
    work["_above_ema20"] = work["close"].gt(work["_ema_fast"]).where(
        work["_fresh_price_eligible"] & work["_ema_fast"].notna()
    )
    work["_above_ema50"] = work["close"].gt(work["_ema_slow"]).where(
        work["_fresh_price_eligible"] & work["_ema_slow"].notna()
    )
    work["_new_high"] = work["_new_intraday_high"].where(
        work["_fresh_price_eligible"]
    )
    work["_new_low"] = work["_new_intraday_low"].where(
        work["_fresh_price_eligible"]
    )

    work["_rupee_turnover"] = (work["close"] * work["volume"]).where(
        work["_flow_eligible"]
    )
    turnover_groups = [work["_ticker"], work["_bar_slot"]]
    prior_turnover = work["_rupee_turnover"].groupby(
        turnover_groups, sort=False, observed=True
    ).shift(1)
    turnover_baseline = (
        prior_turnover.groupby(turnover_groups, sort=False, observed=True)
        .rolling(
            config.relative_volume_sessions,
            min_periods=config.relative_volume_min_sessions,
        )
        .median()
        .reset_index(level=[0, 1], drop=True)
        .reindex(work.index)
    )
    work["_relative_turnover"] = work["_rupee_turnover"].div(
        turnover_baseline.replace(0.0, np.nan)
    )
    turnover_million = work["_rupee_turnover"].div(1_000_000.0)
    work["_amihud_impact"] = work["_stock_return_5m"].abs().div(
        turnover_million.replace(0.0, np.nan)
    )
    work["_vwap_log_distance"] = np.log(
        work["close"].div(work["_session_vwap"].where(work["_session_vwap"].gt(0)))
    ).where(work["_fresh_price_eligible"])

    mapped = work.loc[work["_sector_mapped"]].copy()
    if mapped.empty:
        stocks = work[["_timestamp", "_ticker", "_fresh_price_eligible"]].rename(
            columns={
                "_timestamp": "timestamp",
                "_ticker": "ticker",
                "_fresh_price_eligible": "stock_fresh_price_flag",
            }
        )
        stocks["sector"] = pd.Series(pd.NA, index=stocks.index, dtype="string")
        stocks["stock_sector_mapped_flag"] = np.int8(0)
        stocks["stock_sector_reliable_flag"] = np.int8(0)
        stocks["stock_sector_momentum_ready_flag"] = np.int8(0)
        for column in (
            "stock_peer_momentum_valid_count",
            "stock_peer_intraday_valid_count",
            "stock_peer_vwap_valid_count",
            "stock_peer_rvol_valid_count",
            "stock_outperformance_component_count",
            "stock_leadership_component_count",
        ):
            stocks[column] = np.nan
        for column in (
            "stock_sector_percentile",
            "stock_distance_from_sector_average_pct",
            "stock_relative_momentum_30m_pct",
            "stock_outperformance_score",
            "stock_sector_leadership_score",
            "stock_sector_leadership_percentile",
            "stock_is_sector_leader",
            "stock_is_sector_weakest",
        ):
            stocks[column] = np.nan
        return _finalize_result(_empty_result().sectors, stocks, config)

    mapped["_fresh_member"] = mapped["_fresh_price_eligible"].astype("int8")
    mapped["_momentum_valid"] = mapped["_stock_momentum_30m"].notna().astype("int8")
    stock_rvol = pd.to_numeric(mapped["_stock_rvol"], errors="coerce")
    mapped["_rvol_valid"] = (mapped["_flow_eligible"] & stock_rvol.notna()).astype(
        "int8"
    )
    mapped["_active_volume"] = stock_rvol.ge(1.0).astype("float64").where(
        mapped["_rvol_valid"].eq(1)
    )
    mapped["_turnover_log"] = np.log1p(mapped["_rupee_turnover"].clip(lower=0.0))

    grouped = mapped.groupby(["_timestamp", "sector"], sort=True, observed=True)
    sectors = grouped.agg(
        sector_member_count=("_ticker", "nunique"),
        sector_price_eligible_count=("_price_eligible", "sum"),
        sector_fresh_price_eligible_count=("_fresh_price_eligible", "sum"),
        sector_flow_eligible_count=("_flow_eligible", "sum"),
        sector_momentum_valid_count=("_momentum_valid", "sum"),
        sector_rvol_valid_count=("_rvol_valid", "sum"),
        sector_momentum_30m=("_stock_momentum_30m", "median"),
        sector_directional_momentum_30m=("_stock_momentum_30m", "mean"),
        sector_intraday_return=("_stock_intraday_return", "median"),
        sector_bar_return_5m=("_stock_return_5m", "median"),
        sector_return_5m_valid_count=("_stock_return_5m", "count"),
        sector_return_dispersion_5m=(
            "_stock_return_5m",
            _scaled_mad,
        ),
        sector_fraction_above_vwap=("_above_vwap", "mean"),
        sector_fraction_above_ema20=("_above_ema20", "mean"),
        sector_fraction_above_ema50=("_above_ema50", "mean"),
        sector_fraction_new_highs=("_new_high", "mean"),
        sector_fraction_new_lows=("_new_low", "mean"),
        sector_above_vwap_valid_count=("_above_vwap", "count"),
        sector_above_ema20_valid_count=("_above_ema20", "count"),
        sector_above_ema50_valid_count=("_above_ema50", "count"),
        sector_new_high_low_valid_count=("_new_high", "count"),
        sector_advance_count=("_advance", "sum"),
        sector_decline_count=("_decline", "sum"),
        sector_unchanged_count=("_unchanged", "sum"),
        sector_previous_close_valid_count=("_advance", "count"),
        sector_relative_volume=("_stock_rvol", "median"),
        sector_relative_turnover=("_relative_turnover", "median"),
        sector_relative_turnover_valid_count=("_relative_turnover", "count"),
        sector_total_turnover_rupees=("_rupee_turnover", _numeric_sum_min_count),
        sector_median_turnover_rupees=("_rupee_turnover", "median"),
        sector_median_log_turnover=("_turnover_log", "median"),
        sector_amihud_impact=("_amihud_impact", "median"),
        sector_amihud_valid_count=("_amihud_impact", "count"),
        sector_active_volume_count=("_active_volume", _numeric_sum_min_count),
    ).reset_index()

    explicit_expected = pd.Series(np.nan, index=sectors.index, dtype="float64")
    expected_source_code = pd.Series(np.int8(0), index=sectors.index)
    sector_key = pd.MultiIndex.from_frame(sectors[["_timestamp", "sector"]])
    for column, valid_source_code in (
        ("sector_expected_members", 2),
        ("_sector_expected_members", 1),
    ):
        if column not in mapped:
            continue
        expected_stats = (
            mapped.assign(_expected=pd.to_numeric(mapped[column], errors="coerce"))
            .groupby(["_timestamp", "sector"], sort=True, observed=True)["_expected"]
            .agg(["min", "max", "count"])
            .reindex(sector_key)
        )
        expected_min = pd.Series(expected_stats["min"].to_numpy(), index=sectors.index)
        expected_max = pd.Series(expected_stats["max"].to_numpy(), index=sectors.index)
        expected_count = pd.Series(expected_stats["count"].to_numpy(), index=sectors.index)
        provided = expected_count.gt(0)
        consistent = (
            provided
            & expected_min.eq(expected_max)
            & expected_max.gt(0)
            & expected_max.sub(expected_max.round()).abs().le(1e-9)
            & expected_max.ge(sectors["sector_member_count"])
        )
        unassigned = expected_source_code.eq(0)
        accept = unassigned & consistent
        invalid = unassigned & provided & ~consistent
        explicit_expected.loc[accept] = expected_max.loc[accept]
        expected_source_code.loc[accept] = np.int8(valid_source_code)
        expected_source_code.loc[invalid] = np.int8(-1)
    sectors["_explicit_expected_members"] = explicit_expected.to_numpy()
    sectors["_expected_member_source_code"] = expected_source_code.to_numpy()
    sectors = sectors.sort_values(["sector", "_timestamp"]).reset_index(drop=True)
    explicit_expected = sectors.pop("_explicit_expected_members")
    expected_source_code = sectors.pop("_expected_member_source_code")
    causal_expected = sectors.groupby("sector", sort=False, observed=True)[
        "sector_member_count"
    ].cummax()
    sectors["sector_expected_member_count"] = explicit_expected.fillna(causal_expected)
    sectors["sector_expected_member_source_code"] = expected_source_code.astype("int8")
    sectors["sector_expected_members_valid_flag"] = expected_source_code.gt(0).astype(
        "int8"
    )
    sectors["sector_data_coverage_pct"] = 100.0 * sectors[
        "sector_fresh_price_eligible_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_momentum_coverage_pct"] = 100.0 * sectors[
        "sector_momentum_valid_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_rvol_coverage_pct"] = 100.0 * sectors[
        "sector_rvol_valid_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_flow_coverage_pct"] = 100.0 * sectors[
        "sector_flow_eligible_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_relative_turnover_coverage_pct"] = 100.0 * sectors[
        "sector_relative_turnover_valid_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_amihud_coverage_pct"] = 100.0 * sectors[
        "sector_amihud_valid_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_return_5m_coverage_pct"] = 100.0 * sectors[
        "sector_return_5m_valid_count"
    ].div(sectors["sector_expected_member_count"].replace(0.0, np.nan))
    sectors["sector_reliable_flag"] = (
        sectors["sector_fresh_price_eligible_count"].ge(config.min_sector_members)
        & sectors["sector_data_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
        & sectors["sector_expected_member_source_code"].ne(-1)
    ).astype("int8")
    reliable = sectors["sector_reliable_flag"].eq(1)
    sectors["sector_momentum_ready_flag"] = (
        sectors["sector_momentum_valid_count"].ge(config.min_sector_members)
        & sectors["sector_momentum_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
    ).astype("int8")
    momentum_ready = reliable & sectors["sector_momentum_ready_flag"].eq(1)
    sectors["sector_rvol_ready_flag"] = (
        sectors["sector_rvol_valid_count"].ge(config.min_sector_members)
        & sectors["sector_rvol_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
    ).astype("int8")
    rvol_ready = reliable & sectors["sector_rvol_ready_flag"].eq(1)
    sectors["sector_flow_ready_flag"] = (
        sectors["sector_flow_eligible_count"].ge(config.min_sector_members)
        & sectors["sector_flow_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
    ).astype("int8")
    flow_ready = reliable & sectors["sector_flow_ready_flag"].eq(1)
    sectors["sector_relative_turnover_ready_flag"] = (
        sectors["sector_relative_turnover_valid_count"].ge(config.min_sector_members)
        & sectors["sector_relative_turnover_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
    ).astype("int8")
    relative_turnover_ready = (
        reliable & sectors["sector_relative_turnover_ready_flag"].eq(1)
    )
    sectors["sector_amihud_ready_flag"] = (
        sectors["sector_amihud_valid_count"].ge(config.min_sector_members)
        & sectors["sector_amihud_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
    ).astype("int8")
    amihud_ready = reliable & sectors["sector_amihud_ready_flag"].eq(1)
    sectors["sector_return_5m_ready_flag"] = (
        sectors["sector_return_5m_valid_count"].ge(config.min_sector_members)
        & sectors["sector_return_5m_coverage_pct"].ge(
            100.0 * config.min_sector_data_coverage
        )
    ).astype("int8")
    return_5m_ready = reliable & sectors["sector_return_5m_ready_flag"].eq(1)

    component_contracts = (
        ("previous_close", "sector_previous_close_valid_count", None),
        ("above_vwap", "sector_above_vwap_valid_count", "sector_fraction_above_vwap"),
        ("above_ema20", "sector_above_ema20_valid_count", "sector_fraction_above_ema20"),
        ("above_ema50", "sector_above_ema50_valid_count", "sector_fraction_above_ema50"),
        ("new_high_low", "sector_new_high_low_valid_count", "sector_fraction_new_highs"),
    )
    component_ready: dict[str, pd.Series] = {}
    for name, count_column, fraction_column in component_contracts:
        coverage_column = f"sector_{name}_coverage_pct"
        ready_column = f"sector_{name}_ready_flag"
        sectors[coverage_column] = 100.0 * sectors[count_column].div(
            sectors["sector_expected_member_count"].replace(0.0, np.nan)
        )
        ready = (
            reliable
            & sectors[count_column].ge(config.min_sector_members)
            & sectors[coverage_column].ge(100.0 * config.min_sector_data_coverage)
        )
        sectors[ready_column] = ready.astype("int8")
        component_ready[name] = ready
        if fraction_column is not None:
            sectors.loc[~ready, fraction_column] = np.nan
    sectors.loc[
        ~component_ready["new_high_low"], "sector_fraction_new_lows"
    ] = np.nan
    sectors.loc[~rvol_ready, "sector_relative_volume"] = np.nan
    sectors.loc[
        ~flow_ready,
        [
            "sector_total_turnover_rupees",
            "sector_median_turnover_rupees",
            "sector_median_log_turnover",
        ],
    ] = np.nan
    sectors.loc[~relative_turnover_ready, "sector_relative_turnover"] = np.nan
    sectors.loc[~amihud_ready, "sector_amihud_impact"] = np.nan
    sectors.loc[
        ~return_5m_ready, ["sector_bar_return_5m", "sector_return_dispersion_5m"]
    ] = np.nan

    sectors["sector_momentum_30m_pct"] = 100.0 * sectors["sector_momentum_30m"]
    sectors.loc[~momentum_ready, "sector_momentum_30m_pct"] = np.nan
    sectors["sector_intraday_return_pct"] = 100.0 * sectors["sector_intraday_return"]
    sectors.loc[
        ~component_ready["previous_close"], "sector_intraday_return_pct"
    ] = np.nan
    sectors["sector_return_dispersion_5m_bps"] = (
        10_000.0 * sectors["sector_return_dispersion_5m"]
    )
    sectors["sector_pct_above_vwap"] = 100.0 * sectors["sector_fraction_above_vwap"]
    sectors["sector_pct_above_ema20"] = 100.0 * sectors["sector_fraction_above_ema20"]
    sectors["sector_pct_above_ema50"] = 100.0 * sectors["sector_fraction_above_ema50"]
    sectors["sector_pct_new_intraday_highs"] = 100.0 * sectors[
        "sector_fraction_new_highs"
    ]
    sectors["sector_pct_new_intraday_lows"] = 100.0 * sectors[
        "sector_fraction_new_lows"
    ]
    sectors["sector_total_turnover_crore"] = sectors[
        "sector_total_turnover_rupees"
    ].div(10_000_000.0)
    sectors["sector_median_turnover_lakh"] = sectors[
        "sector_median_turnover_rupees"
    ].div(100_000.0)

    ad_denominator = pd.to_numeric(
        sectors["sector_advance_count"], errors="coerce"
    ) + pd.to_numeric(sectors["sector_decline_count"], errors="coerce") + pd.to_numeric(
        sectors["sector_unchanged_count"], errors="coerce"
    )
    sectors["sector_advance_decline_net"] = (
        sectors["sector_advance_count"] - sectors["sector_decline_count"]
    ).div(ad_denominator.replace(0.0, np.nan))
    sectors.loc[
        ~component_ready["previous_close"], "sector_advance_decline_net"
    ] = np.nan
    breadth_components = [
        sectors["sector_advance_decline_net"],
        2.0 * sectors["sector_fraction_above_vwap"] - 1.0,
        2.0 * sectors["sector_fraction_above_ema20"] - 1.0,
        2.0 * sectors["sector_fraction_above_ema50"] - 1.0,
        sectors["sector_fraction_new_highs"] - sectors["sector_fraction_new_lows"],
    ]
    sectors["sector_breadth"] = _finite_weighted_average(
        breadth_components,
        [0.30, 0.25, 0.20, 0.15, 0.10],
    )
    sectors["sector_breadth_component_count"] = np.isfinite(
        np.column_stack([np.asarray(value, dtype="float64") for value in breadth_components])
    ).sum(axis=1)
    sectors.loc[
        ~reliable | sectors["sector_breadth_component_count"].lt(2),
        "sector_breadth",
    ] = np.nan

    sectors = sectors.rename(columns={"_timestamp": "timestamp"})
    sectors["_session"] = sectors["timestamp"].dt.strftime("%Y-%m-%d")
    sectors = sectors.sort_values(["sector", "timestamp"]).reset_index(drop=True)
    reliable = sectors["sector_reliable_flag"].eq(1)
    momentum_ready = reliable & sectors["sector_momentum_ready_flag"].eq(1)
    rvol_ready = reliable & sectors["sector_rvol_ready_flag"].eq(1)
    flow_ready = reliable & sectors["sector_flow_ready_flag"].eq(1)
    relative_turnover_ready = (
        reliable & sectors["sector_relative_turnover_ready_flag"].eq(1)
    )
    amihud_ready = reliable & sectors["sector_amihud_ready_flag"].eq(1)
    return_5m_ready = reliable & sectors["sector_return_5m_ready_flag"].eq(1)
    reliable_returns = sectors["sector_bar_return_5m"].where(
        return_5m_ready
    )
    sectors["_reliable_bar_return"] = reliable_returns
    short_return = _rolling_exact_sum(
        sectors, "_reliable_bar_return", config.trend_short_bars, config.bar_minutes
    )
    long_return = _rolling_exact_sum(
        sectors, "_reliable_bar_return", config.trend_long_bars, config.bar_minutes
    )
    volatility_sum = _rolling_exact_sum(
        sectors.assign(_squared_return=reliable_returns.pow(2)),
        "_squared_return",
        config.sector_volatility_bars,
        config.bar_minutes,
    )
    return_scale = reliable_returns.groupby(
        [sectors["sector"], sectors["_session"]], sort=False, observed=True
    ).transform(
        lambda values: values.rolling(
            config.trend_volatility_bars, min_periods=4
        ).std(ddof=0)
    ).replace(0.0, np.nan)
    short_z = short_return.div(return_scale * np.sqrt(config.trend_short_bars))
    long_z = long_return.div(return_scale * np.sqrt(config.trend_long_bars))
    trend_return_components = [
        np.tanh(short_z.clip(-4.0, 4.0)),
        np.tanh(long_z.clip(-4.0, 4.0)),
    ]
    trend_components = [
        *trend_return_components,
        2.0 * sectors["sector_fraction_above_vwap"] - 1.0,
        2.0 * sectors["sector_fraction_above_ema20"] - 1.0,
        2.0 * sectors["sector_fraction_above_ema50"] - 1.0,
    ]
    sectors["sector_trend_score"] = 100.0 * _finite_weighted_average(
        trend_components,
        [0.30, 0.25, 0.20, 0.15, 0.10],
    )
    sectors["sector_trend_component_count"] = np.isfinite(
        np.column_stack(
            [np.asarray(value, dtype="float64") for value in trend_components]
        )
    ).sum(axis=1)
    sectors["sector_trend_return_component_count"] = np.isfinite(
        np.column_stack(
            [np.asarray(value, dtype="float64") for value in trend_return_components]
        )
    ).sum(axis=1)
    trend_ready = (
        reliable
        & sectors["sector_trend_component_count"].ge(2)
        & sectors["sector_trend_return_component_count"].ge(1)
    )
    sectors["sector_trend_ready_flag"] = trend_ready.astype("int8")
    sectors.loc[~trend_ready, "sector_trend_score"] = np.nan
    sectors["sector_volatility_60m_bps"] = 10_000.0 * np.sqrt(volatility_sum)

    acceleration_lag = _exact_group_shift(
        sectors,
        "sector_momentum_30m",
        config.sector_acceleration_bars,
        config.bar_minutes,
    )
    sectors["sector_acceleration_30m_pct"] = 100.0 * (
        sectors["sector_momentum_30m"] - acceleration_lag
    )
    momentum_ready_lag = _exact_group_shift(
        sectors,
        "sector_momentum_ready_flag",
        config.sector_acceleration_bars,
        config.bar_minutes,
    )
    acceleration_ready = momentum_ready & momentum_ready_lag.eq(1)
    sectors.loc[~acceleration_ready, "sector_acceleration_30m_pct"] = np.nan

    market_stats = work.groupby("_timestamp", sort=True, observed=True).agg(
        _market_observed_member_count=("_ticker", "nunique"),
        market_momentum_valid_count=("_stock_momentum_30m", "count"),
        _market_momentum=("_stock_momentum_30m", "median"),
    )
    if config.expected_universe_size is None:
        market_stats["market_expected_member_count"] = market_stats[
            "_market_observed_member_count"
        ].cummax()
    else:
        market_stats["market_expected_member_count"] = float(
            config.expected_universe_size
        )
    market_manifest_valid = market_stats["_market_observed_member_count"].le(
        market_stats["market_expected_member_count"]
    )
    market_stats["market_momentum_coverage_pct"] = 100.0 * market_stats[
        "market_momentum_valid_count"
    ].div(market_stats["market_expected_member_count"].replace(0.0, np.nan))
    market_stats["market_momentum_ready_flag"] = (
        market_manifest_valid
        & market_stats["market_momentum_valid_count"].ge(
            max(int(config.min_sector_members), 2)
        )
        & market_stats["market_momentum_coverage_pct"].ge(
            100.0 * config.min_market_coverage
        )
    ).astype("int8")
    for column in (
        "market_expected_member_count",
        "market_momentum_valid_count",
        "market_momentum_coverage_pct",
        "market_momentum_ready_flag",
    ):
        sectors[column] = sectors["timestamp"].map(market_stats[column])
    market_momentum = sectors["timestamp"].map(market_stats["_market_momentum"])
    market_momentum_ready = sectors["market_momentum_ready_flag"].eq(1)
    sectors["sector_relative_momentum_pct"] = 100.0 * (
        sectors["sector_momentum_30m"] - market_momentum
    )
    sectors.loc[
        ~(momentum_ready & market_momentum_ready),
        "sector_relative_momentum_pct",
    ] = np.nan

    cross_sector_stats = sectors.groupby(
        "timestamp", sort=True, observed=True
    ).agg(
        _cross_sector_observed_count=("sector", "nunique"),
        cross_sector_reliable_count=("sector_reliable_flag", "sum"),
    )
    if config.expected_sector_count is None:
        cross_sector_stats["cross_sector_expected_count"] = cross_sector_stats[
            "_cross_sector_observed_count"
        ].cummax()
    else:
        cross_sector_stats["cross_sector_expected_count"] = float(
            config.expected_sector_count
        )
    cross_sector_manifest_valid = cross_sector_stats[
        "_cross_sector_observed_count"
    ].le(cross_sector_stats["cross_sector_expected_count"])
    cross_sector_stats["cross_sector_reliable_coverage_pct"] = 100.0 * (
        cross_sector_stats["cross_sector_reliable_count"].div(
            cross_sector_stats["cross_sector_expected_count"].replace(0.0, np.nan)
        )
    )
    cross_sector_stats["cross_sector_ready_flag"] = (
        cross_sector_manifest_valid
        & cross_sector_stats["cross_sector_reliable_count"].ge(2)
        & cross_sector_stats["cross_sector_reliable_coverage_pct"].ge(
            100.0 * config.min_cross_sector_coverage
        )
    ).astype("int8")
    for column in (
        "cross_sector_expected_count",
        "cross_sector_reliable_count",
        "cross_sector_reliable_coverage_pct",
        "cross_sector_ready_flag",
    ):
        sectors[column] = sectors["timestamp"].map(cross_sector_stats[column])
    cross_sector_ready = sectors["cross_sector_ready_flag"].eq(1)
    cross_sector_expected = sectors["cross_sector_expected_count"].replace(
        0.0, np.nan
    )

    def cross_sectional_zscore(
        source: str,
        available: pd.Series,
    ) -> pd.Series:
        candidate = sectors[source].where(available & cross_sector_ready)
        valid_count = candidate.groupby(
            sectors["timestamp"], sort=False, observed=True
        ).transform("count")
        source_ready = (
            cross_sector_ready
            & valid_count.ge(2)
            & valid_count.div(cross_sector_expected).ge(
                config.min_cross_sector_coverage
            )
        )
        return _grouped_safe_zscore(
            candidate.where(source_ready), [sectors["timestamp"]]
        )

    for source, output, available in (
        (
            "sector_relative_momentum_pct",
            "_relative_strength_z",
            momentum_ready & market_momentum_ready,
        ),
        ("sector_breadth", "_breadth_z", reliable),
        ("sector_acceleration_30m_pct", "_acceleration_z", acceleration_ready),
        ("sector_median_log_turnover", "_absolute_liquidity_z", flow_ready),
    ):
        sectors[output] = cross_sectional_zscore(source, available)

    sectors["_log_relative_volume"] = np.log(
        sectors["sector_relative_volume"].clip(lower=1e-6)
    )
    sectors["_log_relative_turnover"] = np.log(
        sectors["sector_relative_turnover"].clip(lower=1e-6)
    )
    sectors["_log_amihud"] = np.log(
        sectors["sector_amihud_impact"].clip(lower=1e-12)
    )
    for source, output, available in (
        ("_log_relative_volume", "_relative_volume_z", rvol_ready),
        ("_log_relative_turnover", "_relative_turnover_z", relative_turnover_ready),
        ("_log_amihud", "_amihud_z", amihud_ready),
    ):
        sectors[output] = cross_sectional_zscore(source, available)

    sectors["sector_relative_strength_score"] = 100.0 * np.tanh(
        sectors["_relative_strength_z"] / 2.0
    )
    sectors["sector_acceleration_score"] = 100.0 * np.tanh(
        sectors["_acceleration_z"] / 2.0
    )
    liquidity_components = [
        sectors["_absolute_liquidity_z"],
        sectors["_relative_turnover_z"],
        -sectors["_amihud_z"],
    ]
    raw_liquidity = _finite_weighted_average(
        liquidity_components,
        [0.45, 0.30, 0.25],
    )
    sectors["sector_liquidity_component_count"] = np.isfinite(
        np.column_stack(
            [np.asarray(value, dtype="float64") for value in liquidity_components]
        )
    ).sum(axis=1)
    liquidity_ready = (
        reliable
        & cross_sector_ready
        & sectors["sector_liquidity_component_count"].ge(2)
    )
    sectors["sector_liquidity_score"] = 50.0 * (1.0 + np.tanh(raw_liquidity / 2.0))
    sectors.loc[~liquidity_ready, "sector_liquidity_score"] = np.nan
    liquidity_valid_count = sectors["sector_liquidity_score"].groupby(
        sectors["timestamp"], sort=False, observed=True
    ).transform("count")
    liquidity_ready &= (
        liquidity_valid_count.ge(2)
        & liquidity_valid_count.div(cross_sector_expected).ge(
            config.min_cross_sector_coverage
        )
    )
    sectors["sector_liquidity_ready_flag"] = liquidity_ready.astype("int8")
    sectors.loc[~liquidity_ready, "sector_liquidity_score"] = np.nan
    sectors["sector_liquidity_percentile"] = 100.0 * _grouped_midrank_percentile(
        sectors["sector_liquidity_score"], [sectors["timestamp"]]
    )

    strength_components = [
        sectors["_relative_strength_z"],
        sectors["_breadth_z"],
        sectors["_relative_volume_z"],
    ]
    raw_strength = _finite_weighted_average(
        strength_components,
        [0.55, 0.25, 0.20],
    )
    sectors["sector_strength_component_count"] = np.isfinite(
        np.column_stack(
            [np.asarray(value, dtype="float64") for value in strength_components]
        )
    ).sum(axis=1)
    strength_ready = (
        reliable
        & cross_sector_ready
        & sectors["sector_strength_component_count"].ge(2)
    )
    sectors["sector_strength_score"] = 100.0 * np.tanh(raw_strength / 2.0)
    sectors.loc[~strength_ready, "sector_strength_score"] = np.nan
    strength_valid_count = sectors["sector_strength_score"].groupby(
        sectors["timestamp"], sort=False, observed=True
    ).transform("count")
    strength_ready &= (
        strength_valid_count.ge(2)
        & strength_valid_count.div(cross_sector_expected).ge(
            config.min_cross_sector_coverage
        )
    )
    sectors["sector_strength_ready_flag"] = strength_ready.astype("int8")
    sectors.loc[~strength_ready, "sector_strength_score"] = np.nan
    sectors["sector_strength_rank"] = sectors.groupby(
        "timestamp", sort=False, observed=True
    )["sector_strength_score"].rank(method="average", ascending=False)
    sectors["sector_strength_percentile"] = 100.0 * _grouped_midrank_percentile(
        sectors["sector_strength_score"], [sectors["timestamp"]]
    )
    strength_lag = _exact_group_shift(
        sectors,
        "sector_strength_percentile",
        config.rotation_lookback_bars,
        config.bar_minutes,
    )
    sectors["sector_strength_percentile_turnover_pct_points"] = (
        sectors["sector_strength_percentile"] - strength_lag
    ).abs()

    direction_lookup = sectors.set_index(["timestamp", "sector"])[
        "sector_directional_momentum_30m"
    ]
    mapped_key = pd.MultiIndex.from_arrays([mapped["_timestamp"], mapped["sector"]])
    mapped["_sector_direction"] = np.sign(direction_lookup.reindex(mapped_key).to_numpy())
    aligned = (
        mapped["_stock_momentum_30m"].notna()
        & mapped["_sector_direction"].ne(0)
        & (mapped["_sector_direction"] * mapped["_stock_momentum_30m"]).gt(0)
    )
    mapped["_direction_aligned"] = aligned.astype("int8")
    mapped["_active_direction_aligned"] = (
        aligned & mapped["_active_volume"].eq(1)
    ).astype("int8")
    mapped["_active_support_valid"] = (
        mapped["_stock_momentum_30m"].notna() & mapped["_rvol_valid"].eq(1)
    ).astype("int8")
    participation = mapped.groupby(
        ["_timestamp", "sector"], sort=True, observed=True
    ).agg(
        _direction_aligned_count=("_direction_aligned", "sum"),
        _active_direction_aligned_count=("_active_direction_aligned", "sum"),
        _active_support_valid_count=("_active_support_valid", "sum"),
    ).reset_index().rename(columns={"_timestamp": "timestamp"})
    sectors = sectors.merge(
        participation, on=["timestamp", "sector"], how="left", validate="one_to_one"
    )
    expected = sectors["sector_expected_member_count"].replace(0.0, np.nan)
    momentum_denominator = sectors["sector_momentum_valid_count"].replace(0.0, np.nan)
    sectors["sector_participation_pct"] = 100.0 * sectors[
        "_direction_aligned_count"
    ].div(momentum_denominator)
    sectors["sector_expected_member_support_pct"] = 100.0 * sectors[
        "_direction_aligned_count"
    ].div(expected)
    sectors["sector_active_aligned_support_pct"] = 100.0 * sectors[
        "_active_direction_aligned_count"
    ].div(sectors["_active_support_valid_count"].replace(0.0, np.nan))
    sectors["sector_signed_participation_pct"] = (
        np.sign(sectors["sector_directional_momentum_30m"])
        * sectors["sector_participation_pct"]
    )
    participation_available = (
        momentum_ready & sectors["sector_directional_momentum_30m"].notna()
    )
    sectors.loc[~participation_available, [
        "sector_participation_pct",
        "sector_expected_member_support_pct",
        "sector_signed_participation_pct",
    ]] = np.nan
    active_support_available = participation_available & rvol_ready
    sectors.loc[
        ~active_support_available, "sector_active_aligned_support_pct"
    ] = np.nan

    stock_keys = [mapped["_timestamp"], mapped["sector"]]
    stock_is_fresh = mapped["_fresh_price_eligible"].astype(bool)
    mapped["_loo_intraday_return"] = _leave_one_out_mean(
        mapped["_stock_intraday_return"], stock_keys
    )
    mapped["_loo_momentum_30m"] = _leave_one_out_mean(
        mapped["_stock_momentum_30m"], stock_keys
    )
    mapped["stock_distance_from_sector_average_pct"] = 100.0 * (
        mapped["_stock_intraday_return"] - mapped["_loo_intraday_return"]
    )
    mapped["stock_relative_momentum_30m_pct"] = 100.0 * (
        mapped["_stock_momentum_30m"] - mapped["_loo_momentum_30m"]
    )
    stock_readiness_lookup = sectors.set_index(["timestamp", "sector"])[
        [
            "sector_expected_member_count",
            "sector_reliable_flag",
            "sector_momentum_ready_flag",
            "sector_previous_close_ready_flag",
            "sector_above_vwap_ready_flag",
            "sector_rvol_ready_flag",
            "sector_new_high_low_ready_flag",
        ]
    ]
    aligned_stock_readiness = stock_readiness_lookup.reindex(mapped_key)
    for source, destination in (
        ("sector_expected_member_count", "_sector_expected_member_count"),
        ("sector_reliable_flag", "sector_reliable_flag"),
        ("sector_momentum_ready_flag", "stock_sector_momentum_ready_flag"),
        ("sector_previous_close_ready_flag", "_sector_previous_close_ready"),
        ("sector_above_vwap_ready_flag", "_sector_vwap_ready"),
        ("sector_rvol_ready_flag", "_sector_rvol_ready"),
        ("sector_new_high_low_ready_flag", "_sector_new_high_low_ready"),
    ):
        mapped[destination] = aligned_stock_readiness[source].to_numpy()
    mapped["_sector_momentum_component_ready"] = (
        mapped["sector_reliable_flag"].eq(1)
        & mapped["stock_sector_momentum_ready_flag"].eq(1)
    )
    for source, output, count_output, ready_output, sector_ready_column in (
        (
            "_stock_momentum_30m",
            "_momentum_z",
            "stock_peer_momentum_valid_count",
            "_peer_momentum_ready",
            "_sector_momentum_component_ready",
        ),
        (
            "_stock_intraday_return",
            "_intraday_z",
            "stock_peer_intraday_valid_count",
            "_peer_intraday_ready",
            "_sector_previous_close_ready",
        ),
        (
            "_vwap_log_distance",
            "_vwap_distance_z",
            "stock_peer_vwap_valid_count",
            "_peer_vwap_ready",
            "_sector_vwap_ready",
        ),
    ):
        group_valid_count = _grouped_valid_count(mapped[source], stock_keys)
        mapped[count_output] = group_valid_count - mapped[source].notna().astype(
            "int64"
        )
        expected_peer_count = mapped["_sector_expected_member_count"].sub(1.0)
        required_peer_count = max(int(config.min_sector_members) - 1, 1)
        mapped[ready_output] = (
            mapped[count_output].ge(required_peer_count)
            & mapped[count_output]
            .div(expected_peer_count.where(expected_peer_count.gt(0)))
            .ge(config.min_sector_data_coverage)
        )
        mapped[output] = _grouped_safe_zscore(
            mapped[source], [mapped["_timestamp"], mapped["sector"]]
        ).where(mapped[ready_output] & mapped[sector_ready_column].eq(1))
    outperformance_components = [
        mapped["_momentum_z"],
        mapped["_intraday_z"],
        mapped["_vwap_distance_z"],
    ]
    raw_outperformance = _finite_weighted_average(
        outperformance_components,
        [0.50, 0.30, 0.20],
    )
    mapped["stock_outperformance_component_count"] = np.isfinite(
        np.column_stack(
            [np.asarray(value, dtype="float64") for value in outperformance_components]
        )
    ).sum(axis=1)
    mapped["stock_outperformance_score"] = 100.0 * np.tanh(
        raw_outperformance / 2.0
    )

    comparable = mapped["sector_reliable_flag"].eq(1) & stock_is_fresh
    intraday_comparable = comparable & mapped["_peer_intraday_ready"]
    intraday_comparable &= mapped["_sector_previous_close_ready"].eq(1)
    momentum_comparable = (
        comparable
        & mapped["stock_sector_momentum_ready_flag"].eq(1)
        & mapped["_peer_momentum_ready"]
    )
    mapped.loc[
        ~intraday_comparable, "stock_distance_from_sector_average_pct"
    ] = np.nan
    mapped.loc[
        ~momentum_comparable, "stock_relative_momentum_30m_pct"
    ] = np.nan
    outperformance_available = (
        comparable & mapped["stock_outperformance_component_count"].ge(2)
    )
    mapped.loc[~outperformance_available, "stock_outperformance_score"] = np.nan
    outperformance_valid_count = _grouped_valid_count(
        mapped["stock_outperformance_score"], stock_keys
    )
    outperformance_cross_section_ready = (
        outperformance_valid_count.ge(config.min_sector_members)
        & outperformance_valid_count
        .div(mapped["_sector_expected_member_count"].replace(0.0, np.nan))
        .ge(config.min_sector_data_coverage)
    )
    outperformance_available &= outperformance_cross_section_ready
    mapped.loc[~outperformance_available, "stock_outperformance_score"] = np.nan
    mapped["stock_sector_percentile"] = 100.0 * _grouped_midrank_percentile(
        mapped["stock_outperformance_score"],
        [mapped["_timestamp"], mapped["sector"]],
    )

    group_rvol_valid_count = _grouped_valid_count(mapped["_stock_rvol"], stock_keys)
    mapped["stock_peer_rvol_valid_count"] = (
        group_rvol_valid_count - mapped["_stock_rvol"].notna().astype("int64")
    )
    expected_peer_count = mapped["_sector_expected_member_count"].sub(1.0)
    required_peer_count = max(int(config.min_sector_members) - 1, 1)
    mapped["_peer_rvol_ready"] = (
        mapped["stock_peer_rvol_valid_count"].ge(required_peer_count)
        & mapped["stock_peer_rvol_valid_count"]
        .div(expected_peer_count.where(expected_peer_count.gt(0)))
        .ge(config.min_sector_data_coverage)
    )
    mapped["_rvol_percentile"] = _grouped_midrank_percentile(
        mapped["_stock_rvol"], [mapped["_timestamp"], mapped["sector"]]
    ).where(mapped["_peer_rvol_ready"] & mapped["_sector_rvol_ready"].eq(1))
    persistence_components = []
    for column in ("_stock_return_5m", "_stock_return_15m", "_stock_momentum_30m"):
        numeric = pd.to_numeric(mapped[column], errors="coerce")
        persistence_components.append(numeric.gt(0).astype("float64").where(numeric.notna()))
    mapped["_positive_return_persistence"] = _finite_weighted_average(
        persistence_components, [1.0, 1.0, 1.0]
    )
    leadership_components = [
        mapped["stock_sector_percentile"] / 100.0,
        mapped["_rvol_percentile"],
        mapped["_positive_return_persistence"].where(stock_is_fresh),
        mapped["_new_high"].astype("float64").where(
            stock_is_fresh & mapped["_sector_new_high_low_ready"].eq(1)
        ),
        mapped["_above_vwap"].astype("float64").where(
            stock_is_fresh & mapped["_sector_vwap_ready"].eq(1)
        ),
    ]
    mapped["stock_sector_leadership_score"] = 100.0 * _finite_weighted_average(
        leadership_components,
        [0.45, 0.20, 0.15, 0.10, 0.10],
    )
    mapped["stock_leadership_component_count"] = np.isfinite(
        np.column_stack(
            [np.asarray(value, dtype="float64") for value in leadership_components]
        )
    ).sum(axis=1)
    leadership_available = (
        comparable
        & mapped["stock_sector_percentile"].notna()
        & mapped["stock_leadership_component_count"].ge(2)
    )
    mapped.loc[~leadership_available, "stock_sector_leadership_score"] = np.nan
    leadership_valid_count = _grouped_valid_count(
        mapped["stock_sector_leadership_score"], stock_keys
    )
    leadership_cross_section_ready = (
        leadership_valid_count.ge(config.min_sector_members)
        & leadership_valid_count
        .div(mapped["_sector_expected_member_count"].replace(0.0, np.nan))
        .ge(config.min_sector_data_coverage)
    )
    leadership_available &= leadership_cross_section_ready
    mapped.loc[~leadership_available, "stock_sector_leadership_score"] = np.nan
    mapped["stock_sector_leadership_percentile"] = 100.0 * _grouped_midrank_percentile(
        mapped["stock_sector_leadership_score"],
        [mapped["_timestamp"], mapped["sector"]],
    )
    threshold = 100.0 * config.leader_percentile_threshold
    leadership_rank_available = (
        leadership_available & mapped["stock_sector_leadership_percentile"].notna()
    )
    mapped["stock_is_sector_leader"] = (
        mapped["stock_sector_leadership_percentile"].ge(threshold).astype("float64")
    ).where(leadership_rank_available)
    mapped["stock_is_sector_weakest"] = (
        mapped["stock_sector_leadership_percentile"]
        .le(100.0 - threshold)
        .astype("float64")
    ).where(leadership_rank_available)

    mapped["_leader_score"] = mapped["stock_sector_leadership_score"].where(
        mapped["stock_is_sector_leader"].eq(1)
    )
    mapped["_weakest_score"] = mapped["stock_sector_leadership_score"].where(
        mapped["stock_is_sector_weakest"].eq(1)
    )
    mapped["_leader_momentum"] = mapped["_stock_momentum_30m"].where(
        mapped["stock_is_sector_leader"].eq(1)
    )
    mapped["_weakest_momentum"] = mapped["_stock_momentum_30m"].where(
        mapped["stock_is_sector_weakest"].eq(1)
    )
    leader_summary = mapped.groupby(
        ["_timestamp", "sector"], sort=True, observed=True
    ).agg(
        sector_leader_stock_count=("stock_is_sector_leader", _numeric_sum_min_count),
        sector_weakest_stock_count=("stock_is_sector_weakest", _numeric_sum_min_count),
        sector_leader_score_mean=("_leader_score", "mean"),
        sector_weakest_score_mean=("_weakest_score", "mean"),
        sector_leader_momentum_30m=("_leader_momentum", "mean"),
        sector_weakest_momentum_30m=("_weakest_momentum", "mean"),
    ).reset_index().rename(columns={"_timestamp": "timestamp"})
    sectors = sectors.merge(
        leader_summary, on=["timestamp", "sector"], how="left", validate="one_to_one"
    )
    sectors["sector_leader_momentum_30m_pct"] = 100.0 * sectors[
        "sector_leader_momentum_30m"
    ]
    sectors["sector_weakest_momentum_30m_pct"] = 100.0 * sectors[
        "sector_weakest_momentum_30m"
    ]
    sectors["sector_leadership_spread_30m_pct"] = (
        sectors["sector_leader_momentum_30m_pct"]
        - sectors["sector_weakest_momentum_30m_pct"]
    )

    stocks_mapped = mapped[
        [
            "_timestamp",
            "_ticker",
            "sector",
            "_fresh_price_eligible",
            "sector_reliable_flag",
            "stock_sector_momentum_ready_flag",
            "stock_peer_momentum_valid_count",
            "stock_peer_intraday_valid_count",
            "stock_peer_vwap_valid_count",
            "stock_peer_rvol_valid_count",
            "stock_outperformance_component_count",
            "stock_leadership_component_count",
            "stock_sector_percentile",
            "stock_distance_from_sector_average_pct",
            "stock_relative_momentum_30m_pct",
            "stock_outperformance_score",
            "stock_sector_leadership_score",
            "stock_sector_leadership_percentile",
            "stock_is_sector_leader",
            "stock_is_sector_weakest",
        ]
    ].copy()
    stocks_mapped["sector_mapped_flag"] = np.int8(1)
    unmapped = work.loc[
        ~work["_sector_mapped"],
        ["_timestamp", "_ticker", "_fresh_price_eligible"],
    ].copy()
    if not unmapped.empty:
        unmapped["sector"] = pd.Series(pd.NA, index=unmapped.index, dtype="string")
        unmapped["sector_mapped_flag"] = np.int8(0)
        unmapped["sector_reliable_flag"] = np.int8(0)
        unmapped["stock_sector_momentum_ready_flag"] = np.int8(0)
        for column in (
            "stock_peer_momentum_valid_count",
            "stock_peer_intraday_valid_count",
            "stock_peer_vwap_valid_count",
            "stock_peer_rvol_valid_count",
            "stock_outperformance_component_count",
            "stock_leadership_component_count",
        ):
            unmapped[column] = np.nan
        for column in (
            "stock_sector_percentile",
            "stock_distance_from_sector_average_pct",
            "stock_relative_momentum_30m_pct",
            "stock_outperformance_score",
            "stock_sector_leadership_score",
            "stock_sector_leadership_percentile",
            "stock_is_sector_leader",
            "stock_is_sector_weakest",
        ):
            unmapped[column] = np.nan
        stocks = pd.concat([stocks_mapped, unmapped], ignore_index=True, sort=False)
    else:
        stocks = stocks_mapped
    stocks = stocks.rename(columns={"_timestamp": "timestamp", "_ticker": "ticker"})
    stocks = stocks.rename(
        columns={
            "_fresh_price_eligible": "stock_fresh_price_flag",
            "sector_mapped_flag": "stock_sector_mapped_flag",
            "sector_reliable_flag": "stock_sector_reliable_flag",
        }
    )

    internal_sector_columns = [column for column in sectors.columns if column.startswith("_")]
    sectors = sectors.drop(
        columns=internal_sector_columns
        + [
            "sector_momentum_30m",
            "sector_directional_momentum_30m",
            "sector_intraday_return",
            "sector_bar_return_5m",
            "sector_return_dispersion_5m",
            "sector_fraction_above_vwap",
            "sector_fraction_above_ema20",
            "sector_fraction_above_ema50",
            "sector_fraction_new_highs",
            "sector_fraction_new_lows",
            "sector_total_turnover_rupees",
            "sector_median_turnover_rupees",
            "sector_median_log_turnover",
            "sector_leader_momentum_30m",
            "sector_weakest_momentum_30m",
        ],
        errors="ignore",
    )
    return _finalize_result(sectors, stocks, config)


class SectorIntelligenceEngine:
    """Vectorised reference implementation for historical and live snapshots."""

    def __init__(
        self,
        config: SectorIntelligenceConfig | None = None,
        *,
        sector_map: Mapping[str, str] | None = None,
    ) -> None:
        self.config = config or SectorIntelligenceConfig()
        clean_sector_map: dict[str, str] = {}
        for key, value in (sector_map or {}).items():
            if pd.isna(key) or pd.isna(value):
                continue
            clean_key = str(key).upper().strip()
            clean_value = str(value).upper().strip()
            if clean_key in {"", "<NA>", "NAN", "NONE", "NULL"}:
                continue
            if clean_value in {"", "<NA>", "NAN", "NONE", "NULL", "UNMAPPED"}:
                continue
            clean_sector_map[clean_key] = clean_value
        self.sector_map = clean_sector_map

    def compute(self, stock_bars: pd.DataFrame) -> SectorIntelligenceResult:
        panel = _prepare_stock_panel(stock_bars, self.sector_map, self.config)
        if panel.empty:
            return _empty_result()
        if self.sector_map:
            clean_sectors = [
                str(value).upper().strip()
                for value in self.sector_map.values()
                if not pd.isna(value)
                and str(value).upper().strip()
                not in {"", "<NA>", "NAN", "NONE", "NULL", "UNMAPPED"}
            ]
            counts = pd.Series(clean_sectors, dtype="string").value_counts()
            panel["_sector_expected_members"] = panel["sector"].map(counts)
        return _compute_sector_intelligence_from_panel(panel, self.config)

    def latest(
        self,
        stock_bars: pd.DataFrame,
        *,
        asof: Any | None = None,
    ) -> SectorIntelligenceResult:
        result = self.compute(stock_bars)
        if result.sectors.empty and result.stocks.empty:
            return result
        cutoff = (
            pd.Timestamp.now(tz=self.config.timezone)
            if asof is None
            else _to_timezone(pd.Series([asof]), self.config.timezone).iloc[0]
        )
        publication_source = result.stocks if not result.stocks.empty else result.sectors
        eligible = publication_source.loc[publication_source["available_at"].le(cutoff)]
        if eligible.empty:
            return SectorIntelligenceResult(
                result.sectors.iloc[0:0], result.stocks.iloc[0:0]
            )
        timestamp = eligible["timestamp"].max()
        return SectorIntelligenceResult(
            sectors=result.sectors.loc[
                result.sectors["timestamp"].eq(timestamp)
            ].reset_index(drop=True),
            stocks=result.stocks.loc[result.stocks["timestamp"].eq(timestamp)].reset_index(
                drop=True
            ),
        )


def _grouped_asof_attach(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_group_col: str,
    right_group_col: str,
    left_time_col: str,
    right_time_col: str,
    tolerance: pd.Timedelta,
    right_columns: list[str],
) -> pd.DataFrame:
    def add_missing_columns(frame: pd.DataFrame) -> pd.DataFrame:
        additions: dict[str, pd.Series] = {}
        for column in right_columns:
            if column in frame or column == right_group_col:
                continue
            template = right[column]
            if pd.api.types.is_datetime64_any_dtype(template):
                additions[column] = pd.Series(
                    pd.NaT, index=frame.index, dtype=template.dtype
                )
            elif pd.api.types.is_numeric_dtype(template):
                additions[column] = pd.Series(
                    np.nan, index=frame.index, dtype="float64"
                )
            elif isinstance(template.dtype, pd.StringDtype):
                additions[column] = pd.Series(
                    pd.NA, index=frame.index, dtype="string"
                )
            else:
                additions[column] = pd.Series(
                    pd.NA, index=frame.index, dtype="object"
                )
        if not additions:
            return frame.copy()
        return pd.concat(
            [frame.copy(), pd.DataFrame(additions, index=frame.index)], axis=1
        )

    right_groups = {
        value: group[right_columns].copy()
        for value, group in right.loc[right[right_group_col].notna()].groupby(
            right_group_col, sort=False, observed=True
        )
    }
    empty_right = right.iloc[0:0][right_columns].copy()
    parts: list[pd.DataFrame] = []
    for value, group in left.groupby(left_group_col, sort=False, dropna=False):
        one = group.copy()
        available = right_groups.get(value, empty_right) if not pd.isna(value) else empty_right
        if available.empty:
            parts.append(add_missing_columns(one))
            continue
        valid = one[left_time_col].notna()
        joined_parts: list[pd.DataFrame] = []
        if valid.any():
            joined_parts.append(
                pd.merge_asof(
                    one.loc[valid].sort_values(left_time_col),
                    available.drop(columns=[right_group_col], errors="ignore").sort_values(
                        right_time_col
                    ),
                    left_on=left_time_col,
                    right_on=right_time_col,
                    direction="backward",
                    tolerance=tolerance,
                    allow_exact_matches=True,
                )
            )
        if (~valid).any():
            joined_parts.append(add_missing_columns(one.loc[~valid]))
        parts.append(pd.concat(joined_parts, ignore_index=True, sort=False))
    return pd.concat(parts, ignore_index=True, sort=False) if parts else left.copy()


def attach_sector_intelligence_asof(
    candidates: pd.DataFrame,
    intelligence: SectorIntelligenceResult,
    *,
    candidate_time_col: str = "signal_time_ist",
    ticker_col: str = "ticker",
    prefix: str = "si_",
    timezone: str = IST,
    max_staleness_minutes: int = 7,
) -> pd.DataFrame:
    """Backward-attach stock and sector features without filtering candidates.

    The stock snapshot is joined first by ticker.  Its point-in-time sector key
    is then used for the sector join, avoiding a present-day static membership
    map during historical enrichment.
    """

    if candidates is None:
        return pd.DataFrame()
    missing = {candidate_time_col, ticker_col} - set(candidates.columns)
    if missing:
        raise ValueError(f"candidates missing required columns: {sorted(missing)}")
    existing = [column for column in candidates.columns if str(column).startswith(prefix)]
    if existing:
        raise ValueError(f"candidates already contain {prefix!r} columns: {existing[:5]}")
    reserved = [column for column in candidates.columns if str(column).startswith("_si_")]
    if reserved:
        raise ValueError(f"candidates use reserved Sector Intelligence columns: {reserved[:5]}")
    if float(max_staleness_minutes) < 0:
        raise ValueError("max_staleness_minutes must be non-negative")
    if candidates.empty:
        empty_columns: dict[str, pd.Series] = {}
        for scope, feature_columns in (
            ("stock", STOCK_FEATURE_COLUMNS),
            ("sector", SECTOR_FEATURE_COLUMNS),
        ):
            empty_columns[f"{prefix}{scope}_timestamp"] = pd.Series(
                index=candidates.index, dtype=f"datetime64[ns, {timezone}]"
            )
            for column in feature_columns:
                empty_columns[f"{prefix}{column}"] = pd.Series(
                    index=candidates.index, dtype="float64"
                )
            empty_columns[f"{prefix}{scope}_available_at"] = pd.Series(
                index=candidates.index, dtype=f"datetime64[ns, {timezone}]"
            )
            empty_columns[f"{prefix}{scope}_feature_version"] = pd.Series(
                index=candidates.index, dtype="string"
            )
            empty_columns[f"{prefix}{scope}_age_seconds"] = pd.Series(
                index=candidates.index, dtype="float64"
            )
        return pd.concat(
            [candidates.copy(), pd.DataFrame(empty_columns, index=candidates.index)],
            axis=1,
        )
    out = candidates.copy()
    out["_si_row_order"] = np.arange(len(out))
    out["_si_decision_time"] = _to_timezone(out[candidate_time_col], timezone)
    out["_si_ticker_key"] = out[ticker_col].astype("string").str.upper().str.strip()
    out["_si_ticker_key"] = out["_si_ticker_key"].mask(
        out["_si_ticker_key"].isin({"", "<NA>", "NAN", "NONE", "NULL"})
    )
    tolerance = pd.Timedelta(minutes=max_staleness_minutes)

    stocks = intelligence.stocks.copy()
    stocks["available_at"] = _to_timezone(stocks["available_at"], timezone)
    for column in stocks.columns:
        if column not in IDENTIFIER_COLUMNS and pd.api.types.is_numeric_dtype(
            stocks[column]
        ):
            stocks[column] = pd.to_numeric(stocks[column], errors="coerce").astype(
                "float64"
            )
    stock_rename = {
        "ticker": "_si_stock_ticker_key",
        "sector": "_si_effective_sector",
        "timestamp": f"{prefix}stock_timestamp",
        "available_at": f"{prefix}stock_available_at",
        "feature_version": f"{prefix}stock_feature_version",
    }
    stock_rename.update(
        {
            column: f"{prefix}{column}"
            for column in stocks.columns
            if column not in stock_rename
        }
    )
    stocks = stocks.rename(columns=stock_rename)
    stock_columns = list(stocks.columns)
    out = _grouped_asof_attach(
        out,
        stocks,
        left_group_col="_si_ticker_key",
        right_group_col="_si_stock_ticker_key",
        left_time_col="_si_decision_time",
        right_time_col=f"{prefix}stock_available_at",
        tolerance=tolerance,
        right_columns=stock_columns,
    )
    stock_age = (
        out["_si_decision_time"] - out[f"{prefix}stock_available_at"]
    ).dt.total_seconds().rename(f"{prefix}stock_age_seconds")
    out = pd.concat([out, stock_age], axis=1)

    sectors = intelligence.sectors.copy()
    sectors["available_at"] = _to_timezone(sectors["available_at"], timezone)
    for column in sectors.columns:
        if column not in IDENTIFIER_COLUMNS and pd.api.types.is_numeric_dtype(
            sectors[column]
        ):
            sectors[column] = pd.to_numeric(
                sectors[column], errors="coerce"
            ).astype("float64")
    sector_rename = {
        "sector": "_si_sector_key",
        "timestamp": f"{prefix}sector_timestamp",
        "available_at": f"{prefix}sector_available_at",
        "feature_version": f"{prefix}sector_feature_version",
    }
    sector_rename.update(
        {
            column: f"{prefix}{column}"
            for column in sectors.columns
            if column not in sector_rename
        }
    )
    sectors = sectors.rename(columns=sector_rename)
    sector_columns = list(sectors.columns)
    out = _grouped_asof_attach(
        out,
        sectors,
        left_group_col="_si_effective_sector",
        right_group_col="_si_sector_key",
        left_time_col="_si_decision_time",
        right_time_col=f"{prefix}sector_available_at",
        tolerance=tolerance,
        right_columns=sector_columns,
    )
    sector_age = (
        out["_si_decision_time"] - out[f"{prefix}sector_available_at"]
    ).dt.total_seconds().rename(f"{prefix}sector_age_seconds")
    out = pd.concat([out, sector_age], axis=1)

    out = out.sort_values("_si_row_order")
    out = out.drop(
        columns=[
            "_si_row_order",
            "_si_decision_time",
            "_si_ticker_key",
            "_si_stock_ticker_key",
            "_si_effective_sector",
            "_si_sector_key",
        ],
        errors="ignore",
    )
    out.index = candidates.index.copy()
    if not out.columns.is_unique:
        raise AssertionError("sector intelligence attachment produced duplicate columns")
    return out


def sector_intelligence_feature_columns(
    intelligence: SectorIntelligenceResult,
    *,
    prefix: str = "si_",
    include_quality_metadata: bool = False,
) -> list[str]:
    """Return the ordered, prefixed numeric columns suitable for ML trials."""
    ordered = [*SECTOR_FEATURE_COLUMNS, *STOCK_FEATURE_COLUMNS]
    return [
        f"{prefix}{column}"
        for column in ordered
        if include_quality_metadata or column not in QUALITY_FEATURE_COLUMNS
    ]


__all__ = [
    "FORBIDDEN_FEATURE_TOKENS",
    "IDENTIFIER_COLUMNS",
    "QUALITY_FEATURE_COLUMNS",
    "SECTOR_INTELLIGENCE_VERSION",
    "SECTOR_FEATURE_COLUMNS",
    "STOCK_FEATURE_COLUMNS",
    "SectorIntelligenceConfig",
    "SectorIntelligenceEngine",
    "SectorIntelligenceResult",
    "attach_sector_intelligence_asof",
    "sector_intelligence_feature_columns",
]
