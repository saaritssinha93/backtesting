from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from .config import PrefilterConfig


IST = "Asia/Kolkata"

FAMILY_SCORE_COLUMNS = (
    "momentum_long_score",
    "momentum_short_score",
    "reversal_long_score",
    "reversal_short_score",
    "expansion_long_score",
    "expansion_short_score",
)

FAMILY_LABELS = {
    "momentum_long_score": ("LONG", "MOMENTUM"),
    "momentum_short_score": ("SHORT", "MOMENTUM"),
    "reversal_long_score": ("LONG", "REVERSAL"),
    "reversal_short_score": ("SHORT", "REVERSAL"),
    "expansion_long_score": ("LONG", "EXPANSION"),
    "expansion_short_score": ("SHORT", "EXPANSION"),
}


@dataclass(frozen=True)
class FeatureBuildStats:
    input_rows: int
    causal_rows: int
    universe_count: int
    eligible_count: int
    rejected_count: int


def _ist_timestamp(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _timestamp_series(values: pd.Series) -> pd.Series:
    return values.map(_ist_timestamp)


def _numeric(frame: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _rank01(values: pd.Series, eligible: pd.Series, *, ascending: bool = True) -> pd.Series:
    result = pd.Series(0.0, index=values.index, dtype=float)
    valid = eligible & pd.to_numeric(values, errors="coerce").notna()
    result.loc[eligible & ~valid] = 0.5
    count = int(valid.sum())
    if count == 1:
        result.loc[valid] = 0.5
    elif count > 1:
        ranks = values.loc[valid].rank(method="average", ascending=ascending)
        result.loc[valid] = (ranks - 1.0) / float(count - 1)
    return result.clip(0.0, 1.0)


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = pd.to_numeric(denominator, errors="coerce").replace(0.0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / denom


def _finite_median(values: pd.Series, default: float = 0.0) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return float(clean.median()) if not clean.empty else float(default)


def build_features(
    bars: pd.DataFrame,
    slot_ist: object,
    config: PrefilterConfig | None = None,
) -> tuple[pd.DataFrame, FeatureBuildStats]:
    """Build causal lightweight features through ``slot_ist``.

    The input may contain bars beyond the requested slot; those rows are
    discarded before any rolling calculation, which makes historical replay
    safe against accidental future-row leakage.
    """

    cfg = (config or PrefilterConfig()).validate()
    slot = _ist_timestamp(slot_ist)
    if pd.isna(slot):
        raise ValueError("slot_ist is invalid")
    if bars is None or bars.empty:
        empty = pd.DataFrame(columns=["ticker", "eligible", "reject_reason"])
        return empty, FeatureBuildStats(0, 0, 0, 0, 0)

    work = bars.copy()
    if "ticker" not in work.columns:
        if "symbol" in work.columns:
            work = work.rename(columns={"symbol": "ticker"})
        else:
            raise ValueError("bars must contain ticker or symbol")
    time_column = "date" if "date" in work.columns else "datetime" if "datetime" in work.columns else None
    if time_column is None:
        raise ValueError("bars must contain date or datetime")

    input_rows = int(len(work))
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    work["date"] = _timestamp_series(work[time_column])
    work = work.loc[work["ticker"].ne("") & work["date"].notna() & work["date"].le(slot)].copy()
    work = work.sort_values(["ticker", "date"], kind="mergesort")
    work = work.drop_duplicates(["ticker", "date"], keep="last")
    if work.empty:
        empty = pd.DataFrame(columns=["ticker", "eligible", "reject_reason"])
        return empty, FeatureBuildStats(input_rows, 0, 0, 0, 0)

    # Keep enough history for the longest lightweight feature and bound memory.
    work = work.groupby("ticker", sort=False, group_keys=False).tail(cfg.lookback_bars).copy()
    for column in ("open", "high", "low", "close", "volume"):
        if column not in work.columns:
            raise ValueError(f"bars are missing required column: {column}")
        work[column] = _numeric(work, column)
    for column in ("ATR", "EMA_20", "EMA_50", "EMA_200", "RSI", "ADX", "sector"):
        if column not in work.columns:
            work[column] = np.nan if column != "sector" else "UNKNOWN"

    opening_snapshot = (
        work["opening_snapshot"].fillna(False).astype(bool)
        if "opening_snapshot" in work.columns
        else pd.Series(False, index=work.index)
    )
    gap_filled = (
        pd.to_numeric(work["gap_filled"], errors="coerce").fillna(0).ne(0)
        if "gap_filled" in work.columns
        else pd.Series(False, index=work.index)
    )
    work["is_completed_real_bar"] = ~(opening_snapshot | gap_filled)
    groups = work.groupby("ticker", sort=False)
    work["bars_available"] = groups.cumcount() + 1
    work["completed_real_bars_available"] = work["is_completed_real_bar"].astype(int).groupby(work["ticker"], sort=False).cumsum()
    work["bar_return"] = _safe_div(work["close"], groups["close"].shift(1)) - 1.0
    work["ret_15m"] = _safe_div(work["close"], groups["close"].shift(3)) - 1.0
    work["ret_30m"] = _safe_div(work["close"], groups["close"].shift(6)) - 1.0
    work["ret_60m"] = _safe_div(work["close"], groups["close"].shift(12)) - 1.0
    work["bar_range"] = (work["high"] - work["low"]).clip(lower=0.0)
    work["close_location"] = _safe_div(work["close"] - work["low"], work["bar_range"]).clip(0.0, 1.0)
    work["traded_value_rs"] = work["close"] * work["volume"].clip(lower=0.0)

    baseline_volume = work["volume"].where(work["is_completed_real_bar"])
    baseline_value = work["traded_value_rs"].where(work["is_completed_real_bar"])
    baseline_range = work["bar_range"].where(work["is_completed_real_bar"])
    work["prior_volume_median_20"] = baseline_volume.groupby(work["ticker"], sort=False).transform(
        lambda values: values.shift(1).rolling(20, min_periods=5).median()
    )
    work["prior_value_median_20"] = baseline_value.groupby(work["ticker"], sort=False).transform(
        lambda values: values.shift(1).rolling(20, min_periods=5).median()
    )
    work["prior_range_median_10"] = baseline_range.groupby(work["ticker"], sort=False).transform(
        lambda values: values.shift(1).rolling(10, min_periods=4).median()
    )
    work["prior_range_mean_3"] = baseline_range.groupby(work["ticker"], sort=False).transform(
        lambda values: values.shift(1).rolling(3, min_periods=2).mean()
    )
    work["prior_high_12"] = groups["high"].transform(
        lambda values: values.shift(1).rolling(12, min_periods=4).max()
    )
    work["prior_low_12"] = groups["low"].transform(
        lambda values: values.shift(1).rolling(12, min_periods=4).min()
    )
    positive = work["bar_return"].gt(0.0).astype(float)
    work["trend_consistency_6"] = positive.groupby(work["ticker"], sort=False).transform(
        lambda values: values.rolling(6, min_periods=3).mean()
    )

    work["session_day"] = work["date"].dt.strftime("%Y-%m-%d")
    typical = (work["high"] + work["low"] + work["close"]) / 3.0
    pv = typical * work["volume"].clip(lower=0.0)
    session_keys = [work["ticker"], work["session_day"]]
    cumulative_pv = pv.groupby(session_keys, sort=False).cumsum()
    cumulative_volume = work["volume"].clip(lower=0.0).groupby(session_keys, sort=False).cumsum()
    work["session_vwap"] = _safe_div(cumulative_pv, cumulative_volume)

    last = work.groupby("ticker", sort=False, as_index=False).tail(1).copy().reset_index(drop=True)
    last["current_opening_snapshot"] = ~last["is_completed_real_bar"] & (
        last["opening_snapshot"].fillna(False).astype(bool)
        if "opening_snapshot" in last.columns
        else False
    )
    last["current_gap_filled"] = (
        pd.to_numeric(last["gap_filled"], errors="coerce").fillna(0).ne(0)
        if "gap_filled" in last.columns
        else False
    )
    last["staleness_seconds"] = (slot - last["date"]).dt.total_seconds()
    atr = _numeric(last, "ATR")
    atr_fallback = last["prior_range_median_10"].where(last["prior_range_median_10"].gt(0.0))
    last["atr_value"] = atr.where(atr.gt(0.0), atr_fallback)
    last["atr_pct"] = _safe_div(last["atr_value"], last["close"])
    last["rvol_20"] = _safe_div(last["volume"], last["prior_volume_median_20"])
    last["range_expansion"] = _safe_div(last["bar_range"], last["prior_range_median_10"])
    last["prior_compression"] = _safe_div(last["prior_range_mean_3"], last["prior_range_median_10"])
    last["compression_release"] = (
        (1.0 - last["prior_compression"].clip(0.0, 1.0))
        * last["range_expansion"].clip(0.0, 4.0)
    )
    last["vwap_dist_atr"] = _safe_div(last["close"] - last["session_vwap"], last["atr_value"])
    last["ema20_dist_atr"] = _safe_div(last["close"] - _numeric(last, "EMA_20"), last["atr_value"])
    last["breakout_pressure"] = _safe_div(last["close"] - last["prior_high_12"], last["atr_value"])
    last["breakdown_pressure"] = _safe_div(last["prior_low_12"] - last["close"], last["atr_value"])

    finite_ohlc = last[["open", "high", "low", "close"]].notna().all(axis=1)
    eligible = (
        finite_ohlc
        & last["close"].ge(cfg.min_price_rs)
        & last["completed_real_bars_available"].ge(cfg.min_bars)
        & ~last["current_opening_snapshot"]
        & last["staleness_seconds"].between(0.0, cfg.max_staleness_seconds, inclusive="both")
        & last["prior_value_median_20"].fillna(0.0).ge(cfg.min_median_traded_value_rs)
    )
    last["eligible"] = eligible.astype(bool)
    last["reject_reason"] = ""
    last.loc[~finite_ohlc, "reject_reason"] = "invalid_ohlc"
    last.loc[finite_ohlc & last["close"].lt(cfg.min_price_rs), "reject_reason"] = "price_below_min"
    last.loc[finite_ohlc & last["completed_real_bars_available"].lt(cfg.min_bars), "reject_reason"] = "insufficient_bars"
    last.loc[last["current_opening_snapshot"], "reject_reason"] = "opening_snapshot_warmup"
    stale = ~last["staleness_seconds"].between(0.0, cfg.max_staleness_seconds, inclusive="both")
    last.loc[finite_ohlc & stale, "reject_reason"] = "stale_data"
    low_value = last["prior_value_median_20"].fillna(0.0).lt(cfg.min_median_traded_value_rs)
    last.loc[finite_ohlc & low_value, "reject_reason"] = "traded_value_below_min"

    # Cross-sectional relative features are computed only after all ticker-level
    # features are frozen for the requested slot.
    benchmark = last.loc[last["ticker"].eq("NIFTYBEES") & eligible]
    market_ret_15 = (
        float(benchmark["ret_15m"].iloc[0])
        if not benchmark.empty and pd.notna(benchmark["ret_15m"].iloc[0])
        else _finite_median(last.loc[eligible, "ret_15m"])
    )
    market_ret_60 = (
        float(benchmark["ret_60m"].iloc[0])
        if not benchmark.empty and pd.notna(benchmark["ret_60m"].iloc[0])
        else _finite_median(last.loc[eligible, "ret_60m"])
    )
    last["relative_strength_15m"] = last["ret_15m"] - market_ret_15
    last["relative_strength_60m"] = last["ret_60m"] - market_ret_60
    # Sector context is recorded but deliberately not scored in V1; current
    # sector mappings do not yet have sufficient coverage for a safe gate.
    last["sector_relative_strength_15m"] = last["relative_strength_15m"]

    ranks: dict[str, pd.Series] = {
        "rs15": _rank01(last["relative_strength_15m"], eligible),
        "rs60": _rank01(last["relative_strength_60m"], eligible),
        "sector_rs": _rank01(last["sector_relative_strength_15m"], eligible),
        "bar_ret": _rank01(last["bar_return"], eligible),
        "ret30": _rank01(last["ret_30m"], eligible),
        "vwap": _rank01(last["vwap_dist_atr"], eligible),
        "close_loc": _rank01(last["close_location"], eligible),
        "rvol": _rank01(last["rvol_20"], eligible),
        "range": _rank01(last["range_expansion"], eligible),
        "compression": _rank01(last["compression_release"], eligible),
        "trend": _rank01(last["trend_consistency_6"], eligible),
        "breakout": _rank01(last["breakout_pressure"], eligible),
        "breakdown": _rank01(last["breakdown_pressure"], eligible),
        "liquidity": _rank01(np.log1p(last["prior_value_median_20"].clip(lower=0.0)), eligible),
        "atr_pct": _rank01(last["atr_pct"], eligible),
        "rsi": _rank01(_numeric(last, "RSI"), eligible),
    }

    last["activity_score"] = (
        0.35 * ranks["rvol"]
        + 0.30 * ranks["range"]
        + 0.20 * ranks["atr_pct"]
        + 0.15 * ranks["liquidity"]
    )
    last["momentum_long_score"] = (
        0.20 * ranks["rs15"]
        + 0.14 * ranks["rs60"]
        + 0.08 * ranks["rs15"]
        + 0.14 * ranks["vwap"]
        + 0.12 * ranks["close_loc"]
        + 0.12 * ranks["rvol"]
        + 0.10 * ranks["range"]
        + 0.10 * ranks["trend"]
    )
    last["momentum_short_score"] = (
        0.20 * (1.0 - ranks["rs15"])
        + 0.14 * (1.0 - ranks["rs60"])
        + 0.08 * (1.0 - ranks["rs15"])
        + 0.14 * (1.0 - ranks["vwap"])
        + 0.12 * (1.0 - ranks["close_loc"])
        + 0.12 * ranks["rvol"]
        + 0.10 * ranks["range"]
        + 0.10 * (1.0 - ranks["trend"])
    )
    last["reversal_long_score"] = (
        0.20 * (1.0 - ranks["ret30"])
        + 0.20 * ranks["bar_ret"]
        + 0.14 * ranks["close_loc"]
        + 0.16 * last["activity_score"]
        + 0.12 * (1.0 - ranks["rsi"])
        + 0.10 * ranks["compression"]
        + 0.08 * ranks["liquidity"]
    )
    last["reversal_short_score"] = (
        0.20 * ranks["ret30"]
        + 0.20 * (1.0 - ranks["bar_ret"])
        + 0.14 * (1.0 - ranks["close_loc"])
        + 0.16 * last["activity_score"]
        + 0.12 * ranks["rsi"]
        + 0.10 * ranks["compression"]
        + 0.08 * ranks["liquidity"]
    )
    last["expansion_long_score"] = (
        0.22 * ranks["range"]
        + 0.18 * ranks["rvol"]
        + 0.18 * ranks["breakout"]
        + 0.14 * ranks["bar_ret"]
        + 0.10 * ranks["rs15"]
        + 0.08 * ranks["close_loc"]
        + 0.10 * ranks["compression"]
    )
    last["expansion_short_score"] = (
        0.22 * ranks["range"]
        + 0.18 * ranks["rvol"]
        + 0.18 * ranks["breakdown"]
        + 0.14 * (1.0 - ranks["bar_ret"])
        + 0.10 * (1.0 - ranks["rs15"])
        + 0.08 * (1.0 - ranks["close_loc"])
        + 0.10 * ranks["compression"]
    )

    family_max = last[list(FAMILY_SCORE_COLUMNS)].max(axis=1)
    last["long_score"] = 0.88 * last[[
        "momentum_long_score", "reversal_long_score", "expansion_long_score"
    ]].max(axis=1) + 0.12 * last["activity_score"]
    last["short_score"] = 0.88 * last[[
        "momentum_short_score", "reversal_short_score", "expansion_short_score"
    ]].max(axis=1) + 0.12 * last["activity_score"]
    last["overall_score"] = 0.94 * family_max + 0.06 * last["activity_score"]
    for column in (*FAMILY_SCORE_COLUMNS, "activity_score", "long_score", "short_score", "overall_score"):
        last[column] = last[column].where(eligible, 0.0).clip(0.0, 1.0)

    primary_column = last[list(FAMILY_SCORE_COLUMNS)].idxmax(axis=1)
    last["primary_side"] = primary_column.map(lambda name: FAMILY_LABELS[name][0])
    last["primary_family"] = primary_column.map(lambda name: FAMILY_LABELS[name][1])
    last["selection_reason"] = last["primary_side"] + ":" + last["primary_family"]
    last["feature_history_status"] = np.where(
        last["completed_real_bars_available"].ge(cfg.feature_min_observations),
        "READY",
        "WARMUP_NEUTRAL_FEATURES",
    )
    last["data_quality_reason"] = np.select(
        [last["current_gap_filled"], last["feature_history_status"].ne("READY")],
        ["CURRENT_BAR_GAP_FILLED", "LIMITED_HISTORY"],
        default="OK",
    )
    last["slot_ist"] = slot.isoformat()
    last["market_breadth_15m"] = float(last.loc[eligible, "ret_15m"].gt(0.0).mean()) if eligible.any() else np.nan
    last["market_median_return_15m"] = market_ret_15

    stats = FeatureBuildStats(
        input_rows=input_rows,
        causal_rows=int(len(work)),
        universe_count=int(len(last)),
        eligible_count=int(eligible.sum()),
        rejected_count=int((~eligible).sum()),
    )
    return last.sort_values("ticker").reset_index(drop=True), stats


def rank_universe(features: pd.DataFrame) -> pd.DataFrame:
    """Assign a deterministic overall rank to every eligible symbol."""

    if features is None or features.empty:
        return pd.DataFrame() if features is None else features.copy()
    ranked = features.copy()
    ranked["universe_rank"] = pd.Series(pd.NA, index=ranked.index, dtype="Int64")
    eligible_index = (
        ranked.loc[ranked["eligible"].fillna(False)]
        .sort_values(["overall_score", "activity_score", "ticker"], ascending=[False, False, True], kind="mergesort")
        .index
    )
    ranked.loc[eligible_index, "universe_rank"] = np.arange(1, len(eligible_index) + 1)
    return ranked.sort_values(["eligible", "universe_rank", "ticker"], ascending=[False, True, True], na_position="last").reset_index(drop=True)


def _stable_exploration_order(tickers: Sequence[str], slot_ist: str) -> list[str]:
    return sorted(
        tickers,
        key=lambda ticker: hashlib.sha256(f"{slot_ist}|{ticker}".encode("utf-8")).hexdigest(),
    )


def select_candidates(
    ranked: pd.DataFrame,
    budget: int,
    config: PrefilterConfig | None = None,
    *,
    carryover_tickers: Iterable[str] = (),
) -> pd.DataFrame:
    """Select a deterministic, setup-family-aware shadow shortlist."""

    cfg = (config or PrefilterConfig()).validate()
    if budget <= 0:
        raise ValueError("budget must be positive")
    if ranked is None or ranked.empty:
        return pd.DataFrame() if ranked is None else ranked.iloc[0:0].copy()
    if "weighted_selection_rank" in ranked.columns:
        precomputed_rank = pd.to_numeric(ranked["weighted_selection_rank"], errors="coerce")
        selected_frame = ranked.loc[precomputed_rank.le(int(budget))].copy()
        selected_frame = selected_frame.sort_values("weighted_selection_rank", kind="mergesort")
        selected_frame["selection_rank"] = pd.to_numeric(
            selected_frame["weighted_selection_rank"], errors="coerce"
        ).astype(int)
        selected_frame["selection_bucket"] = selected_frame.get(
            "weighted_selection_bucket", "PRECOMPUTED_WEIGHTED_STREAM"
        )
        selected_frame["selection_budget"] = int(budget)
        return selected_frame.reset_index(drop=True)
    pool = ranked.loc[ranked["eligible"].fillna(False)].copy()
    if pool.empty:
        return pool
    pool = pool.sort_values(["overall_score", "activity_score", "ticker"], ascending=[False, False, True], kind="mergesort")
    target = min(int(budget), int(len(pool)))
    selected: list[str] = []
    buckets: dict[str, str] = {}
    pool_by_ticker = set(pool["ticker"].astype(str))
    requested_carryover = {str(value).upper().strip() for value in carryover_tickers}
    slot_value = str(pool["slot_ist"].iloc[0]) if "slot_ist" in pool.columns else ""

    stream_rows: dict[str, list[str]] = {
        "LONG": pool.sort_values(["long_score", "activity_score", "ticker"], ascending=[False, False, True], kind="mergesort")["ticker"].tolist(),
        "SHORT": pool.sort_values(["short_score", "activity_score", "ticker"], ascending=[False, False, True], kind="mergesort")["ticker"].tolist(),
        "ACTIVITY": pool.sort_values(["activity_score", "overall_score", "ticker"], ascending=[False, False, True], kind="mergesort")["ticker"].tolist(),
    }
    stream_weights: dict[str, float] = {
        "LONG": cfg.long_stream_fraction,
        "SHORT": cfg.short_stream_fraction,
        "ACTIVITY": cfg.activity_stream_fraction,
    }
    if cfg.carryover_fraction > 0 and requested_carryover:
        stream_rows["CARRYOVER"] = pool.loc[pool["ticker"].isin(requested_carryover), "ticker"].tolist()
        stream_weights["CARRYOVER"] = cfg.carryover_fraction
    if cfg.exploration_fraction > 0:
        stream_rows["EXPLORATION"] = _stable_exploration_order(pool["ticker"].astype(str).tolist(), slot_value)
        stream_weights["EXPLORATION"] = cfg.exploration_fraction

    # Smooth weighted-fair merge.  The order is built independently of K, so
    # every configured shortlist is an exact prefix of the next larger one.
    cursors = {name: 0 for name in stream_rows}
    served = {name: 0 for name in stream_rows}
    active = {name for name, weight in stream_weights.items() if weight > 0 and stream_rows.get(name)}
    while active and len(selected) < len(pool):
        name = min(active, key=lambda value: (served[value] / stream_weights[value], value))
        values = stream_rows[name]
        added = False
        while cursors[name] < len(values):
            ticker = str(values[cursors[name]]).upper().strip()
            cursors[name] += 1
            if ticker and ticker in pool_by_ticker and ticker not in buckets:
                selected.append(ticker)
                buckets[ticker] = f"WEIGHTED_STREAM:{name}"
                served[name] += 1
                added = True
                break
        if not added:
            active.discard(name)

    # Defensive fill for an exhausted/degenerate stream configuration.
    for ticker in pool["ticker"]:
        ticker_s = str(ticker).upper().strip()
        if ticker_s not in buckets:
            selected.append(ticker_s)
            buckets[ticker_s] = "CORE_RANK_FILL"

    selected = selected[:target]
    selected_frame = pool.set_index("ticker").loc[selected].reset_index()
    selected_frame["selection_rank"] = np.arange(1, len(selected_frame) + 1)
    selected_frame["selection_bucket"] = selected_frame["ticker"].map(buckets)
    selected_frame["selection_budget"] = int(budget)
    return selected_frame


def annotate_budget_grid(
    ranked: pd.DataFrame,
    config: PrefilterConfig | None = None,
    *,
    carryover_tickers: Iterable[str] = (),
) -> pd.DataFrame:
    """Add deterministic selection flags/ranks for every configured K."""

    cfg = (config or PrefilterConfig()).validate()
    output = ranked.copy()
    largest_budget = max((*cfg.budget_grid, cfg.budget))
    selection = select_candidates(
        output,
        largest_budget,
        cfg,
        carryover_tickers=carryover_tickers,
    )
    rank_map = selection.set_index("ticker")["selection_rank"] if not selection.empty else pd.Series(dtype=int)
    bucket_map = selection.set_index("ticker")["selection_bucket"] if not selection.empty else pd.Series(dtype=str)
    output["weighted_selection_rank"] = output["ticker"].map(rank_map).astype("Int64")
    output["weighted_selection_bucket"] = output["ticker"].map(bucket_map)
    for budget in cfg.budget_grid:
        within_budget = output["weighted_selection_rank"].le(int(budget)).fillna(False)
        output[f"selected_k{budget}"] = within_budget.astype(bool)
        output[f"selection_rank_k{budget}"] = output["weighted_selection_rank"].where(within_budget).astype("Int64")
    return output
