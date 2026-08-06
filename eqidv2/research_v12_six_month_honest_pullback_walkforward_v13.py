"""Deterministic six-month walk-forward for the V12 LONG pullback family.

This research-only runner consumes the outcome-blind 3,672-state broad pool
and its independently strict-audited 3,613-state V12 exact outcome cache.  It
does not search exits: every row uses SL 1% / target 2%, statutory costs and
V12 sizing.  A fixed registry of 64 interpretable setup filters is evaluated
with 40 warm-up sessions followed by four prior-only 20-session outer blocks.

All six months have already been inspected elsewhere.  Consequently even a
historical PF strictly above 1.5 is only a research gate and still requires a
genuinely fresh forward holdout.  No production/live file or job is changed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as v9
import research_v12_path_aware_long_rebuild as v2
import research_v12_six_month_frozen_balanced_long_replay_v11 as strict_v11


SETUP = "L_CAUSAL_PULLBACK_WALKFORWARD_V13_RESEARCH"
PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
FRESH_FORWARD_HOLDOUT_REQUIRED = True

START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
RESEARCH_FREEZE_DATE = "2026-08-06"
FRESH_FORWARD_START_POLICY = (
    "FIRST_UNTOUCHED_EXCHANGE_SESSION_AFTER_RESEARCH_FREEZE_DATE"
)
EXPECTED_SESSIONS = 120
WARMUP_SESSIONS = 40
OUTER_BLOCK_SIZE = 20
OUTER_BLOCKS = 4
STOP_LOSS_PCT = 1.0
TARGET_PCT = 2.0
MAX_DAILY_CAP = 15
DAILY_CAPS = (5, 8, 10, 15)
EXPECTED_REGISTRY_SIZE = 64
EXPECTED_CACHE_COUNTS = {
    "broad": 3_672,
    "raw": 3_671,
    "strict_exact": 3_613,
    "strict_rejects": 58,
}
PRIMARY_POSTHOC_DIAGNOSTIC_ID = (
    "PB_DIAG_L009216_MOM_R220_259_ADX25_55_TRUE_NIFTY_NONNEG_CAP15"
)

CACHE_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_six_month_honest_pullback_v12_20260205_20260804"
)
OUTPUT_DIR = CACHE_DIR / "walkforward_v13"
TRUE_NIFTY_PATH = (
    Path(v9.v12.V7_HIST_INDICATORS_5M_DIR)
    / "NIFTY50_INDEX_stocks_indicators_5min.parquet"
)

BROAD_GUARDS: Mapping[str, Any] = {
    "primary_side": "LONG",
    "rank": [200, 300],
    "signal_minute": [600, 780],
    "atr_pct_min": 0.35,
    "session_return_so_far_pct_min": 0.50,
    "vwap_dist_atr_min": 0.25,
    "close_position_in_bar_min": 0.60,
    "ret_5m_pct": [0.10, 0.75],
    "previous_ret_5m_pct_max": -0.05,
    "ema20_dist_atr_min": 0.0,
    "score_margin_min": 0.0,
    "require_contiguous_previous": True,
    "missing_policy": "FAIL_CLOSED",
}

CAUSAL_FILTER_ALLOWLIST = frozenset({
    "primary_family", "selection_rank", "signal_minute", "signal_time_ist",
    "trade_date", "atr_pct", "session_return_so_far_pct", "vwap_dist_atr",
    "close_position_in_bar", "range_pct", "ret_5m_pct",
    "previous_ret_5m_pct", "return_acceleration_5_vs_15",
    "contiguous_previous", "ema20_dist_atr",
    "score_margin", "ADX", "RSI", "volume_ratio20",
    "niftybees_context_available", "niftybees_context_time_ist",
    "niftybees_market_return_pct", "niftybees_regime",
    "niftybees_vwap_available", "niftybees_above_session_vwap",
    "true_nifty_context_available", "true_nifty_context_time_ist",
    "true_nifty_daily_change_pct",
})
FILTER_REFERENCED_FEATURES = frozenset({
    "primary_family", "selection_rank", "signal_minute", "atr_pct",
    "session_return_so_far_pct", "vwap_dist_atr", "close_position_in_bar",
    "range_pct", "ret_5m_pct", "previous_ret_5m_pct",
    "return_acceleration_5_vs_15", "contiguous_previous",
    "ema20_dist_atr", "score_margin", "ADX", "RSI",
    "volume_ratio20", "niftybees_context_available",
    "niftybees_context_time_ist", "niftybees_vwap_available",
    "niftybees_above_session_vwap",
    "true_nifty_context_available", "true_nifty_context_time_ist",
    "true_nifty_daily_change_pct",
})
FORBIDDEN_FILTER_FIELDS = frozenset({
    "entry_time_ist", "entry_price", "exit_time_ist", "exit_price",
    "outcome", "gross_pnl_rs", "cost_rs", "net_pnl_rs", "bars_held",
})

HISTORICAL_GATE = {
    "profit_factor_strictly_above": 1.50,
    "minimum_trades": 80,
    "minimum_trades_per_session": 1.0,
    "minimum_median_trades_per_session": 1.0,
    "minimum_active_days": 48,
    "minimum_profitable_outer_blocks": 3,
    "minimum_pf_one_outer_blocks": 3,
    "minimum_trades_each_outer_block": 10,
    "cost_stress_multiplier": 1.50,
    "cost_stress_minimum_pf": 1.10,
    "maximum_best_day_positive_share": 0.30,
    "maximum_best_ticker_positive_share": 0.25,
}


@dataclass(frozen=True)
class PullbackFilter:
    config_id: str
    blueprint: str
    daily_cap: int
    rank_min: int = 200
    rank_max: int = 300
    signal_minute_min: int = 600
    signal_minute_max: int = 780
    atr_pct_min: float = 0.35
    session_return_min: float = 0.50
    vwap_dist_atr_min: float = 0.25
    close_position_min: float = 0.60
    range_pct_min: float | None = None
    ret_5m_min: float = 0.10
    ret_5m_max: float = 0.75
    previous_ret_5m_max: float = -0.05
    ema20_dist_atr_min: float = 0.0
    score_margin_min: float = 0.0
    primary_family: str | None = None
    adx_min: float | None = None
    adx_max_exclusive: float | None = None
    rsi_min: float | None = None
    rsi_max: float | None = None
    volume_ratio20_min: float | None = None
    return_acceleration_min: float | None = None
    require_niftybees_above_session_vwap: bool = False
    true_nifty_daily_change_min: float | None = None
    diagnostic_only: bool = False


@dataclass(frozen=True)
class OuterFold:
    fold: int
    training_sessions: tuple[str, ...]
    evaluation_sessions: tuple[str, ...]


def json_safe(value: Any) -> Any:
    return v9.json_safe(value)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def config_hash(config: PullbackFilter) -> str:
    raw = json.dumps(json_safe(asdict(config)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def assert_causal_contract() -> None:
    missing = FILTER_REFERENCED_FEATURES - CAUSAL_FILTER_ALLOWLIST
    forbidden = FILTER_REFERENCED_FEATURES & FORBIDDEN_FILTER_FIELDS
    if missing or forbidden:
        raise RuntimeError(
            f"invalid causal filter contract: missing={sorted(missing)}, "
            f"forbidden={sorted(forbidden)}"
        )


def _base(blueprint: str, **changes: Any) -> PullbackFilter:
    return PullbackFilter(
        config_id=f"UNHASHED_{blueprint}",
        blueprint=blueprint,
        daily_cap=15,
        **changes,
    )


def filter_blueprints() -> tuple[PullbackFilter, ...]:
    """Fourteen fixed comparison blueprints plus two post-hoc diagnostics.

    The two L009216+true-NIFTY blueprints are outcome-informed diagnostics and
    are mechanically excluded from every selection/refit pool.
    """

    l009216 = {
        "session_return_min": 1.0,
        "vwap_dist_atr_min": 0.90,
        "close_position_min": 0.65,
        "range_pct_min": 0.25,
        "ret_5m_min": 0.15,
        "ret_5m_max": 0.35,
        "previous_ret_5m_max": -0.10,
        "ema20_dist_atr_min": 1.0,
        "score_margin_min": 0.05,
    }
    diagnostic = {
        **l009216,
        "rank_min": 220,
        "rank_max": 259,
        "primary_family": "MOMENTUM",
        "adx_min": 25.0,
        "adx_max_exclusive": 55.0,
        "true_nifty_daily_change_min": 0.0,
        "diagnostic_only": True,
    }
    return (
        _base("BASE"),
        _base("MOMENTUM", primary_family="MOMENTUM"),
        _base("EXPANSION", primary_family="EXPANSION"),
        _base("ADX25_55", adx_min=25.0, adx_max_exclusive=55.0),
        _base("RSI50_75", rsi_min=50.0, rsi_max=75.0),
        _base("ATR055", atr_pct_min=0.55),
        _base("RANK220_259", rank_min=220, rank_max=259),
        _base("EARLY_600_690", signal_minute_max=690),
        _base("NIFTYBEES_ABOVE_SESSION_VWAP", require_niftybees_above_session_vwap=True),
        _base(
            "MOMENTUM_ADX25_55", primary_family="MOMENTUM",
            adx_min=25.0, adx_max_exclusive=55.0,
        ),
        _base("ORIGINAL_PROXY_ATR105_RANGE125", atr_pct_min=1.05, range_pct_min=1.25),
        _base(
            "MOMENTUM_NIFTYBEES_ABOVE_VWAP", primary_family="MOMENTUM",
            require_niftybees_above_session_vwap=True,
        ),
        _base("ATR055_VOLUME070", atr_pct_min=0.55, volume_ratio20_min=0.70),
        _base("FROZEN_L009216", **l009216),
        _base("DIAG_L009216_MOM_R220_259_ADX25_55_TRUE_NIFTY_NONNEG", **diagnostic),
        _base(
            "DIAG_L009216_MOM_R220_259_ADX25_55_TRUE_NIFTY_NONNEG_BEFORE_NOON",
            **{**diagnostic, "signal_minute_max": 715},
        ),
    )


def candidate_registry() -> tuple[PullbackFilter, ...]:
    configs: list[PullbackFilter] = []
    for blueprint in filter_blueprints():
        for cap in DAILY_CAPS:
            configs.append(replace(
                blueprint,
                config_id=f"PB_{blueprint.blueprint}_CAP{cap:02d}",
                daily_cap=cap,
            ))
    if len(configs) != EXPECTED_REGISTRY_SIZE:
        raise RuntimeError(f"registry size changed: {len(configs)}")
    if len({config.config_id for config in configs}) != len(configs):
        raise RuntimeError("duplicate registry ids")
    if len({config_hash(config) for config in configs}) != len(configs):
        raise RuntimeError("duplicate registry configurations")
    for config in configs:
        if config.daily_cap not in DAILY_CAPS or config.daily_cap > MAX_DAILY_CAP:
            raise RuntimeError(f"invalid daily cap: {config.config_id}")
        if config.rank_min < 200 or config.rank_max > 300:
            raise RuntimeError(f"filter escapes rank union: {config.config_id}")
        if config.signal_minute_min < 600 or config.signal_minute_max > 780:
            raise RuntimeError(f"filter escapes time union: {config.config_id}")
    return tuple(configs)


def registry_hash(registry: Sequence[PullbackFilter]) -> str:
    raw = json.dumps(
        json_safe([asdict(config) for config in registry]), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def posthoc_frontier_configs() -> tuple[PullbackFilter, PullbackFilter]:
    """Outcome-informed frontier diagnostics; never part of the 64 registry."""

    common = {
        "daily_cap": 15,
        "rank_max": 259,
        "primary_family": "MOMENTUM",
        "adx_min": 25.0,
        "adx_max_exclusive": 55.0,
        "session_return_min": 1.0,
        "vwap_dist_atr_min": 0.90,
        "close_position_min": 0.65,
        "range_pct_min": 0.25,
        "ret_5m_min": 0.15,
        "ret_5m_max": 0.35,
        "previous_ret_5m_max": -0.10,
        "ema20_dist_atr_min": 1.0,
        "score_margin_min": 0.05,
        "return_acceleration_min": 0.0,
        "true_nifty_daily_change_min": 0.0,
        "diagnostic_only": True,
    }
    return (
        PullbackFilter(
            config_id="POSTHOC_PRECISION_R230_259_ACCEL_NONNEG",
            blueprint="POSTHOC_PRECISION",
            rank_min=230,
            **common,
        ),
        PullbackFilter(
            config_id="POSTHOC_BALANCED_R220_259_ACCEL_NONNEG",
            blueprint="POSTHOC_BALANCED",
            rank_min=220,
            **common,
        ),
    )


def session_calendar() -> list[str]:
    days = strict_v11.session_calendar()
    if len(days) != EXPECTED_SESSIONS:
        raise RuntimeError(f"calendar changed: {len(days)}")
    return days


def outer_folds(sessions: Sequence[str]) -> tuple[OuterFold, ...]:
    days = list(sessions)
    if len(days) != 120 or len(set(days)) != 120 or days != sorted(days):
        raise ValueError("protocol requires 120 unique sorted sessions")
    folds: list[OuterFold] = []
    for index in range(OUTER_BLOCKS):
        cut = WARMUP_SESSIONS + index * OUTER_BLOCK_SIZE
        training = tuple(days[:cut])
        evaluation = tuple(days[cut:cut + OUTER_BLOCK_SIZE])
        if training[-1] >= evaluation[0]:
            raise RuntimeError("outer-fold lookahead")
        folds.append(OuterFold(index + 1, training, evaluation))
    if [day for fold in folds for day in fold.evaluation_sessions] != days[40:]:
        raise RuntimeError("outer folds do not cover final 80 sessions exactly once")
    return tuple(folds)


def _ist_timestamp(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="raise")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize("Asia/Kolkata")
    return parsed.dt.tz_convert("Asia/Kolkata")


def _load_niftybees_prepared(sessions: Sequence[str]) -> pd.DataFrame:
    for ticker in ("NIFTYBEES", "NIFTYBEES_PROXY"):
        frame = v9.v12._tier123_read_5m(
            ticker, Path(v9.v12.V7_HIST_INDICATORS_5M_DIR), set(sessions)
        )
        if frame is not None and not frame.empty:
            return frame
    return pd.DataFrame()


def attach_exact_completed_market_features(
    frame: pd.DataFrame,
    sessions: Sequence[str],
    *,
    niftybees: pd.DataFrame | None = None,
    true_nifty: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Exact-timestamp market joins; no as-of/future fallback is permitted."""

    work = frame.copy()
    work["signal_time_ist"] = _ist_timestamp(work["signal_time_ist"])
    if niftybees is None:
        niftybees = _load_niftybees_prepared(sessions)
    if true_nifty is None:
        if not TRUE_NIFTY_PATH.exists():
            true_nifty = pd.DataFrame()
        else:
            true_nifty = pd.read_parquet(
                TRUE_NIFTY_PATH, columns=["date", "Daily_Change"]
            )

    if niftybees is None or niftybees.empty:
        bees = pd.DataFrame(columns=[
            "signal_time_ist", "niftybees_market_return_pct",
            "niftybees_regime", "niftybees_above_session_vwap",
        ])
    else:
        bees = niftybees.copy()
        bees["signal_time_ist"] = _ist_timestamp(bees["date"])
        bees = bees.sort_values("signal_time_ist", kind="mergesort").drop_duplicates(
            "signal_time_ist", keep="last"
        )
        day_open = bees.groupby(bees["signal_time_ist"].dt.strftime("%Y-%m-%d"))["open"].transform("first")
        close = pd.to_numeric(bees["close"], errors="coerce")
        vwap = pd.to_numeric(bees["VWAP"], errors="coerce")
        bees["niftybees_market_return_pct"] = (close / pd.to_numeric(day_open) - 1.0) * 100.0
        adx = pd.to_numeric(bees.get("ADX"), errors="coerce")
        bees["niftybees_regime"] = np.select(
            [
                vwap.notna() & close.gt(vwap) & bees["niftybees_market_return_pct"].ge(0.20),
                vwap.notna() & close.lt(vwap) & bees["niftybees_market_return_pct"].le(-0.20),
                adx.ge(25.0),
            ],
            ["BULL", "BEAR", "TREND"],
            default="NEUTRAL",
        )
        bees["niftybees_above_session_vwap"] = vwap.notna() & close.gt(vwap)
        bees = bees[[
            "signal_time_ist", "niftybees_market_return_pct",
            "niftybees_regime", "niftybees_above_session_vwap",
        ]]

    if true_nifty is None or true_nifty.empty:
        true = pd.DataFrame(columns=["signal_time_ist", "true_nifty_daily_change_pct"])
    else:
        true = true_nifty.copy()
        true["signal_time_ist"] = _ist_timestamp(true["date"])
        true["true_nifty_daily_change_pct"] = pd.to_numeric(
            true["Daily_Change"], errors="coerce"
        )
        true = true.sort_values("signal_time_ist", kind="mergesort").drop_duplicates(
            "signal_time_ist", keep="last"
        )[["signal_time_ist", "true_nifty_daily_change_pct"]]

    work = work.merge(bees, on="signal_time_ist", how="left", validate="many_to_one")
    work = work.merge(true, on="signal_time_ist", how="left", validate="many_to_one")
    work["niftybees_context_available"] = work["niftybees_market_return_pct"].notna()
    work["niftybees_vwap_available"] = work["niftybees_above_session_vwap"].notna()
    work["niftybees_context_time_ist"] = work["signal_time_ist"].where(
        work["niftybees_context_available"]
    )
    above_vwap = work["niftybees_above_session_vwap"]
    work["niftybees_above_session_vwap"] = above_vwap.where(
        above_vwap.notna(), False
    ).astype(bool)
    work["niftybees_regime"] = work["niftybees_regime"].fillna("UNKNOWN")
    work["true_nifty_context_available"] = work["true_nifty_daily_change_pct"].notna()
    work["true_nifty_context_time_ist"] = work["signal_time_ist"].where(
        work["true_nifty_context_available"]
    )
    for column in ("niftybees_context_time_ist", "true_nifty_context_time_ist"):
        available = work[column].notna()
        if available.any() and (work.loc[available, column] > work.loc[available, "signal_time_ist"]).any():
            raise RuntimeError(f"future market timestamp in {column}")
    return work


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def broad_mask(frame: pd.DataFrame) -> pd.Series:
    mask = frame["pre_entry_data_invalid"].eq(False)
    mask &= frame["primary_side"].astype(str).str.upper().eq("LONG")
    mask &= _num(frame, "selection_rank").between(200, 300)
    mask &= _num(frame, "signal_minute").between(600, 780)
    mask &= _num(frame, "atr_pct").ge(0.35)
    mask &= _num(frame, "session_return_so_far_pct").ge(0.50)
    mask &= _num(frame, "vwap_dist_atr").ge(0.25)
    mask &= _num(frame, "close_position_in_bar").ge(0.60)
    mask &= _num(frame, "ret_5m_pct").between(0.10, 0.75)
    mask &= _num(frame, "previous_ret_5m_pct").le(-0.05)
    mask &= frame["contiguous_previous"].fillna(False).astype(bool)
    mask &= _num(frame, "ema20_dist_atr").ge(0.0)
    mask &= _num(frame, "score_margin").ge(0.0)
    return mask.fillna(False)


def filter_mask(frame: pd.DataFrame, config: PullbackFilter) -> np.ndarray:
    assert_causal_contract()
    mask = (
        _num(frame, "selection_rank").between(config.rank_min, config.rank_max)
        & _num(frame, "signal_minute").between(config.signal_minute_min, config.signal_minute_max)
        & _num(frame, "atr_pct").ge(config.atr_pct_min)
        & _num(frame, "session_return_so_far_pct").ge(config.session_return_min)
        & _num(frame, "vwap_dist_atr").ge(config.vwap_dist_atr_min)
        & _num(frame, "close_position_in_bar").ge(config.close_position_min)
        & _num(frame, "ret_5m_pct").between(config.ret_5m_min, config.ret_5m_max)
        & _num(frame, "previous_ret_5m_pct").le(config.previous_ret_5m_max)
        & frame["contiguous_previous"].fillna(False).astype(bool)
        & _num(frame, "ema20_dist_atr").ge(config.ema20_dist_atr_min)
        & _num(frame, "score_margin").ge(config.score_margin_min)
    )
    if config.range_pct_min is not None:
        mask &= _num(frame, "range_pct").ge(config.range_pct_min)
    if config.primary_family is not None:
        mask &= frame["primary_family"].astype(str).str.upper().eq(config.primary_family)
    if config.adx_min is not None:
        mask &= _num(frame, "ADX").ge(config.adx_min)
    if config.adx_max_exclusive is not None:
        mask &= _num(frame, "ADX").lt(config.adx_max_exclusive)
    if config.rsi_min is not None:
        mask &= _num(frame, "RSI").ge(config.rsi_min)
    if config.rsi_max is not None:
        mask &= _num(frame, "RSI").le(config.rsi_max)
    if config.volume_ratio20_min is not None:
        mask &= _num(frame, "volume_ratio20").ge(config.volume_ratio20_min)
    if config.return_acceleration_min is not None:
        mask &= _num(frame, "return_acceleration_5_vs_15").ge(
            config.return_acceleration_min
        )
    if config.require_niftybees_above_session_vwap:
        context = pd.to_datetime(frame["niftybees_context_time_ist"], errors="coerce", utc=True)
        signal = pd.to_datetime(frame["signal_time_ist"], errors="coerce", utc=True)
        mask &= frame["niftybees_context_available"].fillna(False).astype(bool)
        mask &= frame["niftybees_vwap_available"].fillna(False).astype(bool)
        mask &= context.notna() & signal.notna() & context.eq(signal)
        mask &= frame["niftybees_above_session_vwap"].fillna(False).astype(bool)
    if config.true_nifty_daily_change_min is not None:
        context = pd.to_datetime(frame["true_nifty_context_time_ist"], errors="coerce", utc=True)
        signal = pd.to_datetime(frame["signal_time_ist"], errors="coerce", utc=True)
        mask &= frame["true_nifty_context_available"].fillna(False).astype(bool)
        mask &= context.notna() & signal.notna() & context.eq(signal)
        mask &= _num(frame, "true_nifty_daily_change_pct").ge(
            config.true_nifty_daily_change_min
        )
    return mask.fillna(False).to_numpy(dtype=bool)


class SearchArrays:
    def __init__(self, frame: pd.DataFrame, sessions: Sequence[str]):
        work = frame.copy()
        work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
        work["trade_date"] = work["trade_date"].astype(str)
        work["signal_time_ist"] = _ist_timestamp(work["signal_time_ist"])
        work = work.sort_values(
            [
                "trade_date", "signal_time_ist", "selection_rank", "ticker",
                "candidate_key" if "candidate_key" in work else "_optimizer_row_id",
            ],
            kind="mergesort",
        ).reset_index(drop=True)
        if work["_optimizer_row_id"].duplicated().any():
            raise RuntimeError("duplicate exact row ids")
        self.frame = work
        self.sessions = list(sessions)
        self.day_lookup = {day: index for index, day in enumerate(self.sessions)}
        if not set(work["trade_date"]).issubset(self.day_lookup):
            raise RuntimeError("exact row outside session calendar")
        self.day_code = work["trade_date"].map(self.day_lookup).to_numpy(dtype=int)
        ticker_day = work["trade_date"] + "|" + work["ticker"]
        self.ticker_day_code = pd.factorize(ticker_day, sort=False)[0]
        self.gross = _num(work, "gross_pnl_rs").to_numpy(dtype=float)
        self.cost = _num(work, "cost_rs").to_numpy(dtype=float)
        self.ticker = work["ticker"].to_numpy(dtype=str)
        self.candidate_key = (
            work["candidate_key"].astype(str).to_numpy(dtype=str)
            if "candidate_key" in work
            else work["_optimizer_row_id"].astype(str).to_numpy(dtype=str)
        )
        entries = pd.to_datetime(
            work["entry_time_ist"] if "entry_time_ist" in work
            else work["signal_time_ist"] + pd.Timedelta(minutes=1),
            errors="raise", utc=True,
        )
        self.entry_ns = entries.astype("int64").to_numpy(dtype=np.int64)
        exits = pd.to_datetime(work["exit_time_ist"], errors="raise", utc=True)
        self.exit_ns = exits.astype("int64").to_numpy(dtype=np.int64)

    def selected_indices(self, config: PullbackFilter) -> np.ndarray:
        eligible = np.flatnonzero(filter_mask(self.frame, config))
        if not len(eligible):
            return np.asarray([], dtype=int)
        _, first = np.unique(self.ticker_day_code[eligible], return_index=True)
        one_per_ticker_day = eligible[np.sort(first)]
        selected: list[np.ndarray] = []
        codes = self.day_code[one_per_ticker_day]
        for day in np.unique(codes):
            selected.append(one_per_ticker_day[codes == day][:config.daily_cap])
        return np.concatenate(selected) if selected else np.asarray([], dtype=int)

    def scoped(self, indices: np.ndarray, sessions: Sequence[str]) -> np.ndarray:
        positions = [self.day_lookup[day] for day in sessions]
        return indices[np.isin(self.day_code[indices], positions)] if len(indices) else np.asarray([], dtype=int)

    def metrics(
        self, indices: np.ndarray, sessions: Sequence[str], *, cost_multiplier: float = 1.0
    ) -> dict[str, Any]:
        days = list(sessions)
        chosen = self.scoped(indices, days)
        pnl = self.gross[chosen] - float(cost_multiplier) * self.cost[chosen]
        gains = float(pnl[pnl > 0].sum())
        losses = float(-pnl[pnl < 0].sum())
        pf = gains / losses if losses else (float("inf") if gains else 0.0)
        median_abs = float(np.median(np.abs(pnl))) if len(pnl) else 0.0
        prior = 2.5 * median_abs
        shrunk = (gains + prior) / (losses + prior) if losses + prior else 0.0
        lookup = {self.day_lookup[day]: offset for offset, day in enumerate(days)}
        daily_trades = np.zeros(len(days), dtype=int)
        daily_pnl = np.zeros(len(days), dtype=float)
        for index, value in zip(chosen, pnl):
            offset = lookup[self.day_code[index]]
            daily_trades[offset] += 1
            daily_pnl[offset] += float(value)
        # Canonical realized-equity ordering is deterministic even when exits
        # share a timestamp: exit, entry, ticker, candidate key.
        order = np.lexsort((
            self.candidate_key[chosen], self.ticker[chosen],
            self.entry_ns[chosen], self.exit_ns[chosen],
        )) if len(chosen) else []
        equity = np.cumsum(pnl[order]) if len(chosen) else np.asarray([])
        peaks = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:] if len(equity) else np.asarray([])
        drawdown = equity - peaks if len(equity) else np.asarray([])
        positive_days = daily_pnl[daily_pnl > 0]
        ticker_profit: dict[str, float] = {}
        for index, value in zip(chosen, pnl):
            if value > 0:
                ticker_profit[self.ticker[index]] = ticker_profit.get(self.ticker[index], 0.0) + float(value)
        ticker_total = sum(ticker_profit.values())
        return {
            "sessions": len(days), "trades": int(len(chosen)),
            "trades_per_session": float(len(chosen) / len(days)) if days else 0.0,
            "median_trades_per_session": float(np.median(daily_trades)) if days else 0.0,
            "active_days": int(np.count_nonzero(daily_trades)),
            "zero_trade_days": int(np.count_nonzero(daily_trades == 0)),
            "net_pnl_rs": float(pnl.sum()), "gross_profit_rs": gains,
            "gross_loss_rs": losses, "profit_factor": float(pf),
            "shrunk_profit_factor": float(shrunk),
            "win_rate_pct": float(np.count_nonzero(pnl > 0) / len(pnl) * 100.0) if len(pnl) else 0.0,
            "max_drawdown_rs": float(drawdown.min()) if len(drawdown) else 0.0,
            "positive_days": int(np.count_nonzero(daily_pnl > 0)),
            "best_day_positive_pnl_share": (
                float(positive_days.max() / positive_days.sum())
                if len(positive_days) and positive_days.sum() > 0 else 0.0
            ),
            "best_ticker_positive_pnl_share": (
                float(max(ticker_profit.values()) / ticker_total) if ticker_total else 0.0
            ),
            "cost_multiplier": float(cost_multiplier),
        }

    def trades(self, indices: np.ndarray) -> pd.DataFrame:
        return self.frame.iloc[indices].copy().reset_index(drop=True)


def _log_pf(value: float) -> float:
    return float(math.log(min(max(float(value), 1e-6), 20.0)))


def training_evidence(
    arrays: SearchArrays, config: PullbackFilter, sessions: Sequence[str]
) -> dict[str, Any]:
    indices = arrays.selected_indices(config)
    full = arrays.metrics(indices, sessions)
    stress = arrays.metrics(indices, sessions, cost_multiplier=1.5)
    blocks = [list(sessions[start:start + 20]) for start in range(0, len(sessions), 20)]
    block_metrics = [arrays.metrics(indices, block) for block in blocks]
    positive_blocks = sum(metric["net_pnl_rs"] > 0 for metric in block_metrics)
    pf_one_blocks = sum(metric["profit_factor"] >= 1.0 for metric in block_metrics)
    min_trades = max(20, int(math.ceil(0.75 * len(sessions))))
    min_active = int(math.ceil(0.50 * len(sessions)))
    stable_required = int(math.ceil(len(blocks) / 2.0))
    gate = bool(
        not config.diagnostic_only and full["trades"] >= min_trades
        and full["active_days"] >= min_active and full["net_pnl_rs"] > 0
        and full["shrunk_profit_factor"] >= 1.05 and stress["net_pnl_rs"] > 0
        and stress["profit_factor"] >= 1.0 and positive_blocks >= stable_required
        and pf_one_blocks >= stable_required
        and all(metric["trades"] >= 8 for metric in block_metrics)
    )
    block_scores = [_log_pf(metric["shrunk_profit_factor"]) for metric in block_metrics]
    score = float(
        np.median(block_scores) - 0.5 * np.std(block_scores)
        + 0.15 * _log_pf(stress["shrunk_profit_factor"])
        + 0.15 * math.log(max(float(full["trades_per_session"]), 0.10))
        - (2.0 if full["trades"] < min_trades else 0.0)
        - (1.0 if full["active_days"] < min_active else 0.0)
    )
    return {
        "config_id": config.config_id, "config_sha256": config_hash(config),
        "blueprint": config.blueprint, "daily_cap": config.daily_cap,
        "diagnostic_only": config.diagnostic_only,
        "selection_eligible": not config.diagnostic_only,
        "training_start": sessions[0], "training_end": sessions[-1],
        "training_sessions": len(sessions), "training_gate_passed": gate,
        "evidence_score": score, "positive_prior_blocks": positive_blocks,
        "pf_one_prior_blocks": pf_one_blocks, "prior_blocks": len(blocks),
        **{f"training_{key}": value for key, value in full.items()},
        **{f"training_cost1p5_{key}": value for key, value in stress.items()},
        "prior_block_metrics": block_metrics,
    }


def select_from_prior(
    arrays: SearchArrays, registry: Sequence[PullbackFilter], sessions: Sequence[str]
) -> tuple[PullbackFilter, dict[str, Any], pd.DataFrame]:
    evidence = [training_evidence(arrays, config, sessions) for config in registry]
    eligible = [row for row in evidence if row["selection_eligible"]]
    gated = [row for row in eligible if row["training_gate_passed"]]
    pool = gated or eligible
    winner = sorted(pool, key=lambda row: (-float(row["evidence_score"]), row["config_id"]))[0]
    winner = dict(winner)
    winner["selection_basis"] = (
        "PRIOR_ONLY_TRAINING_GATE" if gated else "PRIOR_ONLY_FALLBACK_FAILED_GATE"
    )
    by_id = {config.config_id: config for config in registry}
    flat = []
    for row in evidence:
        record = {key: value for key, value in row.items() if key != "prior_block_metrics"}
        record["prior_block_metrics_json"] = json.dumps(
            json_safe(row["prior_block_metrics"]), sort_keys=True
        )
        flat.append(record)
    return by_id[winner["config_id"]], winner, pd.DataFrame(flat)


def historical_gate(
    aggregate: Mapping[str, Any], stress: Mapping[str, Any], folds: pd.DataFrame
) -> dict[str, Any]:
    profitable = int((pd.to_numeric(folds["evaluation_net_pnl_rs"]) > 0).sum())
    pf_one = int((pd.to_numeric(folds["evaluation_profit_factor"]) >= 1.0).sum())
    checks = {
        "pf_strictly_above_1p5": float(aggregate["profit_factor"]) > 1.50,
        "positive_net_pnl": float(aggregate["net_pnl_rs"]) > 0,
        "minimum_trades": int(aggregate["trades"]) >= 80,
        "minimum_trades_per_session": float(aggregate["trades_per_session"]) >= 1.0,
        "minimum_median_trades_per_session": float(aggregate["median_trades_per_session"]) >= 1.0,
        "minimum_active_days": int(aggregate["active_days"]) >= 48,
        "all_fold_selections_passed_prior_gate": bool(folds["training_gate_passed"].all()),
        "minimum_profitable_outer_blocks": profitable >= 3,
        "minimum_pf_one_outer_blocks": pf_one >= 3,
        "minimum_trades_each_outer_block": bool((pd.to_numeric(folds["evaluation_trades"]) >= 10).all()),
        "cost1p5_positive": float(stress["net_pnl_rs"]) > 0,
        "cost1p5_pf_at_least_1p10": float(stress["profit_factor"]) >= 1.10,
        "best_day_concentration": float(aggregate["best_day_positive_pnl_share"]) <= 0.30,
        "best_ticker_concentration": float(aggregate["best_ticker_positive_pnl_share"]) <= 0.25,
    }
    return {
        "passed": bool(all(checks.values())), "checks": checks,
        "thresholds": HISTORICAL_GATE, "profitable_outer_blocks": profitable,
        "pf_one_outer_blocks": pf_one, "pf_is_hard_gate_not_target": True,
        "fresh_forward_holdout_in_this_run": False,
        "production_promotion_allowed": False,
    }


def run_walkforward(
    exact: pd.DataFrame, sessions: Sequence[str], registry: Sequence[PullbackFilter]
) -> dict[str, Any]:
    arrays = SearchArrays(exact, sessions)
    oos_indices: list[int] = []
    fold_rows: list[dict[str, Any]] = []
    ledgers: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    for fold in outer_folds(sessions):
        config, training, ledger = select_from_prior(arrays, registry, fold.training_sessions)
        if training["training_end"] >= fold.evaluation_sessions[0]:
            raise RuntimeError("evaluation information reached selection")
        ledger.insert(0, "outer_fold", fold.fold)
        ledgers.append(ledger)
        selected = arrays.selected_indices(config)
        evaluated = arrays.scoped(selected, fold.evaluation_sessions)
        metrics = arrays.metrics(evaluated, fold.evaluation_sessions)
        stress = arrays.metrics(evaluated, fold.evaluation_sessions, cost_multiplier=1.5)
        oos_indices.extend(evaluated.tolist())
        trades = arrays.trades(evaluated)
        trades["outer_fold"] = fold.fold
        trades["selected_config_id"] = config.config_id
        trades["selection_training_end"] = training["training_end"]
        trade_frames.append(trades)
        fold_rows.append({
            "outer_fold": fold.fold, "training_start": fold.training_sessions[0],
            "training_end": fold.training_sessions[-1],
            "evaluation_start": fold.evaluation_sessions[0],
            "evaluation_end": fold.evaluation_sessions[-1],
            "selected_config_id": config.config_id,
            "selected_config_sha256": config_hash(config),
            "selected_blueprint": config.blueprint,
            "selected_daily_cap": config.daily_cap,
            "selection_basis": training["selection_basis"],
            "training_gate_passed": training["training_gate_passed"],
            **{f"evaluation_{key}": value for key, value in metrics.items()},
            **{f"evaluation_cost1p5_{key}": value for key, value in stress.items()},
        })
    oos = np.asarray(oos_indices, dtype=int)
    if len(oos) != len(np.unique(oos)):
        raise RuntimeError("overlapping outer-fold trades")
    evaluation_days = list(sessions[40:])
    aggregate = arrays.metrics(oos, evaluation_days)
    aggregate_stress = arrays.metrics(oos, evaluation_days, cost_multiplier=1.5)
    folds = pd.DataFrame(fold_rows)
    final_config, final_training, final_ledger = select_from_prior(arrays, registry, sessions)
    final_indices = arrays.selected_indices(final_config)
    diagnostics = []
    for config in registry:
        if config.diagnostic_only:
            diagnostics.append({
                **json_safe(asdict(config)), "config_sha256": config_hash(config),
                **arrays.metrics(arrays.selected_indices(config), sessions),
            })
    return {
        "arrays": arrays, "oos_indices": oos,
        "oos_trades": pd.concat(trade_frames, ignore_index=True),
        "folds": folds, "selection_ledger": pd.concat(ledgers, ignore_index=True),
        "aggregate": aggregate, "aggregate_stress": aggregate_stress,
        "gate": historical_gate(aggregate, aggregate_stress, folds),
        "final_config": final_config, "final_training": final_training,
        "final_ledger": final_ledger, "final_indices": final_indices,
        "final_backcast": arrays.metrics(final_indices, sessions),
        "diagnostics": pd.DataFrame(diagnostics),
    }


def validate_external_cache(cache_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    paths = {
        "candidates": cache_dir / "broad_union_candidates.parquet",
        "raw": cache_dir / "entry_engine_raw.parquet",
        "exact": cache_dir / "exact_candidate_universe.parquet",
        "engine_rejects": cache_dir / "entry_engine_rejects.csv",
        "strict_rejects": cache_dir / "strict_path_rejects.csv",
        "manifest": cache_dir / "v9_exact_cache_manifest.json",
        "build_summary": cache_dir / "build_summary.json",
    }
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"strict broad cache incomplete: {missing}")
    candidates = pd.read_parquet(paths["candidates"])
    raw = pd.read_parquet(paths["raw"])
    exact = pd.read_parquet(paths["exact"])
    strict_rejects = pd.read_csv(paths["strict_rejects"])
    build = json.loads(paths["build_summary"].read_text(encoding="utf-8"))
    actual = {
        "broad": len(candidates), "raw": len(raw), "strict_exact": len(exact),
        "strict_rejects": len(strict_rejects),
    }
    if actual != EXPECTED_CACHE_COUNTS:
        raise RuntimeError(f"strict cache count drift: {actual}")
    if not broad_mask(candidates).all():
        raise RuntimeError("outcome-blind cache contains a state outside fixed broad guards")
    if not exact["strict_path_valid"].fillna(False).all():
        raise RuntimeError("exact cache contains a state without strict V11 path approval")
    required = {
        *FILTER_REFERENCED_FEATURES - {
            "niftybees_context_available", "niftybees_context_time_ist",
            "niftybees_vwap_available", "niftybees_above_session_vwap",
            "true_nifty_context_available",
            "true_nifty_context_time_ist", "true_nifty_daily_change_pct",
        },
        "ticker", "primary_side", "pre_entry_data_invalid", "_optimizer_row_id",
        "gross_pnl_rs", "cost_rs", "net_pnl_rs", "exit_time_ist", "sl_pct", "tgt_pct",
    }
    absent = required - set(exact.columns)
    if absent:
        raise RuntimeError(f"strict cache lacks required columns: {sorted(absent)}")
    if not _num(exact, "sl_pct").eq(1.0).all() or not _num(exact, "tgt_pct").eq(2.0).all():
        raise RuntimeError("strict cache exit pair is not fixed 1%/2%")
    expected_gross = (
        _num(exact, "exit_price") - _num(exact, "entry_price")
    ) * _num(exact, "quantity")
    if float((expected_gross - _num(exact, "gross_pnl_rs")).abs().max()) >= strict_v11.P_AND_L_STORAGE_TOLERANCE_RS:
        raise RuntimeError("strict cache canonical gross-P&L invariant failed")
    # Validate the parent cache with the exact V9/V11 setup contract under
    # which it was built; this checks source hashes and 2,054 input file stats.
    with strict_v11.six_month_v9_contract():
        v9.validate_exact_cache_manifest(paths["manifest"], exact, raw)
    provenance = {
        "counts": actual,
        "build_summary": build,
        "artifact_sha256": {name: sha256(path) for name, path in paths.items()},
        "source_sha256": {
            str(Path(__file__).resolve()): sha256(Path(__file__).resolve()),
            str(Path(strict_v11.__file__).resolve()): sha256(Path(strict_v11.__file__).resolve()),
            str(Path(v9.__file__).resolve()): sha256(Path(v9.__file__).resolve()),
            str(v2.SOURCE.resolve()): sha256(v2.SOURCE),
            str(TRUE_NIFTY_PATH.resolve()): sha256(TRUE_NIFTY_PATH),
        },
    }
    return candidates, exact, provenance


def daily_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    base = pd.DataFrame({"trade_date": list(sessions)})
    grouped = trades.groupby("trade_date", as_index=False).agg(
        trades=("ticker", "size"), gross_pnl_rs=("gross_pnl_rs", "sum"),
        cost_rs=("cost_rs", "sum"), net_pnl_rs=("net_pnl_rs", "sum"),
    ) if not trades.empty else pd.DataFrame(columns=[
        "trade_date", "trades", "gross_pnl_rs", "cost_rs", "net_pnl_rs"
    ])
    result = base.merge(grouped, on="trade_date", how="left").fillna(0)
    result["trades"] = result["trades"].astype(int)
    result["cumulative_net_pnl_rs"] = result["net_pnl_rs"].cumsum()
    return result


def write_config(path: Path, config: PullbackFilter, gate: Mapping[str, Any]) -> None:
    content = f'''"""Retrospective V13 LONG pullback research filter; not production."""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
FRESH_FORWARD_HOLDOUT_REQUIRED = True
SETUP_NAME = {SETUP!r}
CONFIG_ID = {config.config_id!r}
CONFIG_SHA256 = {config_hash(config)!r}
FILTER = {json_safe(asdict(config))!r}
PREFILTER_JOB_CHANGED = False
PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_UNIVERSE_RANKS = (200, 300)
SIGNAL_TIMEFRAME = "completed_5min"
ENTRY_TIMEFRAME = "V12_exact_next_available_1min"
EXIT_TIMEFRAME = "strict_1min_or_conservative_nonsynthetic_5min"
STOP_LOSS_PCT = 1.0
TARGET_PCT = 2.0
ONE_TICKER_PER_DAY = True
DAILY_CAP = {config.daily_cap}
STATUTORY_COSTS = True
V12_RISK_SIZING = True
MISSING_FEATURE_POLICY = "FAIL_CLOSED"
MARKET_JOIN_POLICY = "EXACT_COMPLETED_5MIN_TIMESTAMP_NO_FUTURE_FALLBACK"
RETROSPECTIVE_HISTORICAL_GATE_PASSED = {bool(gate['passed'])!r}
PRODUCTION_PROMOTION_ALLOWED = False
SELECTION_DATA_THROUGH = {END_DATE!r}
RESEARCH_FREEZE_DATE = {RESEARCH_FREEZE_DATE!r}
FRESH_FORWARD_START_POLICY = {FRESH_FORWARD_START_POLICY!r}
'''
    path.write_text(content, encoding="utf-8")


def write_diagnostic_config(
    path: Path,
    config: PullbackFilter,
    full_metrics: Mapping[str, Any],
) -> None:
    content = f'''"""Outcome-informed diagnostic only; never a promotion candidate."""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
DIAGNOSTIC_ONLY = True
OUTCOME_INFORMED_POSTHOC = True
FRESH_FORWARD_HOLDOUT_REQUIRED = True
SETUP_NAME = {SETUP!r}
CONFIG_ID = {config.config_id!r}
CONFIG_SHA256 = {config_hash(config)!r}
FILTER = {json_safe(asdict(config))!r}
PREFILTER_JOB_CHANGED = False
PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_UNIVERSE_RANKS = (200, 300)
SIGNAL_TIMEFRAME = "completed_5min"
ENTRY_TIMEFRAME = "V12_exact_next_available_1min"
EXIT_TIMEFRAME = "strict_1min_or_conservative_nonsynthetic_5min"
STOP_LOSS_PCT = 1.0
TARGET_PCT = 2.0
ONE_TICKER_PER_DAY = True
DAILY_CAP = {config.daily_cap}
STATUTORY_COSTS = True
V12_RISK_SIZING = True
MISSING_FEATURE_POLICY = "FAIL_CLOSED"
MARKET_JOIN_POLICY = "EXACT_COMPLETED_5MIN_TIMESTAMP_NO_FUTURE_FALLBACK"
HISTORICAL_FULL_PERIOD_METRICS = {json_safe(dict(full_metrics))!r}
PRODUCTION_PROMOTION_ALLOWED = False
SELECTION_DATA_THROUGH = {END_DATE!r}
RESEARCH_FREEZE_DATE = {RESEARCH_FREEZE_DATE!r}
FRESH_FORWARD_START_POLICY = {FRESH_FORWARD_START_POLICY!r}
'''
    path.write_text(content, encoding="utf-8")


def emit_diagnostic_bundle(
    out: Path,
    prefix: str,
    config: PullbackFilter,
    arrays: SearchArrays,
    sessions: Sequence[str],
) -> dict[str, Any]:
    indices = arrays.selected_indices(config)
    trades = arrays.trades(indices)
    full = arrays.metrics(indices, sessions)
    first98 = arrays.metrics(indices, sessions[:98])
    last22 = arrays.metrics(indices, sessions[98:])
    periods = pd.DataFrame([
        {"period": "FIRST_98", **first98},
        {"period": "LAST_22_CONTAMINATED", **last22},
        {"period": "FULL_120_POSTHOC", **full},
    ])
    blocks = pd.DataFrame([
        {
            "block": index + 1,
            "start": sessions[index * 20],
            "end": sessions[index * 20 + 19],
            **arrays.metrics(indices, sessions[index * 20:(index + 1) * 20]),
        }
        for index in range(6)
    ])
    months = []
    for month in sorted({day[:7] for day in sessions}):
        month_days = [day for day in sessions if day.startswith(month)]
        months.append({"month": month, **arrays.metrics(indices, month_days)})
    monthly = pd.DataFrame(months)
    cost_stress = pd.DataFrame([
        arrays.metrics(indices, sessions, cost_multiplier=multiplier)
        for multiplier in (1.0, 1.25, 1.5, 2.0)
    ])
    trades.to_csv(out / f"{prefix}_trades.csv", index=False)
    daily_results(trades, sessions).to_csv(out / f"{prefix}_daily.csv", index=False)
    monthly.to_csv(out / f"{prefix}_monthly.csv", index=False)
    periods.to_csv(out / f"{prefix}_periods.csv", index=False)
    blocks.to_csv(out / f"{prefix}_20session_blocks.csv", index=False)
    cost_stress.to_csv(out / f"{prefix}_cost_stress.csv", index=False)
    write_diagnostic_config(out / f"{prefix}_conf.py", config, full)
    result = {
        "config": json_safe(asdict(config)),
        "config_sha256": config_hash(config),
        "full_120_posthoc": full,
        "first_98": first98,
        "last_22_contaminated": last22,
        "twenty_session_blocks": json_safe(blocks.to_dict("records")),
        "cost_stress": json_safe(cost_stress.to_dict("records")),
        "diagnostic_only": True,
        "production_promotion_allowed": False,
    }
    (out / f"{prefix}_result.json").write_text(
        json.dumps(json_safe(result), indent=2), encoding="utf-8"
    )
    return result


def write_result_report(path: Path, summary: Mapping[str, Any]) -> None:
    walk = summary["walkforward_oos"]
    gate = summary["historical_gate"]
    precision = summary["posthoc_diagnostics"]["precision"]
    balanced = summary["posthoc_diagnostics"]["balanced"]
    known = summary["posthoc_diagnostics"]["known_cap15"]
    precision_full = precision["full_120_posthoc"]
    balanced_full = balanced["full_120_posthoc"]
    known_full = known["full_120_posthoc"]
    report = f"""# Six-month LONG strategy research result

## Honest walk-forward verdict

- Verdict: **{summary['verdict']}**
- 80 outer-test sessions: {walk['trades']} trades, {walk['trades_per_session']:.2f}/session, net Rs {walk['net_pnl_rs']:,.2f}, PF {walk['profit_factor']:.3f}, max drawdown Rs {walk['max_drawdown_rs']:,.2f}.
- Historical PF>1.5 hard gate passed: **{gate['checks']['pf_strictly_above_1p5']}**; complete stability/frequency/cost gate passed: **{gate['passed']}**.
- This is retrospective development because all 120 sessions had already been viewed; it is not a fresh holdout.

## Outcome-informed diagnostics (not promotion candidates)

- Known CAP15 L009216+MOMENTUM+rank220-259+ADX25-55+true-NIFTY nonnegative: {known_full['trades']} trades, net Rs {known_full['net_pnl_rs']:,.2f}, PF {known_full['profit_factor']:.3f}.
- Precision (+return acceleration, rank230-259): {precision_full['trades']} trades, net Rs {precision_full['net_pnl_rs']:,.2f}, PF {precision_full['profit_factor']:.3f}; first-98 PF {precision['first_98']['profit_factor']:.3f}, contaminated last-22 PF {precision['last_22_contaminated']['profit_factor']:.3f}.
- Balanced (+return acceleration, rank220-259): {balanced_full['trades']} trades, net Rs {balanced_full['net_pnl_rs']:,.2f}, PF {balanced_full['profit_factor']:.3f}; first-98 PF {balanced['first_98']['profit_factor']:.3f}.
- These rules were found after inspecting outcomes. Their weak 20-session blocks and sub-1.5 PF under 1.5x-cost stress prevent honest promotion.

## ML branch

The bounded expanding-window ML branch was rejected. Logistic PF was 0.629, tree PF 0.790, and histogram-gradient-boosting PF 0.746; mean AUCs were approximately 0.51, 0.51, and 0.48. No ML configuration was emitted.

## Execution and capital contract

- Hourly prefilter unchanged: LONG only, broad ranks 200-300; setup filters may tighten ranks/cap.
- Signal: completed 5-minute bar. Entry: V12 exact next available 1-minute fill. Exit: strict complete 1-minute path or conservative nonsynthetic 5-minute fallback.
- SL 1%, target 2%, stop-first same-bar collision, statutory NSE intraday costs.
- V12 risk sizing: Rs 200,000 equity, 0.25% risk/trade, Rs 50,000-Rs 150,000 notional bounds, 5x intraday leverage. Actual quantity/notional varies by price and risk sizing.

## Decision

No configuration is production-approved. The only defensible next test is to freeze one diagnostic without further edits and evaluate the first untouched exchange session after the {RESEARCH_FREEZE_DATE} research freeze as the start of a genuinely fresh forward holdout.
"""
    path.write_text(report, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=CACHE_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args(argv)
    assert_causal_contract()
    registry = candidate_registry()
    sessions = session_calendar()
    strict_v11.assert_frozen_runtime_contract()
    candidates, exact_raw, provenance = validate_external_cache(args.cache_dir)
    exact = attach_exact_completed_market_features(exact_raw, sessions)
    result = run_walkforward(exact, sessions, registry)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    registry_frame = pd.DataFrame([
        {**json_safe(asdict(config)), "config_sha256": config_hash(config)}
        for config in registry
    ])
    registry_frame.to_csv(out / "candidate_registry.csv", index=False)
    result["selection_ledger"].to_csv(out / "prior_only_selection_ledger.csv", index=False)
    result["folds"].to_csv(out / "outer_fold_results.csv", index=False)
    result["oos_trades"].to_csv(out / "walkforward_oos_trades.csv", index=False)
    daily_results(result["oos_trades"], sessions[40:]).to_csv(
        out / "walkforward_oos_daily.csv", index=False
    )
    result["final_ledger"].to_csv(out / "final_refit_selection_ledger.csv", index=False)
    final_trades = result["arrays"].trades(result["final_indices"])
    final_trades.to_csv(out / "final_config_full_in_sample_trades.csv", index=False)
    result["diagnostics"].to_csv(out / "diagnostic_only_results.csv", index=False)
    write_config(
        out / "final_refit_research_only_not_promoted_conf.py",
        result["final_config"], result["gate"],
    )

    known_config = next(
        config for config in registry if config.config_id == PRIMARY_POSTHOC_DIAGNOSTIC_ID
    )
    precision_config, balanced_config = posthoc_frontier_configs()
    known_bundle = emit_diagnostic_bundle(
        out, "best_posthoc_diagnostic", known_config, result["arrays"], sessions
    )
    precision_bundle = emit_diagnostic_bundle(
        out, "posthoc_precision", precision_config, result["arrays"], sessions
    )
    balanced_bundle = emit_diagnostic_bundle(
        out, "posthoc_balanced", balanced_config, result["arrays"], sessions
    )
    summary = {
        "research_only": True, "production_approved": False,
        "fresh_forward_holdout_required": True,
        "verdict": (
            "HISTORICAL_GATE_PASS_REQUIRES_FRESH_FORWARD_HOLDOUT"
            if result["gate"]["passed"] else "NO_HONEST_PF1P5_PROMOTION_CANDIDATE"
        ),
        "window": [START_DATE, END_DATE, 120],
        "protocol": {
            "warmup_sessions": 40, "outer_blocks": 4, "outer_block_sessions": 20,
            "fixed_registry_size": 64, "registry_sha256": registry_hash(registry),
            "filters_selected_only_from_prior_blocks": True,
            "all_six_months_previously_viewed": True,
            "retrospective_not_fresh_holdout": True,
            "research_freeze_date": RESEARCH_FREEZE_DATE,
            "fresh_forward_start_policy": FRESH_FORWARD_START_POLICY,
        },
        "broad_guards": BROAD_GUARDS,
        "cache_provenance": provenance,
        "walkforward_oos": result["aggregate"],
        "walkforward_oos_cost1p5": result["aggregate_stress"],
        "historical_gate": result["gate"],
        "outer_folds": json_safe(result["folds"].to_dict("records")),
        "final_refit": {
            "config": json_safe(asdict(result["final_config"])),
            "config_sha256": config_hash(result["final_config"]),
            "selection": json_safe({
                key: value for key, value in result["final_training"].items()
                if key != "prior_block_metrics"
            }),
            "full_period_backcast_is_in_sample": True,
            "full_period_backcast": result["final_backcast"],
        },
        "posthoc_diagnostics": {
            "known_cap15": known_bundle,
            "precision": precision_bundle,
            "balanced": balanced_bundle,
            "all_outcome_informed_and_nonpromotable": True,
        },
        "ml_branch_rejected": {
            "emitted_ml_configuration": False,
            "reason": "bounded expanding-window ML had no predictive or economic validation",
            "models": {
                "logistic": {"trades": 466, "net_pnl_rs": -56582, "profit_factor": 0.629, "mean_auc": 0.512},
                "tree": {"trades": 504, "net_pnl_rs": -33384, "profit_factor": 0.790, "mean_auc": 0.507},
                "hist_gradient_boosting": {"trades": 242, "net_pnl_rs": -19573, "profit_factor": 0.746, "mean_auc": 0.478},
            },
            "all_failed_1p5x_cost_stress": True,
        },
        "no_production_mutation": True,
    }
    (out / "summary.json").write_text(
        json.dumps(json_safe(summary), indent=2), encoding="utf-8"
    )
    write_result_report(out / "SIX_MONTH_STRATEGY_RESULT.md", summary)
    diagnostic_artifacts = [
        f"{prefix}_{suffix}"
        for prefix in (
            "best_posthoc_diagnostic", "posthoc_precision", "posthoc_balanced"
        )
        for suffix in (
            "conf.py", "trades.csv", "daily.csv", "monthly.csv", "periods.csv",
            "20session_blocks.csv", "cost_stress.csv", "result.json",
        )
    ]
    artifacts = [
        "candidate_registry.csv", "prior_only_selection_ledger.csv",
        "outer_fold_results.csv", "walkforward_oos_trades.csv",
        "walkforward_oos_daily.csv", "final_refit_selection_ledger.csv",
        "final_config_full_in_sample_trades.csv", "diagnostic_only_results.csv",
        "final_refit_research_only_not_promoted_conf.py", "summary.json",
        "SIX_MONTH_STRATEGY_RESULT.md", *diagnostic_artifacts,
    ]
    manifest = {
        "production_approved": False,
        "artifacts": {
            name: {"sha256": sha256(out / name), "bytes": (out / name).stat().st_size}
            for name in artifacts
        },
        "sources": provenance["source_sha256"],
    }
    (out / "integrity_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(json.dumps(json_safe({
        "output_dir": str(out), "verdict": summary["verdict"],
        "walkforward_oos": result["aggregate"],
        "gate": result["gate"], "final_config": asdict(result["final_config"]),
        "known_posthoc_diagnostic": known_bundle["full_120_posthoc"],
        "posthoc_precision": precision_bundle["full_120_posthoc"],
        "posthoc_balanced": balanced_bundle["full_120_posthoc"],
    }), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
