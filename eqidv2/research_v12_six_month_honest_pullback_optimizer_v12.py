"""Retrospective chronological optimizer for the V12 hourly LONG pullback setup.

This research-only driver keeps the hourly prefilter unchanged.  A stock is
eligible only while it is marked LONG in the causal hourly rank-200..300 list;
signals use completed five-minute bars and the existing V12 entry engine.  The
entry rule is developed on the first 58 sessions, a maximum of three frozen
shortlist configurations are checked on the next 20 sessions, and exactly one
configuration is frozen before the following 20-session retrospective audit.
The final 22 sessions are a contaminated consistency benchmark because they
were used to discover the earlier L009216 rule.

All six months have already been viewed in aggregate, so no result produced by
this file is a fresh holdout.  The fixed execution contract is 1% stop / 2%
target, exact V12 next-available one-minute entry, stop-first collision policy,
statutory costs, V12 risk sizing, one ticker/day, and cap 15/day.  Alternative
exits are deliberately not optimized together with the entry rule.

Nothing here imports, changes, enables, or restarts production configuration.
Every generated configuration remains PRODUCTION_APPROVED=False.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

import research_v12_one_month_long_logic_optimizer_v9 as v9
import research_v12_path_aware_long_rebuild as v2
import research_v12_prefilter_train_test_optimizer as optimizer
import research_v12_six_month_frozen_balanced_long_replay_v11 as strict_v11


SETUP = "SIX_MONTH_HONEST_PULLBACK_LONG_V12"
PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
HISTORICAL_CANDIDATE = False

START_DATE = "2026-02-05"
END_DATE = "2026-08-04"
EXPECTED_SESSIONS = 120
DISCOVERY_SESSIONS = 58
VALIDATION_SESSIONS = 20
AUDIT_SESSIONS = 20
CONTAMINATED_SESSIONS = 22

STOP_LOSS_PCT = 1.0
TARGET_PCT = 2.0
DAILY_CAP = 15
SEARCH_SEED = 20260806
MAX_VALIDATION_SHORTLIST = 3
DEFAULT_WORKERS = 4
SCHEMA_VERSION = 1

OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_six_month_honest_pullback_optimizer_v12_20260205_20260804"
)
PREFILTER_K300 = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_six_month_replay_20260205_20260804_k300"
    r"\hourly_candidates_20260205_20260804_k300.csv"
)

# Outcome-blind structural pool.  Every searched rule may only tighten these
# values.  It remains the same pullback-bounce setup rather than an unrestricted
# search across unrelated signal families.
BROAD_GUARDS: Mapping[str, float | int] = {
    "rank_min": 200,
    "rank_max": 300,
    "minute_min": 585,
    "minute_max": 810,
    "atr_min": 0.35,
    "session_return_min": 0.50,
    "vwap_min": 0.25,
    "close_position_min": 0.50,
    "range_min": 0.15,
    "ret5_min": 0.00,
    "ret5_max": 0.65,
    "ema20_min": 0.25,
    "score_margin_min": -0.05,
    "previous_ret5_max": 0.10,
}

RANK_BANDS = (
    (200, 240), (200, 260), (200, 280), (200, 300),
    (220, 240), (220, 260), (220, 280), (220, 300),
    (240, 260), (240, 280), (240, 300), (260, 300),
)
TIME_WINDOWS = (
    (585, 720), (585, 750), (585, 780), (585, 810),
    (600, 720), (600, 750), (600, 780), (600, 810),
    (615, 750), (615, 780), (615, 810),
    (630, 750), (630, 780), (630, 810),
    (645, 780), (645, 810),
)


@dataclass(frozen=True)
class PullbackConfig:
    core: str
    rank_min: int
    rank_max: int
    minute_min: int
    minute_max: int
    atr_min: float
    session_return_min: float
    vwap_min: float
    close_position_min: float
    range_min: float
    ret5_min: float
    ret5_max: float
    ema20_min: float
    score_margin_min: float
    previous_ret5_max: float
    atr_max: float | None = None
    session_return_max: float | None = None
    vwap_max: float | None = None
    ema20_max: float | None = None
    ret15_max: float | None = None
    volume_ratio_min: float | None = None
    volume_ratio_max: float | None = None
    rsi_min: float | None = None
    rsi_max: float | None = None
    adx_min: float | None = None
    adx_max: float | None = None
    reclaim_ratio_min: float | None = None
    market_return_min: float | None = None
    market_return_max: float | None = None
    allowed_market_regimes: tuple[str, ...] | None = None
    prefilter_long_share_min: float | None = None
    prefilter_long_share_max: float | None = None


CORE_SEEDS: Mapping[str, PullbackConfig] = {
    "FROZEN": PullbackConfig(
        "FROZEN", 200, 300, 600, 780, 0.35, 1.00, 0.90, 0.65,
        0.25, 0.15, 0.35, 1.00, 0.05, -0.10,
    ),
    "MEDIUM": PullbackConfig(
        "MEDIUM", 200, 300, 600, 795, 0.35, 0.75, 0.50, 0.55,
        0.20, 0.05, 0.50, 0.50, 0.00, 0.00,
    ),
    "BROAD": PullbackConfig(
        "BROAD", 200, 300, 585, 810, 0.35, 0.50, 0.25, 0.50,
        0.15, 0.00, 0.65, 0.25, -0.05, 0.10,
    ),
}

# Filters that were not terms in L009216.  Final configurations are capped at
# two additions; changes to existing thresholds do not count as new filters.
NEW_FILTER_FIELDS = frozenset({
    "atr_max", "session_return_max", "vwap_max", "ema20_max", "ret15_max",
    "volume_ratio_min", "volume_ratio_max", "rsi_min", "rsi_max",
    "adx_min", "adx_max", "reclaim_ratio_min", "market_return_min",
    "market_return_max", "allowed_market_regimes", "prefilter_long_share_min",
    "prefilter_long_share_max",
})


def json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv_allow_empty(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def config_hash(config: PullbackConfig) -> str:
    payload = json.dumps(json_safe(asdict(config)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def config_id(config: PullbackConfig) -> str:
    return f"{config.core}_{config_hash(config)[:12]}"


def candidate_fingerprint(frame: pd.DataFrame) -> str:
    columns = [
        "_optimizer_row_id", "ticker", "trade_date", "signal_time_ist",
        "membership_slot_ist", "selection_rank", "primary_side",
    ]
    work = frame[columns].copy().sort_values("_optimizer_row_id", kind="mergesort")
    for column in ("signal_time_ist", "membership_slot_ist"):
        work[column] = pd.to_datetime(work[column], errors="raise", utc=True).astype("int64")
    payload = work.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def new_filter_names(config: PullbackConfig) -> tuple[str, ...]:
    values = asdict(config)
    return tuple(sorted(name for name in NEW_FILTER_FIELDS if values[name] is not None))


def new_filter_count(config: PullbackConfig) -> int:
    # A bounded range is one conceptual filter, not two degrees of freedom.
    groups = (
        ("atr_max",), ("session_return_max",), ("vwap_max",), ("ema20_max",),
        ("ret15_max",), ("volume_ratio_min", "volume_ratio_max"),
        ("rsi_min", "rsi_max"), ("adx_min", "adx_max"),
        ("reclaim_ratio_min",), ("market_return_min", "market_return_max"),
        ("allowed_market_regimes",),
        ("prefilter_long_share_min", "prefilter_long_share_max"),
    )
    values = asdict(config)
    return sum(any(values[name] is not None for name in group) for group in groups)


def session_calendar() -> dict[str, list[str]]:
    source = pd.read_csv(v2.SESSION_SOURCE)
    values = source["trade_date"].astype(str)
    days = sorted(values[values.between(START_DATE, END_DATE)].unique())
    if len(days) != EXPECTED_SESSIONS or days[0] != START_DATE or days[-1] != END_DATE:
        raise RuntimeError(f"unexpected six-month calendar: {len(days)}, {days[:1]}..{days[-1:]}")
    discovery = days[:DISCOVERY_SESSIONS]
    validation = days[DISCOVERY_SESSIONS:DISCOVERY_SESSIONS + VALIDATION_SESSIONS]
    audit_start = DISCOVERY_SESSIONS + VALIDATION_SESSIONS
    audit = days[audit_start:audit_start + AUDIT_SESSIONS]
    contaminated = days[audit_start + AUDIT_SESSIONS:]
    if len(contaminated) != CONTAMINATED_SESSIONS:
        raise RuntimeError("unexpected contaminated benchmark session count")
    return {
        "all": days,
        "discovery": discovery,
        "validation": validation,
        "audit": audit,
        "contaminated": contaminated,
        "development_block_1": discovery[:19],
        "development_block_2": discovery[19:38],
        "development_block_3": discovery[38:],
    }


def _num(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def attach_causal_context(frame: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    work = frame.copy()
    previous = _num(work, "previous_ret_5m_pct")
    current = _num(work, "ret_5m_pct")
    work["reclaim_ratio"] = np.where(previous.lt(0), current / previous.abs(), np.nan)

    if not PREFILTER_K300.exists():
        raise FileNotFoundError(PREFILTER_K300)
    membership = pd.read_csv(
        PREFILTER_K300,
        usecols=["slot_ist", "primary_side", "selection_rank"],
    )
    membership["slot_key"] = pd.to_datetime(
        membership["slot_ist"], errors="raise", utc=True
    ).astype("int64")
    membership["is_long"] = membership["primary_side"].astype(str).str.upper().eq("LONG")
    breadth = membership.groupby("slot_key", sort=False).agg(
        prefilter_total_count=("is_long", "size"),
        prefilter_long_share=("is_long", "mean"),
    )
    work["slot_key"] = pd.to_datetime(
        work["membership_slot_ist"], errors="raise", utc=True
    ).astype("int64")
    work = work.merge(breadth, left_on="slot_key", right_index=True, how="left", validate="many_to_one")
    work = work.drop(columns=["slot_key"])

    context = v9.v12._tier123_market_context(
        Path(v9.v12.V7_HIST_INDICATORS_5M_DIR), set(sessions)
    )
    market_returns: list[float] = []
    market_regimes: list[str] = []
    for row in work[["trade_date", "signal_time_ist"]].itertuples(index=False):
        timestamp = v9.v12._normalise_ts(row.signal_time_ist)
        value, regime = v9.v12._tier123_bar_context(context, str(row.trade_date), timestamp)
        market_returns.append(float(value))
        market_regimes.append(str(regime))
    work["market_return_pct"] = market_returns
    work["market_regime"] = market_regimes
    if work[["prefilter_long_share", "market_return_pct"]].isna().any().any():
        raise RuntimeError("causal market/breadth context coverage is incomplete")
    if work["market_regime"].eq("UNKNOWN").any():
        raise RuntimeError("causal market regime coverage is incomplete")
    return work


def load_broad_candidates(sessions: Sequence[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    source = pd.read_parquet(v2.SOURCE, columns=list(v9.SOURCE_COLUMNS))
    source = source.loc[source["trade_date"].astype(str).between(START_DATE, END_DATE)].copy()
    if len(source) != 440_837:
        raise RuntimeError(f"unexpected source row count: {len(source)}")
    source = v9.add_causal_features(source)
    n = lambda name: _num(source, name)
    mask = source["pre_entry_data_invalid"].eq(False)
    mask &= source["primary_side"].astype(str).str.upper().eq("LONG")
    mask &= n("selection_rank").between(BROAD_GUARDS["rank_min"], BROAD_GUARDS["rank_max"])
    mask &= n("signal_minute").between(BROAD_GUARDS["minute_min"], BROAD_GUARDS["minute_max"])
    mask &= n("atr_pct").ge(BROAD_GUARDS["atr_min"])
    mask &= n("session_return_so_far_pct").ge(BROAD_GUARDS["session_return_min"])
    mask &= n("vwap_dist_atr").ge(BROAD_GUARDS["vwap_min"])
    mask &= n("close_position_in_bar").ge(BROAD_GUARDS["close_position_min"])
    mask &= n("range_pct").ge(BROAD_GUARDS["range_min"])
    mask &= n("ret_5m_pct").between(BROAD_GUARDS["ret5_min"], BROAD_GUARDS["ret5_max"])
    mask &= n("ema20_dist_atr").ge(BROAD_GUARDS["ema20_min"])
    mask &= n("score_margin").ge(BROAD_GUARDS["score_margin_min"])
    mask &= n("previous_ret_5m_pct").le(BROAD_GUARDS["previous_ret5_max"])
    mask &= source["contiguous_previous"].fillna(False)
    candidates = source.loc[mask.fillna(False)].copy()
    candidates = attach_causal_context(candidates, sessions)
    candidates["setup"] = SETUP
    candidates["side"] = "LONG"
    candidates["bar_time_ist"] = candidates["signal_time_ist"]
    candidates["decision_ready_at_ist"] = candidates["signal_time_ist"]
    candidates["decision_ready_source"] = "completed_5min_signal_bar"
    candidates["quality_score"] = 301.0 - _num(candidates, "selection_rank")
    candidates["score"] = candidates["quality_score"]
    candidates["research_source_entry_time_ist"] = candidates["entry_execution_time_ist"]
    candidates["research_source_entry_price"] = candidates["entry_price"]
    candidates = candidates.sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"], kind="mergesort"
    ).reset_index(drop=True)
    candidates["_optimizer_row_id"] = np.arange(len(candidates), dtype=int)
    funnel = pd.DataFrame([
        {"stage": "six_month_source", "rows": len(source)},
        {"stage": "causal_long_rank200_300_pullback_broad_guards", "rows": len(candidates)},
        {"stage": "broad_unique_ticker_days", "rows": candidates[["trade_date", "ticker"]].drop_duplicates().shape[0]},
        {"stage": "broad_active_sessions", "rows": candidates["trade_date"].nunique()},
        {"stage": "broad_unique_tickers", "rows": candidates["ticker"].nunique()},
    ])
    return candidates, funnel


@contextmanager
def _v12_build_context() -> Iterator[Any]:
    names = ("START_DATE", "END_DATE", "SETUP", "STOP_LOSS_PCT", "TARGET_PCT")
    old_v9 = {name: getattr(v9, name) for name in names}
    old_state = {
        "load_1m": getattr(v9.v12, "_load_1m_with_open", None),
        "entry_bars": getattr(v9.v12, "_entry_bars_for_signal", None),
        "day_loader": getattr(v9.v12, "_optimizer_load_1m_day", None),
        "exact_parity": getattr(v9.v12, "_V11_EXACT_LIVE_PARITY", None),
        "cost_model": getattr(v9.v12, "_V11_COST_MODEL", None),
        "slippage_bps": getattr(v9.v12, "_V11_SLIPPAGE_BPS", None),
        "exit_rule": v9.v12.v6.SETUP_EXIT_RULES.get(SETUP),
    }
    v9.START_DATE = START_DATE
    v9.END_DATE = END_DATE
    v9.SETUP = SETUP
    v9.STOP_LOSS_PCT = STOP_LOSS_PCT
    v9.TARGET_PCT = TARGET_PCT
    loader = optimizer.install_windowed_1m_loader(v9.v12, start_date=START_DATE, end_date=END_DATE)
    optimizer.install_day_1m_adapter(v9.v12, loader)
    v9.v12._V11_EXACT_LIVE_PARITY = False
    v9.v12._V11_COST_MODEL = "statutory"
    v9.v12._V11_SLIPPAGE_BPS = 0.0
    v9.v12.v6.SETUP_EXIT_RULES[SETUP] = (STOP_LOSS_PCT, TARGET_PCT)
    try:
        yield loader
    finally:
        for name, value in old_v9.items():
            setattr(v9, name, value)
        if old_state["load_1m"] is not None:
            v9.v12._load_1m_with_open = old_state["load_1m"]
        if old_state["entry_bars"] is not None:
            v9.v12._entry_bars_for_signal = old_state["entry_bars"]
        if old_state["day_loader"] is None:
            if hasattr(v9.v12, "_optimizer_load_1m_day"):
                delattr(v9.v12, "_optimizer_load_1m_day")
        else:
            v9.v12._optimizer_load_1m_day = old_state["day_loader"]
        for key, attribute in (
            ("exact_parity", "_V11_EXACT_LIVE_PARITY"),
            ("cost_model", "_V11_COST_MODEL"),
            ("slippage_bps", "_V11_SLIPPAGE_BPS"),
        ):
            if old_state[key] is not None:
                setattr(v9.v12, attribute, old_state[key])
        if old_state["exit_rule"] is None:
            v9.v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v9.v12.v6.SETUP_EXIT_RULES[SETUP] = old_state["exit_rule"]


def _resolve_outcome_shard(payload: tuple[int, pd.DataFrame]) -> tuple[int, pd.DataFrame]:
    shard_id, raw = payload
    loader = optimizer.install_windowed_1m_loader(v9.v12, start_date=START_DATE, end_date=END_DATE)
    optimizer.prewarm_windowed_1m_loader(loader, raw["ticker"], workers=1)
    optimizer.install_day_1m_adapter(v9.v12, loader)
    v9.v12._V11_EXACT_LIVE_PARITY = False
    v9.v12._V11_COST_MODEL = "statutory"
    v9.v12._V11_SLIPPAGE_BPS = 0.0
    v9.v12.v6.SETUP_EXIT_RULES[SETUP] = (STOP_LOSS_PCT, TARGET_PCT)
    outcomes = optimizer.resolve_exit_grid(
        raw,
        {SETUP: [(STOP_LOSS_PCT, TARGET_PCT)]},
        v9.v12,
        progress_label=f"honest-v12-shard-{shard_id}",
    )
    return shard_id, outcomes


def _ticker_shards(raw: pd.DataFrame, workers: int) -> list[pd.DataFrame]:
    counts = raw.groupby("ticker", sort=False).size().sort_values(ascending=False)
    bins: list[list[str]] = [[] for _ in range(max(1, workers))]
    loads = [0 for _ in bins]
    for ticker, count in counts.items():
        destination = int(np.argmin(loads))
        bins[destination].append(str(ticker))
        loads[destination] += int(count)
    return [raw.loc[raw["ticker"].astype(str).isin(names)].copy() for names in bins if names]


def build_exact_universe(
    candidates: pd.DataFrame,
    *,
    workers: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    with _v12_build_context() as loader:
        prewarm = optimizer.prewarm_windowed_1m_loader(loader, candidates["ticker"], workers=8)
        raw, entry_rejects = v9.v12._v7_entry_engine_raw_rows(candidates)
        if raw.empty:
            raise RuntimeError("entry engine produced no executable broad states")
        if len(raw) != len(candidates) or not entry_rejects.empty:
            raise RuntimeError(
                "broad state entry coverage is incomplete; implement causal reject placeholders "
                f"before search ({len(raw)}/{len(candidates)}, rejects={len(entry_rejects)})"
            )
        raw["_optimizer_row_id"] = _num(raw, "_optimizer_row_id").astype(int)
        if raw["_optimizer_row_id"].duplicated().any():
            raise RuntimeError("entry-engine row ids are not unique")
        # Windows workers do not share this LRU.  Release the parent-wide
        # prewarm before child shards load their own ticker subsets; the parent
        # loader is repopulated only after workers exit for fallback auditing.
        if hasattr(loader, "cache_clear"):
            loader.cache_clear()
        shards = _ticker_shards(raw, min(max(1, workers), raw["ticker"].nunique()))
        print(
            f"[exact] resolving {len(raw):,} states across {len(shards)} ticker-balanced shards",
            flush=True,
        )
        outcome_parts: list[pd.DataFrame] = []
        if len(shards) == 1:
            outcome_parts.append(optimizer.resolve_exit_grid(
                shards[0],
                {SETUP: [(STOP_LOSS_PCT, TARGET_PCT)]},
                v9.v12,
                progress_label="honest-v12-shard-1",
            ))
        else:
            with ProcessPoolExecutor(max_workers=len(shards)) as executor:
                futures = {
                    executor.submit(_resolve_outcome_shard, (number, shard)): number
                    for number, shard in enumerate(shards, 1)
                }
                for future in as_completed(futures):
                    number, outcomes = future.result()
                    outcome_parts.append(outcomes)
                    print(
                        f"[exact] shard {number}/{len(shards)} complete: {len(outcomes):,} outcomes",
                        flush=True,
                    )
        outcomes = pd.concat(outcome_parts, ignore_index=True)
        if len(outcomes) != len(raw) or outcomes["_optimizer_row_id"].duplicated().any():
            raise RuntimeError(f"exact outcome coverage failure: {len(outcomes)}/{len(raw)}")
        fields = [
            "_optimizer_row_id", "ticker", "side", "setup", "entry_time_ist",
            "entry_price", "quantity",
            "sl_pct", "tgt_pct", "outcome", "exit_time_ist", "exit_price",
            "bars_held", "gross_pnl_rs", "cost_rs", "net_pnl_rs", "cost_rates_as_of",
        ]
        exact = raw.merge(
            outcomes[fields], on="_optimizer_row_id", how="inner", validate="one_to_one",
            suffixes=("", "_outcome"),
        )
        for column in ("ticker", "side", "setup"):
            outcome_column = f"{column}_outcome"
            if outcome_column not in exact:
                raise RuntimeError(f"missing exact parity column: {outcome_column}")
            left = exact[column].astype(str).str.upper().str.strip()
            right = exact[outcome_column].astype(str).str.upper().str.strip()
            if not left.eq(right).all():
                raise RuntimeError(f"raw/outcome {column} parity failed")
        raw_entry_time = pd.to_datetime(
            exact["v7_signal_entry_time_ist"], errors="raise", utc=True
        )
        outcome_entry_time = pd.to_datetime(
            exact["entry_time_ist"], errors="raise", utc=True
        )
        if not raw_entry_time.eq(outcome_entry_time).all():
            raise RuntimeError("raw/outcome entry timestamp parity failed")
        signal_entry = _num(exact, "v7_signal_entry_price")
        expected_fill = (signal_entry * (1.0 + float(v9.v12.V7_PAPER_SLIPPAGE_PCT))).round(2)
        if not np.allclose(
            expected_fill.to_numpy(dtype=float),
            _num(exact, "entry_price_outcome").to_numpy(dtype=float),
            atol=1e-9,
            rtol=0.0,
        ):
            raise RuntimeError("adverse entry-slippage fill invariant failed")
        expected_quantity = np.asarray([
            v9.v12._risk_based_qty(
                float(price),
                optimizer._signal_stop_price(float(price), "LONG", STOP_LOSS_PCT),
            )
            for price in signal_entry
        ], dtype=int)
        if not np.array_equal(
            expected_quantity,
            _num(exact, "quantity_outcome").to_numpy(dtype=int),
        ):
            raise RuntimeError("V12 risk-quantity invariant failed")
        exact["entry_engine_candidate_entry_price"] = _num(exact, "entry_price")
        exact["entry_engine_default_quantity"] = _num(exact, "quantity")
        exact["entry_price"] = _num(exact, "entry_price_outcome")
        exact["quantity"] = _num(exact, "quantity_outcome").astype(int)
        exact["trade_date"] = (
            pd.to_datetime(exact["entry_time_ist"], utc=True)
            .dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d")
        )
        for column in ("signal_time_ist", "entry_time_ist", "exit_time_ist"):
            exact[column] = pd.to_datetime(exact[column], errors="raise", utc=True).dt.tz_convert("Asia/Kolkata")
        exact = exact.sort_values(
            ["trade_date", "signal_time_ist", "selection_rank", "ticker"], kind="mergesort"
        ).reset_index(drop=True)
        exact = strict_v11.apply_storage_tolerant_coverage_fallback(exact, loader)
    filtered, strict_rejects = strict_v11.strict_path_filter(exact)
    valid_ids = set(_num(filtered, "_optimizer_row_id").astype(int).tolist())
    exact["strict_path_valid"] = _num(exact, "_optimizer_row_id").astype(int).isin(valid_ids)
    exact["strict_path_policy"] = np.where(
        exact["strict_path_valid"],
        np.where(
            exact["path_fallback_applied"].astype(bool),
            "COMPLETE_NONSYNTHETIC_5MIN_SIGNAL_PLUS_1_ONLY",
            "COMPLETE_VALID_1MIN_GRID",
        ),
        "REJECTED_NO_CAUSAL_RESELECTION",
    )
    expected_gross = (
        _num(exact, "exit_price") - _num(exact, "entry_price")
    ) * _num(exact, "quantity")
    difference = expected_gross - _num(exact, "gross_pnl_rs")
    if float(difference.abs().max()) >= strict_v11.P_AND_L_STORAGE_TOLERANCE_RS:
        raise RuntimeError("strict exact gross-P&L identity failed")
    return exact, raw, entry_rejects, strict_rejects, prewarm


class SearchArrays:
    def __init__(self, frame: pd.DataFrame, sessions: Sequence[str]):
        self.frame = frame.reset_index(drop=True)
        self.sessions = list(sessions)
        lookup = {day: index for index, day in enumerate(self.sessions)}
        self.day_code = self.frame["trade_date"].astype(str).map(lookup).to_numpy(dtype=int)
        ticker_day = self.frame["trade_date"].astype(str) + "|" + self.frame["ticker"].astype(str)
        self.ticker_day_code = pd.factorize(ticker_day, sort=False)[0]
        self.ticker_code = pd.factorize(self.frame["ticker"].astype(str), sort=False)[0]
        self.pnl = _num(self.frame, "net_pnl_rs").to_numpy(dtype=float)
        self.gross = _num(self.frame, "gross_pnl_rs").to_numpy(dtype=float)
        self.cost = _num(self.frame, "cost_rs").to_numpy(dtype=float)
        self.exit_price = _num(self.frame, "exit_price").to_numpy(dtype=float)
        self.quantity = _num(self.frame, "quantity").to_numpy(dtype=float)
        self.entry_time_ns = pd.to_datetime(
            self.frame["entry_time_ist"], errors="raise", utc=True
        ).astype("int64").to_numpy(dtype=np.int64)
        self.exit_time_ns = pd.to_datetime(
            self.frame["exit_time_ist"], errors="raise", utc=True
        ).astype("int64").to_numpy(dtype=np.int64)
        numeric_columns = (
            "selection_rank", "signal_minute", "atr_pct", "session_return_so_far_pct",
            "vwap_dist_atr", "close_position_in_bar", "range_pct", "ret_5m_pct",
            "ret_15m_pct", "ema20_dist_atr", "score_margin", "previous_ret_5m_pct",
            "volume_ratio20", "RSI", "ADX", "reclaim_ratio", "market_return_pct",
            "prefilter_long_share",
        )
        self.values = {
            name: _num(self.frame, name).to_numpy(dtype=float) for name in numeric_columns
        }
        self.contiguous = self.frame["contiguous_previous"].fillna(False).to_numpy(dtype=bool)
        self.strict_path_valid = self.frame["strict_path_valid"].fillna(False).to_numpy(dtype=bool)
        self.regime = self.frame["market_regime"].astype(str).to_numpy(dtype=object)

    def mask(self, config: PullbackConfig) -> np.ndarray:
        v = self.values
        mask = self.contiguous.copy()
        pairs = (
            ("selection_rank", config.rank_min, ">="),
            ("selection_rank", config.rank_max, "<="),
            ("signal_minute", config.minute_min, ">="),
            ("signal_minute", config.minute_max, "<="),
            ("atr_pct", config.atr_min, ">="),
            ("atr_pct", config.atr_max, "<="),
            ("session_return_so_far_pct", config.session_return_min, ">="),
            ("session_return_so_far_pct", config.session_return_max, "<="),
            ("vwap_dist_atr", config.vwap_min, ">="),
            ("vwap_dist_atr", config.vwap_max, "<="),
            ("close_position_in_bar", config.close_position_min, ">="),
            ("range_pct", config.range_min, ">="),
            ("ret_5m_pct", config.ret5_min, ">="),
            ("ret_5m_pct", config.ret5_max, "<="),
            ("ema20_dist_atr", config.ema20_min, ">="),
            ("ema20_dist_atr", config.ema20_max, "<="),
            ("score_margin", config.score_margin_min, ">="),
            ("previous_ret_5m_pct", config.previous_ret5_max, "<="),
            ("ret_15m_pct", config.ret15_max, "<="),
            ("volume_ratio20", config.volume_ratio_min, ">="),
            ("volume_ratio20", config.volume_ratio_max, "<="),
            ("RSI", config.rsi_min, ">="),
            ("RSI", config.rsi_max, "<="),
            ("ADX", config.adx_min, ">="),
            ("ADX", config.adx_max, "<="),
            ("reclaim_ratio", config.reclaim_ratio_min, ">="),
            ("market_return_pct", config.market_return_min, ">="),
            ("market_return_pct", config.market_return_max, "<="),
            ("prefilter_long_share", config.prefilter_long_share_min, ">="),
            ("prefilter_long_share", config.prefilter_long_share_max, "<="),
        )
        for name, threshold, operator in pairs:
            if threshold is None:
                continue
            values = v[name]
            mask &= values >= float(threshold) if operator == ">=" else values <= float(threshold)
        if config.allowed_market_regimes is not None:
            mask &= np.isin(self.regime, np.asarray(config.allowed_market_regimes, dtype=object))
        return mask

    def causal_capped_indices(self, config: PullbackConfig) -> np.ndarray:
        eligible = np.flatnonzero(self.mask(config))
        if not len(eligible):
            return np.asarray([], dtype=int)
        _, first = np.unique(self.ticker_day_code[eligible], return_index=True)
        selected = eligible[np.sort(first)]
        kept: list[np.ndarray] = []
        selected_days = self.day_code[selected]
        for day in np.unique(selected_days):
            kept.append(selected[selected_days == day][:DAILY_CAP])
        return np.concatenate(kept) if kept else np.asarray([], dtype=int)

    def selected_indices(self, config: PullbackConfig) -> np.ndarray:
        capped = self.causal_capped_indices(config)
        # Path validity is an audit fact known only after the trade date.  An
        # invalid earliest state therefore consumes its causal ticker/day and
        # cap position; it must never cause a later state to be reselected.
        return capped[self.strict_path_valid[capped]]

    def invalid_selected_count(
        self, config: PullbackConfig, session_positions: Sequence[int]
    ) -> int:
        capped = self.causal_capped_indices(config)
        positions = np.asarray(list(session_positions), dtype=int)
        scoped = capped[np.isin(self.day_code[capped], positions)]
        return int((~self.strict_path_valid[scoped]).sum())


def performance(
    arrays: SearchArrays,
    indices: np.ndarray,
    session_positions: Sequence[int],
    *,
    cost_multiplier: float = 1.0,
    adverse_exit_slippage_bps: float = 0.0,
) -> dict[str, Any]:
    positions = np.asarray(list(session_positions), dtype=int)
    chosen = indices[np.isin(arrays.day_code[indices], positions)] if len(indices) else indices
    adverse_exit_slippage = (
        arrays.exit_price[chosen]
        * arrays.quantity[chosen]
        * float(adverse_exit_slippage_bps)
        / 10_000.0
    )
    pnl = arrays.gross[chosen] - adverse_exit_slippage - float(cost_multiplier) * arrays.cost[chosen]
    gains = float(pnl[pnl > 0].sum())
    losses = float(-pnl[pnl < 0].sum())
    pf = gains / losses if losses > 0 else (float("inf") if gains > 0 else 0.0)
    median_abs = float(np.median(np.abs(pnl))) if len(pnl) else 0.0
    prior = 2.5 * median_abs
    shrunk_pf = (gains + prior) / (losses + prior) if losses + prior > 0 else 0.0
    day_lookup = {day: offset for offset, day in enumerate(positions)}
    daily_trades = np.zeros(len(positions), dtype=int)
    daily_pnl = np.zeros(len(positions), dtype=float)
    for index, value in zip(chosen, pnl):
        offset = day_lookup[arrays.day_code[index]]
        daily_trades[offset] += 1
        daily_pnl[offset] += float(value)
    if len(chosen):
        realized = (
            pd.Series(pnl, index=arrays.exit_time_ns[chosen])
            .groupby(level=0).sum().sort_index().to_numpy(dtype=float)
        )
    else:
        realized = np.asarray([], dtype=float)
    equity = np.cumsum(realized)
    peaks = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:] if len(equity) else np.array([])
    drawdown = equity - peaks if len(equity) else np.array([])
    positive = daily_pnl[daily_pnl > 0]
    ticker_share = (
        float(pd.Series(arrays.ticker_code[chosen]).value_counts(normalize=True).max())
        if len(chosen) else 0.0
    )
    return {
        "sessions": len(positions),
        "trades": len(chosen),
        "trades_per_session": len(chosen) / len(positions) if len(positions) else 0.0,
        "median_trades_per_session": float(np.median(daily_trades)) if len(positions) else 0.0,
        "active_days": int(np.count_nonzero(daily_trades)),
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": float(pf),
        "shrunk_profit_factor": float(shrunk_pf),
        "win_rate_pct": float(np.count_nonzero(pnl > 0) / len(pnl) * 100) if len(pnl) else 0.0,
        "max_drawdown_rs": float(drawdown.min()) if len(drawdown) else 0.0,
        "max_drawdown_basis": "realized_exit_timestamp_aggregated_not_mark_to_market",
        "positive_days": int(np.count_nonzero(daily_pnl > 0)),
        "best_positive_day_share": float(positive.max() / positive.sum()) if len(positive) else 0.0,
        "maximum_day_trade_share": float(daily_trades.max() / len(chosen)) if len(chosen) else 0.0,
        "largest_ticker_trade_share": ticker_share,
        "mean_net_pnl_trade_rs": float(pnl.mean()) if len(pnl) else 0.0,
    }


def _config_signature(arrays: SearchArrays, config: PullbackConfig, positions: Sequence[int]) -> str:
    selected = arrays.selected_indices(config)
    scoped = selected[np.isin(arrays.day_code[selected], np.asarray(list(positions), dtype=int))]
    return hashlib.sha256(np.asarray(scoped, dtype=np.int64).tobytes()).hexdigest()


def _dedupe_configs(configs: Iterable[PullbackConfig]) -> list[PullbackConfig]:
    result: list[PullbackConfig] = []
    seen: set[str] = set()
    for config in configs:
        if new_filter_count(config) > 2:
            continue
        key = config_hash(config)
        if key not in seen:
            result.append(config)
            seen.add(key)
    return result


def stage_one_configs() -> list[PullbackConfig]:
    return _dedupe_configs(
        replace(seed, rank_min=rank_min, rank_max=rank_max, minute_min=start, minute_max=end)
        for seed in CORE_SEEDS.values()
        for rank_min, rank_max in RANK_BANDS
        for start, end in TIME_WINDOWS
    )


def _expand(parents: Sequence[PullbackConfig], changes: Sequence[Mapping[str, Any]]) -> list[PullbackConfig]:
    values: list[PullbackConfig] = list(parents)
    for parent in parents:
        for change in changes:
            candidate = replace(parent, **change)
            if candidate.atr_max is not None and candidate.atr_max <= candidate.atr_min:
                continue
            if candidate.session_return_max is not None and candidate.session_return_max <= candidate.session_return_min:
                continue
            if candidate.vwap_max is not None and candidate.vwap_max <= candidate.vwap_min:
                continue
            if candidate.ema20_max is not None and candidate.ema20_max <= candidate.ema20_min:
                continue
            if candidate.ret5_max <= candidate.ret5_min:
                continue
            values.append(candidate)
    return _dedupe_configs(values)


ANTI_CHASE_CHANGES = tuple(
    [{"atr_max": value} for value in (0.60, 0.80, 1.00, 1.25, 1.50, 2.00)]
    + [{"session_return_max": value} for value in (1.50, 2.00, 2.50, 3.00, 4.00, 5.00)]
    + [{"vwap_max": value} for value in (1.25, 1.50, 2.00, 2.50, 3.00, 4.00)]
    + [{"ema20_max": value} for value in (1.50, 2.00, 2.50, 3.00, 4.00)]
)
GEOMETRY_CHANGES = tuple(
    [{"previous_ret5_max": value} for value in (-0.30, -0.20, -0.10, 0.00, 0.10)]
    + [{"ret5_min": low, "ret5_max": high} for low, high in (
        (0.00, 0.25), (0.00, 0.35), (0.00, 0.50),
        (0.05, 0.35), (0.05, 0.50), (0.10, 0.35),
        (0.10, 0.50), (0.15, 0.35), (0.20, 0.50),
    )]
    + [{"session_return_min": value} for value in (0.50, 0.75, 1.00, 1.50, 2.00)]
    + [{"vwap_min": value} for value in (0.25, 0.50, 0.90, 1.25)]
    + [{"close_position_min": value} for value in (0.50, 0.55, 0.65, 0.75)]
    + [{"range_min": value} for value in (0.15, 0.20, 0.25, 0.35, 0.50)]
    + [{"ema20_min": value} for value in (0.25, 0.50, 1.00, 1.50)]
    + [{"score_margin_min": value} for value in (-0.05, 0.00, 0.05, 0.10, 0.15)]
)
QUALITY_CHANGES = tuple(
    [{"ret15_max": value} for value in (-0.20, 0.00, 0.20, 0.40)]
    + [{"volume_ratio_max": value} for value in (0.50, 0.75, 1.00, 1.50)]
    + [{"volume_ratio_min": value} for value in (0.50, 0.75, 1.00, 1.50)]
    + [{"rsi_min": low, "rsi_max": high} for low, high in (
        (50.0, 70.0), (55.0, 70.0), (55.0, 75.0),
        (60.0, 75.0), (60.0, 80.0),
    )]
    + [{"adx_min": value} for value in (20.0, 25.0, 30.0, 35.0)]
    + [{"adx_max": value} for value in (40.0, 45.0, 50.0)]
    + [{"reclaim_ratio_min": value} for value in (0.50, 0.75, 1.00, 1.50)]
)
REGIME_CHANGES = tuple(
    [{"market_return_min": value} for value in (-0.50, -0.20, 0.00, 0.20, 0.50)]
    + [{"market_return_max": value} for value in (0.50, 0.75, 1.00, 1.50)]
    + [
        {"allowed_market_regimes": ("BULL", "TREND", "NEUTRAL")},
        {"allowed_market_regimes": ("BULL", "TREND")},
        {"allowed_market_regimes": ("BULL",)},
        {"allowed_market_regimes": ("NEUTRAL", "TREND")},
    ]
    + [{"prefilter_long_share_min": value} for value in (0.48, 0.50, 0.52, 0.54)]
    + [{"prefilter_long_share_max": value} for value in (0.48, 0.50, 0.52, 0.54)]
)


def _discovery_record(
    arrays: SearchArrays,
    config: PullbackConfig,
    session_sets: Mapping[str, Sequence[str]],
    lookup: Mapping[str, int],
    stage: str,
) -> dict[str, Any]:
    selected = arrays.selected_indices(config)
    record: dict[str, Any] = {
        "config_id": config_id(config),
        "config_sha256": config_hash(config),
        "stage": stage,
        "new_filter_count": new_filter_count(config),
        "new_filter_names": "|".join(new_filter_names(config)),
        **asdict(config),
    }
    blocks: list[dict[str, Any]] = []
    for number in (1, 2, 3):
        positions = [lookup[day] for day in session_sets[f"development_block_{number}"]]
        metric = performance(arrays, selected, positions)
        blocks.append(metric)
        record.update({f"block{number}_{key}": value for key, value in metric.items()})
    discovery_positions = [lookup[day] for day in session_sets["discovery"]]
    overall = performance(arrays, selected, discovery_positions)
    record.update({f"discovery_{key}": value for key, value in overall.items()})
    record["discovery_unknown_selected_paths"] = arrays.invalid_selected_count(
        config, discovery_positions
    )
    record["discovery_signature"] = _config_signature(arrays, config, discovery_positions)
    record["positive_development_blocks"] = sum(item["net_pnl_rs"] > 0 for item in blocks)
    record["worst_block_shrunk_pf"] = min(item["shrunk_profit_factor"] for item in blocks)
    record["worst_block_pf"] = min(item["profit_factor"] for item in blocks)
    record["minimum_block_trades"] = min(item["trades"] for item in blocks)
    record["frequency_gate"] = bool(
        record["discovery_unknown_selected_paths"] == 0
        and overall["trades"] >= 87
        and overall["trades_per_session"] >= 1.50
        and overall["median_trades_per_session"] >= 1.0
        and overall["active_days"] >= 41
        and record["minimum_block_trades"] >= 20
    )
    record["development_robust_gate"] = bool(
        record["frequency_gate"]
        and overall["net_pnl_rs"] > 0
        and overall["profit_factor"] >= 1.20
        and record["positive_development_blocks"] >= 2
        and record["worst_block_pf"] >= 0.80
    )
    return record


def _rank_records(records: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(
        records,
        key=lambda row: (
            bool(row["frequency_gate"]),
            int(row["positive_development_blocks"]),
            float(row["worst_block_shrunk_pf"]),
            float(row["discovery_shrunk_profit_factor"]),
            float(row["discovery_net_pnl_rs"]),
            -int(row["new_filter_count"]),
        ),
        reverse=True,
    )


def _top_parents(
    records: Sequence[Mapping[str, Any]],
    configs: Mapping[str, PullbackConfig],
    *,
    limit: int = 12,
) -> list[PullbackConfig]:
    result: list[PullbackConfig] = []
    signatures: set[str] = set()
    for row in _rank_records(records):
        if not row["frequency_gate"]:
            continue
        signature = str(row["discovery_signature"])
        if signature in signatures:
            continue
        result.append(configs[str(row["config_id"])])
        signatures.add(signature)
        if len(result) >= limit:
            break
    if not result:
        raise RuntimeError("no development configuration preserves minimum frequency")
    return result


def staged_development_search(
    exact: pd.DataFrame,
    session_sets: Mapping[str, Sequence[str]],
) -> tuple[pd.DataFrame, list[PullbackConfig], SearchArrays]:
    arrays = SearchArrays(exact, session_sets["all"])
    lookup = {day: index for index, day in enumerate(session_sets["all"])}
    all_records: list[dict[str, Any]] = []
    config_map: dict[str, PullbackConfig] = {}

    stages: list[tuple[str, list[PullbackConfig]]] = [("STAGE1_CORE_RANK_TIME", stage_one_configs())]
    parents: list[PullbackConfig] = []
    for stage_number in range(1, 7):
        if stage_number == 1:
            stage, configs = stages[0]
        elif stage_number == 2:
            stage, configs = "STAGE2_ANTI_CHASE", _expand(parents, ANTI_CHASE_CHANGES)
        elif stage_number == 3:
            stage, configs = "STAGE3_PULLBACK_GEOMETRY_A", _expand(parents, GEOMETRY_CHANGES)
        elif stage_number == 4:
            stage, configs = "STAGE4_PULLBACK_GEOMETRY_B", _expand(parents, GEOMETRY_CHANGES)
        elif stage_number == 5:
            stage, configs = "STAGE5_QUALITY", _expand(parents, QUALITY_CHANGES)
        else:
            stage, configs = "STAGE6_MARKET_REGIME", _expand(parents, REGIME_CHANGES)
        records: list[dict[str, Any]] = []
        for config in configs:
            identifier = config_id(config)
            config_map[identifier] = config
            records.append(_discovery_record(arrays, config, session_sets, lookup, stage))
        all_records.extend(records)
        parents = _top_parents(records, config_map)
        best = _rank_records(records)[0]
        print(
            f"[search] {stage}: tried={len(records):,}, "
            f"best worst-block-shrunk-PF={best['worst_block_shrunk_pf']:.3f}, "
            f"dev PF={best['discovery_profit_factor']:.3f}, trades={best['discovery_trades']}",
            flush=True,
        )

    ledger = pd.DataFrame(all_records).drop_duplicates("config_sha256", keep="first")
    robust = ledger.loc[ledger["development_robust_gate"].eq(True)].copy()
    pool = robust if not robust.empty else ledger.loc[ledger["frequency_gate"].eq(True)].copy()
    ranked = _rank_records(pool.to_dict("records"))
    finalists: list[PullbackConfig] = []
    signatures: set[str] = set()
    for row in ranked:
        signature = str(row["discovery_signature"])
        if signature in signatures:
            continue
        finalists.append(config_map[str(row["config_id"])])
        signatures.add(signature)
        if len(finalists) >= MAX_VALIDATION_SHORTLIST:
            break
    if not finalists:
        raise RuntimeError("no validation finalists")
    return ledger, finalists, arrays


def validate_and_freeze(
    arrays: SearchArrays,
    finalists: Sequence[PullbackConfig],
    session_sets: Mapping[str, Sequence[str]],
) -> tuple[pd.DataFrame, PullbackConfig, dict[str, Any]]:
    lookup = {day: index for index, day in enumerate(session_sets["all"])}
    validation_positions = [lookup[day] for day in session_sets["validation"]]
    rows: list[dict[str, Any]] = []
    for config in finalists:
        metric = performance(arrays, arrays.selected_indices(config), validation_positions)
        unknown_paths = arrays.invalid_selected_count(config, validation_positions)
        gate = bool(
            unknown_paths == 0
            and metric["trades"] >= 30
            and metric["trades_per_session"] >= 1.50
            and metric["active_days"] >= 14
            and metric["net_pnl_rs"] > 0
            and metric["profit_factor"] >= 1.20
        )
        rows.append({
            "config_id": config_id(config),
            "config_sha256": config_hash(config),
            "validation_gate": gate,
            "unknown_selected_paths": unknown_paths,
            **metric,
        })
    table = pd.DataFrame(rows)
    eligible = table.loc[table["validation_gate"]].copy()
    choice_pool = eligible if not eligible.empty else table
    choice = choice_pool.sort_values(
        ["shrunk_profit_factor", "net_pnl_rs", "trades"], ascending=False, kind="mergesort"
    ).iloc[0]
    champion = next(config for config in finalists if config_id(config) == choice["config_id"])
    freeze = {
        "frozen_before_audit": True,
        "selection_used_through": session_sets["validation"][-1],
        "audit_start": session_sets["audit"][0],
        "config_id": config_id(champion),
        "config_sha256": config_hash(champion),
        "config": asdict(champion),
        "development_shortlist_size": len(finalists),
        "validation_gate_passed": bool(choice["validation_gate"]),
        "audit_metrics_not_referenced_by_selection_code": True,
    }
    return table, champion, freeze


def selected_trade_frame(arrays: SearchArrays, config: PullbackConfig) -> pd.DataFrame:
    indices = arrays.selected_indices(config)
    trades = arrays.frame.iloc[indices].copy()
    trades["daily_sequence"] = trades.groupby("trade_date", sort=False).cumcount() + 1
    if trades["daily_sequence"].gt(DAILY_CAP).any():
        raise RuntimeError("daily cap breach")
    if trades.duplicated(["trade_date", "ticker"]).any():
        raise RuntimeError("one-ticker/day breach")
    return trades.reset_index(drop=True)


def daily_results(trades: pd.DataFrame, sessions: Sequence[str]) -> pd.DataFrame:
    base = pd.DataFrame({"trade_date": list(sessions)})
    scoped = trades.loc[trades["trade_date"].isin(sessions)].copy()
    if scoped.empty:
        grouped = pd.DataFrame(columns=["trade_date", "trades", "winners", "gross_pnl_rs", "cost_rs", "net_pnl_rs"])
    else:
        scoped["winner"] = _num(scoped, "net_pnl_rs").gt(0)
        grouped = scoped.groupby("trade_date", as_index=False).agg(
            trades=("ticker", "size"), winners=("winner", "sum"),
            gross_pnl_rs=("gross_pnl_rs", "sum"), cost_rs=("cost_rs", "sum"),
            net_pnl_rs=("net_pnl_rs", "sum"),
        )
    result = base.merge(grouped, on="trade_date", how="left")
    result[["trades", "winners"]] = result[["trades", "winners"]].fillna(0).astype(int)
    for column in ("gross_pnl_rs", "cost_rs", "net_pnl_rs"):
        result[column] = _num(result, column).fillna(0.0)
    return result


def bootstrap_pf_lower_90(trades: pd.DataFrame, sessions: Sequence[str], samples: int = 5000) -> float:
    daily = []
    for day in sessions:
        pnl = _num(trades.loc[trades["trade_date"].eq(day)], "net_pnl_rs").to_numpy(dtype=float)
        daily.append((float(pnl[pnl > 0].sum()), float(-pnl[pnl < 0].sum())))
    values = np.asarray(daily, dtype=float)
    rng = np.random.default_rng(SEARCH_SEED)
    pfs: list[float] = []
    for _ in range(samples):
        sample = values[rng.integers(0, len(values), size=len(values))]
        gain = float(sample[:, 0].sum())
        loss = float(sample[:, 1].sum())
        pfs.append(gain / loss if loss > 0 else (10.0 if gain > 0 else 0.0))
    return float(np.quantile(np.asarray(pfs), 0.10))


def evaluate_champion(
    arrays: SearchArrays,
    champion: PullbackConfig,
    session_sets: Mapping[str, Sequence[str]],
    *,
    development_gate_passed: bool,
    validation_gate_passed: bool,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    lookup = {day: index for index, day in enumerate(session_sets["all"])}
    selected = arrays.selected_indices(champion)
    periods = {
        "discovery": session_sets["discovery"],
        "validation": session_sets["validation"],
        "retrospective_audit": session_sets["audit"],
        "validation_plus_audit": list(session_sets["validation"]) + list(session_sets["audit"]),
        "contaminated_jul_aug_benchmark": session_sets["contaminated"],
        "full_six_month_backcast": session_sets["all"],
    }
    results: dict[str, Any] = {}
    for label, days in periods.items():
        positions = [lookup[day] for day in days]
        results[label] = performance(arrays, selected, positions)
        results[label]["unknown_selected_paths"] = arrays.invalid_selected_count(
            champion, positions
        )
    combined_days = periods["validation_plus_audit"]
    combined_positions = [lookup[day] for day in combined_days]
    results["validation_plus_audit_cost_1p25x"] = performance(
        arrays, selected, combined_positions, cost_multiplier=1.25
    )
    trades = selected_trade_frame(arrays, champion)
    combined_trades = trades.loc[trades["trade_date"].isin(combined_days)]
    results["validation_plus_audit"]["day_bootstrap_pf_lower_90"] = bootstrap_pf_lower_90(
        combined_trades, combined_days
    )
    audit = results["retrospective_audit"]
    validation = results["validation"]
    combined = results["validation_plus_audit"]
    stress = results["validation_plus_audit_cost_1p25x"]
    full = results["full_six_month_backcast"]
    no_fallback = selected[
        ~arrays.frame.iloc[selected]["path_fallback_applied"].astype(bool).to_numpy()
    ]
    complete_slots = selected[
        _num(arrays.frame.iloc[selected], "prefilter_total_count").eq(300).to_numpy()
    ]
    results["validation_plus_audit_exclude_5m_fallback"] = performance(
        arrays, no_fallback, combined_positions
    )
    results["validation_plus_audit_complete_prefilter_slots_only"] = performance(
        arrays, complete_slots, combined_positions
    )
    fallback_sensitivity = results["validation_plus_audit_exclude_5m_fallback"]
    slot_sensitivity = results["validation_plus_audit_complete_prefilter_slots_only"]
    gates = {
        "development_robust_gate_passed": bool(development_gate_passed),
        "validation_shortlist_gate_passed": bool(validation_gate_passed),
        "validation_pf_at_least_1p20": validation["profit_factor"] >= 1.20,
        "validation_net_positive": validation["net_pnl_rs"] > 0,
        "audit_pf_at_least_1p50": audit["profit_factor"] >= 1.50,
        "audit_net_positive": audit["net_pnl_rs"] > 0,
        "combined_pf_at_least_1p50": combined["profit_factor"] >= 1.50,
        "combined_minimum_60_trades": combined["trades"] >= 60,
        "combined_tpd_at_least_1p50": combined["trades_per_session"] >= 1.50,
        "combined_median_at_least_1": combined["median_trades_per_session"] >= 1.0,
        "combined_active_days_at_least_70pct": combined["active_days"] >= 28,
        "combined_max_drawdown_within_15000": combined["max_drawdown_rs"] >= -15_000,
        "combined_net_to_drawdown_at_least_1": (
            combined["net_pnl_rs"] / abs(combined["max_drawdown_rs"])
            if combined["max_drawdown_rs"] < 0 else float("inf")
        ) >= 1.0,
        "cost_1p25x_pf_at_least_1p30": stress["profit_factor"] >= 1.30,
        "cost_1p25x_net_positive": stress["net_pnl_rs"] > 0,
        "bootstrap_pf_lower_90_above_1": combined["day_bootstrap_pf_lower_90"] > 1.0,
        "best_positive_day_share_at_most_25pct": combined["best_positive_day_share"] <= 0.25,
        "maximum_day_trade_share_at_most_10pct": combined["maximum_day_trade_share"] <= 0.10,
        "largest_ticker_trade_share_at_most_10pct": combined["largest_ticker_trade_share"] <= 0.10,
        "exclude_fallback_pf_at_least_1p25": fallback_sensitivity["profit_factor"] >= 1.25,
        "exclude_fallback_net_positive": fallback_sensitivity["net_pnl_rs"] > 0,
        "complete_slot_pf_at_least_1p25": slot_sensitivity["profit_factor"] >= 1.25,
        "complete_slot_net_positive": slot_sensitivity["net_pnl_rs"] > 0,
        "full_six_month_pf_at_least_1p50": full["profit_factor"] >= 1.50,
        "full_six_month_net_positive": full["net_pnl_rs"] > 0,
        "combined_unknown_selected_paths_zero": combined["unknown_selected_paths"] == 0,
        "full_six_month_unknown_selected_paths_zero": full["unknown_selected_paths"] == 0,
    }
    results["historical_candidate_gates"] = gates
    results["historical_candidate_passed"] = bool(all(gates.values()))
    daily = daily_results(trades, session_sets["all"])
    monthly = daily.assign(month=daily["trade_date"].str.slice(0, 7)).groupby("month", as_index=False).agg(
        sessions=("trade_date", "size"), trades=("trades", "sum"), winners=("winners", "sum"),
        gross_pnl_rs=("gross_pnl_rs", "sum"), cost_rs=("cost_rs", "sum"), net_pnl_rs=("net_pnl_rs", "sum"),
    )
    return results, trades, monthly


def write_config(path: Path, config: PullbackConfig, historical_passed: bool) -> None:
    payload = f'''"""Frozen retrospective V12 LONG pullback research configuration."""

SETUP_NAME = {SETUP!r}
CONFIG_ID = {config_id(config)!r}
CONFIG_SHA256 = {config_hash(config)!r}
PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
HISTORICAL_CANDIDATE = {bool(historical_passed)!r}

PREFILTER_JOB_CHANGED = False
REQUIRED_PRIMARY_SIDE = "LONG"
PREFILTER_RANK_INCLUSIVE = ({config.rank_min!r}, {config.rank_max!r})
SIGNAL_TIMEFRAME = "completed_5min"
ENTRY = "V12_exact_next_available_1min_open"
ONE_TICKER_PER_DAY = True
DAILY_CAP = {DAILY_CAP}
STOP_LOSS_PCT = {STOP_LOSS_PCT}
TARGET_PCT = {TARGET_PCT}
SAME_BAR_COLLISION_POLICY = "STOP_FIRST"
COST_MODEL = "NSE_STATUTORY_INTRADAY"
RISK_EQUITY_RS = {float(v9.v12.RISK_EQUITY_RS)!r}
RISK_PCT_PER_TRADE = {float(v9.v12.RISK_PCT_PER_TRADE)!r}
RISK_MIN_NOTIONAL_RS = {float(v9.v12.RISK_MIN_NOTIONAL_RS)!r}
RISK_MAX_NOTIONAL_RS = {float(v9.v12.RISK_MAX_NOTIONAL_RS)!r}
INTRADAY_LEVERAGE = {float(v9.v12.V7_INTRADAY_LEVERAGE)!r}
MISSING_FILTER_VALUE_POLICY = "FAIL_CLOSED"

FILTERS = {asdict(config)!r}

# This is not a production approval.  All six months were already inspected,
# and the Jul/Aug benchmark was used in earlier setup discovery.  A genuinely
# new forward holdout plus live feature-parity work is still mandatory.
'''
    path.write_text(payload, encoding="utf-8")


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    results = summary["champion_results"]
    lines = [
        "# Six-month V12 LONG pullback retrospective optimizer",
        "",
        f"Verdict: **{summary['verdict']}**",
        "",
        "This is a retrospective chronological backcast, not a fresh holdout. The hourly prefilter was not changed.",
        "",
        "| Period | Sessions | Trades | Trades/day | Net P&L | PF | Win rate | Max DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label in (
        "discovery", "validation", "retrospective_audit",
        "validation_plus_audit", "contaminated_jul_aug_benchmark",
        "full_six_month_backcast",
    ):
        metric = results[label]
        lines.append(
            f"| {label} | {metric['sessions']} | {metric['trades']} | "
            f"{metric['trades_per_session']:.2f} | Rs {metric['net_pnl_rs']:,.2f} | "
            f"{metric['profit_factor']:.3f} | {metric['win_rate_pct']:.1f}% | "
            f"Rs {metric['max_drawdown_rs']:,.2f} |"
        )
    lines.extend([
        "",
        "## Frozen configuration",
        "",
        "```json",
        json.dumps(json_safe(summary["champion_config"]), indent=2),
        "```",
        "",
        "## Historical candidate gates",
        "",
    ])
    for name, passed in results["historical_candidate_gates"].items():
        lines.append(f"- {'PASS' if passed else 'FAIL'}: {name}")
    lines.extend([
        "",
        "Even a historical pass remains PRODUCTION_APPROVED=False until a genuinely new forward holdout and live feature parity pass.",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def cache_contract(candidates: pd.DataFrame) -> dict[str, Any]:
    nifty = Path(v9.v12.V7_HIST_INDICATORS_5M_DIR) / "NIFTYBEES_stocks_indicators_5min.parquet"
    sources = [
        Path(__file__).resolve(), Path(v9.__file__).resolve(), Path(strict_v11.__file__).resolve(),
        Path(v9.v12.__file__).resolve(), Path(optimizer.__file__).resolve(), v2.SOURCE.resolve(),
        Path(v9.v12.er.__file__).resolve(), Path(v9.v12.v6.__file__).resolve(),
        Path(v9.nse.__file__).resolve(),
        Path(__file__).with_name("eqidv2_v7_position_sizing.py").resolve(),
        v2.SESSION_SOURCE.resolve(), PREFILTER_K300.resolve(), nifty.resolve(),
    ]
    tickers = sorted(candidates["ticker"].astype(str).str.upper().unique())
    stats: list[tuple[str, int, int]] = []
    for ticker in tickers:
        for path in (
            Path(v9.v12.v6.DATA_1M_DIR) / f"{ticker}_stocks_indicators_1min.parquet",
            Path(v9.v12.V7_HIST_INDICATORS_5M_DIR) / f"{ticker}_stocks_indicators_5min.parquet",
        ):
            if path.exists():
                stat = path.stat()
                stats.append((str(path), int(stat.st_size), int(stat.st_mtime_ns)))
            else:
                stats.append((str(path), -1, -1))
    contract = {
        "schema_version": SCHEMA_VERSION,
        "window": [START_DATE, END_DATE],
        "setup": SETUP,
        "broad_guards": dict(BROAD_GUARDS),
        "sl_pct": STOP_LOSS_PCT,
        "target_pct": TARGET_PCT,
        "candidate_rows": len(candidates),
        "candidate_tickers": len(tickers),
        "candidate_natural_key_sha256": candidate_fingerprint(candidates),
        "source_sha256": {str(path): sha256(path) for path in sources if path.exists()},
        "bar_stat_fingerprint": hashlib.sha256(
            json.dumps(stats, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "path_policy": "COMPLETE_1MIN_ELSE_COMPLETE_NONSYNTHETIC_5MIN_SIGNAL_PLUS_1_ONLY",
        "same_bar_collision_policy": "STOP_FIRST",
        "cost_model": "NSE_STATUTORY_INTRADAY",
        "execution_constants": {
            "risk_sizing_enabled": bool(v9.v12.RISK_SIZING_ENABLED),
            "risk_equity_rs": float(v9.v12.RISK_EQUITY_RS),
            "risk_pct_per_trade": float(v9.v12.RISK_PCT_PER_TRADE),
            "risk_min_notional_rs": float(v9.v12.RISK_MIN_NOTIONAL_RS),
            "risk_max_notional_rs": float(v9.v12.RISK_MAX_NOTIONAL_RS),
            "intraday_leverage": float(v9.v12.V7_INTRADAY_LEVERAGE),
            "paper_entry_slippage_pct": float(v9.v12.V7_PAPER_SLIPPAGE_PCT),
            "worker_count_default": DEFAULT_WORKERS,
            "worker_memory_policy": "PARENT_LRU_CLEARED_BEFORE_PROCESS_SHARDS",
        },
    }
    contract["contract_sha256"] = hashlib.sha256(
        json.dumps(json_safe(contract), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return contract


def write_cache_manifest(path: Path, candidates: pd.DataFrame, exact: pd.DataFrame) -> None:
    payload = {
        "contract": cache_contract(candidates),
        "candidate_sha256": sha256(path.parent / "broad_candidate_states.parquet"),
        "exact_sha256": sha256(path.parent / "exact_candidate_universe.parquet"),
        "exact_rows": len(exact),
        "strict_valid_rows": int(exact["strict_path_valid"].sum()),
        "strict_invalid_rows": int((~exact["strict_path_valid"]).sum()),
    }
    path.write_text(json.dumps(json_safe(payload), indent=2), encoding="utf-8")


def validate_cache_manifest(path: Path, candidates: pd.DataFrame, exact: pd.DataFrame) -> None:
    if not path.exists():
        raise RuntimeError("cache manifest missing")
    payload = json.loads(path.read_text(encoding="utf-8"))
    current = cache_contract(candidates)
    if payload["contract"]["contract_sha256"] != current["contract_sha256"]:
        raise RuntimeError("cache provenance changed")
    if payload["candidate_sha256"] != sha256(path.parent / "broad_candidate_states.parquet"):
        raise RuntimeError("candidate cache hash mismatch")
    if payload["exact_sha256"] != sha256(path.parent / "exact_candidate_universe.parquet"):
        raise RuntimeError("exact cache hash mismatch")
    if int(payload["exact_rows"]) != len(exact):
        raise RuntimeError("strict exact cache validation failed")
    if int(payload["strict_valid_rows"]) != int(exact["strict_path_valid"].sum()):
        raise RuntimeError("strict valid-row count changed")


def integrity_manifest(output_dir: Path) -> dict[str, Any]:
    files = sorted(path for path in output_dir.iterdir() if path.is_file() and path.name != "integrity_manifest.json")
    payload = {
        "artifact_count": len(files),
        "artifacts": [
            {"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)} for path in files
        ],
    }
    payload["all_verified"] = all(item["sha256"] == sha256(Path(item["path"])) for item in payload["artifacts"])
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reuse-exact", action="store_true")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args(argv)
    if args.workers < 1:
        raise ValueError("workers must be positive")

    runtime_contract = strict_v11.assert_frozen_runtime_contract()
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    sessions = session_calendar()
    candidates, funnel = load_broad_candidates(sessions["all"])
    candidate_path = output_dir / "broad_candidate_states.parquet"
    exact_path = output_dir / "exact_candidate_universe.parquet"
    cache_manifest_path = output_dir / "exact_cache_manifest.json"

    audit_outcomes_cached_before_freeze = bool(args.reuse_exact)
    if args.reuse_exact:
        saved_candidates = pd.read_parquet(candidate_path)
        exact = pd.read_parquet(exact_path)
        validate_cache_manifest(cache_manifest_path, saved_candidates, exact)
        if candidate_fingerprint(saved_candidates) != candidate_fingerprint(candidates):
            raise RuntimeError("current broad candidate content differs from cache")
        candidates = saved_candidates
        entry_reject_path = output_dir / "entry_engine_rejects.csv"
        strict_reject_path = output_dir / "strict_path_rejects.csv"
        entry_rejects = read_csv_allow_empty(entry_reject_path)
        strict_rejects = read_csv_allow_empty(strict_reject_path)
        if len(strict_rejects) != int((~exact["strict_path_valid"]).sum()):
            raise RuntimeError("saved strict-reject ledger does not match exact cache")
        prewarm: dict[str, Any] = {"status": "REUSED_STRICT_CACHE"}
        print(
            f"[cache] reused {len(exact):,} exact rows "
            f"({int(exact['strict_path_valid'].sum()):,} strict-valid)",
            flush=True,
        )
        selection_exact = exact.loc[
            exact["trade_date"].astype(str).isin(
                list(sessions["discovery"]) + list(sessions["validation"])
            )
        ].copy()
    else:
        candidates.to_parquet(candidate_path, index=False)
        selection_days = list(sessions["discovery"]) + list(sessions["validation"])
        later_days = list(sessions["audit"]) + list(sessions["contaminated"])
        selection_candidates = candidates.loc[
            candidates["trade_date"].astype(str).isin(selection_days)
        ].copy()
        later_candidates = candidates.loc[
            candidates["trade_date"].astype(str).isin(later_days)
        ].copy()
        selection_exact, selection_raw, selection_entry_rejects, selection_strict_rejects, selection_prewarm = build_exact_universe(
            selection_candidates, workers=args.workers
        )
        selection_exact.to_parquet(
            output_dir / "development_validation_exact_before_freeze.parquet", index=False
        )

    funnel.to_csv(output_dir / "candidate_funnel.csv", index=False)
    ledger, finalists, selection_arrays = staged_development_search(selection_exact, sessions)
    ledger.to_csv(output_dir / "development_trial_ledger.csv", index=False)
    shortlist, champion, freeze = validate_and_freeze(selection_arrays, finalists, sessions)
    freeze["audit_outcomes_cached_before_freeze"] = audit_outcomes_cached_before_freeze
    freeze["audit_outcomes_resolved_only_after_freeze"] = not audit_outcomes_cached_before_freeze
    shortlist.to_csv(output_dir / "validation_shortlist.csv", index=False)
    (output_dir / "champion_freeze_before_audit.json").write_text(
        json.dumps(json_safe(freeze), indent=2), encoding="utf-8"
    )

    if not args.reuse_exact:
        # The configuration hash is now physically on disk before any audit or
        # contaminated-period exit path is resolved.
        later_exact, later_raw, later_entry_rejects, later_strict_rejects, later_prewarm = build_exact_universe(
            later_candidates, workers=args.workers
        )
        exact = pd.concat([selection_exact, later_exact], ignore_index=True).sort_values(
            ["trade_date", "signal_time_ist", "selection_rank", "ticker"], kind="mergesort"
        ).reset_index(drop=True)
        raw = pd.concat([selection_raw, later_raw], ignore_index=True).sort_values(
            ["trade_date", "signal_time_ist", "selection_rank", "ticker"], kind="mergesort"
        ).reset_index(drop=True)
        entry_rejects = pd.concat(
            [selection_entry_rejects, later_entry_rejects], ignore_index=True
        )
        strict_rejects = pd.concat(
            [selection_strict_rejects, later_strict_rejects], ignore_index=True
        )
        prewarm = {
            "development_validation": selection_prewarm,
            "post_freeze_audit_and_contaminated": later_prewarm,
        }
        raw.to_parquet(output_dir / "entry_engine_raw.parquet", index=False)
        exact.to_parquet(exact_path, index=False)
        entry_rejects.to_csv(output_dir / "entry_engine_rejects.csv", index=False)
        strict_rejects.to_csv(output_dir / "strict_path_rejects.csv", index=False)
        write_cache_manifest(cache_manifest_path, candidates, exact)
        print(
            f"[cache] saved {len(exact):,} exact rows; strict rejects={len(strict_rejects):,}",
            flush=True,
        )

    arrays = SearchArrays(exact, sessions["all"])

    champion_ledger_row = ledger.loc[
        ledger["config_sha256"].astype(str).eq(config_hash(champion))
    ]
    if champion_ledger_row.empty:
        raise RuntimeError("frozen champion missing from development ledger")
    development_gate_passed = bool(
        champion_ledger_row.iloc[0]["development_robust_gate"]
    )
    champion_results, trades, monthly = evaluate_champion(
        arrays,
        champion,
        sessions,
        development_gate_passed=development_gate_passed,
        validation_gate_passed=bool(freeze["validation_gate_passed"]),
    )
    historical_passed = bool(champion_results["historical_candidate_passed"])
    trades.to_csv(output_dir / "champion_six_month_trades.csv", index=False)
    daily_results(trades, sessions["all"]).to_csv(output_dir / "champion_six_month_daily.csv", index=False)
    monthly.to_csv(output_dir / "champion_six_month_monthly.csv", index=False)
    write_config(output_dir / "honest_pullback_long_setup_conf.py", champion, historical_passed)

    summary = {
        "setup": SETUP,
        "research_only": True,
        "production_approved": False,
        "historical_candidate": historical_passed,
        "verdict": (
            "HISTORICAL_CANDIDATE_REQUIRES_FRESH_FORWARD_HOLDOUT"
            if historical_passed else "REJECTED_NO_HONEST_PF1P5_HISTORICAL_CANDIDATE"
        ),
        "honesty": {
            "fresh_holdout": False,
            "reason": "all 120 sessions viewed in aggregate; final 22 used in earlier setup discovery",
            "development_window": [sessions["discovery"][0], sessions["discovery"][-1], len(sessions["discovery"])],
            "validation_window": [sessions["validation"][0], sessions["validation"][-1], len(sessions["validation"])],
            "retrospective_audit_window": [sessions["audit"][0], sessions["audit"][-1], len(sessions["audit"])],
            "contaminated_benchmark_window": [sessions["contaminated"][0], sessions["contaminated"][-1], len(sessions["contaminated"])],
            "audit_outcomes_cached_before_freeze": audit_outcomes_cached_before_freeze,
            "audit_outcomes_resolved_only_after_freeze": not audit_outcomes_cached_before_freeze,
            "audit_metrics_not_referenced_by_selection_code": True,
        },
        "execution_contract": {
            "prefilter_job_changed": False,
            "required_primary_side": "LONG",
            "rank_source": "causal_hourly_prefilter_membership",
            "signal_timeframe": "completed_5min",
            "entry": "V12 exact next-available 1min",
            "sl_pct": STOP_LOSS_PCT,
            "target_pct": TARGET_PCT,
            "daily_cap": DAILY_CAP,
            "one_ticker_per_day": True,
            "same_bar_collision_policy": "STOP_FIRST",
            "cost_model": "NSE statutory intraday",
            "risk_equity_rs": float(v9.v12.RISK_EQUITY_RS),
            "risk_pct_per_trade": float(v9.v12.RISK_PCT_PER_TRADE),
            "risk_min_notional_rs": float(v9.v12.RISK_MIN_NOTIONAL_RS),
            "risk_max_notional_rs": float(v9.v12.RISK_MAX_NOTIONAL_RS),
            "intraday_leverage": float(v9.v12.V7_INTRADAY_LEVERAGE),
            "runtime_contract_audit": runtime_contract,
        },
        "search_contract": {
            "broad_guards": dict(BROAD_GUARDS),
            "fixed_exit_during_search": True,
            "maximum_new_filter_groups": 2,
            "complete_trial_ledger_rows": len(ledger),
            "validation_shortlist_maximum": MAX_VALIDATION_SHORTLIST,
            "validation_shortlist_actual": len(finalists),
            "future_day_fields_allowed": False,
            "market_filter_is_point_in_time": True,
        },
        "cache": {
            "broad_candidate_rows": len(candidates),
            "exact_rows": len(exact),
            "strict_valid_rows": int(exact["strict_path_valid"].sum()),
            "strict_invalid_rows": int((~exact["strict_path_valid"]).sum()),
            "entry_rejects": len(entry_rejects),
            "strict_path_rejects": len(strict_rejects),
            "prewarm": prewarm,
        },
        "champion_config_id": config_id(champion),
        "champion_config_sha256": config_hash(champion),
        "champion_config": asdict(champion),
        "freeze": freeze,
        "validation_shortlist": shortlist.to_dict("records"),
        "champion_results": champion_results,
        "limitations": [
            "no genuinely fresh holdout exists inside the six-month window",
            "static current source universe may contain survivorship bias",
            "historical quoted spreads are unavailable; statutory costs and configured entry slippage are used",
            "market/breadth and stateful pullback features require verified live feature parity before any promotion",
            "no portfolio-overlap capital constraint is applied",
        ],
    }
    (output_dir / "summary.json").write_text(
        json.dumps(json_safe(summary), indent=2), encoding="utf-8"
    )
    write_report(output_dir / "RESEARCH_REPORT.md", summary)
    manifest = integrity_manifest(output_dir)
    (output_dir / "integrity_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    if not manifest["all_verified"]:
        raise RuntimeError("integrity manifest verification failed")
    print(json.dumps(json_safe({
        "output_dir": str(output_dir),
        "verdict": summary["verdict"],
        "champion": summary["champion_config_id"],
        "results": champion_results,
    }), indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
