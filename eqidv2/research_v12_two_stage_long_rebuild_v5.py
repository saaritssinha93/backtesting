"""Nested, causal two-stage V12 long-entry research replay.

The experiment is deliberately bounded to eight configurations selected only
from 2026-02-05 through 2026-06-03.  The already-opened 2026-06-04 through
2026-08-03 period is a diagnostic replay and is never used to choose features,
models, exits, thresholds, or sizing.

Stage A asks whether a ticker/day has a worthwhile opportunity.  Its first
causal rolling-threshold crossing opens a ticket.  Stage B estimates ENTER NOW
versus DEFER; DEFER waits one contiguous five-minute state when one remains and
otherwise rejects, with at most two waits.  Exact
next-one-minute V12 entries, one-minute exits, statutory costs, one ticker/day,
and a chronological 15-trade daily cap are retained.

Production configuration is never imported or modified by this module.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import avwap_5min_ID_v12_backtesting as v12
import nse_intraday_costs as nse
import research_v12_ml_long_entry_backtest as replay_helpers
import research_v12_path_aware_long_rebuild as v2
import research_v12_prefilter_train_test_optimizer as optimizer


SETUP = "TWO_STAGE_PREFILTER_LONG_V5"
PRODUCTION_APPROVED = False
OUTPUT_DIR = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_two_stage_long_v5_nested_20260205_20260803"
)

START_DATE = "2026-02-05"
DEVELOPMENT_END = "2026-06-03"
REPLAY_START = "2026-06-04"
REPLAY_END = "2026-08-03"
REPLAY_MIDPOINT = "2026-07-06"

LEVEL12 = (
    "selection_rank",
    "overall_score",
    "long_score",
    "activity_score",
    "signal_minute",
    "ret_5m_pct",
    "session_return_so_far_pct",
    "range_pct",
    "body_pct",
    "upper_wick_pct",
    "distance_from_session_high_pct",
    "log1p_traded_value_rs",
)
SEQ8 = (
    "contiguous_prev_flag",
    "contiguous_prev2_flag",
    "bars_in_active_spell",
    "return_from_prev_active_close_pct",
    "return_over_2_active_bars_pct",
    "delta_distance_from_session_high_pct",
    "delta_upper_wick_pct",
    "positive_body_count_last3",
)
STAGE_B_STATE_FEATURES = ("ticket_step",)

EXIT_PAIRS = ((1.0, 2.0), (1.5, 3.0))
FEATURE_FAMILIES = {
    "LEVEL12": LEVEL12,
    "LEVEL12_SEQ8": LEVEL12 + SEQ8,
}
ROLLING_FRACTIONS = (0.25, 0.40)
ROLLING_SCORE_SESSIONS = 20
STAGE_B_ENTER_PROBABILITY = 0.40
DAILY_CAP = 15
RISK_BUDGET_RS = 500.0
EXPECTED_V12_RISK_CONTRACT = {
    "RISK_SIZING_ENABLED": True,
    "RISK_EQUITY_RS": 200_000.0,
    "RISK_PCT_PER_TRADE": 0.25,
    "RISK_MIN_NOTIONAL_RS": 50_000.0,
    "RISK_MAX_NOTIONAL_RS": 150_000.0,
}

OUTER_FOLDS = (
    ("O1", 48, 48, 58),
    ("O2", 58, 58, 68),
    ("O3", 68, 68, 78),
)

SOURCE_COLUMNS = tuple(dict.fromkeys((
    "ticker", "trade_date", "membership_slot_ist", "primary_side",
    "selection_rank", "overall_score", "long_score", "activity_score",
    "signal_time_ist", "signal_open", "signal_high", "signal_low",
    "signal_close", "signal_minute", "ret_5m_pct",
    "session_return_so_far_pct", "range_pct", "body_pct",
    "upper_wick_pct", "distance_from_session_high_pct",
    "traded_value_rs", "pre_entry_data_invalid", "atr_pct",
    "vwap_dist_atr", "entry_execution_time_ist", "entry_price",
)))


@dataclass(frozen=True)
class Config:
    config_id: str
    feature_family: str
    sl_pct: float
    tgt_pct: float
    rolling_fraction: float

    @property
    def stage_a_features(self) -> tuple[str, ...]:
        return FEATURE_FAMILIES[self.feature_family]

    @property
    def stage_b_features(self) -> tuple[str, ...]:
        return self.stage_a_features + STAGE_B_STATE_FEATURES


class InsufficientEvidenceError(RuntimeError):
    """Expected fold invalidation caused only by label/support scarcity."""


def configurations() -> list[Config]:
    rows: list[Config] = []
    for family in FEATURE_FAMILIES:
        for sl_pct, tgt_pct in EXIT_PAIRS:
            for fraction in ROLLING_FRACTIONS:
                rows.append(Config(
                    config_id=(
                        f"{family}_SL{sl_pct:.1f}_T{tgt_pct:.1f}_F{fraction:.2f}"
                        .replace(".", "p")
                    ),
                    feature_family=family,
                    sl_pct=float(sl_pct),
                    tgt_pct=float(tgt_pct),
                    rolling_fraction=float(fraction),
                ))
    if len(rows) != 8:
        raise RuntimeError(f"configuration contract changed: {len(rows)} != 8")
    return rows


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


def session_split() -> tuple[list[str], list[str], list[str]]:
    calendar = sorted(
        pd.read_csv(v2.SESSION_SOURCE)["trade_date"].astype(str).unique()
    )
    development = [d for d in calendar if START_DATE <= d <= DEVELOPMENT_END]
    replay = [d for d in calendar if REPLAY_START <= d <= REPLAY_END]
    if len(development) != 78 or len(replay) != 41:
        raise RuntimeError(
            f"unexpected session split: development={len(development)}, "
            f"replay={len(replay)}"
        )
    return calendar, development, replay


def v12_risk_contract() -> dict[str, Any]:
    actual = {key: getattr(v12, key) for key in EXPECTED_V12_RISK_CONTRACT}
    for key, expected in EXPECTED_V12_RISK_CONTRACT.items():
        value = actual[key]
        matches = (
            bool(value) is expected
            if isinstance(expected, bool)
            else math.isclose(float(value), float(expected), abs_tol=1e-12)
        )
        if not matches:
            raise RuntimeError(
                f"V12 risk contract drift: {key} actual={value!r} expected={expected!r}"
            )
    derived_budget = (
        float(actual["RISK_EQUITY_RS"])
        * float(actual["RISK_PCT_PER_TRADE"]) / 100.0
    )
    if not math.isclose(derived_budget, RISK_BUDGET_RS, abs_tol=1e-12):
        raise RuntimeError(
            f"risk budget drift: derived={derived_budget}, declared={RISK_BUDGET_RS}"
        )
    return {**actual, "derived_risk_budget_rs": derived_budget}


def _finite(frame: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = set(columns) - set(frame.columns)
    if missing:
        raise RuntimeError(f"{label} missing features: {sorted(missing)}")
    for column in columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise RuntimeError(f"{label} nonfinite feature: {column}")


def add_sequence_features(active: pd.DataFrame) -> pd.DataFrame:
    """Create causal active-list sequence fields without future-bar access."""
    work = active.sort_values(
        ["ticker", "trade_date", "signal_time_ist"], kind="mergesort"
    ).copy()
    group_keys = [work["ticker"], work["trade_date"]]
    previous_time = work.groupby(["ticker", "trade_date"], sort=False)[
        "signal_time_ist"
    ].shift(1)
    elapsed = (work["signal_time_ist"] - previous_time).dt.total_seconds() / 60.0
    contiguous = elapsed.eq(5.0)
    previous_contiguous = (
        contiguous.groupby(group_keys, sort=False).shift(1)
        .astype("boolean").fillna(False).astype(bool)
    )
    work["contiguous_prev_flag"] = contiguous.astype(float)
    work["contiguous_prev2_flag"] = (contiguous & previous_contiguous).astype(float)

    new_spell = (~contiguous).astype(int)
    work["_active_spell"] = new_spell.groupby(group_keys, sort=False).cumsum()
    spell_keys = [work["ticker"], work["trade_date"], work["_active_spell"]]
    work["bars_in_active_spell"] = (
        work.groupby(["ticker", "trade_date", "_active_spell"], sort=False)
        .cumcount() + 1
    ).astype(float)

    close = pd.to_numeric(work["signal_close"], errors="coerce")
    previous_close = close.groupby(group_keys, sort=False).shift(1)
    close_2 = close.groupby(group_keys, sort=False).shift(2)
    work["return_from_prev_active_close_pct"] = np.where(
        contiguous, (close / previous_close - 1.0) * 100.0, 0.0
    )
    work["return_over_2_active_bars_pct"] = np.where(
        contiguous & previous_contiguous,
        (close / close_2 - 1.0) * 100.0,
        0.0,
    )
    for source, target in (
        ("distance_from_session_high_pct", "delta_distance_from_session_high_pct"),
        ("upper_wick_pct", "delta_upper_wick_pct"),
    ):
        values = pd.to_numeric(work[source], errors="coerce")
        previous = values.groupby(group_keys, sort=False).shift(1)
        work[target] = np.where(contiguous, values - previous, 0.0)

    positive = (
        pd.to_numeric(work["signal_close"], errors="coerce")
        > pd.to_numeric(work["signal_open"], errors="coerce")
    ).astype(float)
    work["positive_body_count_last3"] = (
        positive.groupby(spell_keys, sort=False)
        .rolling(3, min_periods=1).sum()
        .reset_index(level=[0, 1, 2], drop=True)
        .sort_index()
    )
    traded_value = pd.to_numeric(work["traded_value_rs"], errors="coerce")
    if traded_value.lt(0).any():
        raise RuntimeError("negative traded value")
    work["log1p_traded_value_rs"] = np.log1p(traded_value)
    return work.drop(columns=["_active_spell"])


def load_ticket_states(
    *,
    start_date: str = START_DATE,
    end_date: str = REPLAY_END,
    rank_min: int = 200,
    rank_max: int = 300,
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    if str(start_date) > str(end_date):
        raise ValueError(f"start_date {start_date} is after end_date {end_date}")
    if int(rank_min) > int(rank_max):
        raise ValueError(f"rank_min {rank_min} is above rank_max {rank_max}")
    source = pd.read_parquet(v2.SOURCE, columns=list(SOURCE_COLUMNS))
    source["ticker"] = source["ticker"].astype(str).str.upper().str.strip()
    source["trade_date"] = source["trade_date"].astype(str)
    source = source.loc[
        source["trade_date"].between(str(start_date), str(end_date))
    ].copy()
    active_mask = (
        source["pre_entry_data_invalid"].eq(False)
        & source["primary_side"].astype(str).str.upper().eq("LONG")
        & pd.to_numeric(source["selection_rank"], errors="coerce").between(
            int(rank_min), int(rank_max)
        )
    )
    active = add_sequence_features(source.loc[active_mask].copy())
    _finite(active, LEVEL12 + SEQ8, "active context")

    base_mask = (
        pd.to_numeric(active["signal_minute"], errors="coerce").between(570, 855)
        & pd.to_numeric(active["atr_pct"], errors="coerce").ge(1.05)
        & pd.to_numeric(active["range_pct"], errors="coerce").ge(1.25)
        & pd.to_numeric(active["vwap_dist_atr"], errors="coerce").ge(0.05)
    )
    tickets = active.loc[base_mask].sort_values(
        ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
        kind="mergesort",
    ).copy().reset_index(drop=True)
    tickets["ticket_id"] = np.arange(len(tickets), dtype=int)
    tickets = tickets.rename(columns={"signal_time_ist": "ticket_time_ist"})

    targets = []
    ticket_key = tickets[["ticket_id", "ticker", "trade_date", "ticket_time_ist"]]
    for step in range(3):
        part = ticket_key.copy()
        part["ticket_step"] = step
        part["signal_time_ist"] = (
            part["ticket_time_ist"] + pd.Timedelta(minutes=5 * step)
        )
        targets.append(part)
    state_keys = pd.concat(targets, ignore_index=True)
    states = state_keys.merge(
        active,
        on=["ticker", "trade_date", "signal_time_ist"],
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    states["state_available"] = states["_merge"].eq("both")
    states = states.drop(columns=["_merge"])
    states["remaining_wait_steps"] = 2 - states["ticket_step"]

    available = states.loc[states["state_available"]].copy()
    _finite(available, LEVEL12 + SEQ8 + STAGE_B_STATE_FEATURES, "ticket state")
    nodes = (
        available.sort_values(
            ["trade_date", "signal_time_ist", "selection_rank", "ticker"],
            kind="mergesort",
        )
        .drop_duplicates(["ticker", "trade_date", "signal_time_ist"])
        .reset_index(drop=True)
    )
    nodes["_optimizer_row_id"] = np.arange(len(nodes), dtype=int)
    node_ids = nodes[[
        "ticker", "trade_date", "signal_time_ist", "_optimizer_row_id"
    ]]
    states = states.merge(
        node_ids,
        on=["ticker", "trade_date", "signal_time_ist"],
        how="left",
        validate="many_to_one",
    )

    nodes["setup"] = SETUP
    nodes["side"] = "LONG"
    nodes["bar_time_ist"] = nodes["signal_time_ist"]
    nodes["decision_ready_at_ist"] = nodes["signal_time_ist"]
    nodes["decision_ready_source"] = "completed_5min_signal_bar"
    nodes["quality_score"] = 301.0 - pd.to_numeric(
        nodes["selection_rank"], errors="raise"
    )
    nodes["score"] = nodes["quality_score"]
    funnel = pd.DataFrame([
        {"stage": "source_date_window", "rows": len(source)},
        {
            "stage": f"active_long_rank{int(rank_min)}_{int(rank_max)}_valid",
            "rows": len(active),
        },
        {"stage": "base_ticket_rows", "rows": len(tickets)},
        {
            "stage": "base_unique_ticker_days",
            "rows": int(tickets[["trade_date", "ticker"]].drop_duplicates().shape[0]),
        },
        {"stage": "available_ticket_states", "rows": len(available)},
        {"stage": "unique_execution_nodes", "rows": len(nodes)},
    ])
    return tickets, states, nodes, funnel


def build_exact_paths(
    states: pd.DataFrame,
    nodes: pd.DataFrame,
    *,
    exit_pairs: Sequence[tuple[float, float]] = EXIT_PAIRS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if nodes.empty:
        raise RuntimeError("no execution nodes in requested phase")
    phase_start = str(nodes["trade_date"].astype(str).min())
    phase_end = str(nodes["trade_date"].astype(str).max())
    loader = optimizer.install_windowed_1m_loader(
        v12, start_date=phase_start, end_date=phase_end
    )
    prewarm = optimizer.prewarm_windowed_1m_loader(
        loader, nodes["ticker"], workers=8
    )
    optimizer.install_day_1m_adapter(v12, loader)
    v12._V11_EXACT_LIVE_PARITY = False
    v12._V11_COST_MODEL = "statutory"
    v12._V11_SLIPPAGE_BPS = 0.0

    old_exit = v12.v6.SETUP_EXIT_RULES.get(SETUP)
    v12.v6.SETUP_EXIT_RULES[SETUP] = (1.0, 2.0)
    try:
        raw, rejects = v12._v7_entry_engine_raw_rows(nodes)
    finally:
        if old_exit is None:
            v12.v6.SETUP_EXIT_RULES.pop(SETUP, None)
        else:
            v12.v6.SETUP_EXIT_RULES[SETUP] = old_exit
    if raw.empty:
        raise RuntimeError("no V12-executable state rows")
    raw["_optimizer_row_id"] = pd.to_numeric(
        raw["_optimizer_row_id"], errors="raise"
    ).astype(int)
    outcomes = optimizer.resolve_exit_grid(
        raw,
        {SETUP: list(exit_pairs)},
        v12,
        progress_label="two-stage-v5",
    )
    expected = len(raw) * len(exit_pairs)
    if len(outcomes) != expected:
        raise RuntimeError(f"exit coverage failure: {len(outcomes)}/{expected}")
    executable = set(raw["_optimizer_row_id"].astype(int))
    states = states.copy()
    states["state_executable"] = (
        pd.to_numeric(states["_optimizer_row_id"], errors="coerce")
        .isin(executable)
    )
    return states, raw, outcomes, rejects, prewarm


def _gross_risk_rs(frame: pd.DataFrame) -> pd.Series:
    entry = pd.to_numeric(frame["entry_price"], errors="raise")
    sl_pct = pd.to_numeric(frame["sl_pct"], errors="raise")
    quantity = pd.to_numeric(frame["quantity"], errors="raise")
    return entry * sl_pct / 100.0 * quantity


def stage_b_training_label(current_net_r: float) -> str:
    """Binary supervised target for the current executable state only."""
    return "ENTER" if float(current_net_r) > 0.0 else "DEFER"


def stage_b_policy_action(enter_probability: float) -> str:
    """Frozen probability-threshold policy; independent of binary argmax."""
    return (
        "ENTER"
        if float(enter_probability) >= STAGE_B_ENTER_PROBABILITY
        else "DEFER"
    )


def make_exit_dataset(
    tickets: pd.DataFrame,
    states: pd.DataFrame,
    raw: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    sl_pct: float,
    tgt_pct: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build Stage-A ticket rows and Stage-B labelled state rows."""
    chosen = outcomes.loc[
        outcomes["sl_pct"].eq(float(sl_pct))
        & outcomes["tgt_pct"].eq(float(tgt_pct))
    ].copy()
    raw_columns = [
        "_optimizer_row_id", "v7_signal_entry_time_ist",
        "v7_signal_entry_price", "selection_rank", "quantity",
    ]
    executable_states = (
        states.loc[states["state_executable"]].copy()
        .merge(
            raw[raw_columns], on="_optimizer_row_id", how="inner",
            validate="many_to_one", suffixes=("", "_raw"),
        )
        .merge(
            chosen,
            on="_optimizer_row_id", how="inner", validate="many_to_one",
            suffixes=("", "_outcome"),
        )
    )
    # The source research entry and entry-engine placeholder sizing collide with
    # the exact optimizer outcome fields.  Canonical trading fields must always
    # mean the slipped fill and quantity for this chosen exit pair.
    if "entry_price_outcome" not in executable_states or "quantity_outcome" not in executable_states:
        raise RuntimeError("exact outcome fill/quantity columns missing after merge")
    executable_states["research_source_entry_price"] = pd.to_numeric(
        executable_states["entry_price"], errors="coerce"
    )
    executable_states["entry_engine_default_quantity"] = pd.to_numeric(
        executable_states["quantity"], errors="coerce"
    )
    executable_states["entry_price"] = pd.to_numeric(
        executable_states["entry_price_outcome"], errors="raise"
    )
    executable_states["quantity"] = pd.to_numeric(
        executable_states["quantity_outcome"], errors="raise"
    ).astype(int)
    expected_gross = (
        pd.to_numeric(executable_states["exit_price"], errors="raise")
        - executable_states["entry_price"]
    ) * executable_states["quantity"]
    if not np.allclose(
        expected_gross.to_numpy(dtype=float),
        pd.to_numeric(executable_states["gross_pnl_rs"], errors="raise").to_numpy(dtype=float),
        atol=1e-6,
        rtol=0.0,
    ):
        delta = float(
            np.max(np.abs(
                expected_gross.to_numpy(dtype=float)
                - executable_states["gross_pnl_rs"].to_numpy(dtype=float)
            ))
        )
        raise RuntimeError(f"exact outcome gross-P&L invariant failed: max delta={delta}")
    recomputed_net = []
    for row in executable_states.itertuples():
        costs = nse.intraday_equity_costs(
            float(row.entry_price), float(row.exit_price), int(row.quantity), "LONG"
        )
        recomputed_net.append(float(costs.net_pnl))
    if not np.allclose(
        np.asarray(recomputed_net),
        executable_states["net_pnl_rs"].to_numpy(dtype=float),
        atol=1e-6,
        rtol=0.0,
    ):
        raise RuntimeError("exact outcome statutory net-P&L invariant failed")
    executable_states["gross_risk_rs"] = _gross_risk_rs(executable_states)
    if executable_states["gross_risk_rs"].le(0).any():
        raise RuntimeError("nonpositive gross risk")
    executable_states["net_r"] = (
        pd.to_numeric(executable_states["net_pnl_rs"], errors="raise")
        / executable_states["gross_risk_rs"]
    )
    executable_states["path_positive"] = (
        executable_states["outcome"].astype(str).eq("TARGET")
        & executable_states["net_pnl_rs"].gt(0)
    ).astype(int)

    # A WAIT action can reach only a contiguous executable prefix.  Never train
    # on a hypothetical 0 -> 2 jump when the +5-minute state is unavailable.
    reachable_parts: list[pd.DataFrame] = []
    for _, group in executable_states.groupby("ticket_id", sort=False):
        by_step = {
            int(row["ticket_step"]): row
            for _, row in group.sort_values("ticket_step").iterrows()
        }
        prefix: list[pd.Series] = []
        for step in range(3):
            if step not in by_step:
                break
            prefix.append(by_step[step])
        if prefix and int(prefix[0]["ticket_step"]) == 0:
            reachable_parts.append(pd.DataFrame(prefix))
    executable_states = (
        pd.concat(reachable_parts, ignore_index=True)
        if reachable_parts else pd.DataFrame(columns=executable_states.columns)
    )
    if executable_states.empty:
        raise RuntimeError("no contiguous executable ticket states")

    executable_states["stage_b_label"] = [
        stage_b_training_label(value)
        for value in executable_states["net_r"].to_numpy(dtype=float)
    ]

    step0 = executable_states.loc[executable_states["ticket_step"].eq(0)].copy()
    if step0["ticket_id"].duplicated().any():
        raise RuntimeError("duplicate executable ticket initiation")
    opportunity = (
        executable_states.groupby("ticket_id", as_index=False, sort=False)
        .agg(opportunity_positive=("path_positive", "max"))
    )
    ticket_columns = [
        "ticket_id", "ticket_time_ist", "ticker", "trade_date",
        *LEVEL12, *SEQ8,
    ]
    ticket_frame = (
        tickets[ticket_columns]
        .merge(step0[["ticket_id"]], on="ticket_id", how="inner", validate="one_to_one")
        .merge(opportunity, on="ticket_id", how="inner", validate="one_to_one")
    )
    _finite(ticket_frame, LEVEL12 + SEQ8, "Stage A")
    _finite(
        executable_states,
        LEVEL12 + SEQ8 + STAGE_B_STATE_FEATURES,
        "Stage B",
    )
    return ticket_frame, executable_states


def unit_weights(frame: pd.DataFrame) -> pd.Series:
    keys = (
        frame["ticker"].astype(str).str.upper().str.strip()
        + "|" + frame["trade_date"].astype(str)
    )
    return 1.0 / keys.groupby(keys).transform("size")


def new_stage_a_model() -> Any:
    return make_pipeline(
        StandardScaler(),
        LogisticRegression(
            C=0.10,
            max_iter=2000,
            random_state=20260805,
        ),
    )


def new_stage_b_model() -> Any:
    return make_pipeline(
        StandardScaler(),
        LogisticRegression(
            C=0.10,
            max_iter=2000,
            random_state=20260805,
        ),
    )


def fit_models(
    tickets: pd.DataFrame,
    states: pd.DataFrame,
    config: Config,
) -> tuple[Any, Any]:
    if tickets["opportunity_positive"].nunique() != 2:
        raise InsufficientEvidenceError("Stage A requires both label classes")
    weights_a = unit_weights(tickets)
    effective_a = {
        int(label): float(weights_a.loc[tickets["opportunity_positive"].eq(label)].sum())
        for label in (0, 1)
    }
    if min(effective_a.values()) < 15.0:
        raise InsufficientEvidenceError(
            f"Stage A effective ticker-day support below 15: {effective_a}"
        )
    weights_b = unit_weights(states)
    stage_b_target = states["stage_b_label"]
    effective_b = {
        action: float(weights_b.loc[stage_b_target.eq(action)].sum())
        for action in ("ENTER", "DEFER")
    }
    if min(effective_b.values()) < 15.0:
        raise InsufficientEvidenceError(
            "Stage B binary effective ticker-day support below 15: "
            + json.dumps(effective_b, sort_keys=True)
        )
    stage_a = new_stage_a_model()
    total_effective = float(weights_a.sum())
    class_factors = {
        label: total_effective / (2.0 * effective_a[label])
        for label in (0, 1)
    }
    balanced_weights_a = weights_a * tickets["opportunity_positive"].map(
        class_factors
    ).astype(float)
    stage_a.fit(
        tickets[list(config.stage_a_features)],
        tickets["opportunity_positive"],
        standardscaler__sample_weight=weights_a,
        logisticregression__sample_weight=balanced_weights_a,
    )
    stage_b = new_stage_b_model()
    stage_b.fit(
        states[list(config.stage_b_features)],
        stage_b_target,
        standardscaler__sample_weight=weights_b,
        logisticregression__sample_weight=weights_b,
    )
    return stage_a, stage_b


def _enter_probability(model: Any, frame: pd.DataFrame, features: Sequence[str]) -> np.ndarray:
    probabilities = model.predict_proba(frame[list(features)])
    classes = list(model.named_steps["logisticregression"].classes_)
    return probabilities[:, classes.index("ENTER")]


def weighted_auc(y: pd.Series, scores: np.ndarray, frame: pd.DataFrame) -> float:
    if pd.Series(y).nunique() != 2:
        return float("nan")
    return float(roc_auc_score(y, scores, sample_weight=unit_weights(frame)))


def confidence_ordering(
    labels: pd.Series,
    scores: np.ndarray,
    frame: pd.DataFrame,
) -> dict[str, float]:
    work = pd.DataFrame({
        "label": pd.to_numeric(labels, errors="raise").to_numpy(dtype=float),
        "score": np.asarray(scores, dtype=float),
        "weight": unit_weights(frame).to_numpy(dtype=float),
    })
    if len(work) < 8 or work["score"].nunique() < 4:
        return {"bottom_quartile_rate": float("nan"), "top_quartile_rate": float("nan")}
    ordered = work.sort_values("score", kind="mergesort").reset_index(drop=True)
    cumulative = ordered["weight"].cumsum().to_numpy(dtype=float)
    total = float(ordered["weight"].sum())
    low_position = int(np.searchsorted(cumulative, total * 0.25, side="left"))
    high_position = int(np.searchsorted(cumulative, total * 0.75, side="left"))
    low_threshold = float(ordered.iloc[min(low_position, len(ordered) - 1)]["score"])
    high_threshold = float(ordered.iloc[min(high_position, len(ordered) - 1)]["score"])
    bottom = ordered.loc[ordered["score"].le(low_threshold)]
    top = ordered.loc[ordered["score"].ge(high_threshold)]
    return {
        "bottom_quartile_rate": float(
            np.average(bottom["label"], weights=bottom["weight"])
        ),
        "top_quartile_rate": float(
            np.average(top["label"], weights=top["weight"])
        ),
    }


def _ticker_day_peaks(scored: pd.DataFrame) -> pd.DataFrame:
    return (
        scored.groupby(["trade_date", "ticker"], as_index=False, sort=True)
        .agg(peak_score=("stage_a_score", "max"))
    )


def rolling_reference_rows(
    history: pd.DataFrame,
    day: str,
    calendar: Sequence[str],
) -> tuple[pd.DataFrame, list[str]]:
    calendar = list(calendar)
    position = calendar.index(day)
    prior_days = calendar[position - ROLLING_SCORE_SESSIONS:position]
    if len(prior_days) != ROLLING_SCORE_SESSIONS:
        raise RuntimeError(f"insufficient rolling history for {day}")
    if history["trade_date"].astype(str).ge(day).any():
        raise RuntimeError(f"noncausal score history on {day}")
    return (
        history.loc[history["trade_date"].astype(str).isin(prior_days)].copy(),
        prior_days,
    )


def _apply_daily_cap(selected: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if selected.empty:
        return selected.copy(), selected.copy()
    work = selected.copy()
    work["_entry_ts"] = pd.to_datetime(
        work["entry_time_ist"], errors="coerce", utc=True
    ).dt.tz_convert("Asia/Kolkata")
    work = work.sort_values(
        ["trade_date", "_entry_ts", "selection_rank", "ticker"],
        kind="mergesort",
    )
    work["daily_sequence"] = work.groupby("trade_date", sort=False).cumcount() + 1
    accepted = work.loc[work["daily_sequence"].le(DAILY_CAP)].copy()
    rejected = work.loc[work["daily_sequence"].gt(DAILY_CAP)].copy()
    if not rejected.empty:
        rejected["decision"] = f"REJECT_DAILY_CAP_{DAILY_CAP}"
    return (
        accepted.drop(columns=["_entry_ts"]).reset_index(drop=True),
        rejected.drop(columns=["_entry_ts"]).reset_index(drop=True),
    )


def simulate_evaluation(
    history_tickets: pd.DataFrame,
    evaluation_tickets: pd.DataFrame,
    all_states: pd.DataFrame,
    evaluation_days: Sequence[str],
    calendar: Sequence[str],
    config: Config,
    stage_a_model: Any,
    stage_b_model: Any,
    *,
    insufficient_reference_policy: str = "raise",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if insufficient_reference_policy not in {"raise", "abstain"}:
        raise ValueError(
            "insufficient_reference_policy must be 'raise' or 'abstain'"
        )
    history = history_tickets.copy()
    evaluation = evaluation_tickets.copy()
    history["stage_a_score"] = stage_a_model.predict_proba(
        history[list(config.stage_a_features)]
    )[:, 1]
    evaluation["stage_a_score"] = stage_a_model.predict_proba(
        evaluation[list(config.stage_a_features)]
    )[:, 1]
    state_by_ticket = {
        int(ticket_id): group.sort_values("ticket_step", kind="mergesort")
        for ticket_id, group in all_states.groupby("ticket_id", sort=False)
    }
    selected_rows: list[pd.Series] = []
    threshold_logs: list[dict[str, Any]] = []
    decision_logs: list[dict[str, Any]] = []
    reached_state_logs: list[dict[str, Any]] = []

    calendar = list(calendar)
    for day in evaluation_days:
        reference_rows, prior_days = rolling_reference_rows(
            history, day, calendar
        )
        reference = _ticker_day_peaks(reference_rows)
        current = evaluation.loc[evaluation["trade_date"].eq(day)].copy()
        reference_error = ""
        if len(reference) < 30 or reference["trade_date"].nunique() < 15:
            reference_error = (
                "INSUFFICIENT_ROLLING_REFERENCE_COVERAGE:"
                f"{len(reference)}_units:"
                f"{reference['trade_date'].nunique()}_active_days"
            )
        tail_k = int(math.ceil(config.rolling_fraction * len(reference)))
        if not reference_error and tail_k < 10:
            reference_error = f"INSUFFICIENT_ROLLING_TAIL:{tail_k}_units"
        if reference_error:
            if insufficient_reference_policy == "raise":
                raise RuntimeError(f"{reference_error} on {day}")
            threshold_logs.append({
                "trade_date": day,
                "reference_sessions": len(prior_days),
                "reference_unique_ticker_days": len(reference),
                "rolling_fraction": config.rolling_fraction,
                "tail_k": tail_k,
                "threshold": float("nan"),
                "candidate_rows": len(current),
                "passing_rows": 0,
                "opened_tickets": 0,
                "stage_b_entries_before_cap": 0,
                "reference_status": "ABSTAIN",
                "abstention_reason": reference_error,
            })
            if not current.empty:
                history = pd.concat([history, current], ignore_index=True)
            continue
        values = reference["peak_score"].to_numpy(dtype=float)
        threshold = float(np.partition(values, len(values) - tail_k)[len(values) - tail_k])
        eligible = current.loc[current["stage_a_score"].ge(threshold)].sort_values(
            ["ticket_time_ist", "selection_rank", "ticker"], kind="mergesort"
        )
        first_tickets = eligible.drop_duplicates(
            ["trade_date", "ticker"], keep="first"
        )
        day_selected = 0
        for ticket in first_tickets.itertuples():
            ticket_id = int(ticket.ticket_id)
            states = state_by_ticket.get(ticket_id)
            final_decision = "REJECT_MISSING_STATE"
            chosen: pd.Series | None = None
            trace: list[str] = []
            if states is not None:
                states_by_step = {
                    int(state["ticket_step"]): state
                    for _, state in states.iterrows()
                }
                for step in range(3):
                    state = states_by_step.get(step)
                    if state is None:
                        final_decision = f"REJECT_MISSING_STATE_{step}"
                        break
                    probabilities = stage_b_model.predict_proba(
                        state[list(config.stage_b_features)].to_frame().T
                    )[0]
                    classes = list(
                        stage_b_model.named_steps["logisticregression"].classes_
                    )
                    predicted = str(classes[int(np.argmax(probabilities))])
                    enter_p = float(probabilities[classes.index("ENTER")])
                    policy_action = stage_b_policy_action(enter_p)
                    reached_state_logs.append({
                        "trade_date": day,
                        "ticker": str(ticket.ticker),
                        "ticket_id": ticket_id,
                        "ticket_step": step,
                        "label": int(str(state["stage_b_label"]) == "ENTER"),
                        "score": enter_p,
                        "true_action": str(state["stage_b_label"]),
                        "model_argmax_action": predicted,
                        "policy_action": policy_action,
                    })
                    trace.append(
                        f"s{step}:argmax={predicted}:policy={policy_action}:p={enter_p:.6f}"
                    )
                    if policy_action == "ENTER":
                        chosen = state.copy()
                        chosen["stage_a_score"] = float(ticket.stage_a_score)
                        chosen["stage_b_enter_probability"] = enter_p
                        chosen["stage_b_predicted_action"] = predicted
                        chosen["stage_b_policy_action"] = policy_action
                        chosen["rolling_threshold"] = threshold
                        final_decision = f"ENTER_STEP_{step}"
                        break
                    if step >= 2:
                        final_decision = "REJECT_STEP2_NO_ENTER"
                        break
                    # The statistically supportable model is binary.  DEFER
                    # means wait exactly one contiguous state when available;
                    # the terminal DEFER is a deterministic rejection.
                    final_decision = f"WAIT_AFTER_STEP_{step}"
            if chosen is not None:
                chosen["decision"] = final_decision
                selected_rows.append(chosen)
                day_selected += 1
            decision_logs.append({
                "trade_date": day,
                "ticker": str(ticket.ticker),
                "ticket_id": ticket_id,
                "ticket_time_ist": ticket.ticket_time_ist,
                "selection_rank": int(ticket.selection_rank),
                "stage_a_score": float(ticket.stage_a_score),
                "rolling_threshold": threshold,
                "decision": final_decision,
                "stage_b_trace": "|".join(trace),
            })
        threshold_logs.append({
            "trade_date": day,
            "reference_sessions": len(prior_days),
            "reference_unique_ticker_days": len(reference),
            "rolling_fraction": config.rolling_fraction,
            "tail_k": tail_k,
            "threshold": threshold,
            "candidate_rows": len(current),
            "passing_rows": len(eligible),
            "opened_tickets": len(first_tickets),
            "stage_b_entries_before_cap": day_selected,
            "reference_status": "OK",
            "abstention_reason": "",
        })
        if not current.empty:
            history = pd.concat([history, current], ignore_index=True)

    selected = (
        pd.DataFrame(selected_rows)
        if selected_rows else pd.DataFrame(columns=all_states.columns)
    )
    accepted, cap_rejects = _apply_daily_cap(selected)
    if not cap_rejects.empty:
        capped_ids = set(cap_rejects["ticket_id"].astype(int))
        for record in decision_logs:
            if int(record["ticket_id"]) in capped_ids:
                record["decision"] = f"REJECT_DAILY_CAP_{DAILY_CAP}"
    reached = pd.DataFrame(reached_state_logs)
    if not reached.empty:
        reached["weight"] = unit_weights(reached).to_numpy(dtype=float)
    else:
        reached = pd.DataFrame(columns=[
            "trade_date", "ticker", "ticket_id", "ticket_step", "label",
            "score", "true_action", "model_argmax_action", "policy_action",
            "weight",
        ])
    return (
        accepted,
        pd.DataFrame(threshold_logs),
        pd.DataFrame(decision_logs),
        reached,
    )


def performance(trades: pd.DataFrame, sessions: Sequence[str]) -> dict[str, Any]:
    return replay_helpers.metrics(trades, list(sessions))


def evaluation_diagnostics(
    evaluation_tickets: pd.DataFrame,
    evaluation_states: pd.DataFrame,
    config: Config,
    stage_a: Any,
    stage_b: Any,
) -> tuple[dict[str, float], pd.DataFrame, pd.DataFrame]:
    a_scores = stage_a.predict_proba(
        evaluation_tickets[list(config.stage_a_features)]
    )[:, 1]
    a_auc = weighted_auc(
        evaluation_tickets["opportunity_positive"], a_scores, evaluation_tickets
    )
    ordering = confidence_ordering(
        evaluation_tickets["opportunity_positive"], a_scores, evaluation_tickets
    )
    a_diagnostics = pd.DataFrame({
        "label": evaluation_tickets["opportunity_positive"].to_numpy(dtype=int),
        "score": a_scores,
        "weight": unit_weights(evaluation_tickets).to_numpy(dtype=float),
    })
    enter_scores = _enter_probability(
        stage_b, evaluation_states, config.stage_b_features
    )
    enter_labels = evaluation_states["stage_b_label"].eq("ENTER").astype(int)
    b_auc = weighted_auc(enter_labels, enter_scores, evaluation_states)
    b_ordering = confidence_ordering(
        enter_labels, enter_scores, evaluation_states
    )
    b_diagnostics = pd.DataFrame({
        "label": enter_labels.to_numpy(dtype=int),
        "score": enter_scores,
        "weight": unit_weights(evaluation_states).to_numpy(dtype=float),
    })
    return ({
        "stage_a_auc": a_auc,
        "stage_b_enter_auc": b_auc,
        **ordering,
        "stage_b_bottom_quartile_enter_rate": b_ordering["bottom_quartile_rate"],
        "stage_b_top_quartile_enter_rate": b_ordering["top_quartile_rate"],
    }, a_diagnostics, b_diagnostics)


def evaluate_config(
    ticket_frame: pd.DataFrame,
    state_frame: pd.DataFrame,
    train_days: Sequence[str],
    evaluation_days: Sequence[str],
    calendar: Sequence[str],
    config: Config,
    *,
    return_models: bool = False,
    insufficient_reference_policy: str = "raise",
) -> dict[str, Any]:
    train_tickets = ticket_frame.loc[ticket_frame["trade_date"].isin(train_days)].copy()
    train_states = state_frame.loc[state_frame["trade_date"].isin(train_days)].copy()
    evaluation_tickets = ticket_frame.loc[
        ticket_frame["trade_date"].isin(evaluation_days)
    ].copy()
    evaluation_states = state_frame.loc[
        state_frame["trade_date"].isin(evaluation_days)
    ].copy()
    if evaluation_tickets.empty or evaluation_states.empty:
        raise RuntimeError("empty evaluation data")
    stage_a, stage_b = fit_models(train_tickets, train_states, config)
    trades, thresholds, decisions, reached_states = simulate_evaluation(
        train_tickets,
        evaluation_tickets,
        state_frame,
        evaluation_days,
        calendar,
        config,
        stage_a,
        stage_b,
        insufficient_reference_policy=insufficient_reference_policy,
    )
    metrics = performance(trades, evaluation_days)
    diagnostics, a_diagnostics, b_all_diagnostics = evaluation_diagnostics(
        evaluation_tickets, evaluation_states, config, stage_a, stage_b
    )
    diagnostics["stage_b_all_states_enter_auc"] = diagnostics["stage_b_enter_auc"]
    if not reached_states.empty:
        reached_auc = (
            float(roc_auc_score(
                reached_states["label"], reached_states["score"],
                sample_weight=reached_states["weight"],
            ))
            if reached_states["label"].nunique() == 2 else float("nan")
        )
        reached_ordering = confidence_ordering(
            reached_states["label"], reached_states["score"], reached_states
        )
    else:
        reached_auc = float("nan")
        reached_ordering = {
            "bottom_quartile_rate": float("nan"),
            "top_quartile_rate": float("nan"),
        }
    diagnostics["stage_b_enter_auc"] = reached_auc
    diagnostics["stage_b_bottom_quartile_enter_rate"] = reached_ordering[
        "bottom_quartile_rate"
    ]
    diagnostics["stage_b_top_quartile_enter_rate"] = reached_ordering[
        "top_quartile_rate"
    ]
    active_days = metrics["sessions"] - metrics["zero_trade_sessions"]
    result: dict[str, Any] = {
        "config_id": config.config_id,
        "feature_family": config.feature_family,
        "sl_pct": config.sl_pct,
        "tgt_pct": config.tgt_pct,
        "rolling_fraction": config.rolling_fraction,
        **metrics,
        "active_trade_days": active_days,
        "mean_net_r_per_trade": (
            float(pd.to_numeric(trades["net_r"], errors="coerce").mean())
            if not trades.empty else 0.0
        ),
        **diagnostics,
        "trades_frame": trades,
        "thresholds_frame": thresholds,
        "decisions_frame": decisions,
        "stage_a_diagnostics_frame": a_diagnostics,
        "stage_b_diagnostics_frame": reached_states,
        "stage_b_all_states_diagnostics_frame": b_all_diagnostics,
    }
    if return_models:
        result["stage_a_model"] = stage_a
        result["stage_b_model"] = stage_b
    return result


def _metric_record(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in result.items()
        if not key.endswith("_frame") and not key.endswith("_model")
    }


def _inner_bounds(n_days: int) -> tuple[tuple[int, int, int], ...]:
    if n_days < 44:
        raise RuntimeError(f"inner selection requires at least 44 days, got {n_days}")
    return (
        (n_days - 24, n_days - 24, n_days - 16),
        (n_days - 16, n_days - 16, n_days - 8),
        (n_days - 8, n_days - 8, n_days),
    )


def inner_select(
    datasets: Mapping[tuple[float, float], tuple[pd.DataFrame, pd.DataFrame]],
    available_days: Sequence[str],
    calendar: Sequence[str],
    *,
    context: str,
) -> tuple[Config, pd.DataFrame, pd.DataFrame, bool]:
    fold_outputs: list[dict[str, Any]] = []
    trades_by_config: dict[str, list[pd.DataFrame]] = {
        config.config_id: [] for config in configurations()
    }
    configs = {config.config_id: config for config in configurations()}
    for config in configurations():
        tickets, states = datasets[(config.sl_pct, config.tgt_pct)]
        for fold_number, (train_end, val_start, val_end) in enumerate(
            _inner_bounds(len(available_days)), 1
        ):
            train_days = list(available_days[:train_end])
            val_days = list(available_days[val_start:val_end])
            try:
                result = evaluate_config(
                    tickets, states, train_days, val_days, calendar, config
                )
                trades = result.pop("trades_frame")
                result.pop("thresholds_frame")
                result.pop("decisions_frame")
                result.pop("stage_a_diagnostics_frame")
                result.pop("stage_b_diagnostics_frame")
                result.pop("stage_b_all_states_diagnostics_frame")
                result.update({
                    "selection_context": context,
                    "inner_fold": f"I{fold_number}",
                    "train_sessions": len(train_days),
                    "validation_sessions": len(val_days),
                    "valid_evaluation": True,
                    "error": "",
                })
                trades["inner_fold"] = f"I{fold_number}"
                trades_by_config[config.config_id].append(trades)
            except InsufficientEvidenceError as exc:
                result = {
                    "config_id": config.config_id,
                    "feature_family": config.feature_family,
                    "sl_pct": config.sl_pct,
                    "tgt_pct": config.tgt_pct,
                    "rolling_fraction": config.rolling_fraction,
                    "selection_context": context,
                    "inner_fold": f"I{fold_number}",
                    "train_sessions": len(train_days),
                    "validation_sessions": len(val_days),
                    "valid_evaluation": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            fold_outputs.append(result)

    folds = pd.DataFrame(fold_outputs)
    aggregate_rows: list[dict[str, Any]] = []
    for config_id, config in configs.items():
        subset = folds.loc[folds["config_id"].eq(config_id)].copy()
        valid = bool(len(subset) == 3 and subset["valid_evaluation"].all())
        if valid:
            combined = pd.concat(trades_by_config[config_id], ignore_index=True)
            validation_days: list[str] = []
            for _, start, end in _inner_bounds(len(available_days)):
                validation_days.extend(available_days[start:end])
            metrics = performance(combined, validation_days)
            active = metrics["sessions"] - metrics["zero_trade_sessions"]
            fold_nets = subset["net_pnl_rs"].astype(float).tolist()
            fold_trades = subset["trades"].astype(int).tolist()
            fold_active = subset["active_trade_days"].astype(int).tolist()
            fold_net_r = subset["mean_net_r_per_trade"].astype(float).tolist()
            positive_folds = sum(value > 0 for value in fold_nets)
            eligible = bool(
                metrics["trades"] >= 18
                and active >= 10
                and min(fold_trades) >= 3
                and min(fold_active) >= 2
                and positive_folds >= 2
                and metrics["net_pnl_rs"] > 0
                and metrics["profit_factor"] >= 1.05
            )
            row = {
                "selection_context": context,
                "config_id": config_id,
                "feature_family": config.feature_family,
                "sl_pct": config.sl_pct,
                "tgt_pct": config.tgt_pct,
                "rolling_fraction": config.rolling_fraction,
                "valid_evaluation": True,
                "inner_eligible": eligible,
                **metrics,
                "active_trade_days": active,
                "positive_folds": positive_folds,
                "worst_fold_net_pnl_rs": min(fold_nets),
                "worst_fold_net_r_per_trade": min(fold_net_r),
                "median_fold_net_r_per_trade": float(np.median(fold_net_r)),
                "minimum_fold_trades": min(fold_trades),
                "minimum_fold_active_days": min(fold_active),
                "mean_stage_a_auc": float(subset["stage_a_auc"].mean()),
                "mean_stage_b_enter_auc": float(subset["stage_b_enter_auc"].mean()),
            }
        else:
            row = {
                "selection_context": context,
                "config_id": config_id,
                "feature_family": config.feature_family,
                "sl_pct": config.sl_pct,
                "tgt_pct": config.tgt_pct,
                "rolling_fraction": config.rolling_fraction,
                "valid_evaluation": False,
                "inner_eligible": False,
                "trades": 0,
                "sessions": 24,
                "active_trade_days": 0,
                "positive_folds": 0,
                "net_pnl_rs": -math.inf,
                "profit_factor": 0.0,
                "worst_fold_net_pnl_rs": -math.inf,
                "worst_fold_net_r_per_trade": -math.inf,
                "median_fold_net_r_per_trade": -math.inf,
                "minimum_fold_trades": 0,
                "minimum_fold_active_days": 0,
                "mean_stage_a_auc": float("nan"),
                "mean_stage_b_enter_auc": float("nan"),
            }
        aggregate_rows.append(row)
    aggregate = pd.DataFrame(aggregate_rows)
    eligible = aggregate.loc[aggregate["inner_eligible"]].copy()
    pool = eligible if not eligible.empty else aggregate.loc[aggregate["valid_evaluation"]].copy()
    if pool.empty:
        errors = folds.loc[~folds["valid_evaluation"], "error"].value_counts().to_dict()
        raise RuntimeError(f"all inner configurations invalid: {errors}")
    # This exact order is frozen before the replay.  If no configuration clears
    # the evidence gate, the same order picks a diagnostic fallback only.
    winner_row = pool.sort_values(
        [
            "positive_folds", "worst_fold_net_r_per_trade",
            "median_fold_net_r_per_trade", "profit_factor", "trades",
            "feature_family", "config_id",
        ],
        ascending=[False, False, False, False, False, True, True],
        kind="mergesort",
    ).iloc[0]
    winner = configs[str(winner_row["config_id"])]
    return winner, aggregate, folds, bool(winner_row["inner_eligible"])


def concentration_metrics(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "largest_ticker_trade_share": 1.0,
            "largest_positive_day_pnl_share": 1.0,
        }
    ticker_share = float(trades["ticker"].value_counts(normalize=True).max())
    daily = trades.groupby("trade_date")["net_pnl_rs"].sum()
    positive = daily.loc[daily.gt(0)]
    positive_share = (
        float(positive.max() / positive.sum()) if not positive.empty else 1.0
    )
    return {
        "largest_ticker_trade_share": ticker_share,
        "largest_positive_day_pnl_share": positive_share,
    }


def run_outer_validation(
    datasets: Mapping[tuple[float, float], tuple[pd.DataFrame, pd.DataFrame]],
    development_days: Sequence[str],
    calendar: Sequence[str],
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame,
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame,
]:
    result_rows: list[dict[str, Any]] = []
    inner_aggregate_parts: list[pd.DataFrame] = []
    inner_fold_parts: list[pd.DataFrame] = []
    trade_parts: list[pd.DataFrame] = []
    stage_a_diagnostic_parts: list[pd.DataFrame] = []
    stage_b_diagnostic_parts: list[pd.DataFrame] = []
    threshold_parts: list[pd.DataFrame] = []
    decision_parts: list[pd.DataFrame] = []
    for fold_name, train_end, test_start, test_end in OUTER_FOLDS:
        train_days = list(development_days[:train_end])
        test_days = list(development_days[test_start:test_end])
        winner, aggregate, folds, inner_eligible = inner_select(
            datasets, train_days, calendar, context=fold_name
        )
        inner_aggregate_parts.append(aggregate)
        inner_fold_parts.append(folds)
        tickets, states = datasets[(winner.sl_pct, winner.tgt_pct)]
        result = evaluate_config(
            tickets, states, train_days, test_days, calendar, winner
        )
        trades = result.pop("trades_frame")
        thresholds = result.pop("thresholds_frame")
        decisions = result.pop("decisions_frame")
        thresholds["outer_fold"] = fold_name
        thresholds["outer_selected_config"] = winner.config_id
        decisions["outer_fold"] = fold_name
        decisions["outer_selected_config"] = winner.config_id
        threshold_parts.append(thresholds)
        decision_parts.append(decisions)
        a_diagnostics = result.pop("stage_a_diagnostics_frame")
        b_diagnostics = result.pop("stage_b_diagnostics_frame")
        result.pop("stage_b_all_states_diagnostics_frame")
        a_diagnostics["outer_fold"] = fold_name
        b_diagnostics["outer_fold"] = fold_name
        stage_a_diagnostic_parts.append(a_diagnostics)
        stage_b_diagnostic_parts.append(b_diagnostics)
        trades["outer_fold"] = fold_name
        trades["outer_selected_config"] = winner.config_id
        trade_parts.append(trades)
        result.update({
            "outer_fold": fold_name,
            "train_sessions": len(train_days),
            "test_sessions": len(test_days),
            "inner_winner_eligible": inner_eligible,
        })
        result_rows.append(result)
    return (
        pd.DataFrame(result_rows),
        pd.concat(trade_parts, ignore_index=True),
        pd.concat(inner_aggregate_parts, ignore_index=True),
        pd.concat(inner_fold_parts, ignore_index=True),
        pd.concat(stage_a_diagnostic_parts, ignore_index=True),
        pd.concat(stage_b_diagnostic_parts, ignore_index=True),
        pd.concat(threshold_parts, ignore_index=True),
        pd.concat(decision_parts, ignore_index=True),
    )


def outer_gate(
    fold_results: pd.DataFrame,
    trades: pd.DataFrame,
    development_days: Sequence[str],
    stage_a_diagnostics: pd.DataFrame,
    stage_b_diagnostics: pd.DataFrame,
) -> dict[str, Any]:
    outer_days = list(development_days[48:78])
    metrics = performance(trades, outer_days)
    active = metrics["sessions"] - metrics["zero_trade_sessions"]
    fold_positive = int(fold_results["net_pnl_rs"].gt(0).sum())
    fold_min_trades = int(fold_results["trades"].min())
    fold_min_active = int(fold_results["active_trade_days"].min())
    fold_mean_r = fold_results["mean_net_r_per_trade"].astype(float)
    stage_a_aucs = fold_results["stage_a_auc"].astype(float)
    stage_b_aucs = fold_results["stage_b_enter_auc"].astype(float)
    def safe_pooled_auc(frame: pd.DataFrame) -> float:
        if frame.empty or frame["label"].nunique() != 2:
            return float("nan")
        return float(roc_auc_score(
            frame["label"], frame["score"], sample_weight=frame["weight"]
        ))

    pooled_stage_a_auc = safe_pooled_auc(stage_a_diagnostics)
    pooled_stage_b_auc = safe_pooled_auc(stage_b_diagnostics)
    ordering_passes = (
        fold_results["top_quartile_rate"]
        > fold_results["bottom_quartile_rate"]
    )
    concentration = concentration_metrics(trades)
    positive_fold_pnl = fold_results.loc[
        fold_results["net_pnl_rs"].gt(0), "net_pnl_rs"
    ].astype(float)
    largest_positive_fold_share = (
        float(positive_fold_pnl.max() / positive_fold_pnl.sum())
        if not positive_fold_pnl.empty else 1.0
    )
    checks = {
        "all_inner_winners_evidence_eligible": bool(
            fold_results["inner_winner_eligible"].all()
        ),
        "aggregate_minimum_20_trades": metrics["trades"] >= 20,
        "aggregate_minimum_12_active_days": active >= 12,
        "each_fold_minimum_3_trades": fold_min_trades >= 3,
        "each_fold_minimum_2_active_days": fold_min_active >= 2,
        "minimum_2_profitable_folds": fold_positive >= 2,
        "aggregate_net_positive": metrics["net_pnl_rs"] > 0,
        "aggregate_pf_at_least_1_10": metrics["profit_factor"] >= 1.10,
        "worst_fold_net_r_per_trade_at_least_minus_0_20": fold_mean_r.min() >= -0.20,
        "pooled_stage_a_auc_at_least_0_55": pooled_stage_a_auc >= 0.55,
        "stage_a_auc_at_least_0_50_in_two_folds": int(stage_a_aucs.ge(0.50).sum()) >= 2,
        "pooled_stage_b_enter_auc_at_least_0_55": pooled_stage_b_auc >= 0.55,
        "stage_b_auc_at_least_0_50_in_two_folds": int(stage_b_aucs.ge(0.50).sum()) >= 2,
        "confidence_ordering_in_two_folds": int(ordering_passes.sum()) >= 2,
        "largest_ticker_trade_share_at_most_0_20": concentration[
            "largest_ticker_trade_share"
        ] <= 0.20,
        "largest_positive_day_pnl_share_at_most_0_40": concentration[
            "largest_positive_day_pnl_share"
        ] <= 0.40,
        "largest_positive_fold_pnl_share_at_most_0_70": (
            largest_positive_fold_share <= 0.70
        ),
    }
    return {
        "metrics": metrics,
        "active_trade_days": active,
        "positive_folds": fold_positive,
        "minimum_fold_trades": fold_min_trades,
        "minimum_fold_active_days": fold_min_active,
        "worst_fold_mean_net_r": float(fold_mean_r.min()),
        "mean_stage_a_auc": float(stage_a_aucs.mean()),
        "mean_stage_b_enter_auc": float(stage_b_aucs.mean()),
        "pooled_stage_a_auc": pooled_stage_a_auc,
        "pooled_stage_b_enter_auc": pooled_stage_b_auc,
        "largest_positive_fold_pnl_share": largest_positive_fold_share,
        **concentration,
        "checks": checks,
        "passed": bool(all(checks.values())),
    }


def strict_risk_quantity(
    entry_price: float,
    sl_pct: float,
    *,
    risk_budget_rs: float = RISK_BUDGET_RS,
) -> tuple[int, float, float]:
    """Largest integer quantity whose statutory-costed stop loss is in budget."""
    entry = round(float(entry_price), 2)
    stop = entry * (1.0 - float(sl_pct) / 100.0)
    if entry <= 0 or stop <= 0 or stop >= entry:
        return 0, stop, 0.0

    def stop_loss(quantity: int) -> float:
        costs = nse.intraday_equity_costs(entry, stop, int(quantity), "LONG")
        return float(-costs.net_pnl)

    if stop_loss(1) > risk_budget_rs:
        return 0, stop, stop_loss(1)
    gross_per_share = max(entry - stop, 1e-12)
    high = max(1, int(math.ceil(risk_budget_rs / gross_per_share)) + 1)
    low = 1
    while low < high:
        middle = (low + high + 1) // 2
        if stop_loss(middle) <= risk_budget_rs + 1e-9:
            low = middle
        else:
            high = middle - 1
    return low, stop, stop_loss(low)


def strict_risk_ledger(trades: pd.DataFrame) -> pd.DataFrame:
    audit_columns = (
        "v12_parity_quantity", "v12_parity_net_pnl_rs",
        "strict_stop_price", "strict_all_in_stop_loss_rs",
        "strict_next_quantity_stop_loss_rs", "strict_notional_rs",
        "strict_gross_risk_rs", "strict_net_r",
        "strict_risk_utilization_pct", "strict_below_50000_min_notional",
        "strict_risk_budget_rs", "strict_sizing_rejected",
        "strict_sizing_reject_reason",
    )
    if trades.empty:
        empty = trades.copy()
        for column in audit_columns:
            if column not in empty:
                empty[column] = pd.Series(dtype=(
                    bool if column in {
                        "strict_below_50000_min_notional",
                        "strict_sizing_rejected",
                    } else object
                ))
        return empty
    rows: list[dict[str, Any]] = []
    for _, row in trades.iterrows():
        quantity, stop_price, stop_loss = strict_risk_quantity(
            float(row["entry_price"]), float(row["sl_pct"])
        )
        if quantity <= 0:
            record = row.to_dict()
            record.update({
                "v12_parity_quantity": int(row["quantity"]),
                "v12_parity_net_pnl_rs": float(row["net_pnl_rs"]),
                "quantity": 0,
                "gross_pnl_rs": 0.0,
                "cost_rs": 0.0,
                "net_pnl_rs": 0.0,
                "strict_stop_price": stop_price,
                "strict_all_in_stop_loss_rs": stop_loss,
                "strict_next_quantity_stop_loss_rs": stop_loss,
                "strict_notional_rs": 0.0,
                "gross_risk_rs": 0.0,
                "net_r": 0.0,
                "strict_gross_risk_rs": 0.0,
                "strict_net_r": 0.0,
                "strict_risk_utilization_pct": 0.0,
                "strict_below_50000_min_notional": True,
                "strict_risk_budget_rs": RISK_BUDGET_RS,
                "strict_sizing_rejected": True,
                "strict_sizing_reject_reason": "one_share_exceeds_all_in_risk_budget",
            })
            rows.append(record)
            continue
        costs = nse.intraday_equity_costs(
            float(row["entry_price"]), float(row["exit_price"]), quantity, "LONG"
        )
        gross = v12._price_pnl_rs(
            "LONG", float(row["entry_price"]), float(row["exit_price"]), quantity
        )
        next_stop_costs = nse.intraday_equity_costs(
            float(row["entry_price"]), stop_price, quantity + 1, "LONG"
        )
        next_stop_loss = float(-next_stop_costs.net_pnl)
        if stop_loss > RISK_BUDGET_RS + 1e-9:
            raise RuntimeError("strict risk quantity exceeded budget")
        if next_stop_loss <= RISK_BUDGET_RS + 1e-9:
            raise RuntimeError("strict risk quantity was not maximal")
        gross_risk = (
            float(row["entry_price"]) * float(row["sl_pct"]) / 100.0 * quantity
        )
        notional = float(row["entry_price"] * quantity)
        record = row.to_dict()
        record.update({
            "v12_parity_quantity": int(row["quantity"]),
            "v12_parity_net_pnl_rs": float(row["net_pnl_rs"]),
            "quantity": int(quantity),
            "gross_pnl_rs": float(gross),
            "cost_rs": float(costs.total_cost),
            "net_pnl_rs": float(costs.net_pnl),
            "strict_stop_price": stop_price,
            "strict_all_in_stop_loss_rs": stop_loss,
            "strict_next_quantity_stop_loss_rs": next_stop_loss,
            "strict_notional_rs": notional,
            "gross_risk_rs": gross_risk,
            "net_r": float(costs.net_pnl / gross_risk),
            "strict_gross_risk_rs": gross_risk,
            "strict_net_r": float(costs.net_pnl / gross_risk),
            "strict_risk_utilization_pct": float(
                stop_loss / RISK_BUDGET_RS * 100.0
            ),
            "strict_below_50000_min_notional": bool(notional < 50_000.0),
            "strict_risk_budget_rs": RISK_BUDGET_RS,
            "strict_sizing_rejected": False,
            "strict_sizing_reject_reason": "",
        })
        rows.append(record)
    return pd.DataFrame(rows)


def parity_risk_audit(trades: pd.DataFrame) -> dict[str, Any]:
    stop_losses = []
    gross_risks = []
    notionals = []
    expected_quantity_mismatches = 0
    for row in trades.itertuples():
        entry = float(row.entry_price)
        stop = entry * (1.0 - float(row.sl_pct) / 100.0)
        costs = nse.intraday_equity_costs(entry, stop, int(row.quantity), "LONG")
        stop_losses.append(float(-costs.net_pnl))
        gross_risks.append(entry * float(row.sl_pct) / 100.0 * int(row.quantity))
        notionals.append(float(entry * int(row.quantity)))
        signal_entry = float(row.v7_signal_entry_price)
        signal_stop = round(
            signal_entry * (1.0 - float(row.sl_pct) / 100.0), 2
        )
        expected_quantity = int(v12._risk_based_qty(signal_entry, signal_stop))
        expected_quantity_mismatches += int(expected_quantity != int(row.quantity))
    return {
        "trades": len(trades),
        "risk_budget_rs": RISK_BUDGET_RS,
        "risk_budget_violations": int(sum(value > RISK_BUDGET_RS + 1e-9 for value in stop_losses)),
        "median_all_in_stop_loss_rs": float(np.median(stop_losses)) if stop_losses else 0.0,
        "maximum_all_in_stop_loss_rs": max(stop_losses, default=0.0),
        "median_gross_stop_risk_rs": float(np.median(gross_risks)) if gross_risks else 0.0,
        "maximum_gross_stop_risk_rs": max(gross_risks, default=0.0),
        "minimum_notional_rs": min(notionals, default=0.0),
        "median_notional_rs": float(np.median(notionals)) if notionals else 0.0,
        "trades_below_50000_notional": int(sum(value < 50_000.0 for value in notionals)),
        "expected_v12_quantity_mismatches": expected_quantity_mismatches,
    }


def replay_gate(
    trades: pd.DataFrame,
    replay_days: Sequence[str],
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    first_days = [day for day in replay_days if day <= REPLAY_MIDPOINT]
    second_days = [day for day in replay_days if day > REPLAY_MIDPOINT]
    first = trades.loc[trades["trade_date"].le(REPLAY_MIDPOINT)].copy()
    second = trades.loc[trades["trade_date"].gt(REPLAY_MIDPOINT)].copy()
    overall_metrics = performance(trades, replay_days)
    first_metrics = performance(first, first_days)
    second_metrics = performance(second, second_days)
    active = overall_metrics["sessions"] - overall_metrics["zero_trade_sessions"]
    concentration = concentration_metrics(trades)
    checks = {
        "minimum_30_trades": overall_metrics["trades"] >= 30,
        "minimum_15_active_days": active >= 15,
        "net_positive": overall_metrics["net_pnl_rs"] > 0,
        "pf_at_least_1_20": overall_metrics["profit_factor"] >= 1.20,
        "first_half_net_positive": first_metrics["net_pnl_rs"] > 0,
        "first_half_pf_at_least_1_05": first_metrics["profit_factor"] >= 1.05,
        "second_half_net_positive": second_metrics["net_pnl_rs"] > 0,
        "second_half_pf_at_least_1_05": second_metrics["profit_factor"] >= 1.05,
        "largest_positive_day_pnl_share_at_most_0_40": concentration[
            "largest_positive_day_pnl_share"
        ] <= 0.40,
    }
    if diagnostics is not None:
        checks["stage_a_confidence_ordering"] = bool(
            float(diagnostics.get("top_quartile_rate", float("nan")))
            > float(diagnostics.get("bottom_quartile_rate", float("nan")))
        )
        checks["stage_b_enter_auc_at_least_0_55"] = bool(
            float(diagnostics.get("stage_b_enter_auc", float("nan"))) >= 0.55
        )
        checks["stage_b_confidence_ordering"] = bool(
            float(diagnostics.get("stage_b_top_quartile_enter_rate", float("nan")))
            > float(diagnostics.get("stage_b_bottom_quartile_enter_rate", float("nan")))
        )
    return {
        "overall": overall_metrics,
        "first_half": first_metrics,
        "second_half": second_metrics,
        "active_trade_days": active,
        **concentration,
        "checks": checks,
        "passed": bool(all(checks.values())),
    }


def model_coefficients(model: Any, features: Sequence[str], stage: str) -> pd.DataFrame:
    logistic = model.named_steps["logisticregression"]
    scaler = model.named_steps["standardscaler"]
    rows: list[dict[str, Any]] = []
    coefficients = np.atleast_2d(logistic.coef_)
    classes: Iterable[Any]
    if coefficients.shape[0] == 1:
        classes = [logistic.classes_[-1]]
    else:
        classes = logistic.classes_
    for class_name, class_coefficients, intercept in zip(
        classes, coefficients, np.atleast_1d(logistic.intercept_)
    ):
        for feature, coefficient, mean, scale in zip(
            features, class_coefficients, scaler.mean_, scaler.scale_
        ):
            rows.append({
                "stage": stage,
                "class": str(class_name),
                "feature": feature,
                "standardized_coefficient": float(coefficient),
                "training_mean": float(mean),
                "training_scale": float(scale),
                "intercept": float(intercept),
            })
    return pd.DataFrame(rows)


def write_config(path: Path, config: Config) -> None:
    content = f'''"""Frozen research-only two-stage V12 long configuration."""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
SETUP = {SETUP!r}

DEVELOPMENT_WINDOW = ({START_DATE!r}, {DEVELOPMENT_END!r}, 78)
DIAGNOSTIC_REPLAY_WINDOW = ({REPLAY_START!r}, {REPLAY_END!r}, 41)
REPLAY_USED_FOR_SELECTION = False
REPLAY_IS_FRESH_HOLDOUT = False

BASE_CONDITIONS = (
    ("primary_side", "==", "LONG"),
    ("selection_rank", "between_inclusive", (200, 300)),
    ("signal_minute", "between_inclusive", (570, 855)),
    ("atr_pct", ">=", 1.05),
    ("range_pct", ">=", 1.25),
    ("vwap_dist_atr", ">=", 0.05),
)
FEATURE_FAMILY = {config.feature_family!r}
STAGE_A_FEATURES = {config.stage_a_features!r}
STAGE_B_FEATURES = {config.stage_b_features!r}
MODEL_ARTIFACT = "two_stage_long_v5_models.joblib"
MODEL = {{
    "family": "standardised_L2_logistic",
    "C": 0.10,
    "ticker_day_total_sample_weight": 1.0,
    "stage_a_effective_class_balancing": True,
    "stage_b_class_balancing": False,
    "minimum_stage_a_effective_units_per_class": 15,
    "stage_b_model_classes": ("ENTER", "DEFER"),
    "minimum_stage_b_effective_units_per_class": 15,
}}
EXIT = {{"sl_pct": {config.sl_pct!r}, "target_pct": {config.tgt_pct!r}}}
LABELS = {{
    "stage_a_opportunity": "any reachable state hits target before stop with positive exact net",
    "stage_b_enter_now": "current exact V12-parity net P&L is positive",
    "stage_b_defer": "current exact V12-parity net P&L is nonpositive",
    "different_stage_estimands_are_intentional": True,
}}
ROLLING_GATE = {{
    "prior_scheduled_sessions": {ROLLING_SCORE_SESSIONS},
    "unique_ticker_day_peak_fraction": {config.rolling_fraction!r},
    "first_crossing_only": True,
    "minimum_reference_unique_ticker_days": 30,
    "minimum_reference_active_sessions": 15,
    "minimum_tail_units": 10,
}}
STATE_MACHINE = {{
    "actions": ("ENTER", "WAIT", "REJECT"),
    "model_classes": ("ENTER", "DEFER"),
    "defer_semantics": "NOT_ENTER_NOW; re-evaluate at next contiguous state",
    "policy_action_rule": "ENTER iff P(ENTER) >= 0.40; argmax is diagnostic only",
    "defer_transition": "WAIT one contiguous state; terminal DEFER becomes REJECT",
    "learned_early_reject": False,
    "reject_sources": ("terminal_defer", "missing_state", "daily_cap"),
    "state_offsets_minutes": (0, 5, 10),
    "enter_probability_minimum": {STAGE_B_ENTER_PROBABILITY!r},
    "maximum_waits": 2,
    "latest_waited_signal_minute": 865,
    "waited_state_requires_active_long_rank_membership": True,
    "reachable_states_must_be_contiguous_prefix": True,
    "no_oracle_future_state_comparison_in_stage_b_label": True,
}}
PORTFOLIO = {{"one_entry_per_ticker_day": True, "daily_cap": {DAILY_CAP}}}
PRIMARY_LEDGER = "V12_PARITY"
DIAGNOSTIC_LEDGER = "STRICT_ALL_IN_RISK_500"
STRICT_RISK_BUDGET_RS = {RISK_BUDGET_RS!r}
V12_RISK_CONTRACT = {EXPECTED_V12_RISK_CONTRACT!r}
SELECTION = {{
    "configurations_predeclared": 8,
    "nested_outer_folds": {OUTER_FOLDS!r},
    "no_eligible_inner_fallback": "same frozen robustness order, diagnostic-only",
}}
'''
    path.write_text(content, encoding="utf-8")


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    winner = summary["final_development_selection"]
    replay = summary["diagnostic_replay"]["v12_parity"]
    strict = summary["diagnostic_replay"]["strict_all_in_risk_500"]
    outer = summary["nested_outer_validation"]
    verdict = summary["verdict"]
    text = f"""# V12 two-stage long rebuild — nested two-month replay

## Conclusion

Verdict: **{verdict}**. The frozen configuration was `{winner['config_id']}`.

The 41-session June 4–August 3 diagnostic replay produced **{replay['overall']['trades']} trades**, net P&L **₹{replay['overall']['net_pnl_rs']:,.2f}**, PF **{replay['overall']['profit_factor']:.3f}**, win rate **{replay['overall']['win_rate_pct']:.2f}%**, and max drawdown **₹{replay['overall']['max_drawdown_rs']:,.2f}** under current V12 sizing.

The strict all-in ₹500-risk ledger on the identical entries produced net P&L **₹{strict['overall']['net_pnl_rs']:,.2f}**, PF **{strict['overall']['profit_factor']:.3f}**, and max drawdown **₹{strict['overall']['max_drawdown_rs']:,.2f}**.

## Development evidence

The nested outer process produced {outer['metrics']['trades']} trades, net P&L ₹{outer['metrics']['net_pnl_rs']:,.2f}, PF {outer['metrics']['profit_factor']:.3f}, with {outer['positive_folds']}/3 profitable outer folds. Its complete promotion gate passed: **{outer['passed']}**.

The replay did not choose the feature family, exit, rolling fraction, models, or thresholds. This date range has already been examined in earlier experiments, so it is a diagnostic replay—not a fresh holdout.

## Frozen mechanics

- Hourly prefilter must mark the ticker LONG, rank 200–300 inclusive.
- Completed five-minute base bar: 09:30–14:15, ATR% ≥ 1.05, range% ≥ 1.25, VWAP-distance/ATR ≥ 0.05.
- Stage A opens only the first rolling-threshold ticket per ticker/day.
- Stage B may enter now, wait one five-minute bar, wait once more, or reject. A 14:15 base ticket can therefore act on completed states through 14:25; it may not skip an unavailable intermediate state.
- Exact next-one-minute entry, exact one-minute stop/target/EOD exit, statutory costs, one ticker/day, and chronological cap 15 are retained.
- `PRODUCTION_APPROVED=False`; no live configuration was enabled or restarted.
"""
    path.write_text(text, encoding="utf-8")


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    runtime_risk_contract = v12_risk_contract()
    calendar, development_days, replay_days = session_split()
    tickets, states, nodes, funnel = load_ticket_states()
    development_states = states.loc[
        states["trade_date"].isin(development_days)
    ].copy()
    development_nodes = nodes.loc[
        nodes["trade_date"].isin(development_days)
    ].copy()
    (
        development_states, development_raw, development_outcomes,
        development_rejects, development_prewarm,
    ) = build_exact_paths(development_states, development_nodes)
    development_rejects["resolution_phase"] = "DEVELOPMENT"

    datasets: dict[tuple[float, float], tuple[pd.DataFrame, pd.DataFrame]] = {}
    for sl_pct, tgt_pct in EXIT_PAIRS:
        datasets[(sl_pct, tgt_pct)] = make_exit_dataset(
            tickets,
            development_states,
            development_raw,
            development_outcomes,
            sl_pct=sl_pct,
            tgt_pct=tgt_pct,
        )

    (
        outer_results, outer_trades, nested_aggregate, nested_folds,
        outer_stage_a_diagnostics, outer_stage_b_diagnostics,
        outer_thresholds, outer_decisions,
    ) = run_outer_validation(datasets, development_days, calendar)
    outer_evidence = outer_gate(
        outer_results, outer_trades, development_days,
        outer_stage_a_diagnostics, outer_stage_b_diagnostics,
    )

    final_winner, final_aggregate, final_folds, final_inner_eligible = inner_select(
        datasets, development_days, calendar, context="FINAL_DEVELOPMENT"
    )

    # Persist the complete selection result before resolving or modelling any
    # diagnostic-replay path.  This file is never rewritten after replay opens.
    development_freeze_path = OUTPUT_DIR / "DEVELOPMENT_FREEZE.json"
    development_freeze = {
        "frozen_before_replay_resolution": True,
        "replay_outcomes_resolved_at_freeze_time": False,
        "development_window": [START_DATE, DEVELOPMENT_END, len(development_days)],
        "configuration": json_safe(final_winner.__dict__),
        "inner_evidence_eligible": final_inner_eligible,
        "nested_outer_validation": outer_evidence,
        "final_inner_aggregate": final_aggregate.to_dict("records"),
        "runtime_v12_risk_contract": runtime_risk_contract,
    }
    development_freeze_path.write_text(
        json.dumps(json_safe(development_freeze), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    replay_states_source = states.loc[states["trade_date"].isin(replay_days)].copy()
    replay_nodes = nodes.loc[nodes["trade_date"].isin(replay_days)].copy()
    (
        replay_states_source, replay_raw, replay_outcomes,
        replay_rejects, replay_prewarm,
    ) = build_exact_paths(
        replay_states_source,
        replay_nodes,
        exit_pairs=((final_winner.sl_pct, final_winner.tgt_pct),),
    )
    replay_rejects["resolution_phase"] = "DIAGNOSTIC_REPLAY"
    replay_ticket_frame, replay_state_frame = make_exit_dataset(
        tickets,
        replay_states_source,
        replay_raw,
        replay_outcomes,
        sl_pct=final_winner.sl_pct,
        tgt_pct=final_winner.tgt_pct,
    )
    development_ticket_frame, development_state_frame = datasets[
        (final_winner.sl_pct, final_winner.tgt_pct)
    ]
    final_tickets = pd.concat(
        [development_ticket_frame, replay_ticket_frame], ignore_index=True
    )
    final_states = pd.concat(
        [development_state_frame, replay_state_frame], ignore_index=True
    )
    raw = pd.concat([development_raw, replay_raw], ignore_index=True)
    outcomes = pd.concat(
        [development_outcomes, replay_outcomes], ignore_index=True
    )
    rejects = pd.concat(
        [development_rejects, replay_rejects], ignore_index=True
    )
    prewarm = {
        "development": development_prewarm,
        "diagnostic_replay": replay_prewarm,
    }
    funnel = pd.concat([
        funnel,
        pd.DataFrame([
            {"stage": "development_v12_executable_nodes", "rows": len(development_raw)},
            {"stage": "development_exact_exit_rows", "rows": len(development_outcomes)},
            {"stage": "replay_v12_executable_nodes", "rows": len(replay_raw)},
            {"stage": "replay_exact_exit_rows_frozen_pair_only", "rows": len(replay_outcomes)},
            {"stage": "entry_engine_rejects_all", "rows": len(rejects)},
        ]),
    ], ignore_index=True)
    replay_result = evaluate_config(
        final_tickets,
        final_states,
        development_days,
        replay_days,
        calendar,
        final_winner,
        return_models=True,
    )
    replay_trades = replay_result.pop("trades_frame")
    replay_thresholds = replay_result.pop("thresholds_frame")
    replay_decisions = replay_result.pop("decisions_frame")
    replay_stage_a_diagnostics = replay_result.pop("stage_a_diagnostics_frame")
    replay_stage_b_diagnostics = replay_result.pop("stage_b_diagnostics_frame")
    replay_stage_b_all_diagnostics = replay_result.pop(
        "stage_b_all_states_diagnostics_frame"
    )
    stage_a_model = replay_result.pop("stage_a_model")
    stage_b_model = replay_result.pop("stage_b_model")

    parity_gate = replay_gate(replay_trades, replay_days, replay_result)
    strict_ledger = strict_risk_ledger(replay_trades)
    if "strict_sizing_rejected" not in strict_ledger:
        strict_ledger["strict_sizing_rejected"] = pd.Series(dtype=bool)
    strict_trades = strict_ledger.loc[
        ~strict_ledger["strict_sizing_rejected"].astype(bool)
    ].copy()
    strict_gate = replay_gate(strict_trades, replay_days, replay_result)
    strict_rejections = int(strict_ledger["strict_sizing_rejected"].sum())
    strict_gate["selected_entries"] = len(strict_ledger)
    strict_gate["executed_entries"] = len(strict_trades)
    strict_gate["sizing_rejections"] = strict_rejections
    strict_gate["checks"]["no_strict_sizing_rejections"] = strict_rejections == 0
    strict_gate["passed"] = bool(all(strict_gate["checks"].values()))
    risk_audit = parity_risk_audit(replay_trades)
    if risk_audit["expected_v12_quantity_mismatches"] != 0:
        raise RuntimeError(
            "V12 parity quantity invariant failed on diagnostic replay: "
            f"{risk_audit['expected_v12_quantity_mismatches']} mismatches"
        )
    outer_strict_ledger = strict_risk_ledger(outer_trades)
    if "strict_sizing_rejected" not in outer_strict_ledger:
        outer_strict_ledger["strict_sizing_rejected"] = pd.Series(dtype=bool)
    outer_strict_trades = outer_strict_ledger.loc[
        ~outer_strict_ledger["strict_sizing_rejected"].astype(bool)
    ].copy()
    outer_strict_metrics = performance(
        outer_strict_trades, development_days[48:78]
    )
    performance_candidate = bool(
        outer_evidence["passed"]
        and final_inner_eligible
        and parity_gate["passed"]
    )
    production_candidate = bool(
        performance_candidate
        and risk_audit["risk_budget_violations"] == 0
    )
    if production_candidate:
        verdict = "RESEARCH_CANDIDATE_REQUIRES_GENUINELY_FRESH_HOLDOUT"
    elif performance_candidate:
        verdict = "PERFORMANCE_CANDIDATE_BLOCKED_BY_CURRENT_V12_RISK_CONTRACT"
    else:
        verdict = "REJECTED_NO_ROBUST_PROFITABLE_EDGE"

    coefficients = pd.concat([
        model_coefficients(stage_a_model, final_winner.stage_a_features, "STAGE_A"),
        model_coefficients(stage_b_model, final_winner.stage_b_features, "STAGE_B"),
    ], ignore_index=True)
    nested_aggregate = pd.concat(
        [nested_aggregate, final_aggregate], ignore_index=True
    )
    nested_folds = pd.concat([nested_folds, final_folds], ignore_index=True)
    daily_parity = replay_helpers.daily_summary(replay_trades, replay_days)
    daily_strict = replay_helpers.daily_summary(strict_trades, replay_days)
    hourly_parity = replay_helpers.hourly_summary(replay_trades)

    summary = {
        "production_approved": False,
        "research_only": True,
        "verdict": verdict,
        "selection_contract": {
            "development_window": [START_DATE, DEVELOPMENT_END, len(development_days)],
            "diagnostic_replay_window": [REPLAY_START, REPLAY_END, len(replay_days)],
            "replay_used_for_selection": False,
            "replay_is_fresh_holdout": False,
            "configurations_predeclared": len(configurations()),
            "feature_families": {
                key: list(value) for key, value in FEATURE_FAMILIES.items()
            },
            "exit_pairs": EXIT_PAIRS,
            "rolling_fractions": ROLLING_FRACTIONS,
            "outer_folds": OUTER_FOLDS,
            "stage_b_actions": ["ENTER", "WAIT", "REJECT"],
            "stage_b_model_classes": ["ENTER", "DEFER"],
            "stage_b_defer_semantics": (
                "NOT_ENTER_NOW; re-evaluate at next contiguous state"
            ),
            "stage_b_policy_action_rule": (
                "ENTER iff P(ENTER) >= 0.40; argmax is diagnostic only"
            ),
            "stage_b_defer_transition": (
                "WAIT one contiguous state; terminal DEFER becomes REJECT"
            ),
            "stage_b_learned_early_reject": False,
            "wait_offsets_minutes": [0, 5, 10],
            "primary_ledger": "V12_PARITY",
            "diagnostic_ledger": "STRICT_ALL_IN_RISK_500",
            "runtime_v12_risk_contract": runtime_risk_contract,
            "development_freeze_artifact": str(development_freeze_path.resolve()),
        },
        "candidate_counts": {
            "base_ticket_rows": len(tickets),
            "unique_ticker_days": int(
                tickets[["trade_date", "ticker"]].drop_duplicates().shape[0]
            ),
            "unique_state_nodes": len(nodes),
            "v12_executable_state_nodes": len(raw),
            "exact_outcome_rows": len(outcomes),
        },
        "prewarm_1m": prewarm,
        "nested_outer_validation": outer_evidence,
        "nested_outer_strict_risk500_diagnostic": {
            "metrics": outer_strict_metrics,
            "selected_entries": len(outer_strict_ledger),
            "executed_entries": len(outer_strict_trades),
            "sizing_rejections": int(
                outer_strict_ledger["strict_sizing_rejected"].sum()
            ),
        },
        "outer_fold_results": outer_results.to_dict("records"),
        "final_development_selection": {
            "config_id": final_winner.config_id,
            "feature_family": final_winner.feature_family,
            "sl_pct": final_winner.sl_pct,
            "tgt_pct": final_winner.tgt_pct,
            "rolling_fraction": final_winner.rolling_fraction,
            "inner_evidence_eligible": final_inner_eligible,
        },
        "diagnostic_replay": {
            "v12_parity": parity_gate,
            "strict_all_in_risk_500": strict_gate,
            "v12_parity_risk_audit": risk_audit,
            "model_diagnostics": _metric_record(replay_result),
        },
        "promotion_candidate_before_fresh_holdout": production_candidate,
        "performance_candidate_before_risk_and_fresh_holdout": performance_candidate,
        "no_production_mutation": True,
    }

    outputs: dict[str, Path] = {
        "candidate_funnel.csv": OUTPUT_DIR / "candidate_funnel.csv",
        "entry_engine_rejects.csv": OUTPUT_DIR / "entry_engine_rejects.csv",
        "nested_inner_aggregate.csv": OUTPUT_DIR / "nested_inner_aggregate.csv",
        "nested_inner_folds.csv": OUTPUT_DIR / "nested_inner_folds.csv",
        "outer_fold_results.csv": OUTPUT_DIR / "outer_fold_results.csv",
        "outer_selected_trades.csv": OUTPUT_DIR / "outer_selected_trades.csv",
        "outer_selected_trades_strict_risk500.csv": OUTPUT_DIR / "outer_selected_trades_strict_risk500.csv",
        "outer_rolling_thresholds.csv": OUTPUT_DIR / "outer_rolling_thresholds.csv",
        "outer_ticket_decisions.csv": OUTPUT_DIR / "outer_ticket_decisions.csv",
        "outer_stage_a_diagnostics.csv": OUTPUT_DIR / "outer_stage_a_diagnostics.csv",
        "outer_stage_b_diagnostics.csv": OUTPUT_DIR / "outer_stage_b_diagnostics.csv",
        "replay_trades_v12_parity.csv": OUTPUT_DIR / "replay_trades_v12_parity.csv",
        "replay_trades_strict_risk500.csv": OUTPUT_DIR / "replay_trades_strict_risk500.csv",
        "replay_daily_v12_parity.csv": OUTPUT_DIR / "replay_daily_v12_parity.csv",
        "replay_daily_strict_risk500.csv": OUTPUT_DIR / "replay_daily_strict_risk500.csv",
        "replay_hourly_v12_parity.csv": OUTPUT_DIR / "replay_hourly_v12_parity.csv",
        "replay_rolling_thresholds.csv": OUTPUT_DIR / "replay_rolling_thresholds.csv",
        "replay_ticket_decisions.csv": OUTPUT_DIR / "replay_ticket_decisions.csv",
        "replay_stage_a_diagnostics.csv": OUTPUT_DIR / "replay_stage_a_diagnostics.csv",
        "replay_stage_b_diagnostics.csv": OUTPUT_DIR / "replay_stage_b_diagnostics.csv",
        "model_coefficients.csv": OUTPUT_DIR / "model_coefficients.csv",
    }
    frames = {
        "candidate_funnel.csv": funnel,
        "entry_engine_rejects.csv": rejects,
        "nested_inner_aggregate.csv": nested_aggregate,
        "nested_inner_folds.csv": nested_folds,
        "outer_fold_results.csv": outer_results,
        "outer_selected_trades.csv": outer_trades,
        "outer_selected_trades_strict_risk500.csv": outer_strict_ledger,
        "outer_rolling_thresholds.csv": outer_thresholds,
        "outer_ticket_decisions.csv": outer_decisions,
        "outer_stage_a_diagnostics.csv": outer_stage_a_diagnostics,
        "outer_stage_b_diagnostics.csv": outer_stage_b_diagnostics,
        "replay_trades_v12_parity.csv": replay_trades,
        "replay_trades_strict_risk500.csv": strict_ledger,
        "replay_daily_v12_parity.csv": daily_parity,
        "replay_daily_strict_risk500.csv": daily_strict,
        "replay_hourly_v12_parity.csv": hourly_parity,
        "replay_rolling_thresholds.csv": replay_thresholds,
        "replay_ticket_decisions.csv": replay_decisions,
        "replay_stage_a_diagnostics.csv": replay_stage_a_diagnostics,
        "replay_stage_b_diagnostics.csv": replay_stage_b_diagnostics,
        "model_coefficients.csv": coefficients,
    }
    for name, frame in frames.items():
        frame.to_csv(outputs[name], index=False)

    config_path = OUTPUT_DIR / "two_stage_long_v5_conf.py"
    model_path = OUTPUT_DIR / "two_stage_long_v5_models.joblib"
    summary_path = OUTPUT_DIR / "summary.json"
    report_path = OUTPUT_DIR / "RESEARCH_REPORT.md"
    write_config(config_path, final_winner)
    joblib.dump(
        {
            "stage_a": stage_a_model,
            "stage_b": stage_b_model,
            "config": json_safe(final_winner.__dict__),
        },
        model_path,
    )
    summary_path.write_text(
        json.dumps(json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    write_report(report_path, summary)

    artifact_paths = list(outputs.values()) + [
        config_path, model_path, summary_path, report_path,
        development_freeze_path,
    ]
    manifest = {
        "production_approved": False,
        "artifacts": [
            {
                "path": str(path.resolve()),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
            for path in sorted(artifact_paths, key=lambda item: item.name)
        ],
        "source_inputs": {
            str(v2.SOURCE.resolve()): sha256(v2.SOURCE),
            str(v2.SESSION_SOURCE.resolve()): sha256(v2.SESSION_SOURCE),
        },
    }
    (OUTPUT_DIR / "integrity_manifest.json").write_text(
        json.dumps(json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )
    print(json.dumps(json_safe({
        "output_dir": str(OUTPUT_DIR),
        "verdict": verdict,
        "winner": final_winner.__dict__,
        "outer": outer_evidence,
        "replay_v12_parity": parity_gate,
        "replay_strict_risk500": strict_gate,
    }), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
