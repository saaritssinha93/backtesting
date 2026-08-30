"""Causal, research-only runtime adapters for the staged FNO V11 replay.

The adapters in this module are deliberately process-local context managers.
They layer one predeclared entry, exit, picker, or portfolio hypothesis over
the frozen V10 state machine and restore every patched engine seam afterward.
No V10 source file is modified and no adapter has live-trading authority.
"""

from __future__ import annotations

import contextlib
import math
from dataclasses import dataclass
from typing import Any, Iterator

import numpy as np
import pandas as pd

import fno_v8_windowed_1m_entry_backtest as engine


RUNTIME_SCHEMA_VERSION = "fno_v11_causal_runtime_hooks_v1"


@dataclass(frozen=True)
class RuntimeSpec:
    """One isolated runtime mechanism layered over the frozen V10 control."""

    entry_setup_id: str | None = None
    entry_not_before_minute: int | None = None
    exit_rule: str | None = None
    exit_activation_r: float | None = None
    same_side_symbol_limit: int = 1

    @property
    def active_mechanisms(self) -> tuple[str, ...]:
        active: list[str] = []
        if self.entry_setup_id is not None:
            active.append("ENTRY_NOT_BEFORE")
        if self.exit_rule is not None:
            active.append("EXIT_RULE")
        if self.same_side_symbol_limit != 1:
            active.append("PORTFOLIO_SYMBOL_LIMIT")
        return tuple(active)

    @property
    def is_neutral(self) -> bool:
        return not self.active_mechanisms

    def validate(self, *, allow_composite: bool = False) -> None:
        if (self.entry_setup_id is None) != (self.entry_not_before_minute is None):
            raise ValueError("entry setup and earliest minute must be specified together")
        if self.entry_not_before_minute is not None:
            if isinstance(self.entry_not_before_minute, bool) or not isinstance(
                self.entry_not_before_minute, int
            ):
                raise ValueError("entry_not_before_minute must be an integer")
            if not 2 <= self.entry_not_before_minute <= 5:
                raise ValueError("entry_not_before_minute must be in [2, 5]")
        allowed_exit_rules = {
            None,
            "BREAK_EVEN_NEXT_BAR",
            "LATE_1430_BREAK_EVEN_NEXT_BAR",
            "TRAIL_1R_AFTER_2R_NEXT_BAR",
        }
        if self.exit_rule not in allowed_exit_rules:
            raise ValueError(f"unsupported V11 exit rule: {self.exit_rule!r}")
        if self.exit_rule is None:
            if self.exit_activation_r is not None:
                raise ValueError("neutral exit rule cannot have an R threshold")
        else:
            if self.exit_activation_r is None or not math.isfinite(
                float(self.exit_activation_r)
            ):
                raise ValueError("exit rule requires a finite R threshold")
            if float(self.exit_activation_r) <= 0:
                raise ValueError("exit R threshold must be positive")
        if isinstance(self.same_side_symbol_limit, bool) or not isinstance(
            self.same_side_symbol_limit, int
        ):
            raise ValueError("same-side symbol limit must be an integer")
        if self.same_side_symbol_limit not in {1, 2}:
            raise ValueError("V11 supports same-side symbol limits 1 or 2 only")
        if len(self.active_mechanisms) > 1:
            allowed_post_hoc_composite = {
                "ENTRY_NOT_BEFORE",
                "PORTFOLIO_SYMBOL_LIMIT",
            }
            if not allow_composite or set(self.active_mechanisms) != allowed_post_hoc_composite:
                raise ValueError(
                    "isolated V11 runtime specs may change one mechanism only; "
                    "the only permitted post-hoc composite is entry timing plus "
                    "the same-side symbol limit"
                )


def _dynamic_stop_is_better(side: str, proposed: float, current: float) -> bool:
    return proposed > current + 1e-12 if side == "LONG" else proposed < current - 1e-12


def _apply_pending_dynamic_stop(
    setup: engine.V8Setup,
    runtime: engine._CandidateRuntime,
    event_ts: pd.Timestamp,
) -> None:
    pending = getattr(runtime, "_v11_pending_stop", None)
    if pending is None or runtime.stop_price is None:
        return
    proposed = float(pending)
    current = float(runtime.stop_price)
    if _dynamic_stop_is_better(setup.side, proposed, current):
        runtime.stop_price = proposed
        runtime._v11_dynamic_stop_active = True
        runtime._v11_dynamic_stop_reason = str(
            getattr(runtime, "_v11_pending_stop_reason", "DYNAMIC")
        )
        runtime._v11_dynamic_stop_activation_count = int(
            getattr(runtime, "_v11_dynamic_stop_activation_count", 0)
        ) + 1
        runtime._v11_dynamic_stop_activated_at = event_ts
    runtime._v11_pending_stop = None
    runtime._v11_pending_stop_reason = None


def _schedule_dynamic_stop(
    setup: engine.V8Setup,
    runtime: engine._CandidateRuntime,
    proposed: float,
    reason: str,
) -> None:
    current = float(runtime.stop_price) if runtime.stop_price is not None else math.nan
    pending = getattr(runtime, "_v11_pending_stop", None)
    comparison = current
    if pending is not None and _dynamic_stop_is_better(
        setup.side, float(pending), current
    ):
        comparison = float(pending)
    if not math.isfinite(comparison) or _dynamic_stop_is_better(
        setup.side, float(proposed), comparison
    ):
        runtime._v11_pending_stop = float(proposed)
        runtime._v11_pending_stop_reason = reason


def _completed_bar_favorable_r(
    setup: engine.V8Setup,
    runtime: engine._CandidateRuntime,
    bar: engine.MinuteBar,
) -> tuple[float, float]:
    assert runtime.entry_price is not None
    entry = float(runtime.entry_price)
    original_stop = float(getattr(runtime, "_v11_original_stop_price"))
    risk = abs(entry - original_stop)
    if risk <= 0:
        raise AssertionError("V11 dynamic exit encountered non-positive initial risk")
    prior_best = float(getattr(runtime, "_v11_best_favorable_price", entry))
    observed = float(bar.high) if setup.side == "LONG" else float(bar.low)
    best = max(prior_best, observed) if setup.side == "LONG" else min(prior_best, observed)
    runtime._v11_best_favorable_price = best
    favorable_distance = best - entry if setup.side == "LONG" else entry - best
    return favorable_distance / risk, risk


def _label_dynamic_stop_exit(
    runtime: engine._CandidateRuntime,
    result: tuple[str, float] | None,
) -> tuple[str, float] | None:
    if result is None or not bool(getattr(runtime, "_v11_dynamic_stop_active", False)):
        return result
    reason, price = result
    if not str(reason).startswith("STOP"):
        return result
    label = str(getattr(runtime, "_v11_dynamic_stop_reason", "DYNAMIC"))
    suffix = "_GAP" if reason == "STOP_GAP" else ""
    return f"STOP_{label}{suffix}", price


@contextlib.contextmanager
def installed_runtime_hooks(
    spec: RuntimeSpec, *, allow_composite: bool = False
) -> Iterator[None]:
    """Install and later restore one causal V11 runtime hypothesis."""

    spec.validate(allow_composite=allow_composite)
    original_entry_fill = engine._entry_fill
    original_exit_on_bar = engine._exit_on_bar
    original_audit_record = engine._audit_record
    original_picker_value = engine._picker_value
    original_portfolio = engine.apply_global_portfolio_constraints

    def v11_picker_value(
        setup: engine.V8Setup, candidate: engine.CandidateInput
    ) -> float:
        if setup.picker == "min_volume":
            return -float(candidate.volume_ratio)
        return original_picker_value(setup, candidate)

    def v11_entry_fill(
        setup: engine.V8Setup,
        runtime: engine._CandidateRuntime,
        bar: engine.MinuteBar,
        policy: engine.EntryPolicy,
    ) -> tuple[float, bool] | None:
        if setup.setup_id == spec.entry_setup_id:
            relative_minute = engine._relative_minute(runtime.candidate, bar.ts)
            runtime._v11_entry_not_before_minute = spec.entry_not_before_minute
            if relative_minute < int(spec.entry_not_before_minute or 0):
                runtime._v11_early_fill_checks_skipped = int(
                    getattr(runtime, "_v11_early_fill_checks_skipped", 0)
                ) + 1
                neutral_fill = original_entry_fill(setup, runtime, bar, policy)
                if neutral_fill is not None:
                    runtime._v11_early_touch_observed = True
                return None
        return original_entry_fill(setup, runtime, bar, policy)

    def v11_exit_on_bar(
        setup: engine.V8Setup,
        runtime: engine._CandidateRuntime,
        bar: engine.MinuteBar,
        *,
        position_open_at_bar_start: bool = True,
    ) -> tuple[str, float] | None:
        if spec.exit_rule is None:
            return original_exit_on_bar(
                setup,
                runtime,
                bar,
                position_open_at_bar_start=position_open_at_bar_start,
            )

        if not hasattr(runtime, "_v11_original_stop_price"):
            if runtime.stop_price is None or runtime.entry_price is None:
                return original_exit_on_bar(
                    setup,
                    runtime,
                    bar,
                    position_open_at_bar_start=position_open_at_bar_start,
                )
            runtime._v11_original_stop_price = float(runtime.stop_price)
            runtime._v11_best_favorable_price = float(runtime.entry_price)
            runtime._v11_dynamic_stop_active = False
            runtime._v11_dynamic_stop_activation_count = 0

        # A threshold observed on bar t becomes executable only on bar t+1.
        if position_open_at_bar_start:
            _apply_pending_dynamic_stop(setup, runtime, bar.ts)
        result = original_exit_on_bar(
            setup,
            runtime,
            bar,
            position_open_at_bar_start=position_open_at_bar_start,
        )
        result = _label_dynamic_stop_exit(runtime, result)
        if result is not None or not position_open_at_bar_start:
            return result

        favorable_r, risk = _completed_bar_favorable_r(setup, runtime, bar)
        runtime._v11_running_mfe_r = favorable_r
        threshold = float(spec.exit_activation_r or 0.0)
        entry = float(runtime.entry_price)
        if spec.exit_rule == "BREAK_EVEN_NEXT_BAR":
            if favorable_r + 1e-12 >= threshold:
                _schedule_dynamic_stop(
                    setup, runtime, entry, f"BREAK_EVEN_AFTER_{threshold:g}R"
                )
                runtime._v11_dynamic_stop_armed_at = bar.ts
        elif spec.exit_rule == "LATE_1430_BREAK_EVEN_NEXT_BAR":
            cutoff = pd.Timestamp(
                f"{runtime.candidate.session_date.isoformat()} 14:30",
                tz=bar.ts.tz,
            )
            if bar.ts >= cutoff and favorable_r + 1e-12 >= threshold:
                _schedule_dynamic_stop(
                    setup, runtime, entry, f"LATE_1430_BREAK_EVEN_AFTER_{threshold:g}R"
                )
                runtime._v11_dynamic_stop_armed_at = bar.ts
        elif spec.exit_rule == "TRAIL_1R_AFTER_2R_NEXT_BAR":
            if favorable_r + 1e-12 >= threshold:
                best = float(runtime._v11_best_favorable_price)
                proposed = best - risk if setup.side == "LONG" else best + risk
                proposed = (
                    engine.round_down_to_tick(proposed, runtime.candidate.tick_size)
                    if setup.side == "LONG"
                    else engine.round_up_to_tick(proposed, runtime.candidate.tick_size)
                )
                _schedule_dynamic_stop(setup, runtime, proposed, "TRAIL_1R_AFTER_2R")
                runtime._v11_dynamic_stop_armed_at = bar.ts
        return None

    def v11_audit_record(
        setup: engine.V8Setup, runtime: engine._CandidateRuntime
    ) -> dict[str, Any]:
        final_active_stop = runtime.stop_price
        initial_stop = getattr(runtime, "_v11_original_stop_price", final_active_stop)
        if initial_stop is not None:
            runtime.stop_price = float(initial_stop)
        try:
            record = original_audit_record(setup, runtime)
        finally:
            runtime.stop_price = final_active_stop
        record.update(
            {
                "v11_runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                "v11_entry_not_before_minute": getattr(
                    runtime, "_v11_entry_not_before_minute", None
                ),
                "v11_early_fill_checks_skipped": int(
                    getattr(runtime, "_v11_early_fill_checks_skipped", 0)
                ),
                "v11_early_touch_observed": bool(
                    getattr(runtime, "_v11_early_touch_observed", False)
                ),
                "v11_exit_rule": spec.exit_rule,
                "v11_exit_activation_r": spec.exit_activation_r,
                "v11_dynamic_stop_activation_count": int(
                    getattr(runtime, "_v11_dynamic_stop_activation_count", 0)
                ),
                "v11_dynamic_stop_active_at_terminal": bool(
                    getattr(runtime, "_v11_dynamic_stop_active", False)
                ),
                "v11_best_favorable_price": getattr(
                    runtime, "_v11_best_favorable_price", None
                ),
                "v11_running_mfe_r": getattr(runtime, "_v11_running_mfe_r", None),
                "v11_final_active_stop_price": final_active_stop,
                "v11_dynamic_stop_armed_at": getattr(
                    runtime, "_v11_dynamic_stop_armed_at", pd.NaT
                ),
                "v11_dynamic_stop_activated_at": getattr(
                    runtime, "_v11_dynamic_stop_activated_at", pd.NaT
                ),
            }
        )
        return record

    engine._picker_value = v11_picker_value
    engine._entry_fill = v11_entry_fill
    engine._exit_on_bar = v11_exit_on_bar
    engine._audit_record = v11_audit_record
    def portfolio_adapter(
        audit: pd.DataFrame, portfolio_policy: engine.PortfolioPolicy
    ) -> pd.DataFrame:
        unconstrained_v11 = audit.set_index("candidate_id")[
            [column for column in audit.columns if column.startswith("v11_")]
        ].copy()
        constrained = (
            apply_same_side_symbol_limit(
                audit,
                portfolio_policy,
                same_side_limit=2,
            )
            if spec.same_side_symbol_limit == 2
            else original_portfolio(audit, portfolio_policy)
        )
        for column in unconstrained_v11.columns:
            mapped = constrained["candidate_id"].map(unconstrained_v11[column])
            constrained[f"unconstrained_{column}"] = mapped
        rejected = constrained["portfolio_decision"].eq("REJECTED")
        for column in unconstrained_v11.columns:
            if pd.api.types.is_bool_dtype(unconstrained_v11[column].dtype):
                constrained.loc[rejected, column] = False
            elif pd.api.types.is_numeric_dtype(unconstrained_v11[column].dtype):
                constrained.loc[rejected, column] = np.nan
            elif column.endswith("_at"):
                constrained.loc[rejected, column] = pd.NaT
            elif column != "v11_runtime_schema_version":
                constrained.loc[rejected, column] = ""
        return constrained

    engine.apply_global_portfolio_constraints = portfolio_adapter
    try:
        yield
    finally:
        engine._entry_fill = original_entry_fill
        engine._exit_on_bar = original_exit_on_bar
        engine._audit_record = original_audit_record
        engine._picker_value = original_picker_value
        engine.apply_global_portfolio_constraints = original_portfolio


def _portfolio_actions(audit: pd.DataFrame) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    terminal_states = {
        engine.SignalState.POSTCONF_CANCELLED.value,
        engine.SignalState.WINDOW_EXPIRED.value,
        engine.SignalState.STOPPED.value,
        engine.SignalState.TARGETED.value,
        engine.SignalState.SQUARE_OFF.value,
        engine.SignalState.DATA_INCOMPLETE.value,
    }
    for row in audit.to_dict("records"):
        candidate_id = str(row["candidate_id"])
        for sequence, event in enumerate(row.get("events", []) or []):
            before = str(event.get("state_before", ""))
            after = str(event.get("state_after", ""))
            action = None
            phase = 1
            if after == engine.SignalState.PENDING_STOP.value:
                action = "RESERVE"
                phase = 2
            elif (
                before == engine.SignalState.PENDING_STOP.value
                and after in terminal_states
            ) or (
                before == engine.SignalState.FILLED_OPEN.value
                and after in terminal_states
            ):
                action = "RELEASE"
                phase = 0
            if action is None:
                continue
            actions.append(
                {
                    "candidate_id": candidate_id,
                    "event_ts": engine._to_ist_timestamp(event["event_ts"]),
                    "phase": phase,
                    "sequence": sequence,
                    "action": action,
                    "signal_time": engine._to_ist_timestamp(row["signal_time"]),
                    "setup_id": str(row["setup_id"]),
                    "frozen_rank": int(row.get("frozen_rank") or 0),
                    "symbol": str(row["symbol"]),
                    "side": str(row["side"]).upper(),
                }
            )
    actions.sort(
        key=lambda item: (
            item["event_ts"],
            item["phase"],
            item["signal_time"],
            item["setup_id"],
            item["frozen_rank"],
            item["symbol"],
            item["candidate_id"],
            item["sequence"],
        )
    )
    return actions


def _initialize_portfolio_output(audit: pd.DataFrame) -> pd.DataFrame:
    out = audit.copy().reset_index(drop=True)
    out["portfolio_decision"] = "NOT_APPLICABLE"
    out["portfolio_reject_reason"] = ""
    out["portfolio_active_at_reservation"] = np.nan
    out["portfolio_reserved_margin_rs"] = np.nan
    for column in engine._EXCURSION_AMBIGUITY_COLUMNS:
        if column in out.columns:
            out[column] = out[column].astype("boolean")
    out["unconstrained_status"] = out["status"]
    out["unconstrained_net_return_pct"] = out["net_return_pct"]
    out["unconstrained_net_pnl_rs"] = out.get("net_pnl_rs", np.nan)
    for column in (
        "events",
        "confirmation_minute",
        "confirmation_time",
        "entry_minute",
        "entry_delay_minutes",
        "entry_time",
        "trigger",
        "entry_price",
        "gap_fill",
        "intrabar_trigger_fill",
        "ambiguous_entry_bar",
        "stop_price",
        "target_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "exit_at_bar_open",
        "gross_return_pct",
        "quantity",
        "position_notional_rs",
        "gross_pnl_rs",
        "estimated_cost_rs",
        *engine._EXCURSION_VALUE_COLUMNS,
        *engine._EXCURSION_AMBIGUITY_COLUMNS,
        "excursion_observed_bar_count",
        "excursion_complete_bar_count",
    ):
        if column in out.columns:
            out[f"unconstrained_{column}"] = out[column]
    return out


def _apply_portfolio_rejection(
    out: pd.DataFrame,
    index: int,
    row: pd.Series,
    reason: str,
) -> None:
    out.at[index, "portfolio_decision"] = "REJECTED"
    out.at[index, "portfolio_reject_reason"] = reason
    out.at[index, "status"] = (
        engine.SignalState.DUPLICATE_REJECTED.value
        if reason.startswith("DUPLICATE")
        else engine.SignalState.PORTFOLIO_REJECTED.value
    )
    terminal_state = str(out.at[index, "status"])
    terminal_reason = f"{reason}:CONSERVATIVE_NO_BACKFILL"
    out.at[index, "reason"] = terminal_reason
    original_events = list(out.at[index, "events"] or [])
    constrained_events: list[dict[str, Any]] = []
    for event in original_events:
        if str(event.get("state_after", "")) == engine.SignalState.PENDING_STOP.value:
            constrained_events.append(
                {
                    "symbol": str(row["symbol"]),
                    "event_ts": engine._to_ist_timestamp(event["event_ts"]),
                    "state_before": engine.SignalState.CONFIRMED_WAITING_CAP.value,
                    "state_after": terminal_state,
                    "reason": terminal_reason,
                }
            )
            break
        constrained_events.append(event)
    out.at[index, "events"] = constrained_events
    out.at[index, "event_count"] = len(constrained_events)
    out.at[index, "filled"] = False
    for column in (
        "entry_minute",
        "entry_delay_minutes",
        "entry_time",
        "entry_price",
        "gap_fill",
        "intrabar_trigger_fill",
        "ambiguous_entry_bar",
        "stop_price",
        "target_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "exit_at_bar_open",
        "quantity",
        "position_notional_rs",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "gross_return_pct",
        "net_return_pct",
        *engine._EXCURSION_VALUE_COLUMNS,
        *engine._EXCURSION_AMBIGUITY_COLUMNS,
        "excursion_observed_bar_count",
        "excursion_complete_bar_count",
    ):
        if column not in out.columns:
            continue
        if column in {"gap_fill", "intrabar_trigger_fill", "ambiguous_entry_bar"}:
            out.at[index, column] = False
        elif column == "quantity":
            out.at[index, column] = 0
        elif column == "exit_reason":
            out.at[index, column] = ""
        elif column == "exit_at_bar_open":
            out.at[index, column] = False
        elif column in engine._EXCURSION_AMBIGUITY_COLUMNS:
            out.at[index, column] = pd.NA
        else:
            out.at[index, column] = pd.NaT if column.endswith("time") else np.nan


def apply_same_side_symbol_limit(
    audit: pd.DataFrame,
    portfolio_policy: engine.PortfolioPolicy,
    *,
    same_side_limit: int = 2,
) -> pd.DataFrame:
    """Allow at most two same-side reservations per symbol, never opposite sides."""

    portfolio_policy.validate()
    if not portfolio_policy.pending_reserves_margin:
        raise ValueError("V11 supports only pending_reserves_margin=True")
    if not portfolio_policy.one_position_per_symbol:
        raise ValueError("V11 uses the parent one-position flag as policy authority")
    if same_side_limit != 2:
        raise ValueError("this staged V11 adapter supports same_side_limit=2 only")
    if audit.empty:
        return audit.copy()

    out = _initialize_portfolio_output(audit)
    actions = _portfolio_actions(out)
    accepted: set[str] = set()
    rejected: dict[str, str] = {}
    active: dict[str, tuple[str, str]] = {}
    active_by_symbol: dict[str, set[str]] = {}
    max_by_margin = int(
        math.floor(portfolio_policy.capital_rs / portfolio_policy.margin_per_entry_rs)
    )
    capacity = min(portfolio_policy.max_concurrent_positions, max_by_margin)
    reservation_stats: dict[str, tuple[int, float]] = {}

    for action in actions:
        candidate_id = str(action["candidate_id"])
        symbol = str(action["symbol"])
        side = str(action["side"])
        if action["action"] == "RELEASE":
            if candidate_id not in active:
                continue
            active.pop(candidate_id, None)
            symbol_ids = active_by_symbol.get(symbol, set())
            symbol_ids.discard(candidate_id)
            if not symbol_ids:
                active_by_symbol.pop(symbol, None)
            continue
        if candidate_id in accepted or candidate_id in rejected:
            continue
        symbol_ids = active_by_symbol.get(symbol, set())
        symbol_sides = {active[item][1] for item in symbol_ids}
        if symbol_sides and symbol_sides != {side}:
            rejected[candidate_id] = "DUPLICATE_SYMBOL_OPPOSITE_SIDE_PENDING_OR_OPEN"
            continue
        if len(symbol_ids) >= same_side_limit:
            rejected[candidate_id] = "DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2"
            continue
        if len(active) >= capacity:
            rejected[candidate_id] = "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
            continue
        accepted.add(candidate_id)
        active[candidate_id] = (symbol, side)
        active_by_symbol.setdefault(symbol, set()).add(candidate_id)
        reservation_stats[candidate_id] = (
            len(active),
            len(active) * portfolio_policy.margin_per_entry_rs,
        )

    for index, row in out.iterrows():
        candidate_id = str(row["candidate_id"])
        if candidate_id in accepted:
            active_count, margin = reservation_stats[candidate_id]
            out.at[index, "portfolio_decision"] = "ACCEPTED"
            out.at[index, "portfolio_active_at_reservation"] = active_count
            out.at[index, "portfolio_reserved_margin_rs"] = margin
        elif candidate_id in rejected:
            _apply_portfolio_rejection(out, index, row, rejected[candidate_id])
    out["v11_same_side_symbol_limit"] = same_side_limit
    out["v11_opposite_side_same_symbol_prohibited"] = True
    out["v11_max_symbol_target_exposure_rs"] = (
        same_side_limit * portfolio_policy.target_exposure_per_entry_rs
    )
    return out
