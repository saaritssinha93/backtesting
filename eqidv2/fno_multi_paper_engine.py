"""Policy-isolated incremental PAPER reducer for V10, V11 and V12.

The proven V8 completed-candle reducer remains the neutral state-machine
core.  Every strategy delta is bound to one immutable profile instance; this
module never replaces a function in another module and three engines can run
concurrently without leaking policy into one another.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import fno_multi_paper_profiles as profiles
import fno_v8_combined_paper_engine as base


SCHEMA_VERSION = "fno_multi_profile_incremental_paper_engine_v1"

PaperCandidate = base.PaperCandidate
CompletedMinuteBar = base.CompletedMinuteBar
PaperEngineError = base.PaperEngineError
ReplayConflictError = base.ReplayConflictError
OutOfOrderMinuteError = base.OutOfOrderMinuteError


def engine_config(profile: profiles.StrategyProfile) -> base.PaperEngineConfig:
    """Translate one immutable profile into the neutral reducer contract."""

    profile.validate()
    setups = tuple(base.SetupPolicy.from_object(asdict(item)) for item in profile.setups)
    entry_policies = MappingProxyType(
        {
            setup_id: base.ResolvedEntryPolicy.from_object(asdict(policy))
            for setup_id, policy in profile.entry_policies
        }
    )
    portfolio = base.PaperPortfolioPolicy.from_object(asdict(profile.portfolio))
    result = base.PaperEngineConfig(
        setups=setups,
        entry_policies=entry_policies,
        portfolio_policy=portfolio,
        setup_book_sha256=profile.setup_book_sha256,
        strategy_fingerprint=profile.fingerprint,
    )
    result.validate()
    return result


def _candidate_key(setup: base.SetupPolicy, candidate: PaperCandidate) -> str:
    return f"{candidate.signal_time.date().isoformat()}|{setup.setup_id}|{candidate.symbol}"


def adverse_gap_bps(side: str, opening: float, trigger: float) -> float | None:
    selected = str(side).strip().upper()
    if not math.isfinite(float(opening)) or not math.isfinite(float(trigger)) or trigger <= 0:
        raise ValueError("opening and trigger must be finite; trigger must be positive")
    if selected == "LONG":
        return (opening - trigger) / trigger * 10_000.0 if opening >= trigger else None
    if selected == "SHORT":
        return (trigger - opening) / trigger * 10_000.0 if opening <= trigger else None
    raise ValueError(f"unsupported side: {side!r}")


class ProfilePaperEngine(base.V8CombinedPaperEngine):
    """One independent portfolio governed by one frozen strategy profile."""

    def __init__(self, config: base.PaperEngineConfig | None = None) -> None:
        if config is None:
            raise ValueError("ProfilePaperEngine requires an explicit frozen config")
        profile = profiles.PROFILE_BY_FINGERPRINT.get(config.strategy_fingerprint)
        if profile is None:
            raise ValueError("engine config is not one of the frozen PAPER profiles")
        self.profile = profile
        self._gap_observations: dict[str, dict[str, Any]] = {}
        self._accepted_active: dict[str, dict[str, str]] = {}
        self._early_fill_skips: dict[str, int] = {}
        self._early_touch_observed: set[str] = set()
        super().__init__(config)

    @classmethod
    def for_profile(cls, value: profiles.StrategyProfile | str) -> "ProfilePaperEngine":
        profile = value if isinstance(value, profiles.StrategyProfile) else profiles.profile_for(value)
        return cls(engine_config(profile))

    def _entry_fill(
        self,
        setup: base.SetupPolicy,
        policy: base.ResolvedEntryPolicy,
        runtime: Any,
        bar: CompletedMinuteBar,
    ) -> tuple[float, bool] | None:
        candidate_id = runtime.candidate_id
        not_before = self.profile.execution.entry_not_before_map().get(setup.setup_id)
        relative_minute = int(
            (bar.timestamp - runtime.candidate.signal_time).total_seconds() // 60
        )
        if not_before is not None and relative_minute < int(not_before):
            self._early_fill_skips[candidate_id] = self._early_fill_skips.get(candidate_id, 0) + 1
            neutral = base.V8CombinedPaperEngine._entry_fill(setup, policy, runtime, bar)
            if neutral is not None:
                self._early_touch_observed.add(candidate_id)
            return None

        fill = base.V8CombinedPaperEngine._entry_fill(setup, policy, runtime, bar)
        if fill is None or not fill[1]:
            return fill
        assert runtime.trigger is not None
        distance = adverse_gap_bps(setup.side, float(bar.open), float(runtime.trigger))
        if distance is None:
            raise AssertionError("neutral reducer labelled a non-gap fill as a gap")
        threshold = self.profile.execution.max_adverse_gap_bps
        rejected = threshold is not None and distance > float(threshold) + 1e-12
        self._gap_observations[candidate_id] = {
            "observed": True,
            "rejected": bool(rejected),
            "bar_open": float(bar.open),
            "trigger": float(runtime.trigger),
            "adverse_bps": float(distance),
            "event_time": bar.timestamp.isoformat(),
        }
        return None if rejected else fill

    def _postconfirm_invalidated(
        self,
        setup: base.SetupPolicy,
        candidate: PaperCandidate,
        bar: CompletedMinuteBar,
    ) -> bool:
        gap = self._gap_observations.get(_candidate_key(setup, candidate))
        if gap is not None and bool(gap.get("rejected")):
            return True
        return base.V8CombinedPaperEngine._postconfirm_invalidated(setup, candidate, bar)

    def _transition(
        self,
        occurrence: Any,
        runtime: Any,
        new_state: str,
        event_time: datetime,
        reason: str,
    ) -> None:
        gap = self._gap_observations.get(runtime.candidate_id)
        if (
            new_state == base.CandidateState.POSTCONF_CANCELLED.value
            and gap is not None
            and bool(gap.get("rejected"))
        ):
            reason = "ADVERSE_GAP_GUARD_REJECTED"
        super()._transition(occurrence, runtime, new_state, event_time, reason)

    def _apply_releases(self, actions: Sequence[Any]) -> None:
        for action in sorted(actions, key=self._action_key):
            runtime = action.runtime
            if runtime.portfolio_decision != "ACCEPTED":
                continue
            if runtime.candidate_id not in self._accepted_active:
                continue
            self._accepted_active.pop(runtime.candidate_id, None)
            self._emit(
                self._occurrences[f"{action.signal_time.date().isoformat()}|{action.setup_id}"],
                runtime,
                action.event_time,
                "PORTFOLIO",
                "RESERVED",
                "RELEASED",
                "TERMINAL_LOCAL_STATE",
            )

    def _apply_reserves(self, actions: Sequence[Any]) -> None:
        portfolio = self.config.portfolio_policy
        limit = int(self.profile.execution.same_side_symbol_limit)
        for action in sorted(actions, key=self._action_key):
            runtime = action.runtime
            occurrence = self._occurrences[
                f"{action.signal_time.date().isoformat()}|{action.setup_id}"
            ]
            if runtime.portfolio_decision != "NOT_APPLICABLE":
                continue
            same_symbol = [
                item
                for item in self._accepted_active.values()
                if item["symbol"] == action.symbol
            ]
            observed_sides = {item["side"] for item in same_symbol}
            if observed_sides and observed_sides != {occurrence.setup.side}:
                reason = "DUPLICATE_SYMBOL_OPPOSITE_SIDE_PENDING_OR_OPEN"
            elif len(same_symbol) >= limit:
                reason = f"DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_{limit}"
            elif len(self._accepted_active) >= portfolio.capacity:
                reason = "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
            else:
                runtime.portfolio_decision = "ACCEPTED"
                self._accepted_active[runtime.candidate_id] = {
                    "symbol": action.symbol,
                    "side": occurrence.setup.side,
                }
                runtime.portfolio_active_at_reservation = len(self._accepted_active)
                runtime.portfolio_reserved_margin_rs = (
                    len(self._accepted_active) * portfolio.margin_per_entry_rs
                )
                self._emit(
                    occurrence,
                    runtime,
                    action.event_time,
                    "PORTFOLIO",
                    "UNRESERVED",
                    "RESERVED",
                    "PORTFOLIO_ACCEPTED",
                )
                continue
            runtime.portfolio_decision = "REJECTED"
            runtime.portfolio_reject_reason = reason
            self._emit(
                occurrence,
                runtime,
                action.event_time,
                "PORTFOLIO",
                base.CandidateState.CONFIRMED_WAITING_CAP.value,
                runtime.constrained_state,
                f"{reason}:CONSERVATIVE_NO_BACKFILL",
            )

    def records(self) -> list[dict[str, Any]]:
        output = super().records()
        for record in output:
            candidate_id = str(record["candidate_id"])
            candidate = self._runtime_by_id[candidate_id].candidate
            gap = self._gap_observations.get(candidate_id, {})
            record.update(
                {
                    "profile_key": self.profile.key,
                    "profile_id": self.profile.profile_id,
                    "profile_fingerprint": self.profile.fingerprint,
                    "gap_guard_max_adverse_bps": self.profile.execution.max_adverse_gap_bps,
                    "gap_guard_observed": bool(gap.get("observed", False)),
                    "gap_guard_rejected": bool(gap.get("rejected", False)),
                    "gap_guard_bar_open": gap.get("bar_open"),
                    "gap_guard_trigger": gap.get("trigger"),
                    "gap_guard_adverse_bps": gap.get("adverse_bps"),
                    "gap_guard_event_time": gap.get("event_time"),
                    "entry_not_before_minute": self.profile.execution.entry_not_before_map().get(
                        str(record["setup_id"])
                    ),
                    "early_fill_checks_skipped": self._early_fill_skips.get(candidate_id, 0),
                    "early_touch_observed": candidate_id in self._early_touch_observed,
                    "same_side_symbol_limit": self.profile.execution.same_side_symbol_limit,
                    "opposite_side_same_symbol_prohibited": self.profile.execution.prohibit_opposite_side,
                    "five_min_open": candidate.five_min_open,
                    "five_min_high": candidate.five_min_high,
                    "five_min_low": candidate.five_min_low,
                    "five_min_close": candidate.five_min_close,
                    "price_change_pct": candidate.price_change_pct,
                    "oi_change_pct": candidate.oi_change_pct,
                    "volume_ratio": candidate.volume_ratio,
                    "traded_value": candidate.traded_value,
                    "ema9": candidate.ema9,
                    "ema20": candidate.ema20,
                    "ema50": candidate.ema50,
                    "oi": candidate.oi,
                    "prev_oi": candidate.prev_oi,
                    "tick_size": candidate.tick_size,
                    "equity_instrument_token": candidate.equity_instrument_token,
                    "futures_instrument_token": candidate.futures_instrument_token,
                    "futures_symbol": candidate.futures_symbol,
                }
            )
        return output

    def state_summary(self) -> dict[str, Any]:
        result = super().state_summary()
        result.update(
            profile_key=self.profile.key,
            profile_id=self.profile.profile_id,
            active_portfolio_candidate_ids=sorted(self._accepted_active),
            active_portfolio_symbols=sorted(
                {item["symbol"] for item in self._accepted_active.values()}
            ),
            active_portfolio_count=len(self._accepted_active),
        )
        return result

    def checkpoint(self) -> dict[str, Any]:
        result = super().checkpoint()
        result["multi_profile_extension"] = {
            "schema_version": SCHEMA_VERSION,
            "profile_key": self.profile.key,
            "profile_id": self.profile.profile_id,
            "profile_fingerprint": self.profile.fingerprint,
            "gap_observations": self._gap_observations,
            "accepted_active": self._accepted_active,
            "early_fill_skips": self._early_fill_skips,
            "early_touch_observed": sorted(self._early_touch_observed),
        }
        return result

    @classmethod
    def from_checkpoint(
        cls,
        payload: Mapping[str, Any],
        config: base.PaperEngineConfig | None = None,
    ) -> "ProfilePaperEngine":
        extension = dict(payload.get("multi_profile_extension") or {})
        if extension.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("profile checkpoint extension is missing or unsupported")
        restored = super().from_checkpoint(payload, config)
        assert isinstance(restored, cls)
        if extension.get("profile_fingerprint") != restored.profile.fingerprint:
            raise ReplayConflictError("checkpoint profile identity changed")
        restored._gap_observations = {
            str(key): dict(value)
            for key, value in dict(extension.get("gap_observations") or {}).items()
        }
        restored._accepted_active = {
            str(key): {"symbol": str(value["symbol"]), "side": str(value["side"])}
            for key, value in dict(extension.get("accepted_active") or {}).items()
        }
        restored._early_fill_skips = {
            str(key): int(value)
            for key, value in dict(extension.get("early_fill_skips") or {}).items()
        }
        restored._early_touch_observed = {
            str(value) for value in extension.get("early_touch_observed") or []
        }
        return restored


@dataclass(frozen=True)
class ProfileEvent:
    profile_key: str
    profile_id: str
    event: base.PaperEvent

    def to_dict(self) -> dict[str, Any]:
        value = self.event.to_dict()
        value["candidate_id"] = f"{self.profile_key}|{value['candidate_id']}"
        return {
            "profile_key": self.profile_key,
            "profile_id": self.profile_id,
            **value,
        }


class MultiStrategyPaperEngine:
    """One single-writer coordinator with three wholly independent ledgers."""

    def __init__(self, engines: Mapping[str, ProfilePaperEngine] | None = None) -> None:
        supplied = dict(engines or {p.key: ProfilePaperEngine.for_profile(p) for p in profiles.PROFILES})
        if set(supplied) != set(profiles.PROFILE_BY_KEY):
            raise ValueError("coordinator requires exactly v10, v11 and v12")
        for key, engine in supplied.items():
            if engine.profile.key != key:
                raise ValueError("profile engine is stored under the wrong key")
        self.engines: Mapping[str, ProfilePaperEngine] = MappingProxyType(supplied)
        self._selection_audit: dict[str, dict[str, dict[str, Any]]] = {
            profile.key: {} for profile in profiles.PROFILES
        }

    @staticmethod
    def _selection_rejection_codes(
        setup: base.SetupPolicy,
        candidate: PaperCandidate,
        constraint: profiles.SelectionConstraint | None,
    ) -> list[str]:
        codes: list[str] = []
        if setup.side == "LONG":
            if not candidate.ema9 > candidate.ema20 > candidate.ema50:
                codes.append("EMA_STRUCTURE")
            if candidate.price_change_pct < setup.price_change_pct:
                codes.append("MOVE_BELOW_MINIMUM")
            directional = candidate.price_change_pct
        else:
            if not candidate.ema9 < candidate.ema20 < candidate.ema50:
                codes.append("EMA_STRUCTURE")
            if candidate.price_change_pct > -setup.price_change_pct:
                codes.append("MOVE_BELOW_MINIMUM")
            directional = -candidate.price_change_pct
        if candidate.oi <= candidate.prev_oi:
            codes.append("OI_NOT_RISING")
        if candidate.oi_change_pct < setup.oi_change_pct:
            codes.append("OI_CHANGE_BELOW_MINIMUM")
        if candidate.volume_ratio < setup.volume_ratio:
            codes.append("VOLUME_RATIO_BELOW_MINIMUM")
        if candidate.traded_value < setup.min_traded_value:
            codes.append("TRADED_VALUE_BELOW_MINIMUM")
        if (
            constraint is not None
            and constraint.max_directional_move_pct is not None
            and directional > float(constraint.max_directional_move_pct)
        ):
            codes.append("MOVE_ABOVE_MAXIMUM")
        if not base.candidate_passes(setup, candidate) and not codes:
            codes.append("INVALID_OR_BASE_GATE_FAILED")
        return codes

    def register_candidates(
        self,
        setup_id: str,
        signal_time: Any,
        candidates: Sequence[PaperCandidate | Mapping[str, Any] | Any],
    ) -> list[ProfileEvent]:
        normalized = tuple(PaperCandidate.from_object(value) for value in candidates)
        events: list[ProfileEvent] = []
        for profile in profiles.PROFILES:
            engine = self.engines[profile.key]
            setup = engine.config.setup_by_id[str(setup_id).strip().upper()]
            constraint = profile.selection_constraint_by_id.get(setup.setup_id)
            selected: list[PaperCandidate] = []
            audit_rows: list[dict[str, Any]] = []
            for candidate in normalized:
                codes = self._selection_rejection_codes(setup, candidate, constraint)
                if not codes:
                    selected.append(candidate)
                picker_value = {
                    "max_oi": candidate.oi_change_pct,
                    "max_volume": candidate.volume_ratio,
                    "max_move": abs(candidate.price_change_pct),
                    "max_liquidity": candidate.traded_value,
                }[setup.picker]
                audit_rows.append(
                    {
                        "profile_key": profile.key,
                        "profile_id": profile.profile_id,
                        "candidate_id": _candidate_key(setup, candidate),
                        "session_date": candidate.signal_time.date().isoformat(),
                        "signal_time": candidate.signal_time.isoformat(),
                        "setup_id": setup.setup_id,
                        "side": setup.side,
                        "symbol": candidate.symbol,
                        "selected_5m": not codes,
                        "selection_status": "SELECTED" if not codes else "REJECTED",
                        "selection_rejection_codes": codes,
                        "selection_reason": "FIVE_MINUTE_GATES_PASSED" if not codes else ";".join(codes),
                        "picker": setup.picker,
                        "picker_value": float(picker_value),
                        "selection_rank": None,
                        "price_change_pct": candidate.price_change_pct,
                        "oi_change_pct": candidate.oi_change_pct,
                        "volume_ratio": candidate.volume_ratio,
                        "traded_value": candidate.traded_value,
                        "ema9": candidate.ema9,
                        "ema20": candidate.ema20,
                        "ema50": candidate.ema50,
                        "oi": candidate.oi,
                        "prev_oi": candidate.prev_oi,
                        "five_min_open": candidate.five_min_open,
                        "five_min_high": candidate.five_min_high,
                        "five_min_low": candidate.five_min_low,
                        "five_min_close": candidate.five_min_close,
                    }
                )
            selected.sort(
                key=lambda item: (
                    -{
                        "max_oi": item.oi_change_pct,
                        "max_volume": item.volume_ratio,
                        "max_move": abs(item.price_change_pct),
                        "max_liquidity": item.traded_value,
                    }[setup.picker],
                    -item.traded_value,
                    item.symbol,
                )
            )
            selected_rank = {item.symbol: rank for rank, item in enumerate(selected, 1)}
            for row in audit_rows:
                if row["selected_5m"]:
                    row["selection_rank"] = selected_rank[row["symbol"]]
                key = str(row["candidate_id"])
                prior = self._selection_audit[profile.key].get(key)
                if prior is not None and prior != row:
                    raise ReplayConflictError(f"five-minute selection audit changed: {key}")
                self._selection_audit[profile.key][key] = row
            for event in engine.register_candidates(setup.setup_id, signal_time, selected):
                events.append(ProfileEvent(profile.key, profile.profile_id, event))
        return events

    def process_completed_minute(
        self,
        timestamp: Any,
        bars_by_symbol: Mapping[str, CompletedMinuteBar | Mapping[str, Any] | Any],
    ) -> list[ProfileEvent]:
        events: list[ProfileEvent] = []
        # Fixed order is part of the combined audit contract.  Ledgers do not
        # share capacity, so this order cannot alter strategy economics.
        for profile in profiles.PROFILES:
            for event in self.engines[profile.key].process_completed_minute(
                timestamp, bars_by_symbol
            ):
                events.append(ProfileEvent(profile.key, profile.profile_id, event))
        return events

    def terminate_for_intervention(
        self,
        timestamp: Any,
        bars_by_symbol: Mapping[str, CompletedMinuteBar | Mapping[str, Any] | Any],
        reason: str,
    ) -> list[ProfileEvent]:
        events: list[ProfileEvent] = []
        for profile in profiles.PROFILES:
            for event in self.engines[profile.key].terminate_for_intervention(
                timestamp, bars_by_symbol, reason
            ):
                events.append(ProfileEvent(profile.key, profile.profile_id, event))
        return events

    def required_symbols(self) -> list[str]:
        return sorted(
            {
                symbol
                for engine in self.engines.values()
                for symbol in engine.required_symbols()
            }
        )

    @property
    def last_processed_minute(self) -> datetime | None:
        values = {engine.last_processed_minute for engine in self.engines.values()}
        if len(values) != 1:
            raise PaperEngineError("profile engines diverged in processed minute")
        return next(iter(values))

    def records_by_profile(self) -> dict[str, list[dict[str, Any]]]:
        return {key: engine.records() for key, engine in self.engines.items()}

    def selection_records_by_profile(self) -> dict[str, list[dict[str, Any]]]:
        return {
            key: sorted(
                (dict(value) for value in rows.values()),
                key=lambda row: (
                    row["signal_time"],
                    row["setup_id"],
                    row["symbol"],
                ),
            )
            for key, rows in self._selection_audit.items()
        }

    def records(self) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for profile in profiles.PROFILES:
            for raw in self.engines[profile.key].records():
                record = dict(raw)
                record["local_candidate_id"] = record["candidate_id"]
                record["candidate_id"] = f"{profile.key}|{record['candidate_id']}"
                output.append(record)
        return output

    def events(self) -> list[ProfileEvent]:
        output: list[ProfileEvent] = []
        for profile in profiles.PROFILES:
            output.extend(
                ProfileEvent(profile.key, profile.profile_id, event)
                for event in self.engines[profile.key].events()
            )
        return sorted(
            output,
            key=lambda item: (
                item.event.event_time,
                item.profile_key,
                item.event.sequence,
            ),
        )

    def state_summary(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "last_processed_minute": (
                self.last_processed_minute.isoformat()
                if self.last_processed_minute is not None
                else None
            ),
            "required_symbols": self.required_symbols(),
            "profiles": {
                key: engine.state_summary() for key, engine in self.engines.items()
            },
        }

    def checkpoint(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "profile_fingerprints": {
                profile.key: profile.fingerprint for profile in profiles.PROFILES
            },
            "engines": {
                key: engine.checkpoint() for key, engine in self.engines.items()
            },
            "selection_audit": self.selection_records_by_profile(),
        }

    @classmethod
    def from_checkpoint(cls, payload: Mapping[str, Any]) -> "MultiStrategyPaperEngine":
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported multi-strategy checkpoint schema")
        expected = {profile.key: profile.fingerprint for profile in profiles.PROFILES}
        if dict(payload.get("profile_fingerprints") or {}) != expected:
            raise ReplayConflictError("multi-strategy checkpoint profiles changed")
        raw_engines = dict(payload.get("engines") or {})
        if set(raw_engines) != set(expected):
            raise ValueError("checkpoint does not contain all profile engines")
        engines = {
            profile.key: ProfilePaperEngine.from_checkpoint(
                raw_engines[profile.key], engine_config(profile)
            )
            for profile in profiles.PROFILES
        }
        result = cls(engines)
        raw_audit = dict(payload.get("selection_audit") or {})
        if set(raw_audit) != set(expected):
            raise ValueError("checkpoint does not contain all selection audits")
        result._selection_audit = {
            key: {str(row["candidate_id"]): dict(row) for row in raw_audit[key]}
            for key in expected
        }
        return result


# Compatibility aliases consumed by the proven V8 source/session helpers.
V8CombinedPaperEngine = MultiStrategyPaperEngine
PaperEngine = MultiStrategyPaperEngine


__all__ = [
    "CompletedMinuteBar",
    "MultiStrategyPaperEngine",
    "OutOfOrderMinuteError",
    "PaperCandidate",
    "PaperEngine",
    "PaperEngineError",
    "ProfileEvent",
    "ProfilePaperEngine",
    "ReplayConflictError",
    "SCHEMA_VERSION",
    "V8CombinedPaperEngine",
    "adverse_gap_bps",
    "engine_config",
]
