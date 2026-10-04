"""Append-only registry for controlled trading-strategy experiments.

This module records proposals, result manifests, and human review decisions. It
does not launch a backtest, change canonical strategy configuration, or promote
anything to live trading. The event journal is hash chained so accidental edits
are detected by ``verify`` and by every mutating command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence


SCHEMA_VERSION = "eqidv2.observability.experiment.v1"
RESULT_SCHEMA_VERSION = "eqidv2.observability.experiment_result.v1"
EVENT_SCHEMA_VERSION = "eqidv2.observability.experiment_event.v1"
ZERO_HASH = "0" * 64
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,80}$")
DECISIONS = {
    "REJECTED",
    "INSUFFICIENT_EVIDENCE",
    "ELIGIBLE_FOR_SHADOW",
    "APPROVED_FOR_SHADOW",
    "CANDIDATE_FOR_MANUAL_LIVE_REVIEW",
    "RETIRED",
}
FORBIDDEN_DECISIONS = {
    "APPROVED_FOR_LIVE",
    "PROMOTED_TO_LIVE",
    "LIVE",
    "AUTO_PROMOTE",
}


class RegistryError(ValueError):
    """A validation, state-transition, or journal-integrity error."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RegistryError(f"{path} must contain one JSON object")
    return value


def _required(mapping: Mapping[str, Any], keys: Sequence[str], location: str) -> None:
    missing = [key for key in keys if key not in mapping]
    if missing:
        raise RegistryError(f"{location} missing required fields: {', '.join(missing)}")


def _nonempty_text(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryError(f"{location} must be non-empty text")
    return value.strip()


def _sha256(value: Any, location: str) -> str:
    text = _nonempty_text(value, location).lower()
    if not HASH_PATTERN.fullmatch(text):
        raise RegistryError(f"{location} must be a lowercase 64-character SHA-256")
    return text


def _bounded_number(
    value: Any, location: str, *, minimum: float, maximum: float
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RegistryError(f"{location} must be numeric")
    number = float(value)
    if not minimum <= number <= maximum:
        raise RegistryError(f"{location} must be between {minimum} and {maximum}")
    return number


def _window(value: Any, location: str) -> tuple[date, date]:
    if not isinstance(value, Mapping):
        raise RegistryError(f"{location} must be an object")
    _required(value, ["start", "end"], location)
    try:
        start = date.fromisoformat(str(value["start"]))
        end = date.fromisoformat(str(value["end"]))
    except ValueError as exc:
        raise RegistryError(f"{location} dates must use YYYY-MM-DD") from exc
    if start > end:
        raise RegistryError(f"{location}.start must be on or before end")
    return start, end


def validate_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return a detached, JSON-normalized experiment spec."""

    _required(
        spec,
        [
            "schema_version",
            "experiment_id",
            "created_at",
            "proposed_by",
            "mode",
            "hypothesis",
            "baseline",
            "challenger",
            "windows",
            "execution_assumptions",
            "trial_budget",
            "decision_rules",
            "data_policy",
        ],
        "spec",
    )
    if spec["schema_version"] != SCHEMA_VERSION:
        raise RegistryError(f"spec.schema_version must be {SCHEMA_VERSION}")
    experiment_id = _nonempty_text(spec["experiment_id"], "spec.experiment_id")
    if not ID_PATTERN.fullmatch(experiment_id):
        raise RegistryError(
            "spec.experiment_id must be 3-81 lowercase letters, digits, dots, underscores or hyphens"
        )
    if spec["mode"] != "research_only":
        raise RegistryError("spec.mode must be research_only")
    _nonempty_text(spec["created_at"], "spec.created_at")
    _nonempty_text(spec["proposed_by"], "spec.proposed_by")
    _nonempty_text(spec["hypothesis"], "spec.hypothesis")

    baseline = spec["baseline"]
    challenger = spec["challenger"]
    if not isinstance(baseline, Mapping) or not isinstance(challenger, Mapping):
        raise RegistryError("spec.baseline and spec.challenger must be objects")
    _required(
        baseline,
        ["strategy_id", "code_sha256", "config_sha256", "source_manifest_sha256"],
        "spec.baseline",
    )
    _required(
        challenger,
        ["name", "code_sha256", "config_sha256", "one_factor_change"],
        "spec.challenger",
    )
    _nonempty_text(baseline["strategy_id"], "spec.baseline.strategy_id")
    _sha256(baseline["code_sha256"], "spec.baseline.code_sha256")
    _sha256(baseline["config_sha256"], "spec.baseline.config_sha256")
    _sha256(
        baseline["source_manifest_sha256"],
        "spec.baseline.source_manifest_sha256",
    )
    _nonempty_text(challenger["name"], "spec.challenger.name")
    _sha256(challenger["code_sha256"], "spec.challenger.code_sha256")
    _sha256(challenger["config_sha256"], "spec.challenger.config_sha256")
    _nonempty_text(challenger["one_factor_change"], "spec.challenger.one_factor_change")

    windows = spec["windows"]
    if not isinstance(windows, Mapping):
        raise RegistryError("spec.windows must be an object")
    _required(windows, ["development", "validation", "final_test"], "spec.windows")
    development = _window(windows["development"], "spec.windows.development")
    validation = _window(windows["validation"], "spec.windows.validation")
    final_test = _window(windows["final_test"], "spec.windows.final_test")
    if not development[1] < validation[0]:
        raise RegistryError("development must end before validation starts")
    if not validation[1] < final_test[0]:
        raise RegistryError("validation must end before final_test starts")

    execution = spec["execution_assumptions"]
    if not isinstance(execution, Mapping):
        raise RegistryError("spec.execution_assumptions must be an object")
    _required(
        execution,
        ["fees_bps", "slippage_bps", "liquidity_model", "capital_rupees"],
        "spec.execution_assumptions",
    )
    _bounded_number(execution["fees_bps"], "fees_bps", minimum=0, maximum=1000)
    _bounded_number(
        execution["slippage_bps"], "slippage_bps", minimum=0, maximum=1000
    )
    _bounded_number(
        execution["capital_rupees"],
        "capital_rupees",
        minimum=1,
        maximum=10**12,
    )
    _nonempty_text(execution["liquidity_model"], "liquidity_model")

    budget = spec["trial_budget"]
    if not isinstance(budget, Mapping):
        raise RegistryError("spec.trial_budget must be an object")
    _required(budget, ["max_variants", "max_runs", "max_compute_minutes"], "trial_budget")
    _bounded_number(budget["max_variants"], "max_variants", minimum=1, maximum=50)
    _bounded_number(budget["max_runs"], "max_runs", minimum=1, maximum=500)
    _bounded_number(
        budget["max_compute_minutes"],
        "max_compute_minutes",
        minimum=1,
        maximum=10080,
    )

    rules = spec["decision_rules"]
    if not isinstance(rules, Mapping):
        raise RegistryError("spec.decision_rules must be an object")
    _required(
        rules,
        [
            "primary_metric",
            "minimum_improvement",
            "maximum_drawdown_degradation_rupees",
            "minimum_forward_shadow_sessions",
        ],
        "decision_rules",
    )
    _nonempty_text(rules["primary_metric"], "primary_metric")
    _bounded_number(
        rules["minimum_improvement"],
        "minimum_improvement",
        minimum=-10**12,
        maximum=10**12,
    )
    _bounded_number(
        rules["maximum_drawdown_degradation_rupees"],
        "maximum_drawdown_degradation_rupees",
        minimum=0,
        maximum=10**12,
    )
    _bounded_number(
        rules["minimum_forward_shadow_sessions"],
        "minimum_forward_shadow_sessions",
        minimum=1,
        maximum=252,
    )

    policy = spec["data_policy"]
    if not isinstance(policy, Mapping):
        raise RegistryError("spec.data_policy must be an object")
    _required(
        policy,
        ["point_in_time_only", "include_failed_variants", "final_test_locked"],
        "data_policy",
    )
    for key in ("point_in_time_only", "include_failed_variants", "final_test_locked"):
        if policy[key] is not True:
            raise RegistryError(f"spec.data_policy.{key} must be true")

    return json.loads(canonical_json(spec).decode("utf-8"))


def validate_result(result: Mapping[str, Any], experiment_id: str) -> dict[str, Any]:
    _required(
        result,
        [
            "schema_version",
            "experiment_id",
            "completed_at",
            "status",
            "evidence_quality",
            "trials_attempted",
            "checks",
            "metrics",
            "forward_shadow_sessions",
        ],
        "result",
    )
    if result["schema_version"] != RESULT_SCHEMA_VERSION:
        raise RegistryError(f"result.schema_version must be {RESULT_SCHEMA_VERSION}")
    if result["experiment_id"] != experiment_id:
        raise RegistryError("result.experiment_id does not match the registered experiment")
    _nonempty_text(result["completed_at"], "result.completed_at")
    if result["status"] not in {"COMPLETED", "FAILED", "INDETERMINATE"}:
        raise RegistryError("result.status must be COMPLETED, FAILED, or INDETERMINATE")
    if result["evidence_quality"] not in {
        "REUSED_DEVELOPMENT",
        "UNTOUCHED_HOLDOUT",
        "PROSPECTIVE_SHADOW",
    }:
        raise RegistryError("result.evidence_quality is invalid")
    _bounded_number(result["trials_attempted"], "trials_attempted", minimum=1, maximum=500)
    _bounded_number(
        result["forward_shadow_sessions"],
        "forward_shadow_sessions",
        minimum=0,
        maximum=2520,
    )
    checks = result["checks"]
    if not isinstance(checks, Mapping):
        raise RegistryError("result.checks must be an object")
    _required(
        checks,
        ["point_in_time", "costs_included", "outputs_hash_verified", "baseline_reproduced"],
        "result.checks",
    )
    for key in ("point_in_time", "costs_included", "outputs_hash_verified", "baseline_reproduced"):
        if not isinstance(checks[key], bool):
            raise RegistryError(f"result.checks.{key} must be boolean")
    if not isinstance(result["metrics"], Mapping):
        raise RegistryError("result.metrics must be an object")
    return json.loads(canonical_json(result).decode("utf-8"))


def _measured_metric(
    result: Mapping[str, Any], section: str, metric: str
) -> float:
    metrics = result.get("metrics")
    values = metrics.get(section) if isinstance(metrics, Mapping) else None
    value = values.get(metric) if isinstance(values, Mapping) else None
    if isinstance(value, bool):
        raise RegistryError(f"result.metrics.{section}.{metric} must be numeric")
    try:
        measured = float(value)
    except (TypeError, ValueError) as exc:
        raise RegistryError(
            f"result.metrics.{section}.{metric} is required for a positive decision"
        ) from exc
    if not math.isfinite(measured):
        raise RegistryError(f"result.metrics.{section}.{metric} must be finite")
    return measured


def _enforce_positive_decision_rules(
    spec: Mapping[str, Any], result: Mapping[str, Any]
) -> None:
    """Fail closed on the preregistered return and drawdown thresholds."""

    rules = spec["decision_rules"]
    primary = str(rules["primary_metric"])
    baseline = _measured_metric(result, "baseline", primary)
    challenger = _measured_metric(result, "challenger", primary)
    difference = _measured_metric(result, "difference", primary)
    computed_difference = challenger - baseline
    tolerance = max(1e-9, abs(computed_difference) * 1e-9)
    if abs(difference - computed_difference) > tolerance:
        raise RegistryError(
            f"result.metrics.difference.{primary} does not equal challenger minus baseline"
        )
    minimum = float(rules["minimum_improvement"])
    if difference < minimum:
        raise RegistryError(
            f"primary-metric improvement {difference:g} is below the preregistered minimum {minimum:g}"
        )

    baseline_drawdown = _measured_metric(
        result, "baseline", "maximum_drawdown_rs"
    )
    challenger_drawdown = _measured_metric(
        result, "challenger", "maximum_drawdown_rs"
    )
    degradation = max(
        0.0,
        abs(min(0.0, challenger_drawdown))
        - abs(min(0.0, baseline_drawdown)),
    )
    allowed = float(rules["maximum_drawdown_degradation_rupees"])
    if degradation > allowed + 1e-9:
        raise RegistryError(
            f"maximum-drawdown degradation {degradation:g} exceeds the preregistered limit {allowed:g}"
        )


class ExperimentRegistry:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.journal = self.root / "registry.jsonl"
        self.experiments = self.root / "experiments"
        self.lock_file = self.root / ".registry.lock"

    def initialize(self) -> None:
        self.experiments.mkdir(parents=True, exist_ok=True)
        self.journal.touch(exist_ok=True)

    @contextmanager
    def _lock(self, timeout_seconds: float = 10.0) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + timeout_seconds
        descriptor: int | None = None
        while descriptor is None:
            try:
                descriptor = os.open(
                    self.lock_file,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                )
                os.write(descriptor, f"pid={os.getpid()} time={utc_now()}".encode("utf-8"))
                os.fsync(descriptor)
            except FileExistsError:
                if time.monotonic() >= deadline:
                    raise RegistryError(f"registry is locked: {self.lock_file}")
                time.sleep(0.05)
        try:
            yield
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                self.lock_file.unlink()
            except FileNotFoundError:
                pass

    def events(self) -> list[dict[str, Any]]:
        self.initialize()
        events: list[dict[str, Any]] = []
        previous = ZERO_HASH
        for line_number, raw_line in enumerate(
            self.journal.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not raw_line.strip():
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise RegistryError(f"journal line {line_number} is invalid JSON") from exc
            if not isinstance(event, dict):
                raise RegistryError(f"journal line {line_number} is not an object")
            observed_hash = event.get("event_hash")
            unsigned = {key: value for key, value in event.items() if key != "event_hash"}
            expected_hash = sha256_bytes(canonical_json(unsigned))
            if observed_hash != expected_hash:
                raise RegistryError(f"journal hash mismatch at line {line_number}")
            if event.get("previous_event_hash") != previous:
                raise RegistryError(f"journal chain mismatch at line {line_number}")
            if event.get("sequence") != len(events) + 1:
                raise RegistryError(f"journal sequence mismatch at line {line_number}")
            previous = str(observed_hash)
            events.append(event)
        return events

    def _append(
        self,
        *,
        event_type: str,
        experiment_id: str,
        actor: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        events = self.events()
        unsigned = {
            "schema_version": EVENT_SCHEMA_VERSION,
            "sequence": len(events) + 1,
            "recorded_at": utc_now(),
            "event_type": event_type,
            "experiment_id": experiment_id,
            "actor": _nonempty_text(actor, "actor"),
            "payload": dict(payload),
            "previous_event_hash": events[-1]["event_hash"] if events else ZERO_HASH,
        }
        event = dict(unsigned)
        event["event_hash"] = sha256_bytes(canonical_json(unsigned))
        with self.journal.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(canonical_json(event).decode("utf-8") + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        return event

    def _events_for(self, experiment_id: str) -> list[dict[str, Any]]:
        return [event for event in self.events() if event["experiment_id"] == experiment_id]

    def register(self, spec_path: Path, actor: str | None = None) -> dict[str, Any]:
        spec = validate_spec(load_json(spec_path))
        experiment_id = spec["experiment_id"]
        with self._lock():
            if self._events_for(experiment_id):
                raise RegistryError(f"experiment already registered: {experiment_id}")
            destination_dir = self.experiments / experiment_id
            destination_dir.mkdir(parents=True, exist_ok=False)
            destination = destination_dir / "spec.json"
            self._atomic_json(destination, spec)
            return self._append(
                event_type="REGISTERED",
                experiment_id=experiment_id,
                actor=actor or spec["proposed_by"],
                payload={
                    "spec_path": str(destination.relative_to(self.root)),
                    "spec_sha256": sha256_file(destination),
                    "mode": "research_only",
                },
            )

    def record_result(
        self, experiment_id: str, result_path: Path, actor: str
    ) -> dict[str, Any]:
        with self._lock():
            experiment_events = self._events_for(experiment_id)
            if not experiment_events or experiment_events[0]["event_type"] != "REGISTERED":
                raise RegistryError(f"unknown experiment: {experiment_id}")
            spec = load_json(self.root / experiment_events[0]["payload"]["spec_path"])
            result = validate_result(load_json(result_path), experiment_id)
            if int(result["trials_attempted"]) > int(spec["trial_budget"]["max_runs"]):
                raise RegistryError("result.trials_attempted exceeds the preregistered max_runs")
            result_number = 1 + sum(
                event["event_type"] == "RESULT_RECORDED" for event in experiment_events
            )
            destination = (
                self.experiments / experiment_id / f"result_{result_number:03d}.json"
            )
            self._atomic_json(destination, result)
            return self._append(
                event_type="RESULT_RECORDED",
                experiment_id=experiment_id,
                actor=actor,
                payload={
                    "result_path": str(destination.relative_to(self.root)),
                    "result_sha256": sha256_file(destination),
                    "status": result["status"],
                    "evidence_quality": result["evidence_quality"],
                    "forward_shadow_sessions": result["forward_shadow_sessions"],
                },
            )

    def decide(
        self,
        experiment_id: str,
        decision: str,
        actor: str,
        reason: str,
    ) -> dict[str, Any]:
        decision = decision.upper()
        if decision in FORBIDDEN_DECISIONS or "LIVE" in decision and decision not in {
            "CANDIDATE_FOR_MANUAL_LIVE_REVIEW"
        }:
            raise RegistryError(
                "the registry cannot promote a strategy to live; only a manual live-review candidate may be recorded"
            )
        if decision not in DECISIONS:
            raise RegistryError(f"unsupported decision: {decision}")
        _nonempty_text(reason, "reason")

        with self._lock():
            events = self._events_for(experiment_id)
            if not events:
                raise RegistryError(f"unknown experiment: {experiment_id}")
            spec_path = self.root / events[0]["payload"]["spec_path"]
            spec = load_json(spec_path)
            proposer = spec["proposed_by"]
            result_events = [event for event in events if event["event_type"] == "RESULT_RECORDED"]
            previous_decisions = [event["payload"]["decision"] for event in events if event["event_type"] == "DECISION"]

            if decision != "RETIRED" and not result_events:
                raise RegistryError("a result manifest must be recorded before this decision")
            if decision in {"APPROVED_FOR_SHADOW", "CANDIDATE_FOR_MANUAL_LIVE_REVIEW"} and actor == proposer:
                raise RegistryError("approval requires a reviewer different from the proposer")
            if decision == "APPROVED_FOR_SHADOW" and "ELIGIBLE_FOR_SHADOW" not in previous_decisions:
                raise RegistryError("ELIGIBLE_FOR_SHADOW must be recorded before APPROVED_FOR_SHADOW")
            if decision == "CANDIDATE_FOR_MANUAL_LIVE_REVIEW":
                if "APPROVED_FOR_SHADOW" not in previous_decisions:
                    raise RegistryError("APPROVED_FOR_SHADOW must precede a manual live-review candidate")
                latest_result_path = self.root / result_events[-1]["payload"]["result_path"]
                latest_result = load_json(latest_result_path)
                required = int(spec["decision_rules"]["minimum_forward_shadow_sessions"])
                observed = int(latest_result["forward_shadow_sessions"])
                if latest_result["evidence_quality"] != "PROSPECTIVE_SHADOW" or observed < required:
                    raise RegistryError(
                        f"manual live review requires PROSPECTIVE_SHADOW evidence and at least {required} sessions; observed {observed}"
                    )

            if decision in {"ELIGIBLE_FOR_SHADOW", "APPROVED_FOR_SHADOW", "CANDIDATE_FOR_MANUAL_LIVE_REVIEW"}:
                latest_result = load_json(self.root / result_events[-1]["payload"]["result_path"])
                if latest_result["status"] != "COMPLETED":
                    raise RegistryError("positive decisions require a COMPLETED result")
                failed_checks = [name for name, passed in latest_result["checks"].items() if not passed]
                if failed_checks:
                    raise RegistryError(
                        "positive decision blocked by failed result checks: " + ", ".join(failed_checks)
                    )
                if (
                    decision in {"ELIGIBLE_FOR_SHADOW", "APPROVED_FOR_SHADOW"}
                    and latest_result["evidence_quality"] != "UNTOUCHED_HOLDOUT"
                ):
                    raise RegistryError(
                        "shadow eligibility/approval requires UNTOUCHED_HOLDOUT evidence"
                    )
                _enforce_positive_decision_rules(spec, latest_result)

            return self._append(
                event_type="DECISION",
                experiment_id=experiment_id,
                actor=actor,
                payload={
                    "decision": decision,
                    "reason": reason,
                    "live_configuration_changed": False,
                },
            )

    def status(self, experiment_id: str) -> dict[str, Any]:
        events = self._events_for(experiment_id)
        if not events:
            raise RegistryError(f"unknown experiment: {experiment_id}")
        decisions = [event for event in events if event["event_type"] == "DECISION"]
        results = [event for event in events if event["event_type"] == "RESULT_RECORDED"]
        state = decisions[-1]["payload"]["decision"] if decisions else (
            "RESULT_RECORDED" if results else "REGISTERED"
        )
        return {
            "experiment_id": experiment_id,
            "state": state,
            "events": len(events),
            "results": len(results),
            "last_event_hash": events[-1]["event_hash"],
            "live_configuration_changed": False,
        }

    def list_statuses(self) -> list[dict[str, Any]]:
        experiment_ids = list(
            dict.fromkeys(event["experiment_id"] for event in self.events())
        )
        return [self.status(experiment_id) for experiment_id in experiment_ids]

    def verify(self) -> dict[str, Any]:
        events = self.events()
        verified_artifacts = 0
        for event in events:
            if event["event_type"] == "REGISTERED":
                path_key, hash_key = "spec_path", "spec_sha256"
            elif event["event_type"] == "RESULT_RECORDED":
                path_key, hash_key = "result_path", "result_sha256"
            else:
                continue
            artifact = (self.root / event["payload"][path_key]).resolve()
            if self.root not in artifact.parents:
                raise RegistryError(f"artifact escapes registry root: {artifact}")
            if not artifact.is_file():
                raise RegistryError(f"registered artifact is missing: {artifact}")
            if sha256_file(artifact) != event["payload"][hash_key]:
                raise RegistryError(f"registered artifact hash mismatch: {artifact}")
            verified_artifacts += 1
        return {
            "ok": True,
            "events": len(events),
            "verified_artifacts": verified_artifacts,
            "last_event_hash": events[-1]["event_hash"] if events else ZERO_HASH,
        }

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)


def scaffold_spec(path: Path, experiment_id: str, proposed_by: str) -> None:
    if path.exists():
        raise RegistryError(f"refusing to overwrite existing file: {path}")
    placeholder = "REPLACE_WITH_LOWERCASE_SHA256"
    spec = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "proposed_by": proposed_by,
        "mode": "research_only",
        "hypothesis": "REPLACE with a falsifiable, one-factor hypothesis",
        "baseline": {
            "strategy_id": "v13-v10-g",
            "code_sha256": placeholder,
            "config_sha256": placeholder,
            "source_manifest_sha256": placeholder,
        },
        "challenger": {
            "name": "REPLACE",
            "code_sha256": placeholder,
            "config_sha256": placeholder,
            "one_factor_change": "REPLACE with exactly one controlled change",
        },
        "windows": {
            "development": {"start": "2026-01-01", "end": "2026-03-31"},
            "validation": {"start": "2026-04-01", "end": "2026-05-31"},
            "final_test": {"start": "2026-06-01", "end": "2026-06-30"},
        },
        "execution_assumptions": {
            "fees_bps": 5.0,
            "slippage_bps": 5.0,
            "liquidity_model": "REPLACE with executable-size and fill assumptions",
            "capital_rupees": 1500000,
        },
        "trial_budget": {
            "max_variants": 1,
            "max_runs": 3,
            "max_compute_minutes": 240,
        },
        "decision_rules": {
            "primary_metric": "net_profit_rupees_after_costs",
            "minimum_improvement": 0,
            "maximum_drawdown_degradation_rupees": 0,
            "minimum_forward_shadow_sessions": 20,
        },
        "data_policy": {
            "point_in_time_only": True,
            "include_failed_variants": True,
            "final_test_locked": True,
        },
    }
    ExperimentRegistry._atomic_json(path, spec)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("research_outputs/observability_experiments"),
        help="Isolated registry root (default: research_outputs/observability_experiments)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="Create an empty registry")
    scaffold = commands.add_parser("scaffold", help="Create an unregistered spec template")
    scaffold.add_argument("--output", type=Path, required=True)
    scaffold.add_argument("--experiment-id", required=True)
    scaffold.add_argument("--proposed-by", required=True)

    validate = commands.add_parser("validate", help="Validate a spec without registering it")
    validate.add_argument("--spec", type=Path, required=True)

    register = commands.add_parser("register", help="Register an immutable experiment spec")
    register.add_argument("--spec", type=Path, required=True)
    register.add_argument("--actor")

    result = commands.add_parser("record-result", help="Record an immutable result manifest")
    result.add_argument("--experiment-id", required=True)
    result.add_argument("--result", type=Path, required=True)
    result.add_argument("--actor", required=True)

    decision = commands.add_parser("decide", help="Record a governed review decision")
    decision.add_argument("--experiment-id", required=True)
    decision.add_argument("--decision", required=True)
    decision.add_argument("--actor", required=True)
    decision.add_argument("--reason", required=True)

    status = commands.add_parser("status", help="Show one experiment state")
    status.add_argument("--experiment-id", required=True)
    commands.add_parser("list", help="List all experiment states")
    commands.add_parser("verify", help="Verify journal chain and artifact hashes")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    registry = ExperimentRegistry(args.root)
    try:
        if args.command == "init":
            registry.initialize()
            output: Any = {"ok": True, "root": str(registry.root)}
        elif args.command == "scaffold":
            scaffold_spec(args.output, args.experiment_id, args.proposed_by)
            output = {"ok": True, "spec": str(args.output), "registered": False}
        elif args.command == "validate":
            validated = validate_spec(load_json(args.spec))
            output = {
                "ok": True,
                "experiment_id": validated["experiment_id"],
                "spec_sha256": sha256_bytes(canonical_json(validated)),
            }
        elif args.command == "register":
            output = registry.register(args.spec, args.actor)
        elif args.command == "record-result":
            output = registry.record_result(args.experiment_id, args.result, args.actor)
        elif args.command == "decide":
            output = registry.decide(
                args.experiment_id, args.decision, args.actor, args.reason
            )
        elif args.command == "status":
            output = registry.status(args.experiment_id)
        elif args.command == "list":
            output = registry.list_statuses()
        elif args.command == "verify":
            output = registry.verify()
        else:  # pragma: no cover - argparse prevents this branch
            parser.error(f"unknown command {args.command}")
            return 2
    except RegistryError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
