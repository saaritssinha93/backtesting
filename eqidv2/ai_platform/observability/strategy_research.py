"""Read-only V13-V10-G strategy research and prediction observability.

The bundle built here is deliberately separated from the live strategy.  It
reads an already-published historical run, produces immutable evidence reports,
and never imports an executor, calls a broker, changes strategy configuration,
or places an order.  Predictions are expanding-window empirical estimates made
from prior trading days only; they are diagnostics, not trading instructions.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean, median, pstdev
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from ai_platform.observability.improvement_opportunities import (
    build_improvement_opportunities,
    render_improvement_opportunities,
)


IST = ZoneInfo("Asia/Kolkata")
SCHEMA_VERSION = "eqidv2.v13_v10_g.strategy_research_bundle.v1"
PREDICTION_SCHEMA_VERSION = "eqidv2.v13_v10_g.prior_only_prediction.v1"
EXPLORATORY_EVIDENCE = "EXPLORATORY_REUSED_HISTORY_NO_UNTOUCHED_TEST"
EXECUTION_RESEARCH_SCHEMA_VERSION = "eqidv2.v13_v10_g.execution_research.v1"
SHADOW_SESSION_SCHEMA_VERSION = "eqidv2.v13_v10_g.prospective_shadow_session.v1"
SHADOW_PREPARED_SCHEMA_VERSION = "eqidv2.v13_v10_g.prospective_shadow_prepared.v1"
SHADOW_DECISIONS_SCHEMA_VERSION = "eqidv2.v13_v10_g.shadow_decisions.v1"
SHADOW_OUTCOMES_SCHEMA_VERSION = "eqidv2.v13_v10_g.shadow_outcomes.v1"

REPORT_FILES: dict[str, str] = {
    "fno_v13_v10_g_research_data_quality":
        "latest_fno_v13_v10_g_research_data_quality.md",
    "fno_v13_v10_g_research_dataset":
        "latest_fno_v13_v10_g_research_dataset.md",
    "fno_v13_v10_g_research_baseline":
        "latest_fno_v13_v10_g_research_baseline.md",
    "fno_v13_v10_g_research_attribution":
        "latest_fno_v13_v10_g_research_attribution.md",
    "fno_v13_v10_g_research_regimes":
        "latest_fno_v13_v10_g_research_regimes.md",
    "fno_v13_v10_g_research_predictions":
        "latest_fno_v13_v10_g_research_predictions.md",
    "fno_v13_v10_g_research_walkforward":
        "latest_fno_v13_v10_g_research_walkforward.md",
    "fno_v13_v10_g_research_shadow":
        "latest_fno_v13_v10_g_research_shadow.md",
}

OBSERVABILITY_REPORT_FILES: dict[str, str] = {
    "fno_v13_v10_g_observability_market_regime":
        "latest_fno_v13_v10_g_observability_market_regime.md",
    "fno_v13_v10_g_observability_selection_funnel":
        "latest_fno_v13_v10_g_observability_selection_funnel.md",
    "fno_v13_v10_g_observability_entry_execution":
        "latest_fno_v13_v10_g_observability_entry_execution.md",
    "fno_v13_v10_g_observability_live_finalized_drift":
        "latest_fno_v13_v10_g_observability_live_finalized_drift.md",
    "fno_v13_v10_g_observability_pnl_attribution":
        "latest_fno_v13_v10_g_observability_pnl_attribution.md",
    "fno_v13_v10_g_observability_regime_profitability":
        "latest_fno_v13_v10_g_observability_regime_profitability.md",
}

IMPROVEMENT_REPORT_FILES: dict[str, str] = {
    "fno_v13_v10_g_research_improvements":
        "latest_fno_v13_v10_g_research_improvements.md",
}

ALL_REPORT_FILES: dict[str, str] = {
    **REPORT_FILES,
    **OBSERVABILITY_REPORT_FILES,
    **IMPROVEMENT_REPORT_FILES,
}

REQUIRED_SOURCE_FILES: tuple[str, ...] = (
    "dataset/dataset_manifest.json",
    "dataset/source_session_eligibility.csv",
    "dataset/setup_audit.parquet",
    "g_backtest/summary.json",
    "g_backtest/run_metadata.json",
    "g_backtest/portfolio_trades.csv",
    "g_backtest/data_coverage_audit.json",
)

DECISION_TIME_FIELDS: tuple[str, ...] = (
    "side",
    "setup_id",
    "nifty_first_bar_return_pct",
    "oi_change_pct",
    "volume_ratio",
    "traded_value",
    "v9_5m_range_pct",
    "v9_5m_distance_vwap_pct",
    "v9_5m_ema_spread_pct",
    "v9_1m_volume_ratio",
    "body_ratio",
    "wick_ratio",
)


@dataclass(frozen=True)
class ResearchBundle:
    run_id: str
    source_run: Path
    run_dir: Path
    latest_dir: Path
    manifest_path: Path
    state: str
    source_through_day: str
    selected_orders: int
    executed_trades: int
    eligible_predictions: int
    shadow_sessions: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_run": str(self.source_run),
            "run_dir": str(self.run_dir),
            "latest_dir": str(self.latest_dir),
            "manifest_path": str(self.manifest_path),
            "state": self.state,
            "source_through_day": self.source_through_day,
            "selected_orders": self.selected_orders,
            "executed_trades": self.executed_trades,
            "eligible_predictions": self.eligible_predictions,
            "shadow_sessions": self.shadow_sessions,
            "execution_authority": False,
            "live_configuration_changed": False,
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _atomic_text(path: Path, text: str) -> None:
    _atomic_bytes(path, text.encode("utf-8"))


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_text(
        path,
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        + "\n",
    )


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def _canonical_sha256(value: Any) -> str:
    """Match the canonical content hash used by execution research manifests."""
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def load_verified_execution_research(
    execution_root: Path | None,
    *,
    source_run: Path,
) -> dict[str, Any]:
    """Load only the hash-verified ``latest`` execution-research publication."""

    unavailable = {
        "state": "UNAVAILABLE",
        "run_id": None,
        "generated_at_utc": None,
        "conclusion": "UNAVAILABLE",
        "scenarios": [],
        "paper_calibration": {},
        "missed_entry_counterfactuals": {},
        "verified_artifacts": {},
        "verified_source_artifacts": 0,
        "immutable_run_verified": False,
        "execution_authority": False,
        "safe_for_live_selection": False,
    }
    if not execution_root or not execution_root.is_dir():
        return unavailable
    root = execution_root.resolve()
    latest = root / "latest"
    manifest_path = latest / "manifest.json"
    if not manifest_path.is_file():
        return unavailable
    try:
        manifest = _json(manifest_path)
        declared_content_hash = str(manifest.get("content_sha256") or "")
        unsigned = dict(manifest)
        unsigned.pop("content_sha256", None)
        if (
            manifest.get("schema_version") != EXECUTION_RESEARCH_SCHEMA_VERSION
            or manifest.get("mode") != "READ_ONLY_RESEARCH"
            or manifest.get("execution_authority") is not False
            or manifest.get("live_configuration_changed") is not False
            or manifest.get("safe_for_live_selection") is not False
            or manifest.get("conclusion") != "INSUFFICIENT_EVIDENCE_FOR_LIVE_CHANGE"
            or len(declared_content_hash) != 64
            or _canonical_sha256(unsigned) != declared_content_hash
        ):
            raise ValueError("execution research manifest contract/hash mismatch")
        declared_source = Path(str(manifest.get("source_run") or "")).resolve()
        if declared_source != source_run.resolve():
            raise ValueError("execution research belongs to a different source run")
        source_metadata = _json(source_run / "g_backtest/run_metadata.json")
        if str(manifest.get("source_through_day") or "") != str(
            source_metadata.get("through_day") or ""
        ):
            raise ValueError("execution research source cutoff mismatch")
        run_id = str(manifest.get("run_id") or "")
        immutable_run = (root / "runs" / run_id).resolve()
        if (
            not run_id
            or (root / "runs").resolve() not in immutable_run.parents
            or not (immutable_run / "manifest.json").is_file()
            or sha256_file(immutable_run / "manifest.json")
            != sha256_file(manifest_path)
        ):
            raise ValueError("execution research latest/immutable-run parity failed")
        reconciliation = manifest.get("frozen_baseline_reconciliation")
        if not isinstance(reconciliation, dict) or not reconciliation or not all(
            value is True for value in reconciliation.values()
        ):
            raise ValueError("execution research baseline is not reconciled")

        verified_artifacts: dict[str, dict[str, Any]] = {
            "manifest.json": {
                "bytes": manifest_path.stat().st_size,
                "sha256": sha256_file(manifest_path),
            }
        }
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict) or set(artifacts) != {
            "report", "scenarios", "missed_entries"
        }:
            raise ValueError("execution research artifact manifest is empty")
        expected_filenames = {
            "report": "latest_fno_v13_v10_g_execution_realism.md",
            "scenarios": "v13_v10_g_execution_scenarios.csv",
            "missed_entries": "v13_v10_g_missed_entry_counterfactuals.csv",
        }
        if any(
            not isinstance(artifacts[key], dict)
            or str(artifacts[key].get("filename") or "") != filename
            for key, filename in expected_filenames.items()
        ):
            raise ValueError("execution research artifact filename contract failed")
        for record in artifacts.values():
            if not isinstance(record, dict):
                raise ValueError("invalid execution research artifact record")
            filename = str(record.get("filename") or "")
            artifact_path = (latest / filename).resolve()
            if (
                not filename
                or latest.resolve() not in artifact_path.parents
                or not artifact_path.is_file()
                or sha256_file(artifact_path) != str(record.get("sha256") or "")
                or not (immutable_run / filename).is_file()
                or sha256_file(immutable_run / filename)
                != str(record.get("sha256") or "")
            ):
                raise ValueError(f"execution research artifact verification failed: {filename}")
            relative = artifact_path.relative_to(latest.resolve()).as_posix()
            verified_artifacts[relative] = {
                "bytes": artifact_path.stat().st_size,
                "sha256": sha256_file(artifact_path),
            }

        source_artifacts = manifest.get("source_artifacts")
        if not isinstance(source_artifacts, dict) or not source_artifacts:
            raise ValueError("execution research source artifact manifest is empty")
        core_source_artifacts = {
            "g_backtest/portfolio_trades.csv",
            "dataset/paths.npz",
            "dataset/dataset_manifest.json",
            "g_backtest/run_metadata.json",
        }
        if not core_source_artifacts.issubset(source_artifacts):
            raise ValueError("execution research core source artifacts are incomplete")
        verified_source_artifacts = 0
        for relative, record in source_artifacts.items():
            if not isinstance(record, dict):
                raise ValueError("invalid execution research source record")
            declared_path = str(record.get("path") or "")
            path = (
                Path(declared_path).resolve()
                if declared_path
                else (source_run.resolve() / str(relative)).resolve()
            )
            if relative in core_source_artifacts and path != (
                source_run.resolve() / relative
            ).resolve():
                raise ValueError(f"execution research core source path mismatch: {relative}")
            if (
                not path.is_file()
                or sha256_file(path) != str(record.get("sha256") or "")
            ):
                raise ValueError(f"execution research source verification failed: {relative}")
            verified_source_artifacts += 1

        scenarios = manifest.get("scenarios")
        if not isinstance(scenarios, list) or not scenarios:
            raise ValueError("execution research scenarios are missing")
        names: set[str] = set()
        for row in scenarios:
            if not isinstance(row, dict) or not str(row.get("scenario") or ""):
                raise ValueError("invalid execution research scenario")
            name = str(row["scenario"])
            if name in names or row.get("safe_for_live_selection") is not False:
                raise ValueError("execution research scenario safety contract failed")
            names.add(name)
        if "FROZEN_BASELINE" not in names:
            raise ValueError("execution research frozen baseline is missing")

        scenarios_record = artifacts.get("scenarios", {})
        scenarios_path = latest / str(scenarios_record.get("filename") or "")
        with scenarios_path.open("r", encoding="utf-8-sig", newline="") as handle:
            csv_scenarios = list(csv.DictReader(handle))
        if [str(row.get("scenario") or "") for row in csv_scenarios] != [
            str(row.get("scenario") or "") for row in scenarios
        ]:
            raise ValueError("execution research scenario CSV/manifest mismatch")
        integer_fields = {
            "delay_bars", "selected_orders", "mechanical_fills", "executed_trades",
            "wins", "losses",
        }
        numeric_fields = {
            "distance_proxy_bps", "win_rate_pct", "gross_profit_rupees",
            "cost_rupees", "net_profit_rupees", "profit_factor",
            "daily_close_drawdown_rupees", "net_delta_vs_baseline_rupees",
        }
        string_fields = {"scenario", "evidence_quality", "interpretation"}
        for csv_row, manifest_row in zip(csv_scenarios, scenarios):
            for field in integer_fields:
                try:
                    matches = int(csv_row.get(field, "")) == int(manifest_row.get(field))
                except (TypeError, ValueError):
                    matches = False
                if not matches:
                    raise ValueError(f"execution scenario field mismatch: {field}")
            for field in numeric_fields:
                left, right = _float(csv_row.get(field)), _float(manifest_row.get(field))
                if left is None and right is None:
                    continue
                if left is None or right is None or not math.isclose(
                    left, right, rel_tol=1e-12, abs_tol=1e-9
                ):
                    raise ValueError(f"execution scenario field mismatch: {field}")
            for field in string_fields:
                if str(csv_row.get(field) or "") != str(manifest_row.get(field) or ""):
                    raise ValueError(f"execution scenario field mismatch: {field}")
            if _bool(csv_row.get("safe_for_live_selection")) is not False:
                raise ValueError("execution scenario CSV has an unsafe selection flag")

        missed_summary = manifest.get("missed_entry_counterfactuals")
        if (
            not isinstance(missed_summary, dict)
            or missed_summary.get("safe_for_selection") is not False
        ):
            raise ValueError("missed-entry safety contract failed")
        missed_record = artifacts["missed_entries"]
        missed_path = latest / str(missed_record.get("filename") or "")
        with missed_path.open("r", encoding="utf-8-sig", newline="") as handle:
            missed_rows = list(csv.DictReader(handle))
        if (
            int(missed_summary.get("rows", -1)) != len(missed_rows)
            or int(missed_summary.get("late_trigger_touches", -1))
            != sum(_bool(row.get("post_expiry_trigger_touched")) for row in missed_rows)
            or any(_bool(row.get("safe_for_selection")) for row in missed_rows)
        ):
            raise ValueError("missed-entry CSV/manifest mismatch")

        return {
            "state": "READY_VERIFIED",
            "run_id": run_id,
            "generated_at_utc": manifest.get("generated_at_utc"),
            "source_through_day": manifest.get("source_through_day"),
            "conclusion": str(manifest.get("conclusion") or "UNAVAILABLE"),
            "scenarios": scenarios,
            "paper_calibration": manifest.get("paper_calibration", {}),
            "missed_entry_counterfactuals": manifest.get(
                "missed_entry_counterfactuals", {}
            ),
            "verified_artifacts": verified_artifacts,
            "verified_source_artifacts": verified_source_artifacts,
            "immutable_run_verified": True,
            "execution_authority": False,
            "safe_for_live_selection": False,
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return {
            **unavailable,
            "state": "INVALID",
            "reason": str(exc),
        }


def _float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    return parsed.astimezone(timezone.utc)


def _fmt(value: Any, digits: int = 2) -> str:
    number = _float(value)
    if number is None:
        return "UNAVAILABLE"
    return f"{number:,.{digits}f}"


def _markdown_table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    escaped_rows = []
    for row in rows:
        escaped_rows.append(
            "| " + " | ".join(str(value).replace("|", "\\|") for value in row) + " |"
        )
    return "\n".join(
        [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *escaped_rows,
        ]
    )


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", errors="strict", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        return [dict(row) for row in reader]


def _slot_text(value: Any) -> str:
    text = str(value or "").strip().replace(":", "")
    if text.isdigit():
        return text.zfill(4)
    return text


def _selection_identity(row: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("session_date") or row.get("day") or ""),
        _slot_text(row.get("signal_end") or row.get("hhmm")),
        str(row.get("tradingsymbol") or row.get("symbol") or ""),
        str(row.get("side") or "").upper(),
        str(row.get("setup_id") or ""),
    )


def _countish(value: Any) -> int:
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    number = _float(value)
    return int(number) if number is not None else 0


def _elapsed_seconds(start: Any, end: Any) -> float | None:
    left = _timestamp(start)
    right = _timestamp(end)
    if left is None or right is None or right < left:
        return None
    return (right - left).total_seconds()


def _numeric_summary(values: Iterable[Any]) -> dict[str, float | int | None]:
    clean = [value for value in (_float(item) for item in values) if value is not None]
    return {
        "count": len(clean),
        "mean": fmean(clean) if clean else None,
        "median": median(clean) if clean else None,
        "maximum": max(clean) if clean else None,
    }


def _order_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    filled_rows = [row for row in rows if str(row.get("entry_at_ist") or "").strip()]
    statuses = Counter(str(row.get("status") or "UNAVAILABLE") for row in rows)
    status_reasons = Counter(
        str(row.get("status_reason") or "UNAVAILABLE") for row in rows
    )
    activation: list[float | None] = []
    for row in rows:
        day = str(row.get("session_date") or "")
        confirmation = str(row.get("confirmation_end") or "")
        confirmation_ts = (
            f"{day}T{confirmation}:00+05:30"
            if day and len(confirmation) == 5 and ":" in confirmation
            else row.get("created_at_ist")
        )
        activation.append(
            _elapsed_seconds(confirmation_ts, row.get("entry_order_activated_at_ist"))
        )
    trigger = [
        _elapsed_seconds(row.get("entry_order_activated_at_ist"), row.get("entry_at_ist"))
        for row in rows
    ]
    trigger_distance_bps: list[float] = []
    for row in filled_rows:
        requested = _float(row.get("trigger_price"))
        actual = _float(row.get("entry_price"))
        if requested is None or actual is None or requested <= 0:
            continue
        side = str(row.get("side") or "").upper()
        signed = actual - requested if side == "LONG" else requested - actual
        trigger_distance_bps.append(signed * 10_000.0 / requested)
    return {
        "orders": len(rows),
        "filled": len(filled_rows),
        "fill_ratio_pct": len(filled_rows) * 100.0 / len(rows) if rows else None,
        "closed": statuses.get("CLOSED", 0),
        "cancelled": statuses.get("CANCELLED", 0),
        "status_counts": dict(sorted(statuses.items())),
        "status_reason_counts": dict(status_reasons.most_common()),
        "activation_latency_seconds": _numeric_summary(activation),
        "trigger_latency_seconds": _numeric_summary(trigger),
        "trigger_to_fill_bps": _numeric_summary(trigger_distance_bps),
        "gross_pnl_rupees": sum(_float(row.get("gross_pnl_rs")) or 0.0 for row in rows),
        "cost_rupees": sum(_float(row.get("estimated_cost_rs")) or 0.0 for row in rows),
        "net_pnl_rupees": sum(_float(row.get("net_pnl_rs")) or 0.0 for row in rows),
    }


def collect_operational_observability(
    *,
    live_root: Path | None,
    replay_root: Path | None,
    historical_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Read persisted live/replay evidence without importing or calling workers."""

    source_artifacts: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    def record(path: Path, *, root: Path, prefix: str) -> None:
        try:
            relative = path.resolve().relative_to(root.resolve()).as_posix()
            source_artifacts[f"{prefix}/{relative}"] = {
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        except (OSError, ValueError) as exc:
            errors.append(f"{prefix}:{path.name}:{type(exc).__name__}")

    def objects(paths: Sequence[Path], *, root: Path, prefix: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for path in paths:
            try:
                result.append(_json(path))
                record(path, root=root, prefix=prefix)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(f"{prefix}:{path.name}:{type(exc).__name__}")
        return result

    live_root = live_root.resolve() if live_root and live_root.is_dir() else None
    replay_root = replay_root.resolve() if replay_root and replay_root.is_dir() else None
    scanner_days = (
        sorted(path.name for path in (live_root / "scanner_5m").iterdir() if path.is_dir())
        if live_root and (live_root / "scanner_5m").is_dir()
        else []
    )
    latest_day = scanner_days[-1] if scanner_days else None
    scanners: list[dict[str, Any]] = []
    confirmations: list[dict[str, Any]] = []
    live_signals: list[dict[str, Any]] = []
    selection_slots: list[dict[str, Any]] = []
    gate_failures: Counter[str] = Counter()
    confirmation_gate_failures: Counter[str] = Counter()
    market: dict[str, Any] = {
        "state": "UNAVAILABLE",
        "vix": None,
        "realised_volatility": None,
        "reason": "No persisted scanner feature ledger was found.",
    }

    if live_root and latest_day:
        scanner_paths = sorted((live_root / "scanner_5m" / latest_day).glob("slot_*.json"))
        confirmation_dir = live_root / "confirmation_1m" / latest_day
        confirmation_paths = sorted(confirmation_dir.glob("slot_*.json")) if confirmation_dir.is_dir() else []
        signal_dir = live_root / "signals" / latest_day
        signal_paths = sorted(signal_dir.glob("*.json")) if signal_dir.is_dir() else []
        scanners = objects(scanner_paths, root=live_root, prefix="live")
        confirmations = objects(confirmation_paths, root=live_root, prefix="live")
        live_signals = objects(signal_paths, root=live_root, prefix="live")
        confirmation_by_slot = {
            _slot_text(item.get("signal_end")): item for item in confirmations
        }
        for item in scanners:
            evaluations = [
                row for row in item.get("feature_evaluations", [])
                if isinstance(row, dict)
            ]
            for row in evaluations:
                gate = str(row.get("first_failed_gate") or "PASS")
                gate_failures[gate] += 1
            slot = _slot_text(item.get("signal_end"))
            confirmation = confirmation_by_slot.get(slot, {})
            confirmation_evaluations = [
                row for row in confirmation.get("feature_evaluations", [])
                if isinstance(row, dict)
            ]
            for row in confirmation_evaluations:
                gate = str(row.get("first_failed_gate") or "PASS")
                confirmation_gate_failures[gate] += 1
            selected = _countish(confirmation.get("selected_signal_ids"))
            if not selected:
                selected = _countish(confirmation.get("selected_long")) + _countish(
                    confirmation.get("selected_short")
                )
            selection_slots.append({
                "slot": slot,
                "universe": _countish(item.get("feature_evaluation_count")) or len(evaluations),
                "strict_pass": sum(_bool(row.get("strict_signal_pass")) for row in evaluations),
                "scanner_candidates": _countish(item.get("candidates")),
                "confirmation_candidates": _countish(confirmation.get("candidate_count")),
                "directional_pass": sum(
                    _bool(row.get("gate_confirmation_direction"))
                    for row in confirmation_evaluations
                ),
                "confirmation_accepted": sum(
                    _bool(row.get("gate_confirmation_direction"))
                    and _bool(row.get("gate_confirmation_volume"))
                    for row in confirmation_evaluations
                ),
                "setup_pass": sum(
                    _bool(row.get("setup_filter_pass"))
                    for row in confirmation_evaluations
                ),
                "selected": selected,
            })
        if scanners:
            current = scanners[-1]
            evaluations = [
                row for row in current.get("feature_evaluations", [])
                if isinstance(row, dict)
            ]
            price_changes = [
                value for value in (_float(row.get("price_change_pct")) for row in evaluations)
                if value is not None
            ]
            oi_changes = [
                value for value in (_float(row.get("oi_change_pct")) for row in evaluations)
                if value is not None
            ]
            all_day_evaluations = [
                row
                for scanner in scanners
                for row in scanner.get("feature_evaluations", [])
                if isinstance(row, dict)
            ]
            nifty_values = [
                value for value in (
                    _float(row.get("nifty_first_bar_return_pct"))
                    for row in all_day_evaluations
                ) if value is not None
            ]
            nifty = median(nifty_values) if nifty_values else None
            direction = (
                "BULLISH" if nifty is not None and nifty >= 0.15
                else "BEARISH" if nifty is not None and nifty <= -0.15
                else "RANGE" if nifty is not None
                else "UNKNOWN"
            )
            market = {
                "state": "READY",
                "session_date": latest_day,
                "slot": _slot_text(current.get("signal_end")),
                "published_at_ist": current.get("published_at_ist"),
                "direction_regime": direction,
                "nifty_first_bar_return_pct": nifty,
                "breadth_total": len(price_changes),
                "breadth_up": sum(value > 0 for value in price_changes),
                "breadth_down": sum(value < 0 for value in price_changes),
                "breadth_flat": sum(value == 0 for value in price_changes),
                "dispersion_pct": pstdev(price_changes) if len(price_changes) > 1 else None,
                "oi_rows": len(oi_changes),
                "oi_positive": sum(value > 0 for value in oi_changes),
                "oi_participation_pct": (
                    sum(value > 0 for value in oi_changes) * 100.0 / len(oi_changes)
                    if oi_changes else None
                ),
                "vix": None,
                "realised_volatility": None,
                "reason": (
                    "VIX and a time-series realised-volatility measure are not present "
                    "in the verified scanner snapshot; dispersion is reported separately."
                ),
            }

    order_rows: dict[str, list[dict[str, Any]]] = {"PAPER": [], "LIVE": []}
    live_event_first_reasons: dict[str, str] = {}
    live_event_last_reasons: dict[str, str] = {}
    live_event_count = 0
    live_event_invalid = 0
    broker_reconciliation: dict[str, Any] = {
        "state": "UNAVAILABLE",
        "broker_truth_available": False,
        "scope_complete": False,
        "mismatch_count": None,
        "active_order_parity_complete": False,
        "active_order_mismatch_count": None,
        "observed_at_ist": None,
    }
    if live_root:
        order_roots = {
            "PAPER": live_root / "orders" / "PAPER",
            "LIVE": live_root / "orders" / "LIVE" / "live_kite_qty1",
        }
        for mode, root in order_roots.items():
            paths = sorted(root.rglob("*.json")) if root.is_dir() else []
            order_rows[mode] = objects(paths, root=live_root, prefix="live")

        # Order-state files are mutable snapshots.  The append-only transition
        # journal preserves the first failure that a later retry/expiry may
        # otherwise mask, so surface both views instead of treating the final
        # status_reason as root-cause truth.
        event_root = live_root / "order_events" / "LIVE"
        event_paths = sorted(event_root.glob("*.jsonl")) if event_root.is_dir() else []
        for event_path in event_paths:
            record(event_path, root=live_root, prefix="live")
            try:
                event_lines = event_path.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError) as exc:
                errors.append(f"live:{event_path.name}:{type(exc).__name__}")
                continue
            for line in event_lines:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                    context = event.get("context", {})
                    data = event.get("data", {})
                    if not isinstance(context, dict) or not isinstance(data, dict):
                        raise ValueError("invalid order-event shape")
                    signal_id = str(context.get("signal_id") or "").strip()
                    reason = str(data.get("reason") or "").strip()
                    live_event_count += 1
                    if signal_id and reason:
                        live_event_first_reasons.setdefault(signal_id, reason)
                        live_event_last_reasons[signal_id] = reason
                except (TypeError, ValueError, json.JSONDecodeError):
                    live_event_invalid += 1

        # This is current, read-only broker/local parity—not realized broker
        # P&L.  Keep that distinction explicit in every downstream report.
        live_status_path = live_root / "live_kite" / "status.json"
        if live_status_path.is_file():
            try:
                live_status = _json(live_status_path)
                record(live_status_path, root=live_root, prefix="live")
                child = dict(
                    dict(live_status.get("children") or {}).get(
                        "broker_reconciliation"
                    )
                    or {}
                )
                truth = child.get("broker_truth_available") is True
                complete = child.get("scope_complete") is True
                active_complete = child.get("active_order_parity_complete") is True
                broker_reconciliation = {
                    "state": (
                        "READY_POSITION_AND_ACTIVE_ORDER_PARITY"
                        if truth and complete and active_complete
                        else "PARTIAL"
                    ),
                    "broker_truth_available": truth,
                    "scope_complete": complete,
                    "mismatch_count": child.get("mismatch_count"),
                    "active_order_parity_complete": active_complete,
                    "active_order_mismatch_count": child.get(
                        "active_order_mismatch_count"
                    ),
                    "observed_at_ist": child.get("updated_at_ist")
                    or live_status.get("updated_at_ist"),
                }
            except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc:
                errors.append(f"live:status.json:{type(exc).__name__}")

    historical_confirmation_latency = _numeric_summary(
        _elapsed_seconds(row.get("signal_ts"), row.get("confirmation_ts"))
        for row in historical_rows
    )
    historical_trigger_latency = _numeric_summary(
        _elapsed_seconds(row.get("confirmation_ts"), row.get("entry_ts"))
        for row in historical_rows if _bool(row.get("filled"))
    )
    historical_overshoot = _numeric_summary(
        row.get("entry_overshoot_bps") for row in historical_rows if _bool(row.get("filled"))
    )
    unfilled_mfe = _numeric_summary(
        row.get("mfe_pct") for row in historical_rows if not _bool(row.get("filled"))
    )
    live_metrics = _order_metrics(order_rows["LIVE"])
    live_metrics.update({
        "transition_journal_events": live_event_count,
        "transition_journal_invalid_events": live_event_invalid,
        "first_observed_reason_counts": dict(
            Counter(live_event_first_reasons.values()).most_common()
        ),
        "last_observed_reason_counts": dict(
            Counter(live_event_last_reasons.values()).most_common()
        ),
        "first_reason_signal_coverage": len(live_event_first_reasons),
    })
    execution = {
        "historical": {
            "orders": len(historical_rows),
            "filled": sum(_bool(row.get("filled")) for row in historical_rows),
            "fill_ratio_pct": (
                sum(_bool(row.get("filled")) for row in historical_rows)
                * 100.0 / len(historical_rows) if historical_rows else None
            ),
            "activation_latency_seconds": historical_confirmation_latency,
            "trigger_latency_seconds": historical_trigger_latency,
            "entry_overshoot_bps": historical_overshoot,
            "missed_entry_mfe_pct": unfilled_mfe,
        },
        "PAPER": _order_metrics(order_rows["PAPER"]),
        "LIVE": live_metrics,
        "broker_reconciliation": broker_reconciliation,
    }

    drift_rows: list[dict[str, Any]] = []
    replay_portfolio_rows: list[dict[str, str]] = []
    if live_root and replay_root:
        signals_root = live_root / "signals"
        for day_dir in sorted(path for path in replay_root.iterdir() if path.is_dir()):
            live_day_dir = signals_root / day_dir.name
            if not live_day_dir.is_dir():
                continue
            completed = sorted(
                run for run in day_dir.iterdir()
                if run.is_dir()
                and (run / "selected_orders.csv").is_file()
                and (run / "portfolio_trades.csv").is_file()
            )
            if not completed:
                continue
            replay_run = completed[-1]
            live_paths = sorted(live_day_dir.glob("*.json"))
            live_rows_for_day = objects(live_paths, root=live_root, prefix="live")
            selected_path = replay_run / "selected_orders.csv"
            portfolio_path = replay_run / "portfolio_trades.csv"
            try:
                finalized = _csv_rows(selected_path)
                replay_portfolio_rows.extend(_csv_rows(portfolio_path))
                record(selected_path, root=replay_root, prefix="finalized_replay")
                record(portfolio_path, root=replay_root, prefix="finalized_replay")
            except (OSError, UnicodeError, csv.Error) as exc:
                errors.append(f"finalized_replay:{day_dir.name}:{type(exc).__name__}")
                continue
            live_map = {_selection_identity(row): row for row in live_rows_for_day}
            final_map = {_selection_identity(row): row for row in finalized}
            live_keys = set(live_map)
            final_keys = set(final_map)
            common = live_keys & final_keys
            price_changes = oi_changes = indicator_changes = 0
            for key in common:
                live_row = live_map[key]
                final_row = final_map[key]
                for fields, bucket in (
                    (("signal_close", "price_change_pct", "volume_ratio", "body_ratio", "wick_ratio"), "price"),
                    (("oi", "prev_oi", "oi_change_pct"), "oi"),
                    (("ema9", "ema20", "ema50"), "indicator"),
                ):
                    changed = any(
                        _float(live_row.get(field)) is not None
                        and _float(final_row.get(field)) is not None
                        and not math.isclose(
                            float(_float(live_row.get(field))),
                            float(_float(final_row.get(field))),
                            rel_tol=1e-9,
                            abs_tol=1e-9,
                        )
                        for field in fields
                    )
                    if changed and bucket == "price":
                        price_changes += 1
                    elif changed and bucket == "oi":
                        oi_changes += 1
                    elif changed:
                        indicator_changes += 1
            changed_candidates = len(live_keys - final_keys) + len(final_keys - live_keys)
            first_divergence = "UNAVAILABLE_NO_STAGE_BUNDLES"
            drift_rows.append({
                "day": day_dir.name,
                "live_selected": len(live_keys),
                "finalized_selected": len(final_keys),
                "common": len(common),
                "live_only": len(live_keys - final_keys),
                "finalized_only": len(final_keys - live_keys),
                "changed_candidates": changed_candidates,
                "price_or_bar_changes": price_changes,
                "oi_changes": oi_changes,
                "indicator_changes": indicator_changes,
                "first_divergence": first_divergence,
            })

    historical_metrics = performance(historical_rows)
    replay_metrics = performance(replay_portfolio_rows)
    replay_metrics.update({
        "gross_profit_rupees": sum(
            _float(row.get("portfolio_gross_profit_rupees")) or 0.0
            for row in replay_portfolio_rows if _bool(row.get("portfolio_executed"))
        ),
        "cost_rupees": sum(
            _float(row.get("portfolio_cost_rupees")) or 0.0
            for row in replay_portfolio_rows if _bool(row.get("portfolio_executed"))
        ),
    })
    pnl = {
        "full_historical_replay": {
            **historical_metrics,
            "gross_profit_rupees": sum(
                _float(row.get("portfolio_gross_profit_rupees")) or 0.0
                for row in historical_rows if _bool(row.get("portfolio_executed"))
            ),
            "cost_rupees": sum(
                _float(row.get("portfolio_cost_rupees")) or 0.0
                for row in historical_rows if _bool(row.get("portfolio_executed"))
            ),
        },
        "recent_finalized_replay": replay_metrics,
        "paper_observed": execution["PAPER"],
        "live_local": execution["LIVE"],
        "broker": {
            **broker_reconciliation,
            "reason": (
                "Current persisted broker reconciliation proves scoped position/active-order "
                "parity only; it does not contain fills, charges or realized P&L attribution."
            ),
        },
    }
    return {
        "state": "READY" if live_root else "UNAVAILABLE",
        "latest_live_day": latest_day,
        "market": market,
        "selection_slots": selection_slots,
        "gate_failures": dict(gate_failures.most_common(15)),
        "confirmation_gate_failures": dict(
            confirmation_gate_failures.most_common(15)
        ),
        "selected_signals": [
            {
                "slot": _slot_text(row.get("signal_end")),
                "symbol": row.get("tradingsymbol"),
                "side": row.get("side"),
                "setup": row.get("setup_id"),
                "rank": row.get("rank_within_scan"),
            }
            for row in live_signals
        ],
        "execution": execution,
        "broker_reconciliation": broker_reconciliation,
        "drift": drift_rows,
        "pnl": pnl,
        "source_artifacts": dict(sorted(source_artifacts.items())),
        "errors": errors,
    }


def discover_source_run(source_root: Path) -> Path:
    """Return the newest complete historical run, never an incomplete fallback."""

    candidates: list[tuple[float, Path]] = []
    if source_root.is_dir() and all((source_root / rel).is_file() for rel in REQUIRED_SOURCE_FILES):
        candidates.append((source_root.stat().st_mtime, source_root))
    if source_root.is_dir():
        for candidate in source_root.glob("run_*"):
            if candidate.is_dir() and all(
                (candidate / rel).is_file() for rel in REQUIRED_SOURCE_FILES
            ):
                candidates.append((candidate.stat().st_mtime, candidate))
    if not candidates:
        raise FileNotFoundError(
            f"no complete V13-V10-G source run beneath {source_root}"
        )
    return max(candidates, key=lambda item: (item[0], item[1].name))[1].resolve()


def load_portfolio_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", errors="strict", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"portfolio CSV has no header: {path}")
        missing = {"day", "tradingsymbol", "side", "setup_id", "filled"} - set(
            reader.fieldnames
        )
        if missing:
            raise ValueError(f"portfolio CSV missing fields: {', '.join(sorted(missing))}")
        return [dict(row) for row in reader]


def classify_regime(row: Mapping[str, Any]) -> dict[str, str]:
    """Classify a row with fixed, decision-time-only rules.

    Thresholds are engineering buckets, not fitted trading parameters.  They
    are intentionally fixed so this report cannot optimize itself on outcomes.
    """

    nifty = _float(row.get("nifty_first_bar_return_pct"))
    side = str(row.get("side", "")).upper()
    market = (
        "BULLISH" if nifty is not None and nifty >= 0.15
        else "BEARISH" if nifty is not None and nifty <= -0.15
        else "RANGE" if nifty is not None
        else "UNKNOWN"
    )
    alignment = (
        "ALIGNED"
        if (side == "LONG" and market == "BULLISH")
        or (side == "SHORT" and market == "BEARISH")
        else "ADVERSE"
        if (side == "LONG" and market == "BEARISH")
        or (side == "SHORT" and market == "BULLISH")
        else "NEUTRAL"
        if market == "RANGE"
        else "UNKNOWN"
    )
    bar_range = _float(row.get("v9_5m_range_pct"))
    volatility = (
        "HIGH" if bar_range is not None and bar_range >= 0.80
        else "LOW" if bar_range is not None and bar_range < 0.40
        else "NORMAL" if bar_range is not None
        else "UNKNOWN"
    )
    oi_change = _float(row.get("oi_change_pct"))
    oi = (
        "ELEVATED" if oi_change is not None and oi_change >= 0.50
        else "NORMAL" if oi_change is not None and oi_change >= 0.10
        else "WEAK" if oi_change is not None
        else "UNKNOWN"
    )
    traded_value = _float(row.get("traded_value"))
    liquidity = (
        "HIGH" if traded_value is not None and traded_value >= 100_000_000
        else "STANDARD" if traded_value is not None and traded_value >= 25_000_000
        else "THIN" if traded_value is not None
        else "UNKNOWN"
    )
    return {
        "market_regime": market,
        "side_alignment": alignment,
        "volatility_regime": volatility,
        "oi_regime": oi,
        "liquidity_regime": liquidity,
    }


def point_in_time_audit(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    checks = Counter()
    violations: list[dict[str, str]] = []
    missing_timestamps = Counter()
    for row in rows:
        signal = _timestamp(row.get("signal_ts"))
        confirmation = _timestamp(row.get("confirmation_ts"))
        entry = _timestamp(row.get("entry_ts"))
        feature_5m = _timestamp(row.get("v9_5m_feature_ts"))
        feature_1m = _timestamp(row.get("v9_1m_feature_ts"))
        available = _timestamp(row.get("v9_feature_available_ts"))
        identity = {
            "day": str(row.get("day", "")),
            "symbol": str(row.get("tradingsymbol", "")),
            "setup": str(row.get("setup_id", "")),
        }

        pairs = (
            ("signal_before_confirmation", signal, confirmation),
            ("5m_feature_by_signal", feature_5m, signal),
            ("1m_feature_by_confirmation", feature_1m, confirmation),
            ("feature_available_by_confirmation", available, confirmation),
        )
        for name, left, right in pairs:
            if left is None or right is None:
                missing_timestamps[name] += 1
                continue
            checks[name] += 1
            if left > right:
                violations.append({**identity, "check": name})
        if entry is not None and confirmation is not None:
            checks["confirmation_before_entry"] += 1
            if confirmation > entry:
                violations.append({**identity, "check": "confirmation_before_entry"})

    return {
        "rows": len(rows),
        "checks": dict(sorted(checks.items())),
        "missing_timestamps": dict(sorted(missing_timestamps.items())),
        "violations": violations,
        "violation_count": len(violations),
        "decision_time_fields": list(DECISION_TIME_FIELDS),
        "outcome_fields_used_for_regime": [],
    }


def _row_return_pct(row: Mapping[str, Any]) -> float:
    explicit = _float(row.get("net_return_on_capital_pct"))
    if explicit is not None:
        return explicit
    pnl = _float(row.get("portfolio_net_profit_rupees")) or 0.0
    capital = _float(row.get("portfolio_trade_capital_rupees"))
    return pnl * 100.0 / capital if capital and capital > 0 else 0.0


def _row_pnl(row: Mapping[str, Any]) -> float:
    return _float(row.get("portfolio_net_profit_rupees")) or 0.0


def performance(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    selected = len(rows)
    filled = sum(_bool(row.get("filled")) for row in rows)
    executed_rows = [row for row in rows if _bool(row.get("portfolio_executed"))]
    pnl = [_row_pnl(row) for row in executed_rows]
    wins = sum(value > 0 for value in pnl)
    losses = sum(value < 0 for value in pnl)
    positive = sum(value for value in pnl if value > 0)
    negative = -sum(value for value in pnl if value < 0)
    equity = peak = drawdown = 0.0
    for value in pnl:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return {
        "selected_orders": selected,
        "filled_orders": filled,
        "executed_trades": len(executed_rows),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": wins * 100.0 / len(executed_rows) if executed_rows else None,
        "net_profit_rupees": sum(pnl),
        "profit_factor": positive / negative if negative > 0 else None,
        "maximum_drawdown_rupees": abs(drawdown),
        "average_net_return_on_capital_pct": (
            fmean(_row_return_pct(row) for row in executed_rows)
            if executed_rows else None
        ),
    }


def _group_performance(
    rows: Sequence[Mapping[str, Any]], key: str
) -> list[tuple[str, dict[str, Any]]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(key, "") or "UNAVAILABLE")].append(row)
    return [(name, performance(group)) for name, group in sorted(groups.items())]


def _audit_summary(path: Path) -> dict[str, Any]:
    try:
        import pandas as pd
    except ImportError:
        return {"state": "UNAVAILABLE", "reason": "pandas is not installed"}
    columns = [
        "day", "selection_status", "causal_rejection_reasons",
        "baseline_setup_eligible", "baseline_selected",
        "all_causal_filters_pass", "setup_thresholds_pass",
    ]
    try:
        frame = pd.read_parquet(path, columns=columns)
    except Exception as exc:  # pyarrow/backend failures remain explicit
        return {"state": "UNAVAILABLE", "reason": f"{type(exc).__name__}: {exc}"}
    statuses = Counter(str(value) for value in frame["selection_status"].fillna("UNAVAILABLE"))
    reasons: Counter[str] = Counter()
    for value in frame["causal_rejection_reasons"].fillna(""):
        for reason in str(value).split("|"):
            if reason:
                reasons[reason] += 1
    days = sorted({str(value)[:10] for value in frame["day"].dropna().astype(str)})
    return {
        "state": "READY",
        "rows": int(len(frame)),
        "sessions": len(days),
        "first_day": days[0] if days else None,
        "last_day": days[-1] if days else None,
        "selection_status": dict(statuses.most_common()),
        "top_rejection_reasons": dict(reasons.most_common(15)),
        "causal_filter_pass": int(frame["all_causal_filters_pass"].fillna(False).sum()),
        "setup_threshold_pass": int(frame["setup_thresholds_pass"].fillna(False).sum()),
        "baseline_eligible": int(frame["baseline_setup_eligible"].fillna(False).sum()),
        "baseline_selected": int(frame["baseline_selected"].fillna(False).sum()),
    }


def _context_key(row: Mapping[str, Any], level: str) -> tuple[str, ...]:
    regime = classify_regime(row)
    if level == "setup_regime":
        return (
            str(row.get("setup_id", "")), str(row.get("side", "")),
            regime["side_alignment"], regime["volatility_regime"],
        )
    if level == "setup_side":
        return (str(row.get("setup_id", "")), str(row.get("side", "")))
    if level == "side":
        return (str(row.get("side", "")),)
    return ("ALL",)


def _actual_stop_before_target(row: Mapping[str, Any]) -> bool | None:
    if not _bool(row.get("filled")) or _bool(row.get("same_bar_ambiguous")):
        return None
    target = _bool(row.get("target_hit"))
    stop = _bool(row.get("stop_hit"))
    if target and stop:
        return None
    return stop and not target


def prior_only_predictions(
    rows: Sequence[Mapping[str, Any]],
    *,
    minimum_history: int = 20,
    minimum_context: int = 5,
) -> list[dict[str, Any]]:
    """Build expanding-window predictions using prior days only.

    All rows from a day are scored before any outcome from that day enters the
    history.  This prevents both row-order leakage and same-session leakage.
    """

    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda row: (
            str(row.get("day", "")), str(row.get("confirmation_ts", "")),
            str(row.get("tradingsymbol", "")),
        ),
    )
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ordered:
        by_day[str(row.get("day", ""))].append(row)
    history: list[dict[str, Any]] = []
    result: list[dict[str, Any]] = []
    for day in sorted(by_day):
        day_rows = by_day[day]
        for row in day_rows:
            prediction: dict[str, Any] = {
                "schema_version": PREDICTION_SCHEMA_VERSION,
                "day": day,
                "symbol": str(row.get("tradingsymbol", "")),
                "side": str(row.get("side", "")),
                "setup_id": str(row.get("setup_id", "")),
                "signal_ts": str(row.get("signal_ts", "")),
                "confirmation_ts": str(row.get("confirmation_ts", "")),
                **classify_regime(row),
                "training_sessions": len({str(item.get("day", "")) for item in history}),
                "training_rows": len(history),
                "training_cutoff_day": max(
                    (str(item.get("day", "")) for item in history), default=None
                ),
                "actual_filled": _bool(row.get("filled")),
                "actual_stop_before_target": _actual_stop_before_target(row),
                "actual_net_return_on_capital_pct": _row_return_pct(row),
                "actual_mfe_pct": _float(row.get("mfe_pct")),
                "actual_mae_pct": _float(row.get("mae_pct")),
            }
            if len(history) < minimum_history:
                prediction.update(
                    prediction_state="INSUFFICIENT_PRIOR_HISTORY",
                    context_level=None,
                    context_samples=0,
                    fill_probability=None,
                    stop_before_target_probability=None,
                    expected_net_return_on_capital_pct=None,
                    expected_mfe_pct=None,
                    expected_mae_pct=None,
                )
                result.append(prediction)
                continue

            context_rows: list[dict[str, Any]] = []
            context_level = "global"
            for level in ("setup_regime", "setup_side", "side", "global"):
                wanted = _context_key(row, level)
                matching = [item for item in history if _context_key(item, level) == wanted]
                if len(matching) >= minimum_context or level == "global":
                    context_rows = matching
                    context_level = level
                    break
            fills = [item for item in context_rows if _bool(item.get("filled"))]
            clear_stops = [
                value for item in fills
                if (value := _actual_stop_before_target(item)) is not None
            ]
            mfe = [
                value for item in fills
                if (value := _float(item.get("mfe_pct"))) is not None
            ]
            mae = [
                value for item in fills
                if (value := _float(item.get("mae_pct"))) is not None
            ]
            prediction.update(
                prediction_state="RESEARCH_ESTIMATE",
                context_level=context_level,
                context_samples=len(context_rows),
                fill_probability=(len(fills) + 1) / (len(context_rows) + 2),
                stop_before_target_probability=(
                    (sum(clear_stops) + 1) / (len(clear_stops) + 2)
                    if clear_stops else None
                ),
                expected_net_return_on_capital_pct=fmean(
                    _row_return_pct(item) for item in context_rows
                ),
                expected_mfe_pct=fmean(mfe) if mfe else None,
                expected_mae_pct=fmean(mae) if mae else None,
            )
            result.append(prediction)
        history.extend(day_rows)
    return result


def evaluate_predictions(predictions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    eligible = [
        row for row in predictions
        if row.get("prediction_state") == "RESEARCH_ESTIMATE"
    ]
    fill_errors = [
        (float(row["fill_probability"]) - (1.0 if row["actual_filled"] else 0.0)) ** 2
        for row in eligible if row.get("fill_probability") is not None
    ]
    stop_errors = [
        (float(row["stop_before_target_probability"])
         - (1.0 if row["actual_stop_before_target"] else 0.0)) ** 2
        for row in eligible
        if row.get("stop_before_target_probability") is not None
        and row.get("actual_stop_before_target") is not None
    ]
    return_errors = [
        abs(float(row["expected_net_return_on_capital_pct"])
            - float(row["actual_net_return_on_capital_pct"]))
        for row in eligible
        if row.get("expected_net_return_on_capital_pct") is not None
        and row.get("actual_net_return_on_capital_pct") is not None
    ]
    predicted_positive = [
        row for row in eligible
        if _float(row.get("expected_net_return_on_capital_pct")) is not None
        and float(row["expected_net_return_on_capital_pct"]) > 0
    ]
    return {
        "total_rows": len(predictions),
        "eligible_predictions": len(eligible),
        "coverage_pct": len(eligible) * 100.0 / len(predictions) if predictions else 0.0,
        "fill_brier_score": fmean(fill_errors) if fill_errors else None,
        "stop_brier_score": fmean(stop_errors) if stop_errors else None,
        "net_return_mae_pct_points": fmean(return_errors) if return_errors else None,
        "predicted_positive_rows": len(predicted_positive),
        "predicted_positive_actual_net_return_pct_sum": sum(
            _float(row.get("actual_net_return_on_capital_pct")) or 0.0
            for row in predicted_positive
        ),
        "evidence_quality": EXPLORATORY_EVIDENCE,
        "research_conclusion": "INSUFFICIENT_EVIDENCE",
        "safe_for_live_selection": False,
    }


def _write_prediction_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fieldnames = [
        "schema_version", "day", "symbol", "side", "setup_id", "signal_ts",
        "confirmation_ts", "market_regime", "side_alignment", "volatility_regime",
        "oi_regime", "liquidity_regime", "training_sessions", "training_rows",
        "training_cutoff_day", "prediction_state", "context_level", "context_samples",
        "fill_probability", "stop_before_target_probability",
        "expected_net_return_on_capital_pct", "expected_mfe_pct", "expected_mae_pct",
        "actual_filled", "actual_stop_before_target",
        "actual_net_return_on_capital_pct", "actual_mfe_pct", "actual_mae_pct",
    ]
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _registry_summary(registry_root: Path) -> dict[str, Any]:
    journal = registry_root / "registry.jsonl"
    if not journal.is_file():
        return {
            "state": "EMPTY", "events": 0, "verified_chain": True,
            "manual_live_review_candidates": 0,
            "manual_candidate_bound_to_shadow_cohort": False,
        }
    previous = "0" * 64
    events = 0
    candidates = 0
    try:
        for line_number, line in enumerate(
            journal.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            event = json.loads(line)
            observed_hash = str(event.get("event_hash", ""))
            unsigned = {key: value for key, value in event.items() if key != "event_hash"}
            expected_hash = hashlib.sha256(
                json.dumps(
                    unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
            ).hexdigest()
            if (
                observed_hash != expected_hash
                or event.get("previous_event_hash") != previous
                or int(event.get("sequence", -1)) != events + 1
            ):
                raise ValueError(f"registry chain mismatch at line {line_number}")
            if (
                event.get("event_type") == "DECISION"
                and event.get("payload", {}).get("decision")
                == "CANDIDATE_FOR_MANUAL_LIVE_REVIEW"
            ):
                candidates += 1
            previous = observed_hash
            events += 1
    except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc:
        return {
            "state": "INVALID", "events": events, "verified_chain": False,
            "manual_live_review_candidates": 0, "reason": str(exc),
            "manual_candidate_bound_to_shadow_cohort": False,
        }
    return {
        "state": "READY" if events else "EMPTY",
        "events": events,
        "verified_chain": True,
        "manual_live_review_candidates": candidates,
        # The current registry schema records an experiment decision, but no
        # strategy/model cohort hash. Do not treat an unrelated historical
        # decision as approval of the active shadow cohort.
        "manual_candidate_bound_to_shadow_cohort": False,
        "last_event_hash": previous,
    }


def _shadow_artifact(
    record: Mapping[str, Any],
    *,
    session_root: Path,
    verified: dict[str, dict[str, Any]],
) -> Path:
    raw_path = str(record.get("path") or record.get("captured_path") or "")
    path = Path(raw_path).resolve()
    expected = str(record.get("sha256") or "")
    if (
        not raw_path
        or session_root.resolve() not in path.parents
        or not path.is_file()
        or len(expected) != 64
        or sha256_file(path) != expected
    ):
        raise ValueError("shadow artifact hash/path mismatch")
    relative = path.relative_to(session_root.parent.resolve()).as_posix()
    verified[relative] = {"bytes": path.stat().st_size, "sha256": expected}
    return path


def _shadow_timestamp(
    value: Any,
    *,
    field: str,
    session_date: str,
    require_session_date: bool = True,
) -> datetime:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid shadow {field}") from exc
    if stamp.tzinfo is None or (
        require_session_date
        and stamp.astimezone(IST).date().isoformat() != session_date
    ):
        raise ValueError(f"shadow {field} is outside its session")
    return stamp.astimezone(IST)


def _verified_shadow_state(
    session_root: Path,
    *,
    journal_rows: Sequence[Mapping[str, Any]],
    verified: dict[str, dict[str, Any]],
) -> tuple[str, str]:
    session_date = session_root.name
    prepared_path = session_root / "prepared_manifest.json"
    if not prepared_path.is_file():
        raise ValueError("prepared manifest missing")
    prepared = _json(prepared_path)
    if (
        prepared.get("schema_version") != SHADOW_PREPARED_SCHEMA_VERSION
        or prepared.get("mode") != "PROSPECTIVE_SHADOW"
        or prepared.get("execution_authority") is not False
        or prepared.get("broker_access") is not False
        or prepared.get("completed") is not False
        or prepared.get("session_date") != session_date
    ):
        raise ValueError("prepared manifest contract mismatch")
    prepared_artifacts = prepared.get("artifacts")
    if not isinstance(prepared_artifacts, dict):
        raise ValueError("prepared artifact manifest missing")
    for name, field in (
        ("dataset", "dataset_sha256"),
        ("model", "model_sha256"),
        ("strategy", "strategy_fingerprint"),
    ):
        record = prepared_artifacts.get(name)
        if not isinstance(record, dict):
            raise ValueError(f"prepared {name} artifact missing")
        _shadow_artifact(record, session_root=session_root, verified=verified)
        if record.get("sha256") != prepared.get(field):
            raise ValueError(f"prepared {name} identity mismatch")
    verified[prepared_path.relative_to(session_root.parent).as_posix()] = {
        "bytes": prepared_path.stat().st_size,
        "sha256": sha256_file(prepared_path),
    }
    prepared_at = _shadow_timestamp(
        prepared.get("prepared_at_ist"),
        field="prepared_at_ist",
        session_date=session_date,
    )
    if (prepared_at.hour, prepared_at.minute, prepared_at.second) >= (9, 15, 0):
        raise ValueError("shadow inputs were not prepared before market open")
    cohort = _canonical_sha256({
        "strategy_fingerprint": prepared.get("strategy_fingerprint"),
        "model_sha256": prepared.get("model_sha256"),
    })

    seal_path = session_root / "decision_seal.json"
    if not seal_path.is_file():
        return "PREPARED", cohort
    seal = _json(seal_path)
    if (
        seal.get("schema_version") != SHADOW_DECISIONS_SCHEMA_VERSION
        or seal.get("mode") != "PROSPECTIVE_SHADOW"
        or seal.get("execution_authority") is not False
        or seal.get("complete") is not True
        or seal.get("session_date") != session_date
        or seal.get("dataset_sha256") != prepared.get("dataset_sha256")
        or seal.get("model_sha256") != prepared.get("model_sha256")
        or seal.get("strategy_fingerprint") != prepared.get("strategy_fingerprint")
    ):
        raise ValueError("decision seal contract mismatch")
    sealed_at = _shadow_timestamp(
        seal.get("sealed_at_ist"),
        field="sealed_at_ist",
        session_date=session_date,
    )
    if sealed_at < prepared_at:
        raise ValueError("shadow decisions were sealed before preparation")
    if (sealed_at.hour, sealed_at.minute, sealed_at.second) >= (15, 30, 0):
        raise ValueError("shadow decisions were not sealed before market close")
    decision_record = seal.get("bundle")
    if not isinstance(decision_record, dict):
        raise ValueError("sealed decision bundle missing")
    decision_path = _shadow_artifact(
        decision_record, session_root=session_root, verified=verified
    )
    decisions = _json(decision_path)
    if (
        decisions.get("schema_version") != SHADOW_DECISIONS_SCHEMA_VERSION
        or decisions.get("execution_authority") is not False
        or decisions.get("complete") is not True
        or decisions.get("session_date") != session_date
        or decisions.get("dataset_sha256") != prepared.get("dataset_sha256")
        or decisions.get("model_sha256") != prepared.get("model_sha256")
        or decisions.get("strategy_fingerprint") != prepared.get("strategy_fingerprint")
        or not isinstance(decisions.get("rows"), list)
    ):
        raise ValueError("sealed decision bundle contract mismatch")
    decision_rows = decisions["rows"]
    decision_ids = [str(row.get("signal_id") or "") for row in decision_rows if isinstance(row, dict)]
    if (
        len(decision_ids) != len(decision_rows)
        or any(not value for value in decision_ids)
        or len(set(decision_ids)) != len(decision_ids)
        or int(seal.get("decision_rows", -1)) != len(decision_rows)
        or seal.get("signal_ids_sha256")
        != hashlib.sha256("\n".join(sorted(decision_ids)).encode("utf-8")).hexdigest()
    ):
        raise ValueError("sealed decision row identity mismatch")
    decision_times: dict[str, datetime] = {}
    for row in decision_rows:
        stamp = _shadow_timestamp(
            row.get("decision_at_ist"),
            field="decision_at_ist",
            session_date=session_date,
        )
        if stamp > sealed_at:
            raise ValueError("shadow decision timestamp follows its seal")
        decision_times[str(row["signal_id"])] = stamp
    verified[seal_path.relative_to(session_root.parent).as_posix()] = {
        "bytes": seal_path.stat().st_size,
        "sha256": sha256_file(seal_path),
    }

    manifest_path = session_root / "manifest.json"
    if not manifest_path.is_file():
        return "DECISIONS_SEALED", cohort
    manifest = _json(manifest_path)
    if (
        manifest.get("schema_version") != SHADOW_SESSION_SCHEMA_VERSION
        or manifest.get("mode") != "PROSPECTIVE_SHADOW"
        or manifest.get("execution_authority") is not False
        or manifest.get("broker_access") is not False
        or manifest.get("completed") is not True
        or manifest.get("outcome_join_state") != "COMPLETE"
        or manifest.get("session_date") != session_date
        or manifest.get("dataset_sha256") != prepared.get("dataset_sha256")
        or manifest.get("model_sha256") != prepared.get("model_sha256")
        or manifest.get("strategy_fingerprint") != prepared.get("strategy_fingerprint")
    ):
        raise ValueError("completed shadow manifest contract mismatch")
    completed_at = _shadow_timestamp(
        manifest.get("completed_at_ist"),
        field="completed_at_ist",
        session_date=session_date,
        require_session_date=False,
    )
    if completed_at < sealed_at:
        raise ValueError("shadow session completed before decisions were sealed")
    session_close = datetime.fromisoformat(f"{session_date}T15:30:00+05:30").astimezone(IST)
    if completed_at < session_close:
        raise ValueError("shadow session completed before market close")
    final_artifacts = manifest.get("artifacts")
    if not isinstance(final_artifacts, dict) or set(final_artifacts) != {
        "prepared_manifest", "decision_seal", "decision_bundle", "outcome_bundle"
    }:
        raise ValueError("completed shadow artifacts missing")
    final_paths: dict[str, Path] = {}
    for name, record in final_artifacts.items():
        if not isinstance(record, dict):
            raise ValueError("invalid completed shadow artifact record")
        final_paths[name] = _shadow_artifact(
            record, session_root=session_root, verified=verified
        )
    if (
        final_paths["prepared_manifest"] != prepared_path.resolve()
        or final_paths["decision_seal"] != seal_path.resolve()
        or final_paths["decision_bundle"] != decision_path.resolve()
    ):
        raise ValueError("completed shadow lifecycle artifact identity mismatch")
    outcomes = _json(final_paths["outcome_bundle"])
    if (
        outcomes.get("schema_version") != SHADOW_OUTCOMES_SCHEMA_VERSION
        or outcomes.get("execution_authority") is not False
        or outcomes.get("complete") is not True
        or outcomes.get("session_date") != session_date
        or not isinstance(outcomes.get("rows"), list)
    ):
        raise ValueError("completed shadow outcome bundle contract mismatch")
    outcome_rows = outcomes["rows"]
    outcome_ids = [str(row.get("signal_id") or "") for row in outcome_rows if isinstance(row, dict)]
    if (
        len(outcome_ids) != len(outcome_rows)
        or any(not value for value in outcome_ids)
        or len(set(outcome_ids)) != len(outcome_ids)
        or set(outcome_ids) != set(decision_ids)
        or int(manifest.get("decision_rows", -1)) != len(decision_rows)
        or int(manifest.get("outcome_rows", -1)) != len(outcome_rows)
    ):
        raise ValueError("completed shadow decision/outcome identity mismatch")
    for row in outcome_rows:
        outcome_at = _shadow_timestamp(
            row.get("outcome_at_ist"),
            field="outcome_at_ist",
            session_date=session_date,
        )
        if outcome_at < decision_times[str(row["signal_id"])] or outcome_at > completed_at:
            raise ValueError("shadow outcome timestamp is outside decision/finalization order")
    manifest_sha = sha256_file(manifest_path)
    matching = [
        row
        for row in journal_rows
        if row.get("session_date") == session_date
        and Path(str(row.get("manifest_path") or "")).resolve()
        == manifest_path.resolve()
        and row.get("manifest_sha256") == manifest_sha
        and row.get("strategy_fingerprint") == manifest.get("strategy_fingerprint")
        and row.get("dataset_sha256") == manifest.get("dataset_sha256")
        and row.get("model_sha256") == manifest.get("model_sha256")
    ]
    if len(matching) != 1:
        raise ValueError("completed shadow manifest has no unique journal link")
    verified[manifest_path.relative_to(session_root.parent).as_posix()] = {
        "bytes": manifest_path.stat().st_size,
        "sha256": manifest_sha,
    }
    return "COMPLETE", cohort


def _active_shadow_cohort(
    session_statuses: Sequence[Mapping[str, str]],
) -> tuple[str | None, int, int]:
    completed_by_cohort = Counter(
        row["cohort_sha256"]
        for row in session_statuses
        if row.get("state") == "COMPLETE" and row.get("cohort_sha256")
    )
    latest_valid = next(
        (
            row for row in reversed(session_statuses)
            if row.get("state") != "INVALID" and row.get("cohort_sha256")
        ),
        None,
    )
    active_cohort = str(latest_valid["cohort_sha256"]) if latest_valid else None
    return active_cohort, completed_by_cohort.get(active_cohort, 0), len(completed_by_cohort)


def _shadow_promotion_gate(shadow: Mapping[str, Any]) -> bool:
    return (
        shadow.get("state") == "READY"
        and int(shadow.get("invalid_sessions", 0)) == 0
        and int(shadow.get("invalid_records", 0)) == 0
        and int(shadow.get("sessions", 0)) >= 20
    )


def _shadow_summary(path: Path, *, allowed_manifest_root: Path) -> dict[str, Any]:
    root = allowed_manifest_root.resolve()
    records = 0
    invalid_records = 0
    journal_rows: list[dict[str, Any]] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            records += 1
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("journal row is not an object")
                manifest_path = Path(str(row.get("manifest_path") or "")).resolve()
                valid = (
                    row.get("schema_version") == SHADOW_SESSION_SCHEMA_VERSION
                    and row.get("mode") == "PROSPECTIVE_SHADOW"
                    and row.get("completed") is True
                    and row.get("execution_authority") is False
                    and row.get("outcome_join_state") == "COMPLETE"
                    and bool(row.get("strategy_fingerprint"))
                    and bool(row.get("dataset_sha256"))
                    and bool(row.get("model_sha256"))
                    and bool(row.get("session_date"))
                    and root in manifest_path.parents
                    and len(str(row.get("manifest_sha256") or "")) == 64
                )
                if not valid:
                    raise ValueError("journal contract mismatch")
                journal_rows.append(row)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                invalid_records += 1

    verified_artifacts: dict[str, dict[str, Any]] = {}
    session_statuses: list[dict[str, str]] = []
    linked_sessions: set[str] = set()
    session_dirs = (
        sorted((item for item in root.iterdir() if item.is_dir()), key=lambda item: item.name)
        if root.is_dir()
        else []
    )
    for session_root in session_dirs:
        session_verified: dict[str, dict[str, Any]] = {}
        try:
            state, cohort = _verified_shadow_state(
                session_root,
                journal_rows=journal_rows,
                verified=session_verified,
            )
            verified_artifacts.update(session_verified)
            if state == "COMPLETE":
                linked_sessions.add(session_root.name)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            state = "INVALID"
            cohort = ""
        session_statuses.append({
            "session_date": session_root.name,
            "state": state,
            "cohort_sha256": cohort,
        })

    invalid_records += sum(
        1 for row in journal_rows if str(row.get("session_date")) not in linked_sessions
    )
    counts = Counter(row["state"] for row in session_statuses)
    active_cohort, cohort_sessions, completed_cohorts = _active_shadow_cohort(
        session_statuses
    )
    invalid_sessions = counts.get("INVALID", 0)
    latest = session_statuses[-1] if session_statuses else {}
    state = (
        "NOT_STARTED"
        if not session_statuses and records == 0
        else "PARTIAL"
        if invalid_records or invalid_sessions
        else "READY"
    )
    return {
        "state": state,
        "sessions": cohort_sessions,
        "completed_sessions": counts.get("COMPLETE", 0),
        "active_cohort_sha256": active_cohort,
        "active_cohort_sessions": cohort_sessions,
        "completed_cohorts": completed_cohorts,
        "prepared_sessions": counts.get("PREPARED", 0),
        "decisions_sealed_sessions": counts.get("DECISIONS_SEALED", 0),
        "invalid_sessions": invalid_sessions,
        "session_directories": len(session_statuses),
        "latest_session_date": latest.get("session_date"),
        "latest_session_state": latest.get("state", "NOT_STARTED"),
        "session_statuses": session_statuses,
        "records": records,
        "invalid_records": invalid_records,
        "verified_artifacts": verified_artifacts,
        "execution_authority": False,
    }


def _report_preamble(
    title: str, *, generated: str, run_id: str, source_run: Path, through_day: str
) -> list[str]:
    return [
        f"# {title}",
        "",
        f"- Generated (IST): `{generated}`",
        f"- Research run: `{run_id}`",
        f"- Source data through: `{through_day or 'UNAVAILABLE'}`",
        f"- Source run: `{source_run}`",
        "- Mode: **READ-ONLY RESEARCH**",
        "- Execution authority: **NO**",
        "- Live configuration changed: **NO**",
        "",
    ]


def generate_research_bundle(
    *,
    source_run: Path,
    output_root: Path,
    registry_root: Path | None = None,
    live_root: Path | None = None,
    replay_root: Path | None = None,
    execution_research_root: Path | None = None,
    now: datetime | None = None,
    minimum_history: int = 20,
    minimum_context: int = 5,
) -> ResearchBundle:
    """Publish research, observability and evidence-backed improvement proposals."""

    source_run = source_run.resolve()
    missing = [rel for rel in REQUIRED_SOURCE_FILES if not (source_run / rel).is_file()]
    if missing:
        raise FileNotFoundError("incomplete source run; missing: " + ", ".join(missing))
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    run_id = now_utc.strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = output_root.resolve() / "runs" / run_id
    latest_dir = output_root.resolve() / "latest"
    run_dir.mkdir(parents=True, exist_ok=False)

    dataset_manifest = _json(source_run / "dataset/dataset_manifest.json")
    summary = _json(source_run / "g_backtest/summary.json")
    metadata = _json(source_run / "g_backtest/run_metadata.json")
    coverage = _json(source_run / "g_backtest/data_coverage_audit.json")
    rows = load_portfolio_rows(source_run / "g_backtest/portfolio_trades.csv")
    rows = sorted(
        rows,
        key=lambda row: (
            str(row.get("day", "")), str(row.get("entry_ts", "")),
            str(row.get("confirmation_ts", "")), str(row.get("tradingsymbol", "")),
        ),
    )
    audit = _audit_summary(source_run / "dataset/setup_audit.parquet")
    pit = point_in_time_audit(rows)
    baseline = performance(rows)
    predictions = prior_only_predictions(
        rows, minimum_history=minimum_history, minimum_context=minimum_context
    )
    prediction_metrics = evaluate_predictions(predictions)
    through_day = str(metadata.get("through_day") or dataset_manifest.get("through_day") or "")
    evidence = str(summary.get("evidence") or summary.get("settings", {}).get("evidence") or "")
    eligibility_path = source_run / "dataset/source_session_eligibility.csv"
    with eligibility_path.open("r", encoding="utf-8-sig", newline="") as handle:
        eligibility_rows = list(csv.DictReader(handle))
    eligible_rows = [row for row in eligibility_rows if _bool(row.get("eligible"))]
    eligible_sessions = len(eligible_rows)
    post_cutoff_eligible_days = sorted({
        str(row.get("day", ""))
        for row in eligible_rows
        if through_day and str(row.get("day", "")) > through_day
    })
    eligible_sessions_through_cutoff = sum(
        not through_day or str(row.get("day", "")) <= through_day
        for row in eligible_rows
    )

    required_hashes = {
        rel: {"bytes": (source_run / rel).stat().st_size, "sha256": sha256_file(source_run / rel)}
        for rel in REQUIRED_SOURCE_FILES
    }
    source_missing = [
        str(item.get("path")) for item in dataset_manifest.get("sources", [])
        if isinstance(item, dict) and not item.get("exists", False)
    ]
    source_sessions = int(metadata.get("session_count", 0) or 0)
    declared = metadata.get("metrics", {}).get("full_history", {})
    source_daily_close_drawdown = abs(float(
        declared.get(
            "daily_close_drawdown_rupees",
            summary.get("maximum_drawdown_rupees", 0.0),
        )
        or 0.0
    ))
    reconciliation = {
        "selected_orders_match": int(declared.get("selected_orders", -1)) == baseline["selected_orders"],
        "trades_match": int(declared.get("trades", -1)) == baseline["executed_trades"],
        "net_profit_match": math.isclose(
            float(declared.get("net_profit_rupees", math.nan)),
            float(baseline["net_profit_rupees"]), rel_tol=1e-9, abs_tol=0.01,
        ),
    }

    grouped_by_day = dict(_group_performance(rows, "day"))
    completed_days = sorted({
        str(day)
        for day in dataset_manifest.get("days", [])
        if str(day) and (not through_day or str(day) <= through_day)
    })
    if not completed_days:
        completed_days = sorted(grouped_by_day)
    by_day = [
        (day, grouped_by_day.get(day, performance([])))
        for day in completed_days
    ]
    zero_selected_days = [
        day for day, metrics in by_day if metrics["selected_orders"] == 0
    ]
    by_side = _group_performance(rows, "side")
    by_setup = _group_performance(rows, "setup_id")
    by_slot = _group_performance(rows, "hhmm")
    regime_rows: list[dict[str, Any]] = []
    for row in rows:
        regime_rows.append({**row, **classify_regime(row)})
    by_alignment = _group_performance(regime_rows, "side_alignment")
    by_volatility = _group_performance(regime_rows, "volatility_regime")
    by_oi = _group_performance(regime_rows, "oi_regime")
    by_liquidity = _group_performance(regime_rows, "liquidity_regime")
    operational = collect_operational_observability(
        live_root=live_root,
        replay_root=replay_root,
        historical_rows=rows,
    )
    execution_research = load_verified_execution_research(
        execution_research_root,
        source_run=source_run,
    )

    registry = _registry_summary(
        (registry_root or Path("research_outputs/observability_experiments")).resolve()
    )
    shadow_path = output_root.resolve() / "shadow_observations.jsonl"
    shadow = _shadow_summary(
        shadow_path,
        allowed_manifest_root=(output_root.resolve() / "shadow_sessions"),
    )
    promotion_gates = {
        "complete_source_bundle": not missing,
        "all_manifest_sources_present": not source_missing,
        "eligibility_metadata_bounded_by_source_cutoff": not post_cutoff_eligible_days,
        "point_in_time_audit_pass": pit["violation_count"] == 0,
        "baseline_reconciled": all(reconciliation.values()),
        "untouched_holdout_evidence": evidence == "UNTOUCHED_HOLDOUT",
        "walk_forward_predictions_available": prediction_metrics["eligible_predictions"] >= 20,
        "minimum_20_prospective_shadow_sessions": _shadow_promotion_gate(shadow),
        "experiment_registry_chain_valid": bool(registry["verified_chain"]),
        "independent_manual_live_review_candidate": (
            registry["manual_live_review_candidates"] > 0
            and registry["manual_candidate_bound_to_shadow_cohort"]
        ),
    }
    promotion_ready = all(promotion_gates.values())
    data_quality_pass = (
        not source_missing
        and not post_cutoff_eligible_days
        and pit["violation_count"] == 0
    )
    generated = now_ist.isoformat(timespec="seconds")

    reports: dict[str, str] = {}

    lines = _report_preamble(
        "V13-V10-G Research Data Quality", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Gate: **{'PASS' if data_quality_pass else 'BLOCKED'}**",
        f"- Required artifacts: `{len(REQUIRED_SOURCE_FILES) - len(missing)}/{len(REQUIRED_SOURCE_FILES)}`",
        f"- Manifest-declared missing sources: `{len(source_missing)}`",
        f"- Eligible metadata rows after source cutoff: `{len(post_cutoff_eligible_days)}`",
        f"- Post-cutoff eligible days ignored: `{', '.join(post_cutoff_eligible_days) if post_cutoff_eligible_days else 'none'}`",
        f"- Point-in-time checks performed: `{sum(pit['checks'].values()):,}`",
        f"- Point-in-time violations: `{pit['violation_count']}`",
        f"- Outcome fields used for regime classification: `0`",
        "",
        "## Reproducibility hashes",
        "",
        _markdown_table(
            ("artifact", "bytes", "sha256"),
            ((name, info["bytes"], info["sha256"]) for name, info in required_hashes.items()),
        ),
        "",
        "## Timestamp audit",
        "",
        _markdown_table(
            ("check", "evaluated", "missing"),
            (
                (name, count, pit["missing_timestamps"].get(name, 0))
                for name, count in pit["checks"].items()
            ),
        ),
        "",
        "A pass proves ordering of the timestamps present in this result bundle. It does not prove profitable predictive power.",
    ]
    reports["fno_v13_v10_g_research_data_quality"] = "\n".join(lines) + "\n"

    lines = _report_preamble(
        "V13-V10-G Historical Research Dataset", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Dataset schema: `{dataset_manifest.get('schema', 'UNAVAILABLE')}`",
        f"- Dataset source fingerprint: `{dataset_manifest.get('source_fingerprint', 'UNAVAILABLE')}`",
        f"- Eligible strategy sessions in completed run: `{source_sessions}`",
        f"- Eligibility rows / eligible (raw): `{len(eligibility_rows)}` / `{eligible_sessions}`",
        f"- Eligible sessions at or before source cutoff: `{eligible_sessions_through_cutoff}`",
        f"- Eligible days after cutoff ignored: `{', '.join(post_cutoff_eligible_days) if post_cutoff_eligible_days else 'none'}`",
        f"- Decision-audit rows: `{audit.get('rows', 'UNAVAILABLE')}`",
        f"- Selected-order rows: `{len(rows)}`",
        f"- First / last completed session: `{metadata.get('first_session')}` / `{metadata.get('last_session')}`",
        "",
        "## Data contract",
        "",
        "The dataset contains equity 5-minute bars, exact confirmation 1-minute features, futures OI, NIFTY context, selection decisions and later outcomes. Prediction inputs are restricted to the declared decision-time fields. Outcomes are used only as labels after their session enters the historical training window.",
        "",
        "## Coverage for explicitly repaired sessions",
        "",
        _markdown_table(
            ("session", "equity 1m symbols", "equity 5m symbols", "futures OI contracts", "partial OI contracts"),
            (
                (
                    day,
                    detail.get("equity_1m", {}).get("symbols_present", "UNAVAILABLE"),
                    detail.get("equity_5m", {}).get("symbols_present", "UNAVAILABLE"),
                    detail.get("futures_oi_5m", {}).get("contracts_present", "UNAVAILABLE"),
                    len(detail.get("futures_oi_5m", {}).get("partial_contracts", {})),
                )
                for day, detail in sorted(coverage.get("dates", {}).items())
            ),
        ),
        "",
        "A present contract is not assumed to have full-bar coverage; partial OI contract counts are shown explicitly. Missing coverage is never converted to a zero return or a synthetic market observation.",
    ]
    reports["fno_v13_v10_g_research_dataset"] = "\n".join(lines) + "\n"

    lines = _report_preamble(
        "V13-V10-G Frozen Baseline Replay", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Evidence quality: **{evidence or 'UNAVAILABLE'}**",
        f"- Selected orders / executed trades: `{baseline['selected_orders']}` / `{baseline['executed_trades']}`",
        f"- Wins / losses: `{baseline['wins']}` / `{baseline['losses']}`",
        f"- Win rate: `{_fmt(baseline['win_rate_pct'])}%`",
        f"- Net profit after recorded costs: `INR {_fmt(baseline['net_profit_rupees'])}`",
        f"- Profit factor: `{_fmt(baseline['profit_factor'], 3)}`",
        f"- Maximum row-order closed-trade drawdown: `INR {_fmt(baseline['maximum_drawdown_rupees'])}`",
        f"- Source daily-close drawdown: `INR {_fmt(source_daily_close_drawdown)}`",
        f"- Baseline reconciliation checks: `{sum(reconciliation.values())}/{len(reconciliation)}`",
        f"- Completed sessions / zero-selection sessions: `{len(by_day)}` / `{len(zero_selected_days)}`",
        "",
        "## Day-wise results",
        "",
        _markdown_table(
            ("day", "selected", "trades", "wins", "losses", "net INR", "PF"),
            (
                (
                    name, metrics["selected_orders"], metrics["executed_trades"],
                    metrics["wins"], metrics["losses"],
                    _fmt(metrics["net_profit_rupees"]), _fmt(metrics["profit_factor"], 3),
                )
                for name, metrics in by_day
            ),
        ),
        "",
        "Positive historical P&L is descriptive only. The source itself states that this is reused exploratory history, not an untouched final test.",
    ]
    reports["fno_v13_v10_g_research_baseline"] = "\n".join(lines) + "\n"

    rejection_rows = list(audit.get("top_rejection_reasons", {}).items())
    lines = _report_preamble(
        "V13-V10-G Decision Attribution", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Full decision-audit rows: `{audit.get('rows', 'UNAVAILABLE')}`",
        f"- Causal-filter pass rows: `{audit.get('causal_filter_pass', 'UNAVAILABLE')}`",
        f"- Setup-threshold pass rows: `{audit.get('setup_threshold_pass', 'UNAVAILABLE')}`",
        f"- Baseline-selected rows: `{audit.get('baseline_selected', 'UNAVAILABLE')}`",
        f"- Selected / filled / executed: `{baseline['selected_orders']}` / `{baseline['filled_orders']}` / `{baseline['executed_trades']}`",
        "",
        "## Most frequent rejection reasons",
        "",
        _markdown_table(("reason", "rows"), rejection_rows or [("UNAVAILABLE", 0)]),
        "",
        "## Performance by side",
        "",
        _markdown_table(
            ("side", "selected", "trades", "win rate %", "net INR", "PF"),
            (
                (
                    name, metrics["selected_orders"], metrics["executed_trades"],
                    _fmt(metrics["win_rate_pct"]), _fmt(metrics["net_profit_rupees"]),
                    _fmt(metrics["profit_factor"], 3),
                )
                for name, metrics in by_side
            ),
        ),
        "",
        "## Performance by setup",
        "",
        _markdown_table(
            ("setup", "selected", "trades", "win rate %", "net INR", "PF"),
            (
                (
                    name, metrics["selected_orders"], metrics["executed_trades"],
                    _fmt(metrics["win_rate_pct"]), _fmt(metrics["net_profit_rupees"]),
                    _fmt(metrics["profit_factor"], 3),
                )
                for name, metrics in by_setup
            ),
        ),
        "",
        "These counts explain where the frozen strategy filtered and selected. They do not recommend relaxing any gate.",
    ]
    reports["fno_v13_v10_g_research_attribution"] = "\n".join(lines) + "\n"

    def regime_table(groups: Sequence[tuple[str, dict[str, Any]]]) -> str:
        return _markdown_table(
            ("regime", "selected", "trades", "win rate %", "net INR", "PF"),
            (
                (
                    name, metrics["selected_orders"], metrics["executed_trades"],
                    _fmt(metrics["win_rate_pct"]), _fmt(metrics["net_profit_rupees"]),
                    _fmt(metrics["profit_factor"], 3),
                )
                for name, metrics in groups
            ),
        )

    lines = _report_preamble(
        "V13-V10-G Market Regimes & Drift", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        "- Regimes use only values observable by the confirmation decision time.",
        "- Fixed buckets (not outcome-fitted): NIFTY +/-0.15%; 5m range 0.40%/0.80%; OI 0.10%/0.50%; traded value INR 25m/100m.",
        "- Outcome fields used to define a regime: `0`",
        "",
        "## Market-side alignment",
        "",
        regime_table(by_alignment),
        "",
        "## Five-minute volatility",
        "",
        regime_table(by_volatility),
        "",
        "## Futures OI participation",
        "",
        regime_table(by_oi),
        "",
        "## Liquidity",
        "",
        regime_table(by_liquidity),
        "",
        "Small regime slices are diagnostic leads only. A regime filter becomes a new strategy version and must pass preregistered walk-forward and prospective shadow gates.",
    ]
    reports["fno_v13_v10_g_research_regimes"] = "\n".join(lines) + "\n"

    latest_prediction_rows = predictions[-12:]
    lines = _report_preamble(
        "V13-V10-G Prediction Quality & Calibration", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        "- Model: transparent empirical estimates with Laplace smoothing; no parameter optimization.",
        "- Training rule: expanding window of **prior trading days only**; same-day outcomes are unavailable while scoring.",
        f"- Minimum prior selected orders / context rows: `{minimum_history}` / `{minimum_context}`",
        f"- Eligible predictions: `{prediction_metrics['eligible_predictions']}/{prediction_metrics['total_rows']}` (`{_fmt(prediction_metrics['coverage_pct'])}%`)",
        f"- Fill-probability Brier score (lower is better): `{_fmt(prediction_metrics['fill_brier_score'], 4)}`",
        f"- Stop-before-target Brier score: `{_fmt(prediction_metrics['stop_brier_score'], 4)}`",
        f"- Expected-return MAE: `{_fmt(prediction_metrics['net_return_mae_pct_points'], 4)}` percentage points",
        "- Research conclusion: **INSUFFICIENT_EVIDENCE**",
        "- Safe for live selection: **NO**",
        "",
        "## Latest chronological estimates",
        "",
        _markdown_table(
            ("day", "symbol", "side/setup", "context", "n", "fill p", "stop p", "expected net %", "actual net %"),
            (
                (
                    row["day"], row["symbol"], f"{row['side']}/{row['setup_id']}",
                    row.get("context_level") or row["prediction_state"], row.get("context_samples", 0),
                    _fmt(row.get("fill_probability"), 3),
                    _fmt(row.get("stop_before_target_probability"), 3),
                    _fmt(row.get("expected_net_return_on_capital_pct"), 3),
                    _fmt(row.get("actual_net_return_on_capital_pct"), 3),
                )
                for row in latest_prediction_rows
            ),
        ),
        "",
        "These are selected-order outcome estimates only. Ranked-out candidates do not yet have an independently verified counterfactual outcome corpus, so this report cannot train or recommend a new candidate-ranking policy. The probabilities describe this historical sample; they are not forecasts of guaranteed profit and are not consumed by the live executor.",
    ]
    reports["fno_v13_v10_g_research_predictions"] = "\n".join(lines) + "\n"

    wf_state = (
        "ELIGIBLE_FOR_INDEPENDENT_REVIEW"
        if evidence == "UNTOUCHED_HOLDOUT"
        and prediction_metrics["eligible_predictions"] >= 20
        else "INSUFFICIENT_EVIDENCE"
    )
    lines = _report_preamble(
        "V13-V10-G Walk-Forward & Holdout Evaluation", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Walk-forward state: **{wf_state}**",
        f"- Evidence quality: **{evidence or 'UNAVAILABLE'}**",
        f"- Eligible prior-only predictions: `{prediction_metrics['eligible_predictions']}`",
        f"- Prediction coverage: `{_fmt(prediction_metrics['coverage_pct'])}%`",
        f"- Baseline hash/result reconciliation: `{sum(reconciliation.values())}/{len(reconciliation)}`",
        "- Untouched final test available: **NO**" if evidence != "UNTOUCHED_HOLDOUT" else "- Untouched final test available: **YES**",
        (
            f"- Prospective shadow evidence included: **YES** "
            f"(`{shadow['sessions']}` verified sessions in one consistent cohort)"
            if shadow["sessions"]
            else "- Prospective shadow evidence included: **NO**"
        ),
        "",
        "## Gate interpretation",
        "",
        "The expanding-window calculation is causally ordered, but it evaluates a strategy and regime taxonomy on reused history. It can reveal instability and calibration problems; it cannot validate a profit-improving strategy change. Reserve a future date range before testing a challenger.",
        "",
        "## Required next experiment",
        "",
        "1. Register one falsifiable change and a trial budget before running it.",
        "2. Lock development, validation and untouched test windows.",
        "3. Include costs, slippage, unfilled orders and missing data.",
        "4. Compare against the frozen G baseline, then collect at least 20 prospective shadow sessions.",
    ]
    reports["fno_v13_v10_g_research_walkforward"] = "\n".join(lines) + "\n"

    blocked = [name for name, passed in promotion_gates.items() if not passed]
    lines = _report_preamble(
        "V13-V10-G Prospective Shadow & Promotion Gate", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Shadow state: **{shadow['state']}**",
        f"- Completed hash-verified prospective shadow sessions in the promotion cohort: `{shadow['sessions']}/20`",
        f"- Completed sessions across all cohorts: `{shadow['completed_sessions']}`; cohorts=`{shadow['completed_cohorts']}`.",
        f"- Current lifecycle: prepared=`{shadow['prepared_sessions']}`, decisions sealed=`{shadow['decisions_sealed_sessions']}`, invalid=`{shadow['invalid_sessions']}`.",
        f"- Latest session/state: `{shadow['latest_session_date'] or 'UNAVAILABLE'}` / **{shadow['latest_session_state']}**.",
        f"- Experiment registry: `{registry['state']}`; events=`{registry['events']}`; chain_verified=`{registry['verified_chain']}`",
        f"- Promotion gate: **{'MANUAL_REVIEW_REQUIRED' if promotion_ready else 'BLOCKED'}**",
        f"- Failed gates: `{', '.join(blocked) if blocked else 'none'}`",
        "- Automatic live promotion supported: **NO**",
        "- Live configuration changed: **NO**",
        "",
        "## Promotion checklist",
        "",
        _markdown_table(
            ("gate", "result"),
            ((name, "PASS" if passed else "BLOCK") for name, passed in promotion_gates.items()),
        ),
        "",
        "Even if every evidence gate passes, this tool can only create a candidate for independent manual review. It cannot change thresholds, sizing, stops, targets, schedules, broker orders or the live strategy.",
    ]
    reports["fno_v13_v10_g_research_shadow"] = "\n".join(lines) + "\n"

    market = operational["market"]
    lines = _report_preamble(
        "V13-V10-G Market Regime", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Latest persisted live session / slot: `{operational.get('latest_live_day') or 'UNAVAILABLE'}` / `{market.get('slot') or 'UNAVAILABLE'}`",
        f"- Current direction regime: **{market.get('direction_regime', 'UNAVAILABLE')}**",
        f"- NIFTY first-bar return: `{_fmt(market.get('nifty_first_bar_return_pct'), 4)}%`",
        f"- VIX: **UNAVAILABLE_NO_CURRENT_VERIFIED_SOURCE**",
        f"- Time-series realised volatility: **UNAVAILABLE_NOT_CAPTURED**",
        f"- Cross-sectional price-change dispersion: `{_fmt(market.get('dispersion_pct'), 4)}%`",
        f"- Breadth up / down / flat: `{market.get('breadth_up', 0)}` / `{market.get('breadth_down', 0)}` / `{market.get('breadth_flat', 0)}` of `{market.get('breadth_total', 0)}`",
        f"- Positive OI participation: `{_fmt(market.get('oi_participation_pct'))}%` (`{market.get('oi_positive', 0)}/{market.get('oi_rows', 0)}`)",
        f"- Snapshot published: `{market.get('published_at_ist') or 'UNAVAILABLE'}`",
        "",
        "## Interpretation",
        "",
        str(market.get("reason") or "No verified live market snapshot is available."),
        "Breadth, dispersion and OI participation are calculated across the latest persisted full-universe scanner feature ledger. They are diagnostic state, not an entry instruction.",
    ]
    reports["fno_v13_v10_g_observability_market_regime"] = "\n".join(lines) + "\n"

    selection_rows = operational["selection_slots"]
    selected_signals = operational["selected_signals"]
    lines = _report_preamble(
        "V13-V10-G Selection Funnel", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Latest persisted live session: `{operational.get('latest_live_day') or 'UNAVAILABLE'}`",
        f"- Scanner slots represented: `{len(selection_rows)}`",
        f"- Selected signal artifacts: `{len(selected_signals)}`",
        "- Threshold-margin series: **UNAVAILABLE** (the current ledger records gate results but not every numeric margin).",
        "- Rank-stability series: **UNAVAILABLE** (no repeated/resampled rank snapshots are published).",
        "",
        "## Slot funnel",
        "",
        _markdown_table(
            ("signal slot", "universe", "scanner candidates", "direction pass", "direction+volume", "setup pass", "selected"),
            (
                (
                    row["slot"], row["universe"], row["scanner_candidates"],
                    row["directional_pass"], row["confirmation_accepted"],
                    row["setup_pass"], row["selected"],
                )
                for row in selection_rows
            ) if selection_rows else [("UNAVAILABLE", 0, 0, 0, 0, 0, 0)],
        ),
        "",
        "## Most frequent first failed gate",
        "",
        _markdown_table(
            ("gate", "rows"),
            operational["gate_failures"].items() or [("UNAVAILABLE", 0)],
        ),
        "",
        "## Confirmation/setup first failed gate",
        "",
        _markdown_table(
            ("gate", "rows"),
            operational["confirmation_gate_failures"].items()
            or [("UNAVAILABLE", 0)],
        ),
        "",
        "## Selected symbols",
        "",
        _markdown_table(
            ("slot", "symbol", "side", "setup", "rank"),
            (
                (row["slot"], row["symbol"], row["side"], row["setup"], row["rank"])
                for row in selected_signals
            ) if selected_signals else [("UNAVAILABLE", "", "", "", "")],
        ),
        "",
        "Counts come from immutable scanner, confirmation and signal snapshots; failed-gate counts overlap neither slots nor rows because only the first failed gate is counted here.",
    ]
    reports["fno_v13_v10_g_observability_selection_funnel"] = "\n".join(lines) + "\n"

    execution = operational["execution"]
    historical_execution = execution["historical"]
    broker_reconciliation = execution["broker_reconciliation"]
    execution_scenarios = execution_research["scenarios"]
    lines = _report_preamble(
        "V13-V10-G Entry and Execution", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        "- Historical activation latency means signal-to-confirmation; PAPER/LIVE activation means scheduled confirmation-to-local order activation.",
        "- Trigger latency means activation/confirmation-to-recorded fill.",
        "- Trigger-to-fill distance is not broker quote slippage; quote-at-submit evidence is not present in these order files.",
        "",
        "## Execution comparison",
        "",
        _markdown_table(
            ("source", "orders", "filled", "fill %", "cancelled", "activation mean sec", "trigger mean sec", "entry distance bps"),
            (
                (
                    "FINALIZED_REPLAY",
                    historical_execution["orders"], historical_execution["filled"],
                    _fmt(historical_execution["fill_ratio_pct"]), "UNAVAILABLE",
                    _fmt(historical_execution["activation_latency_seconds"]["mean"]),
                    _fmt(historical_execution["trigger_latency_seconds"]["mean"]),
                    _fmt(historical_execution["entry_overshoot_bps"]["mean"]),
                ),
                *(
                    (
                        mode, execution[mode]["orders"], execution[mode]["filled"],
                        _fmt(execution[mode]["fill_ratio_pct"]), execution[mode]["cancelled"],
                        _fmt(execution[mode]["activation_latency_seconds"]["mean"]),
                        _fmt(execution[mode]["trigger_latency_seconds"]["mean"]),
                        _fmt(execution[mode]["trigger_to_fill_bps"]["mean"]),
                    )
                    for mode in ("PAPER", "LIVE")
                ),
            ),
        ),
        "",
        f"- PAPER activation median / max: `{_fmt(execution['PAPER']['activation_latency_seconds']['median'])}` / `{_fmt(execution['PAPER']['activation_latency_seconds']['maximum'])}` seconds.",
        f"- PAPER trigger median / max: `{_fmt(execution['PAPER']['trigger_latency_seconds']['median'])}` / `{_fmt(execution['PAPER']['trigger_latency_seconds']['maximum'])}` seconds.",
        f"- Missed-entry MFE available: `{historical_execution['missed_entry_mfe_pct']['count']}` rows; mean=`{_fmt(historical_execution['missed_entry_mfe_pct']['mean'], 4)}%`.",
        "- If missed-entry MFE is unavailable, no counterfactual value is invented.",
        f"- LIVE transition-journal first-reason coverage: `{execution['LIVE']['first_reason_signal_coverage']}/{execution['LIVE']['orders']}` signals; events=`{execution['LIVE']['transition_journal_events']}`; invalid=`{execution['LIVE']['transition_journal_invalid_events']}`.",
        f"- Current broker reconciliation: **{broker_reconciliation['state']}**; position mismatches=`{broker_reconciliation['mismatch_count'] if broker_reconciliation['mismatch_count'] is not None else 'UNAVAILABLE'}`; active-order mismatches=`{broker_reconciliation['active_order_mismatch_count'] if broker_reconciliation['active_order_mismatch_count'] is not None else 'UNAVAILABLE'}`; observed=`{broker_reconciliation['observed_at_ist'] or 'UNAVAILABLE'}`.",
        "",
        "## Verified execution-research scenarios",
        "",
        f"- Evidence state: **{execution_research['state']}**; run=`{execution_research['run_id'] or 'UNAVAILABLE'}`; conclusion=**{execution_research['conclusion']}**.",
        f"- Immutable run verified: `{execution_research['immutable_run_verified']}`; source artifacts verified: `{execution_research['verified_source_artifacts']}`.",
        _markdown_table(
            ("scenario", "delay bars", "distance proxy bps", "mechanical fills", "trades", "net INR", "delta vs baseline INR", "PF", "daily-close DD INR"),
            (
                (
                    row.get("scenario"), row.get("delay_bars"),
                    _fmt(row.get("distance_proxy_bps"), 4),
                    row.get("mechanical_fills"), row.get("executed_trades"),
                    _fmt(row.get("net_profit_rupees")),
                    _fmt(row.get("net_delta_vs_baseline_rupees")),
                    _fmt(row.get("profit_factor"), 3),
                    _fmt(row.get("daily_close_drawdown_rupees")),
                )
                for row in execution_scenarios
            ) if execution_scenarios else [(execution_research["state"], "", "", "", "", "", "", "", "")],
        ),
        "",
        f"- Finalized missed-entry counterfactuals: `{execution_research['missed_entry_counterfactuals'].get('rows', 0)}`; late trigger touches=`{execution_research['missed_entry_counterfactuals'].get('late_trigger_touches', 0)}`.",
        "- Execution scenarios and missed-entry paths are sensitivity diagnostics only; they have no execution authority and are unsafe for live selection.",
        "",
        "## Prospective shadow lifecycle",
        "",
        f"- State: **{shadow['state']}**; latest=`{shadow['latest_session_date'] or 'UNAVAILABLE'}` / **{shadow['latest_session_state']}**.",
        f"- Prepared=`{shadow['prepared_sessions']}`; decisions sealed=`{shadow['decisions_sealed_sessions']}`; completed in active cohort=`{shadow['sessions']}`; invalid=`{shadow['invalid_sessions']}`.",
        "- Only hash-verified no-authority lifecycle artifacts are counted.",
        "",
        "## Persisted status reasons",
        "",
        _markdown_table(
            ("mode", "reason", "orders"),
            (
                (mode, reason, count)
                for mode in ("PAPER", "LIVE")
                for reason, count in execution[mode]["status_reason_counts"].items()
            ),
        ),
        "",
        "## First failure retained by append-only LIVE journal",
        "",
        _markdown_table(
            ("first observed reason", "signals"),
            execution["LIVE"]["first_observed_reason_counts"].items()
            or [("UNAVAILABLE_NO_JOURNAL_EVENT", 0)],
        ),
        "",
        "A mutable final status can hide the original failure. The first-observed table is therefore the preferred root-cause view where journal coverage exists; uncovered historical rows remain indeterminate.",
    ]
    reports["fno_v13_v10_g_observability_entry_execution"] = "\n".join(lines) + "\n"

    drift_rows = operational["drift"]
    lines = _report_preamble(
        "V13-V10-G Live vs Finalized Drift", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        "- Comparison key: session, signal slot, symbol, side and setup.",
        "- Scope: persisted live selected signals versus the newest complete finalized daily replay for the same day.",
        "",
        _markdown_table(
            ("day", "live", "final", "common", "live only", "final only", "bar/price changed", "OI changed", "indicator changed", "first divergence"),
            (
                (
                    row["day"], row["live_selected"], row["finalized_selected"],
                    row["common"], row["live_only"], row["finalized_only"],
                    row["price_or_bar_changes"], row["oi_changes"],
                    row["indicator_changes"], row["first_divergence"],
                )
                for row in drift_rows
            ) if drift_rows else [("UNAVAILABLE", 0, 0, 0, 0, 0, 0, 0, 0, "INDETERMINATE")],
        ),
        "",
        "Natural-key selected-set and common-row value differences are shown as diagnostics only. First divergence stays UNAVAILABLE_NO_STAGE_BUNDLES because complete live/observed/finalized reconciliation bundles and a shared deterministic signal ID are not yet present; the report never guesses the missing stage.",
    ]
    reports["fno_v13_v10_g_observability_live_finalized_drift"] = "\n".join(lines) + "\n"

    pnl = operational["pnl"]
    full_replay = pnl["full_historical_replay"]
    recent_replay = pnl["recent_finalized_replay"]
    paper = pnl["paper_observed"]
    live = pnl["live_local"]
    lines = _report_preamble(
        "V13-V10-G P&L Attribution", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        "- PAPER and LIVE rows are kept separate even when they share a signal ID; they are not added together.",
        f"- Broker reconciliation: **{pnl['broker']['state']}**; position mismatches=`{pnl['broker']['mismatch_count'] if pnl['broker']['mismatch_count'] is not None else 'UNAVAILABLE'}`; active-order mismatches=`{pnl['broker']['active_order_mismatch_count'] if pnl['broker']['active_order_mismatch_count'] is not None else 'UNAVAILABLE'}`.",
        "- Broker fills, charges and realized P&L: **UNAVAILABLE**; position/order parity is not P&L attribution.",
        "",
        _markdown_table(
            ("source", "orders/trades", "gross INR", "cost INR", "net INR", "execution drag INR"),
            (
                (
                    "FULL HISTORICAL REPLAY", full_replay["executed_trades"],
                    _fmt(full_replay.get("gross_profit_rupees")),
                    _fmt(full_replay.get("cost_rupees")),
                    _fmt(full_replay.get("net_profit_rupees")),
                    _fmt((full_replay.get("gross_profit_rupees") or 0.0) - (full_replay.get("net_profit_rupees") or 0.0)),
                ),
                (
                    "RECENT FINALIZED REPLAY", recent_replay["executed_trades"],
                    _fmt(recent_replay.get("gross_profit_rupees")),
                    _fmt(recent_replay.get("cost_rupees")),
                    _fmt(recent_replay.get("net_profit_rupees")),
                    _fmt((recent_replay.get("gross_profit_rupees") or 0.0) - (recent_replay.get("net_profit_rupees") or 0.0)),
                ),
                (
                    "PAPER OBSERVED", paper["orders"], _fmt(paper["gross_pnl_rupees"]),
                    _fmt(paper["cost_rupees"]), _fmt(paper["net_pnl_rupees"]),
                    _fmt(paper["gross_pnl_rupees"] - paper["net_pnl_rupees"]),
                ),
                (
                    "LIVE LOCAL", live["orders"], _fmt(live["gross_pnl_rupees"]),
                    _fmt(live["cost_rupees"]), _fmt(live["net_pnl_rupees"]),
                    _fmt(live["gross_pnl_rupees"] - live["net_pnl_rupees"]),
                ),
                ("BROKER", "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE"),
            ),
        ),
        "",
        "## Verified execution-scenario P&L sensitivity",
        "",
        _markdown_table(
            ("scenario", "trades", "gross INR", "cost INR", "net INR", "delta vs frozen INR", "evidence"),
            (
                (
                    row.get("scenario"), row.get("executed_trades"),
                    _fmt(row.get("gross_profit_rupees")),
                    _fmt(row.get("cost_rupees")),
                    _fmt(row.get("net_profit_rupees")),
                    _fmt(row.get("net_delta_vs_baseline_rupees")),
                    row.get("evidence_quality", "UNAVAILABLE"),
                )
                for row in execution_scenarios
            ) if execution_scenarios else [(execution_research["state"], "", "", "", "", "", "")],
        ),
        "",
        f"- Scenario conclusion: **{execution_research['conclusion']}**; safe for live selection: **NO**.",
        f"- Prospective shadow: **{shadow['state']}**; promotion-cohort sessions=`{shadow['sessions']}/20`; latest=`{shadow['latest_session_date'] or 'UNAVAILABLE'}` / `{shadow['latest_session_state']}`.",
        "- Shadow outcome bundles are lifecycle evidence, not broker P&L. They are not added to replay, PAPER, LIVE-local or broker totals.",
        "",
        "Zero local LIVE P&L does not prove zero broker activity or profitability. The current parity snapshot is point-in-time evidence; historical fills, charges and realized P&L still require a broker tradebook/ledger join.",
    ]
    reports["fno_v13_v10_g_observability_pnl_attribution"] = "\n".join(lines) + "\n"

    def profitability_table(groups: Sequence[tuple[str, dict[str, Any]]]) -> str:
        return _markdown_table(
            ("bucket", "trades", "win rate %", "expectancy INR/trade", "PF", "drawdown INR", "evidence"),
            (
                (
                    name, metrics["executed_trades"], _fmt(metrics["win_rate_pct"]),
                    _fmt(
                        metrics["net_profit_rupees"] / metrics["executed_trades"]
                        if metrics["executed_trades"] else None
                    ),
                    _fmt(metrics["profit_factor"], 3),
                    _fmt(metrics["maximum_drawdown_rupees"]),
                    "INSUFFICIENT(<20)" if metrics["executed_trades"] < 20 else "REUSED_HISTORY",
                )
                for name, metrics in groups
            ),
        )

    lines = _report_preamble(
        "V13-V10-G Regime Profitability", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    for title, groups in (
        ("Market-side alignment", by_alignment),
        ("Five-minute volatility", by_volatility),
        ("Futures OI participation", by_oi),
        ("Liquidity", by_liquidity),
        ("Side", by_side),
        ("Setup", by_setup),
        ("Signal slot", by_slot),
    ):
        lines += [f"## {title}", "", profitability_table(groups), ""]
    lines += [
        "Expectancy and drawdown are descriptive within a selected-trade sample. Small buckets and reused history cannot justify a regime filter without preregistered walk-forward and prospective shadow evidence.",
    ]
    reports["fno_v13_v10_g_observability_regime_profitability"] = "\n".join(lines) + "\n"

    improvements = build_improvement_opportunities(
        baseline=baseline,
        by_setup=by_setup,
        regimes={
            "Market-side alignment": by_alignment,
            "Five-minute volatility": by_volatility,
            "Futures OI participation": by_oi,
            "Liquidity": by_liquidity,
        },
        operational=operational,
        execution_research=execution_research,
        prediction_metrics=prediction_metrics,
        coverage=coverage,
        audit=audit,
        point_in_time=pit,
        shadow=shadow,
        promotion_gates=promotion_gates,
        historical_rows=rows,
    )
    lines = _report_preamble(
        "V13 Evidence-Based Improvement Opportunities", generated=generated,
        run_id=run_id, source_run=source_run, through_day=through_day,
    )
    lines += [
        f"- Latest persisted live session: `{operational.get('latest_live_day') or 'UNAVAILABLE'}`",
        f"- Live market snapshot time: `{operational['market'].get('published_at_ist') or 'UNAVAILABLE'}`",
        "- Historical and operational samples have separate dates and denominators; report generation time is not input freshness.",
        "",
        render_improvement_opportunities(improvements),
    ]
    reports["fno_v13_v10_g_research_improvements"] = "\n".join(lines) + "\n"

    for card_id, filename in ALL_REPORT_FILES.items():
        _atomic_text(run_dir / filename, reports[card_id])
    predictions_name = "v13_v10_g_prior_only_predictions.csv"
    _write_prediction_csv(run_dir / predictions_name, predictions)

    artifact_manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at_utc": now_utc.isoformat().replace("+00:00", "Z"),
        "generated_at_ist": generated,
        "mode": "READ_ONLY_RESEARCH",
        "execution_authority": False,
        "source_run": str(source_run),
        "source_through_day": through_day,
        "source_evidence_quality": evidence,
        "source_artifacts": required_hashes,
        "operational_source_artifacts": operational["source_artifacts"],
        "verified_external_artifacts": {
            "execution_research": {
                "root": str(execution_research_root.resolve())
                if execution_research_root else None,
                "run_id": execution_research["run_id"],
                "artifacts": execution_research["verified_artifacts"],
            },
            "prospective_shadow": {
                "root": str((output_root.resolve() / "shadow_sessions")),
                "artifacts": shadow["verified_artifacts"],
            },
        },
        "analysis": {
            "point_in_time": pit,
            "baseline": baseline,
            "reconciliation": reconciliation,
            "eligibility_cutoff": {
                "raw_rows": len(eligibility_rows),
                "raw_eligible_sessions": eligible_sessions,
                "eligible_sessions_through_cutoff": eligible_sessions_through_cutoff,
                "post_cutoff_eligible_days": post_cutoff_eligible_days,
            },
            "drawdown_definitions": {
                "row_order_closed_trade_rupees": baseline["maximum_drawdown_rupees"],
                "source_daily_close_rupees": source_daily_close_drawdown,
            },
            "daywise_coverage": {
                "completed_sessions": len(by_day),
                "zero_selected_sessions": len(zero_selected_days),
                "zero_selected_days": zero_selected_days,
            },
            "decision_audit": audit,
            "prediction_metrics": prediction_metrics,
            "shadow": shadow,
            "execution_research": execution_research,
            "experiment_registry": registry,
            "operational_observability": operational,
            "improvement_opportunities": improvements,
            "promotion_gates": promotion_gates,
            "promotion_ready_for_manual_review": promotion_ready,
        },
        "reports": {
            card_id: {
                "filename": filename,
                "sha256": sha256_file(run_dir / filename),
            }
            for card_id, filename in ALL_REPORT_FILES.items()
        },
        "predictions": {
            "filename": predictions_name,
            "rows": len(predictions),
            "sha256": sha256_file(run_dir / predictions_name),
        },
        "live_configuration_changed": False,
    }
    _atomic_json(run_dir / "manifest.json", artifact_manifest)

    latest_dir.mkdir(parents=True, exist_ok=True)
    for filename in [*ALL_REPORT_FILES.values(), predictions_name, "manifest.json"]:
        source = run_dir / filename
        temporary = latest_dir / f".{filename}.{os.getpid()}.tmp"
        shutil.copyfile(source, temporary)
        os.replace(temporary, latest_dir / filename)

    bundle_state = (
        "READY_RESEARCH_ONLY"
        if data_quality_pass and all(reconciliation.values())
        else "PARTIAL_EVIDENCE"
    )
    return ResearchBundle(
        run_id=run_id,
        source_run=source_run,
        run_dir=run_dir,
        latest_dir=latest_dir,
        manifest_path=latest_dir / "manifest.json",
        state=bundle_state,
        source_through_day=through_day,
        selected_orders=baseline["selected_orders"],
        executed_trades=baseline["executed_trades"],
        eligible_predictions=prediction_metrics["eligible_predictions"],
        shadow_sessions=shadow["sessions"],
    )


__all__ = [
    "ALL_REPORT_FILES",
    "EXPLORATORY_EVIDENCE",
    "IMPROVEMENT_REPORT_FILES",
    "OBSERVABILITY_REPORT_FILES",
    "PREDICTION_SCHEMA_VERSION",
    "REPORT_FILES",
    "ResearchBundle",
    "classify_regime",
    "collect_operational_observability",
    "discover_source_run",
    "evaluate_predictions",
    "generate_research_bundle",
    "load_verified_execution_research",
    "load_portfolio_rows",
    "performance",
    "point_in_time_audit",
    "prior_only_predictions",
    "sha256_file",
]
