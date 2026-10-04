"""Trusted no-authority producer for prospective V13-V10-G shadow evidence.

This adapter reads only persisted historical, signal, PAPER, and finalized
replay artifacts.  It does not import a live worker, execution coordinator, or
broker client.  Missing preparation is a safe skip; any partially-created or
contradictory evidence after preparation fails closed.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from ai_platform.observability.prospective_shadow import (
    DECISIONS_SCHEMA,
    OUTCOMES_SCHEMA,
    _exclusive_write,
    _json,
    _validate_prepared,
    finalize_shadow_session,
    prepare_shadow_session,
    seal_shadow_decisions,
    sha256_file,
    verify_shadow_session,
)
from ai_platform.observability.strategy_research import discover_source_run
from fno_v13_v10_g_identity import canonical_signal_id


IST = ZoneInfo("Asia/Kolkata")
AUTOMATION_SCHEMA = "eqidv2.v13_v10_g.shadow_automation.v1"
TERMINAL_PAPER_STATES = {
    "CLOSED", "CANCELLED", "NO_FILL", "BLOCKED_SIZING", "BLOCKED_PORTFOLIO"
}


@dataclass(frozen=True)
class AutomationResult:
    session_date: date
    state: str
    detail: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": AUTOMATION_SCHEMA,
            "session_date": self.session_date.isoformat(),
            "state": self.state,
            "mode": "PROSPECTIVE_SHADOW",
            "execution_authority": False,
            **dict(self.detail),
        }


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _now(value: datetime | None) -> datetime:
    stamp = value or datetime.now(timezone.utc)
    if stamp.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return stamp.astimezone(IST)


def _session_root(output_root: Path, session_date: date) -> Path:
    return output_root.resolve() / "shadow_sessions" / session_date.isoformat()


def _stable_json(path: Path) -> tuple[dict[str, Any], str, int]:
    """Read a JSON file twice while its replacement identity remains fixed."""

    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    before = path.stat()
    raw_first = path.read_bytes()
    middle = path.stat()
    raw_second = path.read_bytes()
    after = path.stat()
    identity = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
    if identity(before) != identity(middle) or identity(middle) != identity(after):
        raise RuntimeError(f"artifact changed while being read: {path}")
    if raw_first != raw_second:
        raise RuntimeError(f"artifact content was not hash-stable: {path}")
    try:
        value = json.loads(raw_first.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON artifact: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return value, hashlib.sha256(raw_first).hexdigest(), len(raw_first)


def _artifact(path: Path, digest: str, size: int) -> dict[str, Any]:
    return {"path": str(path.resolve()), "sha256": digest, "bytes": size}


def _verified_source(source_root: Path) -> tuple[Path, Path, Path, Path, dict[str, Any]]:
    source = discover_source_run(source_root.resolve())
    dataset_manifest_path = source / "dataset" / "dataset_manifest.json"
    metadata_path = source / "g_backtest" / "run_metadata.json"
    dataset_manifest, dataset_sha, _ = _stable_json(dataset_manifest_path)
    metadata, metadata_sha, _ = _stable_json(metadata_path)
    if str(metadata.get("source_dataset_manifest_sha256", "")) != dataset_sha:
        raise ValueError("corrected source metadata does not bind the dataset manifest")
    declared_dataset = Path(str(metadata.get("source_dataset", ""))).resolve()
    if declared_dataset != (source / "dataset").resolve():
        raise ValueError("corrected source metadata points at a different dataset")
    output_hashes = dataset_manifest.get("output_sha256")
    if not isinstance(output_hashes, dict) or not output_hashes:
        raise ValueError("dataset manifest has no declared output hashes")
    verified_outputs: dict[str, dict[str, Any]] = {}
    for name, expected in sorted(output_hashes.items()):
        candidate = (source / "dataset" / str(name)).resolve()
        if (source / "dataset").resolve() not in candidate.parents or not candidate.is_file():
            raise FileNotFoundError(f"declared dataset output is missing: {name}")
        observed = sha256_file(candidate)
        if observed != str(expected):
            raise ValueError(f"declared dataset output hash mismatch: {name}")
        verified_outputs[str(name)] = {
            "bytes": candidate.stat().st_size,
            "sha256": observed,
        }
    config_path = Path(str(metadata.get("frozen_g_config", ""))).resolve()
    config_sha = str(metadata.get("frozen_g_config_sha256", ""))
    if not config_path.is_file() or len(config_sha) != 64 or sha256_file(config_path) != config_sha:
        raise ValueError("metadata-declared frozen G configuration failed verification")
    detail = {
        "source_run": str(source),
        "source_through_day": metadata.get("through_day"),
        "dataset_manifest_sha256": dataset_sha,
        "run_metadata_sha256": metadata_sha,
        "frozen_g_config_sha256": config_sha,
        "verified_dataset_outputs": verified_outputs,
    }
    return source, dataset_manifest_path, metadata_path, config_path, detail


def _load_prepared(
    output_root: Path, session_date: date
) -> tuple[Path, dict[str, Any], dict[str, Any]] | None:
    root = _session_root(output_root, session_date)
    prepared_path = root / "prepared_manifest.json"
    adapter_path = root / "automation_prepare.json"
    if not prepared_path.exists():
        if root.exists() and any(root.iterdir()):
            raise ValueError("shadow session has artifacts but no prepared manifest")
        return None
    prepared = _validate_prepared(root, session_date)
    if not adapter_path.is_file():
        raise ValueError("prepared shadow session is missing automation provenance")
    adapter = _json(adapter_path)
    if (
        adapter.get("schema_version") != AUTOMATION_SCHEMA
        or adapter.get("state") != "PREPARED"
        or adapter.get("session_date") != session_date.isoformat()
        or adapter.get("execution_authority") is not False
        or adapter.get("dataset_manifest_sha256") != prepared["dataset_sha256"]
        or adapter.get("run_metadata_sha256") != prepared["model_sha256"]
        or adapter.get("frozen_g_config_sha256") != prepared["strategy_fingerprint"]
    ):
        raise ValueError("automation preparation provenance is invalid")
    return root, prepared, adapter


def automated_prepare(
    *,
    session_date: date,
    source_root: Path,
    output_root: Path,
    now: datetime | None = None,
) -> AutomationResult:
    existing = _load_prepared(output_root, session_date)
    if existing is not None:
        return AutomationResult(
            session_date,
            "ALREADY_PREPARED",
            {"prepared_manifest": str((existing[0] / "prepared_manifest.json").resolve())},
        )
    source, dataset, metadata, config, verification = _verified_source(source_root)
    evidence = prepare_shadow_session(
        session_date=session_date,
        dataset=dataset,
        model=metadata,
        strategy=config,
        output_root=output_root,
        now=now,
    )
    root = evidence.path.parent
    provenance = {
        "schema_version": AUTOMATION_SCHEMA,
        "state": "PREPARED",
        "mode": "PROSPECTIVE_SHADOW",
        "session_date": session_date.isoformat(),
        "execution_authority": False,
        "broker_access": False,
        **verification,
    }
    _exclusive_write(root / "automation_prepare.json", _canonical_bytes(provenance))
    return AutomationResult(
        session_date,
        "PREPARED",
        {"prepared_manifest": str(evidence.path), "source_run": str(source)},
    )


def _slot_map(config: Mapping[str, Any], live_manifest: Mapping[str, Any]) -> dict[str, str]:
    setups = config.get("exit", {}).get("setups", {})
    if not isinstance(setups, dict) or not setups:
        raise ValueError("frozen configuration has no setup slots")
    confirmations = sorted({str(name).split("_", 1)[0] for name in setups})
    if any(len(value) != 4 or not value.isdigit() for value in confirmations):
        raise ValueError("frozen configuration contains an invalid setup slot")
    observed = live_manifest.get("signal_to_confirmation")
    if not isinstance(observed, dict) or not observed:
        raise ValueError("live strategy manifest has no signal-to-confirmation map")
    normalized = {str(key): str(value) for key, value in observed.items()}
    if sorted(value.replace(":", "") for value in normalized.values()) != confirmations:
        raise ValueError("live confirmation slots differ from the frozen configuration")
    return dict(sorted(normalized.items()))


def _decision_row(
    signal: Mapping[str, Any], *, session_date: date, source: Mapping[str, Any]
) -> dict[str, Any]:
    required = (
        "signal_id", "strategy_version", "session_date", "signal_end",
        "confirmation_end", "side", "tradingsymbol", "setup_id", "published_at_ist",
    )
    missing = [name for name in required if not str(signal.get(name, "")).strip()]
    if missing:
        raise ValueError("authoritative signal is missing: " + ", ".join(missing))
    expected = canonical_signal_id(
        str(signal["strategy_version"]),
        session_date,
        str(signal["signal_end"]),
        str(signal["confirmation_end"]),
        str(signal["side"]),
        str(signal["tradingsymbol"]),
    )
    if signal["signal_id"] != expected or signal["session_date"] != session_date.isoformat():
        raise ValueError("authoritative signal failed canonical identity validation")
    return {
        "signal_id": expected,
        "decision_at_ist": signal["published_at_ist"],
        "selected": True,
        "signal_end": signal["signal_end"],
        "confirmation_end": signal["confirmation_end"],
        "side": str(signal["side"]).upper(),
        "tradingsymbol": signal["tradingsymbol"],
        "setup_id": signal["setup_id"],
        "strategy_version": signal["strategy_version"],
        "live_strategy_fingerprint": signal.get("strategy_fingerprint"),
        "source_signal": dict(source),
        "authoritative_signal": dict(signal),
    }


def _validate_automated_decisions(
    bundle: Mapping[str, Any],
    *,
    session_date: date,
    prepared: Mapping[str, Any],
    config: Mapping[str, Any],
) -> None:
    if (
        bundle.get("schema_version") != DECISIONS_SCHEMA
        or bundle.get("session_date") != session_date.isoformat()
        or bundle.get("execution_authority") is not False
        or bundle.get("complete") is not True
        or bundle.get("dataset_sha256") != prepared["dataset_sha256"]
        or bundle.get("model_sha256") != prepared["model_sha256"]
        or bundle.get("strategy_fingerprint") != prepared["strategy_fingerprint"]
    ):
        raise ValueError("automated decision bundle does not match preparation")
    live_manifest = bundle.get("authoritative_strategy_manifest")
    if not isinstance(live_manifest, dict):
        raise ValueError("automated decision bundle lacks its strategy manifest")
    expected_slots = _slot_map(config, live_manifest)
    if live_manifest.get("frozen_config") != config:
        raise ValueError("sealed strategy manifest embeds different configuration")
    slots = bundle.get("configured_slots")
    if not isinstance(slots, list) or len(slots) != len(expected_slots):
        raise ValueError("automated decision bundle has incomplete configured slots")
    observed_pairs: set[tuple[str, str]] = set()
    selected: list[str] = []
    for record in slots:
        if not isinstance(record, dict) or not isinstance(record.get("authoritative_slot"), dict):
            raise ValueError("automated decision bundle lacks authoritative slot content")
        slot = record["authoritative_slot"]
        pair = (str(slot.get("signal_end", "")), str(slot.get("confirmation_end", "")))
        if (
            expected_slots.get(pair[0]) != pair[1]
            or slot.get("session_date") != session_date.isoformat()
            or str(slot.get("state", "")).upper() != "SUCCESS"
            or slot.get("scanner_complete") is not True
            or int(slot.get("error_count", -1)) != 0
        ):
            raise ValueError("sealed confirmation slot is not exact complete SUCCESS")
        observed_pairs.add(pair)
        ids = slot.get("selected_signal_ids")
        if not isinstance(ids, list):
            raise ValueError("sealed confirmation slot IDs are invalid")
        selected.extend(str(value) for value in ids)
    if observed_pairs != set(expected_slots.items()) or len(selected) != len(set(selected)):
        raise ValueError("sealed confirmation slot coverage or identities are invalid")
    rows = bundle.get("rows")
    if not isinstance(rows, list) or {str(row.get("signal_id", "")) for row in rows} != set(selected):
        raise ValueError("sealed decisions do not equal slot-selected identities")
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("authoritative_signal"), dict):
            raise ValueError("sealed decision lacks authoritative signal content")
        rebuilt = _decision_row(
            row["authoritative_signal"],
            session_date=session_date,
            source=row.get("source_signal", {}),
        )
        if rebuilt["signal_id"] != row.get("signal_id"):
            raise ValueError("sealed decision canonical identity changed")


def automated_seal(
    *,
    session_date: date,
    live_root: Path,
    output_root: Path,
    now: datetime | None = None,
) -> AutomationResult:
    loaded = _load_prepared(output_root, session_date)
    if loaded is None:
        return AutomationResult(session_date, "SKIPPED_NOT_PREPARED", {})
    root, prepared, _ = loaded
    seal_path = root / "decision_seal.json"
    if seal_path.is_file():
        seal = _json(seal_path)
        captured = Path(str(seal.get("bundle", {}).get("captured_path", ""))).resolve()
        if (
            seal.get("complete") is not True
            or not captured.is_file()
            or sha256_file(captured) != seal.get("bundle", {}).get("sha256")
        ):
            raise ValueError("existing decision seal is incomplete or corrupt")
        _validate_automated_decisions(
            _json(captured),
            session_date=session_date,
            prepared=prepared,
            config=_json(root / "inputs" / "strategy.snapshot"),
        )
        return AutomationResult(
            session_date, "ALREADY_SEALED", {"decision_seal": str(seal_path.resolve())}
        )
    if (root / "automation_decisions.json").exists():
        raise ValueError("incomplete prior seal attempt exists without a decision seal")

    config = _json(root / "inputs" / "strategy.snapshot")
    live_manifest_path = live_root.resolve() / "strategy_manifest.json"
    live_manifest, live_manifest_sha, live_manifest_size = _stable_json(live_manifest_path)
    if live_manifest.get("frozen_config_sha256") != prepared["strategy_fingerprint"]:
        raise ValueError("live strategy manifest does not match prepared frozen configuration")
    if live_manifest.get("frozen_config") != config:
        raise ValueError("live strategy manifest embeds different frozen configuration content")
    slot_map = _slot_map(config, live_manifest)
    confirmation_dir = live_root.resolve() / "confirmation_1m" / session_date.isoformat()
    signal_dir = live_root.resolve() / "signals" / session_date.isoformat()
    selected_ids: list[str] = []
    slot_artifacts: list[dict[str, Any]] = []
    strategy_versions: set[str] = set()
    live_fingerprints: set[str] = set()
    for signal_end, confirmation_end in slot_map.items():
        path = confirmation_dir / f"slot_{confirmation_end.replace(':', '')}.json"
        slot, digest, size = _stable_json(path)
        if (
            slot.get("session_date") != session_date.isoformat()
            or slot.get("signal_end") != signal_end
            or slot.get("confirmation_end") != confirmation_end
            or str(slot.get("state", "")).upper() != "SUCCESS"
            or slot.get("scanner_complete") is not True
            or int(slot.get("error_count", -1)) != 0
        ):
            raise ValueError(f"confirmation slot is not complete SUCCESS: {path.name}")
        ids = slot.get("selected_signal_ids")
        if not isinstance(ids, list) or any(not isinstance(item, str) or not item for item in ids):
            raise ValueError(f"confirmation slot has invalid selected IDs: {path.name}")
        selected_ids.extend(ids)
        strategy_versions.add(str(slot.get("strategy_version", "")))
        live_fingerprints.add(str(slot.get("strategy_fingerprint", "")))
        slot_artifacts.append({
            **_artifact(path, digest, size),
            "signal_end": signal_end,
            "confirmation_end": confirmation_end,
            "selected_signal_ids": list(ids),
            "authoritative_slot": slot,
        })
    if len(set(selected_ids)) != len(selected_ids):
        raise ValueError("a selected signal ID appears in more than one confirmation slot")
    if len(strategy_versions) != 1 or "" in strategy_versions:
        raise ValueError("confirmation slots do not share one strategy version")
    if len(live_fingerprints) != 1 or "" in live_fingerprints:
        raise ValueError("confirmation slots do not share one strategy fingerprint")
    expected_files = {f"{item}.json" for item in selected_ids}
    observed_files = {path.name for path in signal_dir.glob("*.json")} if signal_dir.is_dir() else set()
    if observed_files != expected_files:
        raise ValueError("authoritative signal directory differs from slot-selected IDs")

    rows: list[dict[str, Any]] = []
    for signal_id in sorted(selected_ids):
        path = signal_dir / f"{signal_id}.json"
        signal, digest, size = _stable_json(path)
        row = _decision_row(
            signal,
            session_date=session_date,
            source=_artifact(path, digest, size),
        )
        if row["strategy_version"] not in strategy_versions:
            raise ValueError("signal strategy version differs from confirmation slots")
        if row["live_strategy_fingerprint"] not in live_fingerprints:
            raise ValueError("signal strategy fingerprint differs from confirmation slots")
        rows.append(row)
    bundle = {
        "schema_version": DECISIONS_SCHEMA,
        "session_date": session_date.isoformat(),
        "execution_authority": False,
        "complete": True,
        "dataset_sha256": prepared["dataset_sha256"],
        "model_sha256": prepared["model_sha256"],
        "strategy_fingerprint": prepared["strategy_fingerprint"],
        "live_strategy_version": next(iter(strategy_versions)),
        "live_strategy_fingerprint": next(iter(live_fingerprints)),
        "source_strategy_manifest": _artifact(
            live_manifest_path, live_manifest_sha, live_manifest_size
        ),
        "authoritative_strategy_manifest": live_manifest,
        "configured_slots": slot_artifacts,
        "rows": rows,
    }
    _validate_automated_decisions(
        bundle, session_date=session_date, prepared=prepared, config=config
    )
    generated = root / "automation_decisions.json"
    _exclusive_write(generated, _canonical_bytes(bundle))
    evidence = seal_shadow_decisions(
        session_date=session_date,
        decisions=generated,
        output_root=output_root,
        now=now,
    )
    return AutomationResult(
        session_date,
        "DECISIONS_SEALED",
        {
            "decision_rows": len(rows),
            "configured_slots": len(slot_artifacts),
            "decision_seal": str(evidence.path),
        },
    )


def _latest_success_replay(replay_root: Path, session_date: date) -> tuple[Path, dict[str, Any]]:
    day_root = replay_root.resolve() / "runs" / session_date.isoformat()
    candidates: list[tuple[int, str, Path, dict[str, Any]]] = []
    if day_root.is_dir():
        for run in day_root.iterdir():
            result_path = run / "replay_result.json"
            if not run.is_dir() or not result_path.is_file():
                continue
            result, _, _ = _stable_json(result_path)
            if (
                result.get("session_date") == session_date.isoformat()
                and str(result.get("state", "")).upper() == "SUCCESS"
                and result.get("complete") is True
            ):
                candidates.append((result_path.stat().st_mtime_ns, run.name, run, result))
    if not candidates:
        raise FileNotFoundError(f"no successful finalized replay for {session_date}")
    _, _, run, result = max(candidates, key=lambda item: (item[0], item[1]))
    portfolio = (run / "portfolio_trades.csv").resolve()
    manifest = (run / "source_manifest.json").resolve()
    for expected, name in ((portfolio, "portfolio_trades"), (manifest, "source_manifest")):
        declared = Path(str(result.get("artifacts", {}).get(name, ""))).resolve()
        if declared != expected or not expected.is_file():
            raise ValueError(f"finalized replay has an invalid {name} artifact")
    return run.resolve(), result


def _bool(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "y"}


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _replay_rows(
    run: Path, result: Mapping[str, Any], session_date: date
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    path = run / "portfolio_trades.csv"
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError("finalized replay portfolio changed while being read")
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    required = {"day", "hhmm", "side", "tradingsymbol", "setup_id", "configured_confirmation_end"}
    if not reader.fieldnames or not required.issubset(reader.fieldnames):
        raise ValueError("finalized replay portfolio is missing identity columns")
    strategy_version = str(result.get("strategy_version", ""))
    if not strategy_version:
        raise ValueError("finalized replay has no strategy version")
    indexed: dict[str, dict[str, Any]] = {}
    for row in reader:
        if row.get("day") != session_date.isoformat():
            raise ValueError("finalized replay portfolio escaped the session date")
        signal_id = canonical_signal_id(
            strategy_version,
            session_date,
            str(row["hhmm"]),
            str(row["configured_confirmation_end"]),
            str(row["side"]),
            str(row["tradingsymbol"]),
        )
        declared = str(row.get("signal_id", "")).strip()
        if declared and declared != signal_id:
            raise ValueError("replay row has a non-canonical signal_id")
        if signal_id in indexed:
            raise ValueError(f"duplicate canonical replay signal_id: {signal_id}")
        indexed[signal_id] = row
    return indexed, {
        "path": str(path.resolve()),
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _paper_outcome(live_root: Path, session_date: date, signal_id: str) -> dict[str, Any]:
    path = live_root.resolve() / "orders" / "PAPER" / session_date.isoformat() / f"{signal_id}.json"
    if not path.exists():
        return {"present": False, "state": "MISSING", "source": None}
    value, digest, size = _stable_json(path)
    if (
        value.get("signal_id") != signal_id
        or value.get("session_date") != session_date.isoformat()
        or str(value.get("mode", "")).upper() != "PAPER"
    ):
        raise ValueError(f"PAPER outcome identity mismatch: {signal_id}")
    status = str(value.get("status", "")).upper()
    return {
        "present": True,
        "terminal": status in TERMINAL_PAPER_STATES,
        "status": status,
        "status_reason": value.get("status_reason"),
        "filled": bool(str(value.get("entry_at_ist", "")).strip()),
        "entry_at_ist": value.get("entry_at_ist"),
        "entry_price": _number(value.get("entry_price")),
        "exit_at_ist": value.get("exit_at_ist"),
        "exit_price": _number(value.get("exit_price")),
        "exit_reason": value.get("exit_reason"),
        "gross_pnl_rs": _number(value.get("gross_pnl_rs")),
        "estimated_cost_rs": _number(value.get("estimated_cost_rs")),
        "net_pnl_rs": _number(value.get("net_pnl_rs")),
        "source": _artifact(path, digest, size),
    }


def _replay_outcome(row: Mapping[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {"present": False, "state": "NOT_SELECTED_FINALIZED"}
    return {
        "present": True,
        "filled": _bool(row.get("filled")),
        "portfolio_executed": _bool(row.get("portfolio_executed")),
        "portfolio_status": row.get("portfolio_status"),
        "exit_reason": row.get("exit_reason"),
        "entry_ts": row.get("entry_ts"),
        "entry_price": _number(row.get("entry_price")),
        "exit_ts": row.get("exit_ts"),
        "exit_price": _number(row.get("exit_price")),
        "gross_return_pct": _number(row.get("gross_return_pct")),
        "net_return_pct": _number(row.get("net_return_pct")),
        "mfe_pct": _number(row.get("mfe_pct")),
        "mae_pct": _number(row.get("mae_pct")),
        "portfolio_gross_profit_rupees": _number(row.get("portfolio_gross_profit_rupees")),
        "portfolio_cost_rupees": _number(row.get("portfolio_cost_rupees")),
        "portfolio_net_profit_rupees": _number(row.get("portfolio_net_profit_rupees")),
    }


def automated_finalize(
    *,
    session_date: date,
    live_root: Path,
    replay_root: Path,
    output_root: Path,
    now: datetime | None = None,
) -> AutomationResult:
    loaded = _load_prepared(output_root, session_date)
    if loaded is None:
        return AutomationResult(session_date, "SKIPPED_NOT_PREPARED", {})
    root, prepared, _ = loaded
    final_path = root / "manifest.json"
    if final_path.is_file():
        verified = verify_shadow_session(session_date=session_date, output_root=output_root)
        if not verified["verified"]:
            raise ValueError("existing finalized shadow session failed verification")
        return AutomationResult(session_date, "ALREADY_COMPLETE", verified)
    if (root / "automation_outcomes.json").exists() or (root / "sealed_outcomes.json").exists():
        raise ValueError("incomplete prior finalize attempt exists without a final manifest")
    seal_path = root / "decision_seal.json"
    if not seal_path.is_file():
        raise ValueError("prepared shadow session has no complete decision seal")
    seal = _json(seal_path)
    decisions_path = Path(str(seal.get("bundle", {}).get("captured_path", ""))).resolve()
    if (
        not decisions_path.is_file()
        or sha256_file(decisions_path) != seal.get("bundle", {}).get("sha256")
    ):
        raise ValueError("sealed decision bundle is missing or corrupt")
    decisions = _json(decisions_path)
    _validate_automated_decisions(
        decisions,
        session_date=session_date,
        prepared=prepared,
        config=_json(root / "inputs" / "strategy.snapshot"),
    )
    decision_rows = decisions.get("rows")
    if not isinstance(decision_rows, list):
        raise ValueError("sealed decision rows are invalid")
    run, replay_result = _latest_success_replay(replay_root, session_date)
    if replay_result.get("strategy_version") != decisions.get("live_strategy_version"):
        raise ValueError("finalized replay strategy version differs from sealed decisions")
    replay_manifest = _json(run / "source_manifest.json")
    if (
        replay_manifest.get("session_date") != session_date.isoformat()
        or replay_manifest.get("complete") is not True
        or replay_manifest.get("frozen_config_sha256") != prepared["strategy_fingerprint"]
    ):
        raise ValueError("finalized replay source manifest is incomplete or uses another config")
    replay_by_id, replay_artifact = _replay_rows(run, replay_result, session_date)
    sealed_ids = {str(row.get("signal_id", "")) for row in decision_rows}
    if "" in sealed_ids or len(sealed_ids) != len(decision_rows):
        raise ValueError("sealed decisions contain missing or duplicate identities")
    outcome_at = datetime.combine(session_date, time(15, 15), tzinfo=IST).isoformat()
    rows: list[dict[str, Any]] = []
    for decision in decision_rows:
        signal_id = str(decision["signal_id"])
        paper = _paper_outcome(live_root, session_date, signal_id)
        replay_row = replay_by_id.get(signal_id)
        replay = _replay_outcome(replay_row)
        identity_match = bool(
            replay_row is not None
            and str(replay_row.get("side", "")).upper() == str(decision.get("side", "")).upper()
            and str(replay_row.get("tradingsymbol", "")) == str(decision.get("tradingsymbol", ""))
            and str(replay_row.get("setup_id", "")) == str(decision.get("setup_id", ""))
        )
        paper_filled = paper.get("filled") if paper.get("present") else None
        replay_filled = replay.get("filled") if replay.get("present") else None
        rows.append({
            "signal_id": signal_id,
            "outcome_at_ist": outcome_at,
            "decision": {
                key: decision.get(key)
                for key in (
                    "decision_at_ist", "signal_end", "confirmation_end", "side",
                    "tradingsymbol", "setup_id", "strategy_version",
                )
            },
            "paper": paper,
            "replay": replay,
            "divergence": {
                "selection_state": "MATCH" if replay_row is not None else "LIVE_ONLY",
                "canonical_identity_fields_match": identity_match,
                "paper_present": bool(paper.get("present")),
                "replay_present": bool(replay.get("present")),
                "paper_vs_replay_fill_match": (
                    paper_filled == replay_filled
                    if paper_filled is not None and replay_filled is not None
                    else None
                ),
                "entry_price_delta_paper_minus_replay": (
                    paper["entry_price"] - replay["entry_price"]
                    if paper.get("entry_price") is not None and replay.get("entry_price") is not None
                    else None
                ),
                "net_pnl_delta_paper_minus_replay_rupees": (
                    paper["net_pnl_rs"] - replay["portfolio_net_profit_rupees"]
                    if paper.get("net_pnl_rs") is not None
                    and replay.get("portfolio_net_profit_rupees") is not None
                    else None
                ),
            },
        })
    replay_only = sorted(set(replay_by_id).difference(sealed_ids))
    sealed_only = sorted(sealed_ids.difference(replay_by_id))
    replay_result_path = run / "replay_result.json"
    outcomes = {
        "schema_version": OUTCOMES_SCHEMA,
        "session_date": session_date.isoformat(),
        "execution_authority": False,
        "complete": True,
        "source_replay_run": str(run),
        "source_replay_result": {
            "path": str(replay_result_path.resolve()),
            "bytes": replay_result_path.stat().st_size,
            "sha256": sha256_file(replay_result_path),
        },
        "source_replay_portfolio": replay_artifact,
        "replay_only_signal_ids": replay_only,
        "sealed_only_signal_ids": sealed_only,
        "rows": rows,
    }
    generated = root / "automation_outcomes.json"
    _exclusive_write(generated, _canonical_bytes(outcomes))
    evidence = finalize_shadow_session(
        session_date=session_date,
        outcomes=generated,
        output_root=output_root,
        now=now,
    )
    verified = verify_shadow_session(session_date=session_date, output_root=output_root)
    if not verified["verified"]:
        raise ValueError("newly finalized shadow evidence failed verification")
    return AutomationResult(
        session_date,
        "COMPLETE",
        {
            "decision_rows": len(rows),
            "replay_only_signals": len(replay_only),
            "sealed_only_signals": len(sealed_only),
            "manifest": str(evidence.path),
            "manifest_sha256": evidence.sha256,
        },
    )


__all__ = [
    "AUTOMATION_SCHEMA",
    "AutomationResult",
    "automated_finalize",
    "automated_prepare",
    "automated_seal",
]
