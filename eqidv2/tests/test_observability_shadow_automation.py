from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ai_platform.observability import shadow_automation as subject
from fno_v13_v10_g_identity import canonical_signal_id


SESSION = date(2026, 9, 25)
VERSION = "FNO_V13_V10_G_RETAINED_TEST"
LIVE_FINGERPRINT = "a" * 64


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _source(parent: Path) -> tuple[Path, dict, Path]:
    run = parent / "run_verified"
    dataset = run / "dataset"
    backtest = run / "g_backtest"
    dataset.mkdir(parents=True)
    backtest.mkdir(parents=True)
    payload = dataset / "payload.bin"
    payload.write_bytes(b"corrected-history")
    config = parent / "frozen_config.json"
    config_value = {
        "exit": {
            "setups": {
                "0926_LONG": {"stop_pct": 0.6, "target_pct": 1.0},
                "0931_SHORT": {"stop_pct": 0.6, "target_pct": 1.0},
            }
        }
    }
    _write_json(config, config_value)
    manifest = {
        "schema": "TEST_CORRECTED_DATASET",
        "through_day": "2026-09-24",
        "output_sha256": {
            "payload.bin": hashlib.sha256(payload.read_bytes()).hexdigest()
        },
    }
    manifest_path = dataset / "dataset_manifest.json"
    _write_json(manifest_path, manifest)
    metadata = {
        "through_day": "2026-09-24",
        "source_dataset": str(dataset.resolve()),
        "source_dataset_manifest_sha256": hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest(),
        "frozen_g_config": str(config.resolve()),
        "frozen_g_config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
    }
    _write_json(backtest / "run_metadata.json", metadata)
    _write_json(backtest / "summary.json", {})
    _write_json(backtest / "data_coverage_audit.json", {})
    (backtest / "portfolio_trades.csv").write_text("day,filled\n", encoding="utf-8")
    (dataset / "source_session_eligibility.csv").write_text(
        "day,eligible\n", encoding="utf-8"
    )
    (dataset / "setup_audit.parquet").write_bytes(b"test")
    return run, config_value, config


def _prepare(tmp_path: Path) -> tuple[Path, Path, dict, Path]:
    source_root = tmp_path / "sources"
    _, config, config_path = _source(source_root)
    output = tmp_path / "research"
    result = subject.automated_prepare(
        session_date=SESSION,
        source_root=source_root,
        output_root=output,
        now=datetime(2026, 9, 25, 3, 20, tzinfo=timezone.utc),  # 08:50 IST
    )
    assert result.state == "PREPARED"
    return output, source_root, config, config_path


def _live(
    root: Path, config: dict, *, selected: bool = True
) -> tuple[Path, str | None]:
    slots = {"09:25": "09:26", "09:30": "09:31"}
    config_sha = hashlib.sha256(
        (root.parent / "sources" / "frozen_config.json").read_bytes()
    ).hexdigest()
    _write_json(
        root / "strategy_manifest.json",
        {
            "strategy_version": VERSION,
            "strategy_fingerprint": LIVE_FINGERPRINT,
            "frozen_config_sha256": config_sha,
            "frozen_config": config,
            "signal_to_confirmation": slots,
        },
    )
    signal_id = canonical_signal_id(
        VERSION, SESSION, "09:30", "09:31", "SHORT", "TEST"
    ) if selected else None
    for signal_end, confirmation_end in slots.items():
        ids = [signal_id] if selected and confirmation_end == "09:31" else []
        _write_json(
            root
            / "confirmation_1m"
            / SESSION.isoformat()
            / f"slot_{confirmation_end.replace(':', '')}.json",
            {
                "schema_version": "test_confirmation",
                "session_date": SESSION.isoformat(),
                "signal_end": signal_end,
                "confirmation_end": confirmation_end,
                "state": "SUCCESS",
                "scanner_complete": True,
                "error_count": 0,
                "strategy_version": VERSION,
                "strategy_fingerprint": LIVE_FINGERPRINT,
                "selected_signal_ids": ids,
            },
        )
    if signal_id:
        _write_json(
            root / "signals" / SESSION.isoformat() / f"{signal_id}.json",
            {
                "schema_version": "test_signal",
                "signal_id": signal_id,
                "strategy_version": VERSION,
                "strategy_fingerprint": LIVE_FINGERPRINT,
                "session_date": SESSION.isoformat(),
                "signal_end": "09:30",
                "confirmation_end": "09:31",
                "side": "SHORT",
                "tradingsymbol": "TEST",
                "setup_id": "0931_SHORT",
                "published_at_ist": "2026-09-25T09:31:08+05:30",
                "trigger_price": 100.0,
            },
        )
    return root, signal_id


def _seal(tmp_path: Path) -> tuple[Path, Path, str]:
    output, _, config, _ = _prepare(tmp_path)
    live, signal_id = _live(tmp_path / "live", config)
    assert signal_id
    result = subject.automated_seal(
        session_date=SESSION,
        live_root=live,
        output_root=output,
        now=datetime(2026, 9, 25, 9, 55, tzinfo=timezone.utc),  # 15:25 IST
    )
    assert result.state == "DECISIONS_SEALED"
    assert result.detail["configured_slots"] == 2
    assert result.detail["decision_rows"] == 1
    return output, live, signal_id


def test_automatic_prepare_seal_and_finalize_exact_identity_join(tmp_path: Path) -> None:
    output, live, signal_id = _seal(tmp_path)
    _write_json(
        live / "orders" / "PAPER" / SESSION.isoformat() / f"{signal_id}.json",
        {
            "signal_id": signal_id,
            "session_date": SESSION.isoformat(),
            "mode": "PAPER",
            "status": "CLOSED",
            "status_reason": "TARGET",
            "entry_at_ist": "2026-09-25T09:32:00+05:30",
            "entry_price": 99.9,
            "exit_at_ist": "2026-09-25T10:00:00+05:30",
            "exit_price": 98.0,
            "net_pnl_rs": 900.0,
        },
    )
    replay_root = tmp_path / "replay"
    run = replay_root / "runs" / SESSION.isoformat() / "run_2"
    run.mkdir(parents=True)
    portfolio = run / "portfolio_trades.csv"
    fields = [
        "day", "hhmm", "side", "tradingsymbol", "setup_id",
        "configured_confirmation_end", "filled", "portfolio_executed",
        "portfolio_status", "entry_price", "exit_price", "exit_reason",
        "portfolio_gross_profit_rupees", "portfolio_cost_rupees",
        "portfolio_net_profit_rupees", "mfe_pct", "mae_pct",
    ]
    with portfolio.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow(
            {
                "day": SESSION.isoformat(),
                "hhmm": "0930",
                "side": "SHORT",
                "tradingsymbol": "TEST",
                "setup_id": "0931_SHORT",
                "configured_confirmation_end": "09:31",
                "filled": True,
                "portfolio_executed": True,
                "portfolio_status": "EXECUTED",
                "entry_price": 100.0,
                "exit_price": 98.0,
                "exit_reason": "TARGET",
                "portfolio_gross_profit_rupees": 1000.0,
                "portfolio_cost_rupees": 100.0,
                "portfolio_net_profit_rupees": 900.0,
                "mfe_pct": 2.0,
                "mae_pct": -0.2,
            }
        )
    prepared = json.loads(
        (
            output
            / "shadow_sessions"
            / SESSION.isoformat()
            / "prepared_manifest.json"
        ).read_text(encoding="utf-8")
    )
    _write_json(
        run / "source_manifest.json",
        {
            "session_date": SESSION.isoformat(),
            "complete": True,
            "frozen_config_sha256": prepared["strategy_fingerprint"],
        },
    )
    _write_json(
        run / "replay_result.json",
        {
            "session_date": SESSION.isoformat(),
            "state": "SUCCESS",
            "complete": True,
            "strategy_version": VERSION,
            "artifacts": {
                "portfolio_trades": str(portfolio.resolve()),
                "source_manifest": str((run / "source_manifest.json").resolve()),
            },
        },
    )
    result = subject.automated_finalize(
        session_date=SESSION,
        live_root=live,
        replay_root=replay_root,
        output_root=output,
        now=datetime(2026, 9, 25, 11, 0, tzinfo=timezone.utc),  # 16:30 IST
    )
    assert result.state == "COMPLETE"
    outcomes = json.loads(
        (
            output
            / "shadow_sessions"
            / SESSION.isoformat()
            / "sealed_outcomes.json"
        ).read_text(encoding="utf-8")
    )
    assert [row["signal_id"] for row in outcomes["rows"]] == [signal_id]
    row = outcomes["rows"][0]
    assert row["paper"]["present"] is True
    assert row["replay"]["present"] is True
    assert row["divergence"]["selection_state"] == "MATCH"
    assert row["divergence"]["paper_vs_replay_fill_match"] is True
    assert row["divergence"]["net_pnl_delta_paper_minus_replay_rupees"] == 0.0


def test_seal_allows_empty_rows_but_requires_every_success_slot(tmp_path: Path) -> None:
    output, _, config, _ = _prepare(tmp_path)
    live, _ = _live(tmp_path / "live", config, selected=False)
    result = subject.automated_seal(
        session_date=SESSION,
        live_root=live,
        output_root=output,
        now=datetime(2026, 9, 25, 9, 55, tzinfo=timezone.utc),
    )
    assert result.state == "DECISIONS_SEALED"
    assert result.detail["decision_rows"] == 0

    other = tmp_path / "other"
    output2, _, config2, _ = _prepare(other)
    live2, _ = _live(other / "live", config2, selected=False)
    (live2 / "confirmation_1m" / SESSION.isoformat() / "slot_0931.json").unlink()
    with pytest.raises(FileNotFoundError):
        subject.automated_seal(
            session_date=SESSION,
            live_root=live2,
            output_root=output2,
            now=datetime(2026, 9, 25, 9, 55, tzinfo=timezone.utc),
        )


def test_absent_preparation_skips_but_partial_state_fails_closed(tmp_path: Path) -> None:
    result = subject.automated_seal(
        session_date=SESSION,
        live_root=tmp_path / "live",
        output_root=tmp_path / "output",
        now=datetime(2026, 9, 25, 9, 55, tzinfo=timezone.utc),
    )
    assert result.state == "SKIPPED_NOT_PREPARED"
    partial = tmp_path / "partial"
    root = partial / "shadow_sessions" / SESSION.isoformat()
    root.mkdir(parents=True)
    (root / "unexpected.txt").write_text("partial", encoding="utf-8")
    with pytest.raises(ValueError, match="no prepared manifest"):
        subject.automated_seal(
            session_date=SESSION,
            live_root=tmp_path / "live",
            output_root=partial,
            now=datetime(2026, 9, 25, 9, 55, tzinfo=timezone.utc),
        )


def test_shadow_scheduler_is_hardened_and_never_starts_tasks_on_install() -> None:
    repo = Path(__file__).resolve().parents[1]
    schedule = (repo / "bat" / "schedule_v13_shadow_weekday.ps1").read_text(
        encoding="utf-8"
    )
    prepare = (repo / "bat" / "run_v13_shadow_prepare.bat").read_text(
        encoding="utf-8"
    )
    seal = (repo / "bat" / "run_v13_shadow_seal.bat").read_text(encoding="utf-8")
    assert 'Time = "08:50"' in schedule
    assert 'Time = "15:25"' in schedule
    assert '"dd/MM/yyyy"' in schedule
    assert "/SD $scheduleStartDate" in schedule
    assert "harden_scheduled_task.ps1" in schedule
    assert "Start-ScheduledTask" not in schedule
    assert "/Run" not in schedule
    assert "FNO_V6_EXECUTION_MODE=LIVE" not in prepare + seal
    assert "live_arm" not in (prepare + seal).lower()
    assert "kill_switch" not in (prepare + seal).lower()
    assert "execution_authority=false" in prepare
    assert "execution_authority=false" in seal
