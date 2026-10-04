from __future__ import annotations

import json
from pathlib import Path

import fno_v5_live as live


def test_opted_in_live_runtime_emits_logs_traces_and_textfile_metrics(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("EQIDV2_OBSERVABILITY_ENABLED", "1")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    monkeypatch.setattr(live.common, "runtime_dir", lambda *parts: tmp_path.joinpath(*parts))
    monkeypatch.setattr(live, "_OBSERVABILITY_RUNTIME", None)
    monkeypatch.setattr(live, "_OBSERVABILITY_INITIALIZED", False)

    assert live._observe_broker_call("unit_call", lambda: "ok") == "ok"
    live._record_worker_observability(
        "long-entry",
        "RUNNING",
        {"execution_mode": "LIVE", "session_date": "2026-09-24"},
        heartbeat=False,
    )
    runtime = live._OBSERVABILITY_RUNTIME
    assert runtime is not None
    assert runtime.flush_spans(timeout_seconds=2.0)

    metric_files = list((tmp_path / "observability" / "metrics").glob("*.prom"))
    assert len(metric_files) == 1
    metrics = metric_files[0].read_text(encoding="utf-8")
    assert "trading_broker_requests_total" in metrics
    assert 'operation="unit_call",outcome="success"' in metrics
    assert "trading_heartbeat_age_seconds" in metrics

    log_files = list((tmp_path / "observability" / "logs").glob("*.jsonl"))
    assert len(log_files) == 1
    events = [json.loads(line) for line in log_files[0].read_text(encoding="utf-8").splitlines()]
    assert {event["event"] for event in events} >= {
        "trace.span.completed",
        "worker.status",
    }
    assert any(event["context"].get("mode") == "live" for event in events)

    runtime.shutdown()
