from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ai_platform.observability.reconciliation import canonical_sha256
from ai_platform.observability.runtime_collector import (
    RuntimeMetricsCollector,
    merge_prometheus_samples,
)


IST = ZoneInfo("Asia/Kolkata")
NOW = datetime(2026, 9, 24, 10, 0, tzinfo=IST)


def _json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _signed_report(payload: dict) -> dict:
    result = dict(payload)
    result["report_sha256"] = canonical_sha256(result)
    return result


def _collector(runtime: Path, profile_registry: Path, **kwargs) -> RuntimeMetricsCollector:
    return RuntimeMetricsCollector(
        runtime,
        observability_root=runtime / "observability",
        profile_registry_path=profile_registry,
        cache_ttl_seconds=0,
        now=lambda: NOW,
        **kwargs,
    )


def test_equity_final_slot_incomplete_excludes_opening_and_watcher_markers(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    marker_dir = runtime / "slot_ready_5m"
    _json(marker_dir / "slot_20260924_0915.json", {
        "slot_ist": "2026-09-24T09:15:00+05:30",
        "published_at_ist": "2026-09-24T09:15:16+05:30",
        "source": "final",
        "complete": False,
    })
    collector = _collector(runtime, tmp_path / "profiles.json")
    assert "trading_data_slot_incomplete" not in collector.render_prometheus_samples()

    _json(marker_dir / "slot_20260924_0955.json", {
        "slot_ist": "2026-09-24T09:55:00+05:30",
        "published_at_ist": "2026-09-24T09:58:05+05:30",
        "source": "final",
        "complete": False,
    })
    _json(marker_dir / "slot_20260924_1000.json", {
        "slot_ist": "2026-09-24T10:00:00+05:30",
        "published_at_ist": "2026-09-24T10:00:00+05:30",
        "source": "watcher",
        "fresh_ratio": 0.75,
    })
    rendered = collector.render_prometheus_samples()
    assert 'trading_data_slot_incomplete{slot="09:55",source="equity",timeframe="5m"} 1' in rendered
    assert 'trading_data_slot_incomplete{slot="10:00"' not in rendered

    _json(marker_dir / "slot_20260924_1000.json", {
        "slot_ist": "2026-09-24T10:00:00+05:30",
        "published_at_ist": "2026-09-24T10:00:00+05:30",
        "source": "final",
        "complete": True,
    })
    rendered = collector.render_prometheus_samples()
    assert 'trading_data_slot_incomplete{slot="10:00",source="equity",timeframe="5m"} 0' in rendered
    assert 'trading_data_slot_incomplete{slot="09:55"' not in rendered


def test_equity_final_slot_incomplete_rejects_old_and_future_evidence(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    marker_dir = runtime / "slot_ready_5m"
    _json(marker_dir / "slot_20260923_0955.json", {
        "slot_ist": "2026-09-23T09:55:00+05:30",
        "published_at_ist": "2026-09-23T09:58:05+05:30",
        "source": "final",
        "complete": False,
    })
    _json(marker_dir / "slot_20260924_0955.json", {
        "slot_ist": "2026-09-24T09:55:00+05:30",
        "published_at_ist": "2026-09-24T10:01:00+05:30",
        "source": "final",
        "complete": False,
    })
    collector = _collector(runtime, tmp_path / "profiles.json")
    assert "trading_data_slot_incomplete" not in collector.render_prometheus_samples()


def test_runtime_collector_reads_live_data_replay_safety_and_reconciliation(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    registry = tmp_path / "profiles.json"
    _json(
        registry,
        {"profiles": {"V13_V10_G": {"strategy_fingerprint": "approved-fingerprint"}}},
    )
    _json(
        runtime / "observability" / "maintenance.json",
        {
            "active": True,
            "starts_at": "2026-09-24T09:00:00+05:30",
            "ends_at": "2026-09-24T10:30:00+05:30",
        },
    )
    live_root = runtime / "fno_oi" / "v13_v10_g_live"
    _json(
        live_root / "live_kite" / "heartbeat.json",
        {
            "session_id": "fno_v13_v10_g_live_kite_qty1",
            "execution_mode": "LIVE",
            "heartbeat_ist": "2026-09-24T09:59:30+05:30",
            "strategy_fingerprint": "wrong-fingerprint",
        },
    )
    _json(
        live_root / "live_kite" / "status.json",
        {
            "session_id": "fno_v13_v10_g_live_kite_qty1",
            "execution_mode": "LIVE",
            "strategy_fingerprint": "wrong-fingerprint",
        },
    )
    _text(
        runtime / "runtime_status" / "fno_v13_v10_g_live_kite_qty1.heartbeat",
        "\n".join(
            (
                "name=fno_v13_v10_g_live_kite_qty1",
                "state=RUNNING",
                "ts_utc=2026-09-24T04:29:20+00:00",
                "restart_count=3",
                "clock_offset_seconds=-0.75",
            )
        ),
    )
    _json(
        runtime / "fno_oi" / "slot_ready" / "slot_20260924_0955.json",
        {
            "slot_ist": "2026-09-24T09:55:00+05:30",
            "stock_coverage_ratio": 0.99,
        },
    )
    _json(
        runtime / "slot_ready_5m" / "slot_20260924_0955.json",
        {
            "slot_ist": "2026-09-24T09:55:00+05:30",
            "tickers_expected": 100,
            "tickers_complete": 98,
        },
    )
    _json(
        runtime
        / "fno_oi"
        / "equity_1m_slot_ready"
        / "v6"
        / "2026-09-24"
        / "slot_0959_1234.json",
        {
            "slot_ist": "2026-09-24T09:59:00+05:30",
            "candidate_count": 4,
            "resolved_count": 4,
        },
    )
    _json(
        runtime
        / "backtesting_result_v13_v10_g"
        / "latest"
        / "latest_backtesting_result_v13_v10_g.json",
        {
            "status": "SUCCESS",
            "session_date": "2026-09-23",
            "updated_at_ist": "2026-09-23T16:10:00+05:30",
            "result": {"state": "SUCCESS", "complete": True, "session_date": "2026-09-23"},
        },
    )
    _json(
        live_root / "orders" / "LIVE" / "live_kite_qty1" / "2026-09-24" / "one.json",
        {
            "mode": "LIVE",
            "session_date": "2026-09-24",
            "status": "OPEN",
            "stop_order_id": "",
            "entry_at_ist": "2026-09-24T09:58:00+05:30",
        },
    )
    _json(
        runtime / "observability" / "reconciliation" / "2026-09-23.json",
        _signed_report({
            "session_date": "2026-09-23",
            "comparisons": {
                "live_vs_observed": {
                    "state": "MISMATCH",
                    "first_divergence_stage": "feature",
                    "classification": "CODE_CONFIG_OR_WARMUP_DIFFERENCE",
                    "stages": [
                        {
                            "stage": "feature",
                            "differences": [{"field": "ema9"}, {"field": "ema9"}],
                        }
                    ],
                },
                "observed_vs_finalized": {
                    "state": "MISMATCH",
                    "first_divergence_stage": "raw_futures_oi",
                    "classification": "LATE_DATA_OR_PROVIDER_REVISION",
                    "stages": [],
                },
            },
            "position_reconciliation": {
                "broker_truth_available": False,
                "mismatch_count": 0,
            },
        }),
    )

    rendered = _collector(runtime, registry).render_prometheus_samples()

    assert "trading_market_open 1" in rendered
    assert "trading_maintenance_window 1" in rendered
    assert (
        'trading_heartbeat_age_seconds{mode="live",service="fno_v13_v10_g_live_kite_qty1"} 30'
        in rendered
    )
    assert "fno_v13_v10_g_live_kite_qty1-supervisor" in rendered
    assert 'trading_data_age_seconds{source="futures_oi",timeframe="5m"} 300' in rendered
    assert 'trading_data_coverage_ratio{source="futures_oi",timeframe="5m"} 0.99' in rendered
    assert 'trading_data_coverage_ratio{source="equity",timeframe="5m"} 0.98' in rendered
    assert 'trading_data_age_seconds{source="equity_confirmation",timeframe="1m"} 60' in rendered
    assert 'trading_replay_due{profile="v13-v10-g",replay_kind="daily"} 0' in rendered
    assert "trading_replay_success_timestamp_seconds" in rendered
    assert 'trading_strategy_fingerprint_mismatch{mode="live"' in rendered
    assert 'strategy="v13_v10_g"} 1' in rendered
    assert "trading_process_restarts_total" in rendered and "} 3" in rendered
    assert "trading_clock_offset_seconds" in rendered and "-0.75" in rendered
    assert 'trading_unprotected_position_seconds{asset="equity",mode="live"} 120' in rendered
    assert "trading_live_eod_mismatch_total" in rendered
    assert 'mismatch_type="expected_data_revision"' in rendered
    assert 'trading_feature_parity_mismatch_total{feature="ema9",strategy="v13_v10_g"} 2' in rendered
    assert "trading_disk_free_bytes" in rendered
    # A stage-parity report cannot prove broker/local position agreement.
    assert "trading_position_reconciliation_mismatch" not in rendered


def test_textfile_samples_are_bounded_fresh_filtered_and_deduplicated(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    metrics = runtime / "observability" / "metrics"
    metrics.mkdir(parents=True)
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    first = metrics / "long.prom"
    second = metrics / "short.prom"
    stale = metrics / "stale.prom"
    _text(
        first,
        """# HELP trading_broker_requests_total ignored metadata
# TYPE trading_broker_requests_total counter
trading_broker_requests_total{operation="orders",outcome="error"} 2
trading_broker_requests_total{operation="orders",outcome="error"} 200
trading_broker_request_duration_seconds_bucket{operation="orders",outcome="error",le="0.1"} 2
trading_broker_request_duration_seconds_bucket{operation="orders",outcome="error",le="+Inf"} 2
trading_broker_request_duration_seconds_bucket{operation="orders",outcome="error",le="banana"} 999
trading_broker_request_duration_seconds_sum{operation="orders",outcome="error"} 0.1
trading_broker_request_duration_seconds_count{operation="orders",outcome="error"} 2
trading_data_age_seconds{source="worker",timeframe="tick"} 7
evil_metric 99
malformed metric here
""",
    )
    _text(
        second,
        """trading_broker_requests_total{outcome="error",operation="orders"} 3
trading_broker_request_duration_seconds_bucket{operation="orders",outcome="error",le="0.1"} 3
trading_broker_request_duration_seconds_bucket{operation="orders",outcome="error",le="+Inf"} 3
trading_broker_request_duration_seconds_sum{operation="orders",outcome="error"} 0.2
trading_broker_request_duration_seconds_count{operation="orders",outcome="error"} 3
trading_data_age_seconds{timeframe="tick",source="worker"} 9
""",
    )
    _text(stale, 'trading_broker_requests_total{operation="orders",outcome="error"} 100\n')
    now_epoch = NOW.timestamp()
    os.utime(first, (now_epoch - 20, now_epoch - 20))
    os.utime(second, (now_epoch - 10, now_epoch - 10))
    os.utime(stale, (now_epoch - 600, now_epoch - 600))

    rendered = _collector(
        runtime, registry, textfile_max_age_seconds=300
    ).render_prometheus_samples()

    counter = 'trading_broker_requests_total{operation="orders",outcome="error"} 5'
    age = 'trading_data_age_seconds{source="worker",timeframe="tick"} 9'
    assert rendered.count(counter) == 1
    assert rendered.count(age) == 1
    assert (
        'trading_broker_request_duration_seconds_bucket{le="0.1",operation="orders",outcome="error"} 5'
        in rendered
    )
    assert (
        'trading_broker_request_duration_seconds_bucket{le="+Inf",operation="orders",outcome="error"} 5'
        in rendered
    )
    assert (
        'trading_broker_request_duration_seconds_count{operation="orders",outcome="error"} 5'
        in rendered
    )
    assert (
        'trading_broker_request_duration_seconds_sum{operation="orders",outcome="error"} 0.3'
        in rendered
    )
    assert 'le="banana"' not in rendered
    assert "100" not in rendered
    assert "evil_metric" not in rendered
    assert "# HELP" not in rendered and "# TYPE" not in rendered


def test_cache_is_at_most_five_seconds_and_corrupt_sources_fail_open(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    registry = tmp_path / "profiles.json"
    _text(registry, "{broken")
    heartbeat = runtime / "fno_oi" / "v13_v10_g_live" / "live_kite" / "heartbeat.json"
    _text(heartbeat, "{broken")
    clock = [0.0]
    collector = RuntimeMetricsCollector(
        runtime,
        observability_root=runtime / "observability",
        profile_registry_path=registry,
        cache_ttl_seconds=99,
        now=lambda: NOW,
        monotonic=lambda: clock[0],
    )
    first = collector.render_prometheus_samples()
    assert "trading_market_open 1" in first
    _json(
        heartbeat,
        {
            "session_id": "live",
            "execution_mode": "LIVE",
            "heartbeat_ist": "2026-09-24T09:59:00+05:30",
        },
    )
    clock[0] = 4.9
    assert collector.render_prometheus_samples() == first
    clock[0] = 5.01
    assert "trading_heartbeat_age_seconds" in collector.render_prometheus_samples()


def test_merge_preserves_registry_metadata_and_has_one_series_per_labelset() -> None:
    base = """# HELP trading_broker_requests_total requests
# TYPE trading_broker_requests_total counter
trading_broker_requests_total{operation="orders",outcome="error"} 2
# HELP trading_data_age_seconds age
# TYPE trading_data_age_seconds gauge
trading_data_age_seconds{source="equity",timeframe="5m"} 10
# HELP trading_data_slot_incomplete final slot state
# TYPE trading_data_slot_incomplete gauge
trading_data_slot_incomplete{slot="10:00",source="equity",timeframe="5m"} 0
"""
    additions = """# TYPE trading_broker_requests_total counter
trading_broker_requests_total{outcome="error",operation="orders"} 3
trading_broker_requests_total{outcome="negative",operation="orders"} -1
trading_data_age_seconds{timeframe="5m",source="equity"} 20
trading_data_slot_incomplete{timeframe="5m",source="equity",slot="10:00"} 1
"""

    merged = merge_prometheus_samples(base, additions)

    assert merged.count("# TYPE trading_broker_requests_total") == 1
    assert merged.count('trading_broker_requests_total{operation="orders",outcome="error"}') == 1
    assert 'trading_broker_requests_total{operation="orders",outcome="error"} 5' in merged
    assert 'outcome="negative"' not in merged
    assert merged.count('trading_data_age_seconds{source="equity",timeframe="5m"}') == 1
    assert 'trading_data_age_seconds{source="equity",timeframe="5m"} 20' in merged
    assert 'trading_data_slot_incomplete{slot="10:00",source="equity",timeframe="5m"} 1' in merged


def test_textfile_contract_rejects_unknown_labels_future_samples_and_caps_series(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    metrics = runtime / "observability" / "metrics"
    metrics.mkdir(parents=True)
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    future_ms = int((NOW.timestamp() + 120) * 1000)
    past_ms = int((NOW.timestamp() - 20) * 1000)
    lines = [
        'trading_broker_requests_total{operation="valid",outcome="ok"} 1',
        (
            'trading_replay_success_timestamp_seconds{profile="v13-v10-g",'
            f'replay_kind="daily"}} {NOW.timestamp() + 120}'
        ),
        (
            'trading_broker_requests_total{operation="timestamped",outcome="ok"} '
            f"4 {past_ms}"
        ),
        'trading_unknown_total{operation="unknown",outcome="ok"} 1',
        'trading_broker_requests_total{operation="extra",outcome="ok",run_id="unbounded"} 1',
        'trading_broker_requests_total{operation="missing"} 1',
        'trading_broker_requests_total{operation="negative",outcome="bad"} -1',
        (
            'trading_broker_requests_total{operation="future",outcome="ok"} '
            f"1 {future_ms}"
        ),
        (
            'trading_broker_requests_total{operation="recover",outcome="ok"} '
            f"9 {future_ms}"
        ),
        'trading_broker_requests_total{operation="recover",outcome="ok"} 2',
        (
            'trading_broker_requests_total{operation="'
            + ("x" * 129)
            + '",outcome="too_long"} 1'
        ),
    ]
    lines.extend(
        f'trading_broker_requests_total{{operation="op_{index}",outcome="ok"}} 1'
        for index in range(1005)
    )
    snapshot = metrics / "bounded.prom"
    _text(snapshot, "\n".join(lines) + "\n")
    now_epoch = NOW.timestamp()
    os.utime(snapshot, (now_epoch - 10, now_epoch - 10))

    rendered = _collector(runtime, registry).render_prometheus_samples()

    assert rendered.count("trading_broker_requests_total{") == 1000
    assert 'operation="valid"' in rendered
    assert (
        'trading_broker_requests_total{operation="timestamped",outcome="ok"} 4\n'
        in rendered
    )
    assert str(past_ms) not in rendered
    assert "trading_unknown_total" not in rendered
    assert "trading_replay_success_timestamp_seconds" not in rendered
    assert 'operation="extra"' not in rendered
    assert 'operation="missing"' not in rendered
    assert 'operation="negative"' not in rendered
    assert 'operation="future"' not in rendered
    assert (
        'trading_broker_requests_total{operation="recover",outcome="ok"} 2\n'
        in rendered
    )
    assert 'outcome="too_long"' not in rendered


def test_recursive_file_search_returns_newest_files_not_walk_order(tmp_path: Path) -> None:
    root = tmp_path / "markers"
    paths = [
        root / "a" / "marker.json",
        root / "b" / "marker.json",
        root / "c" / "marker.json",
    ]
    for index, path in enumerate(paths, start=1):
        _json(path, {"slot_ist": "2026-09-24T09:55:00+05:30"})
        os.utime(path, (index, index))

    found = RuntimeMetricsCollector._bounded_recursive_files(
        root, "marker.json", limit=2
    )

    assert found == [paths[2], paths[1]]


def test_obsolete_terminal_and_future_heartbeats_are_not_exported(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    _json(
        runtime / "fno_oi" / "v13_v10_g_live" / "live_kite" / "heartbeat.json",
        {
            "session_id": "future-live",
            "execution_mode": "LIVE",
            "heartbeat_ist": "2026-09-24T10:03:00+05:30",
        },
    )
    status = runtime / "runtime_status"
    _text(
        status / "active.heartbeat",
        "name=active-live\nstate=RUNNING\nts_utc=2026-09-24T04:29:00+00:00\n",
    )
    _text(
        status / "done.heartbeat",
        "name=done-live\nstate=COMPLETED\nts_utc=2026-09-24T04:29:30+00:00\n",
    )
    _text(
        status / "old.heartbeat",
        "name=old-live\nstate=RUNNING\nts_utc=2026-09-23T04:29:30+00:00\n",
    )

    rendered = _collector(runtime, registry).render_prometheus_samples()

    assert rendered.count("trading_heartbeat_age_seconds{") == 1
    assert 'service="active-live"' in rendered
    assert "future-live" not in rendered
    assert "done-live" not in rendered
    assert "old-live" not in rendered


def test_equivalent_oi_producer_heartbeats_use_freshest_logical_pipeline(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    status = runtime / "runtime_status"
    _text(
        status / "fno_oi_fetch_5min.heartbeat",
        "state=RUNNING\nsession=fno_oi_fetch_5min\nts=2026-09-24T09:05:00+05:30\n",
    )
    _text(
        status / "fno_oi_fetch_5min_fast_production.heartbeat",
        "state=WAITING\nsession=fno_oi_fetch_5min_fast_production\n"
        "ts=2026-09-24T09:59:30+05:30\n",
    )

    rendered = _collector(runtime, registry).render_prometheus_samples()

    expected = (
        'trading_heartbeat_age_seconds{mode="live",'
        'service="fno_oi_5min_production"} 30'
    )
    assert expected in rendered
    assert rendered.count('service="fno_oi_5min_production"') == 1
    assert 'service="fno_oi_fetch_5min"' not in rendered
    assert 'service="fno_oi_fetch_5min_fast_production"' not in rendered


def test_frozen_scanner_schedule_detects_missing_and_late_completion(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    heartbeat = (
        runtime
        / "runtime_status"
        / "fno_v13_v10_g_scanner_5min.heartbeat"
    )

    def render_at(stamp: str) -> str:
        observed = datetime.fromisoformat(stamp)
        return RuntimeMetricsCollector(
            runtime,
            observability_root=runtime / "observability",
            profile_registry_path=registry,
            cache_ttl_seconds=0,
            now=lambda: observed,
        ).render_prometheus_samples()

    series = (
        'trading_pipeline_schedule_overdue{mode="live",'
        'pipeline="v13_v10_g_scanner"}'
    )
    assert f"{series} 0" in render_at("2026-09-24T10:02:59+05:30")
    assert f"{series} 1" in render_at("2026-09-24T10:03:01+05:30")

    _text(
        heartbeat,
        "\n".join(
            (
                "state=SUCCESS",
                "session=fno_v13_v10_g_scanner_5min",
                "ts=2026-09-24T10:00:20+05:30",
                "phase=SLOT_DONE",
                "slot=10:00",
            )
        ),
    )
    assert f"{series} 0" in render_at("2026-09-24T10:03:01+05:30")
    assert f"{series} 1" in render_at("2026-09-24T11:23:01+05:30")

    _text(
        heartbeat,
        "\n".join(
            (
                "state=WAITING",
                "session=fno_v13_v10_g_scanner_5min",
                "ts=2026-09-24T11:20:20+05:30",
                "phase=WAIT_NEXT_SLOT",
                "last_completed_slot=11:20",
                "processed_slots=9",
            )
        ),
    )
    assert f"{series} 0" in render_at("2026-09-24T11:23:01+05:30")


def test_frozen_scanner_terminal_proof_must_be_complete(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    heartbeat = (
        runtime
        / "runtime_status"
        / "fno_v13_v10_g_scanner_5min.heartbeat"
    )
    now = datetime.fromisoformat("2026-09-24T11:23:01+05:30")
    collector = lambda: RuntimeMetricsCollector(
        runtime,
        observability_root=runtime / "observability",
        profile_registry_path=registry,
        cache_ttl_seconds=0,
        now=lambda: now,
    ).render_prometheus_samples()
    series = (
        'trading_pipeline_schedule_overdue{mode="live",'
        'pipeline="v13_v10_g_scanner"}'
    )

    _text(
        heartbeat,
        "state=DONE\nsession=fno_v13_v10_g_scanner_5min\n"
        "ts=2026-09-24T11:20:30+05:30\nphase=ALL_V6_WINDOWS_DONE\n"
        "processed_slots=8\n",
    )
    assert f"{series} 1" in collector()

    _text(
        heartbeat,
        "state=DONE\nsession=fno_v13_v10_g_scanner_5min\n"
        "ts=2026-09-24T11:20:30+05:30\nphase=ALL_V6_WINDOWS_DONE\n"
        "processed_slots=9\n",
    )
    assert f"{series} 0" in collector()


def test_fingerprint_requires_fresh_current_nonterminal_live_evidence(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(
        registry,
        {"profiles": {"V13_V10_G": {"strategy_fingerprint": "approved"}}},
    )
    root = runtime / "fno_oi" / "v13_v10_g_live"
    heartbeat = root / "live_kite" / "heartbeat.json"
    _json(root / "strategy_manifest.json", {"strategy_fingerprint": "approved"})
    collector = _collector(runtime, registry)
    series = "trading_strategy_fingerprint_mismatch"

    def heartbeat_at(stamp: str, *, state: str = "RUNNING", fingerprint: str = "wrong") -> None:
        _json(
            heartbeat,
            {
                "session_id": "live-worker",
                "execution_mode": "LIVE",
                "state": state,
                "heartbeat_ist": stamp,
                "strategy_fingerprint": fingerprint,
            },
        )

    heartbeat_at("2026-09-23T09:59:30+05:30")
    assert series not in collector.render_prometheus_samples()

    heartbeat_at("2026-09-24T09:57:59+05:30")
    assert series not in collector.render_prometheus_samples()

    heartbeat_at("2026-09-24T10:00:01+05:30")
    assert series not in collector.render_prometheus_samples()

    heartbeat_at("2026-09-24T09:59:30+05:30", state="DONE")
    assert series not in collector.render_prometheus_samples()

    heartbeat_at("2026-09-24T09:59:30+05:30", fingerprint="")
    assert series not in collector.render_prometheus_samples()

    heartbeat_at("2026-09-24T09:59:30+05:30")
    assert f'{series}{{mode="live",service="live-worker",strategy="v13_v10_g"}} 1' in (
        collector.render_prometheus_samples()
    )

    heartbeat_at("2026-09-24T09:59:45+05:30", fingerprint="approved")
    assert f'{series}{{mode="live",service="live-worker",strategy="v13_v10_g"}} 0' in (
        collector.render_prometheus_samples()
    )


def test_future_marker_is_rejected_and_latest_valid_marker_is_used(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    marker_root = runtime / "fno_oi" / "slot_ready"
    valid = marker_root / "valid.json"
    future = marker_root / "future.json"
    _json(
        valid,
        {"slot_ist": "2026-09-24T09:55:00+05:30", "coverage_ratio": 1.0},
    )
    _json(
        future,
        {"slot_ist": "2026-09-24T10:05:00+05:30", "coverage_ratio": 1.0},
    )
    _json(
        runtime
        / "fno_oi"
        / "v13_v10_g_live"
        / "orders"
        / "LIVE"
        / "live_kite_qty1"
        / "2026-09-24"
        / "future.json",
        {
            "mode": "LIVE",
            "session_date": "2026-09-24",
            "status": "OPEN",
            "entry_at_ist": "2026-09-24T10:05:00+05:30",
        },
    )
    os.utime(valid, (1, 1))
    os.utime(future, (2, 2))

    rendered = _collector(runtime, registry).render_prometheus_samples()

    assert 'trading_data_age_seconds{source="futures_oi",timeframe="5m"} 300' in rendered
    assert "trading_unprotected_position_seconds" not in rendered


def test_reconciliation_counters_are_process_local_and_nondecreasing(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    reports = runtime / "observability" / "reconciliation"
    first = reports / "first.json"
    second = reports / "second.json"
    mismatch = {
        "state": "MISMATCH",
        "first_divergence_stage": "feature",
        "mismatch_type": "numeric",
        "session_date": "2026-09-24",
    }
    first_payload = _signed_report(
        {**mismatch, "generated_at_ist": "2026-09-24T16:00:00+05:30"}
    )
    _json(first, first_payload)
    collector = _collector(runtime, registry)
    series = (
        'trading_live_eod_mismatch_total{mismatch_type="numeric",stage="feature"}'
    )

    assert f"{series} 1" in collector.render_prometheus_samples()
    assert f"{series} 1" in collector.render_prometheus_samples()

    # Rewriting/copying identical signed content is the same immutable report
    # and must not increment the process-local projection again.
    _json(first, first_payload)
    duplicate = reports / "duplicate.json"
    _json(duplicate, first_payload)
    assert f"{series} 1" in collector.render_prometheus_samples()

    _json(
        second,
        _signed_report(
            {**mismatch, "generated_at_ist": "2026-09-24T16:01:00+05:30"}
        ),
    )
    assert f"{series} 2" in collector.render_prometheus_samples()

    tampered = reports / "tampered.json"
    invalid = _signed_report(
        {**mismatch, "generated_at_ist": "2026-09-24T16:02:00+05:30"}
    )
    invalid["mismatch_type"] = "tampered"
    _json(tampered, invalid)
    assert f"{series} 2" in collector.render_prometheus_samples()
    # A collector restart rebuilds the same value from unique verified report
    # digests rather than counting rewritten/copied evidence again.
    restarted = _collector(runtime, registry)
    assert f"{series} 2" in restarted.render_prometheus_samples()

    first.rename(first.with_suffix(".archived"))
    second.rename(second.with_suffix(".archived"))
    duplicate.rename(duplicate.with_suffix(".archived"))
    tampered.rename(tampered.with_suffix(".archived"))
    assert f"{series} 2" in collector.render_prometheus_samples()


def test_broker_reconciliation_requires_current_session_and_fresh_semantic_time(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    registry = tmp_path / "profiles.json"
    _json(registry, {"profiles": {}})
    reports = runtime / "observability" / "reconciliation"

    def report(
        name: str,
        *,
        session_date: str,
        generated_at_ist: str,
        mismatch_count: int,
        scope_complete: bool,
        active_order_mismatch_count: int = 0,
        active_order_parity_complete: bool = True,
        signed: bool = True,
        tampered: bool = False,
    ) -> Path:
        path = reports / f"{name}.json"
        payload = {
            "schema_version": "v13_v10_g_broker_position_reconciliation_v2",
            "session_date": session_date,
            "generated_at_ist": generated_at_ist,
            "position_reconciliation": {
                "broker_truth_available": True,
                "scope_complete": scope_complete,
                "mismatch_count": mismatch_count,
                "active_order_parity_complete": active_order_parity_complete,
                "active_order_mismatch_count": active_order_mismatch_count,
                "active_order_mismatches": [
                    {"kind": "unit_active_order_mismatch", "index": index}
                    for index in range(active_order_mismatch_count)
                ],
                "tagged_order_count": active_order_mismatch_count,
                "local_expected_active_order_count": 0,
                "local_expected_active_order_ids": [],
                "broker_active_tagged_order_count": 0,
                "broker_active_tagged_order_ids": [],
            },
        }
        if signed:
            payload = _signed_report(payload)
        if tampered:
            payload["generated_at_ist"] = "2026-09-24T09:59:59+05:30"
        _json(path, payload)
        return path

    report(
        "yesterday",
        session_date="2026-09-23",
        generated_at_ist="2026-09-24T09:59:50+05:30",
        mismatch_count=0,
        scope_complete=True,
    )
    report(
        "stale",
        session_date="2026-09-24",
        generated_at_ist="2026-09-24T09:57:59+05:30",
        mismatch_count=0,
        scope_complete=True,
    )
    report(
        "future",
        session_date="2026-09-24",
        generated_at_ist="2026-09-24T10:00:01+05:30",
        mismatch_count=0,
        scope_complete=True,
    )
    fresh_zero = report(
        "fresh_zero",
        session_date="2026-09-24",
        generated_at_ist="2026-09-24T09:59:30+05:30",
        mismatch_count=0,
        scope_complete=True,
    )
    fresh_scoped = report(
        "fresh_scoped",
        session_date="2026-09-24",
        generated_at_ist="2026-09-24T09:59:45+05:30",
        mismatch_count=2,
        scope_complete=False,
        active_order_mismatch_count=3,
        active_order_parity_complete=False,
    )
    report(
        "unsigned",
        session_date="2026-09-24",
        generated_at_ist="2026-09-24T09:59:55+05:30",
        mismatch_count=0,
        scope_complete=True,
        signed=False,
    )
    report(
        "tampered",
        session_date="2026-09-24",
        generated_at_ist="2026-09-24T09:59:56+05:30",
        mismatch_count=0,
        scope_complete=True,
        tampered=True,
    )
    collector = _collector(runtime, registry)
    series = 'trading_position_reconciliation_mismatch{asset="equity",mode="live"}'
    order_series = (
        'trading_active_order_reconciliation_mismatch{asset="equity",mode="live"}'
    )

    rendered = collector.render_prometheus_samples()
    assert f"{series} 2" in rendered
    assert f"{order_series} 3" in rendered

    fresh_scoped.rename(fresh_scoped.with_suffix(".archived"))
    rendered = collector.render_prometheus_samples()
    assert f"{series} 0" in rendered
    assert f"{order_series} 0" in rendered

    fresh_zero.rename(fresh_zero.with_suffix(".archived"))
    rendered = collector.render_prometheus_samples()
    assert series not in rendered
    assert order_series not in rendered
