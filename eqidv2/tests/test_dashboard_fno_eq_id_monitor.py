from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import log_dashboard_server as dashboard


DAY = "2026-09-01"


def _timeline_roots(
    tmp_path: Path,
    monkeypatch,
) -> tuple[Path, Path, Path]:
    slot_root = tmp_path / "slot_ready_5m"
    fno_root = tmp_path / "fno_oi"
    multi_root = tmp_path / "multi_paper"
    monkeypatch.setattr(dashboard, "SLOT_READY_5M_DIR", slot_root)
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", fno_root)
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_ROOT", multi_root)
    monkeypatch.setattr(
        dashboard,
        "FNO_MULTI_PAPER_STATUS_PATH",
        multi_root / "status.json",
    )
    return slot_root, fno_root, multi_root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    assert rows
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _valid_shared_manifest(slot: str) -> dict[str, object]:
    signal_hour, signal_minute = map(int, slot.split(":"))
    due_minute = signal_minute + 1
    payload: dict[str, object] = {
        "schema_version": "fno_multi_paper_5m_source_v2",
        "session_date": DAY,
        "signal_end": slot,
        "signal_timestamp": f"{DAY}T{signal_hour:02d}:{signal_minute:02d}:00+05:30",
        "confirmation_due_ist": f"{DAY}T{signal_hour:02d}:{due_minute:02d}:00+05:30",
        "decision_at_ist": f"{DAY}T{signal_hour:02d}:{signal_minute:02d}:30+05:30",
        "decision_before_confirmation_due": True,
        "universe_count": 1,
        "row_count": 2,
        "symbol_tokens": {"TEST": 1},
        "rows": [{"side": "LONG"}, {"side": "SHORT"}],
    }
    payload["manifest_sha256"] = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return payload


def _scoped_items() -> list[dict[str, object]]:
    disabled = {
        "kiteticker_5min_data",
        "eod_1min_data",
        "fno_v8_combined_paper",
    }
    items: list[dict[str, object]] = []
    for _, card_ids in dashboard.FNO_EQ_ID_MONITOR_GROUPS:
        for card_id in card_ids:
            if card_id in disabled:
                status = {
                    "status": "DISABLED",
                    "scheduler_status": "DISABLED",
                    "scheduler_tasks": f"\\EQIDV2_{card_id}",
                }
            else:
                status = {
                    "status": "RUNNING",
                    "scheduler_status": "RUNNING",
                    "scheduler_tasks": f"\\EQIDV2_{card_id}",
                    "ts": "2026-09-01T10:00:00+05:30",
                }
            if card_id in dashboard.FNO_MULTI_PAPER_CARD_IDS:
                status.update(
                    {
                        "scheduler_tasks": "\\EQIDV2_fno_v10_v11_v12_paper_0915",
                        "candidate_count": "2",
                        "long_candidates": "1",
                        "short_candidates": "1",
                        "gap_guard_rejections": "1",
                        "fill_count": "1",
                        "open_count": "1",
                        "closed_count": "0",
                        "net_pnl_rs": "12.50",
                    }
                )
            if card_id in {
                "live_signals_csv_fno_id_v6_short",
                "live_signals_csv_fno_id_v6_long",
                "live_kite_trades_csv_fno_id_v6",
                "kite_trade_fno_id_v6",
            }:
                status["scheduler_tasks"] = (
                    "\\EQIDV2_fno_v6_live_kite_qty1_0915"
                )
            items.append(
                {
                    "id": card_id,
                    "exists": card_id not in disabled,
                    "mtime": "2026-09-01 10:00:00",
                    "file_name": f"{card_id}.log",
                    "status": status,
                    "tail": "latest slot complete",
                }
            )
    return items


def _empty_timeline(*_args, **_kwargs) -> dict[str, object]:
    return {
        "session_date": DAY,
        "generated_at_ist": f"{DAY}T10:00:00+05:30",
        "five_minute_rows": [],
        "one_minute_rows": [],
        "generic_1m_state": "DISABLED (intentional)",
        "shared_runtime": "RUNNING",
        "shared_phase": "TEST",
        "hard_issue_count": 0,
        "active_hard_issue_count": 0,
        "closed_hard_issue_count": 0,
        "watch_issue_count": 0,
        "issue_count": 0,
    }


def test_monitor_scope_has_exact_requested_29_unique_views() -> None:
    ids = [
        card_id
        for _, card_ids in dashboard.FNO_EQ_ID_MONITOR_GROUPS
        for card_id in card_ids
    ]

    assert len(ids) == 29
    assert len(set(ids)) == 29
    assert [name for name, _ in dashboard.FNO_EQ_ID_MONITOR_GROUPS] == [
        "Live Market Data",
        "FnO",
        "V10 / V11 / V12 Shared Papertrade Session",
        "FnO V6 Live Kite - Quantity 1",
        "V10",
        "V11",
        "V12",
        "Data & Backtesting",
        "SESSION",
    ]


def test_disabled_is_inactive_and_fail_closed_block_is_watch() -> None:
    assert dashboard._fno_eq_id_monitor_state(
        {"status": "RUNNING", "scheduler_status": "DISABLED"}, exists=True
    ) == ("INACTIVE", "RUNNING", False)
    assert dashboard._fno_eq_id_monitor_state(
        {"status": "BLOCKED", "phase": "INCOMPLETE_BY_DEADLINE"}, exists=True
    ) == ("WATCH", "BLOCKED", True)
    assert dashboard._fno_eq_id_monitor_state(
        {"status": "FAILED", "phase": "CRASHED"}, exists=True
    ) == ("PROBLEM", "FAILED", True)
    assert dashboard._fno_eq_id_monitor_state(
        {"status": "STOPPED", "reason": "hard_stop_reached"}, exists=True
    ) == ("OK", "STOPPED", True)
    assert dashboard._fno_eq_id_monitor_state(
        {"status": "STOPPED", "reason": "worker_missing"}, exists=True
    ) == ("PROBLEM", "STOPPED", True)
    assert dashboard._fno_eq_id_monitor_state(
        {
            "status": "RUNNING",
            "scheduler_status": "DISABLED",
            "scheduler_attention": "DISABLED_WHILE_RUNNING",
            "runtime_start_mode": "MANUAL",
        },
        exists=True,
    ) == ("WATCH", "RUNNING", True)


def test_aggregate_deduplicates_shared_task_and_keeps_profile_counters(monkeypatch) -> None:
    items = _scoped_items()
    scanner = next(item for item in items if item["id"] == "fno_v6_scanner_5min")
    scanner["status"] = {
        **scanner["status"],
        "status": "BLOCKED",
        "phase": "INCOMPLETE_BY_DEADLINE",
    }
    monkeypatch.setattr(dashboard, "_fno_eq_id_monitor_detail_path", lambda _card_id: None)
    monkeypatch.setattr(dashboard, "_build_fno_eq_id_strategy_timelines", _empty_timeline)
    monkeypatch.setattr(
        dashboard,
        "_fno_eq_id_monitor_safe_auth_status",
        lambda _today: {
            "authenticated_apps": 8,
            "configured_apps": 8,
            "access_token_present": "YES",
            "auth_session_date": "2026-09-01",
        },
    )

    tail, status, mtime, configured, exists = dashboard._format_fno_eq_id_monitor(
        items,
        now_ist=datetime(2026, 9, 1, 10, 0, tzinfo=dashboard.IST),
    )

    assert configured == 29
    assert exists is True
    assert mtime == "2026-09-01 10:00:00"
    assert status["status"] == "PARTIAL"
    assert status["inactive_sessions"] == "3"
    assert status["physical_tasks"] == "23"
    assert "# FnO EQ ID monitoring" in tail
    assert "gap_guard_rej=1" in tail
    assert "V10 selection + guards + entry + result" in tail
    assert "NO (disabled)" in tail


def test_hard_timeline_evidence_makes_aggregate_failed(monkeypatch) -> None:
    timeline = _empty_timeline()
    timeline["hard_issue_count"] = 1
    timeline["active_hard_issue_count"] = 1
    timeline["issue_count"] = 1
    monkeypatch.setattr(
        dashboard,
        "_build_fno_eq_id_strategy_timelines",
        lambda *_args, **_kwargs: timeline,
    )
    monkeypatch.setattr(dashboard, "_fno_eq_id_monitor_detail_path", lambda _card_id: None)
    monkeypatch.setattr(
        dashboard,
        "_fno_eq_id_monitor_safe_auth_status",
        lambda _today: {"authenticated_apps": 8, "configured_apps": 8},
    )

    _, status, *_ = dashboard._format_fno_eq_id_monitor(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 10, 0, tzinfo=dashboard.IST),
    )

    assert status["status"] == "FAILED"
    assert status["timeline_hard_issues"] == "1"


def test_closed_timeline_gap_is_watch_not_current_failure(monkeypatch) -> None:
    timeline = _empty_timeline()
    timeline["hard_issue_count"] = 2
    timeline["active_hard_issue_count"] = 0
    timeline["closed_hard_issue_count"] = 2
    timeline["issue_count"] = 2
    monkeypatch.setattr(
        dashboard,
        "_build_fno_eq_id_strategy_timelines",
        lambda *_args, **_kwargs: timeline,
    )
    monkeypatch.setattr(dashboard, "_fno_eq_id_monitor_detail_path", lambda _card_id: None)
    monkeypatch.setattr(
        dashboard,
        "_fno_eq_id_monitor_safe_auth_status",
        lambda _today: {"authenticated_apps": 8, "configured_apps": 8},
    )

    _, status, *_ = dashboard._format_fno_eq_id_monitor(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 10, 0, tzinfo=dashboard.IST),
    )

    assert status["status"] == "PARTIAL"
    assert status["flow_status"] == "WATCH"
    assert status["timeline_hard_issues"] == "0"
    assert status["timeline_closed_hard_issues"] == "2"


def test_hard_failure_makes_aggregate_failed(monkeypatch) -> None:
    items = _scoped_items()
    fetch = next(item for item in items if item["id"] == "fno_oi_fetch_5min")
    fetch["status"] = {
        **fetch["status"],
        "status": "FAILED",
        "phase": "CRASHED",
        "reason": "worker exited",
    }
    monkeypatch.setattr(dashboard, "_fno_eq_id_monitor_detail_path", lambda _card_id: None)

    tail, status, *_ = dashboard._format_fno_eq_id_monitor(items)

    assert status["status"] == "FAILED"
    assert status["problem_sessions"] == "1"
    assert "worker exited" in tail


def test_auth_monitor_is_strictly_redacted(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "auth_v2_state.json"
    token_path = tmp_path / "access_token.txt"
    state_path.write_text(
        json.dumps(
            {
                "session_date_ist": "2026-09-01",
                "updated_at_ist": "2026-09-01T08:30:00+05:30",
                "request_token": "DO_NOT_EXPOSE_REQUEST_TOKEN",
                "refresh_token": "DO_NOT_EXPOSE_REFRESH_TOKEN",
                **{
                    f"session_date_ist_app{index}": "2026-09-01"
                    for index in range(2, 9)
                },
            }
        ),
        encoding="utf-8",
    )
    token_path.write_text("DO_NOT_EXPOSE_ACCESS_TOKEN", encoding="utf-8")
    monkeypatch.setattr(dashboard, "AUTH_V2_STATE_FILE", state_path)
    monkeypatch.setattr(dashboard, "AUTH_V2_ACCESS_TOKEN_FILE", token_path)
    monkeypatch.setattr(dashboard, "_fno_eq_id_monitor_detail_path", lambda _card_id: None)
    items = _scoped_items()
    auth = next(item for item in items if item["id"] == "authentication_v2")
    auth["tail"] = "DO_NOT_EXPOSE_LOG_SECRET"

    tail, *_ = dashboard._format_fno_eq_id_monitor(
        items,
        now_ist=datetime(2026, 9, 1, 10, 0, tzinfo=dashboard.IST),
    )

    assert "apps_authenticated=8" in tail
    assert "access_token_file=YES" in tail
    assert "DO_NOT_EXPOSE" not in tail


def test_dashboard_title_and_placement_are_fno_not_v7() -> None:
    source = Path(dashboard.__file__).read_text(encoding="utf-8")
    fno_group = source[
        source.index('key: "fno"') : source.index('key: "forensic-positional"')
    ]
    v7_group = source[
        source.index('key: "v7"') : source.index('key: "backtesting"')
    ]

    assert '"v7_live_5min_monitor": "FnO EQ ID monitoring"' in source
    assert '"v7_live_5min_monitor"' in fno_group
    assert '"v7_live_5min_monitor"' not in v7_group
    assert '"v7_live_5min_monitor"' in source[
        source.index("const MD_REPORT_CARDS") : source.index("const FNO_MULTI_PAPER_CARDS")
    ]


def test_fast_production_old_and_shadow_validator_have_exact_fetch_order() -> None:
    expected_fetch_order = (
        "fno_oi_fetch_5min_fast_production",
        "fno_oi_fetch_5min",
        "fno_oi_fetch_5min_fast_shadow",
    )
    fno_group = dict(dashboard.FNO_EQ_ID_MONITOR_GROUPS)["FnO"]
    production_index = fno_group.index("fno_oi_fetch_5min_fast_production")
    assert fno_group[production_index : production_index + 3] == expected_fetch_order

    log_ids = list(dashboard.LOG_FILES)
    production_index = log_ids.index("fno_oi_fetch_5min_fast_production")
    assert tuple(log_ids[production_index : production_index + 3]) == expected_fetch_order

    assert dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS[
        "fno_oi_fetch_5min_fast_production"
    ] == "FnO Live 5-Minute Futures OI Fetch (Fast Production)"
    assert dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS[
        "fno_oi_fetch_5min"
    ] == "FnO Live 5-Minute Futures OI Fetch (Old)"
    assert dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS[
        "fno_oi_fetch_5min_fast_shadow"
    ] == "FnO Fast Shadow OI Validator"
    assert dashboard.CARD_TASK_NAMES["fno_oi_fetch_5min_fast_production"] == (
        "\\EQIDV2_fno_oi_fetch_5min_fast_production_0905",
    )
    assert (
        dashboard.RESTARTABLE_CARDS["fno_oi_fetch_5min_fast_production"]
        == "run_fno_oi_fetch_5min_fast_production.bat"
    )
    assert (
        dashboard.FNO_OI_CARD_REPORTS["fno_oi_fetch_5min_fast_production"]
        == "latest_fno_oi_fast_production.md"
    )
    assert (
        dashboard.FNO_OI_CARD_REPORTS["fno_oi_fetch_5min"]
        == "latest_fno_oi_fetch_old.md"
    )
    assert (
        dashboard.FNO_OI_CARD_REPORTS["fno_oi_fetch_5min_fast_production"]
        != dashboard.FNO_OI_CARD_REPORTS["fno_oi_fetch_5min"]
    )

    source = Path(dashboard.__file__).read_text(encoding="utf-8")
    adjacency = (
        r'"fno_oi_fetch_5min_fast_production",\s*'
        r'"fno_oi_fetch_5min",\s*'
        r'"fno_oi_fetch_5min_fast_shadow"'
    )
    log_order = source[
        source.index("const LOG_ORDER") : source.index("const LOG_TITLES")
    ]
    active_groups = source[source.index("const ACTIVE_GROUPS") :]
    fno_start = active_groups.index('key: "fno"')
    fno_end = active_groups.index("subgroups:", fno_start)
    assert re.search(adjacency, log_order)
    assert re.search(adjacency, active_groups[fno_start:fno_end])
    assert '"fno_oi_fetch_5min_fast_production": "FnO Live 5-Minute Futures OI Fetch (Fast Production)"' in source
    assert '"fno_oi_fetch_5min": "FnO Live 5-Minute Futures OI Fetch (Old)"' in source
    assert '"fno_oi_fetch_5min_fast_shadow": "FnO Fast Shadow OI Validator"' in source
    assert '{ time: "08:30", id: "authentication_v2", label: "Auth" }' in source


def test_strategy_timelines_have_exact_inclusive_grids_and_windows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _timeline_roots(tmp_path, monkeypatch)

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 9, 20, tzinfo=dashboard.IST),
    )

    five_rows = timeline["five_minute_rows"]
    minute_rows = timeline["one_minute_rows"]
    assert [row["slot"] for row in five_rows] == [
        "09:15",
        "09:20",
        "09:25",
        "09:30",
        "09:35",
        "09:40",
        "09:45",
        "09:50",
    ]
    assert [row["minute"] for row in minute_rows] == [
        f"09:{minute:02d}" for minute in range(15, 51)
    ]
    assert len(five_rows) == 8
    assert len(minute_rows) == 36

    by_slot = {row["slot"]: row for row in five_rows}
    for slot in ("09:15", "09:20", "09:50"):
        assert by_slot[slot]["v6"] == "OFF WINDOW"
        assert by_slot[slot]["shared"] == "OFF WINDOW"
        assert by_slot[slot]["v10"] == "OFF WINDOW"
        assert by_slot[slot]["v11"] == "OFF WINDOW"
        assert by_slot[slot]["v12"] == "OFF WINDOW"
    for slot in ("09:25", "09:30", "09:35", "09:40", "09:45"):
        assert by_slot[slot]["purpose"] == "STRATEGY SELECTION"
        assert by_slot[slot]["shared"] != "OFF WINDOW"


def test_shared_checkpoint_without_union_marker_means_reduced_no_required_symbols(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, _, multi_root = _timeline_roots(tmp_path, monkeypatch)
    processed = {f"{DAY}T09:26:00+05:30": "immutable-input-fingerprint"}
    _write_json(
        multi_root / "sessions" / DAY / "checkpoint.json",
        {
            "engine": {
                "engines": {
                    profile: {"processed_minute_fingerprints": processed}
                    for profile in ("v10", "v11", "v12")
                }
            }
        },
    )
    _write_json(
        multi_root / "status.json",
        {
            "session_date": DAY,
            "status": "RUNNING",
            "phase": "CHRONOLOGICAL_PAPER_REDUCER",
            "last_processed_minute": f"{DAY}T09:26:00+05:30",
        },
    )

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 9, 27, 30, tzinfo=dashboard.IST),
    )

    row = next(
        item for item in timeline["one_minute_rows"] if item["minute"] == "09:26"
    )
    assert row["shared_source"] == "REDUCED; no required symbols"
    for profile in ("v10", "v11", "v12"):
        assert row[profile].startswith("NO EVENT; book O0/C0 R=")
    assert "BLOCKED" not in row["shared_source"]


def test_skipped_shared_five_minute_slot_is_explicit_not_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, _, multi_root = _timeline_roots(tmp_path, monkeypatch)
    _write_json(
        multi_root / "status.json",
        {
            "session_date": DAY,
            "status": "DEGRADED",
            "phase": "FORWARD_ONLY_RECOVERY",
            "skipped_slots": ["09:30"],
            "ingested_slots": ["09:25"],
        },
    )

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 10, 0, tzinfo=dashboard.IST),
    )

    five = next(
        item for item in timeline["five_minute_rows"] if item["slot"] == "09:30"
    )
    assert five["shared"] == "SKIPPED (forward-only recovery)"
    assert [five[profile] for profile in ("v10", "v11", "v12")] == [
        "SKIPPED / NO SIGNAL",
        "SKIPPED / NO SIGNAL",
        "SKIPPED / NO SIGNAL",
    ]
    minute = next(
        item for item in timeline["one_minute_rows"] if item["minute"] == "09:30"
    )
    for profile in ("v10", "v11", "v12"):
        assert minute[profile].startswith("5m SOURCE SKIPPED; book O0/C0 R=")
    assert not any(
        term in five["shared"] for term in ("BLOCKED", "MISMATCH", "INCOMPLETE")
    )
    assert timeline["active_hard_issue_count"] == 0
    assert timeline["closed_hard_issue_count"] == timeline["hard_issue_count"]


def test_v6_signal_confirms_at_s_plus_1_and_entry_book_has_no_future_pnl(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, fno_root, _ = _timeline_roots(tmp_path, monkeypatch)
    v6_root = fno_root / "v6_live"
    _write_json(
        v6_root / "scanner_5m" / DAY / "slot_0925.json",
        {
            "session_date": DAY,
            "signal_end": "09:25",
            "state": "SUCCESS",
            "long_candidates": 1,
            "short_candidates": 0,
        },
    )
    _write_json(
        v6_root / "confirmation_1m" / DAY / "slot_0926.json",
        {
            "session_date": DAY,
            "signal_end": "09:25",
            "confirmation_end": "09:26",
            "state": "SUCCESS",
            "scanner_complete": True,
            "error_count": 0,
            "candidate_count": 1,
            "confirmation_bars": 1,
            "ineligible_no_candle_count": 0,
            "selected_long": 1,
            "selected_short": 0,
            "selected_signal_ids": ["20260901_0925_LONG_TEST_abcdef12"],
        },
    )
    _write_csv(
        v6_root / "consolidated" / f"fno_v6_trades_{DAY}.csv",
        [
            {
                "session_date": DAY,
                "signal_end": "09:25",
                "signal_id": "20260901_0925_LONG_TEST_abcdef12",
                "symbol": "TEST",
                "entry_at_ist": f"{DAY}T09:27:00+05:30",
                "exit_at_ist": f"{DAY}T09:40:00+05:30",
                "net_pnl_rs": "500.00",
            }
        ],
    )

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 9, 50, 30, tzinfo=dashboard.IST),
    )
    minutes = {row["minute"]: row["v6"] for row in timeline["one_minute_rows"]}

    assert "5m SEL L1/S0" in minutes["09:25"]
    assert "1m CONF" not in minutes["09:25"]
    assert "1m CONF 1 [TEST]" in minutes["09:26"]
    assert "ENTRY" not in minutes["09:26"]
    assert "ENTRY 1 [TEST]" in minutes["09:27"]
    assert "book O1/C0" in minutes["09:27"]
    assert "500.00" not in minutes["09:27"]
    assert "book O1/C0" in minutes["09:39"]
    assert "500.00" not in minutes["09:39"]
    assert "EXIT 1 [TEST]" in minutes["09:40"]
    assert "book O0/C1" in minutes["09:40"]
    assert "500.00" in minutes["09:40"]


def test_cross_day_source_marker_is_never_accepted_as_current() -> None:
    cell = dashboard._fno_eq_id_timeline_source_cell(
        {
            "slot_ist": "2026-08-31T09:25:00+05:30",
            "published_at_ist": "2026-08-31T09:25:10+05:30",
            "source": "final",
            "complete": True,
            "written": 10,
            "expected": 10,
        },
        slot="09:25",
        session_date=DAY,
        now_ist=datetime(2026, 9, 1, 9, 30, tzinfo=dashboard.IST),
        complete_fields=("complete",),
        written_fields=("written",),
        expected_fields=("expected",),
    )

    assert cell.startswith("EVIDENCE MISMATCH")


def test_failed_v6_confirmation_is_not_rendered_as_confirmed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, fno_root, _ = _timeline_roots(tmp_path, monkeypatch)
    v6_root = fno_root / "v6_live"
    _write_json(
        v6_root / "scanner_5m" / DAY / "slot_0925.json",
        {
            "session_date": DAY,
            "signal_end": "09:25",
            "state": "SUCCESS",
            "long_candidates": 1,
            "short_candidates": 0,
        },
    )
    _write_json(
        v6_root / "confirmation_1m" / DAY / "slot_0926.json",
        {
            "session_date": DAY,
            "signal_end": "09:25",
            "confirmation_end": "09:26",
            "state": "FAILED",
            "scanner_complete": False,
            "error_count": 1,
            "candidate_count": 1,
            "confirmation_bars": 0,
            "ineligible_no_candle_count": 0,
            "selected_long": 1,
            "selected_short": 0,
            "selected_signal_ids": ["20260901_0925_LONG_TEST_abcdef12"],
        },
    )

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 9, 28, tzinfo=dashboard.IST),
    )
    five = next(row for row in timeline["five_minute_rows"] if row["slot"] == "09:25")
    minute = next(row for row in timeline["one_minute_rows"] if row["minute"] == "09:26")

    assert "CONF BLOCKED/INCOMPLETE" in five["v6"]
    assert "1m CONF 1" not in minute["v6"]
    assert "BLOCKED" in minute["v6"]


def test_processed_shared_slot_requires_profile_selection_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, _, multi_root = _timeline_roots(tmp_path, monkeypatch)
    _write_json(
        multi_root / "status.json",
        {
            "session_date": DAY,
            "status": "RUNNING",
            "phase": "CHRONOLOGICAL_PAPER_REDUCER",
            "ingested_slots": ["09:25"],
        },
    )
    _write_json(
        multi_root / "evidence" / DAY / "five_minute" / "slot_0925" / "manifest.json",
        _valid_shared_manifest("09:25"),
    )

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 9, 27, tzinfo=dashboard.IST),
    )
    five = next(row for row in timeline["five_minute_rows"] if row["slot"] == "09:25")

    assert five["shared"].startswith("PROCESSED rows=2")
    for profile in ("v10", "v11", "v12"):
        assert five[profile] == "MISSING SELECTION AUDIT"


def test_expected_preselection_warmup_does_not_raise_timeline_alarm(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _, _, multi_root = _timeline_roots(tmp_path, monkeypatch)
    processed = {
        f"{DAY}T09:{minute:02d}:00+05:30": f"fingerprint-{minute}"
        for minute in range(15, 21)
    }
    _write_json(
        multi_root / "sessions" / DAY / "checkpoint.json",
        {
            "engine": {
                "engines": {
                    profile: {"processed_minute_fingerprints": processed}
                    for profile in ("v10", "v11", "v12")
                }
            }
        },
    )

    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        _scoped_items(),
        now_ist=datetime(2026, 9, 1, 9, 20, tzinfo=dashboard.IST),
    )

    assert timeline["hard_issue_count"] == 0
    assert timeline["watch_issue_count"] == 0
