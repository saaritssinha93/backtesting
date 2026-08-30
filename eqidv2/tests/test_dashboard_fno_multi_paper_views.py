from __future__ import annotations

import json
import re
from pathlib import Path

import log_dashboard_server as dashboard


PARENT_ID = "fno_v10_v11_v12_paper"
PROFILE_IDS = ("fno_v10_paper", "fno_v11_paper", "fno_v12_paper")
ALL_IDS = (PARENT_ID, *PROFILE_IDS)
TASK = ("\\EQIDV2_fno_v10_v11_v12_paper_0915",)


def _source() -> str:
    return Path(dashboard.__file__).read_text(encoding="utf-8", errors="strict")


def _javascript_set(source: str, name: str) -> set[str]:
    match = re.search(
        rf"const\s+{re.escape(name)}\s*=\s*new Set\(\[(.*?)\]\);",
        source,
        re.DOTALL,
    )
    assert match is not None, f"JavaScript set not found: {name}"
    return set(re.findall(r'"([^"]+)"', match.group(1)))


def test_parent_and_profile_views_share_one_operational_contract() -> None:
    assert dashboard.FNO_MULTI_PAPER_CARD_IDS == ALL_IDS
    assert dashboard.FNO_MULTI_PAPER_CARD_PROFILES == {
        PARENT_ID: "",
        "fno_v10_paper": "v10",
        "fno_v11_paper": "v11",
        "fno_v12_paper": "v12",
    }
    assert {dashboard.LOG_FILES[card_id] for card_id in ALL_IDS} == {
        "fno_v10_v11_v12_paper.log"
    }
    assert {dashboard.CARD_TASK_NAMES[card_id] for card_id in ALL_IDS} == {TASK}
    assert {
        dashboard._runtime_status_path_for_card(card_id) for card_id in ALL_IDS
    } == {dashboard.FNO_MULTI_PAPER_STATUS_PATH}
    assert {
        dashboard._runtime_heartbeat_path_for_card(card_id) for card_id in ALL_IDS
    } == {dashboard.FNO_MULTI_PAPER_HEARTBEAT_PATH}

    # Only the parent is startable/restartable. Profile cards are report views.
    assert dashboard.RESTARTABLE_CARDS[PARENT_ID] == (
        "run_fno_v10_v11_v12_paper_session.bat"
    )
    assert all(card_id not in dashboard.RESTARTABLE_CARDS for card_id in PROFILE_IDS)


def test_each_view_resolves_its_own_report_with_one_shared_log_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    latest = tmp_path / "latest"
    latest.mkdir()
    monkeypatch.setattr(dashboard, "FNO_OI_LATEST_DIR", latest)

    expected_reports = {
        PARENT_ID: "latest_fno_v10_v11_v12_paper.md",
        "fno_v10_paper": "latest_fno_v10_paper.md",
        "fno_v11_paper": "latest_fno_v11_paper.md",
        "fno_v12_paper": "latest_fno_v12_paper.md",
    }
    for card_id, report_name in expected_reports.items():
        assert dashboard.FNO_OI_CARD_REPORTS[card_id] == report_name
        report_path = latest / report_name
        report_path.write_text("# report\n\n| status |\n|---|\n| READY |\n", encoding="utf-8")
        resolved, display = dashboard.resolve_log_target(card_id)
        assert resolved == report_path
        assert display == str(Path("fno_oi") / "latest" / report_name)


def test_shared_json_status_is_projected_without_hiding_profile_failures(
    tmp_path: Path,
    monkeypatch,
) -> None:
    status_path = tmp_path / "status.json"
    heartbeat_path = tmp_path / "heartbeat.json"
    status_path.write_text(
        json.dumps(
            {
                "session_id": PARENT_ID,
                "status": "RUNNING",
                "session_date": "2026-08-31",
                "last_update_ist": "2026-08-31T10:00:00+05:30",
                "message": "shared engine healthy",
                "phase": "CHRONOLOGICAL_PAPER_REDUCER",
                "preferred_app_count": 8,
                "healthy_app_count": 8,
                "healthy_apps": "app1,app2,app3,app4,app5,app6,app7,app8",
                "unhealthy_apps": "",
                "app_pool_state": "HEALTHY",
                "last_app_retry_count": 0,
                "last_app_failure_count": 0,
                "profiles": {
                    "v10": {"profile_id": "V10_MAX050_GAP2", "status": "COMPLETE", "fill_count": 4},
                    "v11": {"profile_id": "V11_STAGE10", "status": "DEGRADED", "fill_count": 3},
                    "v12": {"profile_id": "V12_SELECTED", "status": "BLOCKED", "fill_count": 0},
                },
            }
        ),
        encoding="utf-8",
    )
    heartbeat_path.write_text(
        json.dumps(
            {
                "session_id": PARENT_ID,
                "status": "RUNNING",
                "session_date": "2026-08-31",
                "heartbeat_ist": "2026-08-31T10:00:01+05:30",
                "pid": 1234,
                "message": "minute loop alive",
                "phase": "UNION_COMPLETED_ONE_MINUTE_FETCH",
                "preferred_app_count": 8,
                "healthy_app_count": 7,
                "healthy_apps": "app1,app2,app3,app4,app5,app6,app7",
                "unhealthy_apps": "app8",
                "app_pool_state": "DEGRADED_HEALTHY",
                "last_app_event_minute": "2026-08-31T09:59:00+05:30",
                "last_app_usage": "app1:2/2/err0; app2:1/2/err1",
                "last_app_retry_count": 1,
                "last_app_failure_count": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_STATUS_PATH", status_path)
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_HEARTBEAT_PATH", heartbeat_path)

    parent = dashboard._load_fno_multi_paper_runtime_status(PARENT_ID)
    v10 = dashboard._load_fno_multi_paper_runtime_status("fno_v10_paper")
    v11 = dashboard._load_fno_multi_paper_runtime_status("fno_v11_paper")
    v12 = dashboard._load_fno_multi_paper_runtime_status("fno_v12_paper")

    assert parent["view_scope"] == "SESSION"
    assert parent["status"] == "RUNNING"
    assert parent["heartbeat_state"] == "RUNNING"
    assert parent["ts"] == "2026-08-31T10:00:00+05:30"
    assert parent["phase"] == "UNION_COMPLETED_ONE_MINUTE_FETCH"
    assert parent["derived_status"] == "minute loop alive"
    assert parent["healthy_app_count"] == "7"
    assert parent["unhealthy_apps"] == "app8"
    assert parent["app_pool_state"] == "DEGRADED_HEALTHY"
    assert parent["last_app_retry_count"] == "1"
    assert parent["last_app_failure_count"] == "1"
    assert v10["view_scope"] == "PROFILE"
    assert v10["session_status"] == "RUNNING"
    assert v10["status"] == "SUCCESS"
    assert v10["fill_count"] == "4"
    assert v10["healthy_app_count"] == "7"
    assert v11["status"] == "PARTIAL"
    # A healthy shared heartbeat must not turn a blocked profile green.
    assert v12["status"] == "BLOCKED"


def test_fno_page_has_nested_v10_v11_v12_views_but_one_timeline_session() -> None:
    source = _source()
    fno_group = source[
        source.index('key: "fno"') : source.index('key: "forensic-positional"')
    ]
    timeline = source[
        source.index("const SESSION_TIMELINE") : source.index("const API_TOKEN")
    ]
    restartable = _javascript_set(source, "RESTARTABLE_CARDS")
    markdown_cards = _javascript_set(source, "MD_REPORT_CARDS")

    assert 'title: "V10"' in fno_group
    assert 'title: "V11"' in fno_group
    assert 'title: "V12"' in fno_group
    assert "5m selection | 1m entry | LONG | SHORT | result | logs" in fno_group
    assert all(card_id in fno_group for card_id in ALL_IDS)
    assert all(card_id in markdown_cards for card_id in ALL_IDS)
    assert PARENT_ID in restartable
    assert all(card_id not in restartable for card_id in PROFILE_IDS)
    assert timeline.count(PARENT_ID) == 1
    assert all(card_id not in timeline for card_id in PROFILE_IDS)
    assert "isReadOnlyProfileView(item)" in source
    assert "const FNO_MULTI_PAPER_CARDS" in source
    assert "runtime.healthy_app_count" in source
    assert "runtime.preferred_app_count" in source
    assert "runtime.app_pool_state" in source
    assert "runtime.last_app_retry_count" in source
    assert "runtime.last_app_failure_count" in source
    assert "retry/error:" in source
