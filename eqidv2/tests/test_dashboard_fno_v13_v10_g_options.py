from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest

import log_dashboard_server as dashboard
import fno_v13_v10_g_live_config as equity_config
import fno_v13_v10_g_options_paper as options_engine


ROOT = Path(__file__).resolve().parents[1]
BAT = ROOT / "bat"
OPTION_ROLES = {
    "live_long": "long-entry",
    "live_short": "short-entry",
    "trade_logger": "trade-logger",
    "net_result": "net-result",
}
OPTION_IDS = tuple(f"fno_v13_v10_g_options_{role}" for role in OPTION_ROLES)


def test_options_cards_are_separate_from_existing_g_cards_and_fully_registered() -> None:
    assert dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS == OPTION_IDS
    assert not set(OPTION_IDS) & set(dashboard.FNO_V13_V10_G_CARD_IDS)
    monitored = {
        card_id
        for _group_name, card_ids in dashboard.FNO_EQ_ID_MONITOR_GROUPS
        for card_id in card_ids
    }
    source = Path(dashboard.__file__).read_text(encoding="utf-8")
    titles_match = re.search(r"const LOG_TITLES = (\{.*?\});", source, re.DOTALL)
    assert titles_match
    browser_titles = json.loads(titles_match.group(1))
    for card_id in OPTION_IDS:
        assert card_id in monitored
        assert browser_titles[card_id] == dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS[card_id]
        assert dashboard.FNO_OI_CARD_REPORTS[card_id] == f"latest_{card_id}.md"
        assert dashboard.LOG_FILES[card_id] == f"{card_id}.log"
        assert dashboard.STATUS_FILES[card_id] == f"{card_id}.status"
        assert dashboard.HEARTBEAT_FILES[card_id] == f"{card_id}.heartbeat"
        assert dashboard.CARD_TASK_NAMES[card_id] == (f"\\EQIDV2_{card_id}_0915",)
        assert dashboard.RESTARTABLE_CARDS[card_id] == f"run_{card_id}.bat"


def test_requested_options_session_titles_and_direction_mapping_are_explicit() -> None:
    labels = dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS
    assert labels[OPTION_IDS[0]] == "Options V13-V10-G LONG ATM CE Buy Paper Entry Session"
    assert labels[OPTION_IDS[1]] == "Options V13-V10-G SHORT ATM PE Buy Paper Entry Session"
    assert labels[OPTION_IDS[2]] == "Options V13-V10-G Continuous Paper Trade Log"
    assert labels[OPTION_IDS[3]] == "Options V13-V10-G Paper Net Result"


def test_dashboard_contract_matches_options_engine_publishers() -> None:
    expected_sessions = {
        engine_role: f"fno_v13_v10_g_options_{launcher_role}"
        for launcher_role, engine_role in OPTION_ROLES.items()
    }
    assert options_engine.ROLE_SESSIONS == expected_sessions
    for engine_role, card_id in expected_sessions.items():
        assert options_engine.ROLE_REPORTS[engine_role] == dashboard.FNO_OI_CARD_REPORTS[card_id]
        assert options_engine.ROLE_TITLES[engine_role] == dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS[card_id]

    status = dashboard._apply_fno_v13_v10_g_identity(
        OPTION_IDS[0],
        {"status": "RUNNING", "strategy_version": equity_config.STRATEGY_VERSION},
    )
    assert status["status"] == "RUNNING"
    assert status["strategy_identity_state"] == "MATCH"

    entry_metrics = tuple(field for field, _label in dashboard._FNO_EQ_ID_ACTIVITY_FIELDS[OPTION_IDS[0]])
    reporting_metrics = tuple(field for field, _label in dashboard._FNO_EQ_ID_ACTIVITY_FIELDS[OPTION_IDS[2]])
    assert entry_metrics == (
        "equity_entries", "trades", "open", "closed", "skipped", "blocked",
        "unresolved", "net_pnl_rs", "free_cash_rs",
    )
    assert reporting_metrics == (
        "trades", "open", "closed", "skipped", "blocked", "unresolved",
        "net_pnl_rs", "free_cash_rs",
    )
    assert dashboard._FNO_EQ_ID_ACTIVITY_FIELDS[OPTION_IDS[1]] == dashboard._FNO_EQ_ID_ACTIVITY_FIELDS[OPTION_IDS[0]]
    assert dashboard._FNO_EQ_ID_ACTIVITY_FIELDS[OPTION_IDS[3]] == dashboard._FNO_EQ_ID_ACTIVITY_FIELDS[OPTION_IDS[2]]


@pytest.mark.parametrize("role,engine_role", OPTION_ROLES.items())
def test_options_paper_launchers_have_isolated_session_identity(role: str, engine_role: str) -> None:
    card_id = f"fno_v13_v10_g_options_{role}"
    source = (BAT / f"run_{card_id}.bat").read_text(encoding="utf-8")
    assert 'set "FNO_V6_STRATEGY_PROFILE=V13_V10_G"' in source
    assert 'set "FNO_V13_V10_G_OPTIONS_EXECUTION_MODE=PAPER"' in source
    assert f'set "SESSION_ID={card_id}"' in source
    assert f'fno_v13_v10_g_options_paper.py" --role {engine_role}' in source
    assert f"logs\\{card_id}.log" in source
    assert "place_order" not in source
    if engine_role in {"long-entry", "short-entry"}:
        assert "--max-apps 8" in source


def test_options_scheduler_defines_one_fetch_and_four_paper_tasks_without_starting_them() -> None:
    source = (BAT / "schedule_fno_v13_v10_g_options_weekday.ps1").read_text(encoding="utf-8")
    entries = re.findall(
        r"Name = '([^']+)'; Time = '([^']+)'; Runner = '([^']+)'",
        source,
    )
    expected = {
        (
            "EQIDV2_fno_options_atm_fetch_5min_0907",
            "09:07",
            "run_fno_options_atm_fetch_5min.bat",
        ),
        *{
            (
                f"EQIDV2_fno_v13_v10_g_options_{role}_0915",
                "09:15",
                f"run_fno_v13_v10_g_options_{role}.bat",
            )
            for role in OPTION_ROLES
        },
    }
    assert set(entries) == expected
    assert "Monday, Tuesday, Wednesday, Thursday, Friday" in source
    assert "MultipleInstances IgnoreNew" in source
    assert "if (-not $Apply)" in source
    assert "Start-ScheduledTask" not in source
    assert "schtasks.exe /Run" not in source
    fetch_launcher = (BAT / "run_fno_options_atm_fetch_5min.bat").read_text(encoding="utf-8")
    assert '"--max-apps","8"' in fetch_launcher


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parser")
def test_options_scheduler_parses_without_execution() -> None:
    path = BAT / "schedule_fno_v13_v10_g_options_weekday.ps1"
    escaped = str(path).replace("'", "''")
    command = (
        f"$tokens = $null; $parseErrors = $null; "
        f"[System.Management.Automation.Language.Parser]::ParseFile('{escaped}', "
        "[ref]$tokens, [ref]$parseErrors) | Out-Null; "
        "if ($parseErrors.Count) { throw ($parseErrors | Out-String) }"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr


def test_options_cards_reject_an_old_strategy_identity() -> None:
    result = dashboard._apply_fno_v13_v10_g_identity(
        OPTION_IDS[0], {"status": "RUNNING", "strategy_version": "V6_BEST_NET"}
    )
    assert result["status"] == "PARTIAL"
    assert result["strategy_identity_state"] == "MISMATCH"


def test_other_active_section_is_named_options_v13_and_formats_session_names() -> None:
    source = Path(dashboard.__file__).read_text(encoding="utf-8")
    assert 'renderSectionBanner("Options V13 Strategy"' in source
    assert 'label: "Options V13 Strategy"' in source
    assert 'displayName(id).replaceAll("_", " ").toUpperCase()' in source
    assert "Other Active / Scheduled" not in source
