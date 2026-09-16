from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path

import log_dashboard_server as dashboard


DAY = "2026-09-01"
PAPER_IDS = (
    "fno_v13_v10_g_scanner_5min", "fno_v13_v10_g_equity_1min_feed", "fno_v13_v10_g_confirmation_1min",
    "fno_v13_v10_g_live_long", "fno_v13_v10_g_live_short", "fno_v13_v10_g_trade_logger", "fno_v13_v10_g_net_result",
)
LIVE_IDS = (
    "live_signals_csv_fno_id_v13_v10_g_short", "live_signals_csv_fno_id_v13_v10_g_long",
    "live_kite_trades_csv_fno_id_v13_v10_g", "kite_trade_fno_id_v13_v10_g",
)


def test_visible_g_labels_use_only_canonical_g_card_and_task_connections() -> None:
    source = Path(dashboard.__file__).read_text(encoding="utf-8")
    titles_match = re.search(r"const LOG_TITLES = (\{.*?\});", source, re.DOTALL)
    assert titles_match
    browser_titles = json.loads(titles_match.group(1))
    ids = [key for _, group in dashboard.FNO_EQ_ID_MONITOR_GROUPS for key in group]
    for card_id in (*PAPER_IDS, *LIVE_IDS):
        assert ids.count(card_id) == 1
        assert "V13-V10-G" in browser_titles[card_id]
        assert browser_titles[card_id] == dashboard.FNO_EQ_ID_MONITOR_SESSION_LABELS[card_id]
        assert card_id in dashboard.CARD_TASK_NAMES
        assert dashboard._runtime_status_path_for_card(card_id) is not None
        assert dashboard._runtime_heartbeat_path_for_card(card_id) is not None
    for card_id in PAPER_IDS:
        assert dashboard.RESTARTABLE_CARDS[card_id] == f"run_{card_id}.bat"
        assert dashboard.FNO_OI_CARD_REPORTS[card_id].startswith("latest_fno_v13_v10_g_")
    # The four real-order views still share one executor and acquire no new start control.
    assert len({dashboard.CARD_TASK_NAMES[card_id] for card_id in LIVE_IDS}) == 1
    assert not set(LIVE_IDS) & set(dashboard.RESTARTABLE_CARDS)
    assert dashboard.FNO_V6_LIVE_KITE_ROOT.parts[-2:] == ("v13_v10_g_live", "live_kite")


def test_canonical_g_tasks_all_start_at_0915_without_legacy_operational_aliases() -> None:
    all_ids = (*PAPER_IDS, *LIVE_IDS)
    assert tuple(dashboard.FNO_V13_V10_G_CARD_IDS) == all_ids
    tasks = {task for card_id in all_ids for task in dashboard.CARD_TASK_NAMES[card_id]}
    assert len(tasks) == 8
    assert all(task.startswith("\\EQIDV2_fno_v13_v10_g_") and task.endswith("_0915") for task in tasks)
    for mapping in (dashboard.LOG_FILES, dashboard.FNO_OI_CARD_REPORTS, dashboard.CARD_TASK_NAMES, dashboard.RESTARTABLE_CARDS):
        assert not any(key.startswith("fno_v6_") or "fno_id_v6" in key for key in mapping)
    for card_id in PAPER_IDS:
        assert dashboard.LOG_FILES[card_id] == f"{card_id}.log"
        assert dashboard._runtime_status_path_for_card(card_id).name == f"{card_id}.status"
        assert dashboard._runtime_heartbeat_path_for_card(card_id).name == f"{card_id}.heartbeat"
    assert dashboard.LOG_FILES[LIVE_IDS[-1]] == "fno_v13_v10_g_live_kite_qty1.log"


def test_launchers_select_the_same_g_profile_and_preserve_paper_live_separation() -> None:
    bat_dir = Path(dashboard.__file__).parent / "bat"
    for card_id in PAPER_IDS:
        text = (bat_dir / dashboard.RESTARTABLE_CARDS[card_id]).read_text(encoding="utf-8")
        assert 'set "FNO_V6_STRATEGY_PROFILE=V13_V10_G"' in text
        assert "title FnO V13-V10-G" in text
        assert "FNO_V6_EXECUTION_MODE=LIVE" not in text
        if card_id != "fno_v13_v10_g_equity_1min_feed":
            assert 'set "FNO_V6_EXECUTION_MODE=PAPER"' in text
        else:
            assert "--generation v6" in text
    broker = (bat_dir / "run_fno_v13_v10_g_live_kite_qty1.bat").read_text(encoding="utf-8")
    assert 'set "FNO_V6_STRATEGY_PROFILE=V13_V10_G"' in broker
    assert 'set "FNO_V6_EXECUTION_MODE=LIVE"' in broker
    assert 'set "SESSION_ID=fno_v13_v10_g_live_kite_qty1"' in broker
    assert "I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS" in broker
    assert r"\v13_v10_g_live\live_kite\open_positions_{date}.json" in broker
    assert '-OpenPositionsStateFilePattern "%OPEN_POSITIONS_PATTERN%"' in broker


def test_dashboard_identity_slots_and_reports_match_the_pinned_g_adapter() -> None:
    import fno_v13_v10_g_live_config as config

    assert dashboard.FNO_V13_V10_G_STRATEGY_VERSION == config.STRATEGY_VERSION
    assert dashboard.FNO_V6_LIVE_KITE_ROOT.parent.name == config.LIVE_ROOT_NAME
    assert set(dashboard.FNO_V13_V10_G_SIGNAL_SLOTS) == {setup.signal_end for setup in config.ACTIVE_SETUPS}
    for setup in config.ACTIVE_SETUPS:
        assert dashboard.FNO_EQ_ID_V6_CONFIRMATION_BY_SIGNAL[setup.signal_end] == setup.confirmation_end
    assert all(dashboard.FNO_OI_CARD_REPORTS[key].startswith(f"latest_{config.REPORT_PREFIX}_") for key in PAPER_IDS)


def test_old_successful_runtime_cannot_be_presented_as_current_g() -> None:
    old = {"status": "RUNNING", "strategy_version": "V6_BEST_NET", "processed_slots": 5}
    result = dashboard._apply_fno_v13_v10_g_identity(PAPER_IDS[0], old)
    assert result["status"] == "PARTIAL"
    assert result["strategy_identity_state"] == "MISMATCH"
    assert "V6_BEST_NET" in dashboard._fno_eq_id_monitor_activity(PAPER_IDS[0], result, "")
    assert old["status"] == "RUNNING"
    assert dashboard._apply_fno_v13_v10_g_identity("fno_oi_universe", old) == old


def test_missing_g_identity_waits_and_real_failure_is_not_concealed() -> None:
    absent = dashboard._apply_fno_v13_v10_g_identity(PAPER_IDS[0], {"status": "SUCCESS"})
    assert absent["status"] == "WAITING"
    assert absent["strategy_identity_state"] == "AWAITING_G_RUNTIME"
    failed = dashboard._apply_fno_v13_v10_g_identity(PAPER_IDS[0], {"status": "FAILED"})
    assert failed["status"] == "FAILED"
    current = dashboard._apply_fno_v13_v10_g_identity(
        LIVE_IDS[-1], {"status": "RUNNING", "strategy_version": dashboard.FNO_V13_V10_G_STRATEGY_VERSION, "armed": False},
    )
    assert current["status"] == "RUNNING"
    assert current["strategy_identity_state"] == "MATCH"
    assert current["armed"] is False


def test_late_g_slots_do_not_expand_the_independent_shared_strategy() -> None:
    assert dashboard.FNO_V13_V10_G_SIGNAL_SLOTS == (
        "09:25", "09:30", "09:35", "09:40", "09:45", "09:50", "09:55", "10:00", "11:20",
    )
    assert dashboard.FNO_EQ_ID_STRATEGY_SIGNAL_SLOTS == (
        "09:25", "09:30", "09:35", "09:40", "09:45",
    )
    minutes = dashboard._fno_v13_v10_g_timeline_minutes(DAY, [], [])
    assert "11:21" in minutes and "11:32" in minutes
    assert "10:45" not in minutes
    assert len(minutes) == 71


def test_fresh_disarmed_readiness_preserves_previous_failure_as_history() -> None:
    day = dashboard.dt.datetime.now(dashboard.IST).date().isoformat()
    status = dict(status="FAILED", reason="Earlier supervisor failed", strategy_version=dashboard.FNO_V13_V10_G_STRATEGY_VERSION,
                  readiness_only="true", workers_started="false", execution_enabled="false", armed="false",
                  order_states="0", execution_state="READY_DISARMED", session_date=day,
                  updated_at_ist=f"{day}T12:00:00+05:30", ts=f"{day}T09:00:00+05:30")
    result = dashboard._apply_fno_v13_v10_g_identity(LIVE_IDS[-1], status)
    assert result["status"] == "READY_DISARMED"
    assert result["previous_status"] == "FAILED"
    assert result["previous_reason"] == "Earlier supervisor failed"
    assert "no LIVE workers" in result["derived_status"]
    for change in ({"order_states": "1"}, {"workers_started": "true"},
                   {"updated_at_ist": f"{day}T08:00:00+05:30"}, {"armed": "true"}):
        assert dashboard._apply_fno_v13_v10_g_identity(LIVE_IDS[-1], {**status, **change})["status"] == "FAILED"


def test_sparse_monitor_includes_late_exits_without_cross_day_or_after_cutoff_events() -> None:
    minutes = dashboard._fno_v13_v10_g_timeline_minutes(
        DAY,
        [{"entry_at_ist": f"{DAY}T11:24:00+05:30", "exit_at_ist": f"{DAY}T15:15:00+05:30"}],
        [
            {"entry_time": f"{DAY}T11:24:08+05:30", "exit_time": f"{DAY}T13:44:12+05:30"},
            {"exit_time": "2026-08-31T12:10:00+05:30"},
            {"exit_time": f"{DAY}T15:40:00+05:30"},
        ],
    )
    assert {"11:24", "13:44", "15:15"} <= set(minutes)
    assert "12:10" not in minutes and "15:40" not in minutes
    assert len(minutes) == 73


def test_late_g_scanner_and_confirmation_resolve_only_from_isolated_root(tmp_path, monkeypatch) -> None:
    root = tmp_path / "fno_oi"
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", root)
    monkeypatch.setattr(dashboard, "SLOT_READY_5M_DIR", tmp_path / "cash")
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_ROOT", tmp_path / "multi")
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_STATUS_PATH", tmp_path / "multi" / "status.json")
    for stage, slot, payload in (
        ("scanner_5m", "1120", {"long_candidates": 0, "short_candidates": 0}),
        ("confirmation_1m", "1121", {
            "confirmation_end": "11:21", "scanner_complete": True, "error_count": 0,
            "candidate_count": 0, "confirmation_bars": 0, "selected_long": 0, "selected_short": 0,
        }),
    ):
        destination = root / "v13_v10_g_live" / stage / DAY / f"slot_{slot}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps({
            "session_date": DAY, "signal_end": "11:20", "state": "SUCCESS",
            "strategy_version": dashboard.FNO_V13_V10_G_STRATEGY_VERSION, **payload,
        }), encoding="utf-8")
    legacy = root / "v6_live" / "scanner_5m" / DAY / "slot_0925.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps({
        "session_date": DAY, "signal_end": "09:25", "state": "SUCCESS", "long_candidates": 99,
    }), encoding="utf-8")
    timeline = dashboard._build_fno_eq_id_strategy_timelines(
        [], now_ist=datetime(2026, 9, 1, 11, 22, tzinfo=dashboard.IST),
    )
    five = {row["slot"]: row for row in timeline["five_minute_rows"]}
    assert "SEL L0/S0; CONF L0/S0" in five["11:20"]["v6"]
    assert five["11:20"]["shared"] == "OFF WINDOW"
    assert all(five["11:20"][key] == "OFF WINDOW" for key in ("v10", "v11", "v12"))
    assert "99" not in five["09:25"]["v6"]
    minute = next(row for row in timeline["one_minute_rows"] if row["minute"] == "11:21")
    assert "1m CONF 0" in minute["v6"]
    assert minute["shared_source"] == "OFF MONITOR WINDOW"


def test_live_csv_projection_displays_the_g_setup_and_risk_contract(tmp_path) -> None:
    path = tmp_path / "signals.csv"
    row = {
        "signal_datetime": f"{DAY}T11:21:00+05:30", "tradingsymbol": "TEST",
        "side": "SHORT", "setup_id": "1121_SHORT", "strategy_version": "V13-V10-G",
        "trigger_price": 100.0, "target_price": 98.0, "stop_price": 100.62,
        "stop_pct": 0.62, "target_pct": 2.0, "quantity": 1,
    }
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    rendered = dashboard._format_csv_projection(path, dashboard.FNO_V13_V10_G_LIVE_ENTRY_COLUMNS)
    for expected in ("1121_SHORT", "V13-V10-G", "0.62", "100.62", "TEST"):
        assert expected in rendered
