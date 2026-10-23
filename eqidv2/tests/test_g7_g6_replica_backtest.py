"""G7 V3 keeps V2 entries and adds the requested two-stage exit."""
from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_7_backtest as cli
from research import g6_g4_replica_backtest as g6
from research import g7_g6_replica_backtest as g7


def test_g7_v3_restores_g6_targets_and_changes_only_three_entry_thresholds():
    source = g6.configuration()
    copied = g7.configuration()
    assert copied == g7.configuration_payload()
    assert copied["replica_source_variant"] == g6.VERSION
    assert copied["execution_authority"] is False
    assert copied["scope"] == "BACKTEST_ONLY"
    assert copied["active_setup_count"] == 51
    expected_changes = {
        "1011_LONG": ("oi_change_pct", 0.125),
        "1206_LONG": ("oi_change_pct", 0.10),
        "1226_SHORT": ("min_traded_value", 10_000_000.0),
    }
    for original, replica in zip(source["active_setups"], copied["active_setups"], strict=True):
        expected = {**original, "source_version": g7.VERSION}
        changed = expected_changes.get(replica["setup_id"])
        if changed:
            expected[changed[0]] = changed[1]
        assert replica == expected
    assert copied["target_floor_pct"] == source["target_floor_pct"]
    assert copied["target_cap_pct"] == source["target_cap_pct"]
    assert copied["approved_0920_decisions"]["configuration_changed"] is False
    assert copied["exit_policy"] == g7.TWO_STAGE_EXIT_POLICY


def test_g7_v3_identity_uses_original_per_slot_targets():
    source = g6.Identity(g6.configuration())
    copied = g7.Identity(g7.configuration())
    expected_changes = {
        "1011_LONG": ("oi_change_pct", 0.125),
        "1206_LONG": ("oi_change_pct", 0.10),
        "1226_SHORT": ("min_traded_value", 10_000_000.0),
    }
    assert len(copied.ACTIVE_SETUPS) == 51
    assert len({spec.target_pct for spec in copied.ACTIVE_SETUPS}) > 1
    for original, replica in zip(source.ACTIVE_SETUPS, copied.ACTIVE_SETUPS, strict=True):
        expected = {**asdict(original), "source_version": g7.VERSION}
        changed = expected_changes.get(replica.setup_id)
        if changed:
            expected[changed[0]] = changed[1]
        assert asdict(replica) == expected
        assert replica.target_pct == original.target_pct


def test_g7_v1_config_remains_available_as_rollback_point():
    old = g7.configuration(g7.V1_CONFIG)
    assert old["variant_id"] == g7.V1_VERSION
    assert {spec["target_pct"] for spec in old["active_setups"]} == {0.75}


def test_g7_v2_config_remains_available_as_single_exit_rollback_point():
    old = g7.configuration(g7.V2_CONFIG)
    assert old["variant_id"] == g7.V2_VERSION
    assert "exit_policy" not in old
    assert len(old["slot_entry_filters"]) == 3


def staged_market(side="LONG"):
    stamps = pd.date_range("2026-10-21 10:00", "2026-10-21 15:15",
                           freq="min", tz="Asia/Kolkata")
    path = dict(timestamp_ns=stamps.tz_convert("UTC").asi8,
        open=np.full(len(stamps), 100.0), high=np.full(len(stamps), 100.1),
        low=np.full(len(stamps), 99.9), close=np.full(len(stamps), 100.0))
    if side == "SHORT":
        old = {key:value.copy() for key,value in path.items() if key != "timestamp_ns"}
        path["open"],path["close"] = 200-old["open"],200-old["close"]
        path["high"],path["low"] = 200-old["low"],200-old["high"]
    return path


def set_bar(path, index, opening, high, low, close):
    for field,value in zip(("open","high","low","close"),(opening,high,low,close),strict=True):
        path[field][index] = value


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_two_stage_exit_books_half_at_half_target_and_half_at_full_target(side):
    path = staged_market(side)
    if side == "LONG":
        set_bar(path,1,100.1,100.6,99.9,100.55)
        set_bar(path,2,100.6,101.1,100.55,101.0)
    else:
        set_bar(path,1,99.9,100.1,99.4,99.45)
        set_bar(path,2,99.4,99.45,98.9,99.0)
    row = g7._simulate_two_stage_exit(path,0,100.0,side == "LONG",1.2,1.0,
        tighten_after_minutes=120,tightened_stop_pct=1.0)
    assert row["first_target_hit"] is True
    assert row["runner_target_hit"] is True
    assert row["exit_reason"] == "T1_THEN_FULL_TARGET"
    assert row["gross_return_pct"] == pytest.approx(0.75)


def test_two_stage_exit_moves_runner_stop_to_first_target_from_next_bar():
    path = staged_market()
    set_bar(path,1,100.1,100.6,99.9,100.55)
    set_bar(path,2,100.6,100.7,100.4,100.5)
    row = g7._simulate_two_stage_exit(path,0,100.0,True,1.2,1.0,
        tighten_after_minutes=120,tightened_stop_pct=1.0)
    assert row["exit_reason"] == "T1_THEN_PROFIT_LOCK_STOP"
    assert row["first_target_exit_path_index"] == 1
    assert row["runner_exit_path_index"] == 2
    assert row["gross_return_pct"] == pytest.approx(0.5)
    assert row["stop_hit"] is True


def test_two_stage_exit_uses_full_stop_before_first_target_and_stop_first_tie():
    path = staged_market()
    set_bar(path,1,100.0,100.6,98.7,100.0)
    row = g7._simulate_two_stage_exit(path,0,100.0,True,1.2,1.0,
        tighten_after_minutes=120,tightened_stop_pct=1.0)
    assert row["exit_reason"] == "STOP"
    assert row["staged_exit_outcome"] == "FULL_STOP_BEFORE_T1"
    assert row["first_target_hit"] is False
    assert row["same_bar_ambiguous"] is True
    assert row["gross_return_pct"] == pytest.approx(-1.2)


def test_slot_filters_are_post_ranking_vetoes_without_replacement():
    selected = pd.DataFrame([
        {"sid": 1, "day": "2026-09-17", "setup_id": "1011_LONG", "tradingsymbol": "DROP1", "side": "LONG", "confirmation_ts": "2026-09-17 10:11:00+05:30", "oi_change_pct": 0.10, "traded_value": 20_000_000},
        {"sid": 2, "day": "2026-09-17", "setup_id": "1011_LONG", "tradingsymbol": "KEEP1", "side": "LONG", "confirmation_ts": "2026-09-17 10:11:00+05:30", "oi_change_pct": 0.13, "traded_value": 20_000_000},
        {"sid": 3, "day": "2026-09-17", "setup_id": "1206_LONG", "tradingsymbol": "DROP2", "side": "LONG", "confirmation_ts": "2026-09-17 12:06:00+05:30", "oi_change_pct": 0.09, "traded_value": 20_000_000},
        {"sid": 4, "day": "2026-09-17", "setup_id": "1226_SHORT", "tradingsymbol": "DROP3", "side": "SHORT", "confirmation_ts": "2026-09-17 12:26:00+05:30", "oi_change_pct": 0.20, "traded_value": 9_999_999},
        {"sid": 5, "day": "2026-09-17", "setup_id": "1401_SHORT", "tradingsymbol": "KEEP2", "side": "SHORT", "confirmation_ts": "2026-09-17 14:01:00+05:30", "oi_change_pct": 0.20, "traded_value": 1},
    ])
    kept, audit, rejected = g7.apply_slot_entry_filters(selected, g7.configuration())
    assert kept.sid.tolist() == [2, 5]
    assert rejected.sid.tolist() == [1, 3, 4]
    assert audit.set_index("sid").loc[2, "slot_entry_filter_allowed"]
    assert audit.set_index("sid").loc[5, "slot_entry_filter_reason"] == "NOT_APPLICABLE"


def test_g7_output_root_is_isolated_from_g6_and_g4():
    assert g7.G7_ROOT != g6.G6_ROOT
    assert g7.G7_ROOT != g7.bounded.G4_ROOT


def test_g7_rejects_output_outside_its_root(monkeypatch):
    monkeypatch.setattr(g7, "_approved_research_evidence", lambda: {})
    monkeypatch.setattr(g7.approved1216, "load_evidence", lambda: {})
    with pytest.raises(ValueError, match="isolated G7"):
        g7.run(g7.DEFAULT_CONFIG, g6.G6_ROOT / "runs" / "g7_must_not_write_here")


def test_g7_cli_verifies_without_running(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g7", "--verify-config"])
    monkeypatch.setattr(cli, "run", lambda *a, **k: pytest.fail("Must not run replay"))
    cli.main()
    import json
    value = json.loads(capsys.readouterr().out)
    assert value["variant_id"] == g7.VERSION
    assert value["replica_source_variant"] == g6.VERSION
    assert value["active_setups"] == 51
    assert value["target_policy"]["mode"] == "RESTORE_G6_SETUP_TARGETS"
    assert value["exit_policy"]["mode"] == g7.TWO_STAGE_EXIT_MODE
    assert len(value["slot_entry_filters"]) == 3
    assert value["selected_policy"] == g7.NIFTY_POLICY
    assert value["direction_filter_enabled"] is True
    assert value["execution_authority"] is False
