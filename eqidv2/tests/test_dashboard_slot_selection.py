"""Dashboard slot policy evidence must reconcile and retain strategy identity."""
import json
from pathlib import Path

import pandas as pd
import pytest

import dashboard_flow as flow
import dashboard_slot_selection as selected


def contract(strategy):
    return {"policy": {"strategy": strategy, "threshold_pct": 70},
            "policy_fingerprint": strategy + "-frozen-policy",
            "enabled_setup_ids": ["0931_SHORT"]}


def book(net):
    return pd.DataFrame([
        dict(sid=1, day="2026-10-26", setup_id="0931_SHORT", tradingsymbol="ACCEPTED", side="SHORT",
             filled=True, portfolio_executed=True, portfolio_net_profit_rupees=net,
             portfolio_gross_profit_rupees=net + 5, portfolio_cost_rupees=5,
             entry_ts="2026-10-26T09:32:00+05:30", exit_ts="2026-10-26T10:00:00+05:30"),
        dict(sid=2, day="2026-10-26", setup_id="0931_LONG", tradingsymbol="BLOCKED", side="LONG",
             filled=True, portfolio_executed=False, portfolio_net_profit_rupees=0,
             portfolio_gross_profit_rupees=0, portfolio_cost_rupees=0,
             entry_ts="2026-10-26T09:32:00+05:30", exit_ts="2026-10-26T10:00:00+05:30"),
    ])


@pytest.fixture
def package(tmp_path, monkeypatch):
    monkeypatch.setattr(selected, "_contract", contract)
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(flow, "active_profile_name", lambda: "V13_V10_G_3")
    source = tmp_path / "source"
    source.mkdir()
    (source / "audit.json").write_text('{"passed": true}')
    output = selected.publish_replay(source, {"G3": book(100), "G7_V4_LOCK2": book(-50)},
                                    ["2026-10-26", "2026-10-27"], data_root=tmp_path)
    return Path(output["run_dir"]), Path(output["pointer"])


def repin(run, pointer):
    manifest = {"schema_version": selected.SCHEMA,
                "files": {path.relative_to(run).as_posix(): selected._sha(path)
                          for path in run.rglob("*") if path.is_file() and path.name != "manifest.json"}}
    selected._write_json(run / "manifest.json", manifest)
    saved = selected._json(pointer)
    saved["manifest_sha256"] = selected._sha(run / "manifest.json")
    selected._write_json(pointer, saved)


def test_current_g3_and_distinct_g7_v4_are_reconciled_with_zero_trade_days(package, tmp_path):
    g3 = flow.load_flow_data(strategy="V13-V10-G-3")
    g7 = flow.load_flow_data(strategy="V13-V10-G-7-V4-LOCK")
    assert g3["selected_run"]["current_slot_selection"] is True
    assert g3["summary"]["net_pnl"] == 100
    assert g7["summary"]["net_pnl"] == -50
    assert g3["summary"]["trades"] == g7["summary"]["trades"] == 1
    assert len(g3["daily"]) == 2
    assert g3["daily"][1]["trades"] == 0
    assert g3["daily"][1]["net_pnl"] == 0
    assert g7["selected_run"]["strategy"] != "V13-V10-G-7"
    assert g7["selected_run"]["execution_authority"] is False
    assert g7["selected_run"]["enabled_setup_ids"] == ["0931_SHORT"]
    assert any("not independent validation" in warning for warning in g3["warnings"])
    assert any("every represented slot" in warning and "zero executed" in warning for warning in g3["warnings"])
    assert g3["selected_run"]["selection_scope"] == "ALL_REPRESENTED_SLOTS"
    assert "BLOCKED" not in [row["symbol"] for row in g3["trades"]]
    assert str(tmp_path) not in json.dumps(g3)
    assert flow.load_flow_data()["selected_run"]["strategy"] == "V13-V10-G-3"


def test_changed_ledger_is_rejected_even_when_pointer_is_unchanged(package, tmp_path):
    run, _ = package
    (run / "G3/portfolio_trades.csv").write_text("changed")
    assert selected.discover(tmp_path) == []


def test_rehashed_but_inconsistent_daily_result_is_rejected(package, tmp_path):
    run, pointer = package
    path = run / "G3/daily_results.csv"
    rows = selected._rows(path)
    rows[0]["net_pnl"] = 999999
    selected._write_csv(path, rows)
    repin(run, pointer)
    assert selected.discover(tmp_path) == []


def test_rehashed_disabled_setup_execution_is_rejected(package, tmp_path):
    run, pointer = package
    path = run / "G3/portfolio_trades.csv"
    rows = selected._rows(path)
    rows[1]["portfolio_executed"] = "True"
    selected._write_csv(path, rows)
    repin(run, pointer)
    assert selected.discover(tmp_path) == []


def test_stale_policy_cannot_be_presented_as_current(package, tmp_path, monkeypatch):
    monkeypatch.setattr(selected, "_contract", lambda strategy: {**contract(strategy), "policy_fingerprint": "new"})
    assert selected.discover(tmp_path) == []


@pytest.mark.parametrize("field,value", [("run_dir", "../outside"), ("execution_authority", True)])
def test_invalid_publication_pointer_is_rejected(package, tmp_path, field, value):
    _, pointer = package
    saved = selected._json(pointer)
    saved[field] = value
    selected._write_json(pointer, saved)
    assert selected.discover(tmp_path) == []


def test_bad_new_publication_preserves_previous_current_pointer(package, tmp_path):
    _, pointer = package
    before = pointer.read_bytes()
    invalid = book(100)
    invalid.loc[0, "setup_id"] = "0931_LONG"
    with pytest.raises(ValueError, match="Disabled setup"):
        selected.publish_replay(tmp_path / "source", {"G3": invalid, "G7_V4_LOCK2": book(100)},
                                ["2026-10-26", "2026-10-27"], data_root=tmp_path)
    assert pointer.read_bytes() == before


def test_live_policy_is_separate_from_old_recorded_evidence(monkeypatch):
    from datetime import datetime
    import dashboard_flow_live as live
    monkeypatch.setattr(live, "configured_policy_summary", lambda: {"G3": {"enabled_setup_count": 44}})
    monkeypatch.setattr(live, "list_available_dates", lambda _: ["2026-10-27"])
    monkeypatch.setattr(live, "load_session_market", lambda *args, **kwargs: {})
    monkeypatch.setattr(live, "build_live_evidence", lambda *args, **kwargs: {
        "strategy_version": "ORIGINAL-SAVED-VERSION", "slots": {}, "warnings": [], "sources": []})
    result = live.load_live_flow(Path("unused"), "2026-10-27", now=datetime(2026, 10, 28, 12))
    assert result["strategy"]["version"] == "ORIGINAL-SAVED-VERSION"
    assert result["configured_slot_selection"]["G3"]["enabled_setup_count"] == 44
    assert result["read_only"] is True


def test_historical_g6_g7_keep_exact_frozen_g3_donor():
    from research import g6_g4_replica_backtest as g6
    from research.archived_g3_v14 import contract as frozen
    assert g6.donor is frozen
    assert g6.donor.strategy_fingerprint() == g6.DONOR_FINGERPRINT
