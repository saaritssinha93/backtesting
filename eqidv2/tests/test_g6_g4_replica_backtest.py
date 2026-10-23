"""G6 preserves G4 trading parameters and rejects output into G4 directories."""
from dataclasses import asdict

import pytest
import pandas as pd

import fno_v13_v10_g_6_backtest as cli
from research import g4_complete_inventory_backtest as g4
from research import g6_g4_replica_backtest as g6
from research import g6_nifty_direction as nifty_filter
from research import g6_primary_index_direction as primary_filter


def test_replica_configuration_keeps_the_same_1011_short_entry_threshold():
    original = g4.configuration()
    replica = g6.configuration()
    assert replica == g6.configuration_payload()
    for source, copied in zip(original["active_setups"], replica["active_setups"], strict=True):
        expected = {**source, "source_version": g6.VERSION}
        if source["setup_id"] == "1011_SHORT":
            assert source["price_change_pct"] == .11
        assert copied == expected
    assert replica["entry_override"]["historical_replay_all_sessions"] is True
    assert replica["execution_authority"] is False
    assert replica["scope"] == "BACKTEST_ONLY"


def test_all_51_entry_specs_equal_g4_except_identity():
    original, replica = g4.Identity(g4.configuration()), g6.Identity(g6.configuration())
    assert len(replica.ACTIVE_SETUPS) == 51
    for source, copied in zip(original.ACTIVE_SETUPS, replica.ACTIVE_SETUPS, strict=True):
        expected = {**asdict(source), "source_version": g6.VERSION}
        assert expected == asdict(copied)
    assert replica.STRATEGY_VERSION == g6.VERSION
    assert replica.payload["strategy_profile"] == "V13_V10_G_6"


def test_runner_cannot_write_into_g4_root(monkeypatch):
    monkeypatch.setattr(g6, "_approved_research_evidence", lambda: {})
    monkeypatch.setattr(g6.approved1216, "load_evidence", lambda: {})
    with pytest.raises(ValueError, match="isolated G6"):
        g6.run(g6.DEFAULT_CONFIG, g6.bounded.G4_ROOT / "runs" / "g6_must_not_write_here")


def test_cli_rejects_g4_output_before_running(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g6", "--output-dir", str(g6.bounded.G4_ROOT / "runs")])
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("Must reject before replay"))
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "isolated G6" in capsys.readouterr().err


def test_config_verification_reports_default_filter_without_execution(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g6", "--verify-config"])
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("Verification must not run replay"))
    cli.main()
    import json
    value = json.loads(capsys.readouterr().out)
    assert value["direction_filter_enabled"] is True
    assert value["direction_filter_mode"] == "nifty_both_counter_skip"
    assert value["selected_policy"] == g6.NIFTY_POLICY
    assert value["nifty_direction_filter_enabled"] is True
    assert value["nifty_direction_filter_mode"] == "both_counter_skip"
    assert value["execution_authority"] is False
    assert value["active_setups"] == 51
    assert value["entry_override"]["price_change_pct"] == .11
    assert g6.G6_ROOT != g6.bounded.G4_ROOT


def test_explicit_off_is_available_as_rollback_without_running(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g6", "--verify-config", "--nifty-filter-mode", "off"])
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("Verification must not run replay"))
    cli.main()
    import json
    value = json.loads(capsys.readouterr().out)
    assert value["direction_filter_mode"] == "off"
    assert value["direction_filter_enabled"] is False


def test_legacy_both_counter_mode_selects_nifty_reference(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g6", "--verify-config", "--nifty-filter-mode", "both_counter_skip"])
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("Verification must not run replay"))
    cli.main()
    import json
    value = json.loads(capsys.readouterr().out)
    assert value["direction_filter_mode"] == "nifty_both_counter_skip"
    assert value["selected_policy"] == g6.NIFTY_POLICY
    assert value["nifty_direction_filter_enabled"] is True


def test_primary_index_remains_available_as_explicit_rollback(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["g6", "--verify-config", "--direction-filter-mode",
                                    "primary_index_both_counter_skip"])
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("Must not run replay"))
    cli.main()
    import json
    value = json.loads(capsys.readouterr().out)
    assert value["direction_filter_mode"] == "primary_index_both_counter_skip"
    assert value["selected_policy"] == g6.PRIMARY_POLICY
    assert value["nifty_direction_filter_enabled"] is False
    assert value["execution_authority"] is False


@pytest.mark.parametrize("mode,suffix", [
    (None, "g6_nifty_futures"),
    ("primary_index_both_counter_skip", "g6_primary_index"),
    ("off", "g6_unfiltered"),
])
def test_cli_output_name_matches_selected_filter_without_replaying(monkeypatch, capsys, mode, suffix):
    args = ["g6"] + (["--direction-filter-mode", mode] if mode else [])
    monkeypatch.setattr("sys.argv", args)
    seen = {}

    def fake_run(config_path, output, **kwargs):
        seen.update(output=output, options=kwargs, config=g6.configuration(config_path))
        return dict(variant_id=g6.VERSION, sessions=0, first_date=None, last_date=None,
                    active_setups=51, comparison=[])

    monkeypatch.setattr(cli, "run", fake_run)
    cli.main()
    assert seen["output"].name.endswith("_" + suffix)
    assert seen["options"]["direction_filter_mode"] == mode
    assert seen["config"]["direction_filter"]["selected_policy"] == g6.NIFTY_POLICY
    assert seen["config"]["execution_authority"] is False


@pytest.mark.parametrize("side,slot,session,expected", [
    ("LONG", -.1, -.2, False), ("SHORT", .1, .2, False),
    ("LONG", -.1, .2, True), ("SHORT", .1, -.2, True),
    ("LONG", 0., -.2, True), ("SHORT", 0., .2, True),
])
def test_both_direction_gate_is_symmetric_and_keeps_mixed_or_flat(side, slot, session, expected):
    assert nifty_filter.allowed(side, slot, session) is expected


def test_g6_filter_reallocates_and_preserves_native_exit_evidence():
    day = "2026-10-15"
    rows = []
    for sid, side, clock, gross in [(1, "LONG", "09:30", 2.), (2, "SHORT", "10:00", -1.2)]:
        rows.append(dict(sid=sid, day=day, setup_id=clock.replace(":", "") + "_" + side,
            tradingsymbol=str(sid), side=side, filled=True,
            confirmation_ts=pd.Timestamp(day+" "+clock, tz="Asia/Kolkata"),
            entry_ts=pd.Timestamp(day+" "+clock, tz="Asia/Kolkata")+pd.Timedelta(minutes=1),
            exit_ts=pd.Timestamp(day+" "+clock, tz="Asia/Kolkata")+pd.Timedelta(minutes=3),
            gross_return_pct=gross, net_return_pct=gross-.05, cost_pct=.05,
            initial_stop_pct=1.2, picker="max_move", price_change_pct=1.,
            exit_reason="STOP" if gross < 0 else "TARGET", entry_price=100., exit_price=100.+gross))
    source = pd.DataFrame(rows)
    audit = pd.DataFrame(dict(sid=[1, 2], both_counter_allowed=[True, False]))
    book, _ = nifty_filter.apply_filter(source, audit, g6.configuration())
    assert book.portfolio_executed.tolist() == [True, False]
    assert book.exit_reason.tolist() == source.exit_reason.tolist()
    assert book.portfolio_net_profit_rupees.iloc[1] == 0.
    assert book.portfolio_status.iloc[1] == "G6_NIFTY_BOTH_COUNTER_BLOCKED"
    assert not book.execution_authority.any()


def test_effective_primary_membership_changes_at_official_boundary():
    mapping, _, _ = primary_filter.effective_mapping()
    rows = pd.DataFrame([
        dict(sid=1,day="2026-09-29",tradingsymbol="BSE"),
        dict(sid=2,day="2026-09-30",tradingsymbol="BSE"),
        dict(sid=3,day="2026-09-29",tradingsymbol="WIPRO"),
        dict(sid=4,day="2026-09-30",tradingsymbol="WIPRO"),
        dict(sid=5,day="2026-09-29",tradingsymbol="KPITTECH"),
        dict(sid=6,day="2026-09-30",tradingsymbol="KPITTECH"),
    ])
    resolved = primary_filter.mapping_for_orders(rows,mapping).set_index("sid")
    assert resolved.loc[1,"primary_index"] == "Nifty Midcap 150"
    assert resolved.loc[2,"primary_index"] == "Nifty 50"
    assert resolved.loc[3,"primary_index"] == "Nifty 50"
    assert resolved.loc[4,"primary_index"] == "Nifty Next 50"
    assert resolved.loc[5,"primary_index"] == "Nifty Midcap 150"
    assert resolved.loc[6,"primary_index"] == "Nifty Smallcap 250"


def test_primary_index_input_excludes_opening_snapshot_and_has_exact_sessions():
    proofs, inputs = primary_filter.load_index_inputs(["2026-10-19"])
    assert set(proofs.primary_index) == set(primary_filter.INDEX_SPECS)
    for days in inputs.values():
        frame = days["2026-10-19"]
        assert len(frame) == 75
        assert frame.ts.dt.strftime("%H:%M").iloc[0] == "09:20"
        assert frame.ts.dt.strftime("%H:%M").iloc[-1] == "15:30"
