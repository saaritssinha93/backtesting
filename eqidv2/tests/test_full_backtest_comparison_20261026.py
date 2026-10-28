"""Accounting and chronology checks for the five-strategy comparison package."""
import json

import pandas as pd
import pytest

from research import full_backtest_comparison_20261026 as comparison


DAY = "2026-10-26"


def _book():
    return pd.DataFrame([
        dict(sid=1, day=DAY, setup_id="1011_LONG", tradingsymbol="ALPHA", side="LONG",
             filled="True", portfolio_executed="True", entry_ts=DAY + " 10:12:00+05:30",
             exit_ts=DAY + " 10:30:00+05:30", portfolio_gross_profit_rupees=1000.,
             portfolio_cost_rupees=250., portfolio_net_profit_rupees=750., exit_reason="TARGET"),
        dict(sid=2, day=DAY, setup_id="1021_SHORT", tradingsymbol="BETA", side="SHORT",
             filled="True", portfolio_executed="False", entry_ts=DAY + " 10:22:00+05:30",
             exit_ts=DAY + " 10:40:00+05:30", portfolio_gross_profit_rupees=0.,
             portfolio_cost_rupees=0., portfolio_net_profit_rupees=0., exit_reason="STOP"),
    ])


def test_false_csv_flags_are_not_truthy_and_rejected_candidates_are_retained():
    result = comparison.normalize_book(_book(), [DAY])
    assert result.portfolio_executed.tolist() == [True, False]
    assert result.filled.tolist() == [True, True]
    assert len(result) == 2
    values = comparison.stats(result, [DAY])
    assert values["selected_orders"] == 2
    assert values["qualified_fills"] == 2
    assert values["trades"] == values["long_trades"] == 1
    assert values["short_trades"] == 0
    assert values["net_pnl_rs"] == 750.
    assert values["cost_rs"] == 250.


@pytest.mark.parametrize("field", ["filled", "portfolio_executed"])
def test_missing_or_unknown_boolean_is_rejected(field):
    frame = _book()
    frame.loc[0, field] = "unknown"
    with pytest.raises(ValueError, match="boolean"):
        comparison.normalize_book(frame, [DAY])


def test_duplicate_sid_is_rejected():
    frame = _book()
    frame.loc[1, "sid"] = 1
    with pytest.raises(ValueError, match="Duplicate or missing order SID"):
        comparison.normalize_book(frame, [DAY])


def test_duplicate_trade_with_different_sid_is_rejected():
    frame = pd.concat([_book().iloc[:1], _book().iloc[:1].assign(sid=2)], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate selected trade identity"):
        comparison.normalize_book(frame, [DAY])


def test_executed_candidate_requires_a_fill():
    frame = _book()
    frame.loc[0, "filled"] = "False"
    with pytest.raises(ValueError, match="no permitted fill"):
        comparison.normalize_book(frame, [DAY])


def test_gross_cost_net_must_reconcile():
    frame = _book()
    frame.loc[0, "portfolio_net_profit_rupees"] += 1
    with pytest.raises(ValueError, match="Gross minus costs"):
        comparison.normalize_book(frame, [DAY])


def test_rejected_candidate_cannot_contribute_money_even_when_arithmetic_balances():
    frame = _book()
    frame.loc[1, "portfolio_gross_profit_rupees"] = 10.
    frame.loc[1, "portfolio_net_profit_rupees"] = 10.
    with pytest.raises(ValueError, match="Rejected candidate contributed"):
        comparison.normalize_book(frame, [DAY])


def test_cost_mismatch_rejected_even_when_money_balances():
    frame = _book()
    frame.loc[0, "portfolio_cost_rupees"] = 500.
    frame.loc[0, "portfolio_net_profit_rupees"] = 500.
    with pytest.raises(ValueError, match="5bps modeled cost"):
        comparison.normalize_book(frame, [DAY])


@pytest.mark.parametrize("field,value", [
    ("exit_ts", "2026-10-26 10:00:00+05:30"),
    ("exit_ts", "2026-10-27 10:30:00+05:30"),
    ("entry_ts", "2026-10-23 10:12:00+05:30"),
])
def test_reversed_or_overnight_executions_are_rejected(field, value):
    frame = _book()
    frame.loc[0, field] = value
    with pytest.raises(ValueError, match="chronology|Overnight"):
        comparison.normalize_book(frame, [DAY])


@pytest.mark.parametrize("days", [[DAY, DAY], ["2026-10-27", DAY], ["2026-10-23"]])
def test_invalid_source_calendar_is_rejected(days):
    with pytest.raises(ValueError, match="book/session calendar"):
        comparison.normalize_book(_book(), days)


def test_different_strategy_calendars_fail_before_output_is_created(tmp_path, monkeypatch):
    days = pd.bdate_range(end=DAY, periods=58).strftime("%Y-%m-%d").tolist()
    monkeypatch.setattr(comparison, "RESULT_ROOT", tmp_path)
    def source(name):
        return {"days": days if name != "G8" else days[1:]}
    monkeypatch.setattr(comparison, "load_reference", source)
    output = tmp_path / "comparison"
    with pytest.raises(ValueError, match="exactly the same 58-session calendar"):
        comparison.run(output)
    assert not output.exists()


def test_source_manifest_rejects_changed_member(tmp_path):
    member = tmp_path / "book.csv"
    member.write_text("original", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"files": {member.name: comparison.sha(member)}}), encoding="utf-8")
    pinned = comparison.sha(manifest)
    member.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="Source member changed"):
        comparison.verify_members(tmp_path, pinned)


def test_source_manifest_rejects_path_escape(tmp_path):
    package = tmp_path / "package"
    package.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("data", encoding="utf-8")
    (package / "manifest.json").write_text(json.dumps({"files": {"../outside.csv": comparison.sha(outside)}}), encoding="utf-8")
    with pytest.raises(ValueError, match="Source member changed"):
        comparison.verify_members(package)


def _profile(name):
    config = dict(variant_id=comparison.VERSIONS[name], scope="BACKTEST_ONLY", execution_authority=False,
                  active_setups=[{"setup_id": f"S{i}"} for i in range(51)],
                  capital_per_entry_rs=100000., portfolio_capital_rs=1000000., leverage=5.,
                  model_cost_bps_round_trip=5.)
    summary = dict(variant_id=comparison.VERSIONS[name], scope="BACKTEST_ONLY", execution_authority=False,
                   selected_policy=comparison.SELECTED_POLICIES[name])
    return config, summary


def test_g7_v3_cannot_be_reported_as_g7_v2():
    config, summary = _profile("G7V2")
    config["variant_id"] = "V13-V10-G-7-G6-REPLICA-V3"
    summary["variant_id"] = config["variant_id"]
    with pytest.raises(ValueError, match="Wrong exact strategy version"):
        comparison.verify_profile("G7V2", config, summary)


def test_g8_shock_experiment_cannot_replace_adopted_baseline():
    config, summary = _profile("G8")
    summary["selected_policy"] = "shock_exit"
    with pytest.raises(ValueError, match="Wrong selected policy"):
        comparison.verify_profile("G8", config, summary)


def test_valid_g7_v2_profile_and_consistent_capital_are_required():
    config, summary = _profile("G7V2")
    comparison.verify_profile("G7V2", config, summary)
    config["leverage"] = 10.
    with pytest.raises(ValueError, match="Incomparable capital/cost"):
        comparison.verify_profile("G7V2", config, summary)
