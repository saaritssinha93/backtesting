"""The approved 12:16 comparison cannot alter G4's primary strategy."""
from __future__ import annotations

import copy
import json
import sys

import pandas as pd
import pytest

from research import g4_1216_comparison as comparison


@pytest.fixture
def approved_configuration():
    # Fixed values from the concrete comparison approved by the user.
    spec = dict(signal_end="12:15", confirmation_end="12:16", side="SHORT",
        mode="FILTERED", max_entries=1, picker="max_liquidity",
        price_change_pct=.1, oi_change_pct=.0625, volume_ratio=1., body_ratio=.15,
        max_wick_ratio=.75, min_traded_value=0., stop_pct=1.2, target_pct=2.,
        source_version="V13-V10-G-4-BOUNDED-EXIT-51-SETUPS-V13")
    choice = dict(policy="oi_change_pct_x1.25__stop_100",
        entry=dict(name="oi_change_pct_x1.25", setup_id="1216_SHORT",
            active=copy.deepcopy(spec), core=copy.deepcopy(spec),
            confirmation_volume=0., extension_max=None, complexity=1,
            changes={"oi_change_pct": {"operation": "factor", "value": 1.25}}),
        exit=dict(name="stop_100", target_pct=2., stop_pct=1.,
            tighten_after_minutes=120, tightened_stop_pct=1.,
            protect_after_close_pct=None, protected_profit_pct=0.),
        reason="TRAIN_IMPROVED_VALIDATION_NONWORSENING",
        shortlisted_entries=["baseline", "oi_change_pct_x1.25"])
    proposal = dict(scope="G4_BACKTEST_ONLY", approved=False,
        original_g4_changed=False, recommended_action="ADD_COMPARISON_ONLY",
        setup_id="1216_SHORT", target_changed=False,
        configuration_changes=[
            dict(parameter="active_and_core_minimum_oi_change_pct", before=.05, after=.0625),
            dict(parameter="initial_stop_pct", before=1.2, after=1.)],
        full_configuration=copy.deepcopy(choice), limitations=[])
    return proposal, choice


def test_validates_approved_comparison_without_mutating_inputs(approved_configuration):
    proposal, choice = approved_configuration
    before = copy.deepcopy((proposal, choice))
    comparison.validate_config(proposal, choice)
    assert (proposal, choice) == before


@pytest.mark.parametrize("field,value", [
    ("recommended_action", "PROMOTE_PARAMETERS"),
    ("original_g4_changed", True),
    ("setup_id", "1216_LONG"),
    ("target_changed", True),
])
def test_rejects_expansion_beyond_comparison_only(approved_configuration, field, value):
    proposal, choice = approved_configuration
    proposal[field] = value
    with pytest.raises(ValueError):
        comparison.validate_config(proposal, choice)


@pytest.mark.parametrize("path,value", [
    (("entry", "active", "oi_change_pct"), .075),
    (("entry", "core", "oi_change_pct"), .05),
    (("entry", "active", "max_entries"), 2),
    (("entry", "active", "volume_ratio"), 1.2),
    (("exit", "stop_pct"), .8),
    (("exit", "target_pct"), 2.25),
    (("exit", "tighten_after_minutes"), 60),
])
def test_rejects_changed_parameters_even_when_proposal_matches_choice(
        approved_configuration, path, value):
    proposal, choice = approved_configuration
    node = choice
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    proposal["full_configuration"] = copy.deepcopy(choice)
    with pytest.raises(ValueError):
        comparison.validate_config(proposal, choice)


def test_rejects_proposal_and_frozen_choice_disagreement(approved_configuration):
    proposal, choice = approved_configuration
    proposal["full_configuration"]["exit"]["stop_pct"] = .8
    with pytest.raises(ValueError):
        comparison.validate_config(proposal, choice)


def test_rejects_unpinned_study_before_loading_its_files(tmp_path, monkeypatch):
    (tmp_path / "manifest.json").write_text(json.dumps({"files": {}}), encoding="utf-8")
    monkeypatch.setattr(comparison, "STUDY", tmp_path)
    monkeypatch.setattr(comparison, "STUDY_MANIFEST_SHA256", "0" * 64)
    with pytest.raises(ValueError):
        comparison.load_evidence()


@pytest.fixture
def synthetic_evidence():
    row = dict(scope="1216_SHORT", period="all", before_trades=19, trades=18,
        before_win_rate_pct=100 * 15 / 19, win_rate_pct=100 * 15 / 18,
        before_net_pnl_rs=37083.121703620025, net_pnl_rs=40844.759073411675,
        net_delta_rs=3761.6373697916497, winner_retention_pct=100.)
    return dict(metadata=dict(study_first_date="2026-07-29", study_last_date="2026-10-16",
        study_sessions=53, source_artifact="synthetic frozen study",
        source_manifest_sha256="1" * 64, comparison_only=True,
        original_parameters_changed=False, scope="HISTORICAL_COMPARISON_ONLY"),
        comparison=pd.DataFrame([row]),
        configurations=pd.DataFrame([
            dict(parameter="active_and_core_minimum_oi_change_pct", before=.05, after=.0625),
            dict(parameter="initial_stop_pct", before=1.2, after=1.)]),
        before=pd.DataFrame([dict(sid=1, setup_id="1216_SHORT", day="2026-08-07",
            portfolio_net_profit_rupees=-1761.637369791652)]),
        after=pd.DataFrame(columns=["sid", "setup_id", "day", "portfolio_net_profit_rupees"]))


def test_writer_keeps_comparison_separate_and_does_not_mutate_inputs(tmp_path, synthetic_evidence):
    snapshots = {key: value.copy(deep=True) for key, value in synthetic_evidence.items()
                 if isinstance(value, pd.DataFrame)}
    comparison.write_evidence(tmp_path, synthetic_evidence)
    required = {
        "approved_1216_configuration.csv", "approved_1216_comparison.csv",
        "approved_1216_study_baseline.csv", "approved_1216_study_portfolio.csv",
        "approved_1216_evidence.json",
    }
    assert required <= {path.name for path in tmp_path.iterdir()}
    assert not (tmp_path / "selected_orders.csv").exists()
    assert not (tmp_path / "comparison.csv").exists()
    assert not (tmp_path / "config.json").exists()
    for key, snapshot in snapshots.items():
        pd.testing.assert_frame_equal(synthetic_evidence[key], snapshot)
    saved = pd.read_csv(tmp_path / "approved_1216_comparison.csv")
    pd.testing.assert_frame_equal(saved, synthetic_evidence["comparison"], check_dtype=False)


def test_report_explicitly_labels_frozen_historical_dates(synthetic_evidence):
    rendered = comparison.report_section(synthetic_evidence)
    assert "2026-10-16" in rendered
    assert "historical" in rendered.lower()
    assert "1216_SHORT" in rendered
    assert "approved_1216_comparison.csv" in rendered


def test_cli_comparison_dispatch_does_not_run_primary_backtest(tmp_path, monkeypatch):
    import fno_v13_v10_g_4_backtest as cli
    calls = []
    monkeypatch.setattr(cli, "BACKTEST_ROOT", tmp_path)

    def forbidden(*args, **kwargs):
        pytest.fail("Comparison-only invocation must not run or publish the primary backtest")

    def report(output):
        calls.append(output)
        return dict(scope="HISTORICAL_COMPARISON_ONLY", comparison_only=True,
            original_parameters_changed=False, study_last_date="2026-10-16")

    monkeypatch.setattr(cli, "run_complete", forbidden)
    monkeypatch.setattr(cli, "run_1216_comparison", report)
    output = tmp_path / "comparison"
    monkeypatch.setattr(sys, "argv", ["g4", "--approved-comparison", "1216_SHORT",
        "--output-dir", str(output)])
    cli.main()
    assert calls == [output]


def test_cli_does_not_relabel_frozen_study_as_newer_results(tmp_path, monkeypatch):
    import fno_v13_v10_g_4_backtest as cli
    monkeypatch.setattr(cli, "BACKTEST_ROOT", tmp_path)

    def forbidden(*args, **kwargs):
        pytest.fail("A frozen comparison must not claim a different study cutoff")

    monkeypatch.setattr(cli, "run_1216_comparison", forbidden)
    monkeypatch.setattr(sys, "argv", ["g4", "--approved-comparison", "1216_SHORT",
        "--through", "2026-10-17", "--output-dir", str(tmp_path / "comparison")])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
