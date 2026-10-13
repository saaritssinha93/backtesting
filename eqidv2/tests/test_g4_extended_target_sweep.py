"""Target optimization constraints and independence from later outcomes."""
import copy
import json

import pandas as pd
import pytest

from research import g4_extended_target_sweep as sweep


def trial(target, pnl, wins=75., fills=8, *, setup="1036_SHORT", later_wins=75., later_fills=4):
    row = dict(setup="10:35_SHORT", setup_id=setup, target_pct=target)
    for period in ("all", "train", "holdout"):
        row.update({f"{period}_fills": later_fills if period == "holdout" else fills,
            f"{period}_wins": 6, f"{period}_win_rate_pct": later_wins if period == "holdout" else wins,
            f"{period}_net_pnl_rs": pnl, f"{period}_max_drawdown_rs": 100.})
    return row


def test_choose_maximizes_net_pnl_subject_to_70_and_positive_profit_and_minimum_fills():
    rows = [trial(.1, -10., 100.), trial(.2, 500., 69.99), trial(.3, 1000., 100., fills=4),
            trial(.4, 200., 90.), trial(.5, 300., 70.)]
    assert sweep.choose(rows, "all", 5)["target_pct"] == .5
    assert sweep.choose(rows[:3], "all", 5) is None


def test_training_choice_does_not_consult_later_or_full_period_results():
    rows = [trial(1., 100.), trial(1.5, 200.)]
    before = sweep.choose(rows, "train", 5)
    changed = copy.deepcopy(rows)
    changed[0].update(all_net_pnl_rs=1e9, holdout_win_rate_pct=100.)
    changed[1].update(all_net_pnl_rs=-1e9, holdout_win_rate_pct=0.)
    assert sweep.choose(changed, "train", 5)["target_pct"] == before["target_pct"] == 1.5


def test_chronological_screen_needs_later_minimum_even_at_100_percent():
    config = sweep.configuration()
    rows = [trial(1., 100.), trial(1.5, 200., later_wins=100., later_fills=2)]
    decisions, historical, training, chronological = sweep.decisions_for(rows, ["1036_SHORT"], config)
    assert historical == training == {"1036_SHORT": 1.5}
    assert not decisions.chronological_check_pass.any()
    assert chronological == {}


def test_nonqualifying_setup_is_not_silently_included():
    config = sweep.configuration()
    rows = [trial(1., 100., wins=60.), trial(.5, 200., wins=65.)]
    decisions, historical, training, chronological = sweep.decisions_for(rows, ["1036_SHORT"], config)
    assert not historical and not training and not chronological
    assert decisions.historical_target_pct.isna().all()
    assert not decisions.historical_pass.any()
    assert decisions.best_observed_win_rate_pct.iloc[0] == 65.


def test_mix_uses_setup_specific_targets_without_changing_membership():
    reference = pd.DataFrame([dict(sid=1, setup_id="A"), dict(sid=2, setup_id="B")])
    low = reference.assign(native_target_pct=.5)
    high = reference.assign(native_target_pct=2.)
    actual = sweep.mix_cached({.5: low, 2.: high}, {"A": .5, "B": 2.}, reference)
    assert actual.sid.tolist() == [1, 2]
    assert actual.native_target_pct.tolist() == [.5, 2.]
    assert sweep.mix_cached({.5: low}, {}, reference).empty


@pytest.mark.parametrize("change", [{"execution_authority": True}, {"initial_stop_pct": 1.},
    {"target_min_pct": .05}, {"target_step_pct": .1}])
def test_target_study_rejects_authority_stop_cost_floor_and_grid_drift(tmp_path, change):
    config = sweep.configuration()
    config.update(change)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        sweep.configuration(path)
