import json

import numpy as np
import pandas as pd
import pytest

import research.g3_pullback_tuning as tuning
from research.g3_pullback_tuning import (ORIGINAL, candidates, episodes, freeze_selection,
                                       select_rules, sha, training_score, warning_rule)


def record(**changes):
    row = dict(day="2026-01-01", trade_id="t1", side="LONG", horizon_minutes=5,
               threshold_pct=.3, momentum3_atr=-.5, momentum5_atr=-.5,
               micro_break=False, adverse_body_fraction=.5, volume_ratio20=1.2,
               trend_damage=True, giveback_atr=1., decision_close=99., sma5=100.,
               event=True, warning=True, decision_ts="2026-01-01T09:30:00+05:30")
    row.update(changes)
    return row


def calendar():
    return pd.date_range("2026-01-01", periods=46).strftime("%Y-%m-%d").tolist()


def eligible_fixture():
    rows = []
    days = calendar()
    for number, day in enumerate(days[:30]):
        for horizon, threshold in ((5, .3), (30, .5)):
            rows.append(record(day=day, trade_id=f"{number}a", horizon_minutes=horizon,
                               threshold_pct=threshold))
            rows.append(record(day=day, trade_id=f"{number}b", horizon_minutes=horizon,
                               threshold_pct=threshold, event=False,
                               momentum3_atr=.2, momentum5_atr=.2))
    return pd.DataFrame(rows)


def test_candidates_are_fixed_unique_and_include_originals():
    rules = candidates()
    assert len(rules) == len({rule["candidate_id"] for rule in rules}) == 45
    assert sum(rule["family"] == "FAST" for rule in rules) == 27
    assert sum(rule["family"] == "SLOW" for rule in rules) == 18
    assert sum(rule["distance_from_original"] == 0 for rule in rules) == 2


def test_fast_logic_parentheses_and_micro_break_missing_volume():
    frame = pd.DataFrame([
        record(),
        record(micro_break=True, volume_ratio20=np.nan, adverse_body_fraction=0),
        record(micro_break=True, momentum3_atr=-.49),
        record(volume_ratio20=1.19),
        record(adverse_body_fraction=.49),
    ])
    assert warning_rule(frame, ORIGINAL["FAST"]).tolist() == [True, True, False, False, False]


def test_slow_close_sma5_gate_is_side_symmetric():
    rule = dict(ORIGINAL["SLOW"], trend="close_sma5")
    frame = pd.DataFrame([record(trend_damage=False),
                          record(side="SHORT", decision_close=101., trend_damage=False),
                          record(side="SHORT", decision_close=99.)])
    assert warning_rule(frame, rule).tolist() == [True, True, False]
    assert warning_rule(frame, ORIGINAL["SLOW"]).tolist() == [False, False, True]


def test_selection_does_not_evaluate_held_features_or_outcomes_and_tie_prefers_original():
    frame = eligible_fixture()
    selected, checks = select_rules(frame, calendar())
    poison = pd.DataFrame([record(day=calendar()[30], momentum3_atr="DO_NOT_READ",
                                  momentum5_atr="DO_NOT_READ", event="DO_NOT_READ")])
    after, after_checks = select_rules(pd.concat([frame, poison], ignore_index=True), calendar())
    assert selected == after
    pd.testing.assert_frame_equal(checks, after_checks)
    assert len(checks) == 90
    assert set(checks.fold) == {"FIT20", "CHECK10"}
    for entry in selected.values():
        assert entry["rule"]["distance_from_original"] == 0
        assert entry["minimum_training_f05"] == 1
        assert entry["selection_status"] == "ORIGINAL_RULE_SELECTED_ELIGIBLE"


def test_no_eligible_rule_keeps_original_not_forced_winner():
    frame = pd.DataFrame([record()])
    selected, _ = select_rules(frame, calendar())
    for entry in selected.values():
        assert entry["rule"]["distance_from_original"] == 0
        assert entry["selection_status"] == "NO_ELIGIBLE_IMPROVEMENT_KEEP_ORIGINAL"
        assert entry["minimum_training_f05"] is None


def test_both_folds_must_pass_independently():
    frame = eligible_fixture()
    frame.loc[frame.day.isin(calendar()[20:30]), "event"] = False
    selected, checks = select_rules(frame, calendar())
    assert checks.loc[checks.fold.eq("FIT20"), "fold_eligible"].any()
    assert checks.loc[checks.fold.eq("FIT20") & checks.distance_from_original.eq(0), "fold_eligible"].all()
    assert not checks.loc[checks.fold.eq("CHECK10"), "fold_eligible"].any()
    assert all(entry["eligible_candidates"] == 0 for entry in selected.values())


def test_rule_freeze_is_hashed_and_cannot_be_overwritten(tmp_path):
    digest = freeze_selection(tmp_path, {"chosen": ORIGINAL, "held_outcomes_used_for_selection": False})
    assert digest == sha(tmp_path / "selected_rules.json")
    assert digest in (tmp_path / "selected_rules.sha256").read_text()
    assert json.loads((tmp_path / "selected_rules.json").read_text())["chosen"] == ORIGINAL
    with pytest.raises(FileExistsError):
        freeze_selection(tmp_path, {"changed": True})


def test_dense_episode_cooldown_is_not_a_grid_recall_denominator():
    rows = [record(decision_ts=f"2026-01-01T09:{minute:02d}:00+05:30")
            for minute in (30, 31, 34, 35, 36, 40)]
    result = episodes(pd.DataFrame(rows))
    assert result.decision_ts.dt.minute.tolist() == [0, 5, 10]  # UTC conversion preserves spacing.
    assert len(result) == 3


def test_unknown_labels_are_excluded_from_training_score_not_false_alarms():
    rows = pd.DataFrame([record(outcome_available=True),
                         record(trade_id="t2", outcome_available=False, event="UNKNOWN")])
    score = training_score(rows, pd.Series([True, True]))
    assert score["source_observations"] == 2
    assert score["observations"] == score["tp"] == score["warnings"] == 1
    assert score["fp"] == 0
    assert score["unknown_warnings"] == 1
    assert score["precision"] == 1
    assert score["precision_identification_lower"] == .5
    assert score["precision_identification_upper"] == 1


def test_run_freezes_choice_before_any_held_scoring_and_keeps_outputs_separate(tmp_path, monkeypatch):
    study = tmp_path / "study"
    study.mkdir()
    days = calendar()
    frame = eligible_fixture()
    held = pd.DataFrame([record(day=days[30], trade_id="held", event=False)])
    frame = pd.concat([frame, held], ignore_index=True)
    frame["phase"] = "ENTRY_1_5"
    frame["split"] = np.where(frame.day.isin(days[:30]), "EARLIER30", "LATER16")
    frame["full_horizon_eligible"] = True
    frame["outcome_available"] = True
    frame["censored"] = False
    frame["baseline_warning"] = True
    frame["lead_minutes"] = np.where(frame.event, 1., np.nan)
    frame = tuning.apply_rules(frame, ORIGINAL)
    frame.to_csv(study / "evaluation_windows.csv", index=False)
    frame.to_csv(study / "predictions_1m.csv", index=False)
    (study / "provenance.json").write_text(json.dumps({"days": days}))
    out = tmp_path / "new_output"
    real_summarize = tuning.summarize
    calls = []

    def checked_summarize(*args, **kwargs):
        assert (out / "selected_rules.json").is_file()
        assert (out / "selected_rules.sha256").read_text().startswith(sha(out / "selected_rules.json"))
        calls.append(True)
        return real_summarize(*args, **kwargs)

    monkeypatch.setattr(tuning, "summarize", checked_summarize)
    _, result = tuning.run(study, out, bootstrap_reps=0)
    assert len(calls) == 2
    assert set(result.variant) == {"ORIGINAL", "CHOSEN"}
    assert (out / "tuned_alert_success.csv").is_file()
    assert "recall" not in pd.read_csv(out / "tuned_alert_success.csv").columns
    assert json.loads((out / "validation.json").read_text())["status"] == "PASS"
    with pytest.raises(FileExistsError):
        tuning.run(study, out, bootstrap_reps=0)
