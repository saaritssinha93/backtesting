import copy
import math

import numpy as np
import pandas as pd
import pytest

import research.g3_pullback_selective as selective
from research.g3_pullback_selective_features import FEATURES


def calendar():
    return pd.date_range("2026-07-01", periods=46).strftime("%Y-%m-%d").tolist()


def record(**changes):
    row = {name: 0.0 for name in FEATURES}
    row.update(day="2026-07-01", trade_id="t1", side="LONG", horizon_minutes=5,
               decision_ts=pd.Timestamp("2026-07-01T09:45:00+05:30"),
               model_stage="ENTRY_RISK", event=True, outcome_available=True,
               full_horizon_eligible=True, is_grid=True, lead_minutes=2.0,
               warning=False)
    row.update(changes)
    return row


class KnownScoreModel:
    """Small deterministic model: the fixture's first feature contains its score."""
    named_steps = {"estimator": object()}

    def fit(self, values, labels, **kwargs):
        assert list(values.columns) == list(FEATURES)
        assert set(labels.unique()) == {False, True}
        assert len(kwargs["estimator__sample_weight"]) == len(values)
        self.fit_inputs = values.copy()
        return self

    def predict_proba(self, values):
        scores = values.side_short.astype(float).to_numpy()
        return np.column_stack([1 - scores, scores])


def development_fixture():
    rows = []
    for i, day in enumerate(calendar()[:30]):
        hit = i % 2 == 0
        rows.append(record(day=day, trade_id=f"trade{i}",
                           decision_ts=pd.Timestamp(day, tz="UTC") + pd.Timedelta(hours=4),
                           event=hit, side_short=.98 if hit else .02))
    return pd.DataFrame(rows)


def test_selection_never_reads_later_features_or_outcomes(monkeypatch):
    monkeypatch.setattr(selective, "make_model", lambda name: KnownScoreModel())
    development = development_fixture()
    before, models, model_checks, threshold_checks = selective.select_models(development, calendar())
    poison = record(day=calendar()[30], trade_id="later", event="DO_NOT_READ",
                    outcome_available="DO_NOT_READ", model_stage="DO_NOT_READ")
    poison.update({name: "DO_NOT_READ" for name in FEATURES})
    with_future = pd.concat([development, pd.DataFrame([poison])], ignore_index=True)
    after, later_models, after_models, after_thresholds = selective.select_models(with_future, calendar())
    assert before == after
    pd.testing.assert_frame_equal(model_checks, after_models)
    pd.testing.assert_frame_equal(threshold_checks, after_thresholds)
    assert models.keys() == later_models.keys()
    for key in models:
        pd.testing.assert_frame_equal(models[key].fit_inputs, later_models[key].fit_inputs,
                                      check_dtype=False)


def test_tiny_perfect_sample_cannot_pass_high_confidence_gate(monkeypatch):
    monkeypatch.setattr(selective, "make_model", lambda name: KnownScoreModel())
    selection, _, _, checks = selective.select_models(development_fixture(), calendar())
    perfect = checks.loc[checks.alert_precision.eq(1)]
    assert len(perfect) > 0
    assert perfect.resolved_alerts.eq(5).all()
    assert perfect.watch_eligible.all()
    assert not checks.high_eligible.any()
    assert selection["ENTRY_RISK_5"]["watch_threshold"] is not None
    assert selection["ENTRY_RISK_5"]["high_threshold"] is None
    assert selective.exact_lower(20, 20) < .90
    assert selective.exact_lower(29, 29) >= .90


def test_unknown_alerts_cannot_supply_resolved_trade_or_date_support(monkeypatch):
    monkeypatch.setattr(selective, "make_model", lambda name: KnownScoreModel())
    fit = development_fixture().iloc[:20]
    days = calendar()
    check = [record(day=day, trade_id=f"negative{i}", event=False, side_short=.02,
                    decision_ts=pd.Timestamp(day, tz="UTC") + pd.Timedelta(hours=4))
             for i, day in enumerate(days[20:30])]
    # Five resolved alerts all occur on one trade and date. Two other dates and
    # trades have only unknown alerts, so they cannot establish outcome support.
    check.extend(record(day=days[20], trade_id="one_supported_trade", side_short=.98,
                        decision_ts=pd.Timestamp(days[20], tz="UTC") +
                        pd.Timedelta(hours=5, minutes=6*i)) for i in range(5))
    check.extend(record(day=days[20+i], trade_id=f"unknown{i}", side_short=.98,
                        outcome_available=False, full_horizon_eligible=False,
                        decision_ts=pd.Timestamp(days[20+i], tz="UTC") +
                        pd.Timedelta(hours=5)) for i in (1, 2))
    frame = pd.concat([fit, pd.DataFrame(check)], ignore_index=True)
    selection, _, _, checks = selective.select_models(frame, days)
    assert checks.resolved_alerts.max() == 5
    assert not checks.watch_eligible.any()
    assert selection["ENTRY_RISK_5"]["watch_threshold"] is None


def test_cooldown_crosses_stage_boundary_and_unknown_alert_still_starts_clock():
    frame = pd.DataFrame([
        record(decision_ts=pd.Timestamp("2026-07-01T09:44:00+05:30"),
               outcome_available=False),
        record(decision_ts=pd.Timestamp("2026-07-01T09:46:00+05:30"),
               model_stage="PROFIT_GIVEBACK"),
        record(decision_ts=pd.Timestamp("2026-07-01T09:49:00+05:30"),
               model_stage="PROFIT_GIVEBACK"),
    ])
    warnings = pd.Series(True, index=frame.index)
    emitted = selective.alert_mask(frame, warnings)
    assert emitted.tolist() == [True, False, True]
    score = selective.score_policy(frame, warnings, ["2026-07-01"], bootstrap=False)
    assert score["alert_count"] == 2
    assert score["unknown_alerts"] == score["resolved_alerts"] == 1
    assert score["first_alert_count"] == 1
    assert score["first_resolved_alerts"] == 0
    assert math.isnan(score["first_alert_precision"])
    changed = frame.copy()
    changed["outcome_available"] = ~changed.outcome_available
    changed["event"] = ~changed.event
    pd.testing.assert_series_equal(emitted, selective.alert_mask(changed, warnings))


def test_stage_subgroup_scoring_keeps_full_policy_cooldown():
    frame = pd.DataFrame([
        record(decision_ts=pd.Timestamp("2026-07-01T09:44:00+05:30"), research_watch=True),
        record(decision_ts=pd.Timestamp("2026-07-01T09:46:00+05:30"),
               model_stage="PROFIT_GIVEBACK", research_watch=True),
    ])
    frame["emitted"] = selective.alert_mask(frame, frame.research_watch)
    later = frame.loc[frame.model_stage.eq("PROFIT_GIVEBACK")]
    stat = selective.score_policy(later, later.research_watch, ["2026-07-01"],
                                  emitted=later.emitted, bootstrap=False)
    assert stat["alert_count"] == 0
    assert stat["grid_warnings"] == 1


def test_combined_gate_rejects_stage_whose_warnings_are_suppressed_and_ignores_later_data():
    days = calendar()
    rows = []
    for i, day in enumerate(days[20:25]):
        for stage, minutes in (("ENTRY_RISK", 29), ("PROFIT_GIVEBACK", 31)):
            stamp = pd.Timestamp(day, tz="UTC") + pd.Timedelta(hours=4, minutes=minutes)
            rows.extend([
                record(day=day, trade_id=f"shared{i}", model_stage=stage,
                       decision_ts=stamp, side_short=.9),
                record(day=day, trade_id=f"negative{i}", model_stage=stage,
                       decision_ts=stamp, side_short=.1, event=False),
            ])
    frame = pd.DataFrame(rows)
    initial = {f"{stage}_5": dict(stage=stage, horizon_minutes=5, watch_threshold=.7,
                                  high_threshold=None, beats_constant=True,
                                  status="RESEARCH_WATCH_ONLY")
               for stage in selective.STAGES}
    fitted = {key: KnownScoreModel() for key in initial}
    selection = copy.deepcopy(initial)
    audit = selective.enforce_combined_gates(frame, days, selection, fitted)
    assert selection["ENTRY_RISK_5"]["watch_threshold"] == .7
    assert selection["PROFIT_GIVEBACK_5"]["watch_threshold"] is None
    assert selection["PROFIT_GIVEBACK_5"]["combined_gate_rejection"]
    suppressed = audit.loc[audit.key.eq("PROFIT_GIVEBACK_5")]
    assert suppressed.resolved_alerts.eq(0).all()
    assert not suppressed.passed.any()
    poison = record(day=days[30], trade_id="later", event="DO_NOT_READ")
    poison.update({name: "DO_NOT_READ" for name in FEATURES})
    with_future = pd.concat([frame, pd.DataFrame([poison])], ignore_index=True)
    changed = copy.deepcopy(initial)
    changed_audit = selective.enforce_combined_gates(with_future, days, changed, fitted)
    assert changed == selection
    pd.testing.assert_frame_equal(audit, changed_audit)


def test_no_qualifying_signal_and_insufficient_evidence_are_distinct():
    frame = pd.DataFrame([
        record(trade_id="below", side_short=.1),
        record(trade_id="above", side_short=.9),
        record(trade_id="no_gate", model_stage="PROFIT_GIVEBACK", side_short=.9),
        record(trade_id="out_of_scope", model_stage="LATE_NO_ESTABLISHED_PROFIT"),
    ])
    selected = {
        "ENTRY_RISK_5": dict(stage="ENTRY_RISK", horizon_minutes=5,
                             watch_threshold=.7, high_threshold=None),
        "PROFIT_GIVEBACK_5": dict(stage="PROFIT_GIVEBACK", horizon_minutes=5,
                                  watch_threshold=None, high_threshold=None),
    }
    fitted = {key: KnownScoreModel() for key in selected}
    out = selective.apply_models(frame, selected, fitted)
    assert out.signal_state.tolist() == ["NO_QUALIFYING_SIGNAL", "RESEARCH_WATCH",
                                       "INSUFFICIENT_EVIDENCE", "INSUFFICIENT_EVIDENCE"]
    assert "pullback remains possible" in out.signal_reason.iloc[0]
    assert "outside both model scopes" in out.signal_reason.iloc[3]
    assert out.risk_score.iloc[2] == .9
    assert out.alert_RESEARCH_WATCH.tolist() == [False, True, False, False]
    assert not out.high_confidence.any()


def test_zero_warnings_have_undefined_precision_not_perfect_precision():
    frame = pd.DataFrame([record(event=False), record(trade_id="t2", event=True)])
    stat = selective.score_policy(frame, pd.Series(False, index=frame.index),
                                  ["2026-07-01"], bootstrap=False)
    assert stat["alert_count"] == stat["grid_warnings"] == 0
    for name in ("alert_precision", "grid_precision", "first_alert_precision",
                 "exact_independent_lower", "precision_identification_lower"):
        assert math.isnan(stat[name])
    assert stat["grid_recall"] == 0
    assert stat["no_warning_event_rate"] == .5
    assert stat["npv"] == .5


def test_full_horizon_no_warning_risk_excludes_censored_negatives_and_unknowns():
    frame = pd.DataFrame([
        record(trade_id="complete_negative", event=False),
        record(trade_id="censored_negative", event=False, full_horizon_eligible=False),
        record(trade_id="confirmed_event", event=True),
        record(trade_id="unknown", event=False, outcome_available=False,
               full_horizon_eligible=False),
    ])
    stat = selective.score_policy(frame, pd.Series(False, index=frame.index),
                                  ["2026-07-01"], bootstrap=False)
    assert stat["grid_rows"] == 4
    assert stat["resolved_grid_rows"] == 3
    assert stat["no_warning_event_rate"] == pytest.approx(1 / 3)
    assert stat["full_horizon_no_warning_rows"] == 2
    assert stat["full_horizon_no_warning_event_rate"] == .5


def test_trade_balanced_weights_give_each_trade_equal_total_weight():
    frame = pd.DataFrame([record(trade_id="long_trade") for _ in range(10)] +
                         [record(trade_id="short_trade")])
    weights = selective.balanced_weights(frame)
    assert weights[:10].sum() == pytest.approx(weights[10])
    assert weights.sum() == pytest.approx(len(frame))


def test_coverage_highlights_unmonitored_trade_without_all_clear_claim():
    day = calendar()[0]
    frame = pd.DataFrame([record(day=day)])
    predicted = selective.apply_models(frame, {}, {})
    source = pd.DataFrame([
        dict(day=day, trade_id="t1", tradingsymbol="AAA", side="LONG"),
        dict(day=day, trade_id="no_observation", tradingsymbol="BBB", side="SHORT"),
    ])
    trades, daily = selective.coverage_tables(predicted, source, calendar())
    assert len(trades) == 4
    unavailable = trades.loc[trades.trade_id.eq("no_observation")]
    assert unavailable.availability.eq("NO_POST_ENTRY_OBSERVATION").all()
    assert unavailable.no_high_confidence_signal.all()
    assert unavailable.no_qualifying_watch.all()
    assert unavailable.interpretation.eq("No signal is not proof of no pullback").all()
    assert daily.loc[daily.day.eq(day), "high_confidence_alerts"].eq(0).all()
    assert daily.loc[daily.day.eq(calendar()[1]), "status"].eq("NO_TRADES").all()
