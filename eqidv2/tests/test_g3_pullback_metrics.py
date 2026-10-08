import math

import pandas as pd
import pytest

from research.g3_pullback_metrics import COLUMNS, summarize, summarize_phases


def record(trade_id="t1", day="2026-08-03", **changes):
    result = dict(day=day, trade_id=trade_id, side="LONG", horizon_minutes=5,
                  threshold_pct=0.3, split="EARLIER30", phase="ENTRY_1_5",
                  warning=False, event=False, full_horizon_eligible=True,
                  lead_minutes=math.nan, censored=False, baseline_warning=False)
    result.update(changes)
    return result


def group(result, **filters):
    criteria = dict(analysis="PRIMARY", split="ALL", side="BOTH", horizon_minutes=5,
                    threshold_pct=0.3, phase="ALL")
    criteria.update(filters)
    for column, value in criteria.items():
        result = result.loc[result[column].eq(value)]
    assert len(result) == 1
    return result.iloc[0]


def test_confusion_denominators_and_paired_baseline_are_grid_based():
    data = pd.DataFrame([
        record(warning=True, event=True, lead_minutes=2, baseline_warning=True),
        record(warning=True, event=False),
        record("t2", event=True, lead_minutes=4),
        record("t3", baseline_warning=True),
    ])
    row = group(summarize(data, ["2026-08-03"], bootstrap_reps=20))
    assert [row.tp, row.fp, row.fn, row.tn] == [1, 1, 1, 1]
    for name in ("precision", "recall", "fpr", "specificity", "accuracy", "prevalence",
                 "warning_rate", "baseline_precision"):
        assert row[name] == pytest.approx(0.5)
    assert row.lift == 1
    assert row.precision_minus_prevalence == 0
    assert row.precision_minus_baseline_precision == 0
    assert row.observations == 4
    assert row.trades == 3  # Do not collapse observations to trade outcomes.
    assert row.days == 1
    assert row.median_lead_minutes_tp == 2  # Not the FN event's lead.
    assert row.precision_wilson_low == pytest.approx(0.0945312057)
    assert row.precision_wilson_high == pytest.approx(0.9054687943)


def test_primary_retains_early_exit_negative_secondary_drops_it():
    data = pd.DataFrame([
        record(warning=True, censored=True, full_horizon_eligible=False),
        record("t2", warning=True, event=True, lead_minutes=1),
    ])
    result = summarize(data, ["2026-08-03"], bootstrap_reps=0)
    primary = group(result)
    eligible = group(result, analysis="FULL_HORIZON")
    assert (primary.observations, primary.tp, primary.fp, primary.precision) == (2, 1, 1, 0.5)
    assert primary.censored_observations == 1
    assert eligible.precision == 1
    assert eligible.observations == 1
    assert eligible.censored_observations == 0
    assert eligible.source_censored_observations == 1
    assert eligible.excluded_observations == 1
    assert eligible.source_observations == 2


def test_empty_and_zero_denominators_are_not_reported_as_success():
    empty = summarize(pd.DataFrame(), [])
    assert empty.empty
    assert list(empty.columns) == list(COLUMNS)
    result = summarize(pd.DataFrame([record()]), ["2026-08-03"], bootstrap_reps=10)
    row = group(result)
    for name in ("precision", "recall", "baseline_precision", "lift", "precision_wilson_low",
                 "precision_bootstrap_low", "precision_minus_baseline_precision"):
        assert math.isnan(row[name])
    assert row.specificity == 1
    assert row.accuracy == 1
    assert row.bootstrap_valid_reps == 0
    absent = group(result, side="SHORT")
    assert absent.observations == 0
    assert math.isnan(absent.accuracy)


def test_day_cluster_bootstrap_is_deterministic_and_counts_empty_days():
    days = ["2026-08-03", "2026-08-04", "2026-08-05"]
    rows = [record(str(i), days[0], warning=True, event=True, lead_minutes=1,
                   baseline_warning=True) for i in range(12)]
    rows += [record(str(i), days[1], warning=True, baseline_warning=True) for i in range(12)]
    data = pd.DataFrame(rows)
    first = summarize(data, days, bootstrap_reps=200, seed=7)
    again = summarize(data.sample(frac=1, random_state=3), days, bootstrap_reps=200, seed=7)
    pd.testing.assert_frame_equal(first, again)
    row = group(first)
    assert row.calendar_days == 3
    assert row.days == 2
    assert row.precision_bootstrap_low == 0
    assert row.precision_bootstrap_high == 1
    assert 0 < row.bootstrap_valid_reps < 200  # Some samples contain only the empty day.
    # Paired quantities remain equal on every resample, not independent bootstraps.
    assert row.precision_minus_baseline_precision_bootstrap_low == 0
    assert row.precision_minus_baseline_precision_bootstrap_high == 0
    assert row.precision_minus_prevalence_bootstrap_low == 0
    assert row.precision_minus_prevalence_bootstrap_high == 0


def test_split_side_horizon_threshold_and_phase_are_never_pooled():
    data = pd.DataFrame([
        record(warning=True, event=True, lead_minutes=1),
        record("t2", "2026-08-04", side="SHORT", split="LATER16", horizon_minutes=30,
               threshold_pct=0.5, phase="LATE_31_PLUS", warning=True),
    ])
    days = ["2026-08-03", "2026-08-04"]
    result = summarize(data, days, bootstrap_reps=0)
    assert group(result, split="EARLIER30", side="LONG").precision == 1
    assert group(result, split="LATER16", side="SHORT", horizon_minutes=30,
                 threshold_pct=0.5).precision == 0
    phases = summarize_phases(data, days)
    assert group(phases, phase="ENTRY_1_5").observations == 1
    assert group(phases, phase="EARLY_6_30").observations == 0
    assert (phases.bootstrap_reps == 0).all()


@pytest.mark.parametrize("changes", [
    {"warning": "False"}, {"event": True, "censored": True},
    {"event": True, "full_horizon_eligible": False},
    {"censored": True, "full_horizon_eligible": True},
    {"horizon_minutes": 0}, {"threshold_pct": float("inf")},
    {"side": "BOTH"}, {"lead_minutes": -1},
])
def test_rejects_invalid_input(changes):
    with pytest.raises(ValueError):
        summarize(pd.DataFrame([record(**changes)]), ["2026-08-03"], bootstrap_reps=0)


def test_rejects_missing_or_duplicate_calendar_dates_and_conflicting_splits():
    data = pd.DataFrame([record()])
    with pytest.raises(ValueError, match="absent"):
        summarize(data, [])
    with pytest.raises(ValueError, match="unique"):
        summarize(data, ["2026-08-03", "2026-08-03"])
    with pytest.raises(ValueError, match="multiple splits"):
        summarize(pd.DataFrame([record(), record("t2", split="LATER16")]), ["2026-08-03"])


def test_unknown_exit_order_is_not_a_false_alarm_and_has_identification_bounds():
    frame = pd.DataFrame([
        record(warning=True, event=True, lead_minutes=1, outcome_available=True),
        record("t2", warning=True, outcome_available=True),
        record("t3", warning=True, outcome_available=False, full_horizon_eligible=False, censored=True),
        record("t4", warning=False, outcome_available=False, full_horizon_eligible=False, censored=True),
    ])
    row = group(summarize(frame, ["2026-08-03"], bootstrap_reps=10))
    assert (row.tp, row.fp, row.fn, row.tn) == (1, 1, 0, 0)
    assert row.source_observations == 4
    assert row.observations == 2
    assert row.unknown_observations == 2
    assert row.unknown_warnings == 1
    assert row.precision == .5
    assert row.precision_identification_lower == pytest.approx(1/3)
    assert row.precision_identification_upper == pytest.approx(2/3)
