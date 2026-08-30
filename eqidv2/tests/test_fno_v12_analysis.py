from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

import fno_v12_analysis as analysis


BASELINE = "V11_STAGE0_FROZEN_CONTROL"
GOOD = "V12_GOOD"
BAD = "V12_BAD"
SMALL = "V12_TOO_SMALL"


def _sessions() -> list[date]:
    return [
        date(2026, 6, 29),
        date(2026, 6, 30),
        date(2026, 7, 1),
        date(2026, 7, 2),
        date(2026, 8, 3),
        date(2026, 8, 4),
    ]


def _metrics() -> pd.DataFrame:
    baselines = {
        analysis.REFERENCE_SCENARIO: (100.0, 2.00),
        analysis.STRESS_SCENARIO: (80.0, 1.80),
        analysis.HARSH_SCENARIO: (60.0, 1.50),
    }
    observed = {
        GOOD: {
            analysis.REFERENCE_SCENARIO: (110.0, 1.95),
            analysis.STRESS_SCENARIO: (90.0, 1.76),
            analysis.HARSH_SCENARIO: (64.0, 1.46),
        },
        BAD: {
            analysis.REFERENCE_SCENARIO: (101.0, 1.96),
            analysis.STRESS_SCENARIO: (79.0, 1.76),
            analysis.HARSH_SCENARIO: (61.0, 1.46),
        },
        SMALL: {
            analysis.REFERENCE_SCENARIO: (120.0, 2.10),
            analysis.STRESS_SCENARIO: (100.0, 1.90),
            analysis.HARSH_SCENARIO: (75.0, 1.60),
        },
    }
    rows: list[dict[str, object]] = []
    for scenario, (net, pf) in baselines.items():
        rows.append(
            {
                "variant_id": BASELINE,
                "stage_id": "STAGE_00_FROZEN_V11",
                "family": "CONTROL",
                "period": analysis.FULL_PERIOD,
                "scenario": scenario,
                "net_return_points": net,
                "profit_factor": pf,
                "fills": 100,
                "max_daily_drawdown_points": 10.0,
            }
        )
        rows.append(
            {
                "variant_id": BASELINE,
                "stage_id": "STAGE_00_FROZEN_V11",
                "family": "CONTROL",
                "period": analysis.FORWARD_PERIOD,
                "scenario": scenario,
                "net_return_points": net * 0.10,
                "profit_factor": pf,
                "fills": 10,
                "max_daily_drawdown_points": 2.0,
            }
        )
    for variant_id, scenarios in observed.items():
        for scenario, (net, pf) in scenarios.items():
            rows.append(
                {
                    "variant_id": variant_id,
                    "stage_id": "STAGE_TEST",
                    "family": "TEST",
                    "period": analysis.FULL_PERIOD,
                    "scenario": scenario,
                    "net_return_points": net,
                    "profit_factor": pf,
                    "fills": 80 if variant_id != BAD else 90,
                    "max_daily_drawdown_points": 10.4,
                }
            )
            forward_delta = 1.0 if variant_id != BAD else -1.0
            rows.append(
                {
                    "variant_id": variant_id,
                    "stage_id": "STAGE_TEST",
                    "family": "TEST",
                    "period": analysis.FORWARD_PERIOD,
                    "scenario": scenario,
                    "net_return_points": baselines[scenario][0] * 0.10
                    + forward_delta,
                    "profit_factor": pf,
                    "fills": 8,
                    "max_daily_drawdown_points": 2.0,
                }
            )
    return pd.DataFrame(rows)


def _daywise() -> pd.DataFrame:
    total = {
        analysis.REFERENCE_SCENARIO: 100.0,
        analysis.STRESS_SCENARIO: 80.0,
        analysis.HARSH_SCENARIO: 60.0,
    }
    variant_total_delta = {
        GOOD: {
            analysis.REFERENCE_SCENARIO: 10.0,
            analysis.STRESS_SCENARIO: 10.0,
            analysis.HARSH_SCENARIO: 4.0,
        },
        BAD: {
            analysis.REFERENCE_SCENARIO: 1.0,
            analysis.STRESS_SCENARIO: -1.0,
            analysis.HARSH_SCENARIO: 1.0,
        },
        SMALL: {
            analysis.REFERENCE_SCENARIO: 20.0,
            analysis.STRESS_SCENARIO: 20.0,
            analysis.HARSH_SCENARIO: 15.0,
        },
    }
    rows: list[dict[str, object]] = []
    for scenario, baseline_total in total.items():
        baseline_daily = baseline_total / len(_sessions())
        for session in _sessions():
            rows.append(
                {
                    "variant_id": BASELINE,
                    "scenario": scenario,
                    "session_date": session.isoformat(),
                    "period_label": analysis.FULL_PERIOD,
                    "net_return_pct": baseline_daily,
                    "net_pnl_rs": baseline_daily * 500.0,
                }
            )
        for variant_id, deltas in variant_total_delta.items():
            delta_daily = deltas[scenario] / len(_sessions())
            for session in _sessions():
                # BAD is negative throughout STRESS, making its ex-July and
                # explicit forward paired checks fail transparently.
                rows.append(
                    {
                        "variant_id": variant_id,
                        "scenario": scenario,
                        "session_date": session.isoformat(),
                        "period_label": analysis.FULL_PERIOD,
                        "net_return_pct": baseline_daily + delta_daily,
                        "net_pnl_rs": (baseline_daily + delta_daily) * 500.0,
                    }
                )
    return pd.DataFrame(rows)


def _side_setup() -> pd.DataFrame:
    rows = []
    for variant_id in (GOOD, BAD, SMALL):
        for side, net in (("LONG", 8.0), ("SHORT", 7.0)):
            rows.append(
                {
                    "variant_id": variant_id,
                    "scenario": analysis.HARSH_SCENARIO,
                    "period": analysis.FULL_PERIOD,
                    "side": side,
                    "setup_id": "09:25_" + side,
                    "net_return_points": net,
                }
            )
    return pd.DataFrame(rows)


def test_pairwise_daywise_delta_uses_outer_alignment_and_exposes_missing() -> None:
    daywise = _daywise()
    missing_mask = (
        daywise["variant_id"].eq(BAD)
        & daywise["scenario"].eq(analysis.REFERENCE_SCENARIO)
        & daywise["session_date"].eq(_sessions()[0].isoformat())
    )
    daywise = daywise.loc[~missing_mask]

    result = analysis.pairwise_daywise_deltas(
        daywise, BASELINE, variant_ids=[GOOD, BAD]
    )
    bad = result.loc[
        result["variant_id"].eq(BAD)
        & result["scenario"].eq(analysis.REFERENCE_SCENARIO)
    ]
    assert len(bad) == len(_sessions())
    assert int(bad["paired_session"].sum()) == len(_sessions()) - 1
    missing = bad.loc[~bad["paired_session"]].iloc[0]
    assert bool(missing["baseline_session_present"])
    assert not bool(missing["variant_session_present"])
    assert np.isnan(missing["delta_net_return_points"])


def test_development_gates_pass_fail_insufficient_and_robust_ranking() -> None:
    gates = analysis.isolated_development_gates(
        _metrics(),
        _daywise(),
        _side_setup(),
        BASELINE,
        {GOOD: 50, BAD: 45, SMALL: 29},
        candidate_variant_ids=[GOOD, BAD, SMALL],
        forward_session_dates=_sessions()[-2:],
    ).set_index("variant_id")

    assert gates.at[GOOD, "gate_status"] == "PASS"
    assert bool(gates.at[GOOD, "gate_passed"])
    assert gates.at[BAD, "gate_status"] == "FAIL"
    assert "net_at_least_baseline_stress_20_2" in gates.at[BAD, "failed_checks"]
    assert gates.at[SMALL, "gate_status"] == "INSUFFICIENT"
    assert "affected_decisions_below_30:29" in gates.at[SMALL, "insufficient_reasons"]

    # SMALL is still the raw best-observed result; it is deliberately barred
    # from becoming the best gate-passing result by the materiality floor.
    assert int(gates.at[SMALL, "observed_rank"]) == 1
    best_observed, best_passing = analysis.select_best_variants(gates.reset_index())
    assert best_observed["variant_id"] == SMALL
    assert best_passing["variant_id"] == GOOD


def test_gate_can_use_aggregate_forward_period_and_requires_both_harsh_sides() -> None:
    side = _side_setup()
    side.loc[
        side["variant_id"].eq(GOOD) & side["side"].eq("SHORT"),
        "net_return_points",
    ] = -0.1
    # The staged runner's native schema identifies side summaries by group_id.
    side["group_id"] = "SIDE_" + side["side"]
    side = side.drop(columns="side")
    gates = analysis.isolated_development_gates(
        _metrics(),
        _daywise(),
        side,
        BASELINE,
        {
            GOOD: {
                analysis.REFERENCE_SCENARIO: {"affected_decisions": 40}
            }
        },
        candidate_variant_ids=[GOOD],
    )
    row = gates.iloc[0]
    assert row["gate_status"] == "FAIL"
    assert not bool(row["check_harsh_short_net_positive"])
    assert row["forward_extension_delta_source_reference_15_0"] == (
        "AGGREGATE_MATCHED_FORWARD_PERIOD"
    )
    assert row["forward_extension_paired_delta_reference_15_0"] == pytest.approx(1.0)


def test_bootstrap_is_exactly_2000_replicates_and_deterministic() -> None:
    deltas = analysis.pairwise_daywise_deltas(
        _daywise(), BASELINE, variant_ids=[GOOD]
    )
    first = analysis.paired_bootstrap_and_concentration(deltas)
    second = analysis.paired_bootstrap_and_concentration(deltas)
    pdt.assert_frame_equal(first, second, check_exact=True)
    assert set(first["bootstrap_replicates"]) == {2_000}
    assert set(first["bootstrap_unit"]) == {"PAIRED_SESSION"}
    assert first["pairing_complete"].all()
    reference = first.loc[
        first["scenario"].eq(analysis.REFERENCE_SCENARIO)
    ].iloc[0]
    assert reference["observed_delta_net_points"] == pytest.approx(10.0)
    assert reference["bootstrap_probability_delta_positive"] == pytest.approx(1.0)
    assert reference["positive_delta_sessions"] == len(_sessions())
    assert reference["top_5_absolute_sessions_share_pct"] == pytest.approx(
        5 / 6 * 100.0
    )


def test_analyze_bundle_and_json_safe_summary() -> None:
    bundle = analysis.analyze_v12_results(
        _metrics(),
        _daywise(),
        _side_setup(),
        BASELINE,
        {GOOD: 50},
        candidate_variant_ids=[GOOD],
        forward_session_dates=_sessions()[-2:],
    )
    assert bundle.best_observed["variant_id"] == GOOD
    assert bundle.best_gate_passing["variant_id"] == GOOD
    assert len(bundle.pairwise_daywise_deltas) == len(_sessions()) * 3
    assert len(bundle.bootstrap_and_concentration) == 3
    summary = bundle.summary()
    assert summary["analysis_schema_version"] == analysis.ANALYSIS_SCHEMA_VERSION
    assert summary["best_gate_passing"]["variant_id"] == GOOD
    assert analysis.json_safe(float("nan")) is None
    assert analysis.json_safe(date(2026, 8, 30)) == "2026-08-30"


def test_incomplete_frozen_baseline_is_rejected() -> None:
    metrics = _metrics()
    metrics = metrics.loc[
        ~(
            metrics["variant_id"].eq(BASELINE)
            & metrics["period"].eq(analysis.FULL_PERIOD)
            & metrics["scenario"].eq(analysis.HARSH_SCENARIO)
        )
    ]
    with pytest.raises(ValueError, match="frozen V11 full-history baseline"):
        analysis.isolated_development_gates(
            metrics,
            _daywise(),
            _side_setup(),
            BASELINE,
            {GOOD: 50},
            candidate_variant_ids=[GOOD],
        )
