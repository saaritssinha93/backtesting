from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tools.fno_challenger_robustness_audit import (
    DailySeries,
    _expected_family_notices,
    average_ranks,
    calendar_from_gap,
    choose_exact_cscv_partitions,
    cscv_pbo,
    deduplicate_series,
    deflated_sharpe_probability,
    load_package,
    max_additive_drawdown,
    metrics_for_values,
    sha256_file,
)


def _frame(values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "session_date": [f"2026-01-{index + 1:02d}" for index in range(len(values))],
            "return_points": values,
            "pnl_rs": np.asarray(values) * 100.0,
            "fills": [1] * len(values),
        }
    )


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _write_daily(path: Path, dates: list[str], offset: float = 0.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "session_date": dates,
            "net_return_pct": [offset + index / 100.0 for index in range(len(dates))],
            "net_pnl_rs": [offset * 100.0 + index for index in range(len(dates))],
            "fills": [1] * len(dates),
        }
    ).to_csv(path, index=False)


def _sealed_repaired_v10_fixture(tmp_path: Path) -> tuple[Path, list[str]]:
    run = tmp_path / "sealed_repaired_v10"
    historical_dates = [
        value.date().isoformat()
        for value in pd.bdate_range("2026-05-27", periods=59)
    ]
    today_dates = ["2026-08-27"]

    for dataset_dir, dataset, dates in (
        ("historical_59_sessions", "HISTORICAL", historical_dates),
        ("today_2026_08_27", "TODAY", today_dates),
    ):
        variant_dir = (
            run
            / "individual_filter_suite"
            / "runs"
            / dataset_dir
            / "stage7_control_fixture"
        )
        _write_daily(variant_dir / "daily.csv", dates, offset=0.1)
        _write_json(
            variant_dir / "provenance.json",
            {
                "schema_version": "fno_v10_stage7_followup_challengers_v1",
                "complete": True,
                "dataset": dataset,
                "variant": {
                    "variant": "STAGE7_CONTROL",
                    "description": "fixture control",
                },
                "execution": {"cost_bps": 15.0, "slippage_bps": 0.0},
                "known_limitations": ["NESTED_FILTER_LIMITATION"],
            },
        )

    combo_run = run / "combo_suite" / "runs" / "combo_fixture"
    combo_rows: list[dict[str, Any]] = []
    for dataset, period_label, dates in (
        ("HISTORICAL", "FULL", historical_dates),
        ("TODAY", "TODAY", today_dates),
    ):
        for index, session_date in enumerate(dates):
            combo_rows.append(
                {
                    "dataset": dataset,
                    "period_label": period_label,
                    "scenario": "REFERENCE_15_0",
                    "profile_id": "STAGE7",
                    "session_date": session_date,
                    "net_return_pct": 0.2 + index / 100.0,
                    "net_pnl_rs": 20.0 + index,
                    "fills": 1,
                }
            )
    combo_run.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(combo_rows).to_csv(combo_run / "daywise.csv", index=False)
    combo_provenance = {
        "schema_version": "fno_v10_stage7_postselection_0935max050_gap_combo_v1",
        "research_design": "EXPLORATORY_POST_SELECTION_COMBINATION",
        "research_only": True,
        "promotion_eligible": False,
        "live_or_paper_authority": False,
        "cost_scenarios": [
            {"scenario": "REFERENCE_15_0", "cost_bps": 15.0, "slippage_bps": 0.0}
        ],
        "source_inputs": {
            "HISTORICAL": {"sessions": historical_dates},
            "TODAY": {"sessions": today_dates},
        },
        "limitations": ["NESTED_COMBO_LIMITATION"],
    }
    _write_json(combo_run / "provenance.json", combo_provenance)
    _write_json(
        run / "combo_suite" / "latest.json",
        {
            "schema_version": "fno_v10_stage7_postselection_0935max050_gap_combo_v1",
            "run_dir": str(combo_run.resolve()),
            "provenance_sha256": sha256_file(combo_run / "provenance.json"),
            "research_only": True,
            "promotion_eligible": False,
        },
    )

    for dataset_dir, dataset, dates in (
        ("historical", "HISTORICAL", historical_dates),
        ("today", "TODAY", today_dates),
    ):
        comparator_dir = (
            run / "v8_combined_control" / dataset_dir / "reference_15_0"
        )
        _write_daily(comparator_dir / "daily.csv", dates, offset=0.3)
        _write_json(
            comparator_dir / "summary.json",
            {
                "dataset": dataset,
                "scenario": "REFERENCE_15_0",
                "profile_id": "V8_COMBINED_CONTROL",
                "cost_bps": 15.0,
                "slippage_bps": 0.0,
                "research_only": True,
                "promotion_eligible": False,
            },
        )

    artifacts = []
    for artifact in sorted(path for path in run.rglob("*") if path.is_file()):
        artifacts.append(
            {
                "relative_path": artifact.relative_to(run).as_posix(),
                "bytes": artifact.stat().st_size,
                "sha256": sha256_file(artifact),
            }
        )
    inventory_path = run / "artifact_inventory.json"
    _write_json(
        inventory_path,
        {
            "schema_version": "fno_v10_repaired_snapshot_rerun_v1",
            "artifacts": artifacts,
        },
    )
    _write_json(
        run / "provenance.json",
        {
            "schema_version": "fno_v10_repaired_snapshot_rerun_v1",
            "complete": True,
            "research_only": True,
            "promotion_eligible": False,
            "live_or_paper_authority": False,
            "artifact_inventory": {
                "path": str(inventory_path.resolve()),
                "sha256": sha256_file(inventory_path),
            },
            "individual_variants": [{"variant": "STAGE7_CONTROL"}],
            "combo_profiles": [{"profile_id": "STAGE7"}],
            "cost_scenarios": [
                {
                    "scenario": "REFERENCE_15_0",
                    "cost_bps": 15.0,
                    "slippage_bps": 0.0,
                }
            ],
            "v8_comparator": {
                "profile_id": "V8_COMBINED_CONTROL",
                "five_minute_contract": "fixture comparator",
            },
            "limitations": ["WRAPPER_LIMITATION"],
        },
    )
    return run, historical_dates


def test_max_additive_drawdown_includes_initial_zero() -> None:
    assert max_additive_drawdown([1.0, -2.0, 0.5]) == pytest.approx(2.0)
    assert max_additive_drawdown([-0.75, 1.0]) == pytest.approx(0.75)


def test_metrics_reject_missing_session_instead_of_imputing_zero() -> None:
    frame = _frame([1.0, -0.5])
    expected = ["2026-01-01", "2026-01-02", "2026-01-03"]
    result = metrics_for_values(frame, expected)
    assert result["aligned_eligible"] is False
    assert result["missing_sessions"] == 1
    assert "net_return_points" not in result


def test_metrics_positive_share_and_drawdown() -> None:
    frame = _frame([1.0, -2.0, 0.0, 3.0])
    expected = frame["session_date"].tolist()
    result = metrics_for_values(frame, expected)
    assert result["aligned_eligible"] is True
    assert result["positive_day_share_pct"] == pytest.approx(50.0)
    assert result["net_return_points"] == pytest.approx(2.0)
    assert result["max_daily_drawdown_points"] == pytest.approx(2.0)


def test_duplicate_stage7_control_merges_only_on_exact_parity(tmp_path: Path) -> None:
    source_a = tmp_path / "a.csv"
    source_b = tmp_path / "b.csv"
    source_a.write_text("a", encoding="utf-8")
    source_b.write_text("b", encoding="utf-8")
    left = DailySeries(
        family="V10_STAGE7_GAP_GUARD",
        definition_id="V10_STAGE7_CONTROL",
        variant="CONTROL",
        scenario="REFERENCE_15_0",
        dataset="HISTORICAL",
        frame=_frame([1.0, -0.5]),
        source_path=source_a,
        cost_bps=15.0,
        slippage_bps=0.0,
    )
    right = DailySeries(
        family="V10_STAGE7_FILTERS",
        definition_id="V10_STAGE7_CONTROL",
        variant="STAGE7_CONTROL",
        scenario="REFERENCE_15_0",
        dataset="HISTORICAL",
        frame=_frame([1.0, -0.5]),
        source_path=source_b,
        cost_bps=15.0,
        slippage_bps=0.0,
    )
    canonical, inventory = deduplicate_series([left, right])
    assert len(canonical) == 1
    assert canonical[0].families == {
        "V10_STAGE7_GAP_GUARD",
        "V10_STAGE7_FILTERS",
    }
    assert inventory[1]["duplicate_definition_merged"] is True


def test_duplicate_definition_collision_fails_closed(tmp_path: Path) -> None:
    paths = [tmp_path / "a.csv", tmp_path / "b.csv"]
    for path in paths:
        path.write_text(path.name, encoding="utf-8")
    items = [
        DailySeries(
            family="A",
            definition_id="SAME",
            variant="CONTROL",
            scenario="REFERENCE_15_0",
            dataset="HISTORICAL",
            frame=_frame(values),
            source_path=path,
        )
        for path, values in zip(paths, ([1.0, 0.0], [1.0, 0.1]))
    ]
    with pytest.raises(ValueError, match="collision"):
        deduplicate_series(items)


def test_prime_59_session_calendar_has_no_strict_equal_cscv_partition() -> None:
    assert choose_exact_cscv_partitions(59, preferred=6) is None
    assert choose_exact_cscv_partitions(60, preferred=6) == 6


def test_cscv_formula_is_bounded_and_enumerates_all_splits() -> None:
    index = [f"d{value}" for value in range(24)]
    matrix = pd.DataFrame(
        {
            "A": [0.8, -0.2, 0.5, 0.1, -0.4, 0.7] * 4,
            "B": [-0.3, 0.6, 0.2, -0.1, 0.8, -0.2] * 4,
            "C": [0.1, 0.2, -0.1, 0.3, -0.2, 0.4] * 4,
        },
        index=index,
    )
    result, detail = cscv_pbo(matrix, partitions=4)
    assert result["combinations"] == math.comb(4, 2)
    assert 0.0 <= result["pbo"] <= 1.0
    assert not detail.empty
    assert detail["omega"].between(0.0, 1.0, inclusive="neither").all()


def test_deflated_sharpe_probability_is_half_at_benchmark() -> None:
    probability = deflated_sharpe_probability(
        observed_sharpe=0.25,
        benchmark_sharpe=0.25,
        observations=59,
        skewness=0.0,
        kurtosis=3.0,
    )
    assert probability == pytest.approx(0.5)
    assert deflated_sharpe_probability(0.5, 0.25, 59, 0.0, 3.0) > 0.5


def test_average_ranks_uses_worst_to_best_and_average_ties() -> None:
    ranks = average_ranks([3.0, 1.0, 3.0, 2.0])
    assert ranks.tolist() == pytest.approx([3.5, 1.0, 3.5, 2.0])


def test_repaired_v10_wrapper_loads_nested_filters_combos_and_v8(
    tmp_path: Path,
) -> None:
    run, _ = _sealed_repaired_v10_fixture(tmp_path)

    series = load_package(run)

    assert len(series) == 6
    assert {item.family for item in series} == {
        "V10_STAGE7_FILTERS",
        "V10_STAGE7_POSTSELECTION_COMBO",
        "V10_REPAIRED_V8_COMPARATOR",
    }
    assert {(item.dataset, item.family) for item in series} == {
        (dataset, family)
        for dataset in ("HISTORICAL", "TODAY")
        for family in (
            "V10_STAGE7_FILTERS",
            "V10_STAGE7_POSTSELECTION_COMBO",
            "V10_REPAIRED_V8_COMPARATOR",
        )
    }
    assert all("WRAPPER_LIMITATION" in item.limitations for item in series)
    v8 = [item for item in series if item.family == "V10_REPAIRED_V8_COMPARATOR"]
    assert all(item.cost_bps == pytest.approx(15.0) for item in v8)
    assert all(item.slippage_bps == pytest.approx(0.0) for item in v8)


def test_repaired_v10_wrapper_supplies_sealed_59_session_calendar(
    tmp_path: Path,
) -> None:
    run, expected = _sealed_repaired_v10_fixture(tmp_path)

    dates, source = calendar_from_gap([run])

    assert dates == expected
    assert source.parent.name == "combo_fixture"
    assert source.name == "provenance.json"


def test_repaired_v10_wrapper_rejects_tampered_consumed_artifact(
    tmp_path: Path,
) -> None:
    run, _ = _sealed_repaired_v10_fixture(tmp_path)
    daily_path = (
        run
        / "individual_filter_suite"
        / "runs"
        / "historical_59_sessions"
        / "stage7_control_fixture"
        / "daily.csv"
    )
    daily_path.write_text(daily_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(AssertionError, match="byte size changed"):
        load_package(run)


def test_repaired_v10_wrapper_rejects_incomplete_root(tmp_path: Path) -> None:
    run, _ = _sealed_repaired_v10_fixture(tmp_path)
    provenance_path = run / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["complete"] = False
    _write_json(provenance_path, provenance)

    with pytest.raises(ValueError, match="wrapper is incomplete"):
        load_package(run)


def test_repaired_wrapper_does_not_expect_legacy_gap_family() -> None:
    loaded = {
        "V10_STAGE7_FILTERS",
        "V10_STAGE7_POSTSELECTION_COMBO",
        "V6_CHALLENGERS",
    }

    assert _expected_family_notices(
        loaded,
        repaired_v10_wrapper_loaded=True,
    ) == []
    assert _expected_family_notices(
        loaded,
        repaired_v10_wrapper_loaded=False,
    ) == ["EXPECTED_FAMILY_NOT_LOADED:V10_STAGE7_GAP_GUARD"]
