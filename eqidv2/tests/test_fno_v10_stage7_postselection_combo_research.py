from __future__ import annotations

from pathlib import Path

import pandas as pd

import fno_v10_stage7_postselection_combo_research as combo


def test_bounded_postselection_profile_registry() -> None:
    combo.validate_design()
    assert len(combo.PROFILES) == 6
    assert combo.PROFILE_BY_ID["MAX050_GAP2"].selection_variant == (
        "0935_LONG_MOVE_MAX_050"
    )
    assert combo.PROFILE_BY_ID["MAX050_GAP2"].gap_variant == "MAX_2_BPS"
    assert combo.PROFILE_BY_ID["MAX050_GAP2"].required_composed_profile is True


def test_only_isolated_max050_selection_is_composed() -> None:
    composed = [profile for profile in combo.PROFILES if profile.required_composed_profile]
    assert composed
    assert {profile.selection_variant for profile in composed} == {
        "0935_LONG_MOVE_MAX_050"
    }
    assert {profile.gap_variant for profile in composed} == {
        "CONTROL",
        "MAX_0_BPS",
        "MAX_2_BPS",
        "REJECT_ALL_GAP_FILLS",
    }


def test_exact_audit_parity_accepts_identical_csv(tmp_path: Path) -> None:
    row = {column: None for column in combo.PARITY_COLUMNS}
    row.update(
        {
            "candidate_id": "A",
            "status": "STOPPED",
            "reason": "STOP",
            "filled": True,
            "entry_price": 100.0,
            "exit_price": 99.0,
            "net_return_pct": -1.15,
            "quantity": 500,
            "net_pnl_rs": -575.0,
        }
    )
    frame = pd.DataFrame([row])
    path = tmp_path / "reference.csv"
    frame.to_csv(path, index=False)
    result = combo.exact_audit_parity(frame.copy(), path)
    assert result["passed"] is True
    assert result["candidate_rows"] == 1


def test_train_test_decision_requires_both_periods() -> None:
    rows = []
    for period, delta in (("TRAIN", 1.0), ("TEST", 0.5)):
        rows.append(
            {
                "dataset": "HISTORICAL",
                "period": period,
                "scenario": "REFERENCE_15_0",
                "target_profile": "MAX050_GAP2",
                "comparator_profile": "STAGE7",
                "higher_net": True,
                "higher_profit_factor": True,
                "drawdown_not_worse": True,
                "period_dominates": True,
                "delta_net_return_points": delta,
                "delta_profit_factor": delta / 10,
                "delta_max_daily_drawdown_points": -delta,
            }
        )
    result = combo.train_test_decisions(pd.DataFrame(rows)).iloc[0]
    assert result["higher_net_both_train_test"] == True  # noqa: E712
    assert result["dominates_both_train_test"] == True  # noqa: E712
    assert result["train_delta_net_points"] == 1.0
    assert result["test_delta_net_points"] == 0.5
