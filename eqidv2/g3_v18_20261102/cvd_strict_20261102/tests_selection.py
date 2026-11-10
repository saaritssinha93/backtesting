"""Strict feature thresholds and native full-pool reranking contracts."""
import numpy as np
import pandas as pd
import pytest

from g3_v18_20261102.cvd_advanced_20261102.tests_selection import row as base_row
from . import selection as s


def row(symbol="A", sid=1, side="SHORT", **updates):
    sign = 1. if side == "LONG" else -1.
    out = base_row(symbol, sid, side=side, acceleration=.20*sign)
    out.update(adv_recent15_pressure=.30*sign, adv_rvol20=1.5)
    out.update(updates)
    return out


def test_original_control_is_exact_and_inputs_unchanged():
    raw = pd.DataFrame([row(), row("B", 2, traded_value=900.)])
    before = raw.copy(deep=True)
    expected = s.native.select(raw, "control")
    actual = s.select(raw)
    for a,b in zip(actual,expected):
        pd.testing.assert_frame_equal(a,b)
    pd.testing.assert_frame_equal(raw,before)


@pytest.mark.parametrize("side,sign", [("LONG",1.),("SHORT",-1.)])
def test_strict_inclusive_thresholds_use_raw_acceleration(side,sign):
    raw = pd.DataFrame([row(side=side, adv_recent15_pressure=.10*sign, adv_acceleration=.05*sign, adv_rvol20=1.20)])
    assert s.gate(raw,"strict_rvol_gate")[0].all()
    assert len(s.select(raw,"strict_rvol_gate")[0]) == 1
    raw["adv_acceleration"] = .049*sign
    assert not s.gate(raw,"strict_gate")[0].any()


def test_single_candidate_hard_rejection_and_matched_control():
    raw = pd.DataFrame([row(adv_recent15_pressure=.30)])
    assert len(s.select(raw,"base_control")[0]) == 1
    assert len(s.select(raw,"cvd_ready_control")[0]) == 1
    for variant in ("aligned_gate","strict_gate","strict_rvol_gate"):
        assert s.select(raw,variant)[0].empty


def test_filter_precedes_ranking_and_admits_replacement():
    raw = pd.DataFrame([row("LEADER",1,adv_acceleration=.20), row("NEXT",2,traded_value=900.)])
    assert s.select(raw)[0].tradingsymbol.tolist() == ["LEADER"]
    orders,audit = s.select(raw,"strict_gate")
    assert orders.tradingsymbol.tolist() == ["NEXT"]
    rejected = audit.set_index("tradingsymbol").loc["LEADER"]
    assert rejected.strict_original_selected and rejected.strict_native_filter_pass
    assert not rejected.strict_gate_pass and not rejected.v9_selected


@pytest.mark.parametrize("field,value", [("adv_ready",False), ("adv_recent15_pressure",np.nan),
    ("adv_recent15_pressure",1.01), ("adv_acceleration",np.inf), ("adv_acceleration",-2.01),
    ("adv_feature_ts",pd.Timestamp("2026-10-23 09:55",tz="Asia/Kolkata"))])
def test_missing_invalid_or_future_features_fail_all_gated_arms(field,value):
    raw = pd.DataFrame([row(**{field:value})])
    assert len(s.select(raw)[0]) == 1
    for variant in s.VARIANTS[1:]:
        assert s.select(raw,variant)[0].empty


def test_early_bar_fails_even_if_ready_flag_is_forged():
    item = row()
    item.update(base_row("EARLY",1,clock="09:30",acceleration=-.20))
    item["adv_recent15_pressure"] = -.30
    raw = pd.DataFrame([item])
    assert len(s.select(raw)[0]) == 1
    for variant in s.VARIANTS[1:]:
        assert not s.gate(raw,variant)[0].any()
        assert s.select(raw,variant)[0].empty


def test_alignment_zero_is_rejected_but_ready_control_retains_it():
    raw = pd.DataFrame([row(adv_acceleration=0.)])
    assert len(s.select(raw,"cvd_ready_control")[0]) == 1
    assert s.select(raw,"aligned_gate")[0].empty


def test_rvol_history_and_threshold_use_separate_matched_control():
    raw = pd.DataFrame([row(adv_rvol_ready=False)])
    assert len(s.select(raw,"strict_gate")[0]) == 1
    assert s.select(raw,"rvol_ready_control")[0].empty
    assert s.select(raw,"strict_rvol_gate")[0].empty
    raw["adv_rvol_ready"] = True
    raw["adv_rvol20"] = 1.19
    assert len(s.select(raw,"rvol_ready_control")[0]) == 1
    assert s.select(raw,"strict_rvol_gate")[0].empty


def test_core_first_preserved_among_gate_survivors():
    raw = pd.DataFrame([row("CORE",1,traded_value=900.), row("RELAXED",2,price_change_pct=-.15)])
    assert s.select(raw,"strict_gate")[0].tradingsymbol.tolist() == ["CORE"]
    raw.loc[0,"adv_acceleration"] = .10
    assert s.select(raw,"strict_gate")[0].tradingsymbol.tolist() == ["RELAXED"]


def test_one_missing_candidate_does_not_neutralize_ready_competitor():
    raw = pd.DataFrame([row("MISSING",1,adv_ready=False),row("READY",2,traded_value=900.)])
    assert s.select(raw,"strict_gate")[0].tradingsymbol.tolist() == ["READY"]


def test_empty_pool_and_duplicate_identity_protection():
    raw = pd.DataFrame([row()])
    for variant in s.VARIANTS:
        assert s.select(raw.iloc[:0],variant)[0].empty
    with pytest.raises(ValueError):
        s.select(pd.concat([raw,raw],ignore_index=True),"strict_gate")
