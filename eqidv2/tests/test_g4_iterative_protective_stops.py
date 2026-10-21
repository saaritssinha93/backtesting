"""Protective-stop catalog and wrapper retain shared simulator causality."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_protective_stops as rules
from research.g3_sl_target_simulator import simulate_exit


def _path(rows, start="2026-10-15 15:10", short=False):
    values=np.asarray(rows,dtype=float)
    if short:
        values=200-values
        values[:,[1,2]]=values[:,[2,1]]
    stamps=pd.date_range(start,periods=len(rows),freq="min",tz="Asia/Kolkata")
    return dict(timestamp_ns=stamps.asi8,**{key:values[:,index] for index,key in enumerate(("open","high","low","close"))})


def _row(short=False):
    return dict(filled=True,side="SHORT" if short else "LONG",entry_path_index=0,entry_price=100.,native_target_pct=1.5)


def _policy(family,**params):
    return next(p for p in rules.catalog() if p["family"]==family and all(p["params"][key]==value for key,value in params.items()))


def test_predeclared_catalog_count_and_strict_activation_margin():
    policies=rules.catalog()
    assert len(policies)==105
    assert len({p["id"] for p in policies})==105
    assert sum(p["family"]=="profit_floor" for p in policies)==20
    assert sum(p["family"]=="time_clamp" for p in policies)==12
    assert sum(p["family"]=="profit_floor_time_clamp" for p in policies)==72
    assert all(p["rationale"] for p in policies)
    for policy in policies:
        p=policy["params"]
        if p["activation_floor_pct"] is not None:
            assert p["protected_profit_pct"]<p["activation_floor_pct"]
        assert p["tightened_stop_pct"]<=1.0
    policies[1]["params"]["protected_profit_pct"]=-99
    assert rules.catalog()[1]["params"]["protected_profit_pct"]>=0


@pytest.mark.parametrize("short",[False,True])
def test_baseline_is_shared_native_simulator(short):
    path=_path([[100,100.1,99.9,100],[100,102,99.9,101]],short=short)
    baseline=rules.catalog()[0]
    actual=rules.outcomes_for_trade(_row(short),path,[baseline])["baseline"]
    expected=simulate_exit(path,0,100.,not short,1.2,1.5,tighten_after_minutes=120,tightened_stop_pct=1.)
    assert actual==expected


@pytest.mark.parametrize("short",[False,True])
def test_only_completed_post_entry_close_arms_next_bar_stop(short):
    path=_path([[100,100.1,99.9,100],[100,100.8,99.7,100.6],
                [100.5,100.6,100.05,100.2]],short=short)
    policy=_policy("profit_floor",activation_floor_pct=.2,target_fraction=.25,protected_profit_pct=.1)
    outcome=rules.outcomes_for_trade(_row(short),path,[policy])[policy["id"]]
    assert outcome["protection_armed_path_index"]==1
    assert outcome["protection_effective_path_index"]==2
    assert outcome["exit_path_index"]==2
    assert outcome["exit_reason"]=="PROTECTED_STOP"
    assert outcome["exit_price"]==pytest.approx(99.9 if short else 100.1)


def test_entry_candle_and_intrabar_peak_cannot_arm_profit_floor():
    path=_path([[100,100.9,99.9,100.8],[100.1,100.9,99.8,100.1]]+
               [[100.1,100.2,99.8,100.1]]*4)
    policy=_policy("profit_floor",activation_floor_pct=.2,target_fraction=.25,protected_profit_pct=.1)
    outcome=rules.outcomes_for_trade(_row(),path,[policy])[policy["id"]]
    assert outcome["exit_reason"]=="TIME_EXIT_1515"
    assert outcome["protection_armed_path_index"] is None


@pytest.mark.parametrize("short",[False,True])
def test_gap_through_profit_stop_uses_observed_open(short):
    path=_path([[100,100.1,99.9,100],[100,100.8,99.9,100.6],
                [99.8,103,95,100.2]],short=short)
    policy=_policy("profit_floor",activation_floor_pct=.2,target_fraction=.25,protected_profit_pct=.1)
    outcome=rules.outcomes_for_trade(_row(short),path,[policy])[policy["id"]]
    assert outcome["exit_event"]=="OPEN"
    assert outcome["exit_gap_through"]
    assert outcome["exit_price"]==pytest.approx(100.2 if short else 99.8)
    assert outcome["mfe_pct"]==pytest.approx(.8)


def test_native_target_precedes_close_arming_and_ties_remain_stop_first():
    policy=_policy("profit_floor",activation_floor_pct=.2,target_fraction=.25,protected_profit_pct=.1)
    path=_path([[100,100.1,99.9,100],[100,101.7,99.9,100.8]])
    outcome=rules.outcomes_for_trade(_row(),path,[policy])[policy["id"]]
    assert outcome["exit_reason"]=="TARGET"
    assert outcome["protection_armed_path_index"] is None
    path=_path([[100,100.1,99.9,100],[100,100.8,99.9,100.6],[100.5,102,99.9,101]])
    outcome=rules.outcomes_for_trade(_row(),path,[policy])[policy["id"]]
    assert outcome["exit_reason"]=="PROTECTED_STOP"
    assert outcome["same_bar_ambiguous"]


def test_earlier_time_clamp_waits_for_eligible_open():
    rows=[[100,100.1,99.9,100] for _ in range(18)]
    rows[15]=[100,100.1,99.3,99.8]  # Opens14min after entry; original stop remains98.8.
    rows[16]=[99.8,99.9,99.3,99.5]  # Opens15min after entry; new stop99.4.
    path=_path(rows,start="2026-10-15 14:58")
    policy=_policy("time_clamp",tighten_after_minutes=15,tightened_stop_pct=.6)
    outcome=rules.outcomes_for_trade(_row(),path,[policy])[policy["id"]]
    assert outcome["exit_path_index"]==16
    assert outcome["exit_reason"]=="TIGHTENED_STOP"
    assert outcome["exit_price"]==pytest.approx(99.4)


def test_combination_never_loosens_profit_protection_and_future_cannot_change_exit():
    rows=[[100,100.1,99.9,100]]*18
    rows[1]=[100,101,99.9,100.9]
    rows[2]=[100.9,101,100.3,100.5]
    rows[3]=[100.5,100.7,100.05,100.2]
    path=_path(rows,start="2026-10-15 14:58")
    policy=_policy("profit_floor_time_clamp",target_fraction=.5,protected_profit_pct=.1,tighten_after_minutes=15,tightened_stop_pct=.6)
    before=rules.outcomes_for_trade(_row(),path,[policy])[policy["id"]]
    assert before["exit_reason"]=="PROTECTED_STOP"
    assert before["active_stop_pct_at_exit"]==-.1
    changed={key:value.copy() for key,value in path.items()}
    for key in ("open","high","low","close"):
        changed[key][4:]*=10
    after=rules.outcomes_for_trade(_row(),changed,[policy])[policy["id"]]
    assert before==after


def test_unfilled_rows_return_no_outcomes():
    assert rules.outcomes_for_trade(dict(filled=False),{})=={}
