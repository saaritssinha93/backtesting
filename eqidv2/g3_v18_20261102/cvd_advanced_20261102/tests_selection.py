"""Bounded ranking, neutral fallback and native eligibility preservation."""
import numpy as np
import pandas as pd
import pytest

from g3_v18_20261102.tests_selection import candidate
from . import selection as s


def row(symbol="A", sid=1, clock="09:50", side="SHORT", liquidity=1000., acceleration=0., **updates):
    values = dict(traded_value=liquidity, adv_acceleration=acceleration, adv_ready=True,
                  adv_rvol_ready=True, adv_rvol20=2.)
    if side == "LONG":
        values.update(side="LONG", price_change_pct=.6, ema9=101., ema20=100., ema50=99.,
                      v9_5m_ema_bull=True, v9_5m_ema_bear=False, confirmation_open=100., confirmation_close=100.8,
                      confirmation_high=101., confirmation_low=99.9)
    values.update(updates)
    out = candidate(symbol, clock, sid, **values)
    out["adv_feature_ts"] = out["signal_ts"]
    return out


def test_exact_native_control_and_no_mutation():
    raw = pd.DataFrame([row(), row("B", 2, liquidity=950., acceleration=-2.)])
    before = raw.copy(deep=True)
    fingerprint = s.native.production.strategy_fingerprint()
    expected = s.native.select(raw, "control")
    actual = s.select_all(raw)
    for a, b in zip(actual["base_control"], expected):
        pd.testing.assert_frame_equal(a, b)
    pd.testing.assert_frame_equal(raw, before)
    assert s.native.production.strategy_fingerprint() == fingerprint


@pytest.mark.parametrize("side,sign", [("LONG", 1.), ("SHORT", -1.)])
def test_near_candidate_substitution_both_directions(side, sign):
    raw = pd.DataFrame([row("LEADER", 1, side=side, acceleration=-2*sign),
                        row("NEAR", 2, side=side, liquidity=950., acceleration=2*sign)])
    results = s.select_all(raw)
    assert results["base_control"][0].tradingsymbol.tolist() == ["LEADER"]
    for variant in s.VARIANTS[1:]:
        orders, audit = results[variant]
        assert orders.tradingsymbol.tolist() == ["NEAR"]
        assert audit.adv_rank_selected_changed.sum() == 2
        assert audit.adv_rank_adjustment.abs().max() <= .10
        assert audit.set_index("tradingsymbol").loc["LEADER", "traded_value"] == 1000.


def test_far_candidate_cannot_leap_with_maximum_adjustment():
    raw = pd.DataFrame([row("LEADER", 1, acceleration=2.), row("FAR", 2, liquidity=790., acceleration=-2.)])
    for orders, _ in s.select_all(raw).values():
        assert orders.tradingsymbol.tolist() == ["LEADER"]


@pytest.mark.parametrize("field,value", [("adv_ready", False), ("adv_acceleration", np.nan),
                                        ("adv_acceleration", 2.1), ("adv_feature_ts", pd.Timestamp("2026-10-23 09:55", tz="Asia/Kolkata"))])
def test_any_unavailable_competitor_makes_whole_group_neutral(field, value):
    raw = pd.DataFrame([row("LEADER", 1, acceleration=2.), row("NEAR", 2, liquidity=950., acceleration=-2.)])
    raw.loc[0, field] = value
    for variant in s.VARIANTS[1:]:
        orders, audit = s.select(raw, variant)
        assert orders.tradingsymbol.tolist() == ["LEADER"]
        assert audit.adv_rank_reason.eq("WHOLE_GROUP_FEATURE_UNAVAILABLE").all()
        assert audit.adv_rank_adjustment.eq(0).all()


def test_rvol_warmup_and_low_activity_are_neutral_without_affecting_plain_arm():
    raw = pd.DataFrame([row("LEADER", 1, acceleration=2.), row("NEAR", 2, liquidity=950., acceleration=-2.)])
    raw["adv_rvol_ready"] = False
    results = s.select_all(raw)
    assert results["acceleration_rank"][0].tradingsymbol.tolist() == ["NEAR"]
    assert results["acceleration_rvol_rank"][0].tradingsymbol.tolist() == ["LEADER"]
    raw["adv_rvol_ready"] = True
    raw["adv_rvol20"] = .5
    assert s.select(raw, "acceleration_rvol_rank")[0].tradingsymbol.tolist() == ["LEADER"]


def test_zero_adjustment_ties_preserve_native_picker_liquidity_symbol_order():
    raw = pd.DataFrame([row("Z", 1), row("A", 2)])
    for orders, _ in s.select_all(raw).values():
        assert orders.tradingsymbol.tolist() == ["A"]


def test_core_priority_beats_high_scoring_relaxed_candidate():
    raw = pd.DataFrame([row("CORE", 1, liquidity=950., acceleration=2.),
                        row("RELAXED", 2, liquidity=1000., acceleration=-2., price_change_pct=-.15)])
    for orders, audit in s.select_all(raw).values():
        assert orders.tradingsymbol.tolist() == ["CORE"]
        assert bool(orders.v10_g_f_core.iloc[0])


def test_early_two_entry_quota_keeps_native_core_and_extras_behavior():
    raw = pd.DataFrame([row("CORE_A", 1, clock="09:35", price_change_pct=-.6, liquidity=100.),
                        row("CORE_B", 2, clock="09:35", price_change_pct=-.55, liquidity=90.),
                        row("RELAXED", 3, clock="09:35", price_change_pct=-.4, liquidity=1000., acceleration=-2.)])
    for variant, (orders, audit) in s.select_all(raw).items():
        assert set(orders.tradingsymbol) == {"CORE_A", "CORE_B"}
        if variant != "base_control":
            assert audit.adv_rank_reason.eq("EARLY_SESSION_OR_WARMUP").all()


def test_filter_rejected_candidate_cannot_enter_or_block_available_group():
    raw = pd.DataFrame([row("LEADER", 1, acceleration=2.), row("NEAR", 2, liquidity=950., acceleration=-2.),
                        row("BAD", 3, liquidity=5000., body_ratio=.01, adv_ready=False)])
    orders, audit = s.select(raw, "acceleration_rank")
    assert orders.tradingsymbol.tolist() == ["NEAR"]
    rejected = audit.loc[audit.tradingsymbol.eq("BAD")]
    assert not rejected.v9_selected.any()


def test_single_candidate_and_empty_inputs_keep_native_selection():
    raw = pd.DataFrame([row(adv_ready=False)])
    for variant in s.VARIANTS:
        assert len(s.select(raw, variant)[0]) == 1
        assert s.select(raw.iloc[:0], variant)[0].empty


def test_nonpositive_picker_maximum_keeps_native_tie_order():
    raw = pd.DataFrame([row("A", 1, liquidity=0., acceleration=2.), row("B", 2, liquidity=0., acceleration=-2.)])
    for variant in s.VARIANTS[1:]:
        orders, audit = s.select(raw, variant)
        assert orders.tradingsymbol.tolist() == ["A"]
        assert audit.adv_rank_reason.eq("INVALID_PICKER_MAXIMUM").all()


def test_extra_quota_is_filled_when_no_core_candidate_qualifies():
    raw = pd.DataFrame([row("LEADER", 1, acceleration=2., price_change_pct=-.15),
                        row("NEAR", 2, liquidity=950., acceleration=-2., price_change_pct=-.15)])
    orders, _ = s.select(raw, "acceleration_rank")
    assert orders.tradingsymbol.tolist() == ["NEAR"]
    assert not orders.v10_g_f_core.any()
