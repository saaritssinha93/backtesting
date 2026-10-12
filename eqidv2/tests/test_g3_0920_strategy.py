"""The 09:20 research setup copies G-3 rules without changing live G-3."""
from __future__ import annotations

from dataclasses import asdict, replace

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as contract
from research.g3_0920_strategy import (
    OI_SOURCE,
    reference_profile,
    selection_audit_0920,
    select_orders_0920,
    setup_specs,
)


def feature(symbol="A", *, side="LONG", price=None, volume_ratio=4., oi_change=.3,
            nifty=-.1, day="2026-10-12", **changes):
    long = side == "LONG"
    prev_oi = 100_000.
    cash_close = 100.5 if long else 99.5
    row = dict(
        session_date=day, symbol=symbol, contract=f"{symbol}26OCTFUT",
        research_feature_status="COMPLETE", cash_0920_status="VALID_COMPLETED_FIRST_BAR",
        confirmation_0921_status="VALID_REAL_MINUTE",
        futures_0920_status="VALID_COMPLETED_FUTURES_0920",
        opening_0916_status="VALID_FIRST_MINUTE_EVIDENCE",
        price_first_5m_pct=(.5 if long else -.5) if price is None else price,
        oi_change_pct=oi_change,
        oi_change_source=OI_SOURCE, oi_change_interval_minutes=4,
        oi_change_availability="RETROSPECTIVE_RESEARCH",
        futures_0920_oi=(prev_oi * (1 + oi_change / 100) if oi_change is not None else None),
        opening_0916_oi=prev_oi, cash_close=cash_close,
        ema9=101. if long else 99., ema20=100., ema50=99. if long else 101.,
        volume_ratio=volume_ratio, traded_value=1_000_000.,
        confirmation_open=cash_close,
        confirmation_high=101. if long else 99.6,
        confirmation_low=100.4 if long else 99.,
        confirmation_close=100.8 if long else 99.1,
        confirmation_body_ratio=.6, confirmation_upper_wick_ratio=.2,
        confirmation_lower_wick_ratio=.2, v9_1m_volume_ratio=1.2,
        nifty_context_status="VALID_COMPLETED_NIFTY_FIRST_BAR",
        nifty_first_bar_return_pct=nifty,
    )
    row.update(changes)
    return row


def test_setup_specs_clone_0925_core_and_active_without_mutation():
    before = contract.strategy_fingerprint()
    specs = setup_specs()
    assert set(specs) == {"LONG", "SHORT"}
    for side, (core, active) in specs.items():
        donor_core = next(s for s in contract.CORE_SETUPS if s.signal_end == "09:25" and s.side == side)
        donor_active = contract.setup_for("09:25", side)
        for new, old in ((core, donor_core), (active, donor_active)):
            expected = {**asdict(old), "signal_end": "09:20", "confirmation_end": "09:21",
                        "source_version": new.source_version}
            assert asdict(new) == expected
    assert specs["SHORT"][0].price_change_pct == .2
    assert specs["SHORT"][1].price_change_pct == .13
    assert contract.setup_for("09:20", "LONG") is None
    assert contract.setup_for("09:20", "SHORT") is None
    assert contract.strategy_fingerprint() == before


def test_explicit_reference_profile_matches_default_decisions_and_core_first():
    rows = [
        feature("LONG", traded_value=2_000_000.),
        feature("CORE", side="SHORT", price=-.4, volume_ratio=1.6),
        feature("ACTIVE", side="SHORT", price=-.15, volume_ratio=10.),
    ]
    default = selection_audit_0920(pd.DataFrame(rows))
    explicit = selection_audit_0920(pd.DataFrame(rows), profile=reference_profile())
    columns = ["signal_id", "v9_filter_pass", "v9_selected", "v10_g_f_core",
               "v9_rank_in_setup_day", "v9_decision"]
    pd.testing.assert_frame_equal(default[columns], explicit[columns])
    assert set(explicit.loc[explicit.v9_selected, "tradingsymbol"]) == {"LONG", "CORE", "ACTIVE"}
    assert not bool(explicit.set_index(["tradingsymbol", "side"]).loc[("ACTIVE", "SHORT"), "v10_g_f_core"])


def test_strict_then_five_minute_then_one_minute_relaxation():
    reference = reference_profile()
    strict = replace(reference, long=replace(
        reference.long,
        five_minute=replace(reference.long.five_minute,
                            active_price_pct=.7, core_price_pct=.7,
                            oi_change_pct=.35, volume_ratio=4.5),
        one_minute=replace(reference.long.one_minute,
                           body_ratio=.7, max_wick_ratio=.15,
                           confirmation_volume_ratio=1.3),
    ))
    five_minute_relaxed = replace(strict, long=replace(
        strict.long, five_minute=reference.long.five_minute))
    fully_relaxed = replace(five_minute_relaxed, long=replace(
        five_minute_relaxed.long, one_minute=reference.long.one_minute))
    rows = pd.DataFrame([
        feature("A", traded_value=2_000_000.),
        feature("B", price=.8, volume_ratio=5., oi_change=.4,
                traded_value=1_000_000., confirmation_body_ratio=.8,
                confirmation_upper_wick_ratio=.1, v9_1m_volume_ratio=1.4),
    ])
    strict_audit = selection_audit_0920(rows, profile=strict)
    first_relaxed_audit = selection_audit_0920(rows, profile=five_minute_relaxed)
    fully_relaxed_audit = selection_audit_0920(rows, profile=fully_relaxed)
    for audit in (strict_audit, first_relaxed_audit):
        assert audit.loc[audit.v9_selected, "tradingsymbol"].tolist() == ["B"]
        assert not bool(audit.set_index(["tradingsymbol", "side"]).loc[("A", "LONG"), "v9_filter_pass"])
    assert bool(fully_relaxed_audit.set_index(["tradingsymbol", "side"]).loc[("A", "LONG"), "v9_filter_pass"])
    assert fully_relaxed_audit.loc[fully_relaxed_audit.v9_selected, "tradingsymbol"].tolist() == ["A"]


def test_profile_cannot_weaken_contractual_bases_or_invert_core_price():
    reference = reference_profile()
    bad_volume = replace(reference, short=replace(
        reference.short, one_minute=replace(
            reference.short.one_minute, confirmation_volume_ratio=1.1)))
    bad_core = replace(reference, short=replace(
        reference.short, five_minute=replace(
            reference.short.five_minute, active_price_pct=.3, core_price_pct=.2)))
    for profile in (bad_volume, bad_core):
        with pytest.raises(ValueError):
            selection_audit_0920(pd.DataFrame([feature()]), profile=profile)


def test_extra_confirmation_volume_threshold_applies_after_native_filter():
    reference = reference_profile()
    strict = replace(reference, long=replace(
        reference.long, one_minute=replace(
            reference.long.one_minute, confirmation_volume_ratio=1.3)))
    rows = pd.DataFrame([
        feature("NATIVE_PASS", v9_1m_volume_ratio=1.2),
        feature("STRICT_PASS", v9_1m_volume_ratio=1.4),
    ])
    native = selection_audit_0920(rows)
    adjusted = selection_audit_0920(rows, profile=strict)
    assert bool(native.set_index(["tradingsymbol", "side"]).loc[("NATIVE_PASS", "LONG"), "v9_filter_pass"])
    assert not bool(adjusted.set_index(["tradingsymbol", "side"]).loc[("NATIVE_PASS", "LONG"), "v9_filter_pass"])
    assert bool(adjusted.set_index(["tradingsymbol", "side"]).loc[("STRICT_PASS", "LONG"), "v9_filter_pass"])


def test_selected_long_and_short_have_exact_confirmation_and_simulator_fields():
    audit = selection_audit_0920(pd.DataFrame([feature("LONG_A"), feature("SHORT_A", side="SHORT")]))
    orders = audit.loc[audit.v9_selected]
    assert set(orders.setup_id) == {"0921_LONG", "0921_SHORT"}
    assert orders.sid.is_unique and orders.signal_id.is_unique
    assert orders.signal_ts.dt.strftime("%H:%M").eq("09:20").all()
    assert orders.confirmation_ts.dt.strftime("%H:%M").eq("09:21").all()
    assert orders.v9_1m_feature_ts.dt.strftime("%H:%M").eq("09:21").all()
    assert (orders.confirmation_ts > orders.signal_ts).all()
    assert set(("trigger", "native_stop_pct", "native_target_pct", "initial_stop_pct",
                "tightened_stop_pct", "tighten_after_minutes", "oi_change_pct", "prev_oi")) <= set(orders)
    assert orders.native_stop_pct.eq(1.25).all()
    assert orders.loc[orders.side.eq("LONG"), "native_target_pct"].iloc[0] == .97
    assert orders.loc[orders.side.eq("SHORT"), "native_target_pct"].iloc[0] == 2.67
    assert orders.execution_authority.eq(False).all()
    assert orders.backtest_evidence.eq("RETROSPECTIVE_RESEARCH").all()
    assert orders.oi_change_interval_minutes.eq(4).all()


def test_core_first_short_picker_and_quota():
    rows = [
        feature("CORE_A", side="SHORT", price=-.4, volume_ratio=1.6),
        feature("CORE_B", side="SHORT", price=-.4, volume_ratio=1.7),
        feature("RELAXED", side="SHORT", price=-.15, volume_ratio=10.),
    ]
    selected = select_orders_0920(pd.DataFrame(rows))
    assert set(selected.tradingsymbol) == {"CORE_A", "CORE_B"}
    assert selected.v10_g_f_core.all()
    # Once the core quota has a vacancy, the relaxed .13% active threshold fills it.
    selected = select_orders_0920(pd.DataFrame([rows[0], rows[2]]))
    assert set(selected.tradingsymbol) == {"CORE_A", "RELAXED"}
    assert selected.set_index("tradingsymbol").loc["RELAXED", "v10_g_f_core"] == False


def test_long_quota_and_liquidity_ranking():
    rows = [feature("LOW", traded_value=1_000_000.),
            feature("HIGH", traded_value=2_000_000.)]
    selected = select_orders_0920(pd.DataFrame(rows))
    assert len(selected) == 1
    assert selected.iloc[0].tradingsymbol == "HIGH"


def test_short_nifty_gate_and_confirmation_direction():
    bad_nifty = feature("NIFTY_FAIL", side="SHORT", nifty=0.)
    bad_confirmation = feature("CONFIRM_FAIL", side="SHORT", confirmation_close=99.55)
    good = feature("VALID", side="SHORT")
    audit = selection_audit_0920(pd.DataFrame([bad_nifty, bad_confirmation, good]))
    chosen = audit.loc[audit.v9_selected]
    assert chosen.tradingsymbol.tolist() == ["VALID"]
    assert "NIFTY_FIRST_BAR_SHORT_GATE" in audit.loc[
        audit.tradingsymbol.eq("NIFTY_FAIL") & audit.side.eq("SHORT"), "source_reasons"].iloc[0]


@pytest.mark.parametrize("changes", [
    {"oi_change_pct": None},
    {"opening_0916_oi": 0.},
    {"oi_change_interval_minutes": 5},
    {"oi_change_source": "PROVISIONAL_FULL_QUOTE"},
    {"research_feature_status": "PARTIAL"},
    {"cash_0920_status": "MISSING_OR_FLAGGED_CASH_FIRST_BAR"},
    {"confirmation_0921_status": "MISSING_OR_FLAGGED_0921_MINUTE"},
])
def test_invalid_opening_sources_fail_closed(changes):
    selected = select_orders_0920(pd.DataFrame([feature(**changes)]))
    assert selected.empty


def test_reject_duplicate_dated_mapping_and_missing_schema():
    row = feature()
    with pytest.raises(ValueError, match="duplicate"):
        selection_audit_0920(pd.DataFrame([row, row]))
    with pytest.raises(ValueError, match="required fields"):
        selection_audit_0920(pd.DataFrame([{"session_date": "2026-10-12"}]))
