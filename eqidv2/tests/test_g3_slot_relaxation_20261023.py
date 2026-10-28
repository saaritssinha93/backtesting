"""Boundaries and real sealed-source parity for isolated slot relaxations."""
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research import g3_slot_relaxation_20261023 as research


CACHE = Path(__file__).resolve().parents[1] / "outputs/g3_early_entry_20261023/universe/cache"
DAYS = ("2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16",
        "2026-10-19", "2026-10-21", "2026-10-22", "2026-10-23")


def candidate(clock="09:30", symbol="EXAMPLE", **changes):
    signal = pd.Timestamp("2026-10-23 " + clock, tz="Asia/Kolkata")
    confirmation = signal + pd.Timedelta(minutes=1)
    row = dict(tradingsymbol=symbol, signal_ts=signal, confirmation_ts=confirmation,
        day=signal.date(), side="SHORT", signal_close=100., ema9=99., ema20=100., ema50=101.,
        price_change_pct=-.6, oi_change_pct=.6, oi=1006., prev_oi=1000., volume_ratio=2.,
        confirmation_open=100., confirmation_high=100.1, confirmation_low=99., confirmation_close=99.2,
        body_ratio=.7, wick_ratio=.1, traded_value=1_000_000., v9_1m_volume_ratio=2.,
        v9_1m_feature_ts=confirmation, v9_5m_feature_ts=signal,
        v9_feature_available_ts=confirmation, v9_exact_confirmation_present=True,
        source_1m_count=5, nifty_first_bar_return_pct=-.05,
        opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M")
    row.update(changes)
    return row


def selected(row, profile):
    return len(research.select_orders(pd.DataFrame([row]), profile)) == 1


@pytest.mark.parametrize("day", DAYS)
def test_baseline_exact_complete_output_parity_for_all_eight_cached_days(day):
    path = CACHE / day / "features.parquet"
    if not path.exists():
        pytest.skip("Local sealed research cache is unavailable")
    features = pd.read_parquet(path)
    before = research.production.strategy_fingerprint()
    expected = research.contract.select_orders(features)
    actual = research.select_orders(features, "baseline")
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    assert research.production.strategy_fingerprint() == before


def test_oi_floor_supports_decline_but_rejects_outside_boundary_and_invalid_data():
    row = candidate(oi_change_pct=-3., oi=970.)
    assert not selected(row, "baseline")
    assert selected(row, "targeted_OI")
    assert not selected({**row, "oi_change_pct": np.nextafter(-3., -np.inf)}, "targeted_OI")
    for key, value in (("oi", 0.), ("prev_oi", 0.), ("oi_change_pct", np.nan), ("oi_change_pct", 1.01)):
        assert not selected({**row, key: value}, "targeted_OI")


def test_unchanged_opening_oi_is_permitted_only_in_targeted_slot():
    row = candidate("09:20", oi_change_pct=0., oi=1000.)
    assert selected(row, "targeted_OI")
    assert not selected(row, "baseline")
    assert not selected(candidate("09:25", oi_change_pct=0., oi=1000.), "targeted_OI")


def test_volume_floor_changes_base_setup_and_confirmation_only_for_targeted_slot():
    row = candidate("09:35", volume_ratio=.4, v9_1m_volume_ratio=.5)
    assert selected(row, "targeted_volume")
    assert not selected(row, "baseline")
    assert not selected({**row, "volume_ratio": np.nextafter(.4, 0.)}, "targeted_volume")
    assert not selected({**row, "v9_1m_volume_ratio": np.nextafter(.5, 0.)}, "targeted_volume")
    assert not selected(candidate("09:30", volume_ratio=.4), "targeted_volume")


def test_combined_opening_nifty_body_boundaries_and_source_checks():
    row = candidate("09:20", oi_change_pct=0., oi=1000., body_ratio=.3,
                    v9_1m_volume_ratio=.6, nifty_first_bar_return_pct=.05)
    assert selected(row, "combined_morning")
    assert not selected(row, "targeted_OI")
    assert not selected(row, "targeted_volume")
    for key, value in (("nifty_first_bar_return_pct", np.nextafter(.05, np.inf)),
                       ("body_ratio", np.nextafter(.3, 0.)), ("opening_0920_status", "INVALID"),
                       ("confirmation_source_flagged", True),
                       ("v9_1m_feature_ts", row["confirmation_ts"] + pd.Timedelta(minutes=1)),
                       ("confirmation_close", 100.05)):
        assert not selected({**row, key: value}, "combined_morning")


def test_core_first_priority_retained_even_when_extra_has_largest_liquidity():
    rows = [candidate("09:35", "CORE_A", price_change_pct=-.6, traded_value=100.),
            candidate("09:35", "CORE_B", price_change_pct=-.55, traded_value=90.),
            candidate("09:35", "EXTRA", price_change_pct=-.4, traded_value=1000.)]
    for profile in research.PROFILES:
        orders = research.select_orders(pd.DataFrame(rows), profile)
        assert set(orders.tradingsymbol) == {"CORE_A", "CORE_B"}
        assert orders.v10_g_f_core.all()


def test_second_0931_place_is_only_enabled_by_explicit_quota_variant():
    rows = [candidate(symbol="FIRST", price_change_pct=-.6, oi_change_pct=-1., oi=990., v9_1m_volume_ratio=1.),
            candidate(symbol="SECOND", price_change_pct=-.5, oi_change_pct=-1., oi=990., v9_1m_volume_ratio=1.)]
    ordinary = research.select_orders(pd.DataFrame(rows), "combined_morning")
    expanded = research.select_orders(pd.DataFrame(rows), "combined_morning_two_0931")
    assert ordinary.tradingsymbol.tolist() == ["FIRST"]
    assert expanded.tradingsymbol.tolist() == ["FIRST", "SECOND"]
    assert expanded.native_target_pct.eq(2.).all()


def test_profiles_leave_slots_targets_picker_and_contract_globals_unchanged():
    before = research.production.strategy_fingerprint()
    originals = {s.setup_id: asdict(s) for s in research.contract.ACTIVE_SETUPS}
    for name in research.PROFILES:
        profile = research.make_profile(name)
        assert {s.setup_id for s in profile.active_setups} == set(originals)
        for s in profile.active_setups:
            original = originals[s.setup_id]
            for key in ("signal_end", "confirmation_end", "side", "target_pct", "stop_pct", "picker", "price_change_pct"):
                assert getattr(s, key) == original[key]
            expected_quota = 2 if name == "combined_morning_two_0931" and s.setup_id == "0931_SHORT" else original["max_entries"]
            assert s.max_entries == expected_quota
        absent_long = candidate("11:15", side="LONG", price_change_pct=.6,
            ema9=101., ema20=100., ema50=99., confirmation_open=100.,
            confirmation_high=101.1, confirmation_low=99.9, confirmation_close=101.)
        assert not selected(absent_long, name)
    assert research.production.strategy_fingerprint() == before


def test_research_identity_cannot_be_mistaken_for_production_baseline():
    orders = research.select_orders(pd.DataFrame([candidate()]), "targeted_OI")
    assert orders.strategy_version.str.startswith("G3-SLOT-RELAXATION-").all()
    assert orders.execution_authority.eq(False).all()
    with pytest.raises(ValueError, match="Unknown slot-relaxation"):
        research.make_profile("live")
