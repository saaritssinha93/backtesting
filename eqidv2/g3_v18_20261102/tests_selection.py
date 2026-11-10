"""Causality, replacement, simplification and exact control checks."""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from g3_v18_20261102 import selection as s


def candidate(symbol="EXAMPLE", clock="09:30", sid=1, **changes):
    signal = pd.Timestamp("2026-10-23 " + clock, tz="Asia/Kolkata")
    confirm = signal + pd.Timedelta(minutes=1)
    row = dict(sid=sid, tradingsymbol=symbol, signal_ts=signal, confirmation_ts=confirm,
        day=signal.date(), side="SHORT", hhmm_int=int(clock.replace(":", "")),
        signal_close=100., ema9=99., ema20=100., ema50=101.,
        v9_5m_ema_bull=False, v9_5m_ema_bear=True,
        price_change_pct=-.6, oi_change_pct=.6, oi=1006., prev_oi=1000., volume_ratio=2.,
        confirmation_open=100., confirmation_high=100.1, confirmation_low=99., confirmation_close=99.2,
        body_ratio=.7, v9_1m_upper_wick_ratio=.1, v9_1m_lower_wick_ratio=.2,
        traded_value=1_000_000., v9_1m_volume_ratio=2.,
        v9_1m_feature_ts=confirm, v9_5m_feature_ts=signal, v9_feature_available_ts=confirm,
        v9_exact_confirmation_present=True, source_1m_count=5, nifty_first_bar_return_pct=-.05,
        opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M",
        v18_stock_ready=True, v18_market_ready=True, v18_breadth_coverage_pct=100.,
        v18_breadth_above_open_pct=30., v18_breadth_below_open_pct=70.,
        v18_relative_strength_pct=-.2, v18_vwap_distance_pct=-.2,
        v18_vwap_slope_pct=-.1, v18_pressure_6=-.3, v18_feature_ts=signal)
    row.update(changes)
    return row


def test_exact_control_parity_and_no_mutation():
    frame = pd.DataFrame([candidate("A", sid=12), candidate("B", sid=15, price_change_pct=-.5)])
    before = frame.copy(deep=True)
    digest = s.production.strategy_fingerprint()
    expected = s.control.selection_audit(s.strict_signals(frame))
    orders, audit = s.select(frame, "control")
    pd.testing.assert_frame_equal(audit, expected, check_exact=True)
    pd.testing.assert_frame_equal(orders, expected.loc[expected.v9_selected].reset_index(drop=True), check_exact=True)
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    assert s.production.strategy_fingerprint() == digest


def test_new_gate_precedes_ranking_and_replaces_rejected_leader():
    frame = pd.DataFrame([candidate("LEADER", sid=1, price_change_pct=-.8,
                                   v18_relative_strength_pct=.1),
                          candidate("REPLACEMENT", sid=2, price_change_pct=-.6)])
    assert s.select(frame, "control")[0].tradingsymbol.tolist() == ["LEADER"]
    orders, audit = s.select(frame, "strength_only")
    assert orders.tradingsymbol.tolist() == ["REPLACEMENT"]
    assert orders.sid.tolist() == [2]
    assert audit.set_index("tradingsymbol").loc["LEADER", "v9_decision"] == "ADVANCED_FILTER_REJECTED"


@pytest.mark.parametrize("variant,field", [
    ("market_only", "v18_breadth_below_open_pct"),
    ("strength_only", "v18_relative_strength_pct"),
    ("vwap_pressure", "v18_vwap_slope_pct"),
    ("advanced", "v18_pressure_6"),
])
def test_missing_required_features_fail_closed(variant, field):
    frame = pd.DataFrame([candidate(**{field: np.nan})])
    assert s.select(frame, variant)[0].empty
    assert len(s.select(frame, "control")[0]) == 1


def test_breadth_does_not_count_flat_stocks_as_decliners():
    assert not s.advanced_pass(candidate(v18_breadth_above_open_pct=0.,
        v18_breadth_below_open_pct=10.), "market_only")
    assert not s.advanced_pass(candidate(v18_breadth_coverage_pct=89.99), "market_only")
    assert s.advanced_pass(candidate(v18_breadth_coverage_pct=90.), "market_only")


def test_future_feature_timestamp_rejected():
    row = candidate()
    row["v18_feature_ts"] += pd.Timedelta(minutes=5)
    assert not s.advanced_pass(row, "advanced")


def test_simplification_admits_full_raw_candidates_removed_by_ema_and_body_wick():
    frame = pd.DataFrame([candidate(ema9=102., ema20=101., ema50=100.,
        v9_5m_ema_bull=True, v9_5m_ema_bear=False, body_ratio=.01, v9_1m_lower_wick_ratio=.9)])
    assert s.select(frame, "control")[0].empty
    orders, _ = s.select(frame, "simplified")
    assert orders.tradingsymbol.tolist() == ["EXAMPLE"]
    assert orders.sid.tolist() == [1]
    assert len(s.select(frame, "simplified_advanced")[0]) == 1


@pytest.mark.parametrize("changes", [
    {"oi": 1000.}, {"oi_change_pct": .01}, {"volume_ratio": .1},
    {"v9_1m_volume_ratio": .1}, {"confirmation_close": 100.1},
    {"source_1m_count": 4}, {"confirmation_source_flagged": True},
    {"v9_1m_feature_ts": pd.Timestamp("2026-10-23 09:32", tz="Asia/Kolkata")},
])
def test_simplification_preserves_oi_volume_confirmation_and_integrity(changes):
    assert s.select(pd.DataFrame([candidate(**changes)]), "simplified")[0].empty


@pytest.mark.parametrize("variant", s.VARIANTS)
def test_core_first_and_existing_two_entry_quota(variant):
    frame = pd.DataFrame([
        candidate("CORE_A", "09:35", 10, price_change_pct=-.6, traded_value=100.),
        candidate("CORE_B", "09:35", 11, price_change_pct=-.55, traded_value=90.),
        candidate("RELAXED", "09:35", 12, price_change_pct=-.4, traded_value=1000.),
    ])
    orders, _ = s.select(frame, variant)
    assert set(orders.tradingsymbol) == {"CORE_A", "CORE_B"}
    assert orders.v10_g_f_core.all()
    assert len(orders) == 2


def test_simplified_removes_0926_min_wick_without_changing_quota():
    frame = pd.DataFrame([candidate(clock="09:25", v9_1m_lower_wick_ratio=.05)])
    assert s.select(frame, "control")[0].empty
    assert len(s.select(frame, "simplified")[0]) == 1


def test_simplified_retains_extension_check():
    frame = pd.DataFrame([candidate(clock="10:35", v9_1m_ema20=110., v9_1m_atr14=1.)])
    assert s.select(frame, "simplified")[0].empty


@pytest.mark.parametrize("variant", s.VARIANTS)
def test_empty_inputs_and_duplicate_sid(variant):
    frame = pd.DataFrame([candidate()])
    assert s.select(frame.iloc[:0], variant)[0].empty
    with pytest.raises(ValueError, match="globally unique"):
        s.select(pd.concat([frame, frame], ignore_index=True), variant)


def test_sealed_full_raw_pool_control_parity():
    path = Path(__file__).resolve().parents[1] / "outputs/g3_early_entry_20261023/full_history_source/cache/2026-10-23/features.parquet"
    if not path.is_file():
        pytest.skip("Local sealed full raw research pool is unavailable")
    raw = pd.read_parquet(path)
    raw["sid"] = np.arange(500_000, 500_000 + len(raw))
    expected = s.control.selection_audit(s.strict_signals(raw))
    _, audit = s.select(raw, "control")
    pd.testing.assert_frame_equal(audit, expected, check_exact=True)
