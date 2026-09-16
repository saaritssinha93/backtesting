"""Two-bar source coverage, causal windows and stable execution identities."""
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_two_bar as t


def pool_row(hhmm, close, *, day="2026-09-01", symbol="STOCK", contract="STOCK26SEPFUT", **changes):
    hour, minute = divmod(hhmm, 100)
    stamp = pd.Timestamp(f"{day} {hour:02d}:{minute:02d}", tz="Asia/Kolkata")
    row = dict(tradingsymbol=symbol, futures_tradingsymbol=contract, contract_month="202609",
        day=pd.Timestamp(day).date(), signal_ts=stamp, v9_5m_feature_ts=stamp,
        confirmation_ts=stamp + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1),
        hhmm_int=hhmm, hhmm=f"{hhmm:04d}", open=close-.01, close=close,
        signal_close=close, ema9=102., ema20=101., ema50=100.,
        oi=101., prev_oi=100., oi_change_pct=.5, volume_ratio=3.,
        price_change_pct=.05, traded_value=1_000_000.,
        v9_exact_confirmation_present=True, confirmation_open=close,
        confirmation_close=close+.02, confirmation_high=close+.03,
        confirmation_low=close-.01, body_ratio=.5,
        v9_1m_upper_wick_ratio=.25, v9_1m_lower_wick_ratio=.25,
        v9_1m_volume_ratio=1.2)
    row.update(changes)
    return row


def context():
    return pd.DataFrame([dict(contract_month="202609", day=pd.Timestamp("2026-09-01").date(),
        nifty_first_bar_return_pct=-1., nifty_first_bar_alignment_pct=-1.)])


def frames():
    pool = pd.DataFrame([pool_row(920, 100.), pool_row(925, 101., price_change_pct=1.),
                         pool_row(930, 101.05, price_change_pct=.049504950495)])
    strict = t._strict_pool(pool, context())
    native = strict.loc[strict.price_change_pct.ge(.1)].copy().reset_index(drop=True)
    native["sid"] = 10
    annotated = pd.concat([native, native.assign(sid=99, tradingsymbol="ANNOTATED_ONLY")], ignore_index=True)
    return native, pool, annotated


def setup():
    return replace(t.v9.v5.profile_setups(t.v9.v5.PROFILES["higher_frequency"])[0],
        signal_end="09:30", confirmation_end="09:31", price_change_pct=.8,
        oi_change_pct=.1, volume_ratio=1., body_ratio=.4, max_wick_ratio=.6)


def test_formula_is_compounded_close_to_close_not_sum_or_future():
    frame = pd.DataFrame([pool_row(920, 100.), pool_row(925, 102.), pool_row(930, 103.)])
    result = t._two_bar_features(frame)
    assert result.iloc[-1].v10_g_two_bar_change_pct == pytest.approx(3.)
    assert not result.iloc[1].v10_g_two_bar_valid
    changed = pd.concat([frame, pd.DataFrame([pool_row(935, 1_000_000.)])], ignore_index=True)
    pd.testing.assert_frame_equal(result, t._two_bar_features(changed).iloc[:3])


@pytest.mark.parametrize("changed", [
    {"contract": "STOCK26OCTFUT"}, {"symbol": "OTHER"}, {"day": "2026-08-31"},
])
def test_history_cannot_cross_contract_symbol_or_session(changed):
    frame = pd.DataFrame([pool_row(920, 100., **changed), pool_row(925, 101.), pool_row(930, 102.)])
    result = t._two_bar_features(frame)
    current = result.loc[result.hhmm_int.eq(930)].iloc[0]
    assert not current.v10_g_two_bar_valid
    assert pd.isna(current.v10_g_two_bar_change_pct)


def test_missing_intervening_bar_does_not_become_two_bars():
    frame = pd.DataFrame([pool_row(915, 99.), pool_row(920, 100.), pool_row(930, 102.)])
    result = t._two_bar_features(frame)
    assert not result.iloc[-1].v10_g_two_bar_valid


def test_future_feature_timestamp_fails_closed():
    row = pool_row(930, 102.)
    row["v9_5m_feature_ts"] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="Noncausal"):
        t._two_bar_features(pd.DataFrame([row]))


def test_pool_covers_below_cache_floor_and_preserves_original_rows():
    native, pool, annotated = frames()
    combined, proof = t._augment_frames(native, pool, annotated)
    pd.testing.assert_frame_equal(combined.iloc[:len(native)][native.columns], native)
    assert proof["native_identity_parity"]
    assert proof["native_sid_ceiling"] == 99
    addition = combined.loc[combined.hhmm_int.eq(930)].iloc[0]
    assert addition.sid == 100
    assert addition.price_change_pct < .1
    assert addition.v10_g_two_bar_new_sid
    assert t.eligible(combined, setup()).sid.tolist() == [100]
    assert t.eligible(combined, setup()).iloc[0].price_change_pct == addition.price_change_pct


def test_id_assignment_is_deterministic_and_checks_native_identity():
    native, pool, annotated = frames()
    first, _ = t._augment_frames(native, pool, annotated)
    second, _ = t._augment_frames(native, pool.iloc[::-1], annotated.iloc[::-1])
    pd.testing.assert_frame_equal(first, second)
    bad = native.assign(sid=99)
    with pytest.raises(ValueError, match="ID does not match"):
        t._augment_frames(bad, pool, annotated)


def test_duplicate_native_ids_fail_closed():
    native, pool, annotated = frames()
    annotated.loc[:, "sid"] = 10
    with pytest.raises(ValueError, match="Duplicate frozen signal ID"):
        t._augment_frames(native, pool, annotated)


@pytest.mark.parametrize("field,value", [
    ("open", 102.), ("price_change_pct", -.01),
    ("oi_change_pct", .049), ("volume_ratio", .79),
    ("confirmation_open", 102.), ("confirmation_close", 100.),
    ("v9_exact_confirmation_present", False),
])
def test_new_candidates_keep_direction_and_all_strict_source_guards(field, value):
    native, pool, annotated = frames()
    pool.loc[pool.hhmm_int.eq(930), field] = value
    combined, _ = t._augment_frames(native, pool, annotated)
    assert combined.sid.tolist() == [10]


@pytest.mark.parametrize("field,value", [
    ("oi_change_pct", .099), ("volume_ratio", .99),
    ("body_ratio", .39), ("wick_ratio", .61),
    ("v10_g_latest_body_directional", False), ("v10_g_two_bar_valid", False),
])
def test_alternate_does_not_bypass_setup_filters(field, value):
    native, pool, annotated = frames()
    combined, _ = t._augment_frames(native, pool, annotated)
    combined.loc[combined.hhmm_int.eq(930), field] = value
    assert t.eligible(combined, setup()).empty


def test_native_entries_remain_eligible_without_two_bar_direction_or_history():
    native, pool, annotated = frames()
    combined, _ = t._augment_frames(native, pool, annotated)
    first_setup = replace(setup(), signal_end="09:25", confirmation_end="09:26")
    combined.loc[combined.sid.eq(10), "v10_g_latest_body_directional"] = False
    assert t.eligible(combined, first_setup).sid.tolist() == [10]


def test_forged_alternate_history_clock_fails_closed():
    native, pool, annotated = frames()
    combined, _ = t._augment_frames(native, pool, annotated)
    combined.loc[combined.sid.eq(100), "v10_g_two_bar_base_ts"] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="chronology"):
        t.eligible(combined, setup())


def test_load_paths_rejects_sid_rebound_to_different_symbol():
    native, _, _ = frames()
    source = {"source_verification": {"all_frozen_artifacts_verified": True}, "signals": native}
    with pytest.raises(ValueError, match="verified source identity"):
        t.load_paths(source, native.assign(tradingsymbol="WRONG"))


def test_unverified_source_is_rejected():
    with pytest.raises(ValueError, match="verified frozen source"):
        t.augment_source({})
