"""Counterfactual OI removals must retain all non-OI baseline behavior."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14.diagnostics_oi_ablation import ARCHIVE, select_arms, _verified


def candidate(*, sid=0, symbol="CASH", signal="2026-08-03 10:00", side="LONG", oi_change=.1, **changes):
    stamp = pd.Timestamp(signal, tz="Asia/Kolkata")
    long = side == "LONG"
    row = dict(sid=sid, day=str(stamp.date()), tradingsymbol=symbol, signal_ts=stamp,
               confirmation_ts=stamp + pd.Timedelta(minutes=1), hhmm_int=int(stamp.strftime("%H%M")),
               v9_5m_ema_bull=long, v9_5m_ema_bear=not long, price_change_pct=.8 if long else -.8,
               volume_ratio=3., body_ratio=.8, traded_value=1e7,
               v9_exact_confirmation_present=True, confirmation_high=102., confirmation_low=99.,
               confirmation_open=100. if long else 101., confirmation_close=101. if long else 100.,
               signal_close=100.5, v9_1m_upper_wick_ratio=.1, v9_1m_lower_wick_ratio=.1,
               v9_1m_volume_ratio=1.2, nifty_first_bar_return_pct=-.1,
               oi=100. * (1. + oi_change / 100.), prev_oi=100., oi_change_pct=oi_change)
    return {**row, **changes}


@pytest.mark.parametrize("oi_change,eligible", [(-.5, False), (.049, False), (.05, True),
                                                (.1, True), (1., True), (1.00001, False), (np.nan, False)])
def test_off_arm_retains_pre_oi_candidates_and_on_has_inclusive_native_base_range(oi_change, eligible):
    broad = pd.DataFrame([candidate(oi_change=oi_change)])
    before = broad.copy(deep=True)
    strict, gate, arms = select_arms(broad)
    assert len(strict) == 1  # No OI prefilter may consume the counterfactual pool.
    assert bool(gate.iloc[0]) is eligible
    assert len(arms["OI_ON"]) == int(eligible)
    assert len(arms["OI_OFF"]) == 1
    pd.testing.assert_frame_equal(broad, before)


def test_rising_absolute_oi_is_required_even_when_a_supplied_percentage_passes():
    broad = pd.DataFrame([candidate(oi_change=.1, oi=100., prev_oi=100.)])
    _, gate, arms = select_arms(broad)
    assert not gate.any()
    assert arms["OI_ON"].empty
    assert len(arms["OI_OFF"]) == 1


def test_per_setup_oi_floor_filters_before_unchanged_liquidity_ranking():
    broad = pd.DataFrame([
        candidate(symbol="HIGHER_LIQUIDITY", sid=0, signal="2026-08-03 09:35", oi_change=.149, traded_value=2e7),
        candidate(symbol="AT_NATIVE_FLOOR", sid=1, signal="2026-08-03 09:35", oi_change=.15, traded_value=1e7)])
    strict, gate, arms = select_arms(broad)
    assert len(strict) == 2 and gate.all()  # Base OI gate alone is insufficient.
    assert arms["OI_ON"].tradingsymbol.tolist() == ["AT_NATIVE_FLOOR"]
    assert arms["OI_OFF"].tradingsymbol.tolist() == ["HIGHER_LIQUIDITY"]


def test_original_core_price_reservation_survives_both_oi_arms():
    broad = pd.DataFrame([
        candidate(symbol="EXPANDED", sid=0, signal="2026-08-03 09:50", side="SHORT", oi_change=.05,
                  price_change_pct=-.15, traded_value=9e7),
        candidate(symbol="CORE", sid=1, signal="2026-08-03 09:50", side="SHORT", oi_change=.05,
                  price_change_pct=-.25, traded_value=1e7)])
    _, _, arms = select_arms(broad)
    assert arms["OI_ON"].tradingsymbol.tolist() == ["CORE"]
    assert arms["OI_OFF"].tradingsymbol.tolist() == ["CORE"]


@pytest.mark.parametrize("invalid", [dict(volume_ratio=.79), dict(price_change_pct=.09),
                                      dict(v9_5m_ema_bull=False), dict(v9_exact_confirmation_present=False),
                                      dict(confirmation_close=99.9), dict(v9_1m_volume_ratio=1.09)])
def test_disabling_oi_cannot_relax_cash_ema_price_volume_or_confirmation_gates(invalid):
    _, _, arms = select_arms(pd.DataFrame([candidate(**invalid)]))
    assert arms["OI_ON"].empty and arms["OI_OFF"].empty


@pytest.mark.skipif(not ARCHIVE.exists(), reason="Local sealed G3 archive unavailable")
def test_sealed_counterfactual_source_actually_precedes_oi_filtering():
    provenance = json.loads((ARCHIVE / "source_study_provenance.json").read_text())
    bundle = Path(provenance["source_bundle"])
    _verified(bundle / "bundle_manifest.json", provenance["source_bundle_manifest_sha256"])
    manifest = json.loads((bundle / "bundle_manifest.json").read_text())
    path = bundle / "dataset/all_5m_features.parquet"
    _verified(path, manifest["artifacts"]["dataset/all_5m_features.parquet"]["sha256"])
    broad = pd.read_parquet(path, columns=["oi_change_pct"])
    assert len(broad) == 571968
    assert broad.oi_change_pct.lt(0).any()
    assert broad.oi_change_pct.gt(1).any()
    assert broad.oi_change_pct.isna().any()
    assert (~broad.oi_change_pct.between(.05, 1.)).sum() == 477088
