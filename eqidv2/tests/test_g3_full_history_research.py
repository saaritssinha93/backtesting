"""Broad-entry opening normalization must be causal and cohort-consistent."""
import numpy as np
import pandas as pd
import pytest

from research import g3_slot_relaxation_full_backtest_20261023 as research


def test_opening_normalization_copies_input_and_preserves_nonprice_fields():
    features = pd.DataFrame(dict(
        hhmm_int=[920, 925], open=[100., 102.], close=[102., 103.],
        price_change_pct=[np.nan, .75], oi=[1000., 1001.], prev_oi=[np.nan, 1000.],
        oi_change_pct=[np.nan, .1], opening_0920_status=["MISSING", None]))
    before = features.copy(deep=True)
    normalized = research.normalize_broad_opening(features)
    pd.testing.assert_frame_equal(features, before)
    assert normalized is not features
    assert normalized.loc[0, "price_change_pct"] == pytest.approx(2.)
    pd.testing.assert_frame_equal(normalized.drop(columns="price_change_pct"),
                                  features.drop(columns="price_change_pct"))
    normalized.loc[0, "open"] = 99.
    assert features.loc[0, "open"] == 100.


@pytest.mark.parametrize("opening,close", [
    (0., 100.), (-1., 100.), (np.nan, 100.), (np.inf, 100.),
    (100., 0.), (100., -1.), (100., np.nan), (100., np.inf),
])
def test_invalid_opening_prices_fail_closed_despite_old_usable_move(opening, close):
    features = pd.DataFrame([dict(hhmm_int=920, open=opening, close=close,
                                  price_change_pct=5., oi_change_pct=.5)])
    normalized = research.normalize_broad_opening(features)
    assert pd.isna(normalized.price_change_pct.iloc[0])


def test_0925_and_other_slots_keep_their_original_close_to_close_return():
    features = pd.DataFrame(dict(hhmm_int=[925, 930, 1100],
        open=[0., 100., 100.], close=[102., 105., 90.],
        price_change_pct=[.12, -.7, np.nan]))
    pd.testing.assert_frame_equal(research.normalize_broad_opening(features), features)


def test_opening_cash_move_never_depends_on_oi_availability_or_direction():
    features = pd.DataFrame(dict(hhmm_int=[920]*4, open=[100.]*4, close=[99.]*4,
        price_change_pct=[np.nan, 20., -.1, .6], oi=[np.nan, 0., 990., 1100.],
        prev_oi=[np.nan, np.nan, 1000., 1000.], oi_change_pct=[np.nan, np.nan, -1., 10.],
        opening_0920_status=["MISSING", "INVALID", "VALID", "VALID"]))
    normalized = research.normalize_broad_opening(features)
    assert np.allclose(normalized.price_change_pct, [-1.]*4)
    pd.testing.assert_frame_equal(normalized[["oi", "prev_oi", "oi_change_pct", "opening_0920_status"]],
                                  features[["oi", "prev_oi", "oi_change_pct", "opening_0920_status"]])


def test_opening_cash_move_works_when_no_oi_columns_exist():
    features = pd.DataFrame([dict(hhmm_int=920, open=200., close=201., price_change_pct=np.nan)])
    normalized = research.normalize_broad_opening(features)
    assert normalized.price_change_pct.iloc[0] == pytest.approx(.5)
