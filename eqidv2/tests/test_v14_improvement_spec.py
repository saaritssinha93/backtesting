"""Fixed follow-up bounds, causality and failure-mode checks."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from v14 import improvement_spec as spec


def _rows():
    return pd.DataFrame(dict(traded_value=[10_000_000., 10_000_000.],
        confirmation_close=[100., 100.], confirmation_volume=[50_000., 50_000.],
        close=[100., 100.], high=[101., 101.], low=[99., 99.], ema9=[100., 100.],
        price_change_pct=[1.25, -1.25], interval_rvol=[1.5, 1.5],
        cmf10=[.2, -.2], side=["LONG", "SHORT"]), index=[11, 29])


def _variant(name):
    return next(v for v in spec.variants() if v.name == name)


def test_six_declared_variants_and_thresholds_are_fixed():
    assert [v.name for v in spec.variants()] == ["no_oi_control", "capacity_guard", "anti_chase_guard",
        "capacity_anti_chase", "capacity_anti_chase_rvol1p5", "capacity_anti_chase_rvol1p5_cmf0"]
    assert spec.specification()["thresholds"] == dict(signal_capacity_fraction_max=.05,
        confirmation_capacity_fraction_max=.10, abs_price_change_pct_max=1.25,
        signal_range_pct_max=2.5, ema9_extension_pct_max=2.5,
        interval_rvol_min=1.5, side_cmf_min=0.)
    assert spec.specification()["execution_authority"] is False


def test_inclusive_capacity_rvol_and_move_boundaries_preserve_index_and_source():
    rows = _rows()
    original = rows.copy(deep=True)
    for variant in spec.variants():
        passed = spec.participation_mask(rows, variant)
        assert passed.index.tolist() == [11, 29]
        assert passed.tolist() == [True, True]
    pd.testing.assert_frame_equal(rows, original)
    values = spec.diagnostic_features(rows)
    assert values.signal_capacity_fraction.eq(.05).all()
    assert values.confirmation_capacity_fraction.eq(.10).all()


@pytest.mark.parametrize("close,ema9", [(102.5, 100.), (97.5, 100.)])
def test_ema_extension_boundary_is_inclusive_in_both_directions(close, ema9):
    rows = _rows().iloc[:1].copy()
    rows["close"], rows["ema9"] = close, ema9
    rows["high"], rows["low"] = close + .5, close - .5
    assert spec.diagnostic_features(rows).ema9_extension_pct.iloc[0] == 2.5
    assert spec.participation_mask(rows, _variant("anti_chase_guard")).iloc[0]
    rows["close"] += .00001 if close > ema9 else -.00001
    assert not spec.participation_mask(rows, _variant("anti_chase_guard")).iloc[0]


def test_signal_range_boundary_is_inclusive_and_immediately_above_rejected():
    rows = _rows()
    rows["high"], rows["low"] = 102.5, 100.
    assert spec.participation_mask(rows, _variant("anti_chase_guard")).all()
    rows["high"] += .000001
    assert not spec.participation_mask(rows, _variant("anti_chase_guard")).any()


@pytest.mark.parametrize("field,value,variant", [
    ("traded_value", 9_999_999., "capacity_guard"),
    ("confirmation_volume", 49_999., "capacity_guard"),
    ("price_change_pct", 1.250001, "anti_chase_guard"),
    ("price_change_pct", -1.250001, "anti_chase_guard"),
    ("interval_rvol", 1.499999, "capacity_anti_chase_rvol1p5"),
    ("cmf10", -.00001, "capacity_anti_chase_rvol1p5_cmf0"),
])
def test_each_fixed_gate_rejects_values_beyond_its_bound(field, value, variant):
    rows = _rows().iloc[:1].copy()
    rows[field] = value
    assert not spec.participation_mask(rows, _variant(variant)).iloc[0]


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, "invalid", None])
def test_required_nonfinite_or_missing_values_fail_closed(bad):
    for field in ("traded_value", "confirmation_close", "confirmation_volume", "price_change_pct",
                  "high", "low", "close", "ema9", "interval_rvol", "cmf10"):
        rows = _rows().astype(object)
        rows[field] = bad
        assert not spec.participation_mask(rows, spec.variants()[-1]).any(), field
        assert spec.participation_mask(rows, spec.variants()[0]).all()


@pytest.mark.parametrize("field", ["traded_value", "confirmation_close", "confirmation_volume", "close", "high", "low", "ema9"])
def test_nonpositive_required_prices_volumes_and_turnover_fail_closed(field):
    for value in (0., -1.):
        rows = _rows()
        rows[field] = value
        assert not spec.participation_mask(rows, spec.variants()[-1]).any()


def test_missing_columns_fail_only_enabled_gates_and_empty_input_is_safe():
    rows = _rows().drop(columns=["ema9", "cmf10"])
    assert spec.participation_mask(rows, _variant("capacity_guard")).all()
    assert not spec.participation_mask(rows, _variant("anti_chase_guard")).any()
    assert spec.participation_mask(pd.DataFrame(index=[7]), _variant("no_oi_control")).iloc[0]
    for variant in spec.variants():
        assert spec.participation_mask(_rows().iloc[:0], variant).empty


def test_pressure_direction_zero_boundary_and_unknown_side():
    rows = _rows()
    rows["cmf10"] = 0.
    assert spec.participation_mask(rows, spec.variants()[-1]).all()
    rows["cmf10"] = [.2, .2]
    assert spec.participation_mask(rows, spec.variants()[-1]).tolist() == [True, False]
    rows["side"] = "UNKNOWN"
    assert not spec.participation_mask(rows, spec.variants()[-1]).any()


def test_future_outcomes_realized_exposure_and_other_dates_never_enter_gate():
    rows = _rows()
    expected = [spec.participation_mask(rows, v) for v in spec.variants()]
    rows["portfolio_trade_exposure_rupees"] = [np.nan, 1e15]
    rows["portfolio_net_profit_rupees"] = [-1e15, 1e15]
    rows["exit_reason"] = ["STOP", "TARGET"]
    rows["future_volume"] = [0., 1e15]
    for variant, passed in zip(spec.variants(), expected):
        pd.testing.assert_series_equal(spec.participation_mask(rows, variant), passed)
    later = rows.copy()
    later.index = [31, 47]
    later["traded_value"] = 1.
    combined = pd.concat([rows, later])
    pd.testing.assert_series_equal(spec.participation_mask(combined, _variant("capacity_guard")).loc[rows.index], expected[1])


def test_invalid_range_geometry_and_overflowed_turnover_fail_closed():
    rows = _rows()
    rows["high"] = 99.
    assert not spec.participation_mask(rows, _variant("anti_chase_guard")).any()
    rows = _rows()
    rows["confirmation_close"], rows["confirmation_volume"] = 1e308, 1e308
    assert not spec.participation_mask(rows, _variant("capacity_guard")).any()


@pytest.mark.parametrize("notional", [0., -1., np.nan, np.inf])
def test_invalid_planned_notional_is_a_configuration_error(notional):
    with pytest.raises(ValueError, match="finite and positive"):
        spec.participation_mask(_rows(), _variant("capacity_guard"), planned_notional_rupees=notional)
