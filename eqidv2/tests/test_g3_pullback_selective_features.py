import numpy as np
import pandas as pd
import pytest

from research.g3_pullback_selective_features import FEATURES, build_features


def observation(**changes):
    row = dict(trade_id="t1", side="LONG", decision_ts="2026-07-29T09:46:00+05:30",
               decision_close=101., atr14=.5, sma5=101.2, sma13=101.3,
               horizon_minutes=5, threshold_pct=.3, feature_ready=True,
               momentum3_atr=-.3, momentum5_atr=-.5, adverse_body_fraction=.4,
               micro_break=True, trend_damage=True, giveback_atr=1.,
               volume_ratio20=1.5, event=True, full_horizon_eligible=True)
    row.update(changes)
    return row


def entry(**changes):
    row = dict(trade_id="t1", entry_price=100., entry_ts="2026-07-29T09:15:00+05:30",
               exit_ts="2026-07-29T10:15:00+05:30", net_pnl=1000., exit_price=102.)
    row.update(changes)
    return row


def test_derived_values_use_only_entry_and_completed_monitor_features():
    result = build_features(pd.DataFrame([observation()]), pd.DataFrame([entry()]))
    row = result.iloc[0]
    assert row.trade_age_minutes == 31
    assert row.minutes_since_open == 31
    assert row.atr_pct == pytest.approx(100 * .5 / 101)
    assert row.threshold_atr == pytest.approx(.3 / (100 * .5 / 101))
    assert row.close_sma5_atr == pytest.approx(-.4)
    assert row.sma5_sma13_atr == pytest.approx(-.2)
    assert row.unrealized_atr == 2
    assert row.favorable_excursion_atr == 3
    assert row.giveback_fraction == pytest.approx(1 / 3)
    assert row.model_stage == "PROFIT_GIVEBACK"
    assert "model_stage" not in FEATURES and len(FEATURES) == 17


def test_labels_future_trade_fields_and_input_objects_are_not_used_or_mutated():
    dense = pd.DataFrame([observation()], index=[45])
    trades = pd.DataFrame([entry()])
    before_dense, before_trades = dense.copy(deep=True), trades.copy(deep=True)
    expected = build_features(dense, trades)
    for name in ["event", "full_horizon_eligible", "exit_ts", "net_pnl"]:
        dense[name] = "POISON_DO_NOT_READ"
    for name in ["exit_ts", "exit_price", "net_pnl", "monitoring_minutes"]:
        trades[name] = "POISON_DO_NOT_READ"
    actual = build_features(dense, trades)
    cols = [*FEATURES, "model_stage", "is_grid"]
    pd.testing.assert_frame_equal(expected[cols], actual[cols])
    pd.testing.assert_frame_equal(expected[["event", "full_horizon_eligible"]],
                                  before_dense[["event", "full_horizon_eligible"]])
    pd.testing.assert_frame_equal(before_trades, pd.DataFrame([entry()]))
    assert actual.event.iloc[0] == "POISON_DO_NOT_READ"
    assert actual.index.tolist() == [45]


def test_appending_future_observations_cannot_change_prefix_features_or_grid():
    dense = pd.DataFrame([observation(decision_ts=f"2026-07-29T09:{minute:02d}:00+05:30")
                          for minute in (46, 47, 51, 52, 56)])
    trades = pd.DataFrame([entry()])
    prefix = build_features(dense.iloc[:3], trades)
    complete = build_features(dense, trades)
    pd.testing.assert_frame_equal(prefix, complete.iloc[:3])
    assert complete.is_grid.tolist() == [True, False, True, False, True]


def test_grid_is_independent_per_horizon_and_preserves_input_order():
    dense = pd.DataFrame([
        observation(decision_ts="2026-07-29T10:16:00+05:30", horizon_minutes=30),
        observation(),
        observation(horizon_minutes=30),
        observation(decision_ts="2026-07-29T09:51:00+05:30", horizon_minutes=30),
    ], index=[4, 3, 2, 1])
    result = build_features(dense, pd.DataFrame([entry()]))
    assert result.index.tolist() == [4, 3, 2, 1]
    assert result.is_grid.tolist() == [True, True, True, False]


def test_signed_price_distances_and_stage_are_side_symmetric():
    long = observation()
    short = observation(trade_id="t2", side="SHORT", decision_close=99.,
                        sma5=98.8, sma13=98.7)
    result = build_features(pd.DataFrame([long, short]),
                            pd.DataFrame([entry(), entry(trade_id="t2")]))
    cols = ["close_sma5_atr", "sma5_sma13_atr", "unrealized_atr",
            "favorable_excursion_atr", "giveback_fraction"]
    np.testing.assert_allclose(result.loc[0, cols].astype(float),
                               result.loc[1, cols].astype(float), atol=1e-12)
    assert result.model_stage.tolist() == ["PROFIT_GIVEBACK"] * 2
    assert result.side_short.tolist() == [0, 1]


@pytest.mark.parametrize("age,close,giveback,expected", [
    (30, 100.3, 0, "ENTRY_RISK"),
    (31, 100.3, 0, "PROFIT_GIVEBACK"),
    (31, 100.299, 0, "LATE_NO_ESTABLISHED_PROFIT"),
    (31, 99.8, 1., "PROFIT_GIVEBACK"),
])
def test_stage_boundaries_use_age_and_prior_favorable_excursion(age, close, giveback, expected):
    stamp = pd.Timestamp("2026-07-29T09:15:00+05:30") + pd.Timedelta(minutes=age)
    result = build_features(pd.DataFrame([observation(decision_ts=stamp,
                               decision_close=close, giveback_atr=giveback)]),
                            pd.DataFrame([entry()]))
    assert result.model_stage.iloc[0] == expected


def test_missing_volume_and_no_established_profit_are_retained():
    dense = pd.DataFrame([observation(decision_close=99.5, giveback_atr=1.,
                          feature_ready=False, volume_ratio20=np.nan)])
    result = build_features(dense, pd.DataFrame([entry()]))
    assert pd.isna(result.volume_ratio20.iloc[0])
    assert not result.feature_ready.iloc[0]
    assert result.giveback_fraction.iloc[0] == 0
    assert result.favorable_excursion_atr.iloc[0] == 0
    assert result.model_stage.iloc[0] == "LATE_NO_ESTABLISHED_PROFIT"


@pytest.mark.parametrize("change,match", [
    ({"trade_id": "missing"}, "match an entry"),
    ({"decision_close": 0}, "positive"),
    ({"atr14": 0}, "positive"),
    ({"atr14": np.nan}, "finite"),
    ({"horizon_minutes": 10}, "horizons"),
    ({"decision_ts": "2026-07-29T09:15:00+05:30"}, "after entry"),
    ({"side": "UNKNOWN"}, "LONG or SHORT"),
])
def test_invalid_inputs_are_rejected(change, match):
    with pytest.raises(ValueError, match=match):
        build_features(pd.DataFrame([observation(**change)]), pd.DataFrame([entry()]))


def test_trade_join_rejects_duplicate_missing_and_conflicting_entry_data():
    dense, trades = pd.DataFrame([observation()]), pd.DataFrame([entry()])
    with pytest.raises(ValueError, match="unique"):
        build_features(dense, pd.concat([trades, trades]))
    with pytest.raises(ValueError, match="Missing trade columns"):
        build_features(dense, trades.drop(columns="entry_ts"))
    with pytest.raises(ValueError, match="already contain"):
        build_features(dense.assign(entry_price=1), trades)
    with pytest.raises(ValueError, match="Duplicate trade/horizon/decision"):
        build_features(pd.concat([dense, dense]), trades)


def test_naive_exchange_time_and_aware_time_produce_identical_features():
    aware = build_features(pd.DataFrame([observation()]), pd.DataFrame([entry()]))
    naive = build_features(pd.DataFrame([observation(decision_ts="2026-07-29 09:46:00")]),
                           pd.DataFrame([entry(entry_ts="2026-07-29 09:15:00")]))
    pd.testing.assert_frame_equal(aware, naive)
