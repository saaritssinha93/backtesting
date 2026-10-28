"""Pure selection, causal feature permissions, and executable-share sizing."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_practical_research as study


def feature_frame():
    clock = pd.Timestamp("2026-10-01 10:00", tz="Asia/Kolkata")
    return pd.DataFrame(
        {
            "simple_feature_available_at": [clock] * 4,
            "confirmation_ts": [clock] * 4,
            "sel_candle_ready": [True] * 4,
            "sel_cmf20_ready": [True, True, False, True],
            "sel_signed_cmf20": [0., -.01, np.nan, .01],
            "sel_vwap_slope5_ready": [True, True, False, True],
            "sel_signed_vwap_slope5_pct": [0., -.01, np.nan, -.01],
            "sel_prior5_ready": [True, True, False, True],
            "sel_prior5_failed_breakout": [True, True, True, False],
            "sel_directional_close_location": [.75, .749, .9, .9],
            "sel_directional_body_return_pct": [.1, .1, 0., -.1],
        },
        index=pd.Index([41, 9, 72, 3], name="sid"),
    )


def test_permissions_preserve_sid_alignment_and_exact_boundaries():
    decisions = study.entry_masks(feature_frame())
    assert decisions["cmf20_confirm"].to_dict() == {41: True, 9: False, 72: True, 3: True}
    assert decisions["vwap_slope_failure"].to_dict() == {41: True, 9: False, 72: True, 3: True}
    assert decisions["close75_interaction"].to_dict() == {41: True, 9: False, 72: False, 3: False}


def test_unready_indicators_cannot_reject_and_both_slope_conditions_are_required():
    f = feature_frame()
    f.loc[72, ["sel_signed_cmf20", "sel_signed_vwap_slope5_pct"]] = [-1., -100.]
    f.loc[9, "sel_prior5_ready"] = False
    decisions = study.entry_masks(f)
    assert decisions["cmf20_confirm"].loc[72]
    assert decisions["vwap_slope_failure"].loc[72]
    assert decisions["vwap_slope_failure"].loc[9]
    f.loc[9, "sel_prior5_ready"] = True
    f.loc[9, "sel_signed_vwap_slope5_pct"] = .001
    assert study.entry_masks(f)["vwap_slope_failure"].loc[9]


def test_csv_false_flags_do_not_become_truthy_python_strings():
    f = feature_frame()
    for column in ("sel_candle_ready", "sel_cmf20_ready", "sel_vwap_slope5_ready",
                   "sel_prior5_ready", "sel_prior5_failed_breakout"):
        f[column] = f[column].map({True: "True", False: "False"})
    assert study.entry_masks(f)["cmf20_confirm"].loc[72]
    assert study.entry_masks(f)["vwap_slope_failure"].loc[3]


@pytest.mark.parametrize("bad", ["future", "missing_available", "missing_confirmation",
                                 "ready_nan", "unknown_flag", "location", "body"])
def test_invalid_or_noncausal_feature_permissions_raise(bad):
    f = feature_frame()
    if bad == "future":
        f.loc[41, "simple_feature_available_at"] += pd.Timedelta(minutes=1)
    elif bad == "missing_available":
        f.loc[41, "simple_feature_available_at"] = pd.NaT
    elif bad == "missing_confirmation":
        f.loc[41, "confirmation_ts"] = pd.NaT
    elif bad == "ready_nan":
        f.loc[41, "sel_signed_cmf20"] = np.nan
    elif bad == "unknown_flag":
        f["sel_prior5_ready"] = f.sel_prior5_ready.astype(object)
        f.loc[41, "sel_prior5_ready"] = "unknown"
    elif bad == "location":
        f.loc[41, "sel_directional_close_location"] = 1.01
    else:
        f.loc[41, "sel_directional_body_return_pct"] = np.nan
    with pytest.raises(ValueError):
        study.entry_masks(f)


CONFIG = dict(capital_per_entry_rs=100., leverage=2., portfolio_capital_rs=1_000.,
              model_cost_bps_round_trip=5.)


def test_integer_sizing_rounds_down_once_preserves_source_and_blocks_unaffordable_share():
    raw = pd.DataFrame(dict(sid=[11, 22, 33], filled=[True, True, False],
                            g8_baseline_filled=[True, True, False],
                            entry_price=[73., 201., np.nan], g8_quantity_scale=[1., 1., 1.]),
                       index=[9, 4, 21])
    before = raw.copy(deep=True)
    actual = study.integerize(raw, CONFIG)
    pd.testing.assert_frame_equal(raw, before)
    assert actual.integer_cash_quantity.tolist() == [2., 0., 0.]
    assert actual.g8_quantity_scale.iloc[0] == pytest.approx(146. / 200.)
    assert actual.filled.tolist() == actual.g8_baseline_filled.tolist() == [True, False, False]
    twice = study.integerize(actual, CONFIG)
    assert twice.g8_quantity_scale.tolist() == actual.g8_quantity_scale.tolist()
    assert twice.integer_cash_quantity.tolist() == actual.integer_cash_quantity.tolist()


@pytest.mark.parametrize("price", [0., -1., np.nan, np.inf])
def test_invalid_filled_price_cannot_create_executable_quantity(price):
    raw = pd.DataFrame(dict(filled=[True], g8_baseline_filled=[True], entry_price=[price]))
    with pytest.raises(ValueError, match="filled price"):
        study.integerize(raw, CONFIG)


def test_actual_integer_exposure_drives_costs_and_pnl_on_each_immutable_replay():
    stamp = lambda clock: pd.Timestamp("2026-10-01 " + clock, tz="Asia/Kolkata")
    raw = pd.DataFrame([dict(sid=11, day="2026-10-01", setup_id="1000_LONG", tradingsymbol="EXAMPLE",
        side="LONG", filled=True, g8_baseline_filled=True, entry_price=73., entry_path_index=0,
        confirmation_ts=stamp("10:00"), entry_ts=stamp("10:01"), exit_ts=stamp("10:05"),
        exit_price=73.73, exit_reason="TARGET", gross_return_pct=1., net_return_pct=.95,
        cost_pct=.05, initial_stop_pct=1.2, picker="max_liquidity", traded_value=1_000_000.)])
    before = raw.copy(deep=True)
    plan = dict(raw=raw, allowed=pd.Series({11: True}), integer=True, clock=True, limit=None)
    low = study.execute(plan, CONFIG, "synthetic", cost=5.)
    high = study.execute(plan, CONFIG, "synthetic", cost=15.)
    pd.testing.assert_frame_equal(raw, before)
    for result in (low, high):
        assert result.loc[0, "g8_model_quantity"] == pytest.approx(2.)
        assert result.loc[0, "portfolio_trade_exposure_rupees"] == pytest.approx(146.)
        assert result.loc[0, "portfolio_gross_profit_rupees"] == pytest.approx(1.46)
    assert low.loc[0, "portfolio_cost_rupees"] == pytest.approx(.073)
    assert high.loc[0, "portfolio_cost_rupees"] == pytest.approx(.219)
    assert low.loc[0, "portfolio_net_profit_rupees"] == pytest.approx(1.387)
    assert high.loc[0, "portfolio_net_profit_rupees"] == pytest.approx(1.241)


def selection_rows(candidate=None, period="development"):
    control = dict(policy="execution_control", period=period, net_pnl_rs=1_000.,
                   negative_trade_sum_abs_rs=250., losses=20, wins=100, trades=120,
                   win_rate_pct=100. * 100 / 120, daily_close_drawdown_rs=100.,
                   changed_executions=0, changed_days=0)
    variant = dict(control, policy="rule", net_pnl_rs=1_000., negative_trade_sum_abs_rs=225.,
                   losses=18, trades=118, win_rate_pct=100. * 100 / 118,
                   changed_executions=5, changed_days=3)
    variant.update(candidate or {})
    return pd.DataFrame([control, variant])


def test_exact_ten_percent_loss_reduction_with_equal_net_and_drawdown_is_eligible():
    selected, audit = study.choose(selection_rows(), ["rule"])
    assert selected == "rule"
    assert audit[0]["eligible"]
    assert not audit[0]["failure_reasons"]


@pytest.mark.parametrize("changed,reason", [
    ({"net_pnl_rs": 999.}, "net profit"),
    ({"negative_trade_sum_abs_rs": 225.01}, "below10%"),
    ({"losses": 20}, "loss count"),
    ({"win_rate_pct": 80.}, "win rate"),
    ({"wins": 94}, "winner retention"),
    ({"trades": 95}, "trade retention"),
    ({"daily_close_drawdown_rs": 100.01}, "drawdown"),
    ({"changed_executions": 4}, "changed trades/days"),
    ({"changed_days": 2}, "changed trades/days"),
])
def test_each_acceptance_requirement_can_veto_apparent_improvement(changed, reason):
    selected, audit = study.choose(selection_rows(changed), ["rule"])
    assert selected is None
    assert not audit[0]["eligible"]
    assert reason in audit[0]["failure_reasons"]


def test_later_outcomes_cannot_change_development_choice_or_replace_locked_failure():
    development = selection_rows()
    later = selection_rows({"net_pnl_rs": -1e9}, "validation")
    table = pd.concat([development, later], ignore_index=True)
    assert study.choose(table, ["rule"])[0] == "rule"
    assert study.choose(table, ["rule"], period="validation")[0] is None
    later["net_pnl_rs"] = 1e12
    assert study.choose(pd.concat([development, later]), ["rule"])[0] == "rule"


def test_selection_prefers_profit_then_smaller_loss_amount_without_looking_later():
    table = selection_rows({"net_pnl_rs": 1_100.})
    other = dict(table.iloc[1], policy="other", net_pnl_rs=1_099., negative_trade_sum_abs_rs=150.)
    table = pd.concat([table, pd.DataFrame([other])], ignore_index=True)
    assert study.choose(table, ["rule", "other"])[0] == "rule"
    table.loc[table.policy.eq("other"), "net_pnl_rs"] = 1_100.
    assert study.choose(table, ["rule", "other"])[0] == "other"
