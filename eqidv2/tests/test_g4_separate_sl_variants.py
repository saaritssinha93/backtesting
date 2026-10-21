"""Execution, causality and independent-policy checks for G4 stop variants."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research import g4_separate_sl_variants as rules
from research.g3_sl_target_simulator import simulate_exit


CONFIGS = {
    "floor": "g4_profit_floor_020_v1.json",
    "atr": "g4_atr_trailing_150_v1.json",
    "combined": "g4_profit_floor_atr_150_v1.json",
}


def _config(variant="atr", **overrides):
    config = rules._load_config(rules.CONFIG_DIR / CONFIGS[variant])
    return {**config, **overrides}


def _case(rows, *, short=False, start="2026-10-15 15:08", atr=.2, pad=True):
    rows = [list(row) for row in rows]
    start = pd.Timestamp(start, tz="Asia/Kolkata")
    if pad:
        squareoff = start.normalize() + pd.Timedelta(hours=15, minutes=15)
        count = int((squareoff - start).total_seconds() / 60) + 1
        close = rows[-1][3]
        rows += [[close, close + .05, close - .05, close] for _ in range(count - len(rows))]
    values = np.asarray(rows, dtype=float)
    if short:
        values = 200 - values
        values[:, [1, 2]] = values[:, [2, 1]]
    stamps = pd.date_range(start, periods=len(values), freq="min")
    path = {"timestamp_ns": stamps.as_unit("ns").asi8,
            **{name: values[:, index] for index, name in enumerate(("open", "high", "low", "close"))}}
    context = pd.DataFrame({"ts": stamps, "atr14": np.full(len(stamps), atr)})
    return path, context


def _outcome(path, context, *, variant="atr", short=False, target=1., entry_index=0, **overrides):
    row = {"side": "SHORT" if short else "LONG", "entry_path_index": entry_index,
           "entry_price": 100., "native_target_pct": target}
    return rules._variant_outcome(row, path, context, _config(variant, **overrides))


def _price(long_price, short):
    return 200 - long_price if short else long_price


def _assert_native_parity(actual, expected):
    # ATR policies additionally expose their stop-update audit trace.
    assert expected.keys() <= actual.keys()
    for key, value in expected.items():
        if isinstance(value, float):
            assert actual[key] == pytest.approx(value), key
        else:
            assert actual[key] == value, key


def test_configs_define_three_independent_research_policies():
    configs = {name: _config(name) for name in CONFIGS}
    assert len({config["variant_id"] for config in configs.values()}) == 3
    assert configs["floor"]["protected_profit_pct"] == .2
    assert configs["atr"]["protected_profit_pct"] is None
    assert configs["combined"]["protected_profit_pct"] == .2
    for config in configs.values():
        assert config["scope"] == "BACKTEST_ONLY"
        assert config["execution_authority"] is False
        assert config["initial_stop_pct"] == 1.2
        assert config["tighten_after_minutes"] == 120
        assert config["tightened_stop_pct"] == 1.
        assert config["activation_floor_pct"] == .4
        assert config["activation_target_fraction"] == .9
        assert config["cost_bps_round_trip"] == 5.
    assert configs["atr"]["atr_multiple"] == configs["combined"]["atr_multiple"] == 1.5


@pytest.mark.parametrize("variant", ["atr", "combined"])
@pytest.mark.parametrize("short", [False, True])
def test_completed_close_arms_for_next_candle_only(variant, short):
    # The arming candle trades below the future trail; that earlier low cannot exit.
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 100.98, 99.5, 100.95],
                           [100.8, 100.9, 100.5, 100.7]], short=short)
    outcome = _outcome(path, context, variant=variant, short=short)
    assert outcome["protection_armed_path_index"] == 1
    assert outcome["protection_effective_path_index"] == 2
    assert outcome["exit_path_index"] == 2
    assert outcome["exit_reason"] == ("COMBINED_STOP" if variant == "combined" else "ATR_TRAILING_STOP")
    assert outcome["exit_event"] == "INTRABAR"
    assert outcome["exit_price"] == pytest.approx(_price(100.65, short))


@pytest.mark.parametrize("short", [False, True])
@pytest.mark.parametrize("atr", [.2, 1.])
def test_floor_atr_and_combined_have_distinct_stop_levels(short, atr):
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 100.98, 99.9, 100.95],
                           [100.8, 100.85, 100.1, 100.3],
                           [100.3, 100.4, 99.3, 99.4]], short=short, atr=atr)
    outcomes = {name: _outcome(path, context, variant=name, short=short) for name in CONFIGS}
    assert outcomes["floor"]["exit_price"] == pytest.approx(_price(100.2, short))
    assert outcomes["floor"]["exit_path_index"] == 2
    atr_level = 100.95 - 1.5 * atr
    assert outcomes["atr"]["exit_price"] == pytest.approx(_price(atr_level, short))
    assert outcomes["atr"]["exit_path_index"] == (2 if atr == .2 else 3)
    assert outcomes["combined"]["exit_price"] == pytest.approx(_price(max(100.2, atr_level), short))
    assert outcomes["combined"]["exit_path_index"] == 2


@pytest.mark.parametrize("variant", ["atr", "combined"])
@pytest.mark.parametrize("short", [False, True])
def test_rising_atr_cannot_loosen_an_existing_stop(variant, short):
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 101.95, 99.9, 101.85],
                           [101.85, 101.95, 101.6, 101.9],
                           [101.8, 101.9, 101.5, 101.65]], short=short)
    context.loc[2:, "atr14"] = .6
    outcome = _outcome(path, context, variant=variant, short=short, target=2.)
    assert outcome["exit_path_index"] == 3
    assert outcome["active_stop_price"] == pytest.approx(_price(101.55, short))
    assert outcome["exit_price"] == pytest.approx(_price(101.55, short))


@pytest.mark.parametrize("variant", ["atr", "combined"])
@pytest.mark.parametrize("short", [False, True])
def test_gap_stop_fills_at_open_and_ignores_later_bar_extrema(variant, short):
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 101.95, 99.9, 101.85],
                           [101.85, 101.95, 101.6, 101.9],
                           [101.4, 103, 95, 100]], short=short)
    outcome = _outcome(path, context, variant=variant, short=short, target=2.)
    assert outcome["exit_path_index"] == 3
    assert outcome["exit_event"] == "OPEN"
    assert outcome["exit_gap_through"]
    assert not outcome["same_bar_ambiguous"]
    assert outcome["active_stop_price"] == pytest.approx(_price(101.6, short))
    assert outcome["exit_price"] == pytest.approx(_price(101.4, short))
    assert outcome["mfe_pct"] == pytest.approx(1.95)
    assert outcome["mae_pct"] == pytest.approx(-.1)
    assert outcome["exit_ts"] == context.loc[2, "ts"]


@pytest.mark.parametrize("variant", CONFIGS)
@pytest.mark.parametrize("short", [False, True])
def test_entry_close_and_intrabar_extrema_cannot_arm(variant, short):
    path, context = _case([[100, 101.99, 99.9, 101.9],
                           [100.3, 101.99, 100.1, 100.3]], short=short)
    outcome = _outcome(path, context, variant=variant, short=short, target=2.)
    assert outcome["exit_reason"] == "TIME_EXIT_1515"
    assert outcome["protection_armed_path_index"] is None
    assert not outcome["protection_applied"]


@pytest.mark.parametrize("variant", ["atr", "combined"])
@pytest.mark.parametrize("short", [False, True])
def test_entry_close_is_excluded_from_best_completed_close(variant, short):
    path, context = _case([[100, 101.99, 99.9, 101.9],
                           [100.3, 101.99, 100.1, 100.3],
                           [100.3, 101.95, 100.2, 101.85],
                           [101.8, 101.9, 101.575, 101.65],
                           [101.65, 101.8, 101.5, 101.55]], short=short)
    outcome = _outcome(path, context, variant=variant, short=short, target=2.)
    assert outcome["protection_armed_path_index"] == 2
    assert outcome["exit_path_index"] == 4
    assert outcome["exit_price"] == pytest.approx(_price(101.55, short))


@pytest.mark.parametrize("variant", CONFIGS)
@pytest.mark.parametrize("short", [False, True])
def test_time_tightening_waits_for_bar_open_at_120_minutes(variant, short):
    rows = [[100, 100.1, 99.9, 100] for _ in range(123)]
    rows[120] = [100, 100.1, 98.9, 99.5]  # Opens 119 minutes after entry.
    rows[121] = [99.5, 99.6, 98.9, 99.2]  # Opens 120 minutes after entry.
    path, context = _case(rows, start="2026-10-15 13:13", short=short)
    outcome = _outcome(path, context, variant=variant, short=short)
    assert outcome["exit_path_index"] == 121
    assert outcome["exit_reason"] == "TIGHTENED_STOP"
    assert outcome["exit_price"] == pytest.approx(_price(99., short))
    assert outcome["active_stop_pct_at_exit"] == pytest.approx(1.)


@pytest.mark.parametrize("variant", CONFIGS)
@pytest.mark.parametrize("short", [False, True])
def test_protected_samebar_target_and_stop_is_stop_first(variant, short):
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 100.98, 99.9, 100.95],
                           [100.8, 101.1, 100.1, 100.5]], short=short)
    outcome = _outcome(path, context, variant=variant, short=short)
    assert outcome["stop_hit"] and not outcome["target_hit"]
    assert outcome["same_bar_ambiguous"]
    assert outcome["exit_path_index"] == 2
    assert outcome["exit_event"] == "INTRABAR"
    assert outcome["exit_price"] == pytest.approx(_price(100.2 if variant == "floor" else 100.65, short))


@pytest.mark.parametrize("variant", CONFIGS)
@pytest.mark.parametrize("short", [False, True])
@pytest.mark.parametrize("last_bar", [
    [100, 100.2, 98.7, 99.5],     # Initial stop.
    [98.5, 100.2, 98.4, 99.5],   # Gap below initial stop.
    [100, 101.1, 99.9, 100.95],  # Target before the close can arm.
    [100, 101.1, 98.7, 100.5],   # Initial stop and target on same bar.
])
def test_no_protection_matches_native_even_on_early_partial_path(variant, short, last_bar):
    path, context = _case([[100, 100.1, 99.9, 100], last_bar], short=short, pad=False)
    expected = simulate_exit(path, 0, 100., not short, 1.2, 1.,
                             tighten_after_minutes=120, tightened_stop_pct=1.)
    actual = _outcome(path, context, variant=variant, short=short)
    _assert_native_parity(actual, expected)


@pytest.mark.parametrize("variant", CONFIGS)
@pytest.mark.parametrize("short", [False, True])
def test_no_protection_matches_native_at_squareoff(variant, short):
    path, context = _case([[100, 100.1, 99.9, 100], [100, 100.5, 99.9, 100.4]], short=short)
    expected = simulate_exit(path, 0, 100., not short, 1.2, 1.,
                             tighten_after_minutes=120, tightened_stop_pct=1.)
    _assert_native_parity(_outcome(path, context, variant=variant, short=short), expected)


@pytest.mark.parametrize("variant", ["atr", "combined"])
def test_future_prices_and_atr_cannot_change_a_completed_exit(variant):
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 100.98, 99.9, 100.95],
                           [100.8, 100.9, 100.5, 100.7]])
    before = _outcome(path, context, variant=variant)
    changed = {key: values.copy() for key, values in path.items()}
    for name in ("open", "high", "low", "close"):
        changed[name][3:] *= 10
    changed_context = context.copy()
    changed_context.loc[3:, "atr14"] = 100.
    assert _outcome(changed, changed_context, variant=variant) == before


@pytest.mark.parametrize("variant", ["atr", "combined"])
@pytest.mark.parametrize("invalid", [np.nan, np.inf, 0., -1.])
def test_invalid_atr_at_an_armed_update_fails_closed(variant, invalid):
    path, context = _case([[100, 100.1, 99.9, 100], [100, 100.98, 99.9, 100.95]])
    context.loc[1, "atr14"] = invalid
    with pytest.raises(ValueError, match="(?i)atr"):
        _outcome(path, context, variant=variant)


@pytest.mark.parametrize("variant", ["atr", "combined"])
@pytest.mark.parametrize("missing", ["column", "armed_timestamp"])
def test_missing_atr_context_fails_closed(variant, missing):
    path, context = _case([[100, 100.1, 99.9, 100], [100, 100.98, 99.9, 100.95]])
    context = context.drop(columns="atr14") if missing == "column" else context.drop(index=1)
    with pytest.raises(ValueError, match="(?i)atr"):
        _outcome(path, context, variant=variant)


@pytest.mark.parametrize("variant", ["atr", "combined"])
def test_context_microseconds_are_normalized_to_path_nanoseconds(variant):
    path, context = _case([[100, 100.1, 99.9, 100],
                           [100, 100.98, 99.9, 100.95],
                           [100.8, 100.9, 100.5, 100.7]])
    expected = _outcome(path, context, variant=variant)
    context["ts"] = context.ts.dt.as_unit("us")
    assert context.ts.dtype.unit == "us"
    assert _outcome(path, context, variant=variant) == expected


@pytest.mark.parametrize("variant", CONFIGS)
def test_surviving_path_requires_native_squareoff(variant):
    path, context = _case([[100, 100.1, 99.9, 100], [100, 100.2, 99.9, 100.1]], pad=False)
    with pytest.raises(ValueError, match="15:15"):
        _outcome(path, context, variant=variant)


@pytest.mark.parametrize("variant", CONFIGS)
def test_config_loader_rejects_execution_authority(tmp_path, variant):
    config = _config(variant, execution_authority=True)
    config_path = tmp_path / "unauthorized.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError, match="backtest-only"):
        rules._load_config(config_path)
