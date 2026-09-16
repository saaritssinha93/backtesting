from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v13_corrected_v5_backtest as v5


V13_V3_SHA256 = "85c2ff1c37a342e8e0bc4b73eb115de8ebc5aa7e990db261e46036b68aeafbab"


def _path(
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> dict[str, np.ndarray]:
    timestamps = pd.date_range(
        "2026-08-03 09:27", periods=len(opens), freq="min", tz=v5.common.IST
    )
    return {
        "timestamp_ns": timestamps.asi8.copy(),
        "open": np.asarray(opens, dtype=float),
        "high": np.asarray(highs, dtype=float),
        "low": np.asarray(lows, dtype=float),
        "close": np.asarray(closes, dtype=float),
    }


def _native_order(
    *, sid: int = 1, side: str = "LONG", stop_pct: float = 1.0, target_pct: float = 1.0
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sid": sid,
                "trigger": 100.0,
                "side": side,
                "native_stop_pct": stop_pct,
                "native_target_pct": target_pct,
            }
        ]
    )


def _raw_minute_frame(*, terminal: str = "15:15", missing: str | None = None) -> pd.DataFrame:
    timestamps = pd.date_range(
        "2026-08-03 09:26",
        f"2026-08-03 {terminal}",
        freq="min",
        tz=v5.common.IST,
    )
    if missing is not None:
        missing_stamp = pd.Timestamp(f"2026-08-03 {missing}", tz=v5.common.IST)
        timestamps = timestamps[timestamps != missing_stamp]
    sequence = np.arange(len(timestamps), dtype=float)
    return pd.DataFrame(
        {
            "ts": timestamps,
            "open": 100.0 + sequence / 10_000.0,
            "high": 100.1 + sequence / 10_000.0,
            "low": 99.9 + sequence / 10_000.0,
            "close": 100.0 + sequence / 10_000.0,
        }
    )


def _materialization_order() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sid": 7,
                "day": date(2026, 8, 3),
                "tradingsymbol": "SYNTHETIC",
                "confirmation_ts": pd.Timestamp(
                    "2026-08-03 09:26", tz=v5.common.IST
                ),
            }
        ]
    )


def test_v3_source_is_hash_pinned_and_v5_outputs_are_isolated() -> None:
    v3_path = Path(v5.v13_v3.__file__).resolve()
    before = v5._sha256(v3_path)

    v5.validate_configuration()

    assert before == V13_V3_SHA256
    assert v5.EXPECTED_V13_V3_SOURCE_SHA256 == V13_V3_SHA256
    assert v5._sha256(v3_path) == before

    result_dir = v5.RESULT_DIR.resolve()
    prior_result_dirs = {
        v5.v13_v3.RESULT_DIR.resolve(),
        v5.v13_v2.RESULT_DIR.resolve(),
        v5.v6.RESULT_DIR.resolve(),
    }
    assert result_dir not in prior_result_dirs
    assert all(prior not in result_dir.parents for prior in prior_result_dirs)

    versioned_outputs = (
        v5.CACHE_DIR,
        v5.REPORT_PATH,
        v5.COMPARISON_PATH,
        v5.PARAMETER_REGISTRY_PATH,
        v5.PROVENANCE_PATH,
        v5.ELIGIBILITY_PATH,
    )
    assert all(path.resolve().is_relative_to(result_dir) for path in versioned_outputs)
    assert v5.WORKSPACE_REPORT_PATH.resolve() != v5.v13_v3.WORKSPACE_REPORT_PATH.resolve()
    assert "V5" in v5.WORKSPACE_REPORT_PATH.name


def test_profiles_freeze_exact_runtime_configuration_and_setup_additions() -> None:
    observed_profiles = {
        name: {
            "name": profile.name,
            "excluded_setup_ids": profile.excluded_setup_ids,
            "add_0950_short": profile.add_0950_short,
            "add_1120_short": profile.add_1120_short,
            "wick_cap_delta": profile.wick_cap_delta,
            "exit": asdict(profile.exit),
            "evidence": profile.evidence,
        }
        for name, profile in v5.PROFILES.items()
    }
    assert observed_profiles == {
        "balanced": {
            "name": "balanced",
            "excluded_setup_ids": (),
            "add_0950_short": False,
            "add_1120_short": True,
            "wick_cap_delta": 0.0,
            "exit": {
                "initial_stop_pct": 1.50,
                "first_target_pct": 1.075,
                "partial_pct": 0.10,
                "runner_target_pct": 2.60,
                "runner_stop": "BREAKEVEN",
                "maximum_holding_minutes": None,
            },
            "evidence": (
                "DEVELOPMENT_SELECTED_PARETO_SHADOW;_1120_LEG_ONLY_5_FILLS;_"
                "NO_UNTOUCHED_TEST"
            ),
        },
        "conservative": {
            "name": "conservative",
            "excluded_setup_ids": ("0936_LONG", "0946_LONG"),
            "add_0950_short": False,
            "add_1120_short": False,
            "wick_cap_delta": 0.0,
            "exit": {
                "initial_stop_pct": 1.50,
                "first_target_pct": 1.075,
                "partial_pct": 0.20,
                "runner_target_pct": 2.60,
                "runner_stop": "BREAKEVEN",
                "maximum_holding_minutes": 180,
            },
            "evidence": "DEVELOPMENT_RISK_ABLATION;_LOWER_RETURN_FOR_LOWER_DRAWDOWN",
        },
        "higher_frequency": {
            "name": "higher_frequency",
            "excluded_setup_ids": (),
            "add_0950_short": True,
            "add_1120_short": True,
            "wick_cap_delta": 0.10,
            "exit": {
                "initial_stop_pct": 1.50,
                "first_target_pct": 1.075,
                "partial_pct": 0.10,
                "runner_target_pct": 2.60,
                "runner_stop": "BREAKEVEN",
                "maximum_holding_minutes": None,
            },
            "evidence": (
                "AGGREGATE_COST_ROBUST_FREQUENCY_SHADOW;_TRAIN_NET_EDGE_FAILS_"
                "ABOVE_5BPS;_NO_UNTOUCHED_TEST"
            ),
        },
    }

    base = v5.v13_v3.active_setups()
    base_by_id = {setup.setup_id: asdict(setup) for setup in base}
    expected_ids = {
        "balanced": [*base_by_id, "1121_SHORT"],
        "conservative": [
            setup_id
            for setup_id in base_by_id
            if setup_id not in {"0936_LONG", "0946_LONG"}
        ],
        "higher_frequency": [*base_by_id, "0951_SHORT", "1121_SHORT"],
    }
    for name, profile in v5.PROFILES.items():
        setups = v5.profile_setups(profile)
        assert [setup.setup_id for setup in setups] == expected_ids[name]
        for setup in setups:
            if setup.setup_id in base_by_id:
                if name == "higher_frequency":
                    expected = dict(base_by_id[setup.setup_id])
                    expected["max_wick_ratio"] = min(
                        1.0, expected["max_wick_ratio"] + 0.10
                    )
                    expected["source_version"] = v5.STRATEGY_VERSION
                    assert asdict(setup) == expected
                else:
                    assert asdict(setup) == base_by_id[setup.setup_id]

    higher_added = {
        setup.signal_end: setup
        for setup in v5.profile_setups(v5.PROFILES["higher_frequency"])
        if setup.signal_end in {"09:50", "11:20"}
    }
    assert set(higher_added) == {"09:50", "11:20"}
    assert all(setup.max_wick_ratio == pytest.approx(0.60) for setup in higher_added.values())

    expected_added = {
        "mode": "FILTERED",
        "max_entries": 1,
        "picker": "max_liquidity",
        "price_change_pct": 0.20,
        "oi_change_pct": 0.10,
        "volume_ratio": 1.00,
        "body_ratio": 0.40,
        "max_wick_ratio": 0.50,
        "min_traded_value": 0.0,
        "stop_pct": 1.00,
        "target_pct": 3.00,
        "source_version": v5.STRATEGY_VERSION,
    }
    for signal_end in ("09:50", "11:20"):
        setup = asdict(v5.added_short_setup(signal_end))
        assert setup == {
            "signal_end": signal_end,
            "confirmation_end": {"09:50": "09:51", "11:20": "11:21"}[
                signal_end
            ],
            "side": "SHORT",
            **expected_added,
        }

    assert v5.OFFICIAL_CUTOFF == "1515"
    assert v5.DEFAULT_PROFILE == "higher_frequency"
    assert v5.parse_args([]).profile == "higher_frequency"
    assert v5.parse_args(["--profile", "all"]).profile == "all"
    assert v5.parse_args([]).cutoff == "15:15"
    assert v5.parse_args([]).capital_per_entry_rupees == pytest.approx(100_000.0)
    assert v5.parse_args([]).leverage_factor == pytest.approx(5.0)


@pytest.mark.parametrize(
    ("side", "opens", "highs", "lows", "closes", "expected_entry", "expected_exit"),
    [
        (
            "LONG",
            [102.0, 102.0],
            [102.50, 103.05],
            [101.50, 101.80],
            [102.20, 103.00],
            102.0,
            103.02,
        ),
        (
            "SHORT",
            [98.0, 98.0],
            [98.50, 98.20],
            [97.50, 96.99],
            [97.80, 97.00],
            98.0,
            97.02,
        ),
    ],
)
def test_gap_through_entry_uses_actual_open_and_rebases_native_brackets(
    side: str,
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    expected_entry: float,
    expected_exit: float,
) -> None:
    orders = _native_order(side=side)
    result = v5.simulate_native(
        orders,
        {1: _path(opens, highs, lows, closes)},
        cost_bps=0.0,
    )

    assert bool(result.loc[0, "entry_gap_through"])
    assert result.loc[0, "entry_price"] == pytest.approx(expected_entry)
    assert result.loc[0, "exit_price"] == pytest.approx(expected_exit)
    assert result.loc[0, "exit_reason"] == "TARGET"
    # A trigger-relative 1% target would be touched on bar zero.  The actual
    # open-relative target is not touched until bar one.
    assert result.loc[0, "exit_path_index"] == 1


def test_scaleout_brackets_are_rebased_to_gap_open() -> None:
    orders = _native_order()
    path = _path(
        [102.0, 102.0, 102.0],
        [102.50, 103.05, 104.10],
        [101.50, 102.20, 102.20],
        [102.20, 103.00, 104.05],
    )
    spec = v5.ExitSpec(1.0, 1.0, 0.20, 2.0)

    result = v5.simulate_scaleout(orders, {1: path}, spec, cost_bps=0.0)

    assert result.loc[0, "entry_price"] == pytest.approx(102.0)
    assert result.loc[0, "exit_price"] == pytest.approx(104.04)
    assert result.loc[0, "exit_reason"] == "RUNNER_TARGET"
    assert result.loc[0, "exit_path_index"] == 2
    assert result.loc[0, "gross_return_pct"] == pytest.approx(1.80)


def test_same_bar_stop_wins_native_and_initial_scaleout_target_ties() -> None:
    orders = _native_order()
    path = _path([100.0], [101.10], [98.90], [100.50])

    native = v5.simulate_native(orders, {1: path}, cost_bps=0.0)
    scaleout = v5.simulate_scaleout(
        orders,
        {1: path},
        v5.ExitSpec(1.0, 1.0, 0.20, 2.0),
        cost_bps=0.0,
    )

    assert native.loc[0, "exit_reason"] == "STOP"
    assert native.loc[0, "net_return_pct"] == pytest.approx(-1.0)
    assert bool(native.loc[0, "same_bar_ambiguous"])
    assert scaleout.loc[0, "exit_reason"] == "FULL_STOP"
    assert scaleout.loc[0, "net_return_pct"] == pytest.approx(-1.0)
    assert bool(scaleout.loc[0, "same_bar_ambiguous"])


def test_same_t1_bar_runner_stop_wins_runner_target_tie() -> None:
    orders = _native_order()
    path = _path([100.0], [102.10], [100.00], [101.50])
    result = v5.simulate_scaleout(
        orders,
        {1: path},
        v5.ExitSpec(1.0, 1.0, 0.20, 2.0),
        cost_bps=0.0,
    )

    assert result.loc[0, "exit_reason"] == "T1_THEN_BREAKEVEN"
    assert bool(result.loc[0, "same_bar_ambiguous"])
    assert bool(result.loc[0, "target_hit"])
    assert bool(result.loc[0, "first_target_hit"])
    assert not bool(result.loc[0, "runner_target_hit"])
    assert result.loc[0, "gross_return_pct"] == pytest.approx(0.20)


def test_later_bar_stop_gap_fills_at_adverse_open_but_entry_bar_does_not() -> None:
    orders = _native_order(stop_pct=1.0, target_pct=5.0)
    later_gap = _path(
        [100.0, 98.5],
        [100.2, 99.0],
        [99.5, 98.0],
        [100.0, 98.7],
    )
    result = v5.simulate_native(orders, {1: later_gap}, cost_bps=0.0)

    assert result.loc[0, "exit_reason"] == "STOP"
    assert result.loc[0, "exit_price"] == pytest.approx(98.5)
    assert bool(result.loc[0, "exit_gap_through"])
    assert result.loc[0, "exit_gap_bps"] == pytest.approx(
        abs(98.5 / 99.0 - 1.0) * 10_000.0
    )
    assert result.loc[0, "gross_return_pct"] == pytest.approx(-1.5)

    ambiguous_entry_bar = _path([98.0], [101.1], [97.0], [99.0])
    result = v5.simulate_native(
        orders, {1: ambiguous_entry_bar}, cost_bps=0.0
    )
    assert result.loc[0, "entry_price"] == pytest.approx(100.0)
    assert result.loc[0, "exit_price"] == pytest.approx(99.0)
    assert not bool(result.loc[0, "exit_gap_through"])


def test_materialize_raw_paths_requires_and_returns_exact_continuous_1515(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    minute = _raw_minute_frame()
    monkeypatch.setattr(
        v5.hybrid, "load_equity_one_minute", lambda symbol: minute.copy()
    )

    paths, quality = v5.materialize_raw_paths(
        _materialization_order(), cutoff=v5.OFFICIAL_CUTOFF
    )

    assert set(paths) == {7}
    assert len(paths[7]["timestamp_ns"]) == 349
    first = pd.Timestamp(int(paths[7]["timestamp_ns"][0]), tz="UTC").tz_convert(
        v5.common.IST
    )
    last = pd.Timestamp(int(paths[7]["timestamp_ns"][-1]), tz="UTC").tz_convert(
        v5.common.IST
    )
    assert first.strftime("%H%M") == "0927"
    assert last.strftime("%H%M") == "1515"
    assert quality.loc[0, "last_path_hhmm"] == "1515"
    assert bool(quality.loc[0, "first_forward_minute_present"])
    assert bool(quality.loc[0, "exact_cutoff_present"])
    assert bool(quality.loc[0, "continuous_one_minute_path"])


@pytest.mark.parametrize(
    ("minute", "expected_fragment"),
    [
        (_raw_minute_frame(terminal="15:14"), "terminal=1514, continuous=True"),
        (_raw_minute_frame(missing="12:00"), "terminal=1515, continuous=False"),
        (_raw_minute_frame(missing="09:27"), "first_forward=False"),
    ],
    ids=["missing-1515", "internal-minute-gap", "missing-first-forward-minute"],
)
def test_materialize_raw_paths_fails_closed_on_incomplete_data(
    monkeypatch: pytest.MonkeyPatch,
    minute: pd.DataFrame,
    expected_fragment: str,
) -> None:
    monkeypatch.setattr(
        v5.hybrid, "load_equity_one_minute", lambda symbol: minute.copy()
    )

    with pytest.raises(RuntimeError, match=expected_fragment):
        v5.materialize_raw_paths(
            _materialization_order(), cutoff=v5.OFFICIAL_CUTOFF
        )


def test_metrics_target_hit_rate_means_first_target_not_wins_or_runner_targets() -> None:
    day = date(2026, 8, 3)
    audit = pd.DataFrame(
        [
            # A profitable T1->BE trade counts as a target hit but not a runner hit.
            (True, 0.15, 0.20, "T1_THEN_BREAKEVEN", True, False, False),
            (True, 1.75, 1.80, "RUNNER_TARGET", True, True, False),
            (True, -1.05, -1.00, "FULL_STOP", False, False, True),
            # A profitable time exit is a win, not a target hit.
            (True, 0.25, 0.30, "TIME_EXIT_1515_NO_T1", False, False, False),
            # Unfilled rows are excluded from both numerator and denominator.
            (False, np.nan, np.nan, "UNFILLED", True, True, False),
        ],
        columns=[
            "filled",
            "net_return_pct",
            "gross_return_pct",
            "exit_reason",
            "target_hit",
            "runner_target_hit",
            "stop_hit",
        ],
    )
    audit["day"] = day
    audit["cost_pct"] = [0.05, 0.05, 0.05, 0.05, np.nan]
    audit["holding_minutes"] = [1.0, 2.0, 3.0, 4.0, np.nan]
    audit["mfe_pct"] = [1.0, 2.0, 0.5, 0.4, np.nan]
    audit["mae_pct"] = [-0.2, -0.1, -1.0, -0.3, np.nan]
    audit["same_bar_ambiguous"] = [False, False, False, False, False]
    audit["entry_gap_through"] = [False, False, False, False, False]

    result = v5.metrics(audit, [day], label="SYNTHETIC")

    assert result["selected_orders"] == 5
    assert np.isnan(result["configured_setup_rules"])
    assert result["executed_trades"] == 4
    assert result["wins"] == 3
    assert result["win_rate_pct"] == pytest.approx(75.0)
    assert result["target_hits"] == 2
    assert result["target_hit_rate_pct"] == pytest.approx(50.0)
    assert result["runner_target_hits"] == 1
    assert result["stop_hits"] == 1
    assert result["capital_per_entry_rupees"] == pytest.approx(100_000.0)
    assert result["leverage_factor"] == pytest.approx(5.0)
    assert result["exposure_per_entry_rupees"] == pytest.approx(500_000.0)
    assert result["net_profit_pct"] == pytest.approx(1.10)
    assert result["net_profit_on_capital_pct"] == pytest.approx(5.50)
    assert result["unleveraged_net_profit_rupees"] == pytest.approx(1_100.0)
    assert result["net_profit_rupees"] == pytest.approx(5_500.0)
    assert result["average_profit_per_trade_rupees"] == pytest.approx(1_375.0)
