from dataclasses import asdict
from datetime import date

import pandas as pd

import fno_v5_hybrid_backtest as replay
import fno_v13_corrected_v2_backtest as v13


def test_default_is_explicit_experimental_shadow() -> None:
    args = v13.parse_args([])
    assert args.policy == "V13_V2_COMBINED_SHADOW"
    assert not v13.POLICIES[args.policy].validated
    assert v13.EVIDENCE_STATUS == "EXPERIMENTAL_SHADOW_NOT_PROMOTED"


def test_v6_parity_book_is_exact_and_outputs_are_isolated() -> None:
    v13.validate_configuration()
    parity = v13.policy_setups(v13.POLICIES["V6_PARITY"])
    assert [asdict(setup) for setup in parity] == [
        asdict(setup) for setup in v13.v6.ACTIVE_SETUPS
    ]
    assert v13.RESULT_DIR.resolve() != v13.v6.RESULT_DIR.resolve()
    assert v13.v6.RESULT_DIR.resolve() not in v13.RESULT_DIR.resolve().parents


def test_combined_book_changes_only_declared_setups_and_adds_0955() -> None:
    baseline = {
        (setup.signal_end, setup.side): setup
        for setup in v13.v6.ACTIVE_SETUPS
    }
    combined = v13.policy_setups(v13.POLICIES["V13_V2_COMBINED_SHADOW"])
    observed = {(setup.signal_end, setup.side): setup for setup in combined}
    assert len(combined) == len(baseline) + 1
    assert observed[("09:35", "LONG")].oi_change_pct == 0.15
    assert observed[("09:40", "LONG")].oi_change_pct == 0.075
    assert ("09:55", "LONG") in observed

    for key, setup in baseline.items():
        expected = asdict(setup)
        actual = asdict(observed[key])
        if key in {("09:35", "LONG"), ("09:40", "LONG")}:
            expected["oi_change_pct"] = actual["oi_change_pct"]
        assert actual == expected


def test_global_oi_cap_is_applied_before_picker_and_allows_fallback() -> None:
    setup = v13.v6.ACTIVE_SETUPS[0]
    signals = pd.DataFrame(
        [
            {
                "day": date(2026, 8, 14),
                "hhmm_int": 925,
                "side": "LONG",
                "price_change_pct": 0.50,
                "oi_change_pct": 1.01,
                "volume_ratio": 4.0,
                "body_ratio": 0.80,
                "wick_ratio": 0.10,
                "traded_value": 2_000.0,
                "tradingsymbol": "HIGH_OI_FIRST",
            },
            {
                "day": date(2026, 8, 14),
                "hhmm_int": 925,
                "side": "LONG",
                "price_change_pct": 0.50,
                "oi_change_pct": 0.50,
                "volume_ratio": 4.0,
                "body_ratio": 0.80,
                "wick_ratio": 0.10,
                "traded_value": 1_000.0,
                "tradingsymbol": "FALLBACK",
            },
        ]
    )
    baseline_pick = replay.select_setup_rows(signals, setup)
    assert baseline_pick.iloc[0]["tradingsymbol"] == "HIGH_OI_FIRST"

    bounded = v13.apply_policy(
        signals, v13.POLICIES["OI_BOUNDED_SHADOW"]
    )
    bounded_pick = replay.select_setup_rows(bounded, setup)
    assert bounded_pick.iloc[0]["tradingsymbol"] == "FALLBACK"


def test_period_split_keeps_sep2_separate() -> None:
    days = [date(2026, 8, 13), date(2026, 8, 14), date(2026, 9, 1), date(2026, 9, 2)]
    periods = v13._periods(days)
    assert periods["ORIGINAL_TRAIN"] == [date(2026, 8, 13)]
    assert periods["ORIGINAL_TEST"] == [date(2026, 8, 14), date(2026, 9, 1)]
    assert periods["SEP2_CHECK"] == [date(2026, 9, 2)]
