from dataclasses import asdict
from datetime import date

import pandas as pd

import fno_v13_corrected_v3_backtest as v3


def test_v3_configuration_is_frozen_and_isolated() -> None:
    v3.validate_configuration()
    assert v3.BASE_POLICY_NAME == "V13_V2_COMBINED_SHADOW"
    assert v3.EVIDENCE_STATUS == "EXPERIMENTAL_SHADOW_NOT_PROMOTED"
    assert v3.RESULT_DIR.resolve() != v3.v13_v2.RESULT_DIR.resolve()
    assert v3.RESULT_DIR.resolve() != v3.v6.RESULT_DIR.resolve()

    base = v3.v13_v2.policy_setups(
        v3.v13_v2.POLICIES[v3.BASE_POLICY_NAME]
    )
    observed = v3.active_setups()
    assert [asdict(setup) for setup in observed[:-1]] == [
        asdict(setup) for setup in base
    ]

    extra = observed[-1]
    assert (extra.signal_end, extra.confirmation_end, extra.side) == (
        "10:00",
        "10:01",
        "LONG",
    )
    assert extra.picker == "max_liquidity"
    assert extra.max_entries == 1
    assert extra.price_change_pct == 0.40
    assert extra.oi_change_pct == 0.05
    assert extra.volume_ratio == 1.00
    assert extra.body_ratio == 0.40
    assert extra.max_wick_ratio == 0.50
    assert extra.stop_pct == 1.00
    assert extra.target_pct == 3.00


def test_nifty_gate_applies_only_to_0925_short_and_fails_closed() -> None:
    day_pass = date(2026, 8, 10)
    day_fail = date(2026, 8, 11)
    day_missing = date(2026, 8, 12)
    signals = pd.DataFrame(
        [
            {"contract_month": "26AUG", "day": day_pass, "hhmm_int": 925, "side": "SHORT"},
            {"contract_month": "26AUG", "day": day_pass, "hhmm_int": 925, "side": "LONG"},
            {"contract_month": "26AUG", "day": day_fail, "hhmm_int": 925, "side": "SHORT"},
            {"contract_month": "26AUG", "day": day_fail, "hhmm_int": 930, "side": "SHORT"},
            {"contract_month": "26AUG", "day": day_missing, "hhmm_int": 925, "side": "SHORT"},
        ]
    )
    context = pd.DataFrame(
        [
            {"contract_month": "26AUG", "day": day_pass, "nifty_first_bar_return_pct": -0.06, "nifty_first_bar_alignment_pct": 0.06},
            {"contract_month": "26AUG", "day": day_fail, "nifty_first_bar_return_pct": -0.04, "nifty_first_bar_alignment_pct": 0.04},
        ]
    )

    result = v3.annotate_nifty_gate(signals, context)
    assert result["nifty_first_bar_gate_applies"].tolist() == [
        True,
        False,
        True,
        False,
        True,
    ]
    assert result["nifty_first_bar_gate_pass"].tolist() == [
        True,
        True,
        False,
        True,
        False,
    ]
    assert result["nifty_first_bar_gate_reason"].tolist() == [
        "PASS",
        "NOT_APPLICABLE",
        "NIFTY_FIRST_BAR_NOT_BEARISH_ENOUGH",
        "NOT_APPLICABLE",
        "MISSING_NIFTY_CONTEXT",
    ]


def test_nifty_gate_uses_completed_0920_bar() -> None:
    assert v3.NIFTY_FIRST_BAR_HHMM == 920
    assert v3.GATED_SIGNAL_HHMM == 925
    assert v3.NIFTY_FIRST_BAR_MAX_RETURN_PCT == -0.05

