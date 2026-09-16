from datetime import date

import pandas as pd

import fno_v6_v2_corrected_backtest as v6_v2


def test_market_gate_uses_matching_contract_month_and_side_direction():
    session = date(2026, 8, 20)
    signals = pd.DataFrame(
        [
            {"sid": 1, "contract_month": "26AUG", "day": session, "hhmm_int": 925, "side": "LONG"},
            {"sid": 2, "contract_month": "26SEP", "day": session, "hhmm_int": 925, "side": "LONG"},
            {"sid": 3, "contract_month": "26SEP", "day": session, "hhmm_int": 925, "side": "SHORT"},
        ]
    )
    context = pd.DataFrame(
        [
            {"contract_month": "26AUG", "day": session, "hhmm_int": 925, "nifty_return_from_open_pct": 0.20},
            {"contract_month": "26SEP", "day": session, "hhmm_int": 925, "nifty_return_from_open_pct": -0.30},
        ]
    )

    result = v6_v2.annotate_market_gate(
        signals, context, threshold_pct=0.10, slots=(925,)
    ).set_index("sid")

    assert bool(result.loc[1, "market_gate_pass"])
    assert not bool(result.loc[2, "market_gate_pass"])
    assert bool(result.loc[3, "market_gate_pass"])
    assert result.loc[1, "nifty_alignment_pct"] == 0.20
    assert result.loc[3, "nifty_alignment_pct"] == 0.30


def test_market_gate_fails_closed_only_when_the_gate_applies():
    session = date(2026, 8, 20)
    signals = pd.DataFrame(
        [
            {"sid": 1, "contract_month": "26AUG", "day": session, "hhmm_int": 925, "side": "LONG"},
            {"sid": 2, "contract_month": "26AUG", "day": session, "hhmm_int": 930, "side": "LONG"},
        ]
    )
    empty_context = pd.DataFrame(
        columns=["contract_month", "day", "hhmm_int", "nifty_return_from_open_pct"]
    )

    result = v6_v2.annotate_market_gate(
        signals, empty_context, threshold_pct=0.10, slots=(925,)
    ).set_index("sid")

    assert not bool(result.loc[1, "market_gate_pass"])
    assert result.loc[1, "market_gate_reason"] == "MISSING_NIFTY_CONTEXT"
    assert bool(result.loc[2, "market_gate_pass"])
    assert result.loc[2, "market_gate_reason"] == "NOT_APPLICABLE"
