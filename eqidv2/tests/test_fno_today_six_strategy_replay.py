from __future__ import annotations

import math
from datetime import date

import pandas as pd

from tools import fno_today_six_strategy_replay as today


def test_requested_strategy_registry_is_exact() -> None:
    assert today.EXPECTED_STRATEGIES == (
        "V6_CONTROL",
        "V6_A1_A2_0935_LONG_MAX_050",
        "V8_COMBINED",
        "V10_STAGE7",
        "V10_STAGE7_0935_LONG_MAX_050",
        "V10_STAGE7_0935_LONG_MAX_050_GAP2",
    )


def test_snapshot_capture_roles_match_snapshot_schema() -> None:
    assert today.EXPECTED_CAPTURE_ROLES == {
        "NSE_EQUITY_1M": 210,
        "NFO_FUTURES_5M": 210,
    }


def test_v6_policy_is_resolved_from_v6_engine() -> None:
    today.v6.strict.configure_engine()
    policy = today.v6.engine.entry_policy_for_variant(
        "VS",
        cost_bps=today.COST_BPS,
        slippage_bps=today.SLIPPAGE_BPS,
        square_off=today.SQUARE_OFF,
        eod_policy=today.EOD_POLICY,
    )
    assert policy.cost_bps == today.COST_BPS
    assert policy.square_off == today.SQUARE_OFF


def test_metric_row_uses_only_filled_finite_trades() -> None:
    audit = pd.DataFrame(
        {
            "filled": [True, "False", "true", False],
            "net_return_pct": [1.0, 99.0, -0.25, math.nan],
            "net_pnl_rs": [500.0, 99_000.0, -125.0, math.nan],
        }
    )
    row = today.metric_row(
        audit,
        strategy="V6_CONTROL",
        session=date(2026, 8, 28),
        source_complete=False,
        incomplete_symbol_sessions=210,
    )
    assert row["fills"] == 2
    assert row["wins"] == 1
    assert row["losses"] == 1
    assert row["profit_factor"] == 4.0
    assert row["net_return_points"] == 0.75
    assert row["net_pnl_rs"] == 375.0
    assert row["headline_valid"] is False


def test_bool_series_rejects_ambiguous_values() -> None:
    values = today._bool_series(pd.Series([True, "false", 1, 0, None]))
    assert values.tolist() == [True, False, True, False, False]
