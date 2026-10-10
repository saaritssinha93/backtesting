"""Extension accounting must retain completed zero-trade sessions explicitly."""
import pandas as pd
import pytest

from research.g3_extend_results import reconcile


def frames():
    trades = pd.DataFrame([
        dict(trade_id="2026-10-08|A|STOCK|1", day="2026-10-08", net_pnl=125., cost=25., gross_pnl=150.),
    ])
    daily = pd.DataFrame([
        dict(day="2026-10-08", trades=1, net_pnl=125., cost=25., gross_pnl=150., cumulative_net_pnl=125.),
        dict(day="2026-10-09", trades=0, net_pnl=0., cost=0., gross_pnl=0., cumulative_net_pnl=125.),
    ])
    return daily, trades


def test_completed_zero_trade_session_is_preserved():
    daily, trades = frames()
    reconcile(daily, trades, ["2026-10-08", "2026-10-09"])


def test_invented_or_missing_session_is_rejected():
    daily, trades = frames()
    with pytest.raises(ValueError, match="calendar"):
        reconcile(daily, trades, ["2026-10-08"])
    with pytest.raises(ValueError, match="calendar"):
        reconcile(daily.iloc[:1], trades, ["2026-10-08", "2026-10-09"])


def test_duplicate_trade_cannot_inflate_extension():
    daily, trades = frames()
    with pytest.raises(ValueError, match="Duplicated trade"):
        reconcile(daily, pd.concat([trades, trades]), list(daily.day))


@pytest.mark.parametrize("column", ["net_pnl", "cost", "gross_pnl", "trades", "cumulative_net_pnl"])
def test_extension_accounting_drift_is_rejected(column):
    daily, trades = frames()
    daily.loc[0, column] += 1
    with pytest.raises(ValueError, match="mismatch"):
        reconcile(daily, trades, list(daily.day))
