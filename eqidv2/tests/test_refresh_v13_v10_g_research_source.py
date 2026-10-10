import pandas as pd
import pytest

from tools.refresh_v13_v10_g_research_source import _check_overlap, _coverage


def test_overlap_refuses_changed_frozen_trade() -> None:
    old = pd.DataFrame([{
        "day": "2026-09-23", "hhmm_int": 926, "side": "LONG",
        "setup_id": "0926_LONG", "tradingsymbol": "TEST",
        "filled": True, "portfolio_executed": True, "exit_reason": "TARGET",
        "entry_ts": "2026-09-23T09:27:00+05:30",
        "exit_ts": "2026-09-23T10:00:00+05:30",
        "entry_price": 100.0, "exit_price": 101.0,
        "portfolio_net_profit_rupees": 1000.0,
        "portfolio_cost_rupees": 50.0,
    }])
    _check_overlap(old, old.copy())
    changed = old.copy()
    changed.loc[0, "exit_price"] = 101.01
    with pytest.raises(ValueError, match="historical overlap"):
        _check_overlap(old, changed)


def test_coverage_exposes_partial_oi_without_filling_it() -> None:
    rows = pd.DataFrame({
        "day": ["2026-10-08", "2026-10-08"],
        "tradingsymbol": ["TEST", "TEST"],
        "futures_tradingsymbol": ["TEST26OCTFUT", "TEST26OCTFUT"],
        "source_1m_count": [5, 5],
        "oi": [100.0, float("nan")],
    })
    coverage = _coverage(rows, ["2026-10-08"])["dates"]["2026-10-08"]
    assert coverage["equity_5m"]["total_bars"] == 2
    assert coverage["futures_oi_5m"]["nonnull_oi_bars"] == 1
    assert coverage["futures_oi_5m"]["partial_contracts"] == {"TEST26OCTFUT": 1}
