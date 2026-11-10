"""Accounting and source-alignment tests, without synthetic performance claims."""
from pathlib import Path

import pandas as pd
import pytest

from . import run as r


def test_output_is_isolated_and_never_overwrites(tmp_path):
    with pytest.raises(ValueError, match="NEW child"):
        r.fresh_output(tmp_path / "not_in_proxy")
    with pytest.raises(FileExistsError, match="overwrite"):
        r.fresh_output(Path(r.__file__))
    with pytest.raises(ValueError, match="NEW child"):
        r.fresh_output(r.PACKAGE)


def ledger():
    return pd.DataFrame([dict(day="2026-10-23", tradingsymbol="RELIANCE", setup_id="S", sid=1, signal_id="S1",
        signal_ts="2026-10-23 09:30:00+05:30", confirmation_ts="2026-10-23 09:31:00+05:30",
        filled=True, portfolio_executed=True, entry_price=100., exit_price=101., portfolio_net_profit_rupees=4750.,
        entry_ts="2026-10-23 09:32:00+05:30", exit_ts="2026-10-23 10:00:00+05:30",
        gross_return_pct=1., net_return_pct=.95, cost_pct=.05)])


def test_features_reject_missing_calendar_and_naive_clock():
    source = pd.DataFrame([dict(symbol="RELIANCE", bar_end=pd.Timestamp("2026-10-23 09:30", tz="Asia/Kolkata"),
                                open=100., high=101., low=99., close=100., volume=100.)])
    with pytest.raises(ValueError, match="every requested"):
        r.validate_feature_calendar(source, ["2026-10-29"])
    with pytest.raises(ValueError, match="every requested"):
        r.validate_feature_calendar(source.iloc[:0], ["2026-10-23"])
    valid = r.validate_feature_calendar(source, ["2026-10-23"])
    assert valid.bar_end.iloc[0] == source.bar_end.iloc[0]
    source["bar_end"] = source.bar_end.dt.tz_localize(None)
    with pytest.raises(ValueError, match="timezone-aware"):
        r.validate_feature_calendar(source, ["2026-10-23"])


def test_baseline_parity_checks_identity_and_money():
    original = ledger()
    assert r.baseline_parity(original, original.copy(), ["2026-10-23"])["passed"]
    changed = original.copy()
    changed.loc[0, "portfolio_net_profit_rupees"] += 1
    with pytest.raises(ValueError, match="portfolio_net_profit"):
        r.baseline_parity(changed, original, ["2026-10-23"])
    changed = original.copy()
    changed.loc[0, "tradingsymbol"] = "WRONG"
    with pytest.raises(ValueError, match="identities"):
        r.baseline_parity(changed, original, ["2026-10-23"])


def test_cost_stress_retains_gross_returns_and_converts_bps_once():
    original = ledger()
    out = r.stressed_book(original, 15, lambda frame: frame)
    assert out.gross_return_pct.iloc[0] == 1.
    assert out.net_return_pct.iloc[0] == pytest.approx(.85)
    assert out.cost_pct.iloc[0] == pytest.approx(.15)
    assert 500000 * (original.net_return_pct.iloc[0] - out.net_return_pct.iloc[0]) / 100 == pytest.approx(500.)
    assert original.cost_pct.iloc[0] == .05


def test_candle_differences_reported_without_mutation():
    stamp = pd.Timestamp("2026-10-23 09:30", tz="Asia/Kolkata")
    raw = pd.DataFrame({"tradingsymbol": ["A", "B"], "signal_ts": [stamp, stamp], "close": [100., 200.], "volume": [1000., 2000.]})
    fetched = pd.DataFrame({"symbol": ["A"], "bar_end": [stamp], "close": [100.05], "volume": [1000.]})
    before = raw.copy(deep=True)
    result = r.source_comparison(raw, fetched, "2026-10-23")
    assert result["missing_candle_rows"] == 1
    assert result["close_mismatch_rows"] == 1
    assert result["volume_mismatch_rows"] == 0
    pd.testing.assert_frame_equal(raw, before)


def test_accounting_rejects_wrong_cost_stress_or_entry_clock():
    book = ledger()
    overall = pd.DataFrame([dict(variant="base_control", period="all", cost_bps=c, trades=1,
                                net_pnl_rs=4750. - 50 * (c - 5)) for c in r.COSTS])
    daily = pd.DataFrame([dict(variant="base_control", day="2026-10-23", net_pnl_rs=4750.)])
    monthly = pd.DataFrame([dict(variant="base_control", month="2026-10", net_pnl_rs=4750.)])
    assert r.verify_accounting({"base_control": book}, overall, daily, monthly, ["2026-10-23"])
    cost_daily = pd.DataFrame([dict(variant="base_control", day="2026-10-23", cost_bps=c,
                                   net_pnl_rs=4750. - 50 * (c - 5)) for c in r.COSTS])
    cost_monthly = cost_daily.rename(columns={"day": "month"}).copy()
    assert r.verify_accounting({"base_control": book}, overall, cost_daily, cost_monthly, ["2026-10-23"])
    cost_daily.loc[cost_daily.cost_bps.eq(9), "net_pnl_rs"] += 1
    with pytest.raises(ValueError, match="daily/monthly"):
        r.verify_accounting({"base_control": book}, overall, cost_daily, cost_monthly, ["2026-10-23"])
    wrong = overall.copy()
    wrong.loc[wrong.cost_bps.eq(15), "net_pnl_rs"] += 1
    with pytest.raises(ValueError, match="Fixed-cost"):
        r.verify_accounting({"base_control": book}, wrong, daily, monthly, ["2026-10-23"])
    book.loc[0, "entry_ts"] = book.confirmation_ts.iloc[0]
    with pytest.raises(ValueError, match="chronology"):
        r.verify_accounting({"base_control": book}, overall, daily, monthly, ["2026-10-23"])
