"""The opening SHORT gets bounded NIFTY retries without holding stock readiness."""
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_oi_fetch_5min as fetcher
import fno_v13_v10_g_3_contract as contract


@pytest.fixture
def run_fetch(monkeypatch):
    monkeypatch.setenv("FNO_ACTIVE_STRATEGY_PROFILE", "V13_V10_G_3")
    for name in ("atomic_write_json", "atomic_write_text", "publish_status", "append_observation"):
        monkeypatch.setattr(fetcher.common, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(fetcher, "_cash_marker_state", lambda slot: (True, "complete"))
    delays = []
    monkeypatch.setattr(fetcher.time, "sleep", delays.append)

    def run(*, ready_on=None, clock="09:20", retries=2, include_nifty=True):
        underlyings = ["RELIANCE", "NIFTYFPI"] + (["NIFTY"] if include_nifty else [])
        universe = pd.DataFrame([dict(exchange="NFO", tradingsymbol=name + "26OCTFUT",
            underlying=name, instrument_token=30000 + index, exchange_token=40000 + index,
            expiry=pd.Timestamp("2026-10-27"), lot_size=100, tick_size=.05,
            is_index_future=name != "RELIANCE") for index, name in enumerate(underlyings)])
        calls = []

        def outcomes(frame, *args, **kwargs):
            calls.append(list(frame.tradingsymbol))
            return [dict(tradingsymbol=symbol, state="WRITTEN" if symbol.startswith("RELIANCE")
                or (symbol == "NIFTY26OCTFUT" and ready_on is not None and len(calls) >= ready_on)
                else "NO_CANDLE") for symbol in frame.tradingsymbol]

        args = SimpleNamespace(max_retries=1, slot_retry_attempts=retries,
            slot_retry_delay_sec=2., min_coverage=.99)
        slot = datetime.fromisoformat("2026-10-16T" + clock + ":00+05:30")
        marker = fetcher.run_slot(slot, universe, [SimpleNamespace(app_name="test")],
            args, fetch_contracts_impl=outcomes)
        return marker, calls, delays

    return run


def test_delayed_nifty_only_is_retried_and_arrives(run_fetch):
    marker, calls, delays = run_fetch(ready_on=2)
    assert len(calls) == 2 and calls[1] == ["NIFTY26OCTFUT"]
    assert delays == [2.]
    assert marker["strategy_required_index_state"] == "READY"
    assert marker["strategy_required_index_missing_symbols"] == []
    assert marker["stock_complete"] is True and marker["complete"] is True
    assert marker["index_no_candle_symbols"] == ["NIFTYFPI26OCTFUT"]


@pytest.mark.parametrize("budget", [0, 2])
def test_still_missing_nifty_uses_only_existing_budget_and_is_disclosed(run_fetch, budget):
    marker, calls, delays = run_fetch(retries=budget)
    assert len(calls) == budget + 1 and len(delays) == budget
    assert all(symbols == ["NIFTY26OCTFUT"] for symbols in calls[1:])
    assert marker["slot_retry_attempts_used"] == budget
    assert marker["strategy_required_index_state"] == "MISSING_AFTER_BOUNDED_RETRIES"
    assert marker["strategy_required_index_missing_symbols"] == ["NIFTY26OCTFUT"]
    assert marker["stock_complete"] is True and marker["complete"] is True
    report = fetcher._render_fetch_report(marker, [])
    assert "Opening SHORT index context: MISSING_AFTER_BOUNDED_RETRIES" in report
    # A missing bar must never be turned into an acceptable context return.
    row = dict(ema9=98., ema20=99., ema50=100., price_change_pct=-.2,
        oi_change_pct=.1, volume_ratio=2., oi=1001., prev_oi=1000.,
        opening_0920_status="VALID", price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi_change_source="FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M")
    assert contract.base_signal_side(row, "09:20", nifty_first_bar_return_pct=None) is None
    assert contract.base_signal_side(row, "09:20", nifty_first_bar_return_pct=-.1) == "SHORT"


@pytest.mark.parametrize("profile,clock", [("V13_V10_G", "09:20"), ("V13_V10_G_3", "09:25")])
def test_other_profile_or_slot_keeps_optional_index_early_exit(run_fetch, monkeypatch, profile, clock):
    monkeypatch.setenv("FNO_ACTIVE_STRATEGY_PROFILE", profile)
    marker, calls, delays = run_fetch(clock=clock)
    assert len(calls) == 1 and delays == []
    assert marker["strategy_required_index_state"] == "NOT_REQUIRED"
    assert marker["complete"] is True


def test_unconfigured_opening_short_does_not_extend_fetch(run_fetch, monkeypatch):
    monkeypatch.setattr(contract, "setup_for", lambda *args, **kwargs: None)
    marker, calls, _ = run_fetch()
    assert len(calls) == 1
    assert marker["strategy_required_index_state"] == "NOT_REQUIRED"


def test_missing_nifty_universe_is_explicit_without_unbounded_retries(run_fetch):
    marker, calls, delays = run_fetch(include_nifty=False)
    assert len(calls) == 1 and delays == []
    assert marker["strategy_required_index_state"] == "MISSING_UNIVERSE"
    assert marker["strategy_required_index_underlyings"] == ["NIFTY"]
