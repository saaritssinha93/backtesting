"""Point-in-time and sign checks for V14's optional momentum features."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research import g14_momentum_features as features


DAY = "2026-09-18"
CONTRACT = "NIFTY26SEPFUT"


def market(*clocks: str) -> pd.DataFrame:
    prices = {
        "09:20": (100.0, 100.5),
        "09:25": (100.5, 101.0),
        "09:30": (101.0, 500.0),  # Obvious future-data sentinel.
        "09:35": (101.0, 101.2),
        "09:40": (101.2, 101.4),
    }
    return pd.DataFrame([
        dict(ts=pd.Timestamp(f"{DAY} {clock}", tz=features.IST),
             open=prices[clock][0], close=prices[clock][1],
             high=max(prices[clock]), low=min(prices[clock]),
             tradingsymbol=CONTRACT)
        for clock in clocks
    ])


def row(sid: int, clock: str, *, side: str, opening: float,
        close: float, previous: float) -> dict:
    pct = 100 * (close / (opening if clock == "09:20" else previous) - 1)
    assert (pct > 0) == (side == "LONG")
    return dict(sid=sid, day=DAY,
                signal_ts=pd.Timestamp(f"{DAY} {clock}", tz=features.IST),
                tradingsymbol="ABC", side=side, contract_month="26SEP",
                open=opening, close=close, prev_close=previous,
                price_change_pct=pct, source_1m_count=5,
                opening_0920_status="VALID",
                price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE" if clock == "09:20" else "PREVIOUS_COMPLETED_5M_CLOSE",
                v18_stock_from_open_pct=100 * (close / 100.0 - 1),
                v18_stock_ready=True)


def supply(monkeypatch, frame: pd.DataFrame) -> None:
    monkeypatch.setattr(features.sources, "_nifty_day", lambda day: (
        frame, dict(nifty_contract=CONTRACT,
                    nifty_source_kind="TEST_FROZEN", nifty_snapshot_sha256="abc123")))


def test_signs_exact_prior_and_session_relative_strength(monkeypatch):
    supply(monkeypatch, market("09:20", "09:25", "09:30"))
    raw = pd.DataFrame([
        row(1, "09:20", side="LONG", opening=100, close=101, previous=100),
        row(2, "09:25", side="SHORT", opening=101, close=100.5, previous=101),
    ], index=[7, 3])
    out = features.enrich_day(raw, DAY)
    assert out.index.tolist() == raw.index.tolist()
    assert out.sid.tolist() == raw.sid.tolist()
    assert out.mom_nifty_ready.tolist() == [True, True]
    assert out.mom_stock_prev_bar_ready.tolist() == [False, True]
    assert np.isnan(out.mom_stock_prev_bar_signed_pct.iloc[0])
    assert out.mom_stock_prev_bar_signed_pct.iloc[1] == pytest.approx(-1.0)
    assert out.mom_nifty_session_signed_pct.iloc[0] == pytest.approx(.5)
    assert out.mom_nifty_session_signed_pct.iloc[1] == pytest.approx(-1.0)
    assert out.mom_nifty_bar_signed_pct.iloc[1] == pytest.approx(
        -100 * (101 / 100.5 - 1))
    assert out.mom_stock_bar_signed_pct.iloc[1] == pytest.approx(
        -100 * (100.5 / 101 - 1))
    assert out.mom_stock_relative_open_signed_pct.iloc[1] == pytest.approx(.5)
    assert out.mom_stock_relative_bar_signed_pct.iloc[1] == pytest.approx(
        out.mom_stock_bar_signed_pct.iloc[1] - out.mom_nifty_bar_signed_pct.iloc[1])


def test_future_nifty_bar_does_not_change_earlier_signal(monkeypatch):
    raw = pd.DataFrame([row(2, "09:25", side="SHORT", opening=101,
                            close=100.5, previous=101)])
    supply(monkeypatch, market("09:20", "09:25"))
    earlier = features.enrich_day(raw, DAY)
    supply(monkeypatch, market("09:20", "09:25", "09:30"))
    with_future_bar = features.enrich_day(raw, DAY)
    for column in ("mom_nifty_bar_signed_pct", "mom_nifty_session_signed_pct",
                   "mom_stock_relative_bar_signed_pct"):
        assert earlier[column].iloc[0] == with_future_bar[column].iloc[0]
    assert pd.Timestamp(with_future_bar.mom_nifty_bar_ts.iloc[0]) <= raw.signal_ts.iloc[0]


def test_september_18_missing_0930_nifty_fails_only_that_clock(monkeypatch):
    supply(monkeypatch, market("09:20", "09:25", "09:35"))
    raw = pd.DataFrame([
        row(3, "09:30", side="LONG", opening=100, close=101, previous=100),
        row(4, "09:35", side="LONG", opening=101, close=102, previous=101),
    ])
    out = features.enrich_day(raw, DAY)
    assert out.mom_nifty_status.tolist() == [
        "MISSING_EXACT_NIFTY_BAR", "MISSING_PREVIOUS_NIFTY_BAR"]
    assert out.mom_nifty_ready.tolist() == [False, True]
    assert np.isnan(out.mom_nifty_bar_signed_pct.iloc[0])
    assert np.isfinite(out.mom_nifty_bar_signed_pct.iloc[1])


def test_prior_stock_bar_requires_exact_adjacency_and_matching_close(monkeypatch):
    supply(monkeypatch, market("09:20", "09:25", "09:30"))
    opening = row(1, "09:20", side="LONG", opening=100, close=101, previous=100)
    thirty = row(3, "09:30", side="LONG", opening=101, close=102, previous=101)
    no_0925 = features.enrich_day(pd.DataFrame([opening, thirty]), DAY)
    assert not no_0925.mom_stock_prev_bar_ready.iloc[1]
    assert np.isnan(no_0925.mom_stock_prev_bar_signed_pct.iloc[1])
    twentyfive = row(2, "09:25", side="LONG", opening=101, close=102, previous=101)
    thirty["prev_close"] = 100.0  # The supposed adjacent close was actually 102.
    thirty["price_change_pct"] = 100 * (102 / 100 - 1)
    wrong_previous = features.enrich_day(pd.DataFrame([opening, twentyfive, thirty]), DAY)
    assert not wrong_previous.mom_stock_prev_bar_ready.iloc[2]


def test_stock_open_relative_strength_requires_verified_opening(monkeypatch):
    supply(monkeypatch, market("09:20", "09:25"))
    opening = row(1, "09:20", side="LONG", opening=100, close=101, previous=100)
    later = row(2, "09:25", side="SHORT", opening=101, close=100.5, previous=101)
    later["v18_stock_from_open_pct"] = 99.0  # Not the value at signal.
    out = features.enrich_day(pd.DataFrame([opening, later]), DAY)
    assert out.mom_stock_bar_ready.tolist() == [True, True]
    assert out.mom_relative_open_ready.tolist() == [True, False]
    assert np.isnan(out.mom_stock_relative_open_signed_pct.iloc[1])


def test_late_recovered_row_uses_exact_open_close_when_v18_absent(monkeypatch):
    supply(monkeypatch, market("09:20", "09:40"))
    opening = row(1, "09:20", side="LONG", opening=100, close=101, previous=100)
    late = row(5, "09:40", side="LONG", opening=102, close=103, previous=102)
    late["v18_stock_ready"] = None
    late["v18_stock_from_open_pct"] = np.nan
    out = features.enrich_day(pd.DataFrame([opening, late]), DAY)
    assert out.mom_relative_open_ready.tolist() == [True, True]
    assert out.mom_stock_open_source.tolist() == [
        "V18_VERIFIED", "RAW_OPEN_CLOSE_RECONSTRUCTED"]
    assert out.mom_stock_from_open_pct.iloc[1] == pytest.approx(3.0)
    assert out.mom_stock_relative_open_signed_pct.iloc[1] == pytest.approx(
        3.0 - 1.4)


def test_cash_open_can_anchor_later_momentum_when_opening_oi_missing(monkeypatch):
    supply(monkeypatch, market("09:20", "09:25"))
    opening = row(1, "09:20", side="LONG", opening=100, close=101, previous=100)
    opening["opening_0920_status"] = "MISSING_OR_INVALID_OPENING_EVIDENCE"
    later = row(2, "09:25", side="SHORT", opening=101, close=100.5, previous=101)
    out = features.enrich_day(pd.DataFrame([opening, later]), DAY)
    assert out.mom_relative_open_ready.tolist() == [True, True]
    assert out.mom_stock_relative_open_signed_pct.iloc[1] == pytest.approx(.5)


def test_mismatched_futures_contract_fails_market_feature(monkeypatch):
    supply(monkeypatch, market("09:20"))
    raw = pd.DataFrame([row(1, "09:20", side="LONG", opening=100,
                            close=101, previous=100)])
    raw.loc[0, "contract_month"] = "26OCT"
    out = features.enrich_day(raw, DAY)
    assert not out.mom_nifty_ready.iloc[0]
    assert np.isnan(out.mom_nifty_session_signed_pct.iloc[0])
