"""Point-in-time price-action checks for V14's frozen morning slots."""
from __future__ import annotations

import pandas as pd
import pytest

from research import g14_price_action_features as pa


DAY = "2026-10-28"


def row(sid: int, clock: str, *, symbol: str = "ABC", side: str = "LONG",
        o: float, h: float, low: float, c: float,
        co: float, ch: float, cl: float, cc: float) -> dict:
    signal = pd.Timestamp(f"{DAY} {clock}", tz=pa.IST)
    confirm = signal + pd.Timedelta(minutes=1)
    return dict(sid=sid, day=DAY, tradingsymbol=symbol, side=side,
                signal_ts=signal, confirmation_ts=confirm,
                open=o, high=h, low=low, close=c, signal_close=c,
                source_1m_count=5, confirmation_open=co,
                confirmation_high=ch, confirmation_low=cl,
                confirmation_close=cc, v9_5m_feature_ts=signal,
                v9_1m_feature_ts=confirm, v9_feature_available_ts=confirm,
                v9_exact_confirmation_present=True,
                confirmation_source_flagged=False)


def long_rows() -> pd.DataFrame:
    return pd.DataFrame([
        row(1, "09:20", o=100, h=101, low=99, c=100.8,
            co=100.8, ch=101.4, cl=100.6, cc=101.2),
        row(2, "09:25", o=100.8, h=102, low=100.5, c=101.5,
            co=101.5, ch=102, cl=101.4, cc=101.8),
        row(3, "09:30", o=101.5, h=103, low=100.8, c=102,
            co=102, ch=102.6, cl=101.9, cc=102.3),
    ], index=[11, 4, 9])


def test_long_exact_prior_breakout_and_bar_touch_hold() -> None:
    raw = long_rows()
    out = pa.enrich_day(raw, DAY)
    assert out.index.tolist() == raw.index.tolist()
    assert out.sid.tolist() == raw.sid.tolist()
    assert out.pa_prior_ready.tolist() == [False, True, True]
    assert out.pa_failed_breakout.tolist() == [False, False, True]
    assert out.pa_confirmation_ready.tolist() == [True, True, True]
    assert out.pa_confirmation_breakout.tolist() == [True, False, False]
    assert out.pa_touch_hold.tolist() == [True, True, True]
    assert out.pa_prior_signal_ts.iloc[1] == raw.signal_ts.iloc[0]
    assert out.pa_signal_feature_ts.iloc[0] == raw.signal_ts.iloc[0]
    assert out.pa_confirmation_feature_ts.iloc[0] == raw.confirmation_ts.iloc[0]


def test_short_failed_breakdown_and_confirmation_breakout() -> None:
    raw = pd.DataFrame([
        row(1, "09:20", symbol="XYZ", side="SHORT", o=100, h=101, low=99,
            c=99.3, co=99.3, ch=99.5, cl=98.8, cc=98.9),
        row(2, "09:25", symbol="XYZ", side="SHORT", o=99.3, h=100, low=98,
            c=99, co=99, ch=99.2, cl=98.7, cc=98.8),
    ])
    out = pa.enrich_day(raw, DAY)
    assert out.pa_prior_ready.tolist() == [False, True]
    assert out.pa_failed_breakout.tolist() == [False, True]
    assert out.pa_confirmation_breakout.tolist() == [True, False]
    assert out.pa_touch_hold.tolist() == [True, True]


def test_missing_intermediate_bar_never_becomes_exact_prior() -> None:
    raw = long_rows().iloc[[0, 2]].copy()
    out = pa.enrich_day(raw, DAY)
    assert out.pa_prior_ready.tolist() == [False, False]
    assert not out.pa_failed_breakout.any()


@pytest.mark.parametrize("flag", [
    "gap_filled", "opening_snapshot", "provisional_stale",
    "is_synthetic", "synthetic",
])
def test_flagged_prior_bar_cannot_make_price_action_reference(flag: str) -> None:
    raw = long_rows().iloc[:2].copy()
    raw[flag] = [True, False]
    out = pa.enrich_day(raw, DAY)
    assert not bool(out.pa_confirmation_ready.iloc[0])
    assert not bool(out.pa_prior_ready.iloc[1])
    assert not bool(out.pa_failed_breakout.iloc[1])


def test_late_or_flagged_confirmation_fails_closed() -> None:
    raw = long_rows().iloc[:2].copy()
    raw.loc[raw.index[0], "confirmation_ts"] += pd.Timedelta(minutes=1)
    raw.loc[raw.index[1], "confirmation_source_flagged"] = True
    out = pa.enrich_day(raw, DAY)
    assert out.pa_confirmation_ready.tolist() == [False, False]
    assert not out.pa_confirmation_breakout.any()
    assert not out.pa_touch_hold.any()


def test_future_feature_timestamp_cannot_support_confirmation() -> None:
    raw = long_rows().iloc[:1].copy()
    raw.loc[raw.index[0], "v9_1m_feature_ts"] += pd.Timedelta(minutes=1)
    out = pa.enrich_day(raw, DAY)
    assert not bool(out.pa_confirmation_ready.iloc[0])
    assert not bool(out.pa_confirmation_breakout.iloc[0])

\n