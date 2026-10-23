import numpy as np
import pandas as pd
import pytest

from research.g8_stock_context import stock_context


def fixture(stamp="2026-10-22 09:41:00+05:30"):
    dates = pd.date_range("2026-10-22 09:16:00+05:30", "2026-10-22 10:00:00+05:30", freq="min")
    prices = 100. + np.arange(len(dates)) * .1
    raw = pd.DataFrame(dict(ts=dates, open=prices, high=prices+.2, low=prices-.2, close=prices+.1))
    confirm = raw.loc[raw.ts.eq(pd.Timestamp(stamp))].iloc[0]
    row = dict(sid=1, day="2026-10-22", side="LONG", confirmation_ts=stamp,
               **{"confirmation_"+f: float(confirm[f]) for f in ("open", "high", "low", "close")})
    nifty = dict(nifty_bar_ts=pd.Timestamp(stamp).floor("5min"), slot_candle=.25, session_open=1.)
    return raw, row, nifty


def test_matching_open_anchor_and_side():
    raw, row, nifty = fixture()
    feature = stock_context(raw, row, nifty)
    end = pd.Timestamp(row["confirmation_ts"]).floor("5min")
    window = raw.loc[raw.ts.between(end-pd.Timedelta(minutes=4), end)]
    expected = (window.iloc[-1].close/window.iloc[0].open-1)*100-.25
    assert feature["stock_relative5_pct"] == pytest.approx(expected)
    assert feature["stock_relative_session_pct"] == pytest.approx(
        (window.iloc[-1].close/raw.iloc[0].open-1)*100-1.)
    assert pd.Timestamp(feature["stock_context_available_at"]) == pd.Timestamp(row["confirmation_ts"])
    short = stock_context(raw, {**row, "side": "SHORT"}, nifty)
    assert short["stock_relative5_pct"] == pytest.approx(-expected)


def test_no_future_or_confirmation_atr_leak():
    raw, row, nifty = fixture()
    baseline = stock_context(raw, row, nifty)
    raw.loc[raw.ts.gt(pd.Timestamp(row["confirmation_ts"])), ["open", "high", "low", "close"]] *= 20
    assert stock_context(raw, row, nifty) == baseline
    # Confirmation's own extreme high cannot inflate the prior ATR denominator.
    row["confirmation_high"] += 10
    raw.loc[raw.ts.eq(pd.Timestamp(row["confirmation_ts"])), "high"] += 10
    changed = stock_context(raw, row, nifty)
    assert changed["stock_prior14_atr"] == baseline["stock_prior14_atr"]
    assert changed["confirmation_range_atr"] > baseline["confirmation_range_atr"]
    assert pd.Timestamp(changed["stock_atr_last_bar"]) < pd.Timestamp(row["confirmation_ts"])


def test_missing_or_flagged_history_stays_unavailable():
    raw, row, nifty = fixture()
    end = pd.Timestamp(row["confirmation_ts"]).floor("5min")
    missing = raw.loc[~raw.ts.eq(end-pd.Timedelta(minutes=2))]
    feature = stock_context(missing, row, nifty)
    assert feature["stock_relative5_status"] == "MISSING_OR_DUPLICATE_MINUTE"
    assert np.isnan(feature["stock_relative5_pct"])
    raw["gap_filled"] = False
    raw.loc[raw.ts.eq(end-pd.Timedelta(minutes=2)), "gap_filled"] = True
    assert stock_context(raw, row, nifty)["stock_relative5_status"] == "FLAGGED_INPUT"


def test_early_atr_missing_and_endpoint_mismatch_rejected():
    raw, row, nifty = fixture("2026-10-22 09:21:00+05:30")
    feature = stock_context(raw, row, nifty)
    assert np.isnan(feature["confirmation_range_atr"])
    assert feature["stock_relative5_status"] == "OK"
    with pytest.raises(ValueError, match="endpoints"):
        stock_context(raw, row, {**nifty, "nifty_bar_ts": pd.Timestamp(row["confirmation_ts"])})
