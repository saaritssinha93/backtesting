"""Causal, strict-reference, and contract-roll tests for G-9 market data."""
import numpy as np
import pandas as pd
import pytest

from research import g9_market_features as m


def bars(day="2026-10-01", opening=100.0, step=0.1, vix=False):
    stamps = pd.date_range(day + " 09:20", periods=75, freq="5min", tz=m.IST)
    closes = opening + step * np.arange(1, 76)
    opens = np.r_[opening, closes[:-1]]
    frame = pd.DataFrame({"open": opens, "high": np.maximum(opens, closes) + 0.1,
        "low": np.minimum(opens, closes) - 0.1, "close": closes, "volume": 0})
    if vix:
        frame["date"] = stamps - pd.Timedelta(minutes=5)
    else:
        frame["timestamp"] = stamps
        frame["candle_start"] = stamps - pd.Timedelta(minutes=5)
        frame["quality_state"] = "VALID"
    return frame


def prepared(frame, vix=False):
    vix = vix or "date" in frame
    return m._prepare(frame, "date" if vix else "timestamp", vix)[0]


def day_grid(day="2026-10-01"):
    return m.compute_day(day, prepared(bars(day)), prepared(bars(day, vix=True)))


def test_first_completed_bar_uses_open_not_overnight_close():
    result = day_grid()
    assert str(result.iloc[0].available_at) == "2026-10-01 09:20:00+05:30"
    assert result.iloc[0].rv_pct == pytest.approx(np.log(100.1 / 100) * 100)
    assert result.iloc[0].vix_change_5m_pct == pytest.approx(0.1)
    assert result.iloc[1].vix_change_5m_pct == pytest.approx((100.2 / 100.1 - 1) * 100)


def test_missing_bar_invalidates_only_current_and_later_prefix():
    n = prepared(bars().drop(index=2))
    v = prepared(bars(vix=True))
    result = m.compute_day("2026-10-01", n, v)
    assert result.nifty_prefix_ready[:2].all()
    assert not result.nifty_prefix_ready[2:].any()
    assert result.rv_pct[2:].isna().all()


def test_invalid_future_bar_does_not_filter_earlier_day():
    raw = bars()
    raw.loc[30, "high"] = 1
    result = m.compute_day("2026-10-01", prepared(raw), prepared(bars(vix=True)))
    assert result.nifty_prefix_ready[:30].all()
    assert not result.nifty_prefix_ready[30:].any()


def test_vix_gap_not_bridged_or_treated_as_zero():
    result = m.compute_day("2026-10-01", prepared(bars()), prepared(bars(vix=True).drop(index=1)))
    assert result.vix_prefix_ready.iloc[0]
    assert result.vix_change_5m_pct[1:].isna().all()


def test_conflicting_duplicates_raise_identical_duplicates_audited():
    raw = bars()
    clean, duplicates = m._prepare(pd.concat([raw, raw.iloc[[0]]]), "timestamp")
    assert duplicates == 1 and len(clean) == 75
    extra = raw.iloc[[0]].copy()
    extra["close"] += .01
    with pytest.raises(ValueError, match="Conflicting"):
        m._prepare(pd.concat([raw, extra]), "timestamp")


def reference_grid(n=23):
    dates = pd.bdate_range("2026-08-03", periods=n).strftime("%Y-%m-%d")
    grid = pd.concat([day_grid(d) for d in dates], ignore_index=True)
    grid["rv_pct"] = grid.day.map({d: i + 1.0 for i, d in enumerate(dates)})
    return dates, grid


def test_reference_requires_twenty_and_excludes_current_day():
    days, grid = reference_grid()
    result = m.add_reference(grid)
    assert not result.loc[result.day.isin(days[:20]), "features_ready"].any()
    row = result.loc[result.day.eq(days[20])].iloc[0]
    assert row.rv_prior_count == 20
    assert row.rv_reference_median20 == 10.5
    assert row.rv_ratio == 2 and row.features_ready


def test_reference_does_not_compress_past_missing_session():
    days, grid = reference_grid()
    grid.loc[grid.day.eq(days[10]), "rv_pct"] = np.nan
    result = m.add_reference(grid)
    row = result.loc[result.day.eq(days[22])].iloc[0]
    assert row.rv_prior_count == 19
    assert not row.features_ready


def test_future_and_current_changes_do_not_change_reference():
    days, grid = reference_grid()
    before = m.add_reference(grid)
    grid.loc[grid.day.ge(days[20]), "rv_pct"] *= 100
    after = m.add_reference(grid)
    pd.testing.assert_frame_equal(before.loc[before.day.lt(days[20])], after.loc[after.day.lt(days[20])])
    assert after.loc[after.day.eq(days[20]), "rv_reference_median20"].eq(10.5).all()


def test_zero_reference_is_unavailable():
    days, grid = reference_grid()
    grid.loc[grid.day.isin(days[:20]), "rv_pct"] = 0
    result = m.add_reference(grid)
    assert not result.loc[result.day.eq(days[20]), "features_ready"].any()
    assert result.loc[result.day.eq(days[20]), "missing_reason"].eq("invalid_or_zero_reference").all()


def write_contract(rawdir, symbol, expiry, dates):
    frame = pd.concat([bars(d) for d in dates], ignore_index=True)
    frame["tradingsymbol"] = symbol
    frame["underlying"] = "NIFTY"
    frame["expiry"] = pd.Timestamp(expiry)
    frame.to_parquet(rawdir / (symbol + "_5minute.parquet"), index=False)


def test_absent_near_contract_does_not_roll_to_next_month(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    write_contract(raw, "NIFTY26AUGFUT", "2026-08-25", ["2026-08-20"])
    write_contract(raw, "NIFTY26SEPFUT", "2026-09-29", ["2026-08-20", "2026-08-21", "2026-08-26"])
    vix = tmp_path / "vix.csv"
    pd.concat([bars(d, vix=True) for d in ["2026-08-20", "2026-08-21", "2026-08-26"]]).to_csv(vix, index=False)
    result, proof = m.build_features(["2026-08-21", "2026-08-26"], vix, tmp_path / "out", raw_dir=raw)
    assert result.loc[result.day.eq("2026-08-21"), "nifty_contract"].eq("NIFTY26AUGFUT").all()
    assert not result.loc[result.day.eq("2026-08-21"), "nifty_prefix_ready"].any()
    assert result.loc[result.day.eq("2026-08-26"), "nifty_contract"].eq("NIFTY26SEPFUT").all()
    assert proof["history_sessions_required"] == 20
    assert proof["contract_chain_start"] == "2026-08-01"


def test_earlier_deferred_contract_not_silent_warmup(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    write_contract(raw, "NIFTY26AUGFUT", "2026-08-25", ["2026-07-29", "2026-08-03"])
    vix = tmp_path / "vix.csv"
    pd.concat([bars(d, vix=True) for d in ["2026-07-29", "2026-08-03"]]).to_csv(vix, index=False)
    result, proof = m.build_features(["2026-07-29", "2026-08-03"], vix, tmp_path / "out", raw_dir=raw)
    assert not result.loc[result.day.eq("2026-07-29"), "nifty_prefix_ready"].any()
    assert result.loc[result.day.eq("2026-08-03"), "nifty_prefix_ready"].all()
    assert proof["contract_chain_start"] == "2026-08-01"


def test_strict_twenty_cannot_silently_be_relaxed():
    with pytest.raises(ValueError, match="exactly 20"):
        m.add_reference(day_grid(), history_sessions=10)
