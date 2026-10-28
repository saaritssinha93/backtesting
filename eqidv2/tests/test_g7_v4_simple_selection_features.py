import numpy as np
import pandas as pd
import pytest

from research.g7_v4_simple_selection_features import feature_row


def example(n=25, side="LONG"):
    ts = pd.date_range("2026-10-26 09:16", periods=n + 10, freq="min", tz="Asia/Kolkata")
    close = 100 + np.arange(len(ts)) * .1
    raw = pd.DataFrame(dict(ts=ts, open=close - .05, high=close + .15, low=close - .25,
                            close=close, volume=np.arange(len(ts), dtype=float) + 1))
    bar = raw.iloc[n - 1]
    row = dict(sid=1, day="2026-10-26", side=side, confirmation_ts=bar.ts,
               trigger=bar.high if side == "LONG" else bar.low)
    row.update({"confirmation_" + key: bar[key] for key in ("open", "high", "low", "close")})
    return raw, row


def sync(raw, row):
    bar = raw.loc[raw.ts.eq(row["confirmation_ts"])].iloc[0]
    row.update({"confirmation_" + key: bar[key] for key in ("open", "high", "low", "close")})


def test_formula_cmf_volume_weighting_and_exact_five_minute_vwap_lag():
    raw, row = example()
    raw.loc[20, "close"] = raw.loc[20, "low"]
    result = feature_row(raw, row)
    prefix = raw.iloc[:25]
    last = prefix.tail(20)
    cmf = np.average((2 * last.close - last.high - last.low) / (last.high - last.low), weights=last.volume)
    assert result["sel_signed_cmf20"] == pytest.approx(cmf)
    now = np.average(prefix[["high", "low", "close"]].mean(axis=1), weights=prefix.volume)
    lag_prefix = prefix.iloc[:-5]
    lag = np.average(lag_prefix[["high", "low", "close"]].mean(axis=1), weights=lag_prefix.volume)
    assert result["sel_signed_vwap_slope5_pct"] == pytest.approx((now / lag - 1) * 100)
    assert result["feature_available_at"] == pd.Timestamp(row["confirmation_ts"]).isoformat()


def test_directional_features_and_short_signs():
    raw, row = example()
    long = feature_row(raw, row)
    row["side"] = "SHORT"
    short = feature_row(raw, row)
    for key in ("sel_signed_cmf20", "sel_signed_vwap_slope5_pct", "sel_directional_body_return_pct",
                "sel_trigger_vs_confirmation_close_pct"):
        assert short[key] == pytest.approx(-long[key])
    assert long["sel_directional_close_location"] == pytest.approx(.625)
    assert short["sel_directional_close_location"] == pytest.approx(.375)
    assert long["sel_directional_wick_fraction"] == pytest.approx(.375)
    assert short["sel_directional_wick_fraction"] == pytest.approx(.5)


@pytest.mark.parametrize("n,cmf,vwap,prior", [(1, False, False, False), (5, False, False, False),
                           (6, False, False, True), (9, False, False, True),
                           (10, False, True, True), (19, False, True, True), (20, True, True, True)])
def test_warmup_uses_only_complete_session_prefix(n, cmf, vwap, prior):
    raw, row = example(n)
    result = feature_row(raw, row)
    assert result["sel_cmf20_ready"] is cmf
    assert result["sel_vwap_slope5_ready"] is vwap
    assert result["sel_prior5_ready"] is prior
    if not cmf:
        assert result["sel_signed_cmf20"] is None
        assert result["sel_cmf20_status"] == "NOT_APPLICABLE_WARMUP"


def test_flat_prices_and_zero_volume_are_valid_unavailable_weights():
    raw, row = example()
    raw[["open", "high", "low", "close"]] = 100.
    sync(raw, row)
    result = feature_row(raw, row)
    assert result["sel_signed_cmf20"] == 0
    assert result["sel_signed_vwap_slope5_pct"] == 0
    assert result["sel_directional_close_location"] == .5
    assert result["sel_directional_wick_fraction"] == 0
    raw["volume"] = 0
    zero = feature_row(raw, row)
    assert not zero["sel_cmf20_ready"] and not zero["sel_vwap_slope5_ready"]
    assert zero["sel_cmf20_status"] == zero["sel_vwap_slope5_status"] == "NOT_APPLICABLE_ZERO_VOLUME"


def test_recent_zero_cmf_volume_and_lag_zero_vwap_are_explicitly_unavailable():
    raw, row = example()
    raw.loc[5:24, "volume"] = 0
    result = feature_row(raw, row)
    assert not result["sel_cmf20_ready"] and result["sel_vwap_slope5_ready"]
    raw["volume"] = 0
    raw.loc[22:24, "volume"] = 10
    result = feature_row(raw, row)
    assert result["sel_cmf20_ready"] and not result["sel_vwap_slope5_ready"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_actual_confirmation_extreme_must_break_prior_five_to_fail(side):
    raw, row = example(side=side)
    raw.loc[:24, ["open", "high", "low", "close"]] = [100., 101., 99., 100.]
    raw.loc[24, "high" if side == "LONG" else "low"] = 102. if side == "LONG" else 98.
    sync(raw, row)
    row["trigger"] = 120. if side == "LONG" else 80.  # Trigger alone cannot establish a breakout.
    result = feature_row(raw, row)
    assert result["sel_prior5_breaks"] and result["sel_prior5_failed_breakout"]
    raw.loc[24, "close"] = 101.5 if side == "LONG" else 98.5
    sync(raw, row)
    assert not feature_row(raw, row)["sel_prior5_failed_breakout"]
    raw.loc[24, ["high", "low", "close"]] = [101., 99., 100.]
    sync(raw, row)
    result = feature_row(raw, row)
    assert not result["sel_prior5_breaks"] and not result["sel_prior5_failed_breakout"]


def test_future_bars_other_sessions_and_outcomes_cannot_change_features():
    raw, row = example()
    before = feature_row(raw, row)
    raw.loc[raw.ts.gt(row["confirmation_ts"]), ["open", "high", "low", "close", "volume"]] = np.nan
    raw["is_synthetic"] = raw.ts.gt(row["confirmation_ts"])
    row.update(net_profit_rupees=-1e20, exit_reason="STOP", entry_ts="2030-01-01")
    assert feature_row(raw, row) == before
    earlier = raw.iloc[:2].copy()
    earlier.ts -= pd.Timedelta(days=1)
    earlier["open"] = np.nan
    assert feature_row(pd.concat([earlier, raw]), row) == before


@pytest.mark.parametrize("bad", ["missing", "duplicate", "synthetic", "gap", "unknownflag", "ohlc", "negativevol", "nanvol", "confirm", "day", "side", "trigger", "clock"])
def test_corrupt_or_noncausal_inputs_raise(bad):
    raw, row = example()
    if bad == "missing": raw = raw.drop(index=3)
    if bad == "duplicate": raw = pd.concat([raw, raw.iloc[[3]]])
    if bad == "synthetic": raw["is_synthetic"] = False; raw.loc[3, "is_synthetic"] = True
    if bad == "gap": raw["gap_filled"] = False; raw.loc[3, "gap_filled"] = True
    if bad == "unknownflag": raw["opening_snapshot"] = "unknown"
    if bad == "ohlc": raw.loc[3, "high"] = 1
    if bad == "negativevol": raw.loc[3, "volume"] = -1
    if bad == "nanvol": raw.loc[3, "volume"] = np.nan
    if bad == "confirm": row["confirmation_close"] += 1
    if bad == "day": row["day"] = "2026-10-27"
    if bad == "side": row["side"] = "BUY"
    if bad == "trigger": row["trigger"] = np.nan
    if bad == "clock": row["confirmation_ts"] += pd.Timedelta(seconds=1)
    with pytest.raises(ValueError):
        feature_row(raw, row)


def test_naive_or_utc_source_times_have_same_completed_ist_result():
    raw, row = example()
    expected = feature_row(raw, row)
    raw.ts = raw.ts.dt.tz_convert("UTC")
    assert feature_row(raw, row) == expected
    raw.ts = raw.ts.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    assert feature_row(raw, row) == expected
