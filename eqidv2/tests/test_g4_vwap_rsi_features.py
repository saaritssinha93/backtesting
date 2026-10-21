import numpy as np
import pandas as pd
import pytest

from research import g4_vwap_rsi_features as study


def sample(side="LONG", closes=None, volume=None):
    closes = np.asarray([100.] * 34 + [110., 112., 114., 113., 112., 111.] if closes is None else closes)
    if side == "SHORT":
        closes = 200. - closes
    if volume is None:
        volume = np.r_[np.full(34, 100.), np.ones(6)] if len(closes) == 40 else np.ones(len(closes))
    raw = pd.DataFrame(dict(ts=pd.date_range("2026-10-14 09:16", periods=len(closes), freq="min", tz=study.IST),
        open=closes, high=closes + .1, low=closes - .1, close=closes, volume=volume))
    row = pd.Series(dict(side=side, confirmation_ts=raw.ts.iloc[-1],
        **{"confirmation_" + field: raw[field].iloc[-1] for field in ("open", "high", "low", "close")}))
    return raw, row


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_symmetric_extreme_and_weakening_rejection(side):
    raw, row = sample(side)
    result = study.vwap_rsi_features(raw, row)
    assert result["vwap_rsi_status"] == "APPLICABLE"
    assert result["vwap_rsi_flag"]
    assert result["vwap_rsi_extreme"] and result["vwap_rsi_momentum_weakening"]
    assert (result["rsi14_delta3"] < 0) == (side == "LONG")
    assert (result["session_vwap_z"] > 2.5) if side == "LONG" else (result["session_vwap_z"] < -2.5)


def test_weighted_population_band_hlc3_and_current_bar_are_explicit():
    raw, row = sample()
    raw.loc[0, "high"] += 3
    result = study.vwap_rsi_features(raw, row)
    typical = (raw.high + raw.low + raw.close) / 3
    expected_vwap = float((typical * raw.volume).sum() / raw.volume.sum())
    expected_sigma = float(np.sqrt((raw.volume * (typical - expected_vwap) ** 2).sum() / raw.volume.sum()))
    assert result["session_vwap"] == pytest.approx(expected_vwap)
    assert result["session_volume_weighted_sigma"] == pytest.approx(expected_sigma)
    assert result["session_vwap_upper_band"] == pytest.approx(expected_vwap + 2.5 * expected_sigma)
    assert result["vwap_rsi_session_bar_count"] == 40
    assert result["session_vwap"] != pytest.approx(float(typical.mean()))
    assert result["session_vwap"] != pytest.approx(float(np.average(typical[:-1], weights=raw.volume[:-1])))


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_both_conditions_required(side):
    raw, row = sample(side, closes=[100.] * 34 + [110., 112., 114., 115., 116., 117.])
    result = study.vwap_rsi_features(raw, row)
    assert result["vwap_rsi_extreme"]
    assert not result["vwap_rsi_momentum_weakening"]
    assert not result["vwap_rsi_flag"]
    raw, row = sample(side, volume=np.ones(40))
    result = study.vwap_rsi_features(raw, row)
    assert result["vwap_rsi_momentum_weakening"]
    assert not result["vwap_rsi_extreme"]
    assert not result["vwap_rsi_flag"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_equal_band_boundary_does_not_reject(monkeypatch, side):
    # Isolate strict band comparison at a numerically exact mean of 100.
    monkeypatch.setattr(study, "BAND_SIGMAS", 0.)
    raw, row = sample(side, closes=[100.] * 34 + [98., 100., 102., 101., 99., 100.], volume=np.ones(40))
    result = study.vwap_rsi_features(raw, row)
    assert result["session_vwap"] == row.confirmation_close
    assert result["vwap_rsi_momentum_weakening"]
    assert not result["vwap_rsi_extreme"]
    assert not result["vwap_rsi_flag"]


def test_excludes_previous_session_opening_snapshot_and_future_even_if_flagged():
    raw, row = sample()
    expected = study.vwap_rsi_features(raw, row)
    outside = raw.iloc[:3].copy()
    outside["ts"] = pd.DatetimeIndex([raw.ts.iloc[0] - pd.Timedelta(days=1),
        raw.ts.iloc[0] - pd.Timedelta(minutes=1), raw.ts.iloc[-1] + pd.Timedelta(minutes=1)])
    outside[["open", "high", "low", "close", "volume"]] = np.nan
    outside["gap_filled"] = True
    assert study.vwap_rsi_features(pd.concat([outside, raw], ignore_index=True), row) == expected


def test_naive_ist_and_utc_timestamps_produce_same_results():
    raw, row = sample()
    expected = study.vwap_rsi_features(raw, row)
    raw["ts"] = raw.ts.dt.tz_convert("UTC")
    row["confirmation_ts"] = row.confirmation_ts.tz_convert("UTC")
    assert study.vwap_rsi_features(raw, row) == expected
    raw["ts"] = raw.ts.dt.tz_convert(study.IST).dt.tz_localize(None)
    row["confirmation_ts"] = row.confirmation_ts.tz_convert(study.IST).tz_localize(None)
    assert study.vwap_rsi_features(raw, row) == expected


def test_arithmetic_seed_and_wilder_recurrence_not_ewm_seed():
    closes = np.array([100., 102., 101., 104., 102., 107., 106., 108., 104., 107., 106., 110., 108., 111., 110., 108.])
    rsi = study.wilder_rsi(closes)
    assert np.isnan(rsi[:14]).all()
    changes = np.diff(closes[:15])
    gain, loss = np.maximum(changes, 0).mean(), np.maximum(-changes, 0).mean()
    assert rsi[14] == pytest.approx(100. - 100. / (1. + gain / loss))
    assert rsi[15] == pytest.approx(100. - 100. / (1. + (gain * 13 / 14) / ((loss * 13 + 2) / 14)))
    assert study.wilder_rsi(np.full(20, 100.))[-1] == 50.
    assert study.wilder_rsi(np.arange(100., 120.))[-1] == 100.
    assert study.wilder_rsi(np.arange(120., 100., -1.))[-1] == 0.


def test_exact_three_minute_rsi_lag():
    raw, row = sample()
    result = study.vwap_rsi_features(raw, row)
    reference = study.wilder_rsi(raw.close.to_numpy())
    assert result["rsi14_lag3"] == reference[-4]
    assert result["rsi14"] == reference[-1]


def test_minimum_complete_session_is_not_an_accidental_missing_data_filter():
    raw, row = sample(closes=np.arange(100., 129.))
    result = study.vwap_rsi_features(raw, row)
    assert result["vwap_rsi_status"] == "NOT_APPLICABLE"
    assert result["vwap_rsi_reason"] == "INSUFFICIENT_SESSION_WARMUP"
    assert not result["vwap_rsi_flag"]
    raw, row = sample(closes=np.arange(100., 130.))
    assert study.vwap_rsi_features(raw, row)["vwap_rsi_status"] == "APPLICABLE"
    with pytest.raises(ValueError, match="Missing or duplicate"):
        study.vwap_rsi_features(raw.iloc[1:], row)


def test_zero_variance_preserves_candidate_without_invented_signal():
    raw, row = sample(closes=np.full(40, 100.))
    result = study.vwap_rsi_features(raw, row)
    assert result["vwap_rsi_status"] == "NOT_APPLICABLE"
    assert result["vwap_rsi_reason"] == "ZERO_VARIANCE"
    assert result["session_vwap_z"] is None
    assert not result["vwap_rsi_flag"]


@pytest.mark.parametrize("corruption", ["missing", "duplicate", "nan", "negative_volume", "zero_volume", "bad_high", "drift", "flag", "unknown_flag"])
def test_corrupt_observed_session_aborts_instead_of_filtering_trade(corruption):
    raw, row = sample()
    if corruption == "missing":
        raw = raw.drop(index=5)
    elif corruption == "duplicate":
        raw = pd.concat([raw, raw.iloc[[5]]], ignore_index=True)
    elif corruption == "nan":
        raw.loc[5, "close"] = np.nan
    elif corruption == "negative_volume":
        raw.loc[5, "volume"] = -1
    elif corruption == "zero_volume":
        raw["volume"] = 0
    elif corruption == "bad_high":
        raw.loc[5, "high"] = 90
    elif corruption == "drift":
        row["confirmation_close"] += .01
    elif corruption == "flag":
        raw["provisional_stale"] = False
        raw.loc[5, "provisional_stale"] = True
    else:
        raw["gap_filled"] = "false"
        raw.loc[5, "gap_filled"] = "unrecognized"
    with pytest.raises(ValueError):
        study.vwap_rsi_features(raw, row)


@pytest.mark.parametrize("bad_time", ["2026-10-14 09:15", "2026-10-14 15:31", "2026-10-14 09:55:30"])
def test_confirmation_must_be_completed_end_labelled_minute(bad_time):
    raw, row = sample()
    row["confirmation_ts"] = bad_time
    with pytest.raises(ValueError, match="regular completed session minute"):
        study.vwap_rsi_features(raw, row)
