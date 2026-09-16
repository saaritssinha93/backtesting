from datetime import date

import numpy as np
import pandas as pd
import pytest

import fno_v13_v9_data as data


def _minutes():
    frames = []
    for day, base in (("2026-08-03", 100.0), ("2026-08-04", 102.0)):
        ts = pd.date_range(f"{day} 09:16", periods=40, freq="min", tz=data.common.IST)
        close = base + np.arange(len(ts)) * .03
        frames.append(pd.DataFrame({"ts": ts, "open": close - .02, "high": close + .05, "low": close - .06, "close": close, "volume": 100.0 + np.arange(len(ts))}))
    return pd.concat(frames, ignore_index=True)


def test_all_feature_values_invariant_to_future_mutation():
    minute = _minutes()
    cutoff = minute.loc[50, "ts"]
    baseline = data.causal_bar_features(minute, "v9_1m_")
    modified = minute.copy()
    modified.loc[modified.ts.gt(cutoff), ["open", "high", "low", "close", "volume"]] *= 50
    changed = data.causal_bar_features(modified, "v9_1m_")
    pd.testing.assert_frame_equal(baseline.loc[baseline.ts.le(cutoff), data.feature_columns(baseline)], changed.loc[changed.ts.le(cutoff), data.feature_columns(changed)])
    prefix = data.causal_bar_features(minute.loc[minute.ts.le(cutoff)], "v9_1m_")
    pd.testing.assert_frame_equal(baseline.loc[baseline.ts.le(cutoff), data.feature_columns(baseline)].reset_index(drop=True), prefix[data.feature_columns(prefix)])


def test_session_gap_only_uses_previous_session_final_close():
    minute = _minutes()
    out = data.causal_bar_features(minute, "v9_1m_")
    expected = (minute.loc[40, "open"] / minute.loc[39, "close"] - 1) * 100
    assert out.loc[40:, "v9_1m_gap_pct"].eq(expected).all()
    assert out.loc[:39, "v9_1m_gap_pct"].isna().all()


def test_observed_pool_retains_missing_oi_and_missing_exact_confirmation():
    minute = _minutes()
    removed = pd.Timestamp("2026-08-04 09:26", tz=data.common.IST)
    minute = minute.loc[minute.ts.ne(removed)]
    pool = data.observed_pool(minute, pd.DataFrame(), days={date(2026, 8, 4)}, equity_symbol="TEST", futures_symbol="TEST26AUGFUT", contract_month="26AUG")
    row = pool.loc[pool.hhmm.eq("0925")].iloc[0]
    assert pd.isna(row["oi_change_pct"])
    assert not row["v9_exact_confirmation_present"]
    assert pd.isna(row["v9_1m_feature_ts"])
    assert pool.loc[pool.hhmm.eq("0930")].empty  # incomplete aggregation never fabricated
    data.assert_feature_chronology(pool)


def test_five_minute_features_and_confirmation_are_prefix_causal():
    minute = _minutes()
    five = data.hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    future = five[["ts", "open", "high", "low", "close", "volume"]].copy()
    future["oi"] = 10000 + np.arange(len(future)) * 100
    kwargs = dict(days={date(2026, 8, 4)}, equity_symbol="TEST", futures_symbol="TEST26AUGFUT", contract_month="26AUG")
    out = data.observed_pool(minute, future, **kwargs)
    cutoff = pd.Timestamp("2026-08-04 09:36", tz=data.common.IST)
    prefix = data.observed_pool(minute.loc[minute.ts.le(cutoff)], future.loc[future.ts.le(cutoff)], **kwargs)
    columns = ["signal_ts", *data.feature_columns(out)]
    pd.testing.assert_frame_equal(out.loc[out.confirmation_ts.le(cutoff), columns].reset_index(drop=True), prefix.loc[prefix.confirmation_ts.le(cutoff), columns].reset_index(drop=True))


def test_feature_chronology_fails_closed_on_later_minute():
    stamp = pd.Timestamp("2026-08-04 09:25", tz=data.common.IST)
    frame = pd.DataFrame({"signal_ts": [stamp], "confirmation_ts": [stamp + pd.Timedelta(minutes=1)], "v9_5m_feature_ts": [stamp], "v9_1m_feature_ts": [stamp + pd.Timedelta(minutes=2)]})
    with pytest.raises(AssertionError, match="chronology"):
        data.assert_feature_chronology(frame)


def test_outcome_column_cannot_enter_feature_namespace():
    stamp = pd.Timestamp("2026-08-04 09:25", tz=data.common.IST)
    frame = pd.DataFrame({"signal_ts": [stamp], "confirmation_ts": [stamp], "v9_5m_feature_ts": [stamp], "v9_1m_feature_ts": [stamp], "v9_1m_net_return": [1.]})
    with pytest.raises(AssertionError, match="Outcome"):
        data.assert_feature_chronology(frame)


def test_setup_audit_keeps_ranked_out_and_multiple_rejection_reasons(monkeypatch):
    stamp = pd.Timestamp("2026-08-04 09:25", tz=data.common.IST)
    base = dict(day=stamp.date(), signal_ts=stamp, confirmation_ts=stamp + pd.Timedelta(minutes=1), hhmm="0925", hhmm_int=925,
                contract_month="26AUG", oi=10050., prev_oi=10000., oi_change_pct=.5, price_change_pct=.5, volume_ratio=4., body_ratio=.8,
                signal_close=100., confirmation_open=100.1, confirmation_close=100.5, confirmation_high=100.6, confirmation_low=100.,
                v9_5m_ema_bull=True, v9_5m_ema_bear=False, v9_exact_confirmation_present=True, v9_1m_upper_wick_ratio=.1, v9_1m_lower_wick_ratio=.1)
    pool = pd.DataFrame([{**base, "tradingsymbol": symbol, "futures_tradingsymbol": symbol + "26AUGFUT", "traded_value": value} for symbol, value in (("TOP", 300.), ("SECOND", 200.), ("REJECT", 100.))])
    pool.loc[pool.tradingsymbol.eq("REJECT"), ["volume_ratio", "body_ratio"]] = [.1, .1]
    native = pool.iloc[:2].copy()
    native["sid"] = [7, 9]
    native["side"] = "LONG"
    native["wick_ratio"] = .1
    native["trigger"] = native["confirmation_high"]
    monkeypatch.setattr(data.v5.v13_v3, "load_nifty_first_bar_context", lambda months: pd.DataFrame({"contract_month": ["26AUG"], "day": [stamp.date()], "nifty_first_bar_return_pct": [-.2]}))
    audit = data.make_setup_audit(pool, native, native)
    long_rows = audit.loc[audit.side.eq("LONG")].set_index("tradingsymbol")
    assert long_rows.loc["TOP", "selection_status"] == "SELECTED"
    assert long_rows.loc["SECOND", "selection_status"] == "RANKED_OUT"
    assert long_rows.loc["SECOND", "baseline_setup_eligible"]
    assert long_rows.loc["REJECT", "selection_status"] == "FILTER_REJECTED"
    assert "SETUP_VOLUME" in long_rows.loc["REJECT", "causal_rejection_reasons"]
    assert "SETUP_BODY" in long_rows.loc["REJECT", "causal_rejection_reasons"]
    assert audit.candidate_id.is_unique
    assert len(audit) == 6  # each observed stock at both original 09:25 sides


def test_confirmation_morphology_matches_native_float64_from_float32_storage():
    minute = _minutes().iloc[:1].copy()
    values = {"open": 1138.0, "high": 1138.900024, "low": 1137.699951, "close": 1138.900024}
    for field, value in values.items():
        minute[field] = np.float32(value)
    out = data.causal_bar_features(minute, "v9_1m_").iloc[0]
    o, h, l, c = [float(minute.iloc[0][field]) for field in ("open", "high", "low", "close")]
    assert out["v9_1m_body_ratio"] == abs(c - o) / (h - l)
    assert out["v9_1m_upper_wick_ratio"] == (h - max(o, c)) / (h - l)


def test_failed_native_build_does_not_commit_cache_verification_or_mutate_native_globals(tmp_path, monkeypatch):
    original_cache = data.v5.CACHE_DIR
    original_loader = data.v5._load_verified_v5_cache
    original_seed_loader = data.v5._load_verified_v3_seed
    monkeypatch.setattr(data, "_manifest_sources", lambda through_day: ([], {}, {}))

    def fail(*args, **kwargs):
        assert data.v5.CACHE_DIR == tmp_path / "native_v13_cache"
        assert kwargs["refresh_eligibility"] is True
        assert kwargs["rebuild_cache"] is True
        raise RuntimeError("interrupted native raw refresh")

    monkeypatch.setattr(data.v5, "load_market", fail)
    with pytest.raises(RuntimeError, match="interrupted"):
        data.build_dataset(tmp_path)
    assert not (tmp_path / "native_source_manifest.json").exists()
    assert data.v5.CACHE_DIR == original_cache
    assert data.v5._load_verified_v5_cache is original_loader
    assert data.v5._load_verified_v3_seed is original_seed_loader
