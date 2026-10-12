"""Independent causality and failure-mode checks for isolated V14 research."""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from v14 import features
from v14.index_data import load_nifty_context
from v14.research import COMMON_FEATURES, choose_variant, daily_metrics, metric, path_is_complete, split_for_day


def _minutes(days=3, minutes=31):
    frames = []
    for day_index, day in enumerate(pd.bdate_range("2026-07-01", periods=days)):
        ts = pd.date_range(f"{day:%Y-%m-%d} 09:16", periods=minutes, freq="min", tz=features.IST)
        close = 100 + day_index + np.arange(minutes, dtype=float) / 10
        frames.append(pd.DataFrame(dict(ts=ts, open=close - .05, high=close + .1,
                                        low=close - .1, close=close, volume=100.)))
    return pd.concat(frames, ignore_index=True)


def _participation(frame, history=2):
    clean = features.clean_minutes(frame)
    return features.add_participation(features.aggregate_five(clean), clean, history)


def test_same_clock_rvol_excludes_current_session_and_other_clock_slots():
    minute = _minutes()
    day = minute.ts.dt.date
    last = sorted(day.unique())[-1]
    # Opening interval normally trades ten times the following interval.
    minute["volume"] = np.where(minute.ts.dt.minute.le(20), 1000., 100.)
    minute.loc[day.eq(last), "volume"] *= 2
    out = _participation(minute)
    current = out.loc[out.ts.dt.date.eq(last)]
    assert current.interval_rvol.eq(2).all()
    assert current.cumulative_rvol.eq(2).all()
    assert out.loc[out.ts.dt.date.ne(last), "interval_rvol"].isna().all()


def test_history_requires_twenty_prior_sessions_and_uses_median():
    minute = _minutes(days=22)
    days = sorted(minute.ts.dt.date.unique())
    for i, day in enumerate(days):
        minute.loc[minute.ts.dt.date.eq(day), "volume"] = 100 * (i + 1)
    out = _participation(minute, history=20)
    assert out.loc[out.ts.dt.date.lt(days[20]), "interval_rvol"].isna().all()
    actual = out.loc[out.ts.dt.date.eq(days[20]), "interval_rvol"]
    np.testing.assert_allclose(actual, 21 / 10.5)
    actual = out.loc[out.ts.dt.date.eq(days[21]), "interval_rvol"]
    np.testing.assert_allclose(actual, 22 / 11.5)


def test_future_bars_cannot_change_already_available_features():
    minute = _minutes(days=4)
    cutoff = minute.ts.iloc[-20]
    full = features.build_features(minute, "TEST", history_sessions=2)
    prefix = features.build_features(minute.loc[minute.ts.le(cutoff)], "TEST", history_sessions=2)
    actual = full.loc[full.confirmation_ts.le(cutoff)].reset_index(drop=True)
    expected = prefix.loc[prefix.confirmation_ts.le(cutoff)].reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected)
    altered = minute.copy()
    for column in ("open", "high", "low", "close", "volume"):
        altered.loc[altered.ts.gt(cutoff), column] *= 100
    other = features.build_features(altered, "TEST", history_sessions=2)
    pd.testing.assert_frame_equal(actual, other.loc[other.confirmation_ts.le(cutoff)].reset_index(drop=True))


def test_missing_opening_interval_invalidates_cumulative_and_pressure_not_later_interval():
    minute = _minutes()
    last = minute.ts.dt.date.max()
    minute = minute.loc[~(minute.ts.dt.date.eq(last) & minute.ts.dt.minute.eq(18))]
    out = _participation(minute)
    row = out.loc[out.ts.eq(pd.Timestamp(f"{last} 09:25", tz=features.IST))].iloc[0]
    assert row.interval_rvol == 1
    assert pd.isna(row.cumulative_rvol)
    assert pd.isna(row.cmf10)
    assert pd.isna(row.signed_volume10)


def test_pressure_resets_each_session_and_zero_range_is_neutral():
    minute = _minutes()
    # All previous-session highs/lows differ; current zero-range bars still
    # supply neutral pressure, and cannot inherit yesterday's observations.
    last = minute.ts.dt.date.max()
    for column in ("open", "high", "low"):
        minute.loc[minute.ts.dt.date.eq(last), column] = minute.loc[minute.ts.dt.date.eq(last), "close"]
    out = _participation(minute)
    current = out.loc[out.ts.dt.date.eq(last)].set_index(out.loc[out.ts.dt.date.eq(last), "ts"].dt.strftime("%H:%M"))
    assert pd.isna(current.loc["09:20", "cmf10"])
    assert pd.isna(current.loc["09:20", "signed_volume10"])
    assert current.loc["09:25", "cmf10"] == 0
    assert current.loc["09:25", "mfi5"] == 100


def _previous_signed_volume10(minute):
    """Independent pre-continuity-fix arithmetic for regression comparison."""
    m = features.clean_minutes(minute)
    day = m.ts.dt.date
    signed = np.sign(m.groupby(day).close.diff()) * m.volume
    first = m.groupby(day).cumcount().eq(0)
    signed.loc[first] = np.sign(m.loc[first, "close"] - m.loc[first, "open"]) * m.loc[first, "volume"]
    numerator = signed.groupby(day).transform(lambda x: x.rolling(10, min_periods=10).sum())
    denominator = m.groupby(day).volume.transform(lambda x: x.rolling(10, min_periods=10).sum())
    contiguous = m.ts.sub(m.groupby(day).ts.shift(9)).eq(pd.Timedelta(minutes=9))
    return pd.Series((numerator / denominator.where(denominator.gt(0))).where(contiguous).to_numpy(), index=m.ts)


@pytest.mark.parametrize("missing_minutes,clock", [([25], "09:35"), (list(range(16, 21)), "09:30")])
def test_signed_volume_rejects_missing_window_predecessor_and_late_first_bar(missing_minutes, clock):
    minute = _minutes(days=21)
    last = minute.ts.dt.date.max()
    minute = minute.loc[~(minute.ts.dt.date.eq(last) & minute.ts.dt.minute.isin(missing_minutes))]
    timestamp = pd.Timestamp(f"{last} {clock}", tz=features.IST)
    assert _previous_signed_volume10(minute).loc[timestamp] == 1.
    row = _participation(minute, history=20).set_index("ts").loc[timestamp]
    assert pd.isna(row.signed_volume10)
    assert pd.notna(row.cmf10)  # Ten actual bars exist; only their predecessor is missing.
    assert pd.isna(row.cumulative_rvol)


def test_signed_volume_complete_history_preserves_previous_values_exactly():
    minute = _minutes(days=21)
    offset = minute.groupby(minute.ts.dt.date).cumcount()
    minute["close"] += np.where(offset.mod(3).eq(0), .3, -.2)
    minute["open"] = minute.close + np.where(offset.mod(2).eq(0), .05, -.05)
    minute["high"] = minute[["open", "close"]].max(axis=1) + .1
    minute["low"] = minute[["open", "close"]].min(axis=1) - .1
    minute["volume"] *= 1 + offset.mod(7)
    actual = _participation(minute, history=20).set_index("ts").signed_volume10
    expected = _previous_signed_volume10(minute).reindex(actual.index).rename(actual.name)
    pd.testing.assert_series_equal(actual, expected, check_exact=True)


def test_signed_volume_fix_preserves_common_candidates_with_twenty_session_warmup():
    minute = _minutes(days=22)
    last = minute.ts.dt.date.max()
    minute = minute.loc[~(minute.ts.dt.date.eq(last) & minute.ts.dt.minute.eq(25))]
    current = features.build_features(minute, "TEST")
    current["nifty_first_bar_return_pct"] = -.1
    previous = current.copy()
    previous["signed_volume10"] = previous.signal_ts.map(_previous_signed_volume10(minute))
    # The regression fixture genuinely exercises a changed pressure value.
    assert (previous.signed_volume10.notna() & current.signed_volume10.isna()).any()
    current = features.strict_signals(current)
    previous = features.strict_signals(previous)
    current = current.loc[np.isfinite(current[COMMON_FEATURES]).all(axis=1)].reset_index(drop=True)
    previous = previous.loc[np.isfinite(previous[COMMON_FEATURES]).all(axis=1)].reset_index(drop=True)
    assert not current.empty
    pd.testing.assert_frame_equal(current, previous, check_exact=True)


def test_missing_exact_confirmation_does_not_use_the_following_minute():
    minute = _minutes()
    day = minute.ts.dt.date.max()
    missing = pd.Timestamp(f"{day} 09:26", tz=features.IST)
    out = features.build_features(minute.loc[minute.ts.ne(missing)], "TEST", history_sessions=2)
    row = out.loc[out.signal_ts.eq(missing - pd.Timedelta(minutes=1))].iloc[0]
    assert not row.v9_exact_confirmation_present
    assert pd.isna(row.confirmation_close)


def test_pressure_masks_are_side_symmetric_and_missing_values_fail_closed():
    rows = pd.DataFrame(dict(side=["LONG", "SHORT", "LONG", "SHORT", "LONG"],
                             cmf10=[.2, -.2, np.nan, np.inf, -.2],
                             mfi5=[60., 40., np.nan, np.inf, 40.],
                             interval_rvol=[1.5, 1.5, np.nan, np.inf, 1.5]))
    for variant in (features.Variant("x", "cmf10", .2), features.Variant("x", "mfi5", 60),
                    features.Variant("x", "interval_rvol", 1.5, .2)):
        assert features.participation_mask(rows, variant).tolist() == [True, True, False, False, False]
    assert features.participation_mask(rows, features.Variant("control", "none")).all()


def test_grid_is_bounded_with_unique_names_and_no_oi_feature_dependencies():
    variants = features.variants()
    assert len(variants) == 16
    assert len({v.name for v in variants}) == len(variants)
    pool = features.build_features(_minutes(), "TEST", history_sessions=2)
    assert not {"oi", "prev_oi", "oi_change_pct"}.intersection(pool.columns)


def test_confirmation_geometry_preserves_native_float64_arithmetic():
    minute = _minutes()
    minute["open"] = minute.close - .01723
    minute["high"] = minute.close + .12874
    minute["low"] = minute.close - .06219
    for column in ("open", "high", "low", "close"):
        minute[column] = minute[column].astype("float32")
    out = features.build_features(minute, "TEST", history_sessions=2)
    row = out.iloc[-1]
    candle = minute.loc[minute.ts.eq(row.confirmation_ts)].iloc[0]
    o, h, l, c = [float(candle[column]) for column in ("open", "high", "low", "close")]
    assert float(row.body_ratio) == abs(c - o) / (h - l)
    assert float(row.v9_1m_upper_wick_ratio) == (h - max(o, c)) / (h - l)
    assert float(row.v9_1m_lower_wick_ratio) == (min(o, c) - l) / (h - l)


def test_nifty_gate_applies_only_to_short_0925_and_threshold_is_inclusive():
    pool = pd.DataFrame(dict(v9_5m_ema_bull=[False] * 4, v9_5m_ema_bear=[True] * 4,
                             price_change_pct=[-.3] * 4, volume_ratio=[2.] * 4,
                             v9_exact_confirmation_present=[True] * 4,
                             confirmation_high=[100.] * 4, confirmation_low=[98.] * 4,
                             confirmation_close=[98.5] * 4, confirmation_open=[99.] * 4,
                             signal_close=[99.5] * 4, hhmm_int=[925, 930, 925, 925],
                             nifty_first_bar_return_pct=[np.nan, np.nan, -.049, -.05],
                             v9_1m_upper_wick_ratio=[.1] * 4, v9_1m_lower_wick_ratio=[.1] * 4))
    assert features.strict_signals(pool).index.tolist() == [1, 3]


def _write_index(tmp_path, bars=None, *, duplicate_mapping=False):
    root = tmp_path / "fno"
    (root / "universe").mkdir(parents=True)
    (root / "raw_contracts_5m").mkdir()
    mapping = pd.DataFrame([dict(underlying="NIFTY", tradingsymbol="NIFTY26AUGFUT", expiry=pd.Timestamp("2026-08-25"))])
    if duplicate_mapping:
        mapping = pd.concat([mapping, mapping], ignore_index=True)
    mapping.to_parquet(root / "universe/near_month_2026-08-03.parquet", index=False)
    if bars is None:
        bars = pd.DataFrame([dict(timestamp=pd.Timestamp("2026-08-03 09:20", tz=features.IST),
                                 candle_start=pd.Timestamp("2026-08-03 09:15", tz=features.IST),
                                 tradingsymbol="NIFTY26AUGFUT", underlying="NIFTY", open=100., close=99.9,
                                 high=100., low=99.9, quality_state="VALID")])
    bars.to_parquet(root / "raw_contracts_5m/NIFTY26AUGFUT_5minute.parquet", index=False)
    return root, bars


def _frozen(tmp_path, value=-.1):
    folder = tmp_path / "frozen"
    folder.mkdir()
    path = folder / "trades.csv"
    pd.DataFrame([dict(day="2026-08-03", contract_month="26AUG", nifty_first_bar_return_pct=value)]).to_csv(path, index=False)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (folder / "manifest.json").write_text(json.dumps(dict(artifacts={"trades.csv": dict(sha256=digest)})))
    return folder


def test_index_requires_unique_exact_interval_and_never_uses_later_bar(tmp_path):
    root, bars = _write_index(tmp_path)
    good = load_nifty_context(["2026-08-03"], fno_root=root, use_frozen_fallback=False).iloc[0]
    assert good.quality_status == "PASS_RAW"
    assert good.nifty_first_bar_return_pct == pytest.approx(-.1)
    bars["timestamp"] += pd.Timedelta(minutes=5)
    bars["candle_start"] += pd.Timedelta(minutes=5)
    bars.to_parquet(root / "raw_contracts_5m/NIFTY26AUGFUT_5minute.parquet", index=False)
    missing = load_nifty_context(["2026-08-03"], fno_root=root, use_frozen_fallback=False).iloc[0]
    assert pd.isna(missing.nifty_first_bar_return_pct)
    assert missing.reason == "MISSING_EXACT_0920_BAR"


def test_index_ambiguous_contract_mapping_fails_closed(tmp_path):
    root, _ = _write_index(tmp_path, duplicate_mapping=True)
    row = load_nifty_context(["2026-08-03"], fno_root=root, use_frozen_fallback=False).iloc[0]
    assert row.reason == "MISSING_OR_AMBIGUOUS_DATED_NIFTY_MAPPING"
    assert pd.isna(row.nifty_first_bar_return_pct)


def test_index_duplicate_bar_cannot_be_bypassed_by_frozen_context(tmp_path):
    root, bars = _write_index(tmp_path)
    pd.concat([bars, bars]).to_parquet(root / "raw_contracts_5m/NIFTY26AUGFUT_5minute.parquet", index=False)
    row = load_nifty_context(["2026-08-03"], fno_root=root, frozen_dir=_frozen(tmp_path)).iloc[0]
    assert row.reason == "DUPLICATE_EXACT_0920_BAR"
    assert pd.isna(row.nifty_first_bar_return_pct)


def test_index_frozen_fallback_checks_hash_and_records_its_source(tmp_path):
    root = tmp_path / "nonexistent_raw"
    folder = _frozen(tmp_path)
    row = load_nifty_context(["2026-08-03"], fno_root=root, frozen_dir=folder).iloc[0]
    assert row.quality_status == "PASS_FROZEN_CONTEXT"
    assert row.nifty_first_bar_return_pct == pytest.approx(-.1)
    assert row.source_path.endswith("trades.csv")
    with (folder / "trades.csv").open("a") as stream:
        stream.write("\n")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_nifty_context(["2026-08-03"], fno_root=root, frozen_dir=folder)


def _selection_metrics():
    rows = []
    for name, validation in (("no_oi_control", 500.), ("interval_rvol_1", 700.)):
        for split, trades, net in (("TRAIN", 20, 1000.), ("VALIDATION", 10, validation),
                                   ("TEST", 30, -1e9), ("ALL", 60, -2e9)):
            rows.append(dict(variant=name, split=split, trades=trades, net_pnl=net,
                             max_drawdown=100., additional_bps=0.))
    return pd.DataFrame(rows)


def test_winner_is_invariant_to_holdout_all_date_and_stress_outcomes():
    metrics = _selection_metrics()
    expected = choose_variant(metrics)
    assert expected["name"] == "interval_rvol_1"
    assert expected["qualified"] and not expected["selection_used_test"]
    altered = metrics.copy()
    irrelevant = altered.split.isin(["TEST", "ALL"])
    altered.loc[irrelevant, ["net_pnl", "max_drawdown", "trades"]] = [1e15, 0, 100000]
    stress = metrics.copy()
    stress["additional_bps"] = 20.
    stress["net_pnl"] = 1e15
    stress["variant"] = "no_oi_control"
    pd.testing.assert_series_equal(pd.Series(choose_variant(pd.concat([altered, stress]))), pd.Series(expected))


def test_validation_profit_cannot_override_training_or_trade_count_screens():
    metrics = _selection_metrics()
    metrics.loc[metrics.variant.eq("interval_rvol_1") & metrics.split.eq("TRAIN"), "trades"] = 19
    chosen = choose_variant(metrics)
    assert chosen["name"] == "no_oi_control"
    assert chosen["qualified"]
    metrics.loc[metrics.split.eq("TRAIN"), "net_pnl"] = -1
    failed = choose_variant(metrics)
    assert failed["name"] == "interval_rvol_1"
    assert not failed["qualified"]
    assert failed["reason"] == "NO_CANDIDATE_PASSED_SCREEN_RESEARCH_ONLY"


def test_selection_ties_use_lower_validation_drawdown_then_fixed_grid_order():
    metrics = _selection_metrics()
    metrics.loc[metrics.split.eq("VALIDATION"), "net_pnl"] = 500.
    assert choose_variant(metrics)["name"] == "no_oi_control"
    metrics.loc[metrics.variant.eq("interval_rvol_1") & metrics.split.eq("VALIDATION"), "max_drawdown"] = 99.
    assert choose_variant(metrics)["name"] == "interval_rvol_1"
    with pytest.raises(ValueError, match="Duplicate candidate split"):
        choose_variant(pd.concat([metrics, metrics.iloc[:1]]))


def test_metric_counts_explicit_zero_trade_days_and_initial_loss_drawdown():
    ledger = pd.DataFrame(dict(sid=[1, 2, 3], day=["2026-08-03", "2026-08-04", "2026-08-04"],
                              portfolio_executed=[True, True, False],
                              portfolio_gross_profit_rupees=[-100., 500., 1e8],
                              portfolio_cost_rupees=[250., 250., 0.],
                              portfolio_net_profit_rupees=[-350., 250., 1e8]))
    days = ["2026-08-03", "2026-08-04", "2026-08-05"]
    daily = daily_metrics(ledger, days)
    assert daily.day.tolist() == days
    assert daily.net_pnl.tolist() == [-350., 250., 0.]
    assert daily.drawdown.max() == 350.
    summary = metric(ledger, days, additional_bps=5.)
    assert summary["net_pnl"] == -600.
    assert summary["cost"] == 1000.
    assert summary["max_drawdown"] == 600.
    assert summary["trades"] == 2 and summary["sessions"] == 3
    # Excluded research dates are omitted, rather than republished as flat days.
    assert daily_metrics(ledger, [days[0]]).day.tolist() == [days[0]]


def test_path_completeness_detects_a_middle_gap_without_outcome_based_refill():
    confirmation = pd.Timestamp("2026-08-03 09:26", tz=features.IST)
    timestamps = pd.date_range(confirmation + pd.Timedelta(minutes=1),
                               confirmation.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min")
    path = dict(timestamp_ns=timestamps.asi8,
                **{name: np.full(len(timestamps), 100.) for name in ("open", "high", "low", "close")})
    assert path_is_complete(path, confirmation)
    incomplete = {key: np.delete(value, 10) for key, value in path.items()}
    assert not path_is_complete(incomplete, confirmation)
    assert split_for_day("2026-08-27") == "TRAIN"
    assert split_for_day("2026-08-28") == "VALIDATION"
    assert split_for_day("2026-09-10") == "VALIDATION"
    assert split_for_day("2026-09-11") == "TEST"
