"""Independent causality and failure-mode checks for isolated V14 research."""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from v14 import features
from v14.index_data import load_nifty_context


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
