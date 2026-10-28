"""Tiny real-Parquet integration checks; no historical return computation."""
import numpy as np
import pandas as pd
import pytest

from research import g7_v4_practical_research as practical
from research.g7_v4_simple_selection_features import feature_row


def source_fixture(tmp_path, flags=False):
    frames = []
    for number, day in enumerate(("2026-10-25", "2026-10-26", "2026-10-27", "2026-10-28")):
        times = pd.date_range(day + " 09:16", periods=30, freq="min", tz="Asia/Kolkata")
        close = 100. + number * 10 + np.arange(len(times)) * .05
        frame = pd.DataFrame(dict(date=times, open=close - .02, high=close + .1,
                                   low=close - .1, close=close, volume=np.arange(len(times)) + 1.))
        if flags:
            frame["gap_filled"] = False
            frame["opening_snapshot"] = False
            frame["provisional_stale"] = False
            frame["is_synthetic"] = False
        frames.append(frame)
    raw = pd.concat(frames, ignore_index=True)
    path = tmp_path / "equity.parquet"
    raw.to_parquet(path, index=False)
    native = []
    for sid, day, minute, side in ((42, "2026-10-26", 24, "LONG"),
                                    (7, "2026-10-26", 9, "SHORT"),
                                    (19, "2026-10-27", 19, "LONG")):
        bar = raw.loc[raw.date.dt.strftime("%Y-%m-%d").eq(day)].iloc[minute]
        row = dict(sid=sid, day=day, side=side, confirmation_ts=bar.date,
                   trigger=bar.high if side == "LONG" else bar.low)
        row.update({"confirmation_" + field: bar[field] for field in ("open", "high", "low", "close")})
        native.append(row)
    native = pd.DataFrame(native)
    original = native[["sid", "day", "confirmation_ts"]].copy()
    original["stock_source_path"] = str(path)
    original["stock_source_sha256"] = practical.sha(path)
    original["existing_feature"] = [42.5, 7.5, 19.5]
    original["feature_available_at"] = original.confirmation_ts.astype(str)
    return path, raw, original.set_index("sid", drop=False), native


def test_arrow_day_predicates_and_sid_join_preserve_original_features(tmp_path, monkeypatch):
    path, raw, original, native = source_fixture(tmp_path)
    actual_read = pd.read_parquet
    observed = []

    def read_spy(*args, **kwargs):
        result = actual_read(*args, **kwargs)
        observed.append((args, kwargs, result.copy()))
        return result

    monkeypatch.setattr(practical.pd, "read_parquet", read_spy)
    result = practical.build_features(original, native.iloc[[1, 2, 0]].reset_index(drop=True))
    assert len(observed) == 1  # Same stock and digest are loaded together.
    args, kwargs, loaded = observed[0]
    assert args[0] == path
    assert "filters" in kwargs and len(kwargs["filters"]) == 2
    assert set(loaded.date.dt.strftime("%Y-%m-%d")) == {"2026-10-26", "2026-10-27"}
    for branch in kwargs["filters"]:
        assert branch[0][0:2] == ("date", ">=")
        assert branch[1][0:2] == ("date", "<")
        assert str(branch[0][2].tz) == "Asia/Kolkata"
        assert branch[1][2] - branch[0][2] == pd.Timedelta(days=1)
    assert str(loaded.date.dt.tz) == "Asia/Kolkata"
    pd.testing.assert_frame_equal(result[original.columns], original)
    assert result.index.tolist() == [42, 7, 19]
    expected_raw = raw.assign(ts=raw.date)
    for row in native.to_dict("records"):
        expected = feature_row(expected_raw, row)
        saved = result.loc[row["sid"]]
        assert saved.simple_feature_available_at == expected["feature_available_at"]
        for field, value in expected.items():
            if not field.startswith("sel_"):
                continue
            if value is None:
                assert pd.isna(saved[field])
            else:
                assert saved[field] == pytest.approx(value) if isinstance(value, float) else saved[field] == value
    assert result.loc[42, "sel_cmf20_ready"]
    assert not result.loc[7, "sel_cmf20_ready"]
    assert result.loc[19, "sel_cmf20_ready"]


def test_optional_parquet_integrity_flags_are_loaded_and_checked(tmp_path):
    path, raw, original, native = source_fixture(tmp_path, flags=True)
    result = practical.build_features(original, native)
    assert len(result) == 3
    raw.loc[(raw.date.dt.strftime("%Y-%m-%d").eq("2026-10-26"))
            & raw.date.dt.strftime("%H:%M").eq("09:17"), "gap_filled"] = True
    raw.to_parquet(path, index=False)
    original["stock_source_sha256"] = practical.sha(path)
    with pytest.raises(ValueError, match="Flagged"):
        practical.build_features(original, native)


def test_parquet_data_after_confirmation_cannot_change_built_features(tmp_path):
    path, raw, original, native = source_fixture(tmp_path)
    # Use one candidate, so its future session bars are irrelevant to every row.
    original, native = original.loc[[7]], native.loc[native.sid.eq(7)].copy()
    expected = practical.build_features(original, native)
    stamp = native.confirmation_ts.iloc[0]
    raw.loc[raw.date.gt(stamp), ["open", "high", "low", "close", "volume"]] = np.nan
    raw.to_parquet(path, index=False)
    original = original.copy()
    original["stock_source_sha256"] = practical.sha(path)
    actual = practical.build_features(original, native)
    generated = [field for field in expected if field.startswith("sel_") or field == "simple_feature_available_at"]
    pd.testing.assert_frame_equal(actual[generated], expected[generated])


@pytest.mark.parametrize("when", ["before", "after"])
def test_source_digest_drift_aborts_feature_build(tmp_path, monkeypatch, when):
    path, raw, original, native = source_fixture(tmp_path)
    digest = original.stock_source_sha256.iloc[0]
    values = iter(["changed"] if when == "before" else [digest, "changed"])
    monkeypatch.setattr(practical, "sha", lambda filename: next(values))
    message = "Stock source changed" if when == "before" else "Stock source changed while reading"
    with pytest.raises(ValueError, match=message):
        practical.build_features(original, native)
