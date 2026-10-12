"""Exact archived contract identity and real opening-minute refresh checks."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from v14 import index_data, index_fetch


DAY = "2026-08-03"
CONTRACT = "NIFTY26AUGFUT"


def _mapping(tmp_path, *, token=12345):
    root, fno = tmp_path / "v14", tmp_path / "fno"
    path = fno / "universe" / f"near_month_{DAY}.parquet"
    path.parent.mkdir(parents=True)
    row = dict(underlying="NIFTY", tradingsymbol=CONTRACT, expiry=pd.Timestamp("2026-08-25"))
    if token is not None:
        row["instrument_token"] = token
    pd.DataFrame([row]).to_parquet(path, index=False)
    return root, fno, path


def _minutes():
    return [dict(date=ts, open=100., high=101., low=99., close=99.9, volume=1000.)
            for ts in pd.date_range(DAY + " 09:15", periods=5, freq="min", tz="Asia/Kolkata")]


class FakeMarketData:
    def __init__(self, rows=None):
        self.rows = _minutes() if rows is None else rows
        self.calls = []

    def _call(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.rows

    def instruments(self, exchange):
        raise AssertionError("A historical day must not use a current master")


def _refresh(root, fno, client):
    manifest = index_fetch.refresh_nifty_context([DAY], root, fno_root=fno, client=client)
    return json.loads(manifest.read_text())


def _raw(fno, *, close=99.9, duplicates=False):
    path = fno / "raw_contracts_5m" / f"{CONTRACT}_5minute.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = pd.DataFrame([dict(timestamp=pd.Timestamp(DAY + " 09:20", tz="Asia/Kolkata"),
                             candle_start=pd.Timestamp(DAY + " 09:15", tz="Asia/Kolkata"),
                             open=100., high=101., low=99., close=close,
                             underlying="NIFTY", tradingsymbol=CONTRACT, quality_state="VALID")])
    if duplicates:
        rows = pd.concat([rows, rows], ignore_index=True)
    rows.to_parquet(path, index=False)
    return path


def test_expired_archived_token_fetches_only_five_real_minutes_with_provenance(tmp_path):
    root, fno, mapping = _mapping(tmp_path)
    client = FakeMarketData()
    manifest = _refresh(root, fno, client)
    assert manifest["records"][0]["status"] == "FETCHED_VERIFIED"
    assert client.calls == [(('historical_data', 12345, DAY + ' 09:15:00', DAY + ' 09:19:00', 'minute'),
                             dict(continuous=False, oi=False))]
    cache = root / "data/index1m" / f"{DAY}.parquet"
    minute = pd.read_parquet(cache)
    assert minute.ts.dt.strftime("%H:%M").tolist() == ["09:16", "09:17", "09:18", "09:19", "09:20"]
    assert minute.instrument_token.eq(12345).all()
    meta = json.loads(cache.with_suffix(".json").read_text())
    assert meta["mapping_original_source"] == str(mapping.resolve())
    assert meta["mapping_sha256"] == index_data._sha256(mapping)
    assert index_data._sha256(Path(meta["mapping_source"])) == meta["mapping_sha256"]
    context = index_data.load_nifty_context([DAY], root=root, fno_root=fno).iloc[0]
    assert context.quality_status == "PASS_V14_OPENING_CACHE"
    assert context.nifty_first_bar_return_pct == pytest.approx(-.1)
    assert _refresh(root, fno, client)["records"][0]["status"] == "REUSED_VERIFIED_CONTEXT"
    assert len(client.calls) == 1


@pytest.mark.parametrize("token", [None, 0, 123.5])
def test_missing_or_invalid_archived_token_never_guesses_current_token(tmp_path, token):
    root, fno, _ = _mapping(tmp_path, token=token)
    client = FakeMarketData()
    manifest = _refresh(root, fno, client)
    assert manifest["records"][0]["reason"] == "NO_TRUSTWORTHY_ARCHIVED_NIFTY_TOKEN"
    assert not client.calls


def test_current_master_cannot_supply_historical_identity(tmp_path):
    root, fno = tmp_path / "v14", tmp_path / "fno"
    folder = root / "data/instruments"
    folder.mkdir(parents=True)
    pd.DataFrame([dict(name="NIFTY", tradingsymbol=CONTRACT, expiry="2026-08-25",
                       instrument_token=12345, instrument_type="FUT", segment="NFO-FUT")]).to_parquet(folder / "nfo_latest.parquet")
    client = FakeMarketData()
    record = _refresh(root, fno, client)["records"][0]
    assert record["reason"] == "NO_EXACT_DATED_NIFTY_MAPPING_OR_MASTER"
    assert not client.calls


def test_exact_dated_master_uses_nearest_unexpired_future_and_rejects_conflict(tmp_path):
    root, fno = tmp_path / "v14", tmp_path / "fno"
    folder = root / "data/instruments"
    folder.mkdir(parents=True)
    rows = pd.DataFrame([dict(name="NIFTY", tradingsymbol="NIFTY26JULFUT", expiry="2026-07-28", instrument_token=11),
                         dict(name="NIFTY", tradingsymbol=CONTRACT, expiry="2026-08-25", instrument_token=12345),
                         dict(name="NIFTY", tradingsymbol="NIFTY26SEPFUT", expiry="2026-09-29", instrument_token=33)])
    rows["instrument_type"], rows["segment"] = "FUT", "NFO-FUT"
    rows.to_parquet(folder / f"nfo_{DAY}.parquet", index=False)
    identity = index_fetch._mapping(DAY, root, fno)
    assert (identity["contract"], identity["instrument_token"]) == (CONTRACT, 12345)
    other = fno / "instrument_master" / f"instrument_master_{DAY}.parquet"
    other.parent.mkdir(parents=True)
    rows.loc[1, "instrument_token"] = 98765
    rows.to_parquet(other, index=False)
    with pytest.raises(ValueError, match="MASTERS_DISAGREE"):
        index_fetch._mapping(DAY, root, fno)


@pytest.mark.parametrize("defect", ["missing", "duplicate", "invalid_ohlc"])
def test_bad_opening_minutes_are_explicitly_unavailable_without_a_valid_cache(tmp_path, defect):
    root, fno, _ = _mapping(tmp_path)
    rows = _minutes()
    if defect == "missing":
        rows.pop(2)
    elif defect == "duplicate":
        rows[2] = rows[1].copy()
    else:
        rows[2]["high"] = 1.
    record = _refresh(root, fno, FakeMarketData(rows))["records"][0]
    assert record["status"] == "UNAVAILABLE"
    assert not (root / "data/index1m" / f"{DAY}.parquet").exists()


def test_cache_is_preferred_but_cannot_override_conflicting_or_invalid_raw_context(tmp_path):
    root, fno, _ = _mapping(tmp_path)
    _refresh(root, fno, FakeMarketData())
    _raw(fno)
    context = index_data.load_nifty_context([DAY], root=root, fno_root=fno).iloc[0]
    assert context.quality_status == "PASS_V14_OPENING_CACHE"
    _raw(fno, close=99.8)
    context = index_data.load_nifty_context([DAY], root=root, fno_root=fno).iloc[0]
    assert context.reason == "V14_CACHE_CONFLICTS_WITH_RAW_CONTEXT"
    assert pd.isna(context.nifty_first_bar_return_pct)
    _raw(fno, duplicates=True)
    context = index_data.load_nifty_context([DAY], root=root, fno_root=fno).iloc[0]
    assert context.reason == "DUPLICATE_EXACT_0920_BAR"
    assert pd.isna(context.nifty_first_bar_return_pct)


@pytest.mark.parametrize("tamper", ["cache", "mapping"])
def test_changed_cache_or_mapping_snapshot_fails_closed(tmp_path, tamper):
    root, fno, _ = _mapping(tmp_path)
    _refresh(root, fno, FakeMarketData())
    cache = root / "data/index1m" / f"{DAY}.parquet"
    metadata = json.loads(cache.with_suffix(".json").read_text())
    path = cache if tamper == "cache" else Path(metadata["mapping_source"])
    with path.open("ab") as stream:
        stream.write(b"changed")
    context = index_data.load_nifty_context([DAY], root=root, fno_root=fno).iloc[0]
    assert context.reason == "INVALID_OR_CHANGED_V14_INDEX_CACHE"
    assert pd.isna(context.nifty_first_bar_return_pct)


def test_existing_verified_raw_context_never_authenticates_or_fetches(tmp_path):
    root, fno, _ = _mapping(tmp_path, token=None)
    _raw(fno)
    client = FakeMarketData()
    record = _refresh(root, fno, client)["records"][0]
    assert record["status"] == "REUSED_VERIFIED_CONTEXT"
    assert record["quality_status"] == "PASS_RAW"
    assert not client.calls


def test_cache_identity_must_be_bound_to_the_original_mapping_contents(tmp_path):
    root, fno, _ = _mapping(tmp_path)
    _refresh(root, fno, FakeMarketData())
    cache = root / "data/index1m" / f"{DAY}.parquet"
    metadata = json.loads(cache.with_suffix(".json").read_text())
    minute = pd.read_parquet(cache)
    minute["instrument_token"] = 98765
    minute.to_parquet(cache, index=False)
    metadata["instrument_token"] = 98765
    metadata["sha256"] = index_data._sha256(cache)
    cache.with_suffix(".json").write_text(json.dumps(metadata))
    context = index_data.load_nifty_context([DAY], root=root, fno_root=fno).iloc[0]
    assert context.reason == "V14_INDEX_MAPPING_SNAPSHOT_IDENTITY_MISMATCH"
    assert pd.isna(context.nifty_first_bar_return_pct)
