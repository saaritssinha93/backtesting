from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

import fno_oi_common as common
import fno_oi_ema_confirm_backtest as backtest
import fno_v13_v10_g_3_opening_inputs as opening_inputs


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_snapshot_read_retries_windows_lock_then_decodes_from_memory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    expected = pd.DataFrame({"value": [1, 2]})
    payload = _parquet_bytes(expected)
    target = tmp_path / "locked.parquet"
    reads = 0
    sleeps: list[float] = []
    original_decoder = pd.read_parquet
    decoder_sources: list[object] = []

    def read_bytes(_self: Path) -> bytes:
        nonlocal reads
        reads += 1
        if reads < 3:
            raise PermissionError(13, "sharing violation")
        return payload

    def decode(source: object, **kwargs: object) -> pd.DataFrame:
        decoder_sources.append(source)
        return original_decoder(source, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(common.time_module, "sleep", sleeps.append)
    monkeypatch.setattr(common.pd, "read_parquet", decode)

    actual = common.read_parquet_snapshot(target)

    pd.testing.assert_frame_equal(actual, expected)
    assert reads == 3
    assert sleeps == [0.05, 0.10]
    assert len(decoder_sources) == 1
    assert isinstance(decoder_sources[0], io.BytesIO)


def test_snapshot_read_permission_failure_is_bounded_and_reraised(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reads = 0
    sleeps: list[float] = []

    def locked(_self: Path) -> bytes:
        nonlocal reads
        reads += 1
        raise PermissionError(13, "still locked")

    monkeypatch.setattr(Path, "read_bytes", locked)
    monkeypatch.setattr(common.time_module, "sleep", sleeps.append)

    with pytest.raises(PermissionError, match="still locked"):
        common.read_parquet_snapshot(tmp_path / "locked.parquet", attempts=3)

    assert reads == 3
    assert sleeps == [0.05, 0.10]


def test_snapshot_read_does_not_retry_invalid_parquet(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reads = 0
    sleeps: list[float] = []

    def corrupt(_self: Path) -> bytes:
        nonlocal reads
        reads += 1
        return b"not parquet"

    monkeypatch.setattr(Path, "read_bytes", corrupt)
    monkeypatch.setattr(common.time_module, "sleep", sleeps.append)

    with pytest.raises(Exception):
        common.read_parquet_snapshot(tmp_path / "corrupt.parquet")

    assert reads == 1
    assert sleeps == []


def test_both_g3_futures_loaders_use_snapshot_reader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "TEST26OCTFUT_5minute.parquet"
    path.touch()
    frame = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2026-10-19 09:20", tz="Asia/Kolkata")],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
            "oi": [5000],
            "receipt": ["kept-by-opening-loader"],
        }
    )
    calls: list[dict[str, object]] = []

    def snapshot_reader(source: object, **kwargs: object) -> pd.DataFrame:
        calls.append({"source": source, **kwargs})
        columns = kwargs.get("columns")
        return frame.loc[:, columns].copy() if columns else frame.copy()

    monkeypatch.setattr(common, "raw_contract_path", lambda _symbol: path)
    monkeypatch.setattr(common, "read_parquet_snapshot", snapshot_reader)

    normal = backtest.load_five_minute("TEST26OCTFUT")
    opening = opening_inputs.load_futures_frame(path)

    assert list(calls[0]["columns"]) == [
        "timestamp", "open", "high", "low", "close", "volume", "oi"
    ]
    assert "receipt" not in normal
    assert "receipt" in opening
    assert "columns" not in calls[1]
