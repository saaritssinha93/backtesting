from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import fno_equity_1m_backfill as backfill


def _base_rows(stamps: list[str]) -> pd.DataFrame:
    dates = pd.to_datetime(stamps).tz_localize("Asia/Kolkata")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [100.0 + index for index in range(len(dates))],
            "high": [101.0 + index for index in range(len(dates))],
            "low": [99.0 + index for index in range(len(dates))],
            "close": [100.5 + index for index in range(len(dates))],
            "volume": [1000 + index for index in range(len(dates))],
        }
    )


def test_force_window_fetches_requested_start_instead_of_incremental_tail(
    tmp_path: Path, monkeypatch
) -> None:
    universe_path = tmp_path / "frozen_universe.parquet"
    pd.DataFrame(
        {"equity_symbol": ["ABC"], "equity_instrument_token": [12345]}
    ).to_parquet(universe_path, index=False)

    output_dir = tmp_path / "equity"
    output_dir.mkdir()
    output_path = output_dir / "ABC_stocks_indicators_1min.parquet"
    _base_rows(["2026-08-11 15:30"]).to_parquet(output_path, index=False)

    observed: dict[str, object] = {}
    fetched = _base_rows(["2026-08-12 09:16", "2026-08-13 15:30"])
    monkeypatch.setattr(backfill.stock_1m, "setup_kite_session", lambda: object())
    monkeypatch.setattr(backfill.stock_1m, "_read_holidays", lambda _path: set())
    monkeypatch.setattr(backfill.stock_1m, "get_kite_client_rr", lambda: object())
    monkeypatch.setattr(
        backfill.stock_1m,
        "process_ticker",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("force-window must not use the incremental pipeline")
        ),
    )

    def fake_fetch(client, token, start, end, logger, intraday_ts):
        observed.update(
            {
                "token": token,
                "start": start,
                "end": end,
                "intraday_ts": intraday_ts,
            }
        )
        return fetched

    monkeypatch.setattr(backfill.stock_1m, "fetch_historical_1min_df", fake_fetch)

    outcomes = backfill.run(
        ["ABC"],
        start_date="2026-08-12",
        end_date="2026-08-13",
        output_dir=output_dir,
        backup_root=tmp_path / "backup",
        universe_path=universe_path,
        force_window=True,
    )

    assert observed["token"] == 12345
    assert observed["start"].strftime("%Y-%m-%d %H:%M") == "2026-08-12 00:00"
    assert observed["end"].strftime("%Y-%m-%d %H:%M") == "2026-08-13 15:30"
    assert observed["intraday_ts"] == "end"
    assert outcomes[0]["status"] == "partial"
    assert outcomes[0]["expected_minutes"] == 750
    assert outcomes[0]["missing_or_synthetic_minutes"] == 748
    assert outcomes[0]["force_window"] is True

    stored = pd.read_parquet(output_path, columns=["date"])
    assert set(pd.to_datetime(stored["date"])) == set(
        pd.to_datetime(
            [
                "2026-08-11 15:30+05:30",
                "2026-08-12 09:16+05:30",
                "2026-08-13 15:30+05:30",
            ],
            utc=True,
        ).tz_convert("Asia/Kolkata")
    )
    assert (tmp_path / "backup" / output_path.name).exists()


def _mock_run(tmp_path, monkeypatch, existing, fetched, *, force=True):
    universe = tmp_path / "universe.parquet"
    pd.DataFrame({"equity_symbol": ["ABC"], "equity_instrument_token": [12345]}).to_parquet(universe)
    root = tmp_path / "equity"
    root.mkdir(exist_ok=True)
    path = root / "ABC_stocks_indicators_1min.parquet"
    if existing is not None:
        existing.to_parquet(path, index=False)
    monkeypatch.setattr(backfill.stock_1m, "setup_kite_session", lambda: object())
    monkeypatch.setattr(backfill.stock_1m, "_read_holidays", lambda _: set())
    monkeypatch.setattr(backfill.stock_1m, "get_kite_client_rr", lambda: object())
    monkeypatch.setattr(backfill.stock_1m, "fetch_historical_1min_df", lambda *args: fetched)
    monkeypatch.setattr(backfill.stock_1m, "process_ticker", lambda *a, **k: pytest.fail("Repair must not use the metadata-dropping core writer"))
    if not force:
        monkeypatch.setattr(backfill.stock_1m, "_incremental_start_from_existing", lambda *a, **k: pd.Timestamp("2026-08-12 09:18", tz="Asia/Kolkata").to_pydatetime())
    kwargs = dict(start_date="2026-08-12", end_date="2026-08-12", output_dir=root,
                  backup_root=tmp_path / "backup", universe_path=universe, force_window=force)
    return path, kwargs


def test_recompute_preserves_quality_and_arbitrary_provenance_columns():
    frame = _base_rows(["2026-08-12 09:16", "2026-08-12 09:17"])
    frame["gap_filled"] = [True, False]
    frame["is_synthetic"] = [True, False]
    frame["quality_state"] = ["GAP_FILLED", "VALID"]
    frame["source"] = ["FORWARD_FILL", "KITE"]
    frame["original_source_file"] = ["original-A", "original-B"]
    result = backfill._recompute(frame)
    assert result["gap_filled"].tolist() == [True, False]
    assert result["is_synthetic"].tolist() == [True, False]
    assert result["quality_state"].tolist() == ["GAP_FILLED", "VALID"]
    assert result["original_source_file"].tolist() == ["original-A", "original-B"]


@pytest.mark.parametrize("force", [True, False])
def test_exact_genuine_replacements_upgrade_only_recovered_rows_and_preserve_initial_backup(tmp_path, monkeypatch, force):
    stamps = ["2026-08-12 09:16", "2026-08-12 09:17", "2026-08-12 09:18"]
    existing = _base_rows(stamps)
    existing["gap_filled"] = True
    existing["is_synthetic"] = True
    existing["quality_state"] = "GAP_FILLED"
    existing["source"] = "FORWARD_FILL"
    existing["repair_note"] = ["old-first", "old-second", "old-third"]
    fetched = _base_rows([stamps[2]])
    fetched["close"] = 100.75
    path, kwargs = _mock_run(tmp_path, monkeypatch, existing, fetched, force=force)
    original = path.read_bytes()
    original_output_dir = backfill.stock_1m.DIRS["1min"]["out"]
    outcome = backfill.run(["ABC"], **kwargs)[0]
    result = pd.read_parquet(path)
    assert result["gap_filled"].tolist() == [True, True, False]
    assert result["is_synthetic"].tolist() == [True, True, False]
    assert result["quality_state"].tolist() == ["GAP_FILLED", "GAP_FILLED", "VALID"]
    assert result["source"].tolist() == ["FORWARD_FILL", "FORWARD_FILL", "KITE_HISTORICAL_1M"]
    assert result["repair_note"].iloc[:2].tolist() == ["old-first", "old-second"]
    assert outcome["status"] == "partial" and outcome["accepted_fetched_rows"] == 1
    assert outcome["genuine_minutes"] == 1 and outcome["retained_non_genuine_rows"] == 2
    assert backfill.stock_1m.DIRS["1min"]["out"] == original_output_dir
    backup = kwargs["backup_root"] / path.name
    assert backup.read_bytes() == original
    backfill.run(["ABC"], **kwargs)
    assert backup.read_bytes() == original, "The original archive must survive repeated repairs unchanged"


@pytest.mark.parametrize("metadata", [
    {"gap_filled": True}, {"synthetic_repair": True}, {"repaired_synthetic": True},
    {"source": "CARRY_FORWARD"}, {"source": "carry-forward"},
])
def test_flagged_fetched_rows_do_not_upgrade_existing_synthetic_rows(tmp_path, monkeypatch, metadata):
    existing = _base_rows(["2026-08-12 09:16"])
    existing["is_synthetic"] = True
    existing["repair_note"] = "unrecovered"
    fetched = _base_rows(["2026-08-12 09:16"])
    for key, value in metadata.items():
        fetched[key] = value
    path, kwargs = _mock_run(tmp_path, monkeypatch, existing, fetched)
    original = path.read_bytes()
    outcome = backfill.run(["ABC"], **kwargs)[0]
    assert path.read_bytes() == original
    assert outcome["status"] == "partial"
    assert outcome["accepted_fetched_rows"] == 0 and outcome["rejected_quality_rows"] == 1
    assert outcome["genuine_minutes"] == 0


def test_predecessor_merge_preserves_flags_provenance_and_current_backup(tmp_path):
    root = tmp_path / "equity"
    root.mkdir()
    historical = _base_rows(["2026-08-11 15:29", "2026-08-11 15:30"])
    historical["gap_filled"] = [True, False]
    historical["is_synthetic"] = [True, False]
    historical["source"] = ["FORWARD_FILL", "KITE"]
    historical["original_source_file"] = "LTIM-recorded"
    historical.to_parquet(root / "LTIM_stocks_indicators_1min.parquet", index=False)
    current = _base_rows(["2026-08-12 09:16"])
    current["provisional_stale"] = True
    path = root / "LTM_stocks_indicators_1min.parquet"
    current.to_parquet(path, index=False)
    original = path.read_bytes()
    assert backfill._merge_predecessor(root, "LTM", tmp_path / "backup") == 2
    result = pd.read_parquet(path)
    assert result["gap_filled"].iloc[:2].tolist() == [True, False]
    assert result["is_synthetic"].iloc[:2].tolist() == [True, False]
    assert result["original_source_file"].iloc[:2].tolist() == ["LTIM-recorded", "LTIM-recorded"]
    assert bool(result["provisional_stale"].iloc[-1]) is True
    assert (tmp_path / "backup" / path.name).read_bytes() == original


@pytest.mark.parametrize("column,value", [("close", float("inf")), ("open", float("nan")), ("high", 50), ("volume", -1)])
def test_invalid_fetched_ohlcv_cannot_replace_original(tmp_path, monkeypatch, column, value):
    existing = _base_rows(["2026-08-12 09:16"])
    fetched = _base_rows(["2026-08-12 09:16"])
    fetched[column] = value
    path, kwargs = _mock_run(tmp_path, monkeypatch, existing, fetched)
    original = path.read_bytes()
    with pytest.raises(ValueError, match="OHLCV"):
        backfill.run(["ABC"], **kwargs)
    assert path.read_bytes() == original
    assert (kwargs["backup_root"] / path.name).read_bytes() == original


def test_duplicate_and_non_minute_timestamps_are_rejected_without_silent_deduplication():
    duplicate = _base_rows(["2026-08-12 09:16", "2026-08-12 09:16"])
    with pytest.raises(ValueError, match="timestamps"):
        backfill._recompute(duplicate)
    fractional = _base_rows(["2026-08-12 09:16:30"])
    with pytest.raises(ValueError, match="timestamps"):
        backfill._recompute(fractional)


def test_staged_serialization_validation_failure_keeps_original(tmp_path, monkeypatch):
    existing = _base_rows(["2026-08-12 09:16"])
    fetched = _base_rows(["2026-08-12 09:17"])
    path, kwargs = _mock_run(tmp_path, monkeypatch, existing, fetched)
    original = path.read_bytes()
    read = backfill.pd.read_parquet
    def corrupted_stage(source, *args, **kwargs):
        result = read(source, *args, **kwargs)
        if Path(source).suffix == ".tmp":
            result["close"] = float("inf")
        return result
    monkeypatch.setattr(backfill.pd, "read_parquet", corrupted_stage)
    with pytest.raises(ValueError, match="OHLCV"):
        backfill.run(["ABC"], **kwargs)
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.tmp"))


def test_empty_provider_result_remains_partial_without_fabricating_minutes(tmp_path, monkeypatch):
    path, kwargs = _mock_run(tmp_path, monkeypatch, None, pd.DataFrame())
    outcome = backfill.run(["ABC"], **kwargs)[0]
    assert outcome["status"] == "partial"
    assert outcome["expected_minutes"] == 375 and outcome["genuine_minutes"] == 0
    assert outcome["missing_or_synthetic_minutes"] == 375
    assert not path.exists()


def test_complete_genuine_session_reports_created_instead_of_partial(tmp_path, monkeypatch):
    stamps = pd.date_range("2026-08-12 09:16", "2026-08-12 15:30", freq="min").strftime("%Y-%m-%d %H:%M").tolist()
    path, kwargs = _mock_run(tmp_path, monkeypatch, None, _base_rows(stamps))
    outcome = backfill.run(["ABC"], **kwargs)[0]
    assert outcome["status"] == "created"
    assert outcome["genuine_minutes"] == outcome["expected_minutes"] == 375
    assert outcome["missing_or_synthetic_minutes"] == 0
    assert len(pd.read_parquet(path)) == 375


def test_cli_writes_partial_outcome_and_returns_nonzero(tmp_path, monkeypatch):
    universe = tmp_path / "universe.parquet"
    pd.DataFrame({"equity_symbol": ["ABC"], "equity_instrument_token": [12345]}).to_parquet(universe)
    outcome = {"symbol": "ABC", "status": "partial", "missing_or_synthetic_minutes": 12}
    monkeypatch.setattr(backfill, "run", lambda *a, **k: [outcome])
    reports = []
    monkeypatch.setattr(backfill.common, "atomic_write_json", lambda path, data: reports.append(data))
    assert backfill.main(["--symbols", "ABC", "--universe-path", str(universe)]) == 2
    assert reports[0]["outcomes"] == [outcome]
