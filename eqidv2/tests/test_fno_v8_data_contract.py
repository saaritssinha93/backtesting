from __future__ import annotations

import json
from dataclasses import replace
from datetime import date

import numpy as np
import pandas as pd
import pytest

import fno_v8_windowed_1m_entry_backtest as v8


IST = "Asia/Kolkata"


def _minute_frame(*, missing_index: int | None = None) -> pd.DataFrame:
    timestamps = pd.date_range("2026-07-28 09:16", periods=10, freq="1min", tz=IST)
    frame = pd.DataFrame(
        {
            "ts": timestamps,
            "open": np.arange(10, dtype=float) + 100.0,
            "high": np.arange(10, dtype=float) + 101.0,
            "low": np.arange(10, dtype=float) + 99.0,
            "close": np.arange(10, dtype=float) + 100.5,
            "volume": np.arange(10, dtype=float) + 1_000.0,
            "gap_filled": False,
            "opening_snapshot": False,
            "provisional_stale": False,
        }
    )
    if missing_index is not None:
        frame = frame.drop(index=missing_index).reset_index(drop=True)
    return frame


def test_exact_one_minute_aggregation_requires_all_five_rows() -> None:
    exact = v8.aggregate_equity_one_minute_to_five_minute(_minute_frame())
    assert list(exact["ts"].dt.strftime("%H:%M")) == ["09:20", "09:25"]
    assert exact["source_1m_count"].tolist() == [5, 5]

    missing = v8.aggregate_equity_one_minute_to_five_minute(
        _minute_frame(missing_index=2)
    )
    assert list(missing["ts"].dt.strftime("%H:%M")) == ["09:25"]


@pytest.mark.parametrize(
    "column,value",
    [
        ("gap_filled", True),
        ("opening_snapshot", True),
        ("provisional_stale", True),
        ("open", np.nan),
        ("low", 1_000.0),
    ],
)
def test_aggregation_rejects_invalid_or_flagged_source_rows(
    column: str, value: object
) -> None:
    frame = _minute_frame()
    frame.loc[0, column] = value
    result = v8.aggregate_equity_one_minute_to_five_minute(frame)
    assert list(result["ts"].dt.strftime("%H:%M")) == ["09:25"]


def test_source_snapshot_is_explicit_not_a_v7_default() -> None:
    assert v8.DEFAULT_SOURCE_SNAPSHOT is None
    with pytest.raises(ValueError, match="explicit --source-snapshot"):
        v8.load_validated_source_contract(None)  # type: ignore[arg-type]


def test_default_source_identity_labels_are_preserved() -> None:
    assert v8.BACKTEST_CONTRACT_MONTH_FILTER == "26AUG"
    assert v8.OI_INSTRUMENT == "STATIC_26AUG_NFO_FUTURE_RESEARCH_ONLY"
    assert v8.SOURCE_LIMITATION_LABELS == (
        "STATIC_2026_08_11_UNIVERSE_SURVIVORSHIP_RESEARCH",
        "STATIC_26AUG_FUTURES_OI_NOT_POINT_IN_TIME_ROLLING",
        "LEGACY_EQUITY_1M_HAS_NO_ROW_LINEAGE_FLAGS",
        "SOURCE_SNAPSHOT_IS_PER_FILE_STABLE_NOT_GLOBAL_TRANSACTION",
    )
    assert v8.BASE_PROMOTION_BLOCKER_LABELS == (
        "STATIC_LATER_DATED_UNIVERSE",
        "STATIC_AUGUST_FUTURES_OI_NOT_ROLLING_POINT_IN_TIME",
        "LEGACY_EQUITY_ROW_LINEAGE_UNPROVEN",
        "GLOBAL_PORTFOLIO_LEDGER_USES_CONSERVATIVE_NO_BACKFILL_OVERLAY",
        "PROSPECTIVE_20_SESSIONS_AND_100_FILLS_NOT_COMPLETED",
    )


def test_configured_contract_month_filter_reaches_load_and_snapshot(
    tmp_path, monkeypatch, capsys
) -> None:
    observed_filters: list[str] = []
    mapped = pd.DataFrame([{"equity_symbol": "TEST"}])
    universe_record: dict[str, object] = {}
    snapshot = {"universe_path": str(tmp_path / "near_month_2026-08-24.parquet")}

    def fake_load_backtest_universe(**kwargs):
        observed_filters.append(str(kwargs["contract_month_contains"]))
        return mapped, universe_record

    monkeypatch.setattr(v8, "BACKTEST_CONTRACT_MONTH_FILTER", "26SEP")
    monkeypatch.setattr(v8.provenance, "load_source_snapshot", lambda _: snapshot)
    monkeypatch.setattr(
        v8.provenance, "load_backtest_universe", fake_load_backtest_universe
    )
    monkeypatch.setattr(
        v8.provenance,
        "validate_source_snapshot",
        lambda *_args, **_kwargs: (snapshot, {"entries": []}),
    )

    loaded, *_ = v8.load_validated_source_contract(tmp_path / "manifest.json")
    assert loaded["equity_symbol"].tolist() == ["TEST"]

    manifest = tmp_path / "snapshot" / "manifest.json"
    monkeypatch.setattr(
        v8.provenance,
        "create_source_snapshot",
        lambda *_args, **_kwargs: {"manifest_path": str(manifest)},
    )
    assert v8.main(["snapshot", "--snapshot-root", str(tmp_path)]) == 0
    assert capsys.readouterr().out.strip() == str(manifest)
    assert observed_filters == ["26SEP", "26SEP"]


def test_configured_oi_and_limitation_labels_flow_to_outputs(monkeypatch) -> None:
    monkeypatch.setattr(v8, "OI_INSTRUMENT", "RECONSTRUCTED_26SEP_NFO_FUTURE")
    monkeypatch.setattr(
        v8,
        "SOURCE_LIMITATION_LABELS",
        ("RETROSPECTIVE_ROLLOVER_UNIVERSE_RECONSTRUCTION",),
    )
    monkeypatch.setattr(
        v8,
        "BASE_PROMOTION_BLOCKER_LABELS",
        ("RECONSTRUCTED_ROLLOVER_DIAGNOSTIC_ONLY",),
    )
    contract = v8._cache_contract_payload(
        snapshot={},
        inventory={},
        universe_record={},
        symbols=["TEST"],
        from_day=date(2026, 8, 24),
        through_day=date(2026, 8, 24),
    )
    assert contract["oi_instrument"] == "RECONSTRUCTED_26SEP_NFO_FUTURE"
    assert contract["source_limitations"] == [
        "RETROSPECTIVE_ROLLOVER_UNIVERSE_RECONSTRUCTION"
    ]
    assert (
        v8.strategy_payload()["data_contract"]["oi_instrument"]
        == "RECONSTRUCTED_26SEP_NFO_FUTURE"
    )

    summary, _ = v8.summarize_v8_results(
        pd.DataFrame(),
        session_dates=[date(2026, 8, 24)],
        eod_policy="EXACT_SQUARE_OFF",
        source_complete=True,
    )
    assert summary["promotion_blockers"] == [
        "RECONSTRUCTED_ROLLOVER_DIAGNOSTIC_ONLY"
    ]


def test_frozen_nse_fo_calendar_is_hashed_and_drives_expected_sessions() -> None:
    assert (
        v8.common.canonical_json_sha256(v8.nse_fo_calendar_payload())
        == v8.NSE_FO_CALENDAR_SHA256
    )
    assert v8.expected_regular_session_dates("2026-07-28", "2026-07-29") == [
        date(2026, 7, 28),
        date(2026, 7, 29),
    ]
    sessions = v8.expected_regular_session_dates("2026-05-27", "2026-08-18")
    assert len(sessions) == 58
    assert date(2026, 5, 28) not in sessions
    assert date(2026, 6, 26) not in sessions
    full_year = v8.expected_regular_session_dates("2026-01-01", "2026-12-31")
    assert len(full_year) == 246
    assert date(2026, 1, 15) not in full_year
    assert date(2026, 2, 1) in full_year
    with pytest.raises(ValueError, match="no expected regular"):
        v8.expected_regular_session_dates("2026-08-15", "2026-08-15")
    with pytest.raises(ValueError, match="calendar year 2026 only"):
        v8.expected_regular_session_dates("2025-12-31", "2026-01-02")


def test_futures_oi_delta_uses_exact_previous_five_minute_timestamp(
    tmp_path,
) -> None:
    source = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-07-28 09:20:00+05:30",
                    "2026-07-28 09:22:00+05:30",
                    "2026-07-28 09:25:00+05:30",
                ]
            ),
            "oi": [100.0, 1_000.0, 110.0],
            "quality_state": ["VALID", "VALID", "VALID"],
            "tradingsymbol": "TEST26AUGFUT",
            "instrument_token": 2,
            "expiry": pd.Timestamp("2026-08-25"),
            "contract_month": "2026-08",
        }
    )
    path = tmp_path / "TEST26AUGFUT.parquet"
    source.to_parquet(path, index=False)

    loaded = v8.load_futures_five_minute_history(path, symbol="TEST26AUGFUT")
    signal = loaded.loc[loaded["ts"].dt.strftime("%H:%M").eq("09:25")].iloc[0]
    assert signal["prev_oi"] == pytest.approx(100.0)
    assert signal["oi_change_pct"] == pytest.approx(10.0)
    with pytest.raises(ValueError, match="identity mismatch"):
        v8.load_futures_five_minute_history(path, symbol="OTHER26AUGFUT")


def _write_builder_sources(
    tmp_path,
    *,
    add_off_grid: bool = False,
    session_day: str = "2026-07-28",
    futures_clocks: tuple[str, ...] | None = None,
):
    minute_ts = pd.date_range(
        f"{session_day} 09:16", f"{session_day} 15:30", freq="1min", tz=IST
    )
    minute = pd.DataFrame(
        {
            "date": minute_ts,
            "open": 100.0,
            "high": 100.2,
            "low": 99.8,
            "close": 100.0,
            "volume": 1_000.0,
        }
    )
    if add_off_grid:
        minute = pd.concat(
            [
                minute,
                pd.DataFrame(
                    [
                        {
                            "date": pd.Timestamp(
                                f"{session_day} 09:18:30", tz=IST
                            ),
                            "open": 100.0,
                            "high": 100.2,
                            "low": 99.8,
                            "close": 100.0,
                            "volume": 1_000.0,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        ).sort_values("date")
    futures_ts = pd.DatetimeIndex(
        [
            pd.Timestamp(f"{session_day} {clock}", tz=IST)
            for clock in (
                futures_clocks
                or ("09:20", "09:25", "09:30", "09:35", "09:40", "09:45")
            )
        ]
    )
    futures = pd.DataFrame(
        {
            "timestamp": futures_ts,
            "oi": np.arange(len(futures_ts), dtype=float) + 1_000.0,
            "quality_state": "VALID",
            "tradingsymbol": "TEST26AUGFUT",
            "instrument_token": 2,
            "expiry": pd.Timestamp("2026-08-25"),
            "contract_month": "2026-08",
        }
    )
    equity_path = tmp_path / "TEST_equity.parquet"
    futures_path = tmp_path / "TEST26AUGFUT.parquet"
    minute.to_parquet(equity_path, index=False)
    futures.to_parquet(futures_path, index=False)
    mapped = pd.DataFrame(
        [
            {
                "equity_symbol": "TEST",
                "futures_tradingsymbol": "TEST26AUGFUT",
                "equity_instrument_token": 1,
                "futures_instrument_token": 2,
                "equity_tick_size": 0.05,
                "expiry": pd.Timestamp("2026-08-25"),
                "contract_month": "2026-08",
            }
        ]
    )
    lookup = {
        ("NSE_EQUITY_1M", "TEST"): equity_path,
        ("NFO_FUTURES_5M", "TEST26AUGFUT"): futures_path,
    }
    return mapped, lookup


def _paired_later_setup_book() -> tuple[v8.V8Setup, ...]:
    long_template = next(
        setup for setup in v8.ACTIVE_SETUPS if setup.setup_id == "09:45_LONG"
    )
    short_template = next(
        setup for setup in v8.ACTIVE_SETUPS if setup.setup_id == "09:45_SHORT"
    )
    return tuple(v8.ACTIVE_SETUPS) + (
        replace(long_template, signal_end="09:50"),
        replace(short_template, signal_end="09:50"),
        replace(long_template, signal_end="09:55"),
        replace(short_template, signal_end="09:55"),
    )


def test_later_slot_builder_extends_completeness_candidates_and_paths(
    tmp_path, monkeypatch
) -> None:
    extended = _paired_later_setup_book()
    monkeypatch.setattr(v8, "ACTIVE_SETUPS", extended)
    mapped, lookup = _write_builder_sources(
        tmp_path,
        futures_clocks=(
            "09:20",
            "09:25",
            "09:30",
            "09:35",
            "09:40",
            "09:45",
            "09:50",
            "09:55",
        ),
    )

    def admit_signal_row(joined: pd.DataFrame, setup) -> pd.DataFrame:
        return joined.loc[
            joined["ts"].dt.strftime("%H:%M").eq(setup.signal_end)
        ].copy()

    monkeypatch.setattr(v8, "_setup_eligible_rows", admit_signal_row)
    monkeypatch.setattr(v8, "five_minute_candidate_passes", lambda *_: True)
    candidates, paths, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-07-28",
        through_day="2026-07-28",
    )

    assert coverage.iloc[0]["source_complete_session_count"] == 1
    later = candidates.loc[candidates["signal_end"].isin(["09:50", "09:55"])]
    assert set(later["setup_id"]) == {
        "09:50_LONG",
        "09:50_SHORT",
        "09:55_LONG",
        "09:55_SHORT",
    }
    later_paths = paths.loc[paths["candidate_id"].isin(later["candidate_id"])]
    assert not later_paths.empty
    assert set(later_paths["bar_ts"].dt.date) == {date(2026, 7, 28)}
    assert later_paths["bar_ts"].max().strftime("%H:%M") == "15:30"


@pytest.mark.parametrize("missing_clock", ["09:50", "09:55"])
def test_later_slot_missing_futures_signal_is_source_incomplete(
    tmp_path, monkeypatch, missing_clock: str
) -> None:
    monkeypatch.setattr(v8, "ACTIVE_SETUPS", _paired_later_setup_book())
    clocks = tuple(
        clock
        for clock in (
            "09:20",
            "09:25",
            "09:30",
            "09:35",
            "09:40",
            "09:45",
            "09:50",
            "09:55",
        )
        if clock != missing_clock
    )
    mapped, lookup = _write_builder_sources(tmp_path, futures_clocks=clocks)
    _, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-07-28",
        through_day="2026-07-28",
    )
    assert coverage.iloc[0]["source_complete_session_count"] == 0
    assert coverage.iloc[0]["source_incomplete_session_count"] == 1


def test_predecessor_only_futures_row_need_not_have_its_own_oi_delta(
    tmp_path, monkeypatch
) -> None:
    long_template = v8.ACTIVE_SETUPS[0]
    short_template = v8.ACTIVE_SETUPS[1]
    sparse = (
        replace(long_template, signal_end="09:25"),
        replace(short_template, signal_end="09:25"),
        replace(long_template, signal_end="12:45"),
        replace(short_template, signal_end="12:45"),
    )
    monkeypatch.setattr(v8, "ACTIVE_SETUPS", sparse)
    mapped, lookup = _write_builder_sources(
        tmp_path,
        futures_clocks=("09:20", "09:25", "12:40", "12:45"),
    )
    _, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-07-28",
        through_day="2026-07-28",
    )
    assert coverage.iloc[0]["source_complete_session_count"] == 1


def test_source_completeness_uses_exchange_calendar_not_observed_date_union(
    tmp_path,
) -> None:
    mapped, lookup = _write_builder_sources(tmp_path)
    _, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-07-28",
        through_day="2026-07-29",
    )
    row = coverage.iloc[0]
    assert json.loads(row["session_dates_json"]) == [
        "2026-07-28",
        "2026-07-29",
    ]
    assert row["source_complete_session_count"] == 1
    assert row["source_incomplete_session_count"] == 1
    assert json.loads(row["source_incomplete_session_dates_json"]) == [
        "2026-07-29"
    ]


def test_off_grid_equity_row_makes_source_session_incomplete(tmp_path) -> None:
    mapped, lookup = _write_builder_sources(tmp_path, add_off_grid=True)
    _, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-07-28",
        through_day="2026-07-28",
    )
    assert coverage.iloc[0]["source_complete_session_count"] == 0
    assert coverage.iloc[0]["source_incomplete_session_count"] == 1


def test_holiday_rows_are_excluded_from_candidate_input_and_flagged(
    tmp_path, monkeypatch
) -> None:
    mapped, lookup = _write_builder_sources(tmp_path, session_day="2026-05-28")
    observed_joined_dates: list[date] = []

    def capture(joined: pd.DataFrame, setup) -> pd.DataFrame:
        if not joined.empty:
            observed_joined_dates.extend(joined["ts"].dt.date.tolist())
        return joined.iloc[0:0].copy()

    monkeypatch.setattr(v8, "_setup_eligible_rows", capture)
    candidates, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-05-27",
        through_day="2026-05-29",
    )
    assert candidates.empty
    assert date(2026, 5, 28) not in observed_joined_dates
    row = coverage.iloc[0]
    assert json.loads(row["session_dates_json"]) == [
        "2026-05-27",
        "2026-05-29",
    ]
    assert json.loads(row["unexpected_session_dates_json"]) == ["2026-05-28"]
    assert row["unexpected_session_count"] == 1


def test_union_budget_sunday_is_a_full_regular_feature_session(
    tmp_path, monkeypatch
) -> None:
    mapped, lookup = _write_builder_sources(tmp_path, session_day="2026-02-01")
    observed_joined_dates: list[date] = []

    def capture(joined: pd.DataFrame, setup) -> pd.DataFrame:
        if not joined.empty:
            observed_joined_dates.extend(joined["ts"].dt.date.tolist())
        return joined.iloc[0:0].copy()

    monkeypatch.setattr(v8, "_setup_eligible_rows", capture)
    _, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-01-30",
        through_day="2026-02-02",
    )
    assert date(2026, 2, 1) in observed_joined_dates
    row = coverage.iloc[0]
    assert json.loads(row["session_dates_json"]) == [
        "2026-01-30",
        "2026-02-01",
        "2026-02-02",
    ]
    assert json.loads(row["source_complete_session_dates_json"]) == [
        "2026-02-01"
    ]
    assert json.loads(row["unexpected_session_dates_json"]) == []


def test_manifest_completeness_cannot_override_hashed_coverage(tmp_path) -> None:
    mapped, lookup = _write_builder_sources(tmp_path)
    _, _, coverage = v8.build_v8_candidate_tables(
        mapped,
        lookup,
        from_day="2026-07-28",
        through_day="2026-07-28",
    )
    derived = v8.derive_coverage_completeness(
        coverage,
        selected_symbols=["TEST"],
        expected_session_dates=["2026-07-28"],
    )
    manifest = dict(derived)
    assert v8._manifest_completeness_matches(manifest, derived)
    manifest["headline_source_complete"] = not bool(
        derived["headline_source_complete"]
    )
    assert not v8._manifest_completeness_matches(manifest, derived)
    manifest = dict(derived)
    manifest["source_incomplete_symbol_sessions"] = 999
    assert not v8._manifest_completeness_matches(manifest, derived)


def test_empty_expected_calendar_is_invalid_not_a_valid_flat_run() -> None:
    summary, daily = v8.summarize_v8_results(
        pd.DataFrame(),
        session_dates=[],
        eod_policy="EXACT_SQUARE_OFF",
        source_complete=True,
        source_incomplete_symbol_sessions=0,
    )
    assert daily.empty
    assert summary["sessions"] == 0
    assert summary["headline_valid"] is False
    assert "NO_EXPECTED_EXCHANGE_SESSIONS" in summary["promotion_blockers"]


def _portfolio_audit() -> pd.DataFrame:
    reserve_ts = pd.Timestamp("2026-07-28 09:27", tz=IST)
    release_ts = pd.Timestamp("2026-07-28 10:00", tz=IST)

    def row(candidate_id: str, setup_id: str, rank: int) -> dict[str, object]:
        return {
            "candidate_id": candidate_id,
            "session_date": date(2026, 7, 28),
            "signal_time": pd.Timestamp("2026-07-28 09:25", tz=IST),
            "setup_id": setup_id,
            "side": "LONG",
            "symbol": "SAME",
            "frozen_rank": rank,
            "status": v8.SignalState.SQUARE_OFF.value,
            "reason": "SQUARE_OFF",
            "filled": True,
            "quantity": 10,
            "gross_return_pct": 1.0,
            "net_return_pct": 0.95,
            "gross_pnl_rs": 100.0,
            "estimated_cost_rs": 5.0,
            "net_pnl_rs": 95.0,
            "events": [
                {
                    "event_ts": reserve_ts,
                    "state_before": v8.SignalState.CONFIRMED_WAITING_CAP.value,
                    "state_after": v8.SignalState.PENDING_STOP.value,
                    "reason": "reserve",
                },
                {
                    "event_ts": reserve_ts + pd.Timedelta(minutes=1),
                    "state_before": v8.SignalState.PENDING_STOP.value,
                    "state_after": v8.SignalState.FILLED_OPEN.value,
                    "reason": "fill",
                },
                {
                    "event_ts": release_ts,
                    "state_before": v8.SignalState.FILLED_OPEN.value,
                    "state_after": v8.SignalState.SQUARE_OFF.value,
                    "reason": "exit",
                },
            ],
        }

    return pd.DataFrame(
        [
            row("2026-07-28|09:25_LONG|SAME", "09:25_LONG", 1),
            row("2026-07-28|09:30_LONG|SAME", "09:30_LONG", 1),
        ]
    )


def test_global_portfolio_ledger_rejects_overlapping_duplicate_symbol() -> None:
    constrained = v8.apply_global_portfolio_constraints(
        _portfolio_audit(), v8.PortfolioPolicy()
    )
    first = constrained.iloc[0]
    second = constrained.iloc[1]
    assert first["portfolio_decision"] == "ACCEPTED"
    assert second["portfolio_decision"] == "REJECTED"
    assert second["status"] == v8.SignalState.DUPLICATE_REJECTED.value
    assert np.isnan(second["net_return_pct"])
    assert second["unconstrained_net_return_pct"] == pytest.approx(0.95)


def test_global_portfolio_ledger_enforces_pending_margin_capacity() -> None:
    audit = _portfolio_audit().copy()
    audit.loc[1, "symbol"] = "OTHER"
    constrained = v8.apply_global_portfolio_constraints(
        audit,
        v8.PortfolioPolicy(
            capital_rs=10_000,
            margin_per_entry_rs=10_000,
            max_concurrent_positions=5,
        ),
    )
    assert constrained["portfolio_decision"].tolist() == ["ACCEPTED", "REJECTED"]
    assert constrained.iloc[1]["status"] == v8.SignalState.PORTFOLIO_REJECTED.value
