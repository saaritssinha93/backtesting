from __future__ import annotations

import json
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
    tmp_path, *, add_off_grid: bool = False, session_day: str = "2026-07-28"
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
    futures_ts = pd.date_range(
        f"{session_day} 09:20", f"{session_day} 09:45", freq="5min", tz=IST
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
