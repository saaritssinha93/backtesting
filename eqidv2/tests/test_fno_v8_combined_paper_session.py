from __future__ import annotations

import ast
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from unittest import mock

import fno_v8_combined_paper_config as config
import fno_v8_combined_paper_control as control
import fno_v8_combined_paper_engine as paper_engine
import fno_v8_combined_paper_market_data as market_data
import fno_v8_combined_paper_session as session


DAY = date(2026, 8, 21)


def _bundle() -> str:
    return control.runtime_bundle_sha256()


def _paths(tmp_path: Path) -> session.SessionPaths:
    return session.SessionPaths(
        session_date=DAY,
        root=tmp_path / "v8",
        scanner_root=tmp_path / "v6_scanner",
        five_minute_root=tmp_path / "five",
        futures_five_minute_root=tmp_path / "futures",
        futures_slot_root=tmp_path / "futures_slots",
        near_month_universe_path=tmp_path / "universe" / "latest_near_month.parquet",
        cash_slot_root=tmp_path / "cash_slots",
        status_path=tmp_path / "runtime" / "fno_v8_combined_paper.status",
        heartbeat_path=tmp_path / "runtime" / "fno_v8_combined_paper.heartbeat",
        latest_report_path=tmp_path / "latest" / "latest_fno_v8_combined_paper.md",
        lock_path=tmp_path / "v8" / "paper.lock",
    )


def _cash_marker(*, source: str = "final", complete: bool = True) -> dict[str, object]:
    return {
        "slot_ist": "2026-08-21T09:25:00+05:30",
        "published_at_ist": "2026-08-21T09:25:10+05:30",
        "source": source,
        "complete": complete,
        "tickers_expected": 1,
        "tickers_written": 1,
        "tickers_complete": 1,
        "tickers_failed": 0,
        "unresolved_symbol_count": 0,
        "failed_symbol_count": 0,
        "token_missing_symbol_count": 0,
        "fno_equity_quality_complete": True,
        "fno_equity_expected": 1,
        "fno_equity_ready": 1,
        "fno_equity_failed": 0,
        "partition_failures": [],
        "verification_failed_count": 0,
    }


def _write_cash_marker(root: Path, marker: dict[str, object]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / "slot_20260821_0925.json"
    path.write_text(json.dumps(marker), encoding="utf-8")
    return path


def test_provisional_cash_marker_retries_before_source_deadline(
    tmp_path: Path,
) -> None:
    marker = {
        "slot_ist": "2026-08-21T09:25:00+05:30",
        "published_at_ist": "2026-08-21T09:25:06+05:30",
        "source": "watcher",
        "fresh_count": 18,
        "checked_count": 24,
        "fresh_ratio": 0.75,
    }
    _write_cash_marker(tmp_path, marker)

    with pytest.raises(session.SourceNotReadyError, match="still provisional"):
        session.load_final_cash_slot_marker(
            datetime(2026, 8, 21, 9, 25, tzinfo=config.IST),
            tmp_path,
            observed_at=datetime(2026, 8, 21, 9, 25, 6, tzinfo=config.IST),
        )


def test_provisional_cash_marker_fails_closed_at_source_deadline(
    tmp_path: Path,
) -> None:
    marker = {
        "slot_ist": "2026-08-21T09:25:00+05:30",
        "published_at_ist": "2026-08-21T09:25:06+05:30",
        "source": "watcher",
    }
    _write_cash_marker(tmp_path, marker)

    deadline = datetime(2026, 8, 21, 9, 26, tzinfo=config.IST) + timedelta(
        seconds=session.BOUNDARY_BUFFER_SECONDS
    )
    with pytest.raises(session.SourceIncompleteError, match="source deadline"):
        session.load_final_cash_slot_marker(
            datetime(2026, 8, 21, 9, 25, tzinfo=config.IST),
            tmp_path,
            observed_at=deadline,
        )


def test_complete_final_cash_marker_is_accepted(tmp_path: Path) -> None:
    _write_cash_marker(tmp_path, _cash_marker())

    loaded = session.load_final_cash_slot_marker(
        datetime(2026, 8, 21, 9, 25, tzinfo=config.IST),
        tmp_path,
        observed_at=datetime(2026, 8, 21, 9, 25, 20, tzinfo=config.IST),
    )

    assert loaded["source"] == "final"
    assert loaded["complete"] is True
    assert loaded["marker_path"].endswith("slot_20260821_0925.json")


def test_incomplete_final_cash_marker_remains_terminal(tmp_path: Path) -> None:
    _write_cash_marker(tmp_path, _cash_marker(complete=False))

    with pytest.raises(session.SourceIncompleteError, match="not fully complete"):
        session.load_final_cash_slot_marker(
            datetime(2026, 8, 21, 9, 25, tzinfo=config.IST),
            tmp_path,
            observed_at=datetime(2026, 8, 21, 9, 25, 20, tzinfo=config.IST),
        )


def _control_paths(tmp_path: Path) -> control.ControlPaths:
    root = tmp_path / "control"
    return control.ControlPaths(
        activation_path=root / "activation.json",
        kill_switch_path=root / "kill.json",
        permit_archive_root=root / "permits",
        event_archive_root=root / "events",
    )


def _allowed(bundle: str | None = None) -> control.ActivationDecision:
    digest = bundle or _bundle()
    return control.ActivationDecision(
        True,
        "PAPER_ACTIVATION_VALID",
        DAY.isoformat(),
        permit_id="permit-test",
        permit_sha256="b" * 64,
        strategy_fingerprint=config.strategy_fingerprint(),
        runtime_bundle_sha256=digest,
        permit={},
    )


def _runtimes() -> tuple[market_data.AppRuntime, ...]:
    return tuple(
        market_data.AppRuntime(f"app{index}", object(), pace_seconds=0.34)
        for index in range(1, 9)
    )


def _dashboard_identity(**kwargs) -> dict[str, object]:
    return {
        "schema_version": "eqidv2_log_dashboard_runtime_identity_v1",
        "pid": 1234,
        "source_path": "/reviewed/log_dashboard_server.py",
        "source_sha256": "9" * 64,
        "started_at_utc": "2026-08-21T03:25:00+00:00",
        "process_start_delta_seconds": 0.0,
    }


def _scanner_candidate(
    symbol: str,
    *,
    side: str = "LONG",
    token: int = 100,
    price: float = 0.50,
    oi_change: float = 0.20,
    volume_ratio: float = 4.0,
    traded_value: float = 50_000_000.0,
) -> dict[str, object]:
    long_side = side == "LONG"
    return {
        "tradingsymbol": symbol,
        "instrument_token": token,
        "futures_tradingsymbol": f"{symbol}26AUGFUT",
        "futures_instrument_token": token + 10_000,
        "tick_size": 0.05,
        "underlying": symbol,
        "data_contract": session.V6_SCANNER_DATA_CONTRACT,
        "side": side,
        "signal_end": "09:25",
        "signal_timestamp": "2026-08-21T09:25:00+05:30",
        "signal_close": 100.0,
        "price_change_pct": price if long_side else -abs(price),
        "oi": 1_010.0,
        "prev_oi": 1_000.0,
        "oi_change_pct": oi_change,
        "volume_ratio": volume_ratio,
        "traded_value": traded_value,
        "ema9": 102.0 if long_side else 98.0,
        "ema20": 101.0 if long_side else 99.0,
        "ema50": 100.0,
    }


def _scanner_payload(candidates: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": session.V6_SCANNER_SCHEMA_VERSION,
        "strategy_version": session.V6_SCANNER_STRATEGY_VERSION,
        "strategy_fingerprint": session.V6_SCANNER_STRATEGY_FINGERPRINT,
        "session_date": DAY.isoformat(),
        "signal_end": "09:25",
        "published_at_ist": "2026-08-21T09:25:01+05:30",
        "data_contract": session.V6_SCANNER_DATA_CONTRACT,
        "price_volume_indicator_source": "NSE_EQUITY",
        "equity_five_minute_quality": "COMPLETED_REAL_END_LABELLED_ONLY",
        "oi_source": "NFO_FUTURE",
        "contracts_expected": max(1, len(candidates)),
        "contracts_evaluated": max(1, len(candidates)),
        "contracts_missing_slot": 0,
        "missing_contracts": [],
        "contracts_unexpected_missing": 0,
        "contracts_skipped_no_candle": 0,
        "skipped_no_candle_symbols": [],
        "skipped_no_candle_contracts": [],
        "invalid_candidates": 0,
        "unknown_verified_no_candle_symbols": [],
        "long_candidates": sum(row["side"] == "LONG" for row in candidates),
        "short_candidates": sum(row["side"] == "SHORT" for row in candidates),
        "candidates": candidates,
        "state": "SUCCESS",
    }


def _write_scanner(paths: session.SessionPaths, payload: dict[str, object]) -> Path:
    path = (
        paths.scanner_root / DAY.isoformat() / "slot_0925.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _cash_features(raw: dict[str, object]) -> dict[str, object]:
    return {
        "timestamp": "2026-08-21T09:25:00+05:30",
        "open": 99.5,
        "high": 100.5,
        "low": 99.0,
        "close": 100.0,
        "volume": 500_000.0,
        "price_change_pct": raw["price_change_pct"],
        "volume_ratio": raw["volume_ratio"],
        "traded_value": raw["traded_value"],
        "ema9": raw["ema9"],
        "ema20": raw["ema20"],
        "ema50": raw["ema50"],
        "source_path": f"/{raw['tradingsymbol']}.parquet",
        "source_file_size_bytes": 123,
        "source_file_sha256": "c" * 64,
        "causal_prefix_count": 100,
        "causal_prefix_first_ist": "2026-08-11T09:20:00+05:30",
        "causal_prefix_last_ist": "2026-08-21T09:25:00+05:30",
        "causal_prefix_sha256": "d" * 64,
        "cash_slot_marker_path": "/cash.json",
        "cash_slot_marker_sha256": "e" * 64,
    }


def _oi_pair(raw: dict[str, object]) -> dict[str, object]:
    rows = [
        {"timestamp": "2026-08-21T09:20:00+05:30", "oi": 1000.0, "quality_state": "VALID"},
        {"timestamp": "2026-08-21T09:25:00+05:30", "oi": 1010.0, "quality_state": "VALID"},
    ]
    return {
        "timestamp": "2026-08-21T09:25:00+05:30",
        "previous_timestamp": "2026-08-21T09:20:00+05:30",
        "oi": 1010.0,
        "prev_oi": 1000.0,
        "oi_change_pct": raw["oi_change_pct"],
        "rows": rows,
        "rows_sha256": session.common.canonical_json_sha256(rows),
        "source_path": f"/{raw['futures_tradingsymbol']}.parquet",
        "source_file_size_bytes": 321,
        "source_file_sha256": "f" * 64,
    }


def _proof_and_authority(
    candidates: list[dict[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    contracts: list[dict[str, object]] = []
    eligible: list[dict[str, object]] = []
    universe: list[dict[str, object]] = []
    for raw in candidates:
        pair = _oi_pair(raw)
        mapping = {
            "tradingsymbol": raw["futures_tradingsymbol"],
            "instrument_token": raw["futures_instrument_token"],
            "underlying": raw["tradingsymbol"],
            "expiry": "2026-08-27",
            "contract_month": "2026-08",
            "lot_size": 100,
            "tick_size": 0.05,
            "equity_symbol": raw["tradingsymbol"],
            "equity_instrument_token": raw["instrument_token"],
            "equity_tick_size": raw["tick_size"],
            "data_contract": session.V6_SCANNER_DATA_CONTRACT,
            "source_path": pair["source_path"],
            "source_size_bytes": pair["source_file_size_bytes"],
            "source_file_sha256": pair["source_file_sha256"],
            "predecessor_is_exact_s_minus_5": True,
            "rows": pair["rows"],
            "rows_sha256": pair["rows_sha256"],
        }
        contracts.append(mapping)
        source_row = {
            **raw,
            "_cash_features": _cash_features(raw),
            "_oi_pair": pair,
            "_proof_mapping": mapping,
            "eligible_sides": [raw["side"]],
        }
        universe.append(source_row)
        eligible.append(source_row)
    proof = {"proof_sha256": "1" * 64, "contracts": contracts}
    authority = {
        "candidate_source_sha256": "2" * 64,
        "universe_rows": universe,
        "eligible_rows": eligible,
    }
    return proof, authority


def _write_sealed_authority(
    paths: session.SessionPaths,
    candidates: list[dict[str, object]],
) -> dict[str, object]:
    proof, authority = _proof_and_authority(candidates)
    symbols = [str(row["tradingsymbol"]) for row in authority["universe_rows"]]
    authority.update(
        {
            "schema_version": session.EVIDENCE_SCHEMA_VERSION,
            "kind": "INDEPENDENT_V8_ALL_STOCK_CANDIDATE_SOURCE",
            "session_date": DAY.isoformat(),
            "signal_end": "09:25",
            "signal_timestamp": "2026-08-21T09:25:00+05:30",
            "source_policy_version": session.SOURCE_POLICY_VERSION,
            "authority": "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6_CANDIDATE_ROWS",
            "universe_oi_proof_sha256": proof["proof_sha256"],
            "strict_cash_source_sha256": "6" * 64,
            "universe_count": len(symbols),
            "universe_symbol_set_sha256": session.common.symbol_set_sha256(symbols),
            "eligible_count": len(authority["eligible_rows"]),
            "eligible_by_side": {
                side: sum(
                    str(row["side"]) == side for row in authority["eligible_rows"]
                )
                for side in ("LONG", "SHORT")
            },
            "source_started_at_ist": "2026-08-21T09:25:48+05:30",
            "source_finished_at_ist": "2026-08-21T09:25:49+05:30",
            "confirmation_due_ist": "2026-08-21T09:26:03+05:30",
            "source_completed_before_confirmation_due": True,
        }
    )
    authority.pop("candidate_source_sha256", None)
    authority["candidate_source_sha256"] = session.common.canonical_json_sha256(
        authority
    )
    destination = paths.independent_candidate_source_root / "slot_0925.json"
    session._write_immutable_json(destination, authority)
    return authority


def _direct(candidates: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {
        str(raw["tradingsymbol"]): {
            "open": 99.5,
            "high": 100.5,
            "low": 99.0,
            "close": 100.0,
            "volume": 500_000.0,
            "constituents": [],
            "constituents_sha256": "3" * 64,
            "app_name": "app1",
        }
        for raw in candidates
    }


def _engine_candidate(symbol: str = "AAA", token: int = 100) -> dict[str, object]:
    return {
        "candidate_id": f"{DAY}|09:25_LONG|{symbol}",
        "session_date": DAY.isoformat(),
        "signal_time": "2026-08-21T09:25:00+05:30",
        "signal_end": "09:25",
        "setup_id": "09:25_LONG",
        "side": "LONG",
        "symbol": symbol,
        "equity_instrument_token": token,
        "instrument_token": token,
        "futures_instrument_token": token + 10_000,
        "futures_symbol": f"{symbol}26AUGFUT",
        "five_min_open": 99.5,
        "five_min_high": 100.5,
        "five_min_low": 99.0,
        "five_min_close": 100.0,
        "five_min_volume": 1000.0,
        "price_change_pct": 0.50,
        "oi_change_pct": 0.20,
        "volume_ratio": 4.0,
        "traded_value": 50_000_000.0,
        "ema9": 102.0,
        "ema20": 101.0,
        "ema50": 100.0,
        "oi": 1010.0,
        "prev_oi": 1000.0,
        "tick_size": 0.05,
    }


def _frame(symbol: str, end: str, *, open_: float, high: float, low: float, close: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "instrument_token": 100,
                "app_name": "app1",
                "timestamp": end,
                "candle_start": (pd.Timestamp(end) - pd.Timedelta(minutes=1)).isoformat(),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1000.0,
                "gap_filled": False,
                "opening_snapshot": False,
                "provisional_stale": False,
            }
        ]
    )


def test_session_source_has_no_broker_order_or_legacy_runtime_import() -> None:
    source = Path(session.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    assert "fno_v5_live" not in imports
    assert "fno_v6_live_config" not in imports
    assert "place_order" not in source
    assert "modify_order" not in source
    assert "cancel_order" not in source
    assert "--mode" not in session.build_parser().format_help()


def test_run_without_activation_reports_disabled_before_credentials(tmp_path: Path) -> None:
    called = False

    def authenticate():
        nonlocal called
        called = True
        return _runtimes()

    code = session.run_paper_session(
        DAY,
        paths=_paths(tmp_path),
        control_paths=_control_paths(tmp_path),
        now_provider=lambda: datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        bundle_provider=_bundle,
        authenticator=authenticate,
    )
    assert code == 0
    assert called is False
    assert "status=DISABLED_APPROVAL_REQUIRED" in _paths(tmp_path).status_path.read_text()
    assert "DISABLED_APPROVAL_REQUIRED" in _paths(tmp_path).latest_report_path.read_text()


def test_preflight_activation_gate_precedes_optional_app_auth(
    tmp_path: Path,
) -> None:
    called = False

    def authenticate():
        nonlocal called
        called = True
        return _runtimes()

    code, payload = session.run_preflight(
        DAY,
        require_activation=True,
        authenticate_apps=True,
        observed_now=datetime(2026, 8, 21, 9, 0, tzinfo=config.IST),
        control_paths=_control_paths(tmp_path),
        bundle_provider=control.runtime_bundle_sha256,
        authenticator=authenticate,
        dashboard_identity_provider=_dashboard_identity,
    )
    assert code == 2
    assert payload["ok"] is False
    assert called is False


def test_preflight_authenticates_exact_ordered_eight_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sequence: list[str] = []

    def evaluate(*args, **kwargs):
        sequence.append("gate")
        return _allowed()

    def authenticate():
        sequence.append("apps")
        return _runtimes()

    monkeypatch.setattr(session.control, "evaluate_activation", evaluate)
    code, payload = session.run_preflight(
        DAY,
        require_activation=True,
        authenticate_apps=True,
        observed_now=datetime(2026, 8, 21, 9, 0, tzinfo=config.IST),
        bundle_provider=control.runtime_bundle_sha256,
        authenticator=authenticate,
        dashboard_identity_provider=_dashboard_identity,
    )
    assert code == 0
    assert payload["apps_authenticated"] is True
    assert sequence == ["gate", "apps"]
    with pytest.raises(session.SessionContractError, match="ordered app1..app8"):
        session._assert_exact_roster(tuple(reversed(_runtimes())))


@pytest.mark.parametrize(
    "session_day",
    (date(2026, 8, 22), date(2026, 9, 14), date(2027, 1, 4)),
)
def test_calendar_fails_closed_for_weekend_holiday_or_unfrozen_year(
    session_day: date,
) -> None:
    assert session.is_regular_nse_session(session_day) is False
    assert session.is_regular_nse_session(date(2026, 2, 1)) is True


def test_scanner_requires_complete_zero_no_candle_and_not_future(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    payload = _scanner_payload([_scanner_candidate("AAA")])
    _write_scanner(paths, payload)
    loaded, raw, digest = session.load_finalized_v6_scanner_slot(
        paths,
        "09:25",
        observed_at=datetime(2026, 8, 21, 9, 25, 2, tzinfo=config.IST),
    )
    assert loaded["state"] == "SUCCESS"
    assert digest == session._sha256_bytes(raw)

    payload["contracts_skipped_no_candle"] = 1
    payload["skipped_no_candle_symbols"] = ["HIDDEN26AUGFUT"]
    _write_scanner(paths, payload)
    with pytest.raises(session.SourceIncompleteError, match="no-candle"):
        session.load_finalized_v6_scanner_slot(
            paths,
            "09:25",
            observed_at=datetime(2026, 8, 21, 9, 25, 2, tzinfo=config.IST),
        )

    payload = _scanner_payload([_scanner_candidate("AAA")])
    payload["published_at_ist"] = "2026-08-21T09:25:03+05:30"
    _write_scanner(paths, payload)
    with pytest.raises(session.SourceIncompleteError, match="future"):
        session.load_finalized_v6_scanner_slot(
            paths,
            "09:25",
            observed_at=datetime(2026, 8, 21, 9, 25, 2, tzinfo=config.IST),
        )


def test_candidate_book_reapplies_v8_thresholds_and_freezes_rank(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    candidates = [
        _scanner_candidate("AAA", token=100, price=0.50),
        _scanner_candidate("BBB", token=200, price=0.80),
        _scanner_candidate("TOOLOW", token=300, price=0.20),
        _scanner_candidate("SHORT", side="SHORT", token=400),
    ]
    payload = _scanner_payload(candidates)
    proof, authority = _proof_and_authority(candidates)
    setup = config.setup_for("09:25", "LONG")
    assert setup is not None
    book = session.build_v8_candidate_book(
        payload,
        setup,
        paths,
        source_sha256="a" * 64,
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
        cash_constituent_audit=_direct(candidates),
        universe_oi_proof=proof,
        independent_candidate_source=authority,
    )
    assert [row["symbol"] for row in book] == ["BBB", "AAA"]
    assert [row["frozen_rank"] for row in book] == [1, 2]
    assert all(row["source_policy_version"] == session.SOURCE_POLICY_VERSION for row in book)


def test_no_retro_slot_registration_after_s_plus_one(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(session.SourceIncompleteError, match="no-retro-entry"):
        session._try_ingest_slot(
            paths=paths,
            observed=datetime(2026, 8, 21, 9, 26, 4, tzinfo=config.IST),
            process_started_at=datetime(2026, 8, 21, 9, 26, 3, tzinfo=config.IST),
            engine=object(),
            module=paper_engine,
            telemetry=session.SessionTelemetry(),
            ingested_slots=set(),
            symbol_tokens={},
            clock=lambda: datetime(2026, 8, 21, 9, 26, 4, tzinfo=config.IST),
        )


def test_real_engine_registration_minute_processing_and_checkpoint_roundtrip(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    engine = paper_engine.V8CombinedPaperEngine()
    setup = config.setup_for("09:25", "LONG")
    assert setup is not None
    session.register_candidate_book(
        engine, paper_engine, setup, [_engine_candidate()], paths
    )
    session.process_engine_minute(
        engine,
        paper_engine,
        datetime(2026, 8, 21, 9, 26, tzinfo=config.IST),
        _frame(
            "AAA",
            "2026-08-21T09:26:00+05:30",
            open_=100.0,
            high=101.0,
            low=99.9,
            close=100.8,
        ),
        paths,
    )
    session.process_engine_minute(
        engine,
        paper_engine,
        datetime(2026, 8, 21, 9, 27, tzinfo=config.IST),
        _frame(
            "AAA",
            "2026-08-21T09:27:00+05:30",
            open_=101.1,
            high=101.3,
            low=101.0,
            close=101.2,
        ),
        paths,
    )
    assert engine.records()[0]["entry_time"] is not None
    bundle = _bundle()
    telemetry = session.SessionTelemetry(runtime_bundle_sha256=bundle)
    session.persist_checkpoint(
        paths,
        engine,
        telemetry,
        processed_clock_end=datetime(2026, 8, 21, 9, 27, tzinfo=config.IST),
        ingested_slots={"09:25"},
        symbol_tokens={"AAA": 100},
    )
    restored = session.load_checkpoint(
        paths, paper_engine, expected_runtime_bundle_sha256=bundle
    )
    assert restored is not None
    restored_engine, processed, slots, tokens, _ = restored
    assert restored_engine.checkpoint() == engine.checkpoint()
    assert processed.strftime("%H:%M") == "09:27"
    assert slots == {"09:25"}
    assert tokens == {"AAA": 100}


def test_existing_minute_snapshot_is_reused_without_api_refetch(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    runtimes = _runtimes()
    request = market_data.CandidateRequest("AAA", 100)
    end = datetime(2026, 8, 21, 9, 26, tzinfo=config.IST)
    frame = _frame(
        "AAA",
        end.isoformat(),
        open_=100.0,
        high=101.0,
        low=99.9,
        close=100.8,
    )
    marker = {
        "schema_version": market_data.MINUTE_SNAPSHOT_SCHEMA_VERSION,
        "policy_version": market_data.MARKET_DATA_POLICY_VERSION,
        "expected_end_ist": end.isoformat(),
        "completed_boundary_ist": (end + timedelta(seconds=3)).isoformat(),
        "observed_at_ist": (end + timedelta(seconds=4)).isoformat(),
        "candidate_count": 1,
        "candidate_contract_sha256": session.common.canonical_json_sha256(
            [{"symbol": "AAA", "instrument_token": 100}]
        ),
        "app_roster": market_data.app_roster_payload(runtimes),
        "app_roster_sha256": market_data.app_roster_sha256(runtimes),
        "app_usage": [],
        "outcomes": [],
        "written_count": 1,
        "complete": True,
        "state": "SUCCESS",
    }
    market_data.publish_minute_snapshot_once(
        paths.minute_root,
        frame,
        marker,
        strategy_fingerprint=config.strategy_fingerprint(),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("API re-fetch attempted")

    observed_frame, observed_marker, reused = session.load_or_fetch_completed_minute(
        paths,
        [request],
        runtimes,
        end,
        observed_now=end + timedelta(seconds=5),
        minute_fetcher=forbidden,
    )
    assert reused is True
    assert len(observed_frame) == 1
    assert observed_marker["data_rows"] == 1


def test_terminal_metrics_exclude_unresolved_filled_records() -> None:
    unresolved = {
        "candidate_id": "a",
        "setup_id": "09:25_LONG",
        "side": "LONG",
        "symbol": "AAA",
        "status": "FILLED_OPEN",
        "entry_time": "2026-08-21T09:27:00+05:30",
        "entry_price": 101.0,
        "exit_time": None,
        "exit_price": None,
        "net_return_pct": None,
        "net_pnl_rs": None,
    }
    closed = {
        **unresolved,
        "candidate_id": "b",
        "symbol": "BBB",
        "status": "TARGETED",
        "exit_time": "2026-08-21T09:31:00+05:30",
        "exit_price": 102.0,
        "net_return_pct": 0.8,
        "net_pnl_rs": 400.0,
    }
    assert session._terminal_trade_records([unresolved, closed]) == [closed]
    assert session._unresolved_filled_records([unresolved, closed]) == [unresolved]
    rejected_shadow = {
        **unresolved,
        "candidate_id": "c",
        "portfolio_decision": "REJECTED",
    }
    assert session._unresolved_filled_records([rejected_shadow]) == []
    telemetry = session.SessionTelemetry(state="COMPLETED")
    assert session._valid_economic_result(telemetry, [unresolved, closed]) is False


def test_final_outputs_have_manifest_completion_and_invalid_unresolved_flag(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    engine = paper_engine.V8CombinedPaperEngine()
    setup = config.setup_for("09:25", "LONG")
    assert setup is not None
    session.register_candidate_book(engine, paper_engine, setup, [_engine_candidate()], paths)
    session.process_engine_minute(
        engine,
        paper_engine,
        datetime(2026, 8, 21, 9, 26, tzinfo=config.IST),
        _frame("AAA", "2026-08-21T09:26:00+05:30", open_=100, high=101, low=99.9, close=100.8),
        paths,
    )
    session.process_engine_minute(
        engine,
        paper_engine,
        datetime(2026, 8, 21, 9, 27, tzinfo=config.IST),
        _frame("AAA", "2026-08-21T09:27:00+05:30", open_=101.1, high=101.3, low=101, close=101.2),
        paths,
    )
    telemetry = session.SessionTelemetry(
        state="DATA_INCOMPLETE",
        phase="TEST",
        data_incomplete=True,
        runtime_bundle_sha256=_bundle(),
    )
    session._finalize_session(paths, telemetry, engine)
    artifact = paths.day_session_root / "artifact_manifest.json"
    completion = paths.day_session_root / "completion.json"
    assert artifact.is_file() and completion.is_file()
    payload = json.loads(completion.read_text(encoding="utf-8"))
    assert payload["valid_economic_result"] is False
    assert payload["filled_count"] == 1
    assert payload["closed_trade_count"] == 0
    assert payload["unresolved_filled_count"] == 1
    assert "excluded from PF/win rate" in paths.session_report_path.read_text()


def test_minute_by_minute_control_recheck_terminates_pending_without_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def evaluate(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls <= 2:
            return _allowed()
        return control.ActivationDecision(
            False,
            "KILL_SWITCH_ENGAGED",
            DAY.isoformat(),
            strategy_fingerprint=config.strategy_fingerprint(),
        )

    monkeypatch.setattr(session.control, "evaluate_activation", evaluate)
    now = datetime(2026, 8, 21, 9, 15, 5, tzinfo=config.IST)
    paths = _paths(tmp_path)
    code = session.run_paper_session(
        DAY,
        paths=paths,
        now_provider=lambda: now,
        bundle_provider=control.runtime_bundle_sha256,
        authenticator=_runtimes,
        dashboard_identity_provider=_dashboard_identity,
        max_iterations=2,
    )
    assert code == 2
    completion = json.loads(
        (paths.day_session_root / "completion.json").read_text(encoding="utf-8")
    )
    assert completion["state"] == "STOPPED_CONTROL_INTERVENTION"
    assert completion["valid_economic_result"] is False
    assert calls >= 3


@pytest.mark.parametrize("side", ("LONG", "SHORT"))
def test_setup_thresholds_are_exact_without_epsilon(side: str) -> None:
    setup = config.setup_for("09:25", side)
    assert setup is not None
    row = {
        "side": side,
        "signal_end": "09:25",
        "price_change_pct": (
            setup.price_change_pct if side == "LONG" else -setup.price_change_pct
        ),
        "oi": 1000.0 * (1.0 + setup.oi_change_pct / 100.0),
        "prev_oi": 1000.0,
        "oi_change_pct": setup.oi_change_pct,
        "volume_ratio": setup.volume_ratio,
        "traded_value": setup.min_traded_value,
        "ema9": 102.0 if side == "LONG" else 98.0,
        "ema20": 101.0 if side == "LONG" else 99.0,
        "ema50": 100.0,
    }
    assert session._candidate_passes_setup(row, setup) is True
    microscopic = 1e-13
    below_price = dict(row)
    below_price["price_change_pct"] = (
        setup.price_change_pct - microscopic
        if side == "LONG"
        else -setup.price_change_pct + microscopic
    )
    assert session._candidate_passes_setup(below_price, setup) is False
    below_oi = dict(row, oi_change_pct=setup.oi_change_pct - microscopic)
    assert session._candidate_passes_setup(below_oi, setup) is False


def test_scanner_rejects_incomplete_counts_and_duplicate_identity(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    payload = _scanner_payload([_scanner_candidate("AAA")])
    payload["contracts_evaluated"] = 0
    _write_scanner(paths, payload)
    with pytest.raises(session.SourceIncompleteError, match="contracts evaluated"):
        session.load_finalized_v6_scanner_slot(
            paths,
            "09:25",
            observed_at=datetime(2026, 8, 21, 9, 25, 2, tzinfo=config.IST),
        )

    duplicate = _scanner_candidate("AAA")
    payload = _scanner_payload([_scanner_candidate("AAA"), duplicate])
    _write_scanner(paths, payload)
    with pytest.raises(session.SourceIncompleteError, match="duplicate"):
        session.load_finalized_v6_scanner_slot(
            paths,
            "09:25",
            observed_at=datetime(2026, 8, 21, 9, 25, 2, tzinfo=config.IST),
        )


def _write_one_contract_universe(
    paths: session.SessionPaths,
    *,
    off_grid_predecessor: bool = False,
    global_complete: bool = True,
    general_cash_superset: bool = False,
    drop_predecessor: bool = False,
) -> None:
    symbol = "AAA26AUGFUT"
    cash_symbol = "AAA"
    future_token = 10100
    cash_token = 100
    paths.near_month_universe_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "tradingsymbol": symbol,
                "instrument_token": future_token,
                "underlying": cash_symbol,
                "is_index_future": False,
                "master_date": pd.Timestamp(DAY),
                "expiry": pd.Timestamp("2026-08-27"),
                "contract_month": "2026-08",
                "lot_size": 100,
                "tick_size": 0.05,
                "equity_symbol": cash_symbol,
                "equity_instrument_token": cash_token,
                "equity_tick_size": 0.05,
                "data_contract": session.V6_SCANNER_DATA_CONTRACT,
            }
        ]
    ).to_parquet(paths.near_month_universe_path, index=False)
    stock_hash = session.common.symbol_set_sha256([symbol])
    paths.futures_slot_root.mkdir(parents=True, exist_ok=True)
    (paths.futures_slot_root / "slot_20260821_0925.json").write_text(
        json.dumps(
            {
                "schema_version": session.common.FNO_FETCH_SLOT_SCHEMA_VERSION,
                "source": "final",
                "state": "SUCCESS",
                "complete": True,
                "attempt_complete": True,
                "outcome_symbol_set_complete": True,
                "stock_outcome_symbol_set_complete": True,
                "stock_complete": True,
                "stock_state": "SUCCESS",
                "global_complete": global_complete,
                "slot_ist": "2026-08-21T09:25:00+05:30",
                "published_at_ist": "2026-08-21T09:25:20+05:30",
                "universe_date": DAY.isoformat(),
                "stock_contracts_expected": 1,
                "stock_contracts_written": 1,
                "stock_written_symbols": [symbol],
                "stock_symbol_set_sha256": stock_hash,
                "stock_no_candle_count": 0,
                "stock_no_candle_symbols": [],
                "stock_verified_no_candle_count": 0,
                "stock_verified_no_candle_symbols": [],
                "stock_unverified_no_candle_symbols": [],
                "stock_invalid_data_count": 0,
                "stock_invalid_data_symbols": [],
                "stock_failed_count": 0,
                "stock_failed_symbols": [],
                # Complete upstream data is authoritative even when the
                # finalized writer used only an approved subset of apps.
                "apps_used": ["app2", "app3", "app4"],
            }
        ),
        encoding="utf-8",
    )
    cash_hash = session.common.symbol_set_sha256([cash_symbol])
    general_cash_hash = session.common.symbol_set_sha256(
        [cash_symbol, "EXTRA"] if general_cash_superset else [cash_symbol]
    )
    general_cash_count = 2 if general_cash_superset else 1
    paths.cash_slot_root.mkdir(parents=True, exist_ok=True)
    (paths.cash_slot_root / "slot_20260821_0925.json").write_text(
        json.dumps(
            {
                "slot_ist": "2026-08-21T09:25:00+05:30",
                "published_at_ist": "2026-08-21T09:25:10+05:30",
                "source": "final",
                "complete": True,
                "tickers_expected": general_cash_count,
                "tickers_written": general_cash_count,
                "tickers_complete": general_cash_count,
                "tickers_failed": 0,
                "current_symbol_count": general_cash_count,
                "unresolved_symbol_count": 0,
                "failed_symbol_count": 0,
                "token_missing_symbol_count": 0,
                "fno_equity_quality_complete": True,
                "fno_equity_expected": 1,
                "fno_equity_ready": 1,
                "fno_equity_failed": 0,
                "partition_failures": [],
                "verification_failed_count": 0,
                "universe_sha256": general_cash_hash,
                "fno_equity_universe_sha256": cash_hash,
            }
        ),
        encoding="utf-8",
    )
    ends = [
        datetime(2026, 8, 21, 9, 20, tzinfo=config.IST),
        datetime(2026, 8, 21, 9, 25, tzinfo=config.IST),
    ]
    if off_grid_predecessor:
        ends.insert(1, datetime(2026, 8, 21, 9, 23, tzinfo=config.IST))
    if drop_predecessor:
        # The contract simply did not trade in 09:15-09:20, so the exchange
        # published no S-5 bar at all (the real LICHSGFIN 2026-09-02 case).
        ends = [end for end in ends if end != datetime(2026, 8, 21, 9, 20, tzinfo=config.IST)]
    rows = []
    for index, end in enumerate(ends):
        rows.append(
            {
                "timestamp": end,
                "candle_start": end - timedelta(minutes=5),
                "underlying": cash_symbol,
                "tradingsymbol": symbol,
                "instrument_token": future_token,
                "is_index_future": False,
                "expiry": pd.Timestamp("2026-08-27"),
                "contract_month": "2026-08",
                "lot_size": 100,
                "tick_size": 0.05,
                "oi": 1000.0 + index * 10,
                "quality_state": "VALID",
                "source": "kite_historical",
                "data_version": session.common.RAW_DATA_VERSION,
            }
        )
    paths.futures_five_minute_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(
        paths.futures_five_minute_root / f"{symbol}_5minute.parquet", index=False
    )


def test_universe_oi_proof_binds_full_bytes_and_rejects_off_grid_non_candidate(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _write_one_contract_universe(paths)
    proof = session.prove_v6_oi_shift_is_exact_for_stock_universe(
        paths,
        "09:25",
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
    )
    assert proof["stock_contracts_proven"] == 1
    assert len(proof["contracts"][0]["source_file_sha256"]) == 64

    stock_only = _paths(tmp_path / "stock_only")
    _write_one_contract_universe(stock_only, global_complete=False)
    stock_only_proof = session.prove_v6_oi_shift_is_exact_for_stock_universe(
        stock_only,
        "09:25",
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
    )
    assert stock_only_proof["stock_contracts_proven"] == 1

    cash_superset = _paths(tmp_path / "cash_superset")
    _write_one_contract_universe(cash_superset, general_cash_superset=True)
    cash_superset_proof = session.prove_v6_oi_shift_is_exact_for_stock_universe(
        cash_superset,
        "09:25",
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
    )
    assert cash_superset_proof["stock_contracts_proven"] == 1

    other = _paths(tmp_path / "offgrid")
    _write_one_contract_universe(other, off_grid_predecessor=True)
    with pytest.raises(session.SourceIncompleteError, match="predecessor"):
        session.prove_v6_oi_shift_is_exact_for_stock_universe(
            other,
            "09:25",
            observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
        )
    assert not other.day_evidence_root.exists()


class _HistoricalClient:
    def __init__(self, records_by_token: dict[int, list[dict[str, object]]]):
        self.records_by_token = records_by_token
        self.calls: list[int] = []

    def historical_data(self, token, *args, **kwargs):
        self.calls.append(int(token))
        return self.records_by_token.get(int(token), [])


def _cash_minutes(*, missing_index: int | None = None) -> list[dict[str, object]]:
    values: list[dict[str, object]] = []
    for index in range(5):
        if index == missing_index:
            continue
        values.append(
            {
                "date": datetime(2026, 8, 21, 9, 20 + index, tzinfo=config.IST),
                "open": 99.0 + index * 0.1,
                "high": 100.5,
                "low": 98.5,
                "close": 100.0,
                "volume": 1000 + index,
            }
        )
    return values


def test_direct_cash_audit_uses_one_range_call_and_rejects_missing_constituent(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    clients = [
        _HistoricalClient({100: _cash_minutes()}),
        _HistoricalClient({200: _cash_minutes()}),
        *[_HistoricalClient({}) for _ in range(6)],
    ]
    runtimes = tuple(
        market_data.AppRuntime(f"app{index + 1}", client, pace_seconds=0.0)
        for index, client in enumerate(clients)
    )
    snapshot = {
        "candidates": [
            {"tradingsymbol": "AAA", "instrument_token": 100},
            {"tradingsymbol": "BBB", "instrument_token": 200},
        ]
    }
    fetched, audit = session.fetch_exact_cash_signal_constituents(
        snapshot,
        paths,
        "09:25",
        runtimes,
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
        observations=1,
    )
    assert set(fetched) == {"AAA", "BBB"}
    assert sum(len(client.calls) for client in clients) == 2
    assert audit["source_contract"].startswith("ONE_RANGE_REQUEST_PER_CANDIDATE")

    clients[0] = _HistoricalClient({100: _cash_minutes(missing_index=2)})
    bad_runtimes = tuple(
        market_data.AppRuntime(f"app{index + 1}", client, pace_seconds=0.0)
        for index, client in enumerate(clients)
    )
    with pytest.raises(session.SourceIncompleteError, match="incomplete"):
        session.fetch_exact_cash_signal_constituents(
            {"candidates": [{"tradingsymbol": "AAA", "instrument_token": 100}]},
            paths,
            "09:25",
            bad_runtimes,
            observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
            observations=1,
        )


class _CountingEngine:
    def __init__(self, records: list[dict[str, object]] | None = None):
        self.register_calls = 0
        self._records = records or []

    def register_candidates(self, *args, **kwargs):
        self.register_calls += 1
        return []

    def records(self):
        return list(self._records)


def _install_cached_intake_sources(
    monkeypatch: pytest.MonkeyPatch,
    proof: dict[str, object],
    authority: dict[str, object],
) -> None:
    monkeypatch.setattr(
        session,
        "load_immutable_strict_cash_universe_source",
        lambda *args, **kwargs: (
            {"strict_cash_source_sha256": "6" * 64},
            "6" * 64,
        ),
    )
    monkeypatch.setattr(
        session,
        "load_immutable_universe_oi_proof",
        lambda *args, **kwargs: (proof, "4" * 64),
    )
    monkeypatch.setattr(
        session,
        "load_immutable_independent_candidate_source",
        lambda *args, **kwargs: (
            authority,
            session._sha256_bytes(session._json_bytes(authority)),
        ),
    )


def test_intake_seals_timing_evidence_before_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    raw = _scanner_candidate("AAA")
    _write_scanner(paths, _scanner_payload([raw]))
    proof, authority = _proof_and_authority([raw])
    _install_cached_intake_sources(monkeypatch, proof, authority)
    engine = _CountingEngine()
    ingested: set[str] = set()
    tokens: dict[str, int] = {}
    now = datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST)
    result = session._try_ingest_slot(
        paths=paths,
        observed=now,
        process_started_at=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        engine=engine,
        module=paper_engine,
        telemetry=session.SessionTelemetry(),
        ingested_slots=ingested,
        symbol_tokens=tokens,
        runtimes=_runtimes(),
        cash_constituent_fetcher=lambda *args, **kwargs: (
            _direct([raw]),
            {"kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT"},
        ),
        clock=lambda: now,
    )
    assert result is True
    assert engine.register_calls == 2
    audit = json.loads(
        (
            paths.cash_signal_audit_root / "slot_0925" / "direct_range_audit.json"
        ).read_text(encoding="utf-8")
    )
    assert audit["audit_started_at_ist"] == now.isoformat()
    assert audit["audit_finished_at_ist"] == now.isoformat()
    assert audit["decision_at_ist"] == now.isoformat()
    assert audit["confirmation_due_ist"] == "2026-08-21T09:26:03+05:30"
    assert audit["decision_before_confirmation_due"] is True
    candidate_book = json.loads(
        (paths.candidate_root / "candidate_book_0925_LONG.json").read_text(
            encoding="utf-8"
        )
    )
    candidate = candidate_book["candidates"][0]
    assert candidate["decision_at_ist"] == now.isoformat()
    assert candidate["decision_before_confirmation_due"] is True
    assert candidate["candidate_authority"] == "INDEPENDENT_ALL_MAPPED_STOCKS"
    assert candidate["candidate_authority_artifact_sha256"] == candidate_book[
        "source_sha256"
    ]
    assert candidate["present_in_v6_scanner_diagnostic"] is None
    assert candidate["v6_scanner_scalar_diagnostic"] is None
    assert candidate["v6_scanner_diagnostic_state"] == "PENDING_POST_REGISTRATION"


def test_range_crossing_s_plus_one_has_no_archive_or_engine_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    raw = _scanner_candidate("AAA")
    _write_scanner(paths, _scanner_payload([raw]))
    proof, authority = _proof_and_authority([raw])
    _install_cached_intake_sources(monkeypatch, proof, authority)
    crossed = False

    def fetch(*args, **kwargs):
        nonlocal crossed
        crossed = True
        return _direct([raw]), {"kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT"}

    def clock() -> datetime:
        return datetime(
            2026, 8, 21, 9, 26 if crossed else 25, 3 if crossed else 50,
            tzinfo=config.IST,
        )

    engine = _CountingEngine()
    ingested: set[str] = set()
    tokens: dict[str, int] = {}
    with pytest.raises(session.SourceIncompleteError, match="range audit crossed"):
        session._try_ingest_slot(
            paths=paths,
            observed=clock(),
            process_started_at=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
            engine=engine,
            module=paper_engine,
            telemetry=session.SessionTelemetry(),
            ingested_slots=ingested,
            symbol_tokens=tokens,
            runtimes=_runtimes(),
            cash_constituent_fetcher=fetch,
            clock=clock,
        )
    assert engine.register_calls == 0
    assert ingested == set() and tokens == {}
    assert not paths.cash_signal_audit_root.exists()
    assert not paths.candidate_root.exists()


def test_archive_crossing_s_plus_one_keeps_engine_and_maps_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    raw = _scanner_candidate("AAA")
    _write_scanner(paths, _scanner_payload([raw]))
    proof, authority = _proof_and_authority([raw])
    _install_cached_intake_sources(monkeypatch, proof, authority)
    crossed = False
    original_archive = session.archive_slot_inputs

    def archive_then_cross(*args, **kwargs):
        nonlocal crossed
        result = original_archive(*args, **kwargs)
        crossed = True
        return result

    monkeypatch.setattr(session, "archive_slot_inputs", archive_then_cross)

    def clock() -> datetime:
        return datetime(
            2026, 8, 21, 9, 26 if crossed else 25, 3 if crossed else 50,
            tzinfo=config.IST,
        )

    engine = _CountingEngine()
    ingested: set[str] = set()
    tokens: dict[str, int] = {}
    with pytest.raises(session.SourceIncompleteError, match="archives crossed"):
        session._try_ingest_slot(
            paths=paths,
            observed=clock(),
            process_started_at=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
            engine=engine,
            module=paper_engine,
            telemetry=session.SessionTelemetry(),
            ingested_slots=ingested,
            symbol_tokens=tokens,
            runtimes=_runtimes(),
            cash_constituent_fetcher=lambda *args, **kwargs: (
                _direct([raw]),
                {"kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT"},
            ),
            clock=clock,
        )
    assert engine.register_calls == 0
    assert ingested == set() and tokens == {}
    assert (paths.candidate_root / "candidate_book_0925_LONG.json").is_file()


def test_nine_candidate_intake_registers_before_deadline_without_scanner_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    candidates = [
        _scanner_candidate(f"S{index:02d}", token=100 + index)
        for index in range(9)
    ]
    proof, authority = _proof_and_authority(candidates)
    _install_cached_intake_sources(monkeypatch, proof, authority)
    monkeypatch.setattr(
        session,
        "load_finalized_v6_scanner_slot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("V6 diagnostic entered pre-registration path")
        ),
    )
    # Model the measured 8.345s all-symbol third-attempt retry path after the
    # futures proof is ready at roughly S+49.  Registration still precedes the
    # immutable S+1+3 deadline by five seconds without consulting V6.
    instants = iter(
        (
            datetime(2026, 8, 21, 9, 25, 49, tzinfo=config.IST),
            datetime(2026, 8, 21, 9, 25, 49, tzinfo=config.IST),
            datetime(2026, 8, 21, 9, 25, 49, 100_000, tzinfo=config.IST),
            datetime(2026, 8, 21, 9, 25, 57, 500_000, tzinfo=config.IST),
            datetime(2026, 8, 21, 9, 25, 57, 600_000, tzinfo=config.IST),
        )
    )
    last = datetime(2026, 8, 21, 9, 25, 58, tzinfo=config.IST)

    def clock() -> datetime:
        return next(instants, last)

    def direct_fetch(snapshot, *args, **kwargs):
        assert len(snapshot["candidates"]) == 9
        return _direct(candidates), {"kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT"}

    engine = _CountingEngine()
    telemetry = session.SessionTelemetry()
    ingested: set[str] = set()
    tokens: dict[str, int] = {}
    assert session._try_ingest_slot(
        paths=paths,
        observed=clock(),
        process_started_at=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        engine=engine,
        module=paper_engine,
        telemetry=telemetry,
        ingested_slots=ingested,
        symbol_tokens=tokens,
        runtimes=_runtimes(),
        cash_constituent_fetcher=direct_fetch,
        clock=clock,
    ) is True
    assert engine.register_calls == 2
    assert ingested == {"09:25"}
    assert len(tokens) == 9
    assert telemetry.slots["09:25"]["registration_recheck_at_ist"] == (
        "2026-08-21T09:25:58+05:30"
    )
    assert telemetry.slots["09:25"]["v6_scanner_diagnostic_state"] == "PENDING"


def test_intervention_requests_only_accepted_filled_open_symbols() -> None:
    engine = _CountingEngine(
        [
            {
                "symbol": "PENDING",
                "portfolio_decision": "ACCEPTED",
                "unconstrained_status": "PENDING_STOP",
            },
            {
                "symbol": "ACTUAL",
                "portfolio_decision": "ACCEPTED",
                "unconstrained_status": "FILLED_OPEN",
            },
            {
                "symbol": "SHADOW",
                "portfolio_decision": "REJECTED",
                "unconstrained_status": "FILLED_OPEN",
            },
        ]
    )
    assert session._intervention_open_symbols(engine) == ("ACTUAL",)


def _checkpoint_empty_engine(
    paths: session.SessionPaths,
    *,
    processed: datetime,
    telemetry: session.SessionTelemetry,
) -> None:
    engine = paper_engine.V8CombinedPaperEngine()
    engine.process_completed_minute(pd.Timestamp(processed), {})
    session.persist_checkpoint(
        paths,
        engine,
        telemetry,
        processed_clock_end=processed,
        ingested_slots=set(),
        symbol_tokens={},
    )


def test_terminal_intervention_checkpoint_recovers_before_credentials(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    bundle = _bundle()
    _checkpoint_empty_engine(
        paths,
        processed=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        telemetry=session.SessionTelemetry(
            state="STOPPED_CONTROL_INTERVENTION",
            phase="INTERVENTION_RECONCILED",
            runtime_bundle_sha256=bundle,
        ),
    )
    authenticated = False

    def authenticate():
        nonlocal authenticated
        authenticated = True
        return _runtimes()

    code = session.run_paper_session(
        DAY,
        paths=paths,
        control_paths=_control_paths(tmp_path),
        now_provider=lambda: datetime(2026, 8, 21, 9, 16, tzinfo=config.IST),
        bundle_provider=_bundle,
        authenticator=authenticate,
    )
    assert code == 2
    assert authenticated is False
    completion = json.loads(
        (paths.day_session_root / "completion.json").read_text(encoding="utf-8")
    )
    assert completion["state"] == "STOPPED_CONTROL_INTERVENTION"
    assert completion["valid_economic_result"] is False


@pytest.mark.parametrize("incomplete", (False, True))
def test_exact_1530_checkpoint_finalizes_idempotently_without_activation(
    tmp_path: Path,
    incomplete: bool,
) -> None:
    paths = _paths(tmp_path)
    bundle = _bundle()
    _checkpoint_empty_engine(
        paths,
        processed=datetime(2026, 8, 21, 15, 30, tzinfo=config.IST),
        telemetry=session.SessionTelemetry(
            state="RUNNING",
            phase="CHRONOLOGICAL_PAPER_REDUCER",
            data_incomplete=incomplete,
            runtime_bundle_sha256=bundle,
        ),
    )
    code = session.run_paper_session(
        DAY,
        paths=paths,
        control_paths=_control_paths(tmp_path),
        now_provider=lambda: datetime(2026, 8, 21, 15, 31, tzinfo=config.IST),
        bundle_provider=_bundle,
        authenticator=lambda: (_ for _ in ()).throw(AssertionError("credentials touched")),
    )
    assert code == (2 if incomplete else 0)
    completion_path = paths.day_session_root / "completion.json"
    completion = json.loads(completion_path.read_text(encoding="utf-8"))
    assert completion["state"] == ("DATA_INCOMPLETE" if incomplete else "COMPLETED")
    assert completion["valid_economic_result"] is (not incomplete)
    if not incomplete:
        # Simulate crash after the immutable manifest but before completion and
        # prove the same checkpoint can finish without an artifact collision.
        completion_path.unlink()
        assert session.run_paper_session(
            DAY,
            paths=paths,
            control_paths=_control_paths(tmp_path),
            now_provider=lambda: datetime(2026, 8, 21, 15, 31, tzinfo=config.IST),
            bundle_provider=_bundle,
            authenticator=lambda: (_ for _ in ()).throw(
                AssertionError("credentials touched")
            ),
        ) == 0
        assert completion_path.is_file()


def test_preflight_requires_loaded_dashboard_identity_before_apps(
    tmp_path: Path,
) -> None:
    authenticated = False

    def authenticate():
        nonlocal authenticated
        authenticated = True
        return _runtimes()

    def stale_dashboard(**kwargs):
        raise control.ActivationBlockedError(
            "DASHBOARD_RUNTIME_SOURCE_SHA256_MISMATCH"
        )

    code, payload = session.run_preflight(
        DAY,
        require_activation=False,
        authenticate_apps=False,
        observed_now=datetime(2026, 8, 21, 9, 0, tzinfo=config.IST),
        bundle_provider=_bundle,
        authenticator=authenticate,
        dashboard_identity_provider=stale_dashboard,
    )
    assert code == 2
    assert payload["reason"] == "DASHBOARD_RUNTIME_SOURCE_SHA256_MISMATCH"
    assert authenticated is False

    code, payload = session.run_preflight(
        DAY,
        require_activation=False,
        authenticate_apps=False,
        observed_now=datetime(2026, 8, 21, 9, 0, tzinfo=config.IST),
        bundle_provider=_bundle,
        dashboard_identity_provider=_dashboard_identity,
    )
    assert code == 0
    assert payload["dashboard_runtime_identity"]["source_sha256"] == "9" * 64


def test_strict_cash_is_prewarmed_before_later_futures_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    calls: list[str] = []
    now = datetime(2026, 8, 21, 9, 25, 18, tzinfo=config.IST)
    proof_ready = False
    strict_cache: dict[str, dict[str, object]] = {}
    proof_cache: dict[str, dict[str, object]] = {}
    authority_cache: dict[str, dict[str, object]] = {}

    monkeypatch.setattr(
        session, "load_immutable_strict_cash_universe_source", lambda *a, **k: None
    )
    monkeypatch.setattr(
        session, "load_immutable_universe_oi_proof", lambda *a, **k: None
    )
    monkeypatch.setattr(
        session, "load_immutable_independent_candidate_source", lambda *a, **k: None
    )
    monkeypatch.setattr(session, "_validate_strict_cash_universe_source", lambda *a, **k: None)
    monkeypatch.setattr(session, "_validate_universe_oi_proof_payload", lambda *a, **k: None)
    monkeypatch.setattr(session, "_validate_independent_candidate_source", lambda *a, **k: None)
    written_hashes: dict[str, str] = {}
    original_sha256_file = session._sha256_file

    def fake_write(path, payload):
        digest = session._sha256_bytes(session._json_bytes(payload))
        written_hashes[str(path)] = digest
        return digest

    monkeypatch.setattr(session, "_write_immutable_json", fake_write)
    monkeypatch.setattr(
        session,
        "_sha256_file",
        lambda path: (
            written_hashes[str(path)]
            if str(path) in written_hashes
            else original_sha256_file(path)
        ),
    )

    def cash_loader(*args, **kwargs):
        calls.append("cash")
        return {"kind": "STRICT", "rows": []}

    def proof_loader(*args, **kwargs):
        calls.append("proof")
        if not proof_ready:
            raise session.SourceNotReadyError("not yet")
        return {"kind": "PROOF", "contracts": []}

    def authority_loader(*args, **kwargs):
        calls.append("authority")
        assert kwargs["strict_cash_source"]["strict_cash_source_sha256"]
        return {"eligible_rows": [], "universe_rows": []}

    common = dict(
        paths=paths,
        observed=now,
        process_started_at=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        engine=_CountingEngine(),
        module=paper_engine,
        telemetry=session.SessionTelemetry(),
        ingested_slots=set(),
        symbol_tokens={},
        clock=lambda: now,
        strict_cash_source_loader=cash_loader,
        strict_cash_sources=strict_cache,
        oi_superset_proof_loader=proof_loader,
        oi_superset_proofs=proof_cache,
        independent_source_loader=authority_loader,
        independent_candidate_sources=authority_cache,
        runtimes=_runtimes(),
        cash_constituent_fetcher=lambda *args, **kwargs: (
            {},
            {"kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT"},
        ),
    )
    assert session._try_ingest_slot(**common) is False
    assert calls == ["cash", "proof"]
    proof_ready = True
    now = datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST)
    assert session._try_ingest_slot(**common) is True  # scanner is post-registration only
    assert calls == ["cash", "proof", "proof", "authority"]


def test_v6_scanner_is_archived_only_after_registration_and_is_idempotent(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    independent = _scanner_candidate("AAA")
    _write_sealed_authority(paths, [independent])
    # A structurally complete empty V6 broad book is valid diagnostic evidence;
    # it is not allowed to omit the independently eligible AAA candidate.
    _write_scanner(paths, _scanner_payload([]))
    telemetry = session.SessionTelemetry(
        slots={"09:25": {"state": "REGISTERED", "v6_scanner_diagnostic_state": "PENDING"}}
    )
    observed = datetime(2026, 8, 21, 9, 26, 0, tzinfo=config.IST)
    assert session.archive_pending_v6_scanner_diagnostics(
        paths, telemetry, {"09:25"}, observed_at=observed
    ) is True
    state = telemetry.slots["09:25"]
    assert state["state"] == "REGISTERED"
    assert state["v6_scanner_diagnostic_state"] == "ARCHIVED_POST_REGISTRATION"
    assert state["v6_missing_independent_eligible_count"] == 1
    reconciliation_path = Path(state["v6_scanner_reconciliation_path"])
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    assert reconciliation["v6_scanner_role"] == "POST_REGISTRATION_DIAGNOSTIC_ONLY"
    assert reconciliation["candidate_authority"] == "INDEPENDENT_ALL_MAPPED_STOCKS"
    assert reconciliation["v6_missing_independent_eligible"] == [
        {"side": "LONG", "tradingsymbol": "AAA"}
    ]
    # Simulate a crash after fsync but before its telemetry checkpoint.  The
    # existing evidence is validated/reused; a later observed time cannot collide.
    restarted = session.SessionTelemetry(slots={"09:25": {"state": "REGISTERED"}})
    assert session.archive_pending_v6_scanner_diagnostics(
        paths,
        restarted,
        {"09:25"},
        observed_at=observed + timedelta(seconds=1),
    ) is False
    assert restarted.slots["09:25"]["v6_scanner_diagnostic_state"] == (
        "ARCHIVED_POST_REGISTRATION"
    )


def test_missing_post_registration_v6_diagnostic_fails_at_boundary(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _write_sealed_authority(paths, [_scanner_candidate("AAA")])
    telemetry = session.SessionTelemetry(slots={"09:25": {"state": "REGISTERED"}})
    assert session.archive_pending_v6_scanner_diagnostics(
        paths,
        telemetry,
        {"09:25"},
        observed_at=datetime(2026, 8, 21, 9, 26, 2, tzinfo=config.IST),
    ) is False
    with pytest.raises(session.SourceIncompleteError, match=r"not finalized by S\+1\+3"):
        session.archive_pending_v6_scanner_diagnostics(
            paths,
            telemetry,
            {"09:25"},
            observed_at=datetime(2026, 8, 21, 9, 26, 3, tzinfo=config.IST),
        )


def test_run_checkpoints_registration_before_scanner_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    bundle = _bundle()
    order: list[str] = []
    intake_calls = 0

    monkeypatch.setattr(
        session.control,
        "evaluate_activation",
        lambda *args, **kwargs: _allowed(bundle),
    )

    def fake_intake(**kwargs):
        nonlocal intake_calls
        intake_calls += 1
        if intake_calls > 1:
            return False
        kwargs["ingested_slots"].add("09:25")
        kwargs["telemetry"].slots["09:25"] = {"state": "REGISTERED"}
        order.append("registered")
        return True

    monkeypatch.setattr(session, "_try_ingest_slot", fake_intake)
    monkeypatch.setattr(
        session,
        "persist_checkpoint",
        lambda *args, **kwargs: order.append("checkpoint"),
    )

    def diagnostic(*args, **kwargs):
        assert order[-1] == "checkpoint"
        order.append("diagnostic")
        return False

    now = datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST)
    assert session.run_paper_session(
        DAY,
        paths=paths,
        now_provider=lambda: now,
        sleep_fn=lambda _: None,
        bundle_provider=_bundle,
        authenticator=_runtimes,
        scanner_diagnostic_archiver=diagnostic,
        dashboard_identity_provider=_dashboard_identity,
        max_iterations=1,
    ) == 0
    assert order[:3] == ["registered", "checkpoint", "diagnostic"]


def test_unresolved_terminal_checkpoint_recovers_as_invalid_without_credentials(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _checkpoint_empty_engine(
        paths,
        processed=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        telemetry=session.SessionTelemetry(
            state="STOPPED_CONTROL_UNRESOLVED",
            phase="INTERVENTION_DATA_INCOMPLETE",
            data_incomplete=True,
            runtime_bundle_sha256=_bundle(),
        ),
    )
    code = session.run_paper_session(
        DAY,
        paths=paths,
        control_paths=_control_paths(tmp_path),
        now_provider=lambda: datetime(2026, 8, 21, 9, 16, tzinfo=config.IST),
        bundle_provider=_bundle,
        authenticator=lambda: (_ for _ in ()).throw(AssertionError("credentials touched")),
    )
    assert code == 2
    completion = json.loads(
        (paths.day_session_root / "completion.json").read_text(encoding="utf-8")
    )
    assert completion["state"] == "DATA_INCOMPLETE"
    assert completion["valid_economic_result"] is False


def test_restart_replays_empty_minutes_to_keep_checkpoint_and_engine_aligned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    bundle = _bundle()
    _checkpoint_empty_engine(
        paths,
        processed=datetime(2026, 8, 21, 9, 15, tzinfo=config.IST),
        telemetry=session.SessionTelemetry(
            state="RUNNING", phase="CHECKPOINT", runtime_bundle_sha256=bundle
        ),
    )
    monkeypatch.setattr(
        session.control, "evaluate_activation", lambda *args, **kwargs: _allowed(bundle)
    )
    now = datetime(2026, 8, 21, 9, 17, 4, tzinfo=config.IST)
    code = session.run_paper_session(
        DAY,
        paths=paths,
        now_provider=lambda: now,
        sleep_fn=lambda _: None,
        bundle_provider=_bundle,
        authenticator=_runtimes,
        dashboard_identity_provider=_dashboard_identity,
        max_iterations=1,
    )
    assert code == 0
    restored = session.load_checkpoint(
        paths, paper_engine, expected_runtime_bundle_sha256=bundle
    )
    assert restored is not None
    engine, processed, *_ = restored
    assert processed == datetime(2026, 8, 21, 9, 17, tzinfo=config.IST)
    assert engine.last_processed_minute == processed


def test_restart_reuses_published_active_minute_without_api_refetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    bundle = _bundle()
    engine = paper_engine.V8CombinedPaperEngine()
    setup = config.setup_for("09:25", "LONG")
    assert setup is not None
    session.register_candidate_book(
        engine, paper_engine, setup, [_engine_candidate()], paths
    )
    end_0925 = datetime(2026, 8, 21, 9, 25, tzinfo=config.IST)
    session.process_engine_minute(engine, paper_engine, end_0925, pd.DataFrame(), paths)
    session.persist_checkpoint(
        paths,
        engine,
        session.SessionTelemetry(
            state="RUNNING", phase="CHECKPOINT", runtime_bundle_sha256=bundle
        ),
        processed_clock_end=end_0925,
        ingested_slots={"09:25"},
        symbol_tokens={"AAA": 100},
    )
    runtimes = _runtimes()
    end = datetime(2026, 8, 21, 9, 26, tzinfo=config.IST)
    frame = _frame(
        "AAA", end.isoformat(), open_=100.0, high=101.0, low=99.9, close=100.8
    )
    marker = {
        "schema_version": market_data.MINUTE_SNAPSHOT_SCHEMA_VERSION,
        "policy_version": market_data.MARKET_DATA_POLICY_VERSION,
        "expected_end_ist": end.isoformat(),
        "completed_boundary_ist": (end + timedelta(seconds=3)).isoformat(),
        "observed_at_ist": (end + timedelta(seconds=4)).isoformat(),
        "candidate_count": 1,
        "candidate_contract_sha256": session.common.canonical_json_sha256(
            [{"symbol": "AAA", "instrument_token": 100}]
        ),
        "app_roster": market_data.app_roster_payload(runtimes),
        "app_roster_sha256": market_data.app_roster_sha256(runtimes),
        "app_usage": [],
        "outcomes": [],
        "written_count": 1,
        "complete": True,
        "state": "SUCCESS",
    }
    market_data.publish_minute_snapshot_once(
        paths.minute_root,
        frame,
        marker,
        strategy_fingerprint=config.strategy_fingerprint(),
    )
    monkeypatch.setattr(
        session.control, "evaluate_activation", lambda *args, **kwargs: _allowed(bundle)
    )
    called = False

    def forbidden(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("published snapshot was re-fetched")

    code = session.run_paper_session(
        DAY,
        paths=paths,
        now_provider=lambda: end + timedelta(seconds=4),
        sleep_fn=lambda _: None,
        bundle_provider=_bundle,
        authenticator=lambda: runtimes,
        minute_fetcher=forbidden,
        scanner_diagnostic_archiver=lambda *args, **kwargs: False,
        dashboard_identity_provider=_dashboard_identity,
        max_iterations=1,
    )
    assert code == 0
    assert called is False
    restored = session.load_checkpoint(
        paths, paper_engine, expected_runtime_bundle_sha256=bundle
    )
    assert restored is not None
    assert restored[1] == end
    assert restored[0].last_processed_minute == end


def test_independent_authority_keeps_candidate_missing_from_v6_diagnostic(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    raw = _scanner_candidate("AAA")
    proof, authority = _proof_and_authority([raw])
    setup = config.setup_for("09:25", "LONG")
    assert setup is not None
    book = session.build_v8_candidate_book(
        _scanner_payload([]),
        setup,
        paths,
        source_sha256="a" * 64,
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
        cash_constituent_audit=_direct([raw]),
        universe_oi_proof=proof,
        independent_candidate_source=authority,
    )
    assert [item["symbol"] for item in book] == ["AAA"]
    assert book[0]["present_in_v6_scanner_diagnostic"] is False


def test_candidate_rejects_oi_source_hash_change_after_universe_proof(
    tmp_path: Path,
) -> None:
    raw = _scanner_candidate("AAA")
    proof, authority = _proof_and_authority([raw])
    authority["eligible_rows"][0]["_oi_pair"] = {
        **authority["eligible_rows"][0]["_oi_pair"],
        "source_file_sha256": "0" * 64,
    }
    setup = config.setup_for("09:25", "LONG")
    assert setup is not None
    with pytest.raises(session.SourceIncompleteError, match="changed after universe proof"):
        session.build_v8_candidate_book(
            _scanner_payload([raw]),
            setup,
            _paths(tmp_path),
            source_sha256="a" * 64,
            observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
            cash_constituent_audit=_direct([raw]),
            universe_oi_proof=proof,
            independent_candidate_source=authority,
        )


def test_untraded_contract_is_excluded_from_slot_not_fatal(tmp_path: Path) -> None:
    """A contract with no S-5 bar is dropped from the slot, not session-fatal.

    Regression for 2026-09-02, when LICHSGFIN26SEPFUT did not trade between
    09:15 and 09:20. The exchange published no bar, so there was nothing to
    fetch or repair, yet the all-universe proof vetoed the entire session and
    V10/V11/V12 all produced zero candidates.
    """
    paths = _paths(tmp_path)
    _write_one_contract_universe(paths, drop_predecessor=True)
    proof = session.prove_v6_oi_shift_is_exact_for_stock_universe(
        paths,
        "09:25",
        observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
    )
    assert proof["stock_contracts_proven"] == 0
    assert proof["stock_contracts_excluded"] == 1
    assert proof["stock_contracts_in_universe"] == 1
    assert proof["excluded_contracts"][0]["tradingsymbol"] == "AAA26AUGFUT"
    # expected must stay equal to proven so the payload validator still binds.
    assert proof["stock_contracts_expected"] == proof["stock_contracts_proven"]


def test_excluding_too_many_contracts_is_still_fatal(tmp_path: Path) -> None:
    """A broad feed outage must not pass silently as a pile of exclusions."""
    paths = _paths(tmp_path)
    _write_one_contract_universe(paths, drop_predecessor=True)
    with mock.patch.object(session, "MAX_OI_PROOF_EXCLUSIONS", 0):
        with pytest.raises(session.SourceIncompleteError, match="too many contracts"):
            session.prove_v6_oi_shift_is_exact_for_stock_universe(
                paths,
                "09:25",
                observed_at=datetime(2026, 8, 21, 9, 25, 50, tzinfo=config.IST),
            )
