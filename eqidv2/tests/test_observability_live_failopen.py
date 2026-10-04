from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import threading
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_oi_common as common
import fno_v13_v10_g_live_config as g_config
import ai_platform.observability.runtime as runtime_module
from ai_platform.observability.runtime import _AsyncSpanSink, create_observability


ROOT = Path(__file__).resolve().parents[1]
SESSION_DATE = date(2026, 9, 24)


@pytest.fixture
def g_live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Load the G runtime against temporary local storage, with no broker I/O."""

    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G")
    monkeypatch.delenv("FNO_V6_EXECUTION_SESSION_NAMESPACE", raising=False)
    monkeypatch.delenv("EQIDV2_OBSERVABILITY_ENABLED", raising=False)
    monkeypatch.delenv("EQIDV2_OBS_RUN_ID", raising=False)
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno_oi")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    monkeypatch.setattr(
        common,
        "runtime_dir",
        lambda *parts: tmp_path.joinpath("runtime", *parts),
    )

    spec = importlib.util.spec_from_file_location(
        f"observability_failopen_live_{tmp_path.name}", ROOT / "fno_v5_live.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.config is g_config
    return module


def _scanner_inputs(signal_end: str = "11:20"):
    slot = g_config.slot_datetime(SESSION_DATE, signal_end)
    equity = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp(slot - timedelta(minutes=5)),
                "open": 101.0,
                "high": 101.2,
                "low": 100.7,
                "close": 101.0,
                "volume": 100.0,
            },
            {
                "ts": pd.Timestamp(slot),
                "open": 100.0,
                "high": 100.2,
                "low": 99.0,
                "close": 99.2,
                "volume": 1_000.0,
            },
        ]
    )
    futures = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp(slot - timedelta(minutes=5)),
                "open": 500.0,
                "high": 501.0,
                "low": 499.0,
                "close": 500.0,
                "volume": 20.0,
                "oi": 100_000.0,
            },
            {
                "ts": pd.Timestamp(slot),
                "open": 500.0,
                "high": 501.0,
                "low": 499.0,
                "close": 500.0,
                "volume": 20.0,
                "oi": 100_500.0,
            },
        ]
    )
    featured = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp(slot),
                "open": 100.0,
                "high": 100.2,
                "low": 99.0,
                "close": 99.2,
                "volume": 1_000.0,
                "price_change_pct": -0.8,
                "oi_change_pct": 0.5,
                "oi": 100_500.0,
                "prev_oi": 100_000.0,
                "volume_ratio": 10.0,
                "traded_value": 50_000_000.0,
                "ema9": 99.0,
                "ema20": 100.0,
                "ema50": 101.0,
            }
        ]
    )
    universe = pd.DataFrame(
        [
            {
                "underlying": "EXAMPLE",
                "futures_tradingsymbol": "EXAMPLE26SEPFUT",
                "equity_symbol": "EXAMPLE",
                "futures_instrument_token": 222,
                "equity_instrument_token": 111,
                "equity_tick_size": 0.05,
            }
        ]
    )
    return universe, equity, futures, featured


def test_live_observability_rotating_log_is_unique_per_process(
    g_live, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[tuple[Path, Path]] = []

    def create(_service, *, log_path, journal_path, **_kwargs):
        captured.append((Path(log_path), Path(journal_path)))
        return SimpleNamespace()

    monkeypatch.setenv("EQIDV2_OBSERVABILITY_ENABLED", "1")
    monkeypatch.setattr(runtime_module, "create_observability", create)
    for pid in (101, 202):
        monkeypatch.setattr(g_live.os, "getpid", lambda value=pid: value)
        g_live._OBSERVABILITY_INITIALIZED = False
        g_live._OBSERVABILITY_RUNTIME = None
        assert g_live._observability_runtime() is not None

    assert captured[0][0] != captured[1][0]
    assert captured[0][0].name.endswith("-101.jsonl")
    assert captured[1][0].name.endswith("-202.jsonl")
    assert captured[0][1] == captured[1][1]


def test_scanner_telemetry_exceptions_do_not_change_authoritative_candidates(
    g_live, monkeypatch: pytest.MonkeyPatch
) -> None:
    universe, equity, futures, featured = _scanner_inputs()
    monkeypatch.setattr(
        g_live.backtest, "load_five_minute", lambda _symbol: futures.copy()
    )
    monkeypatch.setattr(
        g_live.hybrid,
        "load_equity_five_minute",
        lambda *_args, **_kwargs: equity.copy(),
    )
    monkeypatch.setattr(
        g_live.hybrid,
        "join_equity_price_with_futures_oi",
        lambda *_args: featured.copy(),
    )

    baseline = g_live.scan_five_minute_slot(universe, SESSION_DATE, "11:20")
    assert baseline["state"] == "SUCCESS"
    assert len(baseline["candidates"]) == 1

    def telemetry_failure(*_args, **_kwargs):
        raise RuntimeError("simulated telemetry sink failure")

    monkeypatch.setattr(g_live, "evaluate_ohlcv", telemetry_failure)
    monkeypatch.setattr(g_live, "evaluate_v13_v10_g_base_row", telemetry_failure)
    observed = g_live.scan_five_minute_slot(universe, SESSION_DATE, "11:20")

    assert observed["state"] == baseline["state"]
    assert observed["contracts_evaluated"] == baseline["contracts_evaluated"]
    assert observed["candidates"] == baseline["candidates"]
    assert observed["feature_evaluations"] == [
        {
            "schema_version": g_live.FEATURE_LEDGER_SCHEMA,
            "session_date": SESSION_DATE.isoformat(),
            "signal_ts": g_config.slot_datetime(SESSION_DATE, "11:20").isoformat(),
            "tradingsymbol": "EXAMPLE",
            "futures_tradingsymbol": "EXAMPLE26SEPFUT",
            "run_id": g_live.RUN_ID,
            "evaluation_state": "TELEMETRY_ERROR",
            "error_type": "RuntimeError",
        }
    ]
    assert observed["raw_data_quality"][0]["status"] == "TELEMETRY_ERROR"


def test_order_state_is_persisted_when_observability_event_fails(
    g_live, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order_root = tmp_path / "orders"
    live_root = tmp_path / "live"
    monkeypatch.setattr(g_live, "ORDER_ROOT", order_root)
    monkeypatch.setattr(g_live, "LIVE_ROOT", live_root)

    class Counter:
        def inc(self, *_args, **_kwargs):
            return True

    class BrokenRuntime:
        standard_metrics = SimpleNamespace(order_events_total=Counter())

        def bind(self, **_fields):
            return contextlib.nullcontext()

        def event(self, *_args, **_kwargs):
            raise RuntimeError("simulated event sink failure")

    monkeypatch.setattr(g_live, "_observability_runtime", lambda: BrokenRuntime())
    state = {
        "session_date": SESSION_DATE.isoformat(),
        "mode": "PAPER",
        "signal_id": "signal-fail-open",
        "status": "PENDING_ENTRY",
        "status_reason": "WAITING_FOR_TRIGGER",
        "strategy_version": g_config.STRATEGY_VERSION,
        "strategy_fingerprint": "test-fingerprint",
        "tradingsymbol": "EXAMPLE",
        "side": "LONG",
        "quantity": 10,
    }

    assert g_live._write_order_state(state) is None

    path = g_live._order_path(SESSION_DATE, "PAPER", "signal-fail-open")
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == state


def test_broker_metric_failure_cannot_mask_successful_callback(
    g_live, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenCounter:
        def inc(self, *_args, **_kwargs):
            raise RuntimeError("counter unavailable")

    class BrokenHistogram:
        def observe(self, *_args, **_kwargs):
            raise RuntimeError("histogram unavailable")

    runtime = SimpleNamespace(
        span=lambda *_args, **_kwargs: contextlib.nullcontext(),
        standard_metrics=SimpleNamespace(
            broker_requests_total=BrokenCounter(),
            broker_request_duration_seconds=BrokenHistogram(),
        ),
    )
    monkeypatch.setattr(g_live, "_observability_runtime", lambda: runtime)
    calls: list[str] = []

    def broker_callback():
        calls.append("called")
        return {"order_id": "broker-123"}

    assert g_live._observe_broker_call("place_order", broker_callback) == {
        "order_id": "broker-123"
    }
    assert calls == ["called"]


def test_broken_span_cannot_prevent_or_duplicate_broker_callback(
    g_live, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenSpanRuntime:
        standard_metrics = SimpleNamespace(
            broker_requests_total=SimpleNamespace(inc=lambda **_kwargs: True),
            broker_request_duration_seconds=SimpleNamespace(
                observe=lambda *_args, **_kwargs: True
            ),
        )

        def span(self, *_args, **_kwargs):
            raise RuntimeError("span provider unavailable")

    monkeypatch.setattr(g_live, "_observability_runtime", lambda: BrokenSpanRuntime())
    calls: list[str] = []

    def broker_callback():
        calls.append("called")
        return "broker-result"

    assert g_live._observe_broker_call("orders", broker_callback) == "broker-result"
    assert calls == ["called"]


def test_async_span_sink_flushes_and_stops_cleanly() -> None:
    output = io.StringIO()
    runtime = create_observability(
        "async-fail-open-test",
        stream=output,
        enable_opentelemetry=False,
        async_span_logging=True,
        span_queue_size=2,
    )
    sink = runtime._async_span_sink
    assert sink is not None

    with runtime.span("unit.async"):
        pass

    assert runtime.flush_spans(timeout_seconds=2.0)
    events = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [event["event"] for event in events] == ["trace.span.completed"]
    assert events[0]["fields"]["span_name"] == "unit.async"

    runtime.shutdown(timeout_seconds=2.0)
    assert not sink._thread.is_alive()


def test_async_span_sink_full_queue_stops_after_timed_out_shutdown() -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def persist(item: str) -> None:
        calls.append(item)
        if item == "first":
            started.set()
            assert release.wait(timeout=2.0)

    sink = _AsyncSpanSink(
        persist,
        on_drop=lambda _reason: None,
        capacity=1,
    )
    sink.submit("first")
    assert started.wait(timeout=1.0)
    sink.submit("second")

    # The first callback deliberately exceeds this bound while the sole queue
    # slot is occupied.  Shutdown must return promptly without dropping the
    # queued callback or leaving the worker permanently blocked afterward.
    sink.shutdown(timeout_seconds=0.01)
    assert sink._thread.is_alive()
    release.set()
    sink._thread.join(timeout=1.0)

    assert not sink._thread.is_alive()
    assert calls == ["first", "second"]
    assert sink._pending == 0
    assert sink._queue.empty()
    sink.shutdown(timeout_seconds=0.01)
    assert calls == ["first", "second"]


def test_registered_atexit_callback_is_idempotent_after_explicit_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registrations: list[tuple[object, tuple[object, ...]]] = []
    monkeypatch.setattr(
        runtime_module.atexit,
        "register",
        lambda callback, *args: registrations.append((callback, args)),
    )
    output = io.StringIO()
    runtime = runtime_module.create_observability(
        "atexit-idempotency-test",
        stream=output,
        enable_opentelemetry=False,
        async_span_logging=True,
    )
    with runtime.span("unit.once"):
        pass
    assert runtime.flush_spans(timeout_seconds=2.0)

    runtime.shutdown(timeout_seconds=2.0)
    assert len(registrations) == 1
    callback, args = registrations[0]
    callback(*args)

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [event["fields"]["span_name"] for event in events] == ["unit.once"]
    assert runtime._async_span_sink is not None
    assert not runtime._async_span_sink._thread.is_alive()


def test_scoped_broker_position_reconciliation_persists_complete_mismatch(
    g_live, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(g_live, "_observability_runtime", lambda: None)

    class Broker:
        def positions(self):
            return {
                "net": [
                    {
                        "exchange": "NSE",
                        "product": "MIS",
                        "tradingsymbol": "EXAMPLE",
                        "quantity": 3,
                    },
                    {
                        "exchange": "NSE",
                        "product": "CNC",
                        "tradingsymbol": "UNRELATED",
                        "quantity": 99,
                    },
                ]
            }

        def orders(self):
            tag = g_live._live_tag("signal-1")
            return [
                {
                    "order_id": "STOP-1",
                    "tag": tag,
                    "tradingsymbol": "EXAMPLE",
                    "status": "TRIGGER PENDING",
                    "exchange": "NSE",
                    "product": "MIS",
                    "transaction_type": "SELL",
                    "order_type": "SL-M",
                    "quantity": 5,
                },
                {
                    "order_id": "TARGET-1",
                    "tag": tag,
                    "tradingsymbol": "EXAMPLE",
                    "status": "OPEN",
                    "exchange": "NSE",
                    "product": "MIS",
                    "transaction_type": "SELL",
                    "order_type": "LIMIT",
                    "quantity": 5,
                }
            ]

    states = [
        {
            "mode": "LIVE",
            "status": "OPEN",
            "signal_id": "signal-1",
            "tradingsymbol": "EXAMPLE",
            "exchange": "NSE",
            "side": "LONG",
            "quantity": 5,
            "stop_order_id": "STOP-1",
            "target_order_id": "TARGET-1",
        }
    ]
    result = g_live._publish_broker_position_reconciliation(
        states, Broker(), SESSION_DATE
    )

    assert result["broker_truth_available"] is True
    assert result["scope_complete"] is True
    assert result["mismatch_count"] == 1
    assert result["mismatches"] == [
        {
            "tradingsymbol": "EXAMPLE",
            "local_expected_quantity": 5,
            "broker_quantity": 3,
        }
    ]
    assert result["active_order_parity_complete"] is True, result[
        "active_order_mismatches"
    ]
    assert result["active_order_mismatch_count"] == 0
    path = (
        tmp_path
        / "runtime"
        / "observability"
        / "reconciliation"
        / f"broker_positions_{SESSION_DATE.isoformat()}.json"
    )
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["schema_version"] == (
        "v13_v10_g_broker_position_reconciliation_v2"
    )
    assert report["position_reconciliation"] == result
    claimed_digest = report.pop("report_sha256")
    assert g_live.common.canonical_json_sha256(report) == claimed_digest


def test_dedicated_broker_reconciliation_role_is_read_only_and_digest_verified(
    g_live, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(g_live, "_observability_runtime", lambda: None)
    calls: list[str] = []

    class ReadOnlyBroker:
        def positions(self):
            calls.append("positions")
            return {"net": []}

        def orders(self):
            calls.append("orders")
            return []

        def place_order(self, **_kwargs):
            raise AssertionError("Reconciliation attempted to place an order")

        def cancel_order(self, **_kwargs):
            raise AssertionError("Reconciliation attempted to cancel an order")

    class ReadOnlyPool:
        def __init__(self, max_apps, timeout_sec):
            assert max_apps == 1
            assert timeout_sec == 1.0
            self.client = ReadOnlyBroker()

        def positions(self):
            return self.client.positions()

        def orders(self):
            return self.client.orders()

    published: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(g_live, "KitePool", ReadOnlyPool)
    monkeypatch.setattr(g_live, "load_order_states", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        g_live,
        "_publish",
        lambda role, state, **extra: published.append((role, state, extra)),
    )
    args = SimpleNamespace(
        execution_mode="LIVE",
        max_apps=1,
        timeout_sec=1.0,
        broker_reconcile_sec=30.0,
        once=True,
    )

    assert g_live.run_broker_reconciliation(args, SESSION_DATE) == 0
    assert calls == ["positions", "orders"]
    assert published[0][0] == "broker-reconciliation"
    assert published[0][2]["broker_position_reconciliation"] == {
        "broker_truth_available": True,
        "scope": "nse_mis_strategy_tagged_symbols_and_active_orders",
        "scope_complete": True,
        "mismatch_count": 0,
        "mismatches": [],
        "unscoped_nonzero_positions": [],
        "local_state_count": 0,
        "tagged_symbol_count": 0,
        "tagged_order_count": 0,
        "local_expected_active_order_count": 0,
        "local_expected_active_order_ids": [],
        "broker_active_tagged_order_count": 0,
        "broker_active_tagged_order_ids": [],
        "active_order_parity_complete": True,
        "active_order_mismatch_count": 0,
        "active_order_mismatches": [],
    }
    path = (
        tmp_path
        / "runtime"
        / "observability"
        / "reconciliation"
        / f"broker_positions_{SESSION_DATE.isoformat()}.json"
    )
    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["schema_version"] == (
        "v13_v10_g_broker_position_reconciliation_v2"
    )
    claimed_digest = report.pop("report_sha256")
    assert g_live.common.canonical_json_sha256(report) == claimed_digest


@pytest.mark.parametrize("finding", ["incomplete_scope", "mismatch"])
def test_dedicated_broker_reconciliation_degrades_on_untrusted_truth(
    g_live,
    monkeypatch: pytest.MonkeyPatch,
    finding: str,
) -> None:
    monkeypatch.setattr(g_live, "_observability_runtime", lambda: None)

    class Pool:
        def __init__(self, *_args, **_kwargs):
            pass

        def positions(self):
            if finding == "incomplete_scope":
                return {
                    "net": [
                        {
                            "exchange": "NSE",
                            "product": "MIS",
                            "tradingsymbol": "UNSCOPED",
                            "quantity": 1,
                        }
                    ]
                }
            return {
                "net": [
                    {
                        "exchange": "NSE",
                        "product": "MIS",
                        "tradingsymbol": "EXAMPLE",
                        "quantity": 2,
                    }
                ]
            }

        def orders(self):
            if finding == "incomplete_scope":
                return []
            tag = g_live._live_tag("signal-1")
            return [
                {
                    "order_id": "STOP-1",
                    "tag": tag,
                    "tradingsymbol": "EXAMPLE",
                    "status": "TRIGGER PENDING",
                    "exchange": "NSE",
                    "product": "MIS",
                    "transaction_type": "SELL",
                    "order_type": "SL-M",
                    "quantity": 1,
                },
                {
                    "order_id": "TARGET-1",
                    "tag": tag,
                    "tradingsymbol": "EXAMPLE",
                    "status": "OPEN",
                    "exchange": "NSE",
                    "product": "MIS",
                    "transaction_type": "SELL",
                    "order_type": "LIMIT",
                    "quantity": 1,
                },
            ]

    states = (
        []
        if finding == "incomplete_scope"
        else [
            {
                "mode": "LIVE",
                "status": "OPEN",
                "signal_id": "signal-1",
                "tradingsymbol": "EXAMPLE",
                "exchange": "NSE",
                "side": "LONG",
                "quantity": 1,
                "stop_order_id": "STOP-1",
                "target_order_id": "TARGET-1",
            }
        ]
    )
    published: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(g_live, "KitePool", Pool)
    monkeypatch.setattr(
        g_live, "load_order_states", lambda *_args, **_kwargs: states
    )
    monkeypatch.setattr(
        g_live,
        "_publish",
        lambda role, state, **extra: published.append((role, state, extra)),
    )
    args = SimpleNamespace(
        execution_mode="LIVE",
        max_apps=1,
        timeout_sec=1.0,
        broker_reconcile_sec=30.0,
        once=True,
    )

    assert g_live.run_broker_reconciliation(args, SESSION_DATE) == 2
    assert published[0][1] == "DEGRADED"
    result = published[0][2]["broker_position_reconciliation"]
    if finding == "incomplete_scope":
        assert result["scope_complete"] is False
        assert result["mismatch_count"] == 0
    else:
        assert result["scope_complete"] is True
        assert result["mismatch_count"] == 1
        assert result["active_order_parity_complete"] is True, result[
            "active_order_mismatches"
        ]


@pytest.mark.parametrize(
    ("local_status", "broker_status", "broker_rows", "expected_kind"),
    [
        (
            "CANCELLED",
            "OPEN",
            "unexpected",
            "unexpected_broker_active_tagged_order",
        ),
        (
            "OPEN",
            "OPEN",
            "missing_target",
            "local_expected_active_order_missing_at_broker",
        ),
        (
            "PENDING_ENTRY",
            "MYSTERY PENDING",
            "expected_entry",
            "broker_active_order_status_unknown",
        ),
    ],
)
def test_active_tagged_order_reconciliation_fails_closed(
    g_live,
    monkeypatch: pytest.MonkeyPatch,
    local_status: str,
    broker_status: str,
    broker_rows: str,
    expected_kind: str,
) -> None:
    monkeypatch.setattr(g_live, "_observability_runtime", lambda: None)
    signal_id = "signal-active-1"
    tag = g_live._live_tag(signal_id)
    state = {
        "mode": "LIVE",
        "status": local_status,
        "signal_id": signal_id,
        "tradingsymbol": "EXAMPLE",
        "exchange": "NSE",
        "side": "LONG",
        "quantity": 1,
        "entry_order_id": "ENTRY-1",
        "stop_order_id": "STOP-1",
        "target_order_id": "TARGET-1",
        "squareoff_order_id": "",
    }

    def order(
        order_id: str, order_type: str, transaction_type: str, status: str
    ) -> dict:
        return {
            "order_id": order_id,
            "tag": tag,
            "tradingsymbol": "EXAMPLE",
            "status": status,
            "exchange": "NSE",
            "product": "MIS",
            "transaction_type": transaction_type,
            "order_type": order_type,
            "quantity": 1,
        }

    if broker_rows == "unexpected":
        orders = [order("ORPHAN-1", "LIMIT", "SELL", broker_status)]
    elif broker_rows == "missing_target":
        orders = [order("STOP-1", "SL-M", "SELL", "TRIGGER PENDING")]
    else:
        orders = [order("ENTRY-1", "SL-M", "BUY", broker_status)]

    class Pool:
        def __init__(self, *_args, **_kwargs):
            pass

        def positions(self):
            return {"net": []}

        def orders(self):
            return orders

    published: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(g_live, "KitePool", Pool)
    monkeypatch.setattr(g_live, "load_order_states", lambda *_args, **_kwargs: [state])
    monkeypatch.setattr(
        g_live,
        "_publish",
        lambda role, status, **extra: published.append((role, status, extra)),
    )
    args = SimpleNamespace(
        execution_mode="LIVE",
        max_apps=1,
        timeout_sec=1.0,
        broker_reconcile_sec=30.0,
        once=True,
    )

    assert g_live.run_broker_reconciliation(args, SESSION_DATE) == 2
    assert published[0][1] == "DEGRADED"
    result = published[0][2]["broker_position_reconciliation"]
    assert result["active_order_parity_complete"] is False
    assert result["active_order_mismatch_count"] >= 1
    assert expected_kind in {
        row["kind"] for row in result["active_order_mismatches"]
    }


def test_terminal_tagged_broker_order_is_evidence_not_active_mismatch(
    g_live, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(g_live, "_observability_runtime", lambda: None)

    class Pool:
        def __init__(self, *_args, **_kwargs):
            pass

        def positions(self):
            return {"net": []}

        def orders(self):
            return [
                {
                    "order_id": "DONE-1",
                    "tag": g_live._live_tag("signal-done"),
                    "tradingsymbol": "EXAMPLE",
                    "status": "CANCELLED",
                    "exchange": "NSE",
                    "product": "MIS",
                    "transaction_type": "BUY",
                    "order_type": "SL-M",
                    "quantity": 1,
                }
            ]

    state = {
        "mode": "LIVE",
        "status": "CANCELLED",
        "signal_id": "signal-done",
        "tradingsymbol": "EXAMPLE",
        "exchange": "NSE",
        "side": "LONG",
        "quantity": 1,
        "entry_order_id": "DONE-1",
    }
    published: list[tuple[str, str, dict]] = []
    monkeypatch.setattr(g_live, "KitePool", Pool)
    monkeypatch.setattr(g_live, "load_order_states", lambda *_args, **_kwargs: [state])
    monkeypatch.setattr(
        g_live,
        "_publish",
        lambda role, status, **extra: published.append((role, status, extra)),
    )
    args = SimpleNamespace(
        execution_mode="LIVE",
        max_apps=1,
        timeout_sec=1.0,
        broker_reconcile_sec=30.0,
        once=True,
    )

    assert g_live.run_broker_reconciliation(args, SESSION_DATE) == 0
    result = published[0][2]["broker_position_reconciliation"]
    assert result["tagged_order_count"] == 1
    assert result["broker_active_tagged_order_count"] == 0
    assert result["active_order_parity_complete"] is True
