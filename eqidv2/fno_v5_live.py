"""Shared live/paper runtime for locked FNO EMA/OI strategy generations.

Seven independently monitored roles share this module:

* ``scanner-5m`` waits for exact-slot final markers from both feeds, reads NSE
  equity price/volume/indicators, joins only OI fields from the mapped future,
  and emits the same loose candidate superset used by the backtest.
* ``confirmation-1m`` reads only the durable completed candidate-equity
  1-minute feed and publishes NSE-equity stop-entry signals. Broker polling is
  isolated in the independent feed producer.
* ``long-entry`` and ``short-entry`` manage one side each.
* ``trade-logger`` continuously consolidates immutable signal/order state.
* ``net-result`` continuously marks the current book and reports net results.
* ``broker-reconciliation`` performs read-only broker/local position checks.

Scheduled runners default to PAPER.  Real broker orders require LIVE mode, an
exact acknowledgement environment variable, and a same-day arm file.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import importlib
import json
import math
import os
import re
import sys
import threading
import time
from dataclasses import asdict
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_equity_fetch_1min as equity_feed
import fno_live_evidence as live_evidence
from ai_platform.observability.data_quality import (
    canonical_frame_sha256,
    canonical_payload_sha256,
    evaluate_ohlcv,
)
from ai_platform.observability.feature_ledger import (
    FEATURE_LEDGER_SCHEMA,
    evaluate_v13_v10_g_base_row,
)
import fno_oi_ema_confirm_backtest as backtest
import fno_oi_hybrid_data as hybrid
from fno_live_profile import config_for_generation, is_g_config
from fno_v13_v10_g_identity import canonical_signal_id
import fno_v13_v10_g_policy as g_policy
LIVE_GENERATION = os.getenv("FNO_LIVE_GENERATION", "v5").strip().lower()
if LIVE_GENERATION not in {"v5", "v6"}:
    raise RuntimeError(f"Unsupported FnO live generation: {LIVE_GENERATION}")
config = config_for_generation(LIVE_GENERATION)
LIVE_LABEL = LIVE_GENERATION.upper()
DISPLAY_LABEL = getattr(config, "DISPLAY_LABEL", LIVE_LABEL)
REPORT_PREFIX = getattr(config, "REPORT_PREFIX", f"fno_{LIVE_GENERATION}")
SESSION_PREFIX = getattr(config, "SESSION_PREFIX", f"fno_{LIVE_GENERATION}")
LIVE_SCHEMA_PREFIX = f"fno_{LIVE_GENERATION}"
EXECUTION_SESSION_NAMESPACE = os.getenv(
    f"FNO_{LIVE_LABEL}_EXECUTION_SESSION_NAMESPACE", ""
).strip().lower()
RUN_ID = os.getenv("EQIDV2_OBS_RUN_ID", "").strip() or (
    f"{SESSION_PREFIX}_{common.now_ist().strftime('%Y%m%dT%H%M%S')}_{os.getpid()}"
)
if EXECUTION_SESSION_NAMESPACE and not re.fullmatch(
    r"[a-z0-9][a-z0-9_-]{0,39}", EXECUTION_SESSION_NAMESPACE
):
    raise RuntimeError(
        "Invalid FnO execution-session namespace: "
        f"{EXECUTION_SESSION_NAMESPACE!r}"
    )


ROLE_SESSIONS = {
    "scanner-5m": f"{SESSION_PREFIX}_scanner_5min",
    "confirmation-1m": f"{SESSION_PREFIX}_confirmation_1min",
    "long-entry": f"{SESSION_PREFIX}_live_long",
    "short-entry": f"{SESSION_PREFIX}_live_short",
    "trade-logger": f"{SESSION_PREFIX}_trade_logger",
    "net-result": f"{SESSION_PREFIX}_net_result",
    "broker-reconciliation": f"{SESSION_PREFIX}_broker_reconciliation",
}
ROLE_REPORTS = {
    "scanner-5m": f"latest_{REPORT_PREFIX}_scanner_5min.md",
    "confirmation-1m": f"latest_{REPORT_PREFIX}_confirmation_1min.md",
    "long-entry": f"latest_{REPORT_PREFIX}_live_long.md",
    "short-entry": f"latest_{REPORT_PREFIX}_live_short.md",
    "trade-logger": f"latest_{REPORT_PREFIX}_trade_logger.md",
    "net-result": f"latest_{REPORT_PREFIX}_net_result.md",
    "broker-reconciliation": (
        f"latest_{REPORT_PREFIX}_broker_reconciliation.md"
    ),
}
if EXECUTION_SESSION_NAMESPACE:
    # A dedicated LIVE worker must not overwrite the promoted PAPER worker's
    # status, heartbeat, or report. Signal and order roots remain canonical;
    # orders themselves are already isolated below orders/LIVE.
    for _role, _suffix in (
        ("long-entry", "long"),
        ("short-entry", "short"),
        ("trade-logger", "trade_logger"),
        ("net-result", "net_result"),
        ("broker-reconciliation", "broker_reconciliation"),
    ):
        ROLE_SESSIONS[_role] = (
            f"{SESSION_PREFIX}_{EXECUTION_SESSION_NAMESPACE}_{_suffix}"
        )
        ROLE_REPORTS[_role] = (
            f"latest_{REPORT_PREFIX}_{EXECUTION_SESSION_NAMESPACE}_{_suffix}.md"
        )

LIVE_ROOT = common.FNO_ROOT / getattr(config, "LIVE_ROOT_NAME", f"{LIVE_GENERATION}_live")
SCANNER_ROOT = LIVE_ROOT / "scanner_5m"
CONFIRMATION_ROOT = LIVE_ROOT / "confirmation_1m"
SIGNAL_ROOT = LIVE_ROOT / "signals"
ORDER_ROOT = LIVE_ROOT / "orders"
CONSOLIDATED_ROOT = LIVE_ROOT / "consolidated"
EVIDENCE_ROOT = LIVE_ROOT / "evidence"
STRATEGY_MANIFEST_PATH = LIVE_ROOT / "strategy_manifest.json"
CONTROL_ROOT = common.FNO_ROOT / getattr(config, "CONTROL_ROOT_NAME", LIVE_ROOT.name)
LIVE_ARM_PATH = CONTROL_ROOT / "live_arm.json"
KILL_SWITCH_PATH = CONTROL_ROOT / "kill_switch.json"
LIVE_ACK_ENV = getattr(config, "LIVE_ACK_ENV", f"FNO_{LIVE_LABEL}_LIVE_ACK")
LIVE_ACK = getattr(
    config, "LIVE_ACK", f"I_UNDERSTAND_REAL_FNO_{LIVE_LABEL}_EQUITY_ORDERS"
)
ORDER_TAG_PREFIX = getattr(config, "ORDER_TAG_PREFIX", f"F{LIVE_LABEL}")
AUTO_MARKET_PROTECTION = -1
ORDER_ROLE_TAG_SUFFIX = {
    "entry": "E",
    "stop": "S",
    "target": "T",
    "squareoff": "X",
}

SESSION_END = dtime.fromisoformat(getattr(config, "SESSION_END", "15:32"))
PIPELINE_DEADLINE = dtime.fromisoformat(getattr(config, "PIPELINE_DEADLINE", "09:50"))
TERMINAL_STATES = {
    "CLOSED",
    "NO_FILL",
    "ENTRY_REJECTED",
    "BLOCKED_SIZING",
    "BLOCKED_PORTFOLIO",
    "CANCELLED",
}
BROKER_TERMINAL_ORDER_STATUSES = frozenset({"COMPLETE", "CANCELLED", "REJECTED"})
BROKER_ACTIVE_ORDER_STATUSES = frozenset(
    {
        "OPEN",
        "OPEN PENDING",
        "TRIGGER PENDING",
        "VALIDATION PENDING",
        "PUT ORDER REQ RECEIVED",
        "MODIFY VALIDATION PENDING",
        "MODIFY PENDING",
        "CANCEL PENDING",
    }
)

for _directory in (
    LIVE_ROOT,
    SCANNER_ROOT,
    CONFIRMATION_ROOT,
    SIGNAL_ROOT,
    ORDER_ROOT,
    CONSOLIDATED_ROOT,
    EVIDENCE_ROOT,
):
    _directory.mkdir(parents=True, exist_ok=True)


_OBSERVABILITY_RUNTIME: Any | None = None
_OBSERVABILITY_INITIALIZED = False
_OBSERVABILITY_LOCK = threading.Lock()
_OBSERVABILITY_EVENT_LOCK = threading.Lock()
_REPORTED_DUPLICATE_ORDER_GROUPS: set[tuple[str, ...]] = set()
_PENDING_DUPLICATE_ORDER_EVENTS: list[dict[str, Any]] = []


def _observability_runtime() -> Any | None:
    """Lazily create fail-open live telemetry only for supervised/opted-in runs."""

    global _OBSERVABILITY_RUNTIME, _OBSERVABILITY_INITIALIZED
    configured = os.getenv("EQIDV2_OBSERVABILITY_ENABLED", "").strip().lower()
    enabled = (
        configured in {"1", "true", "yes", "on"}
        if configured
        else bool(os.getenv("EQIDV2_OBS_RUN_ID", "").strip())
    )
    if not enabled:
        return None
    if _OBSERVABILITY_INITIALIZED:
        return _OBSERVABILITY_RUNTIME
    with _OBSERVABILITY_LOCK:
        if _OBSERVABILITY_INITIALIZED:
            return _OBSERVABILITY_RUNTIME
        _OBSERVABILITY_INITIALIZED = True
        try:
            from ai_platform.observability.runtime import create_observability

            service = f"{REPORT_PREFIX}-live-runtime"
            root = common.runtime_dir("observability")
            _OBSERVABILITY_RUNTIME = create_observability(
                service,
                # RotatingFileHandler is process-safe only when each worker
                # owns its file. Alloy aggregates the per-PID JSONL glob while
                # the stable service label preserves one logical pipeline.
                log_path=root / "logs" / f"{service}-{os.getpid()}.jsonl",
                journal_path=root / "journals" / f"{service}-events.jsonl",
                async_span_logging=True,
            )
        except Exception:
            _OBSERVABILITY_RUNTIME = None
        return _OBSERVABILITY_RUNTIME


def _flush_observability_metrics(runtime: Any | None = None) -> None:
    """Publish a per-process textfile snapshot for the read-only collector."""

    observed = runtime or _observability_runtime()
    if observed is None:
        return
    try:
        path = (
            common.runtime_dir("observability", "metrics")
            / f"{SESSION_PREFIX}_{os.getpid()}.prom"
        )
        common.atomic_write_text(path, observed.metrics.render_prometheus())
    except Exception:
        try:
            observed.standard_metrics.telemetry_dropped_total.inc(
                component="textfile", reason="write_failure"
            )
        except Exception:
            pass


def _observe_broker_call(operation: str, callback: Any) -> Any:
    try:
        runtime = _observability_runtime()
    except Exception:
        # Even a malformed/custom telemetry runtime cannot gate broker I/O.
        return callback()
    if runtime is None:
        return callback()
    started = time.perf_counter()
    outcome = "error"
    span = None
    span_entered = False
    callback_error: BaseException | None = None
    callback_traceback = None
    try:
        try:
            span = runtime.span(
                f"broker.{operation}",
                kind="client",
                attributes={"broker.operation": operation},
            )
            span.__enter__()
            span_entered = True
        except Exception:
            # Span creation/export is diagnostic. Continue with the broker
            # operation exactly once even when the telemetry object is broken.
            span = None
        try:
            result = callback()
            outcome = "success"
        except BaseException as exc:
            callback_error = exc
            callback_traceback = exc.__traceback__
        finally:
            if span_entered and span is not None:
                try:
                    span.__exit__(
                        type(callback_error) if callback_error is not None else None,
                        callback_error,
                        callback_traceback,
                    )
                except Exception:
                    pass
        if callback_error is not None:
            raise callback_error.with_traceback(callback_traceback)
        return result
    finally:
        # Telemetry must not mask a broker exception or replace a successful
        # broker result with an instrumentation failure.
        try:
            elapsed = time.perf_counter() - started
            runtime.standard_metrics.broker_requests_total.inc(
                operation=operation, outcome=outcome
            )
            runtime.standard_metrics.broker_request_duration_seconds.observe(
                elapsed, operation=operation, outcome=outcome
            )
            # The worker heartbeat/status path publishes the coalesced metric
            # snapshot after canonical state has been persisted. Never perform
            # textfile I/O inline with a broker operation.
        except Exception:
            pass


def report_path(role: str) -> Path:
    return common.LATEST_DIR / ROLE_REPORTS[role]


def scanner_slot_path(session_date: date, signal_end: str) -> Path:
    return (
        SCANNER_ROOT
        / session_date.isoformat()
        / f"slot_{signal_end.replace(':', '')}.json"
    )


def confirmation_slot_path(session_date: date, signal_end: str) -> Path:
    confirmation_end = config.SIGNAL_TO_CONFIRMATION[signal_end]
    return (
        CONFIRMATION_ROOT
        / session_date.isoformat()
        / f"slot_{confirmation_end.replace(':', '')}.json"
    )


def signal_day_dir(session_date: date) -> Path:
    return SIGNAL_ROOT / session_date.isoformat()


def order_day_dir(session_date: date, mode: str) -> Path:
    root = ORDER_ROOT / mode.upper()
    if mode.upper() == "LIVE" and EXECUTION_SESSION_NAMESPACE:
        root = root / EXECUTION_SESSION_NAMESPACE
    return root / session_date.isoformat()


def consolidated_csv_path(session_date: date) -> Path:
    return CONSOLIDATED_ROOT / (
        f"{SESSION_PREFIX}_trades_{session_date.isoformat()}.csv"
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return common.read_json(path)
    except (OSError, ValueError, TypeError):
        return {}


def _archive_json_evidence(
    artifact_kind: str,
    session_date: date,
    slot: str,
    payload: dict[str, Any],
) -> Path | None:
    """Capture append-only evidence.

    V6 is fail-closed because a decision that cannot be replayed must never
    become an entry.  V5 retains its original best-effort compatibility mode.
    """

    if not payload:
        if LIVE_GENERATION == "v6":
            raise RuntimeError(
                f"V6 decision evidence is empty: {artifact_kind} "
                f"{session_date} {slot}"
            )
        return None
    try:
        return live_evidence.archive_json_evidence(
            EVIDENCE_ROOT,
            generation=LIVE_GENERATION,
            session_date=session_date,
            slot=slot,
            artifact_kind=artifact_kind,
            payload=payload,
        )
    except Exception as exc:
        if LIVE_GENERATION == "v6":
            raise RuntimeError(
                f"V6 decision evidence archive failed for {artifact_kind} "
                f"{session_date} {slot}: {type(exc).__name__}: {exc}"
            ) from exc
        print(
            f"[EVIDENCE][WARN] {artifact_kind} {session_date} {slot}: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        return None


def _archive_mapped_universe_evidence(
    session_date: date,
    signal_end: str,
    universe: pd.DataFrame | None,
) -> None:
    if universe is None or universe.empty:
        if LIVE_GENERATION == "v6":
            raise RuntimeError(
                f"V6 mapped-universe evidence is missing for "
                f"{session_date} {signal_end}"
            )
        return
    futures_symbols = sorted(
        {
            str(value).strip().upper()
            for value in universe["futures_tradingsymbol"].dropna()
            if str(value).strip()
        }
    )
    equity_symbols = sorted(
        {
            str(value).strip().upper()
            for value in universe["equity_symbol"].dropna()
            if str(value).strip()
        }
    )
    payload = {
        "schema_version": "fno_mapped_stock_universe_evidence_v1",
        "run_id": RUN_ID,
        "generation": LIVE_GENERATION,
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "contract_count": len(futures_symbols),
        "futures_symbols": futures_symbols,
        "futures_symbol_set_sha256": common.symbol_set_sha256(futures_symbols),
        "futures_universe_sha256": _futures_universe_sha256(universe),
        "equity_symbols": equity_symbols,
        "equity_symbol_set_sha256": common.symbol_set_sha256(equity_symbols),
        "equity_universe_sha256": _equity_universe_sha256(universe),
    }
    _archive_json_evidence("mapped_universe", session_date, signal_end, payload)


def _write_scanner_snapshot(
    session_date: date,
    signal_end: str,
    snapshot: dict[str, Any],
) -> None:
    if LIVE_GENERATION == "v6":
        _archive_json_evidence("scanner_snapshot", session_date, signal_end, snapshot)
        common.atomic_write_json(scanner_slot_path(session_date, signal_end), snapshot)
    else:
        common.atomic_write_json(scanner_slot_path(session_date, signal_end), snapshot)
        _archive_json_evidence("scanner_snapshot", session_date, signal_end, snapshot)


def _write_confirmation_snapshot(
    session_date: date,
    signal_end: str,
    snapshot: dict[str, Any],
) -> None:
    if LIVE_GENERATION == "v6":
        _archive_json_evidence(
            "confirmation_snapshot", session_date, signal_end, snapshot
        )
        common.atomic_write_json(
            confirmation_slot_path(session_date, signal_end), snapshot
        )
    else:
        common.atomic_write_json(
            confirmation_slot_path(session_date, signal_end), snapshot
        )
        _archive_json_evidence(
            "confirmation_snapshot", session_date, signal_end, snapshot
        )


def _commit_confirmation_decision(
    session_date: date,
    signal_end: str,
    snapshot: dict[str, Any],
    selected_signals: list[dict[str, Any]],
) -> None:
    """Commit signal files before making their confirmation authoritative.

    Entry workers only accept IDs listed by the canonical confirmation
    snapshot.  Therefore a signal-write failure leaves no SUCCESS evidence,
    while an evidence-archive failure leaves only ignored stray signal files.
    """

    if snapshot.get("state") == "SUCCESS":
        for signal in selected_signals:
            _write_entry_signal(signal)
    _write_confirmation_snapshot(session_date, signal_end, snapshot)


def _read_runtime_status(role: str, session_date: date) -> dict[str, str]:
    path = common.session_status_path(ROLE_SESSIONS[role])
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}
    status: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        status[key.strip().lstrip("\ufeff")] = value.strip()
    recorded_date = status.get("session_date_ist", "")
    if recorded_date:
        return status if recorded_date == session_date.isoformat() else {}
    try:
        stamp = datetime.fromisoformat(status.get("ts", "").replace("Z", "+00:00"))
    except ValueError:
        return {}
    if stamp.tzinfo is not None:
        stamp = stamp.astimezone(common.IST)
    return status if stamp.date() == session_date else {}


def _blocking_pipeline_issue(
    session_date: date,
    roles: tuple[str, ...] = ("scanner-5m", "confirmation-1m"),
) -> dict[str, str]:
    for role in roles:
        status = _read_runtime_status(role, session_date)
        state = status.get("status", "").upper()
        if state not in {"BLOCKED", "FAILED", "CRASHED"}:
            continue
        return {
            "role": role,
            "session": ROLE_SESSIONS[role],
            "state": state,
            "phase": status.get("phase", ""),
            "reason": status.get("reason") or status.get("error") or "",
        }
    return {}


def _pipeline_notice_lines(
    session_date: date,
    roles: tuple[str, ...] = ("scanner-5m", "confirmation-1m"),
) -> tuple[list[str], dict[str, str]]:
    issue = _blocking_pipeline_issue(session_date, roles)
    if not issue:
        return [], {}
    stage = issue["session"]
    phase = issue.get("phase", "")
    reason = issue.get("reason", "") or phase or "upstream session failed"
    lines = [
        f"Pipeline state: **{issue['state']}**",
        f"Blocking stage: `{_md(stage)}`{f' / {_md(phase)}' if phase else ''}",
        f"Reason: {_md(reason)}",
        "",
    ]
    return lines, issue


def _publish_upstream_block(role: str, issue: dict[str, str]) -> None:
    _publish(
        role,
        "BLOCKED",
        phase="UPSTREAM_BLOCKED",
        upstream_session=issue.get("session", ""),
        upstream_state=issue.get("state", ""),
        upstream_phase=issue.get("phase", ""),
        reason=issue.get("reason", "") or issue.get("phase", ""),
    )


def _current_slot_snapshot(
    path: Path,
    session_date: date,
    signal_end: str,
) -> bool:
    snapshot = _read_json(path)
    state = str(snapshot.get("state", "")) if snapshot else ""
    terminal = state in {"SUCCESS", "BLOCKED_STALE_ACTIVATION"} or (
        state == "BLOCKED_INCOMPLETE_DATA"
        and snapshot.get("scanner_complete") is False
    )
    return bool(
        snapshot
        and terminal
        and snapshot.get("strategy_version") == config.STRATEGY_VERSION
        and snapshot.get("strategy_fingerprint") == config.strategy_fingerprint()
        and snapshot.get("session_date") == session_date.isoformat()
        and snapshot.get("signal_end") == signal_end
    )


def _write_manifest() -> None:
    payload = config.strategy_payload()
    payload["strategy_fingerprint"] = config.strategy_fingerprint()
    payload["backtest_attestation"] = config.attest_selected_backtest()
    payload["written_at_ist"] = common.now_ist().isoformat(timespec="seconds")
    common.atomic_write_json(STRATEGY_MANIFEST_PATH, payload)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if np.isfinite(number) else default


def _optional_finite_float(value: Any) -> float | None:
    """Return a JSON-stable optional number for diagnostic snapshot fields."""

    number = _safe_float(value, np.nan)
    return float(number) if np.isfinite(number) else None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_ist_datetime(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp.to_pydatetime()


def _md(value: Any) -> str:
    return str(value if value is not None else "").replace("|", "/").replace("\n", " ")


def _iso_now() -> str:
    return common.now_ist().isoformat(timespec="seconds")


def _signal_id(session_date: date, setup: config.SetupSpec, symbol: str) -> str:
    return canonical_signal_id(
        config.STRATEGY_VERSION,
        session_date,
        setup.signal_end,
        setup.confirmation_end,
        setup.side,
        symbol,
    )


def _load_universe(session_date: date) -> pd.DataFrame:
    universe = common.load_near_month_universe(expected_date=session_date).copy()
    mapped, excluded = hybrid.ensure_equity_mapping(universe)
    unmapped_stock = excluded.loc[
        excluded["reason"].ne("INDEX_FUTURE_HAS_NO_CASH_EQUITY")
    ] if not excluded.empty else excluded
    if not unmapped_stock.empty:
        raise RuntimeError(
            "FNO stock-future equity mapping is incomplete: "
            f"{unmapped_stock.head(10).to_dict('records')}"
        )
    if mapped.empty:
        raise RuntimeError("FNO universe contains no mapped stock futures.")
    mapped.attrs["excluded_index_futures"] = (
        int(excluded["reason"].eq("INDEX_FUTURE_HAS_NO_CASH_EQUITY").sum())
        if not excluded.empty
        else 0
    )
    return mapped


def _g_nifty_first_bar_context(session_date: date) -> dict[str, Any]:
    """Read only the dated near-month NIFTY future's completed 09:20 bar."""
    context = {"nifty_first_bar_return_pct": None, "nifty_context_state": "MISSING"}
    try:
        universe = common.load_near_month_universe(expected_date=session_date)
        rows = universe.loc[universe["underlying"].astype(str).str.upper().eq("NIFTY")]
        if len(rows) != 1:
            raise ValueError("Expected one dated near-month NIFTY future")
        symbol = str(rows.iloc[0]["tradingsymbol"])
        value = config.nifty_context_from_bars(backtest.load_five_minute(symbol), session_date)
        context.update(nifty_futures_tradingsymbol=symbol,
                       nifty_feature_timestamp=config.slot_datetime(session_date, "09:20").isoformat())
        if np.isfinite(value):
            context.update(nifty_first_bar_return_pct=float(value), nifty_context_state="READY")
    except (OSError, KeyError, ValueError, TypeError) as exc:
        context["nifty_context_reason"] = f"{type(exc).__name__}: {exc}"
    return context


def _base_signal_side(row: pd.Series) -> str | None:
    if hasattr(config, "base_signal_side"):
        return config.base_signal_side(row)
    required = (
        "ema9",
        "ema20",
        "ema50",
        "price_change_pct",
        "oi_change_pct",
        "volume_ratio",
        "oi",
        "prev_oi",
    )
    if any(pd.isna(row.get(column)) for column in required):
        return None
    if float(row["oi"]) <= float(row["prev_oi"]):
        return None
    if float(row["oi_change_pct"]) < config.BASE_OI_CHANGE_PCT:
        return None
    if float(row["volume_ratio"]) < config.BASE_VOLUME_RATIO:
        return None
    if (
        float(row["ema9"]) > float(row["ema20"]) > float(row["ema50"])
        and float(row["price_change_pct"]) >= config.BASE_PRICE_CHANGE_PCT
    ):
        return "LONG"
    if (
        float(row["ema9"]) < float(row["ema20"]) < float(row["ema50"])
        and float(row["price_change_pct"]) <= -config.BASE_PRICE_CHANGE_PCT
    ):
        return "SHORT"
    return None


def scan_five_minute_slot(
    universe: pd.DataFrame,
    session_date: date,
    signal_end: str,
    *,
    verified_no_candle_symbols: set[str] | None = None,
) -> dict[str, Any]:
    slot = config.slot_datetime(session_date, signal_end)
    candidates: list[dict[str, Any]] = []
    feature_evaluations: list[dict[str, Any]] = []
    raw_data_quality: list[dict[str, Any]] = []
    verified_skips = {
        str(symbol).strip().upper()
        for symbol in (verified_no_candle_symbols or set())
        if str(symbol).strip()
    }
    expected_futures = {
        str(value).strip().upper()
        for value in universe["futures_tradingsymbol"].dropna()
        if str(value).strip()
    }
    unknown_verified_skips = sorted(verified_skips - expected_futures)
    skipped_no_candle: list[dict[str, Any]] = []
    missing_contracts: list[dict[str, Any]] = []
    evaluated = 0
    invalid = 0
    nifty_context: dict[str, Any] = {}
    g_strategy_fingerprint = config.strategy_fingerprint() if is_g_config(config) else ""
    if is_g_config(config) and signal_end == "09:25":
        nifty_context = _g_nifty_first_bar_context(session_date)
    for contract in universe.to_dict("records"):
        futures_symbol = str(contract["futures_tradingsymbol"])
        equity_symbol = str(contract["equity_symbol"])
        if futures_symbol.strip().upper() in verified_skips:
            skipped_no_candle.append(
                {
                    "state": "SKIPPED_NO_CANDLE",
                    "futures_tradingsymbol": futures_symbol,
                    "equity_symbol": equity_symbol,
                    "underlying": str(contract.get("underlying", "")),
                    "signal_end": signal_end,
                    "reason": "repeatedly_verified_exact_slot_no_candle",
                }
            )
            # This is deliberately decided from the contemporaneous fetch marker.
            # A later backfill must not turn a live skip into a hindsight signal.
            continue
        futures_frame = backtest.load_five_minute(futures_symbol)
        equity_frame = hybrid.load_equity_five_minute(
            equity_symbol, root=hybrid.LIVE_EQUITY_5M_DIR
        )
        if futures_frame.empty or equity_frame.empty:
            missing_contracts.append(
                {
                    "state": "MISSING_DATA",
                    "futures_tradingsymbol": futures_symbol,
                    "equity_symbol": equity_symbol,
                    "signal_end": signal_end,
                    "reason": (
                        "futures_and_equity_files_empty"
                        if futures_frame.empty and equity_frame.empty
                        else "futures_file_empty"
                        if futures_frame.empty
                        else "equity_file_empty"
                    ),
                }
            )
            continue
        effective_input: dict[str, Any] = {}
        if is_g_config(config):
            try:
                equity_columns = [name for name in (
                    "ts", "open", "high", "low", "close", "volume"
                ) if name in equity_frame]
                futures_columns = [name for name in (
                    "ts", "open", "high", "low", "close", "volume", "oi"
                ) if name in futures_frame]
                equity_history = equity_frame.loc[
                    equity_frame["ts"].le(pd.Timestamp(slot)), equity_columns
                ] if "ts" in equity_frame else equity_frame.loc[:, equity_columns]
                futures_pair = futures_frame.loc[
                    futures_frame["ts"].le(pd.Timestamp(slot)), futures_columns
                ].tail(2) if "ts" in futures_frame else futures_frame.loc[:, futures_columns].tail(2)
                effective_input = {
                    "schema_version": "v13_v10_g_live_base_input_slice_v1",
                    "equity_5m_history_sha256": canonical_frame_sha256(
                        equity_history, sort_by=["ts"] if "ts" in equity_history else None
                    ),
                    "equity_5m_history_rows": len(equity_history),
                    "futures_oi_pair_sha256": canonical_frame_sha256(
                        futures_pair, sort_by=["ts"] if "ts" in futures_pair else None
                    ),
                    "futures_oi_pair_rows": len(futures_pair),
                    "signal_ts": slot.isoformat(),
                }
                effective_input["input_slice_sha256"] = canonical_payload_sha256(
                    effective_input
                )
                raw_data_quality.extend([
                    {
                        **evaluate_ohlcv(
                            equity_history,
                            source="NSE_EQUITY_5M_LIVE",
                            symbol=equity_symbol,
                        ).to_dict(),
                        "session_date": session_date.isoformat(),
                        "signal_end": signal_end,
                        "input_slice_sha256": effective_input[
                            "equity_5m_history_sha256"
                        ],
                    },
                    {
                        **evaluate_ohlcv(
                            futures_pair,
                            expected_timestamps=[slot - timedelta(minutes=5), slot],
                            source="NFO_FUTURE_5M_LIVE",
                            symbol=futures_symbol,
                        ).to_dict(),
                        "session_date": session_date.isoformat(),
                        "signal_end": signal_end,
                        "input_slice_sha256": effective_input[
                            "futures_oi_pair_sha256"
                        ],
                    },
                ])
            except Exception as exc:
                # Observability is fail-open. Existing scanner readiness and
                # strategy gates remain the authoritative safety controls.
                raw_data_quality.append({
                    "schema_version": "ai_platform_data_quality_v1",
                    "session_date": session_date.isoformat(),
                    "signal_end": signal_end,
                    "source": "SCANNER_INPUTS",
                    "symbol": equity_symbol,
                    "status": "TELEMETRY_ERROR",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                })
        featured = hybrid.join_equity_price_with_futures_oi(
            equity_frame, futures_frame
        )
        selected = featured.loc[featured["ts"].eq(pd.Timestamp(slot))]
        if selected.empty:
            missing_contracts.append(
                {
                    "state": "MISSING_DATA",
                    "futures_tradingsymbol": futures_symbol,
                    "equity_symbol": equity_symbol,
                    "signal_end": signal_end,
                    "reason": "exact_slot_join_missing",
                }
            )
            continue
        row = selected.iloc[-1]
        evaluated += 1
        if is_g_config(config):
            try:
                feature_evaluations.append(evaluate_v13_v10_g_base_row(
                    {
                        **row.to_dict(),
                        "session_date": session_date.isoformat(),
                        "signal_ts": slot,
                        "confirmation_ts": slot + timedelta(minutes=1),
                        "signal_end": signal_end,
                        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
                        "tradingsymbol": equity_symbol,
                        "futures_tradingsymbol": futures_symbol,
                        "run_id": RUN_ID,
                        "nifty_first_bar_return_pct": nifty_context.get(
                            "nifty_first_bar_return_pct"
                        ),
                        "input_slice_sha256": effective_input.get(
                            "input_slice_sha256", ""
                        ),
                    },
                    nifty_return=nifty_context.get("nifty_first_bar_return_pct"),
                    strategy_version=config.STRATEGY_VERSION,
                    strategy_fingerprint=g_strategy_fingerprint,
                ))
            except Exception as exc:
                feature_evaluations.append({
                    "schema_version": FEATURE_LEDGER_SCHEMA,
                    "session_date": session_date.isoformat(),
                    "signal_ts": slot.isoformat(),
                    "tradingsymbol": equity_symbol,
                    "futures_tradingsymbol": futures_symbol,
                    "run_id": RUN_ID,
                    "evaluation_state": "TELEMETRY_ERROR",
                    "error_type": type(exc).__name__,
                })
        side = (
            config.base_signal_side(row, signal_end=signal_end, session_date=session_date,
                                    nifty_first_bar_return_pct=nifty_context.get("nifty_first_bar_return_pct"))
            if is_g_config(config) else _base_signal_side(row)
        )
        if side is None:
            continue
        values = {
            "tradingsymbol": equity_symbol,
            "exchange": "NSE",
            "underlying": str(contract.get("underlying", "")),
            "instrument_token": _safe_int(contract.get("equity_instrument_token")),
            "lot_size": 1,
            "tick_size": _safe_float(contract.get("equity_tick_size"), 0.05),
            "futures_tradingsymbol": futures_symbol,
            "futures_instrument_token": _safe_int(
                contract.get("futures_instrument_token")
            ),
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "price_source": "NSE_EQUITY",
            "oi_source": "NFO_FUTURE",
            "side": side,
            "signal_end": signal_end,
            "signal_timestamp": slot.isoformat(),
            "open": _optional_finite_float(row.get("open")),
            "high": _optional_finite_float(row.get("high")),
            "low": _optional_finite_float(row.get("low")),
            "close": _optional_finite_float(row.get("close")),
            "volume": _optional_finite_float(row.get("volume")),
            "signal_close": _safe_float(row["close"]),
            "price_change_pct": _safe_float(row["price_change_pct"]),
            "oi_change_pct": _safe_float(row["oi_change_pct"]),
            "oi": _safe_float(row["oi"]),
            "prev_oi": _safe_float(row["prev_oi"]),
            "volume_ratio": _safe_float(row["volume_ratio"]),
            "traded_value": _safe_float(row["traded_value"]),
            "ema9": _safe_float(row["ema9"]),
            "ema20": _safe_float(row["ema20"]),
            "ema50": _safe_float(row["ema50"]),
            "equity_5m_history_sha256": effective_input.get(
                "equity_5m_history_sha256", ""
            ),
            "futures_oi_pair_sha256": effective_input.get(
                "futures_oi_pair_sha256", ""
            ),
            "input_slice_sha256": effective_input.get("input_slice_sha256", ""),
        }
        if is_g_config(config):
            values.update(nifty_context)
            values["strategy_profile"] = config.STRATEGY_PROFILE
            values["feature_available_at_ist"] = slot.isoformat()
        if values["instrument_token"] <= 0 or values["signal_close"] <= 0:
            invalid += 1
            continue
        candidates.append(values)
    candidates.sort(key=lambda item: (item["side"], item["tradingsymbol"]))
    skipped_symbols = sorted(
        item["futures_tradingsymbol"] for item in skipped_no_candle
    )
    missing_symbols = sorted(
        item["futures_tradingsymbol"] for item in missing_contracts
    )
    snapshot = {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_scanner_5m_hybrid_v3",
        "run_id": RUN_ID,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
        "published_at_ist": _iso_now(),
        "contracts_expected": int(len(universe)),
        "excluded_index_futures": int(
            universe.attrs.get("excluded_index_futures", 0)
        ),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "price_volume_indicator_source": "NSE_EQUITY",
        "equity_five_minute_quality": "COMPLETED_REAL_END_LABELLED_ONLY",
        "oi_source": "NFO_FUTURE",
        "contracts_evaluated": evaluated,
        "contracts_skipped_no_candle": len(skipped_no_candle),
        "skipped_no_candle_symbols": skipped_symbols,
        "skipped_no_candle_contracts": skipped_no_candle,
        "contracts_missing_slot": len(missing_contracts),
        "contracts_unexpected_missing": len(missing_contracts),
        "unexpected_missing_symbols": missing_symbols,
        "missing_contracts": missing_contracts,
        "unknown_verified_no_candle_symbols": unknown_verified_skips,
        "invalid_candidates": invalid,
        "long_candidates": sum(item["side"] == "LONG" for item in candidates),
        "short_candidates": sum(item["side"] == "SHORT" for item in candidates),
        "candidates": candidates,
        "index_context": nifty_context,
        "state": (
            "SUCCESS"
            if not missing_contracts
            and invalid == 0
            and not unknown_verified_skips
            else "PARTIAL"
        ),
    }
    if is_g_config(config):
        snapshot.update(
            feature_ledger_schema=FEATURE_LEDGER_SCHEMA,
            feature_evaluation_phase="SCANNER_BASE_GATES",
            feature_evaluation_count=len(feature_evaluations),
            feature_evaluations=feature_evaluations,
            feature_evaluations_sha256=canonical_payload_sha256(feature_evaluations),
            raw_data_quality_count=len(raw_data_quality),
            raw_data_quality=raw_data_quality,
            raw_data_quality_sha256=canonical_payload_sha256(raw_data_quality),
        )
    return snapshot


def confirmation_metrics(
    candidate: dict[str, Any],
    bar: dict[str, Any],
) -> dict[str, Any]:
    if hasattr(config, "confirmation_metrics"):
        return config.confirmation_metrics(candidate, bar)
    result = dict(candidate)
    o = _safe_float(bar.get("open"), np.nan)
    h = _safe_float(bar.get("high"), np.nan)
    low = _safe_float(bar.get("low"), np.nan)
    close = _safe_float(bar.get("close"), np.nan)
    rng = h - low
    result.update(
        {
            "confirm_open": o,
            "confirm_high": h,
            "confirm_low": low,
            "confirm_close": close,
            "confirm_volume": _safe_float(bar.get("volume")),
            "confirmation_timestamp": str(bar.get("timestamp", "")),
        }
    )
    if not all(np.isfinite(value) for value in (o, h, low, close)):
        result.update(confirmed=False, confirmation_reason="invalid_ohlc")
        return result
    if rng <= 0:
        result.update(confirmed=False, confirmation_reason="zero_range")
        return result
    body = abs(close - o)
    upper_wick = h - max(o, close)
    lower_wick = min(o, close) - low
    long_side = str(candidate["side"]).upper() == "LONG"
    direction_ok = (
        close > o and close > float(candidate["signal_close"])
        if long_side
        else close < o and close < float(candidate["signal_close"])
    )
    result.update(
        {
            "body_ratio": body / rng,
            "wick_ratio": (upper_wick if long_side else lower_wick) / rng,
            "trigger": h if long_side else low,
            "confirmed": bool(direction_ok),
            "confirmation_reason": "ok" if direction_ok else "direction_rejected",
        }
    )
    return result


def select_entry_signals(
    confirmed: list[dict[str, Any]],
    session_date: date,
    signal_end: str,
    *,
    capital_rs: float = config.CAPITAL_PER_ENTRY_RS,
    leverage: float = config.LEVERAGE,
) -> list[dict[str, Any]]:
    selected_signals: list[dict[str, Any]] = []
    directional = [row for row in confirmed if bool(row.get("confirmed"))]
    for side in ("LONG", "SHORT"):
        setup = (config.setup_for(signal_end, side, session_date=session_date)
                 if is_g_config(config) else config.setup_for(signal_end, side))
        if setup is None:
            continue
        ranked = (config.rank_candidates(directional, setup, session_date=session_date)
                  if is_g_config(config) else config.rank_candidates(directional, setup))
        for rank, candidate in enumerate(ranked, start=1):
            trigger = config.round_to_tick(
                float(candidate["trigger"]), float(candidate.get("tick_size", 0.05))
            )
            stop_price, target_price = config.bracket_levels(
                trigger,
                side,
                setup.stop_pct,
                setup.target_pct,
                float(candidate.get("tick_size", 0.05)),
            )
            paper_size = config.size_position(
                trigger,
                _safe_int(candidate.get("lot_size"), 1),
                live=False,
                capital_rs=capital_rs,
                leverage=leverage,
            )
            live_size = config.size_position(
                trigger,
                _safe_int(candidate.get("lot_size"), 1),
                live=True,
                capital_rs=capital_rs,
                leverage=leverage,
            )
            signal_id = _signal_id(
                session_date, setup, str(candidate["tradingsymbol"])
            )
            selected_signals.append(
                {
                    **candidate,
                    "schema_version": f"{LIVE_SCHEMA_PREFIX}_equity_entry_signal_v2",
                    "run_id": RUN_ID,
                    "strategy_version": config.STRATEGY_VERSION,
                    "strategy_fingerprint": config.strategy_fingerprint(),
                    "selected_objective": config.SELECTED_OBJECTIVE,
                    "signal_id": signal_id,
                    "session_date": session_date.isoformat(),
                    "confirmation_end": setup.confirmation_end,
                    "entry_activation_deadline_ist": config.activation_deadline(
                        session_date, setup.confirmation_end
                    ).isoformat(timespec="seconds"),
                    "setup_id": setup.setup_id,
                    "setup_source": setup.source_version,
                    "setup_mode": setup.mode,
                    "picker": setup.picker,
                    "rank_within_scan": rank,
                    "max_entries": setup.max_entries,
                    "entry_order_type": "STOP_MARKET",
                    "trigger_price": trigger,
                    "stop_pct": setup.stop_pct,
                    **_g_stop_policy_metadata(session_date),
                    "target_pct": setup.target_pct,
                    "stop_price": stop_price,
                    "target_price": target_price,
                    "square_off": config.SQUARE_OFF,
                    "round_trip_cost_bps": config.ROUND_TRIP_COST_BPS,
                    "capital_rs": float(capital_rs),
                    "leverage": float(leverage),
                    "target_exposure_rs": float(capital_rs * leverage),
                    "paper_sizing": asdict(paper_size),
                    "live_sizing": asdict(live_size),
                    "published_at_ist": _iso_now(),
                }
            )
    return selected_signals


class BrokerMutationUncertain(RuntimeError):
    """A broker mutation may have reached Kite and must be reconciled by tag."""


def _is_explicit_broker_auth_error(exc: Exception) -> bool:
    """Return true only when Kite explicitly rejected the client credentials."""

    if type(exc).__name__ == "TokenException":
        return True
    message = str(exc).lower()
    return "incorrect `api_key` or `access_token`" in message


def _is_explicit_broker_rejection(exc: Exception) -> bool:
    """Return whether Kite definitively rejected a mutation request.

    These exception types are created from a structured broker error response,
    so the request outcome is known and failover/retry would only repeat the
    same rejected mutation.  Transport, malformed-response and server-side
    errors remain ambiguous and continue through tag reconciliation.
    """

    if type(exc).__name__ in {"InputException", "OrderException", "PermissionException"}:
        return True
    try:
        code = int(getattr(exc, "code", 0))
    except (TypeError, ValueError):
        return False
    return 400 <= code < 500


def _is_stop_trigger_relation_rejection(exc: Exception) -> bool:
    """Recognise Kite's definite rejection after a stop trigger was crossed."""

    message = str(exc).lower()
    return (
        _is_explicit_broker_rejection(exc)
        and "trigger price" in message
        and (
            ("stoploss" in message and "last traded price" in message)
            or ("crossed" in message and "ltp" in message)
        )
    )


def _broker_order_type_matches(
    expected: Any,
    observed: Any,
    *,
    protected: bool = False,
) -> bool:
    """Match Kite order types, including protected-market normalization."""

    expected_type = str(expected or "").strip().upper()
    observed_type = str(observed or "").strip().upper()
    if expected_type == observed_type:
        return True
    return protected and expected_type in {"MARKET", "SL-M"} and observed_type == "LIMIT"


def _kite_place_order_compat(client: Any, payload: dict[str, Any]) -> Any:
    """Submit market protection through old Kite SDKs that lack the argument."""

    if payload.get("market_protection") is None:
        return client.place_order(**payload)
    post = getattr(client, "_post", None)
    if not callable(post):
        return client.place_order(**payload)
    response = post(
        "order.place",
        url_args={"variety": payload["variety"]},
        params=dict(payload),
    )
    if isinstance(response, dict) and response.get("order_id"):
        return str(response["order_id"])
    return response


def _kite_modify_order_compat(client: Any, payload: dict[str, Any]) -> Any:
    """Preserve market protection on SDKs predating that modify parameter."""
    put = getattr(client, "_put", None)
    if payload.get("market_protection") is None or not callable(put):
        return client.modify_order(**payload)
    response = put("order.modify", url_args={"variety": payload["variety"],
                   "order_id": payload["order_id"]}, params=dict(payload))
    return str(response["order_id"]) if isinstance(response, dict) else response


class KitePool:
    """Ordered Kite clients with hot credential reload and safe failover."""

    def __init__(
        self,
        max_apps: int,
        timeout_sec: float,
        *,
        credential_loader: Any = None,
        client_factory: Any = None,
    ) -> None:
        self.max_apps = int(max_apps)
        self.timeout_sec = float(timeout_sec)
        self._credential_loader = (
            credential_loader or common.discover_kite_credentials
        )
        self._client_factory = client_factory or common.make_kite_client
        self.app_names: list[str] = []
        self.clients: list[Any] = []
        self._credential_fingerprint = ""
        self._auth_failed_apps: set[str] = set()
        self.credential_reload_count = 0
        self.last_operation = ""
        self.last_operation_app = ""
        self.last_operation_failures: list[dict[str, str]] = []
        self._install_credentials(self._load_credentials(), initial=True)

    def _load_credentials(self) -> list[Any]:
        return list(self._credential_loader(max_apps=self.max_apps))

    @staticmethod
    def _fingerprint(credentials: list[Any]) -> str:
        digest = hashlib.sha256()
        for credential in credentials:
            for value in (
                credential.app_name,
                credential.api_key,
                credential.access_token,
            ):
                digest.update(str(value).encode("utf-8"))
                digest.update(b"\0")
        return digest.hexdigest()

    def _install_credentials(
        self, credentials: list[Any], *, initial: bool = False
    ) -> None:
        if not credentials:
            raise RuntimeError("No authenticated Kite client is available.")
        clients = [
            self._client_factory(credential, timeout_sec=self.timeout_sec)
            for credential in credentials
        ]
        self.app_names = [str(credential.app_name) for credential in credentials]
        self.clients = clients
        self._credential_fingerprint = self._fingerprint(credentials)
        self._auth_failed_apps.clear()
        if not initial:
            self.credential_reload_count += 1

    def refresh_if_changed(self) -> bool:
        """Atomically rebuild clients when any configured credential changes."""

        credentials = self._load_credentials()
        if self._fingerprint(credentials) == self._credential_fingerprint:
            return False
        self._install_credentials(credentials)
        return True

    def _lanes(self) -> list[tuple[str, Any]]:
        self.refresh_if_changed()
        return [
            (app_name, client)
            for app_name, client in zip(self.app_names, self.clients)
            if app_name not in self._auth_failed_apps
        ]

    @staticmethod
    def _failure(app_name: str, exc: Exception) -> dict[str, str]:
        return {
            "app": app_name,
            "error_type": type(exc).__name__,
            "message": str(exc),
        }

    @staticmethod
    def _failure_summary(failures: list[dict[str, str]]) -> str:
        return "; ".join(
            f"{item['app']}={item['error_type']}: {item['message']}"
            for item in failures
        )

    def _record_operation(
        self,
        operation: str,
        app_name: str,
        failures: list[dict[str, str]],
    ) -> None:
        self.last_operation = operation
        self.last_operation_app = app_name
        self.last_operation_failures = list(failures)

    def _call_read(self, operation: str, callback: Any) -> Any:
        failures: list[dict[str, str]] = []
        last_error: Exception | None = None
        lanes = self._lanes()
        for app_name, client in lanes:
            try:
                result = callback(client)
                self._record_operation(operation, app_name, failures)
                return result
            except Exception as exc:
                last_error = exc
                failures.append(self._failure(app_name, exc))
                if _is_explicit_broker_auth_error(exc):
                    self._auth_failed_apps.add(app_name)
        self._record_operation(operation, "", failures)
        unavailable = sorted(self._auth_failed_apps)
        raise RuntimeError(
            f"All configured Kite apps failed {operation}; "
            f"auth_unavailable={unavailable}; failures={self._failure_summary(failures)}"
        ) from last_error

    @staticmethod
    def _order_matches_submission(row: dict[str, Any], payload: dict[str, Any]) -> bool:
        identity_matches = all(
            (
                str(row.get(field, "")).upper()
                if field == "transaction_type"
                else str(row.get(field, ""))
            )
            == (
                str(payload.get(field, "")).upper()
                if field == "transaction_type"
                else str(payload.get(field, ""))
            )
            for field in ("tag", "tradingsymbol", "transaction_type")
        )
        protected = _safe_int(payload.get("market_protection"), 0) != 0
        return (
            identity_matches
            and _broker_order_type_matches(
                payload.get("order_type"),
                row.get("order_type"),
                protected=protected,
            )
            and _safe_int(row.get("quantity"), -1)
            == _safe_int(payload.get("quantity"), -2)
        )

    def _recover_unknown_submission(self, payload: dict[str, Any]) -> str:
        if not str(payload.get("tag", "")):
            return ""
        rows = self._call_read(
            "reconcile_unknown_submission", lambda client: client.orders()
        )
        matches = [
            dict(row)
            for row in rows
            if self._order_matches_submission(dict(row), payload)
        ]
        if len(matches) > 1:
            order_ids = sorted(str(row.get("order_id", "")) for row in matches)
            raise RuntimeError(
                "Multiple broker orders match an uncertain submission: "
                f"{order_ids}"
            )
        return str(matches[0].get("order_id", "")) if matches else ""

    def _call_mutation(self, operation: str, callback: Any, payload: dict[str, Any]) -> Any:
        failures: list[dict[str, str]] = []
        last_error: Exception | None = None
        lanes = self._lanes()
        for app_name, client in lanes:
            try:
                result = callback(client)
                self._record_operation(operation, app_name, failures)
                return result
            except Exception as exc:
                last_error = exc
                failures.append(self._failure(app_name, exc))
                if _is_explicit_broker_auth_error(exc):
                    self._auth_failed_apps.add(app_name)
                    continue
                if _is_explicit_broker_rejection(exc):
                    # Kite returned a structured rejection.  No order was
                    # accepted, so neither app failover nor uncertain-outcome
                    # reconciliation is appropriate.
                    self._record_operation(operation, app_name, failures)
                    raise
                if operation == "place_order":
                    try:
                        recovered_order_id = self._recover_unknown_submission(payload)
                    except Exception as reconcile_exc:
                        failures.append(self._failure(app_name, reconcile_exc))
                        self._record_operation(operation, app_name, failures)
                        raise BrokerMutationUncertain(
                            f"{operation} outcome is unknown on {app_name}; "
                            "tag reconciliation also failed; "
                            f"cause={type(exc).__name__}: {exc}; "
                            "reconciliation_cause="
                            f"{type(reconcile_exc).__name__}: {reconcile_exc}"
                        ) from exc
                    if recovered_order_id:
                        self._record_operation(
                            "place_order_reconciled", app_name, failures
                        )
                        return recovered_order_id
                self._record_operation(operation, app_name, failures)
                raise BrokerMutationUncertain(
                    f"{operation} outcome is unknown on {app_name}; mutation was "
                    "not retried and must be reconciled by deterministic order tag; "
                    f"cause={type(exc).__name__}: {exc}"
                ) from exc
        self._record_operation(operation, "", failures)
        raise RuntimeError(
            f"All configured Kite apps explicitly rejected {operation}; "
            f"failures={self._failure_summary(failures)}"
        ) from last_error

    @property
    def primary(self) -> Any:
        self.refresh_if_changed()
        return self.clients[0]

    def quote_prices(
        self, symbols: list[str]
    ) -> tuple[dict[str, float], str, list[dict[str, str]]]:
        """Fetch paper quotes with one bounded attempt per configured app."""

        if not symbols:
            return {}, "", []
        prices = self._call_read(
            "ltp", lambda client: _quote_prices(client, symbols)
        )
        return prices, self.last_operation_app, list(self.last_operation_failures)

    def orders(self) -> Any:
        return self._call_read("orders", lambda client: client.orders())

    def order_history(self, order_id: str) -> Any:
        return self._call_read(
            "order_history", lambda client: client.order_history(order_id)
        )

    def positions(self) -> Any:
        return self._call_read("positions", lambda client: client.positions())

    def place_order(self, **kwargs: Any) -> Any:
        payload = dict(kwargs)
        return self._call_mutation(
            "place_order",
            lambda client: _kite_place_order_compat(client, payload),
            payload,
        )

    def cancel_order(self, **kwargs: Any) -> Any:
        return self._call_mutation(
            "cancel_order", lambda client: client.cancel_order(**kwargs), dict(kwargs)
        )

    def modify_order(self, **kwargs: Any) -> Any:
        payload = dict(kwargs)
        return self._call_mutation(
            "modify_order", lambda client: _kite_modify_order_compat(client, payload), payload
        )


def _marker_matches_slot(marker: dict[str, Any], expected_slot: datetime) -> bool:
    try:
        observed = pd.Timestamp(marker["slot_ist"])
        if observed.tzinfo is None:
            observed = observed.tz_localize(common.IST)
        else:
            observed = observed.tz_convert(common.IST)
    except Exception:
        return False
    return observed.to_pydatetime() == expected_slot


def _equity_universe_sha256(universe: pd.DataFrame | None) -> str:
    if universe is None or universe.empty or "equity_symbol" not in universe.columns:
        return ""
    symbols = sorted(
        {
            str(value).strip().upper()
            for value in universe["equity_symbol"].dropna()
            if str(value).strip()
        }
    )
    return hashlib.sha256("\n".join(symbols).encode("utf-8")).hexdigest()


def _marker_symbol_set(marker: dict[str, Any], key: str) -> set[str] | None:
    values = marker.get(key)
    if not isinstance(values, list):
        return None
    normalized = [str(value).strip().upper() for value in values]
    if any(not value for value in normalized) or len(set(normalized)) != len(normalized):
        return None
    return set(normalized)


def _futures_universe_symbols(universe: pd.DataFrame | None) -> set[str]:
    if (
        universe is None
        or universe.empty
        or "futures_tradingsymbol" not in universe.columns
    ):
        return set()
    return {
        str(value).strip().upper()
        for value in universe["futures_tradingsymbol"].dropna()
        if str(value).strip()
    }


def _futures_universe_sha256(universe: pd.DataFrame | None) -> str:
    if universe is None or universe.empty:
        return ""
    try:
        return common.universe_sha256(universe)
    except (AttributeError, KeyError, TypeError, ValueError):
        return ""


def _validate_v2_fno_marker(
    marker: dict[str, Any],
    universe: pd.DataFrame | None,
) -> str:
    if universe is None or universe.empty:
        return "fno_fetch_marker_stock_universe_missing"
    if str(marker.get("readiness_policy", "")) != config.FNO_READINESS_POLICY:
        return "fno_fetch_marker_readiness_policy_mismatch"
    declared_minimum_coverage = _safe_float(
        marker.get("minimum_stock_coverage"), -1.0
    )
    if not float(config.MIN_STOCK_FUTURES_COVERAGE) <= declared_minimum_coverage <= 1.0:
        return "fno_fetch_marker_minimum_coverage_mismatch"
    if abs(
        _safe_float(marker.get("minimum_coverage"), -1.0)
        - declared_minimum_coverage
    ) > 1e-12:
        return "fno_fetch_marker_minimum_coverage_alias_mismatch"
    if _safe_int(marker.get("maximum_verified_no_candle_stocks"), -1) != int(
        config.MAX_VERIFIED_NO_CANDLE_STOCKS
    ):
        return "fno_fetch_marker_no_candle_cap_mismatch"
    if _safe_int(marker.get("minimum_no_candle_fetch_attempts"), -1) != int(
        config.MIN_NO_CANDLE_FETCH_ATTEMPTS
    ):
        return "fno_fetch_marker_no_candle_attempt_policy_mismatch"
    if not bool(marker.get("complete")) or not bool(marker.get("stock_complete")):
        return f"fno_fetch_marker_{marker.get('state', 'partial')}"
    if str(marker.get("state", "")).upper() != "SUCCESS":
        return "fno_fetch_marker_state_mismatch"
    if not bool(marker.get("outcome_symbol_set_complete")):
        return "fno_fetch_marker_outcome_symbol_set_incomplete"
    if not bool(marker.get("stock_outcome_symbol_set_complete")):
        return "fno_fetch_marker_stock_symbol_set_incomplete"
    if _safe_int(marker.get("failed_count")) != 0:
        return "fno_fetch_marker_api_failure"
    if _safe_int(marker.get("invalid_data_count")) != 0:
        return "fno_fetch_marker_invalid_data"
    if _safe_int(marker.get("stock_failed_count")) != 0:
        return "fno_fetch_marker_stock_api_failure"
    if _safe_int(marker.get("stock_invalid_data_count")) != 0:
        return "fno_fetch_marker_stock_invalid_data"

    no_candle_symbols = _marker_symbol_set(marker, "no_candle_symbols")
    stock_no_candle_symbols = _marker_symbol_set(
        marker, "stock_no_candle_symbols"
    )
    stock_verified_symbols = _marker_symbol_set(
        marker, "stock_verified_no_candle_symbols"
    )
    stock_unverified_symbols = _marker_symbol_set(
        marker, "stock_unverified_no_candle_symbols"
    )
    stock_written_symbols = _marker_symbol_set(marker, "stock_written_symbols")
    if any(
        values is None
        for values in (
            no_candle_symbols,
            stock_no_candle_symbols,
            stock_verified_symbols,
            stock_unverified_symbols,
            stock_written_symbols,
        )
    ):
        return "fno_fetch_marker_symbol_list_invalid"
    assert no_candle_symbols is not None
    assert stock_no_candle_symbols is not None
    assert stock_verified_symbols is not None
    assert stock_unverified_symbols is not None
    assert stock_written_symbols is not None

    expected_symbols = _futures_universe_symbols(universe)
    if not expected_symbols or len(expected_symbols) != len(universe):
        return "fno_fetch_marker_stock_universe_invalid"
    if _safe_int(marker.get("stock_contracts_expected")) != len(expected_symbols):
        return "fno_fetch_marker_stock_count_mismatch"
    if str(marker.get("stock_symbol_set_sha256", "")) != common.symbol_set_sha256(
        expected_symbols
    ):
        return "fno_fetch_marker_stock_symbol_set_mismatch"
    expected_full_hash = _futures_universe_sha256(universe)
    if not expected_full_hash:
        return "fno_fetch_marker_stock_universe_unattestable"
    if str(marker.get("stock_universe_sha256", "")) != expected_full_hash:
        return "fno_fetch_marker_stock_universe_mismatch"
    if stock_no_candle_symbols - expected_symbols:
        return "fno_fetch_marker_foreign_no_candle_symbol"
    if stock_no_candle_symbols - no_candle_symbols:
        return "fno_fetch_marker_no_candle_list_mismatch"
    if stock_verified_symbols != stock_no_candle_symbols:
        return "fno_fetch_marker_unverified_stock_no_candle"
    if stock_unverified_symbols:
        return "fno_fetch_marker_unverified_stock_no_candle"
    if len(stock_verified_symbols) > int(config.MAX_VERIFIED_NO_CANDLE_STOCKS):
        return "fno_fetch_marker_no_candle_cap_exceeded"
    if stock_written_symbols != expected_symbols - stock_verified_symbols:
        return "fno_fetch_marker_stock_partition_mismatch"

    stock_expected = len(expected_symbols)
    stock_written = len(stock_written_symbols)
    if _safe_int(marker.get("stock_contracts_written")) != stock_written:
        return "fno_fetch_marker_stock_written_count_mismatch"
    if _safe_int(marker.get("stock_no_candle_count")) != len(
        stock_no_candle_symbols
    ):
        return "fno_fetch_marker_stock_no_candle_count_mismatch"
    if _safe_int(marker.get("stock_verified_no_candle_count")) != len(
        stock_verified_symbols
    ):
        return "fno_fetch_marker_verified_no_candle_count_mismatch"
    if stock_written + len(stock_verified_symbols) != stock_expected:
        return "fno_fetch_marker_stock_incomplete_coverage"
    stock_coverage = stock_written / stock_expected
    if stock_coverage < declared_minimum_coverage:
        return "fno_fetch_marker_stock_incomplete_coverage"
    if abs(_safe_float(marker.get("stock_coverage_ratio"), -1.0) - stock_coverage) > 1e-12:
        return "fno_fetch_marker_stock_coverage_ratio_mismatch"

    total_expected = _safe_int(marker.get("contracts_expected"))
    total_written = _safe_int(marker.get("contracts_written"))
    total_no_candle = _safe_int(marker.get("no_candle_count"))
    if total_expected <= 0 or total_no_candle != len(no_candle_symbols):
        return "fno_fetch_marker_total_count_mismatch"
    if total_written + total_no_candle != total_expected:
        return "fno_fetch_marker_incomplete_coverage"

    observations = marker.get("no_candle_observations")
    attempts = marker.get("no_candle_fetch_attempts")
    if not isinstance(observations, dict) or not isinstance(attempts, dict):
        return "fno_fetch_marker_no_candle_evidence_missing"
    normalized_observations = {
        str(symbol).strip().upper(): _safe_int(count, -1)
        for symbol, count in observations.items()
        if str(symbol).strip()
    }
    normalized_attempts = {
        str(symbol).strip().upper(): _safe_int(count, -1)
        for symbol, count in attempts.items()
        if str(symbol).strip()
    }
    required_observations = int(config.MIN_NO_CANDLE_FETCH_ATTEMPTS)
    for symbol in stock_verified_symbols:
        if normalized_observations.get(symbol, -1) < required_observations:
            return "fno_fetch_marker_no_candle_not_repeatedly_verified"
        if normalized_attempts.get(symbol, -1) < required_observations:
            return "fno_fetch_marker_no_candle_attempts_insufficient"
    return ""


def _verified_no_candle_symbols_for_slot(
    session_date: date,
    signal_end: str,
    universe: pd.DataFrame,
) -> set[str]:
    expected_slot = config.slot_datetime(session_date, signal_end)
    marker = _read_json(common.fetch_slot_path(expected_slot))
    if str(marker.get("schema_version", "")) != config.FNO_FETCH_SLOT_SCHEMA_VERSION:
        return set()
    if str(marker.get("source", "")).lower() != "final":
        return set()
    if not _marker_matches_slot(marker, expected_slot):
        return set()
    if _validate_v2_fno_marker(marker, universe):
        return set()
    return _marker_symbol_set(marker, "stock_verified_no_candle_symbols") or set()


def _slot_marker_ready(
    session_date: date,
    signal_end: str,
    universe: pd.DataFrame | None = None,
) -> tuple[bool, str]:
    expected_slot = config.slot_datetime(session_date, signal_end)
    _archive_mapped_universe_evidence(session_date, signal_end, universe)
    fno_marker = _read_json(common.fetch_slot_path(expected_slot))
    if not fno_marker:
        return False, "fno_fetch_marker_missing"
    _archive_json_evidence("fno_fetch_marker", session_date, signal_end, fno_marker)
    if str(fno_marker.get("source", "")).lower() != "final":
        return False, "fno_fetch_marker_not_final"
    if not _marker_matches_slot(fno_marker, expected_slot):
        return False, "fno_fetch_marker_wrong_slot"
    schema_version = str(fno_marker.get("schema_version", ""))
    fno_no_candle = _safe_int(fno_marker.get("no_candle_count"))
    if schema_version == config.FNO_FETCH_SLOT_SCHEMA_VERSION:
        v2_error = _validate_v2_fno_marker(fno_marker, universe)
        if v2_error:
            return False, v2_error
    elif fno_no_candle != 0:
        return False, "fno_fetch_marker_legacy_no_candle_unverifiable"
    elif schema_version not in {"", "fno_oi_fetch_slot_v1"}:
        return False, "fno_fetch_marker_schema_unsupported"
    elif not bool(fno_marker.get("complete")):
        return False, f"fno_fetch_marker_{fno_marker.get('state', 'partial')}"
    if schema_version != config.FNO_FETCH_SLOT_SCHEMA_VERSION:
        fno_expected = _safe_int(fno_marker.get("contracts_expected"))
        fno_written = _safe_int(fno_marker.get("contracts_written"))
        fno_invalid = _safe_int(fno_marker.get("invalid_data_count"))
        fno_failed = _safe_int(fno_marker.get("failed_count"))
        fno_coverage = float(fno_written / fno_expected) if fno_expected else 0.0
        minimum_coverage = max(
            float(config.MIN_STOCK_FUTURES_COVERAGE),
            _safe_float(
                fno_marker.get(
                    "minimum_stock_coverage", fno_marker.get("minimum_coverage")
                ),
                float(config.MIN_STOCK_FUTURES_COVERAGE),
            ),
        )
        if (
            fno_expected <= 0
            or fno_written <= 0
            or fno_written + fno_no_candle != fno_expected
            or fno_invalid != 0
            or fno_failed != 0
            or fno_coverage < minimum_coverage
        ):
            return False, "fno_fetch_marker_incomplete_coverage"

    cash_marker = _read_json(common.cash_slot_path(expected_slot))
    if not cash_marker:
        return False, "cash_5m_marker_missing"
    _archive_json_evidence("cash_5m_marker", session_date, signal_end, cash_marker)
    if str(cash_marker.get("source", "")).lower() != "final":
        return False, "cash_5m_marker_not_final"
    if not _marker_matches_slot(cash_marker, expected_slot):
        return False, "cash_5m_marker_wrong_slot"
    if not bool(cash_marker.get("complete")):
        return False, "cash_5m_marker_incomplete"
    cash_expected = _safe_int(cash_marker.get("tickers_expected"))
    if (
        cash_expected <= 0
        or _safe_int(cash_marker.get("tickers_written")) != cash_expected
        or _safe_int(cash_marker.get("tickers_complete")) != cash_expected
        or _safe_int(cash_marker.get("tickers_failed")) != 0
    ):
        return False, "cash_5m_marker_incomplete_coverage"
    if not bool(cash_marker.get("fno_equity_quality_complete")):
        return False, "cash_5m_marker_fno_equity_quality_incomplete"
    fno_equity_expected = _safe_int(cash_marker.get("fno_equity_expected"))
    if (
        fno_equity_expected <= 0
        or _safe_int(cash_marker.get("fno_equity_ready")) != fno_equity_expected
        or _safe_int(cash_marker.get("fno_equity_failed")) != 0
    ):
        return False, "cash_5m_marker_fno_equity_incomplete_coverage"
    expected_hash = _equity_universe_sha256(universe)
    if expected_hash:
        if fno_equity_expected != len(set(universe["equity_symbol"].astype(str).str.upper())):
            return False, "cash_5m_marker_fno_equity_count_mismatch"
        if str(cash_marker.get("fno_equity_universe_sha256", "")) != expected_hash:
            return False, "cash_5m_marker_fno_equity_universe_mismatch"
    return True, "ready"


def _render_scanner_report(session_date: date) -> str:
    notice, _ = _pipeline_notice_lines(session_date, ("scanner-5m",))
    rows = []
    for signal_end in config.SIGNAL_TO_CONFIRMATION:
        snapshot = _read_json(scanner_slot_path(session_date, signal_end))
        rows.append(
            {
                "signal": signal_end,
                "confirmation": config.SIGNAL_TO_CONFIRMATION[signal_end],
                "state": snapshot.get("state", "WAITING"),
                "evaluated": snapshot.get("contracts_evaluated", 0),
                "skipped": snapshot.get("contracts_skipped_no_candle", 0),
                "missing": snapshot.get("contracts_unexpected_missing", 0),
                "long": snapshot.get("long_candidates", 0),
                "short": snapshot.get("short_candidates", 0),
                "published": snapshot.get("published_at_ist", ""),
            }
        )
    lines = [
        f"# FnO {DISPLAY_LABEL} 5-Minute Scanner",
        "",
        f"Session: {session_date.isoformat()}",
        f"Strategy: {config.STRATEGY_VERSION}",
        f"Fingerprint: `{config.strategy_fingerprint()}`",
        "Readiness gate: exact-slot cash quality plus an attested stock-futures fetch marker.",
        "Repeatedly verified absent futures candles are skipped, never synthesized or forward-filled.",
        "Scanning uses NSE-equity OHLCV/indicators and only OI fields from the mapped future.",
        "Bars are labelled by candle end time. No future candle participates.",
        "",
        *notice,
        "Signal | Confirmation | State | Evaluated | Verified skips | Unexpected missing | LONG base | SHORT base | Published",
        "--- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---",
    ]
    for row in rows:
        lines.append(
            f"{row['signal']} | {row['confirmation']} | {row['state']} | "
            f"{row['evaluated']} | {row['skipped']} | {row['missing']} | "
            f"{row['long']} | {row['short']} | {_md(row['published'])}"
        )
    return "\n".join(lines) + "\n"


def _write_entry_signal(signal: dict[str, Any]) -> None:
    path = signal_day_dir(date.fromisoformat(str(signal["session_date"]))) / (
        f"{signal['signal_id']}.json"
    )
    common.atomic_write_json(path, signal)


def _authoritative_signal_ids(session_date: date) -> set[str]:
    signal_ids: set[str] = set()
    for signal_end in config.SIGNAL_TO_CONFIRMATION:
        snapshot = _read_json(confirmation_slot_path(session_date, signal_end))
        if not snapshot:
            continue
        if snapshot.get("strategy_version") != config.STRATEGY_VERSION:
            continue
        if snapshot.get("strategy_fingerprint") != config.strategy_fingerprint():
            continue
        if snapshot.get("session_date") != session_date.isoformat():
            continue
        if snapshot.get("state") != "SUCCESS":
            continue
        signal_ids.update(str(value) for value in snapshot.get("selected_signal_ids", []))
    return signal_ids


def _validate_signal(signal: dict[str, Any], session_date: date) -> None:
    side = str(signal.get("side", "")).upper()
    signal_end = str(signal.get("signal_end", ""))
    setup = (config.setup_for(signal_end, side, session_date=session_date)
             if is_g_config(config) else config.setup_for(signal_end, side))
    if signal.get("strategy_version") != config.STRATEGY_VERSION:
        raise RuntimeError(f"Signal {signal.get('signal_id')} has a stale strategy version.")
    if signal.get("strategy_fingerprint") != config.strategy_fingerprint():
        raise RuntimeError(f"Signal {signal.get('signal_id')} failed the strategy fingerprint.")
    if signal.get("session_date") != session_date.isoformat():
        raise RuntimeError(f"Signal {signal.get('signal_id')} has the wrong session date.")
    if setup is None:
        raise RuntimeError(
            f"Signal {signal.get('signal_id')} is not an active {LIVE_LABEL} setup."
        )
    expected_id = _signal_id(session_date, setup, str(signal.get("tradingsymbol", "")))
    if signal.get("signal_id") != expected_id:
        raise RuntimeError(f"Signal {signal.get('signal_id')} failed its deterministic ID check.")
    expected_fields = {
        "confirmation_end": setup.confirmation_end,
        "entry_activation_deadline_ist": config.activation_deadline(
            session_date, setup.confirmation_end
        ).isoformat(timespec="seconds"),
        "setup_id": setup.setup_id,
        "setup_source": setup.source_version,
        "setup_mode": setup.mode,
        "picker": setup.picker,
        "max_entries": setup.max_entries,
        "stop_pct": setup.stop_pct,
        **_g_stop_policy_metadata(session_date),
        "target_pct": setup.target_pct,
        "capital_rs": config.CAPITAL_PER_ENTRY_RS,
        "leverage": config.LEVERAGE,
        "target_exposure_rs": config.TARGET_EXPOSURE_RS,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "exchange": "NSE",
    }
    for field, expected in expected_fields.items():
        observed = signal.get(field)
        if isinstance(expected, float):
            matches = abs(_safe_float(observed, np.nan) - expected) <= 1e-9
        else:
            matches = observed == expected
        if not matches:
            raise RuntimeError(
                f"Signal {signal.get('signal_id')} has invalid {field}: "
                f"expected {expected}, observed {observed}."
            )
    rank = _safe_int(signal.get("rank_within_scan"))
    if rank < 1 or rank > setup.max_entries:
        raise RuntimeError(f"Signal {signal.get('signal_id')} has invalid rank {rank}.")
    trigger = _safe_float(signal.get("trigger_price"))
    if trigger <= 0:
        raise RuntimeError(f"Signal {signal.get('signal_id')} has an invalid trigger.")
    if _safe_int(signal.get("instrument_token")) <= 0:
        raise RuntimeError(f"Signal {signal.get('signal_id')} has no equity token.")
    if _safe_int(signal.get("futures_instrument_token")) <= 0:
        raise RuntimeError(
            f"Signal {signal.get('signal_id')} has no futures OI provenance token."
        )
    lot_size = max(1, _safe_int(signal.get("lot_size"), 1))
    expected_paper = asdict(config.size_position(trigger, lot_size, live=False))
    expected_live = asdict(config.size_position(trigger, lot_size, live=True))
    if signal.get("paper_sizing") != expected_paper:
        raise RuntimeError(f"Signal {signal.get('signal_id')} has invalid PAPER sizing.")
    if signal.get("live_sizing") != expected_live:
        raise RuntimeError(f"Signal {signal.get('signal_id')} has invalid LIVE sizing.")


def _render_confirmation_report(session_date: date) -> str:
    notice, _ = _pipeline_notice_lines(session_date)
    snapshots = [
        _read_json(confirmation_slot_path(session_date, signal_end))
        for signal_end in config.SIGNAL_TO_CONFIRMATION
    ]
    signals = load_signals(session_date)
    lines = [
        f"# FnO {DISPLAY_LABEL} 1-Minute Confirmation and Entry Scanner",
        "",
        f"Session: {session_date.isoformat()}",
        f"Selected objective: {config.SELECTED_OBJECTIVE}",
        "Entry is a stop order at the confirmation candle extreme and may activate only afterward.",
        ("Confirmation publication deadline: 90 seconds; pending G entry trigger expires after 10 minutes."
         if is_g_config(config) else
         f"A first-time entry must be armed within {config.ENTRY_ACTIVATION_GRACE_SEC}s of confirmation; stale starts are blocked."),
        f"Only active {DISPLAY_LABEL} setup legs can publish entry signals.",
        "",
        *notice,
        "Signal | Confirm | State | Directional | Ineligible no-candle | Selected L/S | Errors",
        "--- | --- | --- | ---: | ---: | ---: | ---:",
    ]
    for signal_end, snapshot in zip(config.SIGNAL_TO_CONFIRMATION, snapshots):
        lines.append(
            f"{signal_end} | {config.SIGNAL_TO_CONFIRMATION[signal_end]} | "
            f"{snapshot.get('state', 'WAITING')} | {snapshot.get('directional_confirmed', 0)} | "
            f"{snapshot.get('ineligible_no_candle_count', 0)} | "
            f"{snapshot.get('selected_long', 0)}/{snapshot.get('selected_short', 0)} | "
            f"{snapshot.get('error_count', 0)}"
        )
    lines.extend(
        [
            "",
            "Signal ID | Entry | Side | Symbol | Rank/Cap | Trigger | Stop | Target | Paper qty | Live qty/state",
            "--- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---",
        ]
    )
    for signal in signals:
        lines.append(
            f"{signal['signal_id']} | {signal['confirmation_end']} | {signal['side']} | "
            f"{signal['tradingsymbol']} | {signal['rank_within_scan']}/{signal['max_entries']} | "
            f"{float(signal['trigger_price']):.2f} | {float(signal['stop_price']):.2f} | "
            f"{float(signal['target_price']):.2f} | "
            f"{signal['paper_sizing']['quantity']} | {signal['live_sizing']['quantity']}/"
            f"{signal['live_sizing']['state']}"
        )
    if not signals:
        lines.append("(no selected entries yet) | | | | | | | | |")
    return "\n".join(lines) + "\n"


def _load_completed_confirmation_feed(
    snapshot: dict[str, Any],
    session_date: date,
    signal_end: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, Any]]:
    candidates = list(snapshot.get("candidates") or [])
    expected_symbols = {
        str(candidate.get("tradingsymbol", "")).strip().upper()
        for candidate in candidates
    }
    scanner_sha256 = equity_feed.scanner_snapshot_sha256(snapshot)
    confirmation_hhmm = config.SIGNAL_TO_CONFIRMATION[signal_end]
    confirmation_end = config.slot_datetime(session_date, confirmation_hhmm)
    marker_path = common.equity_1m_slot_path(
        confirmation_end,
        generation=LIVE_GENERATION,
        scanner_sha256=scanner_sha256,
    )
    marker = _read_json(marker_path)
    if not marker:
        return {}, {"_feed": "durable_confirmation_marker_missing"}, {}
    _archive_json_evidence(
        "confirmation_feed_marker", session_date, signal_end, marker
    )
    expected_fields = {
        "schema_version": config.CONFIRMATION_FEED_SCHEMA_VERSION,
        "feed_policy": config.CONFIRMATION_FEED_POLICY,
        "source": "final",
        "state": "SUCCESS",
        "complete": True,
        "within_deadline": True,
        "generation": LIVE_GENERATION,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": confirmation_hhmm,
        "slot_ist": confirmation_end.isoformat(),
        "scanner_snapshot_sha256": scanner_sha256,
        "candidate_contract_sha256": equity_feed.candidate_contract_sha256(snapshot),
        "candidate_symbol_set_sha256": common.symbol_set_sha256(expected_symbols),
        "candidate_resolution_policy": equity_feed.NO_CANDLE_RESOLUTION_POLICY,
        "minimum_no_candle_observations": config.CONFIRMATION_NO_CANDLE_OBSERVATIONS,
        "minimum_no_candle_verification_age_sec": config.CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
        "minimum_no_candle_observation_spacing_sec": config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
        "verified_no_candle_cap": None,
        "written_bar_minimum_ratio": None,
    }
    for field, expected in expected_fields.items():
        if field not in marker or marker.get(field) != expected:
            return {}, {"_feed": f"durable_confirmation_marker_{field}_mismatch"}, marker
    if common.canonical_json_sha256(marker.get("scanner_snapshot")) != scanner_sha256:
        return {}, {"_feed": "durable_confirmation_marker_scanner_snapshot_tampered"}, marker
    try:
        published_at = _to_ist_datetime(marker["published_at_ist"])
        marker_deadline = _to_ist_datetime(marker["deadline_ist"])
        no_candle_verification_time = _to_ist_datetime(
            marker["minimum_no_candle_verification_ist"]
        )
    except (KeyError, TypeError, ValueError):
        return {}, {"_feed": "durable_confirmation_marker_time_invalid"}, marker
    expected_deadline = confirmation_end + timedelta(
        seconds=config.ENTRY_ACTIVATION_GRACE_SEC
    )
    expected_no_candle_verification_time = confirmation_end + timedelta(
        seconds=config.CONFIRMATION_NO_CANDLE_MIN_AGE_SEC
    )
    if (
        marker_deadline != expected_deadline
        or no_candle_verification_time != expected_no_candle_verification_time
        or published_at < confirmation_end
        or published_at > expected_deadline
    ):
        return {}, {"_feed": "durable_confirmation_marker_late"}, marker
    list_fields = (
        "candidate_symbols",
        "written_symbols",
        "no_candle_symbols",
        "verified_no_candle_symbols",
        "unverified_no_candle_symbols",
        "resolved_symbols",
        "invalid_symbols",
        "api_failed_symbols",
        "unexpected_missing_symbols",
    )
    if any(not isinstance(marker.get(field), list) for field in list_fields):
        return {}, {"_feed": "durable_confirmation_marker_symbols_invalid"}, marker

    def normalized_symbol_list(field: str) -> set[str] | None:
        values = marker[field]
        normalized = [str(symbol).strip().upper() for symbol in values]
        if any(not symbol for symbol in normalized) or len(set(normalized)) != len(normalized):
            return None
        return set(normalized)

    normalized = {field: normalized_symbol_list(field) for field in list_fields}
    if any(value is None for value in normalized.values()):
        return {}, {"_feed": "durable_confirmation_marker_symbols_invalid"}, marker
    normalized_candidates = normalized["candidate_symbols"] or set()
    normalized_written = normalized["written_symbols"] or set()
    no_candle_symbols = normalized["no_candle_symbols"] or set()
    verified_no_candle = normalized["verified_no_candle_symbols"] or set()
    unverified_no_candle = normalized["unverified_no_candle_symbols"] or set()
    resolved_symbols = normalized["resolved_symbols"] or set()
    if (
        normalized_candidates != expected_symbols
        or normalized_written & verified_no_candle
        or normalized_written | verified_no_candle != expected_symbols
        or no_candle_symbols != verified_no_candle
        or unverified_no_candle
        or resolved_symbols != expected_symbols
        or _safe_int(marker.get("candidate_count"), -1) != len(expected_symbols)
        or _safe_int(marker.get("written_count"), -1) != len(normalized_written)
        or _safe_int(marker.get("verified_no_candle_count"), -1)
        != len(verified_no_candle)
        or _safe_int(marker.get("resolved_count"), -1) != len(expected_symbols)
    ):
        return {}, {"_feed": "durable_confirmation_marker_coverage_mismatch"}, marker
    if any(
        marker.get(field)
        for field in (
            "unverified_no_candle_symbols",
            "invalid_symbols",
            "api_failed_symbols",
            "unexpected_missing_symbols",
            "errors",
        )
    ):
        return {}, {"_feed": "durable_confirmation_marker_failure_list_nonempty"}, marker
    attempts = marker.get("attempts_by_symbol")
    no_candle_observations = marker.get("no_candle_observations")
    observation_history = marker.get("observation_history")
    if (
        not isinstance(attempts, dict)
        or not isinstance(no_candle_observations, dict)
        or not isinstance(observation_history, dict)
        or set(attempts) != expected_symbols
        or set(no_candle_observations) != expected_symbols
        or set(observation_history) != expected_symbols
    ):
        return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
    try:
        configured_spacing = float(
            marker["configured_no_candle_observation_spacing_sec"]
        )
    except (KeyError, TypeError, ValueError):
        return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
    if configured_spacing < config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC:
        return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
    for symbol in expected_symbols:
        history = observation_history.get(symbol)
        try:
            attempt_count = int(attempts[symbol])
            no_candle_count = int(no_candle_observations[symbol])
        except (TypeError, ValueError):
            return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
        if (
            not isinstance(history, list)
            or attempt_count < 0
            or no_candle_count < 0
            or no_candle_count > attempt_count
            or len(history) != attempt_count
            or any(
                not isinstance(item, dict)
                or str(item.get("state", ""))
                not in {"WRITTEN", "NO_CANDLE", "INVALID_DATA", "FAILED"}
                for item in history
            )
            or sum(
                str(item.get("state", "")) == "NO_CANDLE"
                for item in history
            )
            != no_candle_count
        ):
            return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
    for symbol in verified_no_candle:
        history = observation_history.get(symbol)
        try:
            attempt_count = int(attempts[symbol])
            no_candle_count = int(no_candle_observations[symbol])
        except (TypeError, ValueError):
            return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
        if (
            not isinstance(history, list)
            or attempt_count != no_candle_count
            or attempt_count != len(history)
            or no_candle_count < config.CONFIRMATION_NO_CANDLE_OBSERVATIONS
            or not equity_feed._clean_no_candle_history(
                history,
                required_observations=config.CONFIRMATION_NO_CANDLE_OBSERVATIONS,
                minimum_spacing_sec=config.CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
                not_before=expected_no_candle_verification_time,
                not_after=published_at,
            )
            or published_at < expected_no_candle_verification_time
        ):
            return {}, {"_feed": "durable_confirmation_marker_evidence_invalid"}, marker
    expected_data_path = common.equity_1m_slot_data_path(
        confirmation_end,
        generation=LIVE_GENERATION,
        scanner_sha256=scanner_sha256,
    )
    if str(marker.get("slot_data_path", "")) != str(expected_data_path):
        return {}, {"_feed": "durable_confirmation_data_path_mismatch"}, marker
    if not expected_data_path.exists():
        return {}, {"_feed": "durable_confirmation_data_missing"}, marker
    try:
        slot_data_bytes = expected_data_path.read_bytes()
    except OSError as exc:
        return {}, {"_feed": f"durable_confirmation_data_unreadable:{exc}"}, marker
    if hashlib.sha256(slot_data_bytes).hexdigest() != str(
        marker.get("slot_data_sha256", "")
    ):
        return {}, {"_feed": "durable_confirmation_data_hash_mismatch"}, marker
    try:
        # Parse exactly the bytes whose digest was checked above.  Reopening
        # the deterministic path here would permit a concurrent atomic replace
        # between verification and use.
        frame = pd.read_parquet(io.BytesIO(slot_data_bytes))
    except Exception as exc:
        return {}, {"_feed": f"durable_confirmation_data_unreadable:{exc}"}, marker
    if "tradingsymbol" not in frame.columns or len(frame) != len(normalized_written):
        return {}, {"_feed": "durable_confirmation_data_row_count_mismatch"}, marker
    frame["tradingsymbol"] = frame["tradingsymbol"].astype(str).str.strip().str.upper()
    if (
        set(frame["tradingsymbol"]) != normalized_written
        or frame["tradingsymbol"].duplicated().any()
    ):
        return {}, {"_feed": "durable_confirmation_data_symbol_mismatch"}, marker
    candidates_by_symbol = {
        str(candidate["tradingsymbol"]).strip().upper(): candidate
        for candidate in candidates
    }
    bars: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict("records"):
        symbol = str(row["tradingsymbol"]).strip().upper()
        if int(row.get("instrument_token", 0) or 0) != int(
            candidates_by_symbol[symbol]["instrument_token"]
        ):
            return {}, {"_feed": "durable_confirmation_data_token_mismatch"}, marker
        error = equity_feed._validate_bar(row, confirmation_end)
        if error:
            return {}, {"_feed": f"durable_confirmation_data_{error}"}, marker
        if is_g_config(config) and not equity_feed._g_volume_snapshot_valid(row, confirmation_end, config):
            return {}, {"_feed": "durable_confirmation_volume_snapshot_invalid"}, marker
        row["timestamp"] = confirmation_end.isoformat()
        bars[symbol] = row
    return bars, {}, marker


def process_confirmation_slot(
    snapshot: dict[str, Any],
    session_date: date,
    signal_end: str,
    pool: KitePool | None,
    args: argparse.Namespace,
) -> dict[str, Any]:
    if snapshot.get("strategy_version") != config.STRATEGY_VERSION:
        raise RuntimeError("The 5-minute scanner snapshot has a stale strategy version.")
    if snapshot.get("strategy_fingerprint") != config.strategy_fingerprint():
        raise RuntimeError("The 5-minute scanner snapshot failed its fingerprint check.")
    if snapshot.get("session_date") != session_date.isoformat():
        raise RuntimeError("The 5-minute scanner snapshot has the wrong session date.")
    if snapshot.get("signal_end") != signal_end:
        raise RuntimeError("The 5-minute scanner snapshot has the wrong signal slot.")
    if snapshot.get("data_contract") != hybrid.DATA_CONTRACT_VERSION:
        raise RuntimeError("The 5-minute scanner snapshot has the wrong data contract.")
    candidates = list(snapshot.get("candidates") or [])
    scanner_complete = snapshot.get("state") == "SUCCESS"
    _ = pool  # The confirmation consumer deliberately has no broker/API path.
    bars: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    feed_marker: dict[str, Any] = {}
    if scanner_complete:
        bars, errors, feed_marker = _load_completed_confirmation_feed(
            snapshot, session_date, signal_end
        )
    ineligible_no_candle = (
        {
            str(symbol).strip().upper()
            for symbol in feed_marker.get("verified_no_candle_symbols", [])
        }
        if not errors
        else set()
    )
    confirmed_rows = [
        confirmation_metrics(
            candidate,
            bars[str(candidate["tradingsymbol"]).strip().upper()],
        )
        for candidate in candidates
        if str(candidate["tradingsymbol"]).strip().upper() in bars
    ]
    confirmation_feature_evaluations: list[dict[str, Any]] = []
    if is_g_config(config):
        for row in confirmed_rows:
            try:
                confirmation_feature_evaluations.append(
                    evaluate_v13_v10_g_base_row(
                        {**row, "run_id": RUN_ID},
                        nifty_return=row.get("nifty_first_bar_return_pct"),
                        strategy_version=config.STRATEGY_VERSION,
                        strategy_fingerprint=config.strategy_fingerprint(),
                    )
                )
            except Exception as exc:
                confirmation_feature_evaluations.append({
                    "schema_version": FEATURE_LEDGER_SCHEMA,
                    "session_date": session_date.isoformat(),
                    "signal_ts": str(row.get("signal_ts", "")),
                    "tradingsymbol": str(row.get("tradingsymbol", "")),
                    "run_id": RUN_ID,
                    "evaluation_state": "TELEMETRY_ERROR",
                    "error_type": type(exc).__name__,
                })
    candidate_symbols = {
        str(candidate.get("tradingsymbol", "")).strip().upper()
        for candidate in candidates
    }
    confirmation_complete = bool(
        scanner_complete
        and not errors
        and set(bars) | ineligible_no_candle == candidate_symbols
        and not set(bars) & ineligible_no_candle
    )
    complete = scanner_complete and confirmation_complete
    signals = (
        select_entry_signals(
            confirmed_rows,
            session_date,
            signal_end,
            capital_rs=args.capital,
            leverage=args.leverage,
        )
        if complete
        else []
    )
    result = {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_equity_confirmation_1m_v3",
        "run_id": RUN_ID,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "confirmation_source": "DURABLE_COMPLETED_NSE_EQUITY_1M_FEED",
        "confirmation_feed_schema": config.CONFIRMATION_FEED_SCHEMA_VERSION,
        "confirmation_feed_policy": config.CONFIRMATION_FEED_POLICY,
        "confirmation_feed_marker_sha256": (
            common.canonical_json_sha256(feed_marker) if feed_marker else ""
        ),
        "confirmation_bar_snapshot_sha256": str(
            feed_marker.get("slot_data_sha256", "")
        ),
        "published_at_ist": _iso_now(),
        "candidate_count": len(candidates),
        "confirmation_bars": len(bars),
        "ineligible_no_candle_count": len(ineligible_no_candle),
        "ineligible_no_candle_symbols": sorted(ineligible_no_candle),
        "candidate_rejections": {
            **({str(row["tradingsymbol"]): str(row.get("confirmation_reason", "CONFIRMATION_REJECTED"))
                for row in confirmed_rows if not row.get("confirmed")} if is_g_config(config) else {}),
            **{symbol: "INELIGIBLE_NO_CANDLE"
               for symbol in sorted(ineligible_no_candle)},
        },
        "directional_confirmed": sum(bool(row.get("confirmed")) for row in confirmed_rows),
        "selected_long": sum(signal["side"] == "LONG" for signal in signals),
        "selected_short": sum(signal["side"] == "SHORT" for signal in signals),
        "selected_signal_ids": [signal["signal_id"] for signal in signals],
        "error_count": len(errors),
        "errors": errors,
        "scanner_complete": scanner_complete,
        "state": "SUCCESS" if complete else "BLOCKED_INCOMPLETE_DATA",
        "_selected_signals": signals,
    }
    if is_g_config(config):
        result.update(
            feature_ledger_schema=FEATURE_LEDGER_SCHEMA,
            feature_evaluation_phase="CONFIRMATION_AND_SETUP_GATES",
            feature_evaluation_count=len(confirmation_feature_evaluations),
            feature_evaluations=confirmation_feature_evaluations,
            feature_evaluations_sha256=canonical_payload_sha256(
                confirmation_feature_evaluations
            ),
        )
    return result


def _blocked_stale_confirmation(
    source: dict[str, Any],
    session_date: date,
    signal_end: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_equity_confirmation_1m_v3",
        "run_id": RUN_ID,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "confirmation_source": "DURABLE_COMPLETED_NSE_EQUITY_1M_FEED",
        "confirmation_feed_schema": config.CONFIRMATION_FEED_SCHEMA_VERSION,
        "confirmation_feed_policy": config.CONFIRMATION_FEED_POLICY,
        "published_at_ist": _iso_now(),
        "candidate_count": len(source.get("candidates") or []),
        "confirmation_bars": 0,
        "ineligible_no_candle_count": 0,
        "ineligible_no_candle_symbols": [],
        "candidate_rejections": {},
        "directional_confirmed": 0,
        "selected_long": 0,
        "selected_short": 0,
        "selected_signal_ids": [],
        "error_count": 1,
        "errors": {"_slot": reason},
        "scanner_complete": source.get("state") == "SUCCESS",
        "state": "BLOCKED_STALE_ACTIVATION",
    }


def load_signals(session_date: date, side: str = "") -> list[dict[str, Any]]:
    root = signal_day_dir(session_date)
    if not root.exists():
        return []
    authoritative_ids = _authoritative_signal_ids(session_date)
    rows = [
        _read_json(path)
        for path in sorted(root.glob("*.json"))
        if path.stem in authoritative_ids
    ]
    rows = [row for row in rows if row]
    for row in rows:
        _validate_signal(row, session_date)
    observed_ids = {str(row["signal_id"]) for row in rows}
    missing_ids = authoritative_ids - observed_ids
    if missing_ids:
        raise RuntimeError(
            "Authoritative confirmation signal file(s) are missing: "
            + ", ".join(sorted(missing_ids))
        )
    if side:
        rows = [row for row in rows if str(row.get("side")) == side.upper()]
    return rows


def _order_path(session_date: date, mode: str, signal_id: str) -> Path:
    return order_day_dir(session_date, mode) / f"{signal_id}.json"


def load_order_states(
    session_date: date,
    *,
    mode: str = "",
    side: str = "",
) -> list[dict[str, Any]]:
    mode_roots = [order_day_dir(session_date, mode)] if mode else [
        order_day_dir(session_date, "PAPER"),
        order_day_dir(session_date, "LIVE"),
    ]
    rows: list[dict[str, Any]] = []
    for root in mode_roots:
        if root.exists():
            rows.extend(_read_json(path) for path in sorted(root.glob("*.json")))
    rows = [row for row in rows if row]
    if side:
        rows = [row for row in rows if str(row.get("side")) == side.upper()]
    return rows


def create_order_state(
    signal: dict[str, Any],
    mode: str,
    *,
    live_quantity: int | None = None,
) -> dict[str, Any]:
    execution_mode = mode.upper()
    sizing_key = "live_sizing" if execution_mode == "LIVE" else "paper_sizing"
    sizing = dict(signal[sizing_key])
    strategy_quantity = int(sizing["quantity"])
    execution_quantity = strategy_quantity
    if execution_mode == "LIVE" and live_quantity is not None:
        if int(live_quantity) <= 0:
            raise ValueError("LIVE execution quantity must be positive.")
        execution_quantity = min(strategy_quantity, int(live_quantity))
    status = "PENDING_ENTRY" if execution_quantity > 0 else "BLOCKED_SIZING"
    created_at_ist = _iso_now()
    return {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_equity_order_state_v2",
        "run_id": signal.get("run_id", RUN_ID),
        "origin_run_id": signal.get("run_id", RUN_ID),
        "last_managed_run_id": RUN_ID,
        "strategy_version": signal["strategy_version"],
        "strategy_fingerprint": signal["strategy_fingerprint"],
        "signal_id": signal["signal_id"],
        "setup_id": signal.get("setup_id", ""),
        "picker": signal.get("picker", ""),
        "rank": signal.get("rank_within_scan", signal.get("rank", 0)),
        "rank_within_scan": signal.get("rank_within_scan", signal.get("rank", 0)),
        "session_date": signal["session_date"],
        "signal_end": signal["signal_end"],
        "confirmation_end": signal["confirmation_end"],
        "entry_activation_deadline_ist": signal["entry_activation_deadline_ist"],
        "side": signal["side"],
        "tradingsymbol": signal["tradingsymbol"],
        "exchange": signal["exchange"],
        "instrument_token": signal["instrument_token"],
        "futures_tradingsymbol": signal["futures_tradingsymbol"],
        "futures_instrument_token": signal["futures_instrument_token"],
        "data_contract": signal["data_contract"],
        "tick_size": signal["tick_size"],
        "lot_size": signal["lot_size"],
        "mode": execution_mode,
        "status": status,
        "status_reason": sizing["state"],
        "quantity": execution_quantity,
        "strategy_sized_quantity": strategy_quantity,
        "execution_quantity_override": (
            int(live_quantity)
            if execution_mode == "LIVE" and live_quantity is not None
            else None
        ),
        "execution_profile": (
            EXECUTION_SESSION_NAMESPACE
            if execution_mode == "LIVE" and EXECUTION_SESSION_NAMESPACE
            else ""
        ),
        "quantity_policy": (
            "FIXED_ONE_SHARE"
            if execution_mode == "LIVE" and int(live_quantity or 0) == 1
            else "STRATEGY_SIZED"
        ),
        "capital_rs": float(signal["capital_rs"]),
        "leverage": float(signal["leverage"]),
        "target_exposure_rs": float(signal["target_exposure_rs"]),
        "trigger_price": float(signal["trigger_price"]),
        "stop_pct": float(signal["stop_pct"]),
        **_g_stop_policy_metadata(signal["session_date"]),
        "target_pct": float(signal["target_pct"]),
        "stop_price": float(signal["stop_price"]),
        "target_price": float(signal["target_price"]),
        "round_trip_cost_bps": float(signal["round_trip_cost_bps"]),
        "created_at_ist": created_at_ist,
        "updated_at_ist": created_at_ist,
        # Keep the diagnostic cause separate from ``status_reason``.  The
        # latter is a current-state label and used to be overwritten by the
        # activation-deadline transition, which made an authentication or
        # placement failure look like a process that started late.
        "first_entry_blocker_reason": "",
        "last_entry_blocker_reason": "",
        "last_entry_blocker_at_ist": "",
        "execution_error_count": 0,
        "first_execution_error_type": "",
        "first_execution_error_message": "",
        "first_execution_error_at_ist": "",
        "last_execution_error_type": "",
        "last_execution_error_message": "",
        "last_execution_error_at_ist": "",
        "entry_terminal_cause": "",
        "entry_order_type": "",
        "entry_submission_reason": "",
        "entry_submission_attempt_count": 0,
        "entry_submission_uncertain": False,
        "entry_submission_uncertain_at_ist": "",
        "entry_submission_uncertain_reason": "",
        "entry_submission_reconciled_at_ist": "",
        "entry_order_activated_at_ist": "",
        "entry_order_id": "",
        "stop_order_id": "",
        "target_order_id": "",
        "squareoff_order_id": "",
        "entry_price": 0.0,
        "entry_at_ist": "",
        "last_price": 0.0,
        "exit_price": 0.0,
        "exit_at_ist": "",
        "exit_reason": "",
        "gross_pnl_rs": 0.0,
        "estimated_cost_rs": 0.0,
        "net_pnl_rs": 0.0,
        "net_return_exposure_pct": 0.0,
        "return_on_capital_pct": 0.0,
    }


def _validate_order_state(
    state: dict[str, Any],
    signal: dict[str, Any],
    mode: str,
    *,
    live_quantity: int | None = None,
) -> None:
    execution_mode = mode.upper()
    expected_fields = {
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "signal_id": signal["signal_id"],
        "session_date": signal["session_date"],
        "signal_end": signal["signal_end"],
        "confirmation_end": signal["confirmation_end"],
        "entry_activation_deadline_ist": signal["entry_activation_deadline_ist"],
        "side": signal["side"],
        "tradingsymbol": signal["tradingsymbol"],
        "exchange": signal["exchange"],
        "instrument_token": signal["instrument_token"],
        "futures_tradingsymbol": signal["futures_tradingsymbol"],
        "futures_instrument_token": signal["futures_instrument_token"],
        "data_contract": signal["data_contract"],
        "mode": execution_mode,
        "capital_rs": config.CAPITAL_PER_ENTRY_RS,
        "leverage": config.LEVERAGE,
        "target_exposure_rs": config.TARGET_EXPOSURE_RS,
        "trigger_price": signal["trigger_price"],
        "stop_pct": signal["stop_pct"],
        **_g_stop_policy_metadata(signal["session_date"]),
        "target_pct": signal["target_pct"],
        "round_trip_cost_bps": config.ROUND_TRIP_COST_BPS,
    }
    for field, expected in expected_fields.items():
        observed = state.get(field)
        if isinstance(expected, float):
            matches = abs(_safe_float(observed, np.nan) - expected) <= 1e-9
        else:
            matches = observed == expected
        if not matches:
            raise RuntimeError(
                f"Order state {state.get('signal_id')} has invalid {field}: "
                f"expected {expected}, observed {observed}."
            )
    sizing_key = "live_sizing" if execution_mode == "LIVE" else "paper_sizing"
    expected_quantity = int(signal[sizing_key]["quantity"])
    if execution_mode == "LIVE" and live_quantity is not None:
        if int(live_quantity) <= 0:
            raise ValueError("LIVE execution quantity must be positive.")
        expected_quantity = min(expected_quantity, int(live_quantity))
        if _safe_int(state.get("execution_quantity_override"), -1) != int(
            live_quantity
        ):
            raise RuntimeError(
                f"Order state {state.get('signal_id')} does not carry the "
                f"required LIVE quantity override {int(live_quantity)}."
            )
        expected_profile = EXECUTION_SESSION_NAMESPACE
        if str(state.get("execution_profile", "")) != expected_profile:
            raise RuntimeError(
                f"Order state {state.get('signal_id')} has execution profile "
                f"{state.get('execution_profile')!r}; expected {expected_profile!r}."
            )
        expected_policy = (
            "FIXED_ONE_SHARE" if int(live_quantity) == 1 else "STRATEGY_SIZED"
        )
        if str(state.get("quantity_policy", "")) != expected_policy:
            raise RuntimeError(
                f"Order state {state.get('signal_id')} has quantity policy "
                f"{state.get('quantity_policy')!r}; expected {expected_policy!r}."
            )
    observed_quantity = _safe_int(state.get("quantity"), -1)
    status = str(state.get("status", ""))
    if status == "PENDING_ENTRY" or execution_mode == "PAPER":
        quantity_ok = observed_quantity == expected_quantity
    else:
        quantity_ok = 0 <= observed_quantity <= expected_quantity
    if not quantity_ok:
        raise RuntimeError(
            f"Order state {state.get('signal_id')} has invalid quantity "
            f"{observed_quantity}; maximum is {expected_quantity}."
        )


def _close_state(
    state: dict[str, Any],
    exit_price: float,
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    entry = float(state["entry_price"])
    quantity = int(state["quantity"])
    partial_quantity = _safe_int(state.get("staged_partial_exit_quantity"), 0)
    if partial_quantity:
        exit_price = (float(state["staged_partial_exit_notional"])
                      + float(exit_price) * (quantity - partial_quantity)) / quantity
    long_side = state["side"] == "LONG"
    gross = (
        (float(exit_price) - entry) * quantity
        if long_side
        else (entry - float(exit_price)) * quantity
    )
    entry_notional = entry * quantity
    cost = entry_notional * float(state["round_trip_cost_bps"]) / 10_000.0
    net = gross - cost
    state.update(
        {
            "status": "CLOSED",
            "status_reason": reason,
            "exit_price": float(exit_price),
            "exit_at_ist": now.isoformat(timespec="seconds"),
            "exit_reason": reason,
            "last_price": float(exit_price),
            "gross_pnl_rs": gross,
            "estimated_cost_rs": cost,
            "net_pnl_rs": net,
            "net_return_exposure_pct": (
                net / entry_notional * 100.0 if entry_notional else 0.0
            ),
            "return_on_capital_pct": (
                net / float(state["capital_rs"]) * 100.0
                if float(state["capital_rs"]) > 0
                else 0.0
            ),
            "updated_at_ist": now.isoformat(timespec="seconds"),
        }
    )
    return state


def _g_stop_policy_metadata(session_date: Any) -> dict[str, Any]:
    if not is_g_config(config) or not g_policy.enabled_for_session(session_date):
        return {}
    return {
        "stop_policy": "STAGED_125_TO_100_120M",
        "initial_stop_pct": g_policy.INITIAL_STOP_PCT,
        "tightened_stop_pct": g_policy.TIGHTENED_STOP_PCT,
        "tighten_after_minutes": g_policy.TIGHTEN_AFTER_MINUTES,
    }


def _staged_stop_target(state: dict[str, Any], now: datetime) -> float | None:
    """Compute a due stop from the actual fill, never from the signal clock."""
    policy = _g_stop_policy_metadata(state.get("session_date"))
    if not policy or not state.get("entry_at_ist"):
        return None
    entered = datetime.fromisoformat(str(state["entry_at_ist"]))
    if entered.tzinfo is None:
        entered = entered.replace(tzinfo=common.IST)
    due = entered + timedelta(minutes=policy["tighten_after_minutes"])
    state["stop_tighten_due_at_ist"] = due.isoformat(timespec="seconds")
    if now < due:
        return None
    stop, _ = config.bracket_levels(
        float(state["entry_price"]), str(state["side"]),
        policy["tightened_stop_pct"], float(state["target_pct"]),
        float(state["tick_size"]),
    )
    return stop


def _confirm_staged_stop(state: dict[str, Any], stop: float, now: datetime) -> None:
    # Keep stop_pct immutable: it identifies the initial signal contract.
    state.update(stop_price=stop, active_stop_pct=g_policy.TIGHTENED_STOP_PCT,
                 stop_tightened=True, stop_modification_uncertain=False,
                 stop_tighten_status="CONFIRMED")
    state.setdefault("stop_tightened_at_ist", now.isoformat(timespec="seconds"))


def advance_paper_order(
    state: dict[str, Any],
    last_price: float,
    now: datetime,
) -> dict[str, Any]:
    status = str(state["status"])
    if status in TERMINAL_STATES:
        return state
    if is_g_config(config):
        from fno_v13_v10_g_paper import expire_pending
        if expire_pending(state, now):
            return state
        if last_price is None:
            state.update(status_reason="WAITING_FOR_VALID_QUOTE", updated_at_ist=now.isoformat(timespec="seconds"))
            return state
    price = float(last_price)
    if not np.isfinite(price) or price <= 0:
        state.update(status_reason="INVALID_LTP", updated_at_ist=now.isoformat(timespec="seconds"))
        return state
    state["last_price"] = price
    long_side = state["side"] == "LONG"
    square_off = config.slot_datetime(now.date(), config.SQUARE_OFF)
    if status == "PENDING_ENTRY":
        activation_deadline = datetime.fromisoformat(
            str(state["entry_activation_deadline_ist"])
        )
        if now > activation_deadline:
            state.update(
                status="CANCELLED",
                status_reason="ENTRY_ACTIVATION_DEADLINE_EXPIRED",
                updated_at_ist=now.isoformat(timespec="seconds"),
            )
            return state
        if now >= square_off:
            state.update(
                status="NO_FILL",
                status_reason="STOP_ENTRY_NOT_TOUCHED_BY_SQUARE_OFF",
                updated_at_ist=now.isoformat(timespec="seconds"),
            )
            return state
        touched = price >= float(state["trigger_price"]) if long_side else price <= float(state["trigger_price"])
        if touched:
            stop, target = config.bracket_levels(
                price,
                state["side"],
                float(state["stop_pct"]),
                float(state["target_pct"]),
                float(state["tick_size"]),
            )
            state.update(
                status="OPEN",
                status_reason="PAPER_STOP_ENTRY_TOUCHED",
                entry_price=price,
                entry_at_ist=now.isoformat(timespec="seconds"),
                stop_price=stop,
                target_price=target,
                updated_at_ist=now.isoformat(timespec="seconds"),
            )
        return state
    if status == "OPEN":
        tightened_stop = _staged_stop_target(state, now)
        if tightened_stop is not None:
            _confirm_staged_stop(state, tightened_stop, now)
        stop_hit = price <= float(state["stop_price"]) if long_side else price >= float(state["stop_price"])
        target_hit = price >= float(state["target_price"]) if long_side else price <= float(state["target_price"])
        if stop_hit:
            return _close_state(state, price, "STOP", now)
        if target_hit:
            return _close_state(state, price, "TARGET", now)
        if now >= square_off:
            return _close_state(state, price, "SQUARE_OFF", now)
        entry = float(state["entry_price"])
        quantity = int(state["quantity"])
        gross = (price - entry) * quantity if long_side else (entry - price) * quantity
        estimated_cost = entry * quantity * float(state["round_trip_cost_bps"]) / 10_000.0
        state.update(
            gross_pnl_rs=gross,
            estimated_cost_rs=estimated_cost,
            net_pnl_rs=gross - estimated_cost,
            updated_at_ist=now.isoformat(timespec="seconds"),
        )
    return state


def _live_arm_state(session_date: date) -> tuple[bool, str]:
    if os.getenv(LIVE_ACK_ENV, "").strip() != LIVE_ACK:
        return False, "LIVE_ACK_MISSING"
    arm = _read_json(LIVE_ARM_PATH)
    if not bool(arm.get("enabled")):
        return False, "LIVE_ARM_FILE_DISABLED"
    if str(arm.get("session_date", "")) != session_date.isoformat():
        return False, "LIVE_ARM_DATE_MISMATCH"
    if is_g_config(config) and arm.get("strategy_fingerprint") != config.strategy_fingerprint():
        return False, "LIVE_ARM_STRATEGY_MISMATCH"
    kill = _read_json(KILL_SWITCH_PATH)
    if bool(kill.get("enabled")):
        return False, "KILL_SWITCH_ENABLED"
    return True, "LIVE_ARMED"


def _broker_order(client: Any, order_id: str) -> dict[str, Any]:
    if not order_id:
        return {}
    history = _observe_broker_call(
        "order_history", lambda: client.order_history(order_id)
    )
    return dict(history[-1]) if history else {}


def _broker_place(client: Any, **kwargs: Any) -> str:
    payload = dict(kwargs)
    if str(payload.get("order_type", "")).upper() in {"MARKET", "SL-M"}:
        payload.setdefault("market_protection", AUTO_MARKET_PROTECTION)
    return str(
        _observe_broker_call("place_order", lambda: client.place_order(**payload))
    )


def _broker_modify(client: Any, **kwargs: Any) -> str:
    payload = dict(kwargs)
    if str(payload.get("order_type", "")).upper() in {"MARKET", "SL-M"}:
        payload.setdefault("market_protection", AUTO_MARKET_PROTECTION)
    return str(_observe_broker_call("modify_order", lambda: client.modify_order(**payload)))


def _broker_cancel(client: Any, order_id: str) -> None:
    if not order_id:
        return
    try:
        _observe_broker_call(
            "cancel_order",
            lambda: client.cancel_order(variety="regular", order_id=order_id),
        )
    except Exception as exc:
        # Cancellation failures must be visible, but the telemetry path must
        # not replace the existing protective-management behavior.
        try:
            from ai_platform.observability.context import CorrelationContext
            from ai_platform.observability.journal import AppendOnlyEventJournal

            AppendOnlyEventJournal(
                LIVE_ROOT / "order_events" / "broker_errors.jsonl",
                service=f"{REPORT_PREFIX}-broker",
                strict=False,
            ).append(
                "broker_cancel_failed",
                {
                    "order_id": order_id,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                severity="ERROR",
                context=CorrelationContext(
                    service=f"{REPORT_PREFIX}-broker",
                    profile=getattr(config, "STRATEGY_PROFILE", "V13_V10_G"),
                    strategy_version=config.STRATEGY_VERSION,
                    strategy_fingerprint=config.strategy_fingerprint(),
                    run_id=RUN_ID,
                    order_id=order_id,
                ),
            )
        except Exception:
            pass
        print(
            f"[{REPORT_PREFIX}] broker cancel failed for order {order_id}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )


def _broker_find_tagged_order(
    client: Any,
    *,
    tag: str,
    tradingsymbol: str,
    transaction_type: str,
    order_type: str | tuple[str, ...],
) -> dict[str, Any]:
    expected_order_types = (
        (order_type,) if isinstance(order_type, str) else tuple(order_type)
    )
    order_type_label = "|".join(value.upper() for value in expected_order_types)
    matches = []
    for raw in _observe_broker_call("list_orders", client.orders):
        row = dict(raw)
        if str(row.get("tag", "")) != tag:
            continue
        if str(row.get("tradingsymbol", "")) != tradingsymbol:
            continue
        if str(row.get("transaction_type", "")).upper() != transaction_type.upper():
            continue
        if not any(
            _broker_order_type_matches(
                expected,
                row.get("order_type"),
                protected=expected.upper() in {"MARKET", "SL-M"},
            )
            for expected in expected_order_types
        ):
            continue
        matches.append(row)
    if len(matches) > 1:
        order_ids = tuple(sorted(str(row.get("order_id", "")) for row in matches))
        duplicate_key = (
            tag,
            tradingsymbol,
            transaction_type.upper(),
            order_type_label,
            *order_ids,
        )
        with _OBSERVABILITY_EVENT_LOCK:
            first_observation = duplicate_key not in _REPORTED_DUPLICATE_ORDER_GROUPS
            if first_observation:
                _REPORTED_DUPLICATE_ORDER_GROUPS.add(duplicate_key)
                _PENDING_DUPLICATE_ORDER_EVENTS.append(
                    {
                        "tag": tag,
                        "tradingsymbol": tradingsymbol,
                        "transaction_type": transaction_type.upper(),
                        "order_type": order_type_label,
                        "order_ids": list(order_ids),
                        "duplicate_count": len(matches) - 1,
                    }
                )
        if first_observation:
            try:
                runtime = _observability_runtime()
                if runtime is not None:
                    runtime.standard_metrics.duplicate_order_total.inc(
                        len(matches) - 1, mode="live", asset="equity"
                    )
            except Exception:
                pass
    return matches[-1] if matches else {}


def _live_tag(signal_id: str) -> str:
    tag_seed = (
        f"{EXECUTION_SESSION_NAMESPACE}:{signal_id}"
        if EXECUTION_SESSION_NAMESPACE
        else signal_id
    )
    return ORDER_TAG_PREFIX + hashlib.sha1(tag_seed.encode("ascii")).hexdigest()[:14]


def _live_order_tag(signal_id: str, role: str) -> str:
    """Return a unique deterministic tag for one broker-order role."""

    return _live_tag(signal_id) + ORDER_ROLE_TAG_SUFFIX[role]


def _broker_find_role_order(
    client: Any,
    *,
    signal_id: str,
    role: str,
    tradingsymbol: str,
    transaction_type: str,
    order_type: str | tuple[str, ...],
) -> dict[str, Any]:
    """Recover a role-specific order, including the pre-role-tag contract.

    Older live processes used one deterministic tag for every order belonging
    to a signal.  The symbol, side and order type still disambiguate those
    orders.  Looking up the legacy tag after the current role tag prevents a
    restart from placing a duplicate against retained broker state.
    """

    recovered = _broker_find_tagged_order(
        client,
        tag=_live_order_tag(signal_id, role),
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        order_type=order_type,
    )
    if recovered:
        return recovered
    return _broker_find_tagged_order(
        client,
        tag=_live_tag(signal_id),
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        order_type=order_type,
    )


def _entry_trigger_touched(state: dict[str, Any], last_price: float) -> bool:
    trigger = float(state["trigger_price"])
    return last_price >= trigger if str(state["side"]) == "LONG" else last_price <= trigger


def _valid_live_price(value: Any) -> float | None:
    price = _safe_float(value)
    return price if np.isfinite(price) and price > 0 else None


def _live_quote_price(client: Any, tradingsymbol: str) -> float | None:
    """Read one NSE quote through either KitePool or a direct Kite client."""

    quote_prices = getattr(client, "quote_prices", None)
    if callable(quote_prices):
        result = quote_prices([tradingsymbol])
        values = result[0] if isinstance(result, tuple) else result
    else:
        values = _quote_prices(client, [tradingsymbol])
    if not isinstance(values, dict):
        return None
    return _valid_live_price(values.get(tradingsymbol))


def _broker_find_entry_order(
    client: Any,
    *,
    signal_id: str,
    tradingsymbol: str,
    transaction_type: str,
) -> dict[str, Any]:
    """Recover stop or catch-up-market entries by the same role tag."""

    return _broker_find_role_order(
        client,
        signal_id=signal_id,
        role="entry",
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        order_type=("SL-M", "MARKET"),
    )


def _validate_recovered_order_quantity(
    state: dict[str, Any],
    recovered: dict[str, Any],
) -> None:
    """Fail closed before adopting a broker order from a previous process."""

    if not EXECUTION_SESSION_NAMESPACE:
        return
    observed = _safe_int(recovered.get("quantity"), -1)
    expected = int(state["quantity"])
    if observed != expected:
        raise RuntimeError(
            "Recovered broker order quantity does not match the isolated "
            f"execution profile: observed={observed}, expected={expected}."
        )


def _apply_live_entry_fill(
    state: dict[str, Any],
    entry_order: dict[str, Any],
    now: datetime,
) -> bool:
    fill_price = _safe_float(entry_order.get("average_price"))
    filled_quantity = _safe_int(
        entry_order.get("filled_quantity"), int(state["quantity"])
    )
    if fill_price <= 0 or filled_quantity <= 0:
        state.update(
            status_reason="COMPLETE_ENTRY_MISSING_FILL",
            updated_at_ist=now.isoformat(timespec="seconds"),
        )
        return False
    stop, target = config.bracket_levels(
        fill_price,
        str(state["side"]),
        float(state["stop_pct"]),
        float(state["target_pct"]),
        float(state["tick_size"]),
    )
    entered = now
    if _g_stop_policy_metadata(state.get("session_date")):
        try:
            actual_fill = pd.Timestamp(entry_order.get("exchange_update_timestamp"))
            if not pd.isna(actual_fill):
                actual_fill = (actual_fill.tz_localize(common.IST) if actual_fill.tzinfo is None
                               else actual_fill.tz_convert(common.IST))
                if actual_fill.date() == now.date() and actual_fill <= pd.Timestamp(now):
                    entered = actual_fill.to_pydatetime()
                    state["entry_time_source"] = "BROKER_EXCHANGE_UPDATE_TIMESTAMP"
        except (TypeError, ValueError):
            pass
        state.setdefault("entry_time_source", "FILL_OBSERVATION_CLOCK")
    state.update(
        status="OPEN",
        status_reason="LIVE_ENTRY_FILLED",
        entry_price=fill_price,
        quantity=filled_quantity,
        entry_at_ist=entered.isoformat(timespec="seconds"),
        stop_price=stop,
        target_price=target,
        updated_at_ist=now.isoformat(timespec="seconds"),
    )
    return True


def _entry_state_created_by_deadline(
    state: dict[str, Any], activation_deadline: datetime
) -> bool:
    """Return whether this persisted entry state existed inside its live window.

    Missing or malformed legacy timestamps deliberately return ``False`` so a
    process can never use this diagnostic distinction to justify a retroactive
    order.  The result changes only the terminal reason; the deadline remains
    fail-closed in both cases.
    """

    try:
        created = datetime.fromisoformat(str(state.get("created_at_ist", "")))
        if created.tzinfo is None and activation_deadline.tzinfo is not None:
            created = created.replace(tzinfo=activation_deadline.tzinfo)
        return created <= activation_deadline
    except (TypeError, ValueError):
        return False


def _record_live_execution_error(
    state: dict[str, Any], exc: BaseException, now: datetime
) -> None:
    """Retain the first and latest broker-management failure on active state."""

    observed_at = now.isoformat(timespec="seconds")
    error_type = type(exc).__name__
    error_message = str(exc)
    try:
        error_count = int(state.get("execution_error_count", 0)) + 1
    except (TypeError, ValueError):
        error_count = 1
    if not state.get("first_execution_error_type"):
        state.update(
            first_execution_error_type=error_type,
            first_execution_error_message=error_message,
            first_execution_error_at_ist=observed_at,
        )
    state.update(
        execution_error_count=error_count,
        last_execution_error_type=error_type,
        last_execution_error_message=error_message,
        last_execution_error_at_ist=observed_at,
        status_reason=f"{error_type}: {error_message}",
        updated_at_ist=observed_at,
    )


def _pending_entry_terminal_cause(state: dict[str, Any]) -> str:
    error_type = str(state.get("last_execution_error_type", "")).strip()
    if error_type:
        return f"EXECUTION_ERROR:{error_type}"
    blocker = str(state.get("last_entry_blocker_reason", "")).strip()
    if blocker:
        return f"ENTRY_BLOCKER:{blocker}"
    return "NO_ENTRY_ORDER_RECORDED"


def _record_entry_submission_attempt(
    state: dict[str, Any],
    *,
    order_type: str,
    reason: str,
    now: datetime,
) -> None:
    try:
        attempts = int(state.get("entry_submission_attempt_count", 0)) + 1
    except (TypeError, ValueError):
        attempts = 1
    state.update(
        entry_order_type=order_type,
        entry_submission_reason=reason,
        entry_submission_attempt_count=attempts,
        updated_at_ist=now.isoformat(timespec="seconds"),
    )


def _place_live_entry_order(
    state: dict[str, Any],
    client: Any,
    now: datetime,
    *,
    transaction_type: str,
    tag: str,
    order_type: str,
    reason: str,
) -> str:
    _record_entry_submission_attempt(
        state,
        order_type=order_type,
        reason=reason,
        now=now,
    )
    payload: dict[str, Any] = {
        "variety": "regular",
        "exchange": str(state["exchange"]),
        "tradingsymbol": state["tradingsymbol"],
        "transaction_type": transaction_type,
        "quantity": int(state["quantity"]),
        "product": "MIS",
        "order_type": order_type,
        "validity": "DAY",
        "tag": tag,
    }
    if order_type == "SL-M":
        payload["trigger_price"] = float(state["trigger_price"])
    try:
        return _broker_place(client, **payload)
    except BrokerMutationUncertain as exc:
        state.update(
            entry_submission_uncertain=True,
            entry_submission_uncertain_at_ist=now.isoformat(timespec="seconds"),
            entry_submission_uncertain_reason=str(exc),
            status_reason="ENTRY_SUBMISSION_UNCERTAIN_AWAITING_RECONCILIATION",
            updated_at_ist=now.isoformat(timespec="seconds"),
        )
        raise


def _reject_live_entry(
    state: dict[str, Any],
    exc: Exception,
    now: datetime,
) -> None:
    """Persist a definite broker rejection as a terminal entry outcome."""

    _record_live_execution_error(state, exc, now)
    state.update(
        status="ENTRY_REJECTED",
        status_reason=f"BROKER_REJECTED:{type(exc).__name__}: {exc}",
        entry_terminal_cause=f"EXECUTION_ERROR:{type(exc).__name__}",
        updated_at_ist=now.isoformat(timespec="seconds"),
    )


def _begin_live_squareoff(
    state: dict[str, Any],
    client: Any,
    now: datetime,
    exit_transaction: str,
    tag: str,
    reason: str,
) -> None:
    _broker_cancel(client, str(state.get("target_order_id", "")))
    _broker_cancel(client, str(state.get("stop_order_id", "")))
    if not state.get("squareoff_order_id"):
        recovered = _broker_find_role_order(
            client,
            signal_id=str(state["signal_id"]),
            role="squareoff",
            tradingsymbol=str(state["tradingsymbol"]),
            transaction_type=exit_transaction,
            order_type="MARKET",
        )
        if recovered:
            _validate_recovered_order_quantity(
                {**state, "quantity": int(state["quantity"])
                 - _safe_int(state.get("staged_partial_exit_quantity"), 0)}, recovered)
            state["squareoff_order_id"] = str(recovered.get("order_id", ""))
    if not state.get("squareoff_order_id"):
        state["squareoff_order_id"] = _broker_place(
            client,
            variety="regular",
            exchange=str(state["exchange"]),
            tradingsymbol=state["tradingsymbol"],
            transaction_type=exit_transaction,
            quantity=int(state["quantity"]) - _safe_int(state.get("staged_partial_exit_quantity"), 0),
            product="MIS",
            order_type="MARKET",
            validity="DAY",
            tag=tag,
        )
    state.update(
        status="SQUARE_OFF_PENDING",
        status_reason=reason,
        updated_at_ist=now.isoformat(timespec="seconds"),
    )


def _advance_live_staged_stop(
    state: dict[str, Any], client: Any, now: datetime,
    stop_order: dict[str, Any], target_order: dict[str, Any],
    last_price: float | None,
) -> None:
    desired = _staged_stop_target(state, now)
    if desired is None:
        return
    stop_status = str(stop_order.get("status", "")).upper()
    if stop_status not in {"OPEN", "TRIGGER PENDING"}:
        state["stop_tighten_status"] = "WAITING_FOR_STABLE_BROKER_STOP"
        return
    if (_safe_int(stop_order.get("filled_quantity"), 0) > 0
            or (stop_status == "OPEN" and not state.get("stop_exit_market_submitted"))):
        # OPEN (rather than TRIGGER PENDING) means the protective stop has
        # triggered. Allow that existing exit to settle, including partial fills.
        state["stop_tighten_status"] = "TRIGGERED_STOP_AWAITING_FILL"
        return
    observed = _safe_float(stop_order.get("trigger_price"))
    if not state.get("stop_tighten_exit_requested"):
        already_tighter = (observed >= desired if state["side"] == "LONG"
                           else 0 < observed <= desired)
        if already_tighter:
            _confirm_staged_stop(state, observed, now)
            return
        price = _valid_live_price(last_price)
        crossed = price is not None and (price <= desired if state["side"] == "LONG"
                                         else price >= desired)
        if not crossed:
            try:
                _broker_modify(client, variety="regular", order_id=str(state["stop_order_id"]),
                               trigger_price=desired)
            except Exception as exc:
                if not _is_stop_trigger_relation_rejection(exc):
                    state.update(stop_modification_uncertain=not _is_explicit_broker_rejection(exc),
                                 stop_tighten_status="MODIFICATION_NOT_CONFIRMED")
                    raise
                crossed = True
            else:
                refreshed = _broker_order(client, str(state["stop_order_id"]))
                broker_stop = _safe_float(refreshed.get("trigger_price"))
                confirmed = (broker_stop >= desired if state["side"] == "LONG"
                             else 0 < broker_stop <= desired)
                if (str(refreshed.get("status", "")).upper() in {"OPEN", "TRIGGER PENDING"}
                        and confirmed):
                    _confirm_staged_stop(state, broker_stop, now)
                else:
                    state["stop_tighten_status"] = "AWAITING_BROKER_CONFIRMATION"
                return
        if crossed:
            # Persist the exit intent before cancelling anything. Restarts resume
            # this same protective order; they must never submit a second exit.
            state.update(stop_tighten_exit_requested=True,
                         stop_tighten_status="THRESHOLD_CROSSED_EXIT_REQUESTED")
            _write_order_state(state)

    if state.get("stop_tighten_exit_requested"):
        target_id = str(state.get("target_order_id", ""))
        if str(target_order.get("status", "")).upper() not in {"CANCELLED", "REJECTED"}:
            _broker_cancel(client, target_id)
            target_order = _broker_order(client, target_id)
        target_status = str(target_order.get("status", "")).upper()
        if target_status == "COMPLETE":
            _broker_cancel(client, str(state["stop_order_id"]))
            _close_state(state, _safe_float(target_order.get("average_price"),
                                           float(state["target_price"])), "TARGET", now)
            return
        if target_status not in {"CANCELLED", "REJECTED"}:
            state["stop_tighten_status"] = "WAITING_FOR_TARGET_CANCEL_ACK"
            return
        # The target must be gone before converting the existing protective
        # stop. This retains broker identity and avoids a competing market exit.
        target_filled = _safe_int(target_order.get("filled_quantity"), 0)
        if target_filled:
            target_average = _safe_float(target_order.get("average_price"))
            if target_average <= 0 or not 0 < target_filled <= int(state["quantity"]):
                raise RuntimeError("Invalid partial target fill during staged-stop exit")
            state.update(staged_partial_exit_quantity=target_filled,
                         staged_partial_exit_notional=target_filled * target_average,
                         staged_target_exit_quantity=target_filled,
                         staged_target_exit_notional=target_filled * target_average)
            if target_filled == int(state["quantity"]):
                _broker_cancel(client, str(state["stop_order_id"]))
                _close_state(state, target_average, "TARGET", now)
                return
        stop_order = _broker_order(client, str(state["stop_order_id"]))
        stop_status = str(stop_order.get("status", "")).upper()
        if stop_status == "COMPLETE":
            _close_state(state, _safe_float(stop_order.get("average_price"),
                                           float(state["stop_price"])), "STOP", now)
            return
        if stop_status not in {"OPEN", "TRIGGER PENDING"}:
            state["stop_tighten_status"] = "WAITING_FOR_STABLE_BROKER_STOP"
            return
        if (_safe_int(stop_order.get("filled_quantity"), 0) > 0
                or (stop_status == "OPEN" and not state.get("stop_exit_market_submitted"))):
            state["stop_tighten_status"] = "TRIGGERED_STOP_AWAITING_FILL"
            return
        converted = (str(stop_order.get("order_type", "")).upper() == "MARKET"
                     or (state.get("stop_exit_market_submitted")
                         and _safe_float(stop_order.get("trigger_price")) == 0))
        if not converted:
            state["stop_exit_market_submitted"] = True
            _write_order_state(state)
            _broker_modify(client, variety="regular", order_id=str(state["stop_order_id"]),
                           order_type="MARKET", trigger_price=0,
                           quantity=int(state["quantity"]) - target_filled)
        state["stop_tighten_status"] = "PROTECTIVE_STOP_MARKET_EXIT_PENDING"


def advance_live_order(
    state: dict[str, Any],
    client: Any,
    now: datetime,
    *,
    last_price: float | None = None,
) -> dict[str, Any]:
    status = str(state["status"])
    if status in TERMINAL_STATES:
        return state
    armed, arm_reason = _live_arm_state(now.date())
    kill_enabled = bool(_read_json(KILL_SWITCH_PATH).get("enabled"))
    if int(state["quantity"]) <= 0:
        state.update(status="BLOCKED_SIZING", status_reason="LIVE_QUANTITY_ZERO")
        return state
    square_off = config.slot_datetime(now.date(), config.SQUARE_OFF)
    side = str(state["side"])
    long_side = side == "LONG"
    entry_transaction = "BUY" if long_side else "SELL"
    exit_transaction = "SELL" if long_side else "BUY"
    signal_id = str(state["signal_id"])
    entry_tag = _live_order_tag(signal_id, "entry")
    stop_tag = _live_order_tag(signal_id, "stop")
    target_tag = _live_order_tag(signal_id, "target")
    squareoff_tag = _live_order_tag(signal_id, "squareoff")

    if status == "PENDING_ENTRY":
        activation_deadline = datetime.fromisoformat(
            str(state["entry_activation_deadline_ist"])
        )
        if not state.get("entry_order_id"):
            recovered = _broker_find_entry_order(
                client,
                signal_id=signal_id,
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=entry_transaction,
            )
            if recovered:
                _validate_recovered_order_quantity(state, recovered)
                state["entry_order_id"] = str(recovered.get("order_id", ""))
                state["entry_order_type"] = str(
                    state.get("entry_order_type")
                    or recovered.get("order_type")
                    or ""
                ).upper()
                state["entry_order_activated_at_ist"] = str(
                    recovered.get("order_timestamp") or now.isoformat(timespec="seconds")
                )
                if state.get("entry_submission_uncertain"):
                    state.update(
                        entry_submission_uncertain=False,
                        entry_submission_reconciled_at_ist=now.isoformat(
                            timespec="seconds"
                        ),
                    )
        if state.get("entry_order_id"):
            entry_order = _broker_order(client, str(state["entry_order_id"]))
            broker_status = str(entry_order.get("status", "")).upper()
            if broker_status == "COMPLETE":
                _apply_live_entry_fill(state, entry_order, now)
            elif broker_status in {"REJECTED", "CANCELLED"}:
                state.update(
                    status="ENTRY_REJECTED" if broker_status == "REJECTED" else "CANCELLED",
                    status_reason=str(entry_order.get("status_message") or broker_status),
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            elif now > activation_deadline or not armed or now >= square_off:
                _broker_cancel(client, str(state["entry_order_id"]))
                refreshed = _broker_order(client, str(state["entry_order_id"]))
                refreshed_status = str(refreshed.get("status", "")).upper()
                if refreshed_status == "COMPLETE":
                    _apply_live_entry_fill(state, refreshed, now)
                elif refreshed_status == "CANCELLED":
                    deadline_expired = now > activation_deadline
                    state.update(
                        status="NO_FILL" if now >= square_off else "CANCELLED",
                        status_reason=(
                            "SQUARE_OFF_BEFORE_FILL"
                            if now >= square_off
                            else (
                                "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
                                if deadline_expired
                                else arm_reason
                            )
                        ),
                        updated_at_ist=now.isoformat(timespec="seconds"),
                    )
                    return state
                else:
                    cancel_reason = (
                        "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
                        if now > activation_deadline
                        else arm_reason
                    )
                    state.update(
                        status_reason=f"ENTRY_CANCEL_PENDING:{cancel_reason}",
                        updated_at_ist=now.isoformat(timespec="seconds"),
                    )
                    return state
            else:
                state.update(
                    status_reason=(
                        "LIVE_MARKET_ENTRY_WORKING"
                        if str(state.get("entry_order_type", "")).upper() == "MARKET"
                        else "LIVE_STOP_ENTRY_WORKING"
                    ),
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
        else:
            if now > activation_deadline:
                existed_in_window = _entry_state_created_by_deadline(
                    state, activation_deadline
                )
                state.update(
                    status="CANCELLED",
                    status_reason=(
                        "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
                        if existed_in_window
                        else "LATE_START_NO_RETROACTIVE_ENTRY"
                    ),
                    entry_terminal_cause=_pending_entry_terminal_cause(state),
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            if now >= square_off:
                state.update(status="NO_FILL", status_reason="SQUARE_OFF_BEFORE_ENTRY")
                return state
            if not armed:
                state.setdefault("first_entry_blocker_reason", arm_reason)
                if not state.get("first_entry_blocker_reason"):
                    state["first_entry_blocker_reason"] = arm_reason
                state.update(
                    status_reason=arm_reason,
                    last_entry_blocker_reason=arm_reason,
                    last_entry_blocker_at_ist=now.isoformat(timespec="seconds"),
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            if state.get("entry_submission_uncertain"):
                state.update(
                    status_reason="ENTRY_SUBMISSION_UNCERTAIN_AWAITING_RECONCILIATION",
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            observed_price = _valid_live_price(last_price)
            if observed_price is None:
                try:
                    observed_price = _live_quote_price(
                        client, str(state["tradingsymbol"])
                    )
                except Exception:
                    observed_price = None
            if observed_price is None:
                state.update(
                    status_reason="WAITING_FOR_VALID_QUOTE",
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            state["last_price"] = observed_price
            trigger_touched = _entry_trigger_touched(state, observed_price)
            order_type = "MARKET" if trigger_touched else "SL-M"
            submission_reason = (
                "TRIGGER_ALREADY_TOUCHED"
                if trigger_touched
                else "STOP_ENTRY_ARMED_BEFORE_TRIGGER"
            )
            try:
                order_id = _place_live_entry_order(
                    state,
                    client,
                    now,
                    transaction_type=entry_transaction,
                    tag=entry_tag,
                    order_type=order_type,
                    reason=submission_reason,
                )
            except Exception as exc:
                if order_type == "SL-M" and _is_stop_trigger_relation_rejection(exc):
                    state.update(
                        entry_stop_rejection_type=type(exc).__name__,
                        entry_stop_rejection_message=str(exc),
                        entry_stop_rejection_at_ist=now.isoformat(timespec="seconds"),
                    )
                    try:
                        fresh_price = _live_quote_price(
                            client, str(state["tradingsymbol"])
                        )
                    except Exception:
                        fresh_price = None
                    if fresh_price is not None and _entry_trigger_touched(
                        state, fresh_price
                    ):
                        state["last_price"] = fresh_price
                        try:
                            order_id = _place_live_entry_order(
                                state,
                                client,
                                now,
                                transaction_type=entry_transaction,
                                tag=entry_tag,
                                order_type="MARKET",
                                reason="TRIGGER_CROSSED_DURING_STOP_SUBMISSION",
                            )
                        except Exception as market_exc:
                            if _is_explicit_broker_rejection(market_exc):
                                _reject_live_entry(state, market_exc, now)
                                return state
                            raise
                    else:
                        _reject_live_entry(state, exc, now)
                        return state
                elif _is_explicit_broker_rejection(exc):
                    _reject_live_entry(state, exc, now)
                    return state
                else:
                    raise
            state.update(
                entry_order_id=order_id,
                entry_order_activated_at_ist=now.isoformat(timespec="seconds"),
                status_reason=(
                    "LIVE_MARKET_ENTRY_PLACED"
                    if str(state.get("entry_order_type", "")).upper() == "MARKET"
                    else "LIVE_STOP_ENTRY_PLACED"
                ),
                updated_at_ist=now.isoformat(timespec="seconds"),
            )
            return state

    if state["status"] == "OPEN":
        if not state.get("squareoff_order_id"):
            recovered_market = _broker_find_role_order(
                client,
                signal_id=signal_id,
                role="squareoff",
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=exit_transaction,
                order_type="MARKET",
            )
            if recovered_market:
                _validate_recovered_order_quantity(state, recovered_market)
                state["squareoff_order_id"] = str(recovered_market.get("order_id", ""))
                state["status"] = "SQUARE_OFF_PENDING"
        if state["status"] == "OPEN" and not state.get("stop_order_id"):
            recovered_stop = _broker_find_role_order(
                client,
                signal_id=signal_id,
                role="stop",
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=exit_transaction,
                order_type=(("SL-M", "MARKET") if _g_stop_policy_metadata(state.get("session_date"))
                            else "SL-M"),
            )
            if recovered_stop:
                _validate_recovered_order_quantity(
                    {**state, "quantity": int(state["quantity"])
                     - _safe_int(state.get("staged_partial_exit_quantity"), 0)}, recovered_stop)
                state["stop_order_id"] = str(recovered_stop.get("order_id", ""))
        if state["status"] == "OPEN" and not state.get("stop_order_id"):
            try:
                state["stop_order_id"] = _broker_place(
                    client,
                    variety="regular",
                    exchange=str(state["exchange"]),
                    tradingsymbol=state["tradingsymbol"],
                    transaction_type=exit_transaction,
                    quantity=int(state["quantity"]),
                    product="MIS",
                    order_type="SL-M",
                    trigger_price=float(state["stop_price"]),
                    validity="DAY",
                    tag=stop_tag,
                )
            except BrokerMutationUncertain:
                # The stop may exist at Kite even though no response arrived.
                # Leave the state OPEN so the next loop reconciles the exact
                # deterministic tag before attempting another mutation.
                raise
            except Exception as exc:
                _begin_live_squareoff(
                    state,
                    client,
                    now,
                    exit_transaction,
                    squareoff_tag,
                    f"STOP_ORDER_PLACE_FAILED:{type(exc).__name__}",
                )
        if state["status"] == "OPEN" and not state.get("target_order_id"):
            recovered_target = _broker_find_role_order(
                client,
                signal_id=signal_id,
                role="target",
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=exit_transaction,
                order_type="LIMIT",
            )
            if recovered_target:
                _validate_recovered_order_quantity(state, recovered_target)
                state["target_order_id"] = str(recovered_target.get("order_id", ""))
        if state["status"] == "OPEN" and not state.get("target_order_id"):
            state["target_order_id"] = _broker_place(
                client,
                variety="regular",
                exchange=str(state["exchange"]),
                tradingsymbol=state["tradingsymbol"],
                transaction_type=exit_transaction,
                quantity=int(state["quantity"]),
                product="MIS",
                order_type="LIMIT",
                price=float(state["target_price"]),
                validity="DAY",
                tag=target_tag,
            )
    if state["status"] == "OPEN":
        target_order = _broker_order(client, str(state["target_order_id"]))
        stop_order = _broker_order(client, str(state["stop_order_id"]))
        stop_status = str(stop_order.get("status", "")).upper()
        target_status = str(target_order.get("status", "")).upper()
        state.update(
            stop_order_status=stop_status,
            stop_order_status_observed_at_ist=now.isoformat(timespec="seconds"),
            protection_confirmed=stop_status in {"OPEN", "TRIGGER PENDING"},
        )
        if stop_status == "COMPLETE":
            _broker_cancel(client, str(state["target_order_id"]))
            return _close_state(
                state,
                _safe_float(stop_order.get("average_price"), float(state["stop_price"])),
                "STOP",
                now,
            )
        if target_status == "COMPLETE":
            _broker_cancel(client, str(state["stop_order_id"]))
            return _close_state(
                state,
                _safe_float(target_order.get("average_price"), float(state["target_price"])),
                "TARGET",
                now,
            )
        if (stop_status not in {"REJECTED", "CANCELLED"}
                and (state.get("stop_tighten_exit_requested")
                     or (not kill_enabled and now < square_off))
                and _staged_stop_target(state, now) is not None):
            _advance_live_staged_stop(state, client, now, stop_order, target_order, last_price)
            if state.get("stop_tighten_exit_requested"):
                state["updated_at_ist"] = now.isoformat(timespec="seconds")
                return state
        if stop_status in {"REJECTED", "CANCELLED"}:
            if state.get("stop_exit_market_submitted"):
                stop_filled = _safe_int(stop_order.get("filled_quantity"), 0)
                if stop_filled:
                    stop_average = _safe_float(stop_order.get("average_price"))
                    partial_quantity = stop_filled + _safe_int(state.get("staged_target_exit_quantity"), 0)
                    if stop_average <= 0 or not 0 < partial_quantity <= int(state["quantity"]):
                        raise RuntimeError("Invalid partial protective exit during staged-stop recovery")
                    state.update(staged_partial_exit_quantity=partial_quantity,
                                 staged_partial_exit_notional=stop_filled * stop_average
                                 + _safe_float(state.get("staged_target_exit_notional")))
                    if partial_quantity == int(state["quantity"]):
                        return _close_state(state, stop_average, "STOP", now)
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                squareoff_tag,
                f"STOP_ORDER_{stop_status}",
            )
        elif target_status in {"REJECTED", "CANCELLED"}:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                squareoff_tag,
                f"TARGET_ORDER_{target_status}",
            )
        elif kill_enabled:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                squareoff_tag,
                "KILL_SWITCH_SQUARE_OFF",
            )
        elif now >= square_off:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                squareoff_tag,
                "LIVE_SQUARE_OFF_SENT",
            )
    if state["status"] == "SQUARE_OFF_PENDING":
        if not state.get("squareoff_order_id"):
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                squareoff_tag,
                str(state.get("status_reason") or "SQUARE_OFF_RECOVERY"),
            )
        order = _broker_order(client, str(state["squareoff_order_id"]))
        if str(order.get("status", "")).upper() == "COMPLETE":
            exit_price = _safe_float(order.get("average_price"))
            if exit_price <= 0:
                state.update(
                    status_reason="COMPLETE_SQUARE_OFF_MISSING_FILL",
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            return _close_state(
                state,
                exit_price,
                "SQUARE_OFF",
                now,
            )
    state["updated_at_ist"] = now.isoformat(timespec="seconds")
    return state


def _quote_prices(client: Any, symbols: list[str]) -> dict[str, float]:
    if not symbols:
        return {}
    keys = [f"NSE:{symbol}" for symbol in sorted(set(symbols))]
    payload = _observe_broker_call("ltp", lambda: client.ltp(keys))
    prices: dict[str, float] = {}
    for key, row in payload.items():
        symbol = str(key).split(":", 1)[-1]
        price = _safe_float(row.get("last_price"))
        if price > 0:
            prices[symbol] = price
    return prices


def _quote_prices_with_failover(
    clients: list[tuple[str, Any]], symbols: list[str]
) -> tuple[dict[str, float], str, list[dict[str, str]]]:
    """Prefer app1, then try each remaining Kite app once on quote failure."""

    if not symbols:
        return {}, "", []
    failures: list[dict[str, str]] = []
    last_error: Exception | None = None
    for app_name, client in clients:
        try:
            return _quote_prices(client, symbols), app_name, failures
        except Exception as exc:
            last_error = exc
            failures.append(
                {
                    "app": app_name,
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
            )
    failure_summary = "; ".join(
        f"{item['app']}={item['error_type']}: {item['message']}"
        for item in failures
    )
    raise RuntimeError(
        f"All configured Kite quote apps failed ({failure_summary})"
    ) from last_error


def _write_order_state(state: dict[str, Any]) -> None:
    session_date = date.fromisoformat(str(state["session_date"]))
    state_path = _order_path(session_date, str(state["mode"]), str(state["signal_id"]))
    previous: dict[str, Any] = {}
    if state_path.is_file():
        try:
            loaded = json.loads(state_path.read_text(encoding="utf-8"))
            previous = loaded if isinstance(loaded, dict) else {}
        except (OSError, UnicodeError, json.JSONDecodeError):
            previous = {}

    # Persist the recovery authority before any diagnostic disk/network work.
    # A successful broker call must never wait for, or be lost behind, a log,
    # journal, textfile, or exporter failure.
    common.atomic_write_json(state_path, state)

    # This hash-chained journal adds a durable transition history without being
    # able to block order management if observability itself is unavailable.
    significant = (
        "status",
        "status_reason",
        "entry_order_id",
        "stop_order_id",
        "target_order_id",
        "squareoff_order_id",
        "entry_price",
        "stop_price",
        "active_stop_pct",
        "stop_tighten_status",
        "exit_price",
        "quantity",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "exit_reason",
        "first_execution_error_type",
        "entry_terminal_cause",
    )
    changed = [name for name in significant if previous.get(name) != state.get(name)]
    if changed or not previous:
        try:
            from ai_platform.observability.context import CorrelationContext
            from ai_platform.observability.journal import AppendOnlyEventJournal

            journal_path = (
                LIVE_ROOT
                / "order_events"
                / str(state["mode"]).upper()
                / f"{session_date.isoformat()}.jsonl"
            )
            context = CorrelationContext(
                service=f"{REPORT_PREFIX}-order-worker",
                profile=getattr(config, "STRATEGY_PROFILE", "V13_V10_G"),
                strategy_version=str(state.get("strategy_version", "")),
                strategy_fingerprint=str(state.get("strategy_fingerprint", "")),
                mode=str(state.get("mode", "")),
                asset="EQUITY",
                session_date=session_date.isoformat(),
                run_id=RUN_ID,
                signal_id=str(state.get("signal_id", "")),
                order_id=str(
                    state.get("entry_order_id")
                    or state.get("squareoff_order_id")
                    or ""
                ),
            )
            AppendOnlyEventJournal(
                journal_path,
                service=f"{REPORT_PREFIX}-order-worker",
                strict=False,
            ).append(
                "order_state_transition",
                {
                    "state_before": previous.get("status", "UNSEEN"),
                    "state_after": state.get("status", "UNKNOWN"),
                    "reason": state.get("status_reason", ""),
                    "changed_fields": changed,
                    "tradingsymbol": state.get("tradingsymbol", ""),
                    "side": state.get("side", ""),
                    "entry_order_id": state.get("entry_order_id", ""),
                    "stop_order_id": state.get("stop_order_id", ""),
                    "target_order_id": state.get("target_order_id", ""),
                    "squareoff_order_id": state.get("squareoff_order_id", ""),
                    "quantity": state.get("quantity", 0),
                    "entry_price": state.get("entry_price", 0.0),
                    "exit_price": state.get("exit_price", 0.0),
                    "net_pnl_rs": state.get("net_pnl_rs", 0.0),
                    "first_execution_error_type": state.get(
                        "first_execution_error_type", ""
                    ),
                    "first_execution_error_at_ist": state.get(
                        "first_execution_error_at_ist", ""
                    ),
                    "last_execution_error_type": state.get(
                        "last_execution_error_type", ""
                    ),
                    "last_execution_error_at_ist": state.get(
                        "last_execution_error_at_ist", ""
                    ),
                    "execution_error_count": state.get("execution_error_count", 0),
                    "entry_terminal_cause": state.get("entry_terminal_cause", ""),
                    "updated_at_ist": state.get("updated_at_ist", ""),
                    "origin_run_id": state.get("origin_run_id", state.get("run_id", "")),
                },
                severity=(
                    "ERROR"
                    if str(state.get("status", "")).upper() in {"ENTRY_REJECTED", "FAILED"}
                    else "INFO"
                ),
                context=context,
            )
        except Exception:
            # Telemetry is deliberately fail-open; the canonical recovery state
            # below must still be written and managed.
            pass
        try:
            runtime = _observability_runtime()
            if runtime is not None:
                status = str(state.get("status", "unknown")).lower()
                outcome = (
                    "error"
                    if str(state.get("status", "")).upper()
                    in {"ENTRY_REJECTED", "FAILED", "BLOCKED_SIZING", "BLOCKED_PORTFOLIO"}
                    else "success"
                )
                mode = str(state.get("mode", "PAPER")).lower()
                strategy = getattr(config, "STRATEGY_PROFILE", "V13_V10_G").lower()
                with runtime.bind(
                    profile=getattr(config, "STRATEGY_PROFILE", "V13_V10_G"),
                    strategy_version=str(state.get("strategy_version", "")),
                    strategy_fingerprint=str(state.get("strategy_fingerprint", "")),
                    mode=mode,
                    asset="EQUITY",
                    session_date=session_date.isoformat(),
                    run_id=RUN_ID,
                    signal_id=str(state.get("signal_id", "")),
                    order_id=str(
                        state.get("entry_order_id")
                        or state.get("squareoff_order_id")
                        or ""
                    ),
                ):
                    runtime.standard_metrics.order_events_total.inc(
                        strategy=strategy,
                        mode=mode,
                        asset="equity",
                        event=status,
                        outcome=outcome,
                    )
                    semantic_events: list[tuple[str, str]] = []
                    if (
                        mode == "live"
                        and not previous.get("entry_order_id")
                        and state.get("entry_order_id")
                    ) or (
                        mode == "paper"
                        and not previous
                        and str(state.get("status", "")).upper() == "PENDING_ENTRY"
                    ):
                        semantic_events.append(("submit", "success"))
                    if (
                        str(state.get("status", "")).upper() == "OPEN"
                        and str(previous.get("status", "")).upper() != "OPEN"
                    ):
                        semantic_events.append(("fill", "success"))
                    if (
                        str(state.get("status", "")).upper() == "ENTRY_REJECTED"
                        and str(previous.get("status", "")).upper() != "ENTRY_REJECTED"
                    ):
                        semantic_events.append(("submit", "error"))
                    if (
                        str(state.get("status", "")).upper() == "CLOSED"
                        and str(previous.get("status", "")).upper() != "CLOSED"
                    ):
                        semantic_events.append(("exit", "success"))
                    for event_name, event_outcome in semantic_events:
                        runtime.standard_metrics.order_events_total.inc(
                            strategy=strategy,
                            mode=mode,
                            asset="equity",
                            event=event_name,
                            outcome=event_outcome,
                        )
                    runtime.event(
                        "order.state.transition",
                        severity="ERROR" if outcome == "error" else "INFO",
                        state_before=previous.get("status", "UNSEEN"),
                        state_after=state.get("status", "UNKNOWN"),
                        reason=state.get("status_reason", ""),
                        changed_fields=changed,
                        tradingsymbol=state.get("tradingsymbol", ""),
                    )
                _flush_observability_metrics(runtime)
        except Exception:
            # The latest canonical order state below is always the authority.
            pass


def _advance_g_paper_state(state: dict[str, Any], price: float | None,
                           now: datetime) -> dict[str, Any]:
    """Serialize both side workers' capital admission and state persistence."""
    from fno_v13_v10_g_paper import paper_portfolio_lock, enforce_fill_capacity
    day = date.fromisoformat(str(state["session_date"]))
    root = order_day_dir(day, "PAPER")
    path = _order_path(day, "PAPER", str(state["signal_id"]))
    with paper_portfolio_lock(root):
        previous = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else dict(state)
        if (not isinstance(previous, dict) or previous.get("signal_id") != state["signal_id"]
                or previous.get("strategy_fingerprint") != config.strategy_fingerprint()):
            raise ValueError("G paper state identity changed while acquiring portfolio lock")
        proposed = advance_paper_order(dict(previous), price, now)
        result = enforce_fill_capacity(previous, proposed, root)
        _write_order_state(result)
        return result


def _render_worker_report(session_date: date, side: str, mode: str) -> str:
    states = load_order_states(session_date, mode=mode, side=side)
    notice, issue = _pipeline_notice_lines(session_date)
    lines = [
        f"# FnO {DISPLAY_LABEL} {side} Entry Session",
        "",
        f"Session: {session_date.isoformat()}",
        f"Execution mode: **{mode}**",
        f"Capital per entry: Rs {config.CAPITAL_PER_ENTRY_RS:,.0f}",
        f"Target leverage/exposure: {config.LEVERAGE:.1f}x / Rs {config.TARGET_EXPOSURE_RS:,.0f}",
        "PAPER uses quote-observed fills. LIVE requires exact acknowledgement plus a same-day arm file.",
        ("G pending entries expire 10 minutes after confirmation; confirmation data must publish within 90 seconds."
         if is_g_config(config) else
         f"First-time entries are blocked after the {config.ENTRY_ACTIVATION_GRACE_SEC}s activation deadline."),
        "",
        *notice,
        "Confirmation | Symbol | Status | Qty | Trigger | Entry | Stop | Target | Last/Exit | Net Rs | Reason",
        "--- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---",
    ]
    for state in states:
        lines.append(
            f"{state['confirmation_end']} | {state['tradingsymbol']} | {state['status']} | "
            f"{state['quantity']} | {float(state['trigger_price']):.2f} | "
            f"{float(state.get('entry_price', 0)):.2f} | {float(state['stop_price']):.2f} | "
            f"{float(state['target_price']):.2f} | "
            f"{float(state.get('exit_price') or state.get('last_price') or 0):.2f} | "
            f"{float(state.get('net_pnl_rs', 0)):+.2f} | {_md(state.get('status_reason', ''))}"
        )
    if not states:
        label = "(no entries: upstream pipeline blocked)" if issue else "(waiting)"
        lines.append(f"{label} | | | | | | | | | |")
    return "\n".join(lines) + "\n"


def _state_rows(states: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "strategy_version",
        "strategy_fingerprint",
        "signal_id",
        "session_date",
        "signal_end",
        "confirmation_end",
        "entry_activation_deadline_ist",
        "side",
        "tradingsymbol",
        "mode",
        "status",
        "status_reason",
        "quantity",
        "capital_rs",
        "leverage",
        "target_exposure_rs",
        "trigger_price",
        "entry_price",
        "entry_at_ist",
        "entry_order_activated_at_ist",
        "stop_price",
        "target_price",
        "last_price",
        "exit_price",
        "exit_at_ist",
        "exit_reason",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "net_return_exposure_pct",
        "return_on_capital_pct",
        "updated_at_ist",
    ]
    frame = pd.DataFrame(states)
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, columns].sort_values(
        ["mode", "confirmation_end", "side", "tradingsymbol"], kind="stable"
    )


def render_trade_log(session_date: date) -> str:
    states = load_order_states(session_date)
    frame = _state_rows(states)
    notice, issue = _pipeline_notice_lines(session_date)
    common.atomic_write_csv(frame, consolidated_csv_path(session_date))
    lines = [
        f"# FnO {DISPLAY_LABEL} Continuous Trade Log",
        "",
        f"Session: {session_date.isoformat()}",
        f"Updated: {_iso_now()}",
        f"Rows: {len(frame)}",
        f"CSV: `{consolidated_csv_path(session_date)}`",
        "",
        *notice,
        "Mode | Entry | Side | Symbol | Status | Qty | Entry price | Exit/Last | Net Rs | Updated",
        "--- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---",
    ]
    for row in frame.to_dict("records"):
        lines.append(
            f"{row['mode']} | {row['confirmation_end']} | {row['side']} | "
            f"{row['tradingsymbol']} | {row['status']} | {row['quantity']} | "
            f"{_safe_float(row['entry_price']):.2f} | "
            f"{_safe_float(row['exit_price'] or row['last_price']):.2f} | "
            f"{_safe_float(row['net_pnl_rs']):+.2f} | {_md(row['updated_at_ist'])}"
        )
    if frame.empty:
        label = "(no trades: upstream pipeline blocked)" if issue else "(waiting)"
        lines.append(f"{label} | | | | | | | | |")
    return "\n".join(lines) + "\n"


def net_summary(states: list[dict[str, Any]]) -> dict[str, Any]:
    filled = [state for state in states if float(state.get("entry_price", 0)) > 0]
    realized = sum(
        float(state.get("net_pnl_rs", 0))
        for state in filled
        if state.get("status") == "CLOSED"
    )
    unrealized = sum(
        float(state.get("net_pnl_rs", 0))
        for state in filled
        if state.get("status") == "OPEN"
    )
    capital = sum(float(state.get("capital_rs", 0)) for state in filled)
    return {
        "signals": len(states),
        "pending": sum(state.get("status") == "PENDING_ENTRY" for state in states),
        "open": sum(state.get("status") == "OPEN" for state in states),
        "closed": sum(state.get("status") == "CLOSED" for state in states),
        "no_fill": sum(state.get("status") == "NO_FILL" for state in states),
        "cancelled": sum(state.get("status") == "CANCELLED" for state in states),
        "blocked": sum(str(state.get("status", "")).startswith("BLOCKED") for state in states),
        "capital_deployed_rs": capital,
        "realized_net_rs": realized,
        "unrealized_net_rs": unrealized,
        "total_net_rs": realized + unrealized,
        "return_on_capital_pct": (
            (realized + unrealized) / capital * 100.0 if capital else 0.0
        ),
    }


def render_net_result(session_date: date) -> str:
    states = load_order_states(session_date)
    modes = sorted({str(state.get("mode", "")) for state in states}) or ["PAPER"]
    notice, _ = _pipeline_notice_lines(session_date)
    lines = [
        f"# FnO {DISPLAY_LABEL} Net Result",
        "",
        f"Session: {session_date.isoformat()}",
        f"Updated: {_iso_now()}",
        "Net includes the backtest-aligned 5 bps round-trip cost estimate.",
        "",
        *notice,
        "Mode | Signals | Pending | Open | Closed | No fill | Cancelled | Blocked | Capital Rs | Realized Rs | Unrealized Rs | Total net Rs | ROC %",
        "--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
    ]
    for mode in modes:
        summary = net_summary(
            [state for state in states if str(state.get("mode", "")) == mode]
        )
        lines.append(
            f"{mode} | {summary['signals']} | {summary['pending']} | {summary['open']} | "
            f"{summary['closed']} | {summary['no_fill']} | {summary['cancelled']} | "
            f"{summary['blocked']} | "
            f"{summary['capital_deployed_rs']:.2f} | {summary['realized_net_rs']:+.2f} | "
            f"{summary['unrealized_net_rs']:+.2f} | {summary['total_net_rs']:+.2f} | "
            f"{summary['return_on_capital_pct']:+.3f}"
        )
    return "\n".join(lines) + "\n"


def _publish(role: str, state: str, **extra: Any) -> None:
    common.publish_status(
        ROLE_SESSIONS[role],
        state,
        worker_pid=os.getpid(),
        role=role,
        strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(),
        run_id=RUN_ID,
        **extra,
    )
    _record_worker_observability(role, state, extra, heartbeat=False)


def _record_worker_observability(
    role: str,
    state: str,
    extra: dict[str, Any],
    *,
    heartbeat: bool,
) -> None:
    """Record worker health without allowing telemetry to escape to callers."""

    try:
        runtime = _observability_runtime()
        if runtime is None:
            return
        mode = str(
            extra.get("execution_mode")
            or os.getenv(f"FNO_{LIVE_LABEL}_EXECUTION_MODE", "PAPER")
        ).lower()
        runtime.standard_metrics.heartbeat_age_seconds.set(
            0, service=ROLE_SESSIONS[role], mode=mode
        )
        if not heartbeat:
            strategy = getattr(config, "STRATEGY_PROFILE", "V13_V10_G").lower()
            duration = _safe_float(extra.get("stage_duration_sec"), np.nan)
            if math.isfinite(duration) and duration >= 0:
                runtime.standard_metrics.stage_duration_seconds.observe(
                    duration, strategy=strategy, mode=mode, stage=role
                )
            deadline_lag = _safe_float(extra.get("deadline_lag_sec"), np.nan)
            if math.isfinite(deadline_lag):
                runtime.standard_metrics.slot_deadline_lag_seconds.observe(
                    deadline_lag, pipeline=role, mode=mode
                )
            for item in extra.get("signal_funnel", []) or []:
                if not isinstance(item, dict):
                    continue
                count = max(0, _safe_int(item.get("count"), 0))
                if count:
                    runtime.standard_metrics.signal_events_total.inc(
                        count,
                        strategy=strategy,
                        mode=mode,
                        event=str(item.get("event", "unknown"))[:64],
                        outcome=str(item.get("outcome", "unknown"))[:64],
                    )
            for item in extra.get("raw_anomalies", []) or []:
                if not isinstance(item, dict):
                    continue
                count = max(0, _safe_int(item.get("count"), 0))
                if count:
                    runtime.standard_metrics.raw_data_anomaly_total.inc(
                        count,
                        source=str(item.get("source", "unknown"))[:64],
                        anomaly_type=str(item.get("anomaly_type", "unknown"))[:64],
                    )
        if not heartbeat or state not in {"RUNNING", "DONE"}:
            with runtime.bind(
                profile=getattr(config, "STRATEGY_PROFILE", "V13_V10_G"),
                strategy_version=config.STRATEGY_VERSION,
                strategy_fingerprint=config.strategy_fingerprint(),
                mode=mode,
                run_id=RUN_ID,
                session_date=str(
                    extra.get("session_date") or common.now_ist().date()
                ),
            ):
                runtime.event(
                    "worker.heartbeat.state" if heartbeat else "worker.status",
                    severity=(
                        "ERROR"
                        if state in {"FAILED", "BLOCKED"}
                        else "WARNING"
                        if state in {"WAITING", "DEGRADED"}
                        else "INFO"
                    ),
                    role=role,
                    state=state,
                    fields=extra,
                )
        pending_duplicates: list[dict[str, Any]] = []
        with _OBSERVABILITY_EVENT_LOCK:
            if _PENDING_DUPLICATE_ORDER_EVENTS:
                pending_duplicates = list(_PENDING_DUPLICATE_ORDER_EVENTS)
                _PENDING_DUPLICATE_ORDER_EVENTS.clear()
        for duplicate in pending_duplicates:
            runtime.event(
                "order.duplicate.detected",
                severity="ERROR",
                durable=True,
                **duplicate,
            )
        _flush_observability_metrics(runtime)
    except Exception:
        return


def _heartbeat(role: str, state: str, **extra: Any) -> None:
    common.publish_heartbeat(
        ROLE_SESSIONS[role],
        state,
        worker_pid=os.getpid(),
        role=role,
        strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(),
        run_id=RUN_ID,
        **extra,
    )
    _record_worker_observability(role, state, extra, heartbeat=True)


def _continuous_state(now: datetime, session_date: date, once: bool) -> str:
    if once or now.date() != session_date or now.time() >= SESSION_END:
        return "DONE"
    return "RUNNING"


def run_scanner(args: argparse.Namespace, session_date: date) -> int:
    common.atomic_write_text(
        report_path("scanner-5m"), _render_scanner_report(session_date)
    )
    while True:
        try:
            universe = _load_universe(session_date)
            break
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            _heartbeat(
                "scanner-5m",
                "WAITING",
                phase="WAIT_UNIVERSE",
                reason=f"{type(exc).__name__}: {exc}",
            )
            if args.once:
                _publish(
                    "scanner-5m",
                    "WAITING",
                    phase="WAIT_UNIVERSE",
                    reason=f"{type(exc).__name__}: {exc}",
                )
                return 2
            if (
                common.now_ist().date() == session_date
                and common.now_ist().time() >= PIPELINE_DEADLINE
            ):
                _publish(
                    "scanner-5m",
                    "BLOCKED",
                    phase="UNIVERSE_NOT_READY",
                    reason=f"{type(exc).__name__}: {exc}",
                )
                common.atomic_write_text(
                    report_path("scanner-5m"), _render_scanner_report(session_date)
                )
                return 2
            time.sleep(args.poll_sec)
    processed = {
        signal_end
        for signal_end in config.SIGNAL_TO_CONFIRMATION
        if _current_slot_snapshot(
            scanner_slot_path(session_date, signal_end), session_date, signal_end
        )
    }
    while len(processed) < len(config.SIGNAL_TO_CONFIRMATION):
        now = common.now_ist()
        made_progress = False
        for signal_end in config.SIGNAL_TO_CONFIRMATION:
            if signal_end in processed:
                continue
            due = config.slot_datetime(session_date, signal_end) + timedelta(
                seconds=args.boundary_buffer_sec
            )
            if now < due and not args.once:
                continue
            ready, reason = _slot_marker_ready(session_date, signal_end, universe)
            if not ready and not args.ignore_fetch_marker:
                _heartbeat(
                    "scanner-5m",
                    "WAITING",
                    phase="WAIT_FETCH",
                    slot=signal_end,
                    last_completed_slot=max(processed) if processed else "",
                    processed_slots=len(processed),
                    reason=reason,
                )
                if args.once:
                    _publish(
                        "scanner-5m",
                        "WAITING",
                        phase="WAIT_FETCH",
                        slot=signal_end,
                        reason=reason,
                    )
                    return 2
                continue
            verified_skips = (
                _verified_no_candle_symbols_for_slot(
                    session_date, signal_end, universe
                )
                if ready
                else set()
            )
            stage_started = time.perf_counter()
            snapshot = scan_five_minute_slot(
                universe,
                session_date,
                signal_end,
                verified_no_candle_symbols=verified_skips,
            )
            _write_scanner_snapshot(session_date, signal_end, snapshot)
            completed_at = common.now_ist()
            raw_anomalies = [
                {
                    "source": str(report.get("source", "unknown")),
                    "anomaly_type": str(issue.get("code", "unknown")).lower(),
                    "count": max(1, _safe_int(issue.get("count"), 1)),
                }
                for report in snapshot.get("raw_data_quality", []) or []
                if isinstance(report, dict)
                for issue in report.get("issues", []) or []
                if isinstance(issue, dict)
            ]
            evaluated = _safe_int(snapshot.get("contracts_evaluated"), 0)
            candidates = _safe_int(snapshot.get("long_candidates"), 0) + _safe_int(
                snapshot.get("short_candidates"), 0
            )
            processed.add(signal_end)
            made_progress = True
            common.atomic_write_text(report_path("scanner-5m"), _render_scanner_report(session_date))
            _publish(
                "scanner-5m",
                snapshot["state"],
                phase="SLOT_DONE",
                slot=signal_end,
                long_candidates=snapshot["long_candidates"],
                short_candidates=snapshot["short_candidates"],
                stage_duration_sec=time.perf_counter() - stage_started,
                deadline_lag_sec=(completed_at - due).total_seconds(),
                signal_funnel=[
                    {"event": "base_gate", "outcome": "passed", "count": candidates},
                    {
                        "event": "base_gate",
                        "outcome": "rejected",
                        "count": max(0, evaluated - candidates),
                    },
                ],
                raw_anomalies=raw_anomalies,
            )
            if args.once:
                return 0
        if not made_progress:
            if now.date() == session_date and now.time() >= PIPELINE_DEADLINE:
                _publish(
                    "scanner-5m",
                    "BLOCKED",
                    phase="INCOMPLETE_BY_DEADLINE",
                    processed_slots=len(processed),
                )
                common.atomic_write_text(
                    report_path("scanner-5m"), _render_scanner_report(session_date)
                )
                return 2
            remaining = [
                slot for slot in config.SIGNAL_TO_CONFIRMATION if slot not in processed
            ]
            _heartbeat(
                "scanner-5m",
                "WAITING",
                phase="WAIT_NEXT_SLOT",
                next_slot=remaining[0] if remaining else "",
                last_completed_slot=max(processed) if processed else "",
                processed_slots=len(processed),
            )
            time.sleep(args.poll_sec)
    _publish(
        "scanner-5m",
        "DONE",
        phase=f"ALL_{LIVE_LABEL}_WINDOWS_DONE",
        processed_slots=len(processed),
    )
    return 0


def run_confirmation(args: argparse.Namespace, session_date: date) -> int:
    processed = {
        signal_end
        for signal_end in config.SIGNAL_TO_CONFIRMATION
        if _current_slot_snapshot(
            confirmation_slot_path(session_date, signal_end), session_date, signal_end
        )
    }
    common.atomic_write_text(
        report_path("confirmation-1m"),
        _render_confirmation_report(session_date),
    )
    while len(processed) < len(config.SIGNAL_TO_CONFIRMATION):
        now = common.now_ist()
        made_progress = False
        for signal_end, confirmation_end in config.SIGNAL_TO_CONFIRMATION.items():
            if signal_end in processed:
                continue
            due = config.slot_datetime(session_date, confirmation_end) + timedelta(
                seconds=args.boundary_buffer_sec
            )
            if now < due:
                _heartbeat(
                    "confirmation-1m",
                    "WAITING",
                    phase="WAIT_COMPLETED_CANDLE_BOUNDARY",
                    slot=signal_end,
                    due_ist=due.isoformat(),
                )
                if args.once:
                    _publish(
                        "confirmation-1m",
                        "WAITING",
                        phase="WAIT_COMPLETED_CANDLE_BOUNDARY",
                        slot=signal_end,
                        due_ist=due.isoformat(),
                    )
                    return 2
                continue
            source_path = scanner_slot_path(session_date, signal_end)
            source = _read_json(source_path)
            if not source:
                _heartbeat(
                    "confirmation-1m",
                    "WAITING",
                    phase="WAIT_5M_SCANNER",
                    slot=signal_end,
                )
                if args.once:
                    _publish(
                        "confirmation-1m",
                        "WAITING",
                        phase="WAIT_5M_SCANNER",
                        slot=signal_end,
                    )
                    return 2
                continue
            max_wait = config.slot_datetime(session_date, confirmation_end) + timedelta(
                seconds=args.confirmation_max_wait_sec
            )
            if now > max_wait:
                snapshot = _blocked_stale_confirmation(
                    source,
                    session_date,
                    signal_end,
                    "Confirmation was not processed inside the live activation window.",
                )
                _write_confirmation_snapshot(session_date, signal_end, snapshot)
                processed.add(signal_end)
                made_progress = True
                common.atomic_write_text(
                    report_path("confirmation-1m"),
                    _render_confirmation_report(session_date),
                )
                _publish(
                    "confirmation-1m",
                    snapshot["state"],
                    phase="STALE_SLOT_BLOCKED",
                    slot=signal_end,
                )
                continue
            stage_started = time.perf_counter()
            snapshot = process_confirmation_slot(
                source, session_date, signal_end, None, args
            )
            selected_signals = list(snapshot.pop("_selected_signals", []))
            completed_at = common.now_ist()
            if completed_at > max_wait:
                stale_ids = list(snapshot.get("selected_signal_ids") or [])
                snapshot = _blocked_stale_confirmation(
                    source,
                    session_date,
                    signal_end,
                    "Confirmation completed after the live activation window.",
                )
                snapshot["stale_discarded_signal_ids"] = stale_ids
            if (
                snapshot["state"] != "SUCCESS"
                and source.get("state") != "PARTIAL"
                and completed_at <= max_wait
            ):
                _heartbeat(
                    "confirmation-1m",
                    "WAITING",
                    phase="WAIT_CONFIRM_BAR",
                    slot=signal_end,
                    errors=snapshot["error_count"],
                )
                if args.once:
                    _publish(
                        "confirmation-1m",
                        "WAITING",
                        phase="WAIT_CONFIRM_BAR",
                        slot=signal_end,
                        errors=snapshot["error_count"],
                    )
                    return 2
                time.sleep(args.poll_sec)
                continue
            _commit_confirmation_decision(
                session_date, signal_end, snapshot, selected_signals
            )
            processed.add(signal_end)
            made_progress = True
            common.atomic_write_text(
                report_path("confirmation-1m"),
                _render_confirmation_report(session_date),
            )
            _publish(
                "confirmation-1m",
                snapshot["state"],
                phase="SLOT_DONE",
                slot=signal_end,
                selected_long=snapshot["selected_long"],
                selected_short=snapshot["selected_short"],
                stage_duration_sec=time.perf_counter() - stage_started,
                deadline_lag_sec=(completed_at - due).total_seconds(),
                signal_funnel=[
                    {
                        "event": "confirmation_gate",
                        "outcome": "passed",
                        "count": _safe_int(snapshot.get("directional_confirmed"), 0),
                    },
                    {
                        "event": "confirmation_gate",
                        "outcome": "rejected",
                        "count": max(
                            0,
                            _safe_int(snapshot.get("candidate_count"), 0)
                            - _safe_int(snapshot.get("directional_confirmed"), 0),
                        ),
                    },
                    {
                        "event": "selection",
                        "outcome": "selected",
                        "count": _safe_int(snapshot.get("selected_long"), 0)
                        + _safe_int(snapshot.get("selected_short"), 0),
                    },
                ],
            )
            if args.once:
                return 0 if snapshot["state"] == "SUCCESS" else 2
        if not made_progress:
            if now.date() == session_date and now.time() >= PIPELINE_DEADLINE:
                _publish(
                    "confirmation-1m",
                    "BLOCKED",
                    phase="INCOMPLETE_BY_DEADLINE",
                    processed_slots=len(processed),
                )
                common.atomic_write_text(
                    report_path("confirmation-1m"),
                    _render_confirmation_report(session_date),
                )
                return 2
            remaining = [
                slot for slot in config.SIGNAL_TO_CONFIRMATION if slot not in processed
            ]
            _heartbeat(
                "confirmation-1m",
                "WAITING",
                phase="WAIT_NEXT_SLOT",
                next_slot=remaining[0] if remaining else "",
                last_completed_slot=max(processed) if processed else "",
                processed_slots=len(processed),
            )
            time.sleep(args.poll_sec)
    _publish(
        "confirmation-1m",
        "DONE",
        phase=f"ALL_{LIVE_LABEL}_WINDOWS_DONE",
        processed_slots=len(processed),
    )
    return 0


def run_worker(
    args: argparse.Namespace,
    session_date: date,
    side: str,
) -> int:
    live_quantity = getattr(args, "live_quantity", None)
    role = "long-entry" if side == "LONG" else "short-entry"
    mode = args.execution_mode.upper()
    pool: KitePool | None = None
    while True:
        now = common.now_ist()
        signals = load_signals(session_date, side)
        issue = _blocking_pipeline_issue(session_date)
        if not signals and issue:
            common.atomic_write_text(
                report_path(role), _render_worker_report(session_date, side, mode)
            )
            _publish_upstream_block(role, issue)
            return 2
        states: list[dict[str, Any]] = []
        for signal in signals:
            path = _order_path(session_date, mode, str(signal["signal_id"]))
            existing_state = _read_json(path)
            state = existing_state or create_order_state(
                signal,
                mode,
                live_quantity=live_quantity,
            )
            state.setdefault("origin_run_id", state.get("run_id", ""))
            state["last_managed_run_id"] = RUN_ID
            _validate_order_state(
                state,
                signal,
                mode,
                live_quantity=live_quantity,
            )
            if not existing_state and mode == "PAPER" and state["status"] == "PENDING_ENTRY":
                activation_deadline = datetime.fromisoformat(
                    str(state["entry_activation_deadline_ist"])
                )
                if now > activation_deadline:
                    state.update(
                        status="CANCELLED",
                        status_reason="LATE_START_NO_RETROACTIVE_ENTRY",
                        updated_at_ist=now.isoformat(timespec="seconds"),
                    )
                else:
                    state["entry_order_activated_at_ist"] = now.isoformat(
                        timespec="seconds"
                    )
            states.append(state)
        if mode == "PAPER" and is_g_config(config):
            # Persist pending expiries even if authentication or quotes fail next.
            for state in states:
                result = _advance_g_paper_state(state, None, now)
                state.clear()
                state.update(result)
        active = [state for state in states if state.get("status") not in TERMINAL_STATES]
        if active and pool is None:
            pool = KitePool(args.max_apps, args.timeout_sec)
        prices: dict[str, float] = {}
        quote_app = ""
        quote_failures: list[dict[str, str]] = []
        iteration_errors: list[dict[str, str]] = []
        quote_states = (
            active
            if mode == "PAPER"
            else [
                state
                for state in active
                if state.get("status") == "PENDING_ENTRY"
                and not state.get("entry_order_id")
            ]
        )
        if quote_states and pool is not None:
            try:
                prices, quote_app, quote_failures = pool.quote_prices(
                    [str(state["tradingsymbol"]) for state in quote_states],
                )
            except Exception as exc:
                quote_phase = (
                    "QUOTE_FAILED" if mode == "PAPER" else "ENTRY_QUOTE_FAILED"
                )
                iteration_errors.append(
                    {
                        "phase": quote_phase,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
                _heartbeat(
                    role,
                    "DEGRADED",
                    phase=quote_phase,
                    error=f"{type(exc).__name__}: {exc}",
                )
        for state in states:
            # Terminal evidence is immutable.  In particular, a coordinator
            # restart with no active states must not replace the original
            # cancellation/expiry reason with a generic client-unavailable
            # error merely because no KitePool was constructed.
            if mode == "LIVE" and state.get("status") in TERMINAL_STATES:
                continue
            try:
                if mode == "PAPER":
                    price = prices.get(str(state["tradingsymbol"]))
                    if is_g_config(config):
                        result = _advance_g_paper_state(state, price, now)
                        state.clear()
                        state.update(result)
                    elif price is not None:
                        state = advance_paper_order(state, price, now)
                else:
                    if pool is None and state.get("status") not in TERMINAL_STATES:
                        raise RuntimeError("Kite client unavailable for LIVE mode.")
                    runtime = _observability_runtime()
                    if runtime is None:
                        state = advance_live_order(
                            state,
                            pool,
                            now,
                            last_price=prices.get(str(state["tradingsymbol"])),
                        )
                    else:
                        with runtime.bind(
                            profile=getattr(
                                config, "STRATEGY_PROFILE", "V13_V10_G"
                            ),
                            strategy_version=str(
                                state.get("strategy_version", config.STRATEGY_VERSION)
                            ),
                            strategy_fingerprint=str(
                                state.get(
                                    "strategy_fingerprint",
                                    config.strategy_fingerprint(),
                                )
                            ),
                            mode="LIVE",
                            asset="EQUITY",
                            session_date=session_date.isoformat(),
                            run_id=RUN_ID,
                            signal_id=str(state.get("signal_id", "")),
                            order_id=str(state.get("entry_order_id", "")),
                        ):
                            state = advance_live_order(
                                state,
                                pool,
                                now,
                                last_price=prices.get(str(state["tradingsymbol"])),
                            )
            except Exception as exc:
                _record_live_execution_error(state, exc, now)
                iteration_errors.append(
                    {
                        "phase": "ORDER_ADVANCE_FAILED",
                        "signal_id": str(state.get("signal_id", "")),
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
                _write_order_state(state)
            if mode != "PAPER" or not is_g_config(config):
                _write_order_state(state)
        common.atomic_write_text(report_path(role), _render_worker_report(session_date, side, mode))
        counts = {name: sum(state.get("status") == name for state in states) for name in (
            "PENDING_ENTRY", "OPEN", "CLOSED", "NO_FILL", "BLOCKED_SIZING"
        )}
        publish_state = (
            "DEGRADED"
            if iteration_errors
            else _continuous_state(now, session_date, args.once)
        )
        _publish(
            role,
            publish_state,
            execution_mode=mode,
            signals=len(signals),
            error_count=len(iteration_errors),
            errors=iteration_errors[:10],
            quote_app=quote_app,
            quote_failover_count=len(quote_failures),
            quote_failures=quote_failures[:10],
            broker_operation=pool.last_operation if pool is not None else "",
            broker_app=pool.last_operation_app if pool is not None else "",
            broker_failover_count=(
                len(pool.last_operation_failures) if pool is not None else 0
            ),
            broker_failures=(
                pool.last_operation_failures[:10] if pool is not None else []
            ),
            credential_reload_count=(
                pool.credential_reload_count if pool is not None else 0
            ),
            **{key.lower(): value for key, value in counts.items()},
        )
        if args.once or now.time() >= SESSION_END:
            return 2 if iteration_errors else 0
        time.sleep(args.poll_sec)


def run_trade_logger(args: argparse.Namespace, session_date: date) -> int:
    while True:
        now = common.now_ist()
        states = load_order_states(session_date)
        common.atomic_write_text(report_path("trade-logger"), render_trade_log(session_date))
        issue = _blocking_pipeline_issue(session_date)
        if not states and issue:
            _publish_upstream_block("trade-logger", issue)
            return 2
        _publish(
            "trade-logger",
            _continuous_state(now, session_date, args.once),
            rows=len(states),
            closed=sum(state.get("status") == "CLOSED" for state in states),
            output=consolidated_csv_path(session_date),
        )
        if args.once or now.time() >= SESSION_END:
            return 0
        time.sleep(args.reporting_poll_sec)


def _publish_broker_position_reconciliation(
    states: list[dict[str, Any]], client: Any, session_date: date
) -> dict[str, Any]:
    """Publish scoped position and active-order truth using broker reads only."""

    positions_payload = _observe_broker_call("positions", client.positions)
    if not isinstance(positions_payload, dict) or not isinstance(
        positions_payload.get("net"), list
    ):
        raise RuntimeError("Broker positions response has no complete net-position list.")
    broker_rows = positions_payload["net"]
    if any(not isinstance(row, dict) for row in broker_rows):
        raise RuntimeError("Broker net-position list contains a malformed row.")

    order_rows = _observe_broker_call("list_orders", client.orders)
    if not isinstance(order_rows, list):
        raise RuntimeError("Broker orders response is not a complete order list.")
    if any(not isinstance(row, dict) for row in order_rows):
        raise RuntimeError("Broker order list contains a malformed row.")

    tagged_order_rows = [
        row
        for row in order_rows
        if str(row.get("tag", "")).startswith(ORDER_TAG_PREFIX)
    ]
    tagged_symbols = {
        str(row.get("tradingsymbol", "")).strip().upper()
        for row in tagged_order_rows
        if str(row.get("tradingsymbol", "")).strip()
    }
    local_states = [
        state
        for state in states
        if str(state.get("mode", "")).upper() == "LIVE"
    ]
    local_symbols = {
        str(state.get("tradingsymbol", "")).strip().upper()
        for state in local_states
        if str(state.get("tradingsymbol", "")).strip()
    }
    attributed_symbols = local_symbols | tagged_symbols
    expected: dict[str, int] = {symbol: 0 for symbol in attributed_symbols}
    for state in local_states:
        if str(state.get("status", "")).upper() not in {"OPEN", "SQUARE_OFF_PENDING"}:
            continue
        symbol = str(state.get("tradingsymbol", "")).strip().upper()
        quantity = max(0, _safe_int(state.get("quantity"), 0)
                       - _safe_int(state.get("staged_partial_exit_quantity"), 0))
        expected[symbol] = expected.get(symbol, 0) + (
            quantity if str(state.get("side", "")).upper() == "LONG" else -quantity
        )

    observed: dict[str, int] = {}
    unscoped: list[dict[str, Any]] = []
    for row in broker_rows:
        if str(row.get("exchange", "")).upper() != "NSE" or str(
            row.get("product", "")
        ).upper() != "MIS":
            continue
        symbol = str(row.get("tradingsymbol", "")).strip().upper()
        quantity = _safe_int(row.get("quantity"), 0)
        if not symbol or quantity == 0:
            continue
        if symbol not in attributed_symbols:
            unscoped.append({"tradingsymbol": symbol, "quantity": quantity})
            continue
        observed[symbol] = quantity

    mismatches = [
        {
            "tradingsymbol": symbol,
            "local_expected_quantity": expected.get(symbol, 0),
            "broker_quantity": observed.get(symbol, 0),
        }
        for symbol in sorted(attributed_symbols)
        if expected.get(symbol, 0) != observed.get(symbol, 0)
    ]

    active_order_mismatches: list[dict[str, Any]] = []
    expected_active_orders: dict[str, dict[str, Any]] = {}
    expected_roles: dict[str, tuple[str, ...]] = {
        "OPEN": ("stop", "target"),
        "SQUARE_OFF_PENDING": ("squareoff",),
    }
    role_fields = {
        "entry": "entry_order_id",
        "stop": "stop_order_id",
        "target": "target_order_id",
        "squareoff": "squareoff_order_id",
    }
    role_order_types = {
        "entry": "SL-M",
        "stop": "SL-M",
        "target": "LIMIT",
        "squareoff": "MARKET",
    }
    known_local_statuses = TERMINAL_STATES | {
        "PENDING_ENTRY",
        "OPEN",
        "SQUARE_OFF_PENDING",
    }
    for state in local_states:
        status = str(state.get("status", "")).strip().upper()
        signal_id = str(state.get("signal_id", "")).strip()
        symbol = str(state.get("tradingsymbol", "")).strip().upper()
        side = str(state.get("side", "")).strip().upper()
        if status not in known_local_statuses:
            active_order_mismatches.append(
                {
                    "kind": "local_order_state_unclassified",
                    "signal_id": signal_id,
                    "tradingsymbol": symbol,
                    "local_status": status,
                }
            )
            continue

        roles = expected_roles.get(status, ())
        if status == "OPEN" and state.get("stop_exit_market_submitted"):
            roles = ("stop",)
        if status == "PENDING_ENTRY" and str(state.get("entry_order_id", "")).strip():
            roles = ("entry",)
        for role in roles:
            order_id = str(state.get(role_fields[role], "")).strip()
            if not order_id:
                active_order_mismatches.append(
                    {
                        "kind": "local_expected_active_order_id_missing",
                        "signal_id": signal_id,
                        "tradingsymbol": symbol,
                        "local_status": status,
                        "order_role": role,
                    }
                )
                continue
            exit_transaction = "SELL" if side == "LONG" else "BUY"
            expected_row = {
                "order_id": order_id,
                "signal_id": signal_id,
                "tradingsymbol": symbol,
                "local_status": status,
                "order_role": role,
                "tag": _live_order_tag(signal_id, role),
                "legacy_tag": _live_tag(signal_id),
                "exchange": str(state.get("exchange", "NSE")).strip().upper(),
                "product": "MIS",
                "transaction_type": (
                    ("BUY" if side == "LONG" else "SELL")
                    if role == "entry"
                    else exit_transaction
                ),
                "order_type": ("MARKET" if role == "stop" and state.get("stop_exit_market_submitted")
                               else role_order_types[role]),
                "quantity": max(0, _safe_int(state.get("quantity"), 0)
                                - (_safe_int(state.get("staged_partial_exit_quantity"), 0)
                                   if role in {"stop", "squareoff"} else 0)),
            }
            previous = expected_active_orders.get(order_id)
            if previous is not None:
                active_order_mismatches.append(
                    {
                        "kind": "duplicate_local_active_order_reference",
                        "order_id": order_id,
                        "first_signal_id": previous["signal_id"],
                        "second_signal_id": signal_id,
                        "first_order_role": previous["order_role"],
                        "second_order_role": role,
                    }
                )
                continue
            expected_active_orders[order_id] = expected_row

    def broker_order_evidence(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "order_id": str(row.get("order_id", "")).strip(),
            "tag": str(row.get("tag", "")).strip(),
            "tradingsymbol": str(row.get("tradingsymbol", "")).strip().upper(),
            "status": str(row.get("status", "")).strip().upper(),
            "exchange": str(row.get("exchange", "")).strip().upper(),
            "product": str(row.get("product", "")).strip().upper(),
            "transaction_type": str(row.get("transaction_type", "")).strip().upper(),
            "order_type": str(row.get("order_type", "")).strip().upper(),
            "quantity": _safe_int(row.get("quantity"), -1),
            "filled_quantity": _safe_int(row.get("filled_quantity"), 0),
            "pending_quantity": _safe_int(row.get("pending_quantity"), 0),
        }

    all_orders_by_id: dict[str, list[dict[str, Any]]] = {}
    for row in order_rows:
        evidence = broker_order_evidence(row)
        if evidence["order_id"]:
            all_orders_by_id.setdefault(evidence["order_id"], []).append(evidence)

    broker_active_orders: dict[str, dict[str, Any]] = {}
    broker_active_tagged_order_count = 0
    for row in tagged_order_rows:
        evidence = broker_order_evidence(row)
        status = evidence["status"]
        if status in BROKER_TERMINAL_ORDER_STATUSES:
            continue
        broker_active_tagged_order_count += 1
        if status not in BROKER_ACTIVE_ORDER_STATUSES:
            active_order_mismatches.append(
                {
                    "kind": "broker_active_order_status_unknown",
                    **evidence,
                }
            )
        order_id = evidence["order_id"]
        if not order_id:
            active_order_mismatches.append(
                {
                    "kind": "broker_active_order_id_missing",
                    **evidence,
                }
            )
            continue
        if order_id in broker_active_orders:
            active_order_mismatches.append(
                {
                    "kind": "duplicate_broker_active_order_id",
                    "order_id": order_id,
                    "first": broker_active_orders[order_id],
                    "second": evidence,
                }
            )
            continue
        broker_active_orders[order_id] = evidence

    for order_id, expected_order in sorted(expected_active_orders.items()):
        observed_order = broker_active_orders.get(order_id)
        if observed_order is None:
            historical = all_orders_by_id.get(order_id, [])
            active_order_mismatches.append(
                {
                    "kind": "local_expected_active_order_missing_at_broker",
                    **expected_order,
                    "broker_observations": historical,
                }
            )
            continue
        compared_fields = (
            "tag",
            "tradingsymbol",
            "exchange",
            "product",
            "transaction_type",
            "order_type",
            "quantity",
        )
        differences = {}
        for field in compared_fields:
            if field == "tag" and observed_order[field] in {
                expected_order[field],
                expected_order["legacy_tag"],
            }:
                continue
            if field == "order_type" and _broker_order_type_matches(
                expected_order[field],
                observed_order[field],
                protected=expected_order[field] in {"MARKET", "SL-M"},
            ):
                continue
            if expected_order[field] != observed_order[field]:
                differences[field] = {
                    "local_expected": expected_order[field],
                    "broker_observed": observed_order[field],
                }
        if differences:
            active_order_mismatches.append(
                {
                    "kind": "active_order_identity_mismatch",
                    "order_id": order_id,
                    "signal_id": expected_order["signal_id"],
                    "order_role": expected_order["order_role"],
                    "differences": differences,
                }
            )

    for order_id, observed_order in sorted(broker_active_orders.items()):
        if order_id not in expected_active_orders:
            active_order_mismatches.append(
                {
                    "kind": "unexpected_broker_active_tagged_order",
                    **observed_order,
                }
            )

    reconciliation = {
        "broker_truth_available": True,
        "scope": "nse_mis_strategy_tagged_symbols_and_active_orders",
        "scope_complete": not unscoped,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
        "unscoped_nonzero_positions": unscoped,
        "local_state_count": len(local_states),
        "tagged_symbol_count": len(tagged_symbols),
        "tagged_order_count": len(tagged_order_rows),
        "local_expected_active_order_count": len(expected_active_orders),
        "local_expected_active_order_ids": sorted(expected_active_orders),
        "broker_active_tagged_order_count": broker_active_tagged_order_count,
        "broker_active_tagged_order_ids": sorted(broker_active_orders),
        "active_order_parity_complete": not active_order_mismatches,
        "active_order_mismatch_count": len(active_order_mismatches),
        "active_order_mismatches": active_order_mismatches,
    }
    report = {
        "schema_version": "v13_v10_g_broker_position_reconciliation_v2",
        "session_date": session_date.isoformat(),
        "generated_at_ist": _iso_now(),
        "run_id": RUN_ID,
        "position_reconciliation": reconciliation,
    }
    # The collector verifies this digest before a broker/local agreement can
    # become a safety gauge.  It is an integrity check for the local artifact,
    # not an authentication substitute for the broker API response itself.
    report["report_sha256"] = common.canonical_json_sha256(report)
    path = common.runtime_dir("observability", "reconciliation") / (
        f"broker_positions_{session_date.isoformat()}.json"
    )
    common.atomic_write_json(path, report)
    return reconciliation


def run_net_result(args: argparse.Namespace, session_date: date) -> int:
    broker_pool: KitePool | None = None
    next_broker_reconciliation = 0.0
    while True:
        now = common.now_ist()
        states = load_order_states(session_date)
        summary = net_summary(states)
        reconciliation: dict[str, Any] | None = None
        if (
            args.execution_mode.upper() == "LIVE"
            and time.monotonic() >= next_broker_reconciliation
        ):
            next_broker_reconciliation = time.monotonic() + float(
                getattr(args, "broker_reconcile_sec", 30.0)
            )
            try:
                broker_pool = broker_pool or KitePool(args.max_apps, args.timeout_sec)
                reconciliation = _publish_broker_position_reconciliation(
                    states, broker_pool, session_date
                )
            except Exception as exc:
                reconciliation = {
                    "broker_truth_available": False,
                    "error_type": type(exc).__name__,
                }
        common.atomic_write_text(report_path("net-result"), render_net_result(session_date))
        issue = _blocking_pipeline_issue(session_date)
        if not states and issue:
            _publish_upstream_block("net-result", issue)
            return 2
        _publish(
            "net-result",
            _continuous_state(now, session_date, args.once),
            broker_position_reconciliation=reconciliation,
            **summary,
        )
        if args.once or now.time() >= SESSION_END:
            return 0
        time.sleep(args.reporting_poll_sec)


def run_broker_reconciliation(args: argparse.Namespace, session_date: date) -> int:
    """Publish read-only, digest-verified broker position and order truth.

    This dedicated role intentionally has no dependency on scanner or
    confirmation state: broker truth is still required when the strategy has
    no local orders, or when the signal pipeline is blocked.  Its broker
    surface is limited to ``positions`` and ``orders`` by
    :func:`_publish_broker_position_reconciliation`.
    """

    if str(args.execution_mode).upper() != "LIVE":
        raise ValueError("Broker reconciliation requires LIVE mode.")

    broker_pool: KitePool | None = None
    while True:
        now = common.now_ist()
        states = load_order_states(session_date, mode="LIVE")
        try:
            broker_pool = broker_pool or KitePool(args.max_apps, args.timeout_sec)
            reconciliation = _publish_broker_position_reconciliation(
                states, broker_pool, session_date
            )
            trusted = (
                reconciliation.get("broker_truth_available") is True
                and reconciliation.get("scope_complete") is True
                and _safe_int(reconciliation.get("mismatch_count"), -1) == 0
                and reconciliation.get("active_order_parity_complete") is True
                and _safe_int(
                    reconciliation.get("active_order_mismatch_count"), -1
                )
                == 0
            )
            publish_state = (
                _continuous_state(now, session_date, args.once)
                if trusted
                else "DEGRADED"
            )
            exit_code = 0 if trusted else 2
        except Exception as exc:
            reconciliation = {
                "broker_truth_available": False,
                "error_type": type(exc).__name__,
            }
            publish_state = "DEGRADED"
            exit_code = 2
        _publish(
            "broker-reconciliation",
            publish_state,
            execution_mode="LIVE",
            session_date=session_date.isoformat(),
            broker_position_reconciliation=reconciliation,
        )
        if args.once or now.date() != session_date or now.time() >= SESSION_END:
            return exit_code
        time.sleep(float(args.broker_reconcile_sec))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=tuple(ROLE_SESSIONS))
    parser.add_argument("--session-date", default="")
    parser.add_argument("--slot", default="")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--ignore-fetch-marker", action="store_true")
    parser.add_argument(
        "--execution-mode",
        choices=("PAPER", "LIVE"),
        default=os.getenv(
            f"FNO_{LIVE_LABEL}_EXECUTION_MODE", "PAPER"
        ).upper(),
    )
    parser.add_argument("--capital", type=float, default=config.CAPITAL_PER_ENTRY_RS)
    parser.add_argument("--leverage", type=float, default=config.LEVERAGE)
    parser.add_argument(
        "--live-quantity",
        type=int,
        default=None,
        help=(
            "Explicit maximum LIVE order quantity. It never increases the "
            "strategy-sized quantity and is rejected in PAPER mode."
        ),
    )
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--reporting-poll-sec", type=float, default=5.0)
    parser.add_argument("--broker-reconcile-sec", type=float, default=30.0)
    parser.add_argument("--boundary-buffer-sec", type=float, default=3.0)
    parser.add_argument("--confirmation-max-wait-sec", type=float, default=90.0)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser


def run(args: argparse.Namespace) -> int:
    live_quantity = getattr(args, "live_quantity", None)
    if float(getattr(args, "broker_reconcile_sec", 30.0)) < 10:
        raise ValueError("--broker-reconcile-sec must be at least 10 seconds.")
    if live_quantity is not None:
        if str(args.execution_mode).upper() != "LIVE":
            raise ValueError("--live-quantity is valid only with LIVE execution mode.")
        if int(live_quantity) <= 0:
            raise ValueError("--live-quantity must be positive.")
    if LIVE_GENERATION == "v6":
        if abs(
            float(args.confirmation_max_wait_sec)
            - float(config.ENTRY_ACTIVATION_GRACE_SEC)
        ) > 1e-9:
            raise ValueError(
                "V6 confirmation max wait is fingerprint-locked to "
                f"{config.ENTRY_ACTIVATION_GRACE_SEC} seconds."
            )
        if abs(
            float(args.boundary_buffer_sec)
            - float(config.CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC)
        ) > 1e-9:
            raise ValueError(
                "V6 completed-candle boundary buffer is fingerprint-locked to "
                f"{config.CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC} seconds."
            )
        if args.ignore_fetch_marker:
            raise ValueError(
                "V6 cannot bypass fetch-marker or evidence-readiness gates."
            )
    config.validate_strategy()
    config.attest_selected_backtest()
    _write_manifest()
    role = args.role
    session_date = (
        date.fromisoformat(args.session_date)
        if args.session_date
        else common.now_ist().date()
    )
    if (
        abs(args.capital - config.CAPITAL_PER_ENTRY_RS) > 1e-9
        or abs(args.leverage - config.LEVERAGE) > 1e-9
    ):
        raise ValueError(
            f"This locked {DISPLAY_LABEL} runtime requires Rs {config.CAPITAL_PER_ENTRY_RS:,.0f} capital and "
            f"{config.LEVERAGE:g}x leverage per entry."
        )
    if not args.allow_non_trading_day and not common.is_trading_day(
        session_date, common.load_holidays()
    ):
        _publish(role, "SKIPPED_NON_TRADING_DAY", session_date_ist=session_date)
        common.atomic_write_text(
            report_path(role),
            f"# {DISPLAY_LABEL} {role}\n\n- Session date: {session_date}\n"
            "- Status: SKIPPED_NON_TRADING_DAY\n"
            "- No regular NSE session; no selection or execution expected.\n",
        )
        return 0
    if args.slot:
        normalized = args.slot.replace(":", "")
        match = next(
            (
                signal_end
                for signal_end in config.SIGNAL_TO_CONFIRMATION
                if signal_end.replace(":", "") == normalized
                or config.SIGNAL_TO_CONFIRMATION[signal_end].replace(":", "") == normalized
            ),
            None,
        )
        if match is None:
            raise ValueError(f"Unsupported {LIVE_LABEL} slot: {args.slot}")
        if role == "scanner-5m":
            args.once = True
        elif role == "confirmation-1m":
            args.once = True
    _publish(
        role,
        "RUNNING",
        phase="START",
        session_date_ist=session_date,
        execution_mode=args.execution_mode,
        capital_rs=args.capital,
        leverage=args.leverage,
    )
    if role == "scanner-5m":
        if args.slot:
            selected = match
            # Requested-slot runs replace only this generation's live snapshot.
            universe = _load_universe(session_date)
            ready, reason = _slot_marker_ready(session_date, selected, universe)
            if not ready and not args.ignore_fetch_marker:
                _publish(role, "WAITING", phase="WAIT_FETCH", slot=selected, reason=reason)
                return 2
            verified_skips = (
                _verified_no_candle_symbols_for_slot(
                    session_date, selected, universe
                )
                if ready
                else set()
            )
            snapshot = scan_five_minute_slot(
                universe,
                session_date,
                selected,
                verified_no_candle_symbols=verified_skips,
            )
            _write_scanner_snapshot(session_date, selected, snapshot)
            common.atomic_write_text(report_path(role), _render_scanner_report(session_date))
            return 0
        return run_scanner(args, session_date)
    if role == "confirmation-1m":
        if args.slot:
            selected = next(
                signal_end
                for signal_end, confirmation_end in config.SIGNAL_TO_CONFIRMATION.items()
                if signal_end.replace(":", "") == args.slot.replace(":", "")
                or confirmation_end.replace(":", "") == args.slot.replace(":", "")
            )
            confirmation_end = config.slot_datetime(
                session_date, config.SIGNAL_TO_CONFIRMATION[selected]
            )
            due = confirmation_end + timedelta(seconds=args.boundary_buffer_sec)
            now = common.now_ist()
            if now < due:
                _publish(
                    role,
                    "WAITING",
                    phase="WAIT_COMPLETED_CANDLE_BOUNDARY",
                    slot=selected,
                    due_ist=due.isoformat(),
                )
                return 2
            source = _read_json(scanner_slot_path(session_date, selected))
            if not source:
                _publish(role, "WAITING", phase="WAIT_5M_SCANNER", slot=selected)
                return 2
            max_wait = confirmation_end + timedelta(
                seconds=args.confirmation_max_wait_sec
            )
            if now > max_wait:
                snapshot = _blocked_stale_confirmation(
                    source,
                    session_date,
                    selected,
                    "Requested confirmation slot is outside the live activation window.",
                )
                _write_confirmation_snapshot(session_date, selected, snapshot)
                common.atomic_write_text(
                    report_path(role), _render_confirmation_report(session_date)
                )
                return 2
            snapshot = process_confirmation_slot(
                source, session_date, selected, None, args
            )
            selected_signals = list(snapshot.pop("_selected_signals", []))
            if common.now_ist() > max_wait:
                snapshot = _blocked_stale_confirmation(
                    source,
                    session_date,
                    selected,
                    "Confirmation completed after the live activation window.",
                )
            elif snapshot["state"] != "SUCCESS" and source.get("state") != "PARTIAL":
                _publish(
                    role,
                    "WAITING",
                    phase="WAIT_CONFIRM_BAR",
                    slot=selected,
                    errors=snapshot["error_count"],
                )
                return 2
            _commit_confirmation_decision(
                session_date, selected, snapshot, selected_signals
            )
            common.atomic_write_text(report_path(role), _render_confirmation_report(session_date))
            return 0 if snapshot["state"] == "SUCCESS" else 2
        return run_confirmation(args, session_date)
    if role == "long-entry":
        return run_worker(args, session_date, "LONG")
    if role == "short-entry":
        return run_worker(args, session_date, "SHORT")
    if role == "trade-logger":
        return run_trade_logger(args, session_date)
    if role == "net-result":
        return run_net_result(args, session_date)
    return run_broker_reconciliation(args, session_date)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        _publish(args.role, "STOPPED", phase="INTERRUPTED")
        return 0
    except Exception as exc:
        _publish(
            args.role,
            "FAILED",
            phase="FAILED",
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
