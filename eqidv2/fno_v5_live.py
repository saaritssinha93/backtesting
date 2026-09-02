"""Shared live/paper runtime for locked FNO EMA/OI strategy generations.

Six independently monitored roles share this module:

* ``scanner-5m`` waits for exact-slot final markers from both feeds, reads NSE
  equity price/volume/indicators, joins only OI fields from the mapped future,
  and emits the same loose candidate superset used by the backtest.
* ``confirmation-1m`` reads only the durable completed candidate-equity
  1-minute feed and publishes NSE-equity stop-entry signals. Broker polling is
  isolated in the independent feed producer.
* ``long-entry`` and ``short-entry`` manage one side each.
* ``trade-logger`` continuously consolidates immutable signal/order state.
* ``net-result`` continuously marks the current book and reports net results.

Scheduled runners default to PAPER.  Real broker orders require LIVE mode, an
exact acknowledgement environment variable, and a same-day arm file.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import importlib
import os
import re
import sys
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
import fno_oi_ema_confirm_backtest as backtest
import fno_oi_hybrid_data as hybrid
LIVE_GENERATION = os.getenv("FNO_LIVE_GENERATION", "v5").strip().lower()
if LIVE_GENERATION not in {"v5", "v6"}:
    raise RuntimeError(f"Unsupported FnO live generation: {LIVE_GENERATION}")
config = importlib.import_module(f"fno_{LIVE_GENERATION}_live_config")
LIVE_LABEL = LIVE_GENERATION.upper()
LIVE_SCHEMA_PREFIX = f"fno_{LIVE_GENERATION}"
EXECUTION_SESSION_NAMESPACE = os.getenv(
    f"FNO_{LIVE_LABEL}_EXECUTION_SESSION_NAMESPACE", ""
).strip().lower()
if EXECUTION_SESSION_NAMESPACE and not re.fullmatch(
    r"[a-z0-9][a-z0-9_-]{0,39}", EXECUTION_SESSION_NAMESPACE
):
    raise RuntimeError(
        "Invalid FnO execution-session namespace: "
        f"{EXECUTION_SESSION_NAMESPACE!r}"
    )


ROLE_SESSIONS = {
    "scanner-5m": f"fno_{LIVE_GENERATION}_scanner_5min",
    "confirmation-1m": f"fno_{LIVE_GENERATION}_confirmation_1min",
    "long-entry": f"fno_{LIVE_GENERATION}_live_long",
    "short-entry": f"fno_{LIVE_GENERATION}_live_short",
    "trade-logger": f"fno_{LIVE_GENERATION}_trade_logger",
    "net-result": f"fno_{LIVE_GENERATION}_net_result",
}
ROLE_REPORTS = {
    "scanner-5m": f"latest_fno_{LIVE_GENERATION}_scanner_5min.md",
    "confirmation-1m": f"latest_fno_{LIVE_GENERATION}_confirmation_1min.md",
    "long-entry": f"latest_fno_{LIVE_GENERATION}_live_long.md",
    "short-entry": f"latest_fno_{LIVE_GENERATION}_live_short.md",
    "trade-logger": f"latest_fno_{LIVE_GENERATION}_trade_logger.md",
    "net-result": f"latest_fno_{LIVE_GENERATION}_net_result.md",
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
    ):
        ROLE_SESSIONS[_role] = (
            f"fno_{LIVE_GENERATION}_{EXECUTION_SESSION_NAMESPACE}_{_suffix}"
        )
        ROLE_REPORTS[_role] = (
            f"latest_fno_{LIVE_GENERATION}_{EXECUTION_SESSION_NAMESPACE}_{_suffix}.md"
        )

LIVE_ROOT = common.FNO_ROOT / f"{LIVE_GENERATION}_live"
SCANNER_ROOT = LIVE_ROOT / "scanner_5m"
CONFIRMATION_ROOT = LIVE_ROOT / "confirmation_1m"
SIGNAL_ROOT = LIVE_ROOT / "signals"
ORDER_ROOT = LIVE_ROOT / "orders"
CONSOLIDATED_ROOT = LIVE_ROOT / "consolidated"
EVIDENCE_ROOT = LIVE_ROOT / "evidence"
STRATEGY_MANIFEST_PATH = LIVE_ROOT / "strategy_manifest.json"
LIVE_ARM_PATH = LIVE_ROOT / "live_arm.json"
KILL_SWITCH_PATH = LIVE_ROOT / "kill_switch.json"
LIVE_ACK_ENV = getattr(config, "LIVE_ACK_ENV", f"FNO_{LIVE_LABEL}_LIVE_ACK")
LIVE_ACK = getattr(
    config, "LIVE_ACK", f"I_UNDERSTAND_REAL_FNO_{LIVE_LABEL}_EQUITY_ORDERS"
)
ORDER_TAG_PREFIX = getattr(config, "ORDER_TAG_PREFIX", f"F{LIVE_LABEL}")

SESSION_END = dtime(15, 32)
PIPELINE_DEADLINE = dtime(9, 50)
TERMINAL_STATES = {
    "CLOSED",
    "NO_FILL",
    "ENTRY_REJECTED",
    "BLOCKED_SIZING",
    "CANCELLED",
}

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
        f"fno_{LIVE_GENERATION}_trades_{session_date.isoformat()}.csv"
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
    raw = (
        f"{config.STRATEGY_VERSION}|{session_date}|{setup.signal_end}|"
        f"{setup.confirmation_end}|{setup.side}|{symbol}"
    )
    digest = hashlib.sha1(raw.encode("ascii")).hexdigest()[:12]
    return (
        f"{session_date.strftime('%Y%m%d')}_{setup.confirmation_end.replace(':', '')}_"
        f"{setup.side}_{common.safe_contract_stem(symbol)}_{digest}"
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


def _base_signal_side(row: pd.Series) -> str | None:
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
        side = _base_signal_side(row)
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
        }
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
    return {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_scanner_5m_hybrid_v3",
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
        "state": (
            "SUCCESS"
            if not missing_contracts
            and invalid == 0
            and not unknown_verified_skips
            else "PARTIAL"
        ),
    }


def confirmation_metrics(
    candidate: dict[str, Any],
    bar: dict[str, Any],
) -> dict[str, Any]:
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
        setup = config.setup_for(signal_end, side)
        if setup is None:
            continue
        ranked = config.rank_candidates(directional, setup)
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


class KitePool:
    def __init__(self, max_apps: int, timeout_sec: float) -> None:
        credentials = common.discover_kite_credentials(max_apps=max_apps)
        self.clients = [
            common.make_kite_client(credential, timeout_sec=timeout_sec)
            for credential in credentials
        ]
        if not self.clients:
            raise RuntimeError("No authenticated Kite client is available.")

    @property
    def primary(self) -> Any:
        return self.clients[0]


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
        f"# FnO {LIVE_LABEL} 5-Minute Scanner",
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
    setup = config.setup_for(signal_end, side)
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
        f"# FnO {LIVE_LABEL} 1-Minute Confirmation and Entry Scanner",
        "",
        f"Session: {session_date.isoformat()}",
        f"Selected objective: {config.SELECTED_OBJECTIVE}",
        "Entry is a stop order at the confirmation candle extreme and may activate only afterward.",
        f"A first-time entry must be armed within {config.ENTRY_ACTIVATION_GRACE_SEC}s of confirmation; stale starts are blocked.",
        f"Only active {LIVE_LABEL} setup legs can publish entry signals.",
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
            symbol: "INELIGIBLE_NO_CANDLE"
            for symbol in sorted(ineligible_no_candle)
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
    return result


def _blocked_stale_confirmation(
    source: dict[str, Any],
    session_date: date,
    signal_end: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_equity_confirmation_1m_v3",
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
    return {
        "schema_version": f"{LIVE_SCHEMA_PREFIX}_equity_order_state_v2",
        "strategy_version": signal["strategy_version"],
        "strategy_fingerprint": signal["strategy_fingerprint"],
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
        "target_pct": float(signal["target_pct"]),
        "stop_price": float(signal["stop_price"]),
        "target_price": float(signal["target_price"]),
        "round_trip_cost_bps": float(signal["round_trip_cost_bps"]),
        "created_at_ist": _iso_now(),
        "updated_at_ist": _iso_now(),
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


def advance_paper_order(
    state: dict[str, Any],
    last_price: float,
    now: datetime,
) -> dict[str, Any]:
    status = str(state["status"])
    if status in TERMINAL_STATES:
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
    kill = _read_json(KILL_SWITCH_PATH)
    if bool(kill.get("enabled")):
        return False, "KILL_SWITCH_ENABLED"
    return True, "LIVE_ARMED"


def _broker_order(client: Any, order_id: str) -> dict[str, Any]:
    if not order_id:
        return {}
    history = client.order_history(order_id)
    return dict(history[-1]) if history else {}


def _broker_place(client: Any, **kwargs: Any) -> str:
    return str(client.place_order(**kwargs))


def _broker_cancel(client: Any, order_id: str) -> None:
    if not order_id:
        return
    try:
        client.cancel_order(variety="regular", order_id=order_id)
    except Exception:
        pass


def _broker_find_tagged_order(
    client: Any,
    *,
    tag: str,
    tradingsymbol: str,
    transaction_type: str,
    order_type: str,
) -> dict[str, Any]:
    matches = []
    for raw in client.orders():
        row = dict(raw)
        if str(row.get("tag", "")) != tag:
            continue
        if str(row.get("tradingsymbol", "")) != tradingsymbol:
            continue
        if str(row.get("transaction_type", "")).upper() != transaction_type.upper():
            continue
        if str(row.get("order_type", "")).upper() != order_type.upper():
            continue
        matches.append(row)
    return matches[-1] if matches else {}


def _live_tag(signal_id: str) -> str:
    tag_seed = (
        f"{EXECUTION_SESSION_NAMESPACE}:{signal_id}"
        if EXECUTION_SESSION_NAMESPACE
        else signal_id
    )
    return ORDER_TAG_PREFIX + hashlib.sha1(tag_seed.encode("ascii")).hexdigest()[:14]


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
    state.update(
        status="OPEN",
        status_reason="LIVE_ENTRY_FILLED",
        entry_price=fill_price,
        quantity=filled_quantity,
        entry_at_ist=now.isoformat(timespec="seconds"),
        stop_price=stop,
        target_price=target,
        updated_at_ist=now.isoformat(timespec="seconds"),
    )
    return True


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
        recovered = _broker_find_tagged_order(
            client,
            tag=tag,
            tradingsymbol=str(state["tradingsymbol"]),
            transaction_type=exit_transaction,
            order_type="MARKET",
        )
        if recovered:
            state["squareoff_order_id"] = str(recovered.get("order_id", ""))
    if not state.get("squareoff_order_id"):
        state["squareoff_order_id"] = _broker_place(
            client,
            variety="regular",
            exchange=str(state["exchange"]),
            tradingsymbol=state["tradingsymbol"],
            transaction_type=exit_transaction,
            quantity=int(state["quantity"]),
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


def advance_live_order(
    state: dict[str, Any],
    client: Any,
    now: datetime,
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
    tag = _live_tag(str(state["signal_id"]))

    if status == "PENDING_ENTRY":
        activation_deadline = datetime.fromisoformat(
            str(state["entry_activation_deadline_ist"])
        )
        if not state.get("entry_order_id"):
            recovered = _broker_find_tagged_order(
                client,
                tag=tag,
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=entry_transaction,
                order_type="SL-M",
            )
            if recovered:
                _validate_recovered_order_quantity(state, recovered)
                state["entry_order_id"] = str(recovered.get("order_id", ""))
                state["entry_order_activated_at_ist"] = str(
                    recovered.get("order_timestamp") or now.isoformat(timespec="seconds")
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
                    status_reason="LIVE_STOP_ENTRY_WORKING",
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
        else:
            if now > activation_deadline:
                state.update(
                    status="CANCELLED",
                    status_reason="LATE_START_NO_RETROACTIVE_ENTRY",
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            if now >= square_off:
                state.update(status="NO_FILL", status_reason="SQUARE_OFF_BEFORE_ENTRY")
                return state
            if not armed:
                state.update(
                    status_reason=arm_reason,
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
                return state
            order_id = _broker_place(
                client,
                variety="regular",
                exchange=str(state["exchange"]),
                tradingsymbol=state["tradingsymbol"],
                transaction_type=entry_transaction,
                quantity=int(state["quantity"]),
                product="MIS",
                order_type="SL-M",
                trigger_price=float(state["trigger_price"]),
                validity="DAY",
                tag=tag,
            )
            state.update(
                entry_order_id=order_id,
                entry_order_activated_at_ist=now.isoformat(timespec="seconds"),
                status_reason="LIVE_STOP_ENTRY_PLACED",
                updated_at_ist=now.isoformat(timespec="seconds"),
            )
            return state

    if state["status"] == "OPEN":
        if not state.get("squareoff_order_id"):
            recovered_market = _broker_find_tagged_order(
                client,
                tag=tag,
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=exit_transaction,
                order_type="MARKET",
            )
            if recovered_market:
                _validate_recovered_order_quantity(state, recovered_market)
                state["squareoff_order_id"] = str(recovered_market.get("order_id", ""))
                state["status"] = "SQUARE_OFF_PENDING"
        if state["status"] == "OPEN" and not state.get("stop_order_id"):
            recovered_stop = _broker_find_tagged_order(
                client,
                tag=tag,
                tradingsymbol=str(state["tradingsymbol"]),
                transaction_type=exit_transaction,
                order_type="SL-M",
            )
            if recovered_stop:
                _validate_recovered_order_quantity(state, recovered_stop)
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
                    tag=tag,
                )
            except Exception as exc:
                _begin_live_squareoff(
                    state,
                    client,
                    now,
                    exit_transaction,
                    tag,
                    f"STOP_ORDER_PLACE_FAILED:{type(exc).__name__}",
                )
        if state["status"] == "OPEN" and not state.get("target_order_id"):
            recovered_target = _broker_find_tagged_order(
                client,
                tag=tag,
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
                tag=tag,
            )
    if state["status"] == "OPEN":
        target_order = _broker_order(client, str(state["target_order_id"]))
        stop_order = _broker_order(client, str(state["stop_order_id"]))
        if str(stop_order.get("status", "")).upper() == "COMPLETE":
            _broker_cancel(client, str(state["target_order_id"]))
            return _close_state(
                state,
                _safe_float(stop_order.get("average_price"), float(state["stop_price"])),
                "STOP",
                now,
            )
        if str(target_order.get("status", "")).upper() == "COMPLETE":
            _broker_cancel(client, str(state["stop_order_id"]))
            return _close_state(
                state,
                _safe_float(target_order.get("average_price"), float(state["target_price"])),
                "TARGET",
                now,
            )
        stop_status = str(stop_order.get("status", "")).upper()
        target_status = str(target_order.get("status", "")).upper()
        if stop_status in {"REJECTED", "CANCELLED"}:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                tag,
                f"STOP_ORDER_{stop_status}",
            )
        elif target_status in {"REJECTED", "CANCELLED"}:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                tag,
                f"TARGET_ORDER_{target_status}",
            )
        elif kill_enabled:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                tag,
                "KILL_SWITCH_SQUARE_OFF",
            )
        elif now >= square_off:
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                tag,
                "LIVE_SQUARE_OFF_SENT",
            )
    if state["status"] == "SQUARE_OFF_PENDING":
        if not state.get("squareoff_order_id"):
            _begin_live_squareoff(
                state,
                client,
                now,
                exit_transaction,
                tag,
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
    payload = client.ltp(keys)
    prices: dict[str, float] = {}
    for key, row in payload.items():
        symbol = str(key).split(":", 1)[-1]
        price = _safe_float(row.get("last_price"))
        if price > 0:
            prices[symbol] = price
    return prices


def _write_order_state(state: dict[str, Any]) -> None:
    session_date = date.fromisoformat(str(state["session_date"]))
    common.atomic_write_json(
        _order_path(session_date, str(state["mode"]), str(state["signal_id"])),
        state,
    )


def _render_worker_report(session_date: date, side: str, mode: str) -> str:
    states = load_order_states(session_date, mode=mode, side=side)
    notice, issue = _pipeline_notice_lines(session_date)
    lines = [
        f"# FnO {LIVE_LABEL} {side} Entry Session",
        "",
        f"Session: {session_date.isoformat()}",
        f"Execution mode: **{mode}**",
        f"Capital per entry: Rs {config.CAPITAL_PER_ENTRY_RS:,.0f}",
        f"Target leverage/exposure: {config.LEVERAGE:.1f}x / Rs {config.TARGET_EXPOSURE_RS:,.0f}",
        "PAPER uses quote-observed fills. LIVE requires exact acknowledgement plus a same-day arm file.",
        f"First-time entries are blocked after the {config.ENTRY_ACTIVATION_GRACE_SEC}s activation deadline.",
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
        f"# FnO {LIVE_LABEL} Continuous Trade Log",
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
        f"# FnO {LIVE_LABEL} Net Result",
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
        **extra,
    )


def _heartbeat(role: str, state: str, **extra: Any) -> None:
    common.publish_heartbeat(
        ROLE_SESSIONS[role],
        state,
        worker_pid=os.getpid(),
        role=role,
        strategy_version=config.STRATEGY_VERSION,
        **extra,
    )


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
                _heartbeat("scanner-5m", "WAITING", phase="WAIT_FETCH", slot=signal_end, reason=reason)
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
            snapshot = scan_five_minute_slot(
                universe,
                session_date,
                signal_end,
                verified_no_candle_symbols=verified_skips,
            )
            _write_scanner_snapshot(session_date, signal_end, snapshot)
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
        active = [state for state in states if state.get("status") not in TERMINAL_STATES]
        if active and pool is None:
            pool = KitePool(args.max_apps, args.timeout_sec)
        prices: dict[str, float] = {}
        if active and mode == "PAPER" and pool is not None:
            try:
                prices = _quote_prices(
                    pool.primary,
                    [str(state["tradingsymbol"]) for state in active],
                )
            except Exception as exc:
                _heartbeat(role, "DEGRADED", phase="QUOTE_FAILED", error=f"{type(exc).__name__}: {exc}")
        for state in states:
            try:
                if mode == "PAPER":
                    price = prices.get(str(state["tradingsymbol"]))
                    if price is not None:
                        state = advance_paper_order(state, price, now)
                else:
                    if pool is None:
                        raise RuntimeError("Kite client unavailable for LIVE mode.")
                    state = advance_live_order(state, pool.primary, now)
            except Exception as exc:
                state.update(
                    status_reason=f"{type(exc).__name__}: {exc}",
                    updated_at_ist=now.isoformat(timespec="seconds"),
                )
            _write_order_state(state)
        common.atomic_write_text(report_path(role), _render_worker_report(session_date, side, mode))
        counts = {name: sum(state.get("status") == name for state in states) for name in (
            "PENDING_ENTRY", "OPEN", "CLOSED", "NO_FILL", "BLOCKED_SIZING"
        )}
        _publish(
            role,
            _continuous_state(now, session_date, args.once),
            execution_mode=mode,
            signals=len(signals),
            **{key.lower(): value for key, value in counts.items()},
        )
        if args.once or now.time() >= SESSION_END:
            return 0
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


def run_net_result(args: argparse.Namespace, session_date: date) -> int:
    while True:
        now = common.now_ist()
        states = load_order_states(session_date)
        summary = net_summary(states)
        common.atomic_write_text(report_path("net-result"), render_net_result(session_date))
        issue = _blocking_pipeline_issue(session_date)
        if not states and issue:
            _publish_upstream_block("net-result", issue)
            return 2
        _publish(
            "net-result",
            _continuous_state(now, session_date, args.once),
            **summary,
        )
        if args.once or now.time() >= SESSION_END:
            return 0
        time.sleep(args.reporting_poll_sec)


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
            f"This locked {LIVE_LABEL} runtime requires Rs 10,000 capital and "
            "5x leverage per entry."
        )
    if not args.allow_non_trading_day and not common.is_trading_day(
        session_date, common.load_holidays()
    ):
        _publish(role, "SKIPPED_NON_TRADING_DAY", session_date_ist=session_date)
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
    return run_net_result(args, session_date)


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
