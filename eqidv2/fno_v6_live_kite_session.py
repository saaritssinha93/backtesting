"""Isolated quantity-one LIVE session for the selected FnO strategy profile.

The selected scanner and 1-minute confirmation pipeline are the sole signal
producers.  This coordinator starts one LIVE worker per side, pins every
executable order state to one NSE equity share, and publishes dashboard CSVs
from authoritative signals and this profile's LIVE-only order directory.

Real orders remain fail-closed behind the V6 acknowledgement, same-day arm
file, kill switch, and the signal activation deadline enforced by
``fno_v5_live.py``.  This module never creates or changes any safety file.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
from fno_live_profile import config_for_generation, is_g_config


config = config_for_generation("v6")


SCRIPT_DIR = Path(__file__).resolve().parent
SESSION_ID = (
    "fno_v13_v10_g_live_kite_qty1" if is_g_config(config) else "fno_v6_live_kite_qty1"
)
EXECUTION_PROFILE = "live_kite_qty1"
EXECUTION_MODE = "LIVE"
EXECUTION_QUANTITY = 1
QUANTITY_POLICY = "FIXED_ONE_SHARE"

LIVE_ROOT = common.FNO_ROOT / getattr(config, "LIVE_ROOT_NAME", "v6_live")
# Keep the existing controls authoritative across the strategy migration.
# None preserves the legacy test/embedding contract where LIVE_ROOT is injected.
CONTROL_ROOT = (
    common.FNO_ROOT / getattr(config, "CONTROL_ROOT_NAME", "v6_live")
    if getattr(config, "LIVE_ROOT_NAME", "v6_live") != "v6_live"
    else None
)
CONFIRMATION_ROOT = LIVE_ROOT / "confirmation_1m"
SIGNAL_ROOT = LIVE_ROOT / "signals"
PROFILE_ORDER_ROOT = LIVE_ROOT / "orders" / "LIVE" / EXECUTION_PROFILE
EXPORT_ROOT = LIVE_ROOT / "live_kite"
STATUS_PATH = EXPORT_ROOT / "status.json"
HEARTBEAT_PATH = EXPORT_ROOT / "heartbeat.json"

ENTRY_COLUMNS = [
    "signal_datetime",
    "detected_time_ist",
    "ticker",
    "side",
    "entry_price",
    "target_price",
    "stop_price",
    "quantity",
    "strategy_sized_quantity",
    "status",
    "status_reason",
    "signal_end",
    "confirmation_end",
    "activation_deadline_ist",
    "rank_within_scan",
    "setup_id",
    "picker",
    "signal_id",
    "strategy_version",
    "strategy_fingerprint",
    "execution_mode",
    "execution_profile",
    "quantity_policy",
    "stop_pct",
    "target_pct",
]

TRADE_COLUMNS = [
    "ticker",
    "entry_time",
    "exit_time",
    "side",
    "outcome",
    "filled_price",
    "entry_price",
    "exit_price",
    "pnl_rs",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "quantity",
    "status",
    "status_reason",
    "exit_reason",
    "signal_end",
    "confirmation_end",
    "signal_id",
    "entry_order_id",
    "stop_order_id",
    "target_order_id",
    "squareoff_order_id",
    "execution_mode",
    "execution_profile",
    "quantity_policy",
    "updated_at_ist",
    "setup_id",
    "stop_pct",
    "target_pct",
    "stop_price",
    "target_price",
    "strategy_version",
    "strategy_fingerprint",
]


def entry_csv_path(session_date: date, side: str) -> Path:
    strategy_id = "v13_v10_g" if _is_g_profile() else "v6"
    return EXPORT_ROOT / (
        f"signals_{session_date.isoformat()}_fno_id_{strategy_id}_{side.lower()}.csv"
    )


def trades_csv_path(session_date: date) -> Path:
    strategy_id = "v13_v10_g" if _is_g_profile() else "v6"
    return EXPORT_ROOT / f"live_trades_{session_date.isoformat()}_fno_id_{strategy_id}.csv"


def open_positions_path(session_date: date) -> Path:
    return EXPORT_ROOT / f"open_positions_{session_date.isoformat()}.json"


def profile_order_day_dir(session_date: date) -> Path:
    return PROFILE_ORDER_ROOT / session_date.isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = common.read_json(path)
    except (OSError, TypeError, ValueError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _confirmation_path(session_date: date, signal_end: str) -> Path:
    confirmation_end = config.SIGNAL_TO_CONFIRMATION[signal_end]
    return (
        CONFIRMATION_ROOT
        / session_date.isoformat()
        / f"slot_{confirmation_end.replace(':', '')}.json"
    )


def _is_g_profile() -> bool:
    return is_g_config(config)


def _display_label() -> str:
    return "V13-V10-G" if _is_g_profile() else "V6"


def _validate_g_contract(row: dict[str, Any], session_date: date) -> None:
    """Refuse stale V6 rows or G rows whose frozen execution terms changed."""
    setup = config.setup_for(str(row.get("signal_end", "")), str(row.get("side", "")))
    if setup is None:
        raise RuntimeError("V13-V10-G row is not an active setup.")
    expected = {
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": session_date.isoformat(),
        "setup_id": setup.setup_id,
        "confirmation_end": setup.confirmation_end,
        "entry_activation_deadline_ist": config.activation_deadline(
            session_date, setup.confirmation_end
        ).isoformat(timespec="seconds"),
        "stop_pct": float(setup.stop_pct),
        "target_pct": float(setup.target_pct),
    }
    for key, wanted in expected.items():
        actual = row.get(key)
        if isinstance(wanted, float):
            try:
                matches = math.isfinite(float(actual)) and abs(float(actual) - wanted) <= 1e-9
            except (TypeError, ValueError):
                matches = False
        else:
            matches = actual == wanted
        if not matches:
            raise RuntimeError(
                f"V13-V10-G row {row.get('signal_id')} failed {key}: "
                f"expected {wanted!r}, observed {actual!r}"
            )


def load_authoritative_signals(session_date: date) -> list[dict[str, Any]]:
    """Load only IDs committed by a matching strategy confirmation snapshot."""

    expected_fingerprint = config.strategy_fingerprint()
    authoritative_ids: set[str] = set()
    authoritative_slots: dict[str, str] = {}
    for signal_end in config.SIGNAL_TO_CONFIRMATION:
        snapshot = _read_json(_confirmation_path(session_date, signal_end))
        if not snapshot:
            continue
        identity = (
            snapshot.get("strategy_version") == config.STRATEGY_VERSION
            and snapshot.get("strategy_fingerprint") == expected_fingerprint
            and snapshot.get("session_date") == session_date.isoformat()
            and snapshot.get("state") == "SUCCESS"
        )
        if not identity:
            continue
        for value in snapshot.get("selected_signal_ids", []):
            if not value:
                continue
            signal_id = str(value)
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", signal_id):
                raise RuntimeError("Confirmation snapshot contains an unsafe signal ID.")
            if signal_id in authoritative_slots and authoritative_slots[signal_id] != signal_end:
                raise RuntimeError(f"Signal is committed in multiple confirmation slots: {signal_id}")
            authoritative_slots[signal_id] = signal_end
            authoritative_ids.add(signal_id)

    rows: list[dict[str, Any]] = []
    signal_day = SIGNAL_ROOT / session_date.isoformat()
    for signal_id in sorted(authoritative_ids):
        signal = _read_json(signal_day / f"{signal_id}.json")
        if not signal:
            raise RuntimeError(
                f"Authoritative {_display_label()} signal file is missing or invalid: {signal_id}"
            )
        side = str(signal.get("side", "")).upper()
        signal_end = str(signal.get("signal_end", ""))
        setup = config.setup_for(signal_end, side)
        if (
            signal.get("signal_id") != signal_id
            or signal.get("strategy_version") != config.STRATEGY_VERSION
            or signal.get("strategy_fingerprint") != expected_fingerprint
            or signal.get("session_date") != session_date.isoformat()
            or signal_end != authoritative_slots[signal_id]
            or setup is None
            or signal.get("confirmation_end") != setup.confirmation_end
            or signal.get("setup_id") != setup.setup_id
        ):
            raise RuntimeError(f"Authoritative {_display_label()} signal failed identity checks: {signal_id}")
        if _is_g_profile():
            _validate_g_contract(signal, session_date)
        if int(dict(signal.get("live_sizing") or {}).get("quantity", 0)) < 1:
            raise RuntimeError(
                f"Authoritative {_display_label()} signal cannot support one-share execution: {signal_id}"
            )
        rows.append(signal)

    return sorted(
        rows,
        key=lambda row: (
            str(row.get("confirmation_end", "")),
            str(row.get("side", "")),
            str(row.get("tradingsymbol", "")),
        ),
    )


def load_profile_order_states(
    session_date: date,
    authoritative_ids: set[str],
) -> list[dict[str, Any]]:
    """Load and validate only this profile's LIVE quantity-one states."""

    root = profile_order_day_dir(session_date)
    if not root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        state = _read_json(path)
        if not state:
            raise RuntimeError(f"Invalid {_display_label()} LIVE order-state JSON: {path}")
        signal_id = str(state.get("signal_id", ""))
        if signal_id not in authoritative_ids:
            raise RuntimeError(
                f"{_display_label()} LIVE order state is not backed by an authoritative signal: {signal_id}"
            )
        expected = {
            "session_date": session_date.isoformat(),
            "mode": EXECUTION_MODE,
            "execution_profile": EXECUTION_PROFILE,
            "quantity_policy": QUANTITY_POLICY,
            "execution_quantity_override": EXECUTION_QUANTITY,
            "quantity": EXECUTION_QUANTITY,
        }
        mismatches = {
            key: (state.get(key), value)
            for key, value in expected.items()
            if state.get(key) != value
        }
        if mismatches:
            raise RuntimeError(
                f"{_display_label()} LIVE quantity-one state failed validation ({signal_id}): {mismatches}"
            )
        if _is_g_profile():
            _validate_g_contract(state, session_date)
        rows.append(state)
    return rows


def _entry_rows(
    signals: list[dict[str, Any]],
    states_by_id: dict[str, dict[str, Any]],
    side: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for signal in signals:
        if str(signal.get("side", "")).upper() != side.upper():
            continue
        state = states_by_id.get(str(signal["signal_id"]), {})
        strategy_quantity = int(dict(signal["live_sizing"])["quantity"])
        rows.append(
            {
                "signal_datetime": signal.get("confirmation_timestamp")
                or signal.get("signal_timestamp"),
                "detected_time_ist": signal.get("published_at_ist", ""),
                "ticker": signal.get("tradingsymbol", ""),
                "side": side.upper(),
                "entry_price": signal.get("trigger_price", ""),
                "target_price": signal.get("target_price", ""),
                "stop_price": signal.get("stop_price", ""),
                "quantity": EXECUTION_QUANTITY,
                "strategy_sized_quantity": strategy_quantity,
                "status": state.get("status", "WAITING_EXECUTION_STATE"),
                "status_reason": state.get("status_reason", ""),
                "signal_end": signal.get("signal_end", ""),
                "confirmation_end": signal.get("confirmation_end", ""),
                "activation_deadline_ist": signal.get(
                    "entry_activation_deadline_ist", ""
                ),
                "rank_within_scan": signal.get("rank_within_scan", ""),
                "setup_id": signal.get("setup_id", ""),
                "picker": signal.get("picker", ""),
                "signal_id": signal.get("signal_id", ""),
                "strategy_version": signal.get("strategy_version", ""),
                "strategy_fingerprint": signal.get("strategy_fingerprint", ""),
                "execution_mode": EXECUTION_MODE,
                "execution_profile": EXECUTION_PROFILE,
                "quantity_policy": QUANTITY_POLICY,
                "stop_pct": signal.get("stop_pct", ""),
                "target_pct": signal.get("target_pct", ""),
            }
        )
    return pd.DataFrame(rows, columns=ENTRY_COLUMNS)


def _trade_rows(states: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for state in states:
        if float(state.get("entry_price") or 0) <= 0:
            continue
        status = str(state.get("status", ""))
        rows.append(
            {
                "ticker": state.get("tradingsymbol", ""),
                "entry_time": state.get("entry_at_ist", ""),
                "exit_time": state.get("exit_at_ist", ""),
                "side": state.get("side", ""),
                "outcome": state.get("exit_reason") or status,
                "filled_price": state.get("entry_price", ""),
                "entry_price": state.get("entry_price", ""),
                "exit_price": state.get("exit_price", ""),
                "pnl_rs": state.get("net_pnl_rs", 0),
                "gross_pnl_rs": state.get("gross_pnl_rs", 0),
                "estimated_cost_rs": state.get("estimated_cost_rs", 0),
                "quantity": state.get("quantity", ""),
                "status": status,
                "status_reason": state.get("status_reason", ""),
                "exit_reason": state.get("exit_reason", ""),
                "signal_end": state.get("signal_end", ""),
                "confirmation_end": state.get("confirmation_end", ""),
                "signal_id": state.get("signal_id", ""),
                "entry_order_id": state.get("entry_order_id", ""),
                "stop_order_id": state.get("stop_order_id", ""),
                "target_order_id": state.get("target_order_id", ""),
                "squareoff_order_id": state.get("squareoff_order_id", ""),
                "execution_mode": state.get("mode", ""),
                "execution_profile": state.get("execution_profile", ""),
                "quantity_policy": state.get("quantity_policy", ""),
                "updated_at_ist": state.get("updated_at_ist", ""),
                "setup_id": state.get("setup_id", ""),
                "stop_pct": state.get("stop_pct", ""),
                "target_pct": state.get("target_pct", ""),
                "stop_price": state.get("stop_price", ""),
                "target_price": state.get("target_price", ""),
                "strategy_version": state.get("strategy_version", ""),
                "strategy_fingerprint": state.get("strategy_fingerprint", ""),
            }
        )
    return pd.DataFrame(rows, columns=TRADE_COLUMNS)


def _arm_status(session_date: date) -> dict[str, Any]:
    control_root = CONTROL_ROOT if CONTROL_ROOT is not None else LIVE_ROOT
    arm = _read_json(control_root / "live_arm.json")
    kill = _read_json(control_root / "kill_switch.json")
    acknowledgement_valid = (
        os.getenv(config.LIVE_ACK_ENV, "").strip() == config.LIVE_ACK
    )
    arm_enabled = bool(arm.get("enabled"))
    arm_date_matches = str(arm.get("session_date", "")) == session_date.isoformat()
    arm_strategy_matches = (
        not _is_g_profile()
        or arm.get("strategy_fingerprint") == config.strategy_fingerprint()
    )
    kill_enabled = bool(kill.get("enabled"))
    if not acknowledgement_valid:
        reason = "LIVE_ACK_MISSING"
    elif not arm_enabled:
        reason = "LIVE_ARM_FILE_DISABLED"
    elif not arm_date_matches:
        reason = "LIVE_ARM_DATE_MISMATCH"
    elif not arm_strategy_matches:
        reason = "LIVE_ARM_STRATEGY_MISMATCH"
    elif kill_enabled:
        reason = "KILL_SWITCH_ENABLED"
    else:
        reason = "LIVE_ARMED"
    return {
        "armed": reason == "LIVE_ARMED",
        "arm_reason": reason,
        "acknowledgement_valid": acknowledgement_valid,
        "arm_enabled": arm_enabled,
        "arm_date_matches": arm_date_matches,
        "arm_strategy_matches": arm_strategy_matches,
        "kill_switch_enabled": kill_enabled,
    }


def _auto_arm_session(session_date: date) -> Path:
    """Persist explicit authorization for this strategy and session date."""
    control_root = CONTROL_ROOT if CONTROL_ROOT is not None else LIVE_ROOT
    path = control_root / "live_arm.json"
    common.atomic_write_json(
        path,
        {
            "enabled": True,
            "session_date": session_date.isoformat(),
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "source": "V13_V10_G_QTY1_AUTO_ARM",
            "updated_at_ist": common.now_ist().isoformat(timespec="seconds"),
        },
    )
    return path


def export_snapshot(
    session_date: date,
    *,
    child_status: dict[str, Any] | None = None,
    state: str = "RUNNING",
) -> dict[str, Any]:
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    signals = load_authoritative_signals(session_date)
    authoritative_ids = {str(row["signal_id"]) for row in signals}
    states = load_profile_order_states(session_date, authoritative_ids)
    states_by_id = {str(row["signal_id"]): row for row in states}

    long_frame = _entry_rows(signals, states_by_id, "LONG")
    short_frame = _entry_rows(signals, states_by_id, "SHORT")
    trade_frame = _trade_rows(states)
    common.atomic_write_csv(short_frame, entry_csv_path(session_date, "SHORT"))
    common.atomic_write_csv(long_frame, entry_csv_path(session_date, "LONG"))
    common.atomic_write_csv(trade_frame, trades_csv_path(session_date))
    open_states = [
        row
        for row in states
        if str(row.get("status", ""))
        in {"OPEN", "SQUARE_OFF_PENDING"}
    ]
    common.atomic_write_json(
        open_positions_path(session_date),
        {
            "schema_version": "fno_v6_live_kite_qty1_open_positions_v1",
            "session_date": session_date.isoformat(),
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "execution_profile": EXECUTION_PROFILE,
            "quantity_policy": QUANTITY_POLICY,
            "open_trades": [
                {
                    "signal_id": row.get("signal_id", ""),
                    "ticker": row.get("tradingsymbol", ""),
                    "side": row.get("side", ""),
                    "quantity": row.get("quantity", ""),
                    "status": row.get("status", ""),
                }
                for row in open_states
            ],
            "updated_at_ist": common.now_ist().isoformat(timespec="seconds"),
        },
    )

    counts = {
        "signals": len(signals),
        "long_signals": len(long_frame),
        "short_signals": len(short_frame),
        "order_states": len(states),
        "filled_trades": len(trade_frame),
        "pending": sum(row.get("status") == "PENDING_ENTRY" for row in states),
        "open": sum(row.get("status") == "OPEN" for row in states),
        "closed": sum(row.get("status") == "CLOSED" for row in states),
        "cancelled": sum(row.get("status") == "CANCELLED" for row in states),
    }
    observed = common.now_ist()
    payload: dict[str, Any] = {
        "schema_version": "fno_v6_live_kite_qty1_status_v1",
        "session_id": SESSION_ID,
        "session_date": session_date.isoformat(),
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "state": state,
        "execution_mode": EXECUTION_MODE,
        "execution_profile": EXECUTION_PROFILE,
        "quantity": EXECUTION_QUANTITY,
        "quantity_policy": QUANTITY_POLICY,
        "updated_at_ist": observed.isoformat(timespec="seconds"),
        **_arm_status(session_date),
        **counts,
        "outputs": {
            "short_entries_csv": str(entry_csv_path(session_date, "SHORT")),
            "long_entries_csv": str(entry_csv_path(session_date, "LONG")),
            "live_trades_csv": str(trades_csv_path(session_date)),
        },
        "children": dict(child_status or {}),
    }
    common.atomic_write_json(STATUS_PATH, payload)
    common.atomic_write_json(
        HEARTBEAT_PATH,
        {
            "schema_version": "fno_v6_live_kite_qty1_heartbeat_v1",
            "session_id": SESSION_ID,
            "session_date": session_date.isoformat(),
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "state": payload["state"],
            "heartbeat_ist": observed.isoformat(timespec="seconds"),
            "signals": counts["signals"],
            "filled_trades": counts["filled_trades"],
            "arm_reason": payload["arm_reason"],
        },
    )
    return payload


def worker_command(session_date: date, side: str) -> list[str]:
    role = "long-entry" if side.upper() == "LONG" else "short-entry"
    script = "fno_v13_v10_g_live.py" if _is_g_profile() else "fno_v6_live.py"
    return [
        sys.executable,
        "-u",
        str(SCRIPT_DIR / script),
        "--role",
        role,
        "--session-date",
        session_date.isoformat(),
        "--execution-mode",
        EXECUTION_MODE,
        "--live-quantity",
        str(EXECUTION_QUANTITY),
    ]


def worker_environment() -> dict[str, str]:
    env = dict(os.environ)
    env["FNO_LIVE_GENERATION"] = "v6"
    env["FNO_V6_EXECUTION_MODE"] = EXECUTION_MODE
    env["FNO_V6_EXECUTION_SESSION_NAMESPACE"] = EXECUTION_PROFILE
    # Pin the same profile for both child workers even if the parent environment
    # was changed after its configuration was loaded.
    if _is_g_profile():
        env["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G"
    else:
        env.pop("FNO_V6_STRATEGY_PROFILE", None)
    return env


def worker_session_id(side: str) -> str:
    prefix = "fno_v13_v10_g" if _is_g_profile() else "fno_v6"
    return f"{prefix}_{EXECUTION_PROFILE}_{side.lower()}"


def _publish_export_failure(
    session_date: date, child_status: dict[str, Any], exc: Exception
) -> dict[str, Any]:
    """Expose export failures without stopping workers protecting broker positions."""
    observed = common.now_ist().isoformat(timespec="seconds")
    payload = {
        "schema_version": "fno_v6_live_kite_qty1_status_v1",
        "session_id": SESSION_ID,
        "session_date": session_date.isoformat(),
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "state": "DEGRADED",
        "execution_mode": EXECUTION_MODE,
        "execution_profile": EXECUTION_PROFILE,
        "quantity": EXECUTION_QUANTITY,
        "quantity_policy": QUANTITY_POLICY,
        "updated_at_ist": observed,
        "export_unavailable": True,
        "error": f"{type(exc).__name__}: {exc}",
        "children": child_status,
        **_arm_status(session_date),
    }
    # Failure to write reporting files must not terminate the trade managers.
    try:
        common.atomic_write_json(STATUS_PATH, payload)
        common.atomic_write_json(HEARTBEAT_PATH, {**payload, "heartbeat_ist": observed})
    except Exception as reporting_exc:
        print(f"[{SESSION_ID}] reporting unavailable: {reporting_exc}", file=sys.stderr)
    return payload


def _terminate_children(children: dict[str, subprocess.Popen[Any]]) -> None:
    for process in children.values():
        if process.poll() is None:
            process.terminate()
    deadline = time.monotonic() + 5.0
    for process in children.values():
        if process.poll() is not None:
            continue
        try:
            process.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.kill()


def run(args: argparse.Namespace) -> int:
    session_date = (
        date.fromisoformat(args.session_date)
        if args.session_date
        else common.now_ist().date()
    )
    config.validate_strategy()
    config.attest_selected_backtest()
    if getattr(args, "readiness_only", False):
        arm_status = _arm_status(session_date)
        payload = export_snapshot(
            session_date,
            state="READY_DISARMED" if not arm_status["armed"] else "READINESS_ONLY",
        )
        payload.update(
            readiness_only=True,
            workers_started=False,
            execution_enabled=False,
            readiness_note="Configuration checked; CSVs prepared; no LIVE workers or broker requests started.",
        )
        common.atomic_write_json(STATUS_PATH, payload)
        print(
            f"[{SESSION_ID}] {payload['state']} {session_date}: "
            f"strategy={config.STRATEGY_VERSION} signals={payload['signals']} "
            f"arm={payload['arm_reason']} quantity={EXECUTION_QUANTITY}; "
            "no LIVE workers or broker requests started."
        )
        return 0
    if not args.allow_non_trading_day and not common.is_trading_day(
        session_date, common.load_holidays()
    ):
        export_snapshot(session_date, state="SKIPPED_NON_TRADING_DAY")
        print(f"[{SESSION_ID}] non-trading day {session_date}; no workers started.")
        return 0
    if args.once:
        payload = export_snapshot(session_date, state="SNAPSHOT_COMPLETE")
        print(
            f"[{SESSION_ID}] snapshot {session_date}: signals={payload['signals']} "
            f"orders={payload['order_states']} fills={payload['filled_trades']} "
            f"arm={payload['arm_reason']} quantity={EXECUTION_QUANTITY}"
        )
        return 0

    if getattr(args, "auto_arm", False):
        arm_path = _auto_arm_session(session_date)
        print(f"[{SESSION_ID}] auto-armed LIVE quantity 1 for {session_date}: {arm_path}")

    env = worker_environment()
    children: dict[str, subprocess.Popen[Any]] = {}
    final_state = "STOPPED"
    try:
        for side in ("LONG", "SHORT"):
            command = worker_command(session_date, side)
            children[side] = subprocess.Popen(
                command,
                cwd=SCRIPT_DIR,
                env=env,
            )
            print(
                f"[{SESSION_ID}] started {side} LIVE worker pid={children[side].pid} "
                f"profile={EXECUTION_PROFILE} quantity={EXECUTION_QUANTITY}"
            )

        last_log = 0.0
        while True:
            child_status = {
                side.lower(): {
                    "session_id": worker_session_id(side),
                    "pid": process.pid,
                    "return_code": process.poll(),
                }
                for side, process in children.items()
            }
            failures = {
                side: info["return_code"]
                for side, info in child_status.items()
                if info["return_code"] not in (None, 0)
            }
            try:
                payload = export_snapshot(
                    session_date,
                    child_status=child_status,
                    state="DEGRADED" if failures else "RUNNING",
                )
            except Exception as exc:
                payload = _publish_export_failure(session_date, child_status, exc)
            now_monotonic = time.monotonic()
            if now_monotonic - last_log >= 60.0:
                print(
                    f"[{SESSION_ID}] signals={payload.get('signals', 'unavailable')} "
                    f"orders={payload.get('order_states', 'unavailable')} "
                    f"fills={payload.get('filled_trades', 'unavailable')} "
                    f"open={payload.get('open', 'unavailable')} "
                    f"arm={payload['arm_reason']} qty=1 "
                    f"export_error={payload.get('error', '')} worker_failures={failures}"
                )
                last_log = now_monotonic

            return_codes = {
                side: process.poll() for side, process in children.items()
            }
            completed = {side: code for side, code in return_codes.items() if code is not None}
            if len(completed) == len(children):
                failures = {side: code for side, code in completed.items() if code != 0}
                final_state = "FAILED" if failures else "DONE"
                return (next(iter(failures.values())) or 2) if failures else 0
            # A failed side is reported above. Keep the other side alive so that
            # its existing broker positions continue to receive exit management.
            time.sleep(args.poll_sec)
    except KeyboardInterrupt:
        final_state = "INTERRUPTED"
        return 0
    finally:
        _terminate_children(children)
        try:
            export_snapshot(
                session_date,
                state=final_state,
                child_status={
                    side.lower(): {
                        "session_id": worker_session_id(side),
                        "pid": process.pid,
                        "return_code": process.poll(),
                    }
                    for side, process in children.items()
                },
            )
        except Exception as exc:
            print(f"[{SESSION_ID}] final export failed: {exc}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default="")
    parser.add_argument("--poll-sec", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--auto-arm",
        action="store_true",
        help="Write the current dated strategy arm record before starting LIVE workers.",
    )
    parser.add_argument(
        "--readiness-only",
        action="store_true",
        help="Validate and prepare CSV/status outputs without starting LIVE workers or contacting the broker.",
    )
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.poll_sec <= 0:
        raise ValueError("--poll-sec must be positive.")
    try:
        return run(args)
    except Exception as exc:
        observed = common.now_ist()
        EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
        common.atomic_write_json(
            STATUS_PATH,
            {
                "schema_version": "fno_v6_live_kite_qty1_status_v1",
                "session_id": SESSION_ID,
                "state": "FAILED",
                "execution_mode": EXECUTION_MODE,
                "execution_profile": EXECUTION_PROFILE,
                "quantity": EXECUTION_QUANTITY,
                "quantity_policy": QUANTITY_POLICY,
                "error": f"{type(exc).__name__}: {exc}",
                "updated_at_ist": observed.isoformat(timespec="seconds"),
            },
        )
        print(f"[{SESSION_ID}] FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
