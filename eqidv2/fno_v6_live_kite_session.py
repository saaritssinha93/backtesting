"""Isolated quantity-one LIVE execution session for the frozen FnO V6 strategy.

The V6 scanner and 1-minute confirmation pipeline remain the sole signal
producers.  This coordinator starts one LIVE worker per side, pins every
executable order state to one NSE equity share, and publishes dashboard CSVs
from authoritative V6 signals and this profile's LIVE-only order directory.

Real orders remain fail-closed behind the V6 acknowledgement, same-day arm
file, kill switch, and the signal activation deadline enforced by
``fno_v5_live.py``.  This module never creates or changes any safety file.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_v6_live_config as config


SCRIPT_DIR = Path(__file__).resolve().parent
SESSION_ID = "fno_v6_live_kite_qty1"
EXECUTION_PROFILE = "live_kite_qty1"
EXECUTION_MODE = "LIVE"
EXECUTION_QUANTITY = 1
QUANTITY_POLICY = "FIXED_ONE_SHARE"

LIVE_ROOT = common.FNO_ROOT / "v6_live"
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
]


def entry_csv_path(session_date: date, side: str) -> Path:
    return EXPORT_ROOT / (
        f"signals_{session_date.isoformat()}_fno_id_v6_{side.lower()}.csv"
    )


def trades_csv_path(session_date: date) -> Path:
    return EXPORT_ROOT / f"live_trades_{session_date.isoformat()}_fno_id_v6.csv"


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


def load_authoritative_signals(session_date: date) -> list[dict[str, Any]]:
    """Load only IDs committed by a matching frozen-V6 confirmation snapshot."""

    expected_fingerprint = config.strategy_fingerprint()
    authoritative_ids: set[str] = set()
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
        authoritative_ids.update(
            str(value) for value in snapshot.get("selected_signal_ids", []) if value
        )

    rows: list[dict[str, Any]] = []
    signal_day = SIGNAL_ROOT / session_date.isoformat()
    for signal_id in sorted(authoritative_ids):
        signal = _read_json(signal_day / f"{signal_id}.json")
        if not signal:
            raise RuntimeError(
                f"Authoritative V6 signal file is missing or invalid: {signal_id}"
            )
        side = str(signal.get("side", "")).upper()
        signal_end = str(signal.get("signal_end", ""))
        setup = config.setup_for(signal_end, side)
        if (
            signal.get("signal_id") != signal_id
            or signal.get("strategy_version") != config.STRATEGY_VERSION
            or signal.get("strategy_fingerprint") != expected_fingerprint
            or signal.get("session_date") != session_date.isoformat()
            or setup is None
            or signal.get("confirmation_end") != setup.confirmation_end
            or signal.get("setup_id") != setup.setup_id
        ):
            raise RuntimeError(f"Authoritative V6 signal failed identity checks: {signal_id}")
        if int(dict(signal.get("live_sizing") or {}).get("quantity", 0)) < 1:
            raise RuntimeError(
                f"Authoritative V6 signal cannot support one-share execution: {signal_id}"
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
            raise RuntimeError(f"Invalid V6 LIVE order-state JSON: {path}")
        signal_id = str(state.get("signal_id", ""))
        if signal_id not in authoritative_ids:
            raise RuntimeError(
                f"V6 LIVE order state is not backed by an authoritative signal: {signal_id}"
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
                f"V6 LIVE quantity-one state failed validation ({signal_id}): {mismatches}"
            )
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
            }
        )
    return pd.DataFrame(rows, columns=TRADE_COLUMNS)


def _arm_status(session_date: date) -> dict[str, Any]:
    arm = _read_json(LIVE_ROOT / "live_arm.json")
    kill = _read_json(LIVE_ROOT / "kill_switch.json")
    acknowledgement_valid = (
        os.getenv(config.LIVE_ACK_ENV, "").strip() == config.LIVE_ACK
    )
    arm_enabled = bool(arm.get("enabled"))
    arm_date_matches = str(arm.get("session_date", "")) == session_date.isoformat()
    kill_enabled = bool(kill.get("enabled"))
    if not acknowledgement_valid:
        reason = "LIVE_ACK_MISSING"
    elif not arm_enabled:
        reason = "LIVE_ARM_FILE_DISABLED"
    elif not arm_date_matches:
        reason = "LIVE_ARM_DATE_MISMATCH"
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
        "kill_switch_enabled": kill_enabled,
    }


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
    return [
        sys.executable,
        "-u",
        str(SCRIPT_DIR / "fno_v6_live.py"),
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
    return env


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
                    "pid": process.pid,
                    "return_code": process.poll(),
                }
                for side, process in children.items()
            }
            payload = export_snapshot(session_date, child_status=child_status)
            now_monotonic = time.monotonic()
            if now_monotonic - last_log >= 60.0:
                print(
                    f"[{SESSION_ID}] signals={payload['signals']} "
                    f"orders={payload['order_states']} fills={payload['filled_trades']} "
                    f"open={payload['open']} arm={payload['arm_reason']} qty=1"
                )
                last_log = now_monotonic

            return_codes = {
                side: process.poll() for side, process in children.items()
            }
            completed = {side: code for side, code in return_codes.items() if code is not None}
            if completed:
                if len(completed) == len(children) and all(
                    code == 0 for code in completed.values()
                ):
                    final_state = "DONE"
                    return 0
                failures = {side: code for side, code in completed.items() if code != 0}
                if failures:
                    final_state = "FAILED"
                    print(f"[{SESSION_ID}] worker failure: {failures}", file=sys.stderr)
                    return next(iter(failures.values())) or 2
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
