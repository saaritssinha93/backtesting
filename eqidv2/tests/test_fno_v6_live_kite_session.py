from __future__ import annotations

from copy import deepcopy
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import fno_oi_common as common
import fno_v6_live_kite_session as live_session


ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 8, 10)


@pytest.fixture
def isolated_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    live_root = tmp_path / "fno_oi" / "v6_live"
    confirmation_root = live_root / "confirmation_1m"
    signal_root = live_root / "signals"
    profile_order_root = (
        live_root / "orders" / "LIVE" / live_session.EXECUTION_PROFILE
    )
    export_root = live_root / "live_kite"

    monkeypatch.setattr(live_session, "LIVE_ROOT", live_root)
    monkeypatch.setattr(live_session, "CONFIRMATION_ROOT", confirmation_root)
    monkeypatch.setattr(live_session, "SIGNAL_ROOT", signal_root)
    monkeypatch.setattr(live_session, "PROFILE_ORDER_ROOT", profile_order_root)
    monkeypatch.setattr(live_session, "EXPORT_ROOT", export_root)
    monkeypatch.setattr(live_session, "STATUS_PATH", export_root / "status.json")
    monkeypatch.setattr(
        live_session,
        "HEARTBEAT_PATH",
        export_root / "heartbeat.json",
    )
    return live_root


def _signal(
    side: str,
    signal_id: str,
    *,
    ticker: str,
    strategy_quantity: int,
    signal_end: str = "09:25",
) -> dict:
    setup = live_session.config.setup_for(signal_end, side)
    assert setup is not None
    return {
        "signal_id": signal_id,
        "strategy_version": live_session.config.STRATEGY_VERSION,
        "strategy_fingerprint": live_session.config.strategy_fingerprint(),
        "session_date": DAY.isoformat(),
        "side": side,
        "signal_end": signal_end,
        "confirmation_end": setup.confirmation_end,
        "setup_id": setup.setup_id,
        "picker": setup.picker,
        "tradingsymbol": ticker,
        "confirmation_timestamp": f"{DAY.isoformat()}T{setup.confirmation_end}:00+05:30",
        "signal_timestamp": f"{DAY.isoformat()}T{signal_end}:00+05:30",
        "published_at_ist": f"{DAY.isoformat()}T{setup.confirmation_end}:05+05:30",
        "entry_activation_deadline_ist": live_session.config.activation_deadline(
            DAY, setup.confirmation_end
        ).isoformat(),
        "trigger_price": 100.0,
        "target_price": 103.0 if side == "LONG" else 97.0,
        "stop_price": 99.0 if side == "LONG" else 101.0,
        "rank_within_scan": 1,
        "live_sizing": {"quantity": strategy_quantity},
    }


def _write_snapshot(
    signal_end: str,
    selected_signal_ids: list[str],
    *,
    strategy_fingerprint: str | None = None,
) -> None:
    path = live_session._confirmation_path(DAY, signal_end)
    path.parent.mkdir(parents=True, exist_ok=True)
    common.atomic_write_json(
        path,
        {
            "schema_version": "fno_v6_confirmation_slot_v1",
            "session_date": DAY.isoformat(),
            "strategy_version": live_session.config.STRATEGY_VERSION,
            "strategy_fingerprint": (
                strategy_fingerprint
                if strategy_fingerprint is not None
                else live_session.config.strategy_fingerprint()
            ),
            "state": "SUCCESS",
            "selected_signal_ids": selected_signal_ids,
        },
    )


def _write_signal(signal: dict) -> None:
    path = (
        live_session.SIGNAL_ROOT
        / DAY.isoformat()
        / f"{signal['signal_id']}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    common.atomic_write_json(path, signal)


def _state(signal: dict, **overrides: object) -> dict:
    payload = {
        "signal_id": signal["signal_id"],
        "session_date": DAY.isoformat(),
        "mode": "LIVE",
        "execution_profile": live_session.EXECUTION_PROFILE,
        "quantity_policy": live_session.QUANTITY_POLICY,
        "execution_quantity_override": 1,
        "quantity": 1,
        "tradingsymbol": signal["tradingsymbol"],
        "side": signal["side"],
        "signal_end": signal["signal_end"],
        "confirmation_end": signal["confirmation_end"],
        "status": "OPEN",
        "status_reason": "",
        "entry_price": 100.25,
        "entry_at_ist": f"{DAY.isoformat()}T09:26:15+05:30",
        "exit_at_ist": "",
        "exit_price": 0,
        "gross_pnl_rs": 0,
        "estimated_cost_rs": 0.05,
        "net_pnl_rs": -0.05,
        "entry_order_id": "ENTRY-1",
        "stop_order_id": "STOP-1",
        "target_order_id": "TARGET-1",
        "squareoff_order_id": "",
        "updated_at_ist": f"{DAY.isoformat()}T09:26:16+05:30",
    }
    payload.update(overrides)
    return payload


def _write_state(state: dict, filename: str = "state.json") -> None:
    root = live_session.profile_order_day_dir(DAY)
    root.mkdir(parents=True, exist_ok=True)
    common.atomic_write_json(root / filename, state)


def test_frozen_constants_worker_commands_and_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert live_session.SESSION_ID == "fno_v6_live_kite_qty1"
    assert live_session.EXECUTION_MODE == "LIVE"
    assert live_session.EXECUTION_PROFILE == "live_kite_qty1"
    assert live_session.EXECUTION_QUANTITY == 1
    assert live_session.QUANTITY_POLICY == "FIXED_ONE_SHARE"
    assert live_session.PROFILE_ORDER_ROOT == (
        live_session.LIVE_ROOT / "orders" / "LIVE" / "live_kite_qty1"
    )

    command_prefix = [
        live_session.sys.executable,
        "-u",
        str(live_session.SCRIPT_DIR / "fno_v6_live.py"),
    ]
    assert live_session.worker_command(DAY, "LONG") == command_prefix + [
        "--role",
        "long-entry",
        "--session-date",
        DAY.isoformat(),
        "--execution-mode",
        "LIVE",
        "--live-quantity",
        "1",
    ]
    assert live_session.worker_command(DAY, "SHORT") == command_prefix + [
        "--role",
        "short-entry",
        "--session-date",
        DAY.isoformat(),
        "--execution-mode",
        "LIVE",
        "--live-quantity",
        "1",
    ]

    monkeypatch.setenv("FNO_LIVE_GENERATION", "wrong")
    monkeypatch.setenv("FNO_V6_EXECUTION_MODE", "PAPER")
    monkeypatch.setenv("FNO_V6_EXECUTION_SESSION_NAMESPACE", "wrong")
    monkeypatch.setenv(
        live_session.config.LIVE_ACK_ENV,
        live_session.config.LIVE_ACK,
    )
    env = live_session.worker_environment()
    assert env["FNO_LIVE_GENERATION"] == "v6"
    assert env["FNO_V6_EXECUTION_MODE"] == "LIVE"
    assert env["FNO_V6_EXECUTION_SESSION_NAMESPACE"] == "live_kite_qty1"
    assert env[live_session.config.LIVE_ACK_ENV] == live_session.config.LIVE_ACK


def test_confirmation_snapshot_path_matches_confirmation_worker_contract(
    isolated_session: Path,
) -> None:
    assert live_session._confirmation_path(DAY, "09:25").name == "slot_0926.json"


def test_authoritative_signal_filter_uses_only_selected_current_identity(
    isolated_session: Path,
) -> None:
    selected = _signal(
        "LONG",
        "20260810_0925_LONG_SELECTED_11111111",
        ticker="SELECTED",
        strategy_quantity=42,
    )
    unselected = _signal(
        "SHORT",
        "20260810_0925_SHORT_UNSELECTED_22222222",
        ticker="UNSELECTED",
        strategy_quantity=19,
    )
    stale_selected = _signal(
        "LONG",
        "20260810_0930_LONG_STALE_33333333",
        ticker="STALE",
        strategy_quantity=17,
        signal_end="09:30",
    )
    for signal in (selected, unselected, stale_selected):
        _write_signal(signal)
    _write_snapshot("09:25", [selected["signal_id"]])
    _write_snapshot(
        "09:30",
        [stale_selected["signal_id"]],
        strategy_fingerprint="stale-fingerprint",
    )

    rows = live_session.load_authoritative_signals(DAY)

    assert [row["signal_id"] for row in rows] == [selected["signal_id"]]


def test_authoritative_selected_signal_must_exist(
    isolated_session: Path,
) -> None:
    _write_snapshot("09:25", ["missing-selected-signal"])

    with pytest.raises(RuntimeError, match="missing or invalid"):
        live_session.load_authoritative_signals(DAY)


def test_export_csv_headers_and_quantity_are_frozen_to_one(
    isolated_session: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    long_signal = _signal(
        "LONG",
        "20260810_0925_LONG_ALPHA_aaaaaaaa",
        ticker="ALPHA",
        strategy_quantity=47,
    )
    short_signal = _signal(
        "SHORT",
        "20260810_0925_SHORT_BETA_bbbbbbbb",
        ticker="BETA",
        strategy_quantity=31,
    )
    _write_signal(long_signal)
    _write_signal(short_signal)
    _write_snapshot(
        "09:25",
        [long_signal["signal_id"], short_signal["signal_id"]],
    )
    _write_state(_state(long_signal))
    monkeypatch.setenv(
        live_session.config.LIVE_ACK_ENV,
        live_session.config.LIVE_ACK,
    )

    payload = live_session.export_snapshot(DAY)

    long_frame = pd.read_csv(live_session.entry_csv_path(DAY, "LONG"))
    short_frame = pd.read_csv(live_session.entry_csv_path(DAY, "SHORT"))
    trade_frame = pd.read_csv(live_session.trades_csv_path(DAY))
    assert list(long_frame.columns) == live_session.ENTRY_COLUMNS
    assert list(short_frame.columns) == live_session.ENTRY_COLUMNS
    assert list(trade_frame.columns) == live_session.TRADE_COLUMNS
    assert long_frame.loc[0, "quantity"] == 1
    assert short_frame.loc[0, "quantity"] == 1
    assert trade_frame.loc[0, "quantity"] == 1
    assert long_frame.loc[0, "strategy_sized_quantity"] == 47
    assert short_frame.loc[0, "strategy_sized_quantity"] == 31
    assert long_frame.loc[0, "execution_mode"] == "LIVE"
    assert long_frame.loc[0, "execution_profile"] == "live_kite_qty1"
    assert payload["quantity"] == 1
    assert payload["signals"] == 2
    assert payload["order_states"] == 1
    assert payload["filled_trades"] == 1


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("quantity", 2),
        ("execution_quantity_override", 2),
        ("mode", "PAPER"),
        ("execution_profile", "default"),
        ("quantity_policy", "STRATEGY_SIZED"),
    ],
)
def test_profile_state_rejects_non_live_or_non_qty_one_contract(
    isolated_session: Path,
    field: str,
    bad_value: object,
) -> None:
    signal = _signal(
        "LONG",
        "20260810_0925_LONG_STATE_cccccccc",
        ticker="STATE",
        strategy_quantity=25,
    )
    state = _state(signal)
    state[field] = bad_value
    _write_state(state)

    with pytest.raises(RuntimeError, match="quantity-one state failed validation"):
        live_session.load_profile_order_states(DAY, {signal["signal_id"]})


def test_profile_state_rejects_unbacked_signal(isolated_session: Path) -> None:
    signal = _signal(
        "LONG",
        "20260810_0925_LONG_UNBACKED_dddddddd",
        ticker="UNBACKED",
        strategy_quantity=25,
    )
    _write_state(_state(signal))

    with pytest.raises(RuntimeError, match="not backed by an authoritative signal"):
        live_session.load_profile_order_states(DAY, set())


@pytest.mark.parametrize(
    ("acknowledged", "write_arm", "expected_reason"),
    [
        (False, True, "LIVE_ACK_MISSING"),
        (True, False, "LIVE_ARM_FILE_DISABLED"),
    ],
)
def test_status_summary_is_fail_closed_without_both_ack_and_arm(
    isolated_session: Path,
    monkeypatch: pytest.MonkeyPatch,
    acknowledged: bool,
    write_arm: bool,
    expected_reason: str,
) -> None:
    if acknowledged:
        monkeypatch.setenv(
            live_session.config.LIVE_ACK_ENV,
            live_session.config.LIVE_ACK,
        )
    else:
        monkeypatch.delenv(live_session.config.LIVE_ACK_ENV, raising=False)
    arm_path = live_session.LIVE_ROOT / "live_arm.json"
    if write_arm:
        arm_path.parent.mkdir(parents=True, exist_ok=True)
        common.atomic_write_json(
            arm_path,
            {"enabled": True, "session_date": DAY.isoformat()},
        )
        arm_before = deepcopy(common.read_json(arm_path))

    payload = live_session.export_snapshot(DAY)
    saved_status = common.read_json(live_session.STATUS_PATH)
    heartbeat = common.read_json(live_session.HEARTBEAT_PATH)

    assert payload["armed"] is False
    assert payload["arm_reason"] == expected_reason
    assert saved_status["armed"] is False
    assert saved_status["arm_reason"] == expected_reason
    assert saved_status["execution_mode"] == "LIVE"
    assert saved_status["quantity"] == 1
    assert heartbeat["arm_reason"] == expected_reason
    if write_arm:
        assert common.read_json(arm_path) == arm_before
    else:
        assert not arm_path.exists()
    assert not (live_session.LIVE_ROOT / "kill_switch.json").exists()


def test_batch_and_scheduler_preserve_frozen_live_contract() -> None:
    batch = (ROOT / "bat" / "run_fno_v6_live_kite_qty1.bat").read_text(
        encoding="utf-8"
    )
    scheduler = (
        ROOT / "bat" / "schedule_fno_v6_live_kite_qty1_weekday.ps1"
    ).read_text(encoding="utf-8")

    assert 'set "SESSION_ID=fno_v6_live_kite_qty1"' in batch
    assert 'set "SESSION_SCRIPT=%BASE_DIR%\\fno_v6_live_kite_session.py"' in batch
    assert 'set "FNO_V6_EXECUTION_MODE=LIVE"' in batch
    assert (
        'set "FNO_V6_LIVE_ACK=I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS"'
        in batch
    )
    assert 'if not "%~1"==""' in batch
    assert "%*" not in batch
    assert "supervise_command.ps1" in batch
    assert "live_arm" not in batch.lower()
    assert "kill_switch" not in batch.lower()

    assert '$taskLeaf = "EQIDV2_fno_v6_live_kite_qty1_0915"' in scheduler
    assert '$startTime = "09:15"' in scheduler
    assert "/SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $startTime" in scheduler
    assert "& schtasks.exe /Create /F" in scheduler
    assert "schtasks.exe /Run" not in scheduler
    assert "Existing task is running; replacement was refused." in scheduler
    assert "Runner must not create or alter live-arm or kill-switch state." in scheduler
    assert "Task unexpectedly started during installation." in scheduler
