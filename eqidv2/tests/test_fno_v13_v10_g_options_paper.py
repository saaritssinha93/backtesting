from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

import fno_v13_v10_g_live_config as equity_config
import fno_v13_v10_g_options_paper as options
from fno_v13_v10_g_options_execution import simulate_trade
from fno_v13_v10_g_options_one_lot_execution import simulate_one_lot_trade


DAY = date(2026, 9, 15)


def _equity_state(*, side: str = "SHORT", status: str = "OPEN") -> dict:
    return {
        "strategy_version": equity_config.STRATEGY_VERSION,
        "strategy_fingerprint": equity_config.strategy_fingerprint(),
        "signal_id": "20260915_0926_SHORT_TEST_abc",
        "session_date": DAY.isoformat(),
        "side": side,
        "tradingsymbol": "TEST",
        "mode": "PAPER",
        "status": status,
        "entry_price": 101.0,
        "entry_at_ist": "2026-09-15T09:27:05+05:30",
    }


def _master() -> pd.DataFrame:
    return pd.DataFrame([
        {"underlying": "TEST", "expiry": "2026-09-29", "strike": 100.0,
         "lot_size": 50, "tick_size": .05, "instrument_token": 1,
         "tradingsymbol": "TEST26SEP100CE", "instrument_type": "CE"},
        {"underlying": "TEST", "expiry": "2026-09-29", "strike": 100.0,
         "lot_size": 50, "tick_size": .05, "instrument_token": 2,
         "tradingsymbol": "TEST26SEP100PE", "instrument_type": "PE"},
        {"underlying": "TEST", "expiry": "2026-09-29", "strike": 105.0,
         "lot_size": 50, "tick_size": .05, "instrument_token": 3,
         "tradingsymbol": "TEST26SEP105PE", "instrument_type": "PE"},
    ]).assign(expiry=lambda frame: pd.to_datetime(frame.expiry))


def test_only_actual_filled_current_g_equity_states_trigger(tmp_path, monkeypatch) -> None:
    root = tmp_path / "orders"
    monkeypatch.setattr(options, "EQUITY_ORDER_ROOT", root)
    day_root = root / "PAPER" / DAY.isoformat()
    day_root.mkdir(parents=True)
    for index, status in enumerate(("PENDING_ENTRY", "OPEN", "CLOSED", "CANCELLED")):
        row = _equity_state(status=status)
        row["signal_id"] += str(index)
        (day_root / f"{index}.json").write_text(json.dumps(row), encoding="utf-8")
    rows = options.load_equity_entries(DAY, "SHORT", "PAPER")
    assert [row["status"] for row in rows] == ["OPEN", "CLOSED"]
    assert all(row["source_equity_mode"] == "PAPER" for row in rows)


def test_long_maps_ce_short_maps_pe_and_quantity_is_one_lot(tmp_path) -> None:
    master = _master()
    path = tmp_path / "master.parquet"
    path.write_bytes(b"master")
    for side, expected in (("LONG", "CE"), ("SHORT", "PE")):
        equity = _equity_state(side=side)
        equity.update(_equity_symbol="TEST", _equity_entry_price=101.0)
        mapped = options.map_equity_entry(equity, master, path, "digest")
        assert mapped["option_type"] == expected
        assert mapped["position_side"] == "BUY"
        assert mapped["lots"] == 1
        assert mapped["quantity"] == mapped["lot_size"] == 50
        assert mapped["option_strike"] == 100.0


def test_guarded_quote_entry_and_exit_use_full_premium_cash(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(options, "ORDER_ROOT", tmp_path / "book")
    equity = _equity_state()
    equity.update(
        _equity_symbol="TEST", _equity_entry_price=101.0,
        _equity_entry_ts=pd.Timestamp("2026-09-15T09:27:05+05:30"),
        source_equity_mode="PAPER", source_equity_path="source.json", source_equity_sha256="abc",
    )
    mapped = options.map_equity_entry(equity, _master(), tmp_path / "master", "digest")
    state = options.create_state(
        equity, mapped, datetime.fromisoformat("2026-09-15T09:27:06+05:30"), "TEST",
    )
    args = options.build_parser().parse_args(["--role", "short-entry"])
    entry_quote = {
        "last_price": 10.0, "volume": 10000,
        "timestamp": "2026-09-15T09:27:06+05:30",
        "depth": {"buy": [{"price": 9.95}], "sell": [{"price": 10.0}]},
    }
    options.advance_quote_state(state, entry_quote, datetime.fromisoformat("2026-09-15T09:27:06+05:30"), args)
    assert state["status"] == "OPEN"
    assert state["quantity"] == 50 and state["lots"] == 1
    assert state["entry_premium_outlay_rs"] == state["entry_price"] * 50
    exit_quote = {
        "last_price": state["target_price"] + .1, "volume": 20000,
        "timestamp": "2026-09-15T10:00:00+05:30",
        "depth": {"buy": [{"price": state["target_price"] + .05}], "sell": [{"price": state["target_price"] + .1}]},
    }
    options.advance_quote_state(state, exit_quote, datetime.fromisoformat("2026-09-15T10:00:00+05:30"), args)
    assert state["status"] == "CLOSED" and state["exit_reason"] == "TARGET"
    assert state["net_pnl_rs"] == state["gross_pnl_rs"] - state["estimated_cost_rs"]


def test_one_lot_replay_is_opt_in_and_three_lot_research_default_stays_locked() -> None:
    candles = pd.DataFrame([
        {"timestamp": "2026-09-15T09:25:00+05:30", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 10000},
        {"timestamp": "2026-09-15T09:30:00+05:30", "open": 10, "high": 16, "low": 10, "close": 12, "volume": 10000},
    ])
    row = {"trade_id": "x", "day": DAY.isoformat(), "entry_ts": "2026-09-15T09:30:00+05:30", "lot_size": 50, "tick_size": .05}
    result, _ = simulate_one_lot_trade(row, candles, options.STOP_PCT, options.TARGET_PCT)
    assert result["lots"] == 1 and result["quantity"] == 50 and result["status"] == "CLOSED"
    try:
        simulate_trade(row, candles, .125, .25, lots=1)
    except ValueError as exc:
        assert "exactly 3 lots" in str(exc)
    else:
        raise AssertionError("one-lot use must require the explicit paper/replay opt-in")


def test_runtime_is_paper_only_and_pinned_to_eight_quote_apps() -> None:
    source = Path(options.__file__).read_text(encoding="utf-8")
    assert ".place_order(" not in source
    args = options.build_parser().parse_args(["--role", "long-entry"])
    assert args.max_apps == 8
    assert options.STOP_PCT == .30
    assert options.TARGET_PCT == .404
    assert options.ROLE_TITLES["trade-logger"] == "Options V13-V10-G Continuous Paper Trade Log"
    assert options.ROLE_TITLES["net-result"] == "Options V13-V10-G Paper Net Result"


def test_entry_expires_without_a_quote_so_restart_cannot_backfill_a_trade(tmp_path) -> None:
    equity = _equity_state()
    equity.update(
        _equity_symbol="TEST", _equity_entry_price=101.0,
        _equity_entry_ts=pd.Timestamp("2026-09-15T09:27:05+05:30"),
        source_equity_mode="PAPER", source_equity_path="source.json", source_equity_sha256="abc",
    )
    mapped = options.map_equity_entry(equity, _master(), tmp_path / "master", "digest")
    state = options.create_state(
        equity, mapped, datetime.fromisoformat("2026-09-15T09:27:06+05:30"), "TEST",
    )
    args = options.build_parser().parse_args(["--role", "short-entry", "--max-entry-lag-sec", "60"])
    changed = options.expire_without_quote(
        state, datetime.fromisoformat("2026-09-15T09:29:00+05:30"), args,
    )
    assert changed is True
    assert state["status"] == "SKIPPED"
    assert "NO_RETROACTIVE" in state["status_reason"]
