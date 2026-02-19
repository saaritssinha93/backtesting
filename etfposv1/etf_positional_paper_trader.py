# -*- coding: utf-8 -*-
"""
Recurring positional paper-trading tracker for ETFPOSV1.

What this module does:
1) Ingests new LONG signals from out_live_signals/YYYYMMDD/signals_*.parquet
2) Opens paper positions with fixed capital-per-trade (cash constrained)
3) Tracks open positions on every run using latest 15m parquet data
4) Books exits when target is hit (no SL; positional behavior)
5) Persists a full audit trail: state, trade book, cash ledger, run snapshots

Positional semantics:
- No intraday force-close logic.
- Open trades are carried forward across sessions/days until target is hit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pytz


IST = pytz.timezone("Asia/Kolkata")
ROOT = Path(__file__).resolve().parent

DEFAULT_SIGNALS_ROOT = ROOT / "out_live_signals"
DEFAULT_15M_DIR = ROOT / "etf_indicators_15min_pq"
DEFAULT_STATE_DIR = ROOT / "paper_trade"
DEFAULT_STATE_FILE = "paper_trader_state.json"

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 40)


def now_ist() -> datetime:
    return datetime.now(IST)


def parse_ts_any(v: Any) -> pd.Timestamp:
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def to_iso(ts: Any) -> Optional[str]:
    t = parse_ts_any(ts)
    if pd.isna(t):
        return None
    return t.isoformat()


def load_state(state_path: Path, initial_capital: float) -> Dict[str, Any]:
    if not state_path.exists():
        return {
            "version": 1,
            "cash_rs": float(initial_capital),
            "next_trade_id": 1,
            "processed_files": [],
            "open_positions": [],
            "closed_positions": [],
            "cash_ledger": [],
            "skipped_signals": [],
            "last_run_ist": None,
        }
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        raw.setdefault("version", 1)
        raw.setdefault("cash_rs", float(initial_capital))
        raw.setdefault("next_trade_id", 1)
        raw.setdefault("processed_files", [])
        raw.setdefault("open_positions", [])
        raw.setdefault("closed_positions", [])
        raw.setdefault("cash_ledger", [])
        raw.setdefault("skipped_signals", [])
        raw.setdefault("last_run_ist", None)
        return raw
    except Exception:
        return {
            "version": 1,
            "cash_rs": float(initial_capital),
            "next_trade_id": 1,
            "processed_files": [],
            "open_positions": [],
            "closed_positions": [],
            "cash_ledger": [],
            "skipped_signals": [],
            "last_run_ist": None,
        }


def save_state(state: Dict[str, Any], state_path: Path) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state["last_run_ist"] = now_ist().isoformat()
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def signal_key(ticker: str, bar_time_ist: pd.Timestamp) -> str:
    raw = f"{ticker.upper()}|{bar_time_ist.isoformat()}|LONG"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def iter_new_signal_files(signals_root: Path, processed_files: set[str]) -> List[Path]:
    if not signals_root.exists():
        return []
    files = sorted(signals_root.glob("*/signals_*.parquet"))
    return [p for p in files if str(p.resolve()) not in processed_files]


def load_signals_from_file(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df

    if "signal_triggered" in df.columns:
        df = df[df["signal_triggered"] == True].copy()
    if df.empty:
        return df

    if "ticker" not in df.columns:
        return pd.DataFrame()

    if "entry_price" not in df.columns:
        if "close_I" in df.columns:
            df["entry_price"] = pd.to_numeric(df["close_I"], errors="coerce")
        else:
            return pd.DataFrame()

    if "bar_time_ist" not in df.columns:
        if "checked_at_ist" in df.columns:
            df["bar_time_ist"] = df["checked_at_ist"]
        else:
            return pd.DataFrame()

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["entry_price"] = pd.to_numeric(df["entry_price"], errors="coerce")
    df["bar_time_ist"] = pd.to_datetime(df["bar_time_ist"], errors="coerce")
    if getattr(df["bar_time_ist"].dt, "tz", None) is None:
        df["bar_time_ist"] = df["bar_time_ist"].dt.tz_localize(IST)
    else:
        df["bar_time_ist"] = df["bar_time_ist"].dt.tz_convert(IST)

    df = df.dropna(subset=["ticker", "entry_price", "bar_time_ist"]).copy()
    if df.empty:
        return df

    return df.sort_values(["bar_time_ist", "ticker"]).reset_index(drop=True)


def load_intraday_for_ticker(dir_15m: Path, ticker: str) -> pd.DataFrame:
    path = dir_15m / f"{ticker}_etf_indicators_15min.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)
    for c in ("high", "close"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def append_ledger(state: Dict[str, Any], entry: Dict[str, Any]) -> None:
    state["cash_ledger"].append(entry)


def open_position_from_signal(
    state: Dict[str, Any],
    row: pd.Series,
    source_file: Path,
    capital_per_trade: float,
    target_pct: float,
) -> None:
    ticker = str(row["ticker"]).upper()
    entry_time = parse_ts_any(row["bar_time_ist"])
    entry_price = float(row["entry_price"])
    sig_key = signal_key(ticker, entry_time) if pd.notna(entry_time) else None

    if not pd.notna(entry_time) or entry_price <= 0:
        return

    qty = int(capital_per_trade // entry_price)
    if qty <= 0:
        state["skipped_signals"].append(
            {
                "timestamp_ist": now_ist().isoformat(),
                "ticker": ticker,
                "entry_time_ist": entry_time.isoformat(),
                "entry_price": entry_price,
                "signal_key": sig_key,
                "reason": "PRICE_GT_CAPITAL_PER_TRADE",
                "source_file": str(source_file),
            }
        )
        return

    invested = float(qty * entry_price)
    if float(state["cash_rs"]) < invested:
        state["skipped_signals"].append(
            {
                "timestamp_ist": now_ist().isoformat(),
                "ticker": ticker,
                "entry_time_ist": entry_time.isoformat(),
                "entry_price": entry_price,
                "qty": qty,
                "invested_rs": invested,
                "signal_key": sig_key,
                "reason": "NO_CAPITAL",
                "source_file": str(source_file),
            }
        )
        return

    target_price = entry_price * (1.0 + target_pct / 100.0)
    trade_id = int(state["next_trade_id"])
    state["next_trade_id"] = trade_id + 1
    state["cash_rs"] = float(state["cash_rs"]) - invested

    pos = {
        "trade_id": trade_id,
        "signal_key": sig_key,
        "source_file": str(source_file),
        "ticker": ticker,
        "status": "OPEN",
        "entry_time_ist": entry_time.isoformat(),
        "entry_price": entry_price,
        "qty": qty,
        "invested_rs": invested,
        "target_pct": float(target_pct),
        "target_price": float(target_price),
        "exit_time_ist": None,
        "exit_price": None,
        "realized_pnl_rs": None,
        "realized_pnl_pct": None,
        "mtm_time_ist": entry_time.isoformat(),
        "mtm_price": entry_price,
        "mtm_value_rs": invested,
        "mtm_pnl_rs": 0.0,
        "mtm_pnl_pct": 0.0,
        "last_update_ist": now_ist().isoformat(),
    }
    state["open_positions"].append(pos)

    append_ledger(
        state,
        {
            "timestamp_ist": now_ist().isoformat(),
            "event": "ENTRY_DEBIT",
            "trade_id": trade_id,
            "ticker": ticker,
            "cash_change_rs": -invested,
            "cash_after_rs": float(state["cash_rs"]),
            "price": entry_price,
            "qty": qty,
            "note": f"target={target_pct:.2f}%",
        },
    )


def update_open_positions(state: Dict[str, Any], dir_15m: Path) -> Tuple[int, int]:
    updated = 0
    closed = 0
    still_open: List[Dict[str, Any]] = []
    for pos in state["open_positions"]:
        updated += 1
        ticker = str(pos["ticker"]).upper()
        entry_time = parse_ts_any(pos["entry_time_ist"])
        df = load_intraday_for_ticker(dir_15m, ticker)

        if df.empty:
            still_open.append(pos)
            continue

        future = df[df["date"] > entry_time].copy()
        if future.empty:
            still_open.append(pos)
            continue

        target_price = float(pos["target_price"])
        hit = future[future["high"] >= target_price]
        if not hit.empty:
            first = hit.iloc[0]
            exit_time = parse_ts_any(first["date"])
            exit_price = target_price
            ret_value = float(pos["qty"]) * exit_price
            pnl_rs = ret_value - float(pos["invested_rs"])
            pnl_pct = (pnl_rs / float(pos["invested_rs"]) * 100.0) if float(pos["invested_rs"]) > 0 else 0.0

            pos["status"] = "CLOSED_TARGET"
            pos["exit_time_ist"] = exit_time.isoformat()
            pos["exit_price"] = float(exit_price)
            pos["realized_pnl_rs"] = float(pnl_rs)
            pos["realized_pnl_pct"] = float(pnl_pct)
            pos["mtm_time_ist"] = exit_time.isoformat()
            pos["mtm_price"] = float(exit_price)
            pos["mtm_value_rs"] = float(ret_value)
            pos["mtm_pnl_rs"] = float(pnl_rs)
            pos["mtm_pnl_pct"] = float(pnl_pct)
            pos["last_update_ist"] = now_ist().isoformat()

            state["closed_positions"].append(pos)
            closed += 1

            state["cash_rs"] = float(state["cash_rs"]) + float(ret_value)
            append_ledger(
                state,
                {
                    "timestamp_ist": now_ist().isoformat(),
                    "event": "EXIT_CREDIT",
                    "trade_id": int(pos["trade_id"]),
                    "ticker": ticker,
                    "cash_change_rs": float(ret_value),
                    "cash_after_rs": float(state["cash_rs"]),
                    "price": float(exit_price),
                    "qty": int(pos["qty"]),
                    "note": "TARGET_HIT",
                },
            )
            continue

        last = future.iloc[-1]
        mtm_price = float(last["close"]) if "close" in last and pd.notna(last["close"]) else float(pos["entry_price"])
        mtm_value = float(pos["qty"]) * mtm_price
        mtm_pnl = mtm_value - float(pos["invested_rs"])
        mtm_pct = (mtm_pnl / float(pos["invested_rs"]) * 100.0) if float(pos["invested_rs"]) > 0 else 0.0
        pos["mtm_time_ist"] = to_iso(last["date"])
        pos["mtm_price"] = float(mtm_price)
        pos["mtm_value_rs"] = float(mtm_value)
        pos["mtm_pnl_rs"] = float(mtm_pnl)
        pos["mtm_pnl_pct"] = float(mtm_pct)
        pos["last_update_ist"] = now_ist().isoformat()
        still_open.append(pos)

    state["open_positions"] = still_open
    return updated, closed


def write_outputs(state: Dict[str, Any], state_dir: Path) -> Dict[str, Any]:
    state_dir.mkdir(parents=True, exist_ok=True)
    trade_book_csv = state_dir / "trade_book.csv"
    ledger_csv = state_dir / "cash_ledger.csv"
    open_csv = state_dir / "open_positions_latest.csv"
    snapshot_csv = state_dir / "run_snapshots.csv"
    summary_txt = state_dir / "status_latest.txt"

    trade_rows = list(state["closed_positions"]) + list(state["open_positions"])
    trade_df = pd.DataFrame(trade_rows)
    if not trade_df.empty:
        trade_df = trade_df.sort_values(["entry_time_ist", "trade_id"]).reset_index(drop=True)
    trade_df.to_csv(trade_book_csv, index=False)

    ledger_df = pd.DataFrame(state["cash_ledger"])
    if not ledger_df.empty:
        ledger_df = ledger_df.sort_values(["timestamp_ist"]).reset_index(drop=True)
    ledger_df.to_csv(ledger_csv, index=False)

    open_df = pd.DataFrame(state["open_positions"])
    if not open_df.empty:
        open_df = open_df.sort_values(["entry_time_ist", "trade_id"]).reset_index(drop=True)
    open_df.to_csv(open_csv, index=False)

    realized_pnl = float(sum(float(x.get("realized_pnl_rs") or 0.0) for x in state["closed_positions"]))
    unrealized_pnl = float(sum(float(x.get("mtm_pnl_rs") or 0.0) for x in state["open_positions"]))
    open_value = float(sum(float(x.get("mtm_value_rs") or 0.0) for x in state["open_positions"]))
    cash = float(state["cash_rs"])
    equity = cash + open_value

    snapshot = {
        "run_time_ist": now_ist().isoformat(),
        "cash_rs": cash,
        "open_positions": int(len(state["open_positions"])),
        "closed_positions": int(len(state["closed_positions"])),
        "realized_pnl_rs": realized_pnl,
        "unrealized_pnl_rs": unrealized_pnl,
        "open_value_rs": open_value,
        "equity_rs": equity,
    }
    pd.DataFrame([snapshot]).to_csv(
        snapshot_csv,
        mode="a",
        index=False,
        header=not snapshot_csv.exists(),
    )

    lines = [
        "ETFPOSV1 Paper Trader - Latest Status",
        f"Run Time (IST): {snapshot['run_time_ist']}",
        f"Cash (Rs): {cash:,.2f}",
        f"Open Value (Rs): {open_value:,.2f}",
        f"Equity (Rs): {equity:,.2f}",
        f"Realized PnL (Rs): {realized_pnl:,.2f}",
        f"Unrealized PnL (Rs): {unrealized_pnl:,.2f}",
        f"Open Positions: {snapshot['open_positions']}",
        f"Closed Positions: {snapshot['closed_positions']}",
        "",
        f"Trade Book: {trade_book_csv}",
        f"Cash Ledger: {ledger_csv}",
        f"Open Positions: {open_csv}",
        f"Snapshots: {snapshot_csv}",
    ]
    summary_txt.write_text("\n".join(lines), encoding="utf-8")
    return snapshot


def run_cycle(
    state: Dict[str, Any],
    signals_root: Path,
    dir_15m: Path,
    capital_per_trade: float,
    target_pct: float,
) -> Dict[str, Any]:
    processed_files = set(state.get("processed_files", []))
    seen_keys = {
        str(x.get("signal_key"))
        for x in (list(state["open_positions"]) + list(state["closed_positions"]))
        if x.get("signal_key")
    }
    seen_keys.update(str(x.get("signal_key")) for x in state.get("skipped_signals", []) if x.get("signal_key"))

    new_files = iter_new_signal_files(signals_root, processed_files)
    signals_opened = 0
    for f in new_files:
        df = load_signals_from_file(f)
        if not df.empty:
            for _, row in df.iterrows():
                key = signal_key(str(row["ticker"]), parse_ts_any(row["bar_time_ist"]))
                if key in seen_keys:
                    continue
                before_open = len(state["open_positions"])
                open_position_from_signal(
                    state=state,
                    row=row,
                    source_file=f,
                    capital_per_trade=capital_per_trade,
                    target_pct=target_pct,
                )
                after_open = len(state["open_positions"])
                if after_open > before_open:
                    signals_opened += 1
                seen_keys.add(key)
        state["processed_files"].append(str(f.resolve()))

    updated_open, closed_now = update_open_positions(state, dir_15m)
    return {
        "new_files": len(new_files),
        "signals_opened": signals_opened,
        "open_positions_checked": updated_open,
        "closed_now": closed_now,
    }


def in_market_hours(ts: datetime) -> bool:
    if ts.weekday() >= 5:
        return False
    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ETFPOSV1 recurring positional paper trader")
    p.add_argument("--signals-root", default=str(DEFAULT_SIGNALS_ROOT), help="Root folder containing out_live_signals/YYYYMMDD")
    p.add_argument("--dir-15m", default=str(DEFAULT_15M_DIR), help="15m indicators directory")
    p.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="Folder for persistent paper-trader state and reports")
    p.add_argument("--state-file", default=DEFAULT_STATE_FILE, help="State json filename")
    p.add_argument("--initial-capital-rs", type=float, default=1_000_000.0, help="Starting paper capital in Rs")
    p.add_argument("--capital-per-trade-rs", type=float, default=20_000.0, help="Fixed capital allocation per signal")
    p.add_argument("--target-pct", type=float, default=8.0, help="Target percent for positional exit")
    p.add_argument("--interval-sec", type=int, default=60, help="Loop interval in seconds")
    p.add_argument("--run-once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--trading-hours-only", action="store_true", help="Only process cycles during weekday market hours")
    return p


def main() -> None:
    args = build_parser().parse_args()
    signals_root = Path(args.signals_root)
    dir_15m = Path(args.dir_15m)
    state_dir = Path(args.state_dir)
    state_path = state_dir / args.state_file

    state = load_state(state_path, args.initial_capital_rs)

    print("[PAPER] ETFPOSV1 positional paper trader started.")
    print(f"        signals_root={signals_root}")
    print(f"        dir_15m={dir_15m}")
    print(f"        state_path={state_path}")
    print(f"        capital_per_trade={args.capital_per_trade_rs:.2f} target_pct={args.target_pct:.2f}")

    while True:
        try:
            now = now_ist()
            if args.trading_hours_only and not in_market_hours(now):
                if args.run_once:
                    print("[PAPER] Outside market hours; run-once mode exiting.")
                    return
                time.sleep(max(5, int(args.interval_sec)))
                continue

            cycle = run_cycle(
                state=state,
                signals_root=signals_root,
                dir_15m=dir_15m,
                capital_per_trade=float(args.capital_per_trade_rs),
                target_pct=float(args.target_pct),
            )
            snapshot = write_outputs(state, state_dir)
            save_state(state, state_path)

            print(
                f"[{now.strftime('%Y-%m-%d %H:%M:%S%z')}] "
                f"new_files={cycle['new_files']} opened={cycle['signals_opened']} "
                f"closed_now={cycle['closed_now']} open={snapshot['open_positions']} "
                f"cash={snapshot['cash_rs']:.2f} equity={snapshot['equity_rs']:.2f}"
            )

            if args.run_once:
                return
            time.sleep(max(1, int(args.interval_sec)))

        except KeyboardInterrupt:
            print("\n[PAPER] Stopped by user.")
            save_state(state, state_path)
            return
        except Exception as e:
            print(f"[PAPER][ERR] {e}")
            save_state(state, state_path)
            if args.run_once:
                raise
            time.sleep(max(5, int(args.interval_sec)))


if __name__ == "__main__":
    main()
