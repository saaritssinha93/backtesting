"""Paper-trade runner for fundamental_price_action_v1.

Consumes the side-split entry sheets the FPA session publishes
(`signals_<date>_fpa_v1_long.csv` / `_short.csv`) and simulates each entry on a
flat 1% stop-loss and 1% target for both sides, squaring off any survivor at the
session square-off time. Net P&L uses the shared NSE intraday cost model, so the
result CSV lines up column-for-column with the v7 papertrade sheet.

Exit resolution reads the same closed 5-minute bars the strategy itself reads,
so a paper fill can never use a bar the strategy had not seen.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
from nse_intraday_costs import CostConfig, intraday_equity_costs

import fundamental_price_action_v1_session as fpa


SESSION = "fundamental_price_action_v1_papertrade"
SCRIPT_NAME = Path(__file__).name
IST = ZoneInfo("Asia/Kolkata")
BAR_MINUTES = 5
FILE_SUFFIX = "_stocks_indicators_5min.parquet"
LIVE_SIGNAL_DIR = runtime_dir("live_signals")
DEFAULT_LIVE_FOLDER = runtime_dir("stocks_indicators_5min_eq_live")
STATUS_PATH = RUNTIME_STATUS_DIR / f"{SESSION}.status"
HEARTBEAT_PATH = RUNTIME_STATUS_DIR / f"{SESSION}.heartbeat"
PROCESS_START_IST = dt.datetime.now(IST)

TRADE_FIELDS = [
    "trade_id",
    "signal_id",
    "signal_datetime",
    "entry_time",
    "exit_time",
    "ticker",
    "side",
    "quantity",
    "capital_rs",
    "leverage",
    "exposure_rs",
    "entry_price",
    "exit_price",
    "stop_price",
    "target_price",
    "stop_pct",
    "target_pct",
    "outcome",
    "gross_pnl_rs",
    "gross_pnl_pct",
    "brokerage_rs",
    "stt_rs",
    "exch_txn_rs",
    "sebi_rs",
    "ipft_rs",
    "stamp_rs",
    "gst_rs",
    "total_cost_rs",
    "net_pnl_rs",
    "net_pnl_pct",
    "return_on_capital_pct",
    "cost_bps_of_turnover",
    "cost_pct_of_entry",
    "cost_rates_as_of",
    "signal_score",
    "forensic_verdict",
    "market_regime",
    "strategy",
]


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def log(message: str) -> None:
    print(f"[{now_ist().strftime('%H:%M:%S')}] {message}", flush=True)


def warn(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def status_fields(status: str, **extra: Any) -> dict[str, Any]:
    now = now_ist()
    payload: dict[str, Any] = {
        "status": status,
        "script": SCRIPT_NAME,
        "session": SESSION,
        "strategy": SESSION,
        "pid": os.getpid(),
        "ts": now.strftime("%Y-%m-%d_%H:%M:%S"),
        "ts_iso": now.isoformat(),
        "start_ts": PROCESS_START_IST.strftime("%Y-%m-%d_%H:%M:%S"),
    }
    payload.update(extra)
    return payload


def publish_status(status: str, *, heartbeat_state: str | None = None, **extra: Any) -> None:
    """Status writes reuse the session's retrying, never-fatal KV writer."""
    payload = status_fields(status, **extra)
    state = heartbeat_state or (
        "RUNNING" if status in {"RUNNING", "SUCCESS", "WAITING"} else status
    )
    fpa.write_runtime_kv(STATUS_PATH, payload)
    fpa.write_runtime_kv(HEARTBEAT_PATH, {"state": state, **payload})


def publish_heartbeat(status: str = "RUNNING", **extra: Any) -> None:
    publish_status(status, **extra)


def parse_clock(value: str) -> dt.time:
    return dt.datetime.strptime(value, "%H:%M").time()


def to_float(value: Any) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def parse_ist(value: str) -> dt.datetime | None:
    try:
        stamp = dt.datetime.fromisoformat(str(value).strip())
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=IST)
    return stamp.astimezone(IST)


def trades_csv_path(session_date: dt.date) -> Path:
    return LIVE_SIGNAL_DIR / f"paper_trades_{session_date.isoformat()}_fpa_v1.csv"


def read_entry_sheet(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except (OSError, csv.Error) as exc:
        warn(f"[ENTRIES][WARN] cannot read {path.name}: {type(exc).__name__}: {exc}")
        return []


def load_bars(live_folder: Path, ticker: str) -> pd.DataFrame | None:
    path = live_folder / f"{ticker}{FILE_SUFFIX}"
    if not path.exists():
        return None
    for attempt in range(3):
        try:
            frame = pd.read_parquet(path)
            break
        except Exception:  # the live writer may be replacing the file
            if attempt == 2:
                return None
            time.sleep(0.15 * (attempt + 1))
    else:  # pragma: no cover - defensive
        return None
    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(frame.columns):
        return None
    frame = frame.copy()
    stamps = pd.to_datetime(frame["date"], errors="coerce")
    stamps = (
        stamps.dt.tz_localize(IST) if stamps.dt.tz is None else stamps.dt.tz_convert(IST)
    )
    frame["date"] = stamps
    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = (
        frame.dropna(subset=["date", "open", "high", "low", "close"])
        .sort_values("date")
        .drop_duplicates("date", keep="last")
    )
    return frame


def closed_bars_after(
    frame: pd.DataFrame, after: dt.datetime, as_of: dt.datetime, grace_seconds: int
) -> pd.DataFrame:
    """Bars that both opened after the signal and have fully closed by now."""
    cutoff = pd.Timestamp(as_of) - pd.Timedelta(seconds=grace_seconds)
    closed = frame["date"] + pd.Timedelta(minutes=BAR_MINUTES) <= cutoff
    return frame[(frame["date"] > pd.Timestamp(after)) & closed]


def resolve_levels(side: str, entry: float, stop_pct: float, target_pct: float) -> tuple[float, float]:
    if side == "LONG":
        return entry * (1.0 - stop_pct), entry * (1.0 + target_pct)
    return entry * (1.0 + stop_pct), entry * (1.0 - target_pct)


def scan_for_exit(
    side: str, bars: pd.DataFrame, stop_price: float, target_price: float
) -> tuple[str, float, dt.datetime] | None:
    """First bar that resolves the trade.

    When one bar's range spans both levels the stop is taken. Five-minute bars
    hide the intra-bar path, so the adverse fill is the honest assumption.
    """
    for bar in bars.itertuples(index=False):
        high = float(bar.high)
        low = float(bar.low)
        stamp = bar.date.to_pydatetime()
        if side == "LONG":
            hit_stop = low <= stop_price
            hit_target = high >= target_price
        else:
            hit_stop = high >= stop_price
            hit_target = low <= target_price
        if hit_stop:
            return "SL", stop_price, stamp
        if hit_target:
            return "TARGET", target_price, stamp
    return None


def build_trade(
    entry_row: dict[str, str],
    *,
    exit_reason: str,
    exit_price: float,
    exit_time: dt.datetime,
    stop_price: float,
    target_price: float,
    stop_pct: float,
    target_pct: float,
    cfg: CostConfig,
) -> dict[str, str]:
    side = str(entry_row.get("side", "")).strip().upper()
    entry_price = float(entry_row["entry_price"])
    quantity = int(float(entry_row["quantity"]))
    costs = intraday_equity_costs(entry_price, exit_price, quantity, side, cfg)
    entry_notional = entry_price * quantity
    gross_pct = (costs.gross_pnl / entry_notional * 100.0) if entry_notional else 0.0
    net_pct = (costs.net_pnl / entry_notional * 100.0) if entry_notional else 0.0
    # Own capital at risk, not the leveraged exposure: a 1% price move on a 5x
    # position is roughly a 5% move on the capital that funded it.
    capital = to_float(entry_row.get("capital_rs")) or 0.0
    if capital <= 0:
        leverage_used = to_float(entry_row.get("leverage")) or 1.0
        capital = entry_notional / leverage_used if leverage_used > 0 else entry_notional
    roc_pct = (costs.net_pnl / capital * 100.0) if capital else 0.0
    signal_id = str(entry_row.get("signal_id", ""))
    return {
        "trade_id": f"FPA-{uuid.uuid5(uuid.NAMESPACE_URL, signal_id).hex[:12]}",
        "signal_id": signal_id,
        "signal_datetime": str(entry_row.get("signal_datetime", "")),
        "entry_time": str(entry_row.get("signal_datetime", "")),
        "exit_time": exit_time.isoformat(),
        "ticker": str(entry_row.get("ticker", "")),
        "side": side,
        "quantity": str(quantity),
        "capital_rs": f"{capital:.2f}",
        "leverage": str(entry_row.get("leverage", "")),
        "exposure_rs": f"{entry_notional:.2f}",
        "entry_price": f"{entry_price:.2f}",
        "exit_price": f"{exit_price:.2f}",
        "stop_price": f"{stop_price:.2f}",
        "target_price": f"{target_price:.2f}",
        "stop_pct": f"{stop_pct * 100:.2f}",
        "target_pct": f"{target_pct * 100:.2f}",
        "outcome": exit_reason,
        "gross_pnl_rs": f"{costs.gross_pnl:.2f}",
        "gross_pnl_pct": f"{gross_pct:.4f}",
        "brokerage_rs": f"{costs.brokerage:.2f}",
        "stt_rs": f"{costs.stt:.2f}",
        "exch_txn_rs": f"{costs.exch_txn:.2f}",
        "sebi_rs": f"{costs.sebi:.2f}",
        "ipft_rs": f"{costs.ipft:.2f}",
        "stamp_rs": f"{costs.stamp:.2f}",
        "gst_rs": f"{costs.gst:.2f}",
        "total_cost_rs": f"{costs.total_cost:.2f}",
        "net_pnl_rs": f"{costs.net_pnl:.2f}",
        "net_pnl_pct": f"{net_pct:.4f}",
        "return_on_capital_pct": f"{roc_pct:.4f}",
        "cost_bps_of_turnover": f"{costs.cost_bps_of_turnover:.3f}",
        "cost_pct_of_entry": f"{costs.cost_pct_of_entry:.4f}",
        "cost_rates_as_of": cfg.rates_as_of,
        "signal_score": str(entry_row.get("signal_score", "")),
        "forensic_verdict": str(entry_row.get("forensic_verdict", "")),
        "market_regime": str(entry_row.get("market_regime", "")),
        "strategy": fpa.STRATEGY,
    }


def write_trades(path: Path, trades: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=TRADE_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for trade in trades:
                writer.writerow({field: trade.get(field, "") for field in TRADE_FIELDS})
    except OSError:
        fpa.discard(temporary)
        raise
    fpa.atomic_replace(temporary, path)


def collect_signals(session_date: dt.date) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for side in ("LONG", "SHORT"):
        rows.extend(read_entry_sheet(fpa.live_entry_path(session_date, side)))
    resolved: list[dict[str, str]] = []
    for row in rows:
        entry = to_float(row.get("entry_price"))
        quantity = to_float(row.get("quantity"))
        stamp = parse_ist(str(row.get("signal_datetime", "")))
        side = str(row.get("side", "")).strip().upper()
        if entry is None or entry <= 0 or not quantity or quantity < 1:
            continue
        if stamp is None or side not in {"LONG", "SHORT"}:
            continue
        row["_signal_ts"] = stamp.isoformat()
        resolved.append(row)
    resolved.sort(key=lambda item: item["_signal_ts"])
    return resolved


def run_cycle(args: argparse.Namespace, session_date: dt.date) -> dict[str, Any]:
    """Rebuild the full trade sheet from the entry sheets and closed bars.

    The pass is deterministic and idempotent: every cycle re-derives all trades
    from source, so a restart mid-session cannot double-book or lose a trade.
    """
    cfg = CostConfig()
    as_of = now_ist()
    square_off = dt.datetime.combine(session_date, parse_clock(args.square_off_time), tzinfo=IST)
    signals = collect_signals(session_date)

    trades: list[dict[str, str]] = []
    open_positions: list[dict[str, Any]] = []
    # One live position per symbol+side; a repeat signal while that position is
    # still open is a duplicate of the same idea, not a second trade.
    held: dict[tuple[str, str], dt.datetime] = {}
    bars_cache: dict[str, pd.DataFrame | None] = {}
    skipped_duplicate = 0
    skipped_no_data = 0

    for row in signals:
        ticker = str(row.get("ticker", "")).strip()
        side = str(row.get("side", "")).strip().upper()
        signal_ts = parse_ist(row["_signal_ts"])
        assert signal_ts is not None
        key = (ticker, side)
        if key in held and signal_ts <= held[key]:
            skipped_duplicate += 1
            continue
        if ticker not in bars_cache:
            bars_cache[ticker] = load_bars(args.live_folder, ticker)
        frame = bars_cache[ticker]
        if frame is None:
            skipped_no_data += 1
            continue

        entry_price = float(row["entry_price"])
        stop_price, target_price = resolve_levels(
            side, entry_price, args.stop_pct, args.target_pct
        )
        forward = closed_bars_after(frame, signal_ts, as_of, args.close_grace_seconds)
        resolution = scan_for_exit(side, forward, stop_price, target_price)

        if resolution is not None:
            reason, exit_price, exit_time = resolution
        elif as_of >= square_off:
            tail = forward.tail(1)
            if tail.empty:
                skipped_no_data += 1
                continue
            reason = "SQUARE_OFF"
            exit_price = float(tail.iloc[0]["close"])
            exit_time = tail.iloc[0]["date"].to_pydatetime()
        else:
            held[key] = square_off
            open_positions.append(
                {
                    "ticker": ticker,
                    "side": side,
                    "signal_datetime": row.get("signal_datetime", ""),
                    "entry_price": f"{entry_price:.2f}",
                    "stop_price": f"{stop_price:.2f}",
                    "target_price": f"{target_price:.2f}",
                }
            )
            continue

        held[key] = exit_time
        trades.append(
            build_trade(
                row,
                exit_reason=reason,
                exit_price=exit_price,
                exit_time=exit_time,
                stop_price=stop_price,
                target_price=target_price,
                stop_pct=args.stop_pct,
                target_pct=args.target_pct,
                cfg=cfg,
            )
        )

    trades.sort(key=lambda item: item["exit_time"])
    path = trades_csv_path(session_date)
    write_trades(path, trades)

    net_total = sum(float(trade["net_pnl_rs"]) for trade in trades)
    gross_total = sum(float(trade["gross_pnl_rs"]) for trade in trades)
    cost_total = sum(float(trade["total_cost_rs"]) for trade in trades)
    capital_total = sum(float(trade["capital_rs"]) for trade in trades)
    exposure_total = sum(float(trade["exposure_rs"]) for trade in trades)
    wins = sum(1 for trade in trades if float(trade["net_pnl_rs"]) > 0)
    roc_total = (net_total / capital_total * 100.0) if capital_total else 0.0
    return {
        "capital_deployed_rs": round(capital_total, 2),
        "exposure_rs": round(exposure_total, 2),
        "return_on_capital_pct": round(roc_total, 4),
        "signals": len(signals),
        "trades": len(trades),
        "open_positions": len(open_positions),
        "open_detail": open_positions,
        "wins": wins,
        "losses": len(trades) - wins,
        "gross_pnl_rs": round(gross_total, 2),
        "total_cost_rs": round(cost_total, 2),
        "net_pnl_rs": round(net_total, 2),
        "skipped_duplicate": skipped_duplicate,
        "skipped_no_data": skipped_no_data,
        "output": str(path),
    }


def run_session(args: argparse.Namespace) -> int:
    current = now_ist()
    session_date = (
        dt.date.fromisoformat(args.session_date) if args.session_date else current.date()
    )
    if session_date.weekday() >= 5 and not args.allow_weekend:
        publish_status("SKIPPED_WEEKEND", session_date_ist=session_date.isoformat())
        return 0
    if not args.live_folder.is_dir():
        raise FileNotFoundError(f"Live data folder not found: {args.live_folder}")

    end_deadline = dt.datetime.combine(session_date, parse_clock(args.end_time), tzinfo=IST)
    log(
        f"START {SESSION} stop={args.stop_pct * 100:.2f}% target={args.target_pct * 100:.2f}% "
        f"square_off={args.square_off_time} end={args.end_time}"
    )
    loop_errors = 0

    while True:
        try:
            current = now_ist()
            summary = run_cycle(args, session_date)
            log(
                "CYCLE signals={signals} trades={trades} open={open_positions} "
                "skipped_dup={skipped_duplicate} skipped_nodata={skipped_no_data} "
                "W/L={wins}/{losses} gross={gross_pnl_rs} cost={total_cost_rs} "
                "net={net_pnl_rs} capital={capital_deployed_rs} "
                "exposure={exposure_rs} roc={return_on_capital_pct}%".format(**summary)
            )
            for position in summary["open_detail"]:
                log(
                    "  OPEN {ticker} {side} entry={entry_price} "
                    "sl={stop_price} tgt={target_price}".format(**position)
                )
            publish_status(
                "SUCCESS" if summary["trades"] else "RUNNING",
                session_date_ist=session_date.isoformat(),
                phase="CYCLE",
                signals=summary["signals"],
                trades=summary["trades"],
                open_positions=summary["open_positions"],
                wins=summary["wins"],
                losses=summary["losses"],
                gross_pnl_rs=summary["gross_pnl_rs"],
                total_cost_rs=summary["total_cost_rs"],
                net_pnl_rs=summary["net_pnl_rs"],
                capital_deployed_rs=summary["capital_deployed_rs"],
                exposure_rs=summary["exposure_rs"],
                return_on_capital_pct=summary["return_on_capital_pct"],
                stop_pct=f"{args.stop_pct * 100:.2f}",
                target_pct=f"{args.target_pct * 100:.2f}",
                output=summary["output"],
            )
            loop_errors = 0

            if args.once:
                return 0
            current = now_ist()
            if current >= end_deadline:
                publish_status(
                    "DONE",
                    heartbeat_state="DONE",
                    session_date_ist=session_date.isoformat(),
                    phase="END_TIME",
                    trades=summary["trades"],
                    net_pnl_rs=summary["net_pnl_rs"],
                    output=summary["output"],
                    message=f"Session ended at {args.end_time}.",
                )
                log(f"END {SESSION} net={summary['net_pnl_rs']} trades={summary['trades']}")
                return 0
            time.sleep(
                min(args.poll_seconds, max(0.0, (end_deadline - current).total_seconds()))
            )
        except Exception as exc:
            loop_errors += 1
            warn(f"[LOOP][WARN] cycle failed ({loop_errors}/10): {type(exc).__name__}: {exc}")
            publish_heartbeat(
                "RUNNING",
                session_date_ist=session_date.isoformat(),
                phase="LOOP_ERROR",
                loop_errors=loop_errors,
                error=f"{type(exc).__name__}: {exc}",
            )
            if loop_errors >= 10 or args.once:
                publish_status(
                    "FAILED",
                    heartbeat_state="CRASHED",
                    session_date_ist=session_date.isoformat(),
                    error=f"{type(exc).__name__}: {exc}",
                )
                return 1
            time.sleep(args.poll_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"Run {SESSION}.")
    parser.add_argument("--live-folder", type=Path, default=DEFAULT_LIVE_FOLDER)
    parser.add_argument("--session-date", default="")
    parser.add_argument("--stop-pct", type=float, default=0.01)
    parser.add_argument("--target-pct", type=float, default=0.01)
    parser.add_argument("--square-off-time", default="15:15")
    parser.add_argument("--end-time", default="15:30")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    parser.add_argument("--close-grace-seconds", type=int, default=15)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--allow-weekend", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return run_session(args)
    except KeyboardInterrupt:
        publish_status("STOPPED", heartbeat_state="STOPPED", message="Interrupted.")
        return 0
    except Exception as exc:
        publish_status(
            "FAILED", heartbeat_state="CRASHED", error=f"{type(exc).__name__}: {exc}"
        )
        warn(f"[FATAL] {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
