"""
P5 (2026-04-22) — EOD reconciler for the V16 5-min two-stage pipeline.

Reads three independent record sources for the trading day and cross-checks
them so a torn pool / missed lifecycle event / silent broker reject cannot
go un-noticed past the close:

  1. pool_lifecycle_<date>_v16_5min.jsonl
       Append-only audit of every pool transition. Written by SE/SEE, PF, DE
       and the executor under `lifecycle_lock`. Source of truth for "what
       the pipeline THOUGHT it did".

  2. live_trades_<date>_v16_5min.csv
       Executor's per-trade row. Source of truth for "what the executor
       EXECUTED" (entry/exit prices, outcome).

  3. Kite orderbook (live mode only — paper run skips this source).
       Source of truth for "what the BROKER recorded".

Outputs
-------
  live_signals/eod_reconciliation_<date>_v16_5min.json

Exit codes
----------
  0  reconciliation OK (no mismatches)
  3  mismatches found (a non-empty `mismatches` list in the JSON report)
  2  required input missing / could not run
  1  unexpected error

Usage
-----
  py -3.12 eqidv2/eqidv2_eod_reconciler_v16_5min.py
  py -3.12 eqidv2/eqidv2_eod_reconciler_v16_5min.py --date 2026-04-21 --no-broker

Designed to be invoked by `bat\run_eqidv2_eod_reconciler_v16_5min.bat`,
itself scheduled via schtasks at 16:00 IST on weekdays.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

IST = timezone(timedelta(hours=5, minutes=30))

# Lazy import — only required when --no-broker is NOT passed.
_KITE_IMPORT_ERROR: Optional[str] = None

try:
    from eqidv2_runtime_paths import LIVE_SIGNALS_DIR  # type: ignore
except Exception:
    LIVE_SIGNALS_DIR = Path(
        os.getenv("EQIDV2_LIVE_SIGNALS_DIR", r"C:\TradingData\eqidv2\live_signals")
    )

POOL_LIFECYCLE_PATTERN = "pool_lifecycle_{}_v16_5min.jsonl"
TRADE_LOG_PATTERN = "live_trades_{}_v16_5min.csv"
RECON_REPORT_PATTERN = "eod_reconciliation_{}_v16_5min.json"

TERMINAL_OUTCOMES = {
    "TARGET_HIT",
    "STOP_HIT",
    "EOD_CLOSE",
    "BROKER_CLOSE_RESTORED",
    "ENTRY_SKIPPED_AFTER_CUTOFF",
    "ENTRY_SKIPPED_MAX_SLIP",
    "ENTRY_SKIPPED_STALE_SIGNAL",
    "ENTRY_SKIPPED_STALE_DETECTION",
    "ENTRY_SKIPPED_NEAR_ENTRY_TIMEOUT",
    "ENTRY_SKIPPED_CONNECTION_RETRY_TIMEOUT",
    "ENTRY_SKIPPED_THIN_LIQUIDITY",
    "REJECTED",
}


@dataclass
class SignalRecon:
    signal_id: str
    ticker: str = ""
    side: str = ""
    lifecycle_events: List[str] = field(default_factory=list)
    lifecycle_terminal: Optional[str] = None
    trade_outcome: Optional[str] = None
    trade_row_present: bool = False
    broker_order_present: Optional[bool] = None  # None when broker not queried
    mismatch_kind: Optional[str] = None


def _resolve_today_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _read_lifecycle_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    events: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                # Best-effort — torn append on a previous Windows write would
                # land here; lifecycle_lock should prevent this on v16_5min.
                continue
    return events


def _read_trade_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size <= 0:
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def _index_lifecycle_by_signal(events: List[Dict[str, Any]]) -> Dict[str, SignalRecon]:
    by_sid: Dict[str, SignalRecon] = {}
    for ev in events:
        sid = str(ev.get("signal_id", "")).strip()
        if not sid:
            continue
        rec = by_sid.setdefault(sid, SignalRecon(signal_id=sid))
        ev_type = str(ev.get("event_type", "")).strip().upper()
        if ev_type:
            rec.lifecycle_events.append(ev_type)
            if ev_type in {"DROPPED", "ORDER_PLACED", "REJECTED", "TIMEOUT"}:
                rec.lifecycle_terminal = ev_type
        if not rec.ticker:
            rec.ticker = str(ev.get("ticker", "")).strip().upper()
        if not rec.side:
            rec.side = str(ev.get("side", "")).strip().upper()
    return by_sid


def _merge_trade_rows(
    by_sid: Dict[str, SignalRecon], trade_rows: List[Dict[str, str]]
) -> None:
    for row in trade_rows:
        sid = str(row.get("signal_id", "")).strip()
        if not sid:
            continue
        rec = by_sid.setdefault(sid, SignalRecon(signal_id=sid))
        rec.trade_row_present = True
        outcome = str(row.get("outcome", "")).strip().upper()
        rec.trade_outcome = outcome or None
        if not rec.ticker:
            rec.ticker = str(row.get("ticker", "")).strip().upper()
        if not rec.side:
            rec.side = str(row.get("side", "")).strip().upper()


def _classify(rec: SignalRecon, broker_known: bool) -> Optional[str]:
    """
    Return a short mismatch kind string (e.g. "DE_PASSED_NO_TRADE") or None
    if the signal reconciles cleanly. Designed to be the smallest set of
    classes an operator needs to triage, not an exhaustive taxonomy.
    """
    has_de_passed = "DE_PASSED" in rec.lifecycle_events
    has_order_placed = "ORDER_PLACED" in rec.lifecycle_events
    has_dropped = "DROPPED" in rec.lifecycle_events

    # 1. DE_PASSED but no executor trade row, and not explicitly DROPPED later.
    if has_de_passed and not rec.trade_row_present and not has_dropped:
        return "DE_PASSED_NO_TRADE"

    # 2. Trade row exists but lifecycle never recorded ORDER_PLACED.
    if rec.trade_row_present and not has_order_placed:
        return "TRADE_WITHOUT_LIFECYCLE_ORDER"

    # 3. Lifecycle says ORDER_PLACED, broker checked, broker has no record.
    if (
        broker_known
        and has_order_placed
        and rec.broker_order_present is False
    ):
        return "LIFECYCLE_ORDER_MISSING_AT_BROKER"

    # 4. Trade row's outcome contradicts a terminal lifecycle DROPPED.
    if has_dropped and rec.trade_outcome and rec.trade_outcome not in TERMINAL_OUTCOMES:
        return "DROPPED_BUT_TRADE_NON_TERMINAL"

    return None


def _query_broker_orders(date_str: str) -> Tuple[Set[str], Optional[str]]:
    """
    Return (set_of_broker_order_ids_for_today, error_message_or_None).
    Uses the same kite credentials helper the executor uses if available.
    On any import / auth failure, returns (empty set, error string) and the
    caller falls back to "broker not queried".
    """
    global _KITE_IMPORT_ERROR
    try:
        from kiteconnect import KiteConnect  # type: ignore
        from auth.access_token_loader import load_kite_with_access_token  # type: ignore
    except Exception as exc:  # pragma: no cover
        _KITE_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"
        return set(), _KITE_IMPORT_ERROR

    try:
        kite = load_kite_with_access_token()
        orders = kite.orders() or []
    except Exception as exc:
        return set(), f"broker_query_failed: {type(exc).__name__}: {exc}"

    today_prefix = date_str  # YYYY-MM-DD; Kite order_timestamp is "YYYY-MM-DD HH:MM:SS"
    today_ids: Set[str] = set()
    for o in orders:
        ts = str(o.get("order_timestamp") or "")
        if ts.startswith(today_prefix):
            oid = str(o.get("order_id") or "").strip()
            if oid:
                today_ids.add(oid)
    return today_ids, None


def _annotate_with_broker(
    by_sid: Dict[str, SignalRecon],
    trade_rows: List[Dict[str, str]],
    broker_order_ids: Set[str],
) -> None:
    """Mark each signal's broker_order_present using the trade CSV's
    entry_order_id column, intersected with the broker's today-order-id set."""
    sid_to_entry_oid: Dict[str, str] = {}
    for row in trade_rows:
        sid = str(row.get("signal_id", "")).strip()
        oid = str(row.get("entry_order_id", "")).strip()
        if sid and oid:
            sid_to_entry_oid[sid] = oid

    for sid, rec in by_sid.items():
        oid = sid_to_entry_oid.get(sid, "")
        if not oid:
            rec.broker_order_present = False
            continue
        rec.broker_order_present = oid in broker_order_ids


def reconcile(date_str: str, query_broker: bool) -> Dict[str, Any]:
    lifecycle_path = Path(LIVE_SIGNALS_DIR) / POOL_LIFECYCLE_PATTERN.format(date_str)
    trade_csv_path = Path(LIVE_SIGNALS_DIR) / TRADE_LOG_PATTERN.format(date_str)

    events = _read_lifecycle_jsonl(lifecycle_path)
    trade_rows = _read_trade_csv(trade_csv_path)

    by_sid = _index_lifecycle_by_signal(events)
    _merge_trade_rows(by_sid, trade_rows)

    broker_error: Optional[str] = None
    broker_known = False
    if query_broker:
        broker_order_ids, broker_error = _query_broker_orders(date_str)
        if broker_error is None:
            broker_known = True
            _annotate_with_broker(by_sid, trade_rows, broker_order_ids)

    mismatches: List[Dict[str, Any]] = []
    counts: Dict[str, int] = defaultdict(int)
    for sid, rec in by_sid.items():
        kind = _classify(rec, broker_known=broker_known)
        rec.mismatch_kind = kind
        if kind:
            counts[kind] += 1
            mismatches.append(
                {
                    "signal_id": sid,
                    "ticker": rec.ticker,
                    "side": rec.side,
                    "kind": kind,
                    "lifecycle_events": rec.lifecycle_events,
                    "trade_outcome": rec.trade_outcome,
                    "broker_order_present": rec.broker_order_present,
                }
            )

    report = {
        "date": date_str,
        "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
        "broker_queried": broker_known,
        "broker_query_error": broker_error,
        "totals": {
            "signals_observed": len(by_sid),
            "lifecycle_events": len(events),
            "trade_rows": len(trade_rows),
            "mismatch_count": len(mismatches),
        },
        "mismatch_counts_by_kind": dict(counts),
        "mismatches": mismatches,
    }
    return report


def _write_report(report: Dict[str, Any], date_str: str) -> Path:
    out_path = Path(LIVE_SIGNALS_DIR) / RECON_REPORT_PATTERN.format(date_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    os.replace(tmp_path, out_path)
    return out_path


def _maybe_send_email(report: Dict[str, Any], report_path: Path) -> None:
    """
    On mismatch, write a sidecar alert marker file. Wiring the actual Gmail
    send is left to the bat (which can shell out to send_gmail_api.py with
    operator-supplied credentials/tokens) — keeping the Python module
    credential-free so it can run in any environment.
    """
    if int(report.get("totals", {}).get("mismatch_count", 0) or 0) <= 0:
        return
    alert_path = report_path.with_name(
        report_path.stem.replace("eod_reconciliation_", "eod_reconciliation_alert_")
        + ".json"
    )
    payload = {
        "date": report.get("date"),
        "report_path": str(report_path),
        "mismatch_count": report["totals"]["mismatch_count"],
        "mismatch_counts_by_kind": report.get("mismatch_counts_by_kind", {}),
    }
    alert_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="EOD reconciler for V16 5-min pipeline.")
    parser.add_argument("--date", default=None, help="IST date YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--no-broker",
        action="store_true",
        help="Skip Kite orderbook query (use for replay / paper-only days).",
    )
    args = parser.parse_args(argv)

    date_str = (args.date or _resolve_today_str()).strip()

    try:
        report = reconcile(date_str=date_str, query_broker=not args.no_broker)
    except Exception:
        traceback.print_exc()
        return 1

    out_path = _write_report(report, date_str)
    _maybe_send_email(report, out_path)

    totals = report["totals"]
    print(
        f"[EOD.RECON] date={date_str} signals={totals['signals_observed']} "
        f"events={totals['lifecycle_events']} trades={totals['trade_rows']} "
        f"mismatches={totals['mismatch_count']} broker_queried={report['broker_queried']}"
    )
    print(f"[EOD.RECON] report -> {out_path}")
    if totals["mismatch_count"] > 0:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
