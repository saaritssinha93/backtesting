"""v17D structured logging — one JSON event per line, separate streams.

Five separate log streams (so each is greppable / parseable independently):
    signals.log     every detector signal pre-filter
    decisions.log   every filter / governor / kill-switch decision
    orders.log      every order placement / cancel / modify
    fills.log       every broker fill
    pnl.log         every closed-trade PnL event

Files rotate daily; current day's file is `{stream}_YYYYMMDD.jsonl`.
Each line is a JSON object with at minimum:
    { "ts_ist": "2026-05-09T10:32:15.421+05:30", "stream": "...", ... }

Public API:
    log_event(stream, event: dict)
    daily_summary(stream, date=None) -> dict   (counts / aggregates)
    rotate_summary_file(date=None)             (writes daily_summary_YYYYMMDD.txt)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

DEFAULT_LOG_DIR = Path(
    os.environ.get(
        "V17D_LOG_DIR",
        str(Path.home() / "v17D_logs"),
    )
)
VALID_STREAMS = ("signals", "decisions", "orders", "fills", "pnl")
IST = timezone(timedelta(hours=5, minutes=30))


def _ensure_log_dir() -> Path:
    DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_LOG_DIR


def _file_for(stream: str, date: datetime | None = None) -> Path:
    if stream not in VALID_STREAMS:
        raise ValueError(
            f"stream must be one of {VALID_STREAMS}, got {stream!r}"
        )
    log_dir = _ensure_log_dir()
    when = date or datetime.now(IST)
    return log_dir / f"{stream}_{when.strftime('%Y%m%d')}.jsonl"


def log_event(stream: str, event: dict) -> None:
    """Append one JSON line to the named stream's daily file."""
    fp = _file_for(stream)
    enriched = {
        "ts_ist": datetime.now(IST).isoformat(timespec="milliseconds"),
        "stream": stream,
        **event,
    }
    with open(fp, "a", encoding="utf-8") as f:
        f.write(json.dumps(enriched, default=str) + "\n")


def read_stream(stream: str, date: datetime | None = None) -> list:
    """Read all events from one stream for one day. Returns list of dicts."""
    fp = _file_for(stream, date)
    if not fp.exists():
        return []
    out = []
    with open(fp, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def daily_summary(stream: str, date: datetime | None = None) -> dict:
    """Compact counts and key aggregates for one stream/day."""
    events = read_stream(stream, date)
    summary = {"stream": stream, "n_events": len(events)}

    if stream == "signals":
        sides = {"LONG": 0, "SHORT": 0}
        setups: dict = {}
        for e in events:
            if e.get("side") in sides:
                sides[e["side"]] += 1
            s = e.get("setup")
            if s:
                setups[s] = setups.get(s, 0) + 1
        summary["by_side"] = sides
        summary["by_setup"] = setups

    elif stream == "decisions":
        kept = sum(1 for e in events if e.get("decision") == "KEEP")
        dropped = sum(1 for e in events if e.get("decision") == "DROP")
        drop_reasons: dict = {}
        for e in events:
            if e.get("decision") == "DROP":
                r = e.get("reason", "UNKNOWN")
                drop_reasons[r] = drop_reasons.get(r, 0) + 1
        summary["kept"] = kept
        summary["dropped"] = dropped
        summary["drop_reasons"] = drop_reasons

    elif stream == "fills":
        sides = {"LONG": 0, "SHORT": 0}
        for e in events:
            if e.get("side") in sides:
                sides[e["side"]] += 1
        summary["by_side"] = sides

    elif stream == "pnl":
        wins = sum(1 for e in events if (e.get("pnl_rs") or 0) > 0)
        losses = sum(1 for e in events if (e.get("pnl_rs") or 0) < 0)
        total_pnl = sum(e.get("pnl_rs", 0) or 0 for e in events)
        summary["wins"] = wins
        summary["losses"] = losses
        summary["total_pnl_rs"] = total_pnl

    return summary


def rotate_summary_file(date: datetime | None = None) -> Path:
    """Write a human-readable daily summary across all streams."""
    when = date or datetime.now(IST)
    summary_path = _ensure_log_dir() / f"daily_summary_{when.strftime('%Y%m%d')}.txt"
    lines = [f"v17D daily summary — {when.strftime('%Y-%m-%d')}"]
    for stream in VALID_STREAMS:
        s = daily_summary(stream, when)
        lines.append("")
        lines.append(f"[{stream}] n={s['n_events']}")
        for k, v in s.items():
            if k in ("stream", "n_events"):
                continue
            lines.append(f"    {k}: {v}")
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


if __name__ == "__main__":
    print(f"v17D log directory: {DEFAULT_LOG_DIR}")
    print(f"  override with env V17D_LOG_DIR")
    log_event("signals", {
        "ticker": "TEST", "side": "LONG", "setup": "A_MOD_BREAK_C1_HIGH",
        "entry_price": 500.0,
    })
    log_event("decisions", {
        "ticker": "TEST", "decision": "KEEP", "reason": "passed_all_filters",
    })
    log_event("decisions", {
        "ticker": "TEST2", "decision": "DROP", "reason": "ADX_TOO_LOW",
    })
    print("\nWrote 3 test events. Daily summary:")
    print()
    for stream in VALID_STREAMS:
        s = daily_summary(stream)
        if s["n_events"]:
            print(f"  {stream}: {s}")
    summary_path = rotate_summary_file()
    print(f"\nDaily summary file: {summary_path}")
