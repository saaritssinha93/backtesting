"""Read-only intraday market flow and recorded G3 decisions for Dashboard Flow.

Market reconstruction and published strategy evidence remain separate. This
module never imports a trading runner, requests broker data, or writes signals.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import math
from pathlib import Path
import re
from typing import Any

from dashboard_flow_live_data import list_available_dates, load_session_market
from dashboard_flow_live_evidence import build_live_evidence

IST = timezone(timedelta(hours=5, minutes=30))
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _row_evidence(raw: dict) -> dict:
    result = dict(raw)
    result["status"] = raw.get("status") or raw.get("decision") or "NOT_RECORDED"
    result["reasons"] = raw.get("reasons") or []
    return result


def load_live_flow(data_root: Path, session_date: str | None = None, *, now: datetime | None = None) -> dict:
    """Return a single session; external callers supply a date, never a path."""
    now = now or datetime.now(IST)
    now = now.replace(tzinfo=IST) if now.tzinfo is None else now.astimezone(IST)
    today = now.date().isoformat()
    if session_date is not None:
        if not _DATE.fullmatch(session_date):
            raise ValueError("Invalid session date")
        if date.fromisoformat(session_date) > now.date():
            raise ValueError("Future session date")
    dates = sorted({item for item in list_available_dates(Path(data_root)) if _DATE.fullmatch(item) and item <= today})
    session_date = session_date or (today if today in dates or not dates else dates[-1])
    market = load_session_market(Path(data_root), session_date, now=now)
    recorded = build_live_evidence(Path(data_root), session_date, now=now)
    market_slots = market.get("slots") or []
    if isinstance(market_slots, list):
        market_slots = {item["time"]: item for item in market_slots}
    evidence_slots = recorded.get("slots") or {}
    slots = []
    for index in range(76):
        minute = 9 * 60 + 15 + index * 5
        clock = f"{minute // 60:02d}:{minute % 60:02d}"
        boundary = datetime.combine(date.fromisoformat(session_date), datetime.strptime(clock, "%H:%M").time(), IST)
        future = boundary > now
        source = market_slots.get(clock) or {}
        proof = evidence_slots.get(clock) or {}
        records = proof.get("rows") or {}
        if isinstance(records, list):
            records = {row["symbol"]: row for row in records}
        strategy_rows = [{"symbol": symbol, **_row_evidence(row)} for symbol, row in sorted(records.items())] if not future else []
        rows = []
        for raw in ([] if future else source.get("rows") or []):
            row = dict(raw)
            row["oi_change_pct"] = row.get("oi_change5m_pct")
            row["evidence"] = _row_evidence(records.get(row["symbol"]) or {})
            rows.append(row)
        coverage = {**(source.get("coverage") or {}),
            "prices": sum(row.get("price") is not None for row in rows),
            "previous_close": sum(row.get("price_pct") is not None for row in rows),
            "opening_gap": sum(row.get("gap_open_pct") is not None for row in rows),
            "oi": sum(row.get("oi") is not None for row in rows),
            "oi_baseline": sum(row.get("oi_change_pct") is not None for row in rows),
        }
        slots.append({
            **source,
            "time": clock, "state": "future" if future else "available" if rows else "missing",
            "source_state": source.get("state", "MISSING"),
            "label": source.get("label") or ("Reconstructed opening price" if index == 0 else "Completed five-minute bar"),
            "rows": rows, "coverage": coverage, "strategy_rows": strategy_rows,
            "evidence": {
                "state": "FUTURE" if future else proof.get("state", "MISSING_EVIDENCE"),
                "counts": {} if future else proof.get("counts", {}),
                "warnings": proof.get("notes", []) + proof.get("warnings", []),
                "scanner": proof.get("scanner"), "confirmation": proof.get("confirmation"),
                "orders_state": proof.get("orders_state"),
            },
        })
    sources = []
    for source in (market.get("sources") or []) + (recorded.get("sources") or []):
        if source not in sources:
            sources.append(source)
    warnings = list(dict.fromkeys((market.get("warnings") or []) + (recorded.get("warnings") or [])))
    if market.get("error_count"):
        warnings.append(f"{market['error_count']} market source reads failed; affected values are unavailable. Inspect source details.")
    return _json_safe({
        "schema_version": "dashboard_flow_live_v1", "session_date": session_date,
        "today": today, "available_dates": dates, "generated_at": now.isoformat(), "timezone": "Asia/Kolkata",
        "strategy": {"label": "V13-V10-G-3", "version": recorded.get("strategy_version"), "fingerprint": recorded.get("strategy_fingerprint")},
        "read_only": True, "sources": sources, "warnings": warnings, "slots": slots,
        "opening_first_minutes": market.get("opening_first_minutes") or [],
        "opening_observer": market.get("opening_observer"),
        "history": recorded.get("history"),
        "market": {**{key: market.get(key) for key in ("previous_session", "universe_state", "universe_count", "error_count", "errors", "opening_observer")},
                   "semantics": {**(market.get("semantics") or {}), "oi_change_pct": "Same-contract OI versus the previous five-minute boundary, except a source-labelled 09:20 bootstrap may compare against a first-minute candle or provisional full quote near 09:16 over the actual interval", "oi_day_change_pct": "Same-contract OI versus the previous session's 15:30 bar"}},
        "definitions": {
            "price_pct": "Price change from the exact prior-session broker daily close, with valid saved daily/closing-candle fallback. Missing or gap-filled baselines remain unavailable.",
            "price_proxy_pct": "Price change from the last real prior-session bar, explicitly a proxy, not the verified close.",
            "change5m_pct": "Price change from the exact adjacent five-minute boundary; gaps are not carried forward.",
            "gap_open_pct": "(Session opening price / verified previous-session close - 1) * 100. Fixed for the session; unavailable when either input is missing. Never substitutes the prior-real-price proxy.",
            "oi": "Open interest of the dated near-month futures contract, in source units.",
            "oi_change_pct": "Same-contract OI change from the previous five-minute bar, except at 09:20 a validated 09:16 first-minute candle or timely provisional full quote bootstraps this displayed field over the actual interval. Check oi_change5m_source and oi_change5m_interval_minutes before use.",
            "opening_context": "Separate reporting evidence: pre-open comparisons, timestamped opening OI, first-minute history, and a provisional quote near 09:16 when timely. At 09:20 a validated baseline may bootstrap the displayed OI change; its provenance and acquisition time are explicit. Strategy decisions are not rewritten.",
            "opening_first_minutes": "Independent validated futures minute records for 09:15-09:16, exposed after the 09:16 candle end. Futures OHLC is not the equity cash price. OI day % uses the same-contract previous-session 15:30 baseline when verified. Archived records acquired after 09:20 were unavailable to that live decision.",
            "oi_change_since_first_minute_pct": "At 09:20 only: same-contract completed five-minute futures OI versus validated first-minute OI at 09:16 over four minutes. When valid, this value also bootstraps the displayed OI change field; the source and actual interval distinguish it from exact five-minute changes.",
            "oi_change_since_0916_pct": "At 09:20 only: the displayed OI change from a validated 09:16 minute or a timely provisional quote near 09:16 to the completed same-contract futures bar. This matches the 09:20 OI delta display; source and actual interval distinguish the exception.",
            "strategy": "As-recorded G3 evidence; later confirmation/order outcomes are retrospective, not information available at the original signal boundary.",
        },
    })
