"""Read-only, phase-aware stock evidence for the FnO EQ ID dashboard.

This module deliberately imports no trading runtime.  Recorded decisions are
displayed, never re-executed.  Missing evidence cannot become a passed guard.
Prices, P&L and order states are observations, not interpolated minute history.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from functools import lru_cache
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30))
STRATEGY_VERSION = "FNO_V13_V10_G_RETAINED_20260914"
# Read-only display reference, attested against the canonical strategy manifest
# on 2026-10-08. A different fingerprint must not borrow these numeric defaults.
PINNED_RULE_FINGERPRINT = "123bcb8f46dac66b519db20b014f0c1b5e99683bd4011f0b62462de449b18893"
PINNED_RULE_SOURCES = {
    "fno_v13_v10_g_live_config.py": "0e836a406e1ef4b2ddbc29ce724dddfd87b7d0f21d9ac4beac3f9ea06c0f37a0",
    "fno_v13_v10_g_policy.py": "56e0a320f62109ff7d475978961ce778d0ee306f7d7875a83f70266c106005f7",
    "ai_platform/observability/feature_ledger.py": "65ca62f12faeb28fa314bcbc56c75de747bcda55bd5751cf0f979206b39ab720",
}
SLOTS = ("09:25", "09:30", "09:35", "09:40", "09:45", "09:50", "09:55", "10:00", "11:20")
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_FEATURE_ROWS = 2000
MAX_ORDER_FILES = 1000
MAX_EVENT_ROWS = 10000
BASE_GATES = (
    "gate_session_date", "gate_values_finite", "gate_oi_pair_positive", "gate_oi_increasing",
    "gate_base_oi_min", "gate_base_oi_max", "gate_base_volume", "gate_ema_long",
    "gate_price_long", "gate_ema_short", "gate_price_short", "gate_nifty_0925_short",
)
CONFIRMATION_GATES = (
    "gate_confirmation_present", "gate_confirmation_range", "gate_confirmation_direction",
    "gate_confirmation_volume", "gate_setup_exists", "gate_exact_confirmation_clock",
    "gate_setup_price", "gate_setup_oi", "gate_setup_volume", "gate_setup_body",
    "gate_setup_wick", "gate_setup_liquidity",
)
FEATURES = (
    "open", "high", "low", "close", "volume", "prev_close", "signal_close", "ema9", "ema20",
    "ema50", "price_change_pct", "oi", "prev_oi", "oi_change_pct", "volume_ratio", "traded_value",
    "confirmation_open", "confirmation_high", "confirmation_low", "confirmation_close",
    "confirmation_volume", "body_ratio", "wick_ratio", "v9_1m_volume_ratio",
    "nifty_first_bar_return_pct", "picker", "picker_value", "max_entries",
    "required_price_change_pct", "required_oi_change_pct", "required_volume_ratio",
    "required_body_ratio", "maximum_wick_ratio", "minimum_traded_value",
    "ema_alignment_bypassed", "relaxed_0925_long_base_branch",
    "maximum_base_oi_change_pct", "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio",
)

GATE_LABELS = {
    "gate_session_date": "Signal session date", "gate_values_finite": "Required indicator data",
    "gate_oi_pair_positive": "Current and previous OI", "gate_oi_increasing": "Increasing OI",
    "gate_base_oi_min": "Base OI minimum", "gate_base_oi_max": "Base OI maximum",
    "gate_base_volume": "Base 5m volume", "gate_ema_long": "LONG EMA alignment",
    "gate_ema_short": "SHORT EMA alignment", "gate_price_long": "LONG 5m price change",
    "gate_price_short": "SHORT 5m price change", "gate_nifty_0925_short": "09:25 SHORT NIFTY guard",
    "gate_confirmation_present": "Exact 1m candle available", "gate_confirmation_range": "1m OHLC validity",
    "gate_confirmation_direction": "1m directional confirmation", "gate_confirmation_volume": "1m confirmation volume",
    "gate_setup_exists": "Configured setup", "gate_exact_confirmation_clock": "Exact confirmation clock",
    "gate_setup_price": "Setup 5m price change", "gate_setup_oi": "Setup OI change",
    "gate_setup_volume": "Setup 5m volume", "gate_setup_body": "1m candle body / range",
    "gate_setup_wick": "1m adverse wick / range", "gate_setup_liquidity": "Setup traded value",
}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _display(value: Any, unit: str = "", *, exact=False) -> str:
    number = _number(value)
    if number is None:
        return "Unavailable" if value is None or unit else _text(value, 500) or "Unavailable"
    if unit == "ratio_pct":
        number *= 100.
    precision = 4 if unit == "pct" else 2 if unit in {"ratio_pct", "x", "rs"} else 0 if unit == "oi" else 4
    rendered = format(number, ".15g") if exact else format(number, f".{precision}f")
    if not exact and number and float(rendered) == 0:
        rendered = format(number, ".8g")
    return {"pct": f"{rendered}%", "ratio_pct": f"{rendered}%", "x": f"{rendered}×",
            "rs": f"₹{rendered}", "oi": rendered, "seconds": f"{rendered} seconds"}.get(unit, rendered)


@lru_cache(maxsize=16)
def _verified_source(path_text: str, size: int, modified_ns: int, expected: str) -> bool:
    # Cache is keyed by filesystem identity; changed source does not retain a pin.
    try:
        return hashlib.sha256(Path(path_text).read_bytes()).hexdigest() == expected
    except OSError:
        return False


def _pinned_sources_available(row: dict) -> bool:
    if row.get("strategy_version") != STRATEGY_VERSION or row.get("strategy_fingerprint") != PINNED_RULE_FINGERPRINT:
        return False
    for relative, expected in PINNED_RULE_SOURCES.items():
        path = Path(__file__).resolve().parent / relative
        try:
            stat = path.stat()
        except OSError:
            return False
        if not _verified_source(str(path), stat.st_size, stat.st_mtime_ns, expected):
            return False
    return True


def _source_reference(symbol: str) -> str:
    return f"PINNED_G_SOURCE: {symbol}"


def _comparison(label: str, actual: Any, operator: str, required: Any, unit="") -> dict:
    actual, required = _number(actual), _number(required)
    margin = None if actual is None or required is None else (required-actual if operator in ("<", "<=") else actual-required)
    return dict(label=label, actual=actual, operator=operator, required=required,
                unit=unit, margin=margin, strict=operator in ("<", ">"))


def _margin_text(value: Any, unit: str, strict=False) -> str:
    number = _number(value)
    if number is None:
        return "Unavailable"
    scaled = number * 100 if unit == "ratio_pct" else number
    suffix = " pp" if unit in ("pct", "ratio_pct") else "×" if unit == "x" else " price units" if unit == "rs" else ""
    sign = "+" if scaled > 0 else "−" if scaled < 0 else ""
    precision = 4 if unit == "pct" else 2
    rendered = format(abs(scaled), f".{precision}f")
    if scaled and float(rendered) == 0:
        rendered = format(abs(scaled), ".8g")
    text = f"{sign}{rendered}{suffix}"
    return text + (" (shortfall)" if number < 0 else " (strict inequality: equality fails)" if strict and number == 0 else "")


def _display_check(check: dict, *, actual_text: str, required_text: str, source: str,
                   comparisons: list[dict] | None = None, recorded_margin: Any = None,
                   recorded_margin_field: str = "") -> dict:
    """Explain arithmetic without changing any recorded decision flag."""
    checks = comparisons or []
    notes = []
    boundary_details = [f"{item['label']}: {_display(item['actual'], item['unit'], exact=True)} {item['operator']} {_display(item['required'], item['unit'], exact=True)}"
                        for item in checks if item["actual"] is not None and item["required"] is not None
                        and item["actual"] != item["required"]
                        and _display(item["actual"], item["unit"]) == _display(item["required"], item["unit"])]
    if boundary_details:
        actual_text += "; boundary detail: " + "; ".join(boundary_details)
    calculated = [item["margin"] for item in checks]
    fully_known = bool(checks) and all(value is not None for value in calculated)
    check.update(actual_text=actual_text, required_text=required_text, threshold_source=source,
                 comparisons=checks, margin_text="Unavailable", margin_source="UNAVAILABLE")
    if checks:
        texts = [_margin_text(item["margin"], item["unit"], item["strict"]) for item in checks]
        check["margin_text"] = "; ".join(f"{item['label']}: {text}" for item, text in zip(checks, texts)) if len(checks) > 1 else texts[0]
        if any(value is not None for value in calculated):
            check["margin_source"] = "DISPLAY_ARITHMETIC"
        observed_margin = _number(recorded_margin)
        if observed_margin is not None and checks[0]["margin"] is not None:
            check["margin_source"] = f"RECORDED: {recorded_margin_field}"
            if not math.isclose(observed_margin, checks[0]["margin"], abs_tol=1e-10, rel_tol=1e-10):
                notes.append("Recorded margin disagrees with displayed values; neither the margin nor the recorded decision was overwritten.")
                check["margin_text"] += f"; recorded {recorded_margin_field}: {_margin_text(observed_margin, checks[0]['unit'])}"
    elif _number(recorded_margin) is not None:
        notes.append("Recorded margin exists, but its actual value or requirement is unavailable; no shortfall is reconstructed.")
    if fully_known and check["status"] in ("PASS", "FAIL"):
        satisfied = all(value > 0 if item["strict"] else value >= 0 for item, value in zip(checks, calculated))
        if satisfied != (check["status"] == "PASS"):
            notes.append("Recorded status differs from the displayed numeric comparison. Upstream prerequisites or inconsistent evidence may explain this; recorded status is retained.")
    check["evidence_note"] = " ".join(notes)
    return check


def _scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)):
        return value[:500] if isinstance(value, str) else value
    return None


def _text(value: Any, limit: int = 250) -> str:
    return str(value)[:limit] if isinstance(value, (str, int, float)) else ""


def _flag(value: Any) -> bool | None:
    # Do not apply bool("false"), and never equate missing/zero with false.
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return None


def _status(value: Any) -> str:
    flag = _flag(value)
    return "PASS" if flag is True else "FAIL" if flag is False else "UNKNOWN"


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp.astimezone(IST) if stamp.tzinfo is not None else None
    except ValueError:
        return None


def _at(day: str, clock: str) -> datetime:
    return datetime.fromisoformat(f"{day}T{clock}:00+05:30")


def _read(path: Path, warnings: list[str]) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        if path.stat().st_size > MAX_FILE_BYTES:
            warnings.append(f"{path.name}: file exceeds safe read limit; not displayed")
            return {}
        with path.open("rb") as handle:
            raw = handle.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise ValueError("file grew beyond safe read limit")
        value = json.loads(raw.decode("utf-8-sig"))
        if not isinstance(value, dict):
            raise ValueError("expected JSON object")
        return value
    except (OSError, ValueError, UnicodeError, RecursionError) as exc:
        warnings.append(f"{path.name}: unreadable evidence ({type(exc).__name__})")
        return {}


def _parent_state(payload: dict, day: str, slot: str, phase: str, now: datetime) -> str:
    if not payload:
        due = _at(day, slot) + (timedelta(minutes=1) if phase == "1m" else timedelta())
        return "NOT_DUE" if now < due else "MISSING"
    if payload.get("session_date") != day:
        return "STALE_SESSION"
    if payload.get("strategy_version") != STRATEGY_VERSION or not payload.get("strategy_fingerprint"):
        return "INVALID_STRATEGY"
    confirmation = (_at(day, slot) + timedelta(minutes=1)).strftime("%H:%M")
    if payload.get("signal_end") != slot or payload.get("confirmation_end") != confirmation:
        return "INVALID_CLOCK"
    published = _stamp(payload.get("published_at_ist"))
    cutoff = _at(day, confirmation if phase == "1m" else slot)
    if published is None or published.date().isoformat() != day or published < cutoff:
        return "INVALID_TIMESTAMP"
    if published > now + timedelta(seconds=5):
        return "FUTURE_EVIDENCE"
    if payload.get("state") != "SUCCESS":
        return "BLOCKED"
    return "RECORDED"


def _row_state(row: dict, parent: dict, day: str, slot: str, phase: str) -> str:
    if row.get("evaluation_state") == "TELEMETRY_ERROR":
        return "TELEMETRY_ERROR"
    if row.get("session_date") != day:
        return "STALE_SESSION"
    if row.get("strategy_version") != parent.get("strategy_version") or row.get("strategy_fingerprint") != parent.get("strategy_fingerprint"):
        return "INVALID_STRATEGY"
    signal = _stamp(row.get("signal_ts"))
    confirmation = _stamp(row.get("confirmation_ts"))
    if signal != _at(day, slot) or confirmation != signal + timedelta(minutes=1):
        return "INVALID_CLOCK"
    if row.get("signal_end") != slot or row.get("confirmation_end") != confirmation.strftime("%H:%M"):
        return "INVALID_CLOCK"
    if not isinstance(row.get("tradingsymbol"), str) or not row["tradingsymbol"].strip():
        return "INVALID_SYMBOL"
    if any(isinstance(row.get(name), float) and not math.isfinite(row[name]) for name in FEATURES):
        return "INVALID_LEDGER_VALUES"
    return "RECORDED"


def _failed(row: dict) -> list[str]:
    value = row.get("failed_gates", [])
    if isinstance(value, str):
        try:
            value = json.loads(value[:10000])
        except (ValueError, RecursionError):
            return []
    return [_text(item, 100) for item in value[:50] if isinstance(item, str)] if isinstance(value, list) else []


def _check(name: str, status: str, actual: Any = None, rule: str = "", margin: Any = None, reason: str = "") -> dict:
    return dict(name=name, status=status, actual=_scalar(actual), rule=rule, margin=_scalar(margin), reason=reason,
                recorded_status=status, label=GATE_LABELS.get(name, name.replace("_", " ").capitalize()),
                actual_text=_display(_scalar(actual)), required_text=rule or "Unavailable",
                margin_text="Unavailable", threshold_source="RECORDED_EVIDENCE_DESCRIPTION",
                margin_source="UNAVAILABLE", evidence_note="", comparisons=[])


def _enhance_gate(check: dict, row: dict, pinned: bool) -> dict:
    name, side = check["name"], _text(row.get("base_side")).upper()
    relaxed_flag = _flag(row.get("relaxed_0925_long_base_branch"))
    relaxed = relaxed_flag is True
    branch_known = relaxed_flag is not None
    source = _source_reference(name) if pinned else "UNAVAILABLE: strategy fingerprint/source bundle is not pinned for numeric defaults"
    required = "Unavailable (numeric requirement not recorded or source reference unverified)"
    actual, comparisons = _display(check["actual"]), []
    margin_field = ""
    recorded_margin = None

    def scalar_gate(field, threshold, operator, unit, threshold_field=""):
        nonlocal actual, required, source, comparisons
        value = _number(row.get(field))
        actual = _display(value, unit)
        threshold = _number(threshold)
        if threshold_field:
            source = f"RECORDED: {threshold_field}" if threshold is not None else f"UNAVAILABLE: {threshold_field}"
        required = f"{operator} {_display(threshold, unit)}" if threshold is not None else "Unavailable (numeric threshold not recorded/verified)"
        comparisons = [_comparison(field, value, operator, threshold, unit)]

    base_specs = {
        "gate_base_oi_min": ("oi_change_pct", (.10 if relaxed else .05) if pinned and branch_known else None, ">=", "pct"),
        "gate_base_volume": ("volume_ratio", (1.75 if relaxed else .8) if pinned and branch_known else None, ">=", "x"),
        "gate_price_long": ("price_change_pct", (.30 if relaxed else .10) if pinned and branch_known else None, ">=", "pct"),
        "gate_price_short": ("price_change_pct", -.10 if pinned else None, "<=", "pct"),
        "gate_nifty_0925_short": ("nifty_first_bar_return_pct", -.05 if pinned else None, "<=", "pct"),
        "gate_confirmation_volume": ("v9_1m_volume_ratio", 1.20 if pinned else None, ">=", "x"),
    }
    setup_specs = {
        "gate_setup_price": ("price_change_pct", "required_price_change_pct", "margin_price_change_pct", "pct"),
        "gate_setup_oi": ("oi_change_pct", "required_oi_change_pct", "margin_oi_change_pct", "pct"),
        "gate_setup_volume": ("volume_ratio", "required_volume_ratio", "margin_volume_ratio", "x"),
        "gate_setup_body": ("body_ratio", "required_body_ratio", "margin_body_ratio", "ratio_pct"),
        "gate_setup_wick": ("wick_ratio", "maximum_wick_ratio", "margin_wick_ratio", "ratio_pct"),
        "gate_setup_liquidity": ("traded_value", "minimum_traded_value", "margin_traded_value", "rs"),
    }
    if name in base_specs:
        scalar_gate(*base_specs[name])
        if name == "gate_confirmation_volume":
            margin_field = "margin_confirmation_volume_ratio"
            if pinned:
                required += " (current volume / mean of preceding 20 completed minutes; current excluded)"
    elif name == "gate_base_oi_max":
        threshold = _number(row.get("maximum_base_oi_change_pct"))
        if threshold is not None:
            scalar_gate("oi_change_pct", threshold, "<=", "pct", "maximum_base_oi_change_pct")
        else:
            scalar_gate("oi_change_pct", (1.2 if relaxed else 1.) if pinned and branch_known else None, "<=", "pct")
    elif name in setup_specs:
        field, threshold_field, margin_field, unit = setup_specs[name]
        threshold = _number(row.get(threshold_field))
        operator = "<=" if name == "gate_setup_wick" or (name == "gate_setup_price" and side == "SHORT") else ">="
        if name == "gate_setup_price":
            threshold = -threshold if threshold is not None and side == "SHORT" else threshold
            if side not in {"LONG", "SHORT"}:
                threshold = None
        scalar_gate(field, threshold, operator, unit, threshold_field)
        if name == "gate_setup_price" and side in {"LONG", "SHORT"}:
            required += f" ({side}, raw signed 5m return)"
        if name in {"gate_setup_body", "gate_setup_wick"} and pinned:
            boundary, boundary_operator = (1., "<=") if name == "gate_setup_body" else (0., ">=")
            comparisons.append(_comparison("upper bound" if name == "gate_setup_body" else "lower bound", row.get(field), boundary_operator, boundary, unit))
            required += f" and {boundary_operator} {_display(boundary, unit)}"
            source += "; " + _source_reference("passes_selected_filters: bounded body/wick")
        if name == "gate_setup_wick":
            actual += " (upper wick for LONG)" if side == "LONG" else " (lower wick for SHORT)" if side == "SHORT" else " (side unavailable)"
    elif name in {"gate_ema_long", "gate_ema_short"}:
        actual = "; ".join(f"EMA{period}={_display(_number(row.get(f'ema{period}')), 'rs')}" for period in (9,20,50))
        if pinned:
            operator = ">" if name == "gate_ema_long" else "<"
            required = f"EMA9 {operator} EMA20 {operator} EMA50 (strict; equality fails)"
            comparisons = [_comparison("EMA9 vs EMA20", row.get("ema9"), operator, row.get("ema20"), "rs"),
                           _comparison("EMA20 vs EMA50", row.get("ema20"), operator, row.get("ema50"), "rs")]
    elif name in {"gate_oi_pair_positive", "gate_oi_increasing"}:
        actual = f"Current OI={_display(_number(row.get('oi')), 'oi')}; previous OI={_display(_number(row.get('prev_oi')), 'oi')}"
        if pinned:
            if name == "gate_oi_pair_positive":
                required = "Current OI > 0 and previous OI > 0"
                comparisons = [_comparison(key, row.get(key), ">", 0., "oi") for key in ("oi", "prev_oi")]
            else:
                required = "Current OI > previous OI (strict; equality fails)"
                comparisons = [_comparison("Current vs previous OI", row.get("oi"), ">", row.get("prev_oi"), "oi")]
    elif name == "gate_confirmation_direction":
        actual = "; ".join(f"{label}={_display(_number(row.get(key)), 'rs')}" for label, key in
                           (("1m close", "confirmation_close"), ("1m open", "confirmation_open"), ("5m signal close", "signal_close")))
        if pinned and side in {"LONG", "SHORT"}:
            operator = ">" if side == "LONG" else "<"
            required = f"{side}: 1m close {operator} 1m open AND 1m close {operator} 5m signal close (strict)"
            comparisons = [_comparison(label, row.get("confirmation_close"), operator, row.get(key), "rs") for label, key in
                           (("Close vs open", "confirmation_open"), ("Close vs signal", "signal_close"))]
        elif pinned:
            required = "LONG: close > open AND signal close; SHORT: close < both (candidate side unavailable)"
    elif name == "gate_confirmation_range":
        actual = "; ".join(f"{label}={_display(_number(row.get('confirmation_'+key)), 'rs')}" for label,key in
                           (("O","open"),("H","high"),("L","low"),("C","close")))
        if pinned:
            required = "O,H,L,C > 0; H > L; H >= O and C; L <= O and C; candle present"
            comparisons = [_comparison(label, row.get("confirmation_"+key), ">", 0., "rs") for label,key in
                           (("O > 0","open"),("H > 0","high"),("L > 0","low"),("C > 0","close"))]
            comparisons += [_comparison("H vs L", row.get("confirmation_high"), ">", row.get("confirmation_low"), "rs")]
            for key in ("open", "close"):
                comparisons.extend([_comparison("H vs "+key, row.get("confirmation_high"), ">=", row.get("confirmation_"+key), "rs"),
                                    _comparison("L vs "+key, row.get("confirmation_low"), "<=", row.get("confirmation_"+key), "rs")])
    elif name == "gate_values_finite":
        fields = ("ema9", "ema20", "ema50", "price_change_pct", "oi_change_pct", "volume_ratio", "oi", "prev_oi")
        needed = tuple(field for field in fields if not relaxed or not field.startswith("ema"))
        actual = "; ".join(f"{field}={_display(_number(row.get(field)))}" for field in needed)
        if pinned and branch_known:
            required = "Finite recorded values: " + ", ".join(needed)
    elif name == "gate_session_date":
        actual = f"Session={_text(row.get('session_date')) or 'Unavailable'}; signal={_text(row.get('signal_ts')) or 'Unavailable'}"
        if pinned:
            required = "Signal timestamp's IST calendar date equals recorded session date"
    elif name in {"gate_confirmation_present", "gate_exact_confirmation_clock"}:
        actual = f"Signal={_text(row.get('signal_ts')) or 'Unavailable'}; confirmation={_text(row.get('confirmation_ts')) or 'Unavailable'}"
        if name == "gate_confirmation_present":
            actual += "; OHLC=" + ", ".join(_display(_number(row.get('confirmation_'+key)), 'rs') for key in ("open","high","low","close"))
        stamp = _stamp(row.get("signal_ts"))
        if pinned and stamp is not None:
            required = (stamp + timedelta(minutes=1)).isoformat() + (" exact completed candle with finite OHLC" if name == "gate_confirmation_present" else " exact configured confirmation timestamp")
    elif name == "gate_setup_exists":
        actual = _text(row.get("setup_id")) or "Unavailable (no setup ID recorded)"
        if pinned:
            required = f"Configured setup for {side or 'unavailable side'} at {_text(row.get('signal_end')) or 'unavailable clock'}"
    if margin_field:
        recorded_margin = row.get(margin_field)
    check = _display_check(check, actual_text=actual, required_text=required, source=source,
                           comparisons=comparisons, recorded_margin=recorded_margin, recorded_margin_field=margin_field)
    check["rule"] = required
    return check


def _gate(row: dict, name: str, phase: str, *, pinned: bool | None = None) -> dict:
    side = _text(row.get("base_side")).upper()
    relaxed = _flag(row.get("relaxed_0925_long_base_branch")) is True
    status = _status(row.get(name))
    reason = "Recorded gate flag; no trading rule is re-executed."
    # Base rows expose both branches. A non-applicable branch is not a failure
    # of the selected side, nor should a bypass be presented as indicator pass.
    if side == "LONG" and name in {"gate_ema_short", "gate_price_short", "gate_nifty_0925_short"}:
        status, reason = "NOT_APPLICABLE", "SHORT branch is not applicable to this LONG candidate."
    elif side == "SHORT" and name in {"gate_ema_long", "gate_price_long"}:
        status, reason = "NOT_APPLICABLE", "LONG branch is not applicable to this SHORT candidate."
    elif name == "gate_nifty_0925_short" and row.get("signal_end") != "09:25":
        status, reason = "NOT_APPLICABLE", "NIFTY opening guard applies only to 09:25 SHORT."
    elif name == "gate_ema_long" and _flag(row.get("ema_alignment_bypassed")) is True:
        status, reason = "NOT_APPLICABLE", "Recorded dated-policy EMA bypass; alignment is not claimed to pass."
    rules: dict[str, tuple[Any, str, Any]] = {
        "gate_session_date": (row.get("session_date"), "Signal timestamp belongs to requested session", None),
        "gate_values_finite": (None, "Required recorded indicator values are finite", None),
        "gate_oi_pair_positive": (row.get("prev_oi"), "Current OI > 0 and previous OI > 0", None),
        "gate_oi_increasing": (row.get("oi"), f"Current OI > previous OI ({_scalar(row.get('prev_oi'))})", None),
        "gate_base_oi_min": (row.get("oi_change_pct"), f">= {0.10 if relaxed else 0.05}% (pinned base-rule reference)", None),
        "gate_base_oi_max": (row.get("oi_change_pct"), f"<= {_scalar(row.get('maximum_base_oi_change_pct'))}% recorded ceiling" if row.get("maximum_base_oi_change_pct") is not None else "Recorded base OI ceiling; numeric threshold unavailable", None),
        "gate_base_volume": (row.get("volume_ratio"), f">= {1.75 if relaxed else 0.8}x (pinned base-rule reference)", None),
        "gate_ema_long": (row.get("ema9"), "EMA9 > EMA20 > EMA50", None),
        "gate_ema_short": (row.get("ema9"), "EMA9 < EMA20 < EMA50", None),
        "gate_price_long": (row.get("price_change_pct"), f">= {0.30 if relaxed else 0.10}% previous 5m close-to-close", None),
        "gate_price_short": (row.get("price_change_pct"), "<= -0.10% previous 5m close-to-close", None),
        "gate_nifty_0925_short": (row.get("nifty_first_bar_return_pct"), "Opening NIFTY return <= -0.05%", None),
        "gate_confirmation_present": (row.get("confirmation_ts"), "Exact completed signal + 1 minute candle exists", None),
        "gate_confirmation_range": (None, "Valid positive OHLC; high > low; open/close within range", None),
        "gate_confirmation_direction": (row.get("confirmation_close"), "LONG: close > open and 5m close; SHORT: close < both", None),
        "gate_confirmation_volume": (row.get("v9_1m_volume_ratio"), ">= 1.20x prior completed-minute volume (current excluded)", row.get("margin_confirmation_volume_ratio")),
        "gate_setup_exists": (row.get("setup_id"), "Configured dated setup exists for side and signal slot", None),
        "gate_exact_confirmation_clock": (row.get("confirmation_ts"), "Exact configured completed confirmation clock", None),
    }
    setups = {
        "gate_setup_price": ("price_change_pct", "required_price_change_pct", "margin_price_change_pct", "% signed directional move >="),
        "gate_setup_oi": ("oi_change_pct", "required_oi_change_pct", "margin_oi_change_pct", "% OI change >="),
        "gate_setup_volume": ("volume_ratio", "required_volume_ratio", "margin_volume_ratio", "x 5m volume >="),
        "gate_setup_body": ("body_ratio", "required_body_ratio", "margin_body_ratio", "body/range >="),
        "gate_setup_wick": ("wick_ratio", "maximum_wick_ratio", "margin_wick_ratio", "side wick/range <="),
        "gate_setup_liquidity": ("traded_value", "minimum_traded_value", "margin_traded_value", "traded value >="),
    }
    if name in setups:
        actual, threshold, margin, label = setups[name]
        rules[name] = (row.get(actual), f"{label} {_scalar(row.get(threshold))}" if row.get(threshold) is not None else "Numeric threshold not recorded", row.get(margin))
    actual, rule, margin = rules.get(name, (None, "Recorded gate", None))
    check = _check(name, status, actual, rule, margin, reason)
    check["recorded_status"] = _status(row.get(name))
    return _enhance_gate(check, row, _pinned_sources_available(row) if pinned is None else pinned)


def _invalidate_check(check: dict, reason: str) -> dict:
    return {**check, "status": "UNKNOWN", "reason": reason,
            "margin_text": "Unavailable (invalid evidence)", "margin_source": "UNAVAILABLE",
            "comparisons": [], "evidence_note": "Evidence validation failed; numeric observations are unverified and no shortfall is asserted."}


def _feature_row(raw: dict, parent: dict, day: str, slot: str, phase: str, source: str, parent_state: str, index: int) -> dict:
    state = _row_state(raw, parent, day, slot, phase)
    # BLOCKED may still have valid feature evaluations; label them, but never
    # claim a complete selection. All other invalid parent identity suppresses flags.
    if parent_state not in {"RECORDED", "BLOCKED"}:
        state = parent_state
    usable = state == "RECORDED"
    side = _text(raw.get("base_side"))
    symbol = _text(raw.get("tradingsymbol"), 100)
    minute = slot if phase == "5m" else (_at(day, slot) + timedelta(minutes=1)).strftime("%H:%M")
    pinned = _pinned_sources_available(raw)
    checks = [_gate(raw, gate, phase, pinned=pinned) for gate in (BASE_GATES if phase == "5m" else CONFIRMATION_GATES)]
    if not usable:
        checks = [_invalidate_check(check, f"Evidence validation: {state}") for check in checks]
    checks.append(_check("slot_complete", "FAIL" if usable and parent_state == "BLOCKED" else "PASS" if usable else "UNKNOWN", parent.get("state"), "Recorded slot state must be SUCCESS; valid individual filters do not override an incomplete slot"))
    decision = "UNKNOWN"
    if usable and phase == "5m":
        long_pass, short_pass = _flag(raw.get("base_long_pass")), _flag(raw.get("base_short_pass"))
        decision = "BASE_PASS" if True in (long_pass, short_pass) else "BASE_REJECTED" if long_pass is False and short_pass is False else "UNKNOWN"
        checks.append(_check("confirmation_stage", "NOT_EVALUATED", rule="1m confirmation has not been evaluated in a 5m scanner row"))
    elif usable:
        ids = parent.get("selected_signal_ids")
        selected = False
        if isinstance(ids, list):
            # Stock names can include underscores; split only fixed prefixes and final hash.
            for value in ids:
                parts = str(value).split("_", 3)
                if len(parts) == 4:
                    selected_symbol = parts[3].rsplit("_", 1)[0]
                    selected |= parts[0] == day.replace("-", "") and parts[1] == minute.replace(":", "") and parts[2] == side and selected_symbol == symbol
        setup_pass = _flag(raw.get("setup_filter_pass"))
        if parent_state == "BLOCKED":
            decision = "BLOCKED_INCOMPLETE_SLOT"
        elif selected:
            decision = "SELECTED"
        elif setup_pass is True and isinstance(ids, list):
            decision = "FILTER_PASS_NOT_SELECTED"
        elif setup_pass is False:
            decision = "CONFIRMATION_OR_SETUP_REJECTED"
        checks.append(_check("final_selection", "PASS" if selected and parent_state == "RECORDED" else "NOT_EVALUATED" if parent_state == "BLOCKED" else "FAIL" if isinstance(ids, list) else "UNKNOWN", actual=selected if isinstance(ids, list) else None, rule="Recorded selected_signal_ids, not inferred from indicator gates", reason="Filter pass alone does not guarantee ranking/quota selection; exact exclusion reason is not recorded." if decision == "FILTER_PASS_NOT_SELECTED" else ""))
    # The frontend uses the compact display fields. Exact feature values already
    # live in indicators; do not resend the internal compound-comparison tree
    # for every one of tens of thousands of checks on each dashboard refresh.
    for check in checks:
        if check.get("comparisons"):
            check.pop("comparisons")
    return dict(id=f"g:{phase}:{slot}:{index}:{symbol}", strategy="V13-V10-G", session_date=day,
                symbol=symbol, side=side, setup_id=_text(raw.get("setup_id")), signal_time=slot,
                minute=minute, stage="5M_BASE_FILTERS" if phase == "5m" else "1M_CONFIRMATION_SETUP",
                decision=decision, evidence_state=parent_state if usable and parent_state == "BLOCKED" else state,
                source=source, indicators={name: _scalar(raw.get(name)) for name in FEATURES if name in raw},
                checks=checks, first_failed_gate=_text(raw.get("first_failed_gate")) if phase == "1m" else next((gate for gate in _failed(raw) if gate in BASE_GATES), ""),
                failed_gates=[gate for gate in _failed(raw) if gate in (BASE_GATES if phase == "5m" else CONFIRMATION_GATES)],
                published_at_ist=_text(parent.get("published_at_ist")), run_id=_text(parent.get("run_id")),
                strategy_fingerprint=_text(parent.get("strategy_fingerprint")), ledger_row_sha256=_text(raw.get("ledger_row_sha256")))


def _read_phase(root: Path, day: str, slot: str, phase: str, now: datetime, warnings: list[str]) -> tuple[list[dict], dict, dict]:
    clock = slot if phase == "5m" else (_at(day, slot) + timedelta(minutes=1)).strftime("%H:%M")
    path = root / ("scanner_5m" if phase == "5m" else "confirmation_1m") / day / f"slot_{clock.replace(':', '')}.json"
    payload = _read(path, warnings)
    state = _parent_state(payload, day, slot, phase, now)
    raw_rows = payload.get("feature_evaluations")
    if raw_rows is not None and not isinstance(raw_rows, list):
        state, raw_rows = "INVALID_LEDGER", []
    if raw_rows is None:
        raw_rows = []
        if state == "RECORDED":
            state = "LEDGER_UNAVAILABLE"
    if "feature_evaluation_count" in payload and payload["feature_evaluation_count"] != len(raw_rows):
        state = "LEDGER_COUNT_MISMATCH"
    checksum = payload.get("feature_evaluations_sha256")
    if checksum and raw_rows:
        try:
            actual = hashlib.sha256(json.dumps(raw_rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
            if actual != checksum:
                state = "CHECKSUM_MISMATCH"
        except (ValueError, TypeError):
            state = "INVALID_LEDGER_VALUES"
    if len(raw_rows) > MAX_FEATURE_ROWS:
        warnings.append(f"{path.name}: feature rows truncated at {MAX_FEATURE_ROWS}")
    rows = [_feature_row(row, payload, day, slot, phase, str(path.relative_to(root)), state, i)
            for i, row in enumerate(raw_rows[:MAX_FEATURE_ROWS]) if isinstance(row, dict)]
    if phase == "5m":
        quality = payload.get("raw_data_quality", [])
        quality = quality if isinstance(quality, list) else []
        quality_by_symbol = {(_text(item.get("symbol")), _text(item.get("source"))): item for item in quality[:MAX_FEATURE_ROWS * 2] if isinstance(item, dict)}
        for result, raw in zip(rows, (row for row in raw_rows[:MAX_FEATURE_ROWS] if isinstance(row, dict))):
            for label, symbol, source in (("equity", result["symbol"], "NSE_EQUITY_5M_LIVE"), ("futures_oi", raw.get("futures_tradingsymbol"), "NFO_FUTURE_5M_LIVE")):
                entry = quality_by_symbol.get((symbol, source), {})
                valid = entry.get("session_date") == day and entry.get("signal_end") == slot and _stamp(entry.get("timestamp_max")) == _at(day, slot) and result["evidence_state"] in {"RECORDED", "BLOCKED"}
                result["checks"].append(_check(f"{label}_source_quality", _status(entry.get("usable")) if valid else "UNKNOWN", entry.get("timestamp_max"), "Recorded source quality usable flag; source timestamp shown separately", reason="Recorded raw-data quality audit" if valid else "No valid dated source-quality audit"))
                if entry:
                    result["indicators"][f"{label}_latest_completed_at"] = _scalar(entry.get("timestamp_max"))
                    result["indicators"][f"{label}_source_quality"] = _scalar(entry.get("status"))
    invalid_rows = sum(row["evidence_state"] not in {"RECORDED", "BLOCKED"} for row in rows)
    if invalid_rows:
        warnings.append(f"{path.name}: {invalid_rows} stock rows have unavailable/invalid evidence")
    coverage = {"state": state, "rows": len(rows), "reported_rows": _scalar(payload.get("feature_evaluation_count")),
                "contracts_expected": _scalar(payload.get("contracts_expected")), "contracts_evaluated": _scalar(payload.get("contracts_evaluated")),
                "candidates": _scalar(payload.get("candidate_count")), "confirmation_bars": _scalar(payload.get("confirmation_bars")),
                "selected_long": _scalar(payload.get("selected_long")), "selected_short": _scalar(payload.get("selected_short")),
                "published_at_ist": _text(payload.get("published_at_ist")),
                "skipped_no_candle": _scalar(payload.get("contracts_skipped_no_candle")),
                "unexpected_missing": _scalar(payload.get("contracts_unexpected_missing")),
                "ineligible_no_candle": _scalar(payload.get("ineligible_no_candle_count")),
                "source": _text(payload.get("confirmation_source" if phase == "1m" else "price_volume_indicator_source"))}
    # Stocks with no completed candle are not present in the feature ledger.
    omitted = payload.get("ineligible_no_candle_symbols" if phase == "1m" else "unexpected_missing_symbols", [])
    if isinstance(omitted, list) and state in {"RECORDED", "BLOCKED"}:
        for i, symbol in enumerate(omitted[:MAX_FEATURE_ROWS]):
            rows.append(dict(id=f"g:{phase}:{slot}:missing:{i}", strategy="V13-V10-G", session_date=day,
                             symbol=_text(symbol, 100), side="", setup_id="", signal_time=slot, minute=clock,
                             stage="DATA_COVERAGE", decision="NO_CANDLE" if phase == "1m" else "MISSING_SOURCE_DATA",
                             evidence_state="UNAVAILABLE", source=str(path.relative_to(root)), indicators={},
                             checks=[_check("completed_candle_available", "FAIL", False, "Completed real source candle required", reason="Recorded missing/no-candle symbol; not a strategy rejection.")]))
    skipped = payload.get("skipped_no_candle_contracts", [])
    if phase == "5m" and isinstance(skipped, list) and state in {"RECORDED", "BLOCKED"}:
        for i, item in enumerate(skipped[:MAX_FEATURE_ROWS]):
            if not isinstance(item, dict):
                continue
            rows.append(dict(id=f"g:5m:{slot}:no-candle:{i}", strategy="V13-V10-G", session_date=day,
                             symbol=_text(item.get("equity_symbol") or item.get("underlying"), 100), side="", setup_id="", signal_time=slot, minute=clock,
                             stage="DATA_COVERAGE", decision="VERIFIED_NO_FUTURES_CANDLE", evidence_state="UNAVAILABLE", source=str(path.relative_to(root)),
                             indicators={"futures_tradingsymbol": _scalar(item.get("futures_tradingsymbol"))},
                             checks=[_check("futures_oi_available", "FAIL", False, "Exact futures OI candle required", reason=_text(item.get("reason")))]))
    return rows, coverage, payload


def _feed_evidence(fno_root: Path, day: str, slot: str, scan: dict, confirmation: dict, rows: list[dict], now: datetime, warnings: list[str]) -> dict:
    """Read the exact scanner-keyed marker; never follow paths stored in data."""
    clock = (_at(day, slot) + timedelta(minutes=1)).strftime("%H:%M")
    if not scan:
        return {"state": "UNAVAILABLE", "checks": []}
    scanner_hash = hashlib.sha256(json.dumps(scan, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode()).hexdigest()
    path = fno_root / "equity_1m_slot_ready" / "v6" / day / f"slot_{clock.replace(':', '')}_{scanner_hash[:16]}.json"
    marker = _read(path, warnings)
    state = _parent_state(marker, day, slot, "1m", now)
    if marker and (marker.get("strategy_fingerprint") != scan.get("strategy_fingerprint") or marker.get("scanner_snapshot_sha256") != scanner_hash):
        state = "INVALID_STRATEGY"
    expected = confirmation.get("confirmation_feed_marker_sha256")
    if marker and expected:
        actual = hashlib.sha256(json.dumps(marker, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode()).hexdigest()
        if actual != expected:
            state = "CHECKSUM_MISMATCH"
    checks = [
        _check("durable_feed_complete", _status(marker.get("complete")) if state in {"RECORDED", "BLOCKED"} else "UNKNOWN", marker.get("written_count"), "All candidates written or verified no-candle; recorded complete flag"),
        _check("feed_within_deadline", _status(marker.get("within_deadline")) if state in {"RECORDED", "BLOCKED"} else "UNKNOWN", marker.get("published_at_ist"), f"Published by {_text(marker.get('deadline_ist')) or 'recorded deadline unavailable'}"),
    ]
    observations = marker.get("observation_history", {})
    for row in rows:
        if row.get("stage") != "1M_CONFIRMATION_SETUP":
            continue
        row["checks"].extend(checks if row["evidence_state"] in {"RECORDED", "BLOCKED"} else [_invalidate_check(check, "Feature row evidence is invalid") for check in checks])
        row["indicators"]["durable_feed_state"] = state
        row["indicators"]["durable_feed_published_at"] = _scalar(marker.get("published_at_ist"))
        history = observations.get(row["symbol"], []) if isinstance(observations, dict) else []
        if isinstance(history, list) and history and isinstance(history[-1], dict):
            row["indicators"]["bar_last_observed_at"] = _scalar(history[-1].get("observed_at_ist"))
            row["indicators"]["bar_observation_state"] = _scalar(history[-1].get("state"))
    return dict(state=state, checks=checks, candidate_count=_scalar(marker.get("candidate_count")), written_count=_scalar(marker.get("written_count")),
                verified_no_candle_count=_scalar(marker.get("verified_no_candle_count")), slot_ist=_scalar(marker.get("slot_ist")),
                published_at_ist=_scalar(marker.get("published_at_ist")), deadline_ist=_scalar(marker.get("deadline_ist")))


def _order_rows(root: Path, day: str, now: datetime, warnings: list[str]) -> list[dict]:
    result = []
    for mode in ("PAPER", "LIVE"):
        directory = root / "orders" / mode / day
        paths = sorted(directory.glob("*.json")) if directory.is_dir() else []
        if len(paths) > MAX_ORDER_FILES:
            warnings.append(f"{mode}: order snapshots truncated at {MAX_ORDER_FILES}")
        for path in paths[:MAX_ORDER_FILES]:
            row = _read(path, warnings)
            stamp = _stamp(row.get("updated_at_ist"))
            if not row or row.get("session_date") != day or stamp is None or stamp.date().isoformat() != day:
                warnings.append(f"{path.name}: wrong-session or missing order timestamp; not displayed")
                continue
            if row.get("strategy_version") != STRATEGY_VERSION or not row.get("strategy_fingerprint") or stamp > now + timedelta(seconds=5):
                warnings.append(f"{path.name}: invalid order identity/future timestamp; not displayed")
                continue
            checks = []
            admission = row.get("paper_portfolio_admission")
            if isinstance(admission, dict):
                checks.append(_check("paper_capital_admission", _status(admission.get("allowed")), admission.get("available_capital_rs"), f"Required capital: {_scalar(admission.get('required_capital_rs'))}", reason=_text(admission.get("reason"))))
            blocker = _text(row.get("last_entry_blocker_reason") or row.get("first_entry_blocker_reason"))
            checks.append(_check("recorded_entry_blocker", "FAIL" if blocker else "UNKNOWN", blocker or None, "Recorded entry blocker, not a complete guard audit", reason=blocker or "No blocker field does not prove every entry guard passed."))
            checks.append(_check("entry_deadline", "UNKNOWN", row.get("entry_activation_deadline_ist"), "Recorded absolute deadline; not recomputed against current time", reason="Snapshot is latest known order state, not a minute-by-minute guard decision."))
            indicators = {name: _scalar(row.get(name)) for name in ("trigger_price", "last_price", "entry_price", "entry_at_ist", "stop_price", "target_price", "stop_pct", "active_stop_pct", "target_pct", "quantity", "net_pnl_rs", "entry_activation_deadline_ist", "entry_order_activated_at_ist", "status", "status_reason", "exit_reason", "exit_price", "exit_at_ist", "first_entry_blocker_reason", "last_entry_blocker_reason", "entry_terminal_cause", "execution_error_count", "stop_policy", "stop_tighten_due_at_ist") if name in row}
            result.append(dict(id=f"g:order:{mode}:{path.stem}", strategy="V13-V10-G", session_date=day, symbol=_text(row.get("tradingsymbol"), 100), side=_text(row.get("side")), setup_id=_text(row.get("setup_id")), signal_time=_text(row.get("signal_end")), minute=stamp.strftime("%H:%M"), stage=f"{mode}_ORDER_SNAPSHOT", decision=_text(row.get("status_reason") or row.get("status")), evidence_state="RECORDED_SNAPSHOT", source=str(path.relative_to(root)), indicators=indicators, checks=checks, published_at_ist=stamp.isoformat()))
    return result


def _event_rows(root: Path, day: str, now: datetime, warnings: list[str]) -> list[dict]:
    result = []
    for mode in ("PAPER", "LIVE"):
        path = root / "order_events" / mode / f"{day}.jsonl"
        if not path.is_file():
            continue
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                warnings.append(f"{mode} order events exceed safe read limit; use raw event archive")
                continue
            with path.open(encoding="utf-8-sig") as handle:
                for index, line in enumerate(handle):
                    if index >= MAX_EVENT_ROWS:
                        warnings.append(f"{mode}: event rows truncated at {MAX_EVENT_ROWS}")
                        break
                    try:
                        event = json.loads(line)
                        context, data = event.get("context", {}), event.get("data", {})
                        stamp = _stamp(event.get("timestamp_utc"))
                        if not isinstance(context, dict) or not isinstance(data, dict) or context.get("session_date") != day or stamp is None or stamp.date().isoformat() != day:
                            raise ValueError("event identity")
                        if context.get("strategy_version") != STRATEGY_VERSION or not context.get("strategy_fingerprint") or stamp > now + timedelta(seconds=5):
                            raise ValueError("event strategy/time")
                        symbol = _text(data.get("tradingsymbol"), 100)
                        if not symbol:
                            continue
                        reason = _text(data.get("reason"))
                        result.append(dict(id=f"g:event:{mode}:{index}", strategy="V13-V10-G", session_date=day, symbol=symbol, side=_text(data.get("side")), setup_id="", signal_time="", minute=stamp.strftime("%H:%M"), stage=f"{mode}_ORDER_EVENT", decision=reason or _text(event.get("event_type")), evidence_state="RECORDED_EVENT", source=str(path.relative_to(root)), indicators={name: _scalar(data.get(name)) for name in ("state_before", "state_after", "entry_price", "exit_price", "net_pnl_rs", "quantity", "entry_terminal_cause", "execution_error_count") if name in data}, checks=[_check("entry_guard_audit", "UNKNOWN", reason, "Recorded state transition only", reason="This event does not contain a full per-guard evaluation; no guard pass is inferred from a fill.")], published_at_ist=stamp.isoformat()))
                    except (ValueError, TypeError, AttributeError, RecursionError):
                        warnings.append(f"{mode} events: invalid record {index + 1}; not displayed")
        except (OSError, UnicodeError):
            warnings.append(f"{mode} order events are unreadable")
    return result


def build_monitor_detail(fno_root: Path, session_date: str, *, now_ist: datetime | None = None) -> dict[str, Any]:
    """Return bounded, dated observations. Does not fetch, write, or trade."""
    if not isinstance(session_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", session_date):
        raise ValueError("session_date must be YYYY-MM-DD")
    date.fromisoformat(session_date)
    now = now_ist or datetime.now(IST)
    if now.tzinfo is None:
        raise ValueError("now_ist must be timezone-aware")
    now = now.astimezone(IST)
    root = Path(fno_root) / "v13_v10_g_live"
    warnings: list[str] = []
    coverage, rows_5m, rows_1m = [], [], []
    for slot in SLOTS:
        scanner, scan_coverage, scan = _read_phase(root, session_date, slot, "5m", now, warnings)
        confirmation, conf_coverage, conf = _read_phase(root, session_date, slot, "1m", now, warnings)
        conf_coverage["durable_feed"] = _feed_evidence(Path(fno_root), session_date, slot, scan, conf, confirmation, now, warnings)
        if scan and conf and scan.get("strategy_fingerprint") != conf.get("strategy_fingerprint"):
            warnings.append(f"{slot}: scanner and confirmation fingerprints differ")
            conf_coverage["state"] = "INVALID_STRATEGY"
            for row in confirmation:
                row.update(evidence_state="INVALID_STRATEGY", decision="UNKNOWN")
                row["checks"] = [_invalidate_check(check, "Scanner/confirmation strategy identity mismatch") for check in row["checks"]]
        rows_5m.extend(scanner)
        rows_1m.extend(confirmation)
        coverage.append(dict(slot=slot, scanner_state=scan_coverage["state"], confirmation_state=conf_coverage["state"], scanner_rows=scan_coverage["rows"], confirmation_rows=conf_coverage["rows"], scanner=scan_coverage, confirmation=conf_coverage))
    rows_1m.extend(_event_rows(root, session_date, now, warnings))
    rows_1m.extend(_order_rows(root, session_date, now, warnings))
    warnings.append("Order evidence is recorded transitions/latest snapshots, not a complete tick-by-tick or per-minute guard history. Missing checks remain UNKNOWN.")
    warnings.append("Recorded flags determine PASS/FAIL. Recorded setup thresholds take priority; numeric base defaults require matching strategy/source SHA256 pins. Display arithmetic explains margins, never re-executes or changes trading decisions. Positive displayed margin may not clear an upstream prerequisite.")
    due_states = [item[key] for item in coverage for key in ("scanner_state", "confirmation_state") if item[key] != "NOT_DUE"]
    state = "RECORDED" if due_states and all(item == "RECORDED" for item in due_states) else "PARTIAL" if rows_5m or rows_1m else "NO_EVIDENCE"
    return dict(schema_version="fno_eq_id_monitor_detail_v1", session_date=session_date, generated_at_ist=now.isoformat(), state=state,
                rule_reference_provenance={"strategy_fingerprint": PINNED_RULE_FINGERPRINT, "source_sha256": PINNED_RULE_SOURCES},
                warnings=list(dict.fromkeys(warnings))[:100], coverage=coverage, rows_5m=rows_5m,
                rows_1m=sorted(rows_1m, key=lambda row: (row["minute"], row["stage"], row["symbol"], row["id"])))
