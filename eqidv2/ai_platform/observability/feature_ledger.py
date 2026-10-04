"""Explainable, versioned feature and gate ledger for V13-V10-G.

One ledger row represents one evaluated symbol at one signal slot, including
rejections.  It is intentionally wider than the candidate signal file: the
candidate file answers *what passed*, while this ledger answers *why every
member of the observed universe passed or failed*.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from ai_platform.observability.data_quality import (
    canonical_frame_sha256,
    canonical_payload_sha256,
    canonical_row_sha256,
)


FEATURE_LEDGER_SCHEMA = "v13_v10_g_feature_ledger_v1"
FEATURE_LEDGER_MANIFEST_SCHEMA = "v13_v10_g_feature_ledger_manifest_v1"

_FEATURE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "signal_close",
    "prev_close",
    "ema9",
    "ema20",
    "ema50",
    "price_change_pct",
    "oi",
    "prev_oi",
    "oi_change_pct",
    "volume_ratio",
    "traded_value",
    "confirmation_open",
    "confirmation_high",
    "confirmation_low",
    "confirmation_close",
    "confirmation_volume",
    "body_ratio",
    "v9_1m_upper_wick_ratio",
    "v9_1m_lower_wick_ratio",
    "v9_1m_volume_ratio",
    "nifty_first_bar_return_pct",
)


def _value(row: Mapping[str, Any], name: str, *aliases: str) -> Any:
    for candidate in (name, *aliases):
        value = row.get(candidate)
        if value is not None and value is not pd.NA and value is not pd.NaT:
            return value
    return None


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def _json_number(value: Any) -> float | None:
    number = _number(value)
    return number if math.isfinite(number) else None


def _optional_bool(value: Any, default: bool = True) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return default
    try:
        if bool(pd.isna(value)):
            return default
    except (TypeError, ValueError):
        pass
    return bool(value)


def _clock(value: Any) -> str:
    if value is None or value is pd.NaT:
        return ""
    try:
        return pd.Timestamp(value).strftime("%H:%M")
    except (TypeError, ValueError):
        text = str(value)
        return text[:5] if len(text) >= 5 else text


def _timestamp_text(value: Any) -> str:
    if value is None or value is pd.NaT:
        return ""
    try:
        return pd.Timestamp(value).isoformat()
    except (TypeError, ValueError):
        return str(value)


def _gate_result(value: bool) -> bool:
    return bool(value)


def evaluate_v13_v10_g_base_row(
    row: Mapping[str, Any],
    *,
    nifty_return: float | None = None,
    strategy_version: str = "",
    strategy_fingerprint: str = "",
    input_slice_sha256: str | None = None,
) -> dict[str, Any]:
    """Explain V13-V10-G gates for one full-universe feature row.

    The production configuration is imported lazily so this module remains
    safe to use for raw-data tooling that does not start a live runtime.
    """

    import fno_v13_v10_g_live_config as config

    values = {name: _value(row, name) for name in _FEATURE_FIELDS}
    values["confirmation_open"] = _value(row, "confirmation_open", "confirm_open")
    values["confirmation_high"] = _value(row, "confirmation_high", "confirm_high")
    values["confirmation_low"] = _value(row, "confirmation_low", "confirm_low")
    values["confirmation_close"] = _value(row, "confirmation_close", "confirm_close")
    values["confirmation_volume"] = _value(row, "confirmation_volume", "confirm_volume")
    supplied_nifty = (
        nifty_return if nifty_return is not None
        else values.get("nifty_first_bar_return_pct")
    )
    values["nifty_first_bar_return_pct"] = supplied_nifty
    numeric = {name: _number(value) for name, value in values.items()}

    signal_ts = _value(row, "signal_ts", "signal_timestamp")
    confirmation_ts = _value(row, "confirmation_ts", "confirmation_timestamp")
    signal_end = str(_value(row, "signal_end") or _clock(signal_ts))
    confirmation_end = str(
        _value(row, "confirmation_end") or _clock(confirmation_ts)
    )
    symbol = str(_value(row, "tradingsymbol", "symbol") or "").strip().upper()
    futures_symbol = str(_value(row, "futures_tradingsymbol") or "").strip().upper()

    base_names = (
        "ema9", "ema20", "ema50", "price_change_pct", "oi_change_pct",
        "volume_ratio", "oi", "prev_oi",
    )
    finite = all(math.isfinite(numeric[name]) for name in base_names)
    oi_pair_positive = finite and numeric["prev_oi"] > 0 and numeric["oi"] > 0
    oi_increasing = oi_pair_positive and numeric["oi"] > numeric["prev_oi"]
    oi_min = finite and numeric["oi_change_pct"] >= config.BASE_OI_CHANGE_PCT
    oi_max = finite and numeric["oi_change_pct"] <= config.MAX_OI_CHANGE_PCT
    volume_base = finite and numeric["volume_ratio"] >= config.BASE_VOLUME_RATIO
    ema_long = finite and numeric["ema9"] > numeric["ema20"] > numeric["ema50"]
    ema_short = finite and numeric["ema9"] < numeric["ema20"] < numeric["ema50"]
    price_long = finite and numeric["price_change_pct"] >= config.BASE_PRICE_CHANGE_PCT
    price_short = finite and numeric["price_change_pct"] <= -config.BASE_PRICE_CHANGE_PCT
    nifty_context = numeric["nifty_first_bar_return_pct"]
    nifty_short = (
        signal_end != "09:25"
        or (
            math.isfinite(nifty_context)
            and nifty_context <= config.NIFTY_FIRST_BAR_MAX_RETURN_PCT
        )
    )
    common_base = finite and oi_pair_positive and oi_increasing and oi_min and oi_max and volume_base
    base_long = common_base and ema_long and price_long
    base_short = common_base and ema_short and price_short and nifty_short
    base_side = "LONG" if base_long else "SHORT" if base_short else ""

    confirmation_present = bool(
        _optional_bool(_value(row, "v9_exact_confirmation_present"))
        and all(math.isfinite(numeric[name]) for name in (
            "confirmation_open", "confirmation_high", "confirmation_low", "confirmation_close"
        ))
    )
    o = numeric["confirmation_open"]
    high = numeric["confirmation_high"]
    low = numeric["confirmation_low"]
    close = numeric["confirmation_close"]
    confirmation_range = bool(
        confirmation_present
        and min(o, high, low, close) > 0
        and high > low
        and high >= max(o, close)
        and low <= min(o, close)
    )
    signal_close = numeric["signal_close"]
    confirmation_direction = bool(
        confirmation_range
        and math.isfinite(signal_close)
        and (
            (base_side == "LONG" and close > o and close > signal_close)
            or (base_side == "SHORT" and close < o and close < signal_close)
        )
    )
    confirmation_volume = bool(
        math.isfinite(numeric["v9_1m_volume_ratio"])
        and numeric["v9_1m_volume_ratio"] >= config.MIN_CONFIRMATION_VOLUME_RATIO
    )
    strict_signal = bool(base_side and confirmation_present and confirmation_range and confirmation_direction)

    body_ratio = numeric["body_ratio"]
    if not math.isfinite(body_ratio) and confirmation_range:
        body_ratio = abs(close - o) / (high - low)
    upper_wick = numeric["v9_1m_upper_wick_ratio"]
    lower_wick = numeric["v9_1m_lower_wick_ratio"]
    if confirmation_range:
        if not math.isfinite(upper_wick):
            upper_wick = (high - max(o, close)) / (high - low)
        if not math.isfinite(lower_wick):
            lower_wick = (min(o, close) - low) / (high - low)
    wick_ratio = upper_wick if base_side == "LONG" else lower_wick if base_side == "SHORT" else math.nan

    setup = config.setup_for(signal_end, base_side) if base_side else None
    signed_move = numeric["price_change_pct"] * (1 if base_side == "LONG" else -1)
    setup_gates: dict[str, bool] = {
        "gate_setup_exists": setup is not None,
        "gate_exact_confirmation_clock": bool(
            setup
            and confirmation_end == setup.confirmation_end
            and _clock(signal_ts) == setup.signal_end
            and _clock(confirmation_ts) == setup.confirmation_end
        ),
        "gate_setup_price": bool(setup and math.isfinite(signed_move) and signed_move >= setup.price_change_pct),
        "gate_setup_oi": bool(setup and math.isfinite(numeric["oi_change_pct"]) and numeric["oi_change_pct"] >= setup.oi_change_pct),
        "gate_setup_volume": bool(setup and math.isfinite(numeric["volume_ratio"]) and numeric["volume_ratio"] >= setup.volume_ratio),
        "gate_setup_body": bool(setup and math.isfinite(body_ratio) and setup.body_ratio <= body_ratio <= 1),
        "gate_setup_wick": bool(setup and math.isfinite(wick_ratio) and 0 <= wick_ratio <= setup.max_wick_ratio),
        "gate_setup_liquidity": bool(setup and math.isfinite(numeric["traded_value"]) and numeric["traded_value"] >= setup.min_traded_value),
        "gate_confirmation_volume": confirmation_volume,
    }
    candidate = {
        **dict(row),
        "side": base_side,
        "signal_timestamp": _timestamp_text(signal_ts),
        "confirmation_timestamp": _timestamp_text(confirmation_ts),
        "v9_1m_feature_ts": _timestamp_text(_value(row, "v9_1m_feature_ts") or confirmation_ts),
        "confirmed": confirmation_direction,
        "body_ratio": body_ratio,
        "wick_ratio": wick_ratio,
        "nifty_first_bar_return_pct": nifty_context,
    }
    setup_filter_pass = bool(
        strict_signal and setup is not None and config.passes_selected_filters(candidate, setup)
    )

    gates = {
        "gate_values_finite": finite,
        "gate_oi_pair_positive": oi_pair_positive,
        "gate_oi_increasing": oi_increasing,
        "gate_base_oi_min": oi_min,
        "gate_base_oi_max": oi_max,
        "gate_base_volume": volume_base,
        "gate_ema_long": ema_long,
        "gate_price_long": price_long,
        "gate_ema_short": ema_short,
        "gate_price_short": price_short,
        "gate_nifty_0925_short": nifty_short,
        "gate_confirmation_present": confirmation_present,
        "gate_confirmation_range": confirmation_range,
        "gate_confirmation_direction": confirmation_direction,
        **setup_gates,
    }
    applicable_gate_order = [
        "gate_values_finite", "gate_oi_pair_positive", "gate_oi_increasing",
        "gate_base_oi_min", "gate_base_oi_max", "gate_base_volume",
    ]
    if base_side == "LONG" or (ema_long or price_long):
        applicable_gate_order.extend(["gate_ema_long", "gate_price_long"])
    elif base_side == "SHORT" or (ema_short or price_short):
        applicable_gate_order.extend(["gate_ema_short", "gate_price_short", "gate_nifty_0925_short"])
    else:
        applicable_gate_order.extend(["gate_ema_long", "gate_price_long", "gate_ema_short", "gate_price_short"])
    if base_side:
        applicable_gate_order.extend([
            "gate_confirmation_present", "gate_confirmation_range",
            "gate_confirmation_direction", "gate_confirmation_volume",
            "gate_setup_exists", "gate_exact_confirmation_clock", "gate_setup_price",
            "gate_setup_oi", "gate_setup_volume", "gate_setup_body",
            "gate_setup_wick", "gate_setup_liquidity",
        ])
    failed = [name for name in applicable_gate_order if not gates[name]]

    feature_payload = {
        name: _json_number(value) for name, value in values.items()
    }
    feature_payload.update(
        signal_ts=_timestamp_text(signal_ts),
        confirmation_ts=_timestamp_text(confirmation_ts),
        symbol=symbol,
        futures_symbol=futures_symbol,
    )
    picker_value = None
    if setup and setup_filter_pass:
        picker_candidate = {**candidate, "body_ratio": body_ratio, "wick_ratio": wick_ratio}
        picker_value = config.picker_value(picker_candidate, setup.picker)

    result: dict[str, Any] = {
        "schema_version": FEATURE_LEDGER_SCHEMA,
        "run_id": str(_value(row, "run_id") or ""),
        "replay_id": str(_value(row, "replay_id") or ""),
        "strategy_version": strategy_version or config.STRATEGY_VERSION,
        "strategy_fingerprint": strategy_fingerprint or config.strategy_fingerprint(),
        "session_date": str(_value(row, "day", "session_date") or _timestamp_text(signal_ts)[:10]),
        "signal_ts": _timestamp_text(signal_ts),
        "confirmation_ts": _timestamp_text(confirmation_ts),
        "signal_end": signal_end,
        "confirmation_end": confirmation_end,
        "tradingsymbol": symbol,
        "futures_tradingsymbol": futures_symbol,
        "base_side": base_side,
        "base_long_pass": _gate_result(base_long),
        "base_short_pass": _gate_result(base_short),
        "strict_signal_pass": _gate_result(strict_signal),
        "setup_filter_pass": _gate_result(setup_filter_pass),
        "setup_id": setup.setup_id if setup else "",
        "picker": setup.picker if setup else "",
        "max_entries": int(setup.max_entries) if setup else 0,
        "picker_value": picker_value,
        "first_failed_gate": failed[0] if failed else "",
        "failed_gates": json.dumps(failed, separators=(",", ":")),
        "feature_values_sha256": canonical_payload_sha256(feature_payload),
        "input_slice_sha256": input_slice_sha256 or str(_value(row, "input_slice_sha256") or ""),
        **{name: _json_number(value) for name, value in values.items()},
        "body_ratio": body_ratio if math.isfinite(body_ratio) else None,
        "wick_ratio": wick_ratio if math.isfinite(wick_ratio) else None,
        **{name: _gate_result(value) for name, value in gates.items()},
    }
    if setup:
        result.update(
            required_price_change_pct=setup.price_change_pct,
            required_oi_change_pct=setup.oi_change_pct,
            required_volume_ratio=setup.volume_ratio,
            required_body_ratio=setup.body_ratio,
            maximum_wick_ratio=setup.max_wick_ratio,
            minimum_traded_value=setup.min_traded_value,
            margin_price_change_pct=signed_move - setup.price_change_pct if math.isfinite(signed_move) else None,
            margin_oi_change_pct=numeric["oi_change_pct"] - setup.oi_change_pct if math.isfinite(numeric["oi_change_pct"]) else None,
            margin_volume_ratio=numeric["volume_ratio"] - setup.volume_ratio if math.isfinite(numeric["volume_ratio"]) else None,
            margin_body_ratio=body_ratio - setup.body_ratio if math.isfinite(body_ratio) else None,
            margin_wick_ratio=setup.max_wick_ratio - wick_ratio if math.isfinite(wick_ratio) else None,
            margin_confirmation_volume_ratio=(
                numeric["v9_1m_volume_ratio"] - config.MIN_CONFIRMATION_VOLUME_RATIO
                if math.isfinite(numeric["v9_1m_volume_ratio"]) else None
            ),
        )
    result["ledger_row_sha256"] = canonical_row_sha256(result)
    return result


def build_v13_v10_g_feature_ledger(
    pool: pd.DataFrame,
    *,
    nifty_return: float,
    strategy_version: str = "",
    strategy_fingerprint: str = "",
) -> pd.DataFrame:
    """Build a deterministic full-universe feature/gate ledger."""

    if not isinstance(pool, pd.DataFrame):
        raise TypeError("pool must be a pandas.DataFrame")
    rows = [
        evaluate_v13_v10_g_base_row(
            row,
            nifty_return=nifty_return,
            strategy_version=strategy_version,
            strategy_fingerprint=strategy_fingerprint,
            input_slice_sha256=str(row.get("input_slice_sha256") or ""),
        )
        for row in pool.to_dict("records")
    ]
    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows)
    order = [name for name in ("session_date", "signal_ts", "tradingsymbol") if name in result]
    return result.sort_values(order, kind="stable").reset_index(drop=True) if order else result


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def feature_ledger_manifest_path(path: Path | str) -> Path:
    ledger_path = Path(path)
    return ledger_path.with_name(f"{ledger_path.name}.manifest.json")


def write_feature_ledger(frame: pd.DataFrame, path: Path | str) -> dict[str, Any]:
    """Atomically write a CSV ledger plus a digest-verifiable manifest."""

    ledger_path = Path(path)
    if ledger_path.suffix.lower() != ".csv":
        raise ValueError("feature ledger path must use the .csv suffix")
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas.DataFrame")
    encoded = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    _atomic_bytes(ledger_path, encoded)
    manifest = {
        "schema_version": FEATURE_LEDGER_MANIFEST_SCHEMA,
        "feature_schema_version": FEATURE_LEDGER_SCHEMA,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "path": str(ledger_path.resolve()),
        "artifact_sha256": hashlib.sha256(encoded).hexdigest(),
        "content_sha256": canonical_frame_sha256(frame),
        "row_count": len(frame),
        "columns": [str(name) for name in frame.columns],
    }
    manifest["manifest_sha256"] = canonical_payload_sha256(manifest)
    _atomic_bytes(
        feature_ledger_manifest_path(ledger_path),
        (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"),
    )
    return manifest


def read_feature_ledger(path: Path | str, verify: bool = True) -> pd.DataFrame:
    """Read a feature ledger and, by default, verify its immutable artifact hash."""

    ledger_path = Path(path)
    if verify:
        manifest_path = feature_ledger_manifest_path(ledger_path)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        claimed_manifest = manifest.pop("manifest_sha256", None)
        if manifest.get("schema_version") != FEATURE_LEDGER_MANIFEST_SCHEMA:
            raise ValueError("unsupported feature ledger manifest schema")
        if claimed_manifest != canonical_payload_sha256(manifest):
            raise ValueError("feature ledger manifest digest mismatch")
        if manifest.get("artifact_sha256") != _file_sha256(ledger_path):
            raise ValueError("feature ledger artifact digest mismatch")
    if verify and int(manifest["row_count"]) == 0 and not manifest["columns"]:
        result = pd.DataFrame()
    else:
        result = pd.read_csv(ledger_path)
    if verify:
        if len(result) != int(manifest["row_count"]):
            raise ValueError("feature ledger row count mismatch")
        if list(result.columns) != list(manifest["columns"]):
            raise ValueError("feature ledger column schema mismatch")
    return result
