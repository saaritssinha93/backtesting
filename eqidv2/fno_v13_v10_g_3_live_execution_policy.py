"""Pure, separately versioned G3 LIVE sizing; never grants execution authority.

The accepted decision/PAPER contract is deliberately not imported or modified.
Policy reads are explicit, and importing this module performs no I/O.
"""
from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "g3_live_execution_policy_v1"
EXECUTION_PROFILE = "G3_LIVE_10K_5X"
QUANTITY_POLICY = "FLOOR_TARGET_EXPOSURE_EQUITY_SHARES"
CAPITAL_PER_TRADE_RS = 10000.0
LEVERAGE = 5.0
TARGET_EXPOSURE_RS = 50000.0
DISPLAY_LABEL = "G3 LIVE: INR 10,000 capital x 5 (INR 50,000 target exposure)"
CONFIG_PATH = Path(__file__).resolve().parent / "configs/strategies/v13_v10_g_3_live_execution_v1.json"


def policy_payload() -> dict[str, Any]:
    return dict(schema_version=SCHEMA_VERSION, live_execution_profile=EXECUTION_PROFILE,
                quantity_policy=QUANTITY_POLICY, capital_per_trade_rs=CAPITAL_PER_TRADE_RS,
                leverage=LEVERAGE, target_exposure_rs=TARGET_EXPOSURE_RS,
                allowed_lot_size=1, quantity_rounding="FLOOR", overrides="REDUCE_ONLY",
                sizing_reference="SIGNAL_TRIGGER_PRICE", execution_authority=False)


def policy_fingerprint() -> str:
    return hashlib.sha256(json.dumps(policy_payload(), sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode("utf-8")).hexdigest()


def validate_policy(path: Path | str = CONFIG_PATH) -> None:
    if json.loads(Path(path).read_text(encoding="utf-8")) != policy_payload():
        raise ValueError("G3 LIVE execution policy JSON does not match the reviewed policy")


def execution_metadata() -> dict[str, Any]:
    return dict(live_execution_policy_schema_version=SCHEMA_VERSION,
                live_execution_policy_fingerprint=policy_fingerprint(),
                live_execution_profile=EXECUTION_PROFILE, quantity_policy=QUANTITY_POLICY,
                capital_per_trade_rs=CAPITAL_PER_TRADE_RS, leverage=LEVERAGE,
                target_exposure_rs=TARGET_EXPOSURE_RS)


def validate_execution_metadata(row: dict[str, Any]) -> None:
    for key, expected in execution_metadata().items():
        if row.get(key) != expected or isinstance(row.get(key), bool):
            raise ValueError(f"G3 LIVE execution policy mismatch: {key}")


def _positive(value: Any, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite and positive") from exc
    if isinstance(value, bool) or not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return result


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer >= {minimum}")
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value() or number < minimum:
            raise ValueError()
        return int(number)
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(f"{name} must be an integer >= {minimum}") from exc


def size_position(entry_price: Any, lot_size: Any = 1) -> dict[str, Any]:
    entry = _positive(entry_price, "Entry price")
    lot = _integer(lot_size, "Lot size", minimum=1)
    theoretical = int((Decimal(str(TARGET_EXPOSURE_RS)) / Decimal(str(entry))).to_integral_value(rounding=ROUND_FLOOR))
    quantity = theoretical if lot == 1 else 0
    return dict(capital_rs=CAPITAL_PER_TRADE_RS, leverage=LEVERAGE,
                target_exposure_rs=TARGET_EXPOSURE_RS, theoretical_units=theoretical,
                quantity=quantity, lot_size=lot, estimated_exposure_rs=float(Decimal(quantity) * Decimal(str(entry))),
                state="LIVE_EXPOSURE_SIZED" if quantity else "BLOCKED_QUANTITY_OR_BUDGET")


def signal_metadata() -> dict[str, Any]:
    # Signal capital/leverage/exposure continue to describe the accepted PAPER
    # contract. Keep the separately sized LIVE projection in its own envelope.
    return dict(live_execution_policy_fingerprint=policy_fingerprint(),
                live_execution_profile=EXECUTION_PROFILE,
                live_execution_policy=execution_metadata())


def validate_signal_sizing(signal: dict[str, Any]) -> None:
    for key, expected in signal_metadata().items():
        if signal.get(key) != expected:
            raise ValueError(f"G3 LIVE signal execution policy mismatch: {key}")
    expected = size_position(signal.get("trigger_price"), signal.get("lot_size"))
    if signal.get("live_sizing") != expected:
        raise ValueError("G3 LIVE signal sizing does not match the current execution policy")


def requested_quantity(entry_price: Any, lot_size: Any = 1, override: Any = None) -> int:
    quantity = size_position(entry_price, lot_size)["quantity"]
    return min(quantity, _integer(override, "LIVE quantity override", minimum=1)) if override is not None else quantity


def validate_order_sizing(row: dict[str, Any], *, require_pending_quantity: bool = False) -> None:
    """Check immutable sizing separately from slippage and partial-fill quantity."""
    validate_execution_metadata(row)
    sizing = size_position(row.get("sizing_reference_price"), row.get("lot_size"))
    if row.get("sizing_reference_price") != row.get("trigger_price"):
        raise ValueError("G3 LIVE sizing reference is not the signal trigger price")
    if row.get("capital_rs") != CAPITAL_PER_TRADE_RS:
        raise ValueError("G3 LIVE order capital does not match its execution policy")
    if _integer(row.get("strategy_sized_quantity"), "Strategy sized quantity") != sizing["quantity"]:
        raise ValueError("G3 LIVE strategy sized quantity does not match its budget")
    requested = requested_quantity(row.get("sizing_reference_price"), row.get("lot_size"),
                                   row.get("execution_quantity_override"))
    if _integer(row.get("requested_quantity"), "Requested quantity") != requested:
        raise ValueError("G3 LIVE requested quantity does not match its budget/override")
    actual = _integer(row.get("quantity"), "Order quantity")
    if actual > requested or ((require_pending_quantity or row.get("status") == "PENDING_ENTRY") and actual != requested):
        raise ValueError("G3 LIVE order quantity does not match its requested quantity")
    if require_pending_quantity and actual < 1:
        raise ValueError("G3 LIVE entry is not affordable within the execution budget")
