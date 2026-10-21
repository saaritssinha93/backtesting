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
# ``None`` is the serialized, fingerprint-bound unlimited value.  Keep these
# fields in every policy envelope so a missing field cannot be mistaken for an
# operator-approved removal of the portfolio admission limits.
MAX_RESERVED_OPEN_POSITIONS: int | None = None
MAX_DAILY_ENTRIES: int | None = None
MAX_RESERVED_GROSS_EXPOSURE_RS: float | None = None
DISPLAY_LABEL = "G3 LIVE: INR 10,000 capital x 5 (INR 50,000 target exposure)"
CONFIG_PATH = Path(__file__).resolve().parent / "configs/strategies/v13_v10_g_3_live_execution_v1.json"


def policy_payload() -> dict[str, Any]:
    return dict(schema_version=SCHEMA_VERSION, live_execution_profile=EXECUTION_PROFILE,
                quantity_policy=QUANTITY_POLICY, capital_per_trade_rs=CAPITAL_PER_TRADE_RS,
                leverage=LEVERAGE, target_exposure_rs=TARGET_EXPOSURE_RS,
                max_reserved_open_positions=MAX_RESERVED_OPEN_POSITIONS,
                max_daily_entries=MAX_DAILY_ENTRIES,
                max_reserved_gross_exposure_rs=MAX_RESERVED_GROSS_EXPOSURE_RS,
                allowed_lot_size=1, quantity_rounding="FLOOR", overrides="REDUCE_ONLY",
                sizing_reference="SIGNAL_TRIGGER_PRICE", execution_authority=False)


def previous_capped_policy_payload() -> dict[str, Any]:
    """Return the one immediately preceding policy for restart-only recognition."""
    return {**policy_payload(),
            "max_reserved_open_positions": 2,
            "max_daily_entries": 4,
            "max_reserved_gross_exposure_rs": 100000.0}


def _fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode("utf-8")).hexdigest()


def policy_fingerprint() -> str:
    return _fingerprint(policy_payload())


def previous_capped_policy_fingerprint() -> str:
    return _fingerprint(previous_capped_policy_payload())


def validate_policy(path: Path | str = CONFIG_PATH) -> None:
    if json.loads(Path(path).read_text(encoding="utf-8")) != policy_payload():
        raise ValueError("G3 LIVE execution policy JSON does not match the reviewed policy")


def execution_metadata() -> dict[str, Any]:
    return dict(live_execution_policy_schema_version=SCHEMA_VERSION,
                live_execution_policy_fingerprint=policy_fingerprint(),
                live_execution_profile=EXECUTION_PROFILE, quantity_policy=QUANTITY_POLICY,
                capital_per_trade_rs=CAPITAL_PER_TRADE_RS, leverage=LEVERAGE,
                target_exposure_rs=TARGET_EXPOSURE_RS,
                max_reserved_open_positions=MAX_RESERVED_OPEN_POSITIONS,
                max_daily_entries=MAX_DAILY_ENTRIES,
                max_reserved_gross_exposure_rs=MAX_RESERVED_GROSS_EXPOSURE_RS)


def previous_capped_execution_metadata() -> dict[str, Any]:
    return {**execution_metadata(),
            "live_execution_policy_fingerprint": previous_capped_policy_fingerprint(),
            "max_reserved_open_positions": 2,
            "max_daily_entries": 4,
            "max_reserved_gross_exposure_rs": 100000.0}


def _validate_execution_metadata(row: dict[str, Any], expected_metadata: dict[str, Any]) -> None:
    for key, expected in expected_metadata.items():
        if (key not in row or row.get(key) != expected
                or isinstance(row.get(key), bool)):
            raise ValueError(f"G3 LIVE execution policy mismatch: {key}")


def validate_execution_metadata(row: dict[str, Any]) -> None:
    _validate_execution_metadata(row, execution_metadata())


def validate_existing_execution_metadata(row: dict[str, Any]) -> str:
    """Validate current or exact previous metadata for read-only consumers."""
    current = execution_metadata()
    previous = previous_capped_execution_metadata()
    if all(key in row and row.get(key) == value and not isinstance(row.get(key), bool)
           for key, value in current.items()):
        return policy_fingerprint()
    if all(key in row and row.get(key) == value and not isinstance(row.get(key), bool)
           for key, value in previous.items()):
        return previous_capped_policy_fingerprint()
    _validate_execution_metadata(row, current)
    raise AssertionError("unreachable")


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


def previous_capped_signal_metadata() -> dict[str, Any]:
    return dict(
        live_execution_policy_fingerprint=previous_capped_policy_fingerprint(),
        live_execution_profile=EXECUTION_PROFILE,
        live_execution_policy=previous_capped_execution_metadata(),
    )


def validate_signal_sizing(signal: dict[str, Any]) -> None:
    current = signal_metadata()
    previous = previous_capped_signal_metadata()
    if all(key in signal and signal.get(key) == expected
           for key, expected in current.items()):
        pass
    elif all(key in signal and signal.get(key) == expected
             for key, expected in previous.items()):
        # Signals are decision evidence, not execution authority.  The prior
        # policy used the same per-trade sizing, and create_order_state stamps
        # the current unlimited metadata before any new broker submission.
        pass
    else:
        for key, expected in current.items():
            if key not in signal or signal.get(key) != expected:
                raise ValueError(f"G3 LIVE signal execution policy mismatch: {key}")
        raise AssertionError("unreachable")
    expected = size_position(signal.get("trigger_price"), signal.get("lot_size"))
    if signal.get("live_sizing") != expected:
        raise ValueError("G3 LIVE signal sizing does not match the current execution policy")


def requested_quantity(entry_price: Any, lot_size: Any = 1, override: Any = None) -> int:
    quantity = size_position(entry_price, lot_size)["quantity"]
    return min(quantity, _integer(override, "LIVE quantity override", minimum=1)) if override is not None else quantity


def _validate_order_sizing(
    row: dict[str, Any],
    expected_metadata: dict[str, Any],
    *,
    require_pending_quantity: bool = False,
) -> None:
    _validate_execution_metadata(row, expected_metadata)
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


def validate_order_sizing(row: dict[str, Any], *, require_pending_quantity: bool = False) -> None:
    """Validate a new/current-policy order; previous policy cannot enter here."""
    _validate_order_sizing(
        row, execution_metadata(), require_pending_quantity=require_pending_quantity
    )


def validate_existing_order_sizing(row: dict[str, Any]) -> str:
    """Validate current or immediately previous sizing for protective recovery.

    The returned fingerprint identifies which exact metadata envelope matched.
    Callers must continue to use :func:`validate_order_sizing` for every new
    entry, so recognizing the prior capped envelope cannot authorize an order.
    """
    fingerprint = validate_existing_execution_metadata(row)
    expected = (execution_metadata() if fingerprint == policy_fingerprint()
                else previous_capped_execution_metadata())
    _validate_order_sizing(row, expected)
    return fingerprint
