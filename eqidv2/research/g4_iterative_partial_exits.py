"""Two-leg cashflows for the second, explicitly exploratory G4 iteration.

A causal profit-protection signal closes part of the original quantity at its
observed next-minute open. The remainder keeps its original native exit. Full
original capital stays reserved until that native exit, so this calculation
neither changes entries nor claims capital released by a partial sale.
"""
from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np
import pandas as pd

VERSION = "G4-ITERATION2-PARTIAL-NATIVE-REMAINDER-V1"
EXPOSURE_RS = 500_000.
CAPITAL_RS = 100_000.
ROUND_TRIP_COST_BPS = 5.
FRACTIONS = (.25, .50, .75)
PROFIT_FAMILIES = frozenset({"target_giveback", "late_session_trail", "net_breakeven",
                            "rolling_structure", "atr_close_trail"})


def _number(value, name):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{name} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be a finite number")
    return result


def _stamp(value, name):
    try:
        result = pd.Timestamp(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{name} must be an aware timestamp") from exc
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError(f"{name} must be an aware timestamp")
    return result.tz_convert("Asia/Kolkata")


def _index(value, name):
    number = _number(value, name)
    if number != int(number):
        raise ValueError(f"{name} must be an integer")
    return int(number)


def partial_outcome(row: Mapping, event: Mapping, fraction: float, *, path: Mapping | None = None) -> dict:
    """Account for an actual partial close plus an unchanged native remainder.

    ``row`` is an original filled G4 trade; ``event`` is a completed-close
    profit-family event from the causal rule engine. Supply ``path`` to verify
    the event against observed minute opens (or independently validate the
    event before calling without a path). No signal is generated here, and no
    future native outcome affects the partial order's fraction or price.

    As in original G4, fractional quantity is modeled and 5bps round-trip cost
    is charged on original entry exposure, not changing exit notional. The
    fee is allocated equally between entry and exit for cashflow accounting;
    this allocation is bookkeeping, not an exchange fee schedule.
    """
    part = _number(fraction, "fraction")
    if not 0 < part < 1:
        raise ValueError("fraction must be strictly between zero and one")
    filled = row.get("filled")
    if not isinstance(filled, (bool, np.bool_)) or not bool(filled):
        raise ValueError("A partial close requires an originally filled trade")
    side = row.get("side")
    if side not in ("LONG", "SHORT"):
        raise ValueError("Unknown trade side")
    if event.get("family") not in PROFIT_FAMILIES:
        raise ValueError("A partial close requires a prespecified profit-protection family")
    sign = 1 if side == "LONG" else -1
    entry = _number(row["entry_price"], "entry_price")
    original_exit = _number(row["exit_price"], "exit_price")
    partial_exit = _number(event["next_bar_open_price"], "next_bar_open_price")
    if min(entry, original_exit, partial_exit) <= 0:
        raise ValueError("Entry and exit prices must be positive")
    entry_ts = _stamp(row["entry_ts"], "entry_ts")
    final_ts = _stamp(row["exit_ts"], "exit_ts")
    final_execution = _stamp(row.get("exit_execution_ts", row["exit_ts"]), "exit_execution_ts")
    decision = _stamp(event["decision_at_ist"], "decision_at_ist")
    execution = _stamp(event["next_bar_open_at_ist"], "next_bar_open_at_ist")
    reserve_end = _stamp(event["modeled_execution_at_ist"], "modeled_execution_at_ist")
    minute = pd.Timedelta(minutes=1)
    if (decision - minute < entry_ts or execution != decision or reserve_end != execution + minute
            or execution >= final_execution or execution >= final_ts
            or final_execution > final_ts
            or any(stamp.second != 0 or stamp.microsecond != 0 or stamp.nanosecond != 0
                   for stamp in (entry_ts, decision, execution, reserve_end))):
        raise ValueError("Partial event must follow a full post-entry close and precede the native exit")
    if path is not None:
        index = _index(event["execution_path_index"], "execution_path_index")
        entry_index = _index(row["entry_path_index"], "entry_path_index")
        stamps = np.asarray(path["timestamp_ns"])
        opens = np.asarray(path["open"], dtype=float)
        if (stamps.ndim != 1 or opens.ndim != 1 or len(stamps) != len(opens)
                or not np.issubdtype(stamps.dtype, np.integer)
                or not 0 <= entry_index < len(stamps) or not entry_index + 1 < index < len(stamps)):
            raise ValueError("Partial event has inconsistent path indices")
        if (int(stamps[entry_index]) != entry_ts.value
                or int(stamps[index - 1]) != decision.value
                or int(stamps[index]) != reserve_end.value
                or not np.isclose(opens[index], partial_exit, rtol=0, atol=1e-12)):
            raise ValueError("Partial event does not match its observed next-minute open")
    total_quantity = EXPOSURE_RS / entry
    legs = []
    for name, share, price, timestamp in (
            ("partial_protection", part, partial_exit, execution),
            ("original_native_remainder", 1 - part, original_exit, final_execution)):
        quantity = total_quantity * share
        entry_notional = quantity * entry
        exit_notional = quantity * price
        half_cost = EXPOSURE_RS * share * ROUND_TRIP_COST_BPS / 10_000 / 2
        entry_gross_cashflow = -sign * entry_notional
        exit_gross_cashflow = sign * exit_notional
        gross = quantity * sign * (price - entry)
        cost = 2 * half_cost
        legs.append(dict(leg=name, fraction=share, side=side, quantity=quantity,
            entry_price=entry, entry_ts=entry_ts.isoformat(),
            entry_timestamp_convention="Original native entry-bar end; intraminute fill is unknown",
            exit_price=price, exit_execution_ts=timestamp.isoformat(),
            entry_notional_rs=entry_notional, exit_notional_rs=exit_notional,
            entry_gross_cashflow_rs=entry_gross_cashflow,
            exit_gross_cashflow_rs=exit_gross_cashflow,
            entry_cost_rs=half_cost, exit_cost_rs=half_cost,
            entry_cashflow_rs=entry_gross_cashflow - half_cost,
            exit_cashflow_rs=exit_gross_cashflow - half_cost,
            gross_profit_rs=gross, cost_rs=cost, net_profit_rs=gross - cost))
    gross = sum(leg["gross_profit_rs"] for leg in legs)
    cost = sum(leg["cost_rs"] for leg in legs)
    cashflow_sum = sum(leg["entry_cashflow_rs"] + leg["exit_cashflow_rs"] for leg in legs)
    if not math.isclose(cashflow_sum, gross - cost, rel_tol=0, abs_tol=1e-7):
        raise ArithmeticError("Partial trade cashflows do not reconcile")
    return dict(version=VERSION, net_profit_rs=gross - cost, gross_profit_rs=gross,
        cost_rs=cost, cashflow_sum_rs=cashflow_sum, partial_exit_fraction=part,
        remaining_fraction=1 - part, quantity=total_quantity,
        original_entry_exposure_rs=EXPOSURE_RS,
        weighted_exit_price=part * partial_exit + (1 - part) * original_exit,
        final_exit_ts=final_ts.isoformat(), final_execution_ts=final_execution.isoformat(),
        partial_execution_ts=execution.isoformat(), decision_ts=decision.isoformat(),
        capital_reserved_rs=CAPITAL_RS, capital_reserved_until=final_ts.isoformat(),
        original_remainder_exit_unchanged=True,
        path_price_verified=path is not None, legs=legs)
