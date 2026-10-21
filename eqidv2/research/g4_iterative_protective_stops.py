"""Predeclared G4 protective resting-stop hypotheses, research only.

The catalog is fixed before outcomes are calculated. Every candidate delegates
execution to the existing causal minute simulator: only a full post-entry close
can arm profit protection; its stop starts at the following open; observed gaps
fill at that open; ambiguous intrabar stop/target touches use stop-first order.
No entries, targets, sizing, selection, or live settings are changed here.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import itertools

from research.g3_sl_target_simulator import simulate_exit


VERSION = "G4-ITERATIVE-PROTECTIVE-STOPS-V1"
INITIAL_STOP_PCT = 1.2
NATIVE_TIGHTEN_AFTER_MINUTES = 120
NATIVE_TIGHTENED_STOP_PCT = 1.0
ACTIVATION_FLOORS_PCT = (.2, .4)
ACTIVATION_TARGET_FRACTIONS = (.25, .5, .75, .9)
PROTECTED_GROSS_PCT = (.05, .1, .2)
EARLY_TIGHTEN_MINUTES = (15, 30, 60, 90)
EARLY_LOSS_STOPS_PCT = (.6, .8, 1.0)
COMBINED_ACTIVATION_TARGET_FRACTIONS = (.5, .75)
COMBINED_ACTIVATION_FLOOR_PCT = .4
TEMPLATE_RATIONALES = {
    "baseline": "Original 1.20% stop, tightening to 1.00% at the first candle open at or after entry-bar end plus 120 minutes, with the original target.",
    "profit_floor": "After a fully post-entry completed close reaches the greater of a declared percentage floor or target fraction, activate a fixed gross profit stop at the following open.",
    "time_clamp": "Tighten the loss stop at a declared earlier holding time; the earlier stop is already at least as tight as the original 120-minute 1.00% stop.",
    "profit_floor_time_clamp": "Combine a predeclared closing-profit activation/fixed profit floor with an earlier time clamp; neither stop can loosen the other.",
}


def _params(*, floor=None, fraction=None, protected=0., delay=NATIVE_TIGHTEN_AFTER_MINUTES,
            tightened=NATIVE_TIGHTENED_STOP_PCT):
    return dict(activation_floor_pct=floor, target_fraction=fraction,
                protected_profit_pct=protected, tighten_after_minutes=delay,
                tightened_stop_pct=tightened)


def catalog() -> list[dict]:
    """Return baseline +20 profit floors +12 time clamps +72 combinations.

    Profit protection must be strictly below its activation even for the
    smallest target. Therefore protected>=activation_floor combinations are
    excluded deterministically, without examining trades or their outcomes.
    """
    policies = [dict(id="baseline", family="baseline", params=_params(),
                     rationale=TEMPLATE_RATIONALES["baseline"])]
    for floor, fraction, protected in itertools.product(ACTIVATION_FLOORS_PCT,
            ACTIVATION_TARGET_FRACTIONS, PROTECTED_GROSS_PCT):
        if protected >= floor:
            continue
        number = sum(p["family"] == "profit_floor" for p in policies) + 1
        policies.append(dict(id=f"profit_floor_{number:03d}", family="profit_floor",
            params=_params(floor=floor, fraction=fraction, protected=protected),
            rationale=TEMPLATE_RATIONALES["profit_floor"]))
    for number, (delay, tightened) in enumerate(itertools.product(EARLY_TIGHTEN_MINUTES, EARLY_LOSS_STOPS_PCT), 1):
        policies.append(dict(id=f"time_clamp_{number:03d}", family="time_clamp",
            params=_params(delay=delay, tightened=tightened),
            rationale=TEMPLATE_RATIONALES["time_clamp"]))
    for number, (fraction, protected, delay, tightened) in enumerate(itertools.product(
            COMBINED_ACTIVATION_TARGET_FRACTIONS, PROTECTED_GROSS_PCT,
            EARLY_TIGHTEN_MINUTES, EARLY_LOSS_STOPS_PCT), 1):
        policies.append(dict(id=f"profit_floor_time_clamp_{number:03d}", family="profit_floor_time_clamp",
            params=_params(floor=COMBINED_ACTIVATION_FLOOR_PCT, fraction=fraction,
                           protected=protected, delay=delay, tightened=tightened),
            rationale=TEMPLATE_RATIONALES["profit_floor_time_clamp"]))
    return policies


def outcomes_for_trade(row: Mapping, path: Mapping,
                       policies: Sequence[Mapping] | None = None) -> dict[str, dict]:
    """Replay each policy for a filled native entry; return simulator outcomes.

    Each outcome retains the original target. No costs or portfolio allocation
    are applied. Missing native fills return an empty dictionary. The caller
    records the frozen catalog and evaluates winner-profit sacrifice separately.
    """
    if str(row.get("filled", False)).strip().lower() not in ("true", "1", "1.0"):
        return {}
    if row["side"] not in ("LONG", "SHORT"):
        raise ValueError("Unknown native trade side")
    candidates = catalog() if policies is None else list(policies)
    if len({policy["id"] for policy in candidates}) != len(candidates):
        raise ValueError("Protective policy ids must be unique")
    outcomes = {}
    target = float(row["native_target_pct"])
    for policy in candidates:
        params = policy["params"]
        floor, fraction = params["activation_floor_pct"], params["target_fraction"]
        activation = None if floor is None and fraction is None else max(float(floor), float(fraction) * target)
        outcome = simulate_exit(path, int(row["entry_path_index"]), float(row["entry_price"]),
            row["side"] == "LONG", INITIAL_STOP_PCT, target,
            protect_after_close_pct=activation, protected_profit_pct=float(params["protected_profit_pct"]),
            tighten_after_minutes=float(params["tighten_after_minutes"]),
            tightened_stop_pct=float(params["tightened_stop_pct"]))
        outcomes[policy["id"]] = outcome
    return outcomes
