"""Isolated G10 sizing experiments using the existing G7 V4 admission engine.

No override delegates to G7 unchanged. Optional sizing uses entry price,
initial stop and the specified fee scenario only, never realized outcomes.
Initial risk is a planned stop loss plus costs, not a guaranteed loss ceiling:
the native replay's gap fills and realized losses remain unchanged.
"""
from __future__ import annotations

from numbers import Integral

import numpy as np
import pandas as pd

from research import g7_v4_conservative_allocation as admission
from research import g7_v4_practical_research as practical
from research.g8_exit_engine import _flags, _number


VERSION = "V13-V10-G-10-PORTFOLIO-RESEARCH-V1"


def _risk_size(raw, config, risk_per_trade_rs, cost_bps):
    """Floor shares to a cost-inclusive risk budget and the G7 exposure cap."""
    work = practical.integerize(raw, config)
    filled = _flags(work.g8_baseline_filled, "native fills")
    price = pd.to_numeric(work.entry_price, errors="coerce")
    stop = pd.to_numeric(work.initial_stop_pct, errors="coerce")
    if not np.isfinite(stop[filled]).all() or not stop[filled].between(0, 100, inclusive="neither").all():
        raise ValueError("Filled orders require initial_stop_pct between 0 and 100")
    nominal = (_number(config["capital_per_entry_rs"], "capital_per_entry_rs") *
               _number(config["leverage"], "leverage"))
    per_share_risk = price * (stop / 100. + cost_bps / 10000.)
    units = pd.Series(0., index=work.index)
    units.loc[filled] = np.minimum(
        work.loc[filled, "integer_cash_quantity"],
        np.floor(risk_per_trade_rs / per_share_risk.loc[filled]),
    )
    can_trade = filled & units.gt(0)
    scale = pd.Series(1., index=work.index)
    scale.loc[can_trade] = (units.loc[can_trade] * price.loc[can_trade] / nominal).clip(upper=1.)
    work["g10_native_filled"] = _flags(raw.g8_baseline_filled, "native fills")
    work["g10_sizing_blocked"] = work.g10_native_filled & ~can_trade
    work.loc[~can_trade, ["filled", "g8_baseline_filled"]] = False
    work["integer_cash_quantity"] = units
    work["g8_quantity_scale"] = scale
    work["g10_planned_initial_risk_rs"] = (units * per_share_risk).where(can_trade, 0.)
    work["g10_risk_budget_rs"] = risk_per_trade_rs
    return work


def execute(plan, config, name, cost=5., *, risk_per_trade_rs=None,
            max_positions=None, max_open_risk_rs=None):
    """Replay the complete pre-portfolio inventory with optional G10 overrides.

    ``plan`` is the G7 practical plan (raw, allowed, integer, clock, limit).
    None preserves each G7 setting. Risk sizing requires integer shares.
    The existing allocator preserves deterministic same-time priority,
    conservative entry clocks, symbol/capital limits and the strict-after-exit
    daily loss lock. It recomputes the lock from actually admitted after-cost
    losses under each fee scenario; rejected eventual losers cannot count.

    Risk is reserved at its initial amount until the modeled exit. No future
    protection activation or future stop tightening releases capacity early.
    Entry/exit prices, native timestamps and exit returns are never changed.
    """
    if risk_per_trade_rs is None and max_positions is None and max_open_risk_rs is None:
        return practical.execute(plan, config, name, cost)
    cost = _number(cost, "cost_bps", positive=False)
    cfg = dict(config)
    if max_positions is not None:
        if isinstance(max_positions, (bool, np.bool_)) or not isinstance(max_positions, Integral) or max_positions <= 0:
            raise ValueError("max_positions must be a positive integer or None")
        cfg["max_positions"] = int(max_positions)
    if max_open_risk_rs is not None:
        cfg["max_open_risk_rs"] = _number(max_open_risk_rs, "max_open_risk_rs")
    if risk_per_trade_rs is not None:
        risk_per_trade_rs = _number(risk_per_trade_rs, "risk_per_trade_rs")
        if not isinstance(plan["integer"], (bool, np.bool_)) or not plan["integer"]:
            raise ValueError("G10 risk sizing requires integer shares")
        source = _risk_size(plan["raw"], cfg, risk_per_trade_rs, cost)
    else:
        source = practical.integerize(plan["raw"], cfg) if plan["integer"] else plan["raw"]
    cfg.update(max_positions_per_symbol=1, variant_id=VERSION,
               conservative_entry_clock=plan["clock"], integer_cash_shares=plan["integer"],
               daily_loss_limit=plan["limit"], risk_per_trade_rs=risk_per_trade_rs)
    result = admission.allocate(source, plan["allowed"], cfg, name, cost_bps=cost,
                                conservative_entry_clock=plan["clock"], daily_loss_limit=plan["limit"])
    if risk_per_trade_rs is not None:
        blocked = result.g10_sizing_blocked & result.base_entry_allowed & ~result.daily_loss_lock_blocked
        result.loc[blocked, "portfolio_status"] = "G10_SIZE_BELOW_ONE_SHARE"
        result.loc[blocked, "portfolio_reject_reason"] = "RISK_OR_EXPOSURE_BUDGET_BELOW_ONE_SHARE"
    result["strategy"] = "V13-V10-G-10"
    result["strategy_version"] = VERSION
    result["practical_policy"] = name
    result["g10_max_positions"] = pd.Series(cfg.get("max_positions", 10), index=result.index, dtype="Int64")
    result["g10_max_open_risk_rs"] = cfg.get("max_open_risk_rs")
    return result
