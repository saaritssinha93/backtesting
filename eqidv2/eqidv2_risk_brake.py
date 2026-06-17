"""Shared MTM-aware risk brake (rev-2 P0-18).

Pure decision logic so it is unit-testable and identical across the paper and
live executors. The executors supply realized PnL + the open positions' mark-to-
market; this module decides whether to halt entries (and, flag-gated, flatten).

Closes the rev-2 P0-18 gap: the legacy brakes were REALIZED-ONLY and their only
action was refusing new entries — blind to a book of open losers (e.g. the Jun-4
shape: many positions deep in the red, then 76 EOD closes realize at once).

Controls:
  - daily brake on realized + open MTM (every evaluation, not only at trade close)
  - per-trade loss limit (realized, single trade)
  - per-day new-entry throttle
  - per-setup concurrency cap (general, not only C_OR_BREAKOUT)
  - flatten-on-breach is FLAG-GATED (default OFF): until enabled, a breach only
    halts new entries and is logged, so it can be watched in observe mode first.

This module places no orders and never imports a broker. The executor decides
how to act on a tripped decision (halt; or, if flatten requested, route through
the existing kill-switch close path).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Tuple

_TRUE = {"1", "true", "yes", "on", "enable", "enabled"}


def _truthy(v: str, default: str = "0") -> bool:
    return str(os.getenv(v, default)).strip().lower() in _TRUE


@dataclass(frozen=True)
class BrakeConfig:
    daily_loss_rs: float = 10000.0      # halt when realized(+MTM) <= -this
    per_trade_loss_rs: float = 5000.0   # single-trade realized loss limit
    mtm_enabled: bool = True            # include open MTM in the daily brake
    max_entries_per_day: int = 60       # 0 = no throttle
    per_setup_concurrency_cap: int = 0  # 0 = no per-setup cap
    flatten_on_breach: bool = False     # FLAG-GATED: also flatten open positions

    @staticmethod
    def from_env(prefix: str = "EQIDV2_BRAKE_", *, daily_default: float = 10000.0,
                 per_trade_default: float = 5000.0) -> "BrakeConfig":
        return BrakeConfig(
            daily_loss_rs=abs(float(os.getenv(f"{prefix}DAILY_LOSS_RS", str(daily_default)))),
            per_trade_loss_rs=abs(float(os.getenv(f"{prefix}PER_TRADE_LOSS_RS", str(per_trade_default)))),
            mtm_enabled=_truthy(f"{prefix}MTM_ENABLED", "1"),
            max_entries_per_day=int(float(os.getenv(f"{prefix}MAX_ENTRIES_PER_DAY", "60"))),
            per_setup_concurrency_cap=int(float(os.getenv(f"{prefix}PER_SETUP_CONCURRENCY_CAP", "0"))),
            flatten_on_breach=_truthy(f"{prefix}FLATTEN_ON_BREACH", "0"),
        )


@dataclass(frozen=True)
class BrakeDecision:
    tripped: bool
    trigger: str   # "" | "daily_realized" | "daily_mtm"
    reason: str
    flatten: bool  # tripped AND flatten_on_breach

    @property
    def event(self) -> str:
        return f"RISK_BRAKE_{self.trigger.upper()}" if self.tripped else ""


def evaluate_daily_brake(realized_rs: float, open_mtm_rs: float, cfg: BrakeConfig) -> BrakeDecision:
    """Trip when realized + (optional) open MTM breaches the daily loss limit.
    The trigger distinguishes a realized breach from an MTM-only breach (the
    case the legacy realized-only brake was blind to)."""
    limit = -abs(cfg.daily_loss_rs)
    realized_only = float(realized_rs)
    day_total = realized_only + (float(open_mtm_rs) if cfg.mtm_enabled else 0.0)
    if day_total <= limit:
        trigger = "daily_realized" if realized_only <= limit else "daily_mtm"
        reason = (f"day_total(realized={realized_only:,.0f}+mtm={float(open_mtm_rs):,.0f})="
                  f"Rs{day_total:,.0f} <= -Rs{cfg.daily_loss_rs:,.0f}")
        return BrakeDecision(True, trigger, reason, cfg.flatten_on_breach)
    return BrakeDecision(False, "", "", False)


def entry_allowed(
    realized_rs: float,
    open_mtm_rs: float,
    entries_today: int,
    setup: str,
    setup_open_count: int,
    cfg: BrakeConfig,
) -> Tuple[bool, str]:
    """Gate a NEW entry. Returns (allowed, reject_reason). reject_reason is a
    machine-greppable tag so caps/throttle/brake are visible in reject logs."""
    d = evaluate_daily_brake(realized_rs, open_mtm_rs, cfg)
    if d.tripped:
        return False, f"{d.event}: {d.reason}"
    if cfg.max_entries_per_day > 0 and entries_today >= cfg.max_entries_per_day:
        return False, f"RISK_BRAKE_DAILY_THROTTLE: {entries_today} >= {cfg.max_entries_per_day}"
    if cfg.per_setup_concurrency_cap > 0 and setup_open_count >= cfg.per_setup_concurrency_cap:
        return False, (f"RISK_BRAKE_SETUP_CONCURRENCY: {setup} open={setup_open_count} "
                       f">= {cfg.per_setup_concurrency_cap}")
    return True, ""


def open_mtm_rs(positions: Mapping[str, dict], last_price: Mapping[str, float]) -> float:
    """Sum unrealized PnL (Rs) across open positions.

    positions: signal_id -> {side, entry_price, quantity, ticker}
    last_price: ticker -> current price (caller fetches via broker/sim).
    Positions whose ticker has no price are skipped (treated as flat)."""
    total = 0.0
    for pos in positions.values():
        try:
            tk = str(pos.get("ticker", "")).upper()
            px = float(last_price.get(tk, float("nan")))
            ep = float(pos.get("entry_price", float("nan")))
            qty = float(pos.get("quantity", 0))
            if px != px or ep != ep or not qty:
                continue
            side = str(pos.get("side", "")).upper()
            total += (px - ep) * qty if side == "LONG" else (ep - px) * qty
        except Exception:
            continue
    return float(total)
