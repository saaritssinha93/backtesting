"""Minute-aware admission and causal realized-loss locks for G7 V4 research.

Native entry timestamps label the end of an unknown intraminute trigger bar.
The optional conservative clock reserves capacity at that bar's open.  Exit
timestamps keep their existing OPEN/intrabar conventions.  The native entry
timestamp is restored in the returned ledger; allocation_entry_ts discloses
the clock actually used for admission.  Prices, exits, and exposure rules are
delegated unchanged to the existing G8 allocation/accounting engine.
"""
from __future__ import annotations

from numbers import Integral

import numpy as np
import pandas as pd

from research.g8_exit_engine import _flags, allocate_variant


IST = "Asia/Kolkata"
LOSS_EPSILON_RS = 1e-8


def allocate(book, allowed, config, policy, cost_bps=5., *,
             conservative_entry_clock=True, daily_loss_limit=None):
    """Allocate the complete selected inventory, optionally locking after N losses.

    A daily lock counts only executed trades with negative after-cost P&L at
    their actual modeled exit timestamp.  Entries strictly after the Nth such
    exit lose permission; ties retain the allocator's current ordering. Open
    positions are never closed by this rule.  Locks are recomputed at every
    supplied cost, rather than inherited from a lower-cost ledger.

    The first pass supplies chronological executed exit events. Removing only
    later admissions cannot change any admission at/before the lock. A full
    inventory second pass asserts this invariant, including rejected rows.
    """
    if not isinstance(conservative_entry_clock, (bool, np.bool_)):
        raise ValueError("conservative_entry_clock must be boolean")
    if (daily_loss_limit is not None and
            (isinstance(daily_loss_limit, (bool, np.bool_)) or
             not isinstance(daily_loss_limit, Integral) or daily_loss_limit <= 0)):
        raise ValueError("daily_loss_limit must be a positive integer or None")
    if (not isinstance(allowed, pd.Series) or not allowed.index.is_unique or
            "sid" not in book or set(allowed.index) != set(book.sid)):
        raise ValueError("allowed must have exactly one boolean decision per sid")
    base_allowed = _flags(allowed, "allowed")
    work = book.copy(deep=True)
    native_entry = pd.to_datetime(work.entry_ts, errors="coerce", utc=True).dt.tz_convert(IST)
    clock = native_entry - pd.Timedelta(minutes=1) if conservative_entry_clock else native_entry.copy()
    work["entry_ts"] = clock
    initial = allocate_variant(work, base_allowed, config, policy, cost_bps=cost_bps)
    days = initial.day.astype(str).str[:10]
    locks = {}
    if daily_loss_limit is not None:
        executed = _flags(initial.portfolio_executed, "portfolio_executed")
        pnl = pd.to_numeric(initial.portfolio_net_profit_rupees, errors="coerce")
        losses = initial.loc[executed & pnl.lt(-LOSS_EPSILON_RS), ["sid", "day", "exit_ts"]].copy()
        losses["day"] = losses.day.astype(str).str[:10]
        losses = losses.sort_values(["exit_ts", "sid"], kind="stable")
        for day, group in losses.groupby("day", sort=False):
            if len(group) >= daily_loss_limit:
                locks[day] = group.iloc[int(daily_loss_limit) - 1].exit_ts
    lock_ts = pd.to_datetime(days.map(locks), errors="coerce", utc=True).dt.tz_convert(IST)
    blocked_after_lock = clock.gt(lock_ts)
    # Explicit SID alignment: neither source index order nor permission order
    # is allowed to change which opportunity receives a lock decision.
    blocked_by_sid = pd.Series(blocked_after_lock.to_numpy(bool), index=book.sid)
    final_allowed = base_allowed & ~blocked_by_sid.reindex(base_allowed.index)
    result = (allocate_variant(work, final_allowed, config, policy, cost_bps=cost_bps)
              if blocked_after_lock.any() else initial.copy(deep=True))
    expected_executed = (_flags(initial.portfolio_executed, "portfolio_executed") &
                         book.sid.map(final_allowed))
    if not np.array_equal(_flags(result.portfolio_executed, "portfolio_executed").to_numpy(),
                          expected_executed.to_numpy()):
        raise ValueError("Daily loss lock changed an earlier admission or created a replacement fill")
    result["entry_ts"] = native_entry
    result["allocation_entry_ts"] = clock
    result["conservative_entry_clock"] = bool(conservative_entry_clock)
    result["base_entry_allowed"] = book.sid.map(base_allowed).to_numpy(bool)
    result["final_entry_allowed"] = book.sid.map(final_allowed).to_numpy(bool)
    result["final_permission"] = result.final_entry_allowed
    result["loss_lock_ts"] = lock_ts
    result["day_loss_limit"] = pd.Series(daily_loss_limit, index=result.index, dtype="Int64")
    result["daily_loss_lock_blocked"] = result.base_entry_allowed & blocked_after_lock
    result.loc[result.daily_loss_lock_blocked, "portfolio_status"] = "DAILY_LOSS_LIMIT_REACHED"
    result.loc[result.daily_loss_lock_blocked, "portfolio_reject_reason"] = "DAILY_NET_LOSS_COUNT_LIMIT"
    return result
