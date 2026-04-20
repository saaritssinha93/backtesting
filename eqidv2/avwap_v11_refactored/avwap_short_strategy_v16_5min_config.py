# -*- coding: utf-8 -*-
"""
avwap_short_strategy_v16_5min_config.py — SHORT configuration for V16 5-min pipeline
====================================================================================

This module provides the SHORT side StrategyConfig tailored for the V16 5-min
live pipeline with proper signal window boundaries per EQIDV2_V16_5MIN_PIPELINE_REFERENCE.txt
Section 4 (SIGNAL WINDOWS).

Signal Windows (5-min live pipeline):
  Session 1: 09:15 – 11:00 IST (morning, expires at 11:00)
  Session 2: 12:00 – 13:30 IST (afternoon, expires at 13:30)
  SHORT entry cutoff: 13:30 IST (no entries after 13:30)

These windows differ from the backtest windows (09:30-12:00, 13:30-15:15) to reflect
the tighter entry windows used in live 5-min signal detection.

Import and use like:
  from avwap_v11_refactored.avwap_short_strategy_v16_5min_config import (
      short_config_v16_5min,
  )
  cfg = short_config_v16_5min()
"""

from __future__ import annotations

from datetime import time as dtime

from avwap_v11_refactored.avwap_common_v11 import (
    StrategyConfig,
)


def short_config_v16_5min(**overrides) -> StrategyConfig:
    """
    Factory for SHORT side configuration optimized for V16 5-min live pipeline.

    Signal Windows (per Section 4):
      Session 1: 09:15 – 11:00 IST
      Session 2: 12:00 – 13:30 IST
      Entry cutoff: 13:30 IST (no entries after this time)

    Rationale for 5-min pipeline windows vs. backtest:
      - 5-min pipeline has tighter latency requirements
      - Morning window ends at 11:00 (sharper cutoff) to avoid afternoon bleed
      - Afternoon window: 12:00-13:30 (30 min window vs. 15:15 in backtest)
      - Entry cutoff at 13:30 prevents late-session SHORT exhaustion trades
      - Removes momentum-chasing in stale afternoon session

    All other parameters match V11 SHORT defaults from avwap_common_v11.default_short_config()
    except signal_windows is overridden here.

    Note: The "entry cutoff" at 13:30 is enforced by the expires_at field in pending signals.
    Any SHORT signal generated at or after 13:30 will have expires_at set to 13:30,
    immediately expiring it in the Detection Engine.
    """
    base = dict(
        side="SHORT",
        stop_pct=0.0075,
        target_pct=0.0100,
        be_trigger_pct=0.0050,
        trail_pct=0.0030,
        mod_impulse_min_atr=0.45,
        rsi_max_short=55.0,
        stochk_max=75.0,
        topn_per_day=10,
        # ═════════════════════════════════════════════════════════════════
        # V16 5-MIN SIGNAL WINDOWS — Production live pipeline boundaries
        # ═════════════════════════════════════════════════════════════════
        signal_windows=[
            (dtime(9, 15, 0), dtime(11, 0, 0)),    # Session 1: 09:15-11:00
            (dtime(12, 0, 0), dtime(13, 30, 0)),   # Session 2: 12:00-13:30 (entry cutoff)
        ],
    )
    base.update(overrides)
    return StrategyConfig(**base)


# Alias for convenience
config_v16_5min = short_config_v16_5min
