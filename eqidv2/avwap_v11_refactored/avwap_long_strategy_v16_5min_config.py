# -*- coding: utf-8 -*-
"""
avwap_long_strategy_v16_5min_config.py — LONG configuration for V16 5-min pipeline
===================================================================================

This module provides the LONG side StrategyConfig tailored for the V16 5-min
live pipeline with proper signal window boundaries per EQIDV2_V16_5MIN_PIPELINE_REFERENCE.txt
Section 4 (SIGNAL WINDOWS).

Signal Windows (5-min live pipeline):
  Session 1: 09:15 – 11:00 IST (morning, expires at 11:00)
  Session 2: 12:00 – 13:30 IST (afternoon, expires at 13:30)

These windows differ from the backtest windows (09:30-12:00, 13:30-15:15) to reflect
the tighter entry windows used in live 5-min signal detection.

Import and use like:
  from avwap_v11_refactored.avwap_long_strategy_v16_5min_config import (
      long_config_v16_5min,
  )
  cfg = long_config_v16_5min()
"""

from __future__ import annotations

from datetime import time as dtime

from avwap_v11_refactored.avwap_common_v11 import (
    StrategyConfig,
)


def long_config_v16_5min(**overrides) -> StrategyConfig:
    """
    Factory for LONG side configuration optimized for V16 5-min live pipeline.

    Signal Windows (per Section 4):
      Session 1: 09:15 – 11:00 IST
      Session 2: 12:00 – 13:30 IST

    Rationale for 5-min pipeline windows vs. backtest:
      - 5-min pipeline has tighter latency requirements
      - Morning window ends at 11:00 (sharper cutoff) to avoid afternoon bleed
      - Afternoon window: 12:00-13:30 (30 min window vs. 15:15 in backtest)
      - Removes momentum-chasing in stale afternoon session

    All other parameters match V11 LONG defaults from avwap_common_v11.default_long_config()
    except signal_windows is overridden here.
    """
    base = dict(
        side="LONG",
        stop_pct=0.0075,
        target_pct=0.0110,
        be_trigger_pct=0.0075,
        trail_pct=0.0060,
        adx_min=22.0,
        adx_slope_min=0.80,
        mod_impulse_min_atr=0.30,
        volume_min_ratio=1.20,
        rsi_min_long=45.0,
        stochk_min=25.0,
        stochk_max=95.0,
        enable_liquidity_sweep_filter=False,
        enable_avwap_no_trade_zone=False,
        enable_setup_a_close_continuation_break=False,
        enable_setup_b_huge_c1_close_reclaim_break=False,
        enable_topn_per_day=False,
        topn_per_day=0,
        # ═════════════════════════════════════════════════════════════════
        # V16 5-MIN SIGNAL WINDOWS — Production live pipeline boundaries
        # ═════════════════════════════════════════════════════════════════
        signal_windows=[
            (dtime(9, 15, 0), dtime(11, 0, 0)),    # Session 1: 09:15-11:00
            (dtime(12, 0, 0), dtime(13, 30, 0)),   # Session 2: 12:00-13:30
        ],
    )
    base.update(overrides)
    return StrategyConfig(**base)


# Alias for convenience
config_v16_5min = long_config_v16_5min
