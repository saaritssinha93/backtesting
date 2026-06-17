# ===========================================================================
# MIRROR COPY — read-only reference. DO NOT EDIT HERE.
#   Source of truth: <repo-root>/final_setup_conf.py  (written only by
#   setup_train_test.py --approve). The tuner, train_test_conf.py, the v11
#   backtest, and v7 live ALL import the ROOT file — edits to this copy are
#   IGNORED (verified: import binds to root even with this file present).
#   Refresh this mirror:  cp ../final_setup_conf.py Train_and_Test/
# ===========================================================================
"""
final_setup_conf.py — approved per-setup configurations (the gate of record).
===========================================================================

Single source of truth for honest, train-then-tested setup parameters, to be
consumed by the v11 backtester (and, later, live) when running the approved book.

Currently contains SIXTEEN probation setups (all sample-limited — adopt, do not size up):
  - A_PULLBACK_C2_THEN_BREAK_C2_LOW (SHORT)            — exit 1.20/1.50, raw detection
  - B_AVWAP_RECLAIM_REVERSAL (LONG)                    — un-inverted mask vwap_dist_atr <= 1.0
  - B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG)               — regime != BULL (categorical)
  - D_EMA20_REJECTION (SHORT)                          — pre-momentum-gated (the gate is the edge)
  - E_VWAP_LOSE_EARLY_SHORT (SHORT)                    — vol_ratio band [1.8,3.2] (STRONG: p 0.004); pre-momentum DROPPED
  - G_HIGHER_HIGH_BREAK (LONG)                         — pre-momentum-gated (pre2_mom_r>=0.55 & adx>=26); exit 0.90/2.50 (STRONG: train 2.38/test 2.66, p 0.005)
  - L_DOUBLE_BOTTOM_VWAP (LONG)                        — pre-momentum-gated (pre_entry_momentum_score>=79 & adx>=28); exit 0.90/1.50 (STRONG: train 2.55/test 3.57, p 0.033) *** RAW-POOL caveat: see provenance.gating_caveat ***
  - L_PRESSURE_BURST_VWAP (LONG)                       — quality_score<=25 mask + pre1_adx>=44 gate; exit 0.70/1.25 (WEAK/CAUTION: train 2.24/test 2.03 but fails monotonic-sensitivity + multi-exit-sig; USER_APPROVED_OVERRIDE_WEAK; RAW-POOL) TIER123-OVERLAY + thin-test(n=8) caveat *** SCANNER-ENRICHED feats (ema20_slope/rsi3max) wiring caveat + thin May-only test (n=12/6d) ***
  - L_RS_LEADER_VWAP_HOLD (LONG)                       — RS-leader VWAP test-and-hold; mask quality_score>=97 & vol_ratio>=2.16 & vwap_dist_atr<=1.49 (near-VWAP) & signal_minute<=660; exit 0.50/1.25 (STRONG on CORRECTED VWAP: train 2.74/test 3.82, 5/5 exits, 82% months) *** SCANNER-SOURCE; rescued by the 2026-06-13 VWAP fix ***
  - P_PDH_BREAK_RETEST_LONG (LONG)                     - TIER123/scanner-source; prev-day-high break-retest; mask body_pct<=0.75 + pre-mom pre_entry_momentum_score>=75 & pre3_range_r>=0.50; exit 0.50/0.60 (STRONG corrected-VWAP: train 2.39/test 6.88, 5/5 exits, 83% months) *** scanner-source ***
  - E_ORB_RETEST_HOLD_LONG (LONG)                      - TIER123/scanner-source; opening-range retest-and-hold; mask vol_ratio>=2.42 & quality_score>=86.6 & signal_minute>=605 + pre-mom sig5_adx_calc>=42.4; exit 0.90/1.25 (STRONG corrected-VWAP: train 2.54/test 2.50, 4/5 exits, 91% months, monotone) *** scanner-source ***
  - V_RECLAIM_PULLBACK_LONG (LONG)                     - TIER123/scanner-source; reclaim-pullback in a strong-ADX RS leader; mask rs_pct>=0.37 + pre-mom pre_entry_momentum_score<=58 & sig5_adx_calc>=33.9; exit 0.50/0.80 (STRONG corrected-VWAP, improved gate: train 2.02/test 5.28, 5/5 exits, even halves, monotone ADX) *** scanner-source ***
  - B_HUGE_RED_FAILED_BOUNCE (SHORT)                   - mined short; failed-bounce after huge red bar; pre-mom pre3_close_pos<=0.58 & sig5_rsi_dir<=64 & pre5_mom_r<=0.28; exit 0.90/1.25 (STRONG: train 2.90/test 3.49, 5/5 exits, even halves, top1d 26%)
  - C_OR_BREAKDOWN (SHORT)                             - mined short; OR-breakdown in a strong downtrend; SIMPLE 2-term pre-mom sig5_adx_calc>=39.7 & pre1_adx<=21.4; exit 0.90/2.00 (STRONG: train 2.78/test 5.26, 5/5 exits, top1d 29%; halves imbalanced)
  - A_MOD_BREAK_C1_LOW (SHORT)                         - mined short (deeper A_MOD mine); momentum breakdown from a TIGHT pre-break range; mask vol_ratio>=1.96 + pre-mom pre5_mom_r>=0.43 & pre3_range_r<=0.20; exit 1.10/1.00 (STRONG: train 2.58/test 2.83, even halves, 88% months, MONOTONE sens)
  - G_LOWER_LOW_BREAK (SHORT)                          - mined short (SELECTIVE); volume-climax exhaustion lower-low break; mask vol_ratio>=4.1 & quality_score>=76 + pre-mom sig5_rsi_dir>=68.7; exit 1.10/1.00 (4/5 exits, even halves, top1d27%, 100% months) *** thin count: test n=9, needs 4x volume ***

It ALSO contains a RESEARCH_WATCH_CONF block (enabled=False — NEVER traded by v11) for
setups that were deeply diagnosed (exit sweeps x sub-populations x pre-momentum on/off)
and found to have NO validated edge. They are recorded with their best-found config +
evidence + a re-validation trigger so the file is the complete record, but they MUST NOT
be promoted until the trigger is met:
  - E_ORB_BREAKOUT_SHORT (SHORT)  — best-found train 1.04 / test 0.94 (the churn/cost sink)
  - E_ORB_BREAKOUT_LONG  (LONG)   — best-found train 0.96 / test 0.91 (22% immediate-fail)
  - E_VWAP_BAND_FADE     (SHORT)  — best-found train 1.14 / test 1.34 (n=7, p 0.276 — closest, but no train edge)

Cost basis           : net of statutory NSE intraday costs (nse_intraday_costs).
Train window         : 2025-11-01 .. 2026-04-30
Test  window         : 2026-05-01 .. 2026-06-10
Search method        : setup_train_test.py --family A --setups A_PULLBACK_C2_THEN_BREAK_C2_LOW
                       (target train PF band [1.5, 2.0] at MAX trade count; honest OOS test)

Schema per setup
----------------
  "detection"          : the raw 5-minute setup definition (read-only reference;
                         defined in avwap_5min_ID_v2_backtesting.py / candidate_scan).
                         These are NOT tuned here — they are what MAKES a candidate
                         this setup. Listed so the config is self-describing.
  "exit"               : {"sl_pct", "tgt_pct"} resolved on 1-min OHLC to 15:20 EOD.
  "mask_terms"         : extra selected-strategy threshold filters, AND-combined
                         (feature, op, value). [] = raw detection only.
  "pre_momentum_terms" : per-setup 1-min pre-entry momentum gate, AND-combined. [].
  "entry_guards"       : time-window / Top-N guards. {} = none beyond the live
                         09:30-14:30 entry window + one-ticker-per-day dedupe.
  "provenance"         : train/test evidence + gate status.
"""

from __future__ import annotations

import os

COST_BASIS = "net_of_nse_intraday_costs"

TIER123_SCAN_SOURCE_CSV = os.getenv(
    "EQIDV2_FINAL_CONF_TIER123_SOURCE_CSV",
    r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current\tier123\tier123_standalone_trades.csv",
)
NEW_SETUPS_SCAN_SOURCE_CSV = os.getenv(
    "EQIDV2_FINAL_CONF_NEW_SETUPS_SOURCE_CSV",
    r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current\new_setups\new_setups_standalone_trades.csv",
)

# Honest acceptance gate (judged on the TEST window). A config "passes" only if
# all hold; entries flagged USER_APPROVED_OVERRIDE were kept despite a marginal miss.
ACCEPT_GATE = {
    "train_pf_min": 1.50,
    "train_pf_max": 2.00,
    "test_min_net_pf": 1.30,
    "test_max_day_block_p": 0.10,
    "min_test_train_pf_ratio": 0.55,
    "min_test_trades": 8,
}

FINAL_SETUP_CONF = {
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW": {
        "side": "SHORT",
        # -------------------------------------------------------------------
        # RAW DETECTION (reference only — defined in avwap_5min_ID_v2_backtesting.py,
        # reason tag "bear_pullback_c2_break_low"). A 5-min bar is this setup when
        # ALL of the following hold. Idea: after a 2-bar up-pullback in a non-bull
        # regime, price loses VWAP and breaks the prior bar's low on volume.
        # -------------------------------------------------------------------
        "detection": {
            "reason_tag": "bear_pullback_c2_break_low",
            "scan_window_ist": ["09:30", "14:30"],         # SIGNAL_START..SIGNAL_END
            "conditions": [
                # short_struct: down bar closing in the lower 40% of its range
                ("close", "<", "open"),
                ("close_loc", "<=", 0.40),                  # CLOSE_LOC_SHORT_MAX
                # below_vwap: closes under the session VWAP
                ("close", "<", "VWAP"),
                # breaks the previous bar's low
                ("close", "<", "prev_bar_low"),
                # the pullback: prior two bars were rising (prev close > prev-2 close)
                ("prev_close", ">", "prev2_close"),
                # participation
                ("vol_ratio", ">=", 1.4),
                # regime filter
                ("regime", "!=", "BULL"),
            ],
            # Common liquidity/sanity gate applied to every candidate (_passes_common):
            "common_gate": {
                "min_close_price": 80.0,                    # MIN_PRICE
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR (no blow-off bar)",
            },
            # Feature definitions (avwap_5min_ID_v2_backtesting._prepare_5m):
            "feature_defs": {
                "VWAP": "intraday session VWAP (resets daily)",
                "ATR": "per-day ATR",
                "close_loc": "(close - low) / (high - low)",
                "body_pct": "abs(close - open) / (high - low)",
                "vol_ratio": "volume / Volume_SMA20  (SMA20 = volume.shift(1).rolling(20,min8).mean)",
                "atr_pct": "ATR / close",
                "vwap_dist_atr": "(close - VWAP) / ATR",
                "rs_pct": "stock_intraday_ret% - NIFTY_intraday_ret%  (RS_LOOKBACK_BARS=6)",
                "regime": "BULL/BEAR/TREND/NEUTRAL from NIFTY ret vs VWAP (see _bar_context)",
            },
        },
        # -------------------------------------------------------------------
        # TUNED v11 CONFIG (this is what train/test selected and what v11 uses).
        # The raw detection above already IS the edge; no extra selected-strategy
        # mask or pre-momentum gate was added. Exit is from the CLEAN consistent-pool
        # re-validation. NOTE: the earlier combined pool had picked 0.85/0.80 — the
        # exit is NOT stable across data slices (see provenance.exit_instability).
        # -------------------------------------------------------------------
        "exit": {"sl_pct": 1.20, "tgt_pct": 1.50},          # clean-pool tuned (v6 default 0.85/1.00)
        "mask_terms": [],                                   # raw detection only
        "pre_momentum_terms": [],                           # none
        "entry_guards": {},                                 # live 09:30-14:30 window + 1-ticker/day dedupe only
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        # -------------------------------------------------------------------
        "provenance": {
            "approved_on": "2026-06-11",
            "family": "A",
            "cost_basis": "net_of_nse_intraday_costs",
            "pool": ("outputs_ID_v11_cleanpool — consistent Nov3-Jun10 (148 days), "
                     "profile=none + ab_gate=quality_top_slot, 8-worker monthly chunks"),
            "train_window": ["2025-11-01", "2026-04-30"],
            "test_window": ["2026-05-01", "2026-06-10"],
            "train": {"trades": 48, "net_pf": 1.331, "net_pnl_rs": 8460.0},
            "test": {"trades": 8, "net_pf": 1.852, "net_pnl_rs": 3255.0,
                     "day_block_p": 0.217, "test_train_pf_ratio": 1.39},
            "full_history_note": "day-clustered bootstrap p=0.024 over 32 trades (earlier full-history run)",
            "gate_status": "USER_APPROVED_OVERRIDE_WEAK",
            "gate_miss": ("train_pf 1.33 < 1.50; test day_block_p 0.217 > 0.10 (NOT significant); "
                          "exit unstable across pools"),
            "exit_instability": "combined pool chose SL/Tgt 0.85/0.80; clean pool chose 1.20/1.50 -> exit not robust",
            "kept_because": ("still net-positive and generalising OOS (test PF 1.85, ratio 1.39, "
                             "75% win in the good weeks) + full-history p=0.024; user-directed keep "
                             "despite weak clean-pool significance"),
            "status_label": "WEAK / WATCHLIST — re-confirm with more history before trusting at size",
        },
    },

    # =======================================================================
    # B_AVWAP_RECLAIM_REVERSAL — un-inverted reclaim mask (B* deep diagnosis 2026-06-11)
    # =======================================================================
    "B_AVWAP_RECLAIM_REVERSAL": {
        "side": "LONG",
        # RAW DETECTION (avwap_5min_ID_v2_backtesting.py L680, reason
        # "reclaim_session_vwap_from_below", min quality_score >= 6.0). Idea: a stock
        # that was BELOW session VWAP reclaims it on a strong up-bar in a non-bear
        # regime -> momentum reversal from weakness.
        "detection": {
            "reason_tag": "reclaim_session_vwap_from_below",
            "scan_window_ist": ["09:30", "14:30"],
            "min_quality_score": 6.0,
            "conditions": [
                ("close", ">", "open"),                       # long_struct
                ("close_loc", ">=", 0.60),                    # CLOSE_LOC_LONG_MIN
                ("prev_close", "<", "prev_VWAP"),             # prior bar was below VWAP
                ("close", ">", "VWAP"),                       # current bar reclaims VWAP
                ("rs_pct", ">", -0.10),
                ("vol_ratio", ">=", 1.4),
                ("regime", "!=", "BEAR"),
            ],
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR (no blow-off bar)",
            },
            "feature_defs": {
                "VWAP": "intraday session VWAP (resets daily)", "ATR": "per-day ATR",
                "close_loc": "(close - low) / (high - low)",
                "vol_ratio": "volume / Volume_SMA20 (SMA20 = volume.shift(1).rolling(20,min8).mean)",
                "atr_pct": "ATR / close", "vwap_dist_atr": "(close - VWAP) / ATR",
                "rs_pct": "stock_intraday_ret% - NIFTY_intraday_ret% (RS_LOOKBACK_BARS=6)",
                "regime": "BULL/BEAR/TREND/NEUTRAL from NIFTY ret vs VWAP",
            },
        },
        # TUNED v11 CONFIG (B* deep diagnosis). The PRODUCTION mask was INVERTED:
        # vwap_dist_atr >= 0.60 selected EXTENDED reclaims that fail (PF 0.6, 43%
        # immediate-fail); the edge is a reclaim NEAR VWAP. PF rises monotonically as
        # the cut tightens; the cut sweep picked vwap_dist_atr <= 1.0 as the best
        # balance of positive test + enough test sample (<=0.75 collapsed test to 2
        # losers). Also DROPPED the data-mined pre_entry_momentum_score <= 64.7678.
        "exit": {"sl_pct": 0.70, "tgt_pct": 1.50},            # v6 default
        "mask_terms": [["vwap_dist_atr", "<=", 1.0]],         # REPLACES inverted >= 0.60
        "pre_momentum_terms": [],                             # dropped 6-decimal momentum gate
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-11", "family": "B",
            "cost_basis": "net_of_nse_intraday_costs",
            "pool": "outputs_ID_v11_cleanpool (Nov3-Jun10, 148 days)",
            "exit_basis": "fixed 0.70/1.50, pre-dedupe (B* diagnosis isolates raw setup behaviour)",
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "train": {"trades": 27, "net_pf": 1.45},
            "test": {"trades": 5, "net_pf": 1.20, "win_pct": 40.0, "day_block_p": 0.185},
            "net_pnl_rs_total": 5736.0,
            "diagnosis": ("production mask vwap_dist_atr>=0.60 was INVERTED; edge is a near-VWAP "
                          "reclaim; PF monotonic vs cut; <=1.0 keeps test positive on 5 trades"),
            "gate_status": "PROBATION",
            "gate_miss": "test n=5 (small); day_block_p 0.185 not < 0.10",
            "kept_because": ("un-inverts a confirmed-wrong production mask; train PF 1.45 monotonic "
                             "confirmation; test positive (PF 1.20, 40% win)"),
            "status_label": "PROBATION — adopt the corrected (un-inverted) mask; do NOT size up until more history confirms OOS",
        },
    },

    # =======================================================================
    # B_HUGE_C1_CLOSE_RECLAIM_BREAK — regime-gated (B* deep diagnosis 2026-06-11)
    # NOTE: mask term ("regime","!=","BULL") is CATEGORICAL. v11 must apply it as a
    # string inequality (keep regime in {NEUTRAL, TREND}; detection already excludes
    # BEAR), NOT a numeric threshold.
    # =======================================================================
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK": {
        "side": "LONG",
        # RAW DETECTION (avwap_5min_ID_v2_backtesting.py L695, reason
        # "huge_green_reclaim_then_break", min quality_score >= 7.0). Idea: momentum
        # continuation — break of a prior HUGE GREEN bar's high in a non-bear regime.
        "detection": {
            "reason_tag": "huge_green_reclaim_then_break",
            "scan_window_ist": ["09:30", "14:30"],
            "min_quality_score": 7.0,
            "conditions": [
                ("prev_range", ">=", "1.80 * prev_ATR"),      # huge_prev
                ("prev_close", ">", "prev_open"),             # prior bar green
                ("close", ">", "open"),                       # long_struct
                ("close_loc", ">=", 0.60),
                ("close", ">", "prev_bar_high"),              # breaks prior huge-bar high
                ("close", ">", "VWAP"),
                ("vol_ratio", ">=", 1.3),
                ("regime", "!=", "BEAR"),
            ],
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR (no blow-off bar)",
            },
            "feature_defs": {
                "huge_prev": "prev bar range >= 1.80 * prev bar ATR",
                "VWAP": "intraday session VWAP", "ATR": "per-day ATR",
                "close_loc": "(close - low) / (high - low)",
                "vol_ratio": "volume / Volume_SMA20", "atr_pct": "ATR / close",
                "vwap_dist_atr": "(close - VWAP) / ATR",
                "regime": "BULL/BEAR/TREND/NEUTRAL from NIFTY ret vs VWAP",
            },
        },
        # TUNED v11 CONFIG (B* deep diagnosis). The PRODUCTION mask rs_pct <= 10.7 is a
        # NO-OP (rs rarely that high). The real driver is REGIME: NEUTRAL wins
        # (PF 1.96), BULL loses (PF 0.66) — the break in a bull market is a late chase.
        # Filter -> regime != BULL. With detection's regime != BEAR, the effective
        # regime universe is {NEUTRAL, TREND}.
        "exit": {"sl_pct": 0.70, "tgt_pct": 1.50},            # v6 default
        "mask_terms": [["regime", "!=", "BULL"]],             # CATEGORICAL; REPLACES no-op rs_pct<=10.7
        "effective_regime_universe": ["NEUTRAL", "TREND"],    # detection excludes BEAR; mask excludes BULL
        "pre_momentum_terms": [],
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-11", "family": "B",
            "cost_basis": "net_of_nse_intraday_costs",
            "pool": "outputs_ID_v11_cleanpool (Nov3-Jun10, 148 days)",
            "exit_basis": "fixed 0.70/1.50, pre-dedupe",
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "train": {"trades": 29, "net_pf": 1.11},
            "test": {"trades": 5, "net_pf": "inf", "win_pct": 100.0},
            "full_sample_day_block_p": 0.076,
            "net_pnl_rs_total": 8596.0, "trading_days": 25,
            "diagnosis": ("regime-gated continuation; BULL is the losing regime (chase), NEUTRAL the "
                          "winner; the rs_pct<=10.7 production mask is a no-op"),
            "gate_status": "PROBATION",
            "gate_miss": ("walk-forward gate n_oos=0 — setup trades only ~25 days, so 20-day test "
                          "folds are empty and it cannot be certified; test n=5"),
            "kept_because": ("day-clustered bootstrap p=0.076 (borderline significant); train+test "
                             "both positive (test 5/5 winners); regime thesis mechanically sound "
                             "(a huge-green-break in a bull tape is a late chase)"),
            "status_label": "PROBATION — day-block-significant but WF-uncertifiable at 34 trades; needs more history",
        },
    },

    # =======================================================================
    # D_EMA20_REJECTION — pre-momentum-gated trend-rejection short (D* deep diagnosis 2026-06-11)
    # The PRE-MOMENTUM GATE IS THE EDGE: without it the setup is a coin-flip loser
    # (train PF 0.71); with it, train 1.23 / test 4.07 / full 1.35. The production
    # body_pct>=0.89 & ranker_score>=0.39 mask is DROPPED (it over-tightens to n=6).
    # =======================================================================
    "D_EMA20_REJECTION": {
        "side": "SHORT",
        # RAW DETECTION (avwap_5min_ID_v2_backtesting.py L731, reason
        # "ema20_trend_rejection"). Idea: in a downtrend stack, price retests a falling
        # EMA20 and rejects (resumes down) in a non-bull regime.
        "detection": {
            "reason_tag": "ema20_trend_rejection",
            "scan_window_ist": ["09:30", "14:30"],
            "conditions": [
                ("abs(close - EMA20)", "<=", "0.35 * ATR"),  # near_ema20
                ("close", "<", "open"),                       # short_struct
                ("close_loc", "<=", 0.40),                    # CLOSE_LOC_SHORT_MAX
                ("close", "<", "EMA20"),                      # below EMA20
                ("EMA20", "<=", "EMA50"),                     # downtrend_stack
                ("rs_pct", "<", 0.10),
                ("vol_ratio", ">=", 1.3),
                ("regime", "!=", "BULL"),
            ],
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR (no blow-off bar)",
            },
            "feature_defs": {
                "EMA20/EMA50": "exponential moving averages on the 5-min bars",
                "ATR": "per-day ATR", "near_ema20": "abs(close-EMA20) <= 0.35*ATR",
                "downtrend_stack": "close<EMA20 AND EMA20<=EMA50",
                "close_loc": "(close - low) / (high - low)", "vol_ratio": "volume / Volume_SMA20",
                "rs_pct": "stock_intraday_ret% - NIFTY_intraday_ret%", "regime": "BULL/BEAR/TREND/NEUTRAL",
            },
        },
        # TUNED v11 CONFIG. The pre-momentum gate (below) is the EDGE; the default
        # exit fits (53% win, mfe_R 0.8). No selected-strategy mask (production
        # body/ranker mask dropped — collapses the sample to n=6).
        "exit": {"sl_pct": 0.75, "tgt_pct": 1.30},            # v6 default
        "mask_terms": [],                                     # production body>=0.89 & ranker>=0.39 DROPPED (over-tight)
        # PRE-ENTRY MOMENTUM GATE (the edge) — 1-min features at entry; ALL required.
        # Demands recent downward momentum within a contained, trending move:
        "pre_momentum_terms": [
            ["pre10_mom_r", "<=", 0.156614],   # not already over-extended down over 10 bars
            ["pre5_mom_r", ">=", 0.12493],     # genuine recent down-momentum (5 bars), risk-normalised
            ["sig5_adx_calc", ">=", 20.0],     # 5-min ADX confirms a trend (not chop)
        ],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-11", "family": "D",
            "cost_basis": "net_of_nse_intraday_costs",
            "pool": "D_EMA20_REJECTION from admitted pre-dedupe clean pool (Nov3-Jun10, 148 days)",
            "exit_basis": "fixed 0.75/1.30",
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "train": {"trades": 52, "days": 32, "net_pf": 1.23, "net_pnl_rs": 4158.0, "win_pct": 50.0,
                      "target_sl_eod_pct": [25, 40, 35]},
            "test": {"trades": 5, "days": 4, "net_pf": 4.07, "net_pnl_rs": 2553.0, "win_pct": 80.0,
                     "day_block_p": 0.133},
            "full_sample": {"trades": 57, "days": 36, "net_pf": 1.35, "net_pnl_rs": 6711.0, "day_block_p": 0.179},
            "premom_effect": "WITHOUT the gate train PF 0.71 (coin-flip loser); WITH it 1.35 -> the gate IS the edge",
            "monthly_note": "inconsistent: Nov/Jan/May strong, but Mar PF 0.14 (-3,118) and Feb 0.81; edge not month-stable",
            "gate_status": "PROBATION",
            "gate_miss": "day-block p 0.179 (not significant); test n=5 (thin); monthly-inconsistent",
            "kept_because": ("positive in both train (1.23) and test (4.07), full PF 1.35; pre-momentum gate "
                             "mechanically sound (recent down-momentum + ADX trend = real rejection); immediate-fail 0%"),
            "status_label": "PROBATION — adopt the PM-gated config; do NOT size up (p 0.179, n=57, Mar was a losing month)",
        },
    },

    # =======================================================================
    # E_VWAP_LOSE_EARLY_SHORT — volume-banded (E* deep diagnosis 2026-06-11).
    # STRONGEST edge found across A/B/C/D/E. The unbanded setup is a churn loser
    # (PF 0.86); restricting to vol_ratio in [1.8,3.2] gives PF 2.01 (train 2.06 /
    # test 1.76), day-block p 0.004. The pre-momentum gate is DROPPED (it dilutes
    # the band edge: vol-band+premom 1.31 vs band-alone 2.24).
    # =======================================================================
    "E_VWAP_LOSE_EARLY_SHORT": {
        "side": "SHORT",
        # RAW DETECTION (avwap_5min_ID_v7_candidate_scan.py L514, EARLY engine,
        # reason "early_vwap_lose_break_prev_low", min quality_score >= 6.0). Idea:
        # an early-session VWAP failure — price was at/above VWAP, loses it, breaks
        # the prior bar low on a weak close, lagging the market.
        "detection": {
            "reason_tag": "early_vwap_lose_break_prev_low",
            "engine": "EARLY (eqidv2_signal_discovery_v7 early-slot scan)",
            "min_quality_score": 6.0,
            "conditions": [
                ("close", "<", "open"),                       # common_short
                ("prev_close", ">=", "prev_VWAP"),            # was at/above VWAP
                ("close", "<", "VWAP"),                       # loses VWAP
                ("close", "<", "prev_low"),                   # breaks prior bar low
                ("close_loc", "<=", 0.35),                    # weak close
                ("rs_pct", "<=", -0.10),                      # lagging the market
                ("vwap_dist_atr", ">=", -1.80),               # not already far below VWAP
            ],
            "early_extra_gate": ["rs_pct >= -1.20", "close_loc >= 0.08", "atr_pct <= 0.008"],
            "entry_guard": "signal time >= 09:45 IST (ENTRY_E_VWAP_EARLY_SHORT_MIN_SLOT)",
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR",
            },
            "feature_defs": {
                "VWAP": "intraday session VWAP", "vol_ratio": "volume / Volume_SMA20",
                "close_loc": "(close - low)/(high - low)", "atr_pct": "ATR/close",
                "vwap_dist_atr": "(close - VWAP)/ATR", "rs_pct": "stock_ret% - NIFTY_ret%",
            },
        },
        # TUNED v11 CONFIG — the VOLUME BAND is the edge.
        "exit": {"sl_pct": 0.70, "tgt_pct": 1.00},            # v6 default
        "mask_terms": [["vol_ratio", ">=", 1.8], ["vol_ratio", "<=", 3.2]],  # THE EDGE (band)
        "pre_momentum_terms": [],                             # DROPPED (dilutes the band edge)
        "entry_guards": {"min_slot": "09:45"},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-11", "family": "E",
            "cost_basis": "net_of_nse_intraday_costs",
            "pool": "admitted pre-dedupe clean pool (Nov3-Jun10, 148 days)",
            "exit_basis": "fixed 0.70/1.00",
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "all_volumes": {"full_pf": 0.86, "note": "unbanded = churn loser; DO NOT trade unbanded"},
            "band_1p8_3p2": {"train_n": 47, "train_pf": 2.06, "test_n": 9, "test_pf": 1.76, "full_pf": 2.01, "day_block_p": 0.004},
            "band_2_3_tight": {"train_n": 37, "train_pf": 2.27, "test_n": 7, "test_pf": 2.05, "full_pf": 2.24, "day_block_p": 0.008},
            "diagnosis": ("volume conviction band: 2-3x SMA = institutional VWAP-lose follow-through; "
                          "<1.8 no conviction, >3.2 exhaustion/climax bounce; monotonic + mechanical"),
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=9 (thin) despite day-block p 0.004; confirm on more history before sizing",
            "kept_because": "best train+test edge found (PF>2 both periods, day-block significant); pre-momentum gate dropped (it diluted the edge)",
            "status_label": "STRONG PROBATION — strongest candidate in the book; band edge significant (p 0.004) but n=56; do not size up yet",
        },
    },

    # =======================================================================
    # G_HIGHER_HIGH_BREAK — pre-momentum-gated breakout (G* aggressive iteration 2026-06-12).
    # The UNGATED setup is a net loser (train 1.05 / test 0.79, -Rs 26,855 / 750 trades) —
    # a late 20-bar-high chase. An aggressive train-PF>2 search (greedy + 2-term + 40k random
    # combos, exit co-optimized) found a ROBUST momentum/ADX pocket: demanding a genuine
    # ADX-confirmed pre-entry momentum thrust (pre2_mom_r>=0.55 & sig5_adx_calc>=26) keeps the
    # real breakouts and drops the chase. The production gate (pre3_close_pos<=0.985 &
    # sig5_rsi_dir<=67.878) is DROPPED — it made the loss worse. Validated NOT a knife-edge:
    # train PF>1.5 & test PF>1.3 across a CONTIGUOUS, MONOTONIC neighbourhood (mom[0.4,0.6] x
    # adx[24,30]); both train halves strong (2.47/2.30); test-positive at every exit tested.
    # =======================================================================
    "G_HIGHER_HIGH_BREAK": {
        "side": "LONG",
        # RAW DETECTION (avwap_5min_ID_v2_backtesting.py L745, "twenty_bar_higher_high_break").
        # Idea: momentum-continuation long — break of the prior 20-bar high on strong volume.
        "detection": {
            "reason_tag": "twenty_bar_higher_high_break",
            "scan_window_ist": ["09:30", "14:30"],
            "conditions": [
                ("close", ">", "open"),                       # long_struct
                ("close_loc", ">=", 0.60),                    # CLOSE_LOC_LONG_MIN
                ("close", ">", "VWAP"),                        # above_vwap
                ("close", ">", "prev_20bar_high"),            # rh: breaks the 20-bar higher high
                ("rs_pct", ">", 0.00),
                ("vol_ratio", ">=", 1.4),
                ("regime", "!=", "BEAR"),
            ],
            "entry_profile_note": ("UNGATED this is a LATE CHASE — median vwap_dist_atr 2.9 (2.9 ATR above "
                                   "VWAP), vol_ratio 3.45x — and loses money. The pre-momentum gate below is "
                                   "what isolates the genuine (non-chase) breakouts."),
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR (no blow-off bar)",
            },
            "feature_defs": {
                "VWAP": "intraday session VWAP", "ATR": "per-day ATR",
                "close_loc": "(close - low)/(high - low)", "vol_ratio": "volume / Volume_SMA20",
                "vwap_dist_atr": "(close - VWAP)/ATR", "rs_pct": "stock_ret% - NIFTY_ret%",
                "prev_20bar_high": "rolling 20-bar prior high (rh)", "regime": "BULL/BEAR/TREND/NEUTRAL",
            },
        },
        # TUNED v11 CONFIG — the PRE-MOMENTUM GATE is the edge.
        "exit": {"sl_pct": 0.90, "tgt_pct": 2.50},            # wide target lets the ADX-confirmed runner run
        "mask_terms": [],                                     # no selected-strategy mask
        # PRE-ENTRY MOMENTUM GATE (the edge) — 1-min features at entry; ALL required.
        # Demands a real, trend-confirmed momentum thrust into the breakout (not a one-bar chase):
        "pre_momentum_terms": [
            ["pre2_mom_r", ">=", 0.55],        # strong recent 2-bar up-momentum, risk-normalised
            ["sig5_adx_calc", ">=", 26.0],     # 5-min ADX confirms a genuine trend (not chop/exhaustion)
        ],
        "pre_momentum_missing_action": "block",
        "dropped_production_gate": [["pre3_close_pos", "<=", 0.985417], ["sig5_rsi_dir", "<=", 67.878]],  # this HURT (ON -21k vs OFF -5.8k)
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-12", "family": "G",
            "cost_basis": "net_of_nse_intraday_costs",
            "pool": "G_HIGHER_HIGH_BREAK from clean pool (Nov3-Jun10, 148 days, 750 candidates)",
            "exit_basis": "fixed 0.90/2.50, 1-min path to 15:20 EOD",
            "search_method": ("G_iterate.py — aggressive train-PF>2 search (greedy forward-selection + "
                              "exhaustive 2-term + 40k randomized 3-term, exit co-optimized, robust=min train-halves); "
                              "validated by G_validate_passer.py (threshold sensitivity + halves + monthly + exits)"),
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "ungated_reference": {"train_pf": 1.05, "test_pf": 0.79, "full_net_rs": 2857, "note": "net loser; the gate creates the edge"},
            "train": {"trades": 39, "net_pf": 2.38, "half1_pf": 2.47, "half2_pf": 2.30},
            "test": {"trades": 8, "net_pf": 2.66, "win_pct": 62.0, "day_block_p": 0.005},
            "full": {"net_pf": 2.42, "net_rs": 22620},
            "exit_robustness": {"0.9/2.5": "test 2.66 p0.005", "0.9/1.5": "test 4.35 p0.019",
                                "0.7/1.25": "test 3.26 p0.055", "0.7/1.0": "test 2.98 p0.120"},
            "threshold_robustness": ("NOT a knife-edge: train>1.5 & test>1.3 across the contiguous monotonic "
                                     "region pre2_mom_r[0.4,0.6] x sig5_adx_calc[24,30]"),
            "monthly": ("6/8 months positive; losers are Nov (-1.1k,n5) & Dec (-1.8k,n3) — smallest/earliest; "
                        "Jan-Jun all positive incl. the May-Jun test"),
            "diagnosis": ("the 20-bar-high breakout only pays with a genuine ADX-confirmed momentum thrust at "
                          "entry; the gate drops the late/chase/exhaustion breakouts that made it a loser"),
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=8 (thin) despite day-block p 0.005; found via a wide search (multiple-testing) but survives strong anti-overfit checks",
            "kept_because": ("robust momentum/ADX pocket: train 2.38 + test 2.66 (p0.005), both train halves "
                             "strong, monotonic+contiguous sensitivity, test-positive at every exit, mechanically "
                             "sound, live-computable (same pre-momentum gate family as D)"),
            "status_label": ("STRONG PROBATION — among the best edges in the book (train 2.38/test 2.66, p0.005); "
                             "n=39/8 small, do NOT size up; confirm on more history. Live runners should DROP the "
                             "old production gate and use this one."),
        },
    },

    # =======================================================================
    # L_DOUBLE_BOTTOM_VWAP — pre-momentum-gated double-bottom reclaim (L* aggressive iteration 2026-06-12).
    # *** EVALUATED ON THE RAW PRE-GATE POOL *** — the gated clean pool starves the L* family
    # (only L_BB_SQUEEZE survives, n=1 test), so this was searched/validated on raw candidates.
    # The UNGATED setup is a heavy loser (train 0.71 / test 0.57, -Rs 143k). The aggressive
    # train-PF>2 search (L_iterate.py) found a robust momentum/ADX gate: a double-bottom-at-VWAP
    # reclaim only pays when entered with a confirmed momentum thrust + trend strength — the SAME
    # mechanism that salvaged G_HIGHER_HIGH_BREAK. Validated NOT a knife-edge (L_validate_passer.py):
    # contiguous monotonic sensitivity pocket, both train halves strong (2.40/3.24), ALL 8 months
    # positive, test-positive at every exit. The sibling gate pre2_mom_r>=0.42 & adx>=28 confirms it.
    # =======================================================================
    "L_DOUBLE_BOTTOM_VWAP": {
        "side": "LONG",
        # RAW DETECTION (avwap_5min_ID_v2_backtesting.py L900, "double_bottom_vwap_reclaim").
        # Idea: a retest of the intraday 8-bar low (double bottom) that holds above VWAP and
        # closes strong on volume — a reversal long off support.
        "detection": {
            "reason_tag": "double_bottom_vwap_reclaim",
            "scan_window_ist": ["09:30", "14:30"],
            "conditions": [
                ("abs(low - intraday_low_8)", "<=", "0.40 * ATR"),  # double-bottom: near the 8-bar low
                ("close", ">", "VWAP"),                              # above_vwap (holds support above VWAP)
                ("close", ">", "open"),                              # long_struct
                ("close_loc", ">=", 0.60),                           # CLOSE_LOC_LONG_MIN (strong close)
                ("vol_ratio", ">=", 1.5),
            ],
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR (no blow-off bar)",
            },
            "feature_defs": {
                "intraday_low_8": "rolling 8-bar intraday low (the double-bottom reference)",
                "VWAP": "intraday session VWAP", "ATR": "per-day ATR",
                "close_loc": "(close - low)/(high - low)", "vol_ratio": "volume / Volume_SMA20",
            },
        },
        # TUNED v11 CONFIG — the PRE-MOMENTUM/ADX GATE is the edge.
        "exit": {"sl_pct": 0.90, "tgt_pct": 1.50},            # vs v6 default 0.70/0.80
        "mask_terms": [],
        # PRE-ENTRY MOMENTUM GATE (the edge) — 1-min features at entry; ALL required.
        # The reclaim only pays with a confirmed momentum thrust + trend strength:
        "pre_momentum_terms": [
            ["pre_entry_momentum_score", ">=", 79.0],    # strong composite pre-entry momentum (top ~30%)
            ["sig5_adx_calc", ">=", 28.0],               # 5-min ADX confirms a real trend
        ],
        "pre_momentum_alt_gate": [["pre2_mom_r", ">=", 0.42], ["sig5_adx_calc", ">=", 28.0]],  # equivalent (G-style); train 2.25/test 3.35
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-12", "family": "L",
            "cost_basis": "net_of_nse_intraday_costs",
            "evaluated_on": "RAW_PRE_GATE_POOL",
            "gating_caveat": ("*** Validated on RAW pre-gate candidates (historical_all_available_raw_candidates), "
                              "NOT the gated clean pool. The production v8/research-layer gates currently REMOVE "
                              "L_DOUBLE_BOTTOM_VWAP almost entirely (it is absent from the clean pool, and the live "
                              "discovery research filter blocks the L* family). To trade this, the research-layer "
                              "block must be lifted and the setup must rely on THIS momentum/ADX gate as its quality "
                              "filter. Confirm on a gated-pool rebuild before sizing. ***"),
            "pool": "L_DOUBLE_BOTTOM_VWAP raw candidates (Nov3-Jun10), train sampled to 700 + all test",
            "search_method": ("L_iterate.py aggressive train-PF>2 search (greedy + 2-term + 40k random, exit "
                              "co-optimized, robust=min train-halves); validated by L_validate_passer.py "
                              "(threshold sensitivity + halves + monthly + exits + term drop-out)"),
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "ungated_reference": {"train_pf": 0.71, "test_pf": 0.57, "full_net_rs": -143167, "note": "heavy loser; the gate creates the edge"},
            "train": {"trades": 33, "net_pf": 2.55, "half1_pf": 2.40, "half2_pf": 3.24},
            "test": {"trades": 13, "net_pf": 3.57, "win_pct": 69.0, "day_block_p": 0.033},
            "full": {"net_pf": 2.80, "net_rs": 22623},
            "exit_robustness": {"0.9/1.5": "train 2.55 test 3.57 p0.033", "0.9/1.25": "train 2.18 test 2.99 p0.056",
                                "0.7/1.5": "train 1.74 test 4.45 p0.019"},
            "threshold_robustness": ("contiguous monotonic pocket; pre_entry_momentum_score[75,85] x sig5_adx_calc[26,32] "
                                     "test-positive; sibling gate pre2_mom_r>=0.42 & adx>=28 also passes (train 2.25/test 3.35)"),
            "monthly": "ALL 8 months positive (Nov 2.25, Dec 1.45, Jan 4.05, Feb 2.57, Mar 5.15, Apr +, May 3.42, Jun +)",
            "diagnosis": ("a double-bottom-at-VWAP reclaim only pays with a confirmed momentum thrust + ADX trend at "
                          "entry; the gate drops the dead-cat/no-follow-through reclaims that made it a -Rs143k loser. "
                          "Same momentum/ADX mechanism as G_HIGHER_HIGH_BREAK (cross-setup consistency)."),
            "gate_status": "STRONG_PROBATION",
            "gate_miss": ("test n=13 (thin); evaluated on RAW pre-gate pool (gating caveat above); found via wide "
                          "search (multiple-testing) but survives strong anti-overfit checks"),
            "kept_because": ("robust momentum/ADX pocket: train 2.55 (both halves 2.40/3.24) + test 3.57 (p0.033), "
                             "ALL months positive, monotonic+contiguous sensitivity, exit-robust, mechanistically "
                             "identical to the validated G edge, live-computable gate"),
            "status_label": ("STRONG PROBATION (RAW-POOL) — strong edge (train 2.55/test 3.57, p0.033, all months +), "
                             "but evaluated on raw pre-gate candidates: reconcile with live gating before sizing. "
                             "n=33/13 small, do NOT size up."),
        },
    },

    # =======================================================================
    # L_PRESSURE_BURST_VWAP — WEAK/CAUTION probation (L_rejects_improve.py, 2026-06-12).
    # *** USER_APPROVED_OVERRIDE_WEAK *** — kept at user direction despite FAILING the anti-overfit
    # bar that G/L_DOUBLE_BOTTOM passed. The ungated setup is a heavy loser (train 0.66 / test 0.69,
    # -Rs 180k). A robustness-first 2-term search found an all-period-positive gate
    # (quality_score<=25 & pre1_adx>=44, exit 0.70/1.25 -> train 2.24 [halves 2.59/2.03] / test 2.03,
    # appears at 3 exits) — a real improvement over its old fragile 4-term overfit, BUT it FAILS the
    # validation checks: (1) test sensitivity is NON-monotonic (adx>=42 -> test 1.14, adx>=44 -> 2.03,
    # adx>=46 -> 1.77); (2) significant at only 1 of 4 exits (p 0.086; multiple-testing-inflated);
    # (3) monthly is thin/lumpy (Dec loser; carried by Nov n3 + Feb n5); (4) quality_score<=25 selects
    # LOW quality (counterintuitive). Treat as speculative — do NOT size; re-validate on a larger/gated
    # sample. The momentum/ADX mechanism (G/L_DOUBLE) does NOT work here (stays train-negative).
    # =======================================================================
    "L_PRESSURE_BURST_VWAP": {
        "side": "LONG",
        # RAW DETECTION (avwap_5min_ID_v2_backtesting.py L914, "buy_pressure_burst_vwap").
        # Idea: a buying-pressure burst (pressure_ratio>=3) above VWAP & EMA20 in a mid-RSI band.
        "detection": {
            "reason_tag": "buy_pressure_burst_vwap",
            "scan_window_ist": ["09:30", "14:30"],
            "conditions": [
                ("pressure_ratio", ">=", 3.0),               # buy pressure burst (up-vol / down-vol)
                ("close", ">", "VWAP"),                       # above_vwap
                ("close", ">", "EMA20"),
                ("vol_ratio", ">=", 1.5),
                ("RSI", "between", [50, 75]),                 # 50 <= rsi <= 75
            ],
            "common_gate": {
                "min_close_price": 80.0,
                "min_5m_traded_value_rs": "MIN_5M_TRADED_VALUE_RS (liquidity floor)",
                "min_day_value_by_1000_rs": "MIN_DAY_VALUE_BY_1000_RS (after 10:00 IST)",
                "max_candle_range_atr": "range <= MAX_CANDLE_RANGE_ATR * ATR",
            },
            "feature_defs": {
                "pressure_ratio": "up-volume / down-volume proxy (buy pressure)", "VWAP": "session VWAP",
                "EMA20": "5-min EMA20", "RSI": "5-min RSI", "vol_ratio": "volume / Volume_SMA20",
                "quality_score": "scanner composite quality metric (here LOW values are selected)",
            },
        },
        # TUNED v11 CONFIG — robustness-first gate (SPECULATIVE; see header).
        "exit": {"sl_pct": 0.70, "tgt_pct": 1.25},            # vs v6 default 1.10/0.90
        "mask_terms": [["quality_score", "<=", 25.0]],        # selects LOW scanner quality (counterintuitive)
        "pre_momentum_terms": [["pre1_adx", ">=", 44.0]],     # very high pre-entry ADX (strong trend)
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-12", "family": "L",
            "cost_basis": "net_of_nse_intraday_costs",
            "evaluated_on": "RAW_PRE_GATE_POOL",
            "gating_caveat": ("*** Validated on RAW pre-gate candidates; L_PRESSURE_BURST_VWAP is removed by the "
                              "production v8/research gates. Reconcile live gating before any use. ***"),
            "search_method": ("L_rejects_improve.py robustness-first 2-term search (objective = min(train_h1, "
                              "train_h2, test) PF); sensitivity-checked but FAILED the anti-overfit bar"),
            "train_window": ["2025-11-01", "2026-04-30"], "test_window": ["2026-05-01", "2026-06-10"],
            "ungated_reference": {"train_pf": 0.66, "test_pf": 0.69, "full_net_rs": -180265, "note": "heavy loser"},
            "train": {"trades": 26, "net_pf": 2.24, "half1_pf": 2.59, "half2_pf": 2.03},
            "test": {"trades": 12, "net_pf": 2.03, "win_pct": 67.0, "day_block_p": 0.086},
            "full": {"net_pf": 2.17, "net_rs": 10557},
            "exit_consistency": {"0.7/1.25": "train 2.24 test 2.03 p0.086", "0.7/1.0": "train 1.92 test 1.78 p0.123",
                                 "0.9/1.25": "train 1.87 test 1.84 p0.130", "0.5/2.5": "train 1.92 test 1.02 (collapses)"},
            "overfit_warnings": [
                "test sensitivity NON-monotonic across pre1_adx (42->1.14, 44->2.03, 46->1.77)",
                "significant at only 1 of 4 exits (p0.086, multiple-testing-inflated from a wide search)",
                "monthly thin/lumpy: Dec loser, carried by Nov(n3)+Feb(n5)",
                "quality_score<=25 selects LOW scanner quality (counterintuitive)",
                "the G/L_DOUBLE momentum/ADX mechanism does NOT work here (train stays negative)",
            ],
            "gate_status": "USER_APPROVED_OVERRIDE_WEAK",
            "gate_miss": "fails monotonic-sensitivity + multi-exit-significance; raw-pool; n=26/12 small",
            "kept_because": "user-directed (asked to make the rejects selectable); it IS the best of the 3 rejects and is all-period-positive, but treat as speculative",
            "status_label": ("WEAK / CAUTION (RAW-POOL) — speculative; promoted at user direction despite failing the "
                             "anti-overfit bar. Do NOT size. Re-validate on a larger/gated sample before trusting."),
        },
    },



    # =======================================================================
    # L_RS_LEADER_VWAP_HOLD — RS-leader VWAP test-and-hold continuation (LONG).
    # *** SCANNER-SOURCE setup *** (detection in new_setups_scan_v11.py). PROMOTED 2026-06-13 after
    # RE-VALIDATION on CORRECTED VWAP/regime data: the VWAP-bug fix RESCUED it (rejected before for 5
    # test trades; corrected data gives 204 test + a strong gate whose near-VWAP term is only meaningful
    # post-fix). Ungated -Rs120k loser; the gate is the edge. Replaces ground lost to S_UPTHRUST/T_TREND.
    # =======================================================================
    "L_RS_LEADER_VWAP_HOLD": {
        "side": "LONG",
        "detection": {
            "reason_tag": "rs_leader_vwap_test_hold_continuation",
            "source": "new_setups_scan_v11.py structural scan (live 5-min feed, CORRECTED session VWAP)",
            "scan_window_ist": ["09:45", "14:00"],
            "conditions": [
                ("rs_pct", ">=", 0.75), ("stock_ret", ">=", 0.30),
                ("close", ">", "EMA20"), ("EMA20", ">=", "EMA50"), ("ema20_slope_3bar", ">", 0),
                ("low", "<=", "VWAP + 0.30*ATR"), ("close", ">", "VWAP"),
                ("close", ">", "open"), ("close_loc", ">=", 0.60), ("close", ">", "prev_bar_high"),
                ("vol_ratio", ">=", 1.3), ("ADX", ">=", 20), ("RSI", "between", [50, 72]),
                ("regime", "!=", "BEAR"),
            ],
            "feature_defs": {
                "VWAP": "intraday SESSION VWAP (CORRECTED 2026-06-13: typical-price x volume, reset daily)",
                "vwap_dist_atr": "(close - session_VWAP)/ATR; near-VWAP = the 'VWAP test' (sane only post-fix)",
                "quality_score": "scanner composite quality", "signal_minute": "IST minute of the signal bar",
            },
        },
        # TUNED v11 CONFIG (Gate B). HIGH-quality RS leader, MORNING (<=11:00), STRONG volume, NEAR VWAP (the test).
        "exit": {"sl_pct": 0.50, "tgt_pct": 1.25},
        "exit_alt": {"sl_pct": 0.90, "tgt_pct": 1.25, "note": "cleaner day-spread (top1d 49%) but train 1.71"},
        "mask_terms": [
            ["quality_score", ">=", 97.121022],
            ["vol_ratio", ">=", 2.164331],
            ["vwap_dist_atr", "<=", 1.49336],
            ["signal_minute", "<=", 660.0],
        ],
        "pre_momentum_terms": [],
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "L (new, scanner-source)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "NEW_SETUPS_SCAN",
            "scan_source_csv": NEW_SETUPS_SCAN_SOURCE_CSV,
            "pool": "structural scan on CORRECTED session VWAP/regime (rebuilt 2026-06-13)",
            "exit_basis": "fixed 0.50/1.25, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.81, "test_pf": 0.51, "full_net_rs": -120981},
            "train": {"trades": 26, "net_pf": 2.74, "half1_pf": 2.65, "half2_pf": 2.81},
            "test": {"trades": 13, "days": 6, "net_pf": 3.82, "win_pct": 62.0, "day_block_p": 0.001, "top1day_pct": 58},
            "full": {"net_pf": 3.04, "net_rs": 14696},
            "exit_robustness": {"0.5/1.25": "test3.82 p0.001 top1d58", "0.7/1.25": "test2.94 p0.004",
                                "0.5/1.5": "test3.97 p0.004", "0.9/1.25": "test3.64 p0.003 top1d49", "0.5/1.0": "test3.27 p0.002"},
            "sig_exits": "5 of 5 (p 0.001-0.004)",
            "term_dropout": {"drop_quality_score": "train 1.17", "drop_signal_minute": "train 1.23",
                             "drop_vol_ratio": "train 2.31 (droppable)", "drop_vwap_dist_atr": "train 1.94 (refinement)",
                             "note": "robust 2-term core = quality_score & signal_minute"},
            "threshold_robustness": "monotone: quality 85->97 train 1.36->2.74; vwap_dist_atr tighter(near-VWAP)=better",
            "monthly": "9/11 months positive (82%); losers Jul/Aug 2025 (earliest/smallest)",
            "vwap_fix_note": "RESCUED by the 2026-06-13 VWAP fix; the bug had HIDDEN this real edge (see project_vwap_regime_databug_2026_06_13)",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=13/6d (all May); top1day 58% at primary exit (0.9/1.25 -> 49%); scanner-source",
            "wiring_caveat": "SCANNER-source (like the removed S_UPTHRUST): inject via the conf-mode external-candidate path; uses CORRECTED session VWAP",
            "status_label": "STRONG PROBATION - real edge on CORRECTED data (train 2.74/test 3.82, 5/5 exits, 82% months); thin May test. Do NOT size up.",
        },
    },

    # =======================================================================
    # P_PDH_BREAK_RETEST_LONG - prev-day-high break-and-retest continuation (LONG).
    # *** TIER123 / SCANNER-SOURCE *** (research_v11_tier123_new_setups.py, Tier 3). PROMOTED 2026-06-13
    # after validation on the CORRECTED-VWAP tier123 probe (survives the full anti-overfit battery that
    # exposed S_UPTHRUST/T_TREND as artifacts). Ungated -Rs173k loser; the gate is the edge.
    # =======================================================================
    "P_PDH_BREAK_RETEST_LONG": {
        "side": "LONG",
        "detection": {
            "reason_tag": "pdh_break_retest_long",
            "source": "research_v11_tier123_new_setups.py (Tier 3); CORRECTED session VWAP/regime",
            "idea": "price breaks the previous-day high, pulls back and retests it, then resumes up",
        },
        "exit": {"sl_pct": 0.50, "tgt_pct": 0.60},
        "exit_alt": {"sl_pct": 0.50, "tgt_pct": 0.80, "note": "wider target: test 4.14 p0.034 top1d33"},
        "mask_terms": [["body_pct", "<=", 0.749993]],
        "pre_momentum_terms": [
            ["pre_entry_momentum_score", ">=", 75.071712],
            ["pre3_range_r", ">=", 0.499787],
        ],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "P (new tier123, scanner-source)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "TIER123_OVERLAY_PROBE",
            "scan_source_csv": TIER123_SCAN_SOURCE_CSV,
            "pool": "tier123 probe on CORRECTED session VWAP/regime (rebuilt 2026-06-13)",
            "exit_basis": "fixed 0.50/0.60, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.56, "test_pf": 0.60, "full_net_rs": -173278},
            "train": {"trades": 34, "net_pf": 2.39, "half1_pf": 2.50, "half2_pf": 2.32},
            "test": {"trades": 10, "days": 9, "net_pf": 6.88, "win_pct": 90.0, "day_block_p": 0.004, "top1day_pct": 25},
            "full": {"net_pf": 2.87, "net_rs": 10267},
            "exit_robustness": {"0.5/0.6": "test6.88 p0.004 top1d25", "0.7/0.6": "test5.12 p0.016",
                                "0.5/0.8": "test4.14 p0.034", "0.7/0.8": "test3.08 p0.064", "0.5/1.0": "test3.00 p0.082"},
            "sig_exits": "5 of 5 (p 0.004-0.082)",
            "term_dropout": {"drop_pre_entry_momentum_score": "train 0.72", "drop_body_pct": "train 0.90",
                             "drop_pre3_range_r": "train 1.97 (refinement)"},
            "threshold_robustness": "pre3_range_r monotone-robust; pre_entry_mom>=75 is a cliff but battery clean",
            "monthly": "10/12 months positive (83%)",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=10/9d; tight 0.60-target scalp; tier123/scanner-source; wide-search origin (battery is the defense)",
            "wiring_caveat": "TIER123/SCANNER-source: inject via the conf-mode external-candidate path; uses CORRECTED session VWAP",
            "status_label": "STRONG PROBATION (corrected-VWAP) - 5/5 exits, 83% months, top1d 25-44%, even halves. Do NOT size up.",
        },
    },

    # =======================================================================
    # E_ORB_RETEST_HOLD_LONG - opening-range retest-and-hold continuation (LONG).
    # *** TIER123 / SCANNER-SOURCE *** (research_v11_tier123_new_setups.py, Tier 1). PROMOTED 2026-06-13
    # after validation on the CORRECTED-VWAP tier123 probe. Ungated -Rs196k loser; MONOTONE ADX sensitivity.
    # =======================================================================
    "E_ORB_RETEST_HOLD_LONG": {
        "side": "LONG",
        "detection": {
            "reason_tag": "orb_retest_hold_long",
            "source": "research_v11_tier123_new_setups.py (Tier 1); CORRECTED session VWAP/regime",
            "idea": "after an opening-range-high break, price retests the OR high and HOLDS, then continues up",
        },
        "exit": {"sl_pct": 0.90, "tgt_pct": 1.25},
        "exit_alt": {"sl_pct": 0.70, "tgt_pct": 1.00, "note": "test 3.60 p0.007 top1d45"},
        "mask_terms": [
            ["vol_ratio", ">=", 2.423842],
            ["quality_score", ">=", 86.575268],
            ["signal_minute", ">=", 605.0],
        ],
        "pre_momentum_terms": [["sig5_adx_calc", ">=", 42.41646]],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "E (new tier123, scanner-source)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "TIER123_OVERLAY_PROBE",
            "scan_source_csv": TIER123_SCAN_SOURCE_CSV,
            "pool": "tier123 probe on CORRECTED session VWAP/regime (rebuilt 2026-06-13)",
            "exit_basis": "fixed 0.90/1.25, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.78, "test_pf": 0.62, "full_net_rs": -196186},
            "train": {"trades": 27, "net_pf": 2.54, "half1_pf": 2.46, "half2_pf": 2.63},
            "test": {"trades": 18, "days": 7, "net_pf": 2.50, "win_pct": 72.0, "day_block_p": 0.017, "top1day_pct": 41},
            "full": {"net_pf": 2.52, "net_rs": 18231},
            "exit_robustness": {"0.9/1.25": "test2.50 p0.017 top1d41", "0.7/1.0": "test3.60 p0.007",
                                "0.9/1.0": "test2.86 p0.025", "1.1/1.0": "test2.38 p0.049", "0.5/1.25": "p0.168 FAIL"},
            "sig_exits": "4 of 5 (p 0.007-0.049)",
            "term_dropout": {"drop_sig5_adx_calc": "train 1.46", "drop_vol_ratio": "train 1.56",
                             "drop_quality_score": "train 1.85 (refinement)", "drop_signal_minute": "train 2.38 (refinement)",
                             "note": "robust 2-term core = sig5_adx_calc & vol_ratio"},
            "threshold_robustness": "MONOTONE on sig5_adx_calc (30->48 train 1.33->2.90) - stronger trend = better, not a cliff",
            "monthly": "10/11 months positive (91%)",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=18/7d; high-ADX selective; tier123/scanner-source; wide-search origin (battery is the defense)",
            "wiring_caveat": "TIER123/SCANNER-source: inject via the conf-mode external-candidate path; uses CORRECTED session VWAP",
            "status_label": "STRONG PROBATION (corrected-VWAP) - monotone sens, 4/5 exits, 91% months, top1d 41-56%, even halves. Do NOT size up.",
        },
    },

    # =======================================================================
    # V_RECLAIM_PULLBACK_LONG - reclaim-pullback continuation in a strong-trend RS leader (LONG).
    # *** TIER123 / SCANNER-SOURCE *** (research_v11_tier123_new_setups.py, Tier 1). ADDED 2026-06-13
    # after IMPROVING the weak first gate: the robust altB gate (a pullback = low pre-entry momentum,
    # in a strong-ADX RS-positive leader) survives the full battery (5/5 exits, even halves, monotone ADX).
    # =======================================================================
    "V_RECLAIM_PULLBACK_LONG": {
        "side": "LONG",
        "detection": {
            "reason_tag": "reclaim_pullback_long",
            "source": "research_v11_tier123_new_setups.py (Tier 1); CORRECTED session VWAP/regime",
            "idea": "after a pullback, price reclaims and resumes the uptrend",
        },
        "exit": {"sl_pct": 0.50, "tgt_pct": 0.80},
        "exit_alt": {"sl_pct": 0.90, "tgt_pct": 0.80, "note": "test 3.13 p0.014 top1d34"},
        "mask_terms": [["rs_pct", ">=", 0.372426]],
        "pre_momentum_terms": [
            ["pre_entry_momentum_score", "<=", 58.013438],
            ["sig5_adx_calc", ">=", 33.932755],
        ],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "V (new tier123, scanner-source)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "TIER123_OVERLAY_PROBE",
            "scan_source_csv": TIER123_SCAN_SOURCE_CSV,
            "pool": "tier123 probe on CORRECTED session VWAP/regime (rebuilt 2026-06-13)",
            "exit_basis": "fixed 0.50/0.80, 1-min path to 15:20 EOD",
            "improved_from": "first gate sig5_adx<=16.9 & pre5_dir_count<=2 was a narrow low-ADX CLIFF (rejected); altB is robust",
            "ungated_reference": {"train_pf": 0.73, "test_pf": 0.70, "full_net_rs": -120314},
            "train": {"trades": 28, "net_pf": 2.02, "half1_pf": 2.04, "half2_pf": 2.01},
            "test": {"trades": 12, "days": 7, "net_pf": 5.28, "win_pct": 83.0, "day_block_p": 0.001, "top1day_pct": 29},
            "full": {"net_pf": 2.58, "net_rs": 10644},
            "exit_robustness": {"0.5/0.8": "test5.28 p0.001 top1d29", "0.7/0.8": "test3.93 p0.006",
                                "0.5/1.0": "test3.92 p0.004", "0.9/0.8": "test3.13 p0.014", "0.5/1.25": "test3.25 p0.019"},
            "sig_exits": "5 of 5 (p 0.001-0.019)",
            "term_dropout": {"drop_pre_entry_momentum_score": "train 1.08", "drop_sig5_adx_calc": "train 1.02", "drop_rs_pct": "train 1.21"},
            "threshold_robustness": "MONOTONE on sig5_adx_calc (24->48 train 1.17->2.64); pre_entry_mom<=58 peaks (not a hard cliff)",
            "monthly": "7/10 months positive (70%)",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=12/7d; monthly 70% (borderline); tier123/scanner-source; wide-search origin (battery is the defense)",
            "wiring_caveat": "TIER123/SCANNER-source: inject via the conf-mode external-candidate path; uses CORRECTED session VWAP",
            "status_label": "STRONG PROBATION (corrected-VWAP, improved gate) - 5/5 exits, even halves, monotone ADX, top1d 29-44%. Do NOT size up.",
        },
    },

    # =======================================================================
    # B_HUGE_RED_FAILED_BOUNCE - failed-bounce short after a huge red bar (SHORT).
    # Reverse-engineered (short_mine) from the big clean-pool short pool; pre-momentum-gated.
    # PROMOTED 2026-06-13 on CORRECTED-VWAP data; survives the full battery (5/5 exits, EVEN halves,
    # robust plateaus, top1day 21-28% over 16 test days). NOT market_ret-conditioned. Ungated -Rs142k loser.
    # =======================================================================
    "B_HUGE_RED_FAILED_BOUNCE": {
        "side": "SHORT",
        "detection": {
            "reason_tag": "huge_red_failed_bounce_short",
            "source": "production clean-pool scanner (raw_candidates); CORRECTED session VWAP/regime",
            "idea": "after a huge red bar, price bounces weakly and fails -> resume down",
        },
        "exit": {"sl_pct": 0.90, "tgt_pct": 1.25},
        "mask_terms": [],
        "pre_momentum_terms": [
            ["pre3_close_pos", "<=", 0.581797],
            ["sig5_rsi_dir", "<=", 64.104659],
            ["pre5_mom_r", "<=", 0.284145],
        ],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "B (mined short)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "RAW_PRE_GATE_POOL",
            "pool": "B_HUGE_RED_FAILED_BOUNCE clean-pool shorts (sampled 1300tr/628te), CORRECTED VWAP",
            "exit_basis": "fixed 0.90/1.25, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.82, "test_pf": 0.84, "full_net_rs": -142044},
            "train": {"trades": 30, "net_pf": 2.90, "half1_pf": 2.66, "half2_pf": 3.06},
            "test": {"trades": 20, "days": 16, "net_pf": 3.49, "win_pct": 70.0, "day_block_p": 0.011, "top1day_pct": 26},
            "full": {"net_pf": 3.15, "net_rs": 15905},
            "exit_robustness": {"0.9/1.25": "test3.49 p0.011 top1d26", "0.9/0.8": "test2.82 p0.025",
                                "1.1/1.25": "test3.08 p0.022", "0.7/1.25": "test3.10 p0.010", "0.9/1.5": "test3.21 p0.015"},
            "sig_exits": "5 of 5 (p 0.010-0.025)",
            "term_dropout": {"drop_pre3_close_pos": "train 0.88 (the edge)", "drop_sig5_rsi_dir": "train 1.40", "drop_pre5_mom_r": "train 1.97 (refinement)"},
            "threshold_robustness": "ROBUST plateaus (not cliffs): pre3_close_pos<=0.4..0.7 train 2.36-2.90; pre5_mom_r<=0.1..0.55 train 2.3-2.9",
            "monthly": "6/8 months positive (75%)",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "test n=20/16d (good spread); mined via wide search (battery is the defense); sampled pool",
            "status_label": "STRONG PROBATION (corrected-VWAP) - the cleanest mined short: 5/5 exits, even halves, top1d 21-28%, robust. Do NOT size up.",
        },
    },

    # =======================================================================
    # C_OR_BREAKDOWN - opening-range breakdown continuation in a strong downtrend (SHORT).
    # Reverse-engineered (short_mine); SIMPLE 2-term pre-momentum gate. PROMOTED 2026-06-13 on
    # CORRECTED-VWAP data; 5/5 exits significant, top1day 27-37%, NOT market_ret-conditioned.
    # =======================================================================
    "C_OR_BREAKDOWN": {
        "side": "SHORT",
        "detection": {
            "reason_tag": "or_breakdown_short",
            "source": "production clean-pool scanner (raw_candidates); CORRECTED session VWAP/regime",
            "idea": "break of the opening-range low; continuation down",
        },
        "exit": {"sl_pct": 0.90, "tgt_pct": 2.00},
        "exit_alt": {"sl_pct": 0.90, "tgt_pct": 1.50, "note": "train 4.28 test 4.85 p0.003"},
        "mask_terms": [],
        "pre_momentum_terms": [
            ["sig5_adx_calc", ">=", 39.670518],
            ["pre1_adx", "<=", 21.368044],
        ],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "C (mined short)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "RAW_PRE_GATE_POOL",
            "pool": "C_OR_BREAKDOWN clean-pool shorts (sampled 1300tr/1300te), CORRECTED VWAP",
            "exit_basis": "fixed 0.90/2.00, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.84, "test_pf": 0.99, "full_net_rs": -94224},
            "train": {"trades": 29, "net_pf": 2.78, "half1_pf": 1.71, "half2_pf": 4.21},
            "test": {"trades": 19, "days": 13, "net_pf": 5.26, "win_pct": 63.0, "day_block_p": 0.002, "top1day_pct": 29},
            "full": {"net_pf": 3.47, "net_rs": 23357},
            "exit_robustness": {"0.9/2.0": "test5.26 p0.002 top1d29", "1.1/2.0": "test4.89 p0.002",
                                "0.9/1.5": "test4.85 p0.003", "0.7/1.5": "test3.75 p0.012", "1.1/1.5": "test4.51 p0.003"},
            "sig_exits": "5 of 5 (p 0.002-0.012)",
            "term_dropout": {"drop_sig5_adx_calc": "train 0.87", "drop_pre1_adx": "train 1.12", "note": "both load-bearing"},
            "threshold_robustness": "sig5_adx peaks ~40 (broad: 35-45 all train>1.5/test>2); pre1_adx<=~21 monotone (lower=better)",
            "diagnosis": "strong 5-min downtrend (sig5_adx>=40) breaking the OR-low after a LOW pre-entry-ADX pause (pre1_adx<=21) = controlled breakdown continuation",
            "monthly": "6/8 months positive (75%)",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "IMBALANCED train halves (h1 1.71 / h2 4.21 - edge stronger in later train history); test n=19/13d; sampled pool",
            "status_label": "STRONG PROBATION (corrected-VWAP) - simple 2-term, 5/5 exits, top1d 27-37%; halves imbalanced (h1 weak). Do NOT size up.",
        },
    },

    # =======================================================================
    # A_MOD_BREAK_C1_LOW - momentum breakdown from a tight pre-break range (SHORT).
    # Reverse-engineered (deeper mine of the biggest short pool, 43808). Pre-momentum + volume gated.
    # PROMOTED 2026-06-13 on CORRECTED-VWAP data: MONOTONE sensitivity (tighter pre3_range -> higher PF),
    # even halves, 88% months, NOT market_ret-conditioned. Ungated -Rs211k loser.
    # =======================================================================
    "A_MOD_BREAK_C1_LOW": {
        "side": "SHORT",
        "detection": {
            "reason_tag": "a_mod_break_c1_low_short",
            "source": "production clean-pool scanner (raw_candidates); CORRECTED session VWAP/regime",
            "idea": "break of the prior C1 (first-candle) low; momentum continuation down",
        },
        "exit": {"sl_pct": 1.10, "tgt_pct": 1.00},
        "exit_alt": {"sl_pct": 0.90, "tgt_pct": 1.00, "note": "cleaner day-spread top1d 33% (train 2.10/test 1.99)"},
        "mask_terms": [["vol_ratio", ">=", 1.955814]],
        "pre_momentum_terms": [
            ["pre5_mom_r", ">=", 0.425861],
            ["pre3_range_r", "<=", 0.202087],
        ],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "A (mined short)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "RAW_PRE_GATE_POOL",
            "pool": "A_MOD_BREAK_C1_LOW clean-pool shorts (deeper mine, sampled 3000tr/3000te), CORRECTED VWAP",
            "exit_basis": "fixed 1.10/1.00, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.82, "test_pf": 0.99, "full_net_rs": -210904},
            "train": {"trades": 38, "net_pf": 2.58, "half1_pf": 2.52, "half2_pf": 2.62},
            "test": {"trades": 30, "days": 13, "net_pf": 2.83, "win_pct": 70.0, "day_block_p": 0.030, "top1day_pct": 55},
            "full": {"net_pf": 2.69, "net_rs": 23276},
            "exit_robustness": {"1.1/1.0": "test2.83 p0.030 top1d55", "0.9/1.0": "test1.99 p0.022 top1d33",
                                "1.1/1.5": "test2.63 p0.021 top1d45", "0.9/2.0": "train1.80 test2.05 p0.014 top1d28", "0.7/1.5": "p0.070"},
            "sig_exits": "4 of 5 (p 0.014-0.030)",
            "term_dropout": {"drop_pre5_mom_r": "train 0.75 (the edge)", "drop_pre3_range_r": "train 0.98 (the edge)", "drop_vol_ratio": "train 2.14 (refinement)"},
            "threshold_robustness": "MONOTONE: pre3_range_r tighter -> much higher PF (<=0.12 train 14.19); vol_ratio higher -> higher (>=3.0 train 4.03); pre5_mom_r>=0.43 threshold",
            "diagnosis": "an A-mod C1-low breakdown with strong recent down-momentum (pre5_mom_r) out of a TIGHT pre-break range (pre3_range_r low) on volume = momentum-down continuation from consolidation",
            "monthly": "7/8 months positive (88%)",
            "alt_gate_note": "a 2nd A_MOD gate (pre1_adx<=21 & vwap_dist_atr<=-5.37 + ATR band) tests even higher (test 4.4, top1d 30%) but leans on a tuned ATR band -> not promoted",
            "gate_status": "STRONG_PROBATION",
            "gate_miss": "top1day 55% at primary exit (use 0.9/1.0 for 33%); mined via wide search (battery is the defense); sampled pool",
            "status_label": "STRONG PROBATION (corrected-VWAP) - monotone tight-range/momentum gate, even halves, 88% months. Do NOT size up.",
        },
    },

    # =======================================================================
    # G_LOWER_LOW_BREAK - volume-climax exhaustion lower-low break (SHORT). Reverse-engineered (deeper mine).
    # PROMOTED 2026-06-13 on CORRECTED-VWAP data: 4/5 exits sig, EVEN halves, top1day 27%, 100% months,
    # NOT market_ret-conditioned. *** SELECTIVE: needs ~4x volume -> low trade count (test n=9). ***
    # =======================================================================
    "G_LOWER_LOW_BREAK": {
        "side": "SHORT",
        "detection": {
            "reason_tag": "lower_low_break_short",
            "source": "production clean-pool scanner (raw_candidates); CORRECTED session VWAP/regime",
            "idea": "break of a recent lower-low; here gated to a high-volume climax/exhaustion break",
        },
        "exit": {"sl_pct": 1.10, "tgt_pct": 1.00},
        "exit_alt": {"sl_pct": 0.90, "tgt_pct": 0.80, "note": "test 7.19 p0.005 top1d28"},
        "mask_terms": [["vol_ratio", ">=", 4.129044], ["quality_score", ">=", 76.444124]],
        "pre_momentum_terms": [["sig5_rsi_dir", ">=", 68.747209]],
        "pre_momentum_missing_action": "block",
        "entry_guards": {},
        "entry_model": "next_1min_open_after_5min_signal + 5bps paper slippage",
        "exit_model": "resolve on 1-min OHLC to 15:20 IST (TARGET / SL / EOD)",
        "provenance": {
            "approved_on": "2026-06-13", "family": "G (mined short, selective)",
            "cost_basis": "net_of_nse_intraday_costs", "evaluated_on": "RAW_PRE_GATE_POOL",
            "pool": "G_LOWER_LOW_BREAK clean-pool shorts (deeper mine, ~1900tr/470te), CORRECTED VWAP",
            "exit_basis": "fixed 1.10/1.00, 1-min path to 15:20 EOD",
            "ungated_reference": {"train_pf": 0.73, "test_pf": 1.16, "full_net_rs": -174682},
            "train": {"trades": 51, "net_pf": 2.25, "half1_pf": 2.46, "half2_pf": 2.11},
            "test": {"trades": 9, "days": 6, "net_pf": 9.12, "win_pct": 67.0, "day_block_p": 0.004, "top1day_pct": 27},
            "full": {"net_pf": 2.50, "net_rs": 17097},
            "exit_robustness": {"1.1/1.0": "test9.12 p0.004 top1d27", "0.9/0.8": "test7.19 p0.005", "0.9/1.0": "test9.12 p0.004", "1.1/0.8": "test7.19 p0.005", "0.7/1.0": "train1.69"},
            "sig_exits": "4 of 5 (p 0.004-0.005)",
            "term_dropout": {"drop_vol_ratio": "train 0.94", "drop_sig5_rsi_dir": "train 0.88", "drop_quality_score": "train 1.34", "note": "all load-bearing"},
            "threshold_robustness": "vol_ratio>=4 monotone-ish; sig5_rsi_dir>=68.7 is a cliff (narrow); quality>=76 peaks",
            "diagnosis": "a lower-low break on a VOLUME CLIMAX (>=4x) with high RSI-direction = capitulation/exhaustion short",
            "monthly": "8/8 months positive (100%)",
            "gate_status": "WEAK_SELECTIVE_PROBATION",
            "gate_miss": "SELECTIVE (4x volume) -> thin count: test n=9/6d; sig5_rsi_dir cliff; mined via wide search",
            "status_label": "WEAK/SELECTIVE PROBATION (corrected-VWAP) - passes battery (4/5 exits, even halves, top1d27%, 100% months) but fires rarely (4x vol). Do NOT size up; lowest-count short.",
        },
    },
}


# ===========================================================================
# RESEARCH_WATCH_CONF — DISABLED. These are NOT part of the tradeable book.
# ---------------------------------------------------------------------------
# Three E* setups that were deeply salvaged (E_rejected_salvage.py: entry->EOD
# 1-min price PATHS swept over SL in {0.5,0.7,0.85,1.0} x Tgt in {0.4,0.6,0.8,
# 1.0,1.2,1.5} x sub-populations {regime, market_ret, vol_ratio, time, vwap_dist}
# x pre-momentum ON/OFF, train/test split, day-block bootstrap, NET of cost).
# NONE produced a config clearing the acceptance bar (train PF>=1.5 & test PF>=1.3
# & day-block p<0.10). They are recorded here ONLY so the config-of-record is
# complete and re-validation has a documented starting point. v11 must SKIP any
# entry with enabled=False. Promoting any of these reintroduces confirmed losers
# (E_ORB_BREAKOUT_SHORT was the single biggest churn/cost sink in the portfolio).
# ===========================================================================
RESEARCH_WATCH_CONF = {

    "D_AVWAP_LOSE_REVERSAL": {
        "enabled": False,
        "side": "SHORT",
        "production_candidate": "REJECT (clean gate was a small-sample fluke; only down-market gates work)",
        "detection": {"reason_tag": "avwap_lose_reversal_short", "source": "production clean-pool scanner",
                      "idea": "lose anchored-VWAP reversal short"},
        "best_found": {"non_market_gate": "atr_pct>=0.0035 & pre10_mom_r<=0.105 & sig5_adx_calc<=28.7",
                       "first_mine_n26": {"train_pf": 2.98}, "deeper_mine_n82": {"train_pf": 1.06}},
        "why_rejected": ("the first short-mine showed train PF 2.98 on only 26 gated trades; the DEEPER mine (82 gated "
                         "trades, more representative) collapses it to train PF 1.06 - a small-sample fluke. Its only "
                         "PF>2 gates on the deeper sample are market_ret<=-0.63 (down-market conditioned = the trap). "
                         "No robust non-market edge. See project_mined_shorts_2026_06_13."),
        "revalidation_trigger": "do NOT promote unless a NON-market-conditioned gate holds train PF>=1.5 on a >=80-trade gated sample with >=3 exits",
    },

    "E_ORB_RETEST_HOLD_SHORT": {
        "enabled": False,
        "side": "SHORT",
        "production_candidate": "REJECT (no robust gate after improvement attempt 2026-06-13)",
        "detection": {"reason_tag": "orb_retest_hold_short", "source": "research_v11_tier123_new_setups.py (Tier 1)",
                      "idea": "opening-range-low break, retest-and-hold, continue down"},
        "best_found": {"gate": "pre5_dir_count>=3 & signal_minute>=620 & pre1_adx>=36.2 & close_loc>=0.1", "exit": {"sl_pct": 0.9, "tgt_pct": 2.0},
                       "train_pf": 3.57, "test_pf": 2.53, "test_n": 9, "day_block_p": 0.045, "top1day_pct": 43},
        "why_rejected": ("the short side has no robust edge. altA (pre10_mom/vol/pre3_close/pre3_range) works ONLY at the "
                         "tight 0.6 target (1/5 exits; 0.8-target exits are losers, top1day 156-426%). altB depends on a "
                         "NARROW TIME CLIFF (signal_minute 620-690 only -> drop it and train falls to 1.33) with IMBALANCED "
                         "train halves (5.0/2.6). Neither survives the full battery. See tier123_new_setups_diagnosis_v11.md."),
        "revalidation_trigger": "do NOT promote unless a <=3-term, time-window-robust gate passes >=3 exits with balanced halves & top1day<=55% on CORRECTED data",
    },

    "T_TREND_DAY_EMA_STAIR_SHORT": {
        "enabled": False,
        "side": "SHORT",
        "production_candidate": "REJECT (FAILED re-validation on corrected VWAP/regime data 2026-06-13)",
        "detection": {"reason_tag": "trend_day_ema20_stair_short", "source": "tier123 probe (research_v11_tier123_new_setups)",
                      "idea": "downtrend EMA20-stair continuation short, weak close below VWAP, before 14:00"},
        "former_config": {"mask_terms": [["vol_ratio", "<=", 1.33]], "pre_momentum_terms": [["pre3_range_r", ">=", 0.404]],
                          "exit": {"sl_pct": 0.90, "tgt_pct": 0.80}},
        "evidence": {
            "original_broken_data": {"train_pf": 2.53, "test_pf": 4.49, "test_n": 8, "day_block_p": 0.056, "months_pos": "7/9"},
            "corrected_vwap_regime_2026_06_13": {"train_pf": 0.49, "train_halves": [0.50, 0.49], "test_n": 1,
                                                 "note": "TRAIN LOSER all exits (0.37-0.49); corrected close<VWAP filter cut population 2498->676; gate selects 15 train / 1 test"},
        },
        "why_rejected": ("its train-2.53 edge was a BROKEN-VWAP artifact. The stale NIFTY/stock VWAP made the detection's "
                         "'close<VWAP' condition a near-no-op (population 2498). With corrected session VWAP the condition "
                         "is real (population 676), and the book gate (vol_ratio<=1.33 & pre3_range_r>=0.404) collapses to "
                         "15 train trades at PF 0.49 with 1 test trade. The corrected search finds 0 train-PF>=2 configs; "
                         "robustness-first only finds regime==BEAR/down-market gates (top1day 89-111%, the May-12 trap). "
                         "See project_vwap_regime_databug_2026_06_13."),
        "revalidation_trigger": "do NOT promote unless a <=3-term gate on CORRECTED tier123 data shows train PF>=1.5 & test PF>=1.3 & p<0.10 & top1day<=55% over >=2 exits with test n>=8",
    },

    "S_UPTHRUST_TRAP_FADE": {
        "enabled": False,
        "side": "SHORT",
        "production_candidate": "REJECT (FAILED re-validation on corrected VWAP/regime data 2026-06-13)",
        "detection": {"reason_tag": "failed_high_upthrust_trap_distribution", "source": "new_setups_scan_v11.py",
                      "idea": "broke prior 10-bar high then closed back below on volume + upper wick (distribution)"},
        "former_config": {"mask_terms": [["ema20_slope", "<=", -0.005899], ["rsi3max", ">=", 50.655474]],
                          "pre_momentum_terms": [["pre2_mom_r", ">=", 0.128328]], "exit": {"sl_pct": 0.70, "tgt_pct": 0.80}},
        "evidence": {
            "original_broken_data": {"train_pf": 1.67, "test_pf": 2.76, "day_block_p": 0.001, "months_pos": "7/11"},
            "corrected_vwap_regime_2026_06_13": {"train_pf": 0.59, "train_halves": [0.56, 0.64], "months_pos": "3/11",
                                                 "note": "TRAIN LOSER at every exit (0.57-0.69); test still ~ungated level"},
        },
        "why_rejected": ("its train-1.67 edge was an ARTIFACT of the broken regime (NIFTY VWAP stale -> regime ~always "
                         "BEAR -> the regime!=BULL filter was a no-op + altered the candidate population). On corrected "
                         "VWAP/regime data the same gate is a train loser (PF 0.59); the corrected search only finds "
                         "regime==BEAR / down-market gates that are day-concentrated (top1day 76-120%, the May-12 trap). "
                         "No clean replacement gate. See project_vwap_regime_databug_2026_06_13."),
        "revalidation_trigger": "do NOT promote unless a <=3-term gate on CORRECTED data shows train PF>=1.5 & test PF>=1.3 & p<0.10 & top1day<=55% over >=2 exits",
    },

    "E_ORB_BREAKOUT_SHORT": {
        "enabled": False,
        "side": "SHORT",
        "production_candidate": "REJECT",
        # RAW DETECTION (OR-low breakdown short). Reference only.
        "detection": {"reason_tag": "orb_breakout_short", "idea": "break of the opening-range low"},
        # Best config the salvage sweep could find (still a loser — documented for re-validation only).
        "best_found": {
            "subpop": "time10-11 (signal 10:00-11:00 IST)",
            "exit": {"sl_pct": 1.00, "tgt_pct": 1.20},
            "pre_momentum_terms": [],                     # PM gate HURTS test (0.56) — dropped
        },
        "evidence": {
            "fixed_exit_full": {"train_pf": 0.90, "test_pf": 0.74, "note": "every regime/market/vol bucket sub-1.0"},
            "salvage_best": {"subpop": "time10-11", "sl": 1.00, "tgt": 1.20,
                             "train_n": 656, "train_pf": 1.04, "test_n": 133, "test_pf": 0.94,
                             "test_win_pct": 52.6, "day_block_p": 0.586},
            "premom_off_best": {"train_pf": 0.91, "test_pf": 0.99},
        },
        "why_rejected": ("the big churn/cost-sink (~1,200 train trades); no sub-population at any exit "
                         "reaches a tradeable PF — best is train 1.04 / test 0.94, p 0.586 (random); "
                         "the pre-momentum gate makes the test WORSE (0.56)."),
        "revalidation_trigger": "do NOT promote unless a mechanically-justified sub-population shows train PF>=1.5 AND test PF>=1.3 AND day-block p<0.10 on >=2x more history",
    },

    "E_ORB_BREAKOUT_LONG": {
        "enabled": False,
        "side": "LONG",
        "production_candidate": "REJECT",
        "detection": {"reason_tag": "orb_breakout_long", "idea": "break of the opening-range high"},
        "best_found": {
            "subpop": "less_extended (vwap_dist_atr < 1.0 at entry)",
            "exit": {"sl_pct": 0.70, "tgt_pct": 1.50},
            "pre_momentum_terms": [],
        },
        "evidence": {
            "fixed_exit_full": {"train_pf": 0.69, "test_pf": 0.73, "immediate_fail_pct": 22,
                                "note": "22% immediate-fail = chasing OR-high breakouts that reverse"},
            "salvage_best": {"subpop": "less_extended_vwap<1", "sl": 0.70, "tgt": 1.50,
                             "train_n": 58, "train_pf": 0.96, "test_n": 27, "test_pf": 0.91,
                             "test_win_pct": 33.3, "day_block_p": 0.589},
        },
        "why_rejected": ("breakout-chase with 22% immediate-fail; even restricting to the less-extended "
                         "entries (the most defensible cell) and sweeping exits, best is train 0.96 / "
                         "test 0.91, p 0.589 — no edge at any exit."),
        "revalidation_trigger": "do NOT promote unless an entry-timing fix (later/confirmed break) lifts a sub-population to train PF>=1.5 AND test PF>=1.3 AND p<0.10",
    },

    "E_VWAP_BAND_FADE": {
        "enabled": False,
        "side": "SHORT",
        "production_candidate": "REJECT (closest of the three — but no train edge)",
        "detection": {"reason_tag": "vwap_band_fade", "idea": "fade an over-extension at the VWAP band"},
        # The salvage CONFIRMED the diagnosis: the production 0.60% target was unreachable
        # (mfe_R 0.60). Widening the target to 1.20 + restricting to the near-VWAP cell lifts
        # train 0.67 -> 1.14 and test to 1.34 — but train still < 1.5 and test n=7 (p 0.276).
        "best_found": {
            "subpop": "near_vwap (vwap_dist_atr in [-1, 0] at entry)",
            "exit": {"sl_pct": 1.00, "tgt_pct": 1.20},    # widened from the unreachable 0.70/0.60
            "pre_momentum_terms": [],
        },
        "evidence": {
            "fixed_exit_full": {"train_pf": 0.67, "test_pf": 0.78, "mfe_R": 0.60,
                                "note": "0.60% target unreachable -> fade does not run; THIS was the bug"},
            "salvage_best": {"subpop": "near_vwap_-1..0", "sl": 1.00, "tgt": 1.20,
                             "train_n": 65, "train_pf": 1.14, "test_n": 7, "test_pf": 1.34,
                             "test_win_pct": 57.1, "day_block_p": 0.276},
            "exit_widening_effect": "widening the target from 0.60 to 1.20 lifted train PF 0.67 -> 1.14 (the unreachable-target bug is real)",
        },
        "why_rejected": ("the most salvageable of the three — fixing the unreachable target lifts it to "
                         "train 1.14 / test 1.34 — but train still misses the 1.5 bar and the test cell "
                         "is only 7 trades (p 0.276, not significant). Promising mechanism, insufficient edge."),
        "revalidation_trigger": ("re-run on >=2x more history with exit 1.00/1.20 + near-VWAP cell; promote "
                                 "ONLY if train PF>=1.5 AND test PF>=1.3 (n>=15) AND day-block p<0.10. This is "
                                 "the first to re-check when more data lands."),
    },

    # NOTE: G_HIGHER_HIGH_BREAK was initially parked here as a REJECT (ungated net loser),
    # but the aggressive G_iterate.py search (2026-06-12) found a robust pre-momentum/ADX
    # pocket (pre2_mom_r>=0.55 & sig5_adx_calc>=26, exit 0.90/2.50) that passes the honest
    # test (train 2.38 / test 2.66, p 0.005) and all anti-overfit checks. It has therefore
    # been PROMOTED to the active FINAL_SETUP_CONF above. Trade ONLY the gated version.

    # ----- L* family rejects (L_iterate.py / L_validate_passer.py / L_rejects_improve.py, raw pool) -----
    # L_DOUBLE_BOTTOM_VWAP (STRONG) and L_PRESSURE_BURST_VWAP (WEAK/CAUTION) were PROMOTED to the
    # active book above. These two have NO robust edge even after the robustness-first retry:
    "L_BB_SQUEEZE_LONG": {
        "enabled": False, "side": "LONG", "production_candidate": "REJECT",
        "detection": {"reason_tag": "bb_squeeze_upside_expansion",
                      "idea": "BB squeeze + close>upper band + vol>=2 + body>=0.65 (the only L* setup in the gated clean pool, n=1 test)"},
        "evaluated_on": "RAW_PRE_GATE_POOL",
        "evidence": {"ungated": {"train_pf": 0.69, "test_pf": 0.72, "net_rs": -71741, "raw_test_n": 55},
                     "robustness_first_best": {"terms": ["regime==NEUTRAL", "pre3_range_r<=0.887"],
                                               "exit": {"sl_pct": 0.90, "tgt_pct": 2.50}, "train_pf": 1.8,
                                               "half1": 1.85, "half2": 1.69, "test_pf": 1.73, "day_block_p": 0.121,
                                               "note": "all-period-positive BUT p 0.121 (not significant) and train<2"},
                     "momentum_adx_mechanism": "fails (train stays ~0.6)"},
        "why_rejected": ("robustness-first found a marginal NEUTRAL+low-range config (train 1.8, all periods ~1.7) but "
                         "p 0.121 misses significance; only 55 raw test trades total. The earlier train-PF>=2 'passer' "
                         "was a 0.02-wide market_ret band (curve-fit)."),
        "revalidation_trigger": "needs a materially larger test population; recheck regime==NEUTRAL & pre3_range_r<=0.887 @ 0.9/2.5 if so",
    },
    "L_TREND_PULLBACK": {
        "enabled": False, "side": "LONG", "production_candidate": "REJECT",
        "detection": {"reason_tag": "stacked_uptrend_ema20_pullback",
                      "idea": "EMA20>EMA50>EMA200 stacked uptrend + near-EMA20 pullback; ON PROBATION/BLOCKED in live discovery"},
        "evaluated_on": "RAW_PRE_GATE_POOL",
        "production_pre_momentum_terms": [["pre_entry_momentum_score", ">=", 73.021], ["pre2_mom_r", ">=", 0.233909]],
        "evidence": {"ungated": {"train_pf": 0.56, "test_pf": 0.41, "net_rs": -171659},
                     "premom_on": {"train_pf": 0.57, "test_pf": 0.36},
                     "robustness_first_best": {"terms": ["market_ret_pct>=-0.286", "pre2_mom_r>=0.217"],
                                               "exit": {"sl_pct": 0.50, "tgt_pct": 2.50}, "train_pf": 1.49,
                                               "half1": 1.53, "half2": 1.44, "test_pf": 1.71, "day_block_p": 0.232,
                                               "note": "all-period-positive but weak (train 1.49) and p 0.232"},
                     "momentum_adx_mechanism": "fails (best train 1.30)"},
        "why_rejected": ("robustness-first found only weak all-period-positive configs (best train 1.49, test 1.71, "
                         "p 0.232 — not significant, train<1.7); consistent with its live probation/block. No edge."),
        "revalidation_trigger": "do NOT promote; the stacked-uptrend pullback has no significant OOS edge in this period",
    },

    # ----- S* family rejects (S_iterate.py, 2026-06-12) -----
    # Only 2 S* setups have data; both REJECT. The others emit nothing (ENABLE_NOISY_ADVANCED_SHORTS off).
    "S_BB_SQUEEZE_SHORT": {
        "enabled": False, "side": "SHORT", "production_candidate": "REJECT",
        "detection": {"reason_tag": "bb_squeeze_downside_expansion",
                      "idea": "BB squeeze + close<lower_band*0.997 + vol>=2 + body>=0.65 (short BB-squeeze break)"},
        "evaluated_on": "GATED_CLEAN_POOL",
        "production_exit": {"sl_pct": 1.00, "tgt_pct": 1.50},
        "evidence": {"baseline": {"train_n": 196, "train_pf": 0.94, "test_n": 16, "test_pf": 0.98, "net_rs": -6147},
                     "trainPFmax_overfit": {"terms": ["body_pct<=0.94", "vol_ratio>=2.32"], "train_pf": 2.08, "test_pf": 0.58, "note": "test collapses"},
                     "robustness_first_best": {"terms": ["vol_ratio>=2.32", "pre1_adx<=38.4", "vwap_dist_atr<=-1.81", "pre3_close_pos>=0.93"],
                                               "exit": {"sl_pct": 1.10, "tgt_pct": 1.50}, "train_pf": 1.91, "half1": 1.88, "half2": 1.94,
                                               "test_pf": 2.66, "test_n": 8, "day_block_p": 0.146}},
        "why_rejected": ("near-breakeven baseline; train-PF-max configs overfit (test 0.46-0.58); robustness-first "
                         "configs are all-period-positive but 3-4 terms, insignificant (p 0.12-0.33) and capped at "
                         "16 clean-pool test trades (cut to n=8). Sample-limited, no significant edge."),
        "revalidation_trigger": "needs more clean-pool test trades; recheck the extended-down (vwap_dist_atr<=-1.8) + high-vol configs if so",
    },
    "S_MACD_HIST_FLIP": {
        "enabled": False, "side": "SHORT", "production_candidate": "REJECT (single-day artifact)",
        "detection": {"reason_tag": "macd_hist_negative_flip",
                      "idea": "MACD-hist flips negative + below VWAP + RSI<=55 (gated behind ENABLE_NOISY_ADVANCED_SHORTS)"},
        "evaluated_on": "RAW_PRE_GATE_POOL",
        "production_exit": {"sl_pct": 0.70, "tgt_pct": 1.50},
        "evidence": {"baseline": {"train_n": 1500, "train_pf": 0.83, "test_n": 297, "test_pf": 1.14, "net_rs": -73456},
                     "gaudy_but_day_concentrated": {"terms": ["vwap_dist_atr<=-4.498", "pre2_mom_r<=-0.029"],
                                                     "exit": {"sl_pct": 0.90, "tgt_pct": 1.00}, "train_pf": 2.07,
                                                     "test_pf": 5.51, "test_n": 14, "test_days": 5, "day_block_p": 0.175,
                                                     "day_concentration": "ONE day (2026-05-12) = Rs7,233 of Rs7,043 total test PnL (top-2-day share 112%)"}},
        "why_rejected": ("search threw up gaudy PFs (train 2-5, test 5-10) but NONE reach p<0.10 (best 0.12-0.18). "
                         "The day-concentration check is decisive: the entire OOS profit of the cleanest config is a "
                         "SINGLE crash day (2026-05-12). The pattern (vwap_dist_atr<=-4.5, regime==BEAR, deeply "
                         "extended-down) is a falling-knife/bear-day short — a tail-event lottery, not a daily edge."),
        "revalidation_trigger": "do NOT promote; it is a bear-crash-day artifact, not a durable edge. Raw-pool gated.",
    },

    # ----- T*/MR* family rejects (T_iterate.py, tier123 overlay probe, 2026-06-12) -----
    # T_TREND_DAY_EMA_STAIR_SHORT was the keeper -> PROMOTED to the active book above.
    # These three have no robust edge:
    "T_TREND_DAY_EMA_STAIR_LONG": {
        "enabled": False, "side": "LONG", "production_candidate": "REJECT",
        "detection": {"reason_tag": "trend_day_ema20_stair_long", "tier": "tier123 Tier-2",
                      "idea": "uptrend EMA20-stair continuation long (mirror of the SHORT keeper)"},
        "evaluated_on": "TIER123_OVERLAY_PROBE",
        "evidence": {"ungated": {"train_pf": 0.63, "test_pf": 0.43, "net_rs": -179402, "prior_tier123_holdout_pf": 0.50},
                     "best_4term": {"terms": ["quality_score>=85.2", "sig5_rsi_dir<=59.2", "atr_pct>=0.0023", "sig5_adx_calc<=36.1"],
                                    "exit": {"sl_pct": 1.10, "tgt_pct": 2.00}, "train_pf": 2.66, "test_pf": 2.74, "test_n": 13, "day_block_p": 0.100},
                     "term_dropout": {"drop_quality_score": "train 2.66->0.73", "drop_rsi": "->0.92", "drop_atr_pct": "->0.90", "drop_adx": "->1.98 (p0.205)"}},
        "why_rejected": "only passer is a fragile 4-term config (p right at 0.100); dropping quality_score/rsi/atr each collapses it; no robust <=2-term core (unlike the SHORT side)",
        "revalidation_trigger": "promote only if a <=2-term core shows train>=2 & test>=1.3 & p<0.10 with good day-spread on more data",
    },
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": {
        "enabled": False, "side": "LONG", "production_candidate": "REJECT",
        "detection": {"reason_tag": "controlled_vwap_extreme_fade_long", "tier": "tier123 Tier-3",
                      "idea": "controlled fade of a VWAP extreme (long)"},
        "evaluated_on": "TIER123_OVERLAY_PROBE",
        "evidence": {"ungated": {"train_pf": 0.94, "test_pf": 0.27, "net_rs": -7738, "test_n": 11, "prior_tier123_holdout_pf": 0.88},
                     "trainPFmax_overfit": {"terms": ["rs_pct<=-1.27", "market_ret_pct>=-0.51"], "train_pf": 2.15, "test_pf": 0.97},
                     "robustness_first": {"best_minpf": 1.18, "top1day_share_pct": 548, "note": "day-concentrated, p 0.43"}},
        "why_rejected": "only 11 test trades; train-PF-max configs collapse in test (0.27-0.97); robustness-first 'bests' are single-day artifacts (top1day 548-1396%). No edge.",
        "revalidation_trigger": "needs a materially larger test population; do not promote on 11 test trades",
    },
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": {
        "enabled": False, "side": "SHORT", "production_candidate": "REJECT",
        "detection": {"reason_tag": "controlled_vwap_extreme_fade_short", "tier": "tier123 Tier-3",
                      "idea": "controlled fade of a VWAP extreme (short)"},
        "evaluated_on": "TIER123_OVERLAY_PROBE",
        "evidence": {"ungated": {"train_pf": 0.61, "test_pf": 0.58, "net_rs": -28295, "test_n": 12, "prior_tier123_holdout_pf": 0.67},
                     "robustness_first": {"best": {"train_pf": 1.61, "test_pf": 1.42, "day_block_p": 0.266, "top1day_share_pct": 105, "n_terms": 4},
                                          "note": "all-period-positive but insignificant, day-concentrated, 4-term"}},
        "why_rejected": "0 train-PF>=2 configs; robustness-first best (4-term train 1.61/test 1.42) p 0.266 not significant + day-concentrated (top1d 105%); only 12 test trades. No edge.",
        "revalidation_trigger": "do NOT promote; insufficient test sample and no significant edge",
    },
}

