"""
all_setups_catalog.py — single collated catalog of every LONG / SHORT trading
setup defined across the eqidv2 main folder.
============================================================================

This file does NOT define new trading logic. It is a read-only INDEX that
collates, in one place, every setup that is *detectable* or *configured*
anywhere in the project, with its side (LONG/SHORT), the engine that defines
its raw detection, the internal reason-tag it emits, and a one-line idea.

Sources collated (all in the main eqidv2 folder)
------------------------------------------------
  A) avwap_5min_ID_v2_backtesting.py   -> candidate_scan() catalog (49 setups)
        The core 5-minute raw-detection engine. `add_catalog(name, side, cond,
        reason)` / `_make_candidate(...)` calls L587-L1007.
  B) avwap_5min_ID_v7_candidate_scan.py -> EARLY-engine catalog (14 E_* setups)
        First-hour / opening-range "early" scan. `add(name, side, cond, reason,
        min_qs)` calls L479-L576.
  C) new_setups_scan_v11.py             -> 4 overlay/new setups
        L_RS_LEADER_VWAP_HOLD, S_UPTHRUST_TRAP_FADE, N_HIGH_RS_EMA_BOUNCE_LONG,
        N_MORNING_ZERO_WICK_SHORT.
  D) tier123 probe (research_v11_tier123_new_setups / tier123_new_validate.py)
        V_RECLAIM_PULLBACK_LONG, P_PDH_BREAK_RETEST_LONG, T_TREND_DAY_EMA_STAIR_*,
        MR_CONTROLLED_VWAP_EXTREME_FADE_*  (E_ORB_RETEST_HOLD_* counted under B).
  E) discovery-engine candidates (Train_and_Test/long_setup_discovery_from_raw_data)
        Standalone-mined near-misses kept for the record (NOT promoted).

Live status (ACTIVE / RESEARCH_WATCH / RAW_ONLY) is NOT hard-coded here — it is
derived at runtime from the gate-of-record, final_setup_conf.py:
    ACTIVE          = name in FINAL_SETUP_CONF       (traded by the live conf book)
    RESEARCH_WATCH  = name in RESEARCH_WATCH_CONF     (enabled=False, never traded)
    RAW_ONLY        = detectable but never promoted to a tuned conf
    DISCOVERY       = mined candidate (source E), never wired into a scanner
so this catalog stays correct as the conf book changes. Run it directly to print
a summary:  py -3.12 all_setups_catalog.py
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

LONG = "LONG"
SHORT = "SHORT"

# --- source labels --------------------------------------------------------
SRC_V2 = "avwap_5min_ID_v2_backtesting.py::candidate_scan"
SRC_V7_EARLY = "avwap_5min_ID_v7_candidate_scan.py::EARLY"
SRC_NEW = "new_setups_scan_v11.py"
SRC_TIER123 = "research_v11_tier123_new_setups / tier123_new_validate.py"
SRC_DISCOVERY = "Train_and_Test/long_setup_discovery_from_raw_data"


@dataclass(frozen=True)
class Setup:
    name: str
    side: str          # LONG or SHORT
    source: str        # engine/scanner that defines the raw detection
    reason_tag: str    # internal reason string emitted by that scanner
    idea: str          # one-line plain-English description
    notes: str = ""


# ===========================================================================
# A) v2 5-minute raw-detection engine (avwap_5min_ID_v2_backtesting.py)
# ===========================================================================
V2_SETUPS: list[Setup] = [
    Setup("A_MOMENTUM_BREAKOUT", LONG, SRC_V2, "breakout_above_or_or_20bar_high",
          "Momentum break above opening-range / 20-bar high."),
    Setup("A_MOMENTUM_BREAKDOWN", SHORT, SRC_V2, "breakdown_below_or_or_20bar_low",
          "Momentum break below opening-range / 20-bar low."),
    Setup("C_OVEREXTENDED_BREAKOUT_FADE", SHORT, SRC_V2, "fade_overextended_breakout",
          "Fade a long breakout that is over-extended above VWAP."),
    Setup("B_FAILED_BREAKDOWN_REVERSAL", LONG, SRC_V2, "failed_or_low_break_reclaim",
          "Prior bar broke OR-low, current bar reclaims it (bear trap)."),
    Setup("B_FAILED_BREAKOUT_REVERSAL", SHORT, SRC_V2, "failed_or_high_break_reject",
          "Prior bar broke OR-high, current bar rejects it (bull trap)."),
    Setup("A_MOD_BREAK_C1_HIGH", LONG, SRC_V2, "moderate_impulse_break_prior_high",
          "Moderate-impulse bar breaks the prior bar's high above VWAP."),
    Setup("A_MOD_BREAK_C1_LOW", SHORT, SRC_V2, "moderate_impulse_break_prior_low",
          "Moderate-impulse bar breaks the prior bar's low below VWAP."),
    Setup("A_MOD_CLOSE_CONTINUATION_BREAK", LONG, SRC_V2, "moderate_close_near_high_continuation",
          "Close-near-high continuation breaking the prior bar high."),
    Setup("A_PULLBACK_C2_THEN_BREAK_C2_LOW", SHORT, SRC_V2, "bear_pullback_c2_break_low",
          "After a 2-bar up-pullback, lose VWAP and break prior low."),
    Setup("A_PULLBACK_C2_THEN_BREAK_C2_HIGH", LONG, SRC_V2, "bull_pullback_c2_break_high",
          "After a 2-bar down-pullback, hold VWAP and break prior high."),
    Setup("B_AVWAP_RECLAIM_REVERSAL", LONG, SRC_V2, "reclaim_session_vwap_from_below",
          "Below-VWAP stock reclaims session VWAP on a strong up-bar."),
    Setup("D_AVWAP_LOSE_REVERSAL", SHORT, SRC_V2, "lose_session_vwap_from_above",
          "Above-VWAP stock loses session VWAP on a strong down-bar."),
    Setup("B_HUGE_C1_CLOSE_RECLAIM_BREAK", LONG, SRC_V2, "huge_green_reclaim_then_break",
          "Break of a prior HUGE green bar's high (continuation)."),
    Setup("B_HUGE_RED_FAILED_BOUNCE", SHORT, SRC_V2, "huge_red_failed_bounce_breakdown",
          "Failed bounce after a huge red bar -> break prior low."),
    Setup("B_HUGE_PULLBACK_HOLD_BREAK", LONG, SRC_V2, "huge_impulse_pullback_hold_break",
          "Huge green impulse, shallow pullback holds, then breaks high."),
    Setup("B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK", LONG, SRC_V2, "huge_green_pullback_hold_then_break",
          "Huge green bar, pullback holds the body, close-near-high break."),
    Setup("B_HUGE_FAILED_BOUNCE", SHORT, SRC_V2, "huge_candle_failed_bounce",
          "Huge bar, failed bounce, breaks prior low below VWAP."),
    Setup("C_OR_BREAKOUT", LONG, SRC_V2, "opening_range_breakout",
          "Break of the opening-range high above VWAP."),
    Setup("C_OR_BREAKDOWN", SHORT, SRC_V2, "opening_range_breakdown",
          "Break of the opening-range low below VWAP."),
    Setup("C_SHORT_CONTINUATION_BREAK", SHORT, SRC_V2, "short_continuation_pivot_low_break",
          "Down continuation breaking a 20-bar pivot low after lower closes."),
    Setup("D_EMA20_BOUNCE", LONG, SRC_V2, "ema20_trend_bounce",
          "Uptrend stack retests rising EMA20 and bounces."),
    Setup("D_EMA20_REJECTION", SHORT, SRC_V2, "ema20_trend_rejection",
          "Downtrend stack retests falling EMA20 and rejects."),
    Setup("E_VWAP_BAND_FADE", SHORT, SRC_V2, "upper_band_vwap_fade",
          "Fade an over-extension that tags the upper BB band below VWAP."),
    Setup("G_HIGHER_HIGH_BREAK", LONG, SRC_V2, "twenty_bar_higher_high_break",
          "Break of the prior 20-bar high on strong volume."),
    Setup("G_LOWER_LOW_BREAK", SHORT, SRC_V2, "twenty_bar_lower_low_break",
          "Break of the prior 20-bar low on strong volume."),
    Setup("L_BB_SQUEEZE_LONG", LONG, SRC_V2, "bb_squeeze_upside_expansion",
          "Bollinger squeeze + close above upper band (upside expansion)."),
    Setup("S_BB_SQUEEZE_SHORT", SHORT, SRC_V2, "bb_squeeze_downside_expansion",
          "Bollinger squeeze + close below lower band (downside expansion)."),
    Setup("H_FAILED_BREAKOUT_TRAP", SHORT, SRC_V2, "failed_breakout_trap",
          "Prior bar pierced OR-high, current bar closes back below (trap)."),
    Setup("L_MACD_BULL_VWAP", LONG, SRC_V2, "macd_bull_cross_vwap_trend",
          "MACD bull cross in an above-VWAP uptrend stack."),
    Setup("S_MACD_BEAR_VWAP", SHORT, SRC_V2, "macd_bear_cross_vwap_trend",
          "MACD bear cross in a below-VWAP downtrend stack."),
    Setup("L_TREND_PULLBACK", LONG, SRC_V2, "stacked_uptrend_ema20_pullback",
          "EMA20>EMA50>EMA200 stacked uptrend, near-EMA20 pullback."),
    Setup("S_TREND_REJECT", SHORT, SRC_V2, "stacked_downtrend_ema20_reject",
          "EMA20<EMA50<EMA200 stacked downtrend, near-EMA20 rejection."),
    Setup("L_MFI_OVERSOLD_RECLAIM", LONG, SRC_V2, "mfi_oversold_reclaim",
          "MFI crosses up out of oversold with a lower-wick reclaim."),
    Setup("S_MFI_OVERBOUGHT_FAIL", SHORT, SRC_V2, "mfi_overbought_fail",
          "MFI rolls down out of overbought with an upper-wick fail.",
          notes="gated behind ENABLE_NOISY_ADVANCED_SHORTS"),
    Setup("L_CCI_EXTREME_FLIP", LONG, SRC_V2, "cci_negative_extreme_flip",
          "CCI flips up from a negative extreme above EMA20."),
    Setup("S_CCI_EXTREME_FLIP", SHORT, SRC_V2, "cci_positive_extreme_flip",
          "CCI flips down from a positive extreme below EMA20."),
    Setup("L_DOUBLE_BOTTOM_VWAP", LONG, SRC_V2, "double_bottom_vwap_reclaim",
          "Double-bottom near the 8-bar low while holding above VWAP."),
    Setup("S_DOUBLE_TOP_VWAP", SHORT, SRC_V2, "double_top_vwap_fail",
          "Double-top near the 8-bar high failing below VWAP."),
    Setup("L_PRESSURE_BURST_VWAP", LONG, SRC_V2, "buy_pressure_burst_vwap",
          "Buy-pressure burst (pressure_ratio>=3) above VWAP and EMA20."),
    Setup("S_PRESSURE_DUMP_VWAP", SHORT, SRC_V2, "sell_pressure_dump_vwap",
          "Sell-pressure dump (pressure_ratio<=0.33) below VWAP and EMA20.",
          notes="gated behind ENABLE_NOISY_ADVANCED_SHORTS"),
    Setup("L_PREV_DAY_LOW_SWEEP", LONG, SRC_V2, "previous_day_level_sweep_reclaim",
          "Sweep below prior-day close then reclaim with a lower wick."),
    Setup("S_PREV_DAY_HIGH_FAIL", SHORT, SRC_V2, "previous_day_high_sweep_fail",
          "Sweep above prior-day high then fail back below (morning only)."),
    Setup("L_GAP_DOWN_REVERSAL", LONG, SRC_V2, "gap_down_reversal_above_prev_close",
          "Gap-down that reverses back above the prior-day close."),
    Setup("S_GAP_UP_REJECTION", SHORT, SRC_V2, "gap_up_rejection_below_prev_close",
          "Gap-up that gets rejected back below the prior-day close.",
          notes="gated behind ENABLE_NOISY_ADVANCED_SHORTS"),
    Setup("S_MOMENTUM_BREAKDOWN_OR", SHORT, SRC_V2, "advanced_or_breakdown_rs_volume_avwap",
          "Advanced OR-low breakdown with RS<0 and volume surge."),
    Setup("S_LIQUIDITY_SWEEP_REVERSAL", SHORT, SRC_V2, "advanced_prev_day_high_liquidity_sweep",
          "Prior-day-high liquidity sweep then reversal short.",
          notes="gated behind ENABLE_NOISY_ADVANCED_SHORTS"),
    Setup("S_MACD_HIST_FLIP", SHORT, SRC_V2, "macd_hist_negative_flip",
          "MACD histogram flips negative below VWAP with RSI<=55.",
          notes="gated behind ENABLE_NOISY_ADVANCED_SHORTS"),
    Setup("L_OBV_DIVERGENCE_VWAP", LONG, SRC_V2, "bullish_obv_divergence_vwap",
          "Price makes a lower low but OBV rises (bullish divergence)."),
    Setup("S_OBV_DIVERGENCE_VWAP", SHORT, SRC_V2, "bearish_obv_divergence_vwap",
          "Price makes a higher high but OBV falls (bearish divergence)."),
]

# ===========================================================================
# B) v7 EARLY engine (avwap_5min_ID_v7_candidate_scan.py) — first-hour scans
# ===========================================================================
V7_EARLY_SETUPS: list[Setup] = [
    Setup("E_ORB_BREAKOUT_LONG", LONG, SRC_V7_EARLY, "early_opening_range_breakout_long",
          "Early opening-range high breakout with RS.",
          notes="also a native selected-strategy setup in new_setups_scan_v11.py"),
    Setup("E_ORB_BREAKOUT_SHORT", SHORT, SRC_V7_EARLY, "early_opening_range_breakout_short",
          "Early opening-range low breakdown with relative weakness."),
    Setup("E_ORB_RETEST_HOLD_LONG", LONG, SRC_V7_EARLY, "early_orb_retest_hold_long",
          "Opening-range high break, retest-and-hold, continue up.",
          notes="also tuned in the tier123 probe"),
    Setup("E_ORB_RETEST_HOLD_SHORT", SHORT, SRC_V7_EARLY, "early_orb_retest_hold_short",
          "Opening-range low break, retest-and-hold, continue down.",
          notes="also tuned in the tier123 probe"),
    Setup("E_VWAP_RECLAIM_EARLY_LONG", LONG, SRC_V7_EARLY, "early_vwap_reclaim_break_prev_high",
          "Early VWAP reclaim from below + break prior bar high."),
    Setup("E_VWAP_LOSE_EARLY_SHORT", SHORT, SRC_V7_EARLY, "early_vwap_lose_break_prev_low",
          "Early VWAP loss + break prior bar low, lagging the market."),
    Setup("E_GAP_HOLD_CONTINUATION_LONG", LONG, SRC_V7_EARLY, "early_gap_up_hold_continuation",
          "Gap-up holds the open then breaks OR-high (continuation)."),
    Setup("E_GAP_HOLD_CONTINUATION_SHORT", SHORT, SRC_V7_EARLY, "early_gap_down_hold_continuation",
          "Gap-down holds the open then breaks OR-low (continuation)."),
    Setup("E_RS_FIRST_HOUR_BREAK_LONG", LONG, SRC_V7_EARLY, "early_relative_strength_first_hour_break",
          "First-hour relative-strength leader breaking range/prior high."),
    Setup("E_RS_FIRST_HOUR_BREAK_SHORT", SHORT, SRC_V7_EARLY, "early_relative_weakness_first_hour_break",
          "First-hour relative-weakness laggard breaking range/prior low."),
    Setup("E_OPENING_DRIVE_CONTINUATION_LONG", LONG, SRC_V7_EARLY, "early_opening_drive_continuation_long",
          "Strong first-15-min up-drive continuing through prior/OR high."),
    Setup("E_OPENING_DRIVE_CONTINUATION_SHORT", SHORT, SRC_V7_EARLY, "early_opening_drive_continuation_short",
          "Strong first-15-min down-drive continuing through prior/OR low."),
    Setup("E_FAILED_OR_BREAKOUT_TRAP_SHORT", SHORT, SRC_V7_EARLY, "early_failed_or_breakout_trap_short",
          "OR-high breakout that traps with an upper wick (fade short)."),
    Setup("E_FAILED_OR_BREAKDOWN_TRAP_LONG", LONG, SRC_V7_EARLY, "early_failed_or_breakdown_trap_long",
          "OR-low breakdown that traps with a lower wick (fade long)."),
]

# ===========================================================================
# C) new_setups_scan_v11.py — overlay / mined setups
# ===========================================================================
NEW_SETUPS: list[Setup] = [
    Setup("L_RS_LEADER_VWAP_HOLD", LONG, SRC_NEW, "rs_leader_vwap_test_and_hold",
          "RS-leader tests session VWAP and holds (continuation)."),
    Setup("S_UPTHRUST_TRAP_FADE", SHORT, SRC_NEW, "failed_high_upthrust_trap_distribution",
          "Broke prior 10-bar high, closed back below on volume + upper wick."),
    Setup("N_HIGH_RS_EMA_BOUNCE_LONG", LONG, SRC_NEW, "high_rs_overlay_on_ema20_bounce",
          "High-RS overlay on D_EMA20_BOUNCE.", notes="RESEARCH_ONLY overlay"),
    Setup("N_MORNING_ZERO_WICK_SHORT", SHORT, SRC_NEW, "morning_zero_lower_wick_overlay",
          "Morning zero-lower-wick overlay on four short families.",
          notes="RESEARCH_ONLY overlay"),
]

# ===========================================================================
# D) tier123 overlay probe (research_v11_tier123_new_setups / tier123_new_validate)
#    (E_ORB_RETEST_HOLD_* are catalogued under the v7 EARLY engine above.)
# ===========================================================================
TIER123_SETUPS: list[Setup] = [
    Setup("V_RECLAIM_PULLBACK_LONG", LONG, SRC_TIER123, "reclaim_pullback_strong_adx",
          "Reclaim-pullback in a strong-ADX RS leader."),
    Setup("P_PDH_BREAK_RETEST_LONG", LONG, SRC_TIER123, "prev_day_high_break_retest",
          "Prior-day-high break-and-retest long."),
    Setup("T_TREND_DAY_EMA_STAIR_LONG", LONG, SRC_TIER123, "trend_day_ema20_stair_long",
          "Uptrend-day EMA20-stair continuation long."),
    Setup("T_TREND_DAY_EMA_STAIR_SHORT", SHORT, SRC_TIER123, "trend_day_ema20_stair_short",
          "Downtrend-day EMA20-stair continuation short."),
    Setup("MR_CONTROLLED_VWAP_EXTREME_FADE_LONG", LONG, SRC_TIER123, "controlled_vwap_extreme_fade_long",
          "Controlled mean-reversion fade of a downside VWAP extreme."),
    Setup("MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT", SHORT, SRC_TIER123, "controlled_vwap_extreme_fade_short",
          "Controlled mean-reversion fade of an upside VWAP extreme."),
]

# ===========================================================================
# E) discovery-engine mined candidates (Train_and_Test/long_setup_discovery...)
#    Kept for the record; never wired into a live scanner. status = DISCOVERY.
# ===========================================================================
DISCOVERY_SETUPS: list[Setup] = [
    Setup("NEW_LONG_FAST_PULLBACK_075_100", LONG, SRC_DISCOVERY, "F5_PULLBACK_CONT",
          "Fast pullback continuation long (b_075_100 bracket).",
          notes="REJECT — net-negative after realistic costs (best near-miss only)"),
    Setup("NEW_SHORT_S9_MIDDAY_LOSE", SHORT, SRC_DISCOVERY, "S9_MIDDAY_LOSE",
          "Midday VWAP-lose short (x_125_250 bracket).",
          notes="near-miss; train PF 1.26 / test PF 1.34 @5bps, fails at 15bps"),
]

# Full collated catalog (deduplicated by name; each setup listed once).
ALL_SETUPS: list[Setup] = (
    V2_SETUPS + V7_EARLY_SETUPS + NEW_SETUPS + TIER123_SETUPS + DISCOVERY_SETUPS
)


# ---------------------------------------------------------------------------
# Live-status enrichment, derived from the gate-of-record (final_setup_conf.py).
# ---------------------------------------------------------------------------
ACTIVE = "ACTIVE"                  # in FINAL_SETUP_CONF -> traded by the live conf book
RESEARCH_WATCH = "RESEARCH_WATCH"  # in RESEARCH_WATCH_CONF -> enabled=False, never traded
RAW_ONLY = "RAW_ONLY"              # detectable but never promoted to a tuned conf
DISCOVERY = "DISCOVERY"            # mined candidate, never wired into a scanner


def _load_conf() -> tuple[dict, dict]:
    """Best-effort import of the gate-of-record. Returns ({}, {}) if unavailable."""
    try:
        import final_setup_conf as fsc  # only depends on `os`; safe to import
        return dict(fsc.FINAL_SETUP_CONF), dict(fsc.RESEARCH_WATCH_CONF)
    except Exception:
        return {}, {}


def status_of(name: str) -> str:
    """Live status of a setup name per final_setup_conf.py (falls back to static)."""
    active, watch = _load_conf()
    if name in active:
        return ACTIVE
    if name in watch:
        return RESEARCH_WATCH
    if any(s.name == name and s.source == SRC_DISCOVERY for s in ALL_SETUPS):
        return DISCOVERY
    return RAW_ONLY


# ---------------------------------------------------------------------------
# Convenience accessors.
# ---------------------------------------------------------------------------
def longs() -> list[Setup]:
    return [s for s in ALL_SETUPS if s.side == LONG]


def shorts() -> list[Setup]:
    return [s for s in ALL_SETUPS if s.side == SHORT]


def by_source(source: str) -> list[Setup]:
    return [s for s in ALL_SETUPS if s.source == source]


def by_status(status: str) -> list[Setup]:
    return [s for s in ALL_SETUPS if status_of(s.name) == status]


def active_book() -> list[Setup]:
    return by_status(ACTIVE)


def research_watch() -> list[Setup]:
    return by_status(RESEARCH_WATCH)


def find(name: str) -> Optional[Setup]:
    for s in ALL_SETUPS:
        if s.name == name:
            return s
    return None


def _print_summary() -> None:
    active, watch = _load_conf()
    conf_loaded = bool(active or watch)

    n_long = len(longs())
    n_short = len(shorts())
    print(f"Collated setups: {len(ALL_SETUPS)}  ({n_long} LONG / {n_short} SHORT)")
    print("Sources:")
    for src in (SRC_V2, SRC_V7_EARLY, SRC_NEW, SRC_TIER123, SRC_DISCOVERY):
        items = by_source(src)
        print(f"  - {len(items):>2}  {src}")
    print()

    if conf_loaded:
        print(f"Live status (from final_setup_conf.py: {len(active)} active, "
              f"{len(watch)} research-watch):")
    else:
        print("Live status (final_setup_conf.py NOT importable — showing RAW/DISCOVERY only):")

    for status in (ACTIVE, RESEARCH_WATCH, RAW_ONLY, DISCOVERY):
        group = by_status(status)
        if not group:
            continue
        gl = sum(1 for s in group if s.side == LONG)
        gs = sum(1 for s in group if s.side == SHORT)
        print(f"\n  [{status}]  {len(group)}  ({gl}L / {gs}S)")
        for s in sorted(group, key=lambda x: (x.side, x.name)):
            tag = "L" if s.side == LONG else "S"
            print(f"      {tag}  {s.name:<38} {s.idea}")


if __name__ == "__main__":
    _print_summary()
