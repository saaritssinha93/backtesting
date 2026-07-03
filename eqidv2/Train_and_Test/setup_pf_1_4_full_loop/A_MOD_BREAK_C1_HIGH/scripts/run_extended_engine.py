r"""run_extended_engine.py — pf_band_fitval_loop with the ENRICHED mask-feature space.

Monkeypatches the engine's MASK_FEATS (adds the ~35 recomputed indicator / pre-momentum /
structural columns) and MAX_SLOTS (adds the 11:05 production boundary), then delegates to
the engine's own main(). Everything else — objective, robustness, gates, artifacts —
is the stock approval-loop engine.

Usage: py -3.12 run_extended_engine.py <engine args...>
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve()
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import pf_band_fitval_loop as eng  # noqa: E402

EXTRA_MASK_FEATS = [
    "rsi_x", "rsi_slope3", "adx_x", "cci_x", "mfi_x", "stoch_k", "stoch_cross",
    "macd_hist_x", "macd_hist_delta3", "macd_above_sig",
    "ema20_dist_atr", "ema50_dist_atr", "ema20_slope5_atr", "ema_stack",
    "bb_pos", "bb_width_pct", "obv_slope5_norm",
    "pre1_ret_atr", "pre3_ret_atr", "pre5_ret_atr", "green_streak_pre",
    "pre3_vol_ratio", "range_compress3", "pre_rsi", "vwap_hold_bars",
    "break_margin_atr", "is_20bar_high", "dist_20bar_high_atr",
    "or_high_dist_atr", "above_or_high", "pdh_dist_atr", "above_pdh",
    "gap_pct", "day_ret_pct", "day_range_pos", "upmove_from_daylow_atr",
    "bar_of_day", "price_level", "notional_5m_rs", "dow",
]

eng.MASK_FEATS = list(dict.fromkeys(list(eng.MASK_FEATS) + EXTRA_MASK_FEATS))
if "11:05" not in eng.MAX_SLOTS:
    eng.MAX_SLOTS = ["11:05"] + list(eng.MAX_SLOTS)
print(f"[ext-engine] MASK_FEATS={len(eng.MASK_FEATS)} MAX_SLOTS={eng.MAX_SLOTS}")

if __name__ == "__main__":
    raise SystemExit(eng.main())
