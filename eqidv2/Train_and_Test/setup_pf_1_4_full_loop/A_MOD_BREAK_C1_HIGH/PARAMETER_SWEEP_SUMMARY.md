# A_MOD_BREAK_C1_HIGH — Stage-3 Parameter Sweep Summary

_Generated 2026-07-02. 119 single-knob configs, FIT (31 sessions) + VAL (21 sessions), 15 bps._
_Raw base (production exit 0.70/1.00): FIT PF 0.244 / VAL PF 0.198._
_Full table: `stage3_sweep_results.csv`._

## Exit surface (42 SL×Tgt combos on the raw book)

| finding | value |
|---|---|
| best big-n FIT | SL 1.5/Tgt 1.5 → FIT 0.290 / VAL 0.252 |
| runner-ups | 1.0/1.25, 1.5/1.25, 1.2/1.25, 1.2/1.5 (all FIT 0.28-0.29) |
| direction | wider SL ≥ 1.0 uniformly beats the production 0.70 SL; target 1.25-1.5 best |
| production exit 0.70/1.00 | FIT 0.244 — **near the bottom of the surface**; 69% SL-rate |

## Indicator terms

| knob | tested | best (FIT/VAL) | verdict |
|---|---|---|---|
| rs_pct ≥ | 1,2,3,4,5 | ≥4: 0.225/0.220 | flat, no lift — current gate's rs≥2 does nothing |
| atr_pct ≤ | .004,.005,.006,.008 | ≤.004: 0.266/**0.177** | FIT lift, VAL collapse — overfit direction; current gate's ≤.006 unhelpful |
| vol_ratio ≥ | 1.8,2.2,2.6,3.0 | ≥3.0: 0.286/0.204 | mild FIT lift, VAL flat |
| quality_score ≥ | q25,q50,q75 | q25: 0.248/0.196 | nothing |
| rsi / adx / ranker_score / macd_hist_delta / ema20_slope | q25/50/75 | **n=0 — columns are NaN in the raw pool; masks on them empty the book** | unusable knobs |

## Non-indicator / candle terms

| knob | best (FIT/VAL) | verdict |
|---|---|---|
| body_pct ≥ q75 | 0.255/0.154 | VAL collapse |
| close_loc ≥ q25 | 0.249/0.189 | nothing |
| upper_wick ≤ q75 | 0.257/0.184 | nothing |
| signal_range ≥ q25 | 0.255/0.186 | nothing |
| wick_skew ≤ q75 | 0.248/0.182 | nothing |
| vwap_dist_atr ≤ 2.8 | 0.260/0.172 | anti-chase FIT lift, VAL collapse |
| regime == BULL | 0.208/0.148 | **worse** than !=BEAR |
| market_ret ≥ 0 | 0.206/0.156 | worse |

## Time / crowding / risk guards

| knob | best (FIT/VAL) | verdict |
|---|---|---|
| **max_slot 11:05** | **0.346 / 0.324** | the ONLY knob that lifts FIT and holds VAL; n=359/275 (small) |
| max_slot 12:00 | 0.334/0.222 | FIT lift, VAL fades |
| min_slot any | ~base | dead — detector fires mostly ≥11:00 anyway |
| top_n 1/2/3 | top1: 0.284/0.230 | mild; top2 FIT-better but VAL-worse |
| daily_loss 4000 | 0.236/0.211 | slight VAL smoothing |
| max_positions 10 | 0.233/0.214 | slight VAL smoothing |

## Pre-momentum terms

`tt._premom` returned empty feature dicts on a 1,000-row FIT sample → **all 8 pre-momentum knobs unavailable for this pool**. The Optuna stage runs with masks/exits/guards only.

## Stable-range conclusions feeding Stage 4

1. Exits: SL 1.0-1.5 × Tgt 1.25-1.5 region only.
2. Morning-only (≤11:05) is the single defensible structural filter.
3. vol_ratio ≥ 2.6-3.0 the only indicator with a non-negative joint signature.
4. Rejected as overfit-direction: atr_pct ≤ .004, body q75, vwap_dist ≤ 2.8 (FIT-up/VAL-down).
5. Even stacking every "best" knob multiplicatively projects PF ≈ 0.35-0.5 — the 1.30 floor requires interaction effects the single sweeps cannot see; treat any Optuna config reaching it with n<40 as suspect by construction.
