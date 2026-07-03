# A_MOD_BREAK_C1_HIGH — Iteration Log

_Generated 2026-07-03. ~1,340 configs evaluated across 6 stages. Per-config detail lives in the
referenced CSVs; this log records every decision-level iteration with command, result, verdict._

**Windows everywhere:** TRAIN 2026-03-04..05-29 (52 sessions; FIT/VAL split inside), TEST 2026-06-01..07-01 (22 sessions), 15 bps, repo cost model. TEST evaluated only for full-TRAIN band members.

## Stage 1 — Pool recreation (3 iterations)

| # | action | result |
|---|---|---|
| 1.1 | master extract (Mar-Jun24) | 23,340 rows / 66 sessions — found mid-June missing |
| 1.2 | + fresh tail 06-25..07-01 (`historical_all_available`, 4 workers) | 24,634 rows / 70 sessions — classified 10 more missing weekdays |
| 1.3 | + gap-fill 06-17..06-23 (05-28, 06-26 unrecoverable — raw-store holes) | **26,277 rows / 74 sessions** final |

## Stage 2 — Baseline + failure study (2 iterations)

| # | config | TRAIN | TEST | verdict |
|---|---|---|---|---|
| 2.1 | raw detector, exit 0.70/1.00 | n=3,538 PF 0.224 | n=1,395 PF 0.176 | deeply negative core |
| 2.2 | production gate (rs≥2, atr≤.006, ≤11:10, top2) | n=67 PF 0.315 | n=38 PF 0.216 | live config is a loser; 06-09 validation was a 10-session artifact |

Failure anatomy → `FAILURE_ANALYSIS.md`: 69% SL-rate, avg loss > avg win, no feature quintile > PF 0.29.

## Stage 3 — Single-parameter sweeps (119 iterations)

CSV: `stage3_sweep_results.csv`. Command: `scripts/stage3_sweeps.py pools/pool_full`.
Groups: 42 exits, 20 indicator terms, 17 candle terms, 13 guards/time, 2 risk, premom (unavailable in this path), regime/market.

Keep/reject per group → `PARAMETER_SWEEP_SUMMARY.md`. **Kept for Stage 4:** max_slot 11:05 (only VAL-holding term), vol_ratio ≥2.6-3.0 (neutral-positive), SL 1.0-1.5 × Tgt 1.25-2.0 (least-bad exits). **Rejected as overfit-direction:** atr≤.004, body≥q75, vwap_dist≤2.8 (FIT-up/VAL-down). **Unusable:** rsi/adx/ranker/macd/ema20_slope masks (NaN columns → empty book).

## Stage 4/5 — Optuna TPE, full pool (25 trials, 3 seeds — budget-starved)

Command per seed: `pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool pools/pool_full --trials 700 --time_budget_min 12 --seed {7,23,41} --train_start 2026-03-01 --test_start 2026-06-01 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 ...`
Artifacts: `deep_runs/seed{7,23,41}/`.

| # | seed | trials | best TRAIN | TEST | verdict |
|---|---|---:|---|---|---|
| 4.1 | 7 | 12 | n=89 PF 0.293 | n=59 PF 0.255 | REJECT (8 hard reasons) |
| 4.2 | 23 | 3 | n=721 PF 0.247 | n=219 PF 0.149 | REJECT |
| 4.3 | 41 | 10 | n=302 PF 0.404 | n=116 PF 0.281 | REJECT |

**Learning:** ~60s/trial on the 20k-entry pool → search starvation. Fix in Stage 6: shrink pool to the one VAL-surviving structural subset.

## Stage 6 — Rescue loops (morning ≤11:05 pool: 1,168 rows / 56 sessions)

### 6a. Optuna on morning pool (1,000 trials, 2 seeds — full coverage)

Artifacts: `deep_runs/morning_seed{7,23}/`.

| # | seed | trials | best TRAIN | TEST | verdict |
|---|---|---:|---|---|---|
| 6.1 | 7 | 500 | n=48 PF 0.916 dayp 0.62 | n=31 PF 0.208 | REJECT |
| 6.2 | 23 | 500 | **n=23 PF 1.660 dayp 0.085 (IN BAND)** | **n=17 PF 0.277 net -15,311** | REJECT — the in-band pocket collapses OOS; neighborhood + term-dropout both fail |

### 6b. Evidence-driven combination grid (193 iterations)

CSV: `stage4_combo_results.csv`. Command: `scripts/stage4_combo_grid.py pools/pool_morning`.
Grid: vol_ratio {off,2.2,2.6,3.0} × top_n {off,1,2} × SL {0.85,1.0,1.2,1.5} × Tgt {1.0,1.25,1.5,2.0} + production reference.

**Result: 0/193 cleared even the relaxed FIT≥1.20 & VAL≥1.20 pre-gate.** Best honest combos: vol≥3.0 & 1.0/2.0 → FIT 0.864/VAL 0.510; vol≥3.0 & 1.5/2.0 → FIT 0.799/VAL 0.578.

### 6c. Strict TRAIN-band audits (all tried configs, band 1.30–1.80, TEST>1.40 + positive net)

- `rescore_fullpool/` — all unique full-pool trial configs re-scored on full TRAIN.
- `rescore_morning/` — all ~1,000 morning trial configs re-scored; every in-band survivor gets its single honest TEST shot.

Results in `CANDIDATE_CONFIGS.md` / `APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md`.

## CAMPAIGN 2 (2026-07-03) — Enriched feature space (user-directed exhaustive expansion)

Pool enriched with **40 recomputed features** direct from the 5-min parquet (leak-safe, bars ≤ signal):
all indicators (RSI+slope, ADX, CCI, MFI, Stoch, MACD hist/delta/cross, EMA20/50 dist+slope+stack,
BB pos/width, OBV slope), engineered pre-momentum (pre1/3/5 ret, green streak, vol trend, range
compression, pre-RSI, VWAP-hold bars), structure (20-bar-high, break margin, OR/PDH distance, gap,
day-position, bar-of-day, dow, price, notional). Script: `scripts/enrich_pool_features.py`.
Pools: `pool_enriched` (26,277) / `pool_enriched_first` (15,780; FIRST signal per ticker-day) /
`pool_enriched_first_am` (1,078) / `pool_enriched_first_20bh` (8,753; + genuine 20-bar-high break).

| # | iteration block | configs | key result |
|---|---|---:|---|
| 7.1 | Stage-3E sweeps, dedupe pool | 142 | **dedupe alone doubles base PF: FIT 0.557/VAL 0.475** (vs 0.276/0.258); `is_20bar_high` VAL-stable 0.517→0.550; NO term passes lift gate |
| 7.2 | Stage-3E sweeps, full enriched pool | 142 | top VAL 0.29 — dedupe is the driver, not any indicator |
| 7.3 | Extended Optuna (60-feat mask space), morning-dedupe pool | 1,200 trials | REJECT ×2 (best TRAIN 0.968/0.878; TEST 0.0/0.40) |
| 7.4 | Extended Optuna, 20bh pool, 3 seeds | 58 trials | REJECT ×3; **bests ≈ unmasked base (0.53-0.54)** — optimizer cannot beat "no mask" |
| 7.5 | Staged combos (10 shortlist terms → pairs × exits × guards), 20bh pool | 82 | **0 reach FIT PF 1.05** — stable terms are correlated; stacking removes trades, not losers |
| 7.6 | Strict rescore, ext_am trials | 760 unique | **0 in TRAIN band 1.30-1.80** |
| 7.7 | Strict rescore, ext_20bh trials | 51 unique | **0 in band** (best TRAIN 0.782 @ n=34) |

Campaign-2 subtotal: **~1,630 additional configs. Zero candidates.**
Grand total both campaigns: **~2,970 configs, 4 ever in-band (campaign 1 mine), 0 pass TEST.**

## Failure classifications across iterations

| classification | count (approx) | examples |
|---|---|---|
| TRAIN PF too low (core negative expectancy) | ~95% of configs | everything with n>100 |
| FIT-up/VAL-down (overfit direction) | atr≤.004, body≥q75, vwap≤2.8, max12:00 | stage 3 |
| in-band TRAIN, TEST collapse | morning seed 23 best (1.66→0.277) | stage 6a |
| robustness fail (neighborhood/dropout) | all engine bests | stages 4-6 |
| target-fill < 12% | all engine bests | exit structure cannot fill targets |
| day/symbol concentration flags | all engine bests | dayp ≈ 1.0 everywhere |
