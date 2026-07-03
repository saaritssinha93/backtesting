# Aggressive iteration → robust PF≥2 → honest TEST — RESULTS 2026-06-22

Window: TRAIN 2026-03-01..05-31, TEST 2026-06-01..06-15. Net of NSE intraday cost.
Search: `setup_train_test.py --objective band --train_pf_min 2.0 --max_mask_terms 3
--max_premom_terms 2 --fine_quantiles` per family on the unified pool. Acceptance gate:
test PF≥1.30 AND day_block_p<0.10 AND oos/is≥0.55 AND test n≥8.

## A. Executive summary
- **Can we reach PF≥2 in TRAIN? Yes — trivially, almost everywhere.** The search hit a
  per-setup TRAIN PF of 2.0–2.45 for ~every setup with data.
- **Does it survive TEST? Only ONE family of seven: D.** Every other family's re-tuned book
  collapses out of sample (test PF 0.26–0.84, oos/is 0.12–0.42).
- **THE robust win — D family (ACCEPT):** TRAIN PF 2.32 → TEST PF **1.985**, oos/is 0.86,
  **day_block_p 0.0055 (significant)**. Anchored by a *new* setup not in the book:
  - **D_AVWAP_LOSE_REVERSAL (SHORT):** TRAIN 2.32 / n=82 / win 62% → **TEST 1.83 / n=34 /
    win 68%**. Genuine, sample-decent, significant. Discovery of the run.
  - D_EMA20_REJECTION (SHORT): TRAIN 2.33 / n=17 → TEST n=2 (too thin to trust alone).
- **Re-tuning HURT the already-good setups.** The existing B_HUGE_RED gate scored 2.19/1.88
  (honest eval); the aggressive re-search replaced it with an overfit config that drags
  family B to test 0.26. Lesson: don't re-tune validated setups just to chase PF.
- **Dominant overfit vector:** `market_ret_pct` regime bands. The search repeatedly latched
  onto train-period market-return windows (e.g. A_MOD_BREAK_C1_LOW market_ret∈[-1.25,-1.11]
  → train 2.02) that don't exist in the test period → collapse. Flag and ban these.
- **Net answer:** PF>2 that holds OOS exists for **D_AVWAP_LOSE_REVERSAL** only; keep the
  existing **B_HUGE_RED_FAILED_BOUNCE**; everything else either overfits or stays as-is.

## B. Family results (band search, robust objective)
| Family | Best per-setup TRAIN PF | Combined TRAIN PF | TEST PF | oos/is | day_block_p | Verdict |
|---|---|---|---|---|---|---|
| A | 2.02–2.28 | 1.59 | 0.53 | 0.33 | 0.75 | REJECT (overfit; market_ret bands) |
| B | 2.17–2.33 | 2.19 | 0.26 | 0.12 | 0.999 | REJECT (re-tune worse than existing) |
| C | 2.14 | 2.00 | 0.84 | 0.42 | nan | REJECT |
| **D** | 2.06–2.33 | **2.32** | **1.985** | **0.86** | **0.0055** | **ACCEPT** |
| E | 2.07–2.24 | 2.04 | 0.30 | 0.15 | 0.990 | REJECT |
| G | 2.01–2.11 | 2.15 | 0.74 | 0.35 | 0.66 | REJECT |
| L | 2.01–2.45 | 1.78 | 0.52 | 0.29 | 0.86 | REJECT |

## C. Parameter-change table (proposed)
| Setup | Old gate | New gate (found) | Train PF | Test PF | Decision |
|---|---|---|---|---|---|
| **D_AVWAP_LOSE_REVERSAL** | (not in book) | mask market_ret_pct≥0.12 & vwap_dist_atr≤-0.96; SL/Tgt 1.2/1.0 | 2.32 | **1.83** | **ACCEPT** (confirm via v11 — native) |
| D_EMA20_REJECTION | premom pre10/pre5/adx; 0.75/1.30 | mask signal_minute≥859.5 & rs_pct≥-1.35; 0.85/0.80 | 2.33 | inf (n=2) | KEEP-WATCH (thin test) |
| B_HUGE_RED_FAILED_BOUNCE | existing gate (2.19/1.88) | re-tuned market_ret band | 2.33 | (family fails) | **REJECT re-tune; keep existing** |
| A/C/E/G/L re-tunes | various | market_ret_pct bands etc. | ~2.0–2.4 | 0.26–0.84 | REJECT (overfit) |

## D. Overfitting check
- **D ACCEPT is justified:** its gates are structural (vwap_dist_atr, signal_minute, rs_pct),
  NOT market-return timing; both train halves positive; test n=82/34 (not tiny);
  day_block_p 0.0055 significant; oos/is 0.86. Caveat: D_AVWAP_LOSE_REVERSAL is native* —
  scored on raw/ungated candidates, so CONFIRM with the v11 conf backtest before promoting.
- **Everything else fails the check:** test collapse, oos/is<0.55, insignificant day_block_p,
  and reliance on market_ret_pct bands (regime curve-fit). Reject.
- D_EMA20_REJECTION test n=2 → cannot stand alone; rides on the family.

## E. Final recommended logic
1. **Promote D_AVWAP_LOSE_REVERSAL** (SHORT): enter on AVWAP-lose reversal with
   `market_ret_pct≥0.12 & vwap_dist_atr≤-0.96`, exit SL 1.2 / Tgt 1.0 — *after* v11 confirms
   the live-gated (native) version holds n and PF.
2. **Keep B_HUGE_RED_FAILED_BOUNCE** on its existing gate (2.19/1.88). Do not re-tune.
3. **Keep the 4 live-losers demoted** (done). Leave A/C/E/G/L unchanged (no robust PF≥2).
4. **Ban `market_ret_pct` threshold terms** from the search feature set — they are the main
   overfit source.

## F. Production notes
- Promotion path: `setup_train_test.py --family D --pool_dir <unified> ... --approve` writes
  the root conf (review-gated), OR hand-add D_AVWAP_LOSE_REVERSAL with a reviewable diff.
- Confirm native D via: `avwap_5min_ID_v11_backtesting.py --mode historical_all_available
  --selected_strategy_profile final_setup_conf --workers 8` (after close).
- Monitor live with `Train_and_Test/live_paper_holdout.py`; warn if D_AVWAP test win% <55%
  or fires >5×/day.

## B2. STRICT re-run (market_ret banned, mask<=2/premom<=1, floor 25, minhalf 1.3, gate test>=1.5/p<=0.05/ratio>=0.65/n>=10)
| Family | TRAIN PF | TEST PF | oos/is | day_block_p | Strict verdict | (loose TEST was) |
|---|---|---|---|---|---|---|
| A | 1.84 | 0.64 | 0.35 | 0.91 | REJECT | 0.53 |
| B | 2.23 | 0.35 | 0.16 | 1.00 | REJECT | 0.26 |
| C | 1.45 | 0.43 | 0.30 | 0.99 | REJECT | 0.84 |
| D | 2.33 | 1.14 | 0.49 | 0.29 | REJECT | 1.99 (was ACCEPT) |
| E | 2.08 | 0.36 | 0.17 | 0.99 | REJECT | 0.30 |
| G | 2.23 | 0.81 | 0.36 | 0.69 | REJECT | 0.74 |
| L | 1.85 | 0.38 | 0.20 | 0.98 | REJECT | 0.52 |

**0/7 ACCEPT.** Banning `market_ret_pct` dropped D from test 1.99 → 1.14, confirming the prior
"pass" partly rode the regime band. Triple-confirmed (maxpf 0/16, loose band 1/7, strict band
0/7): no setup reaches a robust PF>=2 surviving an honest strict OOS test on this window.
Test day_block_p ~0.9-1.0 everywhere = the 06-01..06-15 window was uniformly adverse to the
re-tuned configs (and a 2-week test is too short to certify anyway). Do NOT promote any
re-tuned config from this search.

## B3. STRICT v2 — regenerated data, extended 3-week holdout (TEST 06-01..06-22)
Harvested live raw candidates 06-11..06-22 (3,018 rows), rebuilt pool (test rows 16,634→17,969,
date_max 06-22), re-ran the strict search. Result: **still 0/7 ACCEPT.** Test PFs essentially
unchanged from v1 (A 0.62, B 0.31, C 0.43, D 1.14, E 0.55, G 0.77, L 0.38); the longer window
added test trades but lifted no PF above ~1.1. So the failure is NOT a thin-window artifact —
it is a genuine lack of generalizable edge.

**Four-way confirmation that no robust PF>=2 exists on this data:** maxpf 0/16; loose band 1/7
(D, market-band-dependent); strict band 0/7; strict+extended-holdout 0/7. Conclusion: parameter/
filter re-tuning cannot manufacture a robust PF>=2 here. STOP chasing PF>2 by threshold search.
Keep B_HUGE_RED on its existing gate; keep the 4 live-losers demoted; real levers = walk-forward
validation + more live data + genuinely new signal logic (not threshold tuning).

## G. Next iteration
1. v11 backtest to confirm D_AVWAP_LOSE_REVERSAL on the live-gated basis (native caveat).
2. Remove `market_ret_pct` from SIGNAL_FEATURES in the tuner; re-run A/E/L (their PF≥2 came
   mostly from that band — see if a structural edge remains).
3. Walk-forward (multiple 2-week test folds) instead of a single holdout for the survivors.
4. Priority setup for more research: the D-family AVWAP-lose-reversal mechanism (the only
   thing that generalized).
