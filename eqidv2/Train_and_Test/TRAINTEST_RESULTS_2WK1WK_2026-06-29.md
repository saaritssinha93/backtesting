# Per-Setup Train/Test Tuning — 2-week train / 1-week test

**Run:** 2026-06-29 · **Tool:** [setup_train_test.py](setup_train_test.py) (looping search) · **Pool:** `outputs_ID_v11_unified_pool` (built 2026-06-25, data ≤ 2026-06-24)

## Window (anchored to data max, not today — data ends 2026-06-24)
- **TRAIN = 2026-06-04 … 2026-06-17** (2 weeks, ~10 trading days)
- **TEST  = 2026-06-18 … 2026-06-24** (last 1 week, 5 trading days)

## Method (per user: keep window, anti-overfit)
Looping search per setup over: **SL grid × Target grid × ≤1 indicator/non-indicator mask term × ≤1 pre-momentum gate term × guards**, objective `band` (add the *fewest* terms to reach train PF ≥ 1.40 with the *most* trades — the anti-overfit objective), coarse quantile grid, both train-halves must be PF ≥ 1.0. Net of NSE statutory costs + 15 bps/leg slippage. Sample gates relaxed for the short window: min 8 train / 3 test trades; day-block p not gated (meaningless on 5 days).

> **Why anti-overfit:** the unconstrained `maxpf` pilot (2 mask + 2 premom terms, fine grid) produced **train PF 5.6–∞ → test PF 0.0–0.14** on the same window (A_MOD_BREAK_C1_LOW: train 5.56/n11 → test 0.00/n3; A_PULLBACK: train ∞/n15 → test 0.46/n2). Fitting ~5 knobs to ~13 train points and "validating" on 2–3 test trades is pure curve-fit — consistent with the conf history (`maxpf` was 0/16 robust, "PF>2 fake", lost money live). So the loop was constrained to report honestly.

---

## RESULTS — side by side (all 12 active-book setups)

| # | Setup | Side | Train PF | Train n | Test PF | Test n | Tuned config (SL/Tgt · mask · pre-mom) | Verdict |
|--:|---|---|--:|--:|--:|--:|---|---|
| 1 | A_PULLBACK_C2_THEN_BREAK_C2_LOW | SHORT | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |
| 2 | A_MOD_BREAK_C1_LOW | SHORT | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |
| 3 | **B_HUGE_RED_FAILED_BOUNCE** | SHORT | **1.55** | 23 | inf | **1** | 0.85/0.80 · `upper_wick_pct≥0.0999` · `pre3_range_r≤0.8696` | ⚠️ train-only (test n=1) |
| 4 | B_AVWAP_RECLAIM_REVERSAL | LONG | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |
| 5 | B_HUGE_C1_CLOSE_RECLAIM_BREAK | LONG | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |
| 6 | C_OR_BREAKDOWN | SHORT | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |
| 7 | D_EMA20_REJECTION | SHORT | — | — | — | — | DROP_NO_EDGE (only 15 train / 8 test candidate rows) | ❌ too few |
| 8 | E_VWAP_LOSE_EARLY_SHORT | SHORT | — | — | — | **0** | DROP_NO_EDGE — **0 test candidate rows** | ❌ no test data |
| 9 | G_HIGHER_HIGH_BREAK | LONG | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |
| 10 | **G_LOWER_LOW_BREAK** | SHORT | **1.64** | 34 | **0.42** | 3 | 0.85/1.00 · `quality_score≤42.6954` · `pre_entry_momentum_score≤71.0682` | ❌ test loses (−Rs629) |
| 11 | **L_DOUBLE_BOTTOM_VWAP** | LONG | **1.59** | 22 | inf | **1** | 1.00/0.80 · `upper_wick_pct≥0.1362` · `pre1_adx≤22.5549` | ⚠️ train-only (test n=1) |
| 12 | L_PRESSURE_BURST_VWAP | LONG | — | — | — | — | DROP_NO_EDGE | ❌ no train edge |

**Score: 0 / 12 profitable in both train AND test on a trustworthy sample.**
- 8 setups: **DROP_NO_EDGE** — could not build even a train edge (PF ≥ 1.40, both halves ≥ 1.0, ≥ 8 trades) with ≤1 mask + ≤1 pre-mom on 2 weeks.
- 3 setups got a train edge (PF 1.55–1.64) but the test is **n=1** (B_HUGE_RED, L_DOUBLE_BOTTOM — not evaluable) or an outright **loss** (G_LOWER_LOW, test PF 0.42).
- 1 setup (E_VWAP_LOSE_EARLY_SHORT) has **zero test-week candidates** — can't be tested at all.

---

## Detail — the 3 setups that reached a TRAIN edge

### B_HUGE_RED_FAILED_BOUNCE (SHORT) — train 1.55 / test n=1
- **SL/Target:** 0.85 / 0.80
- **Filter (mask):** `upper_wick_pct ≥ 0.0999`
- **Gate (pre-mom):** `pre3_range_r ≤ 0.8696`
- **Train:** n=23, 65% win, +Rs 2,981; halves W23 PF 3.44 / W24 0.69 / W25 6.60 (uneven).
- **Test:** n=1 (one trade, +Rs 567) → **not evaluable.**

### G_LOWER_LOW_BREAK (SHORT) — train 1.64 / test 0.42 ❌
- **SL/Target:** 0.85 / 1.00
- **Filter (mask):** `quality_score ≤ 42.6954`
- **Gate (pre-mom):** `pre_entry_momentum_score ≤ 71.0682`
- **Train:** n=34, 65% win, +Rs 4,639, both halves ≥ 1.0 (minHalf 1.46).
- **Test:** n=3, 67% win but **PF 0.42, −Rs 629** (the wins were small, one big stop). OOS/IS ratio 0.26 — fails.

### L_DOUBLE_BOTTOM_VWAP (LONG) — train 1.59 / test n=1
- **SL/Target:** 1.00 / 0.80
- **Filter (mask):** `upper_wick_pct ≥ 0.1362`
- **Gate (pre-mom):** `pre1_adx ≤ 22.5549`
- **Train:** n=22, 64% win, +Rs 2,918; but W23 PF 0.26 (−1,619), carried by W24 PF 44.2 — lumpy.
- **Test:** n=1 (+Rs 566) → **not evaluable.**

> Note: these three tuned masks (`upper_wick_pct`, `quality_score`, `pre_entry_momentum_score`, `pre1_adx`, `pre3_range_r`) are **not** the production gates from `final_setup_conf.py` — they are what this short window's search latched onto. On 22–34 train trades they are low-confidence and should not be promoted.

---

## Root cause — the window, not the setups
A 1-week test = **2–5 deduped trades per setup**; a 2-week train = **11–34**. That is below the floor for any honest train→test inference:
- enough train trades to fit a threshold + SL/Tgt → almost any setup shows a pretty in-sample PF;
- too few test trades (often 1) to confirm anything → "profitable in both" is luck, not edge.

This is the same trap recorded in project memory (overfit conf, live PF 0.25). The setups aren't necessarily broken — the **measurement** is too thin to tune on.

## Recommended next step (pick one)
1. **Longer TRAIN, keep TEST = last week:** TRAIN = 3 months before (2026-03-18 … 2026-06-17), TEST = 2026-06-18 … 2026-06-24. ~10× the train sample; test still your "last week". Re-run the same loop — results you can act on.
2. **Walk-forward:** roll TRAIN=2wk/TEST=1wk across the last ~4 months; keep only setups profitable across most folds (robust to any single week).
3. **Rebuild the pool to today** if you truly need 2026-06-25…29 in the test (data currently ends 06-24), then option 1 or 2.

Reproduce any row:
```
py -3.12 Train_and_Test/setup_train_test.py --family <F> --setups <SETUP> \
  --pool_dir "C:/TradingData/eqidv2/outputs_ID_v11_unified_pool" \
  --train_start 2026-06-04 --train_end 2026-06-17 \
  --test_start 2026-06-18 --test_end 2026-06-24 \
  --objective band --min_train_trades 8 --min_test_trades 3 \
  --min_minhalf_pf 1.0 --max_day_block_p 1.0 --min_oos_ratio 0.0 \
  --max_mask_terms 1 --max_premom_terms 1 --no_fdr
```
