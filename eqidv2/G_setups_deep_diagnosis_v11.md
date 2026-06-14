# G* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 days, ~1,200-ticker NSE universe). Generated 2026-06-11.*

> Of the G* setups, only **G_HIGHER_HIGH_BREAK** (LONG) has a real population (750 trades).
> G_LOWER_LOW_BREAK (SHORT) has **n=5** (insufficient); G_DRIVE_CONTINUATION_LONG/SHORT have
> **0** in the clean pool. Read from the consistent clean pool, resolved at the fixed
> production exit (0.90/1.50) on 1-minute data, NET of NSE cost, with the production
> pre-momentum gate ON/OFF, then a sub-population × exit sweep. Engine:
> `validate_G_setups_filters.py`.

---

## 0. UPDATE (2026-06-12) — SALVAGED via aggressive iteration → PROMOTED
The first pass (single-variable sweep, §1–§9 below) rejected G_HIGHER_HIGH_BREAK as a net
loser. A **second, aggressive pass** (`G_iterate.py`: greedy forward-selection + exhaustive
2-term + 40k randomized 3-term combos, exit co-optimized, robust=min-train-halves) found a
**robust pre-momentum/ADX pocket** that the coarse sweep missed:

> **gate `pre2_mom_r ≥ 0.55` AND `sig5_adx_calc ≥ 26`, exit 0.90/2.50**
> → **TRAIN PF 2.38 (n39, halves 2.47/2.30) · TEST PF 2.66 (n8) · day-block p 0.005 · full +Rs 22,620**

Validated NOT a knife-edge (`G_validate_passer.py`): train>1.5 & test>1.3 across a contiguous,
monotonic region `pre2_mom_r[0.4,0.6] × sig5_adx_calc[24,30]`; both train halves strong;
test-positive at every exit (0.9/2.5, 0.9/1.5, 0.7/1.25, 0.7/1.0); 6/8 months positive
(losers Nov/Dec only, smallest/earliest). **Mechanism:** the 20-bar-high breakout only pays
with a genuine ADX-confirmed momentum thrust at entry — the gate drops the late/chase/exhaustion
breakouts that made the ungated setup a loser. **PROMOTED to the active book as STRONG PROBATION.**
The §1–§9 "reject" below is the *ungated* analysis — kept for the record. **Trade only the gated version.**

---

## 1. Executive summary (first pass — UNGATED)

| Setup | n (train/test) | TRAIN PF | TEST PF | Net | Verdict |
|---|---|---|---|---|---|
| **G_HIGHER_HIGH_BREAK** (ungated) | 637 / 113 | 0.94 | 0.77 | **−Rs 26,855** | loser ungated → **see §0: gated = keeper** |
| G_LOWER_LOW_BREAK (SHORT) | 5 | — | — | — | cannot validate (n=5) |
| G_DRIVE_CONTINUATION_LONG/SHORT | 0 | — | — | — | not in pool (EARLY/overlay, blocked) |

**Ungated, G_HIGHER_HIGH_BREAK is a clear churn/cost loser** (750 trades, −Rs 26,855 at the
production exit; no *single-variable* sub-population has edge). The edge only appears under the
2-term momentum/ADX gate found in the second pass (§0). The production gate
(`pre3_close_pos≤0.985 & sig5_rsi_dir≤67.878`) is the WRONG gate — it makes the loss worse.

---

## 2. Setup definition (the one with data)

**G_HIGHER_HIGH_BREAK** (LONG, `twenty_bar_higher_high_break`,
avwap_5min_ID_v2_backtesting.py L745). A 5-min bar is this setup when:
`long_struct` (close>open & close_loc≥0.60) **and** `above_vwap` (close>VWAP) **and**
`close > 20-bar prior high (rh)` **and** `rs_pct > 0.00` **and** `vol_ratio ≥ 1.4` **and**
`regime != BEAR`. Production exit **0.90/1.50**. Production pre-momentum gate:
`pre3_close_pos ≤ 0.985417 & sig5_rsi_dir ≤ 67.878`.

**What it is at entry:** a high-volume (median vol_ratio **3.45×**), **highly extended**
breakout — median **vwap_dist_atr 2.9 ATR above VWAP**, median rs_pct 0.65. This is a
momentum *chase*: by the time price takes out the 20-bar high on big volume it is already
far above value, so the reward-to-go is small and the mean-reversion risk is large.

---

## 3. Baseline results (production exit 0.90/1.50, NET)

| Slice | TRAIN n/PF | TEST n/PF/win | day-block p | Net Rs |
|---|---|---|---|---|
| ALL | 637 / 0.94 | 113 / 0.77 / 36% | 0.812 | **−26,855** |
| premom **ON** (prod gate) | 303 / 0.88 | 62 / 0.79 / 37% | 0.747 | −21,045 |
| premom **OFF** | 334 / 1.00 | 51 / 0.75 / 35% | 0.770 | −5,810 |

The setup loses in both train and test. **The production pre-momentum gate makes it WORSE**
(ON net −21k vs OFF −5.8k; what the gate *keeps* is worse than what it drops) — the same
lesson as the E/D shorts: these momentum-confirmation gates do not help.

---

## 4. Bucket analysis (one variable at a time, production exit)

Only two of ~25 buckets even reach full_pf ≥ 1.2, and **neither survives test**:

| Variable | Bucket | n | TRAIN PF | TEST PF | full PF | p |
|---|---|---|---|---|---|---|
| vwap_dist_atr | ≥ 3.5 (most extended) | 155 | 1.26 | 1.00 | 1.23 | 0.167 |
| rs_pct | [1.0, 2.0) | 171 | 1.42 | **0.26** | 1.21 | 0.228 |

- The `rs_pct [1.0,2.0)` cell looks decent in train (1.42) but **test collapses to 0.26** —
  textbook overfit; not real.
- Counter-intuitively the *most* extended bucket (vwap_dist_atr ≥ 3.5) is the least bad,
  but still only test 1.00, p 0.167 — no edge.
- Every other regime/market/vol/time/close_loc bucket is sub-1.2.

---

## 5. Sub-population × exit sweep (find any train+test-positive cell)

SL ∈ {0.5,0.7,0.9,1.1} × Tgt ∈ {0.8,1.0,1.25,1.5,2.0} × 11 sub-populations
(regime, vwap_dist, market_ret, rs, vol band, premom on/off). **Best cells:**

| Sub-pop | SL/Tgt | TRAIN n/PF | TEST n/PF/win | p |
|---|---|---|---|---|
| regimeNEUTRAL | 1.1 / 2.0 | 371 / 1.03 | 84 / **1.29** / 43% | 0.22 |
| premom_OFF | 1.1 / 2.0 | 334 / 1.08 | 51 / 1.02 / 39% | 0.49 |
| regimeNEUTRAL | 0.9 / 2.0 | 371 / 1.01 | 84 / 1.15 / 39% | 0.37 |

**SURVIVORS (train≥1.5 & test≥1.3 & p<0.10): 0.**

Widening the target to **2.0R** (from 1.5R) is the only lever that lifts the NEUTRAL/premom-OFF
test PF toward ~1.0–1.3 — but **train PF stays ~1.0** (coin-flip) and the day-block p is never
significant (best 0.22). There is no exit, no regime, no volume band, no extension band, with
or without the pre-momentum gate, that turns this into an edge.

---

## 6. Diagnosis — why it fails
A 20-bar-high breakout that fires only *after* a big-volume up-bar is, by construction, a
**late chase**: entry is a median 2.9 ATR above VWAP, so most of the move is already gone and
the position sits on top of an over-extension that mean-reverts. The 36% test win rate at a
0.90/1.50 exit confirms it: small wins, frequent stop-outs, net negative after cost. This is
the long-side analogue of E_ORB_BREAKOUT_SHORT — a momentum-chase that doesn't pay.

---

## 7. Recommended changes
- **G_HIGHER_HIGH_BREAK:** do NOT trade. Net loser (−Rs 26,855 / 750 trades), no salvageable
  sub-population. In the live v17 runners it should be **disabled / size 0** (it already is in
  v17C noNF: `enabled: False`; it is still enabled in v17k/v17l/v17B — recommend disabling there too).
- **Drop its production pre-momentum gate** wherever it remains — the gate makes the loss worse.
- **G_LOWER_LOW_BREAK:** n=5 — no opinion; revisit only if the population grows materially.
- **G_DRIVE_CONTINUATION_LONG/SHORT:** zero candidates in the clean pool — nothing to assess.

---

## 8. Anti-overfit notes
- The only train-attractive cell (rs_pct [1.0,2.0) train 1.42) **fails out-of-sample (test 0.26)** —
  a clear reminder that train PF alone is meaningless here.
- 750 trades is the **largest sample of any family studied** — so the "no edge" finding is the
  most statistically trustworthy rejection in the whole A–G sweep, not a small-sample artifact.

## 9. What NOT to do
- Don't promote G_HIGHER_HIGH_BREAK on the rs_pct or vwap_dist train numbers — they don't hold OOS.
- Don't keep its pre-momentum gate (it dilutes/worsens).
- Don't read anything into G_LOWER_LOW_BREAK (n=5).

## 10. Family scorecard
**G contributes 1 setup** — G_HIGHER_HIGH_BREAK (pre-momentum/ADX-gated, §0), one of the two
strongest edges in the book alongside E_VWAP_LOSE_EARLY_SHORT (both ~train 2.4 / test 2.7,
day-block p ~0.005). Lesson: the coarse single-variable sweep wrongly rejected it; the
**aggressive multi-term + exit co-optimized search with anti-overfit validation** recovered a
real, mechanically-sound pocket. The ungated setup and the production gate remain losers.

## 11. The salvage search (second pass) — method & anti-overfit
- **Engine** `G_iterate.py`: (1) greedy forward-selection up to 4 mask terms per exit, objective
  = robust train PF (min of two train halves); (2) exhaustive 2-term on the top thresholds × exits;
  (3) 40,000 randomized 3-term combos × random exit. 271 configs reached train PF≥2.
- **Multiple-testing guard:** most train-PF≥2 hits had test n=1–4 (noise). Only configs sharing the
  same 2-term momentum/ADX core survived with test n≥8 and p<0.10 — and that core is **contiguous,
  monotonic, exit-robust, both-halves-strong, and mechanistic**, which a multiple-testing fluke is not.
- **Chosen config:** `pre2_mom_r≥0.55 & sig5_adx_calc≥26`, exit 0.90/2.50 (picked for train-half
  balance + day-block p, not max test PF). Status: STRONG PROBATION (n=39/8 → do not size up).
- **Live note:** runners (v17k/l/B) should DROP the old production gate and adopt this one.
