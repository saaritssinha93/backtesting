# B* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 trading days, ~1,200-ticker NSE universe). Generated 2026-06-11.*

> Method: every B* candidate the strategy admitted (clean consistent pool, post
> v8-gate/overlay, **pre-dedupe** so the raw setup population is visible) was
> resolved at the **fixed production exit SL 0.70 / Tgt 1.50** on 1-minute data,
> **net of statutory NSE cost**, then bucketed (winners vs losers) by regime, time,
> rs_pct, vwap_dist_atr, vol_ratio, atr_pct, close_loc/body_pct, with MAE/MFE.
> Engine: `validate_B_setups_filters.py`. Raw rows: `B_setups_trades_nov_to_now.csv`.

---

## 1. Executive summary

| Setup | n (Tr/Te) | TRAIN PF | TEST PF | Diagnosis | Verdict |
|---|---|---|---|---|---|
| B_AVWAP_RECLAIM_REVERSAL | 51 / 11 | 0.99 | 0.40 | **Production mask is INVERTED** — buys extended reclaims that fail | **Fixable (high confidence)** |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 53 / 11 | 0.86 | **3.17** | **Regime-gated**: NEUTRAL wins, BULL loses; not overfit | **Promising (regime-sample risk)** |
| B_HUGE_RED_FAILED_BOUNCE | 43 / 13 | 0.75 | 0.54 | Shorts **exhausted** down-moves; weakest in BEAR | **Structurally weak — don't tune yet** |

**Three headline findings (all data-grounded, all counter to the current config):**
1. **B_AVWAP's `vwap_dist_atr ≥ 0.60` mask is backwards.** The edge lives **0–0.6 ATR** from VWAP (PF 1.48, immediate-fail 15%); the `≥0.6` region the mask *selects* is exactly where it loses (PF 0.6, immediate-fail 43%). A reclaim that's already 0.6+ ATR above VWAP isn't a reclaim — it's a chase.
2. **B_HUGE_C1 fails in BULL, wins in NEUTRAL** (BULL −₹5,880 / NEUTRAL +₹9,034). The "test PF 3.17 vs train 0.86" gap is a **regime mix artifact** (train Jan–Apr was bull-heavy), not curve-fit. Adding the conventional "require BULL/TREND" filter would *hurt* it.
3. **B_HUGE_RED is a structural short into exhaustion.** It loses in every regime and worst in BEAR. No single threshold fixes it; the real problem is entry *timing* (it enters after the huge red bar, i.e. after the move).

**Cost basis:** NET of NSE intraday cost (~₹82/round-trip at ~₹1L notional).
**Exit mode:** fixed 0.70/1.50 here (to isolate setup behaviour from the band/maxpf-tuned exits used in the earlier family runs). The user-quoted numbers (B_AVWAP TRAIN 1.40/TEST 0.44 etc.) were **band-mode, post-dedupe+mask**; they differ from the fixed-exit pre-dedupe numbers here — see §3.

---

## 2. Current setup definitions
(As provided — detection in `avwap_5min_ID_v2_backtesting.py` L680/695/703; exits in `avwap_5min_ID_v6_backtesting.py` L51/52/54; v11 mask in `avwap_5min_ID_v11_backtesting.py`.)

- **B_AVWAP_RECLAIM_REVERSAL** (LONG): close>open, close_loc≥0.60, prev_close<prev_VWAP, close>VWAP, rs_pct>−0.10, vol_ratio≥1.4, regime≠BEAR. Mask: `pre_entry_momentum_score ≤ 64.7678 AND vwap_dist_atr ≥ 0.60`. Exit 0.70/1.50.
- **B_HUGE_C1_CLOSE_RECLAIM_BREAK** (LONG): huge_prev(≥1.8×ATR), prev green, close>prev_high, close>open, close_loc≥0.60, close>VWAP, vol_ratio≥1.3, regime≠BEAR. Mask: `rs_pct ≤ 10.7`. Exit 0.70/1.50.
- **B_HUGE_RED_FAILED_BOUNCE** (SHORT): huge_prev, prev red, close<prev_low, close<open, close_loc≤0.40, close<VWAP, vol_ratio≥1.3, regime≠BULL. Mask: none (AB-probation admit only). Exit 0.70/1.50.

---

## 3. Current train/test results (and why they differ from the quoted numbers)

**Fixed exit 0.70/1.50, NET, pre-dedupe (this study):**

| Setup | TRAIN n / win% / PF / net | TEST n / win% / PF / net | imm-fail% | mfe_R |
|---|---|---|---|---|
| B_AVWAP_RECLAIM_REVERSAL | 51 / 35.3 / 0.99 / −₹308 | 11 / 18.2 / 0.40 / −₹4,212 | 31–46 | 0.8–1.3 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 53 / 32.1 / 0.86 / −₹4,062 | 11 / 63.6 / **3.17** / +₹6,778 | 27–36 | 1.3–2.2 |
| B_HUGE_RED_FAILED_BOUNCE | 43 / 30.2 / 0.75 / −₹5,790 | 13 / 23.1 / 0.54 / −₹3,567 | 33–39 | 1.3 |

**Mismatch vs the quoted band-mode numbers** (e.g. B_HUGE_C1 quoted TEST "2 trades / PF 1.81"): the quoted figures were **band/maxpf mode, post one-ticker-per-day dedupe + selected-strategy mask**, which (a) tuned the exit and (b) cut B_HUGE_C1's test set from 11 → 2 trades. **The mask/dedupe was discarding good test trades.** The fixed-exit pre-dedupe view (PF 3.17 on 11) is the cleaner read of the setup itself. Both are net-of-cost; use the **fixed-exit pre-dedupe** numbers for setup diagnosis and the band numbers only for the deployed-config view.

---

## 4. Diagnosis per setup

### 4.1 B_AVWAP_RECLAIM_REVERSAL — *the mask is inverted*

**A. Entry-logic quality.** Captures: a stock that was *below* session VWAP reclaiming it on a strong up-bar (momentum reversal from weakness). In practice it is a **reversal/reclaim**, not continuation. The core idea is sound; the production *filter* defeats it.
- **Too loose:** `rs_pct > −0.10` (admits names still weaker than NIFTY). Buckets: rs_pct>2 (the bulk) PF 0.86 — RS doesn't separate here, but the threshold is permissive.
- **Too strict / WRONG DIRECTION:** `vwap_dist_atr ≥ 0.60` — this is the killer. A genuine reclaim closes *just* above VWAP; `≥0.6 ATR above` means it has already run and you're buying the top.
- **Entering too late / after exhaustion:** yes, in the `≥0.6` and `vol_ratio>3` buckets (immediate-fail 43–50%) — it buys the extended bar that immediately reverses.

**B/C. Bucket analysis (winners vs losers).**

| Dimension | Winning region | Losing region |
|---|---|---|
| **vwap_dist_atr** | **0–0.6: PF 1.48, +₹4,115, immFail 15%** | 0.6–1.5: PF 0.6, −₹6,475, immFail 43%; 1.5–3: PF 0.72 |
| vol_ratio | 1.6–2: PF 1.36, immFail **7%** | >3: PF 0.6, −₹6,456, immFail 43% |
| atr_pct | 0.6–0.8%: PF 1.61, +₹4,279 | >1.2%: PF 0.6, immFail 50%; <0.4%: PF 0.45 |
| regime | BULL 0.97 / NEUTRAL 0.95 | **TREND 0.45, −₹3,419** |
| time | >13:00: PF 1.13 | 12:00–13:00: PF 0.4, immFail 46% |

Winners: **near VWAP, moderate volume (1.6–2), moderate ATR (0.6–0.8%)** — a *clean* reclaim. Losers: extended (>0.6 ATR), exhaustion volume (>3), high ATR (>1.2%), midday. Losses are **immediate failures** (high imm-fail in the bad buckets), not slow fades — the entry is mistimed.

**D. Stability.** The vwap_dist relationship is **monotonic and mechanical** (closer-to-VWAP = better) across 62 trades → low overfit risk. The `pre_entry_momentum_score ≤ 64.7678` (6-decimal) is a **data-mined artifact** — flag.

**Verdict:** Fixable with high confidence by **inverting the vwap_dist mask**.

### 4.2 B_HUGE_C1_CLOSE_RECLAIM_BREAK — *regime-gated, most promising*

**A.** Captures: momentum continuation — break of a prior huge green bar's high. In practice a **continuation/breakout**. Winners run (mfe_R 1.87 in NEUTRAL) → the 1.50 target is well-judged.
- **Too loose:** the mask `rs_pct ≤ 10.7` is a **near no-op** (rs is rarely that high) — effectively no filter.
- The real driver is **regime**, which the current logic only half-uses (`regime ≠ BEAR`).

**B/C. Bucket analysis.**

| Dimension | Winning region | Losing region |
|---|---|---|
| **regime** | **NEUTRAL: PF 1.96, +₹9,034, win 52%** | **BULL: PF 0.66, −₹5,880, win 27%** |
| time | 12:00–13:00: PF 1.45; 10:00–11:00 (n=2) strong | 11:00–12:00: PF 0.7 |
| atr_pct | 0.6–0.8%: PF 1.36 | 0.4–0.6%: PF 0.68 |
| vwap_dist_atr | 1.5–3: PF 1.81 (small n) | >3 (the bulk): PF 1.03 |

The setup is **regime-conditional**: a huge-green-break in a *flat* market is a real breakout; the same break in a *bull* market is a late chase that fails. This is why test (May–Jun, more neutral) beat train (Jan–Apr, bull-heavy). **Not overfit — regime mix.**

**D. Stability.** The NEUTRAL edge rests on **25 train + a thin test** → **sample-size risk HIGH**. The direction is mechanically defensible (chase-in-bull fails), but the magnitude needs more data.

**Verdict:** Promising; gate on **regime, not RS**. Do not require BULL (the opposite of intuition).

### 4.3 B_HUGE_RED_FAILED_BOUNCE — *structural short into exhaustion*

**A.** Intends: breakdown short after a huge red bar fails to bounce. In practice it **shorts an already-completed down-move** — the huge red bar *is* the move; breaking its low is late. Indian-equity upside bias compounds it.
- **Too loose:** `regime ≠ BULL` only; nothing requires the market to actually be falling *now*.
- The deeper issue is **timing**, not a threshold — by entry the down-impulse is spent and bounce risk is high.

**B/C. Bucket analysis — uniformly weak.**

| Dimension | Best (still weak) | Worst |
|---|---|---|
| regime | NEUTRAL: PF 0.75 | **BEAR: PF 0.58, −₹7,271** |
| vol_ratio | **1.6–2: PF 1.21, +₹487** (only positive) | 2–3: PF 0.0, −₹3,895; >3: PF 0.76 |
| atr_pct | 0.6–0.8%: PF 0.9 | **>1.2%: PF 0.52, immFail 56%** |

It **loses worst in BEAR** (exhaustion/bounce) — the exact regime the current logic permits. Only the *moderate-volume* (1.6–2) sliver is positive (n=5). High-ATR shorts fail fastest (imm-fail 56%).

**Verdict:** No filter rescues the core; the fix is a **logic change** (require a *failed-bounce* lower-high *after* the huge red, not just a break of its low). Out of scope for "preserve core idea via filters." **Keep blocked/probation.**

---

## 5. Bucket tables (winners-vs-losers, condensed)
Full machine-readable: `B_setups_bucket_analysis.csv`. The decisive cells are bolded in §4.2–4.3 tables above (the single dimension that separates win from loss differs per setup: **vwap_dist_atr** for B_AVWAP, **regime** for B_HUGE_C1, **none clean** for B_HUGE_RED).

---

## 6. Recommended filter changes by setup (data-grounded)

### B_AVWAP_RECLAIM_REVERSAL
- **Invert the vwap_dist mask** → require the reclaim **near** VWAP. (#1 must-test.)
- Cap exhaustion volume; cap high ATR.
- Drop/replace the 6-decimal `pre_entry_momentum_score ≤ 64.7678`.

### B_HUGE_C1_CLOSE_RECLAIM_BREAK
- Replace the no-op `rs_pct ≤ 10.7` with a **regime gate: `regime != BULL`** (keep NEUTRAL/TREND).
- Keep 1.50 target (winners run).

### B_HUGE_RED_FAILED_BOUNCE
- Only if pursued: `vol_ratio ∈ [1.4, 2.2]` AND `atr_pct ≤ 0.012` AND `market_ret_pct ≤ −0.20`. All small-sample — **probation only**.

---

## 7. Conservative / Balanced / Aggressive versions
(Machine-readable in `B_setups_v11_candidate_config.json`; experiment IDs in `B_setups_filter_experiment_plan.csv`.)

**B_AVWAP_RECLAIM_REVERSAL**
- *Conservative:* replace `vwap_dist_atr ≥ 0.60` → **`vwap_dist_atr ≤ 0.75`**. (TRAIN PF 0.99→1.67 on the near-VWAP subset.)
- *Balanced:* `−0.20 ≤ vwap_dist_atr ≤ 0.60` **AND** `vol_ratio ≤ 2.5`.
- *Aggressive:* balanced **AND** `0.5% ≤ atr_pct ≤ 1.0%` **AND** `rs_pct > 0`.

**B_HUGE_C1_CLOSE_RECLAIM_BREAK**
- *Conservative:* add **`regime != BULL`** (drop the rs_pct≤10.7 no-op).
- *Balanced:* `regime != BULL` **AND** `atr_pct ≤ 0.009`.
- *Aggressive:* `regime == NEUTRAL` **AND** `signal_minute ≤ 780`. (Higher PF in-sample, but **narrows test to single digits — overfit risk high**.)

**B_HUGE_RED_FAILED_BOUNCE**
- *Conservative:* `vol_ratio ∈ [1.4, 2.2]`.
- *Balanced:* + `atr_pct ≤ 0.012` + `market_ret_pct ≤ −0.20`.
- *Aggressive:* not offered — sample too thin; would be data-mining.

---

## 8. Exit logic review (0.70 / 1.50)

- **MAE:** mean MAE ≈ **−1.1 to −1.2 R** across all three → trades routinely exceed the −1.0R (0.70%) stop on the 1-minute path. Combined with 27–46% **immediate-fail** rates, the issue is **entry timing, not stop width**. Tightening the stop would only raise imm-fail; widening it would worsen losers. *Don't touch the stop — fix the entry.*
- **MFE:** winners reach mfe_R **1.3–2.2** (B_HUGE_C1 NEUTRAL 1.87). The 1.50% target = ~2.14R at a 0.70% stop, which the **good** trades reach. So the **target is not too ambitious for qualified trades** — it's the *unqualified* trades dragging PF, not the exit.
- **Setup-specific exits:** not needed yet. The 1.50 target suits all three (continuation/reclaim that runs). A **VWAP-failure stop** for B_AVWAP (exit if price loses VWAP again) is worth testing *after* the vwap_dist fix, as a second-order improvement. A **time-stop** is *not* indicated (winners take time to reach MFE; cutting early would hurt mfe_R 1.8+ runners).

---

## 9. Anti-overfit warnings

1. **Sample size.** B_HUGE_C1 and B_HUGE_RED test sets are **11 and 13 trades**. Any TEST PF on these is high-variance — treat as directional, not conclusive.
2. **B_HUGE_C1's NEUTRAL edge** rests on 25 train trades; the regime story is mechanically sound but **must be re-confirmed on more history** before sizing.
3. **Drop the 6-decimal `pre_entry_momentum_score ≤ 64.7678`** — a classic data-mined threshold with no distributional support here.
4. **Do not stack** the aggressive filters; each added condition on a <60-trade base is a fresh overfit surface.
5. Prefer the **B_AVWAP vwap_dist fix** above all — it's the one change with a monotonic, mechanical, large-sample basis.

---

## 10. Final recommended next-experiment list (priority order)

1. **[must-test] B_AVWAP:** `vwap_dist_atr ≤ 0.75` (replace the inverted `≥0.60`).
2. **[must-test] B_HUGE_C1:** `regime != BULL` (replace the no-op rs mask).
3. **[must-test] B_AVWAP:** add `vol_ratio ≤ 2.5` on top of #1.
4. [optional] B_AVWAP: `atr_pct ≤ 0.010`.
5. [optional] B_HUGE_C1: `atr_pct ≤ 0.009`.
6. [only-if-sample-expands] B_HUGE_RED: `vol_ratio ∈ [1.4,2.2] AND market_ret ≤ −0.20`.

Validate each via walk-forward + the day-clustered bootstrap (the strict gate), not a single train/test split, before any production change.

---

## 11. Exact config/code changes proposed (candidates only — do NOT auto-apply)

In `avwap_5min_ID_v11_backtesting.py`, `_selected_strategy_mask`, the B rules currently:
```python
# B_AVWAP_RECLAIM_REVERSAL (max_pnl): vwap_dist_atr >= MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR (0.60)
# B_HUGE_C1_CLOSE_RECLAIM_BREAK (ab_filtered_relaxed): rs_pct <= 10.7
```
Proposed candidate replacements (test first):
```python
# B_AVWAP: require a NEAR-VWAP reclaim, not an extended one (INVERTED from 0.60):
#   setup.eq("B_AVWAP_RECLAIM_REVERSAL") & (vwap_dist_atr <= 0.75) & (vol_ratio <= 2.5)
# B_HUGE_C1: gate on regime, drop the rs no-op:
#   setup.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK") & (regime != "BULL")
```
No production file is modified by this study. Apply only after gate validation.

---

## 12. What NOT to change yet
- **Do not** add a `regime == BULL/TREND` requirement to B_AVWAP — TREND is its *worst* regime (PF 0.45).
- **Do not** require `regime == BEAR` for B_HUGE_RED — BEAR is its *worst* regime (−₹7,271).
- **Do not** tighten or widen the 0.70/1.50 exit — the leverage is in entry filtering.
- **Do not** keep the 6-decimal momentum threshold.
- **Do not** productionize B_HUGE_RED — it needs a core-logic redesign, not a filter.

---

## 13. What data is insufficient
- **Previous-candle-size and gap-up/down context:** the pre-dedupe pool does not carry prev-bar OHLC, so within-`huge_prev` bucketing and gap analysis could not be done. Requires re-extracting from the 5-minute store. (TODO in `validate_B_setups_filters.py`.)
- **Borrow/MIS shortability** for B_HUGE_RED — not in the data; a real live-short constraint.
- **Test samples (11–13)** are too small for confident OOS PF on B_HUGE_C1/RED.

---

## 14. Validation plan
1. Run `validate_B_setups_filters.py` (done) → baseline + bucket + filter-experiment CSVs.
2. For the top-3 must-test filters, run a **purged walk-forward + day-clustered bootstrap** (the strict gate) over the full Nov→now history, not one split.
3. Accept a candidate only if: OOS net PF ≥ 1.3, day-block p < 0.10, OOS/IS PF ratio ≥ 0.55, ≥ 30 OOS trades.
4. Re-extract prev-bar/gap features and re-run the B_HUGE bucketing before any B_HUGE decision.
5. Only then encode the survivor(s) into the v11 mask and re-validate end-to-end.
