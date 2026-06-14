# L* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 days). Generated 2026-06-12.*

> **Pool note (important):** the gated clean pool **starves the L* family** — only
> L_BB_SQUEEZE_LONG survives it, with **n=1 test trade**. The production v8/research-layer
> gates remove the other L* setups entirely. So L* was searched/validated on the **RAW
> pre-gate candidate pool** (`historical_all_available_raw_candidates`) — the only place with
> a real test sample. **Any L* survivor must be reconciled with live gating before sizing.**
> Engines: `L_iterate.py` (aggressive search) + `L_validate_passer.py` (anti-overfit).

---

## 1. Executive summary

| Setup | raw n (tr/te) | UNGATED train/test | Verdict |
|---|---|---|---|
| **L_DOUBLE_BOTTOM_VWAP** (LONG) | 4113 / 1415 | 0.71 / 0.57 (−Rs143k) | **PROMOTE (gated)** — momentum/ADX gate → train 2.55 / test 3.57, p 0.033 |
| L_PRESSURE_BURST_VWAP (LONG) | 20138 / 4933 | 0.66 / 0.69 (−Rs180k) | reject — fragile 4-term overfit, no robust core |
| L_BB_SQUEEZE_LONG (LONG) | 503 / 55 | 0.69 / 0.72 (−Rs72k) | reject — 0.02-wide market band overfit + n=55 test |
| L_TREND_PULLBACK (LONG) | 723 / 163 | 0.56 / 0.41 (−Rs172k) | reject — 0 passers (train 6.5 → test 1.16) |

All four are heavy **raw losers** (which is why the production gates remove them). Only
L_DOUBLE_BOTTOM_VWAP yields a robust, anti-overfit-validated edge under a pre-momentum/ADX gate.

### 1.1 🟢 The keeper — L_DOUBLE_BOTTOM_VWAP × momentum/ADX gate
> **gate `pre_entry_momentum_score ≥ 79` AND `sig5_adx_calc ≥ 28`, exit 0.90/1.50**
> TRAIN **2.55** [h1 **2.40** / h2 **3.24**] · TEST **3.57** (n13, **p 0.033**) · full 2.80, **+Rs 22,623**
> **Monthly: all 8 months positive** (Nov 2.25, Dec 1.45, Jan 4.05, Feb 2.57, Mar 5.15, May 3.42)

Sibling gate `pre2_mom_r ≥ 0.42 & sig5_adx_calc ≥ 28` independently confirms it (train 2.25
[2.11/2.83], test 3.35, p 0.049) — the **same momentum/ADX mechanism that salvaged G**.

---

## 2. Setup definitions (raw detection)
- **L_DOUBLE_BOTTOM_VWAP** (LONG, `double_bottom_vwap_reclaim`, v2 L900): `abs(low − intraday_low_8) ≤ 0.40·ATR` (retest of the 8-bar low) & `close > VWAP` & `long_struct` (close>open, close_loc≥0.60) & `vol_ratio ≥ 1.5`. Exit 0.70/0.80 (prod).
- **L_PRESSURE_BURST_VWAP** (LONG, `buy_pressure_burst_vwap`, v2 L914): `pressure_ratio ≥ 3.0` & above VWAP & close>EMA20 & vol_ratio≥1.5 & 50≤RSI≤75. Exit 1.10/0.90.
- **L_TREND_PULLBACK** (LONG, `stacked_uptrend_ema20_pullback`, v2 L856): EMA20>EMA50>EMA200 & near_ema20 & long_struct & close>EMA20. Exit 0.70/0.90. *(On probation/blocked in the live discovery layer.)*
- **L_BB_SQUEEZE_LONG** (LONG, `bb_squeeze_upside_expansion`, v2 L759): squeeze & long_struct & close>upper_band·1.003 & vol_ratio≥2.0 & body_pct≥0.65. Exit 0.75/0.75. *(Only L* setup in the gated clean pool, n=1 test.)*

---

## 3. Baseline (production exit, RAW pool, NET)
| Setup | TRAIN n/PF | TEST n/PF/win | Net Rs |
|---|---|---|---|
| L_PRESSURE_BURST_VWAP | 800 / 0.66 | 450 / 0.69 / 47% | −180,265 |
| L_DOUBLE_BOTTOM_VWAP | 700 / 0.71 | 449 / 0.57 / 38% | −143,167 |
| L_TREND_PULLBACK | 723 / 0.56 | 162 / 0.41 / 29% | −171,659 |
| L_BB_SQUEEZE_LONG | 503 / 0.69 | 55 / 0.72 / 49% | −71,741 |

---

## 4. Diagnosis

### 4.1 L_DOUBLE_BOTTOM_VWAP — the keeper (gated)
Ungated it's a −Rs143k loser (dead-cat reclaims with no follow-through). The edge is entirely
in the **momentum/ADX gate**: a double-bottom reclaim only pays when the entry has a genuine
momentum thrust (`pre_entry_momentum_score ≥ 79`, top ~30%) AND a confirmed trend
(`sig5_adx_calc ≥ 28`). Sensitivity is a **contiguous, monotonic pocket** (test-positive across
mom[75,85] × adx[26,32]; PF rises as both tighten); **both train halves strong** (2.40/3.24);
**all 8 months positive**; **test-positive at every exit** (0.9/1.5, 0.9/1.25, 0.7/1.5). The
pre2_mom_r sibling gate confirms the mechanism (and it's the same edge G_HIGHER_HIGH_BREAK has).

### 4.2 L_PRESSURE_BURST_VWAP — reject (fragile 4-term overfit)
The only train-PF≥2 passers are 4-term masks on n=25. **Term drop-out test:** dropping
`regime==NEUTRAL` → train 2.27→0.99; dropping `vol_ratio≥4.69` → 1.15; dropping `pre10_mom_r`
→ train 1.48. The edge exists ONLY when all four stack — **no robust 2-term core** → overfit. No keeper.

### 4.3 L_BB_SQUEEZE_LONG — reject
The one "passer" is `market_ret_pct ∈ [−0.235, −0.017]` (a **0.02-wide** band) & pre1_adx≥32 —
a razor-thin market-return slice, not a mechanism. With only 55 raw test trades and masks cutting
test to n=1–2, nothing is estimable. Reject.

### 4.4 L_TREND_PULLBACK — reject
**0 honest passers.** Train-PF≥2 configs collapse OOS (greedy pre10_mom_r≥0.43 → train 6.47 /
test 1.16; others test 0.7–1.5, all p>0.10). Consistent with its live probation/block. No edge.

---

## 5. Pre-momentum ON/OFF
Only L_TREND_PULLBACK has a production gate (pre_entry_momentum_score≥73.021 & pre2_mom_r≥0.234);
it does not rescue it (premom-ON train 0.57 / test 0.36). For L_DOUBLE_BOTTOM the *searched*
momentum/ADX gate (not the production one) is the edge.

## 6. Anti-overfit notes
- L_DOUBLE_BOTTOM survives the full battery (sensitivity neighbourhood, both halves, all-months,
  multi-exit, sibling-feature confirmation, cross-setup mechanism match with G) — trustworthy
  despite n=33/13 and the wide search.
- L_PRESSURE_BURST and L_BB_SQUEEZE are textbook overfits (term-drop-out collapse; 0.02-wide band).

## 7. Recommended changes
- **PROMOTE L_DOUBLE_BOTTOM_VWAP** (gate `pre_entry_momentum_score≥79 & sig5_adx_calc≥28`, exit
  0.90/1.50) — STRONG PROBATION, **conditional on reconciling live gating** (it's gated out today).
- **Reject** L_PRESSURE_BURST_VWAP, L_BB_SQUEEZE_LONG, L_TREND_PULLBACK.

## 8. Rejects-improvement retry (robustness-first, 2026-06-12) — `L_rejects_improve.py`
User asked to push the 3 rejects to be selectable. New angle: **robustness-first** objective
(maximize min(train_h1, train_h2, test) PF) + forced momentum/ADX grids + wider exits.

| Setup | best robust config | train [h1/h2] | test | p | outcome |
|---|---|---|---|---|---|
| **L_PRESSURE_BURST_VWAP** | quality_score≤25 & pre1_adx≥44 (0.70/1.25) | 2.24 [2.59/2.03] | 2.03 (n12) | 0.086 | **promoted WEAK** (user-directed) |
| L_BB_SQUEEZE_LONG | regime==NEUTRAL & pre3_range_r≤0.887 (0.9/2.5) | 1.80 [1.85/1.69] | 1.73 | 0.121 | reject (not sig, n55) |
| L_TREND_PULLBACK | market_ret≥−0.286 & pre2_mom_r≥0.217 (0.5/2.5) | 1.49 [1.53/1.44] | 1.71 | 0.232 | reject (weak, not sig) |

- The **forced momentum/ADX mechanism** (which salvaged G & L_DOUBLE) **fails on all three** —
  train stays negative. So the L_DOUBLE edge does not generalise to these.
- **L_PRESSURE_BURST_VWAP** got materially better than its old fragile 4-term overfit (now a
  real all-period-positive 2-term gate), but it **FAILS the anti-overfit bar** that G/L_DOUBLE
  passed: test sensitivity is **non-monotonic** (pre1_adx 42→1.14, 44→2.03, 46→1.77), significant
  at **only 1 of 4 exits** (p0.086, multiple-testing-inflated), monthly **thin/lumpy** (Dec loser),
  and `quality_score≤25` selects LOW quality (counterintuitive). Promoted as **WEAK/CAUTION
  (USER_APPROVED_OVERRIDE_WEAK)** at user direction — speculative, do NOT size, re-validate on more data.
- **L_BB_SQUEEZE** & **L_TREND_PULLBACK** remain rejects — best configs all-period-positive but
  insignificant (p 0.12 / 0.23) and weak/thin. No durable edge.

## 9. Family scorecard
**L contributes 2 setups** — L_DOUBLE_BOTTOM_VWAP (STRONG, momentum/ADX-gated) and
L_PRESSURE_BURST_VWAP (WEAK/CAUTION, user-override). With G, L_DOUBLE_BOTTOM is the **third
independent confirmation** that a 5-min long pays only with a pre-entry momentum thrust + ADX
trend. **Caveat:** BOTH L setups were validated on the RAW pre-gate pool — reconcile with the
production v8/research gates before sizing; L_PRESSURE_BURST additionally fails the anti-overfit bar.
