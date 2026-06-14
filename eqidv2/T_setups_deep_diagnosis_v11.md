# T*/MR* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-06 → 2026-05-19 (tier123 probe). Generated 2026-06-12.*

> The T*/MR* setups are **tier123_balanced OVERLAY setups** (research_v11_tier123_new_setups.py),
> admitted via the v11 overlay — they fire **zero candidates in the standard scanner pool**. Data
> for them lives in the tier123 probe (`outputs_ID_v11_tier123_new_setup_probe/tier123_standalone_trades.csv`).
> Four setups have populations. **Prior tier123 research had all four as HOLDOUT LOSERS** (PF 0.50–0.88).
> Engine: `T_iterate.py` (greedy + 2-term + **60k** random + forced momentum/ADX, robustness-first,
> day-block bootstrap + day-concentration on finalists). NET of cost.

---

## 1. Executive summary — T*/MR* contributes 1 setup

| Setup | n (tr/te) | ungated train/test | Verdict |
|---|---|---|---|
| **T_TREND_DAY_EMA_STAIR_SHORT** | 2498 (1200/212→8 gated) | 0.67 / 0.56 (−Rs180k) | **PROMOTE** — low-vol gate, train 2.53/test 4.49, p 0.056 |
| T_TREND_DAY_EMA_STAIR_LONG | 2498 (1200/120) | 0.63 / 0.43 (−Rs179k) | reject — fragile 4-term overfit |
| MR_CONTROLLED_VWAP_EXTREME_FADE_LONG | 287 (276/11) | 0.94 / 0.27 | reject — 11 test, day-concentrated |
| MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | 240 (228/12) | 0.61 / 0.58 | reject — 12 test, insignificant |

All four are heavy raw losers (consistent with the prior holdout-loser finding). Only the SHORT
trend-day-stair yields a clean, anti-overfit-validated edge.

### 1.1 🟢 The keeper — T_TREND_DAY_EMA_STAIR_SHORT × low-vol gate
> **`vol_ratio ≤ 1.33` AND `pre3_range_r ≥ 0.404`**, exit 0.90/0.80
> TRAIN **2.53** [h1 1.96 / h2 2.83] · TEST **4.49** (n8, **p 0.056**) · full 2.82, +Rs 11,946
> **top1day 42% (not day-concentrated) · 7 of 9 months positive**

Mechanism: a downtrend EMA20-stair continuation short only pays on **low volume** (controlled
drift-down; high volume = capitulation that bounces) with **adequate pre-entry range**. Tight 0.80
target scalps the continuation.

---

## 2. Setup definitions (tier123)
- **T_TREND_DAY_EMA_STAIR_SHORT/LONG** (Tier-2, `trend_day_ema20_stair_short/long`): downtrend
  (uptrend) stack EMA20<EMA50 (>), EMA20 falling (rising), price retests to within 0.25·ATR of EMA20
  and rejects, weak close below (above) VWAP, lagging (leading) the market, before 14:00. Probe exit 0.70/1.00.
- **MR_CONTROLLED_VWAP_EXTREME_FADE_LONG/SHORT** (Tier-3): controlled fade of a VWAP extreme. Probe exit 0.70/0.80.

---

## 3. T_TREND_DAY_EMA_STAIR_SHORT — the keeper
Ungated: train 0.67 / test 0.56, −Rs180k (heavy loser; prior holdout PF 0.60). The 2-term gate
`vol_ratio≤1.33 & pre3_range_r≥0.404` transforms it. Validation battery:
- **Sensitivity (NOT a knife-edge):** the pocket is monotonic — tighter vol (≤1.33) + higher
  pre3_range (≥0.4) both raise PF (vol≤1.25/pre3≥0.4 = 3.70/3.86; vol≤1.33/pre3≥0.4 = 2.58/2.27);
  loosening vol to 1.5–1.7 degrades to <1.0.
- **Exit-robust at tight targets:** 0.7/0.8 (p0.035), 0.9/0.8 (p0.056), 1.1/0.8 (p0.073) — the edge
  prefers a 0.80 target (scalp); wider targets weaken it (0.7/1.0 p0.132).
- **Both train halves positive** (1.96 / 2.83).
- **Day-spread good:** top1day 39–44% — NOT a single-day artifact (passes the S_MACD test).
- **7 of 9 months positive** across Jun-2025→May-2026 (losers Dec/Jan, smallest n=3 each).
- 2-term → low overfit dimensionality; mechanistically coherent.
- **Caveats:** test n=8 (the tier123 data ends 2026-05-19, so only ~19 days of May test); tier123
  OVERLAY / futures-universe provenance → must be reconciled with the standard pool before sizing.
**Verdict: PROMOTE as STRONG PROBATION** (with the overlay + thin-test caveats).

## 4. T_TREND_DAY_EMA_STAIR_LONG — reject (fragile 4-term)
Only passer: `quality_score≥85 & rsi≤59 & atr_pct≥0.0023 & adx≤36` (train 2.66/test 2.74, p0.100).
**Term drop-out is damning:** dropping quality_score → 0.73, rsi → 0.92, atr → 0.90. Three load-bearing
terms; no robust ≤2-term core (unlike the SHORT side). 4-term on n=28/13, p right at the 0.10 edge. Reject.

## 5. MR_CONTROLLED_VWAP_EXTREME_FADE_LONG / SHORT — reject
Only 11 / 12 test trades. LONG: train-PF-max configs collapse in test (0.27–0.97); robustness-first
"bests" are single-day artifacts (top1day 548–1396%). SHORT: 0 train-PF≥2; robustness-first best
(4-term, train 1.61/test 1.42) p0.266, top1day 105%. Both insignificant and day-concentrated. Reject.

## 6. Family scorecard
**T*/MR* contributes 1 setup** — T_TREND_DAY_EMA_STAIR_SHORT (low-vol gate, STRONG PROBATION,
tier123-overlay caveat). The day-concentration metric again did real work (killed the MR "bests").
This completes the A–T sweep.
