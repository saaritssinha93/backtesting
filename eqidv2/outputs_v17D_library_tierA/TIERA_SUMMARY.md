# v17D Step 2.10 — Library Tier-A backtest summary

Run date: 2026-05-13
Universe: 36 F&O tickers (intersection of stub `fo_list.json` ∩ ADV ≥ Rs 50 cr ∩ Rs 50–5000 price band)
Window: 229 trading days (2025-06-02 → 2026-05-05) from `stocks_indicators_5min_eq_live2`
Cost tier: A (lab — 0.16% TGT / 0.19% SL, no realistic slippage)
Total trades scanned: **232,431** across 38 (side, setup) combos

---

## Tier-A drop-fast gate

Per the roadmap (line 494–496):
> Drop if PF < 1.30 or n < 30 over 60d.

| Threshold | Pass count |
|---|---|
| PF ≥ 1.50 | **1 / 38** |
| PF ≥ 1.30 | **1 / 38** |
| n ≥ 30 | 33 / 38 |
| Both | **1 / 38** |

The single survivor: **SHORT TC_4_TRENDDAY_VWAP_PULLBACK** (PF 1.518, n=17 — fails n≥30 floor).

**Verdict: 0 of 20 detectors clear Tier-A.**

---

## Why every detector failed

Win rates are uniformly 5–30% across all 38 combos (vs Cand-E4's 73%). The detectors fire 100–200×/day per ticker for the loudest setups:

| Worst over-firing | per-day (across 36 tickers) |
|---|---|
| SHORT MR_2_VWAP_FADE | 214.2 |
| LONG MR_2_VWAP_FADE | 160.4 |
| LONG TC_1_PULLBACK_EMA20 | 107.5 |
| SHORT TC_1_PULLBACK_EMA20 | 96.3 |
| LONG VO_3_OBV_DIVERGENCE | 65.5 |

These are **detection without filtering**. The trigger conditions (e.g. "price touches Lower BB + RSI ≤ 20") fire constantly and most fires don't predict a profitable move. The roadmap's Step 2.0 Pareto search is supposed to attach per-detector filter chains (ADX gates, sector RS, multi-TF stacks) to lift PF — but with raw PF 0.4–0.6, it'd take aggressive filtering to clear PF 1.50, and that would crush the count budget.

## Honest read

The 20-detector library, as specified, **does not add net edge**. Specifically:
- Mean-reversion family (MR_1–4): firing in trend regimes; ADX-gate is critical and missing
- Trend-continuation family (TC_1–4): every pullback fires; ADX/multi-TF stack required
- Pattern family (PT_1–3): generic SL/TGT (0.75%/0.80%) doesn't match these setup types' MAE/MFE distributions
- Volume family (VO_1–3): OBV-divergence detector is too eager; fires 65×/day per ticker
- Time-of-day family (TD_1–2): TD_2 LATE_DAY_REVERSAL is the worst (PF 0.21–0.24)

This is exactly what the roadmap predicts in the "wide net then filter" funnel:
> "Dropping a setup is success, not failure. Graveyard list should be longer than production list."

Tier-A's job is to **discard non-edge candidates fast at low cost** — it did its job.

---

## Recommended actions

1. **Move all 20 detectors to graveyard** with reason `NO_EDGE` or `OPERATIONAL` (over-firing). Document in `eqidv2/SETUP_GRAVEYARD.md`.
2. **Re-spec a smaller, tighter library** (5–8 candidates) where each detector includes built-in regime gates (ADX threshold, sector RS sign, multi-TF alignment) BEFORE the Pareto search. The current detectors push all filtering to Step 2.0, but with PF starting at 0.5, no filter combo can lift to 1.50 without crushing count.
3. **Pivot to Cand-E4 refinement** instead of expansion — the existing 6 setups already have lab PF 1.7–2.5 individually. More leverage from:
   - Universe tightening (Phase 1.1) → +PF on PESSIMISTIC bucket
   - MAE/MFE SL/TGT picks (Phase 3.1) → +Rs return, cost-shock resilience
   - Drop SHORT G_LOWER_LOW_BREAK (only fragile setup per Phase 0.2 perturbation)
4. **Library re-attempt later** — once Phase 1+3 ship, revisit a smaller library v2 with regime gates built in. Don't burn weeks on the current 20.

Outputs:
- [v17D_library_trades_tierA_20260513_104729.csv](v17D_library_trades_tierA_20260513_104729.csv) (232,431 rows)
- [v17D_library_summary_tierA_20260513_104729.csv](v17D_library_summary_tierA_20260513_104729.csv)
