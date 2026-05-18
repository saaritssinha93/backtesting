# v17D Phase 0 Diligence — Summary

Run date: 2026-05-13
Input: `outputs_v17C_noNF_live_5min/avwap_longshort_trades_v16_5min_ALL_DAYS_20260505_080858_candE3.csv`
n=987 trades (canonical Cand-E4 spec, 6 setups: 4 LONG + 2 SHORT)

---

## Step 0.1 — Slippage stress test (sizing-aware)

Patched [v17D_slippage_stress_test.py](../../v17D_slippage_stress_test.py) to apply per-setup sizing multipliers (`candE_size_mult`) and use `pnl_pct_gross_price` so we don't double-subtract baseline costs already baked into `pnl_pct_price`.

| Scenario | T/S costs % | PF | win% | day_win% | sumPnL% | Rs |
|---|---|---|---|---|---|---|
| BASELINE | 0.160/0.190 | **1.941** | 73.4 | 71.7 | +1170.6 | +234,113 |
| OPTIMISTIC | 0.207/0.237 | 1.711 | 73.4 | 68.8 | +929.0 | +185,798 |
| **REALISTIC** | **0.307/0.337** | **1.289** | 73.4 | 62.0 | +416.9 | +83,388 |
| PESSIMISTIC | 0.507/0.537 | 0.645 | 73.4 | 45.4 | -607.2 | -121,432 |

**Verdict:** **YELLOW** — REALISTIC PF=1.289, just below the 1.30 RED gate; below the 1.50 GREEN gate.

Lab claim is PF=2.16 at baseline costs; we get 1.94 sizing-aware. Small remaining gap likely from rolling-cool/R:R-protection factors not modeled in the stress script. The bigger issue: PESSIMISTIC bucket (long-tail liquidity) goes to PF=0.645 — strategy fails on illiquid names.

**Implications:**
- Strategy edge is real but thin under realistic costs.
- Universe filter (Phase 1.1) becomes critical — must enforce ADV >= Rs 50 cr to keep pessimistic bucket out of production.
- Phase 1.2 cost model integration is mandatory before any live capital.
- Roadmap action under YELLOW: proceed at 0.3x pilot size (per Phase 0 exit decision in roadmap).

---

## Step 0.4 — MAE/MFE per-setup recommendations

All 6 setups want **wider** stops than current Cand-E4 picks:

| Side | Setup | n | Current SL/TGT | Recommended SL/TGT | R:R |
|---|---|---|---|---|---|
| LONG | A_MOD_BREAK_C1_HIGH | 183 | 0.75/0.80 | **0.91/1.02** | 1.12 |
| LONG | B_AVWAP_RECLAIM_REVERSAL | 327 | 0.75/0.80 | **0.93/1.00** | 1.09 |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 32 | 0.75/0.90 | **0.92/0.95** | 1.03 |
| LONG | D_EMA20_BOUNCE | 133 | 0.80/0.80 | **1.02/1.02** | 1.00 |
| SHORT | A_MOD_BREAK_C1_LOW | 61 | 0.80/0.85 | **0.88/0.90** | 1.02 |
| SHORT | G_LOWER_LOW_BREAK | 251 | 0.80/0.80 | **0.95/0.95** | 1.00 |

**Implications:**
- Current Cand-E4 SL is too tight on every setup — losers' MAE distributions extend further than the 0.75-0.80% stops, so trades that would eventually be winners are getting stopped out on noise.
- Wider stops + wider targets preserve R:R but should lift win rate (fewer noise stop-outs) at the cost of larger per-loss size.
- Net PF effect needs Phase 3.1 backtest to confirm (likely positive).
- B_HUGE_C1_CLOSE_RECLAIM_BREAK n=32 is too small to trust — flag for re-tuning after more data.

Output: `mae_mfe_v17C_candE4_summary.csv`

---

## Step 0.5 — Setup correlation audit

Pairwise (date, ticker, 5-min bar) Jaccard across all 6 setups: **max = 0.000**.

**Verdict:** **PASS** — every setup fires on a unique (date, ticker, bar). No double-counting risk.

Output: `correlation_v17C_candE4_canonical.csv` (15 setup pairs).

---

## Step 0.2 — Threshold perturbation (sensitivity)

Swept each numeric filter threshold by ±5/10/15% on the raw 2,107-trade pre-Cand-E CSV.

| Side | Setup | PF -10% | PF 0% | PF +10% | Robust |
|---|---|---|---|---|---|
| LONG | A_MOD_BREAK_C1_HIGH | 2.234 | 2.234 | 2.428 | YES |
| LONG | B_AVWAP_RECLAIM_REVERSAL | 2.108 | 2.108 | 2.178 | YES |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 2.449 | 2.449 | 5.581 | YES |
| LONG | D_EMA20_BOUNCE | 1.725 | 1.725 | 2.169 | YES |
| SHORT | A_MOD_BREAK_C1_LOW | 1.913 | 1.913 | 1.702 | YES |
| SHORT | G_LOWER_LOW_BREAK | 1.418 | 1.418 | 1.365 | NO |

**Verdict:** **PASS** — 5 of 6 setups robust (gate ≥ 4 of 6). G_LOWER_LOW_BREAK is the only fragile one (PF ~1.42, just under the 1.50 floor at all perturbations). Worth dropping or re-tuning.

Note: these are the RAW pre-cost / pre-leverage / pre-sizing PFs; values track the lab claim of 2.16 well.

Output: `perturbation_v17C_candE4.csv` + `perturbation_v17C_candE4_verdict.csv`

---

## Step 3.1 (preview) — Re-resolve trades with MAE/MFE SL/TGT

Re-walked 1-min bars on all 987 Cand-E4 trades using MAE/MFE-recommended wider stops/targets. Outcome mix: TARGET 67.1% (was 73.4%), SL 26.8%, EOD 6.1% — wider stops let more trades reach target without hitting SL on noise, but TGT is also wider so hit rate falls slightly.

**Stress test on re-resolved CSV (sizing-aware):**

| Scenario | Original SL/TGT PF | MAE/MFE SL/TGT PF | Δ PF | MAE/MFE sumRs |
|---|---|---|---|---|
| BASELINE | 1.941 | 1.806 | -0.135 | +261,781 |
| OPTIMISTIC | 1.711 | 1.629 | -0.082 | +213,466 |
| **REALISTIC** | **1.289** | **1.299** | **+0.010** | **+111,056** |
| PESSIMISTIC | 0.645 | 0.784 | +0.139 | -93,764 |

**Verdict:** marginal lift in REALISTIC PF (1.299 still YELLOW), but materially better Rs return at REALISTIC (+Rs 111k vs +Rs 83k = +33% more) and PESSIMISTIC PF lifts 0.65 → 0.78 (more cost-resilient). Wider stops are the right call from a Rs-PnL standpoint even though per-trade PF dilutes.

Output: `trades_reresolved_mae_mfe.csv` + `stress_reresolved_mae_mfe_SIZED.csv`

---

## Phase 0 + 3.1 (preview) overall verdict

| Step | Result | Gate | Status |
|---|---|---|---|
| 0.1 Slippage (current SL/TGT) | REALISTIC PF=1.289 | ≥ 1.30 | YELLOW |
| 0.1 Slippage (MAE/MFE SL/TGT) | REALISTIC PF=1.299 | ≥ 1.30 | YELLOW |
| 0.2 Threshold perturbation | 5/6 setups robust at ±10% | ≥ 4/6 | PASS |
| 0.4 MAE/MFE | 6 new SL/TGT picks | distributions complete | DONE |
| 0.5 Correlation | max J=0.000 | < 0.5 | PASS |

**Roadmap-mandated next step (per Phase 0 exit decision, line 405-407):**

YELLOW (PF in 1.20–1.30 band) → "proceed at 0.3x pilot size". Our REALISTIC PF stays at 1.289–1.299 either way (current vs MAE/MFE picks). Recommended path:

1. **Drop SHORT G_LOWER_LOW_BREAK** — only setup that fails Step 0.2 robustness (PF ~1.42 across perturbations). Trim it from the production set or rebuild with stricter gates.
2. **Tighten the universe** (Phase 1.1) so the PESSIMISTIC bucket is structurally excluded — this likely lifts the realized REALISTIC PF closer to OPTIMISTIC (1.629–1.711).
3. **Adopt MAE/MFE SL/TGT picks** for production — per-trade PF roughly flat but +33% more Rs/year and +0.14 PF on PESSIMISTIC bucket (cost-shock resilience).
4. **Phase 2.10 library Tier-A** to find new setups that lift count + diversification (running in background).

Skip Phase 0.3 (walk-forward) until after Phase 1.2 cost model is integrated — re-test per-setup gates end-to-end under realistic costs.
