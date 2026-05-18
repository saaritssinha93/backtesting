# Cand-E4 Refinement Results

Run date: 2026-05-13
Base: 987-trade canonical Cand-E4 (`avwap_..._20260505_080858_candE3.csv`)

---

## What was tried

Four refinements applied incrementally to the canonical Cand-E4 trades:

1. **Drop SHORT G_LOWER_LOW_BREAK** — failed Phase 0.2 robustness (PF ~1.42 across ±10% perturbation)
2. **Adopt MAE/MFE-recommended SL/TGT** — 6 wider stops/targets per Phase 0.4
3. **Tighten universe by ADV bucket** — three variants tested
4. **Re-stress** with sizing-aware cost model

---

## Stress-test matrix (sizing-aware, REALISTIC = ADV-bucketed Indian intraday costs)

| Variant | n | BASELINE PF | REALISTIC PF | PESSIMISTIC PF | sumRs REALISTIC | Verdict |
|---|---|---|---|---|---|---|
| Original Cand-E4 | 987 | 1.941 | 1.289 | 0.645 | +83,388 | YELLOW |
| + MAE/MFE SL/TGT picks | 987 | 1.806 | 1.299 | 0.784 | +111,056 | YELLOW |
| **+ drop G_LOWER_LOW_BREAK** | **736** | **1.832** | **1.323** | **0.803** | **+104,882** | **YELLOW (clears 1.30 gate)** |
| + ADV ≥ Rs 50 cr (top100+mid) | 230 | 1.750 | 1.259 | 0.761 | +27,226 | RED |
| + ADV ≥ Rs 500 cr (top100 only) | 22 | 2.561 | **1.864** | 1.144 | +6,715 | **GREEN** but n=0.1/day ✗ |

---

## Key findings

### 1. Drop G_LOWER + MAE/MFE picks crosses the RED → YELLOW boundary

REALISTIC PF lifts from **1.289 → 1.323** (above 1.30 gate). Sum-Rs at REALISTIC stays close to original (+105k vs +83k); PESSIMISTIC bucket gets materially more resilient (PF 0.65 → 0.80).

This is the **production-viable refinement** — clears the RED kill threshold cleanly and improves cost-shock robustness.

### 2. Top-100-only is GREEN but operationally non-viable

REALISTIC PF=1.864 looks great on paper but only 22 trades over 222 days = 0.10 trades/day. Roadmap floor is 4.5/day. Cand-E4 simply doesn't generate enough top-100 signals to operate at that universe size.

### 3. Mid+top100 filter HURT PF (counterintuitive)

Filtering out long_tail (< Rs 50 cr ADV) trades **dropped** PF from 1.323 to 1.259. The long_tail bucket contains disproportionate winners — the strategy's edge is partly in less-liquid names where price discovery is choppier.

This is a real warning: the v17D universe filter (Phase 1.1) at Rs 50 cr ADV floor would actively damage the strategy as currently calibrated.

### 4. The PESSIMISTIC bucket consistently kills the strategy

Across every variant, PESSIMISTIC (long_tail bucket cost = 0.51/0.54%) drops PF below 1.0 except at top-100. Realized cost will land between OPTIMISTIC and PESSIMISTIC depending on order size and time of day. Phase 1.2 cost model integration must accurately bucket each trade.

---

## Recommended production posture

**Adopt the "drop G_LOWER + MAE/MFE picks, keep all ADV" variant** as the new production Cand-E4 baseline:
- 5 active setups (4 LONG + 1 SHORT), no G_LOWER_LOW_BREAK
- Per-setup SL/TGT from MAE/MFE recommendations (wider stops):
  - LONG A_MOD_BREAK_C1_HIGH        SL 0.91 / TGT 1.02
  - LONG B_AVWAP_RECLAIM_REVERSAL   SL 0.93 / TGT 1.00
  - LONG B_HUGE_C1_CLOSE_RECLAIM_BR SL 0.92 / TGT 0.95
  - LONG D_EMA20_BOUNCE             SL 1.02 / TGT 1.02
  - SHORT A_MOD_BREAK_C1_LOW        SL 0.88 / TGT 0.90
- Universe: keep current (no Rs 50cr ADV floor — applying it now hurts edge)

Phase 0 exit decision per roadmap line 405–407: REALISTIC PF=1.323 → **YELLOW band**, proceed at **0.3x pilot size**.

---

## What's NOT solved

To clear the GREEN 1.50 gate without dropping count, one of these is needed:
1. **Add more high-quality setups** that fire on top-100 universe (Phase 2.10 library v2 — but library v1 just produced 0 survivors, so this needs a different design)
2. **Add per-setup ADX/sector-RS gates** to the Cand-E4 setups themselves (Step 2.0 Pareto search) — selectively cull losing trades on long_tail without losing winners
3. **Improve fills** via smarter order routing (limit orders instead of market) — outside backtest scope but real

The cleanest next chunk of work is **Step 2.0 Pareto search per setup** — applies the existing v17D infrastructure to the 5-setup refined Cand-E4 and tries to lift PF without crushing count.

---

## Files

- [trades_candE4_refined_alladv.csv](trades_candE4_refined_alladv.csv) — 736 trades, recommended production baseline
- [stress_refined_alladv.csv](stress_refined_alladv.csv) — 4-scenario stress on the recommended variant
- [trades_candE4_refined_top100mid.csv](trades_candE4_refined_top100mid.csv) — 230 trades, mid+top100 ADV
- [stress_refined_top100mid.csv](stress_refined_top100mid.csv)
- [trades_candE4_refined.csv](trades_candE4_refined.csv) — 22 trades, top-100 only
- [stress_refined_top100only.csv](stress_refined_top100only.csv)
