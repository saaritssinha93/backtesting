# L_PRESSURE_BURST_VWAP — Baseline

**Side:** LONG  **Status:** PARKED 2026-06-29 (WEAK / USER_APPROVED_OVERRIDE; failed anti-overfit checks).
**Processed:** 2026-06-29.  **Faithfulness:** readmit basis → loop-faithful (live research layer also blocks L*).

## Split (printed)
- TRAIN = 2026-04-13 .. 2026-05-25; TEST = 2026-05-26 .. 2026-06-24.
- Pool: per-setup slice of `outputs_ID_v11_unified_pool` (26,001 raw rows). Net of v6 cost; 5 + 15 bps/leg.

## Conf gate of record
- exit SL 0.70 / Tgt 1.25
- mask_terms: `quality_score ≤ 25.0` (counterintuitive: selects LOW scanner quality)
- pre_momentum_terms: `pre1_adx ≥ 44.0` (very high pre-entry ADX; doc notes this is NON-monotonic)

## Baseline result (conf gate, fresh window) — heavy loser, LARGE sample

| Slippage | Period | n | net PF | net Rs | win% | T/SL/EOD% |
|---|---|---:|---:|---:|---:|---:|
| 15 bps/leg | TRAIN | 138 | **0.51** | -33,520 | 33.3 | 21/45/34 |
| 15 bps/leg | TEST  | 67 | **0.39** | -20,173 | 28.4 | 15/45/40 |

Reference — **ungated raw**: TRAIN PF 0.32 (n=1781, −Rs772k), TEST 0.28 (n=1028). The conf gate (0.51) barely
improves on ungated (0.32) → the gate adds little; the underlying setup is a structural loser.

## Observation
- **Worst setup in the audit at realistic cost** — and on a LARGE sample (n=138 train), so it is not noise: this
  is a genuine, robust loser (PF 0.51 train / 0.39 test @15 bps).
- 45% SL rate; losers spread across midday/afternoon (12:25, 13:31 train).
- The `quality_score≤25` mask (selecting LOW quality) and non-monotonic `pre1_adx≥44` gate were always speculative
  (doc: USER_APPROVED_OVERRIDE_WEAK, failed monotonic-sensitivity + multi-exit checks). The fresh window confirms it.

## Loss-mode analysis + iterations
See `L_PRESSURE_BURST_VWAP_experiment_log.md`.
