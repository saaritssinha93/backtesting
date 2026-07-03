# L_DOUBLE_BOTTOM_VWAP — Baseline

**Side:** LONG  **Status:** PARKED 2026-06-29 (raw-pool caveat).  **Processed:** 2026-06-29.
**Faithfulness:** readmit basis → fast pool harness loop-faithful (but doc notes the live research layer blocks the
L* family — so even a good result needs the research-layer block lifted to ever trade live).

## Split (printed)
- TRAIN = 2026-04-13 .. 2026-05-25 (6 weeks); TEST = 2026-05-26 .. 2026-06-24.
- Pool: per-setup slice of `outputs_ID_v11_unified_pool` (5,686 raw rows). Net of v6 cost; 5 + 15 bps/leg.

## Conf gate of record
- exit SL 0.90 / Tgt 1.50
- mask_terms: none
- pre_momentum_terms (ALL, missing→block): `pre_entry_momentum_score ≥ 79.0`, `sig5_adx_calc ≥ 28.0`
- (alt G-style gate documented: `pre2_mom_r ≥ 0.42 & sig5_adx_calc ≥ 28`)

## Backtest-vs-live logic check
Bootstrap reads the conf gate verbatim, BUT doc §5.4 / provenance: the **live research layer blocks the L* family**,
so the conf-gate result is evaluated on the RAW pre-gate pool — to trade live, the research-layer block must be
lifted. This is a known live/backtest gating gap (recorded, not a port bug).

## Baseline result (conf gate, fresh window)

| Slippage | Period | n | net PF | net Rs | win% | T/SL/EOD% | day_block_p |
|---|---|---:|---:|---:|---:|---:|---:|
| 15 bps/leg | TRAIN | 38 | 0.88 | -2,378 | 44.7 | 34/42/24 | 0.68 |
| 15 bps/leg | TEST  | 29 | **0.29** | -16,892 | 27.6 | 14/**72**/14 | 1.00 |

(5 bps figures appended from the batch run.)

## Observation — structural SL failure on TEST
TEST is a disaster: **72% SL rate** (21 of 29 trades stopped), PF 0.29. The double-bottom-reclaim **longs got
walled by stops** in late-May/June 2026 — a weak/choppy regime for long reversals. This is a structural failure
(SL hits), not a cost artifact, so lower slippage won't rescue it. The doc's published train 2.55 / test 3.57 is
**not reproduced** (and was a RAW-pool result with a known live-gating caveat). Losers cluster in the morning
(11:9, 12:9 on test).

## Loss-mode analysis + iterations
See `L_DOUBLE_BOTTOM_VWAP_experiment_log.md`.
