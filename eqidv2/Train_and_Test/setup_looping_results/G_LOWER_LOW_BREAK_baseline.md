# G_LOWER_LOW_BREAK — Baseline

**Side:** SHORT  **Status in source-of-truth:** ACTIVE (survival-book mined short, SELECTIVE).  **Processed:** 2026-06-29.
**Faithfulness:** readmit basis → fast pool harness live-faithful.

## Split (printed)
- TRAIN = 2026-04-13 .. 2026-05-25 (6 weeks); TEST = 2026-05-26 .. 2026-06-24.
- Pool: per-setup slice of `outputs_ID_v11_unified_pool` (2,418 raw rows total — the smallest active short).
- Net of v6 cost; reported at both 5 and 15 bps/leg.

## Conf gate of record (live today)
- exit SL 1.10 / Tgt 1.00
- mask_terms: `vol_ratio ≥ 4.129044` (≈4× volume climax) AND `quality_score ≥ 76.444124`
- pre_momentum_terms: `sig5_rsi_dir ≥ 68.747209` (missing→block)
- entry_guards: none

## Backtest-vs-live logic check
**No mismatch** (bootstrap-only, faithful port; not in the v11 overlay universe — doc §5.4).

## Baseline result (conf gate, fresh window) — **TOO FEW TRADES**

| Slippage | Period | n | net PF | net Rs | win% | day_block_p | top1day |
|---|---|---:|---:|---:|---:|---:|---:|
| 15 bps/leg | TRAIN | **6** | 3.42 | +1,480 | 66.7 | 0.147 | 0.81 |
| 15 bps/leg | TEST  | **6** | 0.75 | -510 | 33.3 | 0.581 | — |
| 5 bps/leg  | TRAIN | **6** | 10.27 | +2,645 | 83.3 | 0.033 | 0.78 |
| 5 bps/leg  | TEST  | **6** | 1.27 | +384 | 50.0 | 0.373 | 2.26 (one-day) |

## Observation
The `vol_ratio ≥ 4.1 & quality ≥ 76` mask is so selective that the fresh 10-week window yields only **6 train /
6 test** trades. No honest statistical read is possible at n=6 (the acceptance rules forbid tiny lucky samples).
The conf already flagged this (WEAK/SELECTIVE, test n=9, `sig5_rsi_dir` cliff). The ungated pool is a heavy loser
(TRAIN PF 0.49–0.83, TEST 0.61–1.00). To get a tradeable read, the mask must be loosened — see experiment log.

## Loss-mode analysis + iterations
See `G_LOWER_LOW_BREAK_experiment_log.md`.
