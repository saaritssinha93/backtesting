# DOC5B_MOMO_BREAKOUT_LONG (LONG) - BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline Definition

- No config of record exists in `final_setup_conf.py`; this setup is a doc-derived research detector.
- Baseline used here = raw DOC5B detections, no mask terms, no pre-momentum terms, no entry guards.
- Doc-default exit bracket from `Train_and_Test/doc5_long_setups/scan_doc5_long_setups.py`: SL 0.85% / target 1.50%.
- Portfolio overlay: max positions 20, daily loss stop off.
- Costs/execution: repo `setup_train_test.py` path, 15 bps/leg slippage, next 1-min open entry.

## Sessions

- TEST = calendar sessions >= 2026-06-20; available completed TEST sessions: 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-29, 2026-06-30.
- TRAIN = 2026-05-18..2026-06-19 (22 sessions).
- FIT = 2026-05-18..2026-06-03 (11 sessions).
- VAL = 2026-06-04..2026-06-19 (11 sessions).

## Baseline Metrics

| window | raw entries | trades | PF | net PnL | win% | trades/day | day-block p |
|---|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | 434 | 422 | 0.299 | Rs-196,547 | 25.4 | 19.18 | 1.0000 |
| TEST | 133 | 126 | 0.242 | Rs-71,896 | 23.0 | 21.00 | 0.9966 |

## Initial Diagnosis

- Baseline is deeply negative on both TRAIN and TEST, with too many trades/day and no sign of a profitable raw edge.
- The optimizer therefore needed to find a compact subset, but the best FIT/VAL pocket still confirmed far below the TRAIN PF 1.30 floor.
