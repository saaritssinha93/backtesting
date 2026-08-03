# Structural Rework Result - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

Research-only. Generated from full `stocks_indicators_5min_eq_live2` bars through 2026-07-02.

## Status
- Structural pool rows: 15263
- Structural local-month distribution: 2026-03=3556, 2026-04=7940, 2026-05=3712, 2026-06=45, 2026-07=10
- Path validation: {'sampled': 282, 'matched': 279, 'match_rate': 0.9894, 'mismatches': [{'sid': 9482, 'ticker': 'NORTHARC', 'mine': 'SL', 'canonical': 'EOD'}, {'sid': 9109, 'ticker': 'PAISALO', 'mine': 'SL', 'canonical': 'TARGET'}, {'sid': 11976, 'ticker': 'AEQUS', 'mine': 'SL', 'canonical': 'TARGET'}]}
- Iterations: 520
- Passing approval-required candidates: 0

## Best TEST Rows
No candidate reached TEST because no structural variant reached the controlled TRAIN gate.

## Controlled TRAIN Rows
none

## Closest Structural Rows
- Best TRAIN PF row: TRAIN n=2139 PF=0.6098 net=Rs -622,415; FIT n=1332 PF=0.6658 net=Rs -319,022; VAL n=803 PF=0.5306 net=Rs -297,372.
- Best FIT PF row: FIT n=1236 PF=0.6735 net=Rs -302,907; VAL n=736 PF=0.5178 net=Rs -302,785; TRAIN n=1970 PF=0.6006 net=Rs -622,112.
- Conclusion: the structural rewrite did not miss the TRAIN PF > 1.30 requirement narrowly; it remained materially negative before TEST.

## Passing Candidates
No structural candidate passed TRAIN PF > 1.30 and TEST PF > 1.40 with positive PnL/stability gates.
