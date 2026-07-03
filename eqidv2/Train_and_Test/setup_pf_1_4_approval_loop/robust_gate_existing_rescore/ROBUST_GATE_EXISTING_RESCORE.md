# Robust Gate Existing-Run Re-score

_Generated 2026-07-01. Fast re-score only; no optimizer rerun; no final config edits._

This applies the cheap robust-gate pieces to the completed 12 setup runs:

- complexity cap: <=1 mask term and <=1 pre-momentum term
- TRAIN PF >= 1.30
- TRAIN target-fill >= 12%
- TEST PF >= 1.30 and day-block p <= 0.10 when TEST has enough trades
- TEST n < 6 is classified as INSUFFICIENT_OOS, not hard rejection
- neighborhood/dropout robustness still requires the slower full rerun

Verdict counts: `{"REJECT": 12}`

| setup | side | verdict | TRAIN n/PF/tgt% | TEST n/PF/p/tgt% | terms mask/pm | hard reasons | insufficient | warnings |
|---|---|---|---:|---:|---:|---|---|---|
| A_MOD_BREAK_C1_LOW | SHORT | REJECT | 58/1.36/6.9 | 13/0.542/0.9886/0.0 | 0/2 | TRAIN target-fill<12.0%; premom_terms>1; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| B_AVWAP_RECLAIM_REVERSAL | LONG | REJECT | 21/0.865/23.8 | 1/0.0/None/0.0 | 1/1 | TRAIN PF<1.3 | TEST n<6 | neighborhood/dropout FULL_RERUN_REQUIRED |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | LONG | REJECT | 104/0.595/9.6 | 18/0.741/0.8785/5.6 | 1/0 | TRAIN PF<1.3; TRAIN target-fill<12.0%; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| B_HUGE_RED_FAILED_BOUNCE | SHORT | REJECT | 161/0.674/23.0 | 42/0.378/1.0/16.7 | 0/0 | TRAIN PF<1.3; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| E_VWAP_LOSE_EARLY_SHORT | SHORT | REJECT | 21/1.155/0.0 | 11/0.457/1.0/0.0 | 2/0 | TRAIN PF<1.3; TRAIN target-fill<12.0%; mask_terms>1; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| G_HIGHER_HIGH_BREAK | LONG | REJECT | 18/1.424/50.0 | 11/0.613/0.9624/27.3 | 1/0 | TRAIN n<20; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| G_LOWER_LOW_BREAK | SHORT | REJECT | 55/1.733/41.8 | 5/0.387/None/20.0 | 2/1 | mask_terms>1 | TEST n<6 | neighborhood/dropout FULL_RERUN_REQUIRED |
| L_BB_SQUEEZE_LONG | LONG | REJECT | 181/0.647/31.5 | 7/1.82/0.2554/57.1 | 1/0 | TRAIN PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| L_DOUBLE_BOTTOM_VWAP | LONG | REJECT | 251/0.418/16.3 | 42/0.536/0.9279/21.4 | 0/0 | TRAIN PF<1.3; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| L_PRESSURE_BURST_VWAP | LONG | REJECT | 153/0.549/15.0 | 39/0.537/1.0/17.9 | 0/2 | TRAIN PF<1.3; premom_terms>1; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |
| MR_CONTROLLED_VWAP_EXTREME_FADE_LONG | LONG | REJECT | 14/1.826/64.3 | 5/0.0/1.0/0.0 | 1/0 | TRAIN n<20 | TEST n<6 | neighborhood/dropout FULL_RERUN_REQUIRED |
| MR_VWAP_EXTREME_RECLAIM_LONG | LONG | REJECT | 56/0.703/1.8 | 7/0.232/0.9624/0.0 | 0/1 | TRAIN PF<1.3; TRAIN target-fill<12.0%; TEST PF<1.3; TEST day-block p>0.1 | - | neighborhood/dropout FULL_RERUN_REQUIRED |

Interpretation:

- `REJECT`: failed a robust gate that can be assessed from existing completed runs.
- `INSUFFICIENT_OOS`: train-side evidence exists, but TEST is too thin to confirm/reject.
- `PENDING_FULL_RERUN`: cheap gates are clear, but neighborhood/dropout checks still need the full engine.
