# B_AVWAP_RECLAIM_REVERSAL - Claude Config Exit Sweep

Claude's filters were kept fixed. Only `sl_pct` and `tgt_pct` were swept.
No edits were made to `final_setup_conf.py` or `Train_and_Test/final_setup_conf.py`.

## Fixed Filters

- `vwap_dist_atr <= 1.0`
- `vol_ratio >= 3.537825`
- `atr_pct <= 0.003921`
- `pre1_adx >= 30.675856`
- `pre5_mom_r >= 0.317166`
- entry guard: `max_slot = 14:00`
- `max_positions = 20`
- `daily_loss_rs = 0`

## Sweep

- Cost assumption: 5 bps/leg
- TRAIN: 2026-05-18 through 2026-06-16
- TEST: 2026-06-22 and 2026-06-24
- Grid size: 342 SL/target combinations
- Passing configs: 0

## Original Claude Exit

- Exit: SL 0.90%, target 3.00%
- TRAIN: 30 trades, PF 2.2789, net Rs 13,049
- TEST: 4 trades, PF 0.6027, net Rs -926
- Result: reject; TRAIN PF is above approval band and TEST is negative

## Best Exit-Only Match

This was the best match after requiring TRAIN PF in the 1.30-1.70 band and at
least 5 TEST trades.

- Exit: SL 0.55%, target 1.75%
- TRAIN: 37 trades, PF 1.3608, net Rs 4,476
- TEST: 6 trades, PF 0.5964, net Rs -1,093
- Result: reject; TEST PF is below 1.40 and net is negative

## Other Close Train-Band Variants

| SL | Target | TRAIN n | TRAIN PF | TRAIN net | TEST n | TEST PF | TEST net |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.60 | 1.75 | 36 | 1.5146 | Rs 6,068 | 6 | 0.5654 | Rs -1,242 |
| 0.65 | 1.75 | 36 | 1.4246 | Rs 5,323 | 6 | 0.5375 | Rs -1,390 |
| 0.55 | 2.00 | 37 | 1.4595 | Rs 5,700 | 6 | 0.5185 | Rs -1,304 |
| 0.55 | 2.25 | 37 | 1.5573 | Rs 6,913 | 6 | 0.5185 | Rs -1,304 |

## Conclusion

Exit tuning alone does not rescue Claude's filter stack. The best TRAIN-band
exits still fail out-of-sample, and the TEST window remains negative across the
best practical candidates.

Artifacts:

- `claude_exit_sweep_results.json`
- `claude_exit_sweep_grid.csv`
