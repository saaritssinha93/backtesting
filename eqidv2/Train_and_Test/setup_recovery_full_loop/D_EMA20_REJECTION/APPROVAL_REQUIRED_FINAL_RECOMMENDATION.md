# D_EMA20_REJECTION Approval Required Final Recommendation

Approval recommendation: **NO**

- Previous approach failure: The setup-card says the pre-momentum gate is the edge. The later force-promoted Optuna version was explicitly screen-only/firehose-derived, with top_n not enforced by the live conf-mask path and a dominance warning. That is the failure mode to avoid.
- Pool recreation: succeeded; pool rows 1201, entry rows 1201.
- Baseline TRAIN: n=118 PF=0.6979 net=-14159.71
- Baseline TEST: n=20 PF=0.3719 net=-4792.93
- Best/selected TRAIN: n=21 PF=2.243 net=7188.94
- Best/selected TEST: not run
- TEST PF crossed 1.40: False
- Candidate config path: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop\D_EMA20_REJECTION\candidates\final_candidate_needing_approval.json`
- Final config needing approval: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop\D_EMA20_REJECTION\candidates\final_candidate_needing_approval.json`

## Final Logic

- Entry logic: Short trend-continuation rejection when a downtrend stack retests EMA20 and resumes lower.
- Indicator values: `[['ema20_dist_atr', '<=', -0.325336]]`
- Non-indicator rules: `{'max_slot': '12:30'}`
- Pre-momentum filters: `[]`
- Filters and guards: `{'mask_terms': [['ema20_dist_atr', '<=', -0.325336]], 'entry_guards': {'max_slot': '12:30'}}`
- SL/target/exit: SL 1.0%, target 3.0%, 1-minute SL/target/EOD resolver.

## Domination Check

- Not applicable; selected train-side candidate did not reach TEST eligibility.

## Remaining Risks

- Thin samples and day clustering remain binding risks unless the approval flag is YES.
- Any top_n guard must be verified in the live/conf path before promotion.
- No production files were edited.

## Rerun Commands

```powershell
python Train_and_Test\setup_recovery_full_loop\_shared\scripts\recover_d_setups.py --setups D_EMA20_REJECTION --iterations 75 --scan-workers 1 --slippage-bps 5.0
```
