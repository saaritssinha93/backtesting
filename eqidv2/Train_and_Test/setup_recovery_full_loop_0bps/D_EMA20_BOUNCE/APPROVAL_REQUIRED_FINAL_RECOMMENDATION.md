# D_EMA20_BOUNCE Approval Required Final Recommendation

Approval recommendation: **NO**

- Previous approach failure: This is not in the active conf book and appears as an overlay/leak candidate in the setup-card cross-check. The live survival audit saw only one recent leaked trade and it lost money. Older production-core filters were thin and not part of the current gate of record.
- Pool recreation: succeeded; pool rows 1228, entry rows 1228.
- Baseline TRAIN: n=799 PF=0.6783 net=-102814.2
- Baseline TEST: n=296 PF=0.731 net=-28691.63
- Best/selected TRAIN: not run
- Best/selected TEST: not run
- TEST PF crossed 1.40: False
- Candidate config path: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps\D_EMA20_BOUNCE\candidates\final_candidate_needing_approval.json`
- Final config needing approval: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps\D_EMA20_BOUNCE\candidates\final_candidate_needing_approval.json`

## Final Logic

- Entry logic: Long trend-continuation bounce when an uptrend stack retests EMA20 and closes back strong.
- Indicator values: `[]`
- Non-indicator rules: `{'max_slot': '11:30'}`
- Pre-momentum filters: `[]`
- Filters and guards: `{'mask_terms': [], 'entry_guards': {'max_slot': '11:30'}}`
- SL/target/exit: SL 0.7%, target 1.5%, 1-minute SL/target/EOD resolver.

## Domination Check

- Not applicable; selected train-side candidate did not reach TEST eligibility.

## Remaining Risks

- Thin samples and day clustering remain binding risks unless the approval flag is YES.
- Any top_n guard must be verified in the live/conf path before promotion.
- No production files were edited.

## Rerun Commands

```powershell
python Train_and_Test\setup_recovery_full_loop\_shared\scripts\recover_d_setups.py --setups D_EMA20_BOUNCE --iterations 75 --scan-workers 1 --slippage-bps 0.0
```
