# D_EMA20_REJECTION Approval Required Final Recommendation

Approval recommendation: **NO**

- Previous approach failure: The setup-card says the pre-momentum gate is the edge. The later force-promoted Optuna version was explicitly screen-only/firehose-derived, with top_n not enforced by the live conf-mask path and a dominance warning. That is the failure mode to avoid.
- Pool recreation: succeeded; pool rows 1201, entry rows 1201.
- Baseline TRAIN: n=118 PF=0.8733 net=-5338.4
- Baseline TEST: n=20 PF=0.585 net=-2430.37
- Best/selected TRAIN: n=119 PF=1.5413 net=20571.04
- Best/selected TEST: n=43 PF=0.7389 net=-4705.65
- TEST PF crossed 1.40: False
- Candidate config path: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps\D_EMA20_REJECTION\candidates\final_candidate_needing_approval.json`
- Final config needing approval: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps\D_EMA20_REJECTION\candidates\final_candidate_needing_approval.json`

## Final Logic

- Entry logic: Short trend-continuation rejection when a downtrend stack retests EMA20 and resumes lower.
- Indicator values: `[]`
- Non-indicator rules: `{'top_n': 1}`
- Pre-momentum filters: `[['pre10_close_pos', '>=', 0.9828]]`
- Filters and guards: `{'mask_terms': [], 'entry_guards': {'top_n': 1}}`
- SL/target/exit: SL 1.0%, target 2.5%, 1-minute SL/target/EOD resolver.

## Domination Check

- TEST top trade gross share: 0.1809
- TEST top day net share: None
- TEST top symbol net share: None

## Remaining Risks

- Thin samples and day clustering remain binding risks unless the approval flag is YES.
- Any top_n guard must be verified in the live/conf path before promotion.
- No production files were edited.

## Rerun Commands

```powershell
python Train_and_Test\setup_recovery_full_loop\_shared\scripts\recover_d_setups.py --setups D_EMA20_REJECTION --iterations 75 --scan-workers 1 --slippage-bps 0.0
```
