# APPROVAL_REQUIRED_FINAL_RECOMMENDATION - L_RS_LEADER_VWAP_HOLD (LONG)

DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES

## Recommendation
NO - do not promote.

No second-pass staged sweep candidate cleared the approval gate.

## Best Meaningful Near-Miss
- n=39 PF=0.9645 net=Rs-598 win=48.72% t/s/e=9/13/17 dom=0.076/9.99/9.99
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", "<=", 21.924144]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- Failure: TRAIN PF too low

## Final Config File Requiring Approval
- `final_setup_conf.py`
- `Train_and_Test/final_setup_conf.py` mirror only after explicit approval

## Rerun Commands
```powershell
py -3.12 Train_and_Test\setup_loop_runner.py --setup L_RS_LEADER_VWAP_HOLD --pool Train_and_Test\setup_pf_1_4_approval_loop\L_RS_LEADER_VWAP_HOLD\pool --configs Train_and_Test\setup_pf_1_4_approval_loop\L_RS_LEADER_VWAP_HOLD\baseline_config.json --train_start 2026-03-16 --train_end 2026-05-13 --test_start 2026-05-14 --test_end 2026-05-27 --slippage_bps 15

py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\L_RS_LEADER_VWAP_HOLD\scripts\parameter_sweep_again.py --pool Train_and_Test\setup_pf_1_4_approval_loop\L_RS_LEADER_VWAP_HOLD\pool --max_iterations 100 --slippage_bps 15
```
