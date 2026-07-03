# D_AVWAP_LOSE_REVERSAL Approval Required Final Recommendation

Approval recommendation: **NO**

- Previous approach failure: Prior evidence was a small-sample short-mine: the first gate looked strong on only 26 trades, but the deeper 82-trade mine collapsed to about train PF 1.06. The high-PF pockets were mostly down-market conditioned, which is a regime bet rather than a clean setup edge.
- Pool recreation: succeeded; pool rows 4064, entry rows 4063.
- Baseline TRAIN: n=1365 PF=0.8612 net=-84853.73
- Baseline TEST: n=550 PF=0.9186 net=-17755.33
- Best/selected TRAIN: not run
- Best/selected TEST: not run
- TEST PF crossed 1.40: False
- Candidate config path: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps\D_AVWAP_LOSE_REVERSAL\candidates\final_candidate_needing_approval.json`
- Final config needing approval: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps\D_AVWAP_LOSE_REVERSAL\candidates\final_candidate_needing_approval.json`

## Final Logic

- Entry logic: Short reversal when a stock that was above session VWAP loses VWAP on a strong down bar.
- Indicator values: `[['market_abs_ret_pct', '>=', 0.058395]]`
- Non-indicator rules: `{'max_slot': '11:30'}`
- Pre-momentum filters: `[['pre5_range_r', '>=', 0.161068]]`
- Filters and guards: `{'mask_terms': [['market_abs_ret_pct', '>=', 0.058395]], 'entry_guards': {'max_slot': '11:30'}}`
- SL/target/exit: SL 0.5%, target 3.0%, 1-minute SL/target/EOD resolver.

## Domination Check

- Not applicable; selected train-side candidate did not reach TEST eligibility.

## Remaining Risks

- Thin samples and day clustering remain binding risks unless the approval flag is YES.
- Any top_n guard must be verified in the live/conf path before promotion.
- No production files were edited.

## Rerun Commands

```powershell
python Train_and_Test\setup_recovery_full_loop\_shared\scripts\recover_d_setups.py --setups D_AVWAP_LOSE_REVERSAL --iterations 75 --scan-workers 1 --slippage-bps 0.0
```
