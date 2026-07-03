# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.25
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "12:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=237 PF=0.757 net=Rs-27,286 win%=37.1 avgW=Rs965 avgL=Rs-753 maxDD=Rs-37,463 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.85 tradeDom=0.013 dayDom=9.99 symDom=9.99 dbp=0.9402 | n=237 PF=0.757 net=Rs-27,286 win%=37.1 avgW=Rs965 avgL=Rs-753 maxDD=Rs-37,463 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.85 tradeDom=0.013 dayDom=9.99 symDom=9.99 dbp=0.9402 |
| TEST  | n=66 PF=0.512 net=Rs-16,519 win%=27.3 avgW=Rs963 avgL=Rs-705 maxDD=Rs-20,160 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.0 tradeDom=0.064 dayDom=9.99 symDom=9.99 dbp=0.9702 | n=66 PF=0.512 net=Rs-16,519 win%=27.3 avgW=Rs963 avgL=Rs-705 maxDD=Rs-20,160 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=11.0 tradeDom=0.064 dayDom=9.99 symDom=9.99 dbp=0.9702 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vA --trials 400 --time_budget_min 5.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 66 trades / 6 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.064 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).