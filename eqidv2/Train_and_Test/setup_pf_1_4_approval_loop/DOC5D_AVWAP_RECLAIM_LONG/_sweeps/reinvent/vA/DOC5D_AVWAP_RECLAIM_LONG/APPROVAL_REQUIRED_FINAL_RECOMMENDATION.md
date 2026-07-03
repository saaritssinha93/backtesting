# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "ranker_score",
      "<=",
      64.444809
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "10:30",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=38 PF=1.233 net=Rs3,926 win%=47.4 avgW=Rs1,152 avgL=Rs-841 maxDD=Rs-7,085 SL/TGT/EOD=11/6/21 tgt%=15.8 tpd=2.11 tradeDom=0.114 dayDom=1.391 symDom=0.602 dbp=0.3406 | n=38 PF=1.233 net=Rs3,926 win%=47.4 avgW=Rs1,152 avgL=Rs-841 maxDD=Rs-7,085 SL/TGT/EOD=11/6/21 tgt%=15.8 tpd=2.11 tradeDom=0.114 dayDom=1.391 symDom=0.602 dbp=0.3406 |
| TEST  | n=24 PF=0.728 net=Rs-3,116 win%=45.8 avgW=Rs758 avgL=Rs-881 maxDD=Rs-7,573 SL/TGT/EOD=7/2/15 tgt%=8.3 tpd=4.0 tradeDom=0.284 dayDom=9.99 symDom=9.99 dbp=0.6945 | n=24 PF=0.728 net=Rs-3,116 win%=45.8 avgW=Rs758 avgL=Rs-881 maxDD=Rs-7,573 SL/TGT/EOD=7/2/15 tgt%=8.3 tpd=4.0 tradeDom=0.284 dayDom=9.99 symDom=9.99 dbp=0.6945 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vA --trials 450 --time_budget_min 6.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 24 trades / 6 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.284 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).