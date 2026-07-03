# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "close_loc",
      ">=",
      0.894691
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_vol_ratio20",
      ">=",
      2.650909
    ]
  ],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=16 PF=1.086 net=Rs513 win%=50.0 avgW=Rs806 avgL=Rs-742 maxDD=Rs-3,180 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.6 tradeDom=0.196 dayDom=3.381 symDom=2.466 dbp=0.4413 | n=16 PF=1.086 net=Rs513 win%=50.0 avgW=Rs806 avgL=Rs-742 maxDD=Rs-3,180 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.6 tradeDom=0.196 dayDom=3.381 symDom=2.466 dbp=0.4413 |
| TEST  | n=3 PF=0.0 net=Rs-2,576 win%=0.0 avgW=Rs0 avgL=Rs-859 maxDD=Rs-1,798 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 | n=3 PF=0.0 net=Rs-2,576 win%=0.0 avgW=Rs0 avgL=Rs-859 maxDD=Rs-1,798 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 10.0 --seed 41 --gap_lambda 0.4 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 3 trades / 3 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).