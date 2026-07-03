# DOC5C_ORB_GAP_GO_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "vol_ratio",
      ">=",
      2.637798
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_range_r",
      ">=",
      0.377377
    ]
  ],
  "entry_guards": {
    "max_slot": "12:30",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=15 PF=0.581 net=Rs-4,209 win%=33.3 avgW=Rs1,168 avgL=Rs-1,005 maxDD=Rs-5,466 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.217 dayDom=9.99 symDom=9.99 dbp=0.8108 | n=15 PF=0.581 net=Rs-4,209 win%=33.3 avgW=Rs1,168 avgL=Rs-1,005 maxDD=Rs-5,466 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.217 dayDom=9.99 symDom=9.99 dbp=0.8108 |
| TEST  | n=8 PF=0.258 net=Rs-4,253 win%=25.0 avgW=Rs739 avgL=Rs-955 maxDD=Rs-3,244 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=0.855 dayDom=9.99 symDom=9.99 dbp=0.9986 | n=8 PF=0.258 net=Rs-4,253 win%=25.0 avgW=Rs739 avgL=Rs-955 maxDD=Rs-3,244 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=0.855 dayDom=9.99 symDom=9.99 dbp=0.9986 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5C_ORB_GAP_GO_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 8 trades / 6 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.855 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).