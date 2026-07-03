# B_AVWAP_RECLAIM_REVERSAL (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "signal_range_pct",
      ">=",
      1.209043
    ],
    [
      "body_pct",
      "<=",
      0.9375
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      20.175403
    ]
  ],
  "entry_guards": {
    "min_slot": "10:30",
    "max_slot": "13:00",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=17 PF=1.203 net=Rs1,704 win%=47.1 avgW=Rs1,261 avgL=Rs-931 maxDD=Rs-2,455 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.7 tradeDom=0.126 dayDom=0.743 symDom=0.743 dbp=0.3004 | n=17 PF=1.203 net=Rs1,704 win%=47.1 avgW=Rs1,261 avgL=Rs-931 maxDD=Rs-2,455 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.7 tradeDom=0.126 dayDom=0.743 symDom=0.743 dbp=0.3004 |
| TEST  | n=2 PF=0.0 net=Rs-1,856 win%=0.0 avgW=Rs0 avgL=Rs-928 maxDD=Rs-930 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None | n=2 PF=0.0 net=Rs-1,856 win%=0.0 avgW=Rs0 avgL=Rs-928 maxDD=Rs-930 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --trials 700 --time_budget_min 10.0 --seed 41 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 2 trades / 1 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).