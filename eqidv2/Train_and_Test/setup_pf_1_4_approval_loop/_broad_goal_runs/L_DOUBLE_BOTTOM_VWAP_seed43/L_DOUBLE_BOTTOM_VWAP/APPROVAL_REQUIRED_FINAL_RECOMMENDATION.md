# L_DOUBLE_BOTTOM_VWAP (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "close_loc",
      "<=",
      1.0
    ],
    [
      "vwap_dist_atr",
      "<=",
      0.180032
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "09:30",
    "max_slot": "13:00"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=34 PF=0.929 net=Rs-1,312 win%=38.2 avgW=Rs1,320 avgL=Rs-880 maxDD=Rs-5,270 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.132 dayDom=9.99 symDom=9.99 dbp=0.6483 | n=34 PF=0.929 net=Rs-1,312 win%=38.2 avgW=Rs1,320 avgL=Rs-880 maxDD=Rs-5,270 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.132 dayDom=9.99 symDom=9.99 dbp=0.6483 |
| TEST  | n=4 PF=0.0 net=Rs-4,094 win%=0.0 avgW=Rs0 avgL=Rs-1,024 maxDD=Rs-3,015 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 | n=4 PF=0.0 net=Rs-4,094 win%=0.0 avgW=Rs0 avgL=Rs-1,024 maxDD=Rs-3,015 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_DOUBLE_BOTTOM_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_DOUBLE_BOTTOM_VWAP --trials 700 --time_budget_min 10.0 --seed 43 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 4 trades / 3 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).