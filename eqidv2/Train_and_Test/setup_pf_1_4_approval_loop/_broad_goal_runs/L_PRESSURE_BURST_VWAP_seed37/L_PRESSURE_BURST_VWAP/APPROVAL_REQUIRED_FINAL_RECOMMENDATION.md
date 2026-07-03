# L_PRESSURE_BURST_VWAP (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_range_r",
      ">=",
      0.326861
    ],
    [
      "sig5_adx_calc",
      "<=",
      19.549259
    ]
  ],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=103 PF=0.528 net=Rs-28,832 win%=33.0 avgW=Rs947 avgL=Rs-884 maxDD=Rs-31,791 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.72 tradeDom=0.055 dayDom=9.99 symDom=9.99 dbp=0.982 | n=103 PF=0.528 net=Rs-28,832 win%=33.0 avgW=Rs947 avgL=Rs-884 maxDD=Rs-31,791 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.72 tradeDom=0.055 dayDom=9.99 symDom=9.99 dbp=0.982 |
| TEST  | n=29 PF=0.51 net=Rs-10,051 win%=24.1 avgW=Rs1,497 avgL=Rs-933 maxDD=Rs-14,909 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.8 tradeDom=0.168 dayDom=9.99 symDom=9.99 dbp=0.8569 | n=29 PF=0.51 net=Rs-10,051 win%=24.1 avgW=Rs1,497 avgL=Rs-933 maxDD=Rs-14,909 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.8 tradeDom=0.168 dayDom=9.99 symDom=9.99 dbp=0.8569 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_PRESSURE_BURST_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_PRESSURE_BURST_VWAP --trials 700 --time_budget_min 10.0 --seed 37 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 29 trades / 5 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.168 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).