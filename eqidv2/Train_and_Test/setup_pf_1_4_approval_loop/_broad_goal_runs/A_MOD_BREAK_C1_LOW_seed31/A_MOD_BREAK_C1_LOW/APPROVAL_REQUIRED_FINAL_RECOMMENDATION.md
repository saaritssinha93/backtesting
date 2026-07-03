# A_MOD_BREAK_C1_LOW (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "body_pct",
      ">=",
      0.976744
    ],
    [
      "atr_pct",
      ">=",
      0.001522
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=43 PF=1.067 net=Rs1,251 win%=46.5 avgW=Rs992 avgL=Rs-808 maxDD=Rs-5,322 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=4.3 tradeDom=0.114 dayDom=3.37 symDom=1.808 dbp=0.4471 | n=43 PF=1.067 net=Rs1,251 win%=46.5 avgW=Rs992 avgL=Rs-808 maxDD=Rs-5,322 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=4.3 tradeDom=0.114 dayDom=3.37 symDom=1.808 dbp=0.4471 |
| TEST  | n=10 PF=0.002 net=Rs-8,996 win%=10.0 avgW=Rs22 avgL=Rs-1,002 maxDD=Rs-7,764 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0 | n=10 PF=0.002 net=Rs-8,996 win%=10.0 avgW=Rs22 avgL=Rs-1,002 maxDD=Rs-7,764 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_LOW --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\A_MOD_BREAK_C1_LOW --trials 700 --time_budget_min 10.0 --seed 31 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 10 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 1.0 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).