# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.25
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre5_mom_r",
      ">=",
      1.580359
    ]
  ],
  "entry_guards": {
    "min_slot": "10:30",
    "top_n": 3
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=15 PF=1.073 net=Rs549 win%=53.3 avgW=Rs1,013 avgL=Rs-1,079 maxDD=Rs-4,309 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.67 tradeDom=0.125 dayDom=3.695 symDom=1.85 dbp=0.4541 | n=15 PF=1.073 net=Rs549 win%=53.3 avgW=Rs1,013 avgL=Rs-1,079 maxDD=Rs-4,309 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.67 tradeDom=0.125 dayDom=3.695 symDom=1.85 dbp=0.4541 |
| TEST  | n=3 PF=0.47 net=Rs-1,144 win%=33.3 avgW=Rs1,014 avgL=Rs-1,079 maxDD=Rs-1,082 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None | n=3 PF=0.47 net=Rs-1,144 win%=33.3 avgW=Rs1,014 avgL=Rs-1,079 maxDD=Rs-1,082 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool C:/TradingData/eqidv2/outputs_ID_v11_conf_fresh_20260629 --trials 250 --time_budget_min 14.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 3 trades / 2 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 1.0 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).