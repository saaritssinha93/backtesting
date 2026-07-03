# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      "<=",
      0.666632
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "13:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=48 PF=0.916 net=Rs-2,791 win%=52.1 avgW=Rs1,214 avgL=Rs-1,441 maxDD=Rs-8,966 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.78 tradeDom=0.042 dayDom=9.99 symDom=9.99 dbp=0.6239 | n=48 PF=0.916 net=Rs-2,791 win%=52.1 avgW=Rs1,214 avgL=Rs-1,441 maxDD=Rs-8,966 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.78 tradeDom=0.042 dayDom=9.99 symDom=9.99 dbp=0.6239 |
| TEST  | n=31 PF=0.208 net=Rs-26,801 win%=22.6 avgW=Rs1,004 avgL=Rs-1,409 maxDD=Rs-27,343 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.82 tradeDom=0.18 dayDom=9.99 symDom=9.99 dbp=0.9969 | n=31 PF=0.208 net=Rs-26,801 win%=22.6 avgW=Rs1,004 avgL=Rs-1,409 maxDD=Rs-27,343 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.82 tradeDom=0.18 dayDom=9.99 symDom=9.99 dbp=0.9969 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_morning --trials 500 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 31 trades / 11 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.18 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).