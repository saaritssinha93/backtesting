# B_HUGE_RED_FAILED_BOUNCE (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

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
      "signal_range_pct",
      "<=",
      0.523252
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre_entry_momentum_score",
      ">=",
      69.370728
    ]
  ],
  "entry_guards": {
    "min_slot": "10:00",
    "max_slot": "14:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=18 PF=1.113 net=Rs970 win%=44.4 avgW=Rs1,192 avgL=Rs-857 maxDD=Rs-4,379 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.64 tradeDom=0.238 dayDom=3.737 symDom=2.338 dbp=0.4466 | n=18 PF=1.113 net=Rs970 win%=44.4 avgW=Rs1,192 avgL=Rs-857 maxDD=Rs-4,379 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.64 tradeDom=0.238 dayDom=3.737 symDom=2.338 dbp=0.4466 |
| TEST  | n=6 PF=0.039 net=Rs-4,251 win%=33.3 avgW=Rs87 avgL=Rs-1,106 maxDD=Rs-3,149 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.739 dayDom=9.99 symDom=9.99 dbp=0.9624 | n=6 PF=0.039 net=Rs-4,251 win%=33.3 avgW=Rs87 avgL=Rs-1,106 maxDD=Rs-3,149 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.739 dayDom=9.99 symDom=9.99 dbp=0.9624 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_RED_FAILED_BOUNCE --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --trials 700 --time_budget_min 10.0 --seed 53 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 6 trades / 3 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.739 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).