# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    [
      "vol_ratio",
      ">=",
      3.244737
    ],
    [
      "wick_skew_pct",
      ">=",
      0.005197
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre_entry_momentum_score",
      ">=",
      63.297377
    ]
  ],
  "entry_guards": {
    "min_slot": "10:00",
    "max_slot": "13:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=21 PF=0.968 net=Rs-351 win%=28.6 avgW=Rs1,762 avgL=Rs-728 maxDD=Rs-7,272 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.167 dayDom=9.99 symDom=9.99 dbp=0.5336 | n=21 PF=0.968 net=Rs-351 win%=28.6 avgW=Rs1,762 avgL=Rs-728 maxDD=Rs-7,272 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.167 dayDom=9.99 symDom=9.99 dbp=0.5336 |
| TEST  | n=12 PF=0.0 net=Rs-8,746 win%=0.0 avgW=Rs0 avgL=Rs-729 maxDD=Rs-8,019 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 | n=12 PF=0.0 net=Rs-8,746 win%=0.0 avgW=Rs0 avgL=Rs-729 maxDD=Rs-8,019 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_am --trials 600 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 12 trades / 8 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).