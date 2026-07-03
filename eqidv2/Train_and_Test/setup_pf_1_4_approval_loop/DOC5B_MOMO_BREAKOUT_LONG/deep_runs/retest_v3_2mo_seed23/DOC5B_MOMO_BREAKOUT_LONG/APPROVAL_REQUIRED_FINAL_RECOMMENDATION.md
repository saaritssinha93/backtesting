# DOC5B_MOMO_BREAKOUT_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 0.8
  },
  "mask_terms": [
    [
      "breakout_strength_atr",
      "<=",
      0.774244
    ],
    [
      "ranker_score",
      ">=",
      112.177566
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "09:30",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=24 PF=0.527 net=Rs-5,898 win%=50.0 avgW=Rs547 avgL=Rs-1,039 maxDD=Rs-4,358 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.71 tradeDom=0.086 dayDom=9.99 symDom=9.99 dbp=0.8707 | n=24 PF=0.527 net=Rs-5,898 win%=50.0 avgW=Rs547 avgL=Rs-1,039 maxDD=Rs-4,358 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.71 tradeDom=0.086 dayDom=9.99 symDom=9.99 dbp=0.8707 |
| TEST  | n=7 PF=0.434 net=Rs-2,937 win%=57.1 avgW=Rs564 avgL=Rs-1,730 maxDD=Rs-4,630 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.937 | n=7 PF=0.434 net=Rs-2,937 win%=57.1 avgW=Rs564 avgL=Rs-1,730 maxDD=Rs-4,630 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.937 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test/doc5_long_setups/pool_retest_v3_2mo --trials 700 --time_budget_min 10.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 7 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.251 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).