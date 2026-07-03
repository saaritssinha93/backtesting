# DOC5B_MOMO_BREAKOUT_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 1.0
  },
  "mask_terms": [
    [
      "ranker_score",
      "<=",
      72.064624
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "12:30"
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=41 PF=0.743 net=Rs-3,854 win%=48.8 avgW=Rs557 avgL=Rs-714 maxDD=Rs-5,870 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.16 tradeDom=0.069 dayDom=9.99 symDom=9.99 dbp=0.8099 | n=41 PF=0.743 net=Rs-3,854 win%=48.8 avgW=Rs557 avgL=Rs-714 maxDD=Rs-5,870 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.16 tradeDom=0.069 dayDom=9.99 symDom=9.99 dbp=0.8099 |
| TEST  | n=21 PF=0.553 net=Rs-5,483 win%=52.4 avgW=Rs617 avgL=Rs-1,228 maxDD=Rs-8,542 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.62 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.8973 | n=21 PF=0.553 net=Rs-5,483 win%=52.4 avgW=Rs617 avgL=Rs-1,228 maxDD=Rs-8,542 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.62 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.8973 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 21 trades / 13 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.113 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).