# DOC5B_MOMO_BREAKOUT_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    [
      "quality_score",
      ">=",
      109.099605
    ],
    [
      "lower_wick_pct",
      ">=",
      0.022843
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre_entry_momentum_score",
      "<=",
      68.3712
    ],
    [
      "sig5_vol_ratio20",
      "<=",
      2.777304
    ]
  ],
  "entry_guards": {
    "max_slot": "13:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=29 PF=0.653 net=Rs-5,767 win%=41.4 avgW=Rs905 avgL=Rs-978 maxDD=Rs-6,157 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.23 tradeDom=0.163 dayDom=9.99 symDom=9.99 dbp=0.8792 | n=29 PF=0.653 net=Rs-5,767 win%=41.4 avgW=Rs905 avgL=Rs-978 maxDD=Rs-6,157 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.23 tradeDom=0.163 dayDom=9.99 symDom=9.99 dbp=0.8792 |
| TEST  | n=10 PF=0.329 net=Rs-7,157 win%=20.0 avgW=Rs1,758 avgL=Rs-1,334 maxDD=Rs-5,738 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.33 tradeDom=0.502 dayDom=9.99 symDom=9.99 dbp=1.0 | n=10 PF=0.329 net=Rs-7,157 win%=20.0 avgW=Rs1,758 avgL=Rs-1,334 maxDD=Rs-5,738 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.33 tradeDom=0.502 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool_rs_breadth_v2 --trials 800 --time_budget_min 12.0 --seed 17 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 10 trades / 3 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.502 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).