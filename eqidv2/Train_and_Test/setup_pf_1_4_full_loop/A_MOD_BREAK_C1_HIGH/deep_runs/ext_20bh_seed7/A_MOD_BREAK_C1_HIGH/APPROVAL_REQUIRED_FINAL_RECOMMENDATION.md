# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "quality_score",
      ">=",
      67.940675
    ],
    [
      "bb_width_pct",
      ">=",
      1.402912
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      "<=",
      0.874992
    ]
  ],
  "entry_guards": {
    "min_slot": "10:30",
    "max_slot": "13:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=252 PF=0.528 net=Rs-95,201 win%=40.1 avgW=Rs1,055 avgL=Rs-1,336 maxDD=Rs-100,732 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.0 tradeDom=0.012 dayDom=9.99 symDom=9.99 dbp=0.9995 | n=252 PF=0.528 net=Rs-95,201 win%=40.1 avgW=Rs1,055 avgL=Rs-1,336 maxDD=Rs-100,732 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.0 tradeDom=0.012 dayDom=9.99 symDom=9.99 dbp=0.9995 |
| TEST  | n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None | n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_20bh --trials 600 --time_budget_min 14.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 0 trades / 0 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).