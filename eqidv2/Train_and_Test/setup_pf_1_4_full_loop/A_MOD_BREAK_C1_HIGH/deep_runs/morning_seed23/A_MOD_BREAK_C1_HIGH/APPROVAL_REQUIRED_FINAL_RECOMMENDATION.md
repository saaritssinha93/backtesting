# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    [
      "vol_ratio",
      ">=",
      3.283571
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "12:30",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=23 PF=1.66 net=Rs8,474 win%=60.9 avgW=Rs1,522 avgL=Rs-1,426 maxDD=Rs-3,453 SL/TGT/EOD=5/11/7 tgt%=47.8 tpd=1.28 tradeDom=0.083 dayDom=0.304 symDom=0.208 dbp=0.0851 | n=23 PF=1.903 net=Rs10,784 win%=60.9 avgW=Rs1,624 avgL=Rs-1,327 maxDD=Rs-3,256 SL/TGT/EOD=5/11/7 tgt%=47.8 tpd=1.28 tradeDom=0.082 dayDom=0.257 symDom=0.173 dbp=0.0411 |
| TEST  | n=17 PF=0.277 net=Rs-15,311 win%=23.5 avgW=Rs1,469 avgL=Rs-1,630 maxDD=Rs-15,989 SL/TGT/EOD=11/2/4 tgt%=11.8 tpd=1.55 tradeDom=0.301 dayDom=9.99 symDom=9.99 dbp=0.9901 | n=17 PF=0.316 net=Rs-13,626 win%=23.5 avgW=Rs1,571 avgL=Rs-1,531 maxDD=Rs-14,805 SL/TGT/EOD=11/2/4 tgt%=11.8 tpd=1.55 tradeDom=0.297 dayDom=9.99 symDom=9.99 dbp=0.9837 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_morning --trials 500 --time_budget_min 12.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 17 trades / 11 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.301 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).