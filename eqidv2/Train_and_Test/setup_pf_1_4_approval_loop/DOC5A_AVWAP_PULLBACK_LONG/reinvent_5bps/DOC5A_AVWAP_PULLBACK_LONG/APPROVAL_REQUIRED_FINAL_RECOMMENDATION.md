# DOC5A_AVWAP_PULLBACK_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "pullback_depth_atr",
      ">=",
      0.27969
    ],
    [
      "signal_range_pct",
      "<=",
      0.207383
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      "<=",
      0.25
    ]
  ],
  "entry_guards": {
    "max_slot": "12:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=13 PF=1.619 net=Rs2,306 win%=46.2 avgW=Rs1,005 avgL=Rs-532 maxDD=Rs-2,297 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.348 dayDom=1.159 symDom=0.909 dbp=0.2611 | n=13 PF=1.619 net=Rs2,306 win%=46.2 avgW=Rs1,005 avgL=Rs-532 maxDD=Rs-2,297 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.348 dayDom=1.159 symDom=0.909 dbp=0.2611 |
| TEST  | n=8 PF=0.002 net=Rs-7,410 win%=12.5 avgW=Rs12 avgL=Rs-1,060 maxDD=Rs-6,086 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.67 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0 | n=8 PF=0.002 net=Rs-7,410 win%=12.5 avgW=Rs12 avgL=Rs-1,060 maxDD=Rs-6,086 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.67 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5A_AVWAP_PULLBACK_LONG --pool C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_approval_loop\DOC5A_AVWAP_PULLBACK_LONG\variant_pool --trials 900 --time_budget_min 26.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 8 trades / 3 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 1.0 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).