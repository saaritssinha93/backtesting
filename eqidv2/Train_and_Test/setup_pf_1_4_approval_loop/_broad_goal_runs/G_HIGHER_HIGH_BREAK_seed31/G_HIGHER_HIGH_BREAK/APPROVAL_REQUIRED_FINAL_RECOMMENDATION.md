# G_HIGHER_HIGH_BREAK (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "close_loc",
      "<=",
      0.953488
    ],
    [
      "vwap_dist_atr",
      "<=",
      3.401347
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_range_r",
      "<=",
      0.401292
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=20 PF=1.494 net=Rs3,671 win%=55.0 avgW=Rs1,009 avgL=Rs-825 maxDD=Rs-3,679 SL/TGT/EOD=2/8/10 tgt%=40.0 tpd=2.86 tradeDom=0.114 dayDom=0.682 symDom=0.345 dbp=0.1778 | n=20 PF=1.859 net=Rs5,619 win%=60.0 avgW=Rs1,014 avgL=Rs-818 maxDD=Rs-3,284 SL/TGT/EOD=2/8/10 tgt%=40.0 tpd=2.86 tradeDom=0.112 dayDom=0.517 symDom=0.243 dbp=0.0821 |
| TEST  | n=12 PF=1.194 net=Rs914 win%=50.0 avgW=Rs937 avgL=Rs-784 maxDD=Rs-3,239 SL/TGT/EOD=1/3/8 tgt%=25.0 tpd=3.0 tradeDom=0.225 dayDom=3.71 symDom=1.386 dbp=0.427 | n=12 PF=1.503 net=Rs2,115 win%=58.3 avgW=Rs903 avgL=Rs-842 maxDD=Rs-2,767 SL/TGT/EOD=1/3/8 tgt%=25.0 tpd=3.0 tradeDom=0.216 dayDom=1.842 symDom=0.647 dbp=0.2777 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_HIGHER_HIGH_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_HIGHER_HIGH_BREAK --trials 700 --time_budget_min 10.0 --seed 31 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 12 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 3.71, top-symbol 1.386, top-trade 0.225 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).