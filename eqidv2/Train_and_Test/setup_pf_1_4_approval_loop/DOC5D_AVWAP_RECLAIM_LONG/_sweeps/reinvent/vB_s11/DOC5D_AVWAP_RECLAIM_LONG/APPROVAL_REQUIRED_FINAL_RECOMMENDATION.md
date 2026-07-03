# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    [
      "wick_skew_pct",
      ">=",
      0.04464
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre1_adx",
      "<=",
      18.768368
    ]
  ],
  "entry_guards": {},
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=20 PF=1.687 net=Rs6,050 win%=45.0 avgW=Rs1,650 avgL=Rs-800 maxDD=Rs-4,765 SL/TGT/EOD=10/7/3 tgt%=35.0 tpd=1.67 tradeDom=0.126 dayDom=0.917 symDom=0.308 dbp=0.225 | n=20 PF=1.687 net=Rs6,050 win%=45.0 avgW=Rs1,650 avgL=Rs-800 maxDD=Rs-4,765 SL/TGT/EOD=10/7/3 tgt%=35.0 tpd=1.67 tradeDom=0.126 dayDom=0.917 symDom=0.308 dbp=0.225 |
| TEST  | n=2 PF=0.0 net=Rs-982 win%=0.0 avgW=Rs0 avgL=Rs-491 maxDD=Rs-829 SL/TGT/EOD=1/0/1 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None | n=2 PF=0.0 net=Rs-982 win%=0.0 avgW=Rs0 avgL=Rs-491 maxDD=Rs-829 SL/TGT/EOD=1/0/1 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vB --trials 400 --time_budget_min 5.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 2 trades / 2 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).