# G_LOWER_LOW_BREAK (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre_entry_momentum_score",
      "<=",
      46.220727
    ]
  ],
  "entry_guards": {
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=50 PF=1.077 net=Rs1,027 win%=56.0 avgW=Rs512 avgL=Rs-605 maxDD=Rs-4,253 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.054 dayDom=2.647 symDom=0.747 dbp=0.4037 | n=50 PF=1.077 net=Rs1,027 win%=56.0 avgW=Rs512 avgL=Rs-605 maxDD=Rs-4,253 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.054 dayDom=2.647 symDom=0.747 dbp=0.4037 |
| TEST  | n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None | n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_LOWER_LOW_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_LOWER_LOW_BREAK --trials 700 --time_budget_min 10.0 --seed 59 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 0 trades / 0 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).