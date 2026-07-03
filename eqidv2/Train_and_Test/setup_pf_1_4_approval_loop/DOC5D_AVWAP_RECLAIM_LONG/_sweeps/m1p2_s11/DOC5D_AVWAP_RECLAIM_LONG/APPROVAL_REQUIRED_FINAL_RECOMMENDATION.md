# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 0.8
  },
  "mask_terms": [
    [
      "ranker_score",
      ">=",
      84.792783
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "12:30",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=13 PF=0.769 net=Rs-1,225 win%=61.5 avgW=Rs510 avgL=Rs-1,062 maxDD=Rs-4,097 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.139 dayDom=9.99 symDom=9.99 dbp=0.6442 | n=13 PF=0.769 net=Rs-1,225 win%=61.5 avgW=Rs510 avgL=Rs-1,062 maxDD=Rs-4,097 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.139 dayDom=9.99 symDom=9.99 dbp=0.6442 |
| TEST  | n=2 PF=0.423 net=Rs-766 win%=50.0 avgW=Rs561 avgL=Rs-1,327 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None | n=2 PF=0.423 net=Rs-766 win%=50.0 avgW=Rs561 avgL=Rs-1,327 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 10.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 2 trades / 2 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 1.0 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).