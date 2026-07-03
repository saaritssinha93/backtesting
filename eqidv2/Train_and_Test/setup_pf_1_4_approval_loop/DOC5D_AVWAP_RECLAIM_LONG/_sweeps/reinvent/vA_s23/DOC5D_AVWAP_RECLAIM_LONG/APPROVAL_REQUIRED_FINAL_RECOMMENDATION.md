# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      20.8676
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=95 PF=0.99 net=Rs-469 win%=42.1 avgW=Rs1,134 avgL=Rs-833 maxDD=Rs-10,805 SL/TGT/EOD=33/11/51 tgt%=11.6 tpd=5.0 tradeDom=0.052 dayDom=9.99 symDom=9.99 dbp=0.5304 | n=95 PF=0.99 net=Rs-469 win%=42.1 avgW=Rs1,134 avgL=Rs-833 maxDD=Rs-10,805 SL/TGT/EOD=33/11/51 tgt%=11.6 tpd=5.0 tradeDom=0.052 dayDom=9.99 symDom=9.99 dbp=0.5304 |
| TEST  | n=30 PF=1.448 net=Rs4,847 win%=50.0 avgW=Rs1,044 avgL=Rs-721 maxDD=Rs-4,078 SL/TGT/EOD=7/4/19 tgt%=13.3 tpd=5.0 tradeDom=0.151 dayDom=0.687 symDom=0.488 dbp=0.1602 | n=30 PF=1.448 net=Rs4,847 win%=50.0 avgW=Rs1,044 avgL=Rs-721 maxDD=Rs-4,078 SL/TGT/EOD=7/4/19 tgt%=13.3 tpd=5.0 tradeDom=0.151 dayDom=0.687 symDom=0.488 dbp=0.1602 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vA --trials 400 --time_budget_min 5.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 30 trades / 6 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 0.687, top-symbol 0.488, top-trade 0.151 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).