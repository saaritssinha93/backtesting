# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "vwap_dist_atr",
      ">=",
      1.027686
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "10:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=29 PF=1.329 net=Rs4,540 win%=34.5 avgW=Rs1,833 avgL=Rs-726 maxDD=Rs-5,735 SL/TGT/EOD=14/5/10 tgt%=17.2 tpd=1.81 tradeDom=0.129 dayDom=0.626 symDom=0.521 dbp=0.2666 | n=29 PF=1.329 net=Rs4,540 win%=34.5 avgW=Rs1,833 avgL=Rs-726 maxDD=Rs-5,735 SL/TGT/EOD=14/5/10 tgt%=17.2 tpd=1.81 tradeDom=0.129 dayDom=0.626 symDom=0.521 dbp=0.2666 |
| TEST  | n=9 PF=0.544 net=Rs-2,378 win%=22.2 avgW=Rs1,421 avgL=Rs-746 maxDD=Rs-2,488 SL/TGT/EOD=5/1/3 tgt%=11.1 tpd=2.25 tradeDom=0.832 dayDom=9.99 symDom=9.99 dbp=0.9483 | n=9 PF=0.544 net=Rs-2,378 win%=22.2 avgW=Rs1,421 avgL=Rs-746 maxDD=Rs-2,488 SL/TGT/EOD=5/1/3 tgt%=11.1 tpd=2.25 tradeDom=0.832 dayDom=9.99 symDom=9.99 dbp=0.9483 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vB --trials 400 --time_budget_min 5.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 9 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.832 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).