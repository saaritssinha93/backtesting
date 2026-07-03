# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      15.935959
    ]
  ],
  "entry_guards": {
    "min_slot": "10:30",
    "max_slot": "14:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=20 PF=0.92 net=Rs-659 win%=45.0 avgW=Rs842 avgL=Rs-749 maxDD=Rs-3,042 SL/TGT/EOD=7/2/11 tgt%=10.0 tpd=2.5 tradeDom=0.246 dayDom=9.99 symDom=9.99 dbp=0.5994 | n=20 PF=0.92 net=Rs-659 win%=45.0 avgW=Rs842 avgL=Rs-749 maxDD=Rs-3,042 SL/TGT/EOD=7/2/11 tgt%=10.0 tpd=2.5 tradeDom=0.246 dayDom=9.99 symDom=9.99 dbp=0.5994 |
| TEST  | n=6 PF=1.825 net=Rs1,340 win%=50.0 avgW=Rs988 avgL=Rs-541 maxDD=Rs-982 SL/TGT/EOD=1/1/4 tgt%=16.7 tpd=1.5 tradeDom=0.63 dayDom=1.356 symDom=1.394 dbp=0.2968 | n=6 PF=1.825 net=Rs1,340 win%=50.0 avgW=Rs988 avgL=Rs-541 maxDD=Rs-982 SL/TGT/EOD=1/1/4 tgt%=16.7 tpd=1.5 tradeDom=0.63 dayDom=1.356 symDom=1.394 dbp=0.2968 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vB --trials 450 --time_budget_min 6.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 6 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 1.356, top-symbol 1.394, top-trade 0.63 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).