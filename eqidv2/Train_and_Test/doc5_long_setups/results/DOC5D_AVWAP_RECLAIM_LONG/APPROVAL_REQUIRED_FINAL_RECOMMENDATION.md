# DOC5D_AVWAP_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 1.25
  },
  "mask_terms": [
    [
      "ranker_score",
      ">=",
      87.752247
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre1_adx",
      ">=",
      16.925369
    ]
  ],
  "entry_guards": {
    "min_slot": "09:45",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=22 PF=1.252 net=Rs2,086 win%=59.1 avgW=Rs798 avgL=Rs-922 maxDD=Rs-1,950 SL/TGT/EOD=5/8/9 tgt%=36.4 tpd=1.47 tradeDom=0.098 dayDom=0.575 symDom=0.487 dbp=0.2765 | n=22 PF=1.578 net=Rs4,281 win%=59.1 avgW=Rs899 avgL=Rs-823 maxDD=Rs-1,655 SL/TGT/EOD=5/8/9 tgt%=36.4 tpd=1.47 tradeDom=0.096 dayDom=0.327 symDom=0.261 dbp=0.1148 |
| TEST  | n=8 PF=0.467 net=Rs-2,467 win%=37.5 avgW=Rs719 avgL=Rs-925 maxDD=Rs-3,306 SL/TGT/EOD=3/2/3 tgt%=25.0 tpd=1.0 tradeDom=0.471 dayDom=9.99 symDom=9.99 dbp=0.8288 | n=8 PF=0.592 net=Rs-1,695 win%=37.5 avgW=Rs818 avgL=Rs-830 maxDD=Rs-2,929 SL/TGT/EOD=3/2/3 tgt%=25.0 tpd=1.0 tradeDom=0.455 dayDom=9.99 symDom=9.99 dbp=0.7436 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 8 trades / 8 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.471 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).