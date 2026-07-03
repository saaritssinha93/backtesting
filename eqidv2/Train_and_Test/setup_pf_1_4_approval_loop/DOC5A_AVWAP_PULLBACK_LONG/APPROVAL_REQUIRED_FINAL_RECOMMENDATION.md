# DOC5A_AVWAP_PULLBACK_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.25
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_range_r",
      ">=",
      0.308943
    ]
  ],
  "entry_guards": {
    "max_slot": "12:30",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=64 PF=0.428 net=Rs-22,294 win%=35.9 avgW=Rs726 avgL=Rs-951 maxDD=Rs-25,687 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.2 tradeDom=0.061 dayDom=9.99 symDom=9.99 dbp=0.9977 | n=64 PF=0.428 net=Rs-22,294 win%=35.9 avgW=Rs726 avgL=Rs-951 maxDD=Rs-25,687 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.2 tradeDom=0.061 dayDom=9.99 symDom=9.99 dbp=0.9977 |
| TEST  | n=12 PF=0.499 net=Rs-4,066 win%=33.3 avgW=Rs1,012 avgL=Rs-1,014 maxDD=Rs-5,457 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.4 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.9561 | n=12 PF=0.499 net=Rs-4,066 win%=33.3 avgW=Rs1,012 avgL=Rs-1,014 maxDD=Rs-5,457 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.4 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.9561 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5A_AVWAP_PULLBACK_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 700 --time_budget_min 22.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 12 trades / 5 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.251 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).