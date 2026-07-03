# D_EMA20_BOUNCE (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.0
  },
  "mask_terms": [
    [
      "body_pct",
      ">=",
      0.878969
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "14:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=19 PF=0.537 net=Rs-4,228 win%=42.1 avgW=Rs612 avgL=Rs-829 maxDD=Rs-4,993 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.9 tradeDom=0.157 dayDom=9.99 symDom=9.99 dbp=0.9383 | n=19 PF=0.537 net=Rs-4,228 win%=42.1 avgW=Rs612 avgL=Rs-829 maxDD=Rs-4,993 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.9 tradeDom=0.157 dayDom=9.99 symDom=9.99 dbp=0.9383 |
| TEST  | n=7 PF=0.323 net=Rs-2,963 win%=28.6 avgW=Rs708 avgL=Rs-876 maxDD=Rs-4,380 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.541 dayDom=9.99 symDom=9.99 dbp=0.9506 | n=7 PF=0.323 net=Rs-2,963 win%=28.6 avgW=Rs708 avgL=Rs-876 maxDD=Rs-4,380 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.541 dayDom=9.99 symDom=9.99 dbp=0.9506 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup D_EMA20_BOUNCE --pool C:/TradingData/eqidv2/outputs_ID_v11_conf_fresh_20260629 --trials 250 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 7 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.541 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).