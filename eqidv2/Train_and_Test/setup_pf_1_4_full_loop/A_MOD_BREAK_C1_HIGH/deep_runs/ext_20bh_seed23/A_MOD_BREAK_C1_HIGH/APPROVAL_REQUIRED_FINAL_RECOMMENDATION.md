# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 2.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "09:45",
    "max_slot": "13:00"
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=1275 PF=0.539 net=Rs-448,920 win%=30.4 avgW=Rs1,359 avgL=Rs-1,098 maxDD=Rs-448,605 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=25.5 tradeDom=0.004 dayDom=9.99 symDom=9.99 dbp=1.0 | n=1275 PF=0.539 net=Rs-448,920 win%=30.4 avgW=Rs1,359 avgL=Rs-1,098 maxDD=Rs-448,605 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=25.5 tradeDom=0.004 dayDom=9.99 symDom=9.99 dbp=1.0 |
| TEST  | n=398 PF=0.395 net=Rs-200,953 win%=25.6 avgW=Rs1,287 avgL=Rs-1,123 maxDD=Rs-204,709 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=18.95 tradeDom=0.017 dayDom=9.99 symDom=9.99 dbp=1.0 | n=398 PF=0.395 net=Rs-200,953 win%=25.6 avgW=Rs1,287 avgL=Rs-1,123 maxDD=Rs-204,709 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=18.95 tradeDom=0.017 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_20bh --trials 600 --time_budget_min 14.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 398 trades / 21 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.017 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).