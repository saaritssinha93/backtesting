# A_MOD_BREAK_C1_HIGH (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-02._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 0.8
  },
  "mask_terms": [
    [
      "atr_pct",
      "<=",
      0.003086
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre5_mom_r",
      "<=",
      0.171743
    ],
    [
      "sig5_vol_ratio20",
      "<=",
      1.735451
    ]
  ],
  "entry_guards": {
    "top_n": 1
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=89 PF=0.293 net=Rs-31,727 win%=33.7 avgW=Rs438 avgL=Rs-760 maxDD=Rs-32,105 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.043 dayDom=9.99 symDom=9.99 dbp=1.0 | n=89 PF=0.293 net=Rs-31,727 win%=33.7 avgW=Rs438 avgL=Rs-760 maxDD=Rs-32,105 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.043 dayDom=9.99 symDom=9.99 dbp=1.0 |
| TEST  | n=59 PF=0.255 net=Rs-23,225 win%=30.5 avgW=Rs441 avgL=Rs-760 maxDD=Rs-25,357 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.47 tradeDom=0.071 dayDom=9.99 symDom=9.99 dbp=1.0 | n=59 PF=0.255 net=Rs-23,225 win%=30.5 avgW=Rs441 avgL=Rs-760 maxDD=Rs-25,357 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.47 tradeDom=0.071 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_full --trials 700 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 59 trades / 17 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.071 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).