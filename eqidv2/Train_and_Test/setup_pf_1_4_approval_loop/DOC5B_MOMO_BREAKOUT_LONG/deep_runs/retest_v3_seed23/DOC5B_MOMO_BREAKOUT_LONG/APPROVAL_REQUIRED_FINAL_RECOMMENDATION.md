# DOC5B_MOMO_BREAKOUT_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6); TRAIN PF above preferred band (>1.70))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 0.8
  },
  "mask_terms": [
    [
      "retest_depth_atr",
      ">=",
      0.350946
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      ">=",
      21.266639
    ]
  ],
  "entry_guards": {
    "min_slot": "10:00",
    "max_slot": "12:30"
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=12 PF=1.816 net=Rs2,384 win%=83.3 avgW=Rs531 avgL=Rs-1,461 maxDD=Rs-1,719 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.107 dayDom=0.713 symDom=0.238 dbp=0.1455 | n=12 PF=1.816 net=Rs2,384 win%=83.3 avgW=Rs531 avgL=Rs-1,461 maxDD=Rs-1,719 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.107 dayDom=0.713 symDom=0.238 dbp=0.1455 |
| TEST  | n=4 PF=0.0 net=Rs-5,857 win%=0.0 avgW=Rs0 avgL=Rs-1,464 maxDD=Rs-4,142 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None | n=4 PF=0.0 net=Rs-5,857 win%=0.0 avgW=Rs0 avgL=Rs-1,464 maxDD=Rs-4,142 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool_retest_v3 --trials 700 --time_budget_min 10.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 4 trades / 2 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).