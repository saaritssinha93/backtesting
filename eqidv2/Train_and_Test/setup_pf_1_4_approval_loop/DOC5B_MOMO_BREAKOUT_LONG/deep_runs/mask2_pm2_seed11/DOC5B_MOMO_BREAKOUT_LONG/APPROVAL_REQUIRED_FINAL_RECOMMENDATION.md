# DOC5B_MOMO_BREAKOUT_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 2.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_vol_ratio20",
      "<=",
      1.77545
    ],
    [
      "pre1_adx",
      "<=",
      44.037386
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=34 PF=0.763 net=Rs-3,557 win%=41.2 avgW=Rs816 avgL=Rs-749 maxDD=Rs-5,487 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.27 tradeDom=0.198 dayDom=9.99 symDom=9.99 dbp=0.7144 | n=34 PF=0.763 net=Rs-3,557 win%=41.2 avgW=Rs816 avgL=Rs-749 maxDD=Rs-5,487 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.27 tradeDom=0.198 dayDom=9.99 symDom=9.99 dbp=0.7144 |
| TEST  | n=8 PF=0.662 net=Rs-1,517 win%=37.5 avgW=Rs989 avgL=Rs-897 maxDD=Rs-2,621 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.695 dayDom=9.99 symDom=9.99 dbp=0.798 | n=8 PF=0.662 net=Rs-1,517 win%=37.5 avgW=Rs989 avgL=Rs-897 maxDD=Rs-2,621 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.695 dayDom=9.99 symDom=9.99 dbp=0.798 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool --trials 600 --time_budget_min 12.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 8 trades / 4 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.695 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).