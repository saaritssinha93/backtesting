# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6); TRAIN PF above preferred band (>1.70))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "close_loc",
      ">=",
      0.929858
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      31.269015
    ]
  ],
  "entry_guards": {
    "min_slot": "09:30",
    "max_slot": "12:00",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=21 PF=1.779 net=Rs6,433 win%=47.6 avgW=Rs1,469 avgL=Rs-750 maxDD=Rs-4,874 SL/TGT/EOD=2/6/13 tgt%=28.6 tpd=2.1 tradeDom=0.154 dayDom=0.58 symDom=0.352 dbp=0.153 | n=21 PF=2.188 net=Rs8,510 win%=47.6 avgW=Rs1,567 avgL=Rs-651 maxDD=Rs-3,779 SL/TGT/EOD=2/6/13 tgt%=28.6 tpd=2.1 tradeDom=0.151 dayDom=0.474 symDom=0.278 dbp=0.0896 |
| TEST  | n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=None dayDom=None symDom=None dbp=None | n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=None dayDom=None symDom=None dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_C1_CLOSE_RECLAIM_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_C1_CLOSE_RECLAIM_BREAK --trials 700 --time_budget_min 10.0 --seed 47 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 0 trades / 0 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day None, top-symbol None, top-trade None (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).