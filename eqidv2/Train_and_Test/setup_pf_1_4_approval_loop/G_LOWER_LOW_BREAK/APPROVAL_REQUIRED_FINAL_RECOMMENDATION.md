# G_LOWER_LOW_BREAK (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (too few trades (test_n<6); TRAIN PF too high / overfit risk (>1.70); TEST PF below 1.40; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.0
  },
  "mask_terms": [
    [
      "vwap_dist_atr",
      "<=",
      -3.89879
    ],
    [
      "lower_wick_pct",
      ">=",
      0.0
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre5_mom_r",
      "<=",
      0.261893
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00"
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=55 PF=1.733 net=Rs8,989 win%=60.0 avgW=Rs644 avgL=Rs-557 maxDD=Rs-3,977 SL/TGT/EOD=4/23/28 tpd=4.58 tradeDom=0.036 dayDom=0.257 symDom=0.17 dbp=0.0001 | n=55 PF=2.416 net=Rs14,430 win%=63.6 avgW=Rs703 avgL=Rs-509 maxDD=Rs-3,049 SL/TGT/EOD=4/23/28 tpd=4.58 tradeDom=0.035 dayDom=0.201 symDom=0.119 dbp=0.0 |
| TEST  | n=5 PF=0.387 net=Rs-1,401 win%=40.0 avgW=Rs443 avgL=Rs-763 maxDD=Rs-2,164 SL/TGT/EOD=1/1/3 tpd=2.5 tradeDom=0.86 dayDom=9.99 symDom=9.99 dbp=None | n=5 PF=0.545 net=Rs-904 win%=40.0 avgW=Rs542 avgL=Rs-663 maxDD=Rs-1,963 SL/TGT/EOD=1/1/3 tpd=2.5 tradeDom=0.794 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_LOWER_LOW_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_LOWER_LOW_BREAK --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 5 trades / 2 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.86 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).