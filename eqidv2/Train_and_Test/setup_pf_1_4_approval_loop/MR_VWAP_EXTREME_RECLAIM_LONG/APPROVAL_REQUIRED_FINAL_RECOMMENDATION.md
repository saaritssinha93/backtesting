# MR_VWAP_EXTREME_RECLAIM_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_rsi_dir",
      "<=",
      46.939049
    ]
  ],
  "entry_guards": {
    "min_slot": "09:30",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=56 PF=0.703 net=Rs-6,170 win%=41.1 avgW=Rs636 avgL=Rs-630 maxDD=Rs-9,885 SL/TGT/EOD=4/1/51 tpd=1.87 tradeDom=0.12 dayDom=9.99 symDom=9.99 dbp=0.8552 | n=56 PF=0.966 net=Rs-608 win%=46.4 avgW=Rs657 avgL=Rs-590 maxDD=Rs-7,904 SL/TGT/EOD=4/1/51 tpd=1.87 tradeDom=0.108 dayDom=9.99 symDom=9.99 dbp=0.544 |
| TEST  | n=7 PF=0.232 net=Rs-2,989 win%=28.6 avgW=Rs452 avgL=Rs-779 maxDD=Rs-2,505 SL/TGT/EOD=2/0/5 tpd=2.33 tradeDom=0.825 dayDom=9.99 symDom=9.99 dbp=0.9624 | n=7 PF=0.326 net=Rs-2,297 win%=42.9 avgW=Rs370 avgL=Rs-852 maxDD=Rs-2,116 SL/TGT/EOD=2/0/5 tpd=2.33 tradeDom=0.761 dayDom=9.99 symDom=9.99 dbp=0.9624 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup MR_VWAP_EXTREME_RECLAIM_LONG --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_VWAP_EXTREME_RECLAIM_LONG --trials 200 --time_budget_min 8.0 --seed 7
```

## Remaining risks

- TEST sample = 7 trades / 3 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.825 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).