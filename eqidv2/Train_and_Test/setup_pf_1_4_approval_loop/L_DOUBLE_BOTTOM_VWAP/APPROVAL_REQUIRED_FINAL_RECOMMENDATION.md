# L_DOUBLE_BOTTOM_VWAP (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "14:30",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=251 PF=0.418 net=Rs-88,802 win%=31.1 avgW=Rs817 avgL=Rs-882 maxDD=Rs-92,535 SL/TGT/EOD=94/41/116 tpd=13.94 tradeDom=0.02 dayDom=9.99 symDom=9.99 dbp=0.9997 | n=251 PF=0.529 net=Rs-63,880 win%=32.3 avgW=Rs886 avgL=Rs-798 maxDD=Rs-68,462 SL/TGT/EOD=94/41/116 tpd=13.94 tradeDom=0.019 dayDom=9.99 symDom=9.99 dbp=0.995 |
| TEST  | n=42 PF=0.536 net=Rs-11,984 win%=35.7 avgW=Rs921 avgL=Rs-956 maxDD=Rs-15,975 SL/TGT/EOD=19/9/14 tpd=8.4 tradeDom=0.092 dayDom=9.99 symDom=9.99 dbp=0.9279 | n=42 PF=0.662 net=Rs-7,850 win%=40.5 avgW=Rs903 avgL=Rs-928 maxDD=Rs-14,335 SL/TGT/EOD=19/9/14 tpd=8.4 tradeDom=0.089 dayDom=9.99 symDom=9.99 dbp=0.815 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_DOUBLE_BOTTOM_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_DOUBLE_BOTTOM_VWAP --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 42 trades / 5 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.092 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).