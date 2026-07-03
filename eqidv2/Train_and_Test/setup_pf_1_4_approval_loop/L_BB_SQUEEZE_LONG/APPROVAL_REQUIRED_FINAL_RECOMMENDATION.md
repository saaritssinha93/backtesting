# L_BB_SQUEEZE_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "vol_ratio",
      ">=",
      3.627005
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=181 PF=0.647 net=Rs-43,303 win%=42.5 avgW=Rs1,030 avgL=Rs-1,179 maxDD=Rs-46,487 SL/TGT/EOD=84/57/40 tpd=3.55 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=0.9882 | n=181 PF=0.775 net=Rs-25,301 win%=44.2 avgW=Rs1,090 avgL=Rs-1,114 maxDD=Rs-30,122 SL/TGT/EOD=84/57/40 tpd=3.55 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=0.9085 |
| TEST  | n=7 PF=1.82 net=Rs2,276 win%=57.1 avgW=Rs1,263 avgL=Rs-926 maxDD=Rs-1,332 SL/TGT/EOD=2/4/1 tpd=1.75 tradeDom=0.251 dayDom=0.556 symDom=0.556 dbp=0.2554 | n=7 PF=2.2 net=Rs2,977 win%=57.1 avgW=Rs1,365 avgL=Rs-827 maxDD=Rs-1,233 SL/TGT/EOD=2/4/1 tpd=1.75 tradeDom=0.251 dayDom=0.506 symDom=0.459 dbp=0.0515 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_BB_SQUEEZE_LONG --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_BB_SQUEEZE_LONG --trials 200 --time_budget_min 8.0 --seed 7
```

## Remaining risks

- TEST sample = 7 trades / 4 day(s) (thin June data).
- TEST concentration: top-day 0.556, top-symbol 0.556, top-trade 0.251 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).