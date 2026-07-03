# E_VWAP_LOSE_EARLY_SHORT (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "atr_pct",
      "<=",
      0.004977
    ],
    [
      "upper_wick_pct",
      "<=",
      0.043922
    ]
  ],
  "pre_momentum_terms": [],
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
| TRAIN | n=21 PF=1.155 net=Rs1,429 win%=52.4 avgW=Rs966 avgL=Rs-919 maxDD=Rs-2,775 SL/TGT/EOD=10/0/11 tpd=1.75 tradeDom=0.152 dayDom=1.128 symDom=1.128 dbp=0.3657 | n=21 PF=1.423 net=Rs3,469 win%=52.4 avgW=Rs1,061 avgL=Rs-820 maxDD=Rs-2,475 SL/TGT/EOD=10/0/11 tpd=1.75 tradeDom=0.147 dayDom=0.493 symDom=0.493 dbp=0.2138 |
| TEST  | n=11 PF=0.457 net=Rs-2,761 win%=45.5 avgW=Rs465 avgL=Rs-847 maxDD=Rs-3,222 SL/TGT/EOD=5/0/6 tpd=2.75 tradeDom=0.629 dayDom=9.99 symDom=9.99 dbp=1.0 | n=11 PF=0.623 net=Rs-1,688 win%=45.5 avgW=Rs559 avgL=Rs-747 maxDD=Rs-2,821 SL/TGT/EOD=5/0/6 tpd=2.75 tradeDom=0.558 dayDom=9.99 symDom=9.99 dbp=0.9483 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup E_VWAP_LOSE_EARLY_SHORT --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 11 trades / 4 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.629 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).