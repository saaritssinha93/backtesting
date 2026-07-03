# L_PRESSURE_BURST_VWAP (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_vol_ratio20",
      ">=",
      3.84695
    ],
    [
      "pre3_close_pos",
      ">=",
      0.392861
    ]
  ],
  "entry_guards": {
    "max_slot": "13:00"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=153 PF=0.549 net=Rs-44,645 win%=31.4 avgW=Rs1,134 avgL=Rs-944 maxDD=Rs-48,783 SL/TGT/EOD=68/23/62 tpd=8.5 tradeDom=0.032 dayDom=9.99 symDom=9.99 dbp=0.9997 | n=153 PF=0.669 net=Rs-29,455 win%=34.6 avgW=Rs1,123 avgL=Rs-890 maxDD=Rs-34,997 SL/TGT/EOD=68/23/62 tpd=8.5 tradeDom=0.031 dayDom=9.99 symDom=9.99 dbp=0.984 |
| TEST  | n=39 PF=0.537 net=Rs-13,010 win%=30.8 avgW=Rs1,260 avgL=Rs-1,042 maxDD=Rs-12,096 SL/TGT/EOD=20/7/12 tpd=9.75 tradeDom=0.117 dayDom=9.99 symDom=9.99 dbp=1.0 | n=39 PF=0.642 net=Rs-9,140 win%=35.9 avgW=Rs1,172 avgL=Rs-1,022 maxDD=Rs-8,684 SL/TGT/EOD=20/7/12 tpd=9.75 tradeDom=0.114 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_PRESSURE_BURST_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_PRESSURE_BURST_VWAP --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 39 trades / 4 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.117 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).