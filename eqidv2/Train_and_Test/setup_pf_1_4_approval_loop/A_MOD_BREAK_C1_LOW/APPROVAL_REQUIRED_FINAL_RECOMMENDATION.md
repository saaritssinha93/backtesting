# A_MOD_BREAK_C1_LOW (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TEST PF below 1.40; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 2.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre_entry_momentum_score",
      ">=",
      38.397393
    ],
    [
      "pre3_close_pos",
      "<=",
      0.300039
    ]
  ],
  "entry_guards": {
    "max_slot": "13:00",
    "top_n": 3
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=58 PF=1.36 net=Rs7,119 win%=53.4 avgW=Rs867 avgL=Rs-732 maxDD=Rs-4,546 SL/TGT/EOD=19/4/35 tpd=4.83 tradeDom=0.084 dayDom=0.498 symDom=0.318 dbp=0.1884 | n=58 PF=1.747 net=Rs12,838 win%=56.9 avgW=Rs910 avgL=Rs-688 maxDD=Rs-4,056 SL/TGT/EOD=19/4/35 tpd=4.83 tradeDom=0.079 dayDom=0.315 symDom=0.184 dbp=0.0662 |
| TEST  | n=13 PF=0.542 net=Rs-2,713 win%=38.5 avgW=Rs642 avgL=Rs-740 maxDD=Rs-2,061 SL/TGT/EOD=6/0/7 tpd=2.6 tradeDom=0.347 dayDom=9.99 symDom=9.99 dbp=0.9886 | n=13 PF=0.718 net=Rs-1,455 win%=38.5 avgW=Rs740 avgL=Rs-644 maxDD=Rs-1,411 SL/TGT/EOD=6/0/7 tpd=2.6 tradeDom=0.327 dayDom=9.99 symDom=9.99 dbp=0.8737 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_LOW --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\A_MOD_BREAK_C1_LOW --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 13 trades / 5 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.347 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).