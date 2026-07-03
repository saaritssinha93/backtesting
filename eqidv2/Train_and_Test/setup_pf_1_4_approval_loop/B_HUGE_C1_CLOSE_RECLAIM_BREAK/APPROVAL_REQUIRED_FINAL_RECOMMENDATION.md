# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "quality_score",
      "<=",
      49.69124
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "10:00",
    "top_n": 1
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=104 PF=0.595 net=Rs-24,980 win%=29.8 avgW=Rs1,186 avgL=Rs-846 maxDD=Rs-29,841 SL/TGT/EOD=38/10/56 tpd=7.43 tradeDom=0.062 dayDom=9.99 symDom=9.99 dbp=0.9322 | n=104 PF=0.732 net=Rs-14,646 win%=32.7 avgW=Rs1,179 avgL=Rs-782 maxDD=Rs-22,289 SL/TGT/EOD=38/10/56 tpd=7.43 tradeDom=0.059 dayDom=9.99 symDom=9.99 dbp=0.7987 |
| TEST  | n=18 PF=0.741 net=Rs-2,294 win%=38.9 avgW=Rs937 avgL=Rs-805 maxDD=Rs-2,933 SL/TGT/EOD=4/1/13 tpd=4.5 tradeDom=0.345 dayDom=9.99 symDom=9.99 dbp=0.8785 | n=18 PF=0.937 net=Rs-501 win%=50.0 avgW=Rs821 avgL=Rs-877 maxDD=Rs-2,560 SL/TGT/EOD=4/1/13 tpd=4.5 tradeDom=0.32 dayDom=9.99 symDom=9.99 dbp=0.6322 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_C1_CLOSE_RECLAIM_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_C1_CLOSE_RECLAIM_BREAK --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 18 trades / 4 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.345 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).