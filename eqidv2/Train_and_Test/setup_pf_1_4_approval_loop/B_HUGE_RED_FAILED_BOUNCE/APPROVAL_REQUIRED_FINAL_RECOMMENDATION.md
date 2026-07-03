# B_HUGE_RED_FAILED_BOUNCE (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {
    "min_slot": "10:30",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=161 PF=0.674 net=Rs-27,584 win%=40.4 avgW=Rs876 avgL=Rs-880 maxDD=Rs-31,499 SL/TGT/EOD=56/37/68 tpd=10.06 tradeDom=0.022 dayDom=9.99 symDom=9.99 dbp=0.985 | n=161 PF=0.845 net=Rs-11,645 win%=43.5 avgW=Rs908 avgL=Rs-826 maxDD=Rs-16,523 SL/TGT/EOD=56/37/68 tpd=10.06 tradeDom=0.021 dayDom=9.99 symDom=9.99 dbp=0.8174 |
| TEST  | n=42 PF=0.378 net=Rs-17,551 win%=26.2 avgW=Rs969 avgL=Rs-910 maxDD=Rs-19,475 SL/TGT/EOD=20/7/15 tpd=8.4 tradeDom=0.119 dayDom=9.99 symDom=9.99 dbp=1.0 | n=42 PF=0.467 net=Rs-13,395 win%=26.2 avgW=Rs1,067 avgL=Rs-811 maxDD=Rs-15,913 SL/TGT/EOD=20/7/15 tpd=8.4 tradeDom=0.116 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_RED_FAILED_BOUNCE --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 42 trades / 5 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.119 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).