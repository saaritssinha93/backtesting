# G_HIGHER_HIGH_BREAK (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low / too few trades (train_n<20); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "close_loc",
      "<=",
      0.994667
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "12:30"
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=18 PF=1.424 net=Rs3,658 win%=61.1 avgW=Rs1,117 avgL=Rs-1,233 maxDD=Rs-3,812 SL/TGT/EOD=2/9/7 tpd=2.57 tradeDom=0.103 dayDom=1.188 symDom=0.346 dbp=0.2608 | n=18 PF=1.681 net=Rs5,406 win%=61.1 avgW=Rs1,214 avgL=Rs-1,135 maxDD=Rs-3,315 SL/TGT/EOD=2/9/7 tpd=2.57 tradeDom=0.102 dayDom=0.874 symDom=0.253 dbp=0.1669 |
| TEST  | n=11 PF=0.613 net=Rs-2,968 win%=45.5 avgW=Rs940 avgL=Rs-1,278 maxDD=Rs-3,751 SL/TGT/EOD=3/3/5 tpd=3.67 tradeDom=0.27 dayDom=9.99 symDom=9.99 dbp=0.9624 | n=11 PF=0.739 net=Rs-1,871 win%=54.5 avgW=Rs883 avgL=Rs-1,434 maxDD=Rs-3,054 SL/TGT/EOD=3/3/5 tpd=3.67 tradeDom=0.258 dayDom=9.99 symDom=9.99 dbp=0.7424 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_HIGHER_HIGH_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_HIGHER_HIGH_BREAK --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 11 trades / 3 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.27 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).