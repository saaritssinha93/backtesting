# MR_CONTROLLED_VWAP_EXTREME_FADE_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low / too few trades (train_n<20); too few trades (test_n<6); TRAIN PF too high / overfit risk (>1.70); TEST PF below 1.40; TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.0
  },
  "mask_terms": [
    [
      "vwap_dist_atr",
      ">=",
      -2.830013
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
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
| TRAIN | n=14 PF=1.826 net=Rs3,109 win%=64.3 avgW=Rs764 avgL=Rs-753 maxDD=Rs-971 SL/TGT/EOD=3/9/2 tpd=1.4 tradeDom=0.111 dayDom=0.492 symDom=0.247 dbp=0.0969 | n=14 PF=2.38 net=Rs4,512 win%=64.3 avgW=Rs865 avgL=Rs-654 maxDD=Rs-832 SL/TGT/EOD=3/9/2 tpd=1.4 tradeDom=0.111 dayDom=0.384 symDom=0.192 dbp=0.0354 |
| TEST  | n=5 PF=0.0 net=Rs-3,535 win%=0.0 avgW=Rs0 avgL=Rs-707 maxDD=Rs-2,604 SL/TGT/EOD=3/0/2 tpd=1.25 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 | n=5 PF=0.0 net=Rs-3,041 win%=0.0 avgW=Rs0 avgL=Rs-608 maxDD=Rs-2,209 SL/TGT/EOD=3/0/2 tpd=1.25 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0 |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup MR_CONTROLLED_VWAP_EXTREME_FADE_LONG --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_CONTROLLED_VWAP_EXTREME_FADE_LONG --trials 200 --time_budget_min 8.0 --seed 7
```

## Remaining risks

- TEST sample = 5 trades / 4 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).