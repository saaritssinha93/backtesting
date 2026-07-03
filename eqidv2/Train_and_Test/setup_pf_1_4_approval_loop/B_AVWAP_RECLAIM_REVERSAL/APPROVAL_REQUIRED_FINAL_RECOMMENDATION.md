# B_AVWAP_RECLAIM_REVERSAL (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (too few trades (test_n<6); TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "body_pct",
      ">=",
      0.992271
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_rsi_dir",
      ">=",
      57.996594
    ]
  ],
  "entry_guards": {
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=21 PF=0.865 net=Rs-1,179 win%=33.3 avgW=Rs1,077 avgL=Rs-623 maxDD=Rs-4,381 SL/TGT/EOD=8/5/8 tpd=2.1 tradeDom=0.168 dayDom=9.99 symDom=9.99 dbp=0.6168 | n=21 PF=1.124 net=Rs919 win%=38.1 avgW=Rs1,041 avgL=Rs-570 maxDD=Rs-3,292 SL/TGT/EOD=8/5/8 tpd=2.1 tradeDom=0.164 dayDom=3.062 symDom=1.489 dbp=0.4264 |
| TEST  | n=1 PF=0.0 net=Rs-921 win%=0.0 avgW=Rs0 avgL=Rs-921 maxDD=Rs0 SL/TGT/EOD=1/0/0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None | n=1 PF=0.0 net=Rs-823 win%=0.0 avgW=Rs0 avgL=Rs-823 maxDD=Rs0 SL/TGT/EOD=1/0/0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the band gate on TRAIN+TEST. No edit to `final_setup_conf.py` is recommended. If it is currently active, consider it a **DEMOTE** candidate; if it is research-watch/raw, keep it parked.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --trials 220 --time_budget_min 9.0 --seed 7
```

## Remaining risks

- TEST sample = 1 trades / 1 day(s) (thin June data).
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 9.99 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).