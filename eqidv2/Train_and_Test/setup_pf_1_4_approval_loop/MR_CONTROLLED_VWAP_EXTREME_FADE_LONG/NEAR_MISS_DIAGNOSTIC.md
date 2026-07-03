# MR_CONTROLLED_VWAP_EXTREME_FADE_LONG - Near-Miss Diagnostic

Run basis:
- Pool: `C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_CONTROLLED_VWAP_EXTREME_FADE_LONG`
- Source rows: `tier123_standalone_trades.csv`
- TRAIN: 2025-06-03..2026-03-30, 113 sessions, 240 entries
- TEST: 2026-04-01..2026-05-29, 22 sessions, 70 entries
- Cost/resolution: 15 bps per leg, normal 1-min resolver/EOD behavior

Baseline:
- Config: SL 0.70%, target 0.80%, no masks, no pre-momentum, no guards
- TRAIN: 240 trades, PF 0.279, net Rs -75,681
- TEST: 70 trades, PF 0.246, net Rs -28,660

Best balanced FIT/VAL family:
- Config: SL 1.20%, target 1.25%, `upper_wick_pct <= 0.094222`, guard `max_slot=12:00`, `top_n=1`
- FIT/VAL: 14 trades PF 0.983 / 16 trades PF 1.030
- Full TRAIN: 30 trades, PF 1.015, net Rs 150
- TEST: 12 trades, PF 0.295, net Rs -6,780

Only full-TRAIN-band configs found in the logged unique configs:

| SL | Target | Mask | TRAIN n/PF/net | TEST n/PF/net | Reason rejected |
|---:|---:|---|---|---|---|
| 1.20 | 1.25 | `upper_wick_pct >= 0.01547`, `max_slot=12:00`, `top_n=2` | 27 / 1.330 / Rs 2,646 | 10 / 0.000 / Rs -12,100 | TEST collapse, dominance fail |
| 1.20 | 1.25 | `upper_wick_pct >= 0.025725`, `max_slot=12:00`, `top_n=1` | 21 / 1.607 / Rs 3,231 | 6 / 0.000 / Rs -7,771 | TEST collapse, dominance fail |
| 1.20 | 1.25 | `upper_wick_pct >= 0.033666`, `max_slot=12:00`, `top_n=1` | 16 / 1.523 / Rs 2,094 | 5 / 0.000 / Rs -6,411 | TEST collapse, dominance fail |

Conclusion:
- Approval recommendation remains NO.
- Keep the setup parked in `RESEARCH_WATCH_CONF`.
- There is no MR final-config promotion from this run.
