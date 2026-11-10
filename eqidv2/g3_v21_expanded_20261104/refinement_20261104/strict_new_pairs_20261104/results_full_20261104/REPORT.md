# G3 V21: cap one and a small strength gate on all 39 new pairs

Research-only, unlimited-capital replay. The 34 original V21 pairs retain cap three. The 27 newly enabled donor pairs and 12 added 13:35–14:00 pairs each receive cap one. The tested entry gate raises each new pair's completed 5-minute volume-ratio minimum by 0.10 absolute. It runs after core-first cap-one ranking: a veto does not promote the next stock. Original V21 pairs and all saved target/stop rules are unchanged.

Every native fill executes with ₹100,000 capital per entry, 5× exposure and no effective portfolio capital limit. The same accepted portfolio engine exactly reproduced the previously saved unlimited expanded V21 totals.

## Full 64 sealed sessions

| Policy | Selected | Trades | Losing trades | Win rate | Net P&L (₹) | Losing days | Worst day (₹) | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| frozen_v21 | 635 | 561 | 158 | 71.84% | 1,122,397.79 | 13 | -24,501.07 | 28,166.32 |
| expanded_all73_cap3 | 1320 | 1158 | 423 | 63.47% | 1,212,221.43 | 14 | -62,286.27 | 108,801.51 |
| donor27_cap1_late12_cap3 | 1145 | 1003 | 341 | 66.00% | 1,262,630.80 | 17 | -34,474.78 | 47,754.89 |
| new39_cap1 | 1115 | 978 | 330 | 66.26% | 1,257,117.67 | 17 | -34,474.78 | 47,754.89 |
| new39_cap1_5m_volume_plus_0p10 | 1074 | 943 | 314 | 66.70% | 1,261,419.10 | 15 | -34,474.78 | 40,721.16 |
| new39_cap1_body_plus_0p05 | 1085 | 952 | 321 | 66.28% | 1,222,462.36 | 16 | -34,474.78 | 47,754.89 |
| new39_cap1_volume_and_body | 1046 | 919 | 306 | 66.70% | 1,227,372.45 | 14 | -34,474.78 | 40,721.16 |

## Five later sessions (28 October–3 November)

| Policy | Selected | Trades | Losing trades | Win rate | Net P&L (₹) | Losing days | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|
| frozen_v21 | 57 | 53 | 23 | 56.60% | 13,675.18 | 2 | 28,166.32 |
| expanded_all73_cap3 | 123 | 108 | 48 | 55.56% | 28,215.27 | 2 | 42,167.37 |
| donor27_cap1_late12_cap3 | 108 | 94 | 42 | 55.32% | 27,233.14 | 2 | 47,754.89 |
| new39_cap1 | 108 | 94 | 42 | 55.32% | 27,233.14 | 2 | 47,754.89 |
| new39_cap1_5m_volume_plus_0p10 | 102 | 90 | 39 | 56.67% | 42,983.14 | 2 | 34,754.89 |
| new39_cap1_body_plus_0p05 | 103 | 91 | 41 | 54.95% | 20,961.73 | 2 | 47,754.89 |
| new39_cap1_volume_and_body | 98 | 88 | 38 | 56.82% | 40,461.73 | 2 | 34,754.89 |

## Interpretation

The 5-minute volume +0.10 arm is the smallest tested strength adjustment that reduces selected orders, native trades, and losing trades versus new39 cap one without selecting individual stocks or dates. The body +0.05 and combined arms are shown to make the trade-off reviewable. The rule is retrospective: these variants were tested after seeing historical results, and the five later sessions are a small retrospective check, not independent validation. October 1 was excluded from the inherited sealed source; 116 added stock-clock rows on October 28 remained unavailable because four stocks lacked prior sealed warm-up minutes.

**Baseline parity:** PASS_SAVED_FROZEN_EXPANDED_AND_DONOR_CAP1. **New-pair cap:** 39 pairs at one. **Original cap:** 34 pairs at three.

[Overall CSV](overall.csv) | [Day-wise CSV](daily.csv) | [Admission audit](admission_audit.csv) | [Configuration](config.json) | [Source provenance](provenance.json)
