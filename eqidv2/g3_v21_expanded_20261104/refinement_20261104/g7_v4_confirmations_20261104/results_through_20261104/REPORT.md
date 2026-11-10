# G3 V21 with G7 V4 entry confirmations, through 4 November 2026

The first 64 sessions reproduce the isolated 3 November V21/G7 replay. A new, hash-verified sealed 4 November G3 source supplies V21's same 73 configured setup pairs and original native execution engine. The 39 new pairs retain cap one plus the 5-minute volume increment; the 34 original pairs retain cap three. G7 Nifty and extreme/efficiency/RSI checks cover every selected pair; G7 ATR and OI/value guards cover their named pairs. Vetoes do not promote the next ranked stock. V21 exits and unlimited-capital per-entry sizing are unchanged.

## All 65 sealed sessions

| Entry policy | Selected | Trades | Losses | Win rate | Net P&L (₹) | Losing days | Worst day (₹) | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| v21_new39_cap1_volume_gate | 1092 | 961 | 323 | 66.39% | 1,248,545.54 | 16 | -34,474.78 | 40,721.16 |
| plus_g7_nifty | 922 | 819 | 274 | 66.54% | 1,094,487.14 | 16 | -34,474.78 | 39,221.16 |
| plus_g7_low_efficiency | 1076 | 945 | 319 | 66.24% | 1,227,105.74 | 15 | -33,884.08 | 42,749.18 |
| plus_g7_atr | 1053 | 925 | 301 | 67.46% | 1,301,901.63 | 15 | -34,474.78 | 40,721.16 |
| plus_g7_slot_postrank | 1087 | 956 | 319 | 66.63% | 1,263,266.56 | 16 | -34,474.78 | 40,721.16 |
| all_g7_entry_confirmations | 871 | 771 | 247 | 67.96% | 1,149,524.05 | 16 | -30,637.98 | 35,384.36 |

## 4 November alone

| Entry policy | Selected | Trades | Wins | Losses | Net P&L (₹) |
|:---|---:|---:|---:|---:|---:|
| v21_new39_cap1_volume_gate | 18 | 18 | 9 | 9 | -12,873.56 |
| plus_g7_nifty | 16 | 16 | 9 | 7 | -3,924.33 |
| plus_g7_low_efficiency | 18 | 18 | 9 | 9 | -12,873.56 |
| plus_g7_atr | 18 | 18 | 9 | 9 | -12,873.56 |
| plus_g7_slot_postrank | 18 | 18 | 9 | 9 | -12,873.56 |
| all_g7_entry_confirmations | 16 | 16 | 9 | 7 | -3,924.33 |

**64-session parity:** PASS_ALL_SIX_ARMS. **4 November selected after V21 cap/volume:** 18. **4 November admitted after all G7 entry checks:** 16.

G7's retrospective win-rate slot allowlist, daily two-loss lock and profit protection are not included. This is an entry-confirmation comparison, using historical rules examined on overlapping sessions; 4 November is a newly sealed session in this run, not a prospective live trial. The source proofs record whether a dated Nifty decision used an immutable snapshot or a hashed historical futures file.

[Overall results](overall.csv) · [Daily results](daily.csv) · [73-pair results](slot_results.csv) · [Per-order decisions](admission_audit.csv) · [Executed trades](trades.csv) · [Source proof](provenance.json)
