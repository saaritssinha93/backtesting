# G3 V21 with G7 V4 entry confirmations

Research-only replay of the 64 sealed V21 sessions, 29 July–3 November 2026. The V21 policy first caps its 39 new pairs at one and requires their completed 5-minute volume ratio to exceed the setup minimum by 0.10. The 34 original pairs retain cap three. G7's Nifty and low-efficiency checks then apply to **every one of the 73 pairs**. G7's ATR and OI/traded-value thresholds apply to the specific G7 setup IDs that define them. Vetoes do not replace a selected stock.

The G7 historical ≥70% slot allowlist, daily two-loss lock, profit-protection stop and different targets/stops are not entry confirmations and are not included. All arms reuse the same V21 native fill and exit paths, ₹100,000 capital per order, 5× exposure, 5 bps modeled round-trip costs and unlimited portfolio capital.

## Full 64 sessions

| Entry policy | Selected | Trades | Losses | Win rate | Net P&L (₹) | Losing days | Worst day (₹) | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| v21_new39_cap1_volume_gate | 1074 | 943 | 314 | 66.70% | 1,261,419.10 | 15 | -34,474.78 | 40,721.16 |
| plus_g7_nifty | 906 | 803 | 267 | 66.75% | 1,098,411.46 | 15 | -34,474.78 | 39,221.16 |
| plus_g7_low_efficiency | 1058 | 927 | 310 | 66.56% | 1,239,979.30 | 14 | -33,884.08 | 42,749.18 |
| plus_g7_atr | 1035 | 907 | 292 | 67.81% | 1,314,775.19 | 14 | -34,474.78 | 40,721.16 |
| plus_g7_slot_postrank | 1069 | 938 | 310 | 66.95% | 1,276,140.12 | 15 | -34,474.78 | 40,721.16 |
| all_g7_entry_confirmations | 855 | 755 | 240 | 68.21% | 1,153,448.37 | 15 | -30,637.98 | 35,384.36 |

## Five later sessions, 28 October–3 November

| Entry policy | Trades | Losses | Win rate | Net P&L (₹) | Losing days | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|
| v21_new39_cap1_volume_gate | 90 | 39 | 56.67% | 42,983.14 | 2 | 34,754.89 |
| plus_g7_nifty | 75 | 32 | 57.33% | 46,566.72 | 2 | 24,353.54 |
| plus_g7_low_efficiency | 87 | 39 | 55.17% | 31,388.85 | 2 | 42,749.18 |
| plus_g7_atr | 83 | 35 | 57.83% | 51,528.29 | 2 | 28,959.74 |
| plus_g7_slot_postrank | 90 | 39 | 56.67% | 42,983.14 | 2 | 34,754.89 |
| all_g7_entry_confirmations | 68 | 29 | 57.35% | 45,454.40 | 2 | 24,615.85 |

## Gate coverage and interpretation

- Saved V21 cap-one/volume baseline reproduction: **PASS_SAVED_V21_NEW39_CAP1_VOLUME_GATE**.
- Universal Nifty decisions: 1074 checked; 168 vetoed.
- Universal G7 V4 low-efficiency decisions: 1074 checked; 16 vetoed.
- G7 named ATR guards: 178 applicable; 39 vetoed. G7 named post-ranking OI/value guards: 33 applicable; 5 vetoed.
- G7's 09:26 SHORT minimum lower wick of 0.20 was already present in V21 and was verified, not added twice.

The historical thresholds and the V21 volume policy were developed using overlapping history; the five later sessions have also been inspected. This is a retrospective comparison, not an independent forward result. Early stock history comes from hash-verified retrospective snapshots, with each decision restricted to completed bars and prior same-stock sessions. Dated Nifty bars use immutable day snapshots where available; otherwise the hashed historical futures file is used. The source kind for each day is recorded in the proof files. No completed 4 November V21 session is present in this sealed source.

[Overall results](overall.csv) · [Day-wise results](daily.csv) · [73-pair results](slot_results.csv) · [Per-order gate audit](admission_audit.csv) · [Final trades](trades.csv) · [Configuration](config.json) · [Stock source proof](stock_source_proof.json) · [Nifty source proof](nifty_source_proof.json)
