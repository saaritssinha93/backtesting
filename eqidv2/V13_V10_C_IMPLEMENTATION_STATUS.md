# V13-v10-C completed

V13-v10-C keeps every V13-v10-B stop unchanged and sets every full-position target to exactly twice the SL. There are no partial exits or break-even moves. Positions that reach neither level exit at 15:15.

| Signal time | Side | SL | Target |
| --- | --- | ---: | ---: |
| 09:25 | Long | 0.60% | 1.20% |
| 09:25 | Short | 0.60% | 1.20% |
| 09:30 | Long | 0.85% | 1.70% |
| 09:30 | Short | 0.89% | 1.78% |
| 09:35 | Long | 0.60% | 1.20% |
| 09:35 | Short | 0.82% | 1.64% |
| 09:40 | Long | 0.60% | 1.20% |
| 09:40 | Short | 0.60% | 1.20% |
| 09:45 | Long | 0.82% | 1.64% |
| 09:45 | Short | 0.82% | 1.64% |
| 09:50 | Short | 0.60% | 1.20% |
| 09:55 | Long | 0.60% | 1.20% |
| 10:00 | Long | 0.60% | 1.20% |
| 11:20 | Short | 0.62% | 1.24% |

## Results at 5bps round-trip cost

| Period | Version | Trades | Win rate | PF | Modeled net profit | Daily-close DD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Full 31 sessions | V10-A | 93 | 67.74% | 5.1638 | Rs 238,298.38 | Rs 6,979.26 |
| Full 31 sessions | V10-B | 87 | 59.77% | 2.8622 | Rs 203,060.39 | Rs 13,179.26 |
| Full 31 sessions | V10-C | 86 | 60.47% | 2.3787 | Rs 151,213.13 | Rs 13,179.26 |
| July 29-31 | V10-C | 10 | 80.00% | 10.4389 | Rs 37,271.80 | Rs 0.00 |
| August, 19 eligible sessions | V10-C | 56 | 62.50% | 2.4688 | Rs 103,695.27 | Rs 13,179.26 |
| September 1-11 | V10-C | 20 | 45.00% | 1.2917 | Rs 10,246.06 | Rs 9,750.00 |

Across all sessions, V10-C selected the same 115 orders, 102 triggered, and 86 entered the chronological three-position portfolio. It produced 52 wins and 34 losses. Exit counts were 37 targets, 32 stops and 17 time exits.

At 9bps cost stress, the full result is 60.47% wins, PF 2.1506 and modeled net profit Rs 134,013.13. September is 45% wins, PF 1.1673 and Rs 6,246.06.

The exact 2:1 target rule gives V10-C a slightly higher win rate than B, but lower PF and profit. It remains weaker than V10-A on the full historical period. These variants derive from V10-A settings fitted on the same history, so this is a requested in-sample sensitivity comparison, not independent validation.

Accounting remains modeled Rs 100,000 capital per entry, 5x exposure, Rs 300,000 portfolio, three simultaneous positions and flat 5bps round-trip costs. The replay preserves original V10 selection, one-minute confirmation, next-minute entry, S+10 expiry, stop-first same-bar ordering and adverse-gap stop fills.

Files:

- `fno_v13_v10_c_backtest.py`
- `fno_v13_v10_c_research.py`
- `tests/test_fno_v13_v10_c.py`
- `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_c/run_20260913/V13_V10_C_DETAILED_RESULTS.md`

Reproduce:

```powershell
python -B fno_v13_v10_c_research.py
python -B fno_v13_v10_c_backtest.py
```
