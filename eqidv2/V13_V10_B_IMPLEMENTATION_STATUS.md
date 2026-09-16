# V13-v10-B completed

V13-v10-B applies the requested volatility adjustment to V13-v10-A:

- Every V10-A SL below 0.50% becomes 0.60%.
- Its target is scaled using the original target:SL ratio, rounded to 0.01%.
- Adjusted targets are capped at 3.00%.
- Pairs whose original SL was at least 0.50% remain unchanged.
- Full exits only; no partial exits or break-even stops. Unclosed trades exit at 15:15.

| Signal time | Side | V10-A SL | V10-A target | V10-B SL | V10-B target |
| --- | --- | ---: | ---: | ---: | ---: |
| 09:25 | Long | 0.21% | 0.34% | 0.60% | 0.97% |
| 09:25 | Short | 0.45% | 2.00% | 0.60% | 2.67% |
| 09:30 | Long | 0.85% | 2.00% | 0.85% | 2.00% |
| 09:30 | Short | 0.89% | 2.00% | 0.89% | 2.00% |
| 09:35 | Long | 0.60% | 1.73% | 0.60% | 1.73% |
| 09:35 | Short | 0.82% | 1.23% | 0.82% | 1.23% |
| 09:40 | Long | 0.11% | 1.24% | 0.60% | 3.00% |
| 09:40 | Short | 0.39% | 1.58% | 0.60% | 2.43% |
| 09:45 | Long | 0.82% | 1.23% | 0.82% | 1.23% |
| 09:45 | Short | 0.82% | 1.23% | 0.82% | 1.23% |
| 09:50 | Short | 0.27% | 0.42% | 0.60% | 0.93% |
| 09:55 | Long | 0.16% | 0.24% | 0.60% | 0.90% |
| 10:00 | Long | 0.29% | 2.00% | 0.60% | 3.00% |
| 11:20 | Short | 0.62% | 2.00% | 0.62% | 2.00% |

## Results at 5bps round-trip cost

| Period | Version | Trades | Win rate | PF | Modeled net profit | Daily-close DD |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Full 31 sessions | V10-A | 93 | 67.74% | 5.1638 | Rs 238,298.38 | Rs 6,979.26 |
| Full 31 sessions | V10-B | 87 | 59.77% | 2.8622 | Rs 203,060.39 | Rs 13,179.26 |
| July 29-31 | V10-B | 10 | 90.00% | 84.6121 | Rs 58,424.76 | Rs 0.00 |
| August, 19 eligible sessions | V10-B | 57 | 59.65% | 2.7518 | Rs 128,266.99 | Rs 13,179.26 |
| September 1-11 | V10-B | 20 | 45.00% | 1.4660 | Rs 16,368.64 | Rs 10,017.23 |

The wider stops and proportionally changed targets altered exit timing and portfolio admission. Executions fell from 93 to 87, while executed stop exits increased from 27 to 32. On this history, V10-B is weaker than V10-A in full-period win rate, PF, net profit and drawdown. It remains profitable, but it does not retain V10-A's >=65% overall win result.

The results reuse V10-A's full-history-fitted settings, so this is a deterministic in-sample sensitivity test rather than independent validation. Accounting remains Rs 100,000 capital per trade, 5x exposure, Rs 300,000 portfolio, three positions and flat 5bps round-trip cost. Adverse gaps can exceed the configured SL. Verification: 103 relevant tests passed; independent CLI replay matched all 115 selections, exit timestamps, prices, reasons, portfolio decisions and P&L exactly.

Files:

- `fno_v13_v10_b_backtest.py`
- `fno_v13_v10_b_research.py`
- `tests/test_fno_v13_v10_b.py`
- `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_b/run_20260913/V13_V10_B_DETAILED_RESULTS.md`

Reproduce:

```powershell
python -B fno_v13_v10_b_research.py
python -B fno_v13_v10_b_backtest.py
```
