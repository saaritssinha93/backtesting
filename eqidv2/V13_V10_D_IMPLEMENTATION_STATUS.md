# V13-v10-D completed

V13-v10-D preserves V13-v10-C's full-position exits and adds one selection rule: the completed one-minute confirmation candle must have volume at least 1.20 times the causal mean of the preceding 20 one-minute bars. The filter runs before each setup's native ranking, allowing a qualifying lower-ranked candidate to replace a rejected candidate.

Entry remains the original next-minute confirmation breakout with a ten-minute expiry. The tested five-minute expiry produced identical D executions, so it was not added. SL remains 0.60%-0.89% by setup and every target remains exactly twice its SL. There are no partial exits or break-even moves; unclosed trades exit at 15:15.

| Period | Version | Selected | Executed | Win rate | PF | Modeled net profit | Daily-close DD |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Full 31 sessions | V10-C | 115 | 86 | 60.47% | 2.3787 | Rs 151,213.13 | Rs 13,179.26 |
| Full 31 sessions | V10-D | 59 | 46 | 71.74% | 4.1541 | Rs 128,806.99 | Rs 9,750.00 |
| July 29-31 | V10-D | 5 | 4 | 100.00% | infinity | Rs 18,311.97 | Rs 0.00 |
| August, 19 eligible sessions | V10-D | 40 | 34 | 67.65% | 3.5443 | Rs 88,968.52 | Rs 9,750.00 |
| September 1-11 | V10-D | 14 | 8 | 75.00% | 4.6674 | Rs 21,526.50 | Rs 5,869.65 |

The 59 selected orders contain 54 original C selections and five replacements admitted after filtering and native reranking. Sixty-one C selections were removed. All D selections pass the 1.20 threshold. Of 59 selected orders, 53 triggered and 46 entered the chronological three-position portfolio.

At 9bps cost stress, September remains 75% wins, PF 4.178 and Rs 19,926.50 net. The full period remains 71.74% wins, PF 3.7535 and Rs 119,606.99 net.

V10-D improves historical win rate, PF and drawdown over C while reducing full-period modeled profit because it trades much less. The September result contains only eight executions. The volume filter was selected after September had already been examined, so these results are descriptive and require new sessions before treating the improvement as reliable.

Verification: 115 relevant tests passed. The independent CLI replay matched all 59 selections, exits, portfolio decisions and P&L exactly. The frozen V10-C control also matches its published ledger.

Files:

- `fno_v13_v10_d_backtest.py`
- `fno_v13_v10_d_research.py`
- `tests/test_fno_v13_v10_d.py`
- `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_d/run_20260913/V13_V10_D_DETAILED_RESULTS.md`
- `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_d/run_20260913/daily_detailed.csv`

Reproduce:

```powershell
python -B fno_v13_v10_d_research.py
python -B fno_v13_v10_d_backtest.py
```
