# Portfolio-capacity correction ? 14 September 2026

The user clarified portfolio capital is greater than Rs10 lakh, with Rs1 lakh per trade. The earlier Rs3 lakh / three-position constraint was incorrect. The current capacity replay uses Rs10 lakh as a conservative lower bound, Rs1 lakh per trade and no separate three-position cap. Selection, volume >=1.20, entry rules and exits are unchanged.

| Measure | Old 3 lakh cap | Corrected >=10 lakh capacity |
|---|---:|---:|
| Selected | 67 | 67 |
| Executed | 55 | 61 |
| Wins / losses | 36 / 19 | 39 / 22 |
| Trades per session | 1.77 | 1.97 |
| Win rate | 65.45% | 63.93% |
| PF | 3.7687 | 3.4973 |
| Net profit with inherited 5x exposure | Rs164742.05 | Rs172942.51 |
| Daily-close drawdown with inherited 5x exposure | Rs7950.00 | Rs7950.00 |

Peak concurrent positions were five, allocating Rs5 lakh. Doubling replay portfolio capital from Rs10 lakh to Rs20 lakh produces identical executions. Six capital-rejected trades are recovered; six selections still do not trigger. More available capital cannot recover those untriggered trades.

**Sizing clarification pending:** the original model treats Rs1 lakh as allocated capital with 5x exposure (Rs5 lakh position value). If the user means Rs1 lakh total position value, the 1x corrected modeled net is Rs34588.50, with the same 61 trades, 63.93% win rate and 3.4973 PF. Neither interpretation changes SL/target percentages. Both are retained explicitly in the report; no new leverage instruction is inferred.

Current capital-correction report: C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_f/run_20260914_portfolio10l/V10_F_CORRECTED_PORTFOLIO_RESULTS.md

Reproduce the corrected capital allocation: `python -B fno_v13_v10_f_capital_replay.py`

Validation: frozen F source-artifact hashes verified; original 55-fill allocation reproduced per row; all 67 selection identities, underlying fills, SL and targets unchanged; 1x/5x scaling verified; greater-capital execution invariance verified. This allocation replay uses saved, hash-verified F candidate fills, not a new selection optimization. Historical outputs remain intact. Daily and monthly reports are in the current replay folder.

---

# Historical F run: strict E volume filter with the old three-position assumption

The selection rules below remain F. The historical capital-limited results below are superseded by the capacity correction above.

Every selection must pass the same completed 1m confirmation volume ratio >=1.20 as V10-E. The sole final setup change halves the minimum OI-change threshold for SHORT setups. Native ranking then runs again. Long thresholds, volume gates, price/body/wick requirements, setup quotas, ten-minute entry expiry, portfolio capital and B's exact SL/target table are preserved.

| Short signal time | E minimum OI change | F minimum OI change |
| --- | ---: | ---: |
| 09:25 | 0.100% | 0.050% |
| 09:30 | 0.250% | 0.125% |
| 09:35 | 1.000% | 0.500% |
| 09:40 | 0.100% | 0.050% |
| 09:45 | 0.750% | 0.375% |
| 09:50 | 0.100% | 0.050% |
| 11:20 | 0.100% | 0.050% |

| Period | Selected | Executed | Wins-losses | Win rate | PF | Modeled net profit | Daily-close DD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full 31 sessions | 67 | 55 | 36-19 | 65.45% | 3.7687 | Rs 164,742.05 | Rs 7,950.00 |
| July 29-31 | 5 | 4 | 4-0 | 100.00% | infinity | Rs 26,243.15 | Rs 0.00 |
| August | 45 | 40 | 24-16 | 60.00% | 3.0146 | Rs 102,718.72 | Rs 7,950.00 |
| September 1-11 | 17 | 11 | 8-3 | 72.73% | 5.2024 | Rs 35,780.18 | Rs 4,080.46 |

Compared with E, F has 8 more selections and 8 more executions (+17.02%). It retains 57 E selections, adds 10 and replaces 2 through native ranking. Executions undergo a fresh portfolio replay. Full profit rises Rs 3,405.95 (+2.11%); full win rate and PF decline, and August profit declines Rs 11,338.26. September profit rises Rs 14,744.21. This is a participation/quality trade-off.

At 9bps cost, full profit is Rs 153,742.05 (PF 3.4287); September profit is Rs 33,580.18 (PF 4.6843). Capital remains Rs 100,000 per entry at 5x exposure, Rs 300,000 portfolio, at most three simultaneous positions.

Validation: 50 relevant tests passed, E/B controls reproduce their saved ledgers, and the independently invoked F CLI matches all 67 orders with zero P&L difference. All F orders pass volume >=1.20; the minimum observed ratio is 1.2075148.

Reproduce with:
- python -B fno_v13_v10_f_research.py
- python -B fno_v13_v10_f_backtest.py

Historical artifacts: C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_f/run_20260913_volume120/

The 23 configurations (including control) were evaluated in three adaptive stages on previously reviewed history. None of the candidates simultaneously met the preferred 20% execution increase and full win>=65%; the selected variant increases executions 17% with full win>=65% and PF>=3. There is no untouched test. B's exits were fitted on this same history.
