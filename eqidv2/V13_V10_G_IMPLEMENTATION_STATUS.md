# V13-v10-G: active configuration and completed expansion study

The morning-slot and two-candle extensions are implemented as explicit G configuration flags and tested. **Neither extension passed the fixed quality requirements, so the active G retains its previous rules and 66-trade result.** The default replay now reads the active configuration in `run_20260914_opportunity_expansion/frozen_config.json`; both extension flags are disabled there. Each tested configuration is retained in its own `cases` folder.

| Four fixed cases, all 31 available sessions | Trades | Trades/day | Win rate | PF | Modeled net | Daily-close drawdown |
|---|---:|---:|---:|---:|---:|---:|
| Previous G / active G | 66 | 2.13 | 65.15% | 3.43 | Rs178,875 | Rs7,950 |
| Three morning slots | 82 | 2.65 | 58.54% | 2.58 | Rs165,737 | Rs11,200 |
| Two-candle continuation | 80 | 2.58 | 60.00% | 2.60 | Rs169,365 | Rs12,300 |
| Both extensions | 101 | 3.26 | 56.44% | 2.21 | Rs169,977 | Rs15,550 |

All 73 previous G selections and 66 executions are retained unchanged in every case. The added executions are individually unprofitable as groups: morning slots add 16 trades/5 wins and lose Rs13,138; two-candle continuation adds 14/5 and loses Rs9,510; both add 35/14 and lose Rs8,898. These results fail the predeclared 63% win rate, PF3.30, maximum 1.2x daily-close drawdown, at-least-old-G net and profitable-added-trades requirements.

The morning slots are 09:50 LONG using the 09:55 LONG F donor, and 09:55/10:00 SHORT using the 09:50 SHORT F donor. Their price threshold stays 0.20%; current G's short price relaxation is not applied to them. Fixed exits are 0.60% SL / 0.90% target for the new long and 0.60% / 0.93% for the new shorts. Quota remains one, with all donor filters unchanged.

The continuation alternative requires exact same-session/symbol/contract closes at t, t-5 and t-10 minutes, a sufficient net directional two-bar move, and a directional latest close-to-close move and candle body. It fills only vacant setup quota after original single-bar choices. Original price values remain unchanged for ranking. The frozen 4,676 original signals reproduce exactly, including values/dtypes/IDs; 2,014 additional causal candidates were reconstructed across the scanned clocks. Six new selected minute paths were separately materialized and source-hash checked.

September alone improves with both extensions: 23 trades, 69.57% wins, PF3.59 and Rs52,681 net versus prior G's 13 trades, 61.54%, PF2.95 and Rs29,280. August worsens, so this month-specific contrast does not justify enabling the extensions only in September after seeing their outcomes.

Latest validation: **183 tests passed**, independent ledger/path/source audit, exact standalone CLI parity for both the active default and combined experimental configuration, and month/day/portfolio accounting checks. Rupee amounts retain the existing 5x exposure per Rs1 lakh allocated capital; they divide by five for Rs1 lakh total position value. All history is previously reviewed.

[Latest report with monthly and daily comparisons](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/V13_V10_G_EXPANSION_RESULTS.md).

Run the four-case study with `python -B fno_v13_v10_g_expansion_research.py`. Run active G with `python -B fno_v13_v10_g_backtest.py`. The original 74-case study below remains reproducible separately with `python -B fno_v13_v10_g_research.py`.

## Previous threshold study

The rejected old G output folder `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/run_20260914` was deleted. The new G engine starts from V10-F, with corrected portfolio capacity. It does not implement the rejected delayed-confirmation/later-window strategy.

**The approximately three-trades/day objective was not achieved with similar win rate and PF.** Of 74 registered configurations (61 distinct selection sets), none met both the 2.8-3.2 trades/session band and the quality requirements. The saved G is the registered fallback that increases executions while preserving those quality limits.

| 31 available sessions, July 29-September 11, 2026 | Corrected F | New G |
|---|---:|---:|
| Selected orders | 67 | 73 |
| Executed trades | 61 | 66 |
| Trades per available session | 1.97 | 2.13 |
| Winners / losers | 39 / 22 | 43 / 23 |
| Win rate | 63.93% | 65.15% |
| Profit factor | 3.50 | 3.43 |
| Modeled net profit | Rs172,942.51 | Rs178,875.19 |
| Daily-close realized drawdown | Rs7,950.00 | Rs7,950.00 |

New rule: multiply F's SHORT five-minute directional price-move minimum by 0.65. For example, a 0.20% required fall becomes 0.13%. Preserve F's same-timestamp native top choices, then fill vacant existing setup quota with newly eligible candidates in native rank order. LONG thresholds, OI, both volume filters, confirmation body/wick thresholds, setup quotas/times, entry expiry and exits are unchanged. The native-reranking diagnostic happens to give identical selections and executions for this chosen configuration.

All 67 original selections and 61 F executions are retained. Six newly selected orders add five executions: four winners and one loser, net Rs5,932.68. The extra executions are all in August. September remains 13 executions, 61.54% win rate, PF2.95, net Rs29,280.18. G has seven zero-trade sessions and eleven sessions with at least three executions; it is not a daily minimum-trade rule.

The near-three/day candidates produced 88-92 executions, only 49.45-53.41% win rate and PF1.78-1.93. They failed the quality requirements and were not selected. The milder SHORT price multiplier 0.75 is documented in the complete sweep: 65 executions, 66.15% wins, PF3.65. The registered fallback chooses the most executions within the quality limits, hence the saved 0.65 configuration; it does not maximize PF.

All money results retain F's modeled Rs1 lakh allocated capital per entry and 5x exposure (Rs5 lakh position value), flat 5bps costs, and Rs10 lakh portfolio capacity with no separate three-position cap. Peak allocation is Rs5 lakh across five positions; doubling portfolio capacity changes no executions. If Rs1 lakh means total position value, the saved 1x replay has identical fills/win rate/PF and one-fifth the rupee profit/drawdown.

These are reused-history results, not out-of-sample evidence. The inherited exit table was itself fitted on this history. Available data covers three July sessions, nineteen August sessions, and nine September sessions through September 11; averages include zero-trade sessions. Drawdown is daily-close realized P&L, not intraday mark-to-market. No live dashboard or trading deployment was changed.

Implementation: `fno_v13_v10_g_backtest.py`, `fno_v13_v10_g_research.py`, `tests/test_fno_v13_v10_g.py`.

Results: [Detailed report and daily comparison](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_careful_thresholds/V13_V10_G_DETAILED_RESULTS.md).

Reproduce the study: `python -B fno_v13_v10_g_research.py`

Replay the frozen candidate: `python -B fno_v13_v10_g_backtest.py`

Validation: independent causal-selection/configuration/source-integrity review; 113 relevant tests passed; original F artifact hashes and corrected F order/fill/exit/portfolio-PnL parity; standalone G CLI replay; 1x/5x scaling and larger-capital invariance. Frozen dataset artifact checks pass. Two pinned, previously understood metadata changes (contract registry refresh and common calendar code) are recorded explicitly and do not rebuild frozen signals or prices.
