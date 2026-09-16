# V13-v10 completed

The active V13-v10 configuration is the balanced second-round selection at:

`C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10/run_20260913/balanced/frozen_config.json`

The backtest CLI automatically uses this configuration when it exists. The first-round maximum-win configuration and its research artifacts remain in the parent directory for audit; they are not the active CLI default.

Exit rules:

- Initial stop: 1.45% from actual entry price.
- Exit 25% at +0.65% in the trade direction.
- Move the remaining 75% stop to entry after that partial exit.
- Exit the remainder at +1.30% or at the inherited 15:15 square-off.
- Preserve V9 signal selection, one-minute confirmation, S10 entry expiry, costs, exposure and three-position capital allocation. New exit times can change which selected orders fit within capital.

All targets remain below 2%; the initial stop is below 1.5%. Adverse gaps can produce a realized loss beyond the configured stop distance.

| Period/version | Executed | Win rate | PF | Modeled net profit |
| --- | ---: | ---: | ---: | ---: |
| Full 31 sessions, V9 | 77 | 70.13% | 2.7443 | Rs 188,829.63 |
| Full 31 sessions, V10 | 87 | 75.86% | 2.1344 | Rs 122,476.00 |
| September 1–11, V9 | 18 | 50.00% | 1.0254 | Rs 981.61 |
| September 1–11, V10 | 19 | 57.89% | 1.1327 | Rs 4,989.94 |

This improves historical win rate and retains full-period PF above 2, with lower total profit than V9. September PF remains modest. Results use the inherited modeled cash-price/futures-OI system with fractional exposure, Rs 300,000 capital, 5x exposure and flat 5 bps round-trip costs.

Research: 5,294 initial configurations plus 229 refined configurations, 5,523 distinct parameter sets in total. The first maximum-win choice had poor later PF, leading to an explicitly adaptive second round. The revised selection ranks development outcomes using pooled win rate, per-split PF and at least 50% development-profit retention. All history is previously seen; these are research results, not an untouched performance test.

Verification: 84 relevant tests passed. V9 baseline parity and independent V10 CLI replay match all 115 selected orders exactly, with zero P&L difference. Daily equity reconciles to the reported P&L.

Reproduce the complete study:

```powershell
python -B fno_v13_v10_research.py
python -B fno_v13_v10_balanced_research.py
python -B fno_v13_v10_backtest.py --output-dir 'C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10\run_20260913\balanced\cli_replay'
python -B fno_v13_v10_review.py
```

Current report: `balanced/V13_V10_DETAILED_RESULTS.md` inside the research directory. The grid, complete development results, selected configuration, trade ledgers, cost stress, exit breakdown and source hashes are retained alongside it.
