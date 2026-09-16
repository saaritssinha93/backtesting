# V13-V10-G ATM options: three lots and five-minute execution

## Active configuration: 30% stop / 40.4% target

The options backtest/replay and options paper runtime share the active 30% premium stop and 40.4% premium target configuration. The full frozen historical three-lot replay produced **Rs180,189.04 net, 20 trades, 14 wins / 6 losses, 70% win rate and PF5.56**. This full-sample maximum is post-hoc and must be evaluated through forward paper trading.

Reproduce the fixed historical backtest with `python -B fno_v13_v10_g_options_fixed_profile.py`; its defaults use 30 and 40.4.

[Active 30% / 40.4% detailed results and trade ledger](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl30_target40p4/V13_V10_G_OPTIONS_FIXED_PROFILE_RESULTS.md).

## Archived 12.5% stop / 25% target study

The fixed-profile replay on the same frozen inputs produces **Rs114,943.08 net, 20 trades, 13 wins / 7 losses, 65% win rate, PF2.82**, and Rs5,515.89 daily realized drawdown. Net improves by Rs15,269.12 versus the earlier 17.5% / 22.5% settings. The original later-date partition returns Rs14,988.42. This is a post-hoc comparison on the same limited history.

[Updated detailed results and exact trade levels](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl12p5_target25/V13_V10_G_OPTIONS_FIXED_PROFILE_RESULTS.md).

The archived artifact can be reproduced with `python -B fno_v13_v10_g_options_fixed_profile.py --stop-pct 12.5 --target-pct 25`.

## Options projections and interactive HTML

The [interactive G report](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/V13_V10_G_INTERACTIVE_BACKTEST.html) now includes **Options results** and **Options projections** in its navigation, plus a link from the existing stock projections section. It contains observed daily/monthly charts, all 20 option trades, four selectable one-year scenarios, P10–P90 bands, monthly ledgers and CSV/SVG exports. Original stock content is preserved byte for byte after stripping the tagged options additions.

The model starts from **Rs16,14,943.08**, comprising the original Rs15 lakh and retained historical profit. It samples 5,000 paths of 252 additional sessions in circular five-session blocks from all 13 covered sessions, including three no-entry days. Full premium and entry fees must fit available cash, with proceeds released only when the exit is observable. The fixed policy keeps three lots; the HTML also compares two monthly sizing policies.

The user's requested **profit-based monthly increases** start with three lots in M1. After a profitable model month, add `1 + floor(max(0, cumulative future net profit) / Rs5,00,000)` lots at the next month's opening. This starts at +1 lot, becomes +2 at Rs5 lakh future profit, +3 at Rs10 lakh, and continues. A flat or losing month holds size. The Rs5 lakh milestone is an explicit illustrative modeling choice. Each path uses its own already-realized results, and orders that cannot fund the full planned quantity are skipped.

The separate **monthly equity-based policy** floors a fractional lot budget tied to month-opening equity, with a 10% monthly increase cap on that fractional budget. It can reduce whole-lot size following losses. All monthly policies hold their selected size for 21 assumed sessions, and fees are recalculated at actual quantities.

| Winning payoff retained | Fixed future net | Equity-based future net | Profit-based increases: future net | Profit-based increases: ending balance | M12 median lots with profit increases |
|---|---:|---:|---:|---:|---:|
| 100% reference | Rs22.30 lakh | Rs34.97 lakh | Rs246.32 lakh | Rs262.47 lakh | 116 |
| 75% stress | Rs13.00 lakh | Rs16.43 lakh | Rs78.48 lakh | Rs94.63 lakh | 52 |
| 50% stress | Rs4.25 lakh | Rs4.23 lakh | Rs13.14 lakh | Rs29.29 lakh | 18 |
| 20% stress | -Rs6.25 lakh | -Rs4.40 lakh | -Rs6.35 lakh | Rs9.80 lakh | 3 |

These are conditional simulation means. The reference profit-based rule reaches a median 116 lots in M12 versus three historical lots. Its much larger modeled profit assumes execution at quantities that have not been checked against available option volume or bid/ask depth. Larger-order market impact is not modeled; these results are not forecasts or evidence of achievable capacity. Monthly medians summarize different paths and are not a predetermined schedule.

The original fixed-size comparison remains available:

| Positive gross winning payoff retained | Mean future-year net | Mean ending equity |
|---|---:|---:|
| 100% reference | Rs22,29,539 | Rs38,44,482 |
| 75% stress | Rs13,00,396 | Rs29,15,339 |
| 50% stress | Rs4,25,204 | Rs20,40,147 |
| 20% stress | -Rs6,25,026 | Rs9,89,917 |

Losses remain unscaled; fees are recomputed on scenario sale premiums. The three stress cases add a 10 bps execution buffer on each side. These are conditional scenarios from only 20 trades, not forecasts; simulated percentiles exclude unseen regimes and are not calibrated market probabilities. The 12.5% SL / 25% target source was chosen after inspecting prior results, and nine August trades use reconstructed metadata.

[Options one-year report and data](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl12p5_target25/one_year_scenarios/V13_V10_G_OPTIONS_ONE_YEAR_SCENARIOS.md). [Build instructions](docs/v13_v10_g_options/README.md).

## Original baseline study

The options adaptation is implemented in `fno_v13_v10_g_options_backtest.py`. It preserves retained G stock signals and underlying trigger eligibility, then buys three exchange lots of ATM CE for LONG or ATM PE for SHORT at the next observable five-minute open. Option premiums determine stops, targets and P&L; full premium plus fees is reserved from a Rs15 lakh cash account.

The predeclared **17.5% premium SL / 22.5% target** produces **Rs99,673.96 modeled net, 20 trades, 14 wins / 6 losses, 70% win rate, PF2.48**, over the measurable August 26–September 11, 2026 sample. Daily realized drawdown is Rs5,639.82; this is not intraday MTM drawdown. Peak premium plus entry fees is Rs3,22,919.75.

Training picked 25% SL / 40% target, but that challenger lost Rs4,373.08 on the later nine trades versus the baseline's Rs13,274.21 gain. The baseline remains a provisional paper-testing rule. Each individual setup has at most two training trades, so separate optimized per-slot/CE/PE exits are unsupported. Existing live settings were not changed.

There are 73 source selections: seven never triggered, 40 require unavailable August-expiry option metadata/history, and six more fail entry-data or prior-volume checks. Nine executed August trades use September-expiry metadata reconstructed from a later snapshot; eleven September trades have dated metadata. The whole G strategy was already selected using this history, so these results are exploratory.

- [Detailed results, all trades, setup rules and sensitivity](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914/V13_V10_G_OPTIONS_3LOTS_RESULTS.md)
- [Primary trade ledger](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914/options_trades.csv)
- [Every monitored five-minute candle](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914/options_5min_bar_audit.csv)

Reproduce from SHA-256-checked normalized inputs:

```powershell
python -B fno_v13_v10_g_options_backtest.py --frozen-input C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914/frozen_input --output-dir C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_replay
```

Read-only market-data refresh uses `fno_v13_v10_g_options_fetch.py` with the saved exact-contract missing-data plan. It never submits orders or changes the shared live cache.

Validation: 57 focused tests pass across signal causality, timestamp/ATM mapping, whole-lot execution, volume checks, stop/target ordering, fees, unresolved exposure, training isolation and premium cash accounting. An independent audit passed 331 checks and verified 71 source hashes. A replay from the frozen normalized inputs reproduced ten main CSV artifacts exactly.
