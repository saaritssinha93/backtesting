# G3 V7: fifteen afternoon setups, October 14 preparation

The 44 existing G3 setups plus exactly the fifteen selected 12:35–13:30
afternoon additions now share `V13-V10-G-3-PRODUCTION-V7` across normal
historical backtesting, main-dashboard daily backtesting, PAPER and LIVE
configuration. The authoritative specification is
`configs/strategies/v13_v10_g_3_production_v7.json`.

The LIVE configuration is prepared and the actual October 14 coordinator
check returned **READY_DISARMED**. No dated arm was written, no LIVE task
was enabled, and no trading workers or broker orders were started. The
existing dated activation procedure remains in
[the operator handoff](G3_LIVE_OPERATOR_ACTIVATION_2026-10-13.md).

## Results

The normal historical CLI completed through October 13: 50 complete sessions
from July 29. October 1 remains excluded for incomplete immutable input.
All previous 44 setups retain their rules, selected stocks, native outcomes,
portfolio executions and PnL. G3 capital allocation is recomputed independently;
the G4 portfolio totals are not copied.

| Scope | Trades | Wins / losses | Win rate | Net PnL (Rs) |
| --- | ---: | --- | ---: | ---: |
| Previous 44 setups | 388 | 295 / 93 | 76.03% | 1,124,904.74 |
| New fifteen additions | 180 | 154 / 26 | 85.56% | 197,264.51 |
| Combined 59 setups | 568 | 449 / 119 | 79.05% | 1,322,169.25 |

Combined gross PnL: Rs1,464,169.25; modeled costs: Rs142,000.00.
Daily-realized maximum drawdown: Rs9,698.64; profit factor: 4.23.
There are 43 profitable sessions and seven losing sessions. The 597 native
fills become 568 executions under the shared capital limit.

October 13 was a losing day: combined 15 trades, eight wins, **−Rs2,544.20**;
the new afternoon group made four executions, two wins, **−Rs5,151.49**.
The old eleven daily executions and their Rs2,607.29 net PnL are unchanged.

All fifteen still have at least five native fills and actual executions,
positive net PnL and at least 70% net wins both standalone and after G3
capital allocation over the full 50 sessions. 12:45 SHORT is exactly 70%
(seven wins from ten actual trades). Small counts remain visible below.

| Signal | Side | Stage | Target | Native win rate | Actual trades | Actual win rate | Actual net PnL (Rs) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 12:35 | LONG | relax_1m | 0.37% | 91.67% | 10 | 100.00% | 16,000.00 |
| 12:35 | SHORT | relax_5m | 1.24% | 77.78% | 16 | 75.00% | 18,853.68 |
| 12:45 | LONG | relax_1m | 0.23% | 82.35% | 15 | 86.67% | 8,052.06 |
| 12:45 | SHORT | relax_1m | 0.49% | 72.73% | 10 | 70.00% | 4,845.08 |
| 12:50 | LONG | relax_1m | 0.26% | 92.31% | 13 | 92.31% | 12,568.82 |
| 12:50 | SHORT | relax_5m | 0.14% | 100.00% | 7 | 100.00% | 3,150.00 |
| 12:55 | SHORT | relax_1m | 0.14% | 100.00% | 11 | 100.00% | 4,950.00 |
| 13:00 | SHORT | relax_5m | 0.88% | 71.43% | 19 | 78.95% | 26,513.07 |
| 13:05 | LONG | relax_5m | 0.18% | 100.00% | 14 | 100.00% | 9,100.00 |
| 13:15 | LONG | relax_5m | 0.17% | 93.75% | 14 | 92.86% | 6,024.86 |
| 13:15 | SHORT | strict | 0.33% | 75.00% | 6 | 83.33% | 6,417.11 |
| 13:20 | SHORT | relax_1m | 1.01% | 78.95% | 14 | 78.57% | 26,472.81 |
| 13:25 | LONG | relax_1m | 0.41% | 76.47% | 16 | 75.00% | 13,033.85 |
| 13:25 | SHORT | strict | 0.76% | 71.43% | 5 | 80.00% | 12,884.57 |
| 13:30 | LONG | strict | 1.17% | 80.00% | 10 | 80.00% | 28,398.60 |

The nine excluded afternoon clock/side pairs remain absent. Filters and
targets were copied exactly from the final G4 dual-70 result and were not
retuned after October 13. The original 49 sessions participated in target
selection; they are fitted historical evidence, not independent validation.
The one extra day does not establish a future minimum win rate.

## Execution and operational preparation

- Exact next-minute confirmation; one highest-liquidity qualifying stock per new setup.
- Initial stop 1.25%, tightening to 1% after 120 minutes from entry; 10-minute trigger expiry; 15:15 square-off.
- SHORT thresholds are already effective and receive no second legacy multiplier.
- Zero minimum confirmation volume still requires finite causal features and runtime warmup.
- PAPER/backtest: Rs100,000 capital per entry, five-times exposure, Rs1,000,000 portfolio capital, five basis points round-trip modeled cost. Slippage, market impact and extra broker charges are not modeled.
- LIVE retains the separate Rs10,000 margin / Rs50,000 target exposure and whole-share sizing. Backtest rupee PnL is not a LIVE PnL estimate.
- Forty signal clocks now end at 13:30; final confirmation 13:31, publication grace to 13:32:30, scanner/feed/confirmation cutoff 13:33.
- Shared Python contracts and import-safe setup rows drive the session and BAT wrappers. No separate hand-maintained strategy rules were added to launchers.
- Both existing dashboard servers were reloaded under their established ownership and verified through authenticated APIs. Historical, daily and October 14 PAPER/LIVE views identify V7; tomorrow has no trading events or orders.
- All 19 captured scheduled-task states, actions and triggers are unchanged. G3 PAPER/feed tasks are enabled at 09:15, upstream futures feed at 09:05, opening recovery at 17:00 and daily/history at 17:15.
- The daily BAT retains its `_1620` filename for installed-task compatibility. Its recorded exit 2 came from the older 16:20 run before opening recovery; tomorrow already uses 17:15.
- The separate options ATM feed has an existing stale-heartbeat supervisor failure (21 restarts versus maximum 20). Equity configuration readiness passes; options-feed health is not attested.
- Today’s raw replay includes 212 of 213 stocks. KAYNES is excluded for missing required 09:20 futures OI; eleven opening-only exclusions are separately recorded. Missing data is not replaced with a price/OI proxy.

## Reproduction and artifacts

The actual normal CLI completed successfully:

```powershell
python fno_v13_v10_g_3_backtest.py --through 2026-10-13
```

The actual daily publisher completed from raw dated inputs:

```powershell
python backtesting_result_v13_v10_g_3_daily.py --date 2026-10-13
```

Final historical run: `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-13/20261013_afternoon_fifteen_v7_normal_cli`.

[Full HTML report](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-13/20261013_afternoon_fifteen_v7_normal_cli/report.html) · [Daily CSV](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-13/20261013_afternoon_fifteen_v7_normal_cli/daily_results.csv) · [All setup results](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-13/20261013_afternoon_fifteen_v7_normal_cli/setup_results.csv) · [Trades](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-13/20261013_afternoon_fifteen_v7_normal_cli/portfolio_trades.csv)

Daily run: `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/production/runs/2026-10-13/20261013T222838087345_6e7355c3`.

The previous V6 JSON and Python readers are archived unchanged. Latest
pointers identify the verified V7 manifests; older strategy evidence remains
under its own version. Strategy and execution-analysis artifacts were refreshed
with V7. Prospective PAPER calibration remains awaiting new V7 evidence.

## Completed verification

- 312 root contract, preservation, sizing, dashboard and execution tests passed.
- 47 additional API/registry/cockpit tests passed in the API environment.
- 189 operations tests, 95 daily/dashboard tests and 32 historical tests passed. These groups overlap; their totals are not added together.
- 180 G4 selection, target-search and portfolio regression tests passed after the G3 promotion.
- Independent heap-based allocation and full report accounting passed 858 checks.
- Raw daily reconstruction, all input/code/artifact hashes, all 40 clocks, independent fifteen-setup selection and prior V6 preservation passed 965 checks.
- Every report link resolves; HTML was rendered and visually inspected. Both live dashboard APIs and the actual main daily card reconcile with the final artifacts.
- Actual October 14 readiness is READY_DISARMED; all 59 setup IDs, 40 clocks, separate LIVE sizing and inactive trading controls verified.

Strategy fingerprint: `68e842bb99002c950304cc40a41aa40e93a6394b131199ade872ec5e852a281f`.

Final historical manifest: `05ed1a712e0fc80d31bb76084ae24b502e91a9f471595374a0fc649a069ba6d0`.

Receipts and backups: `.codex_tmp/g3_afternoon_production_20261013/`,
`.codex_tmp/g3_afternoon_daily_20261013/`, `.codex_tmp/g3_afternoon_ops_20261013/`.
