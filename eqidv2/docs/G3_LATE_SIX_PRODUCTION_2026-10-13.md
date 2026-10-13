# G3 V4: six selected late setups

The current G3 profile contains its previous sixteen setups plus the six
additions below. The normal historical backtest was completed before production
promotion, then repeated under the promoted contract with identical result
CSVs. Daily replay, PAPER and the LIVE rule book share the same configuration.

Version: `V13-V10-G-3-PRODUCTION-V4`.

Fingerprint: `181c37b6a43291311525b0b6970ea0019d5237bce172735913418cba623fb6b9`.

## Added setups

Times are completed signal candles in IST. Confirmation must be the exact next
completed minute. Every addition permits one entry per setup/day, ranked by
highest traded value. Existing setup quotas are preserved.

| Signal | Side | Confirmation | Target | Price move minimum | OI increase minimum | 5m volume minimum | Body/range minimum | Wick/range maximum | 1m volume minimum |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10:05 | LONG | 10:06 | 0.56% | +0.10% | 0.05% | 1.30 | 0.70 | 0.50 | 0.60 |
| 10:10 | SHORT | 10:11 | 1.56% | -0.13% | 0.05% | 1.00 | 0.40 | 0.60 | 1.20 |
| 10:15 | LONG | 10:16 | 0.96% | +0.30% | 0.05% | 1.30 | 0.50 | 0.50 | 0.60 |
| 10:15 | SHORT | 10:16 | 0.68% | -0.13% | 0.05% | 1.00 | 0.40 | 0.60 | 1.20 |
| 10:25 | SHORT | 10:26 | 2.85% | -0.13% | 0.05% | 1.00 | 0.40 | 0.60 | 1.20 |
| 10:30 | SHORT | 10:31 | 1.66% | -0.13% | 0.05% | 1.00 | 0.40 | 0.60 | 1.20 |

For SHORT, price return must be at or below the listed negative threshold.
The late SHORT thresholds are already effective G4 thresholds: they are not
multiplied again by the older G3 expansion factor. Their core and expanded
candidate pools are identical, preserving the researched liquidity ranking.

10:05 SHORT, 10:10 LONG, both 10:20 sides, 10:25 LONG and 10:30 LONG are absent.
The 09:20 SHORT setup remains absent. Existing sixteen G3 setups retain every
selection, confirmation and exit parameter.

All setups retain EMA alignment, positive increasing OI with the 1% cap,
directional confirmation, causal volume calculation, source-quality checks,
1.25% initial stop, 1% stop after 120 minutes, ten-minute trigger expiry, and
15:15 square-off. PAPER/backtest uses Rs100,000 per entry at 5x with Rs1,000,000
portfolio capital and 5 bps round-trip costs. LIVE retains its separate
Rs10,000 per entry at 5x, with whole-share quantity sizing.

## Normal historical result

49 included sessions, July 29 through October 12, 2026. October 1 is excluded
because its immutable source is incomplete.

| Metric | Previous G3 V3 | Current G3 V4 | Added six |
| --- | ---: | ---: | ---: |
| Selected orders | 131 | 168 | 37 |
| Filled/executed trades | 120 | 152 | 32 |
| Wins / losses | 86 / 34 | 112 / 40 | 26 / 6 |
| Win rate | 71.67% | 73.68% | 81.25% |
| Gross P&L | Rs385,021.88 | Rs472,939.88 | Rs87,918.00 |
| Costs | Rs30,000.00 | Rs38,000.00 | Rs8,000.00 |
| Net P&L | Rs355,021.88 | Rs434,939.88 | Rs79,918.00 |
| Maximum daily-close drawdown | Rs14,142.14 | Rs10,500.00 | Rs5,250.00 |

| Addition | Filled trades | Wins | Win rate | Net P&L |
| --- | ---: | ---: | ---: | ---: |
| 10:05 LONG | 10 | 8 | 80.00% | Rs12,594.36 |
| 10:10 SHORT | 3 | 3 | 100.00% | Rs10,394.92 |
| 10:15 LONG | 5 | 4 | 80.00% | Rs13,071.40 |
| 10:15 SHORT | 4 | 3 | 75.00% | Rs4,200.00 |
| 10:25 SHORT | 6 | 5 | 83.33% | Rs28,123.58 |
| 10:30 SHORT | 4 | 3 | 75.00% | Rs11,533.73 |

The prior 131 entry selections are hash-verified evidence for unchanged rules.
The six added pools are reselected using the shared contract. Every exit is
replayed from verified minute inputs and portfolio allocation is recomputed.
All 120 existing G3 trades retain their execution result and net P&L.

Targets were optimized retrospectively on these sessions. The earlier strict
training/holdout qualification was not met; the displayed >=70% rates are
historical fit results on small samples. Opening OI evidence is retrospective.
These figures use PAPER/backtest sizing and are not a projection of LIVE P&L.

Current historical package:

`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-12\20261013T014611944148_69d958f4_current_late_six`

Contains `report.html`, `summary.json`, `config.json`, `daily_results.csv`,
`variant_trades.csv`, `baseline_trades.csv`, `added_six_trades.csv`,
`selected_orders.csv`, `added_six_orders.csv`, `selection_audit.csv`,
`setup_results.csv` and artifact hashes in `manifest.json`.

Normal historical command: `python fno_v13_v10_g_3_backtest.py`.
This study's source interval is pinned through October 12. New sessions use
the separate native daily publisher below.

## Native daily result and dashboards

The native October 12 replay rebuilt all 213 dated-universe stocks under V4;
212 passed the existing stock-day coverage policy. ALKEM lacked a required
opening futures bar; seven other stocks lacked opening OI and were excluded
only from 09:20. There were no blocking coverage problems.

POLYCAB 09:20 LONG and DELHIVERY 09:25 SHORT produced two fills, one win and
one loss, net **Rs3,874.26**. The added late setups selected no orders on this
day. The selection audit retains their evaluated candidates. The daily result
matches the October 12 row of the normal historical run.

Native package:

`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\production\runs\2026-10-12\20261013T014207730091_e85b3b32`

Current daily/history pointers and execution/strategy analysis have been
refreshed. Native V4 history starts its own revision cohort; older V1/V2/V3
results retain their identities. Dashboard Flow shows the full 49-session V4
historical run. The main daily card shows the native daily result.

Both dashboard services on ports 8787 and 8790 were restarted. Authenticated
HTTP checks passed for the main page, Flow, current result API, PAPER cockpit
and main snapshot. Unauthenticated requests still receive 401. The public
tunnel process was preserved.

## Runtime and schedules

The existing scanner, exact-minute feed, confirmation worker, PAPER LONG and
SHORT workers, loggers and result sessions load the shared G3 contract at
startup. The five newly used signal clocks map to 10:06, 10:11, 10:16, 10:26
and 10:31 confirmations; both sides at 10:15 share the 10:16 candle. The
existing pipeline deadline of 11:23 covers all active setups.

All fifteen relevant scheduled launchers and their paths were checked. The
weekday tasks are enabled except the previously disabled LIVE coordinator.
PAPER workers start at 09:15, opening observer at 08:55, daily backtest at
16:20, and opening-history recovery retry at 17:00. Their weekday triggers
cover October 13 and October 14. No duplicate tasks were created.

Daily command after close:

`bat\run_backtesting_result_v13_v10_g_3_1620.bat --date YYYY-MM-DD`

The daily launcher recovers opening evidence, runs current native replay and
refreshes analysis. Other existing G3 BAT/session wrappers already select the
G3 profile, so their rules update through the common contract.

**LIVE rules are configured; broker execution is not activated or armed.**
`configs/strategies/active_profile.json` retains `PAPER_VALIDATION` and
`live_activation_authorized=false`. The ordinary LIVE launcher remains
readiness-only and its scheduled coordinator remains disabled. Existing
requirements are three reconciled PAPER sessions, broker position/open-order
reconciliation, explicit LIVE deployment approval, acknowledgement, and a
dated arm record bound to this new strategy fingerprint and the execution
policy. An arm record for V3 cannot authorize V4. No broker orders were sent.

## Files and verification

Primary files: `fno_v13_v10_g_3_late_setups.py`,
`fno_v13_v10_g_3_contract.py`, `fno_v13_v10_g_3_live_config.py`,
`fno_v13_v10_g_3_policy.py`, `fno_v13_v10_g_3_daily_replay.py`,
`fno_v13_v10_g_3_preflight.py`, `fno_v13_v10_g_3_backtest.py`,
`research/g3_current_backtest.py`, `dashboard_flow.py`,
`configs/strategies/v13_v10_g_3_production_v4.json`, and the active profile.
The daily BAT and observer session description now identify V4. Shared
runtime and monitor modules already derive their clocks from the contract.

245 focused tests passed. Coverage includes threshold boundaries, exact
confirmation, all six PAPER fills and LIVE order configurations, separate
sizing, late SHORT liquidity ranking, original sixteen-rule parity,
portfolio/partial-fill handling, revision isolation and dashboard routing.

Additional checks proved two normal runs byte-identical for result CSVs,
historical configuration equal to production configuration, native daily
code hashes current, native daily equal to historical day, and the previous
daily trades unchanged. Local preflight reports ready with 22 setups.

Evidence:

`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\readiness\2026-10-13\late_six_v4`

## Rollback material

Pre-change files and G3 latest pointers are saved in
`.codex_tmp/g3_late_six_promotion_20261013/`, with SHA-256 hashes in
`backup_manifest.json`. The separate `deployment_changes.json` identifies
changed source files and their post-change hashes. Task XML exports are also
saved; tasks were not changed.

A rollback should stop any trading workers first, verify that files have not
been edited again, restore only the changed files from `files/`, and restore
the saved G3 `latest/` pointers. Preserve all immutable result directories.
Then validate the restored profile, refresh the two analysis tools against
the restored history, and restart both dashboard services. New V4 files may
remain as inactive evidence. No blanket git reset is needed, so earlier
uncommitted work is preserved. Rollback has not been executed.
