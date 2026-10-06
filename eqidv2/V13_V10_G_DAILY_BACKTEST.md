# Daily V13-V10-G dashboard backtest

The dashboard session is **Backtesting result v13-v10-G**, with ID `backtesting_result_v13_v10_g`. Its launcher is `bat/run_backtesting_result_v13_v10_g_1620.bat`, which calls `backtesting_result_v13_v10_g_daily.py`. The dated reconstruction and execution implementation is `fno_v13_v10_g_daily_replay.py`.

The session runs only the session-effective V13-V10-G strategy for one explicit IST date. The default is today's date, including on holidays and weekends: there is no fallback to Friday or to a previously completed backtest. The old runner and scheduler installer are compatibility forwarders to G. Historical multi-strategy research files remain available separately.

From **2026-10-06**, the daily replay automatically enables the promoted 09:25 LONG rules: OI maximum 1.20%, five-minute volume minimum 1.75x, confirmation body minimum 54%, and bypassed EMA alignment. Existing G selections have priority within the unchanged setup quota. All equity setups start with a 1.25% stop, tightened to 1.00% after 120 minutes; targets remain unchanged. Minute replay measures the delay from the entry-bar end and activates at the first bar open at/after that time. Earlier session dates retain the original entry and fixed-stop rules. `fno_v13_v10_g_policy.py` records the effective date and the report records the applied policy.

The main standalone file now uses the same dated production replay by default:
`python fno_v13_v10_g_backtest.py` runs today's IST session after its close.
Use `--session-date YYYY-MM-DD` (or `--date`) for a specific session. Each run
defaults to a new dated folder under
`C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/production_replays/`.
An explicit production `--output-dir` must be new or empty. This standalone
command writes its own replay artifacts and does not publish dashboard status.
To reproduce the older sealed research, use
`python fno_v13_v10_g_backtest.py --frozen-research`; `--source-dir` and
`--config-json` are restricted to that explicit research mode.

## Schedule and data flow

`EQIDV2_backtesting_result_v13_v10_g_1620` runs Monday-Friday at **16:20 IST**. On a regular trading day, the runner waits up to 90 minutes for that date's 15:45 backtesting-data producer to finish successfully. It then generates a fresh, dated FnO data verification proof in a unique G verification directory and performs the stricter G source checks. NSE holidays and weekends publish `SKIPPED_NON_TRADING_DAY` before waiting for data or executing a strategy. A current session cannot be replayed before 15:30; a future trading date is blocked.

The reconstruction uses the requested day's persisted equity-to-futures universe, local equity one-minute history, exact futures OI bars, and the dated near-month NIFTY context. Earlier history only warms up causal features. Candidates, selections, execution paths and performance statistics belong to the requested day. Frozen historical signal caches and multi-session optimization are not daily inputs.

The retained G baseline and dated promotion are pinned by the strategy fingerprint. Setup ranking, volume confirmation, targets, ten-minute pending entry expiry, 15:15 square-off, full exits and modeled costs remain the G rules. The backtest uses Rs 1,00,000 allocation per trade, modeled 5x exposure and a Rs 10,00,000 portfolio capital book. This session publishes historical simulations and does not submit paper or broker orders.

During verification and replay, a 30-second heartbeat keeps the dashboard current. The heartbeat stops before the terminal result is published, so a late progress update cannot replace SUCCESS or a blocked status. G reconstruction may take several minutes for the full stock universe; its progress is available in the dated running log.

## Results and failures

The dedicated current report is `C:/TradingData/eqidv2/backtesting_result_v13_v10_g/latest/latest_backtesting_result_v13_v10_g.md`; the corresponding JSON contains status, session date, configuration identity and result artifacts. Each replay keeps dated candidate, selection, coverage and source-manifest files. A complete run also produces an executed portfolio ledger and one daily-results row. The report's trade table includes only executed, filled trades and their portfolio P&L.

Missing or changed source data blocks a complete result and lists the coverage problems. The existing shared data verifier can pass with a small proportion of missing stocks; G's additional completeness checks do not treat that coarse PASS as proof that every required stock is present. During migration, the September 11 dated universe contained 210 stocks, but IDEA and LTM had no one-minute bars for that session. That data condition must be reported rather than silently dropping the two stocks and declaring a complete daily backtest.

No result from the old V6/V8/V10/V11/V12 report is used when the G report is missing. Logs, status, heartbeat, restart mapping, task mapping and the 16:20 dashboard timeline use the canonical G session ID.

For a deliberate historical replay, use the canonical BAT with `--date YYYY-MM-DD`. It validates one date and uses it consistently in the log and replay. The Python entry point additionally accepts `--output-root` for isolated research artifacts; it still publishes the canonical session status. Run the normal current-day launcher afterwards if a historical diagnostic was run while the dashboard is active.

## Verification record

The task migration preserves the original task XML under `C:/TradingData/eqidv2/backtesting_result_v13_v10_g/migration_20260914/`. The replacement is registered disabled, its action is checked, the old task is disabled, and only then is the G task enabled and its future first trigger verified. The old registration is removed after that verification.

The first scheduled G daily run is configured for **2026-09-15 16:20 IST**. September 14 is a holiday in the configured NSE calendar, so its current report should show an explicit skip, without an earlier day's result substituted.

Completed on September 14:

- The canonical launcher ran for September 14 and exited successfully with `SKIPPED_NON_TRADING_DAY`. It did not run the verifier, historical replay, or another strategy.
- The actual replacement task is enabled and Ready, with next run September 15 at 16:20. Its canonical BAT exists; the old task registration is absent. Interactive logon and IgnoreNew duplicate-instance handling were preserved.
- The dashboard was restarted through its existing supervisor. Authenticated HTML and snapshot requests returned HTTP 200; the source SHA matched the running code. The snapshot showed the canonical G daily card, the September 14 holiday report, and the new task's next run. All eleven previously migrated G paper/live views still had strategy identity MATCH. Snapshot response time was approximately 0.19 seconds. Verification is saved beside the task XML as `dashboard_verification.json` and `renamed_task.json`.
- 325 regression tests passed across G strategy, execution, dashboard, launchers, data readiness and preopen integration. The final daily/replay suite passed 51 tests, including the added heartbeat and completeness checks. Tests include native-feature equivalence, new dates beyond the frozen research period, no future-bar leakage, incomplete-data blocking and correct zero-trade behavior.
- A reconstruction of all 210 September 11 stocks from raw inputs reproduced all three retained G selected order identities and fill flags. The available-data diagnostic produced two fills and Rs 15,736.89 modeled net P&L, matching the retained ledger. This remains explicitly unpublishable because IDEA/LTM source data is missing. A bounded recheck confirmed the new completeness guard for the traded stocks and the two missing stocks. The proof is `.codex_tmp/g_daily_replay_sep11_raw_parity.json`; it was not published as today's result.
