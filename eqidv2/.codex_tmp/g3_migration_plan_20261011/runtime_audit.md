**G to G-3 operational migration audit ? plan only, 11 October 2026**

No executable code, task registration, process, broker state, credential, or strategy was changed. The audit reread current source, inspected directory metadata, and queried scheduled-task names/actions/settings without retaining raw arguments or secrets. Exact file lists and 91 additions to the root source inventory are in `runtime_audit.json`; current task evidence is in `runtime_tasks_sanitized.json`.

**What exists today**

The live transport is shared code, not an independent G engine. `fno_v13_v10_g_live.py:6` selects transport `v6` and profile `V13_V10_G` before importing `fno_v5_live`. `fno_v13_v10_g_live_kite_session.py` does the same before importing `fno_v6_live_kite_session`. `fno_live_profile.py:11` accepts only v5/v6 transports, and only the exact G profile for the selected v6 profile branch. Setting that environment variable to G3 currently raises an unsupported-profile error.

The seven common G role launchers cover scanner, candidate-scoped equity feed, confirmation, PAPER long entry, PAPER short entry, PAPER trade logger, and PAPER net result. The four entry/logger/result BAT files whose names contain `live` explicitly set execution mode PAPER. Only `bat/run_fno_v13_v10_g_live_kite_qty1.bat` enables LIVE and launches the coordinator with `--auto-arm`. Its live quantity is exactly one equity share, regardless of strategy-sized research exposure. The coordinator spawns LONG, SHORT, and read-only broker-reconciliation children. All of these identities must be carried together into G3.

Current G profile constants are a G strategy version/fingerprint, `v13_v10_g_live` runtime root, `fno_v13_v10_g` session/report prefixes, and the legacy shared `v6_live` control root. The frozen config and selected-ledger attestation still point into `fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion`. Preserve those original files and hashes for history and rollback.

**Current scheduler inventory**

The read-only snapshot contains 45 relevant registered tasks, 33 enabled. The 12 G intraday tasks and the daily G result task are enabled. Their exact actions are the matching BAT files in `runtime_audit.json`.

| Current task family | Current schedule / important constraint | Planned disposition |
| --- | --- | --- |
| `EQIDV2_fno_v13_v10_g_scanner_5min_0915` | Weekdays 09:15, repeat every 5 minutes for 2h10m, IgnoreNew | G3 scanner action and identity; preserve recovery semantics |
| `..._equity_1min_feed_0915` | Same repetition | G3 candidate/feed contract |
| `..._confirmation_1min_0915` | Same repetition | Exact G3 completed-minute confirmation |
| `..._live_long_0915`, `..._live_short_0915` | Weekdays 09:15, PAPER | G3 PAPER execution |
| `..._trade_logger_0915`, `..._net_result_0915` | Weekdays 09:15, PAPER | G3 PAPER state and reports |
| `..._live_kite_qty1_0915` | Weekdays 09:15, IgnoreNew, no scheduler time limit | G3 isolated qty-one coordinator after promotion gate |
| Four `..._options_*_0915` roles | Weekdays 09:15, PAPER | Explicit G3 options-consumer decision; do not silently leave coupled to old G fills |
| `EQIDV2_backtesting_result_v13_v10_g_1620` | Weekdays 16:20, IgnoreNew | Retain source production until G3 publishes equivalent sealed artifacts |
| `EQIDV2_g3_dated_once_20261009` | Completed one-time trigger; registered Ready but no next run | Preserve historical task evidence; not a recurring G3 scheduler |

The next recurring G sessions are Monday 12 October at 09:15 and daily replay at 16:20. Scanner/feed/confirmation last ran at 11:25 with result 2. The PAPER entry/logger/result tasks also show result 2. LIVE qty-one, options roles, and daily result show result 0. These are historical result codes, not proof of current health or inactivity. In particular, explain the expected post-window exit-code contract before classifying the 11:25 results as failures. Preopen autofix shows 1 and shadow prepare shows 2, also requiring explanation before claiming a clean baseline.

**Required code and configuration changes, rather than a global rename**

1. Add a first-class G3 profile/config and thin G3 live/coordinator entry points; retain the old G profile unchanged. `fno_live_profile.py`, `fno_v5_live.py`, `fno_equity_fetch_1min.py`, and `fno_v6_live_kite_session.py` need explicit profile-aware dispatch. Simply broadening `is_g_config` is insufficient: shared code imports G policy/identity/paper helpers directly and has G-only conditionals. G3 must keep all safety checks while applying its frozen rule contract, not fall into legacy v6 behavior.
2. Create separate strategy identity, version/fingerprint, signal IDs, report/session prefixes, deterministic broker-tag namespace, runtime state paths, and selected-backtest attestation for G3. Review `fno_v13_v10_g_live_config.py`, `fno_v13_v10_g_policy.py`, `fno_v13_v10_g_identity.py`, `fno_v13_v10_g_selection.py`, `fno_v13_v10_g_two_bar.py`, and `fno_v13_v10_g_paper.py` as source contracts. Preserve immutable G/G2/G3 research artifacts; new production adapters should consume the accepted G3 freeze without rewriting it.
3. Port scanner/confirmation timing, candidate selection, next-minute volume history, entry-window expiry, staged-stop/target transition state, PAPER portfolio accounting/locks, and fills into the same production decision contract. `fno_equity_fetch_1min.py:316` checks literal `V13_V10_G` and imports G config for multi-session volume history. It must recognize G3 and publish the correct denominator/evidence; otherwise a new label could silently skip required history. Existing `equity_1m_slot_ready/v6/<date>/slot_<time>_<scanner-hash>.json/.parquet` is content-scoped by scanner hash; maintain profile/fingerprint provenance and verify safe coexistence instead of deleting or overwriting shared markers.
4. Generalize coordinator routing and export naming. `fno_v6_live_kite_session.py:37,129,136,461,569,587,610,617` pins G session IDs, output names, auto-arm source, child script, child profile and reconciliation labels. Validators at 220?264 and 279?316 require exact strategy/fingerprint and authoritative signal ownership. All must agree with G3. Preserve `FIXED_ONE_SHARE`, live order namespace, quantity validation, per-session arm check, kill switch, broker error handling, unknown-submission reconciliation by deterministic tag, and continuous protective exit management.
5. Update or add the 13 matching launchers and the scheduler definitions in `bat/schedule_fno_oi_weekday.ps1/.bat`, `bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1`, `bat/schedule_fno_v13_v10_g_options_weekday.ps1`, and `bat/schedule_backtesting_result_v13_v10_g_weekday.bat`. Prefer explicit G3 task/session names and roots, created disabled before cutover. Keep existing schedule, machine Python executable, working directory, holiday handling, and hardening unless G3's validated timing demands a specific difference. No scheduler installer should be run during implementation review.
6. Update `preopen_session_healthcheck.py` and `preopen_session_autofix.py` with G3 task/action names and readiness expectations. The autofix launcher map can otherwise reactivate old G services. Review `bat/check_pf_supervisor_health.ps1` and the exact paths passed to `bat/supervise_command.ps1`; do not change the generic supervisor algorithm merely to rename a strategy.
7. Update operational consumers together: dashboard routes/maps/assets, API repository/adapters, `ai_platform/observability/runtime_collector.py` and G feature/reconciliation/live-trust consumers, metric-contract/SLO/Prometheus/Grafana configuration, and research/shadow automation tools. The collector hardcodes the G scanner session, live heartbeat/order roots, daily result root, labels and reconciliation schema. `bat/observability/Test-V13V10GLiveTrust.ps1` is an easily missed explicit operational file. Keep G history accessible and ensure new G3 data never appears under a G label.

The machine-readable file groups enumerate exact existing files; not every reviewed file needs an edit. Proposed new G3 profile/wrapper/BAT files do not exist yet and should have explicit names selected consistently with `backtesting_result_v13_v10_g_3` rather than masquerading as G.

**Environment and launcher contract**

The JSON records environment/launcher names only, never secret values. Profile and execution include `FNO_LIVE_GENERATION`, `FNO_V6_STRATEGY_PROFILE`, `FNO_V6_EXECUTION_MODE`, `FNO_V6_EXECUTION_SESSION_NAMESPACE`, `FNO_V6_LIVE_ACK`, and `FNO_V13_V10_G_OPTIONS_EXECUTION_MODE`. Runtime overrides are the `EQIDV2_RUNTIME_ROOT`, runtime-status/data/cache/report/signal/slot-directory variables in `eqidv2_runtime_paths.py`. Preserve correlation through `EQIDV2_OBS_RUN_ID` and inherited telemetry enablement/exporter settings. Update BAT-local `SESSION_ID`, `SESSION_SCRIPT`, `STATUS_FILE`, `HEARTBEAT_FILE`, `LOCK_FILE`, `SPAWN_RECORD_FILE`, `OPEN_POSITIONS_PATTERN`, and worker status/heartbeat paths consistently; preserve Python executable, working directory and UTF-8/unbuffered settings. Do not assume renaming an environment variable changes readers that construct the old name from transport `v6`.

**Critical daily backtest dependency**

`fno_v13_v10_g_3_backtest.py` exports a historical archive. It is not an operational replacement for `backtesting_result_v13_v10_g_daily.py`. `research/g3_dated_replay.py:31` resolves the successful daily G source, its sealed input-snapshot manifest, and `feature_ledger.csv` plus manifest; it verifies hashes before replay. `tools/run_g3_dated_once.py` explicitly waits for that completed G source. G's daily wrapper supplies waiting, data verification, running heartbeat, status, latest Markdown/JSON publication, run/report layout, and observability.

Do not remove the G daily producer until either (a) an independent G3 daily wrapper emits the complete equivalent sealed input/feature-ledger contract and all G3 source resolvers use it, or (b) the G task is deliberately retained as a clearly labeled source-only producer. G3 currently has only `backtesting_result_v13_v10_g_3/runs`; the G root also has `latest`, `reports`, `status.json`, `verification`, and migration history. Protect `moving_files.py`, `bat/run_moving_files_1545.bat`, `bat/run_data_for_backtesting_parallel.ps1`, `data_for_backtesting_verify.py`, `wait_for_data_backtesting_ready.py` and the shared historical data fetchers.

**Options are a real downstream dependency**

`fno_v13_v10_g_options_paper.py` imports G equity config directly. Its equity order root is `fno_oi/v13_v10_g_live/orders`; AUTO source prefers `LIVE/live_kite_qty1/<date>` over `PAPER/<date>` and refuses invalid preferred LIVE evidence rather than silently substituting PAPER. It checks exact G strategy version/fingerprint and requires actually filled OPEN/CLOSED equity states. Changing the equity profile/root alone would stop option mirroring or leave it consuming old G.

For migrating all strategy-dependent PAPER consumers, add a G3 option source profile/root/fingerprint and separately named option ledgers/reports while preserving current one-lot options PAPER mode, premium-cash allocation, quote freshness, depth/spread/volume gates, cost model and exit rules. Do not convert options to broker LIVE orders as part of an equity G3 migration. Review the complete options file group and four BAT/task definitions. Retain the shared ATM options fetch task at 09:07 and its market-data roots. If options are deliberately excluded from the promotion, record that explicitly and prevent stale G option cards from being advertised as G3.

**Runtime and data paths**

All following paths are relative to `C:/TradingData/eqidv2` unless stated otherwise. Current existence is recorded in JSON.

| Scope | Current paths | Migration rule |
| --- | --- | --- |
| G canonical strategy state | `fno_oi/v13_v10_g_live/{scanner_5m,confirmation_1m,signals,orders,consolidated,evidence,order_events,live_kite}`, `strategy_manifest.json` | Preserve; create an isolated G3 sibling root and matching namespace |
| PAPER and LIVE ownership | `.../orders/PAPER/<date>`, `.../orders/LIVE/live_kite_qty1/<date>` | Never relabel/copy old G active orders as G3 |
| Recovery authority | `.../live_kite/open_positions_<date>.json`, `status.json`, `heartbeat.json`; canonical order JSON and broker-event journal | Preserve for the process that owns the broker orders until reconciled |
| Controls | `fno_oi/v6_live/live_arm.json`, `kill_switch.json` when present | Do not copy an armed G control into G3. Define profile-specific arm and deliberate shared emergency-kill coverage |
| Operational transport | `runtime_status/{session}.status/.heartbeat/.supervisor.lock/.supervisor.spawn`, workspace `logs/{session}*.log` | New coherent G3 IDs/paths, including supervisor worker status/run-ID and open-position pattern |
| Options PAPER | `fno_oi/v13_v10_g_options_paper/{orders,consolidated,bar_audit,research}` | Preserve old history; distinct G3 output if options follow G3 |
| EOD results | `backtesting_result_v13_v10_g`, `backtesting_result_v13_v10_g_3` | Preserve source contract and historical runs; add complete G3 publication lifecycle |
| Frozen research | `fno_oi/strategy_research/v13_corrected_v10_g`, `_g_2`, `_g_3` | Protect all. G3 accepted freeze is `.../_g_3/frozen_20261008_long110_nextminute_v1` |
| Shared FnO sources | `fno_oi/{instrument_master,universe,raw_contracts_5m,slot_ready,raw_equity_1m,equity_1m_slot_ready,contract_features,feature_snapshots,rankings,ranking_ready,latest,eod_qc}` | Reuse with verified provenance; do not rename/delete as strategy state |
| Shared options sources | `fno_oi/{raw_options_5m,raw_options_1m,options_slot_ready,options_atm_map}` | Preserve feed and API infrastructure |
| Shared cash/index data | `stocks_indicators_5min_eq_live2`, `stocks_indicators_1min_eq`, `stocks_indicators_15min_eq`, caches, slot markers, NIFTY readiness | Protect fetchers, manifests and historical data |
| Monitoring/research | `observability`, `v13_v10_g_strategy_research`, `v13_v10_g_execution_research`, workspace `configs`, `logs`, `kite_exports` | New G3 attribution and clear old-history retention |

The qty-one launcher gives the supervisor exact worker status/heartbeat and open-position files; it enforces worker run-ID matching, 180-second stale/start-grace checks, 20 restarts, cooldown, 15:45 cutoff, and position-aware stop behavior. Changing only SESSION_ID without those paths can make supervision act on the wrong strategy.

**Protected infrastructure and reactivation paths**

Reuse existing authentication/API applications and file locations. `authentication_v2.py` and its scheduled task reference `api_key.txt`, `api_key2.txt` through `api_key8.txt`, corresponding `request_token*.txt`, `access_token*.txt`, optional `refresh_token.txt`, and `auth_v2_state.json`. Only names/existence were inspected; no secret values were read or copied. Preserve authentication backup/v3, token cache, shared Kite export scheduler and process, data fetcher credentials/rate limits, generic supervisor/task hardener, dashboard/tunnel/email login infrastructure, calendar and costs. Do not generate replacement secrets or alter accounts for a strategy migration.

Review the entire explicit legacy list in JSON. `fno_v6_live.py`, `bat/run_fno_v6_*.bat`, `bat/schedule_fno_v6_live_kite_qty1_weekday.ps1`, old backtesting-v11 aliases/scheduler, and historical migration scripts can lead back to G or recreate old task names. Preserve them as historical/rollback assets; make their disposition explicit so an operator, dashboard restart or preopen autofix cannot accidentally run G concurrently with G3. Disabled v5/v8/shared-v10-v12 registered tasks remain disabled unless separately authorized. One-time backfill/repair task registrations are not recurring G3 services.

**Proposed cutover sequence ? future actions only**

1. Freeze the accepted G3 strategy identity and production behavior; capture source hashes, sanitized operational manifest, current task configuration and local rollback evidence. Export full task XML only to a protected local rollback location during implementation; do not publish raw action arguments.
2. Implement isolated G3 profile, shared-engine dispatch, feed/confirmation contract, PAPER/LIVE state schema, coordinator, EOD producer/source adapters, options decision and all consumers. Keep frozen G/G2/G3 histories and current G defaults intact.
3. Run deterministic fixture/replay parity and failure-path validation before registering active services. Cover exact +1 minute confirmation, LONG/SHORT thresholds, timezone/holiday/cutoff handling, missing-candle/history behavior, scanner hash provenance, rank order, re-entry and portfolio capacity, staged stop/target persistence, restart recovery, unknown order submission, duplicate tags, quantity-one live overrides, kill/arm checks, broker reconciliation, dashboard/API/metrics and options source preference. No broker order is needed for these acceptance checks.
4. Stage G3 tasks disabled, with the intended task/action/workdir/trigger/hardening/supervisor-path manifest. Evaluate source-only EOD retention explicitly. Compare all configured writers and readers for namespace consistency; audit legacy compatibility launchers and autofix maps.
5. Select an after-close boundary. Confirm old G orders and protective orders have reached a reconciled state using canonical local ownership plus broker verification at cutover. A stale or empty export alone is not proof of a flat account. If any old G position remains, keep its existing exit manager alive; do not kill it, move its state, or start a conflicting G3 account-level live owner.
6. When safe, disable the old G restart/scheduled entry paths and stop only the intended G processes/supervisor; preserve shared authentication, market data, Kite exports, dashboard and observability infrastructure. Complete any G options PAPER lifecycle before switching its consumer. Save all existing state and logs intact.
7. Enable only the intended G3 producer/PAPER/report sequence after readiness validation. Compare PAPER/live-candidate output and G3 daily replay on sealed evidence. Promote the dedicated qty-one LIVE coordinator only under the user's separately approved production cutover, with a fresh G3 strategy/day-specific arm; never inherit an old G arm file. Verify there is exactly one LIVE writer for the account/profile and that reconciliation/exit management starts with it.
8. Observe a full session including close, order reconciliation, daily source production, G3 report publication and dashboard/metrics freshness. Record the actual selected profile, config hash, run ID, service IDs and source-vintage identifiers in the acceptance record.

**Rollback**

Before any G3 fill, rollback can disable its writers/tasks, restore the verified old G source/task/consumer configuration, and enable only old G after readiness. After any G3 order/fill, first prevent new G3 entries while preserving its position and protective-order manager. Reconcile/close or explicitly transfer ownership through an approved recovery procedure; never merely switch namespaces while an order exists. Keep G3 state, broker identifiers and journals. Restore old G tasks and readers only when ownership is unambiguous; revalidate the old fingerprint, old control scope, source contract and duplicate-process exclusion. No historical artifacts should be deleted in either direction.

This is an operational dependency plan, not approval to execute the cutover. Dynamic environment overrides and future external task edits remain a final manifest-check requirement; the audit does not claim that text matching proves absence of every runtime dependency.
