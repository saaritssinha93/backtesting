# G3 migration: dashboard, backtesting, reports and data plan

Plan only. No source/config/data mutation, strategy or test execution, service changes, broker operations, or messages.

## Main conclusion

The dashboard already reads G3 historical packages in Dashboard Flow, but the active dashboard, scheduled daily publisher and analysis defaults still use G. G3 currently provides a frozen archive exporter and a research-only dated replay that depends on G-produced evidence. A complete migration needs a production G3 strategy and publication contract, not renamed G labels.

## Verified findings

- **F1 ? The public G3 backtest CLI verifies/exports a frozen historical archive; it is not a production daily replay.**
  Evidence: fno_v13_v10_g_3_backtest.py:1-6,12-36.
  Action: Implement a real G3 daily runner/publisher before changing schedules.

- **F2 ? G3 dated replay currently depends on a successful dated G run and its feature_ledger.csv + manifest + snapshot, and explicitly has execution_authority=False.**
  Evidence: research/g3_dated_replay.py:31-47,49-94.
  Action: Separate strategy-neutral verified source preparation from G execution; retain the dated research path as parity reference.

- **F3 ? Main dashboard is G-specific across identity, card/report/log/status/heartbeat/task/restart maps, live CSV projections and current-day timelines. Version checks reject other identities.**
  Evidence: log_dashboard_server.py:101-191,268-394,466-618,824,2919-2959,4539,4597-4617,4789,11881-11970; dashboard_fno_monitor.js:223.
  Action: Change producer and consumer contracts as one cutover; never merely relabel G success as G3.

- **F4 ? Flow already defaults G3 and preserves separate G/G2/G3 histories, but discovers CSVs by fixed roots/globs and orders by family/name, not a verified promotion/current manifest. G3 is classed generically backtest; G alone has explicit daily kind.**
  Evidence: dashboard_flow.py:20-24,109-146; dashboard_flow.js:24-34; dashboard_flow_compare.js:58,152.
  Action: Use explicit run kind/completion/strategy/config/source identities and accepted latest history/current pointers; keep partial/research/preview runs out of active defaults.

- **F5 ? G3 output root currently contains runs only, no root status.json/latest publisher. Latest observed full history summary is through 2026-10-09 with 48 sessions/98 trades. G main latest still says V13-V10-G SUCCESS for 2026-10-09.**
  Evidence: C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3 directory listing; C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/runs/2026-10-10/20261010T_through_20261009_pullbacks/summary.json; C:/TradingData/eqidv2/backtesting_result_v13_v10_g/latest/latest_backtesting_result_v13_v10_g.json.
  Action: Add independent G3 production status/latest/report and validated historical pointer; do not copy G latest into G3.

- **F6 ? Research/observability defaults point at G full-history, G runtime, G replay and G paper calibration roots; source discovery requires G bundle files and chooses latest by mtime.**
  Evidence: tools/v13_strategy_research.py:22-29; tools/v13_execution_research.py:22-26; ai_platform/observability/strategy_research.py:1020-1037.
  Action: Adapt data schemas and attribution/fingerprints for G3; keep independent versioned analysis and model cohorts.

- **F7 ? Current daily G report describes relaxed 09:25 LONG/session-effective G policy, while accepted G3 configuration is W1 exact next minute, LONG 1.10/SHORT1.20 and G2 staged stop. Old G projection pins 31 sessions/66 fills.**
  Evidence: backtesting_result_v13_v10_g_daily.py:305-323; research/g3_dated_replay.py:65-88; fno_v13_v10_g_projection_chart.py:18-49.
  Action: Generate G3 rule explanations from canonical spec and use G3-ledger projections; retain frozen G historical reports.

- **F8 ? G3 pullback report is an observer of frozen entries/exits, with hindsight actual-pullback labels and reused research split limitations. Warnings do not change actions or P&L.**
  Evidence: research/g3_daywise_backtest.py:1-6,176-195.
  Action: Keep observer data separate from production decision authority and make availability/research semantics visible.

- **F9 ? Old G/G2 data and predecessor source packages remain G3 provenance dependencies; dataset manifests pin paths/hashes. Generic G options master loading still searches old V5/V6 caches and all research nfo_master files.**
  Evidence: .codex_tmp/archive_inventory_20261010/data_audit.json; research/g3_dated_replay.py:54-55; fno_v13_v10_g_options_paper.py:259-267; fno_v13_v10_g_options_data.py:75-84,178-192.
  Action: Retain these at existing paths, logically mark historical/read-only; no move/delete/archive as part of migration.

## Relevant files and recommended actions

### Main operational dashboard

Switch active profile identity, data roots, card titles/IDs, report/log/status/heartbeat/task/restart wiring and timeline/filter thresholds together. Reject G/G2 evidence in active G3 views. Keep market/auth/freshness/disabled/readiness semantics. Keep shared CSS unless visual additions require it.

- `dashboard_fno_monitor.css`
- `dashboard_fno_monitor.js`
- `dashboard_ops_state.js`
- `fno_eq_id_monitor_detail.py`
- `log_dashboard_server.py`

### Dashboard Flow

Retain historical G/G2/G3 family selection and comparisons. Add explicit active G3 and archived historical labels, manifest-backed current/complete identity selection and daily versus cumulative run kind. Preserve read-only/auth/path boundaries, common-calendar comparison, missing evidence and ledger reconciliation.

- `bat/run_dashboard_flow.bat`
- `dashboard_flow.css`
- `dashboard_flow.html`
- `dashboard_flow.js`
- `dashboard_flow.py`
- `dashboard_flow_chart.js`
- `dashboard_flow_compare.js`
- `dashboard_flow_core.js`
- `tools/run_dashboard_flow.py`

### Daily backtest publication and source generation

Build a real current-date G3 publisher with verified raw inputs/shared feature generation, authoritative G3 execution, explicit date, complete/blocked states, atomic current pointer and independent immutable outputs. Do not point active schedule at archive-export CLI. Decouple G3 source generation from successful G replay while retaining provenance-compatible archived G replay.

- `backtesting_result_v13_v10_g_daily.py`
- `bat/migrate_backtesting_task_to_v13_v10_g.ps1`
- `bat/run_backtesting_result_v13_v10_g_1620.bat`
- `bat/schedule_backtesting_result_v13_v10_g_weekday.bat`
- `data_for_backtesting_verify.py`
- `eqidv2_runtime_paths.py`
- `fno_v13_v10_g_daily_replay.py`
- `fno_v13_v10_g_policy.py`
- `fno_v13_v10_g_selection.py`
- `fno_v13_v10_g_staged_replay.py`
- `wait_for_data_backtesting_ready.py`

### Accepted G3 and research/report exports

Preserve frozen G3 as immutable research baseline. Extract a reviewed production strategy adapter using accepted W1/LONG1.10/SHORT1.20 + exact next-minute/staged-stop rules. Reuse report accounting/provenance; do not activate six-arm tuning, entry previews, hindsight pullback labels or observer warnings as trade authority. Dated research replay is reference/test evidence, not production publisher.

- `fno_v13_v10_g_3_backtest.py`
- `research/g3_dated_replay.py`
- `research/g3_daywise_backtest.py`
- `research/g3_daywise_report.py`
- `research/g3_daywise_workbook.mjs`
- `research/g3_entry_preview.py`
- `research/g3_extend_results.py`
- `research/g3_freeze.py`
- `research/g3_pullback_followthrough.py`
- `research/g3_pullback_metrics.py`
- `research/g3_pullback_monitor.py`
- `research/g3_pullback_protocol_v1.json`
- `research/g3_pullback_report.py`
- `research/g3_pullback_selective.py`
- `research/g3_pullback_selective_features.py`
- `research/g3_pullback_selective_report.py`
- `research/g3_pullback_tuning.py`
- `research/g3_pullback_tuning_protocol_v1.json`
- `research/g3_results_report.py`
- `research/g3_sep24_raw_pool.py`
- `research/g3_six_confirmation_variants.py`

### Historical G/G2 replay and history extension

Retain explicit versioned historical behavior and source/fingerprint checks. Active scheduling/pointers should target G3; historical tools remain opt-in reference replays. Never relabel archived G/G2 P&L as G3 or overwrite their frozen configs.

- `fno_v13_v10_g_2_backtest.py`
- `fno_v13_v10_g_2_extend_results.py`
- `fno_v13_v10_g_2_long_leaders_backtest.py`
- `fno_v13_v10_g_2_relaxed_0925_backtest.py`
- `fno_v13_v10_g_backtest.py`
- `tools/correct_v13_v10_g_cutoff_bundle.py`
- `tools/extend_v13_v10_g2_dashboard_history.py`
- `tools/extend_v13_v10_g_dated_results.py`
- `tools/refresh_v13_v10_g_research_source.py`

### Live/paper log and net-result integration

Coordinate with execution audit: logger/net-result functions must use G3 identity, roots, version/fingerprint and strategy-specific stop/target/confirmation fields for future records. Preserve historical CSV/logs, broker-order identities and paper/live/options separation. Options rules need explicit G3 integration rather than a label-only change.

- `bat/run_fno_v13_v10_g_net_result.bat`
- `bat/run_fno_v13_v10_g_options_net_result.bat`
- `bat/run_fno_v13_v10_g_options_trade_logger.bat`
- `bat/run_fno_v13_v10_g_trade_logger.bat`
- `fno_v13_v10_g_identity.py`
- `fno_v13_v10_g_live.py`
- `fno_v13_v10_g_live_config.py`
- `fno_v13_v10_g_live_kite_session.py`
- `fno_v13_v10_g_options_config.py`
- `fno_v13_v10_g_options_paper.py`
- `fno_v13_v10_g_options_signals.py`
- `fno_v13_v10_g_paper.py`
- `fno_v6_live.py`
- `fno_v6_live_config.py`

### Research/analysis and observability

Make active G3 source registry/schema/identity explicit; generate new G3 strategy/execution/attribution/regime/prediction/walkforward/shadow cohorts from the G3 ledger and same source calendar. Existing G-only source discovery and required bundle structure are not directly compatible with G3 CSV package. Retain old G analyses as historical. Do not mix G calibration/training and G3 cohorts without labelled validation.

- `ai_platform/adapters/daily_replay.py`
- `ai_platform/adapters/equity_orders.py`
- `ai_platform/adapters/evidence.py`
- `ai_platform/adapters/options_orders.py`
- `ai_platform/adapters/snapshot.py`
- `ai_platform/adapters/status.py`
- `ai_platform/adapters/strategy.py`
- `ai_platform/observability/catalog.py`
- `ai_platform/observability/execution_research.py`
- `ai_platform/observability/feature_ledger.py`
- `ai_platform/observability/improvement_opportunities.py`
- `ai_platform/observability/live_trust.py`
- `ai_platform/observability/profitability.py`
- `ai_platform/observability/prospective_shadow.py`
- `ai_platform/observability/reconciliation.py`
- `ai_platform/observability/runtime_collector.py`
- `ai_platform/observability/shadow_automation.py`
- `ai_platform/observability/strategy_research.py`
- `bat/run_v13_shadow_prepare.bat`
- `bat/run_v13_shadow_seal.bat`
- `bat/run_v13_strategy_research_refresh.bat`
- `bat/schedule_v13_shadow_weekday.ps1`
- `tools/v13_execution_research.py`
- `tools/v13_prospective_shadow.py`
- `tools/v13_shadow_automation.py`
- `tools/v13_strategy_research.py`

### Static historical charts and projections

Keep existing G report/site/projection sources and published evidence labelled historical G. Any active G3 chart/report must consume verified G3 results and declare capital/cost/calendar basis; rebuild scenario inputs and validations separately rather than rename old G projections. Do not update hosting or publish in plan phase.

- `build_v13_v10_g_document.py`
- `docs/v13_v10_g_options/README.md`
- `docs/v13_v10_g_options/options.css`
- `docs/v13_v10_g_options/options.html`
- `docs/v13_v10_g_options/options.js`
- `docs/v13_v10_g_web/README.md`
- `docs/v13_v10_g_web/app.js`
- `docs/v13_v10_g_web/build.mjs`
- `docs/v13_v10_g_web/data.json`
- `docs/v13_v10_g_web/document.html`
- `docs/v13_v10_g_web/model.js`
- `docs/v13_v10_g_web/package.json`
- `docs/v13_v10_g_web/serve.mjs`
- `docs/v13_v10_g_web/style.css`
- `docs/v13_v10_g_web/tests.mjs`
- `fno_v13_v10_g_options_chart.py`
- `fno_v13_v10_g_options_fixed_profile.py`
- `fno_v13_v10_g_options_html.py`
- `fno_v13_v10_g_options_projection.py`
- `fno_v13_v10_g_projection_chart.py`

### Documentation and registries

Add a G3 migration/runbook and authoritative identity/rules/source/current-pointer contract; update operational instructions/defaults and registry examples after validated implementation. Keep dated G result reports unchanged with historical cross-links. Describe G3 no-trade days and original excluded dates explicitly. Documentation must distinguish accepted frozen research from production authority.

- `OPTIONS_V13_V10_G_PAPER_TRADING.md`
- `V13_V10_G_DAILY_BACKTEST.md`
- `V13_V10_G_DASHBOARD_INTEGRATION.md`
- `V13_V10_G_FULL_BACKTEST_RESULT_20261004.md`
- `V13_V10_G_IMPLEMENTATION_STATUS.md`
- `V13_V10_G_OPTIONS_BACKTEST.md`
- `docs/DASHBOARD_FLOW.md`
- `docs/OBSERVABILITY_ANALYSIS_2026-09-25.md`
- `docs/OBSERVABILITY_OPERATIONS.md`
- `docs/V13_IMPROVEMENT_OPPORTUNITIES.md`
- `docs/V13_V10_G_AI_PLATFORM_PLAN.md`
- `docs/V13_V10_G_CODEX_STAGE_PLAN.md`
- `docs/ai_platform/BASELINE.md`
- `docs/ai_platform/STAGE2_API.md`
- `docs/ai_platform/fixture_manifest.json`
- `docs/ai_platform/import_write_path_audit.md`
- `docs/ai_platform/profile_registry.json`
- `docs/ai_platform/source_registry.json`

### Regression and migration tests

Preserve historical G/G2/G3 fixtures; add G3 active-identity/no-stale-fallback tests, canonical accepted-rule parity, publisher failure/zero-trade semantics, source independence, full-history versus daily selection, ledger/P&L/calendar reconciliation, immutable archives, no execution from dashboards/analysis, and launcher/report/status wiring. Tests were inspected only, not run.

- `tests/dashboard_flow_core.test.cjs`
- `tests/test_ai_platform_api_stage2.py`
- `tests/test_ai_platform_observability_core.py`
- `tests/test_ai_platform_stage1.py`
- `tests/test_backtesting_result_v13_v10_g_daily.py`
- `tests/test_backtesting_result_v13_v10_g_launcher.py`
- `tests/test_correct_v13_v10_g_cutoff_bundle.py`
- `tests/test_dashboard_backtesting_card_label.py`
- `tests/test_dashboard_flow.py`
- `tests/test_dashboard_flow_compare.cjs`
- `tests/test_dashboard_flow_routes.py`
- `tests/test_dashboard_fno_eq_id_monitor.py`
- `tests/test_dashboard_fno_monitor_detail.py`
- `tests/test_dashboard_fno_multi_paper_views.py`
- `tests/test_dashboard_fno_v13_v10_g.py`
- `tests/test_dashboard_fno_v13_v10_g_improvement_views.py`
- `tests/test_dashboard_fno_v13_v10_g_observability_views.py`
- `tests/test_dashboard_fno_v13_v10_g_options.py`
- `tests/test_dashboard_fno_v13_v10_g_research_views.py`
- `tests/test_dashboard_ops_state.py`
- `tests/test_dashboard_v7_controls.py`
- `tests/test_extend_v13_v10_g_dated_results.py`
- `tests/test_fno_daily_strategy_dashboard.py`
- `tests/test_fno_dashboard_holidays.py`
- `tests/test_fno_today_combined_dashboard_publish.py`
- `tests/test_fno_v13_v10_g.py`
- `tests/test_fno_v13_v10_g_2.py`
- `tests/test_fno_v13_v10_g_2_0925_comparison.py`
- `tests/test_fno_v13_v10_g_2_long_leaders.py`
- `tests/test_fno_v13_v10_g_2_relaxed_0925.py`
- `tests/test_fno_v13_v10_g_2_unused_slots.py`
- `tests/test_fno_v13_v10_g_backtest_cli.py`
- `tests/test_fno_v13_v10_g_daily_replay.py`
- `tests/test_fno_v13_v10_g_expansion.py`
- `tests/test_fno_v13_v10_g_launchers.py`
- `tests/test_fno_v13_v10_g_live_execution.py`
- `tests/test_fno_v13_v10_g_live_strategy.py`
- `tests/test_fno_v13_v10_g_morning.py`
- `tests/test_fno_v13_v10_g_options_backtest.py`
- `tests/test_fno_v13_v10_g_options_data.py`
- `tests/test_fno_v13_v10_g_options_execution.py`
- `tests/test_fno_v13_v10_g_options_paper.py`
- `tests/test_fno_v13_v10_g_options_projection.py`
- `tests/test_fno_v13_v10_g_options_signals.py`
- `tests/test_fno_v13_v10_g_paper.py`
- `tests/test_fno_v13_v10_g_pipeline.py`
- `tests/test_fno_v13_v10_g_production_replay.py`
- `tests/test_fno_v13_v10_g_projection_chart.py`
- `tests/test_fno_v13_v10_g_promotion.py`
- `tests/test_fno_v13_v10_g_promotion_consumers.py`
- `tests/test_fno_v13_v10_g_staged_runtime.py`
- `tests/test_fno_v13_v10_g_two_bar.py`
- `tests/test_g3_dated_replay.py`
- `tests/test_g3_daywise_backtest.py`
- `tests/test_g3_extend_results.py`
- `tests/test_g3_pullback_followthrough.py`
- `tests/test_g3_pullback_metrics.py`
- `tests/test_g3_pullback_monitor.py`
- `tests/test_g3_pullback_selective.py`
- `tests/test_g3_pullback_selective_features.py`
- `tests/test_g3_pullback_tuning.py`
- `tests/test_observability_cli.py`
- `tests/test_observability_data_features.py`
- `tests/test_observability_execution_shadow.py`
- `tests/test_observability_improvement_opportunities.py`
- `tests/test_observability_live_failopen.py`
- `tests/test_observability_live_integration.py`
- `tests/test_observability_live_trust.py`
- `tests/test_observability_metric_contract.py`
- `tests/test_observability_ops.py`
- `tests/test_observability_reconciliation_profitability.py`
- `tests/test_observability_runtime_collector.py`
- `tests/test_observability_shadow_automation.py`
- `tests/test_observability_strategy_research.py`
- `tests/test_refresh_v13_v10_g_research_source.py`

## Data retention and active selection

Historical G/G2 stays available for reproducibility and read-only comparison. Preserve original labels and paths. Active G3 requires its own verified producer identity, dated immutable runs and atomic current pointers. Keep historic full-history, daily production, research and preview run kinds distinct; directory sorting alone must not select the active strategy.

- `backtesting_result_v13_v10_g`: Retain all immutable dated runs, source snapshots/feature ledgers and reports/logs. Stop being active default only after production G3 validation; still needed by current G3 dated importer.
- `fno_oi/strategy_research/v13_v10_g_full_history`: Retain all source bundles/manifests and historical G history; read-only comparison.
- `fno_oi/strategy_research/v13_corrected_v10_g`: Retain frozen configurations, stock/options histories and projections; source dependency and historical label.
- `fno_oi/strategy_research/v13_corrected_v10_g_2`: Retain all G2 frozen histories, simulations, sources and extension proofs; read-only comparison.
- `fno_oi/strategy_research/v13_corrected_v10_g_3`: Retain entire frozen baseline, source study, pullback models/research and all manifests; production promotion creates separate identity without mutating frozen artifact flags/hashes.
- `backtesting_result_v13_v10_g_3`: Retain historical full-history/daily/research/preview runs separately; add explicit production run-type/current pointers without overwrites.
- `fno_oi/v13_v10_g_live`: Preserve old paper/live order state and logs; coordinate position reconciliation with execution audit and never reuse old order state as G3.
- `v13_v10_g_strategy_research; v13_v10_g_execution_research`: Preserve historical analysis, shadow cohorts and provenance; create versioned G3 cohort outputs.
- `Common market data, auth, tokens, API/Kite settings and historical source dependencies`: Keep in place. Reuse shared verified market data; no credential changes from dashboard/report migration.

## Verification after implementation approval

- Exact accepted rule parity on frozen known trades plus new-date adversarial fixtures: 1.10 vs 1.20 LONG boundary, SHORT1.20, exact +1 minute, absent/late confirmation, staged stop timing and targets.
- Production G3 source preparation works without publishing a G result and cannot accept wrong day/strategy/incomplete/stale/hash-mismatched snapshot or feature ledger.
- True completed zero-trade session publishes zero with eligible date; failure, missing coverage and excluded date never masquerade as zero. Preserve October1 gap in historical G3.
- Atomic G3 status/current/report publication; wrong identity/root never appears healthy; heartbeat stops before final status; telemetry failure cannot repeat authoritative work.
- Main dashboard active cards, logger/net P&L, monitor thresholds, timeline and manual restart all resolve same G3 profile; legacy G success yields mismatch/unavailable.
- Flow default uses promoted complete G3 history; dated/preview/research dirs cannot supersede it. G/G2 archive IDs remain selectable without writes. Compare calendars and reconciliation unchanged.
- G3 ledger aggregates exactly to daily/session/monthly P&L/cost/counts; PF from executed net trades; NaN/missing data unavailable; drawdown and filters/CSV export agree.
- Analysis/shadow cohorts validate G3 fingerprints and source version; old model/calibration cannot silently act on G3. Pullback observer never changes execution.
- Archive SHA checks before/after migration prove G/G2/frozen G3/provenance unchanged. Shared market/auth/API and dashboard authentication remain unchanged.
- Inspect responsive light/dark main dashboard and Flow, current/stale/blocked/disarmed states and live/paper separation after implementation; no UI/service action in this plan.

## Boundaries

- Static audit and small JSON metadata reads only; no large market-data/CSV content scan or strategy/test execution.
- File groups include integration-review surfaces; not every listed file necessarily needs edits. CSS/auth helpers should remain unchanged unless required.
- Historical G/G2 remains read-only as a product/operational policy; no OS ACL change or physical archive operation is proposed.
- Full live order transition and scheduler activation controlled by parent execution/operations plan.

## Exact route integration map

- **GET /** (`log_dashboard_server.py:7117-7133`): Authenticated main operational dashboard; embedded card/control UI consumes /api/snapshot. Keep URL/auth. Select active G3 profile/card group and verified G3 producer roots.
- **GET /api/snapshot** (`log_dashboard_server.py:7150-7154`): _snapshot returns card statuses/log/report projections and timeline data from G maps. Migrate G3 identity, task/restart/status/heartbeat and source rules together. Include observed and expected fingerprints; no fallback to archived G success.
- **GET /api/fno-monitor?date=YYYY-MM-DD** (`log_dashboard_server.py:7155-7169; dashboard_fno_monitor.js:186`): _fno_eq_id_stock_detail -> fno_eq_id_monitor_detail; frontend hardcodes G heading. Read version-qualified G3 root/threshold contract and label; retain dated G evidence only via explicit historical profile selector if required.
- **GET /api/log?name=<allowlisted-card>** (`log_dashboard_server.py:7170-7180`): resolve_log_target uses LOG_IDS/report maps; _backtesting_v13_v10_g_view at2455 picks dated running G log or dedicated G latest report. Versioned G3 allowlist and requested date/report identity checks. Retain historical G log references; never permit arbitrary paths.
- **POST /api/restart; POST /api/kill** (`log_dashboard_server.py:7183-7196`): Authenticated main dashboard controls. Only approved G3 launcher/task contracts become active. Auth, kill semantics and protected execution controls stay in place; plan phase performs none.
- **GET /dashboard-flow[/]** (`log_dashboard_server.py:7134-7136; main header9378/10084`): _send_flow_html embeds local Flow assets; same authentication, new-tab main header link. Keep URL and read-only UI, active G3 indicator plus explicit historical G/G2/G3 choices.
- **GET /api/dashboard-flow?run=<discovered-id>** (`log_dashboard_server.py:7137-7149; dashboard_flow.py:304-339`): load_flow_data defaults first discovered run; accepts only IDs from discovery. _load_trades at151 prefers trades_with_pullbacks.csv over portfolio_trades.csv. Bind discovered IDs to verified run manifest/role/strategy. Production financial source is explicit portfolio ledger from manifest; pullback-enriched CSV can be validated secondary overlay. Reject wrong role/strategy/hash/incomplete production package; do not use newer research filename as active result.
- **Standalone 127.0.0.1:8790/dashboard-flow** (`tools/run_dashboard_flow.py:20-59; bat/run_dashboard_flow.bat`): FlowPreviewHandler reuses authenticated GET rendering, rejects all POSTs. Keep loopback/read-only/temp-auth behavior. Do not restart main8787 from this launcher.

## Proposed G3 publication contract

Base: `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3`. New production runs: `production/runs/<YYYY-MM-DD>/<run-id>/`. New cumulative history: `history/runs/<through-date>/<run-id>/ (new validated cumulative outputs only; existing runs tree kept unchanged as historical/research input)`. Existing evidence stays unchanged.

- `status.json`: Current attempted production status with session, state, run id, exact G3 strategy/version/fingerprint; explicit blocked/failed/waiting states.
- `latest/latest_backtesting_result_v13_v10_g_3.json`: Current attempt/status + verified successful artifact references; do not retain prior success metrics on failure.
- `latest/latest_backtesting_result_v13_v10_g_3.md`: Human-readable exact-date G3 report explaining accepted rules and exclusions.
- `latest/current_run.json`: Atomic pointer to last validated production run with manifest hash and role=production_daily; paired current status makes stale dates explicit.
- `latest/current_history.json`: Independent atomic pointer to last accepted cumulative G3 history manifest, never a daily/preview package.
- `reports/<YYYY-MM-DD>/backtesting_result_v13_v10_g_3.md`: Immutable date-specific G3 report per completed attempt/run policy.
- `production/runs/.../run_manifest.json`: Written last; complete, status, role, date/calendar, producer version, strategy/version/config fingerprint, source manifests/hashes, artifact paths/hashes, coverage/exclusions, execution model/cost/capital, historical vs production authority.
- `production/runs/.../portfolio_trades.csv`: Authoritative execution ledger with G3 identity/rules fields, filled/portfolio executed flags, cost/gross/net attribution.
- `production/runs/.../daily_results.csv`: Exact dated accounting; retain zero trades only for verified eligible completed session.
- `production/runs/.../feature_ledger.csv + manifest and input snapshot references`: Verified canonical source evidence independent of the historical G strategy decision stage.
- `runtime_status/backtesting_result_v13_v10_g_3.status and .heartbeat`: Existing runtime status mechanism with G3 session identity; exact relative shared runtime paths resolved through runtime helper.
- `run_logs or established LOG_DIR/backtesting_result_v13_v10_g_3_<date>.log`: New G3 logs preserve G historical logs; publisher/run id links consistent.

Active operational dashboard reads production current attempt. Flow default reads accepted cumulative G3 current_history; daily production is separate selectable kind. Historical G/G2 remain versioned read-only comparisons. Existing G3 runs including previews remain historical/research roles and cannot win production selection solely by directory sort.

## Exact protected sources

- `fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/frozen_config.json`
- `fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/final/V13_V10_G/portfolio_trades.csv`
- `fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/daily_detailed.csv`
- `fno_oi/strategy_research/v13_v10_g_full_history/run_20260925_cutoff_corrected_through_20260923/`
- `fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1/`
- `fno_oi/strategy_research/v13_corrected_v10_g_3/run_20261008_six_confirmation_variants/`
- `fno_oi/strategy_research/v13_corrected_v10_g_3/pullback_selective_v2_20261008_final/`
- `backtesting_result_v13_v10_g_3/runs/2026-10-08/20261008T164833_pullbacks/`
- `backtesting_result_v13_v10_g_3/runs/2026-10-10/20261010T_through_20261009_pullbacks/`
- `fno_oi/strategy_research/v13_corrected_v10_g_2/run_20261010_staged125_to100_120m_through_20261009/`
- `fno_oi/strategy_research/v13_v10_g_full_history/run_20261010_through_20261009_dated_extension/`
- `fno_oi/strategy_research/fno_historical_repair_20260827/`
- `fno_oi/strategy_research/v13_corrected_v9/run_20260913/dataset/dataset_manifest.json and every source dependency path pinned by it`
- `fno_oi/strategy_research/v13_corrected_v5/ and v6_derivative_backtests/market_cache/ (current options input dependencies)`
- `fno_oi/strategy_research/**/nfo_master_*.parquet (generic current options discovery)`
