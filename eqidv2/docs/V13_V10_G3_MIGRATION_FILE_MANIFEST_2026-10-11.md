# G-3 migration: file, task and data manifest

**Original inventory: 11 October 2026; implementation coverage appended 12 October 2026.** Read alongside the [migration plan](V13_V10_G3_MIGRATION_PLAN_2026-10-11.md).

The merged audit covers **548 individually named existing files**, **41 proposed new files**, **45 observed scheduled tasks**, and the runtime/source paths below. The initial automated scan read 1710 non-secret source/support texts and identified 441 source/reference/dependency files with zero Python parse errors; manual operational and dashboard audits expanded that set.

Paths in the source tables are relative to the project root unless stated otherwise. Credentials are listed by filename only; their contents were not part of this audit. Generated data is covered by exact roots and artifact patterns, rather than listing hundreds of thousands of candle files individually.

## Action legend

| Code | Meaning |
| --- | --- |
| M | Review/refactor active integration; an edit is needed only where the G-specific contract changes. |
| C | Preserve explicit historical/rollback launcher; switch approved callers and guard legacy reactivation. |
| T | Extend tests/fixtures while retaining historical regression assertions. |
| D | Update active documentation/registry after implementation. |
| V | Verify shared or transitive compatibility; normally retain unchanged. |
| H | Preserve historical/research evidence and semantics; no relabelling as production G3. |
| S | Protect shared infrastructure, credentials or state; names only for secrets. |
| U | Preserve unrelated V14 work; verify only shared dependency compatibility. |
| NEW | Proposed implementation file, not created as executable/config by this plan. |

All codes are planning dispositions. No file is approved for deletion, moving or archiving. A shared file can need a small profile-aware change while all its legacy behavior remains protected.

## Existing file coverage

| Action | Existing files |
| --- | ---: |
| C | 28 |
| D | 19 |
| H | 72 |
| M | 100 |
| S | 63 |
| T | 94 |
| U | 44 |
| V | 128 |

### Root source, documentation and protected configuration (201 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `../.gitignore` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `FNO_V6_LIVE_STRATEGY.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `FNO_V6_STRATEGY_AND_FLOWS.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `OPTIONS_V13_V10_G_PAPER_TRADING.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `V13_V10_G_DAILY_BACKTEST.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `V13_V10_G_DASHBOARD_INTEGRATION.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `V13_V10_G_FULL_BACKTEST_RESULT_20261004.md` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `V13_V10_G_IMPLEMENTATION_STATUS.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `V13_V10_G_OPTIONS_BACKTEST.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `access_token.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token2.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token3.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token4.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token5.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token6.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token7.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `access_token8.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key2.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key3.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key4.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key5.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key6.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key7.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `api_key8.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `auth_v2_state.json` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `authentication_backup.py` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `authentication_v2.py` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `authentication_v3.py` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `avwap_5min_ID_v2_backtesting.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `avwap_5min_ID_v3_backtesting.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `avwap_5min_ID_v5_backtesting.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `avwap_5min_ID_v6_backtesting.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `avwap_5min_ID_v7_backtesting.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `avwap_5min_ID_v7_candidate_scan.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `avwap_combined_runner.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `backtesting_result_v13_v10_g_daily.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `build_v13_v10_g_document.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `dashboard_flow.css` | V | Regression-check shared UI/assets; preserve design unless contract/status wiring requires change. |
| `dashboard_flow.html` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `dashboard_flow.js` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `dashboard_flow.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `dashboard_flow_chart.js` | V | Regression-check shared UI/assets; preserve design unless contract/status wiring requires change. |
| `dashboard_flow_compare.js` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `dashboard_flow_core.js` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `dashboard_fno_monitor.css` | V | Regression-check shared UI/assets; preserve design unless contract/status wiring requires change. |
| `dashboard_fno_monitor.js` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `dashboard_ops_state.js` | V | Regression-check shared UI/assets; preserve design unless contract/status wiring requires change. |
| `data_for_backtesting_verify.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `eqidv2_conf_tier_c_live_scan.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_decision_funnel.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_eod_scheduler_for_15mins_data.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_eod_scheduler_for_5mins_data_live_minimal.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_final_conf_live_bootstrap.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_late_bb10_compression.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_live_combined_analyser_csv_v15.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_nifty_guard_fetcher_supervised_v16_5min.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_runtime_manifest.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `eqidv2_runtime_paths.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `eqidv2_setup_conf_loader.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_signal_discovery_v7_5min_id_persistent.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `eqidv2_v11_live_overlay.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `filtered_stocks_MIS.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_eq_id_monitor_detail.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_equity_1m_backfill.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_equity_5m_repair.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_equity_fetch_1min.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_live_evidence.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_live_profile.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_multi_paper_session.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_backfill_5min.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_backtest_provenance.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_common.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_0925_0930_0935_v4.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_0925_0930_best_combo_v3.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_0925_0930_pf_v3.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_optimize.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_ema_confirm_sweep.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_eod_qc.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_feature_ranker.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_fetch_1m_history.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_fetch_5min.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_fetch_5min_fast_production.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_fetch_5min_fast_shadow.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_hybrid_data.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_oi_rank_history.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_oi_universe.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_options_atm_fetch_5min.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `fno_v10_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_backtest_config.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_experiment_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_experiment_config.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_followup_challenger_research.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_gap_guard_research.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_repaired_snapshot_rerun.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_stage7_postselection_combo_research.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v10_unified_5m_1m_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v11_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v11_execution_runtime.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v11_gap_runtime.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v12_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v12_execution_runtime.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v12_selection_runtime.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v12_variant_registry.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_corrected_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_corrected_ungated_oi_research.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_corrected_v2_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_corrected_v3_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_corrected_v5_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_run_vintage.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_a_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_b_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_c_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_d_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_d_research.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_e_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_f_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_g_2_0925_comparison.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_2_backtest.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_2_extend_results.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_2_long_leaders_backtest.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_2_relaxed_0925_backtest.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_2_unused_slots_data.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_2_unused_slots_research.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_3_backtest.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `fno_v13_v10_g_3l_capital_replay.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_backtest.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_daily_replay.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_expansion_research.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_identity.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_live.py` | C | Keep explicit G rollback entrypoint; add G3 sibling and switch approved callers. |
| `fno_v13_v10_g_live_config.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_live_kite_session.py` | C | Keep explicit G rollback entrypoint; add G3 sibling and switch approved callers. |
| `fno_v13_v10_g_options_backtest.py` | H | Preserve G option research; add separate G3-source reporting if required. |
| `fno_v13_v10_g_options_chart.py` | H | Preserve G option research; add separate G3-source reporting if required. |
| `fno_v13_v10_g_options_config.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_options_data.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_options_execution.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_options_fetch.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_options_fixed_profile.py` | H | Preserve G option research; add separate G3-source reporting if required. |
| `fno_v13_v10_g_options_html.py` | H | Preserve G option research; add separate G3-source reporting if required. |
| `fno_v13_v10_g_options_one_lot_execution.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_options_paper.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_options_projection.py` | H | Preserve G option research; add separate G3-source reporting if required. |
| `fno_v13_v10_g_options_signals.py` | M | Adapt equity-source profile/identity; preserve option mechanics and PAPER-only guard. |
| `fno_v13_v10_g_paper.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_policy.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_projection_chart.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_research.py` | H | Retain original G/G2 research/reference behavior and provenance. |
| `fno_v13_v10_g_selection.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_staged_replay.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v13_v10_g_two_bar.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v10_research.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v5_derivative_data.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v6_portfolio_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v9_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v13_v9_data.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v5_hybrid_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v5_hybrid_optimize.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v5_live.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v5_live_config.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v6_corrected_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v6_live.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v6_live_config.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v6_live_kite_session.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `fno_v8_combined_paper_config.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v8_combined_paper_control.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v8_combined_paper_market_data.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v8_combined_paper_session.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v8_strict_v6_logic_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v8_v9_last_10_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `fno_v8_windowed_1m_entry_backtest.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `hilega_milega_setups.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `log_dashboard_server.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `moving_files.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `nse_intraday_costs.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `nse_market_calendar.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `preopen_session_autofix.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `preopen_session_healthcheck.py` | M | Review/refactor active profile contract and integration; preserve legacy G behavior. |
| `request_token.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token2.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token3.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token4.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token5.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token6.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token7.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `request_token8.txt` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `stocks_tokens_cache.json` | S | Protect auth/credential infrastructure; values excluded from plan. |
| `trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `v17D_exit_resolver.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `wait_for_data_backtesting_ready.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |
| `walkforward_gate.py` | V | Transitive/reference dependency: verify compatibility; no default rewrite or removal. |
| `zerodha_kite_export.py` | S | Protect shared data/broker/auth utility; review compatibility without changing unrelated behavior. |

### ai_platform (45 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `ai_platform/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/common.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/daily_replay.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/equity_orders.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/evidence.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/options_orders.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/snapshot.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/status.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/adapters/strategy.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/app.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/auth.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/cache.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/config.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/errors.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/registry.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/repository.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/api/serialization.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/catalog.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/context.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/data_quality.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/execution_research.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/feature_ledger.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/improvement_opportunities.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/journal.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/live_trust.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/metrics.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/profitability.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/prospective_shadow.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/reconciliation.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/redaction.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/runtime.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/runtime_collector.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/shadow_automation.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/strategy_research.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/structured_logging.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/observability/tracing.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/research/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/research/v13h_execution.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/research/v13h_research.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/schemas/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/services/__init__.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |
| `ai_platform/services/accounting.py` | M | Review version-aware source/schema/analysis consumer; edit only G-specific assumptions. |

### bat (86 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `bat/assert_fno_oi_fast_production_trial_exclusive.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/check_pf_supervisor_health.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/dashboard_public_link_helpers.ps1` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/fno_oi_fast_production_trial_date_gate.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/harden_scheduled_task.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/migrate_backtesting_task_to_v13_v10_g.ps1` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/migrate_fno_v6_tasks_to_v13_v10_g.ps1` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/observability/Test-V13V10GLiveTrust.ps1` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/rotate_log_if_large.ps1` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_authentication_backup.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_authentication_v2.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_backtesting_result_v11_1600.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_backtesting_result_v13_v10_g_1620.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_dashboard_flow.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_data_for_backtesting_parallel.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_eqidv2_eod_scheduler_for_15mins_data.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_5min_rankings_backfill.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_oi_eod_qc.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_oi_feature_ranker.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_oi_fetch_5min.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_oi_fetch_5min_fast_production.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_oi_fetch_5min_fast_shadow.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_oi_universe.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_options_atm_fetch_5min.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_today_data_repair_20260831.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v10_v11_v12_paper_session.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v13_v10_g_confirmation_1min.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_equity_1min_feed.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_live_kite_qty1.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_live_long.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_live_short.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_net_result.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_options_live_long.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_options_live_short.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_options_net_result.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_options_trade_logger.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_scanner_5min.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v13_v10_g_trade_logger.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v5_confirmation_1min.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v5_live_long.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v5_live_short.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v5_net_result.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v5_scanner_5min.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v5_trade_logger.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v6_confirmation_1min.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_equity_1min_feed.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_live_kite_qty1.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_live_long.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_live_short.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_net_result.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_scanner_5min.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v6_trade_logger.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/run_fno_v8_combined_paper_session.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_fno_v8_v9_last_10_backtest.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_log_dashboard_public_link.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_log_dashboard_public_link_capture.ps1` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_log_dashboard_public_link_scheduled.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_log_dashboard_restart_keep_url.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_log_dashboard_server.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_log_dashboard_stop.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_moving_files_1545.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_nifty_guard_fetcher_v15.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_nifty_guard_fetcher_v16_5min.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_pf_supervisor_healthcheck.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_preopen_session_autofix.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_preopen_session_healthcheck.bat` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |
| `bat/run_v13_shadow_prepare.bat` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/run_v13_shadow_seal.bat` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/run_v13_strategy_research_refresh.bat` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/run_zerodha_kite_export_scheduler.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/run_zerodha_kite_export_stop.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/schedule_authentication_v2_weekday.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/schedule_backtesting_result_v11_weekday.bat` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/schedule_backtesting_result_v13_v10_g_weekday.bat` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/schedule_fno_oi_weekday.bat` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/schedule_fno_oi_weekday.ps1` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/schedule_fno_v13_v10_g_options_weekday.ps1` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/schedule_fno_v6_live_kite_qty1_weekday.ps1` | C | Preserve original launcher; add G3 route and prevent legacy reactivation after approved cutover. |
| `bat/schedule_log_dashboard_weekday.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/schedule_v13_shadow_weekday.ps1` | M | Review task/profile mapping and recovery; apply only during approved migration. |
| `bat/schedule_zerodha_kite_export_weekday.bat` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/send_gmail_api.py` | S | Protect shared service/auth/export infrastructure; verify continuity. |
| `bat/supervise_command.ps1` | V | Verify shared task/launcher/supervisor dependency and preserve other strategies. |

### configs (6 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `configs/observability/compose.yaml` | M | Review profile, labels, metrics and source mappings; preserve shared settings. |
| `configs/observability/grafana/dashboards/data-parity-research.json` | M | Review profile, labels, metrics and source mappings; preserve shared settings. |
| `configs/observability/grafana/dashboards/trading-control-plane.json` | M | Review profile, labels, metrics and source mappings; preserve shared settings. |
| `configs/observability/metric-contract.yml` | M | Review profile, labels, metrics and source mappings; preserve shared settings. |
| `configs/observability/prometheus/rules/trading-alerts.yml` | M | Review profile, labels, metrics and source mappings; preserve shared settings. |
| `configs/observability/slo.yml` | M | Review profile, labels, metrics and source mappings; preserve shared settings. |

### docs (30 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `docs/ARCHIVE_CANDIDATES_REVIEW_2026-10-10.md` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/DASHBOARD_FLOW.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/OBSERVABILITY_ANALYSIS_2026-09-25.md` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/OBSERVABILITY_OPERATIONS.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/V13_IMPROVEMENT_OPPORTUNITIES.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/V13_V10_G_AI_PLATFORM_PLAN.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/V13_V10_G_CODEX_STAGE_PLAN.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/ai_platform/BASELINE.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/ai_platform/STAGE2_API.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/ai_platform/fixture_manifest.json` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/ai_platform/import_write_path_audit.md` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/ai_platform/profile_registry.json` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/ai_platform/source_registry.json` | D | Update active operational instructions/registry after implementation; preserve dated evidence. |
| `docs/v13_v10_g_options/README.md` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_options/options.css` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_options/options.html` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_options/options.js` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/.openai/hosting.json` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/README.md` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/app.js` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/build.mjs` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/data.json` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/dist/V13_V10_G_INTERACTIVE_BACKTEST.html` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/dist/index.html` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/document.html` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/model.js` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/package.json` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/serve.mjs` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/style.css` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |
| `docs/v13_v10_g_web/tests.mjs` | H | Retain original historical G/archival evidence; create separately labelled G3 outputs. |

### research (27 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `research/__init__.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g2_audit_history_v2.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g2_audit_report_v2.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g2_audit_selection_v2.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g2_session_forensic_audit.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g2_session_historical_volume_test.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g2_session_preflight_20261008.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_dated_replay.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_daywise_backtest.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_daywise_report.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_daywise_workbook.mjs` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_entry_preview.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_extend_results.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_freeze.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_followthrough.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_metrics.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_monitor.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_protocol_v1.json` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_report.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_selective.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_selective_features.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_selective_report.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_tuning.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_pullback_tuning_protocol_v1.json` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_results_report.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_sep24_raw_pool.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |
| `research/g3_six_confirmation_variants.py` | H | Preserve research/archive semantics; use as reference, not production entry authority. |

### tests (97 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `tests/__init__.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/dashboard_flow_core.test.cjs` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/daily_running.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/daily_success.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/equity_live_cancelled.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/equity_paper_closed.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/option_closed.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/option_open.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/option_unresolved.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/fixtures/ai_platform/status_stale.json` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_ai_platform_api_stage2.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_ai_platform_observability_core.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_ai_platform_stage1.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_backtesting_result_v13_v10_g_daily.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_backtesting_result_v13_v10_g_launcher.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_correct_v13_v10_g_cutoff_bundle.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_backtesting_card_label.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_flow.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_flow_compare.cjs` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_flow_routes.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_eq_id_monitor.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_monitor_detail.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_multi_paper_views.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_v13_v10_g.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_v13_v10_g_improvement_views.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_v13_v10_g_observability_views.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_v13_v10_g_options.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_fno_v13_v10_g_research_views.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_ops_state.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_dashboard_v7_controls.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_extend_v13_v10_g_dated_results.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_daily_strategy_dashboard.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_dashboard_holidays.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_eq_id_monitor_detail.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_equity_fetch_1min.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_oi_hybrid_data.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_today_combined_dashboard_publish.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_d.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_2.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_2_0925_comparison.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_2_long_leaders.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_2_relaxed_0925.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_2_unused_slots.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_backtest_cli.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_daily_replay.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_expansion.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_launchers.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_live_execution.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_live_strategy.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_morning.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_options_backtest.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_options_data.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_options_execution.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_options_paper.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_options_projection.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_options_signals.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_paper.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_pipeline.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_production_replay.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_projection_chart.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_promotion.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_promotion_consumers.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_staged_runtime.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_g_two_bar.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v13_v10_h.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v5_live.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v6_live.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_fno_v6_live_kite_session.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_dated_replay.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_daywise_backtest.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_extend_results.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_pullback_followthrough.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_pullback_metrics.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_pullback_monitor.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_pullback_selective.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_pullback_selective_features.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_g3_pullback_tuning.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_cli.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_data_features.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_execution_shadow.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_improvement_opportunities.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_live_failopen.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_live_integration.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_live_trust.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_metric_contract.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_ops.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_reconciliation_profitability.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_runtime_collector.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_shadow_automation.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_observability_strategy_research.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_preopen_fast_production_readiness.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_refresh_v13_v10_g_research_source.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_supervise_command_run_id_compat.py` | T | Retain historical assertions; extend G3 integration/compatibility coverage where relevant. |
| `tests/test_v14_baseline.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `tests/test_v14_path_evidence.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `tests/test_v14_pullbacks.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |

### tools (15 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `tools/build_v13_v10_h_source.py` | H | Retain historical/reference helper; do not schedule as active G3 producer. |
| `tools/correct_v13_v10_g_cutoff_bundle.py` | H | Retain historical/reference helper; do not schedule as active G3 producer. |
| `tools/extend_v13_v10_g2_dashboard_history.py` | H | Retain historical/reference helper; do not schedule as active G3 producer. |
| `tools/extend_v13_v10_g_dated_results.py` | H | Retain historical/reference helper; do not schedule as active G3 producer. |
| `tools/fno_daily_strategy_dashboard.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |
| `tools/fno_today_six_strategy_replay.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |
| `tools/fno_v6_isolated_challenger_replay.py` | H | Retain historical/reference helper; do not schedule as active G3 producer. |
| `tools/observability_experiment_registry.py` | V | Verify adapter/launcher compatibility; retain generic behavior. |
| `tools/refresh_v13_v10_g_research_source.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |
| `tools/run_dashboard_flow.py` | V | Verify adapter/launcher compatibility; retain generic behavior. |
| `tools/run_g3_dated_once.py` | H | Retain historical/reference helper; do not schedule as active G3 producer. |
| `tools/v13_execution_research.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |
| `tools/v13_prospective_shadow.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |
| `tools/v13_shadow_automation.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |
| `tools/v13_strategy_research.py` | M | Make active strategy/source/cohort explicit; preserve historical readers. |

### v14 (41 files)

| File | Action | Review purpose |
| --- | --- | --- |
| `v14/README.md` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/__init__.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/__init__.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/execution.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/frozen_config.json` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/snapshot_manifest.json` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/ai_platform/observability/data_quality.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/ai_platform/observability/feature_ledger.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_2_0925_comparison.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_2_backtest.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_2_extend_results.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_3_backtest.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_backtest.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_daily_replay.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_identity.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_live_config.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_policy.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_selection.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_staged_replay.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/fno_v13_v10_g_two_bar.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/research/g2_session_forensic_audit.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/research/g3_freeze.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/research/g3_results_report.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/research/g3_sep24_raw_pool.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source/research/g3_six_confirmation_variants.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source_g_config.json` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/source_study_provenance.json` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/_baseline/upstream_manifest.json` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/baseline.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/data.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/diagnosis_report.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/diagnostics_baseline.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/features.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/index_data.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/index_fetch.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/monitoring.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/path_evidence.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/pullbacks.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/replay.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/research.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |
| `v14/selection.py` | U | Preserve unrelated V14 work; verify shared dependency compatibility. |

## Proposed new files

Names are concrete planning proposals. During implementation, a suitable existing profile-neutral module may replace a proposed shared helper, with the manifest updated accordingly. G3 wrappers should reuse the reviewed core rather than duplicate strategy logic.

| Proposed path | Purpose |
| --- | --- |
| `backtesting_result_v13_v10_g_3_daily.py` | Status/manifest/report publisher and atomic current pointers. |
| `bat/migrate_fno_g_to_g3.ps1` | Dry-run/export/apply/rollback of exact task/profile routing, with explicit execution gates. |
| `bat/run_backtesting_result_v13_v10_g_3_1620.bat` | G3 daily backtest plus explicitly versioned analysis/shadow follow-ups. |
| `bat/run_fno_v13_v10_g_3_confirmation_1min.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_equity_1min_feed.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_live_kite_qty1.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_live_long.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_live_short.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_net_result.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_options_live_long.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_options_live_short.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_options_net_result.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_options_trade_logger.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_scanner_5min.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/run_fno_v13_v10_g_3_trade_logger.bat` | G3 launcher with explicit profile/mode, isolated state/log/status and matching supervision. |
| `bat/schedule_backtesting_result_v13_v10_g_3_weekday.bat` | Date-aware weekday G3 replay scheduling after source independence. |
| `bat/schedule_fno_v13_v10_g_3_live_kite_qty1_weekday.ps1` | Separately gated G3 quantity-one LIVE scheduling. |
| `bat/schedule_fno_v13_v10_g_3_options_weekday.ps1` | G3-source options PAPER scheduling. |
| `bat/schedule_fno_v13_v10_g_3_weekday.ps1` | G3 input/PAPER service scheduling initially disabled until gates pass. |
| `configs/strategies/v13_v10_g_3_production_v1.json` | Normalized effective14-setup spec, baseline hash, profile and execution-model metadata. |
| `docs/V13_V10_G3_OPERATIONS.md` | Approved effective rules, configuration, launch/recovery and rollback runbook. |
| `fno_strategy_registry.py` | Central active profile/runtime/task/report contract; use existing registry if suitable instead. |
| `fno_strategy_source_snapshot.py` | Proposed strategy-neutral sealed source/feature preparation extracted from G daily flow. |
| `fno_v13_v10_g_3_daily_replay.py` | Actual date-aware production G3 replay; distinct from frozen export. |
| `fno_v13_v10_g_3_live.py` | Explicit G3 scanner/confirmation/PAPER/logging entrypoint through shared engine. |
| `fno_v13_v10_g_3_live_config.py` | Validated import-safe G3 profile and centralized runtime paths. |
| `fno_v13_v10_g_3_live_kite_session.py` | G3 broker-coordinator wrapper preserving quantity-one constraints. |
| `fno_v13_v10_g_3_options_config.py` | G3 equity-source adapter; retain independent option rules and PAPER-only mode. |
| `fno_v13_v10_g_3_paper.py` | G3 profile adapter for PAPER lifecycle; reuse shared implementation. |
| `fno_v13_v10_g_3_policy.py` | Canonical side thresholds and staged-stop policy; pure functions. |
| `fno_v13_v10_g_3_selection.py` | Explicit G3 confirmation/ranking/quota adapter; no volume-rescaling shim. |
| `tests/test_fno_v13_v10_g_3_analysis_options.py` | G3 cohorts/options source mapping, attribution and no trading from observers. |
| `tests/test_fno_v13_v10_g_3_contract.py` | All14 effective setups, config precedence/hash, thresholds, strict09:25 and profile identity. |
| `tests/test_fno_v13_v10_g_3_daily_publication.py` | Wrong-day/stale/partial/zero-trade/idempotency and atomic current/history pointers. |
| `tests/test_fno_v13_v10_g_3_dashboard_integration.py` | Profile-aware cards, status/timeline/restart, routes and historical selection. |
| `tests/test_fno_v13_v10_g_3_live_execution.py` | Fake-broker qty1, arming, ownership, duplicate suppression and reconciliation. |
| `tests/test_fno_v13_v10_g_3_migration_rollback.py` | Dry-run/task mapping, legacy recovery paths and controlled rollback. |
| `tests/test_fno_v13_v10_g_3_runtime.py` | Scanner/confirmation/PAPER/recovery/staged-stop behavior across profile branches. |
| `tests/test_fno_v13_v10_g_3_selection_parity.py` | Full candidate/selected/nofill decision equivalence and stable quota/ranking. |
| `tests/test_fno_v13_v10_g_3_source_independence.py` | G3 replay builds valid sources without G result publication. |
| `tools/g3_runtime_preflight.py` | Proposed readiness-only route: cannot auto-arm, submit orders or spawn trading workers; existing qty1 BAT is not a safe preflight. |

## Effective accepted G-3 setup contract

Read from the frozen configuration: `setup_rules.core/expanded` supplies selection fields, while `exit.setups` supplies effective stop/target fields. Price-change numbers below are magnitudes, applied in the direction of the side. Global base gates, upper OI bound, EMA gates, stable tie-breaks and the 09:25 SHORT NIFTY gate still apply.

| Setup | Signal → confirmation | Quota | Picker | Core / expanded price % | Min OI % | Min 5m volume ratio |
| --- | --- | ---: | --- | --- | ---: | ---: |
| 0926_LONG | 09:25 → 09:26 | 1 | max_liquidity | 0.3 / 0.3 | 0.1 | 3.0 |
| 0926_SHORT | 09:25 → 09:26 | 2 | max_volume | 0.2 / 0.13 | 0.05 | 1.5 |
| 0931_LONG | 09:30 → 09:31 | 1 | max_move | 0.65 / 0.65 | 0.1 | 1.0 |
| 0931_SHORT | 09:30 → 09:31 | 1 | max_move | 0.2 / 0.13 | 0.125 | 1.0 |
| 0936_LONG | 09:35 → 09:36 | 1 | max_liquidity | 0.2 / 0.2 | 0.15 | 1.0 |
| 0936_SHORT | 09:35 → 09:36 | 2 | max_liquidity | 0.5 / 0.325 | 0.5 | 1.0 |
| 0941_LONG | 09:40 → 09:41 | 1 | max_liquidity | 0.2 / 0.2 | 0.075 | 2.0 |
| 0941_SHORT | 09:40 → 09:41 | 1 | max_move | 0.2 / 0.13 | 0.05 | 1.0 |
| 0946_LONG | 09:45 → 09:46 | 1 | max_move | 0.65 / 0.65 | 0.1 | 1.0 |
| 0946_SHORT | 09:45 → 09:46 | 1 | max_volume | 0.2 / 0.13 | 0.375 | 1.0 |
| 0951_SHORT | 09:50 → 09:51 | 1 | max_liquidity | 0.2 / 0.13 | 0.05 | 1.0 |
| 0956_LONG | 09:55 → 09:56 | 1 | max_liquidity | 0.2 / 0.2 | 0.1 | 1.0 |
| 1001_LONG | 10:00 → 10:01 | 1 | max_liquidity | 0.4 / 0.4 | 0.05 | 1.0 |
| 1121_SHORT | 11:20 → 11:21 | 1 | max_liquidity | 0.2 / 0.13 | 0.05 | 1.0 |

| Setup | Min 1m body ratio | Max 1m wick ratio | Min 1m volume ratio | Initial → tightened stop % | Effective target % |
| --- | ---: | ---: | ---: | --- | ---: |
| 0926_LONG | 0.6 | 0.6 | 1.1 | 1.25 → 1.0 | 0.97 |
| 0926_SHORT | 0.4 | 0.6 | 1.2 | 1.25 → 1.0 | 2.67 |
| 0931_LONG | 0.5 | 0.6 | 1.1 | 1.25 → 1.0 | 2.0 |
| 0931_SHORT | 0.4 | 0.6 | 1.2 | 1.25 → 1.0 | 2.0 |
| 0936_LONG | 0.6 | 0.6 | 1.1 | 1.25 → 1.0 | 1.73 |
| 0936_SHORT | 0.4 | 0.6 | 1.2 | 1.25 → 1.0 | 1.23 |
| 0941_LONG | 0.5 | 0.6 | 1.1 | 1.25 → 1.0 | 3.0 |
| 0941_SHORT | 0.4 | 0.6 | 1.2 | 1.25 → 1.0 | 2.43 |
| 0946_LONG | 0.4 | 0.6 | 1.1 | 1.25 → 1.0 | 1.23 |
| 0946_SHORT | 0.4 | 0.4 | 1.2 | 1.25 → 1.0 | 1.23 |
| 0951_SHORT | 0.4 | 0.6 | 1.2 | 1.25 → 1.0 | 0.93 |
| 0956_LONG | 0.4 | 0.6 | 1.1 | 1.25 → 1.0 | 0.9 |
| 1001_LONG | 0.4 | 0.6 | 1.1 | 1.25 → 1.0 | 3.0 |
| 1121_SHORT | 0.4 | 0.6 | 1.2 | 1.25 → 1.0 | 2.0 |

The stop tightens once after120minutes using the execution-mode clock described in the plan. No partial exits or breakeven are introduced.

## Dashboard routes and consumer mapping

| Route | Owner/current contract | Planned mapping |
| --- | --- | --- |
| `GET /` | log_dashboard_server.py:7117-7133; Authenticated main operational dashboard; embedded card/control UI consumes /api/snapshot. | Keep URL/auth. Select active G3 profile/card group and verified G3 producer roots. |
| `GET /api/snapshot` | log_dashboard_server.py:7150-7154; _snapshot returns card statuses/log/report projections and timeline data from G maps. | Migrate G3 identity, task/restart/status/heartbeat and source rules together. Include observed and expected fingerprints; no fallback to archived G success. |
| `GET /api/fno-monitor?date=YYYY-MM-DD` | log_dashboard_server.py:7155-7169; dashboard_fno_monitor.js:186; _fno_eq_id_stock_detail -> fno_eq_id_monitor_detail; frontend hardcodes G heading. | Read version-qualified G3 root/threshold contract and label; retain dated G evidence only via explicit historical profile selector if required. |
| `GET /api/log?name=<allowlisted-card>` | log_dashboard_server.py:7170-7180; resolve_log_target uses LOG_IDS/report maps; _backtesting_v13_v10_g_view at2455 picks dated running G log or dedicated G latest report. | Versioned G3 allowlist and requested date/report identity checks. Retain historical G log references; never permit arbitrary paths. |
| `POST /api/restart; POST /api/kill` | log_dashboard_server.py:7183-7196; Authenticated main dashboard controls. | Only approved G3 launcher/task contracts become active. Auth, kill semantics and protected execution controls stay in place; plan phase performs none. |
| `GET /dashboard-flow[/]` | log_dashboard_server.py:7134-7136; main header9378/10084; _send_flow_html embeds local Flow assets; same authentication, new-tab main header link. | Keep URL and read-only UI, active G3 indicator plus explicit historical G/G2/G3 choices. |
| `GET /api/dashboard-flow?run=<discovered-id>` | log_dashboard_server.py:7137-7149; dashboard_flow.py:304-339; load_flow_data defaults first discovered run; accepts only IDs from discovery. _load_trades at151 prefers trades_with_pullbacks.csv over portfolio_trades.csv. | Bind discovered IDs to verified run manifest/role/strategy. Production financial source is explicit portfolio ledger from manifest; pullback-enriched CSV can be validated secondary overlay. Reject wrong role/strategy/hash/incomplete production package; do not use newer research filename as active result. |
| `Standalone 127.0.0.1:8790/dashboard-flow` | tools/run_dashboard_flow.py:20-59; bat/run_dashboard_flow.bat; FlowPreviewHandler reuses authenticated GET rendering, rejects all POSTs. | Keep loopback/read-only/temp-auth behavior. Do not restart main8787 from this launcher. |

## Scheduled tasks: observed snapshot, not a deployment

These states are a read-only 11 October snapshot. Nonzero last results require interpretation against each launcher contract; some scanner/feed/confirmation runs occurred after their pipeline deadline. A result0 is not by itself proof of strategy correctness. Re-export and compare actions/triggers immediately before any approved cutover.

| Existing task | State / enabled | Last result | Next run as reported | Proposed disposition |
| --- | --- | ---: | --- | --- |
| `\EQIDV2_authentication_v2_0900` | Ready / True | 0 | 10/12/2026 08:30:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_backtesting_result_v13_v10_g_1620` | Ready / True | 0 | 10/12/2026 16:20:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_data_for_backtesting_1545` | Ready / True | 0 | 10/12/2026 15:45:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_data_for_backtesting_once_1232` | Ready / True | 0 | None | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_eod_5mins_data_0900` | Ready / True | 0 | 10/12/2026 09:00:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_5min_rankings_backfill_once_20260826` | Ready / True | 0 | None | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_oi_eod_qc_1540` | Ready / True | 0 | 10/12/2026 15:40:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_oi_feature_ranker_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_oi_fetch_5min_0905` | Disabled / False | 0 | 10/12/2026 09:05:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_oi_fetch_5min_fast_production_0905` | Ready / True | 0 | 10/12/2026 09:05:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_oi_fetch_5min_fast_shadow_0906` | Disabled / False | 0 | 10/12/2026 09:06:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_oi_universe_0850` | Ready / True | 0 | 10/12/2026 08:50:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_options_atm_fetch_5min_0907` | Ready / True | 0 | 10/12/2026 09:07:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_today_data_repair_once_20260831_1705` | Ready / True | 0 | None | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v10_v11_v12_paper_0915` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v13_v10_g_confirmation_1min_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_equity_1min_feed_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_live_kite_qty1_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_live_long_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_live_short_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_net_result_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_options_live_long_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_options_live_short_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_options_net_result_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_options_trade_logger_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_scanner_5min_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v13_v10_g_trade_logger_0915` | Ready / True | 2 | 10/12/2026 09:15:00 | Replace approved active route with corresponding G3 task after applicable gates; preserve exported G definition. |
| `\EQIDV2_fno_v5_confirmation_1min_0919` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v5_live_long_0920` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v5_live_short_0920` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v5_net_result_0920` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v5_scanner_5min_0918` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v5_trade_logger_0920` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v8_combined_paper_0915` | Disabled / False | 267011 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_fno_v8_v9_backtest_10d_once_20260826` | Disabled / False | 267011 | None | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_g3_dated_once_20261009` | Ready / True | 0 | None | Retain one-off research evidence; not the new recurring G3 daily publisher. |
| `\EQIDV2_kite_export_start_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_kite_export_stop_1535` | Ready / True | 0 | 10/12/2026 15:35:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_nifty_guard_fetch_v15_0915` | Disabled / False | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_nifty_guard_fetch_v16_5min_0915` | Ready / True | 0 | 10/12/2026 09:15:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_pf_supervisor_healthcheck_0935` | Ready / True | 0 | 10/12/2026 09:35:00 | Protect shared/other-strategy schedule and current enablement; verify dependency readiness. |
| `\EQIDV2_preopen_session_autofix_0904` | Ready / True | 1 | 10/12/2026 09:04:00 | Review checks/autofix targets so legacy G cannot be reactivated. |
| `\EQIDV2_preopen_session_healthcheck_0905` | Ready / True | 0 | 10/12/2026 09:05:00 | Review checks/autofix targets so legacy G cannot be reactivated. |
| `\EQIDV2_v13_shadow_prepare_0850` | Ready / True | 2 | 10/12/2026 08:50:00 | Version source/cohort mapping for G3 and preserve separate G history. |
| `\EQIDV2_v13_shadow_seal_1525` | Ready / True | 0 | 10/12/2026 15:25:00 | Version source/cohort mapping for G3 and preserve separate G history. |

## Existing runtime directories and control paths

Existing paths remain in place. Preserve prior order ownership, source hashes and logs; create isolated G3 production state rather than copying old G arm/order records. Recheck externally configured paths before deployment.

| Existing path | Exists at audit | Protection/use |
| --- | --- | --- |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/scanner_5m` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/confirmation_1m` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/signals` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/orders/PAPER` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/orders/LIVE/live_kite_qty1` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/consolidated` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/evidence` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/order_events` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/live_kite` | True | Preserve G signal/order/evidence history and outstanding ownership; add isolated G3 counterpart. |
| `C:/TradingData/eqidv2/fno_oi/v6_live` | True | Legacy arm/kill and compatibility state; keep emergency behavior, never copy arming authorization. |
| `C:/TradingData/eqidv2/fno_oi/v13_v10_g_options_paper` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/backtesting_result_v13_v10_g` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g` | True | Immutable historical/provenance input; no in-place promotion or relocation. |
| `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_2` | True | Immutable historical/provenance input; no in-place promotion or relocation. |
| `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3` | True | Immutable historical/provenance input; no in-place promotion or relocation. |
| `C:/TradingData/eqidv2/fno_oi/instrument_master` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/universe` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/raw_contracts_5m` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/slot_ready` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/raw_equity_1m` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/equity_1m_slot_ready` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/contract_features` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/feature_snapshots` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/rankings` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/ranking_ready` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/latest` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/eod_qc` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/raw_options_5m` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/raw_options_1m` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/options_slot_ready` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/fno_oi/options_atm_map` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/runtime_status` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/runtime_state` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/runtime_manifests` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/live_signals` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/reports` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/stocks_indicators_1min_eq` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/stocks_indicators_15min_eq` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/stocks_cache_5min_eq` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/slot_ready_5m` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/nifty_slot_ready_5m` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/observability` | True | Preserve shared data/control/history dependency; validate readers during cutover. |
| `C:/TradingData/eqidv2/v13_v10_g_strategy_research` | True | Immutable historical/provenance input; no in-place promotion or relocation. |
| `C:/TradingData/eqidv2/v13_v10_g_execution_research` | True | Preserve shared data/control/history dependency; validate readers during cutover. |

### Exact protected research/provenance sources

All paths below are relative to `C:/TradingData/eqidv2`. Directory entries cover every child, source snapshot and manifest-pinned dependency.

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
- `fno_oi/strategy_research/v13_corrected_v5/` (current options input dependency)
- `fno_oi/strategy_research/v6_derivative_backtests/market_cache/` (current options input dependency)
- `fno_oi/strategy_research/**/nfo_master_*.parquet (generic current options discovery)`

### Proposed G3 publication structure

Under `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3`, separate new production and cumulative-history trees from the existing historical `runs` tree. Rows explicitly marked EXTERNAL use the shared runtime path helper/configured log root rather than this publication root.

| Proposed location | Contract |
| --- | --- |
| `production/runs/<YYYY-MM-DD>/<run-id>/` | Immutable new daily production attempts; isolated from archived research exports. |
| `history/runs/<through-date>/<run-id>/` | New validated cumulative G3 history; existing runs tree stays unchanged as historical/research input. |
| `status.json` | Current attempted production status with session, state, run id, exact G3 strategy/version/fingerprint; explicit blocked/failed/waiting states. |
| `latest/latest_backtesting_result_v13_v10_g_3.json` | Current attempt/status + verified successful artifact references; do not retain prior success metrics on failure. |
| `latest/latest_backtesting_result_v13_v10_g_3.md` | Human-readable exact-date G3 report explaining accepted rules and exclusions. |
| `latest/current_run.json` | Atomic pointer to last validated production run with manifest hash and role=production_daily; paired current status makes stale dates explicit. |
| `latest/current_history.json` | Independent atomic pointer to last accepted cumulative G3 history manifest, never a daily/preview package. |
| `reports/<YYYY-MM-DD>/<run-id>/backtesting_result_v13_v10_g_3.md` | Immutable per-attempt G3 report; run-id prevents same-date rerun collisions. |
| `production/runs/.../run_manifest.json` | Written last; complete, status, role, date/calendar, producer version, strategy/version/config fingerprint, source manifests/hashes, artifact paths/hashes, coverage/exclusions, execution model/cost/capital, historical vs production authority. |
| `production/runs/.../portfolio_trades.csv` | Authoritative execution ledger with G3 identity/rules fields, filled/portfolio executed flags, cost/gross/net attribution. |
| `production/runs/.../daily_results.csv` | Exact dated accounting; retain zero trades only for verified eligible completed session. |
| `production/runs/.../feature_ledger.csv + manifest and input snapshot references` | Verified canonical source evidence independent of the historical G strategy decision stage. |
| `EXTERNAL: runtime_status/backtesting_result_v13_v10_g_3.status and .heartbeat` | Existing runtime status mechanism with G3 session identity; exact relative shared runtime paths resolved through runtime helper. |
| `EXTERNAL: run_logs or established LOG_DIR/backtesting_result_v13_v10_g_3_<date>.log` | New G3 logs preserve G historical logs; publisher/run id links consistent. |

Flow must read the manifest-designated production portfolio ledger. Its current preference for `trades_with_pullbacks.csv` is suitable for historical enriched reports, but an observer overlay must not become the authoritative source of production P&L or executed trades. Keep overlay IDs/fingerprints validated against the canonical ledger.

### Runtime-generated files covered recursively

| Root/pattern | Coverage and migration rule |
| --- | --- |
| `fno_oi/v13_v10_g_live/**` and proposed `fno_oi/v13_v10_g_3_live/**` | Scanner/confirmation snapshots, signals, signal CSV/JSON, PAPER/LIVE orders, open/pending/closed state, locks, seen-ID journals, broker events, trade/net-result CSVs, evidence, reports and config/session manifests. Keep version/mode namespaces separate. |
| `fno_oi/v6_live/**` and proposed G3 control namespace | Existing arm/kill state and compatibility controls; preserve global emergency behavior and require new explicit G3 authorization. |
| Existing G options PAPER root and proposed G3-source counterpart | Equity-source identity, option-master references, one-lot/cash state, orders, events and reports; no cross-version state reuse or mode escalation. |
| `backtesting_result_v13_v10_g*/**` | Existing dated snapshots, input/feature manifests, candidates/rejects/selected/fills, portfolio/daily CSV, HTML/MD/XLSX exports, validation and hashes. Preserve historical trees; new G3 production uses declared roles/pointers. |
| `fno_oi/strategy_research/**` selected/pinned dependencies | Frozen configs/source code, minute/feature snapshots, instrument masters, ledgers, models, protocols and report artifacts remain protected even when named G2/V5/V6. |
| `runtime_status/**`, project `logs/**`, supervisor records | Task/process status, heartbeat, spawn records, dated logs, worker locks and restart attribution; introduce consistent G3 IDs while preserving old diagnostics. |
| Shared market/cache/calendar/Kite export roots | Dated raw candles, rankings/OI, instrument caches, trading calendar, broker exports and reconcilable actual fills; reuse verified inputs without rewriting historical data. |
| G and proposed G3 analysis/shadow roots | Features, cohorts, models, data-quality outputs, predictions, metrics, reports and timestamps remain explicitly versioned; no silent reuse of G calibration as G3. |

## Environment and deployment contract (names only)

| Group | Names to audit |
| --- | --- |
| profile_and_execution | `FNO_LIVE_GENERATION`, `FNO_V6_STRATEGY_PROFILE`, `FNO_V6_EXECUTION_MODE`, `FNO_V6_EXECUTION_SESSION_NAMESPACE`, `FNO_V6_LIVE_ACK`, `FNO_V13_V10_G_OPTIONS_EXECUTION_MODE` |
| shared_runtime_locations | `EQIDV2_RUNTIME_ROOT`, `EQIDV2_RUNTIME_STATUS_DIR`, `EQIDV2_DATA_5M_DIR`, `EQIDV2_DATA_1MIN_DIR`, `EQIDV2_DATA_15M_DIR`, `EQIDV2_REPORTS_DIR`, `EQIDV2_LIVE_SIGNALS_DIR`, `EQIDV2_CACHE_15M_DIR`, `EQIDV2_CACHE_5MIN_DIR`, `EQIDV2_SLOT_READY_PENDING_DIR`, `EQIDV2_NIFTY_SLOT_READY_DIR`, `EQIDV2_NIFTY_SLOT_FAIL_DIR`, `EQIDV2_NIFTY_OPEN_SLOT_DIR` |
| correlation_and_telemetry | `EQIDV2_OBSERVABILITY_ENABLED`, `EQIDV2_OBS_RUN_ID`, `AI_PLATFORM_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`, `OTEL_SDK_DISABLED` |
| launcher_local_variables | `SESSION_ID`, `SESSION_SCRIPT`, `BASE_DIR`, `PYTHON_EXE`, `RUNTIME_STATUS_DIR`, `LOG_DIR`, `LOG_FILE`, `STATUS_FILE`, `HEARTBEAT_FILE`, `LOCK_FILE`, `SPAWN_RECORD_FILE`, `OPEN_POSITIONS_PATTERN`, `WORKER_STATUS_FILE`, `WORKER_HEARTBEAT_FILE`, `PYTHONUNBUFFERED`, `PYTHONIOENCODING` |

Also preserve `LOG_DASH_*`, Gmail/SMTP configuration and broker/API credential locations. Use their current values in place; never place values in the plan or change authentication as a side effect of a strategy migration. New profile/environment aliases must have explicit precedence and reject conflicting mode/profile combinations.

## Coverage and verification limits

The file inventory combines current text references, static import closure, reverse strategy consumers, manual profile/runtime/dashboard audits and task-launcher dependencies. Static import edges alone do not prove a component runs, and filenames alone do not prove it is obsolete. Generated files are covered by roots and patterns; secrets by metadata only. Future dynamic paths or task changes must be added to the implementation manifest before deployment.

Detailed local audit evidence is retained under `.codex_tmp/g3_migration_plan_20261011/`: `source_inventory.json`, `strategy_audit.json`, `runtime_audit.json`, `runtime_tasks_sanitized.json` and `dashboard_data_audit.json`. No executable migration was performed.

## Implemented file coverage — 12 October 2026

The implementation inventory below covers **88 concrete files**. The original 548-file dependency review remains above; unchanged transitive dependencies and frozen evidence are retained, not renamed. Exact file hashes are recorded in `.codex_tmp/g3_migration_plan_20261011/implementation_files.json`. Concurrent visual and V14 changes in the shared workspace are not claimed as migration work.

Planned name substitutions: `fno_v13_v10_g_3_selection.py` is implemented inside the shared pure `fno_v13_v10_g_3_contract.py`; the neutral snapshot module is `fno_v13_replay_source.py`; readiness is `fno_v13_v10_g_3_preflight.py`; task conversion/scheduling are handled by `bat/migrate_fno_v13_v10_g_to_g3.ps1` and `bat/g3_task_xml_helpers.ps1`; options routing uses the G3 adapter with the unchanged shared options configuration.

Runtime additions remain protected under `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/{production,history,reports,latest}`, `fno_oi/v13_v10_g_3_live`, `fno_oi/v13_v10_g_3_options_paper`, `v13_v10_g_3_strategy_research` and `v13_v10_g_3_execution_research`. Task rollback XMLs are in `.codex_tmp/g3_task_rollback_20261012_retry`; sanitized deployment verification is in `.codex_tmp/g3_migration_plan_20261011/deployed_tasks_audit.json`. No archive operation was performed.

### Contract and routing

| File | Implementation disposition |
| --- | --- |
| `configs/strategies/active_profile.json` | Added / newly inventoried integration |
| `configs/strategies/v13_v10_g_3_production_v1.json` | Added / newly inventoried integration |
| `fno_live_entry_guard.py` | Added / newly inventoried integration |
| `fno_live_profile.py` | Updated existing integration |
| `fno_strategy_registry.py` | Added / newly inventoried integration |

### G3 adapters and independent replay

| File | Implementation disposition |
| --- | --- |
| `backtesting_result_v13_v10_g_3_daily.py` | Added / newly inventoried integration |
| `fno_v13_replay_source.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_backtest.py` | Retained dependency |
| `fno_v13_v10_g_3_contract.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_daily_replay.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_live.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_live_config.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_live_kite_session.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_options_paper.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_paper.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_policy.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3_preflight.py` | Added / newly inventoried integration |
| `fno_v13_v10_g_3l_capital_replay.py` | Retained dependency |

### Shared runtime

| File | Implementation disposition |
| --- | --- |
| `ai_platform/observability/feature_ledger.py` | Updated existing integration |
| `fno_equity_fetch_1min.py` | Updated existing integration |
| `fno_v13_v10_g_options_paper.py` | Updated existing integration |
| `fno_v13_v10_g_paper.py` | Updated existing integration |
| `fno_v5_live.py` | Updated existing integration |
| `fno_v6_live_kite_session.py` | Updated existing integration |
| `preopen_session_autofix.py` | Updated existing integration |
| `preopen_session_healthcheck.py` | Updated existing integration |

### Dashboards

| File | Implementation disposition |
| --- | --- |
| `dashboard_cockpit.py` | Added / newly inventoried integration |
| `dashboard_flow.js` | Updated existing integration |
| `dashboard_flow.py` | Updated existing integration |
| `fno_eq_id_monitor_detail.py` | Updated existing integration |
| `log_dashboard_server.py` | Updated existing integration |

### AI and analysis

| File | Implementation disposition |
| --- | --- |
| `ai_platform/adapters/daily_replay.py` | Updated existing integration |
| `ai_platform/adapters/strategy.py` | Updated existing integration |
| `ai_platform/api/app.py` | Updated existing integration |
| `ai_platform/api/registry.py` | Updated existing integration |
| `ai_platform/api/repository.py` | Updated existing integration |
| `ai_platform/observability/g3_analysis.py` | Added / newly inventoried integration |
| `ai_platform/observability/g3_execution_analysis.py` | Added / newly inventoried integration |
| `ai_platform/observability/g3_shadow_gate.py` | Added / newly inventoried integration |
| `ai_platform/observability/live_trust.py` | Updated existing integration |
| `ai_platform/observability/runtime_collector.py` | Updated existing integration |
| `bat/observability/Test-V13V10GLiveTrust.ps1` | Added / newly inventoried integration |
| `docs/ai_platform/profile_registry.json` | Updated existing integration |
| `docs/ai_platform/source_registry.json` | Updated existing integration |
| `tools/v13_execution_research.py` | Updated existing integration |
| `tools/v13_prospective_shadow.py` | Updated existing integration |
| `tools/v13_shadow_automation.py` | Updated existing integration |
| `tools/v13_strategy_research.py` | Updated existing integration |

### Launchers and scheduler migration

| File | Implementation disposition |
| --- | --- |
| `bat/activate_fno_v13_v10_g_3_live_kite_qty1.bat` | Added / newly inventoried integration |
| `bat/g3_task_xml_helpers.ps1` | Added / newly inventoried integration |
| `bat/migrate_fno_v13_v10_g_to_g3.ps1` | Added / newly inventoried integration |
| `bat/preflight_fno_v13_v10_g_3.ps1` | Added / newly inventoried integration |
| `bat/rollback_fno_v13_v10_g3_tasks.ps1` | Added / newly inventoried integration |
| `bat/run_backtesting_result_v13_v10_g_3_1620.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_confirmation_1min.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_equity_1min_feed.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_live_kite_qty1.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_live_long.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_live_short.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_net_result.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_options_live_long.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_options_live_short.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_options_net_result.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_options_trade_logger.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_scanner_5min.bat` | Added / newly inventoried integration |
| `bat/run_fno_v13_v10_g_3_trade_logger.bat` | Added / newly inventoried integration |
| `bat/run_v13_strategy_research_refresh.bat` | Updated existing integration |
| `bat/schedule_backtesting_result_v13_v10_g_weekday.bat` | Updated existing integration |
| `bat/schedule_fno_oi_weekday.ps1` | Updated existing integration |
| `bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1` | Updated existing integration |
| `bat/schedule_fno_v13_v10_g_options_weekday.ps1` | Updated existing integration |

### Focused tests

| File | Implementation disposition |
| --- | --- |
| `tests/test_ai_platform_api_stage2.py` | Updated existing integration |
| `tests/test_ai_platform_g3.py` | Added / newly inventoried integration |
| `tests/test_dashboard_fno_eq_id_monitor.py` | Updated existing integration |
| `tests/test_dashboard_g3_migration.py` | Added / newly inventoried integration |
| `tests/test_fno_v13_v10_g_3_contract.py` | Added / newly inventoried integration |
| `tests/test_fno_v13_v10_g_3_daily_replay.py` | Added / newly inventoried integration |
| `tests/test_fno_v13_v10_g_3_runtime.py` | Added / newly inventoried integration |
| `tests/test_g3_execution_analysis.py` | Added / newly inventoried integration |
| `tests/test_g3_legacy_installer_guards.py` | Added / newly inventoried integration |
| `tests/test_g3_task_migration.ps1` | Added / newly inventoried integration |
| `tests/test_observability_live_trust.py` | Updated existing integration |

### Operational documentation

| File | Implementation disposition |
| --- | --- |
| `FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md` | Updated existing integration |
| `OPTIONS_V13_V10_G_PAPER_TRADING.md` | Updated existing integration |
| `V13_V10_G_DAILY_BACKTEST.md` | Updated existing integration |
| `V13_V10_G_DASHBOARD_INTEGRATION.md` | Updated existing integration |
| `docs/DASHBOARD_FLOW.md` | Updated existing integration |
| `docs/OBSERVABILITY_OPERATIONS.md` | Updated existing integration |
