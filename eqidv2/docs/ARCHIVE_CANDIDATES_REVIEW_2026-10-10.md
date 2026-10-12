# Future archive inventory — approval required

**Status: inventory only. No files or folders have been moved, archived or deleted. No archive operation is approved.**

Prepared on 10 October 2026 for the requested G, G-2, G-3 and dashboard cleanup review. Only the rows marked CANDIDATE are proposals for a future archive review. HOLD and KEEP are excluded from that proposal.

## Scope and path keys

- **P — project:** `C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2`
- **D — trading data:** `C:/TradingData/eqidv2`
- An exact directory row covers all scanned descendants recursively. Individual file rows cover only that file. Rows do not overlap.
- Neither the parent Git checkout nor the rest of Downloads/Desktop was inventoried. Links/reparse directories were not traversed. `.git` metadata and this inventory work directory are retained and excluded from counts.

## Summary

| Classification | Listed units | Scanned files covered | Logical size | Future action |
| --- | ---: | ---: | ---: | --- |
| CANDIDATE | 305 | 2,824 | 2.414 GiB | Only after explicit approval of selected IDs and a fresh dependency check |
| HOLD | 791 | 171,795 | 39.741 GiB | Keep in place; unresolved or mixed usage |
| KEEP | 932 | 257,729 | 184.602 GiB | Protected; excluded from archiving |

Metadata scan: `2026-10-10T09:23:05+00:00` to `2026-10-10T09:23:50+00:00`. Scanned 432,348 files and 14,747 directories, with 0 enumeration errors. Logical bytes are file lengths, not guaranteed space reclaimable on disk (OneDrive, hard links and compression may differ). Latest modification is context, not proof of disuse.

## Protected exclusions

- All discovered G/G-2/G-3 results, histories, frozen configurations, source code, launchers, live/paper execution state and supporting research remain protected. V13 predecessor evidence is retained conservatively.
- Both dashboards, their assets, APIs, launchers and monitored results remain protected. An older version or disabled scheduled task is not automatically obsolete.
- Kite/Zerodha/authentication files, API-key text files, access tokens, token caches, credential-related filenames and controls remain protected. Credential contents and raw process/task arguments are not included in this document.
- Shared market data, instrument masters, calendars, repairs, indicators, ready markers, runtime state, configuration, related tests, documentation, HTML assets, reports and logs remain in place.
- Loose documentation/log/text/media files are held conservatively. A CANDIDATE directory may contain its own unrelated historical report or log; the proposal covers that named experiment only, never related G/dashboard evidence.

### Non-obvious dependencies that must stay

| Dependency | Why it is protected |
| --- | --- |
| `fno_v5_live.py`, `fno_v5_live_config.py`, `fno_v6_live_kite_session.py`, `fno_v6_live_config.py` | G wrappers and the dynamic live profile reuse old-named engines/configuration. |
| `fno_oi/v6_live/` | Shared arm/kill and live control state; do not remove because of the older version label. |
| V13 V10 F/E/D/C/B/A, V9 and corrected V2–V6 ancestors | G backtest imports/default inputs still inherit these implementations and evidence. |
| `fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/` | Canonical G frozen configuration and portfolio evidence. |
| `fno_oi/strategy_research/fno_historical_repair_20260827/` | G3 confirmation research reads repaired minute data. |
| Every `nfo_master_*.parquet` beneath `fno_oi/strategy_research/` | G options paper searches the entire research tree recursively for these masters. No containing candidate directory is allowed. |
| `fno_oi/strategy_research/v13_corrected_v5/`, `v6_derivative_backtests/` | Old-looking derivative market data remains an input for G options paper. |
| `backtesting_result_v11/` | Current G readiness and operational consumers still reference this older output root. |
| V7/V15/V16/V17 sources and result paths referenced by dashboard/tasks | Some old strategies remain monitored, scheduled or imported; the dependency closure takes precedence over name-based grouping. |
| `tools/run_g3_dated_once.py` | Referenced by the registered G3 dated task. |

## Archive candidates — pending approval

These are conservative proposals with explicit legacy/experiment naming and no protected dependency found by the recorded checks. Absence of a static reference is not a guarantee of disuse. Do not execute this table as a move list.

| ID | Root | Exact relative path | Files | Logical size | Latest modified (UTC) | Reason / hold |
| --- | --- | --- | ---: | ---: | --- | --- |
| A-0001 | D | `outputs_ID_v11_ab_probe_from_v10_raw_q250/` | 24 | 238.50 MiB | 2026-06-01 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0002 | D | `fno_oi_storage_writer_benchmark_20260901/` | 1,512 | 158.30 MiB | 2026-09-01 | Data review found an isolated dated benchmark and no source references or protected descendants; runtime/dependency exclusion checks also passed. |
| A-0003 | D | `outputs_ID_v11_tier123_balanced_full_cached_2026-06-03/` | 25 | 36.07 MiB | 2026-06-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0004 | D | `outputs_ID_v11_tier123_balanced_full_cached_final_2026-06-03/` | 25 | 30.47 MiB | 2026-06-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0005 | D | `outputs_ID_v11_tier123_balanced_full_cached_fixed_2026-06-03/` | 25 | 30.47 MiB | 2026-06-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0006 | D | `outputs_ID_v11_tier123_balanced_full_cached_matched_2026-06-03/` | 37 | 26.66 MiB | 2026-06-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0007 | D | `outputs_hilega_milega_5m_lastweek_20260803_20260807/` | 5 | 17.04 MiB | 2026-08-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0008 | D | `outputs_ID_v11_future_paper_pair_20260729/` | 16 | 14.41 MiB | 2026-07-29 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0009 | D | `outputs_ID_v11_5min_blocked/` | 30 | 8.42 MiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0010 | D | `v11_book_smoke_20260722/` | 52 | 7.91 MiB | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0011 | D | `outputs_ID_v11_late_day_intraday_momentum/` | 9 | 3.52 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0012 | D | `outputs_ID_v11_gd_v2_pool/` | 3 | 3.45 MiB | 2026-06-25 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0013 | D | `outputs_ID_v11_new_overlay_forward_test/` | 21 | 2.88 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0014 | D | `outputs_ID_v11_conf_tier_c_current_smoke/` | 8 | 1.29 MiB | 2026-06-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0015 | D | `outputs_ID_v11_market_relative_response/` | 9 | 1.00 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0016 | D | `outputs_ID_v11_last2wk_rnd/` | 95 | 980.2 KiB | 2026-07-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0017 | D | `outputs_ID_v10_v7_live_paper_replay_2026-05-22_to_2026-06-04/` | 9 | 739.4 KiB | 2026-06-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0018 | D | `outputs_ID_v10_backend_validation_post_T_probation_rerun_2026-05-21_to_2026-06-03/` | 9 | 484.0 KiB | 2026-06-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0019 | D | `outputs_ID_v10_backend_validation_v7_live_paper_replay_2026-05-21_to_2026-06-03/` | 9 | 484.0 KiB | 2026-06-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0020 | D | `outputs_ID_v11_tier123_balanced_integration_verify/` | 0 | 0 B | - | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0021 | D | `v11_Tnew_paths_cache.pkl` | 1 | 85.16 MiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0022 | D | `v11_new3_paths_cache.pkl` | 1 | 43.96 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0023 | D | `v11_shortmine_paths_cache.pkl` | 1 | 43.58 MiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0024 | D | `v11_short2_paths_cache.pkl` | 1 | 28.03 MiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0025 | D | `v11_newsetups_paths_cache.pkl` | 1 | 26.52 MiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0026 | D | `v11_amod_paths_cache.pkl` | 1 | 22.97 MiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0027 | D | `v11_new2_paths_cache.pkl` | 1 | 17.91 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0028 | D | `v11_L_paths_cache.pkl` | 1 | 14.30 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0029 | D | `v11_T_paths_cache.pkl` | 1 | 11.68 MiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0030 | D | `v11_salvage_L_paths_cache.pkl` | 1 | 9.45 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0031 | D | `v11_S_paths_cache.pkl` | 1 | 6.87 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0032 | D | `v11_portfolio_built_records.pkl` | 1 | 5.63 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0033 | D | `v11_salvage_N_paths_cache.pkl` | 1 | 5.19 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0034 | D | `v11_G_paths_cache.pkl` | 1 | 2.94 MiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0035 | D | `fno_oi/strategy_research/ema_confirm_v9_extended_slots_trades.csv` | 1 | 541.9 KiB | 2026-08-20 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0036 | D | `fno_oi/strategy_research/ema_confirm_v9_extended_slots_setups.csv` | 1 | 19.9 KiB | 2026-08-20 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0037 | D | `fno_oi/strategy_research/ema_confirm_v9_extended_slots_daily.csv` | 1 | 14.1 KiB | 2026-08-20 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0038 | P | `outputs_avwap_ID_5min_v2_allsetups_internal/` | 7 | 300.69 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0039 | P | `outputs_avwap_ID_5min_v2_short_adv_probe/` | 8 | 247.57 MiB | 2026-05-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0040 | P | `outputs_avwap_ID_5min_v2_short_pf2_promoted/` | 7 | 215.32 MiB | 2026-05-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0041 | P | `outputs_avwap_ID_5min_v2_short_pf2_expanded/` | 7 | 215.21 MiB | 2026-05-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0042 | P | `outputs_avwap_ID_5min_v2_tune_raw/` | 6 | 129.35 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0043 | P | `outputs_avwap_ID_5min_v2_internal_pf2/` | 7 | 128.75 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0044 | P | `outputs_avwap_ID_5min_v2_internal_tight/` | 7 | 128.68 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0045 | P | `outputs_v17D_library_tierA/` | 4 | 92.44 MiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0046 | P | `outputs_v17D_library_smoke_full/` | 2 | 12.53 MiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0047 | P | `outputs_avwap_ID_5min_v1_fade/` | 6 | 11.93 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0048 | P | `outputs_avwap_ID_5min_v1_improved/` | 6 | 6.55 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0049 | P | `replay_out_full_pipeline/` | 223 | 5.36 MiB | 2026-04-23 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0050 | P | `outputs_v17D_phase0/` | 26 | 5.18 MiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0051 | P | `outputs_avwap_ID_5min_v1_research_catalog_rest_expanded/` | 4 | 4.87 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0052 | P | `outputs_avwap_ID_5min_v2_research_catalog_rest_expanded/` | 4 | 4.87 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0053 | P | `outputs_avwap_ID_5min_v2_allsetups_smoke/` | 7 | 4.46 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0054 | P | `outputs_avwap_ID_5min_v1_improved_balanced/` | 6 | 4.36 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0055 | P | `outputs_v17D_backtest/` | 17 | 4.02 MiB | 2026-05-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0056 | P | `sweep_results/` | 6 | 3.09 MiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0057 | P | `outputs_avwap_ID_5min_v1_pf_tuned/` | 6 | 2.33 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0058 | P | `outputs_avwap_ID_5min_v2_v17_cascade/` | 4 | 2.29 MiB | 2026-05-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0059 | P | `outputs_avwap_ID_5min_v1_research_short_expanded_normalized/` | 4 | 2.26 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0060 | P | `outputs_avwap_ID_5min_v1_research_short_expanded/` | 4 | 2.21 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0061 | P | `outputs_avwap_ID_5min_v1/` | 6 | 2.19 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0062 | P | `outputs_avwap_ID_5min_v1_extra_slots/` | 6 | 2.19 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0063 | P | `outputs_avwap_ID_5min_v1_top1_baseline/` | 6 | 2.13 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0064 | P | `outputs_avwap_ID_5min_v2_smoke/` | 6 | 1.99 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0065 | P | `outputs_avwap_ID_5min_v1_research_plus_e4_addons/` | 4 | 1.93 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0066 | P | `outputs_avwap_ID_5min_v1_research_pareto_pf26/` | 4 | 1.61 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0067 | P | `outputs_avwap_ID_5min_v1_research_reresolved_mae_mfe/` | 4 | 1.06 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0068 | P | `outputs_avwap_ID_5min_v1_pf_final/` | 6 | 974.3 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0069 | P | `outputs_avwap_mom_v1_5min/` | 205 | 853.3 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0070 | P | `replay_out_see/` | 9 | 564.0 KiB | 2026-04-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0071 | P | `outputs_avwap_ID_5min_v1_research_e4_top3/` | 4 | 463.1 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0072 | P | `outputs_avwap_ID_5min_v1_smoke/` | 6 | 362.5 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0073 | P | `outputs_avwap_ID_5min_v1_advanced_top3_probe/` | 1 | 332.5 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0074 | P | `outputs_avwap_ID_5min_v1_smoke2/` | 6 | 213.3 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0075 | P | `outputs_avwap_ID_5min_v2_short_pf2_final/` | 4 | 83.0 KiB | 2026-05-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0076 | P | `outputs_v17D_library_smoke/` | 2 | 54.1 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0077 | P | `replay_out/` | 1 | 23.4 KiB | 2026-04-21 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0078 | P | `outputs_avwap_ID_5min_v2_v17_cascade_pkg_smoke/` | 0 | 0 B | - | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0079 | P | `outputs_avwap_ID_5min_v2_v17_cascade_smoke/` | 0 | 0 B | - | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0080 | P | `_v17r_advsetups_v2_trades.csv` | 1 | 3.26 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0081 | P | `outputs_avwap_ID_5min_v1_research_reresolved_plus_e4_addons_tmp.csv` | 1 | 1.92 MiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0082 | P | `_v17r_breadth_cache.parquet` | 1 | 608.3 KiB | 2026-05-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0083 | P | `avwap_combined_runner_v19a5_5min.py` | 1 | 252.9 KiB | 2026-04-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0084 | P | `avwap_combined_runner_v19a4_5min.py` | 1 | 252.4 KiB | 2026-04-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0085 | P | `avwap_combined_runner_v19a3_5min.py` | 1 | 252.2 KiB | 2026-04-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0086 | P | `avwap_combined_runner_v19a2_5min.py` | 1 | 250.4 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0087 | P | `avwap_combined_runner_v19a1_5min.py` | 1 | 248.9 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0088 | P | `avwap_combined_runner_v19a_5min.py` | 1 | 247.5 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0089 | P | `avwap_combined_runner_v18c5_5min.py` | 1 | 232.7 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0090 | P | `avwap_combined_runner_v18c3_5min.py` | 1 | 229.2 KiB | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0091 | P | `avwap_combined_runner_v18c_5min.py` | 1 | 229.1 KiB | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0092 | P | `avwap_combined_runner_v18c4_5min.py` | 1 | 228.3 KiB | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0093 | P | `avwap_combined_runner_v18c2_5min.py` | 1 | 226.8 KiB | 2026-04-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0094 | P | `avwap_combined_runner_v18b1_5min.py` | 1 | 215.8 KiB | 2026-04-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0095 | P | `avwap_combined_runner_v18b_5min.py` | 1 | 213.6 KiB | 2026-04-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0096 | P | `avwap_combined_runner_v18_5min.py` | 1 | 205.6 KiB | 2026-04-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0097 | P | `avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min_sl_limit.py` | 1 | 159.5 KiB | 2026-05-04 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0098 | P | `avwap_combined_runner_v15_5min.py` | 1 | 148.6 KiB | 2026-03-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0099 | P | `_v17r_advsetups_trades.csv` | 1 | 146.3 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0100 | P | `final_setup_conf_v11_lab.py` | 1 | 136.1 KiB | 2026-07-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0101 | P | `final_setup_conf_v11_rnd.py` | 1 | 133.7 KiB | 2026-07-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0102 | P | `avwap_trade_execution_PAPER_TRADE_FALSE_v5.py` | 1 | 120.0 KiB | 2026-03-09 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0103 | P | `avwap_combined_runner_v14.py` | 1 | 99.5 KiB | 2026-03-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0104 | P | `avwap_combined_runner_v9_sweep.py` | 1 | 87.7 KiB | 2026-03-17 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0105 | P | `avwap_5min_ID_v10_backtesting.py` | 1 | 87.0 KiB | 2026-06-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0106 | P | `avwap_combined_runner_v11.py` | 1 | 86.8 KiB | 2026-03-17 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0107 | P | `avwap_combined_runner_v11_newlong.py` | 1 | 86.7 KiB | 2026-03-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0108 | P | `avwap_combined_runner_v12.py` | 1 | 86.2 KiB | 2026-03-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0109 | P | `avwap_combined_runner_v6_sweep.py` | 1 | 83.3 KiB | 2026-03-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0110 | P | `tools/package_fno_v10_full_history_trio.py` | 1 | 75.0 KiB | 2026-08-27 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0111 | P | `tools/package_fno_v10_stage5_to_stage8.py` | 1 | 70.7 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0112 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v3.py` | 1 | 69.3 KiB | 2026-02-27 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0113 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v4_short.py` | 1 | 66.3 KiB | 2026-02-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0114 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v4_long.py` | 1 | 66.3 KiB | 2026-02-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0115 | P | `avwap_combined_runner_v17q_5min.py` | 1 | 56.7 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0116 | P | `eqidv2_live_trading_signal_15m_v11_combined_parquet.py` | 1 | 56.2 KiB | 2026-02-20 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0117 | P | `tools/package_fno_v10_stage10.py` | 1 | 53.4 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0118 | P | `research_v11_context_week.py` | 1 | 51.0 KiB | 2026-08-06 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0119 | P | `tools/package_fno_v10_stage2.py` | 1 | 50.3 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0120 | P | `avwap_5min_ID_v1_backtesting.py` | 1 | 48.6 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0121 | P | `avwap_combined_runner_v18a_5min.py` | 1 | 45.2 KiB | 2026-04-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0122 | P | `avwap_combined_runner_v17t_live_5min.py` | 1 | 44.8 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0123 | P | `tools/package_fno_v10_stage3.py` | 1 | 40.7 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0124 | P | `generate_v17b_reference_pdf.py` | 1 | 39.4 KiB | 2026-04-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0125 | P | `research_v11_market_relative_response.py` | 1 | 33.1 KiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0126 | P | `avwap_5min_ID_v9_backtesting.py` | 1 | 33.1 KiB | 2026-05-29 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0127 | P | `avwap_5min_mom_v1_backtesting.py` | 1 | 29.7 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0128 | P | `compose_fno_v8_v9_last_10_diagnostic.py` | 1 | 28.0 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0129 | P | `tools/package_fno_v10_stage4.py` | 1 | 27.8 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0130 | P | `research_v11_late_day_intraday_momentum.py` | 1 | 27.7 KiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0131 | P | `v18c6_param_sweep.py` | 1 | 27.3 KiB | 2026-04-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0132 | P | `avwap_exit_scenarios.py` | 1 | 27.1 KiB | 2026-03-03 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0133 | P | `avwap_combined_runner_v12_sweep.py` | 1 | 26.3 KiB | 2026-03-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0134 | P | `v11_ID_backtesting.py` | 1 | 25.2 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0135 | P | `avwap_combined_runner_v17r_setup_lab_5min.py` | 1 | 23.9 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0136 | P | `avwap_combined_runner_v3.py` | 1 | 23.0 KiB | 2026-03-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0137 | P | `v19a_setup_param_sweep.py` | 1 | 22.8 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0138 | P | `research_v10_per_setup_honest.py` | 1 | 22.4 KiB | 2026-06-01 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0139 | P | `_v17r_nonf_advsetups_v2.py` | 1 | 22.2 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0140 | P | `avwap_combined_runner_v17s_5min.py` | 1 | 21.2 KiB | 2026-04-27 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0141 | P | `optimize_avwap_v7_sweep_walkforward.py` | 1 | 20.6 KiB | 2026-03-07 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0142 | P | `research_v11_deep_addon_search.py` | 1 | 20.1 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0143 | P | `research_v11_leftout_setup_iterations.py` | 1 | 19.6 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0144 | P | `research_v12_one_month_long_logic_posthoc_v10.py` | 1 | 19.2 KiB | 2026-08-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0145 | P | `avwap_combined_runner_v3_tgt11.py` | 1 | 19.1 KiB | 2026-03-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0146 | P | `avwap_combined_runner_v17c_shorts_boosted_5min.py` | 1 | 18.8 KiB | 2026-04-19 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0147 | P | `_v17r_setup_rescue.py` | 1 | 18.1 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0148 | P | `avwap_5min_ID_v8_backtesting.py` | 1 | 18.1 KiB | 2026-05-29 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0149 | P | `_v17r_setup_rescue_v2.py` | 1 | 18.1 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0150 | P | `avwap_5min_ID_v8_walkforward_optimizer.py` | 1 | 17.9 KiB | 2026-05-21 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0151 | P | `v17D_tax_analytics.py` | 1 | 16.8 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0152 | P | `avwap_combined_runner_v17l_5min.py` | 1 | 16.1 KiB | 2026-04-25 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0153 | P | `avwap_combined_runner_v17r_nonf_5min.py` | 1 | 16.0 KiB | 2026-05-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0154 | P | `research_v10_setup_deep_dive.py` | 1 | 16.0 KiB | 2026-06-01 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0155 | P | `avwap_combined_runner_v18c6_5min.py` | 1 | 16.0 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0156 | P | `new_setups_search_v11.py` | 1 | 15.8 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0157 | P | `tools/package_fno_v10_stage1.py` | 1 | 15.7 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0158 | P | `research_v11_hourly_prefilter_backtest.py` | 1 | 15.2 KiB | 2026-08-04 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0159 | P | `research_v11_a_mod_c1_low_relax.py` | 1 | 15.1 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0160 | P | `research_v11_residual_deeper_overlay.py` | 1 | 14.8 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0161 | P | `_v17r_nonf_advsetups_scan.py` | 1 | 14.7 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0162 | P | `avwap_combined_runner_v17C_noNF_live_5min_1week.py` | 1 | 14.2 KiB | 2026-05-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0163 | P | `v11_lab_book_validator.py` | 1 | 13.9 KiB | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0164 | P | `tools/report_v10_backtesting_range.py` | 1 | 13.0 KiB | 2026-05-30 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0165 | P | `eqidv2_v7_v11_parity_preflight.py` | 1 | 12.8 KiB | 2026-07-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0166 | P | `research_v11_new_addon_strategies.py` | 1 | 12.6 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0167 | P | `avwap_combined_runner_v17_5min.py` | 1 | 12.5 KiB | 2026-04-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0168 | P | `research_v11_tier123_filter_mining.py` | 1 | 12.5 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0169 | P | `avwap_combined_runner_v17C_noNF_live_5min_1whole.py` | 1 | 12.4 KiB | 2026-05-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0170 | P | `_v17r_validate.py` | 1 | 12.4 KiB | 2026-04-27 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0171 | P | `validate_B_setups_filters.py` | 1 | 12.3 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0172 | P | `v17D_backtest.py` | 1 | 12.3 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0173 | P | `_v17r_post_filter.py` | 1 | 12.2 KiB | 2026-04-30 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0174 | P | `research_v11_new_overlay_forward_test.py` | 1 | 11.8 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0175 | P | `avwap_combined_runner_v17e_5min.py` | 1 | 11.7 KiB | 2026-04-19 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0176 | P | `research_v11_tier123_subfilter_mine.py` | 1 | 11.6 KiB | 2026-06-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0177 | P | `_v17t_live_relax_high_pf.py` | 1 | 11.5 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0178 | P | `avwap_5min_ID_v8_walkforward_portfolio_optimizer.py` | 1 | 11.4 KiB | 2026-05-21 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0179 | P | `new_setups_v11_candidate_config.json` | 1 | 11.1 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0180 | P | `check_v18c6_exit_tgt06.py` | 1 | 11.0 KiB | 2026-04-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0181 | P | `validate_G_setups_filters.py` | 1 | 10.9 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0182 | P | `validate_D_setups_filters.py` | 1 | 10.7 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0183 | P | `_v17t_live_deep_optimizer.py` | 1 | 10.7 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0184 | P | `_v17t_live_relax_aggressive.py` | 1 | 10.7 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0185 | P | `_v17r_nonf_setup_lab.py` | 1 | 10.4 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0186 | P | `portfolio_backtest_v11.py` | 1 | 10.3 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0187 | P | `tools/package_fno_v10_stage0.py` | 1 | 10.1 KiB | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0188 | P | `quick_v18c6_patch_dates.py` | 1 | 10.0 KiB | 2026-04-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0189 | P | `validate_C_setups_filters.py` | 1 | 9.9 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0190 | P | `bat/schedule_fno_v8_combined_paper_disabled.ps1` | 1 | 9.4 KiB | 2026-08-21 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0191 | P | `validate_E_setups_filters.py` | 1 | 9.3 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0192 | P | `bat/schedule_fno_v10_v11_v12_paper_weekday.ps1` | 1 | 9.3 KiB | 2026-09-01 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0193 | P | `v20_sweep.py` | 1 | 9.1 KiB | 2026-04-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0194 | P | `avwap_5min_ID_v4_backtesting.py` | 1 | 8.9 KiB | 2026-05-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0195 | P | `research_v10_target_setups_pf13.py` | 1 | 8.7 KiB | 2026-06-01 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0196 | P | `_v17r_nonf_analyzer.py` | 1 | 8.7 KiB | 2026-05-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0197 | P | `C_setups_iterate.py` | 1 | 8.7 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0198 | P | `research/validate_v17g.py` | 1 | 8.6 KiB | 2026-04-20 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0199 | P | `v17D_enrich_features.py` | 1 | 8.5 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0200 | P | `new3_setups_scan_v11.py` | 1 | 8.5 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0201 | P | `v11_research_book_results.py` | 1 | 8.3 KiB | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0202 | P | `research/sweep_v17j_targets.py` | 1 | 8.1 KiB | 2026-04-25 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0203 | P | `_v17r_nonf_breadth_research.py` | 1 | 8.1 KiB | 2026-05-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0204 | P | `new2_setups_scan_v11.py` | 1 | 8.0 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0205 | P | `v17D_pareto_search.py` | 1 | 7.9 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0206 | P | `_analyze_shorts_v17c.py` | 1 | 7.6 KiB | 2026-04-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0207 | P | `v17D_trailing_stop.py` | 1 | 6.7 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0208 | P | `avwap_combined_runner_v17r_5min.py` | 1 | 6.7 KiB | 2026-04-27 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0209 | P | `v17D_drift_detection.py` | 1 | 6.6 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0210 | P | `v17D_slippage_stress_test.py` | 1 | 6.6 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0211 | P | `v17D_sector_rs.py` | 1 | 6.6 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0212 | P | `v17D_threshold_perturbation.py` | 1 | 6.4 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0213 | P | `v17D_apply_realistic_costs.py` | 1 | 6.3 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0214 | P | `v17D_reconcile.py` | 1 | 6.1 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0215 | P | `quick_reresolve_v18c3_longfilter.py` | 1 | 6.1 KiB | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0216 | P | `new_setups_salvage_search_v11.py` | 1 | 6.1 KiB | 2026-06-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0217 | P | `_analyze_v17bf_strategies.py` | 1 | 5.8 KiB | 2026-04-19 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0218 | P | `l_rs_leader_loose_scan_v11.py` | 1 | 5.8 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0219 | P | `v17D_logging.py` | 1 | 5.7 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0220 | P | `v17D_multi_tf.py` | 1 | 5.7 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0221 | P | `v17D_reresolve_with_mae_mfe.py` | 1 | 5.7 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0222 | P | `C_setups_rs_breakout.py` | 1 | 5.6 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0223 | P | `new_setups_overlay_gen_v11.py` | 1 | 5.5 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0224 | P | `_v17r_nonf_exit_sweep.py` | 1 | 5.3 KiB | 2026-05-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0225 | P | `v17D_pre_flight.py` | 1 | 5.2 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0226 | P | `_v17r_nonf_stack.py` | 1 | 5.0 KiB | 2026-05-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0227 | P | `_analyze_v17d_shorts.py` | 1 | 5.0 KiB | 2026-04-19 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0228 | P | `new_setups_salvage_gen_v11.py` | 1 | 5.0 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0229 | P | `L_setups_v11_candidate_config.json` | 1 | 4.9 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0230 | P | `v17D_kill_switch.py` | 1 | 4.9 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0231 | P | `v17D_time_stop.py` | 1 | 4.8 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0232 | P | `research/compare_v17g_v17h.py` | 1 | 4.8 KiB | 2026-04-21 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0233 | P | `v17D_atr_rank.py` | 1 | 4.7 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0234 | P | `B_setups_validate_recs.py` | 1 | 4.7 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0235 | P | `v17D_vol_sizing.py` | 1 | 4.6 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0236 | P | `research/compare_v17g_v17j.py` | 1 | 4.5 KiB | 2026-04-21 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0237 | P | `v17D_graveyard.py` | 1 | 4.4 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0238 | P | `B_setups_filter_experiment_plan.csv` | 1 | 4.2 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0239 | P | `B_setups_v11_candidate_config.json` | 1 | 4.2 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0240 | P | `_v17r_nonf_research.py` | 1 | 4.1 KiB | 2026-05-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0241 | P | `_v17q_compare_runs.py` | 1 | 4.1 KiB | 2026-04-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0242 | P | `B_setups_apply_suggested.py` | 1 | 4.0 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0243 | P | `run_fno_v8_sweep_all_setups.bat` | 1 | 3.8 KiB | 2026-08-19 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0244 | P | `v17D_test_all.py` | 1 | 3.6 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0245 | P | `_v17q_f12_compare.py` | 1 | 3.5 KiB | 2026-04-27 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0246 | P | `T_setups_v11_candidate_config.json` | 1 | 3.5 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0247 | P | `bat/run_backtest_v19_5min_avwap_rs.bat` | 1 | 3.4 KiB | 2026-04-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0248 | P | `G_setups_v11_candidate_config.json` | 1 | 3.4 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0249 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v7_sweep.bat` | 1 | 3.2 KiB | 2026-03-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0250 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v5.bat` | 1 | 3.1 KiB | 2026-03-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0251 | P | `v17D_refine_candE4.py` | 1 | 3.1 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0252 | P | `v17D_pre_entry_check.py` | 1 | 3.1 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0253 | P | `v17D_correlation_audit.py` | 1 | 2.9 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0254 | P | `_v17t_live_extract_balanced.py` | 1 | 2.9 KiB | 2026-04-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0255 | P | `run_fno_v8_setup_sweep.bat` | 1 | 2.9 KiB | 2026-08-19 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0256 | P | `avwap_combined_runner_v17r_nonf_5min_more.py` | 1 | 2.8 KiB | 2026-05-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0257 | P | `D_setups_v11_candidate_config.json` | 1 | 2.8 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0258 | P | `E_setups_v11_candidate_config.json` | 1 | 2.8 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0259 | P | `D_setups_filter_experiment_plan.csv` | 1 | 2.6 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0260 | P | `C_setups_v11_candidate_config.json` | 1 | 2.6 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0261 | P | `v17D_build_universe.py` | 1 | 2.4 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0262 | P | `C_setups_filter_experiment_plan.csv` | 1 | 2.4 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0263 | P | `E_setups_filter_experiment_plan.csv` | 1 | 2.3 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0264 | P | `final_setup_conf_v11_conf_c.py` | 1 | 2.3 KiB | 2026-07-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0265 | P | `S_setups_v11_candidate_config.json` | 1 | 2.2 KiB | 2026-06-12 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0266 | P | `bat/run_backtest_v20_5min_rai.bat` | 1 | 2.1 KiB | 2026-04-08 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0267 | P | `v17D_position_check.py` | 1 | 2.0 KiB | 2026-05-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0268 | P | `final_setup_conf_v11_book_b_time_filtered.py` | 1 | 1.9 KiB | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0269 | P | `G_setups_filter_experiment_plan.csv` | 1 | 1.8 KiB | 2026-06-11 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0270 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep.bat` | 1 | 1.7 KiB | 2026-03-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0271 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_v5.bat` | 1 | 1.6 KiB | 2026-03-09 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0272 | P | `final_setup_conf_v11_conf_b.py` | 1 | 1.6 KiB | 2026-07-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0273 | P | `final_setup_conf_v11_book_a_reduced_core.py` | 1 | 1.5 KiB | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0274 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE.bat` | 1 | 1.5 KiB | 2026-03-02 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0275 | P | `research_v12_rank200_240_six_month_walkforward_sl0p75_t1p5_v8.py` | 1 | 1.3 KiB | 2026-08-05 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0276 | P | `quick_reresolve_v18c6.py` | 1 | 1.2 KiB | 2026-04-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0277 | P | `quick_reresolve_v19a1.py` | 1 | 1.2 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0278 | P | `quick_reresolve_v19a2.py` | 1 | 1.2 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0279 | P | `quick_reresolve_v19a.py` | 1 | 1.2 KiB | 2026-04-15 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0280 | P | `_v17r_nonf_detail.py` | 1 | 1.1 KiB | 2026-05-14 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0281 | P | `debug_v10_short.py` | 1 | 1,011 B | 2026-03-07 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0282 | P | `bat/schedule_v11_lab_shadow_monitor_weekday.bat` | 1 | 960 B | 2026-07-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0283 | P | `final_setup_conf_v11_book_c_common.py` | 1 | 930 B | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0284 | P | `quick_reresolve_v19a3.py` | 1 | 780 B | 2026-04-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0285 | P | `quick_reresolve_v19a5.py` | 1 | 780 B | 2026-04-16 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0286 | P | `final_setup_conf_v11_conf_a.py` | 1 | 752 B | 2026-07-10 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0287 | P | `bat/run_avwap_v10_sweep.bat` | 1 | 633 B | 2026-03-07 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0288 | P | `bat/run_v7_v11_parity_preflight.bat` | 1 | 491 B | 2026-07-28 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0289 | P | `final_setup_conf_v11_book_c_trailing.py` | 1 | 279 B | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0290 | P | `final_setup_conf_v11_book_c_breakeven.py` | 1 | 261 B | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0291 | P | `final_setup_conf_v11_book_c_time120.py` | 1 | 250 B | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0292 | P | `quick_reresolve_v18c3.py` | 1 | 246 B | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0293 | P | `quick_reresolve_v18c4.py` | 1 | 246 B | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0294 | P | `final_setup_conf_v11_book_c_time90.py` | 1 | 245 B | 2026-07-22 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0295 | P | `quick_reresolve_v17c.py` | 1 | 245 B | 2026-04-18 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0296 | P | `quick_reresolve_v17b.py` | 1 | 243 B | 2026-04-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0297 | P | `tools/package_fno_v10_stage6.py` | 1 | 242 B | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0298 | P | `tools/package_fno_v10_stage7.py` | 1 | 242 B | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0299 | P | `tools/package_fno_v10_stage8.py` | 1 | 237 B | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0300 | P | `tools/package_fno_v10_stage9.py` | 1 | 237 B | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0301 | P | `tools/package_fno_v10_stage5.py` | 1 | 236 B | 2026-08-26 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0302 | P | `avwap_papertrade_true_v7_sweep.py` | 1 | 211 B | 2026-03-06 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0303 | P | `avwap_papertrade_true_v15.py` | 1 | 200 B | 2026-03-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0304 | P | `bat/run_avwap_papertrade_true_v7_sweep.bat` | 1 | 193 B | 2026-03-06 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |
| A-0305 | P | `bat/run_avwap_papertrade_true_v15.bat` | 1 | 187 B | 2026-03-13 | Legacy strategy/experiment bundle; no protected import, launcher, registered-task or exact directory reference found in this audit. |

## Hold for review — excluded from archive candidates

Mixed folders, separate personal material, ambiguous scripts, environment files, backups and unresolved historical evidence stay where they are. A mixed parent must never be archived wholesale.

| ID | Root | Exact relative path | Files | Logical size | Latest modified (UTC) | Reason / hold |
| --- | --- | --- | ---: | ---: | --- | --- |
| H-0001 | D | `_backup_eq_live2_20260613_115600/` | 1,291 | 3.487 GiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0002 | D | `fno_oi/strategy_research/_signal_cache/` | 3 | 21.48 MiB | 2026-08-11 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0003 | D | `fno_oi/strategy_research/_signal_cache_equity_1m_aggregated_5m_futures_oi_v3/` | 3 | 33.86 MiB | 2026-08-17 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0004 | D | `fno_oi/strategy_research/_signal_cache_hybrid_equity_price_futures_oi_real_bars_v2/` | 3 | 20.66 MiB | 2026-08-11 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0005 | D | `fno_oi/strategy_research/_signal_cache_hybrid_equity_price_futures_oi_v1/` | 3 | 20.34 MiB | 2026-08-11 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0006 | D | `fno_oi/strategy_research/archive/` | 11 | 131.1 KiB | 2026-08-11 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0007 | D | `fno_oi/strategy_research/fno_today_20260828/` | 422 | 2.986 GiB | 2026-08-28 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0008 | D | `fno_oi/strategy_research/fno_today_20260901/` | 422 | 3.005 GiB | 2026-09-01 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0009 | D | `fno_oi/strategy_research/fno_v6_v8_v10_repaired_comparison_20260828_v1/` | 31 | 1.04 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0010 | D | `fno_oi/strategy_research/symmetric_1pct_exit_counterfactual/` | 2 | 6.4 KiB | 2026-09-03 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0011 | D | `fno_oi/strategy_research/today_current_refresh_20260831/` | 5 | 71.91 MiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0012 | D | `fno_oi/strategy_research/today_six_strategy_replays_20260901/` | 66 | 1.29 MiB | 2026-09-01 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0013 | D | `fno_oi/strategy_research/today_v10_v11_refreshed_paths_v1/` | 14 | 186.5 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0014 | D | `fno_oi/strategy_research/today_v10_v11_v12_current_20260831_v1/` | 25 | 411.6 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0015 | D | `fno_oi/strategy_research/today_v10_v11_v12_final_20260831_v1/` | 25 | 411.1 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0016 | D | `fno_oi/strategy_research/today_v11_stage10_current_v1/` | 8 | 86.6 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0017 | D | `fno_oi/strategy_research/today_v12_selected_refreshed_paths_v1/` | 22 | 143.93 MiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0018 | D | `fno_oi/strategy_research/today_v12_selected_replays_v1/` | 10 | 86.7 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0019 | D | `fno_oi/strategy_research/today_v6_v8_current_compare_20260831/` | 25 | 411.6 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0020 | D | `fno_oi/strategy_research/today_v6_v8_final_20260831/` | 25 | 411.0 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0021 | D | `fno_oi/strategy_research/today_v6_v8_v10_v11_v12_final_20260831/` | 25 | 411.1 KiB | 2026-08-31 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0022 | D | `fno_oi/strategy_research/v10_stage7_locked_backtest_v1/` | 11 | 1.01 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0023 | D | `fno_oi/strategy_research/v10_v12_day_20260902/` | 846 | 6.011 GiB | 2026-09-03 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0024 | D | `fno_oi/strategy_research/v11_fno_staged_research_v1/` | 1,005 | 740.04 MiB | 2026-08-30 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0025 | D | `fno_oi/strategy_research/v12_corrected/` | 847 | 6.030 GiB | 2026-09-04 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0026 | D | `fno_oi/strategy_research/v14_master_20260913_122535/` | 770 | 2.223 GiB | 2026-09-13 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0027 | D | `fno_oi/strategy_research/v14_regression_20260913/` | 74 | 12.97 MiB | 2026-09-13 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0028 | D | `fno_oi/strategy_research/v6_isolated_challengers_full59_today_20260827T2310_v2/` | 175 | 51.18 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0029 | D | `fno_oi/strategy_research/v6_isolated_challengers_repaired_full59_today_20260827T2318_v1/` | 237 | 61.10 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0030 | D | `fno_oi/strategy_research/v6_isolated_challengers_repaired_full59_today_20260827T2318_v2/` | 4 | 322.2 KiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0031 | D | `fno_oi/strategy_research/v6_isolated_challengers_repaired_full59_today_20260827T2318_v3/` | 251 | 61.63 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0032 | D | `fno_oi/strategy_research/v6_isolated_challengers_repaired_full59_today_20260828T0005_v4/` | 257 | 61.99 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0033 | D | `fno_oi/strategy_research/v6_last_10_rollover_diagnostic/` | 6 | 32.1 KiB | 2026-08-26 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0034 | D | `fno_oi/strategy_research/v6_today_selection_audit_2026-09-03/` | 7 | 187.9 KiB | 2026-09-03 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0035 | D | `fno_oi/strategy_research/v6_v10_challenger_robustness_audit_v1/` | 76 | 3.46 MiB | 2026-08-27 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0036 | D | `fno_oi/strategy_research/v6_v2_corrected/` | 9 | 1.89 MiB | 2026-09-03 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0037 | D | `fno_oi/strategy_research/v9_block_tuning/` | 1 | 1.6 KiB | 2026-08-20 | Historical or predecessor research package, cache or archive. Current-source dependency closure, protected descendants and provenance references must be excluded before any future archive. Not confirmed unused. |
| H-0038 | D | `fno_oi_fast_production_history_clone_20260901/` | 881 | 69.44 MiB | 2026-09-01 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0039 | D | `fno_oi_fast_production_trial_validation_20260901/` | 222 | 2.83 MiB | 2026-09-01 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0040 | D | `hm_iterative/` | 191 | 20.72 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0041 | D | `outputs_ID_v10_full_historical_v7_live_logic/` | 20,226 | 1.214 GiB | 2026-06-01 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0042 | D | `outputs_ID_v10_full_historical_v7_live_logic_bad_primary_20260531_233240/` | 1,148 | 676.3 KiB | 2026-05-31 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0043 | D | `outputs_ID_v10_full_v7_live_style_replay_2026-05-22_to_2026-06-04/` | 625 | 50.96 MiB | 2026-06-05 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0044 | D | `outputs_ID_v10_full_v7_live_style_replay_2026-05-22_to_2026-06-04_live2/` | 625 | 3.96 MiB | 2026-06-05 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0045 | D | `outputs_ID_v10_latest_v7_live_logic_replay_2026-05-22_to_2026-06-04/` | 625 | 56.95 MiB | 2026-06-05 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0046 | D | `outputs_ID_v10_latest_v7_live_logic_replay_2026-05-22_to_2026-06-04_live2/` | 625 | 3.85 MiB | 2026-06-05 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0047 | D | `outputs_ID_v11_conf_16_full/` | 14,393 | 1.860 GiB | 2026-06-13 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0048 | D | `outputs_ID_v11_conf_16_smoke/` | 2,465 | 328.13 MiB | 2026-06-13 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0049 | D | `outputs_ID_v11_conf_20260616/` | 113 | 11.71 MiB | 2026-06-16 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0050 | D | `outputs_ID_v11_conf_book_20260504_20260701/` | 3,473 | 399.52 MiB | 2026-07-03 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0051 | D | `outputs_ID_v11_conf_ext_test/` | 146 | 7.46 MiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0052 | D | `outputs_ID_v11_conf_ext_verify/` | 113 | 11.80 MiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0053 | D | `outputs_ID_v11_conf_full/` | 21,449 | 1.967 GiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0054 | D | `outputs_ID_v11_conf_full_10/` | 2,352 | 140.93 MiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0055 | D | `outputs_ID_v11_conf_full_readmit/` | 9,156 | 553.27 MiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0056 | D | `outputs_ID_v11_conf_paper_20260615/` | 113 | 11.13 MiB | 2026-06-15 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0057 | D | `outputs_ID_v11_conf_readmit/` | 1,289 | 125.84 MiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0058 | D | `outputs_ID_v11_conf_test/` | 984 | 95.36 MiB | 2026-06-15 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0059 | D | `outputs_ID_v11_conf_test_range/` | 281 | 16.90 MiB | 2026-06-12 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0060 | D | `outputs_ID_v11_conf_TRAIN/` | 4,985 | 680.98 MiB | 2026-06-15 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0061 | D | `outputs_ID_v11_full_historical_ab_only_probe_q250/` | 86 | 3.02 MiB | 2026-06-01 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0062 | D | `outputs_ID_v11_full_historical_production_core/` | 20,690 | 1.189 GiB | 2026-06-01 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0063 | D | `outputs_ID_v11_historical_all_data/` | 20,194 | 1.179 GiB | 2026-06-01 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0064 | D | `outputs_ID_v11_last2wk_baseline/` | 971 | 93.15 MiB | 2026-07-08 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0065 | D | `outputs_ID_v11_oos_0526_0620/` | 1,665 | 173.89 MiB | 2026-07-08 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0066 | D | `outputs_ID_v11_parity_test/` | 152 | 3.72 MiB | 2026-06-11 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0067 | D | `outputs_ID_v11_production_core_ab_good_probe/` | 20,690 | 1.196 GiB | 2026-06-02 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0068 | D | `outputs_ID_v11_tier123_balanced_smoke_2026-05-29/` | 62 | 819.5 KiB | 2026-06-02 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0069 | D | `replay/` | 427 | 13.59 MiB | 2026-07-26 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0070 | D | `replay_parity_fix_20260728/` | 232 | 7.65 MiB | 2026-07-28 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0071 | D | `v11_research_books_20260722/` | 3,623 | 441.39 MiB | 2026-07-22 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0072 | D | `v14/` | 2,489 | 2.152 GiB | 2026-10-10 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0073 | D | `v14_data_smoke/` | 6 | 4.09 MiB | 2026-10-10 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0074 | D | `v7_entry_timing_comparison/` | 12 | 81.34 MiB | 2026-06-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0075 | D | `v7_entry_timing_comparison_smoke/` | 10 | 1.48 MiB | 2026-06-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0076 | D | `v7_live_1min_backtest_reports/` | 2 | 239.8 KiB | 2026-06-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0077 | D | `fno_oi/fetch_manifest.json` | 1 | 2.4 KiB | 2026-09-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0078 | D | `fno_oi/fetch_outcomes.csv` | 1 | 1.1 KiB | 2026-09-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0079 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_best_full_daily.csv` | 1 | 10.2 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0080 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_all_objectives_daily.csv` | 1 | 26.2 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0081 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_best_net_daywise_with_entries.csv` | 1 | 14.9 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0082 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_best_net_entries.csv` | 1 | 51.4 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0083 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_ranked_portfolios.csv` | 1 | 2.8 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0084 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_daily.csv` | 1 | 8.6 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0085 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_setups.csv` | 1 | 5.0 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0086 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_full_history_selected_trades.csv` | 1 | 123.1 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0087 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_latest_replay_setups.csv` | 1 | 756 B | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0088 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_latest_replay_trades.csv` | 1 | 25.6 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0089 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_selected_setups.csv` | 1 | 994 B | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0090 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_selected_trades.csv` | 1 | 28.1 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0091 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v6_best_net_daily.csv` | 1 | 13.1 KiB | 2026-08-31 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0092 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_20260811.csv` | 1 | 11.2 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0093 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_current_source_20260818_v1.csv` | 1 | 11.4 KiB | 2026-08-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0094 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v6_best_net_selected_current_source_20260818_v1.provenance.json` | 1 | 164.9 KiB | 2026-08-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0095 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v6_best_net_setups.csv` | 1 | 2.2 KiB | 2026-08-31 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0096 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v6_best_net_trades.csv` | 1 | 118.4 KiB | 2026-08-31 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0097 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_daily.csv` | 1 | 14.8 KiB | 2026-08-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0098 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_setups.csv` | 1 | 2.5 KiB | 2026-08-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0099 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v7_extreme_break_trades.csv` | 1 | 241.5 KiB | 2026-08-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0100 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_v4_best_full_daily.csv` | 1 | 86.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0101 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_v4_ranked_LONG.csv` | 1 | 719.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0102 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_v4_ranked_SHORT.csv` | 1 | 670.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0103 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_v4_selected_daily.csv` | 1 | 27.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0104 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_v4_selected_setups.csv` | 1 | 927 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0105 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_v4_selected_trades.csv` | 1 | 28.1 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0106 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_short2_expansion_v3_daily.csv` | 1 | 99.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0107 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_short2_expansion_v3_ranked.csv` | 1 | 904.1 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0108 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_short2_expansion_v3_selected_daily.csv` | 1 | 32.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0109 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_short2_expansion_v3_selected_trades.csv` | 1 | 17.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0110 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_best_side_combo_daily.csv` | 1 | 50.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0111 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_force_daily_cumulative_pf.csv` | 1 | 47.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0112 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_force_daily_daily_pf.csv` | 1 | 88.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0113 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_force_daily_portfolio_daily_pf.csv` | 1 | 52.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0114 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_force_daily_ranked_LONG.csv` | 1 | 3.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0115 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_force_daily_ranked_SHORT.csv` | 1 | 12.6 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0116 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_force_daily_selected_trades.csv` | 1 | 90.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0117 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_once_cumulative_pf.csv` | 1 | 34.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0118 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_once_daily_pf.csv` | 1 | 57.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0119 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_once_portfolio_daily_pf.csv` | 1 | 28.9 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0120 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_once_ranked_LONG.csv` | 1 | 79.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0121 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_once_ranked_SHORT.csv` | 1 | 88.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0122 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_once_selected_trades.csv` | 1 | 24.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0123 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_v3_selected_daily.csv` | 1 | 25.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0124 | D | `fno_oi/strategy_research/ema_confirm_0925_best_side_combo_daily.csv` | 1 | 8.2 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0125 | D | `fno_oi/strategy_research/ema_confirm_0925_force_daily_cumulative_pf.csv` | 1 | 26.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0126 | D | `fno_oi/strategy_research/ema_confirm_0925_force_daily_daily_pf.csv` | 1 | 42.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0127 | D | `fno_oi/strategy_research/ema_confirm_0925_force_daily_portfolio_daily_pf.csv` | 1 | 21.2 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0128 | D | `fno_oi/strategy_research/ema_confirm_0925_force_daily_ranked_LONG.csv` | 1 | 23.9 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0129 | D | `fno_oi/strategy_research/ema_confirm_0925_force_daily_ranked_SHORT.csv` | 1 | 14.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0130 | D | `fno_oi/strategy_research/ema_confirm_0925_long2_expansion_v2_daily.csv` | 1 | 39.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0131 | D | `fno_oi/strategy_research/ema_confirm_0925_long2_expansion_v2_ranked.csv` | 1 | 666.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0132 | D | `fno_oi/strategy_research/ema_confirm_0925_long2_expansion_v2_selected_trades.csv` | 1 | 53.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0133 | D | `fno_oi/strategy_research/ema_confirm_0925_once_cumulative_pf.csv` | 1 | 25.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0134 | D | `fno_oi/strategy_research/ema_confirm_0925_once_daily_pf.csv` | 1 | 38.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0135 | D | `fno_oi/strategy_research/ema_confirm_0925_once_portfolio_daily_pf.csv` | 1 | 18.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0136 | D | `fno_oi/strategy_research/ema_confirm_0925_once_ranked_LONG.csv` | 1 | 41.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0137 | D | `fno_oi/strategy_research/ema_confirm_0925_once_ranked_SHORT.csv` | 1 | 50.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0138 | D | `fno_oi/strategy_research/ema_confirm_0925_short_expansion_v2_daily.csv` | 1 | 54.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0139 | D | `fno_oi/strategy_research/ema_confirm_0925_short_expansion_v2_selected_trades.csv` | 1 | 27.1 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0140 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_best_side_combo_daily.csv` | 1 | 25.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0141 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_force_daily_cumulative_pf.csv` | 1 | 44.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0142 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_force_daily_daily_pf.csv` | 1 | 74.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0143 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_force_daily_portfolio_daily_pf.csv` | 1 | 41.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0144 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_force_daily_ranked_LONG.csv` | 1 | 30.6 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0145 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_force_daily_ranked_SHORT.csv` | 1 | 18.9 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0146 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_force_daily_selected_trades.csv` | 1 | 57.2 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0147 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_once_cumulative_pf.csv` | 1 | 41.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0148 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_once_daily_pf.csv` | 1 | 65.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0149 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_once_portfolio_daily_pf.csv` | 1 | 34.9 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0150 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_once_ranked_LONG.csv` | 1 | 52.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0151 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_once_ranked_SHORT.csv` | 1 | 62.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0152 | D | `fno_oi/strategy_research/ema_confirm_0925_v2_once_selected_trades.csv` | 1 | 33.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0153 | D | `fno_oi/strategy_research/ema_confirm_2026-08-10_0925.csv` | 1 | 1.1 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0154 | D | `fno_oi/strategy_research/ema_confirm_fullrun_trades.csv` | 1 | 72.1 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0155 | D | `fno_oi/strategy_research/ema_confirm_fullrun_trades_smoke.csv` | 1 | 21.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0156 | D | `fno_oi/strategy_research/ema_confirm_sweep_52d.csv` | 1 | 3.36 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0157 | D | `fno_oi/strategy_research/ema_confirm_sweep_today.csv` | 1 | 1.65 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0158 | D | `fno_oi/strategy_research/ema_confirm_v5_0940_ranked_LONG.csv` | 1 | 2 B | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0159 | D | `fno_oi/strategy_research/ema_confirm_v5_0940_ranked_SHORT.csv` | 1 | 2.17 MiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0160 | D | `fno_oi/strategy_research/ema_confirm_v5_0940_retained_portfolios.csv` | 1 | 198.6 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0161 | D | `fno_oi/strategy_research/ema_confirm_v5_0945_ranked_LONG.csv` | 1 | 3.9 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0162 | D | `fno_oi/strategy_research/ema_confirm_v5_0945_ranked_SHORT.csv` | 1 | 743.4 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0163 | D | `fno_oi/strategy_research/ema_confirm_v5_0945_retained_portfolios.csv` | 1 | 214.5 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0164 | D | `fno_oi/strategy_research/ema_confirm_v5_full_history_0940_ranked_LONG.csv` | 1 | 2.26 MiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0165 | D | `fno_oi/strategy_research/ema_confirm_v5_full_history_0940_ranked_SHORT.csv` | 1 | 1.61 MiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0166 | D | `fno_oi/strategy_research/ema_confirm_v5_full_history_0940_retained_portfolios.csv` | 1 | 544.8 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0167 | D | `fno_oi/strategy_research/ema_confirm_v5_full_history_0945_ranked_LONG.csv` | 1 | 1.44 MiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0168 | D | `fno_oi/strategy_research/ema_confirm_v5_full_history_0945_ranked_SHORT.csv` | 1 | 1.59 MiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0169 | D | `fno_oi/strategy_research/ema_confirm_v5_full_history_0945_retained_portfolios.csv` | 1 | 258.7 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0170 | D | `fno_oi/strategy_research/ema_confirm_v6_0926_only_trades.csv` | 1 | 32.7 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0171 | D | `fno_oi/strategy_research/fno_v6_legacy_selected_mismatch_audit_20260818.json` | 1 | 2.0 KiB | 2026-08-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0172 | D | `fno_oi/strategy_research/latest_results.json` | 1 | 9.3 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0173 | D | `fno_oi/strategy_research/optimize_shortlist_LONG.csv` | 1 | 8.2 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0174 | D | `fno_oi/strategy_research/optimize_shortlist_SHORT.csv` | 1 | 2.5 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0175 | D | `fno_oi/strategy_research/pf_two_metrics_LONG.csv` | 1 | 20.81 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0176 | D | `fno_oi/strategy_research/pf_two_metrics_SHORT.csv` | 1 | 41.25 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0177 | D | `fno_oi/strategy_research/slot0925_LONG.csv` | 1 | 1.56 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0178 | D | `fno_oi/strategy_research/slot0925_once_LONG.csv` | 1 | 1.79 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0179 | D | `fno_oi/strategy_research/slot0925_once_SHORT.csv` | 1 | 4.57 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0180 | D | `fno_oi/strategy_research/slot0925_SHORT.csv` | 1 | 4.53 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0181 | D | `fno_oi/strategy_research/trades_L1_LONG_UNWINDING.csv` | 1 | 100.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0182 | D | `fno_oi/strategy_research/trades_L2_FADE_SHORT_BUILDUP.csv` | 1 | 2.15 MiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0183 | D | `fno_oi/strategy_research/trades_S1_FADE_LONG_BUILDUP.csv` | 1 | 1,011.4 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0184 | D | `fno_oi/strategy_research/trades_S2_FADE_LONG_BUILDUP_PM.csv` | 1 | 383.2 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0185 | D | `fno_oi/strategy_research/trades_S3_FADE_LONG_BUILDUP_VOLSPIKE.csv` | 1 | 644.7 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0186 | D | `fno_oi/strategy_research/v5_0926_all_history_day_pf_manifest.json` | 1 | 4.9 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0187 | D | `fno_oi/strategy_research/v5_0926_all_history_day_pf_primary_daily.csv` | 1 | 9.7 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0188 | D | `fno_oi/strategy_research/v5_0926_all_history_day_pf_primary_setups.csv` | 1 | 1.1 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0189 | D | `fno_oi/strategy_research/v5_0926_all_history_day_pf_primary_trades.csv` | 1 | 16.7 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0190 | D | `fno_oi/strategy_research/v5_0926_all_history_day_pf_ranked_portfolios.csv` | 1 | 30.0 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0191 | D | `fno_oi/strategy_research/v5_0926_day_pf_manifest.json` | 1 | 4.8 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0192 | D | `fno_oi/strategy_research/v5_0926_day_pf_primary_daily.csv` | 1 | 9.8 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0193 | D | `fno_oi/strategy_research/v5_0926_day_pf_primary_setups.csv` | 1 | 1.1 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0194 | D | `fno_oi/strategy_research/v5_0926_day_pf_primary_trades.csv` | 1 | 22.3 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0195 | D | `fno_oi/strategy_research/v5_0926_day_pf_ranked_portfolios.csv` | 1 | 29.2 KiB | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0196 | D | `fno_oi/strategy_research/v5_hybrid_optimize_primary_daily.csv` | 1 | 10.1 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0197 | D | `fno_oi/strategy_research/v5_hybrid_optimize_primary_setups.csv` | 1 | 996 B | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0198 | D | `fno_oi/strategy_research/v5_hybrid_optimize_primary_trades.csv` | 1 | 28.3 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0199 | D | `fno_oi/strategy_research/v5_hybrid_optimize_ranked_portfolios.csv` | 1 | 12.5 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0200 | D | `fno_oi/strategy_research/v5_protected_selected_manifest.json` | 1 | 734 B | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0201 | D | `fno_oi/strategy_research/v6_provenance_replay.stderr.log` | 1 | 736 B | 2026-08-17 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0202 | D | `fno_oi/strategy_research/v6_provenance_replay.stdout.log` | 1 | 247 B | 2026-08-17 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0203 | D | `fno_oi/strategy_research/v7_extreme_break_20260818_143904.stderr.log` | 1 | 845 B | 2026-08-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0204 | D | `fno_oi/strategy_research/v7_extreme_break_20260818_143904.stdout.log` | 1 | 237 B | 2026-08-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0205 | D | `fno_oi/strategy_research/v7_frozen_20260818_145531.stderr.log` | 1 | 0 B | 2026-08-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0206 | D | `fno_oi/strategy_research/v7_frozen_20260818_145531.stdout.log` | 1 | 744 B | 2026-08-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0207 | D | `fno_oi/strategy_research/v8_combined_best_per_leg_run.stderr.log` | 1 | 1.8 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0208 | D | `fno_oi/strategy_research/v8_combined_best_per_leg_run.stdout.log` | 1 | 2.0 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0209 | D | `outputs_ID_v11_conf_book_20260504_20260701_run.log` | 1 | 85.8 KiB | 2026-07-03 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0210 | D | `v11_3wk_stderr.log` | 1 | 0 B | 2026-07-24 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0211 | D | `v11_3wk_stdout.log` | 1 | 3.3 KiB | 2026-07-24 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0212 | D | `v11_C_diag.log` | 1 | 860 B | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0213 | D | `v11_C_diag2.log` | 1 | 2.6 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0214 | D | `v11_C_iterate.log` | 1 | 3.0 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0215 | D | `v11_C_iterate2.log` | 1 | 3.1 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0216 | D | `v11_cleanpool_build.log` | 1 | 172.9 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0217 | D | `v11_conf_16_full.log` | 1 | 168.4 KiB | 2026-06-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0218 | D | `v11_conf_16_smoke2.log` | 1 | 33.0 KiB | 2026-06-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0219 | D | `v11_conf_ext_test.log` | 1 | 3.8 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0220 | D | `v11_conf_ext_verify.log` | 1 | 4.3 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0221 | D | `v11_conf_full.log` | 1 | 244.3 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0222 | D | `v11_conf_full_10.log` | 1 | 24.8 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0223 | D | `v11_conf_full_readmit.log` | 1 | 104.7 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0224 | D | `v11_conf_readmit.log` | 1 | 17.4 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0225 | D | `v11_conf_test.log` | 1 | 3.0 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0226 | D | `v11_conf_test_range.log` | 1 | 5.5 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0227 | D | `v11_D_diag.log` | 1 | 7.8 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0228 | D | `v11_E_diag.log` | 1 | 8.2 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0229 | D | `v11_E_salvage.log` | 1 | 2.3 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0230 | D | `v11_G_diag.log` | 1 | 1.9 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0231 | D | `v11_G_iterate.log` | 1 | 4.9 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0232 | D | `v11_ID_parity_test.log` | 1 | 3.7 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0233 | D | `v11_ID_parity_test2.log` | 1 | 4.7 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0234 | D | `v11_L_iterate.log` | 1 | 8.9 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0235 | D | `v11_newsetups_scan.log` | 1 | 1.3 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0236 | D | `v11_newsetups_search.log` | 1 | 6.9 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0237 | D | `v11_pool_build.log` | 1 | 55.8 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0238 | D | `v11_portfolio.log` | 1 | 2.4 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0239 | D | `v11_S_iterate.log` | 1 | 5.6 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0240 | D | `v11_T_iterate.log` | 1 | 10.4 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0241 | P | `.claude/` | 2 | 1.1 KiB | 2026-06-30 | Environment, tool configuration or generated cache; retain pending a separate retention decision. |
| H-0242 | P | `.codex_tmp/g_daily_validation/` | 2 | 2.7 KiB | 2026-09-14 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0243 | P | `.codex_tmp/g_options/` | 58 | 1.11 MiB | 2026-09-14 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0244 | P | `.codex_tmp/oi_gate_report/` | 677 | 27.11 MiB | 2026-09-02 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0245 | P | `.codex_tmp/options_projection/` | 47 | 1.54 MiB | 2026-09-14 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0246 | P | `.pytest_cache/` | 6 | 357.2 KiB | 2026-10-10 | Environment, tool configuration or generated cache; retain pending a separate retention decision. |
| H-0247 | P | `.tmp_resume_review/` | 7 | 1.28 MiB | 2026-07-29 | Separate personal/project material; unrelated to trading does not establish that it is unused. |
| H-0248 | P | `.venv-ai-platform/` | 4,611 | 51.05 MiB | 2026-09-24 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0249 | P | `__pycache__/` | 604 | 21.26 MiB | 2026-10-10 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0250 | P | `bat/__pycache__/` | 1 | 8.7 KiB | 2026-04-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0251 | P | `experimental_prefilter/` | 20 | 186.6 KiB | 2026-08-04 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0252 | P | `house-of-yeashi-growth-kit/` | 12 | 8.32 MiB | 2026-08-05 | Separate personal/project material; unrelated to trading does not establish that it is unused. |
| H-0253 | P | `outputs/` | 19 | 15.24 MiB | 2026-10-04 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0254 | P | `research/__pycache__/` | 37 | 755.9 KiB | 2026-10-10 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0255 | P | `research/results/` | 70 | 43.25 MiB | 2026-05-16 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0256 | P | `tmp_task_backups/` | 1 | 3.6 KiB | 2026-06-10 | Mixed research, training, shared library or backup bundle; usage cannot be established from names alone. |
| H-0257 | P | `tools/__pycache__/` | 39 | 1.12 MiB | 2026-10-10 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0258 | P | `v14/` | 77 | 1.55 MiB | 2026-10-10 | Mixed directory contains protected files/references. Do not archive the parent as a unit. |
| H-0259 | P | `.codex_tmp/build_sl_innovation_report.py` | 1 | 18.9 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0260 | P | `.codex_tmp/build_sl_tradeoff_report.py` | 1 | 18.8 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0261 | P | `.codex_tmp/conservative_probe.csv` | 1 | 65.3 KiB | 2026-09-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0262 | P | `.codex_tmp/followthrough_extended_check.cjs` | 1 | 5.4 KiB | 2026-10-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0263 | P | `.codex_tmp/followthrough_report.js` | 1 | 30.5 KiB | 2026-10-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0264 | P | `.codex_tmp/followthrough_runtime_check.cjs` | 1 | 3.1 KiB | 2026-10-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0265 | P | `.codex_tmp/g_daily_replay_sep11_raw_parity.json` | 1 | 30.9 KiB | 2026-09-14 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0266 | P | `.codex_tmp/g_document_cover.png` | 1 | 1.48 MiB | 2026-09-14 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0267 | P | `.codex_tmp/g_preopen_names.patch` | 1 | 3.9 KiB | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0268 | P | `.codex_tmp/sl_innovation_common.py` | 1 | 5.1 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0269 | P | `.codex_tmp/sl_innovation_confirmation.py` | 1 | 14.2 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0270 | P | `.codex_tmp/sl_innovation_finalist_audit.py` | 1 | 11.3 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0271 | P | `.codex_tmp/sl_innovation_neighbors.py` | 1 | 7.0 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0272 | P | `.codex_tmp/sl_innovation_progress.py` | 1 | 7.5 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0273 | P | `.codex_tmp/sl_innovation_protocol.json` | 1 | 1.9 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0274 | P | `.codex_tmp/sl_innovation_volatility.py` | 1 | 11.4 KiB | 2026-10-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0275 | P | `.codex_tmp/sl_tradeoff_pullbacks.py` | 1 | 17.2 KiB | 2026-10-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0276 | P | `.codex_tmp/sl_tradeoff_static.py` | 1 | 12.8 KiB | 2026-10-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0277 | P | `.codex_tmp/sl_tradeoff_timing.py` | 1 | 14.9 KiB | 2026-10-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0278 | P | `.codex_tmp/v10_f_volume120_replay.log` | 1 | 29.5 KiB | 2026-09-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0279 | P | `.codex_tmp/verify_fno_check_reporting.py` | 1 | 2.4 KiB | 2026-10-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0280 | P | `.codex_tmp/volume_entry_diagnostic_20261005.json` | 1 | 43.5 KiB | 2026-10-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0281 | P | `.codex_tmp/volume_entry_diagnostic_20261005.py` | 1 | 9.2 KiB | 2026-10-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0282 | P | `0.7` | 1 | 49 B | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0283 | P | `1.0` | 1 | 53 B | 2026-03-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0284 | P | `1.2` | 1 | 56 B | 2026-03-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0285 | P | `1.5` | 1 | 47 B | 2026-03-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0286 | P | `2.0` | 1 | 47 B | 2026-03-04 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0287 | P | `6` | 1 | 44 B | 2026-03-03 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0288 | P | `_analyze_long_trades.py` | 1 | 10.3 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0289 | P | `_analyze_new_scenarios.py` | 1 | 2.1 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0290 | P | `_analyze_today.py` | 1 | 1.1 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0291 | P | `_check_5min_dirs.py` | 1 | 615 B | 2026-05-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0292 | P | `_count_configs.bat` | 1 | 193 B | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0293 | P | `_count_configs.py` | 1 | 1.6 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0294 | P | `_count_configs2.bat` | 1 | 233 B | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0295 | P | `_csv_scan_list.txt` | 1 | 30.4 KiB | 2026-05-16 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0296 | P | `_deep3_loss_study.py` | 1 | 10.6 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0297 | P | `_deep3_loss_study_out.txt` | 1 | 26.5 KiB | 2026-04-06 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0298 | P | `_deep_loss_analysis.py` | 1 | 9.9 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0299 | P | `_deep_loss_analysis_out.txt` | 1 | 35.2 KiB | 2026-04-06 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0300 | P | `_kill_sweep.bat` | 1 | 94 B | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0301 | P | `_or_close_filter_out.txt` | 1 | 26 B | 2026-09-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0302 | P | `_or_close_filter_test.py` | 1 | 11.5 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0303 | P | `_reprice_optionB.py` | 1 | 3.9 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0304 | P | `_show_window_progress.py` | 1 | 1.5 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0305 | P | `_sl_tgt_sweep.py` | 1 | 10.6 KiB | 2026-04-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0306 | P | `_sweep_status.bat` | 1 | 192 B | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0307 | P | `_sweep_status.py` | 1 | 1.8 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0308 | P | `_tmp_frontier_pf2p0.csv` | 1 | 1.92 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0309 | P | `_tmp_frontier_pf2p1.csv` | 1 | 1.92 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0310 | P | `_tmp_frontier_pf2p2.csv` | 1 | 1.90 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0311 | P | `_tmp_frontier_pf2p3.csv` | 1 | 1.88 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0312 | P | `_tmp_frontier_pf2p4.csv` | 1 | 1.85 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0313 | P | `_tmp_frontier_pf2p5.csv` | 1 | 1.84 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0314 | P | `_tmp_frontier_pf2p6.csv` | 1 | 1.79 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0315 | P | `_tmp_frontier_simple_pf2p0.csv` | 1 | 1.92 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0316 | P | `_tmp_frontier_simple_pf2p1.csv` | 1 | 1.92 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0317 | P | `_tmp_frontier_simple_pf2p2.csv` | 1 | 1.87 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0318 | P | `_tmp_frontier_simple_pf2p3.csv` | 1 | 1.85 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0319 | P | `_tmp_frontier_simple_pf2p4.csv` | 1 | 1.77 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0320 | P | `_tmp_frontier_simple_pf2p5.csv` | 1 | 1.59 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0321 | P | `_tmp_frontier_simple_pf2p6.csv` | 1 | 1.60 MiB | 2026-05-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0322 | P | `advanced_indicators_calculator.py` | 1 | 25.3 KiB | 2026-05-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0323 | P | `alert_zero_entry_watchdog.py` | 1 | 9.2 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0324 | P | `all_setups_catalog.py` | 1 | 21.9 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0325 | P | `amod_mine_gen.py` | 1 | 1.6 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0326 | P | `ANALYSIS_V16_5MIN_ENTRY_SKIP_ISSUES.md` | 1 | 23.2 KiB | 2026-04-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0327 | P | `audit_prefilter_long_opportunity_count.py` | 1 | 5.0 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0328 | P | `audit_stocks_indicators_coverage.py` | 1 | 15.5 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0329 | P | `authentication_backup.py` | 1 | 10.5 KiB | 2026-02-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0330 | P | `authentication_v3.py` | 1 | 24.1 KiB | 2026-04-18 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0331 | P | `avwap_5min_ID_v11_backtesting.md` | 1 | 34.3 KiB | 2026-07-28 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0332 | P | `avwap_combined_runner_v17p_5min.py.tmp.21620.1777185116536` | 1 | 14.2 KiB | 2026-04-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0333 | P | `AVWAP_V15_5MIN_STRATEGY_DOCUMENT.txt` | 1 | 29.6 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0334 | P | `AVWAP_V16_5min_avwap_runner.txt.txt` | 1 | 12.4 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0335 | P | `B_setups_deep_diagnosis_v11.md` | 1 | 15.7 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0336 | P | `bat/_replay_run_now.bat` | 1 | 773 B | 2026-06-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0337 | P | `bat/configure_defender_exclusions_v16_5min.ps1` | 1 | 4.6 KiB | 2026-04-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0338 | P | `bat/create_task_eod_1min_data_0915.bat` | 1 | 1.1 KiB | 2026-06-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0339 | P | `bat/create_task_v7_causality_audit_1605.bat` | 1 | 979 B | 2026-06-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0340 | P | `bat/monitor_recent_fno_changes.ps1` | 1 | 4.1 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0341 | P | `bat/restart_live_trade_executor_v16_5min.ps1` | 1 | 2.5 KiB | 2026-04-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0342 | P | `bat/run_aggressive_pf_tuner.bat` | 1 | 1.6 KiB | 2026-06-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0343 | P | `bat/run_ai_platform_api.bat` | 1 | 753 B | 2026-09-24 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0344 | P | `bat/run_alert_zero_entry_watchdog.bat` | 1 | 1.3 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0345 | P | `bat/run_authentication_backup.bat` | 1 | 3.3 KiB | 2026-02-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0346 | P | `bat/run_codex_post_trade_advisor_v15_new.bat` | 1 | 778 B | 2026-03-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0347 | P | `bat/run_conf_live_executor.bat` | 1 | 863 B | 2026-07-28 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0348 | P | `bat/run_eqidv2_entry_engine_1min_v5_id.bat` | 1 | 5.4 KiB | 2026-07-28 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0349 | P | `bat/run_eqidv2_entry_engine_v7_replay_today.bat` | 1 | 2.6 KiB | 2026-07-28 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0350 | P | `bat/run_eqidv2_eod_reconciler_v16_5min.bat` | 1 | 2.7 KiB | 2026-04-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0351 | P | `bat/run_eqidv2_lf_prewarm_v16_5min.bat` | 1 | 1.2 KiB | 2026-04-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0352 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long.bat` | 1 | 7.6 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0353 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_01.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0354 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_02.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0355 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_03.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0356 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_04.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0357 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_05.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0358 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_06.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0359 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_07.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0360 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_08.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0361 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_09.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0362 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_10.bat` | 1 | 169 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0363 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_long_shard_common.bat` | 1 | 7.0 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0364 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short.bat` | 1 | 7.6 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0365 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_01.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0366 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_02.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0367 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_03.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0368 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_04.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0369 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_05.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0370 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_06.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0371 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_07.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0372 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_08.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0373 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_09.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0374 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_10.bat` | 1 | 195 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0375 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_short_shard_common.bat` | 1 | 7.8 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0376 | P | `bat/run_eqidv2_live_combined_analyser_csv_v5_long.bat` | 1 | 6.5 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0377 | P | `bat/run_eqidv2_live_combined_analyser_csv_v5_short.bat` | 1 | 6.3 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0378 | P | `bat/run_eqidv2_live_combined_analyser_csv_v5_unified.bat` | 1 | 7.8 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0379 | P | `bat/run_eqidv2_live_combined_analyser_csv_v7_sweep_long.bat` | 1 | 7.2 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0380 | P | `bat/run_eqidv2_live_combined_analyser_csv_v7_sweep_short.bat` | 1 | 6.9 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0381 | P | `bat/run_eqidv2_parallel_runner_v5.bat` | 1 | 7.4 KiB | 2026-03-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0382 | P | `bat/run_eqidv2_pf_waiting_ready_alert_v16_5min.bat` | 1 | 3.2 KiB | 2026-04-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0383 | P | `bat/run_eqidv2_signal_discovery_v7_replay_today.bat` | 1 | 5.5 KiB | 2026-07-28 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0384 | P | `bat/run_eqidv2_signal_engine_v16_5min.bat` | 1 | 3.3 KiB | 2026-04-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0385 | P | `bat/run_eqidv2_v16_5min_stack_stop.bat` | 1 | 432 B | 2026-03-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0386 | P | `bat/run_fno_oi_backfill_5min.bat` | 1 | 895 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0387 | P | `bat/run_fno_oi_backfill_daily.bat` | 1 | 816 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0388 | P | `bat/run_fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.bat` | 1 | 1.2 KiB | 2026-08-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0389 | P | `bat/run_fno_oi_ema_confirm_0925_0930_pf_v3.bat` | 1 | 1.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0390 | P | `bat/run_fno_oi_ema_confirm_0925_0930_short2_expansion_v3.bat` | 1 | 1.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0391 | P | `bat/run_fno_oi_ema_confirm_0925_0930_v3_best_combo.bat` | 1 | 926 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0392 | P | `bat/run_fno_oi_ema_confirm_0925_best_combo.bat` | 1 | 892 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0393 | P | `bat/run_fno_oi_ema_confirm_0925_long2_expansion_v2.bat` | 1 | 1.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0394 | P | `bat/run_fno_oi_ema_confirm_0925_pf.bat` | 1 | 983 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0395 | P | `bat/run_fno_oi_ema_confirm_0925_pf_v2.bat` | 1 | 1,012 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0396 | P | `bat/run_fno_oi_ema_confirm_0925_short_expansion_v2.bat` | 1 | 431 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0397 | P | `bat/run_fno_oi_ema_confirm_0925_v2_best_combo.bat` | 1 | 903 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0398 | P | `bat/run_fno_oi_ema_confirm_optimize.bat` | 1 | 1.0 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0399 | P | `bat/run_fno_v5_0926_all_history_day_pf_optimize.bat` | 1 | 109 B | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0400 | P | `bat/run_fno_v5_0926_day_pf_optimize.bat` | 1 | 97 B | 2026-08-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0401 | P | `bat/run_fno_v6_confirmation_1min.bat` | 1 | 167 B | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0402 | P | `bat/run_fno_v6_equity_1min_feed.bat` | 1 | 166 B | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0403 | P | `bat/run_fno_v6_live_long.bat` | 1 | 159 B | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0404 | P | `bat/run_fno_v6_live_short.bat` | 1 | 160 B | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0405 | P | `bat/run_fno_v6_net_result.bat` | 1 | 160 B | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0406 | P | `bat/run_fno_v6_trade_logger.bat` | 1 | 162 B | 2026-09-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0407 | P | `bat/run_public_ip_monitor.bat` | 1 | 912 B | 2026-03-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0408 | P | `bat/run_v14_backtest.bat` | 1 | 809 B | 2026-10-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0409 | P | `bat/schedule_authentication_v2_weekday.bat` | 1 | 1.3 KiB | 2026-09-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0410 | P | `bat/schedule_collect_filtered_stock_data_weekday.bat` | 1 | 884 B | 2026-08-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0411 | P | `bat/schedule_eqidv2_daily_live_v7_research_session_weekday.bat` | 1 | 1.1 KiB | 2026-06-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0412 | P | `bat/schedule_eqidv2_eod_scheduler_for_5mins_data_live_minimal_weekday.bat` | 1 | 1.1 KiB | 2026-03-31 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0413 | P | `bat/schedule_eqidv2_id_5min_v7_live_weekday.bat` | 1 | 2.7 KiB | 2026-06-29 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0414 | P | `bat/schedule_eqidv2_parallel_runner_v5_weekday.bat` | 1 | 219 B | 2026-03-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0415 | P | `bat/schedule_eqidv2_suggestions_v7_live_research_weekday.bat` | 1 | 1,023 B | 2026-05-28 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0416 | P | `bat/schedule_eqidv2_v15_live_weekday.bat` | 1 | 3.6 KiB | 2026-03-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0417 | P | `bat/schedule_eqidv2_v15_new_live_weekday.bat` | 1 | 1.1 KiB | 2026-03-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0418 | P | `bat/schedule_eqidv2_v15_weekday.bat` | 1 | 3.6 KiB | 2026-03-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0419 | P | `bat/schedule_eqidv2_v16_5min_live_weekday.bat` | 1 | 4.4 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0420 | P | `bat/schedule_eqidv2_v16_5min_two_stage_weekday.bat` | 1 | 5.5 KiB | 2026-04-24 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0421 | P | `bat/schedule_eqidv2_v4_weekday.bat` | 1 | 655 B | 2026-03-09 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0422 | P | `bat/schedule_eqidv2_v5_live_weekday.bat` | 1 | 210 B | 2026-03-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0423 | P | `bat/schedule_eqidv2_v5_weekday.bat` | 1 | 210 B | 2026-03-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0424 | P | `bat/schedule_eqidv2_v7_pre_momentum_filter_analyst_weekday.bat` | 1 | 1.0 KiB | 2026-06-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0425 | P | `bat/schedule_eqidv2_v7_research_layer_weekday.bat` | 1 | 1,010 B | 2026-05-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0426 | P | `bat/schedule_eqidv2_v7_sweep_live_weekday.bat` | 1 | 144 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0427 | P | `bat/schedule_eqidv2_v7_sweep_weekday.bat` | 1 | 134 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0428 | P | `bat/schedule_fno_oi_fast_production_trial_20260902.ps1` | 1 | 4.6 KiB | 2026-09-01 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0429 | P | `bat/schedule_fno_oi_weekday.bat` | 1 | 317 B | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0430 | P | `bat/schedule_fundamental_price_action_v1_papertrade_weekday.bat` | 1 | 1.1 KiB | 2026-08-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0431 | P | `bat/schedule_fundamental_price_action_v1_weekday.bat` | 1 | 928 B | 2026-08-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0432 | P | `bat/schedule_moving_files_weekday.bat` | 1 | 995 B | 2026-09-01 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0433 | P | `bat/schedule_nifty_guard_fetcher_v15_weekday.bat` | 1 | 1.1 KiB | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0434 | P | `bat/schedule_nifty_guard_fetcher_v16_5min_weekday.bat` | 1 | 1.1 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0435 | P | `bat/schedule_pf_supervisor_healthcheck_weekday.bat` | 1 | 1.1 KiB | 2026-04-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0436 | P | `bat/schedule_preopen_session_autofix_weekday.bat` | 1 | 1.1 KiB | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0437 | P | `bat/schedule_preopen_session_healthcheck_weekday.bat` | 1 | 1.1 KiB | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0438 | P | `bat/schedule_v7_full_pipeline_entry_research_v2_weekday.bat` | 1 | 957 B | 2026-07-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0439 | P | `bat/schedule_v7_full_pipeline_entry_research_weekday.bat` | 1 | 999 B | 2026-07-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0440 | P | `bat/schedule_v7_gate_promotion_weekday.bat` | 1 | 1.1 KiB | 2026-06-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0441 | P | `bat/schedule_v7_nse_id_cost_weekday.bat` | 1 | 933 B | 2026-06-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0442 | P | `bat/schedule_v7_qualification_weekday.bat` | 1 | 1.1 KiB | 2026-06-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0443 | P | `bat/schedule_v7_shadow_candidate_monitor_weekday.bat` | 1 | 967 B | 2026-07-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0444 | P | `bat/schedule_v7_walkforward_gate_weekday.bat` | 1 | 1,009 B | 2026-06-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0445 | P | `bat/stop_eqidv2_v16_5min_stack.ps1` | 1 | 5.0 KiB | 2026-03-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0446 | P | `bat/stop_eqidv2_v16_5min_two_stage_stack.ps1` | 1 | 4.8 KiB | 2026-04-24 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0447 | P | `bench_eqidv2_live_v15_new_slots.py` | 1 | 5.8 KiB | 2026-03-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0448 | P | `bench_v15_advisor_short_backend.py` | 1 | 11.1 KiB | 2026-03-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0449 | P | `bench_v15_new_shadow_bundle_parity.py` | 1 | 8.1 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0450 | P | `bench_v15_short_early_entry_backend.py` | 1 | 15.0 KiB | 2026-03-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0451 | P | `bench_v15_sl_tgt_075_backend.py` | 1 | 11.8 KiB | 2026-03-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0452 | P | `build_fno_rollover_diagnostic_universe.py` | 1 | 5.4 KiB | 2026-08-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0453 | P | `build_prefilter_long_compact_results.py` | 1 | 2.2 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0454 | P | `C_setups_deep_diagnosis_v11.md` | 1 | 11.7 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0455 | P | `codex_plan.md` | 1 | 6.1 KiB | 2026-04-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0456 | P | `codex_post_trade_advisor_v15_new.py` | 1 | 46.1 KiB | 2026-03-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0457 | P | `compare_twostage_v16_5min.py` | 1 | 18.9 KiB | 2026-04-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0458 | P | `conf_adherence_check.py` | 1 | 5.3 KiB | 2026-06-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0459 | P | `conf_live_preflight.py` | 1 | 5.6 KiB | 2026-06-29 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0460 | P | `CONF_PAPER_DAY_RUNBOOK.md` | 1 | 4.3 KiB | 2026-06-15 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0461 | P | `count_configs.bat` | 1 | 1.5 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0462 | P | `CRITICAL_ISSUES_QUICK_REFERENCE.md` | 1 | 13.3 KiB | 2026-04-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0463 | P | `CTradingDataeqidv2v11_conf_16_smoke.log` | 1 | 32.8 KiB | 2026-06-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0464 | P | `D_setups_deep_diagnosis_v11.md` | 1 | 7.5 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0465 | P | `DAILY_JOURNAL_TEMPLATE.md` | 1 | 3.0 KiB | 2026-05-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0466 | P | `dedupe_5min_store.py` | 1 | 4.3 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0467 | P | `deep_analysis_strategy_comparison_2026-03-07.txt` | 1 | 5.7 KiB | 2026-03-07 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0468 | P | `E_setups_deep_diagnosis_v11.md` | 1 | 7.3 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0469 | P | `eqidv2_daily_combined_analyser_csv.py` | 1 | 9.0 KiB | 2026-02-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0470 | P | `eqidv2_daily_combined_analyser_csv_tester.py` | 1 | 2.8 KiB | 2026-02-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0471 | P | `eqidv2_daily_combined_analyser_csv_tester_n_compare.py` | 1 | 3.9 KiB | 2026-02-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0472 | P | `eqidv2_eod_reconciler_v16_5min.py` | 1 | 12.9 KiB | 2026-04-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0473 | P | `eqidv2_lf_prewarm_v16_5min.py` | 1 | 5.1 KiB | 2026-04-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0474 | P | `eqidv2_live_combined_analyser.py` | 1 | 474 B | 2026-02-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0475 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_01.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0476 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_02.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0477 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_03.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0478 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_04.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0479 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_05.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0480 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_06.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0481 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_07.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0482 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_08.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0483 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_09.py` | 1 | 409 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0484 | P | `eqidv2_live_combined_analyser_csv_v15_long_shard_10.py` | 1 | 410 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0485 | P | `eqidv2_live_combined_analyser_csv_v15_long_sharded.py` | 1 | 4.6 KiB | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0486 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_01.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0487 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_02.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0488 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_03.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0489 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_04.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0490 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_05.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0491 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_06.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0492 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_07.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0493 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_08.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0494 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_09.py` | 1 | 437 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0495 | P | `eqidv2_live_combined_analyser_csv_v15_short_shard_10.py` | 1 | 438 B | 2026-03-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0496 | P | `eqidv2_live_combined_analyser_csv_v3.py` | 1 | 14.7 KiB | 2026-02-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0497 | P | `eqidv2_live_combined_analyser_parquet.py` | 1 | 9.2 KiB | 2026-02-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0498 | P | `eqidv2_live_combined_analyser_rowbyrow.py` | 1 | 8.1 KiB | 2026-02-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0499 | P | `eqidv2_live_fetch_n_latestsignalprint.py` | 1 | 8.4 KiB | 2026-02-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0500 | P | `eqidv2_ml_backtest.py` | 1 | 4.7 KiB | 2026-02-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0501 | P | `eqidv2_parallel_runner_v5.py` | 1 | 16.3 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0502 | P | `eqidv2_pf_waiting_ready_alert_v16_5min.py` | 1 | 8.6 KiB | 2026-04-23 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0503 | P | `EQIDV2_V16_5MIN_PIPELINE_REFERENCE.txt` | 1 | 129.7 KiB | 2026-04-21 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0504 | P | `eqidv5_combined_runner.py` | 1 | 50.6 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0505 | P | `eqidv5_deep_optimizer.py` | 1 | 21.2 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0506 | P | `eqidv5_eval_one.py` | 1 | 5.1 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0507 | P | `eqidv5_long_strategy.py` | 1 | 27.1 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0508 | P | `eqidv5_param_sweep.py` | 1 | 8.4 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0509 | P | `eqidv5_pdf_sweep.py` | 1 | 6.4 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0510 | P | `eqidv5_short_strategy.py` | 1 | 27.0 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0511 | P | `eqidv5_strategy_common.py` | 1 | 27.2 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0512 | P | `filtered_stocks_fno_MIS_v3.py` | 1 | 41.6 KiB | 2026-09-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0513 | P | `filtered_stocks_MIS_v1_backup_20260519.py` | 1 | 11.3 KiB | 2026-04-01 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0514 | P | `filtered_stocks_NSE_cash_full.py` | 1 | 167.2 KiB | 2026-04-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0515 | P | `filtered_stocks_NSE_cash_intraday_5x.py` | 1 | 25.6 KiB | 2026-04-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0516 | P | `filtered_stocks_NSE_options_only.py` | 1 | 3.4 KiB | 2026-02-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0517 | P | `filtered_stocks_NSE_stock_intraday_5x_price_ge_25_liquid.py` | 1 | 14.7 KiB | 2026-04-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0518 | P | `filtered_stocks_two_session_afternoon.py` | 1 | 5.8 KiB | 2026-04-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0519 | P | `filtered_stocks_two_session_history.py` | 1 | 22.75 MiB | 2026-04-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0520 | P | `filtered_stocks_two_session_history_v1.py` | 1 | 82.13 MiB | 2026-04-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0521 | P | `filtered_stocks_two_session_history_v2.py` | 1 | 82.15 MiB | 2026-04-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0522 | P | `filtered_stocks_two_session_history_v3_3times.py` | 1 | 137.41 MiB | 2026-04-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0523 | P | `filtered_stocks_two_session_latest_v3_3times.py` | 1 | 12.7 KiB | 2026-04-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0524 | P | `filtered_stocks_two_session_mid_v3_3times.py` | 1 | 12.7 KiB | 2026-04-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0525 | P | `final_setup_conf.py.bak_2026-07-08_pre_last2wk_rnd` | 1 | 130.7 KiB | 2026-07-08 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0526 | P | `final_setup_conf.py.bak_20260622_demote` | 1 | 100.5 KiB | 2026-06-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0527 | P | `final_setup_conf.py.bak_pre_amod` | 1 | 91.5 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0528 | P | `final_setup_conf.py.bak_pre_glower` | 1 | 95.4 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0529 | P | `final_setup_conf.py.bak_pre_shorts` | 1 | 84.9 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0530 | P | `final_setup_conf.py.bak_pre_tier123` | 1 | 72.6 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0531 | P | `final_setup_conf.py.bak_pre_vreclaim` | 1 | 79.9 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0532 | P | `final_setup_conf.py.bak_promote_20260629_215137` | 1 | 106.8 KiB | 2026-06-29 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0533 | P | `final_setup_conf.py.bak_vwap` | 1 | 75.6 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0534 | P | `final_setup_conf.py.bak_wirefix` | 1 | 100.0 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0535 | P | `finalize_v12_one_month_long_research.py` | 1 | 23.2 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0536 | P | `FNO_DATA_TO_ENTRY_FLOW.html` | 1 | 49.6 KiB | 2026-09-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0537 | P | `FNO_DATA_TO_ENTRY_FLOW.md` | 1 | 23.9 KiB | 2026-09-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0538 | P | `FNO_STRATEGY_LIBRARY.html` | 1 | 38.4 KiB | 2026-08-31 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0539 | P | `FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.html` | 1 | 85.6 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0540 | P | `FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md` | 1 | 42.4 KiB | 2026-08-30 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0541 | P | `FNO_V10_STAGE7_BACKTEST.md` | 1 | 2.8 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0542 | P | `FNO_V10_UNIFIED_5M_1M_BACKTEST.md` | 1 | 1.9 KiB | 2026-08-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0543 | P | `FNO_V11_STAGE10_BACKTEST_STRATEGY.html` | 1 | 93.2 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0544 | P | `FNO_V11_STAGE10_BACKTEST_STRATEGY.md` | 1 | 39.9 KiB | 2026-08-30 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0545 | P | `FNO_V12_LATE_SHORT_VOLUME_BACKTEST_STRATEGY.html` | 1 | 104.4 KiB | 2026-08-31 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0546 | P | `FNO_V12_LATE_SHORT_VOLUME_BACKTEST_STRATEGY.md` | 1 | 45.7 KiB | 2026-08-31 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0547 | P | `FNO_V6_LIVE_STRATEGY.html` | 1 | 77.9 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0548 | P | `FNO_V6_LIVE_STRATEGY.md` | 1 | 48.6 KiB | 2026-09-14 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0549 | P | `FNO_V6_STRATEGY_AND_FLOWS.md` | 1 | 33.3 KiB | 2026-09-14 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0550 | P | `FnO_V6_Strategy_and_Flows.pdf` | 1 | 47.7 KiB | 2026-08-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0551 | P | `FNO_V6_V7_BACKTEST_STRATEGY_AND_RESULTS.md` | 1 | 24.5 KiB | 2026-08-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0552 | P | `FNO_V6_V7_V8_20260819_RESULTS.md` | 1 | 7.0 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0553 | P | `FNO_V6_V7_V8_LAST_40_SESSIONS_TO_20260819.md` | 1 | 6.6 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0554 | P | `FNO_V6_V8_COMBINED_V9_FULL_COMPARISON.md` | 1 | 36.5 KiB | 2026-08-21 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0555 | P | `FNO_V6_V8_V10_REPAIRED_COMPARISON_20260828.md` | 1 | 11.6 KiB | 2026-08-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0556 | P | `FNO_V8_1M_OPTIMIZATION_RESULTS.md` | 1 | 12.2 KiB | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0557 | P | `FNO_V8_B0_DATEWISE_TRADE_RESULTS.md` | 1 | 34.6 KiB | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0558 | P | `FNO_V8_B0_FULL_HISTORY_DETAILED_RESULTS.md` | 1 | 6.1 KiB | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0559 | P | `FNO_V8_BACKTEST_STRATEGY.html` | 1 | 80.1 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0560 | P | `FNO_V8_BACKTEST_STRATEGY.md` | 1 | 48.4 KiB | 2026-08-30 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0561 | P | `FNO_V8_COMBINED_BEST_PER_LEG_RESULTS.md` | 1 | 4.8 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0562 | P | `FNO_V8_COMBINED_PAPER_LIVE_RUNBOOK.md` | 1 | 16.5 KiB | 2026-08-21 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0563 | P | `FNO_V8_PER_LEG_STRICT_VS_RETUNED_COMPARISON.md` | 1 | 5.1 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0564 | P | `FNO_V8_STRICT_40D_RESULTS_AND_COMPARISON.md` | 1 | 5.7 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0565 | P | `fno_v8_sweep_all_setups_runs.txt` | 1 | 1.3 KiB | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0566 | P | `FNO_V8_WINDOWED_1M_BACKTEST_README.md` | 1 | 6.1 KiB | 2026-08-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0567 | P | `FNO_V9_0950_0955_HONEST_OPTIMIZATION_RESULTS.md` | 1 | 21.5 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0568 | P | `FNO_V9_EXTENDED_SLOT_HONEST_OPTIMIZATION_FEASIBILITY.md` | 1 | 3.6 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0569 | P | `FUTURE_PAPER_SETUP_RESEARCH_2026_07_29.md` | 1 | 3.7 KiB | 2026-07-29 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0570 | P | `G_setups_deep_diagnosis_v11.md` | 1 | 8.8 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0571 | P | `generate_fno_v6_reference_pdf.py` | 1 | 76.6 KiB | 2026-08-17 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0572 | P | `generate_nse_cash_universe_filters.py` | 1 | 20.7 KiB | 2026-04-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0573 | P | `GO_LIVE_CHECKLIST_conf.md` | 1 | 2.8 KiB | 2026-06-15 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0574 | P | `HILEGA_MILEGA_ITERATIVE_RESEARCH_REPORT.md` | 1 | 3.6 KiB | 2026-08-10 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0575 | P | `HILEGA_MILEGA_SETUP_SPEC.md` | 1 | 3.3 KiB | 2026-08-10 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0576 | P | `HOURLY_ATR_IMPULSE_LONG_REVIEW.md` | 1 | 4.3 KiB | 2026-08-05 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0577 | P | `ID_v8_live_1min_entry_plan.md` | 1 | 4.9 KiB | 2026-05-21 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0578 | P | `IMPLEMENTATION_SUMMARY_V16_FIXES.md` | 1 | 8.6 KiB | 2026-04-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0579 | P | `L_setups_deep_diagnosis_v11.md` | 1 | 7.9 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0580 | P | `live_v15_slot_delta_bundle_shadow.py` | 1 | 5.9 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0581 | P | `lrs_revalidate.py` | 1 | 2.3 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0582 | P | `MARKET_CONTEXT_ENGINE.md` | 1 | 22.6 KiB | 2026-08-06 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0583 | P | `market_context_engine.py` | 1 | 59.6 KiB | 2026-08-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0584 | P | `Merged_NSE_BSE_Instruments.csv` | 1 | 1.39 MiB | 2026-02-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0585 | P | `meta_dataset.csv` | 1 | 119.4 KiB | 2026-02-16 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0586 | P | `new2_setups_diagnosis_v11.md` | 1 | 3.2 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0587 | P | `new3_setups_diagnosis_v11.md` | 1 | 3.5 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0588 | P | `new3_validate.py` | 1 | 5.1 KiB | 2026-06-12 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0589 | P | `new_setups_deep_diagnosis_v11.md` | 1 | 13.2 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0590 | P | `new_setups_research_v11.md` | 1 | 8.5 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0591 | P | `new_setups_validate_passers.py` | 1 | 8.3 KiB | 2026-06-12 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0592 | P | `new_setups_validate_salvage.py` | 1 | 3.5 KiB | 2026-06-12 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0593 | P | `observability_devops_interview_answers.md` | 1 | 148.1 KiB | 2026-05-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0594 | P | `opt_out.txt` | 1 | 187 B | 2026-03-03 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0595 | P | `outputs_v15_5min_run.txt` | 1 | 21.2 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0596 | P | `outputs_v15_5min_run2.txt` | 1 | 21.2 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0597 | P | `outputs_v15_5min_run3.txt` | 1 | 21.2 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0598 | P | `outputs_v15_5min_run4.txt` | 1 | 21.2 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0599 | P | `outputs_v15_5min_run5.txt` | 1 | 21.2 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0600 | P | `outputs_v15_run.txt` | 1 | 20.6 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0601 | P | `outputs_v16_5min_run1.txt` | 1 | 21.1 KiB | 2026-03-26 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0602 | P | `outputs_v16_5min_run2.txt` | 1 | 21.2 KiB | 2026-03-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0603 | P | `outputs_v16_5min_run3.txt` | 1 | 21.3 KiB | 2026-03-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0604 | P | `outputs_v16_5min_run4.txt` | 1 | 21.3 KiB | 2026-03-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0605 | P | `outputs_v16_5min_run5.txt` | 1 | 21.3 KiB | 2026-03-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0606 | P | `outputs_v16_5min_run6.txt` | 1 | 21.3 KiB | 2026-03-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0607 | P | `parity_check_id_5min_v7.py` | 1 | 8.0 KiB | 2026-05-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0608 | P | `parity_report.md` | 1 | 28.5 KiB | 2026-07-07 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0609 | P | `parity_report_2026-04-27.csv` | 1 | 2.7 KiB | 2026-05-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0610 | P | `parity_report_2026-04-28.csv` | 1 | 116.2 KiB | 2026-05-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0611 | P | `plan_1104.txt` | 1 | 4.0 KiB | 2026-04-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0612 | P | `PROMPT_fno_options_atm_fetch_5min.md` | 1 | 17.1 KiB | 2026-09-04 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0613 | P | `public_ip_monitor.py` | 1 | 6.0 KiB | 2026-03-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0614 | P | `quick_reresolve.py` | 1 | 17.8 KiB | 2026-04-18 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0615 | P | `README_eqidv2.md` | 1 | 10.1 KiB | 2026-02-24 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0616 | P | `regenerate_conf_tier_c_sources.py` | 1 | 5.1 KiB | 2026-06-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0617 | P | `repair_prefilter_long_hit_timestamps.py` | 1 | 2.0 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0618 | P | `replay_today_full_pipeline_v16_5min.py` | 1 | 27.8 KiB | 2026-04-24 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0619 | P | `replay_today_v16_5min.py` | 1 | 21.7 KiB | 2026-04-21 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0620 | P | `replay_v16_5min_live_day.py` | 1 | 10.2 KiB | 2026-04-28 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0621 | P | `report_v10.md` | 1 | 44.1 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0622 | P | `report_v11.md` | 1 | 104.7 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0623 | P | `report_v12.md` | 1 | 112.1 KiB | 2026-08-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0624 | P | `research/core.py` | 1 | 7.7 KiB | 2026-04-20 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0625 | P | `research/diag_round4.py` | 1 | 11.9 KiB | 2026-04-21 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0626 | P | `research/exits.py` | 1 | 14.4 KiB | 2026-04-20 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0627 | P | `research_honest_pf_validator.py` | 1 | 22.2 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0628 | P | `research_hourly_atr_impulse_long_conf.json` | 1 | 5.4 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0629 | P | `research_prefilter_long_5m_gt5pct.py` | 1 | 76.9 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0630 | P | `research_prefilter_long_ml_entry_strategy.py` | 1 | 71.0 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0631 | P | `research_prefilter_multi_day_replay.py` | 1 | 22.7 KiB | 2026-08-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0632 | P | `research_v10_per_setup_honest.pid` | 1 | 7 B | 2026-06-01 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0633 | P | `research_v10_per_setup_honest_stderr.log` | 1 | 0 B | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0634 | P | `research_v10_per_setup_honest_stdout.log` | 1 | 325 B | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0635 | P | `research_v10_setup_deep_dive.pid` | 1 | 7 B | 2026-06-01 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0636 | P | `research_v10_setup_deep_dive_stderr.log` | 1 | 0 B | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0637 | P | `research_v10_setup_deep_dive_stdout.log` | 1 | 3.5 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0638 | P | `research_v10_target_setups_pf13.pid` | 1 | 7 B | 2026-06-01 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0639 | P | `research_v10_target_setups_pf13_stderr.log` | 1 | 0 B | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0640 | P | `research_v10_target_setups_pf13_stdout.log` | 1 | 204 B | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0641 | P | `research_v11_deep_addon_search.err.log` | 1 | 0 B | 2026-06-02 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0642 | P | `research_v11_deep_addon_search.log` | 1 | 10.8 KiB | 2026-06-02 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0643 | P | `research_v12_build_canonical_5m_from_1m.py` | 1 | 15.3 KiB | 2026-08-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0644 | P | `research_v12_build_strict_entry_prefilter.py` | 1 | 17.3 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0645 | P | `research_v12_hourly_atr_impulse_long_backtest.py` | 1 | 32.1 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0646 | P | `research_v12_hourly_prefilter_backtest.py` | 1 | 23.1 KiB | 2026-08-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0647 | P | `research_v12_hourly_pullback_long_backtest.py` | 1 | 27.5 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0648 | P | `research_v12_hourly_two_bar_long_backtest.py` | 1 | 76.8 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0649 | P | `research_v12_late_compression_simple_conf.json` | 1 | 4.1 KiB | 2026-08-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0650 | P | `research_v12_late_compression_simple_long_backtest.py` | 1 | 49.5 KiB | 2026-08-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0651 | P | `research_v12_merge_chunk_backtests.py` | 1 | 8.7 KiB | 2026-08-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0652 | P | `research_v12_missing_setups_prefilter_replay.py` | 1 | 38.9 KiB | 2026-08-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0653 | P | `research_v12_path_aware_long_rebuild_v3.py` | 1 | 32.6 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0654 | P | `research_v12_rank200_240_six_month_walkforward_t1_v7.py` | 1 | 1.3 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0655 | P | `research_v12_rank200_240_six_month_walkforward_v6.py` | 1 | 29.7 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0656 | P | `research_v12_reprice_locked_atr_entries.py` | 1 | 21.9 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0657 | P | `research_v12_single_day_exit_sweep.py` | 1 | 5.0 KiB | 2026-08-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0658 | P | `research_v12_six_month_honest_pullback_optimizer_v12.py` | 1 | 71.1 KiB | 2026-08-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0659 | P | `research_v12_strict_canonical_prefilter_replay.py` | 1 | 4.1 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0660 | P | `research_v12_two_stage_long_rebuild_v5.py` | 1 | 87.3 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0661 | P | `RUN5_OPTIMIZATION_REPORT.md` | 1 | 25.4 KiB | 2026-04-28 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0662 | P | `run_base_and_v7.bat` | 1 | 960 B | 2026-03-09 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0663 | P | `run_compare_all5.bat` | 1 | 1.7 KiB | 2026-03-09 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0664 | P | `run_deep_opt.bat` | 1 | 2.9 KiB | 2026-03-04 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0665 | P | `run_hilega_milega_5m_research_backtest.py` | 1 | 27.8 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0666 | P | `run_optimized.bat` | 1 | 3.1 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0667 | P | `run_optimizer.bat` | 1 | 271 B | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0668 | P | `run_optimizer.py` | 1 | 430 B | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0669 | P | `run_quick_sweep.py` | 1 | 5.1 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0670 | P | `run_quick_test.py` | 1 | 2.1 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0671 | P | `run_sweep.bat` | 1 | 2.3 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0672 | P | `run_target_test.bat` | 1 | 1.2 KiB | 2026-03-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0673 | P | `run_v12.bat` | 1 | 315 B | 2026-03-09 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0674 | P | `run_v12_sweep.bat` | 1 | 6.9 KiB | 2026-03-09 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0675 | P | `run_v15_long_sweep.bat` | 1 | 1.0 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0676 | P | `run_wick_test.py` | 1 | 2.6 KiB | 2026-03-03 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0677 | P | `RUNBOOK.md` | 1 | 7.9 KiB | 2026-05-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0678 | P | `S_setups_deep_diagnosis_v11.md` | 1 | 4.1 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0679 | P | `SECTOR_INTELLIGENCE.md` | 1 | 34.8 KiB | 2026-08-06 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0680 | P | `sector_intelligence.py` | 1 | 80.1 KiB | 2026-08-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0681 | P | `short2_mine_gen.py` | 1 | 1.8 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0682 | P | `short_mine_gen.py` | 1 | 2.4 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0683 | P | `SIGNAL_WINDOW_VERIFICATION_REPORT.md` | 1 | 7.6 KiB | 2026-04-18 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0684 | P | `strategy_map_v16_5min.md` | 1 | 13.5 KiB | 2026-04-27 | Referenced by G/dashboard-related documentation; retain until that relationship is reviewed. |
| H-0685 | P | `strategy_map_v17q_5min.md` | 1 | 25.8 KiB | 2026-04-27 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0686 | P | `strategy_v2.txt` | 1 | 49.8 KiB | 2026-04-23 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0687 | P | `strategy_v2_fix.txt` | 1 | 20.2 KiB | 2026-04-23 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0688 | P | `summarize_latest_outputs_csv.py` | 1 | 9.7 KiB | 2026-02-20 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0689 | P | `sweep_hilega_milega_research.py` | 1 | 23.2 KiB | 2026-08-10 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0690 | P | `sweep_v15_5min_long_lags.py` | 1 | 11.2 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0691 | P | `sweep_v15_5min_long_lags_strict_nifty.py` | 1 | 1.1 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0692 | P | `sweep_v15_5min_long_quality.py` | 1 | 12.6 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0693 | P | `sweep_v15_long_lags.py` | 1 | 11.2 KiB | 2026-03-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0694 | P | `sweep_v15_long_lags_strict_nifty.py` | 1 | 1.1 KiB | 2026-03-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0695 | P | `sweep_v15_long_quality.py` | 1 | 12.6 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0696 | P | `T_setups_deep_diagnosis_v11.md` | 1 | 4.6 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0697 | P | `test_import.bat` | 1 | 498 B | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0698 | P | `test_risk_brake.py` | 1 | 3.7 KiB | 2026-06-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0699 | P | `tier123_new_iterate.py` | 1 | 2.4 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0700 | P | `tier123_new_setups_diagnosis_v11.md` | 1 | 4.2 KiB | 2026-06-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0701 | P | `tier123_new_validate.py` | 1 | 3.0 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0702 | P | `timeline_per_ticker_tree.py` | 1 | 10.2 KiB | 2026-04-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0703 | P | `timeline_v16_5min.py` | 1 | 22.2 KiB | 2026-04-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0704 | P | `tmp_minbars_dual_abtest.py` | 1 | 8.9 KiB | 2026-02-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0705 | P | `tmp_v15_latest_run.log` | 1 | 41.3 KiB | 2026-03-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0706 | P | `tmp_v6_sweep_eval.py` | 1 | 11.1 KiB | 2026-03-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0707 | P | `tools/audit_fno_historical_repair.py` | 1 | 11.3 KiB | 2026-08-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0708 | P | `tools/compare_v7_entry_timing.py` | 1 | 27.3 KiB | 2026-06-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0709 | P | `tools/observability_experiment_registry.py` | 1 | 32.3 KiB | 2026-09-24 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0710 | P | `tools/replay_early_emit_v16_5min.py` | 1 | 5.4 KiB | 2026-04-22 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0711 | P | `tools/replay_v7_early_scanner_5sessions.py` | 1 | 28.2 KiB | 2026-05-29 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0712 | P | `tools/report_v7_live_backtest_1min.py` | 1 | 18.0 KiB | 2026-05-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0713 | P | `tools/snapshot_fno_current_sep.py` | 1 | 2.6 KiB | 2026-08-27 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0714 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1hr.py` | 1 | 64.9 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0715 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_3hr.py` | 1 | 66.7 KiB | 2026-06-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0716 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_daily.py` | 1 | 62.5 KiB | 2026-05-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0717 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_weekly.py` | 1 | 63.8 KiB | 2026-05-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0718 | P | `two_session_shortlist_history_builder.py` | 1 | 25.6 KiB | 2026-04-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0719 | P | `two_session_shortlist_history_builder_v1.py` | 1 | 43.2 KiB | 2026-04-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0720 | P | `two_session_shortlist_history_builder_v2.py` | 1 | 43.2 KiB | 2026-04-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0721 | P | `two_session_shortlist_history_builder_v3_3times.py` | 1 | 35.8 KiB | 2026-04-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0722 | P | `two_session_shortlist_test.py` | 1 | 28.8 KiB | 2026-04-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0723 | P | `two_session_shortlist_test_v1.py` | 1 | 38.0 KiB | 2026-04-11 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0724 | P | `two_session_shortlist_test_v2.py` | 1 | 39.6 KiB | 2026-04-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0725 | P | `two_session_shortlist_test_v3_3times.py` | 1 | 46.4 KiB | 2026-04-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0726 | P | `two_stage_v16_5min_implementation_plan.txt` | 1 | 30.4 KiB | 2026-04-16 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0727 | P | `v10_all_available_v7_live_paper_replay.md` | 1 | 17.3 KiB | 2026-05-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0728 | P | `v10_backtesting_2026-05-21 to 2026-05-29.md` | 1 | 17.3 KiB | 2026-05-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0729 | P | `v10_deep_setup_by_setup_honest_research_2026-06-01.md` | 1 | 8.1 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0730 | P | `v10_historical_2026-05-01 to 2026-05-29.md` | 1 | 129.3 KiB | 2026-05-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0731 | P | `v10_honest_holdout_improvement_solution_2026-06-01.md` | 1 | 77.4 KiB | 2026-06-08 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0732 | P | `v10_honest_pf_improvement_research_2026-06-01.md` | 1 | 4.5 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0733 | P | `v10_latest_all_setups_selected_result_2026-06-01.md` | 1 | 3.3 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0734 | P | `v10_live_paper_replay_2026-05-01 to 2026-05-29.md` | 1 | 19.4 KiB | 2026-05-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0735 | P | `v10_live_vs_v10_historical_vs_v11_historical_2026-05-21_to_2026-05-29.md` | 1 | 10.1 KiB | 2026-05-31 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0736 | P | `v10_per_setup_honest_pf_research_2026-06-01.md` | 1 | 4.8 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0737 | P | `v10_target_pf13_six_setups_research_2026-06-01.md` | 1 | 5.1 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0738 | P | `v10_vs_v11_backtesting_2026-05-22_to_2026-05-29.md` | 1 | 10.7 KiB | 2026-05-31 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0739 | P | `v11_all_setups_with_ab_probe_detailed_result_2026-06-01.md` | 1 | 9.6 KiB | 2026-06-01 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0740 | P | `v11_backtesting_2026-05-22_to_2026-05-29.md` | 1 | 30.0 KiB | 2026-05-31 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0741 | P | `v11_historical_full_backtesting_2025-06-02_to_2026-05-29.md` | 1 | 12.0 KiB | 2026-05-31 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0742 | P | `V11_LAB_SETUP_IMPROVEMENT_REPORT_2026_07_10.md` | 1 | 5.6 KiB | 2026-07-10 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0743 | P | `v11_live_parity_notes.md` | 1 | 14.1 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0744 | P | `V11_NEW_SETUP_RESEARCH_2026-06-12.md` | 1 | 4.4 KiB | 2026-06-12 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0745 | P | `V11_RESEARCH_BOOK_SELECTION_2026_07_22.md` | 1 | 2.6 KiB | 2026-07-22 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0746 | P | `v15_5min_long_exit_search.py` | 1 | 9.3 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0747 | P | `v15_5min_sltarget_search.py` | 1 | 22.3 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0748 | P | `v15_long_exit_search.py` | 1 | 9.3 KiB | 2026-03-13 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0749 | P | `v15_long_param_sweep.py` | 1 | 23.4 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0750 | P | `v15_long_realistic_entry_search.py` | 1 | 16.0 KiB | 2026-03-25 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0751 | P | `v15_long_recovery_sweep.py` | 1 | 7.8 KiB | 2026-03-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0752 | P | `v15_long_sltgt_eod_sweep.py` | 1 | 15.3 KiB | 2026-03-26 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0753 | P | `v15_new_entry_timing_search.py` | 1 | 14.9 KiB | 2026-03-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0754 | P | `v15_sltarget_search.py` | 1 | 22.3 KiB | 2026-03-14 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0755 | P | `v16_5min_long_scenario_search.py` | 1 | 25.2 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0756 | P | `v16_5min_ml_parameter_advisor.py` | 1 | 53.4 KiB | 2026-04-07 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0757 | P | `v16_5min_sltarget_search.py` | 1 | 23.8 KiB | 2026-03-30 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0758 | P | `v16_5min_window_search.py` | 1 | 16.7 KiB | 2026-04-06 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0759 | P | `v16_long_exit_search.py` | 1 | 9.3 KiB | 2026-03-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0760 | P | `v16_sltarget_search.py` | 1 | 22.3 KiB | 2026-03-19 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0761 | P | `V17D_PRODUCTION_ROADMAP.md` | 1 | 47.3 KiB | 2026-05-13 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0762 | P | `v17r_nonf_setups_catalog.md` | 1 | 15.7 KiB | 2026-05-16 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0763 | P | `V17R_SETUP_LAB_REPORT.md` | 1 | 27.8 KiB | 2026-04-28 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0764 | P | `V5_signal_generation_strategy_guide.txt` | 1 | 5.2 KiB | 2026-03-05 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0765 | P | `v6_20260819_rebuild.stderr.log` | 1 | 835 B | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0766 | P | `v6_20260819_rebuild.stdout.log` | 1 | 247 B | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0767 | P | `V6_signal_generation_strategy_guide_with_sweeps.txt` | 1 | 5.9 KiB | 2026-03-05 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0768 | P | `v7_20260819_rebuild.stderr.log` | 1 | 0 B | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0769 | P | `v7_20260819_rebuild.stdout.log` | 1 | 572 B | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0770 | P | `v7_fix_20260608_111939.md` | 1 | 33.4 KiB | 2026-06-08 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0771 | P | `v7_live_2026-05-21 to 2026-05-29.md` | 1 | 20.0 KiB | 2026-05-30 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0772 | P | `v7_live_2026-05-22 to 2026-06-04.md` | 1 | 95.9 KiB | 2026-06-05 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0773 | P | `v7_live_ecosystem_audit_2026-06-09.md` | 1 | 26.4 KiB | 2026-06-09 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0774 | P | `v7_live_plan.md` | 1 | 31.5 KiB | 2026-06-07 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0775 | P | `v7_live_review_2026-06-09.md` | 1 | 34.0 KiB | 2026-06-09 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0776 | P | `v7_live_strategy_full_documentation_today.md` | 1 | 76.4 KiB | 2026-06-10 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0777 | P | `V7_PUNCHLIST.md` | 1 | 10.1 KiB | 2026-06-10 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0778 | P | `V7_PUNCHLIST_REV2.md` | 1 | 10.4 KiB | 2026-06-11 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0779 | P | `V7_signal_generation_strategy_guide_v5plus_sweeps_regime_risk.txt` | 1 | 7.1 KiB | 2026-03-05 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0780 | P | `V7_V11_PARITY_TOMORROW_TEST_2026-07-29.md` | 1 | 2.1 KiB | 2026-07-28 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0781 | P | `v8_b0_20260819_5bps.stderr.log` | 1 | 1.8 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0782 | P | `v8_b0_20260819_5bps.stdout.log` | 1 | 1.6 KiB | 2026-08-20 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0783 | P | `v8_optimizer_search.stderr.log` | 1 | 0 B | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0784 | P | `v8_optimizer_search.stdout.log` | 1 | 0 B | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0785 | P | `v8_optimizer_search_final.stderr.log` | 1 | 709.5 KiB | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0786 | P | `v8_optimizer_search_final.stdout.log` | 1 | 145 B | 2026-08-19 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0787 | P | `V8_signal_generation_strategy_guide_v5plus_sweeps_regime_risk.txt` | 1 | 25.0 KiB | 2026-03-05 | Documentation/report/log/media/text retained unless its relationship and retention are explicitly reviewed. |
| H-0788 | P | `validate_conf_wiring.py` | 1 | 3.8 KiB | 2026-06-29 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0789 | P | `validate_final_conf_live_wiring.py` | 1 | 4.4 KiB | 2026-06-29 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0790 | P | `validate_prefilter_long_gt5_outputs.py` | 1 | 10.4 KiB | 2026-08-05 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |
| H-0791 | P | `validate_premom_feature_parity.py` | 1 | 4.4 KiB | 2026-06-15 | Purpose or dynamic usage remains uncertain; not eligible for an archive operation. |

## Protected path register — do not archive

This register includes conservatively retained related material as well as direct runtime/import dependencies. It does not claim every protected file ran today.

| ID | Root | Exact relative path | Files | Logical size | Latest modified (UTC) | Reason / hold |
| --- | --- | --- | ---: | ---: | --- | --- |
| K-0001 | D | `backtesting_result_v11/` | 11,208 | 1.356 GiB | 2026-10-09 | v7_walkforward_gate_report.py:31 reachable static dependency directory; not proof of execution; backtesting_result_v11_daily.py:35 reachable static dependency directory; not proof of execution |
| K-0002 | D | `backtesting_result_v13_v10_g/` | 4,509 | 32.476 GiB | 2026-10-09 | log_dashboard_server.py:103 dashboard path; dashboard_flow.py:20 V13-V10-G consumer glob runs/*/*/daily_results.csv |
| K-0003 | D | `backtesting_result_v13_v10_g_3/` | 116 | 62.83 MiB | 2026-10-10 | dashboard_flow.py:20 V13-V10-G-3 consumer glob runs/*/*/daily_results.csv |
| K-0004 | D | `collect_filtered_stock_data/` | 15 | 7.6 KiB | 2026-08-31 | log_dashboard_server.py:99 dashboard path; log_dashboard_server.py:99 reachable static dependency directory; not proof of execution |
| K-0005 | D | `daily_live_v7_research_session/` | 133 | 1.15 MiB | 2026-08-31 | log_dashboard_server.py:75 dashboard path; log_dashboard_server.py:75 reachable static dependency directory; not proof of execution |
| K-0006 | D | `dashboard_server/` | 2 | 390.6 KiB | 2026-06-16 | Dashboard paths still reference these roots, including older V7/V11 research. |
| K-0007 | D | `dynamic_pre_momentum/` | 1 | 8.4 KiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0008 | D | `entry_engine_1min_v5_ID/` | 45,672 | 163.94 MiB | 2026-09-01 | log_dashboard_server.py:11678 reachable static dependency directory; not proof of execution; eqidv2_entry_engine_1min_v5_id.py:67 reachable static dependency directory; not proof of execution |
| K-0009 | D | `fno_oi/contract_features/` | 652 | 159.89 MiB | 2026-10-09 | fno_oi_common.py:35 reachable static dependency directory; not proof of execution |
| K-0010 | D | `fno_oi/daily_continuous/` | 0 | 0 B | - | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0011 | D | `fno_oi/eod_qc/` | 68 | 2.16 MiB | 2026-10-09 | fno_oi_common.py:40 reachable static dependency directory; not proof of execution |
| K-0012 | D | `fno_oi/equity_1m_slot_ready/` | 379 | 87.22 MiB | 2026-10-09 | fno_oi_common.py:34 reachable static dependency directory; not proof of execution |
| K-0013 | D | `fno_oi/feature_snapshots/` | 2,806 | 329.09 MiB | 2026-10-09 | fno_oi_common.py:36 reachable static dependency directory; not proof of execution |
| K-0014 | D | `fno_oi/historical_repair/` | 3,601 | 19.905 GiB | 2026-09-24 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0015 | D | `fno_oi/instrument_master/` | 70 | 15.31 MiB | 2026-10-09 | fno_oi_common.py:28 reachable static dependency directory; not proof of execution |
| K-0016 | D | `fno_oi/latest/` | 94 | 851.5 KiB | 2026-10-09 | log_dashboard_server.py:102 dashboard path; log_dashboard_server.py:102 reachable static dependency directory; not proof of execution |
| K-0017 | D | `fno_oi/multi_strategy_paper_v1/` | 205 | 15.30 MiB | 2026-09-15 | log_dashboard_server.py:154 dashboard path; log_dashboard_server.py:154 reachable static dependency directory; not proof of execution |
| K-0018 | D | `fno_oi/operations/` | 121 | 86.6 KiB | 2026-09-14 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0019 | D | `fno_oi/options_atm_map/` | 988 | 29.08 MiB | 2026-10-09 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0020 | D | `fno_oi/options_slot_ready/` | 985 | 10.26 MiB | 2026-10-09 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0021 | D | `fno_oi/rank_history/` | 116 | 320.40 MiB | 2026-08-31 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0022 | D | `fno_oi/ranking_ready/` | 2,599 | 1.53 MiB | 2026-10-09 | log_dashboard_server.py:4726 reachable static dependency directory; not proof of execution; fno_oi_common.py:38 reachable static dependency directory; not proof of execution |
| K-0023 | D | `fno_oi/rankings/` | 11,224 | 620.76 MiB | 2026-10-09 | fno_oi_common.py:37 reachable static dependency directory; not proof of execution |
| K-0024 | D | `fno_oi/raw_contracts_1m/` | 2 | 34.4 KiB | 2026-08-10 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0025 | D | `fno_oi/raw_contracts_1m_hist/` | 429 | 93.53 MiB | 2026-08-31 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0026 | D | `fno_oi/raw_contracts_5m/` | 665 | 93.54 MiB | 2026-10-09 | fno_oi_common.py:31 reachable static dependency directory; not proof of execution |
| K-0027 | D | `fno_oi/raw_equity_1m/` | 1,515 | 18.40 MiB | 2026-10-09 | fno_oi_common.py:33 reachable static dependency directory; not proof of execution |
| K-0028 | D | `fno_oi/raw_options_1m/` | 5,585 | 179.55 MiB | 2026-10-09 | G uses the v6_live control namespace; preserve all live/paper and options inputs. |
| K-0029 | D | `fno_oi/raw_options_5m/` | 5,582 | 135.79 MiB | 2026-10-09 | G uses the v6_live control namespace; preserve all live/paper and options inputs. |
| K-0030 | D | `fno_oi/shadow_fast_fetch/` | 967 | 7.45 MiB | 2026-09-04 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0031 | D | `fno_oi/slot_ready/` | 2,636 | 16.62 MiB | 2026-10-09 | log_dashboard_server.py:4723 reachable static dependency directory; not proof of execution; preopen_session_healthcheck.py:46 reachable static dependency directory; not proof of execution |
| K-0032 | D | `fno_oi/strategy_research/_signal_cache_equity_1m_aggregated_5m_futures_oi_v4/` | 3 | 22.96 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0033 | D | `fno_oi/strategy_research/_signal_cache_equity_1m_aggregated_5m_futures_oi_v7_high_low_breakout_v1/` | 3 | 53.70 MiB | 2026-08-20 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0034 | D | `fno_oi/strategy_research/_source_snapshots_v7_high_low_breakout_v1/` | 836 | 5.786 GiB | 2026-08-20 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0035 | D | `fno_oi/strategy_research/backtest_provenance/` | 9 | 1.67 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0036 | D | `fno_oi/strategy_research/daily_fno_comparison_v1/` | 3,158 | 21.384 GiB | 2026-09-11 | Still-referenced scheduled and one-shot dashboard publication bundles. |
| K-0037 | D | `fno_oi/strategy_research/dashboard_v6_v8_v10_v11_v12_20260831/` | 50 | 822.1 KiB | 2026-08-31 | Still-referenced scheduled and one-shot dashboard publication bundles. |
| K-0038 | D | `fno_oi/strategy_research/fno_historical_repair_20260827/` | 1,261 | 8.891 GiB | 2026-08-27 | G3 explicitly reads the historical DALBHARAT 1m repair snapshot. |
| K-0039 | D | `fno_oi/strategy_research/fno_today_20260831/` | 422 | 2.988 GiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0040 | D | `fno_oi/strategy_research/today_five_strategy_current_refresh_v1/` | 93 | 1.47 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0041 | D | `fno_oi/strategy_research/today_six_strategy_replays_v1/` | 102 | 2.34 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0042 | D | `fno_oi/strategy_research/today_v10_v11_refreshed_paths_1144_v1/` | 14 | 188.0 KiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0043 | D | `fno_oi/strategy_research/today_v12_selected_refreshed_paths_1144_v1/` | 17 | 72.04 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0044 | D | `fno_oi/strategy_research/today_v6_v10_stage7_20260827/` | 443 | 2.978 GiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0045 | D | `fno_oi/strategy_research/v10_max050_gap2_cap_sweep_v1/` | 40 | 24.24 MiB | 2026-08-30 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0046 | D | `fno_oi/strategy_research/v10_max050_gap2_full_history_v1/` | 127 | 51.58 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0047 | D | `fno_oi/strategy_research/v10_recent_detailed_results_v1/` | 21 | 1.61 MiB | 2026-08-30 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0048 | D | `fno_oi/strategy_research/v10_repaired_snapshot_reruns_20260827_v1/` | 358 | 76.70 MiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0049 | D | `fno_oi/strategy_research/v10_stage1_isolated_experiments_v1/` | 544 | 94.79 MiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0050 | D | `fno_oi/strategy_research/v10_stage7_followup_challengers_v1/` | 138 | 34.11 MiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0051 | D | `fno_oi/strategy_research/v10_stage7_gap_guard_research_v1/` | 135 | 22.83 MiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0052 | D | `fno_oi/strategy_research/v10_stage7_postselection_0935max050_gap_combo_v1/` | 163 | 27.15 MiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0053 | D | `fno_oi/strategy_research/v10_unified_5m_1m_v1/` | 86 | 14.72 MiB | 2026-09-03 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0054 | D | `fno_oi/strategy_research/v11_fno_staged_research_v2/` | 1,102 | 815.72 MiB | 2026-08-30 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0055 | D | `fno_oi/strategy_research/v11_stage10_fixed_full_history_v1/` | 145 | 50.82 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0056 | D | `fno_oi/strategy_research/v12_fno_staged_research_v1/` | 2,084 | 1.701 GiB | 2026-08-30 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0057 | D | `fno_oi/strategy_research/v12_s06_late_short_volume_min150_full_history_v1/` | 200 | 64.31 MiB | 2026-08-31 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0058 | D | `fno_oi/strategy_research/v13_advanced_oi_policies/` | 4 | 626.6 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0059 | D | `fno_oi/strategy_research/v13_confirmation_window/` | 5 | 172.5 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0060 | D | `fno_oi/strategy_research/v13_corrected/` | 9 | 141.8 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0061 | D | `fno_oi/strategy_research/v13_corrected_ungated_oi_research/` | 8 | 5.06 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0062 | D | `fno_oi/strategy_research/v13_corrected_v10/` | 63 | 14.33 MiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0063 | D | `fno_oi/strategy_research/v13_corrected_v10_a/` | 63 | 11.05 MiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0064 | D | `fno_oi/strategy_research/v13_corrected_v10_b/` | 25 | 1.68 MiB | 2026-09-13 | Frozen G chain inherits V9 source, F corrected comparison and B frozen settings; predecessor version labels are not evidence of obsolescence. |
| K-0065 | D | `fno_oi/strategy_research/v13_corrected_v10_c/` | 26 | 1.68 MiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0066 | D | `fno_oi/strategy_research/v13_corrected_v10_d/` | 23 | 903.7 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0067 | D | `fno_oi/strategy_research/v13_corrected_v10_e/` | 24 | 909.5 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0068 | D | `fno_oi/strategy_research/v13_corrected_v10_f/` | 78 | 3.13 MiB | 2026-09-14 | Frozen G chain inherits V9 source, F corrected comparison and B frozen settings; predecessor version labels are not evidence of obsolescence. |
| K-0069 | D | `fno_oi/strategy_research/v13_corrected_v10_g/` | 559 | 54.36 MiB | 2026-09-15 | fno_v13_v10_g_2_backtest.py:38 reachable static dependency directory; not proof of execution |
| K-0070 | D | `fno_oi/strategy_research/v13_corrected_v10_g_2/` | 811 | 3.321 GiB | 2026-10-10 | dashboard_flow.py:20 V13-V10-G-2 consumer glob run_*/daily_results.csv; fno_v13_v10_g_2_backtest.py:41 reachable static dependency directory; not proof of execution |
| K-0071 | D | `fno_oi/strategy_research/v13_corrected_v10_g_3/` | 184 | 161.88 MiB | 2026-10-08 | G dependency path in research/g3_pullback_report.py:17 |
| K-0072 | D | `fno_oi/strategy_research/v13_corrected_v2/` | 25 | 16.28 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0073 | D | `fno_oi/strategy_research/v13_corrected_v2_late_slot_research/` | 6 | 110.2 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0074 | D | `fno_oi/strategy_research/v13_corrected_v2_parameter_robustness/` | 5 | 191.4 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0075 | D | `fno_oi/strategy_research/v13_corrected_v3/` | 18 | 9.32 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0076 | D | `fno_oi/strategy_research/v13_corrected_v3_exit_sweep/` | 3 | 79.1 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0077 | D | `fno_oi/strategy_research/v13_corrected_v3_scaleout_sweep/` | 3 | 1.38 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0078 | D | `fno_oi/strategy_research/v13_corrected_v4/` | 10 | 145.2 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0079 | D | `fno_oi/strategy_research/v13_corrected_v5/` | 463 | 64.17 MiB | 2026-09-15 | Active G options loaders consume old V5 derivative data/daily options and V6 market_cache; preserve containing packages. |
| K-0080 | D | `fno_oi/strategy_research/v13_corrected_v6/` | 28 | 349.6 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0081 | D | `fno_oi/strategy_research/v13_corrected_v6_option_selector/` | 3 | 163.8 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0082 | D | `fno_oi/strategy_research/v13_corrected_v6_options/` | 18 | 532.1 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0083 | D | `fno_oi/strategy_research/v13_corrected_v7/` | 34 | 2.40 MiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0084 | D | `fno_oi/strategy_research/v13_corrected_v8_feature_shadow/` | 8 | 68.0 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0085 | D | `fno_oi/strategy_research/v13_corrected_v9/` | 141 | 323.52 MiB | 2026-09-13 | Frozen G chain inherits V9 source, F corrected comparison and B frozen settings; predecessor version labels are not evidence of obsolescence. |
| K-0086 | D | `fno_oi/strategy_research/v13_dashboard_vintage/` | 2 | 10.3 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0087 | D | `fno_oi/strategy_research/v13_g2_long_leaders/` | 24 | 3.33 MiB | 2026-10-05 | G dependency path in fno_v13_v10_g_2_long_leaders_backtest.py:26 |
| K-0088 | D | `fno_oi/strategy_research/v13_g2_relaxed_0925/` | 16 | 592.3 KiB | 2026-10-05 | G dependency path in fno_v13_v10_g_2_relaxed_0925_backtest.py:18 |
| K-0089 | D | `fno_oi/strategy_research/v13_g2_session_audit/` | 372 | 183.42 MiB | 2026-10-09 | G dependency path in research/g2_session_forensic_audit.py:18 |
| K-0090 | D | `fno_oi/strategy_research/v13_september_asof_20260911/` | 6 | 35.4 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0091 | D | `fno_oi/strategy_research/v13_september_detail_20260913/` | 6 | 18.6 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0092 | D | `fno_oi/strategy_research/v13_v10_g_full_history/` | 99 | 744.70 MiB | 2026-10-10 | dashboard_flow.py:20 V13-V10-G consumer glob run_*/g_backtest/portfolio_trades.csv; tools\v13_shadow_automation.py:24 reachable static dependency directory; not proof of execution |
| K-0093 | D | `fno_oi/strategy_research/v6_corrected/` | 17 | 19.42 MiB | 2026-09-03 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0094 | D | `fno_oi/strategy_research/v6_corrected_oi_threshold_research/` | 12 | 291.4 KiB | 2026-09-03 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0095 | D | `fno_oi/strategy_research/v6_derivative_backtests/` | 30 | 1.15 MiB | 2026-09-01 | Active G options loaders consume old V5 derivative data/daily options and V6 market_cache; preserve containing packages. |
| K-0096 | D | `fno_oi/strategy_research/v8_combined_best_per_leg_v1/` | 489 | 2.970 GiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0097 | D | `fno_oi/strategy_research/v8_strict_v6_logic_v1/` | 47 | 24.38 MiB | 2026-08-27 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0098 | D | `fno_oi/strategy_research/v8_v9_last_10_backtests/` | 476 | 2.920 GiB | 2026-08-26 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0099 | D | `fno_oi/strategy_research/v8_windowed_strict_v1/` | 1,389 | 6.056 GiB | 2026-09-04 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0100 | D | `fno_oi/strategy_research/v9_0950_0955_honest_v1/` | 13 | 5.74 MiB | 2026-08-20 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0101 | D | `fno_oi/strategy_research/v9_honest_v8_combined_v1/` | 64 | 15.01 MiB | 2026-08-26 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0102 | D | `fno_oi/universe/` | 42 | 1.41 MiB | 2026-10-09 | fno_oi_common.py:29 reachable static dependency directory; not proof of execution; trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py:133 reachable static dependency directory; not proof of execution |
| K-0103 | D | `fno_oi/universe_reports/` | 39 | 76.2 KiB | 2026-10-09 | Shared FnO inputs, live/paper controls, operational outputs or related historical evidence |
| K-0104 | D | `fno_oi/v13_v10_g_live/` | 1,246 | 297.63 MiB | 2026-10-09 | log_dashboard_server.py:4597 reachable static dependency directory; not proof of execution; tools\v13_shadow_automation.py:27 reachable static dependency directory; not proof of execution |
| K-0105 | D | `fno_oi/v13_v10_g_options_paper/` | 57 | 1.09 MiB | 2026-10-09 | All retained G/G2/G3 results, experiments, frozen inputs, runtime and research are explicitly protected; Dashboard Flow reads G/G2/G3 histories. |
| K-0106 | D | `fno_oi/v5_live/` | 98 | 475.3 KiB | 2026-10-05 | G uses the v6_live control namespace; preserve all live/paper and options inputs. |
| K-0107 | D | `fno_oi/v6_live/` | 873 | 6.49 MiB | 2026-10-09 | G uses the v6_live control namespace; preserve all live/paper and options inputs. |
| K-0108 | D | `fundamental_price_action_v1/` | 1,615 | 18.53 MiB | 2026-08-31 | log_dashboard_server.py:97 dashboard path; log_dashboard_server.py:97 reachable static dependency directory; not proof of execution |
| K-0109 | D | `live_id_5min_v7_persistent/` | 99 | 38.5 KiB | 2026-06-05 | eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.py:151 reachable static dependency directory; not proof of execution |
| K-0110 | D | `live_research_v7_research_layer/` | 1,779 | 2.344 GiB | 2026-08-31 | log_dashboard_server.py:73 dashboard path; log_dashboard_server.py:73 reachable static dependency directory; not proof of execution |
| K-0111 | D | `live_signals/` | 917 | 96.47 MiB | 2026-10-09 | log_dashboard_server.py:64 dashboard path; eqidv2_runtime_paths.py shared runtime default |
| K-0112 | D | `live_v15_shared_scan/` | 0 | 0 B | - | live_v15_shared_scan_cache.py:13 reachable static dependency directory; not proof of execution |
| K-0113 | D | `nifty_open_slot_5m/` | 107 | 32.1 KiB | 2026-10-09 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:56 reachable static dependency directory; not proof of execution |
| K-0114 | D | `nifty_slot_fail_5m/` | 38 | 4.1 KiB | 2026-10-09 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:47 reachable static dependency directory; not proof of execution |
| K-0115 | D | `nifty_slot_ready_5m/` | 7,229 | 2.83 MiB | 2026-10-09 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:38 reachable static dependency directory; not proof of execution |
| K-0116 | D | `observability/` | 2,454 | 467.59 MiB | 2026-10-09 | backtesting_result_v13_v10_g_daily.py:43 reachable static dependency directory; not proof of execution |
| K-0117 | D | `out_eqidv2_live_checks_15m_v2/` | 0 | 0 B | - | eqidv2_live_combined_analyser_csv_v15.py:92 reachable static dependency directory; not proof of execution |
| K-0118 | D | `out_eqidv2_live_signals_15m_v2/` | 0 | 0 B | - | eqidv2_live_combined_analyser_csv_v15.py:93 reachable static dependency directory; not proof of execution |
| K-0119 | D | `outputs_ID_v11_5min/` | 21,280 | 1.340 GiB | 2026-06-11 | avwap_5min_ID_v11_backtesting.py:54 reachable static dependency directory; not proof of execution |
| K-0120 | D | `outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250/` | 56 | 476.50 MiB | 2026-06-02 | G dependency path in avwap_5min_ID_v11_backtesting.py:4936 |
| K-0121 | D | `outputs_ID_v11_cleanpool/` | 12,664 | 1.128 GiB | 2026-06-11 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0122 | D | `outputs_ID_v11_conf_fresh_20260629/` | 4,229 | 483.64 MiB | 2026-06-29 | G dependency path in final_setup_conf.py:532 |
| K-0123 | D | `outputs_ID_v11_conf_tier_c_current/` | 10 | 63.94 MiB | 2026-06-15 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0124 | D | `outputs_ID_v11_new_setups_probe/` | 14 | 61.11 MiB | 2026-06-15 | new_setups_scan_v11.py:31 reachable static dependency directory; not proof of execution |
| K-0125 | D | `outputs_ID_v11_tier123_new_setup_probe/` | 31 | 129.92 MiB | 2026-06-15 | research_v11_tier123_new_setups.py:18 reachable static dependency directory; not proof of execution |
| K-0126 | D | `outputs_ID_v11_traintest_pool/` | 4,941 | 292.14 MiB | 2026-06-15 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0127 | D | `outputs_ID_v11_unified_pool/` | 40 | 193.72 MiB | 2026-06-29 | v7_fpe_v2_common.py:32 reachable static dependency directory; not proof of execution |
| K-0128 | D | `outputs_ID_v11_unified_recent_raw/` | 291 | 21.19 MiB | 2026-06-15 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0129 | D | `outputs_ID_v11_unified_recent_raw_0622/` | 1 | 2.07 MiB | 2026-06-22 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0130 | D | `outputs_ID_v11_unified_recent_raw_0624/` | 1 | 891.2 KiB | 2026-06-24 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0131 | D | `outputs_ID_v8_5min_research_restore/` | 10 | 18.41 MiB | 2026-06-10 | v11_lab_shadow_monitor.py:56 reachable static dependency directory; not proof of execution |
| K-0132 | D | `repairs/` | 848 | 3.929 GiB | 2026-09-24 | Retain strategy/dashboard results, operational evidence, shared repairs or runtime state |
| K-0133 | D | `reports/` | 8 | 0 B | 2026-10-09 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:18 reachable static dependency directory; not proof of execution |
| K-0134 | D | `run_logs/` | 8 | 414.0 KiB | 2026-06-01 | Retain strategy/dashboard results, operational evidence, shared repairs or runtime state |
| K-0135 | D | `runtime_manifests/` | 163 | 998.6 KiB | 2026-09-02 | Retain strategy/dashboard results, operational evidence, shared repairs or runtime state |
| K-0136 | D | `runtime_state/` | 0 | 0 B | - | eqidv2_live_combined_analyser_csv_v15.py:97 reachable static dependency directory; not proof of execution |
| K-0137 | D | `runtime_status/` | 240 | 133.1 KiB | 2026-10-10 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:62 reachable static dependency directory; not proof of execution |
| K-0138 | D | `setup_pools_2026_06_29/` | 27 | 73.94 MiB | 2026-06-30 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0139 | D | `signal_discovery_v7_5mins_ID/` | 26,931 | 842.53 MiB | 2026-08-31 | log_dashboard_server.py:65 dashboard path; log_dashboard_server.py:65 reachable static dependency directory; not proof of execution |
| K-0140 | D | `slot_ready_15m/` | 0 | 0 B | - | eqidv2_eod_scheduler_for_15mins_data_live_minimal.py:299 reachable static dependency directory; not proof of execution; eqidv2_live_combined_analyser_csv_v15.py:101 reachable static dependency directory; not proof of execution |
| K-0141 | D | `slot_ready_5m/` | 9,112 | 6.90 MiB | 2026-10-09 | log_dashboard_server.py:68 dashboard path; log_dashboard_server.py:68 reachable static dependency directory; not proof of execution |
| K-0142 | D | `slot_ready_5m_pending/` | 0 | 0 B | - | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:30 reachable static dependency directory; not proof of execution |
| K-0143 | D | `slot_ready_kiteticker_5m/` | 29 | 17.1 KiB | 2026-07-31 | eqidv2_kiteticker_5min_live.py:82 reachable static dependency directory; not proof of execution |
| K-0144 | D | `stocks_cache_15min_eq/` | 0 | 0 B | - | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:23 reachable static dependency directory; not proof of execution |
| K-0145 | D | `stocks_cache_1min_eq/` | 0 | 0 B | - | trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py:70 reachable static dependency directory; not proof of execution |
| K-0146 | D | `stocks_cache_5min_eq/` | 0 | 0 B | - | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:26 reachable static dependency directory; not proof of execution |
| K-0147 | D | `stocks_cache_5min_eq_live/` | 0 | 0 B | - | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0148 | D | `stocks_cache_daily_eq/` | 0 | 0 B | - | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0149 | D | `stocks_indicators_15min_eq/` | 2,088 | 690.70 MiB | 2026-05-16 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:13 reachable static dependency directory; not proof of execution |
| K-0150 | D | `stocks_indicators_1min_eq/` | 2,503 | 17.863 GiB | 2026-10-09 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:16 reachable static dependency directory; not proof of execution |
| K-0151 | D | `stocks_indicators_5min_eq/` | 1,044 | 2.085 GiB | 2026-04-08 | trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py:70 reachable static dependency directory; not proof of execution |
| K-0152 | D | `stocks_indicators_5min_eq_live/` | 1,396 | 142.56 MiB | 2026-10-09 | v7_causality_audit.py:36 reachable static dependency directory; not proof of execution; v7_full_pipeline_entry_research.py:42 reachable static dependency directory; not proof of execution |
| K-0153 | D | `stocks_indicators_5min_eq_live2/` | 1,307 | 3.540 GiB | 2026-10-09 | eqidv2_runtime_paths.py shared runtime default; eqidv2_runtime_paths.py:10 reachable static dependency directory; not proof of execution |
| K-0154 | D | `stocks_indicators_5min_eq_live_kiteticker/` | 1,237 | 100.06 MiB | 2026-07-31 | eqidv2_kiteticker_5min_live.py:62 reachable static dependency directory; not proof of execution |
| K-0155 | D | `stocks_indicators_5min_eq_live_kiteticker.invalid_seed_20260731_1306/` | 358 | 27.05 MiB | 2026-07-31 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0156 | D | `stocks_indicators_daily_eq/` | 2,085 | 139.15 MiB | 2026-05-16 | G dependency path in eqidv2_entry_engine_1min_v5_id.py:194 |
| K-0157 | D | `stocks_raw_1min_entry_v5_id_live/` | 1,082 | 23.27 MiB | 2026-08-31 | avwap_5min_ID_v11_backtesting.py:113 reachable static dependency directory; not proof of execution; eqidv2_entry_engine_1min_v5_id.py:68 reachable static dependency directory; not proof of execution |
| K-0158 | D | `v11_lab_shadow/` | 1,423 | 224.79 MiB | 2026-08-28 | log_dashboard_server.py:95 dashboard path; log_dashboard_server.py:95 reachable static dependency directory; not proof of execution |
| K-0159 | D | `v13_v10_g_execution_research/` | 60 | 235.2 KiB | 2026-10-09 | tools\v13_execution_research.py:26 reachable static dependency directory; not proof of execution; tools\v13_strategy_research.py:28 reachable static dependency directory; not proof of execution |
| K-0160 | D | `v13_v10_g_strategy_research/` | 445 | 7.77 MiB | 2026-10-09 | log_dashboard_server.py:105 dashboard path; log_dashboard_server.py:105 reachable static dependency directory; not proof of execution |
| K-0161 | D | `v13_v10_h_research/` | 1,307 | 1.172 GiB | 2026-09-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0162 | D | `v7_causality_audit/` | 185 | 6.31 MiB | 2026-08-28 | log_dashboard_server.py:81 dashboard path; log_dashboard_server.py:81 reachable static dependency directory; not proof of execution |
| K-0163 | D | `v7_full_pipeline_entry_research/` | 409 | 794.45 MiB | 2026-08-28 | log_dashboard_server.py:89 dashboard path; log_dashboard_server.py:89 reachable static dependency directory; not proof of execution |
| K-0164 | D | `v7_full_pipeline_entry_research_v2/` | 77 | 1.49 MiB | 2026-08-28 | log_dashboard_server.py:91 dashboard path; log_dashboard_server.py:91 reachable static dependency directory; not proof of execution |
| K-0165 | D | `v7_gate_promotion/` | 164 | 211.6 KiB | 2026-08-28 | log_dashboard_server.py:83 dashboard path; log_dashboard_server.py:83 reachable static dependency directory; not proof of execution |
| K-0166 | D | `v7_nse_id_cost/` | 170 | 351.2 KiB | 2026-08-28 | log_dashboard_server.py:77 dashboard path; log_dashboard_server.py:77 reachable static dependency directory; not proof of execution |
| K-0167 | D | `v7_pre_momentum_filter_analyst/` | 248 | 1.31 MiB | 2026-08-31 | log_dashboard_server.py:87 dashboard path; log_dashboard_server.py:87 reachable static dependency directory; not proof of execution |
| K-0168 | D | `v7_qualification/` | 110 | 389.8 KiB | 2026-08-28 | log_dashboard_server.py:85 dashboard path; log_dashboard_server.py:85 reachable static dependency directory; not proof of execution |
| K-0169 | D | `v7_shadow_candidates/` | 80 | 3.66 MiB | 2026-08-28 | log_dashboard_server.py:93 dashboard path; log_dashboard_server.py:93 reachable static dependency directory; not proof of execution |
| K-0170 | D | `v7_walkforward_gate/` | 446 | 58.33 MiB | 2026-08-28 | log_dashboard_server.py:79 dashboard path; log_dashboard_server.py:79 reachable static dependency directory; not proof of execution |
| K-0171 | D | `fno_oi/strategy_research/ema_confirm_0925_0930_0935_0940_0945_v5_selected_daily.csv` | 1 | 10.0 KiB | 2026-08-11 | Live configuration reads this legacy selected daily ledger. |
| K-0172 | P | `.codex_tmp/chrome_v13_anchor2/` | 173 | 9.39 MiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0173 | P | `.codex_tmp/chrome_v13_mobile/` | 171 | 5.39 MiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0174 | P | `.codex_tmp/chrome_v13_report/` | 173 | 9.39 MiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0175 | P | `.codex_tmp/chrome_v13_results/` | 171 | 9.39 MiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0176 | P | `.codex_tmp/dashboard_flow/` | 114 | 89.20 MiB | 2026-10-10 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0177 | P | `.codex_tmp/dashboard_repairs_20260915/` | 11 | 28.04 MiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0178 | P | `.codex_tmp/edge_v13_anchor/` | 242 | 6.60 MiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0179 | P | `.codex_tmp/v13_daywise_compare_work/` | 9 | 602.0 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0180 | P | `.codex_tmp/v13_v5_exit_robustness/` | 28 | 1.24 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0181 | P | `.codex_tmp/v13_v5_profile_eval/` | 22 | 14.80 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0182 | P | `.codex_tmp/v13_v5_timing_entry/` | 46 | 2.90 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0183 | P | `.codex_tmp/v13_v5_v7cache/` | 4 | 17.72 MiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0184 | P | `.codex_tmp/v13_v5_validation_audit/` | 8 | 82.1 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0185 | P | `.codex_tmp/v13_v6_smoke/` | 97 | 4.01 MiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0186 | P | `.codex_tmp/v13_v9_diagnostics_smoke/` | 3 | 21.4 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0187 | P | `ai_platform/` | 100 | 1.46 MiB | 2026-10-05 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0188 | P | `avwap_v11_refactored/` | 79 | 2.04 MiB | 2026-05-14 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0189 | P | `bat/observability/` | 7 | 20.4 KiB | 2026-09-25 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0190 | P | `configs/` | 29 | 100.8 KiB | 2026-09-29 | dashboard, launchers, scheduler and API configuration/log support |
| K-0191 | P | `docs/` | 36 | 7.70 MiB | 2026-10-10 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0192 | P | `docs_early_emit/` | 3 | 26.7 KiB | 2026-04-22 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0193 | P | `ID_backtesting/` | 34 | 929.8 KiB | 2026-05-18 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0194 | P | `isolated_long_3m_research_20260730/` | 73 | 96.03 MiB | 2026-08-05 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0195 | P | `kite_exports/` | 5 | 15.6 KiB | 2026-10-09 | log_dashboard_server.py:402 dashboard path; dashboard, launchers, scheduler and API configuration/log support |
| K-0196 | P | `logs/` | 2,447 | 1.607 GiB | 2026-10-10 | G-family related documentation or logs |
| K-0197 | P | `outputs_advanced_indicators_5min/` | 1,046 | 5.650 GiB | 2026-05-16 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0198 | P | `outputs_advanced_indicators_5min_current/` | 1,045 | 5.570 GiB | 2026-05-16 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0199 | P | `outputs_advanced_indicators_5min_smoke/` | 9 | 27.94 MiB | 2026-05-16 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0200 | P | `outputs_avwap_ID_5min_v2/` | 7 | 129.38 MiB | 2026-05-16 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0201 | P | `outputs_avwap_ID_5min_v2_all_setups_ungated/` | 7 | 236.36 MiB | 2026-05-18 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0202 | P | `report_v10_assets/` | 32 | 469.3 KiB | 2026-08-30 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0203 | P | `report_v11_assets/` | 83 | 2.83 MiB | 2026-08-30 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0204 | P | `report_v12_assets/` | 90 | 4.05 MiB | 2026-08-30 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0205 | P | `reports/` | 5,730 | 2.159 GiB | 2026-10-09 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0206 | P | `research_outputs/` | 719 | 111.24 MiB | 2026-09-25 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0207 | P | `resume_output/` | 1 | 6.2 KiB | 2026-07-29 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0208 | P | `stocks_cache_1min_eq/` | 0 | 0 B | - | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0209 | P | `stocks_indicators_1min_eq/` | 2,001 | 10.316 GiB | 2026-05-05 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0210 | P | `stocks_indicators_daily_eq/` | 1,041 | 49.71 MiB | 2026-04-10 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0211 | P | `tests/` | 640 | 11.90 MiB | 2026-10-10 | Retain related logs, documentation, configuration, tests, reports or shared support code |
| K-0212 | P | `Train_and_Test/` | 4,457 | 1.296 GiB | 2026-08-06 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0213 | P | `v17D_setup_library/` | 20 | 244.1 KiB | 2026-08-05 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0214 | P | `v7_research_layer/` | 13 | 1.03 MiB | 2026-07-29 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0215 | P | `v7_v11_parity/` | 26 | 189.6 KiB | 2026-07-26 | Directory name referenced by protected source or launcher; retained conservatively |
| K-0216 | P | `.codex_tmp/build_v13_g2_three_sl_daywise.py` | 1 | 4.8 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0217 | P | `.codex_tmp/build_v13_v5_complete_report.py` | 1 | 91.3 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0218 | P | `.codex_tmp/g3_daywise_report.js` | 1 | 14.9 KiB | 2026-10-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0219 | P | `.codex_tmp/g3_daywise_report_check.cjs` | 1 | 3.0 KiB | 2026-10-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0220 | P | `.codex_tmp/g_dashboard_snapshot_validation.json` | 1 | 12.0 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0221 | P | `.codex_tmp/g_dashboard_validation.json` | 1 | 2.8 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0222 | P | `.codex_tmp/options_v13_v10_g_20260915_fetch_plan.csv` | 1 | 631 B | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0223 | P | `.codex_tmp/run_v13_v5_options_today_20260908.ps1` | 1 | 3.0 KiB | 2026-09-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0224 | P | `.codex_tmp/sweep_options_v13_v10_g_20260915.py` | 1 | 3.1 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0225 | P | `.codex_tmp/v10_g_options_initial_data_audit.csv` | 1 | 139.9 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0226 | P | `.codex_tmp/v10_g_options_initial_fetch_plan.csv` | 1 | 2.2 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0227 | P | `.codex_tmp/v10_g_options_postfetch_data_audit.csv` | 1 | 26.9 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0228 | P | `.codex_tmp/v10_g_options_postfetch_source_conflicts.json` | 1 | 2 B | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0229 | P | `.codex_tmp/v13_g2_daywise_audit.json` | 1 | 103.0 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0230 | P | `.codex_tmp/v13_g2_daywise_audit.py` | 1 | 3.6 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0231 | P | `.codex_tmp/v13_g2_sl_sweep.py` | 1 | 7.3 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0232 | P | `.codex_tmp/v13_g2_sl_sweep_diagnostics.py` | 1 | 2.4 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0233 | P | `.codex_tmp/v13_g2_sl_sweep_monthly.csv` | 1 | 9.9 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0234 | P | `.codex_tmp/v13_g2_sl_sweep_results.csv` | 1 | 14.3 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0235 | P | `.codex_tmp/v13_g2_sl_sweep_summary.json` | 1 | 4.2 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0236 | P | `.codex_tmp/v13_report_anchor2.png` | 1 | 12.6 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0237 | P | `.codex_tmp/v13_report_diagram_selenium.png` | 1 | 165.9 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0238 | P | `.codex_tmp/v13_report_mobile.png` | 1 | 2.9 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0239 | P | `.codex_tmp/v13_report_mobile_final.png` | 1 | 21.7 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0240 | P | `.codex_tmp/v13_report_mobile_selenium.png` | 1 | 21.7 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0241 | P | `.codex_tmp/v13_report_results.png` | 1 | 12.6 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0242 | P | `.codex_tmp/v13_report_results_edge.png` | 1 | 12.6 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0243 | P | `.codex_tmp/v13_report_results_selenium.png` | 1 | 62.7 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0244 | P | `.codex_tmp/v13_report_top.png` | 1 | 69.9 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0245 | P | `.codex_tmp/v13_v10_g2_daywise_sl_1p00_1p25_2p75_through_2026-09-30.csv` | 1 | 11.5 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0246 | P | `.codex_tmp/v13_v10_g_full_options_sweep.py` | 1 | 5.0 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0247 | P | `.codex_tmp/v13_v5_build_v7.py` | 1 | 1.7 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0248 | P | `.codex_tmp/v13_v5_conservative_probe.py` | 1 | 3.5 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0249 | P | `.codex_tmp/v13_v5_exit_gap_probe.py` | 1 | 2.2 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0250 | P | `.codex_tmp/v13_v5_intraday_snapshot.py` | 1 | 3.1 KiB | 2026-09-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0251 | P | `.codex_tmp/v13_v5_probe.py` | 1 | 4.0 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0252 | P | `.codex_tmp/v13_v5_today_v6_exits.py` | 1 | 3.0 KiB | 2026-09-07 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0253 | P | `.codex_tmp/v13_v9_control_parity.py` | 1 | 1.6 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0254 | P | `.codex_tmp/v13g_doc_architecture.json` | 1 | 30.0 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0255 | P | `.codex_tmp/v13g_doc_results.json` | 1 | 22.2 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0256 | P | `.codex_tmp/v13g_doc_strategy.json` | 1 | 26.6 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0257 | P | `.codex_tmp/v13g_document_deployment.tar.gz` | 1 | 2.04 MiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0258 | P | `.codex_tmp/verify_g3_pullback_followthrough.py` | 1 | 9.2 KiB | 2026-10-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0259 | P | `.codex_tmp/verify_g3_selective_v2.py` | 1 | 14.6 KiB | 2026-10-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0260 | P | `.codex_tmp/verify_g_daily_dashboard.py` | 1 | 2.9 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0261 | P | `.dashboard-review-dark-final.png` | 1 | 135.6 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0262 | P | `.dashboard-review-desktop-after.png` | 1 | 149.7 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0263 | P | `.dashboard-review-desktop-final.png` | 1 | 131.8 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0264 | P | `.dashboard-review-desktop-final2.png` | 1 | 136.0 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0265 | P | `.dashboard-review-desktop-loaded.png` | 1 | 154.0 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0266 | P | `.dashboard-review-desktop.png` | 1 | 32.4 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0267 | P | `.dashboard-review-mobile-after-dsf.png` | 1 | 25.6 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0268 | P | `.dashboard-review-mobile-after.png` | 1 | 25.6 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0269 | P | `.dashboard-review-mobile-after2.png` | 1 | 23.8 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0270 | P | `.dashboard-review-mobile-final.png` | 1 | 33.6 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0271 | P | `.dashboard-review-mobile-final2.png` | 1 | 33.9 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0272 | P | `.dashboard-review-mobile-loaded.png` | 1 | 31.4 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0273 | P | `.dashboard-review-mobile-selenium.png` | 1 | 28.0 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0274 | P | `.dashboard-review-mobile.png` | 1 | 15.6 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0275 | P | `.dashboard-review-restart-btn-final.png` | 1 | 135.6 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0276 | P | `.dashboard-review-tablet-after.png` | 1 | 65.4 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0277 | P | `.dashboard-review-tablet-final.png` | 1 | 63.8 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0278 | P | `.dashboard-review-tablet-final2.png` | 1 | 59.4 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0279 | P | `.dashboard-review-tablet-loaded.png` | 1 | 70.5 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0280 | P | `.dashboard-review-tablet.png` | 1 | 23.4 KiB | 2026-05-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0281 | P | `_v17q_run5_max_optimizer.py` | 1 | 15.7 KiB | 2026-04-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0282 | P | `_v17q_run5_optimizer.py` | 1 | 25.7 KiB | 2026-04-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0283 | P | `_v17q_run5_per_setup_optimizer.py` | 1 | 12.3 KiB | 2026-04-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0284 | P | `_v17r_setup_lab_analyzer.py` | 1 | 53.1 KiB | 2026-04-28 | Referenced by a protected source/launcher; conservative static code closure |
| K-0285 | P | `access_token.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0286 | P | `access_token2.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0287 | P | `access_token3.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0288 | P | `access_token4.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0289 | P | `access_token5.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0290 | P | `access_token6.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0291 | P | `access_token7.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0292 | P | `access_token8.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0293 | P | `api_key.txt` | 1 | 112 B | 2026-02-10 | G-family import/launcher dependency audit |
| K-0294 | P | `api_key2.txt` | 1 | 112 B | 2026-02-27 | G-family import/launcher dependency audit |
| K-0295 | P | `api_key3.txt` | 1 | 112 B | 2026-02-27 | G-family import/launcher dependency audit |
| K-0296 | P | `api_key4.txt` | 1 | 112 B | 2026-02-27 | G-family import/launcher dependency audit |
| K-0297 | P | `api_key5.txt` | 1 | 112 B | 2026-03-24 | G-family import/launcher dependency audit |
| K-0298 | P | `api_key6.txt` | 1 | 112 B | 2026-03-24 | G-family import/launcher dependency audit |
| K-0299 | P | `api_key7.txt` | 1 | 112 B | 2026-08-31 | G-family import/launcher dependency audit |
| K-0300 | P | `api_key8.txt` | 1 | 112 B | 2026-03-25 | G-family import/launcher dependency audit |
| K-0301 | P | `auth_v2_state.json` | 1 | 1.2 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0302 | P | `authentication_v2.py` | 1 | 35.8 KiB | 2026-09-29 | G-family import/launcher dependency audit |
| K-0303 | P | `avwap_5min_ID_v11_backtesting.py` | 1 | 227.1 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0304 | P | `avwap_5min_ID_v12_backtesting.py` | 1 | 235.5 KiB | 2026-08-06 | Referenced by a protected source/launcher; conservative static code closure |
| K-0305 | P | `avwap_5min_ID_v2_backtesting.py` | 1 | 82.0 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0306 | P | `avwap_5min_ID_v3_backtesting.py` | 1 | 13.6 KiB | 2026-05-18 | G-family import/launcher dependency audit |
| K-0307 | P | `avwap_5min_ID_v5_backtesting.py` | 1 | 9.0 KiB | 2026-05-18 | G-family import/launcher dependency audit |
| K-0308 | P | `avwap_5min_ID_v5_exit_sweep.py` | 1 | 10.9 KiB | 2026-05-18 | Referenced by a protected source/launcher; conservative static code closure |
| K-0309 | P | `avwap_5min_ID_v6_backtesting.py` | 1 | 12.9 KiB | 2026-06-06 | G-family import/launcher dependency audit |
| K-0310 | P | `avwap_5min_ID_v7_backtesting.py` | 1 | 22.2 KiB | 2026-06-10 | G-family import/launcher dependency audit |
| K-0311 | P | `avwap_5min_ID_v7_candidate_scan.py` | 1 | 63.5 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0312 | P | `avwap_5min_ID_v7_live_scan.py` | 1 | 32.7 KiB | 2026-08-06 | G-family import/launcher dependency audit |
| K-0313 | P | `avwap_combined_runner.py` | 1 | 79.7 KiB | 2026-03-09 | G-family import/launcher dependency audit |
| K-0314 | P | `avwap_combined_runner_v13.py` | 1 | 92.6 KiB | 2026-03-12 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0315 | P | `avwap_combined_runner_v15.py` | 1 | 149.8 KiB | 2026-03-31 | G-family import/launcher dependency audit |
| K-0316 | P | `avwap_combined_runner_v16_5min.py` | 1 | 175.7 KiB | 2026-05-05 | G-family import/launcher dependency audit |
| K-0317 | P | `avwap_combined_runner_v17b_5min.py` | 1 | 30.2 KiB | 2026-04-18 | G-family import/launcher dependency audit |
| K-0318 | P | `avwap_combined_runner_v17B_live_5min.py` | 1 | 33.1 KiB | 2026-04-30 | G-family import/launcher dependency audit |
| K-0319 | P | `avwap_combined_runner_v17c_5min.py` | 1 | 20.6 KiB | 2026-04-18 | G-family import/launcher dependency audit |
| K-0320 | P | `avwap_combined_runner_v17C_noNF_live_5min.py` | 1 | 61.4 KiB | 2026-05-05 | G-family import/launcher dependency audit |
| K-0321 | P | `avwap_combined_runner_v17d_5min.py` | 1 | 20.2 KiB | 2026-04-18 | G-family import/launcher dependency audit |
| K-0322 | P | `avwap_combined_runner_v17f_5min.py` | 1 | 13.1 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0323 | P | `avwap_combined_runner_v17g_5min.py` | 1 | 17.4 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0324 | P | `avwap_combined_runner_v17h_5min.py` | 1 | 10.3 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0325 | P | `avwap_combined_runner_v17i_5min.py` | 1 | 10.2 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0326 | P | `avwap_combined_runner_v17j_5min.py` | 1 | 11.2 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0327 | P | `avwap_combined_runner_v17k_5min.py` | 1 | 17.0 KiB | 2026-04-25 | G-family import/launcher dependency audit |
| K-0328 | P | `avwap_combined_runner_v17m_5min.py` | 1 | 16.9 KiB | 2026-04-25 | G-family import/launcher dependency audit |
| K-0329 | P | `avwap_combined_runner_v17n_5min.py` | 1 | 14.1 KiB | 2026-04-26 | G-family import/launcher dependency audit |
| K-0330 | P | `avwap_combined_runner_v17o_5min.py` | 1 | 11.6 KiB | 2026-04-26 | G-family import/launcher dependency audit |
| K-0331 | P | `avwap_combined_runner_v17p_5min.py` | 1 | 14.4 KiB | 2026-04-26 | G-family import/launcher dependency audit |
| K-0332 | P | `avwap_combined_runner_v7_sweep.py` | 1 | 85.2 KiB | 2026-03-17 | G-family import/launcher dependency audit |
| K-0333 | P | `avwap_live_signal_generator.py` | 1 | 50.0 KiB | 2026-02-20 | G-family import/launcher dependency audit |
| K-0334 | P | `avwap_ml_backtest_runner.py` | 1 | 10.6 KiB | 2026-02-19 | Referenced by a protected source/launcher; conservative static code closure |
| K-0335 | P | `avwap_trade_execution_PAPER_TRADE_FALSE.py` | 1 | 64.9 KiB | 2026-03-02 | Referenced by a protected source/launcher; conservative static code closure |
| K-0336 | P | `avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py` | 1 | 210.1 KiB | 2026-07-30 | bat\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat:21 launcher/asset reference; avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py:4832 module-name reference |
| K-0337 | P | `avwap_trade_execution_PAPER_TRADE_FALSE_v15.py` | 1 | 142.0 KiB | 2026-03-25 | G-family import/launcher dependency audit |
| K-0338 | P | `avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py` | 1 | 201.1 KiB | 2026-05-04 | G-family import/launcher dependency audit |
| K-0339 | P | `avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep.py` | 1 | 120.2 KiB | 2026-03-16 | Referenced by a protected source/launcher; conservative static code closure |
| K-0340 | P | `avwap_trade_execution_PAPER_TRADE_TRUE.py` | 1 | 67.3 KiB | 2026-02-26 | Referenced by a protected source/launcher; conservative static code closure |
| K-0341 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py` | 1 | 157.5 KiB | 2026-07-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0342 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v15.py` | 1 | 83.1 KiB | 2026-03-25 | G-family import/launcher dependency audit |
| K-0343 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.py` | 1 | 105.3 KiB | 2026-05-04 | G-family import/launcher dependency audit |
| K-0344 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v2.py` | 1 | 69.2 KiB | 2026-02-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0345 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v4.py` | 1 | 72.3 KiB | 2026-03-02 | bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_v4.bat:6 launcher/asset reference |
| K-0346 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v5.py` | 1 | 76.6 KiB | 2026-03-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0347 | P | `avwap_trade_execution_PAPER_TRADE_TRUE_v7_sweep.py` | 1 | 76.8 KiB | 2026-03-16 | Referenced by a protected source/launcher; conservative static code closure |
| K-0348 | P | `backtesting_result_v11_daily.py` | 1 | 49.9 KiB | 2026-07-28 | reconcile.py:175 literal reference; backtesting_result_v11_daily.py:1128 module-name reference |
| K-0349 | P | `backtesting_result_v13_v10_g_daily.py` | 1 | 18.9 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0350 | P | `bat/assert_fno_oi_fast_production_trial_exclusive.ps1` | 1 | 1.3 KiB | 2026-09-01 | bat\run_fno_oi_fetch_5min_fast_production.bat:8 launcher/asset reference |
| K-0351 | P | `bat/check_pf_supervisor_health.ps1` | 1 | 9.2 KiB | 2026-07-02 | bat\run_pf_supervisor_healthcheck.bat:7 launcher/asset reference; bat\run_pf_supervisor_healthcheck.bat:12 launcher/asset reference |
| K-0352 | P | `bat/fix_task_trigger_backtesting_1545.bat` | 1 | 1.3 KiB | 2026-06-10 | Referenced by a protected source/launcher; conservative static code closure |
| K-0353 | P | `bat/fno_oi_fast_production_trial_date_gate.ps1` | 1 | 1.4 KiB | 2026-09-01 | bat\run_fno_oi_fetch_5min.bat:5 launcher/asset reference; bat\run_fno_oi_fetch_5min_fast_shadow.bat:5 launcher/asset reference |
| K-0354 | P | `bat/gmail_client_secret.json` | 1 | 408 B | 2026-02-23 | bat\run_log_dashboard_gmail_api_bootstrap.bat:12 launcher/asset reference; bat\run_log_dashboard_public_link.bat:20 launcher/asset reference |
| K-0355 | P | `bat/gmail_token.json` | 1 | 740 B | 2026-09-18 | bat\run_log_dashboard_gmail_api_bootstrap.bat:14 launcher/asset reference; bat\run_log_dashboard_public_link.bat:22 launcher/asset reference |
| K-0356 | P | `bat/harden_scheduled_task.ps1` | 1 | 3.6 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0357 | P | `bat/migrate_backtesting_task_to_v13_v10_g.ps1` | 1 | 4.6 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0358 | P | `bat/migrate_fno_v6_tasks_to_v13_v10_g.ps1` | 1 | 5.8 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0359 | P | `bat/restore_fno_v6_1m_after_v8_paper.ps1` | 1 | 6.5 KiB | 2026-09-01 | fno_v8_combined_paper_control.py:75 literal reference |
| K-0360 | P | `bat/rotate_log_if_large.ps1` | 1 | 709 B | 2026-08-31 | bat\run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat:57 launcher/asset reference |
| K-0361 | P | `bat/run_authentication_v2.bat` | 1 | 3.7 KiB | 2026-09-29 | scheduled task \EQIDV2_authentication_v2_0900 state=Ready; log_dashboard_server.py:822 literal reference |
| K-0362 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat` | 1 | 3.0 KiB | 2026-06-09 | scheduled task \EQIDV2_live_trade_id_5min_v7_0900 state=Disabled |
| K-0363 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_v15.bat` | 1 | 1.8 KiB | 2026-03-23 | scheduled task \EQIDV2_avwap_live_trade_v15_0905 state=Disabled; preopen_session_autofix.py:30 literal reference |
| K-0364 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat` | 1 | 4.0 KiB | 2026-04-27 | scheduled task \EQIDV2_live_trade_v16_5min_0900 state=Disabled |
| K-0365 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat` | 1 | 4.9 KiB | 2026-07-28 | Referenced by a protected source/launcher; conservative static code closure |
| K-0366 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v15.bat` | 1 | 3.3 KiB | 2026-03-23 | scheduled task \EQIDV2_avwap_paper_trade_v15_0900 state=Disabled; preopen_session_autofix.py:29 literal reference |
| K-0367 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.bat` | 1 | 3.9 KiB | 2026-04-24 | scheduled task \EQIDV2_paper_trade_v16_5min_0900 state=Disabled |
| K-0368 | P | `bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v4.bat` | 1 | 720 B | 2026-03-09 | scheduled task \EQIDV2_avwap_paper_trade_v4_0900 state=Disabled |
| K-0369 | P | `bat/run_backtesting_result_v11_1600.bat` | 1 | 165 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0370 | P | `bat/run_backtesting_result_v13_v10_g_1620.bat` | 1 | 4.8 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0371 | P | `bat/run_collect_filtered_stock_data_session.bat` | 1 | 1.0 KiB | 2026-08-07 | scheduled task \EQIDV2_collect_filtered_stock_data_0930 state=Disabled; log_dashboard_server.py:819 literal reference |
| K-0372 | P | `bat/run_conf_paper_entry_engine.bat` | 1 | 1.5 KiB | 2026-07-28 | scheduled task \EQIDV2_entry_engine_1min_v5_ID state=Disabled; log_dashboard_server.py:802 literal reference |
| K-0373 | P | `bat/run_conf_paper_executor.bat` | 1 | 1.7 KiB | 2026-07-28 | scheduled task \EQIDV2_paper_trade_id_5min_v7_0900 state=Disabled; log_dashboard_server.py:804 literal reference |
| K-0374 | P | `bat/run_conf_paper_signal_discovery.bat` | 1 | 716 B | 2026-07-28 | scheduled task \EQIDV2_signal_discovery_v7_5mins_ID state=Disabled; log_dashboard_server.py:803 literal reference |
| K-0375 | P | `bat/run_dashboard_flow.bat` | 1 | 106 B | 2026-10-10 | dashboard launcher/configuration |
| K-0376 | P | `bat/run_data_for_backtesting_parallel.ps1` | 1 | 10.5 KiB | 2026-10-08 | bat\run_moving_files_1545.bat:26 launcher/asset reference |
| K-0377 | P | `bat/run_eqidv2_daily_live_v7_research_session.bat` | 1 | 1.2 KiB | 2026-06-04 | scheduled task \EQIDV2_daily_live_v7_research_0917 state=Disabled; log_dashboard_server.py:806 literal reference |
| K-0378 | P | `bat/run_eqidv2_detection_engine_v16_5min.bat` | 1 | 5.5 KiB | 2026-04-27 | scheduled task \EQIDV2_detection_engine_v16_5min_0900 state=Disabled; log_dashboard_server.py:797 literal reference |
| K-0379 | P | `bat/run_eqidv2_eod_scheduler_for_1540_update.bat` | 1 | 895 B | 2026-02-23 | scheduled task \EQIDV2_eod_1540_update_1540 state=Disabled; preopen_session_healthcheck.py:1279 literal reference |
| K-0380 | P | `bat/run_eqidv2_eod_scheduler_for_15mins_data.bat` | 1 | 3.1 KiB | 2026-03-16 | bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_v15.bat:4 launcher/asset reference; bat\run_eqidv2_eod_scheduler_for_15mins_data_live_minimal.bat:4 launcher/asset reference |
| K-0381 | P | `bat/run_eqidv2_eod_scheduler_for_15mins_data_live_minimal.bat` | 1 | 5.2 KiB | 2026-03-25 | scheduled task \EQIDV2_eod_15mins_data_0900 state=Disabled; preopen_session_autofix.py:25 literal reference |
| K-0382 | P | `bat/run_eqidv2_eod_scheduler_for_1min_data_live.bat` | 1 | 3.6 KiB | 2026-08-31 | scheduled task \EQIDV2_eod_1min_data_0915 state=Disabled; log_dashboard_server.py:780 literal reference |
| K-0383 | P | `bat/run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat` | 1 | 6.2 KiB | 2026-09-29 | scheduled task \EQIDV2_eod_5mins_data_0900 state=Ready; log_dashboard_server.py:779 literal reference |
| K-0384 | P | `bat/run_eqidv2_kiteticker_5min_live.bat` | 1 | 3.4 KiB | 2026-07-31 | scheduled task \EQIDV2_kiteticker_5mins_data_0900 state=Disabled |
| K-0385 | P | `bat/run_eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.bat` | 1 | 5.8 KiB | 2026-06-09 | scheduled task \EQIDV2_live_combined_csv_id_5min_v7_0909 state=Disabled |
| K-0386 | P | `bat/run_eqidv2_live_combined_analyser_csv_v15_new_persistent.bat` | 1 | 5.4 KiB | 2026-04-20 | scheduled task \EQIDV2_live_combined_csv_v15_new_0900 state=Disabled; preopen_session_autofix.py:28 literal reference |
| K-0387 | P | `bat/run_eqidv2_live_combined_analyser_csv_v16_5min.bat` | 1 | 4.9 KiB | 2026-04-28 | scheduled task \EQIDV2_live_combined_csv_v16_5min_0900 state=Disabled |
| K-0388 | P | `bat/run_eqidv2_live_combined_analyser_csv_v4_long.bat` | 1 | 1.0 KiB | 2026-04-20 | scheduled task \EQIDV2_live_combined_csv_v4_long_0900 state=Disabled |
| K-0389 | P | `bat/run_eqidv2_live_combined_analyser_csv_v4_short.bat` | 1 | 1.0 KiB | 2026-04-20 | scheduled task \EQIDV2_live_combined_csv_v4_short_0900 state=Disabled |
| K-0390 | P | `bat/run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat` | 1 | 5.2 KiB | 2026-06-13 | scheduled task \EQIDV2_nifty_guard_fetch_v16_5min_0915 state=Ready; log_dashboard_server.py:778 literal reference |
| K-0391 | P | `bat/run_eqidv2_pending_data_fetcher_v16_5min.bat` | 1 | 7.4 KiB | 2026-04-27 | scheduled task \EQIDV2_pending_data_fetcher_v16_5min_0900 state=Disabled; log_dashboard_server.py:798 literal reference |
| K-0392 | P | `bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat` | 1 | 8.5 KiB | 2026-07-31 | Referenced by a protected source/launcher; conservative static code closure |
| K-0393 | P | `bat/run_eqidv2_signal_early_engine_v16_5min.bat` | 1 | 3.3 KiB | 2026-04-24 | scheduled task \EQIDV2_signal_early_engine_v16_5min_0900 state=Disabled; log_dashboard_server.py:796 literal reference |
| K-0394 | P | `bat/run_eqidv2_suggestions_v7_live_research_1605.bat` | 1 | 1.0 KiB | 2026-05-28 | scheduled task \EQIDV2_suggestions_v7_live_research_1615 state=Disabled |
| K-0395 | P | `bat/run_eqidv2_v15_stack_stop.bat` | 1 | 427 B | 2026-03-17 | scheduled task \EQIDV2_v15_stack_stop_1615 state=Ready |
| K-0396 | P | `bat/run_eqidv2_v7_pre_momentum_filter_analyst.bat` | 1 | 1.3 KiB | 2026-06-05 | scheduled task \EQIDV2_v7_pre_momentum_filter_analyst_0917 state=Disabled; log_dashboard_server.py:812 literal reference |
| K-0397 | P | `bat/run_eqidv2_v7_research_layer.bat` | 1 | 1.1 KiB | 2026-06-04 | scheduled task \EQIDV2_v7_research_layer_0917 state=Disabled; log_dashboard_server.py:805 literal reference |
| K-0398 | P | `bat/run_fno_5min_rankings_backfill.ps1` | 1 | 3.2 KiB | 2026-08-26 | scheduled task \EQIDV2_fno_5min_rankings_backfill_once_20260826 state=Ready |
| K-0399 | P | `bat/run_fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5_full_history.bat` | 1 | 1.1 KiB | 2026-08-11 | Referenced by a protected source/launcher; conservative static code closure |
| K-0400 | P | `bat/run_fno_oi_ema_confirm_0925_0930_0935_v4.bat` | 1 | 1.0 KiB | 2026-08-10 | Referenced by a protected source/launcher; conservative static code closure |
| K-0401 | P | `bat/run_fno_oi_eod_qc.bat` | 1 | 793 B | 2026-08-10 | scheduled task \EQIDV2_fno_oi_eod_qc_1540 state=Ready |
| K-0402 | P | `bat/run_fno_oi_feature_ranker.bat` | 1 | 1.7 KiB | 2026-08-10 | scheduled task \EQIDV2_fno_oi_feature_ranker_0915 state=Ready; log_dashboard_server.py:783 literal reference |
| K-0403 | P | `bat/run_fno_oi_fetch_5min.bat` | 1 | 2.3 KiB | 2026-09-01 | scheduled task \EQIDV2_fno_oi_fetch_5min_0905 state=Disabled |
| K-0404 | P | `bat/run_fno_oi_fetch_5min_fast_production.bat` | 1 | 2.4 KiB | 2026-09-04 | scheduled task \EQIDV2_fno_oi_fetch_5min_fast_production_0905 state=Ready; log_dashboard_server.py:781 literal reference |
| K-0405 | P | `bat/run_fno_oi_fetch_5min_fast_shadow.bat` | 1 | 2.5 KiB | 2026-09-01 | scheduled task \EQIDV2_fno_oi_fetch_5min_fast_shadow_0906 state=Disabled |
| K-0406 | P | `bat/run_fno_oi_universe.bat` | 1 | 842 B | 2026-08-12 | scheduled task \EQIDV2_fno_oi_universe_0850 state=Ready |
| K-0407 | P | `bat/run_fno_options_atm_fetch_5min.bat` | 1 | 2.2 KiB | 2026-09-16 | G-family import/launcher dependency audit |
| K-0408 | P | `bat/run_fno_today_data_repair_20260831.bat` | 1 | 3.8 KiB | 2026-08-31 | scheduled task \EQIDV2_fno_today_data_repair_once_20260831_1705 state=Ready |
| K-0409 | P | `bat/run_fno_v10_v11_v12_paper_session.bat` | 1 | 1.4 KiB | 2026-08-31 | scheduled task \EQIDV2_fno_v10_v11_v12_paper_0915 state=Disabled; log_dashboard_server.py:795 literal reference |
| K-0410 | P | `bat/run_fno_v13_v10_g_confirmation_1min.bat` | 1 | 807 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0411 | P | `bat/run_fno_v13_v10_g_equity_1min_feed.bat` | 1 | 767 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0412 | P | `bat/run_fno_v13_v10_g_live_kite_qty1.bat` | 1 | 4.6 KiB | 2026-09-29 | G-family import/launcher dependency audit |
| K-0413 | P | `bat/run_fno_v13_v10_g_live_long.bat` | 1 | 772 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0414 | P | `bat/run_fno_v13_v10_g_live_short.bat` | 1 | 776 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0415 | P | `bat/run_fno_v13_v10_g_net_result.bat` | 1 | 766 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0416 | P | `bat/run_fno_v13_v10_g_options_live_long.bat` | 1 | 839 B | 2026-09-15 | G-family import/launcher dependency audit |
| K-0417 | P | `bat/run_fno_v13_v10_g_options_live_short.bat` | 1 | 843 B | 2026-09-15 | G-family import/launcher dependency audit |
| K-0418 | P | `bat/run_fno_v13_v10_g_options_net_result.bat` | 1 | 810 B | 2026-09-15 | G-family import/launcher dependency audit |
| K-0419 | P | `bat/run_fno_v13_v10_g_options_trade_logger.bat` | 1 | 826 B | 2026-09-15 | G-family import/launcher dependency audit |
| K-0420 | P | `bat/run_fno_v13_v10_g_scanner_5min.bat` | 1 | 870 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0421 | P | `bat/run_fno_v13_v10_g_trade_logger.bat` | 1 | 782 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0422 | P | `bat/run_fno_v13_v10_h_backtest.bat` | 1 | 139 B | 2026-09-27 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0423 | P | `bat/run_fno_v5_confirmation_1min.bat` | 1 | 579 B | 2026-08-10 | scheduled task \EQIDV2_fno_v5_confirmation_1min_0919 state=Disabled |
| K-0424 | P | `bat/run_fno_v5_live_long.bat` | 1 | 566 B | 2026-08-10 | scheduled task \EQIDV2_fno_v5_live_long_0920 state=Disabled |
| K-0425 | P | `bat/run_fno_v5_live_short.bat` | 1 | 568 B | 2026-08-10 | scheduled task \EQIDV2_fno_v5_live_short_0920 state=Disabled |
| K-0426 | P | `bat/run_fno_v5_net_result.bat` | 1 | 567 B | 2026-08-10 | scheduled task \EQIDV2_fno_v5_net_result_0920 state=Disabled |
| K-0427 | P | `bat/run_fno_v5_scanner_5min.bat` | 1 | 647 B | 2026-08-11 | scheduled task \EQIDV2_fno_v5_scanner_5min_0918 state=Disabled |
| K-0428 | P | `bat/run_fno_v5_trade_logger.bat` | 1 | 571 B | 2026-08-10 | scheduled task \EQIDV2_fno_v5_trade_logger_0920 state=Disabled |
| K-0429 | P | `bat/run_fno_v6_live_kite_qty1.bat` | 1 | 164 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0430 | P | `bat/run_fno_v6_scanner_5min.bat` | 1 | 162 B | 2026-09-14 | fno_v8_combined_paper_control.py:66 literal reference |
| K-0431 | P | `bat/run_fno_v8_combined_paper_session.bat` | 1 | 955 B | 2026-08-21 | scheduled task \EQIDV2_fno_v8_combined_paper_0915 state=Disabled; fno_v8_combined_paper_control.py:78 literal reference |
| K-0432 | P | `bat/run_fno_v8_v9_last_10_backtest.ps1` | 1 | 1.4 KiB | 2026-08-26 | scheduled task \EQIDV2_fno_v8_v9_backtest_10d_once_20260826 state=Disabled |
| K-0433 | P | `bat/run_fundamental_price_action_v1_papertrade.bat` | 1 | 3.7 KiB | 2026-08-07 | scheduled task \EQIDV2_fundamental_price_action_v1_papertrade_0920 state=Disabled; log_dashboard_server.py:818 literal reference |
| K-0434 | P | `bat/run_fundamental_price_action_v1_session.bat` | 1 | 3.7 KiB | 2026-08-07 | scheduled task \EQIDV2_fundamental_price_action_v1_0915 state=Disabled; log_dashboard_server.py:817 literal reference |
| K-0435 | P | `bat/run_log_dashboard_gmail_api_bootstrap.bat` | 1 | 1.4 KiB | 2026-02-23 | dashboard launcher/configuration; bat\run_log_dashboard_gmail_api_bootstrap.bat:16 launcher/asset reference |
| K-0436 | P | `bat/run_log_dashboard_public_link.bat` | 1 | 5.3 KiB | 2026-04-20 | dashboard launcher/configuration; bat\run_log_dashboard_public_link.bat:58 launcher/asset reference |
| K-0437 | P | `bat/run_log_dashboard_public_link.bat.20260223_162526.bak` | 1 | 4.2 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0438 | P | `bat/run_log_dashboard_public_link.bat.20260223_164435.autosend.bak` | 1 | 3.4 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0439 | P | `bat/run_log_dashboard_public_link.bat.20260223_165340.waba.bak` | 1 | 3.4 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0440 | P | `bat/run_log_dashboard_public_link.bat.20260223_170022.gmail.bak` | 1 | 4.2 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0441 | P | `bat/run_log_dashboard_public_link.bat.20260223_171948.gmailapi.bak` | 1 | 4.2 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0442 | P | `bat/run_log_dashboard_public_link_capture.ps1` | 1 | 9.2 KiB | 2026-03-16 | dashboard launcher/configuration; bat\run_log_dashboard_public_link.bat:95 launcher/asset reference |
| K-0443 | P | `bat/run_log_dashboard_public_link_capture.ps1.20260223_164435.autosend.bak` | 1 | 2.1 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0444 | P | `bat/run_log_dashboard_public_link_capture.ps1.20260223_165340.waba.bak` | 1 | 4.7 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0445 | P | `bat/run_log_dashboard_public_link_capture.ps1.20260223_170023.gmail.bak` | 1 | 9.6 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0446 | P | `bat/run_log_dashboard_public_link_capture.ps1.20260223_171137.smtpfix.bak` | 1 | 5.4 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0447 | P | `bat/run_log_dashboard_public_link_capture.ps1.20260223_171948.gmailapi.bak` | 1 | 5.4 KiB | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0448 | P | `bat/run_log_dashboard_public_link_scheduled.bat` | 1 | 1.4 KiB | 2026-04-20 | dashboard launcher/configuration; scheduled task \EQIDV2_log_dashboard_start_0855 state=Ready |
| K-0449 | P | `bat/run_log_dashboard_public_link_scheduled.bat.20260223_165340.waba.bak` | 1 | 640 B | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0450 | P | `bat/run_log_dashboard_public_link_scheduled.bat.20260223_170023.gmail.bak` | 1 | 881 B | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0451 | P | `bat/run_log_dashboard_public_link_scheduled.bat.20260223_171948.gmailapi.bak` | 1 | 971 B | 2026-02-23 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0452 | P | `bat/run_log_dashboard_restart_keep_url.bat` | 1 | 1.8 KiB | 2026-02-25 | dashboard launcher/configuration; bat\run_log_dashboard_restart_keep_url.bat:12 launcher/asset reference |
| K-0453 | P | `bat/run_log_dashboard_server.bat` | 1 | 3.1 KiB | 2026-09-25 | dashboard launcher/configuration; running PID 28264 |
| K-0454 | P | `bat/run_log_dashboard_stop.bat` | 1 | 1.9 KiB | 2026-02-24 | dashboard launcher/configuration; scheduled task \EQIDV2_log_dashboard_stop_1700 state=Ready |
| K-0455 | P | `bat/run_moving_files_1545.bat` | 1 | 1.9 KiB | 2026-07-06 | scheduled task \EQIDV2_data_for_backtesting_1545 state=Ready; scheduled task \EQIDV2_data_for_backtesting_once_1232 state=Ready |
| K-0456 | P | `bat/run_nifty_guard_fetcher_v15.bat` | 1 | 3.1 KiB | 2026-03-16 | scheduled task \EQIDV2_nifty_guard_fetch_v15_0915 state=Disabled; preopen_session_autofix.py:31 literal reference |
| K-0457 | P | `bat/run_nifty_guard_fetcher_v16_5min.bat` | 1 | 5.7 KiB | 2026-06-13 | bat\run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat:4 launcher/asset reference |
| K-0458 | P | `bat/run_pf_supervisor_healthcheck.bat` | 1 | 674 B | 2026-04-27 | scheduled task \EQIDV2_pf_supervisor_healthcheck_0935 state=Ready |
| K-0459 | P | `bat/run_preopen_session_autofix.bat` | 1 | 1.1 KiB | 2026-02-26 | scheduled task \EQIDV2_preopen_session_autofix_0904 state=Ready |
| K-0460 | P | `bat/run_preopen_session_healthcheck.bat` | 1 | 779 B | 2026-02-25 | scheduled task \EQIDV2_preopen_session_healthcheck_0905 state=Ready; log_dashboard_server.py:823 literal reference |
| K-0461 | P | `bat/run_v11_lab_shadow_monitor.bat` | 1 | 1.5 KiB | 2026-07-10 | scheduled task \EQIDV2_v11_lab_shadow_monitor_1655 state=Disabled; log_dashboard_server.py:816 literal reference |
| K-0462 | P | `bat/run_v13_shadow_prepare.bat` | 1 | 1.5 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0463 | P | `bat/run_v13_shadow_seal.bat` | 1 | 1.5 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0464 | P | `bat/run_v13_strategy_research_refresh.bat` | 1 | 663 B | 2026-09-25 | G-family import/launcher dependency audit |
| K-0465 | P | `bat/run_v7_causality_audit.bat` | 1 | 785 B | 2026-06-10 | scheduled task \EQIDV2_v7_causality_audit_1605 state=Disabled; log_dashboard_server.py:809 literal reference |
| K-0466 | P | `bat/run_v7_full_pipeline_entry_research.bat` | 1 | 1.2 KiB | 2026-07-08 | scheduled task \EQIDV2_v7_full_pipeline_entry_research_1620 state=Disabled; log_dashboard_server.py:813 literal reference |
| K-0467 | P | `bat/run_v7_full_pipeline_entry_research_v2.bat` | 1 | 1.2 KiB | 2026-07-08 | scheduled task \EQIDV2_v7_full_pipeline_entry_research_v2_1630 state=Disabled; log_dashboard_server.py:814 literal reference |
| K-0468 | P | `bat/run_v7_gate_promotion_report.bat` | 1 | 1.3 KiB | 2026-06-11 | scheduled task \EQIDV2_v7_gate_promotion_1625 state=Disabled; log_dashboard_server.py:810 literal reference |
| K-0469 | P | `bat/run_v7_nse_id_cost_report.bat` | 1 | 1.0 KiB | 2026-06-10 | scheduled task \EQIDV2_v7_nse_id_cost_1605 state=Disabled; log_dashboard_server.py:807 literal reference |
| K-0470 | P | `bat/run_v7_qualification_report.bat` | 1 | 1.5 KiB | 2026-06-15 | scheduled task \EQIDV2_v7_qualification_1630 state=Disabled; log_dashboard_server.py:811 literal reference |
| K-0471 | P | `bat/run_v7_shadow_candidate_monitor.bat` | 1 | 1.1 KiB | 2026-07-08 | scheduled task \EQIDV2_v7_shadow_candidate_monitor_1645 state=Disabled; scheduled task \EQIDV2_v7_shadow_candidate_monitor_1650 state=Disabled |
| K-0472 | P | `bat/run_v7_walkforward_gate_report.bat` | 1 | 1.1 KiB | 2026-06-10 | scheduled task \EQIDV2_v7_walkforward_gate_1620 state=Disabled; log_dashboard_server.py:808 literal reference |
| K-0473 | P | `bat/run_zerodha_kite_export_scheduler.bat` | 1 | 2.2 KiB | 2026-02-25 | scheduled task \EQIDV2_kite_export_start_0915 state=Ready; log_dashboard_server.py:820 literal reference |
| K-0474 | P | `bat/run_zerodha_kite_export_stop.bat` | 1 | 1.0 KiB | 2026-02-25 | scheduled task \EQIDV2_kite_export_stop_1535 state=Ready |
| K-0475 | P | `bat/schedule_backtesting_result_v11_weekday.bat` | 1 | 171 B | 2026-09-14 | Referenced by a protected source/launcher; conservative static code closure |
| K-0476 | P | `bat/schedule_backtesting_result_v13_v10_g_weekday.bat` | 1 | 1.3 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0477 | P | `bat/schedule_eqidv2_kiteticker_5min_live_weekday.bat` | 1 | 1.5 KiB | 2026-07-31 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0478 | P | `bat/schedule_fno_oi_weekday.ps1` | 1 | 4.1 KiB | 2026-09-29 | Referenced by a protected source/launcher; conservative static code closure |
| K-0479 | P | `bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1` | 1 | 5.1 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0480 | P | `bat/schedule_fno_v13_v10_g_options_weekday.ps1` | 1 | 4.2 KiB | 2026-09-15 | G-family import/launcher dependency audit |
| K-0481 | P | `bat/schedule_fno_v6_live_kite_qty1_weekday.ps1` | 1 | 216 B | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0482 | P | `bat/schedule_log_dashboard_weekday.bat` | 1 | 1.9 KiB | 2026-05-28 | dashboard launcher/configuration |
| K-0483 | P | `bat/schedule_v13_shadow_weekday.ps1` | 1 | 4.3 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0484 | P | `bat/schedule_zerodha_kite_export_weekday.bat` | 1 | 1.6 KiB | 2026-03-17 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0485 | P | `bat/send_gmail_api.py` | 1 | 6.3 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0486 | P | `bat/stop_eqidv2_v15_stack.ps1` | 1 | 6.7 KiB | 2026-03-18 | bat\run_eqidv2_v15_stack_stop.bat:5 launcher/asset reference; bat\stop_eqidv2_v15_stack.ps1:50 launcher/asset reference |
| K-0487 | P | `bat/supervise_command.ps1` | 1 | 59.8 KiB | 2026-09-29 | G-family import/launcher dependency audit |
| K-0488 | P | `bat/switch_fno_v6_1m_to_v8_paper_after_approval.ps1` | 1 | 17.7 KiB | 2026-09-01 | fno_v8_combined_paper_control.py:74 literal reference |
| K-0489 | P | `build_v13_september_detail_report.py` | 1 | 9.5 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0490 | P | `build_v13_v10_g_document.py` | 1 | 7.7 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0491 | P | `collect_filtered_stock_data_session.py` | 1 | 6.3 KiB | 2026-08-13 | bat\run_collect_filtered_stock_data_session.bat:11 launcher/asset reference |
| K-0492 | P | `dashboard_flow.css` | 1 | 39.3 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:7413 literal reference |
| K-0493 | P | `dashboard_flow.html` | 1 | 10.3 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:7412 literal reference |
| K-0494 | P | `dashboard_flow.js` | 1 | 23.2 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:7414 literal reference |
| K-0495 | P | `dashboard_flow.py` | 1 | 15.1 KiB | 2026-10-10 | dashboard entry point; log_dashboard_server.py:7138 import |
| K-0496 | P | `dashboard_flow_chart.js` | 1 | 13.8 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:7415 literal reference |
| K-0497 | P | `dashboard_flow_compare.js` | 1 | 17.2 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:7417 literal reference |
| K-0498 | P | `dashboard_flow_core.js` | 1 | 3.2 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:7416 literal reference |
| K-0499 | P | `dashboard_fno_monitor.css` | 1 | 4.5 KiB | 2026-10-08 | dashboard asset; log_dashboard_server.py:11616 literal reference |
| K-0500 | P | `dashboard_fno_monitor.js` | 1 | 20.2 KiB | 2026-10-08 | dashboard asset; log_dashboard_server.py:11617 literal reference |
| K-0501 | P | `dashboard_ops_state.js` | 1 | 5.8 KiB | 2026-10-10 | dashboard asset; log_dashboard_server.py:11618 literal reference |
| K-0502 | P | `DASHBOARD_SESSION_READINESS_AUDIT_2026-09-02.md` | 1 | 32.2 KiB | 2026-09-01 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0503 | P | `data_for_backtesting_verify.py` | 1 | 13.7 KiB | 2026-09-01 | G-family import/launcher dependency audit |
| K-0504 | P | `E_rejected_salvage.py` | 1 | 7.6 KiB | 2026-06-11 | Referenced by a protected source/launcher; conservative static code closure |
| K-0505 | P | `eqidv2_conf_tier_c_live_scan.py` | 1 | 10.8 KiB | 2026-06-15 | G-family import/launcher dependency audit |
| K-0506 | P | `eqidv2_config_attestation.py` | 1 | 10.2 KiB | 2026-04-22 | G-family import/launcher dependency audit |
| K-0507 | P | `eqidv2_decision_funnel.py` | 1 | 3.8 KiB | 2026-07-28 | G-family import/launcher dependency audit |
| K-0508 | P | `eqidv2_detection_engine_v16_5min.py` | 1 | 116.7 KiB | 2026-05-04 | bat\run_eqidv2_detection_engine_v16_5min.bat:39 launcher/asset reference; eqidv2_detection_engine_v16_5min.py:155 literal reference |
| K-0509 | P | `eqidv2_early_emit_v16_5min.py` | 1 | 24.4 KiB | 2026-04-22 | eqidv2_signal_early_engine_v16_5min.py:91 import |
| K-0510 | P | `eqidv2_entry_engine_1min_v5_id.py` | 1 | 111.4 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0511 | P | `eqidv2_eod_scheduler_for_1540_update.py` | 1 | 6.2 KiB | 2026-02-13 | bat\run_eqidv2_eod_scheduler_for_1540_update.bat:9 launcher/asset reference; eqidv2_eod_scheduler_for_1540_update.py:33 resolved static path |
| K-0512 | P | `eqidv2_eod_scheduler_for_15mins_data.py` | 1 | 23.6 KiB | 2026-03-16 | bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_v15.bat:3 launcher/asset reference; bat\run_eqidv2_eod_scheduler_for_15mins_data_live_minimal.bat:3 launcher/asset reference |
| K-0513 | P | `eqidv2_eod_scheduler_for_15mins_data_live_minimal.py` | 1 | 37.6 KiB | 2026-03-25 | bat\run_eqidv2_eod_scheduler_for_15mins_data_live_minimal.bat:13 launcher/asset reference; eqidv2_eod_scheduler_for_15mins_data_live_minimal.py:57 resolved static path |
| K-0514 | P | `eqidv2_eod_scheduler_for_1min_data_live.py` | 1 | 18.5 KiB | 2026-08-31 | bat\run_eqidv2_eod_scheduler_for_1min_data_live.bat:32 launcher/asset reference; eqidv2_eod_scheduler_for_1min_data_live.py:52 resolved static path |
| K-0515 | P | `eqidv2_eod_scheduler_for_5mins_data_live_minimal.py` | 1 | 105.5 KiB | 2026-08-31 | G-family import/launcher dependency audit |
| K-0516 | P | `eqidv2_final_conf_live_bootstrap.py` | 1 | 13.8 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0517 | P | `eqidv2_kiteticker_5min_live.py` | 1 | 51.4 KiB | 2026-07-31 | bat\run_eqidv2_kiteticker_5min_live.bat:32 launcher/asset reference; eqidv2_kiteticker_5min_live.py:796 resolved static path |
| K-0518 | P | `eqidv2_late_bb10_compression.py` | 1 | 12.6 KiB | 2026-08-06 | G-family import/launcher dependency audit |
| K-0519 | P | `eqidv2_live_combined_analyser_csv.py` | 1 | 85.3 KiB | 2026-02-26 | G-family import/launcher dependency audit |
| K-0520 | P | `eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.py` | 1 | 31.4 KiB | 2026-06-09 | G-family import/launcher dependency audit |
| K-0521 | P | `eqidv2_live_combined_analyser_csv_v15.py` | 1 | 122.1 KiB | 2026-07-28 | G-family import/launcher dependency audit |
| K-0522 | P | `eqidv2_live_combined_analyser_csv_v15_long.py` | 1 | 20.7 KiB | 2026-03-24 | G-family import/launcher dependency audit |
| K-0523 | P | `eqidv2_live_combined_analyser_csv_v15_new.py` | 1 | 12.0 KiB | 2026-03-23 | G-family import/launcher dependency audit |
| K-0524 | P | `eqidv2_live_combined_analyser_csv_v15_new_persistent.py` | 1 | 20.2 KiB | 2026-03-25 | bat\run_eqidv2_live_combined_analyser_csv_v15_new_persistent.bat:29 launcher/asset reference |
| K-0525 | P | `eqidv2_live_combined_analyser_csv_v15_short.py` | 1 | 22.0 KiB | 2026-03-23 | G-family import/launcher dependency audit |
| K-0526 | P | `eqidv2_live_combined_analyser_csv_v15_short_sharded.py` | 1 | 4.6 KiB | 2026-03-17 | G-family import/launcher dependency audit |
| K-0527 | P | `eqidv2_live_combined_analyser_csv_v16_5min.py` | 1 | 68.4 KiB | 2026-05-05 | G-family import/launcher dependency audit |
| K-0528 | P | `eqidv2_live_combined_analyser_csv_v2.py` | 1 | 92.5 KiB | 2026-03-16 | G-family import/launcher dependency audit |
| K-0529 | P | `eqidv2_live_combined_analyser_csv_v4_long.py` | 1 | 14.9 KiB | 2026-02-26 | bat\run_eqidv2_live_combined_analyser_csv_v4_long.bat:8 launcher/asset reference; eqidv2_live_combined_analyser_csv_v4_long.py:48 resolved static path |
| K-0530 | P | `eqidv2_live_combined_analyser_csv_v4_short.py` | 1 | 5.3 KiB | 2026-02-26 | bat\run_eqidv2_live_combined_analyser_csv_v4_short.bat:8 launcher/asset reference; eqidv2_live_combined_analyser_csv_v4_short.py:27 resolved static path |
| K-0531 | P | `eqidv2_live_combined_analyser_csv_v5_long.py` | 1 | 23.2 KiB | 2026-03-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0532 | P | `eqidv2_live_combined_analyser_csv_v5_short.py` | 1 | 8.9 KiB | 2026-03-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0533 | P | `eqidv2_live_combined_analyser_csv_v5_unified.py` | 1 | 32.1 KiB | 2026-03-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0534 | P | `eqidv2_live_combined_analyser_csv_v7_sweep_long.py` | 1 | 26.2 KiB | 2026-03-16 | G-family import/launcher dependency audit |
| K-0535 | P | `eqidv2_live_combined_analyser_csv_v7_sweep_short.py` | 1 | 11.8 KiB | 2026-03-16 | G-family import/launcher dependency audit |
| K-0536 | P | `eqidv2_live_signal_writer.py` | 1 | 13.5 KiB | 2026-07-28 | G-family import/launcher dependency audit |
| K-0537 | P | `eqidv2_meta_label_triple_barrier.py` | 1 | 12.9 KiB | 2026-02-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0538 | P | `eqidv2_meta_train_walkforward.py` | 1 | 11.2 KiB | 2026-02-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0539 | P | `eqidv2_nifty_guard_fetcher_supervised_v16_5min.py` | 1 | 13.2 KiB | 2026-06-13 | bat\run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat:41 launcher/asset reference; eqidv2_nifty_guard_fetcher_supervised_v16_5min.py:40 module-name reference |
| K-0540 | P | `eqidv2_pending_data_fetcher_v16_5min.py` | 1 | 60.8 KiB | 2026-04-28 | bat\run_eqidv2_pending_data_fetcher_v16_5min.bat:64 launcher/asset reference; eqidv2_pending_data_fetcher_v16_5min.py:96 literal reference |
| K-0541 | P | `eqidv2_pool_lock.py` | 1 | 5.1 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0542 | P | `eqidv2_pre_momentum.py` | 1 | 10.1 KiB | 2026-07-28 | G-family import/launcher dependency audit |
| K-0543 | P | `eqidv2_risk_brake.py` | 1 | 5.5 KiB | 2026-06-15 | Referenced by a protected source/launcher; conservative static code closure |
| K-0544 | P | `eqidv2_runtime_manifest.py` | 1 | 5.8 KiB | 2026-07-28 | G-family import/launcher dependency audit |
| K-0545 | P | `eqidv2_runtime_paths.py` | 1 | 3.3 KiB | 2026-05-16 | G-family import/launcher dependency audit |
| K-0546 | P | `eqidv2_setup_conf_loader.py` | 1 | 2.8 KiB | 2026-07-26 | G-family import/launcher dependency audit |
| K-0547 | P | `eqidv2_signal_discovery_v7_5min_id_persistent.py` | 1 | 91.3 KiB | 2026-07-31 | G-family import/launcher dependency audit |
| K-0548 | P | `eqidv2_signal_early_engine_v16_5min.py` | 1 | 25.7 KiB | 2026-04-24 | bat\run_eqidv2_signal_early_engine_v16_5min.bat:20 launcher/asset reference; eqidv2_signal_early_engine_v16_5min.py:97 literal reference |
| K-0549 | P | `eqidv2_signal_engine_v16_5min.py` | 1 | 74.7 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0550 | P | `eqidv2_signal_id_selfcheck_v16_5min.py` | 1 | 3.9 KiB | 2026-04-20 | G-family import/launcher dependency audit |
| K-0551 | P | `eqidv2_v11_live_overlay.py` | 1 | 50.1 KiB | 2026-08-06 | G-family import/launcher dependency audit |
| K-0552 | P | `eqidv2_v7_position_sizing.py` | 1 | 2.7 KiB | 2026-07-28 | G-family import/launcher dependency audit |
| K-0553 | P | `eqidv2_v7_signal_contract.py` | 1 | 6.2 KiB | 2026-06-08 | G-family import/launcher dependency audit |
| K-0554 | P | `fetch_india_vix.py` | 1 | 2.5 KiB | 2026-03-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0555 | P | `filtered_fno_MIS_v2.py` | 1 | 4.5 KiB | 2026-08-08 | G-family import/launcher dependency audit |
| K-0556 | P | `filtered_stocks.py` | 1 | 6.4 KiB | 2026-02-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0557 | P | `filtered_stocks_MIS.py` | 1 | 20.6 KiB | 2026-04-24 | G-family import/launcher dependency audit |
| K-0558 | P | `filtered_stocks_MIS_v2.py` | 1 | 20.6 KiB | 2026-07-31 | G-family import/launcher dependency audit |
| K-0559 | P | `filtered_stocks_NSE_futures_only.py` | 1 | 3.4 KiB | 2026-02-27 | G-family import/launcher dependency audit |
| K-0560 | P | `filtered_stocks_NSE_stock_intraday_5x.py` | 1 | 23.2 KiB | 2026-04-24 | Referenced by a protected source/launcher; conservative static code closure |
| K-0561 | P | `filtered_stocks_NSE_stock_intraday_5x_price_ge_25.py` | 1 | 22.5 KiB | 2026-04-24 | Referenced by a protected source/launcher; conservative static code closure |
| K-0562 | P | `filtered_stocks_NSE_stock_universe.py` | 1 | 47.4 KiB | 2026-04-24 | Referenced by a protected source/launcher; conservative static code closure |
| K-0563 | P | `final_setup_conf.py` | 1 | 146.1 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0564 | P | `final_setup_conf_v11_conf_d.py` | 1 | 2.1 KiB | 2026-07-10 | v11_lab_shadow_monitor.py:41 module-name reference |
| K-0565 | P | `final_setup_conf_v11_lab_prune2.py` | 1 | 139.4 KiB | 2026-07-10 | final_setup_conf_v11_conf_d.py:13 import |
| K-0566 | P | `final_setup_conf_v11_working.py` | 1 | 131.9 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0567 | P | `final_setup_conf_v12.py` | 1 | 135.8 KiB | 2026-08-06 | Referenced by a protected source/launcher; conservative static code closure |
| K-0568 | P | `final_setup_conf_v12_base.py` | 1 | 138.0 KiB | 2026-08-06 | Referenced by a protected source/launcher; conservative static code closure |
| K-0569 | P | `fno_eq_id_monitor_detail.py` | 1 | 51.3 KiB | 2026-10-08 | log_dashboard_server.py:224 import; fno_eq_id_monitor_detail.py:109 resolved static path |
| K-0570 | P | `fno_equity_1m_backfill.py` | 1 | 10.4 KiB | 2026-08-26 | bat\run_fno_today_data_repair_20260831.bat:38 launcher/asset reference; fno_equity_1m_backfill.py:126 module-name reference |
| K-0571 | P | `fno_equity_5m_repair.py` | 1 | 14.5 KiB | 2026-08-11 | bat\run_fno_today_data_repair_20260831.bat:42 launcher/asset reference |
| K-0572 | P | `fno_equity_fetch_1min.py` | 1 | 54.1 KiB | 2026-09-29 | G-family import/launcher dependency audit |
| K-0573 | P | `fno_live_evidence.py` | 1 | 8.4 KiB | 2026-08-17 | G-family import/launcher dependency audit |
| K-0574 | P | `fno_live_profile.py` | 1 | 861 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0575 | P | `fno_multi_paper_engine.py` | 1 | 26.9 KiB | 2026-08-31 | fno_multi_paper_session.py:28 import; fno_multi_paper_session.py:559 literal reference |
| K-0576 | P | `fno_multi_paper_live_source.py` | 1 | 76.9 KiB | 2026-09-15 | fno_multi_paper_session.py:803 import; fno_multi_paper_session.py:767 import |
| K-0577 | P | `fno_multi_paper_parity.py` | 1 | 4.4 KiB | 2026-08-31 | fno_multi_paper_session.py:31 import; fno_multi_paper_report.py:11 import |
| K-0578 | P | `fno_multi_paper_profiles.py` | 1 | 10.8 KiB | 2026-08-31 | fno_multi_paper_session.py:29 import; fno_multi_paper_session.py:558 literal reference |
| K-0579 | P | `fno_multi_paper_report.py` | 1 | 13.1 KiB | 2026-08-31 | fno_multi_paper_session.py:30 import; fno_multi_paper_session.py:560 literal reference |
| K-0580 | P | `fno_multi_paper_session.py` | 1 | 45.8 KiB | 2026-09-01 | bat\run_fno_v10_v11_v12_paper_session.bat:6 launcher/asset reference; fno_multi_paper_session.py:561 literal reference |
| K-0581 | P | `fno_oi_backfill_5min.py` | 1 | 20.5 KiB | 2026-08-26 | bat\run_fno_5min_rankings_backfill.ps1:44 launcher/asset reference; bat\run_fno_today_data_repair_20260831.bat:46 launcher/asset reference |
| K-0582 | P | `fno_oi_backfill_daily.py` | 1 | 27.7 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0583 | P | `fno_oi_backtest_provenance.py` | 1 | 36.1 KiB | 2026-08-18 | G-family import/launcher dependency audit |
| K-0584 | P | `fno_oi_common.py` | 1 | 30.6 KiB | 2026-09-24 | G-family import/launcher dependency audit |
| K-0585 | P | `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py` | 1 | 61.0 KiB | 2026-08-11 | G-family import/launcher dependency audit |
| K-0586 | P | `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py` | 1 | 24.3 KiB | 2026-08-17 | G-family import/launcher dependency audit |
| K-0587 | P | `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7.py` | 1 | 17.5 KiB | 2026-08-18 | fno_v8_windowed_1m_entry_backtest.py:191 module-name reference |
| K-0588 | P | `fno_oi_ema_confirm_0925_0930_0935_v4.py` | 1 | 42.8 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0589 | P | `fno_oi_ema_confirm_0925_0930_best_combo_v3.py` | 1 | 29.3 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0590 | P | `fno_oi_ema_confirm_0925_0930_pf_v3.py` | 1 | 46.1 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0591 | P | `fno_oi_ema_confirm_0925_0930_short2_expansion_v3.py` | 1 | 29.2 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0592 | P | `fno_oi_ema_confirm_0925_best_combo.py` | 1 | 12.2 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0593 | P | `fno_oi_ema_confirm_0925_best_combo_v2.py` | 1 | 14.8 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0594 | P | `fno_oi_ema_confirm_0925_long2_expansion_v2.py` | 1 | 24.0 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0595 | P | `fno_oi_ema_confirm_0925_pf.py` | 1 | 33.9 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0596 | P | `fno_oi_ema_confirm_0925_pf_v2.py` | 1 | 44.1 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0597 | P | `fno_oi_ema_confirm_0925_short_expansion_v2.py` | 1 | 14.8 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0598 | P | `fno_oi_ema_confirm_0925_top5_pf.py` | 1 | 33.9 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0599 | P | `fno_oi_ema_confirm_backtest.py` | 1 | 24.3 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0600 | P | `fno_oi_ema_confirm_fullrun.py` | 1 | 16.6 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0601 | P | `fno_oi_ema_confirm_optimize.py` | 1 | 28.9 KiB | 2026-08-20 | G-family import/launcher dependency audit |
| K-0602 | P | `fno_oi_ema_confirm_sweep.py` | 1 | 29.9 KiB | 2026-08-20 | G-family import/launcher dependency audit |
| K-0603 | P | `fno_oi_ema_confirm_v7_signal_cache.py` | 1 | 11.0 KiB | 2026-08-20 | fno_v8_windowed_1m_entry_backtest.py:195 module-name reference; fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7.py:32 import |
| K-0604 | P | `fno_oi_ema_confirm_v9_extended_slots.py` | 1 | 20.3 KiB | 2026-08-20 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0605 | P | `fno_oi_eod_qc.py` | 1 | 10.9 KiB | 2026-08-10 | log_dashboard_server.py:332 module-name reference; log_dashboard_server.py:368 module-name reference |
| K-0606 | P | `fno_oi_feature_ranker.py` | 1 | 27.8 KiB | 2026-09-14 | log_dashboard_server.py:315 module-name reference; log_dashboard_server.py:351 module-name reference |
| K-0607 | P | `fno_oi_fetch_1m_history.py` | 1 | 8.8 KiB | 2026-08-10 | bat\run_fno_today_data_repair_20260831.bat:50 launcher/asset reference; fno_oi_fetch_1m_history.py:31 module-name reference |
| K-0608 | P | `fno_oi_fetch_5min.py` | 1 | 37.3 KiB | 2026-09-24 | G-family import/launcher dependency audit |
| K-0609 | P | `fno_oi_fetch_5min_fast_production.py` | 1 | 28.9 KiB | 2026-09-29 | G-family import/launcher dependency audit |
| K-0610 | P | `fno_oi_fetch_5min_fast_shadow.py` | 1 | 91.0 KiB | 2026-09-04 | G-family import/launcher dependency audit |
| K-0611 | P | `fno_oi_hybrid_data.py` | 1 | 18.9 KiB | 2026-08-18 | G-family import/launcher dependency audit |
| K-0612 | P | `fno_oi_intraday_strategies.py` | 1 | 22.2 KiB | 2026-08-10 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0613 | P | `fno_oi_rank_history.py` | 1 | 28.5 KiB | 2026-08-10 | bat\run_fno_5min_rankings_backfill.ps1:56 launcher/asset reference; bat\run_fno_5min_rankings_backfill.ps1:67 launcher/asset reference |
| K-0614 | P | `fno_oi_universe.py` | 1 | 7.9 KiB | 2026-08-11 | G-family import/launcher dependency audit |
| K-0615 | P | `fno_options_atm_fetch_5min.py` | 1 | 48.2 KiB | 2026-09-16 | G-family import/launcher dependency audit |
| K-0616 | P | `FNO_PAPERTRADE_DASHBOARD_OPERATIONS.html` | 1 | 54.5 KiB | 2026-08-31 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0617 | P | `FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md` | 1 | 30.7 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0618 | P | `fno_rollover_diagnostic_config.py` | 1 | 3.2 KiB | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0619 | P | `FNO_TODAY_2026-09-03_V6_V10_V12_V13V2.md` | 1 | 5.5 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0620 | P | `fno_v10_backtest.py` | 1 | 57.4 KiB | 2026-08-30 | fno_multi_paper_parity.py:21 import; fno_v10_backtest.py:1461 resolved static path |
| K-0621 | P | `fno_v10_backtest_config.py` | 1 | 7.6 KiB | 2026-08-27 | fno_multi_paper_parity.py:22 import; fno_v10_backtest.py:28 import |
| K-0622 | P | `fno_v10_experiment_backtest.py` | 1 | 51.0 KiB | 2026-08-26 | fno_v10_backtest.py:29 import; fno_v10_followup_challenger_research.py:31 import |
| K-0623 | P | `fno_v10_experiment_compare.py` | 1 | 22.8 KiB | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0624 | P | `fno_v10_experiment_config.py` | 1 | 7.8 KiB | 2026-08-26 | fno_v10_backtest_config.py:7 import; fno_v10_gap_guard_research.py:45 import |
| K-0625 | P | `fno_v10_experiment_stage_sequence.json` | 1 | 4.6 KiB | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0626 | P | `fno_v10_followup_challenger_research.py` | 1 | 44.6 KiB | 2026-08-27 | fno_multi_paper_parity.py:23 import; fno_v10_backtest.py:30 import |
| K-0627 | P | `fno_v10_full_historical_report.py` | 1 | 64.3 KiB | 2026-08-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0628 | P | `fno_v10_gap_guard_research.py` | 1 | 39.9 KiB | 2026-08-27 | fno_multi_paper_parity.py:24 import; fno_v10_backtest.py:31 import |
| K-0629 | P | `fno_v10_recent_detailed_report.py` | 1 | 35.0 KiB | 2026-08-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0630 | P | `fno_v10_repaired_snapshot_rerun.py` | 1 | 34.8 KiB | 2026-08-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0631 | P | `fno_v10_stage10_cost_stress_plan.json` | 1 | 7.2 KiB | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0632 | P | `fno_v10_stage7_postselection_combo_research.py` | 1 | 26.8 KiB | 2026-08-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0633 | P | `fno_v10_unified_5m_1m_backtest.py` | 1 | 15.6 KiB | 2026-08-27 | fno_multi_paper_parity.py:25 import; fno_v10_unified_5m_1m_backtest.py:95 resolved static path |
| K-0634 | P | `fno_v10_unified_rollover_diagnostic.py` | 1 | 950 B | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0635 | P | `fno_v11_backtest.py` | 1 | 40.3 KiB | 2026-08-30 | fno_multi_paper_parity.py:26 import; fno_v11_backtest.py:1021 literal reference |
| K-0636 | P | `fno_v11_execution_runtime.py` | 1 | 25.1 KiB | 2026-08-30 | fno_v11_backtest.py:41 import; fno_v12_backtest.py:37 import |
| K-0637 | P | `fno_v11_full_historical_report.py` | 1 | 114.6 KiB | 2026-08-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0638 | P | `fno_v11_gap_runtime.py` | 1 | 5.4 KiB | 2026-08-30 | fno_v11_backtest.py:42 import; fno_v12_backtest.py:38 import |
| K-0639 | P | `fno_v11_selection_runtime.py` | 1 | 19.0 KiB | 2026-08-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0640 | P | `fno_v11_staged_backtest.py` | 1 | 113.6 KiB | 2026-08-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0641 | P | `fno_v11_variant_registry.py` | 1 | 21.9 KiB | 2026-08-30 | Referenced by a protected source/launcher; conservative static code closure |
| K-0642 | P | `fno_v12_analysis.py` | 1 | 44.7 KiB | 2026-08-30 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0643 | P | `fno_v12_backtest.py` | 1 | 45.0 KiB | 2026-08-30 | fno_multi_paper_parity.py:27 import; fno_v12_backtest.py:1039 literal reference |
| K-0644 | P | `fno_v12_corrected_backtest.py` | 1 | 21.3 KiB | 2026-09-04 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0645 | P | `fno_v12_execution_runtime.py` | 1 | 21.1 KiB | 2026-08-30 | fno_v12_backtest.py:39 import |
| K-0646 | P | `fno_v12_full_historical_report.py` | 1 | 87.2 KiB | 2026-08-30 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0647 | P | `fno_v12_resources.py` | 1 | 13.8 KiB | 2026-08-30 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0648 | P | `fno_v12_selection_runtime.py` | 1 | 26.1 KiB | 2026-08-30 | fno_v12_backtest.py:40 import |
| K-0649 | P | `fno_v12_staged_backtest.py` | 1 | 68.4 KiB | 2026-08-30 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0650 | P | `fno_v12_variant_registry.py` | 1 | 32.4 KiB | 2026-08-30 | fno_v12_backtest.py:41 import; fno_v12_selection_runtime.py:21 import |
| K-0651 | P | `fno_v13_corrected_backtest.py` | 1 | 15.4 KiB | 2026-09-03 | G-family import/launcher dependency audit |
| K-0652 | P | `fno_v13_corrected_ungated_oi_research.py` | 1 | 25.8 KiB | 2026-09-03 | G-family import/launcher dependency audit |
| K-0653 | P | `fno_v13_corrected_v2_backtest.py` | 1 | 49.2 KiB | 2026-09-03 | G-family import/launcher dependency audit |
| K-0654 | P | `fno_v13_corrected_v2_late_slot_research.py` | 1 | 26.0 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0655 | P | `fno_v13_corrected_v3_backtest.py` | 1 | 36.9 KiB | 2026-09-03 | G-family import/launcher dependency audit |
| K-0656 | P | `FNO_V13_CORRECTED_V3_DETAILED_RESULTS.md` | 1 | 35.8 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0657 | P | `fno_v13_corrected_v4_backtest.py` | 1 | 20.6 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0658 | P | `FNO_V13_CORRECTED_V4_DETAILED_RESULTS.md` | 1 | 33.0 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0659 | P | `fno_v13_corrected_v5_backtest.py` | 1 | 117.6 KiB | 2026-09-04 | G-family import/launcher dependency audit |
| K-0660 | P | `FNO_V13_CORRECTED_V5_DETAILED_RESULTS.md` | 1 | 44.8 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0661 | P | `fno_v13_run_vintage.py` | 1 | 10.7 KiB | 2026-09-09 | log_dashboard_server.py:41 import |
| K-0662 | P | `fno_v13_v10_a_backtest.py` | 1 | 2.7 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0663 | P | `fno_v13_v10_a_research.py` | 1 | 21.2 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0664 | P | `fno_v13_v10_a_review.py` | 1 | 4.2 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0665 | P | `fno_v13_v10_b_backtest.py` | 1 | 3.9 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0666 | P | `fno_v13_v10_b_research.py` | 1 | 5.8 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0667 | P | `fno_v13_v10_backtest.py` | 1 | 10.1 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0668 | P | `fno_v13_v10_balanced_research.py` | 1 | 8.5 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0669 | P | `fno_v13_v10_c_backtest.py` | 1 | 2.3 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0670 | P | `fno_v13_v10_c_research.py` | 1 | 5.1 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0671 | P | `fno_v13_v10_d_backtest.py` | 1 | 3.9 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0672 | P | `fno_v13_v10_d_research.py` | 1 | 5.9 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0673 | P | `fno_v13_v10_e_backtest.py` | 1 | 3.2 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0674 | P | `fno_v13_v10_e_research.py` | 1 | 5.4 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0675 | P | `fno_v13_v10_f_backtest.py` | 1 | 10.1 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0676 | P | `fno_v13_v10_f_capital_replay.py` | 1 | 7.5 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0677 | P | `fno_v13_v10_f_research.py` | 1 | 10.9 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0678 | P | `fno_v13_v10_g_2_0925_comparison.py` | 1 | 16.9 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0679 | P | `fno_v13_v10_g_2_backtest.py` | 1 | 39.7 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0680 | P | `fno_v13_v10_g_2_extend_results.py` | 1 | 38.6 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0681 | P | `fno_v13_v10_g_2_long_leaders_backtest.py` | 1 | 20.4 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0682 | P | `fno_v13_v10_g_2_relaxed_0925_backtest.py` | 1 | 11.7 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0683 | P | `fno_v13_v10_g_2_unused_slots_data.py` | 1 | 21.3 KiB | 2026-10-06 | G-family import/launcher dependency audit |
| K-0684 | P | `fno_v13_v10_g_2_unused_slots_research.py` | 1 | 29.9 KiB | 2026-10-06 | G-family import/launcher dependency audit |
| K-0685 | P | `fno_v13_v10_g_3_backtest.py` | 1 | 1.8 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0686 | P | `fno_v13_v10_g_3l_capital_replay.py` | 1 | 9.8 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0687 | P | `fno_v13_v10_g_backtest.py` | 1 | 20.0 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0688 | P | `fno_v13_v10_g_daily_replay.py` | 1 | 59.7 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0689 | P | `fno_v13_v10_g_expansion_research.py` | 1 | 20.7 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0690 | P | `fno_v13_v10_g_identity.py` | 1 | 2.5 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0691 | P | `fno_v13_v10_g_live.py` | 1 | 438 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0692 | P | `fno_v13_v10_g_live_config.py` | 1 | 26.8 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0693 | P | `fno_v13_v10_g_live_kite_session.py` | 1 | 558 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0694 | P | `fno_v13_v10_g_options_backtest.py` | 1 | 27.9 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0695 | P | `fno_v13_v10_g_options_chart.py` | 1 | 8.3 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0696 | P | `fno_v13_v10_g_options_config.py` | 1 | 151 B | 2026-09-15 | G-family import/launcher dependency audit |
| K-0697 | P | `fno_v13_v10_g_options_data.py` | 1 | 21.0 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0698 | P | `fno_v13_v10_g_options_execution.py` | 1 | 19.5 KiB | 2026-09-15 | G-family import/launcher dependency audit |
| K-0699 | P | `fno_v13_v10_g_options_fetch.py` | 1 | 4.5 KiB | 2026-09-15 | G-family import/launcher dependency audit |
| K-0700 | P | `fno_v13_v10_g_options_fixed_profile.py` | 1 | 12.3 KiB | 2026-09-15 | G-family import/launcher dependency audit |
| K-0701 | P | `fno_v13_v10_g_options_html.py` | 1 | 8.0 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0702 | P | `fno_v13_v10_g_options_one_lot_execution.py` | 1 | 19.4 KiB | 2026-09-15 | G-family import/launcher dependency audit |
| K-0703 | P | `fno_v13_v10_g_options_paper.py` | 1 | 41.0 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0704 | P | `fno_v13_v10_g_options_projection.py` | 1 | 36.1 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0705 | P | `fno_v13_v10_g_options_signals.py` | 1 | 9.2 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0706 | P | `fno_v13_v10_g_paper.py` | 1 | 9.4 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0707 | P | `fno_v13_v10_g_policy.py` | 1 | 1.4 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0708 | P | `fno_v13_v10_g_projection_chart.py` | 1 | 39.1 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0709 | P | `fno_v13_v10_g_research.py` | 1 | 21.6 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0710 | P | `fno_v13_v10_g_selection.py` | 1 | 5.4 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0711 | P | `fno_v13_v10_g_staged_replay.py` | 1 | 4.1 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0712 | P | `fno_v13_v10_g_two_bar.py` | 1 | 13.1 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0713 | P | `fno_v13_v10_h_backtest.py` | 1 | 3.1 KiB | 2026-09-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0714 | P | `fno_v13_v10_research.py` | 1 | 17.9 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0715 | P | `fno_v13_v10_review.py` | 1 | 7.4 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0716 | P | `FNO_V13_V2_INDICATOR_ENTRY_RESEARCH_REPORT.md` | 1 | 21.3 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0717 | P | `fno_v13_v2_parameter_robustness_research.py` | 1 | 36.8 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0718 | P | `fno_v13_v3_exit_sweep.py` | 1 | 15.7 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0719 | P | `FNO_V13_V3_EXIT_SWEEP_RESULTS.md` | 1 | 11.2 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0720 | P | `fno_v13_v3_scaleout_sweep.py` | 1 | 15.4 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0721 | P | `FNO_V13_V3_TARGET_HIT_OVER_50_RESULTS.md` | 1 | 8.3 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0722 | P | `fno_v13_v5_derivative_data.py` | 1 | 56.0 KiB | 2026-09-04 | G-family import/launcher dependency audit |
| K-0723 | P | `fno_v13_v5_options_backtest.py` | 1 | 38.0 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0724 | P | `fno_v13_v5_options_sl_target_optimization.py` | 1 | 11.4 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0725 | P | `fno_v13_v5_research.py` | 1 | 29.5 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0726 | P | `FNO_V13_V5_RESEARCH_AUDIT.md` | 1 | 7.4 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0727 | P | `fno_v13_v6_option_liquidity_selector.py` | 1 | 16.8 KiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0728 | P | `fno_v13_v6_options_backtest.py` | 1 | 45.5 KiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0729 | P | `fno_v13_v6_portfolio_backtest.py` | 1 | 15.3 KiB | 2026-09-09 | G-family import/launcher dependency audit |
| K-0730 | P | `fno_v13_v6_research.py` | 1 | 11.1 KiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0731 | P | `fno_v13_v7_exit_shadow.py` | 1 | 16.7 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0732 | P | `fno_v13_v8_feature_shadow.py` | 1 | 17.4 KiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0733 | P | `fno_v13_v9_backtest.py` | 1 | 22.9 KiB | 2026-09-13 | G-family import/launcher dependency audit |
| K-0734 | P | `fno_v13_v9_data.py` | 1 | 29.9 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0735 | P | `fno_v13_v9_diagnostics.py` | 1 | 34.3 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0736 | P | `fno_v13_v9_research.py` | 1 | 31.6 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0737 | P | `fno_v5_0926_all_history_day_pf_optimize.py` | 1 | 16.6 KiB | 2026-08-13 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0738 | P | `fno_v5_0926_day_pf_optimize.py` | 1 | 25.5 KiB | 2026-08-13 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0739 | P | `fno_v5_hybrid_backtest.py` | 1 | 12.6 KiB | 2026-08-11 | G-family import/launcher dependency audit |
| K-0740 | P | `fno_v5_hybrid_optimize.py` | 1 | 45.3 KiB | 2026-08-11 | G-family import/launcher dependency audit |
| K-0741 | P | `fno_v5_live.py` | 1 | 229.6 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0742 | P | `fno_v5_live_config.py` | 1 | 17.4 KiB | 2026-08-17 | G-family import/launcher dependency audit |
| K-0743 | P | `fno_v6_corrected_backtest.py` | 1 | 30.4 KiB | 2026-09-02 | G-family import/launcher dependency audit |
| K-0744 | P | `fno_v6_corrected_oi_threshold_research.py` | 1 | 44.8 KiB | 2026-09-03 | Referenced by a protected source/launcher; conservative static code closure |
| K-0745 | P | `fno_v6_derivative_backtest.py` | 1 | 50.5 KiB | 2026-09-01 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0746 | P | `fno_v6_derivative_comparison.py` | 1 | 7.7 KiB | 2026-09-01 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0747 | P | `fno_v6_f_backtesting.py` | 1 | 2.4 KiB | 2026-09-01 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0748 | P | `fno_v6_last_10_rollover_diagnostic.py` | 1 | 14.5 KiB | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0749 | P | `fno_v6_live.py` | 1 | 406 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0750 | P | `fno_v6_live_config.py` | 1 | 23.6 KiB | 2026-08-17 | G-family import/launcher dependency audit |
| K-0751 | P | `fno_v6_live_kite_session.py` | 1 | 34.9 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0752 | P | `fno_v6_o_backtesting.py` | 1 | 2.3 KiB | 2026-09-01 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0753 | P | `fno_v6_parity_replay.py` | 1 | 49.5 KiB | 2026-08-17 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0754 | P | `fno_v6_v2_corrected_backtest.py` | 1 | 25.4 KiB | 2026-09-03 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0755 | P | `fno_v8_combined_best_per_leg_backtest.py` | 1 | 5.7 KiB | 2026-08-20 | fno_v8_v9_last_10_backtest.py:387 literal reference; fno_v8_v9_last_10_backtest.py:420 literal reference |
| K-0756 | P | `fno_v8_combined_paper_config.py` | 1 | 14.2 KiB | 2026-08-21 | fno_v8_combined_paper_session.py:46 import; fno_v8_combined_paper_control.py:30 import |
| K-0757 | P | `fno_v8_combined_paper_control.py` | 1 | 38.7 KiB | 2026-08-21 | fno_v8_combined_paper_session.py:47 import; preopen_session_healthcheck.py:187 import |
| K-0758 | P | `fno_v8_combined_paper_engine.py` | 1 | 74.5 KiB | 2026-08-21 | fno_v8_combined_paper_session.py:2731 module-name reference; fno_multi_paper_engine.py:18 import |
| K-0759 | P | `fno_v8_combined_paper_market_data.py` | 1 | 46.1 KiB | 2026-08-31 | fno_multi_paper_session.py:32 import; fno_v8_combined_paper_session.py:48 import |
| K-0760 | P | `fno_v8_combined_paper_session.py` | 1 | 220.3 KiB | 2026-09-03 | bat\run_fno_v8_combined_paper_session.bat:14 launcher/asset reference; fno_multi_paper_session.py:629 import |
| K-0761 | P | `fno_v8_combined_rollover_diagnostic.py` | 1 | 1,003 B | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0762 | P | `fno_v8_setup_param_sweep.py` | 1 | 64.0 KiB | 2026-08-19 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0763 | P | `fno_v8_strict_v6_logic_backtest.py` | 1 | 5.2 KiB | 2026-08-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0764 | P | `fno_v8_v9_last_10_backtest.py` | 1 | 18.0 KiB | 2026-08-26 | bat\run_fno_v8_v9_last_10_backtest.ps1:28 launcher/asset reference; fno_v8_v9_last_10_backtest.py:38 resolved static path |
| K-0765 | P | `fno_v8_windowed_1m_entry_backtest.py` | 1 | 194.7 KiB | 2026-08-26 | fno_v8_combined_best_per_leg_backtest.py:29 import; fno_v9_honest_v8_backtest.py:25 import |
| K-0766 | P | `fno_v8_windowed_1m_entry_optimize.py` | 1 | 86.1 KiB | 2026-08-19 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0767 | P | `fno_v9_0950_0955_honest_optimize.py` | 1 | 132.3 KiB | 2026-08-20 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0768 | P | `fno_v9_block_tuner.py` | 1 | 13.2 KiB | 2026-08-20 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0769 | P | `fno_v9_honest_data_repair.py` | 1 | 97.4 KiB | 2026-08-20 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0770 | P | `fno_v9_honest_rollover_diagnostic.py` | 1 | 978 B | 2026-08-26 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0771 | P | `fno_v9_honest_v8_backtest.py` | 1 | 20.7 KiB | 2026-08-20 | fno_v8_v9_last_10_backtest.py:445 literal reference; fno_v8_v9_last_10_backtest.py:457 literal reference |
| K-0772 | P | `fundamental_price_action_v1_papertrade.py` | 1 | 21.0 KiB | 2026-08-07 | bat\run_fundamental_price_action_v1_papertrade.bat:16 launcher/asset reference; fundamental_price_action_v1_papertrade.py:34 module-name reference |
| K-0773 | P | `fundamental_price_action_v1_session.py` | 1 | 38.4 KiB | 2026-08-07 | bat\run_fundamental_price_action_v1_session.bat:13 launcher/asset reference; fundamental_price_action_v1_session.py:20 resolved static path |
| K-0774 | P | `G_iterate.py` | 1 | 12.4 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0775 | P | `G_validate_passer.py` | 1 | 5.5 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0776 | P | `gate_promotion.py` | 1 | 14.6 KiB | 2026-06-11 | bat\run_v7_gate_promotion_report.bat:4 launcher/asset reference; gate_promotion.py:146 literal reference |
| K-0777 | P | `hilega_milega_setups.py` | 1 | 10.8 KiB | 2026-08-10 | G-family import/launcher dependency audit |
| K-0778 | P | `india_vix.parquet` | 1 | 3.2 KiB | 2026-03-05 | G-family import/launcher dependency audit |
| K-0779 | P | `kite_fetch_nifty50_15m_for_regime.py` | 1 | 10.4 KiB | 2026-03-11 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0780 | P | `kite_testing.py` | 1 | 8.9 KiB | 2026-04-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0781 | P | `kite_ticker_pf_plan.md` | 1 | 22.0 KiB | 2026-04-24 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0782 | P | `kite_vix_fetch_5m_15m.py` | 1 | 10.2 KiB | 2026-03-05 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0783 | P | `L_iterate.py` | 1 | 13.7 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0784 | P | `L_rejects_improve.py` | 1 | 8.3 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0785 | P | `L_validate_passer.py` | 1 | 6.9 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0786 | P | `live_v15_shared_scan_cache.py` | 1 | 3.6 KiB | 2026-03-23 | G-family import/launcher dependency audit |
| K-0787 | P | `live_v15_slot_snapshot_v15_new.py` | 1 | 33.6 KiB | 2026-03-25 | G-family import/launcher dependency audit |
| K-0788 | P | `live_v16_5min_slot_snapshot.py` | 1 | 25.7 KiB | 2026-04-09 | G-family import/launcher dependency audit |
| K-0789 | P | `log_dashboard_server.py` | 1 | 543.9 KiB | 2026-10-10 | dashboard entry point; running PID 36544 |
| K-0790 | P | `ml_meta_filter.py` | 1 | 6.3 KiB | 2026-02-13 | G-family import/launcher dependency audit |
| K-0791 | P | `moving_files.py` | 1 | 6.2 KiB | 2026-06-13 | bat\run_data_for_backtesting_parallel.ps1:19 launcher/asset reference; bat\run_data_for_backtesting_parallel.ps1:20 launcher/asset reference |
| K-0792 | P | `new_setups_scan_v11.py` | 1 | 20.3 KiB | 2026-06-12 | G-family import/launcher dependency audit |
| K-0793 | P | `nse_holidays.csv` | 1 | 468 B | 2026-09-14 | G-family import/launcher dependency audit |
| K-0794 | P | `nse_intraday_costs.py` | 1 | 8.9 KiB | 2026-06-10 | G-family import/launcher dependency audit |
| K-0795 | P | `nse_market_calendar.py` | 1 | 2.1 KiB | 2026-09-14 | G-family import/launcher dependency audit |
| K-0796 | P | `OPTIONS_V13_V10_G_PAPER_TRADING.md` | 1 | 3.5 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0797 | P | `preopen_session_autofix.py` | 1 | 11.4 KiB | 2026-09-29 | bat\run_preopen_session_autofix.bat:10 launcher/asset reference; preopen_session_autofix.py:17 resolved static path |
| K-0798 | P | `preopen_session_healthcheck.py` | 1 | 53.4 KiB | 2026-09-29 | bat\run_preopen_session_healthcheck.bat:10 launcher/asset reference; preopen_session_autofix.py:20 literal reference |
| K-0799 | P | `qualification_tracker.py` | 1 | 13.0 KiB | 2026-06-11 | bat\run_v7_qualification_report.bat:4 launcher/asset reference; v7_qualification_report.py:53 import |
| K-0800 | P | `reconcile.py` | 1 | 58.8 KiB | 2026-07-07 | ai_platform\observability\cli.py:954 module-name reference; ai_platform\observability\cli.py:763 module-name reference |
| K-0801 | P | `request_token.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0802 | P | `request_token2.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0803 | P | `request_token3.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0804 | P | `request_token4.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0805 | P | `request_token5.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0806 | P | `request_token6.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0807 | P | `request_token7.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0808 | P | `request_token8.txt` | 1 | 33 B | 2026-10-09 | G-family import/launcher dependency audit |
| K-0809 | P | `research/__init__.py` | 1 | 88 B | 2026-04-20 | G-family import/launcher dependency audit |
| K-0810 | P | `research/diagnostics.py` | 1 | 3.8 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0811 | P | `research/experiments.py` | 1 | 14.8 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0812 | P | `research/experiments_v2.py` | 1 | 5.6 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0813 | P | `research/filters.py` | 1 | 4.2 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0814 | P | `research/g2_audit_history_v2.py` | 1 | 25.5 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0815 | P | `research/g2_audit_movements_v2.py` | 1 | 24.5 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0816 | P | `research/g2_audit_report_v2.py` | 1 | 28.9 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0817 | P | `research/g2_audit_selection_v2.py` | 1 | 27.5 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0818 | P | `research/g2_audit_verify_v2.py` | 1 | 6.4 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0819 | P | `research/g2_session_finalize_report.py` | 1 | 15.9 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0820 | P | `research/g2_session_forensic_audit.py` | 1 | 32.4 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0821 | P | `research/g2_session_historical_volume_test.py` | 1 | 7.2 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0822 | P | `research/g2_session_preflight_20261008.py` | 1 | 10.0 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0823 | P | `research/g3_dated_replay.py` | 1 | 6.6 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0824 | P | `research/g3_daywise_backtest.py` | 1 | 13.5 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0825 | P | `research/g3_daywise_report.py` | 1 | 44.9 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0826 | P | `research/g3_daywise_workbook.mjs` | 1 | 29.3 KiB | 2026-10-08 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0827 | P | `research/g3_entry_preview.py` | 1 | 8.2 KiB | 2026-10-09 | G-family import/launcher dependency audit |
| K-0828 | P | `research/g3_extend_results.py` | 1 | 19.0 KiB | 2026-10-10 | G-family import/launcher dependency audit |
| K-0829 | P | `research/g3_freeze.py` | 1 | 18.2 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0830 | P | `research/g3_pullback_followthrough.py` | 1 | 18.4 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0831 | P | `research/g3_pullback_metrics.py` | 1 | 13.2 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0832 | P | `research/g3_pullback_monitor.py` | 1 | 24.4 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0833 | P | `research/g3_pullback_protocol_v1.json` | 1 | 2.8 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0834 | P | `research/g3_pullback_report.py` | 1 | 23.6 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0835 | P | `research/g3_pullback_selective.py` | 1 | 25.7 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0836 | P | `research/g3_pullback_selective_features.py` | 1 | 7.1 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0837 | P | `research/g3_pullback_selective_report.py` | 1 | 28.3 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0838 | P | `research/g3_pullback_tuning.py` | 1 | 18.5 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0839 | P | `research/g3_pullback_tuning_protocol_v1.json` | 1 | 1.4 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0840 | P | `research/g3_results_report.py` | 1 | 10.4 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0841 | P | `research/g3_sep24_raw_pool.py` | 1 | 10.2 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0842 | P | `research/g3_six_confirmation_variants.py` | 1 | 19.6 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0843 | P | `research/run_exit_sweep.py` | 1 | 3.7 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0844 | P | `research/validate.py` | 1 | 2.6 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0845 | P | `research/walk_forward.py` | 1 | 2.0 KiB | 2026-04-20 | Referenced by a protected source/launcher; conservative static code closure |
| K-0846 | P | `research_v11_tier123_new_setups.py` | 1 | 45.7 KiB | 2026-06-13 | G-family import/launcher dependency audit |
| K-0847 | P | `research_v12_ml_long_entry_backtest.py` | 1 | 21.5 KiB | 2026-08-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0848 | P | `research_v12_one_month_long_logic_optimizer_v9.py` | 1 | 65.1 KiB | 2026-08-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0849 | P | `research_v12_path_aware_long_rebuild.py` | 1 | 27.5 KiB | 2026-08-05 | Referenced by a protected source/launcher; conservative static code closure |
| K-0850 | P | `research_v12_prefilter_train_test_optimizer.py` | 1 | 76.5 KiB | 2026-08-04 | Referenced by a protected source/launcher; conservative static code closure |
| K-0851 | P | `research_v12_six_month_frozen_balanced_long_replay_v11.py` | 1 | 52.7 KiB | 2026-08-06 | Referenced by a protected source/launcher; conservative static code closure |
| K-0852 | P | `research_v12_six_month_frozen_l009216_backtest.py` | 1 | 34.7 KiB | 2026-08-06 | Referenced by a protected source/launcher; conservative static code closure |
| K-0853 | P | `research_v12_six_month_honest_pullback_walkforward_v13.py` | 1 | 53.6 KiB | 2026-08-06 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0854 | P | `S_iterate.py` | 1 | 13.5 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0855 | P | `stocks_fetcher_15m_run.log` | 1 | 318.6 KiB | 2026-05-16 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0856 | P | `stocks_fetcher_1min_run.log` | 1 | 80.4 KiB | 2026-10-09 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0857 | P | `stocks_fetcher_5m_run.log` | 1 | 1.4 KiB | 2026-10-09 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0858 | P | `stocks_fetcher_daily_run.log` | 1 | 377.6 KiB | 2026-05-16 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0859 | P | `stocks_fetcher_live_minimal_run.log` | 1 | 0 B | 2026-10-09 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0860 | P | `stocks_invalid_symbols.json` | 1 | 6.8 KiB | 2026-08-11 | Shared market data/indicator input; freshness or an older name is not evidence of disuse |
| K-0861 | P | `stocks_tokens_cache.json` | 1 | 28.7 KiB | 2026-10-05 | G-family import/launcher dependency audit |
| K-0862 | P | `T_iterate.py` | 1 | 14.5 KiB | 2026-06-12 | Referenced by a protected source/launcher; conservative static code closure |
| K-0863 | P | `tools/build_v13_v10_h_dataset_snapshot.py` | 1 | 1.3 KiB | 2026-09-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0864 | P | `tools/build_v13_v10_h_source.py` | 1 | 6.2 KiB | 2026-09-29 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0865 | P | `tools/correct_v13_v10_g_cutoff_bundle.py` | 1 | 16.4 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0866 | P | `tools/extend_v13_v10_g2_dashboard_history.py` | 1 | 15.3 KiB | 2026-10-10 | G-family import/launcher dependency audit |
| K-0867 | P | `tools/extend_v13_v10_g_dated_results.py` | 1 | 18.6 KiB | 2026-10-10 | G-family import/launcher dependency audit |
| K-0868 | P | `tools/fno_challenger_robustness_audit.py` | 1 | 78.2 KiB | 2026-08-27 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0869 | P | `tools/fno_daily_strategy_dashboard.py` | 1 | 20.4 KiB | 2026-09-03 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0870 | P | `tools/fno_today_combined_dashboard_publish.py` | 1 | 18.6 KiB | 2026-08-31 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0871 | P | `tools/fno_today_current_refresh_replay.py` | 1 | 18.9 KiB | 2026-08-31 | Referenced by a protected source/launcher; conservative static code closure |
| K-0872 | P | `tools/fno_today_six_strategy_replay.py` | 1 | 23.5 KiB | 2026-09-03 | Referenced by a protected source/launcher; conservative static code closure |
| K-0873 | P | `tools/fno_today_symmetric_1pct_counterfactual.py` | 1 | 14.3 KiB | 2026-09-03 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0874 | P | `tools/fno_v6_isolated_challenger_replay.py` | 1 | 129.9 KiB | 2026-08-27 | Referenced by a protected source/launcher; conservative static code closure |
| K-0875 | P | `tools/fno_v6_today_selection_audit.py` | 1 | 25.1 KiB | 2026-09-03 | FnO source namespace retained for shared engines, optional research and provenance discovery |
| K-0876 | P | `tools/refresh_v13_v10_g_research_source.py` | 1 | 7.8 KiB | 2026-10-08 | G-family import/launcher dependency audit |
| K-0877 | P | `tools/run_dashboard_flow.py` | 1 | 2.3 KiB | 2026-10-10 | dashboard entry point; running PID 11884 |
| K-0878 | P | `tools/run_g3_dated_once.py` | 1 | 2.6 KiB | 2026-10-09 | scheduled task \EQIDV2_g3_dated_once_20261009 state=Ready; tools\run_g3_dated_once.py:12 resolved static path |
| K-0879 | P | `tools/v13_execution_research.py` | 1 | 1.8 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0880 | P | `tools/v13_prospective_shadow.py` | 1 | 3.1 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0881 | P | `tools/v13_shadow_automation.py` | 1 | 3.3 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0882 | P | `tools/v13_strategy_research.py` | 1 | 2.6 KiB | 2026-09-25 | G-family import/launcher dependency audit |
| K-0883 | P | `trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py` | 1 | 9.7 KiB | 2026-06-13 | bat\run_nifty_guard_fetcher_v15.bat:10 launcher/asset reference; bat\stop_eqidv2_v15_stack.ps1:102 launcher/asset reference |
| K-0884 | P | `trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py` | 1 | 29.3 KiB | 2026-06-25 | bat\run_nifty_guard_fetcher_v16_5min.bat:12 launcher/asset reference; eqidv2_nifty_guard_fetcher_supervised_v16_5min.py:88 literal reference |
| K-0885 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py` | 1 | 67.4 KiB | 2026-06-13 | G-family import/launcher dependency audit |
| K-0886 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py` | 1 | 39.4 KiB | 2026-06-13 | trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py:26 import; trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly.py:152 resolved static path |
| K-0887 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min.py` | 1 | 76.9 KiB | 2026-09-25 | bat\run_data_for_backtesting_parallel.ps1:25 launcher/asset reference; bat\run_data_for_backtesting_parallel.ps1:26 launcher/asset reference |
| K-0888 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_5minonly.py` | 1 | 43.4 KiB | 2026-08-07 | fno_equity_5m_repair.py:26 import; trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_5minonly.py:155 resolved static path |
| K-0889 | P | `trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py` | 1 | 129.1 KiB | 2026-09-01 | G-family import/launcher dependency audit |
| K-0890 | P | `v11_exit_policy_resolver.py` | 1 | 5.7 KiB | 2026-07-30 | G-family import/launcher dependency audit |
| K-0891 | P | `v11_gate_report.py` | 1 | 11.0 KiB | 2026-06-11 | Referenced by a protected source/launcher; conservative static code closure |
| K-0892 | P | `v11_lab_shadow_monitor.py` | 1 | 29.7 KiB | 2026-07-30 | log_dashboard_server.py:506 module-name reference; log_dashboard_server.py:601 module-name reference |
| K-0893 | P | `v11_setup_rescue_search.py` | 1 | 10.9 KiB | 2026-06-11 | Referenced by a protected source/launcher; conservative static code closure |
| K-0894 | P | `V13_V10_A_IMPLEMENTATION_STATUS.md` | 1 | 4.6 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0895 | P | `V13_V10_B_IMPLEMENTATION_STATUS.md` | 1 | 2.9 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0896 | P | `V13_V10_C_IMPLEMENTATION_STATUS.md` | 1 | 2.7 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0897 | P | `V13_V10_D_IMPLEMENTATION_STATUS.md` | 1 | 2.7 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0898 | P | `V13_V10_E_IMPLEMENTATION_STATUS.md` | 1 | 1.4 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0899 | P | `V13_V10_F_IMPLEMENTATION_STATUS.md` | 1 | 4.9 KiB | 2026-09-14 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0900 | P | `V13_V10_G_DAILY_BACKTEST.md` | 1 | 7.8 KiB | 2026-10-05 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0901 | P | `V13_V10_G_DASHBOARD_INTEGRATION.md` | 1 | 16.8 KiB | 2026-10-05 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0902 | P | `V13_V10_G_FULL_BACKTEST_RESULT_20261004.md` | 1 | 9.2 KiB | 2026-10-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0903 | P | `V13_V10_G_IMPLEMENTATION_STATUS.md` | 1 | 10.8 KiB | 2026-10-05 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0904 | P | `V13_V10_G_OPTIONS_BACKTEST.md` | 1 | 8.1 KiB | 2026-09-15 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0905 | P | `V13_V10_IMPLEMENTATION_STATUS.md` | 1 | 2.8 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0906 | P | `V13_V5_FNO_COMPLETE_REPORT.html` | 1 | 148.7 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0907 | P | `V13_V5_FNO_COMPLETE_REPORT.md` | 1 | 101.1 KiB | 2026-09-04 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0908 | P | `V13_V5_OPTIONS_BACKTEST_RESULTS.md` | 1 | 17.8 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0909 | P | `V13_V5_V6_V7_V8_COMPARISON_THROUGH_2026-09-08.md` | 1 | 6.4 KiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0910 | P | `V13_V6_V7_IMPLEMENTATION_STATUS.md` | 1 | 6.8 KiB | 2026-09-09 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0911 | P | `V13_V9_IMPLEMENTATION_STATUS.md` | 1 | 3.1 KiB | 2026-09-13 | Protected family/dashboard/Kite path or conservatively retained V13 predecessor evidence |
| K-0912 | P | `v17D_config.py` | 1 | 7.6 KiB | 2026-05-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0913 | P | `v17D_cost_model.py` | 1 | 6.8 KiB | 2026-05-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0914 | P | `v17D_exit_resolver.py` | 1 | 6.3 KiB | 2026-05-13 | G-family import/launcher dependency audit |
| K-0915 | P | `v17D_library_backtest.py` | 1 | 8.2 KiB | 2026-05-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0916 | P | `v17D_mae_mfe_analyzer.py` | 1 | 6.8 KiB | 2026-05-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0917 | P | `v17D_signal_to_row.py` | 1 | 6.9 KiB | 2026-05-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0918 | P | `v17D_universe_filter.py` | 1 | 5.2 KiB | 2026-05-13 | Referenced by a protected source/launcher; conservative static code closure |
| K-0919 | P | `v17g_ab_runner.py` | 1 | 8.1 KiB | 2026-04-22 | Referenced by a protected source/launcher; conservative static code closure |
| K-0920 | P | `v5_exit_resolver.py` | 1 | 347 B | 2026-05-18 | Referenced by a protected source/launcher; conservative static code closure |
| K-0921 | P | `v7_causality_audit.py` | 1 | 26.6 KiB | 2026-06-15 | log_dashboard_server.py:81 module-name reference; log_dashboard_server.py:499 module-name reference |
| K-0922 | P | `v7_fpe_v2_common.py` | 1 | 10.5 KiB | 2026-07-08 | v7_full_pipeline_entry_research_v2.py:41 import; v7_fpe_v2_common.py:35 resolved static path |
| K-0923 | P | `v7_full_pipeline_entry_research.py` | 1 | 47.5 KiB | 2026-07-08 | log_dashboard_server.py:89 module-name reference; log_dashboard_server.py:503 module-name reference |
| K-0924 | P | `v7_full_pipeline_entry_research_v2.py` | 1 | 24.9 KiB | 2026-07-08 | log_dashboard_server.py:91 module-name reference; log_dashboard_server.py:504 module-name reference |
| K-0925 | P | `v7_gate_promotion_report.py` | 1 | 16.1 KiB | 2026-06-11 | bat\run_v7_gate_promotion_report.bat:15 launcher/asset reference |
| K-0926 | P | `v7_nse_id_cost_report.py` | 1 | 23.6 KiB | 2026-06-10 | bat\run_v7_nse_id_cost_report.bat:11 launcher/asset reference |
| K-0927 | P | `v7_qualification_report.py` | 1 | 19.0 KiB | 2026-06-15 | bat\run_v7_qualification_report.bat:18 launcher/asset reference |
| K-0928 | P | `v7_shadow_candidate_monitor.py` | 1 | 32.2 KiB | 2026-07-08 | log_dashboard_server.py:505 module-name reference; log_dashboard_server.py:600 module-name reference |
| K-0929 | P | `v7_walkforward_gate_report.py` | 1 | 25.2 KiB | 2026-06-10 | bat\run_v7_walkforward_gate_report.bat:11 launcher/asset reference |
| K-0930 | P | `wait_for_data_backtesting_ready.py` | 1 | 4.5 KiB | 2026-09-01 | G-family import/launcher dependency audit |
| K-0931 | P | `walkforward_gate.py` | 1 | 19.5 KiB | 2026-06-11 | G-family import/launcher dependency audit |
| K-0932 | P | `zerodha_kite_export.py` | 1 | 18.4 KiB | 2026-03-27 | bat\run_zerodha_kite_export_scheduler.bat:9 launcher/asset reference; bat\run_zerodha_kite_export_stop.bat:12 launcher/asset reference |

## Evidence and limitations

- Runtime audit inspected 396 reachable source/launcher files and 86 registered EQIDV2 tasks (36 Ready; 50 Disabled). Registration is a reason to retain the target, not proof of recent execution.
- Source tracing used static imports, launcher targets and path references; strategy/trading modules were not imported or executed. A further conservative source-name closure protects references from the combined audits.
- No token/API-key text contents were read. Metadata enumeration uses filenames, byte lengths and modification timestamps. No trade, operational service, scheduled task or dashboard state was changed.
- Running-process/task snapshots are time-bound. Open editor tabs, external tools, dynamic imports, environment overrides and arbitrary glob consumers cannot be exhaustively proven from static code. HOLD paths are intentionally not promoted to candidates.
- The inventories describe a live workspace, not a filesystem snapshot. Files created after the metadata scan are outside this count and are not approved by this document.
- Scan exclusions:
  - P: `docs/v13_v10_g_web/.git` — Inventory work directory or repository metadata; retained in place.
  - P: `.codex_tmp/archive_inventory_20261010` — Inventory work directory or repository metadata; retained in place.
  - P: `.codex_tmp/v13_daywise_compare_work/node_modules` — Link/reparse directory; not traversed or proposed for archiving.

Local audit evidence (metadata and filenames; no credential contents):

- `.codex_tmp/archive_inventory_20261010/metadata.json` — full scanned metadata.
- `.codex_tmp/archive_inventory_20261010/strategy_dependencies.json` — G-family import and data-path evidence.
- `.codex_tmp/archive_inventory_20261010/dashboard_runtime.json` — dashboards, process targets, task targets and source references.
- `.codex_tmp/archive_inventory_20261010/data_audit.json` — data-tree dependency review, when present.
- `.codex_tmp/archive_inventory_20261010/classification.json` — disjoint units, IDs, counts and classifications.

## Future approval record

**Approval: NOT GRANTED. Destination: NOT SELECTED. Archive date: NOT SCHEDULED.**

Before any archive action, obtain your explicit approval of the exact A-IDs/paths and destination. Recheck running processes, task actions, imports, dynamic paths and any changes since this inventory. HOLD and KEEP rows require a separate, newly reviewed decision; approval of candidates does not include them.

| Approval field | Value |
| --- | --- |
| Approved candidate IDs | None |
| Approved destination | Not selected |
| Approved operation/date | None |
| Approved by | Awaiting user |
| Files moved, deleted or archived in this task | 0 |
