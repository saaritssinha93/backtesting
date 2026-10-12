# G to accepted G-3 strategy migration audit - plan only

PLAN ONLY: source and nonsecret frozen-config inspection for migrating current G equity live, PAPER, backtest and analysis consumers to accepted G-3. No strategy imports, backtests, broker access, scheduler changes or production writes performed. v14 is unrelated and must remain untouched.

No AGENTS.md found in the workspace or ancestor chain during this audit; root independently reconfirmed for current turn.

## Baseline and recommendation

fno_v13_v10_g_3_backtest.py defaults to verifying/exporting archived results. New dates require research/g3_dated_replay.py consuming completed G daily features and sealed minute inputs after 15:35 IST, then research/g3_extend_results.py for archive/observer aggregation.

Archived 46-session research book ending 2026-10-07; 102 selected orders, 93 filled/executed; October 1 excluded. This is not a deployed live strategy or an untouched validation set.

Use **G3_W1_V1p1** as the target: exact-next-minute confirmation, LONG 1.10x and SHORT 1.20x; original strict setup book; G2 staged stops. Preserve frozen research and create an independent production G3 profile and pure decision core. Quantity-one LIVE stays. Pullback analysis stays observer-only.

## Exact changes and retained rules

### One-minute confirmation volume threshold

- **frozen G and G2:** Both LONG and SHORT >= 1.20
- **current G:** Both LONG and SHORT >= 1.20
- **accepted G3:** LONG >= 1.10; SHORT >= 1.20
- **implementation:** Research rescales LONG ratio by 1.2/threshold to reuse G's hardcoded threshold, then restores observed ratio. Replace this shim with an explicit side-dependent threshold in a pure shared selector, while retaining historical G defaults.

Evidence: `research/g3_six_confirmation_variants.py:144`, `fno_v13_v10_g_backtest.py:111`, `fno_v13_v10_g_live_config.py:331`, `fno_v13_v10_g_live_config.py:369`.

### Confirmation clock

- **all:** Exactly one completed minute after the 5m signal, with feature timestamp equal to confirmation time for production. Trigger only after confirmation close.
- **excluded variants:** Six-arm research also studied +1 through +2/+3 windows. Accepted G3 is W1 only; do not enable delayed retries as trading rules.

Evidence: `research/g3_dated_replay.py:79`, `research/g3_six_confirmation_variants.py:1`, `research/g3_six_confirmation_variants.py:160`.

### 09:25 LONG relaxation

- **current G:** From 2026-10-06: preserve original G choice, then fill vacant original quota from relaxed raw candidates: OI min 0.10/max1.20%, price>=0.30%, 5m volume>=1.75, body>=0.54, wick<=0.60, ignore EMA, confirmation volume>=1.20.
- **accepted G3:** Not included. Frozen strict 09:25 LONG retains EMA9>EMA20>EMA50, OI max1.00%, setup 5m volume>=3.00 and body>=0.60, with G3 confirmation volume>=1.10.
- **decision:** Recommended baseline is exact accepted G3 fidelity. Keeping the current G relaxed 09:25 addition creates a separately named hybrid requiring its own evidence and approval.

Evidence: `fno_v13_v10_g_policy.py:10`, `fno_v13_v10_g_policy.py:15`, `fno_v13_v10_g_selection.py:12`, `fno_v13_v10_g_live_config.py:197`, `research/g3_dated_replay.py:76`.

### Stops

- **frozen G:** Inherited per-setup static stops
- **current G:** From 2026-10-06: initial 1.25%, tighten once to 1.00% after 120 minutes
- **accepted G3:** G2 staged stops for all accepted G3 historical dates, not gated by current G promotion date. Targets inherited unchanged. No partial exits or breakeven.

Evidence: `fno_v13_v10_g_policy.py:10`, `fno_v13_v10_g_2_backtest.py:28`, `research/g3_six_confirmation_variants.py:200`, `research/g3_freeze.py:147`.

### Selection and quotas

- **all:** Original fourteen setup slots; F core selected first within each setup/day quota; G SHORT expansion uses price multiplier 0.65 with floor 0.10%, and F SHORT OI thresholds already halved with floor 0.05%. Stable picker, traded value, symbol ordering.
- **G3 effect:** The newly eligible LONG candidates are reranked with all candidates. G3 is not necessarily G2 plus extra trades: a LONG selection may be replaced. SHORT selections should remain equal, but executed SHORTs can differ through changed capital occupancy.

Evidence: `fno_v13_v10_g_backtest.py:76`, `fno_v13_v10_g_backtest.py:144`, `research/g3_six_confirmation_variants.py:144`, `research/g3_six_confirmation_variants.py:226`.

## Effective fourteen-setup contract

These effective rows were read from the existing nonsecret frozen G3 JSON. LONG price is positive change; SHORT price is absolute decline. OI/price/stop/target values are percentages. Volume/body/wick values are ratios. Base EMA/OI/volume and confirmation-direction gates still apply. Stop is 1.25% initially, 1.00% after 120 minutes for every row.

| Setup | Signal -> confirmation | Quota / picker | Price core -> expanded | OI min | 5m vol | 1m body / wick max | 1m vol min | Target % |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 0926_LONG | 09:25 -> 09:26 | 1 / max_liquidity | 0.3 -> 0.3 | 0.1 | 3 | 0.6 / 0.6 | 1.1 | 0.97 |
| 0926_SHORT | 09:25 -> 09:26 | 2 / max_volume | 0.2 -> 0.13 | 0.05 | 1.5 | 0.4 / 0.6 | 1.2 | 2.67 |
| 0931_LONG | 09:30 -> 09:31 | 1 / max_move | 0.65 -> 0.65 | 0.1 | 1 | 0.5 / 0.6 | 1.1 | 2 |
| 0931_SHORT | 09:30 -> 09:31 | 1 / max_move | 0.2 -> 0.13 | 0.125 | 1 | 0.4 / 0.6 | 1.2 | 2 |
| 0936_LONG | 09:35 -> 09:36 | 1 / max_liquidity | 0.2 -> 0.2 | 0.15 | 1 | 0.6 / 0.6 | 1.1 | 1.73 |
| 0936_SHORT | 09:35 -> 09:36 | 2 / max_liquidity | 0.5 -> 0.325 | 0.5 | 1 | 0.4 / 0.6 | 1.2 | 1.23 |
| 0941_LONG | 09:40 -> 09:41 | 1 / max_liquidity | 0.2 -> 0.2 | 0.075 | 2 | 0.5 / 0.6 | 1.1 | 3 |
| 0941_SHORT | 09:40 -> 09:41 | 1 / max_move | 0.2 -> 0.13 | 0.05 | 1 | 0.4 / 0.6 | 1.2 | 2.43 |
| 0946_LONG | 09:45 -> 09:46 | 1 / max_move | 0.65 -> 0.65 | 0.1 | 1 | 0.4 / 0.6 | 1.1 | 1.23 |
| 0946_SHORT | 09:45 -> 09:46 | 1 / max_volume | 0.2 -> 0.13 | 0.375 | 1 | 0.4 / 0.4 | 1.2 | 1.23 |
| 0951_SHORT | 09:50 -> 09:51 | 1 / max_liquidity | 0.2 -> 0.13 | 0.05 | 1 | 0.4 / 0.6 | 1.2 | 0.93 |
| 0956_LONG | 09:55 -> 09:56 | 1 / max_liquidity | 0.2 -> 0.2 | 0.1 | 1 | 0.4 / 0.6 | 1.1 | 0.9 |
| 1001_LONG | 10:00 -> 10:01 | 1 / max_liquidity | 0.4 -> 0.4 | 0.05 | 1 | 0.4 / 0.6 | 1.1 | 3 |
| 1121_SHORT | 11:20 -> 11:21 | 1 / max_liquidity | 0.2 -> 0.13 | 0.05 | 1 | 0.4 / 0.6 | 1.2 | 2 |

**Exit precedence:** `frozen_config.exit.setups` is authoritative. Donor `setup_rules.original/core/expanded` stop/target metadata is historical and must not define G3 brackets.

- **instrument:** NSE equity prices, volume, indicators, entry/exit; dated stock-future OI; dated near-month NIFTY-future first bar for 09:25 SHORT gate
- **base_gates:** Positive increasing OI with OI change0.05..1.00%; 5m volume ratio>=0.80; LONG EMA9>EMA20>EMA50 and price change>=0.10%; SHORT inverse EMA and price change<=-0.10%. Per-setup stricter gates apply. 09:25 SHORT additionally requires 09:20 NIFTY return<=-0.05%.
- **confirmation:** Real exact next minute; finite positive consistent OHLC, nonzero range; LONG green and close>5m signal close, SHORT red and close<signal close; per-setup body/wick thresholds; volume ratio=current volume/mean previous 20 observed completed1m volumes, excluding current candle, mathematical min_periods 5.
- **execution:** Stop entry at confirmation high LONG/low SHORT; ten-minute trigger window; retained targets; stop-first ambiguous bars; adverse gap fill at later-bar open; time exit 15:15 IST. Live uses tick rounding and actual broker fills; PAPER uses observed quotes.
- **sizing:** Frozen research/PAPER 100000 capital per filled entry, 5x target exposure, 1000000 portfolio capital, no explicit max_positions. Existing LIVE quantity-one coordinator is a separate deployment constraint and should remain unchanged unless expressly changed.
- **cost:** Five basis-point round-trip approximation in archived research/PAPER; retain distinct actual broker accounting and do not call estimated costs realized fees.

## Implementation gaps, timing and causality risks

- **No G3 runtime routing exists:** fno_live_profile only accepts V13_V10_G for v6; canonical G wrappers force that profile. Adding a G3 config alone cannot activate it. Several is_g_config branches guard volume readiness, validation, paper locking, expiry, observability and staged stops. Evidence: `fno_live_profile.py:7`, `fno_v13_v10_g_live.py:6`, `fno_v5_live.py:55`.
- **Hardcoded G policy and identity in shared V5/V6 engines:** fno_v5_live imports G policy and G identity directly; _g_stop_policy_metadata uses is_g_config and the G date gate. Paper helpers import G config globally. Refactor these to an immutable profile contract and pass it explicitly. Preserve legacy G behavior and old state readers. Evidence: `fno_v5_live.py:56`, `fno_v5_live.py:2735`, `fno_v13_v10_g_paper.py:22`, `fno_v13_v10_g_identity.py:15`.
- **Effective frozen exits differ from setup donor metadata:** G3 frozen_config.setup_rules original/core/expanded retain donor stops/targets; authoritative effective exits are config.exit.setups. Existing G config invariant pins uniform1.20 and baseline reward/risk>=1.5; that invariant is incompatible with effective G3 targets0.90/0.93/0.97 against1.25SL. Build a normalized G3 contract with clear precedence and independent validation. Evidence: `research/g3_freeze.py:147`, `research/g3_freeze.py:155`, `fno_v13_v10_g_live_config.py:460`.
- **Cross-mode stop timing is deliberately different:** Bar replay starts120minute timer at entry candle end and activates at the first bar open at/after that time. Live/PAPER start from actual fill timestamp. Expect up to one bar of clock difference; preserve explicit semantics and compare only what is observable. Evidence: `fno_v13_v10_g_2_backtest.py:307`, `fno_v5_live.py:2746`.
- **OHLC fills cannot exactly reproduce tick execution:** Native replay chooses first trigger hit in a minute, adverse gap entry at open, and stop-first same-bar exits. Within-bar event order is unknown; do not assert tick-level equality. PAPER LTP polling can miss between-poll touches; broker fills and reconciliation are authoritative for LIVE. Evidence: `fno_v13_corrected_v5_backtest.py:758`, `fno_v13_v10_g_2_backtest.py:307`, `fno_v5_live.py:2774`.
- **Warmup and quality differences:** Research mathematical denominator accepts min5 prior observations; current G live and daily replay quality checks require20 prior valid regular-session observations. Retain fail-closed20 operational requirement, report incompleteness, and verify acceptance book has sufficient history. Never use later minute data or current volume in denominator. Evidence: `research/g3_six_confirmation_variants.py:39`, `fno_v13_v10_g_live_config.py:55`, `fno_v13_v10_g_daily_replay.py:559`.
- **Full-day research completeness must not become a live gate:** Historical replay requires full09:16–15:15 paths and can exclude stock-days with later missing bars. That is data eligibility, not a condition known to the morning scanner. Prefix-based live readiness and historical path completeness must be tracked separately. Evidence: `fno_v13_v10_g_daily_replay.py:559`, `research/g3_dated_replay.py:100`.
- **Frozen exporter does not prove full selector equivalence:** Freeze copies102selected ledger rows and recomputes93executed trade exits/sizing from archived inputs. Promotion tests must compare full candidate decisions, rankings,102selected orders and 9 nofills as well as P&L; frozen export alone is insufficient. Evidence: `research/g3_freeze.py:163`, `research/g3_freeze.py:241`.
- **Pullback research is observer-only:** G3_W1_V1p1 strategy does not include pullback warnings as entries/exits. max_future_adverse_pct, outcome labels, eventual frozen exit and followthrough classes are hindsight analysis. Separate live feature-only inference from later outcome evaluation if observers are migrated; never feed future labels into trading decisions. Frozen models are reused exploratory evidence, not permission to retrain or alter stops. Evidence: `research/g3_pullback_monitor.py:77`, `research/g3_pullback_selective_features.py:49`, `research/g3_pullback_selective.py:367`, `research/g3_pullback_followthrough.py:1`, `research/g3_daywise_backtest.py:183`.
- **Identity/state migration must be explicit:** Signal IDs hash strategy_version and profile fingerprint is validated in signals, snapshots, orders and controls. New G3 version/root/report prefix/order tag prevents collision. Existing G OPEN/PENDING orders must be reconciled under G rules, not silently relabeled. Do not copy G arming authorization into a G3 profile. Evidence: `fno_v13_v10_g_identity.py:39`, `fno_v5_live.py:1896`, `fno_v5_live.py:2605`, `fno_v5_live.py:2858`, `fno_v13_v10_g_paper.py:84`.
- **Options follower is a separate strategy product:** G options PAPER reads equity orders through hardcoded G config/root. Migration may redirect equity-source profile, but accepted equity G3 does not authorize changing option selection, sizing, stops/targets, cash limits or enabling options LIVE. Include routing/report adaptation if user scope includes that existing follower. Evidence: `fno_v13_v10_g_options_paper.py:26`, `fno_v13_v10_g_options_paper.py:56`, `fno_v13_v10_g_options_paper.py:174`.

## Planned acceptance checks

1. Direct threshold cases: LONG1.099999/1.10/1.199999/1.20; SHORT1.199999/1.20; missing/NaN/negative/current-bar volume; exact feature time; opposite-direction candles.
2. Accepted W1 clock rejects +2/+3 despite research variants; stale feed, flagged/gap-filled candles, wrong session, missing exact minute fail closed.
3. Strict09:25 cases distinguish accepted G3 from current G dated relaxation: OI1.01..1.20, absent EMA alignment, volume1.75..2.999,body0.54..0.599 rejected by accepted G3.
4. F-core-first ranking, stable tie-breaks, existing quotas, LONG replacement candidate, unchanged raw SHORT picks, later portfolio-capacity effects.
5. Fourteen targets exactly match authoritative frozen exit map; all initial stops1.25 and tighten1.00 once; stop already-crossed at tightening, broker modification uncertain/restart recovery, no loosening.
6. Trigger/entry expiry exact ten-minute boundary, post-confirmation first tradable event, quote absent expiry, adverse gaps, same-bar stop/target ambiguity,15:15 exit, same-time portfolio release/admission.
7. Frozen historical parity for all 102orders/93fills and all 46 days; candidate reason ledger and retained target/config hashes, daily/PAPER/LIVE same input prefix decision parity. Historical replay quality exclusions must be visible.
8. No broker mutation from offline/research/analysis/observer import; profile isolation in IDs, paths, reports, arm/kill state, locks, idempotency tags, and restart reconciliation.
9. Observer inference uses only completed past bars; outcome labels evaluated later; no strategy action changes from warning states; archived analysis remains reproducible.
10. Existing G regression suite retains G/G2 policy semantics and old artifact readers; new G3 cross-mode contract suites validate the new profile. No tests were run during this plan audit.

## File-by-file action inventory

Static Python import and literal source-reference closure of G/G2/G3 equity+options family, G3 research and analysis helpers, plus matching launchers/tests. Other team audits cover dashboard/config/docs/task files. New names are proposals, not created code.

| File | Proposed action | Reason |
|---|---|---|
| `ai_platform/observability/catalog.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/context.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/data_quality.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/execution_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `ai_platform/observability/feature_ledger.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/improvement_opportunities.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/journal.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/metrics.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/prospective_shadow.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/redaction.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/runtime.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/shadow_automation.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/strategy_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `ai_platform/observability/structured_logging.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `ai_platform/observability/tracing.py` | review/refactor | Retain shared observability; replace hardcoded G contract labels/gates with profile metadata where applicable; keep nontrading evidence/observer roles. |
| `backtesting_result_v13_v10_g_daily.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `build_v13_v10_g_document.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `data_for_backtesting_verify.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `eqidv2_runtime_paths.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_equity_fetch_1min.py` | refactor | Support G3 profile metadata/readiness while retaining shared durable completed-minute feed and20-bar warmup. |
| `fno_live_evidence.py` | refactor | Check profile-neutral signal/evidence source identities and fingerprints for G3. |
| `fno_live_profile.py` | refactor | Add explicit G3 routing; capability-based retained-equity checks must preserve all G operational protections. |
| `fno_oi_backtest_provenance.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_common.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_0925_0930_0935_v4.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_0925_0930_best_combo_v3.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_0925_0930_pf_v3.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_optimize.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_ema_confirm_sweep.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_fetch_5min.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_fetch_5min_fast_shadow.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_hybrid_data.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_oi_universe.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_options_atm_fetch_5min.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_corrected_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_corrected_ungated_oi_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_corrected_v2_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_corrected_v3_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_corrected_v5_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_a_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_b_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_c_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_d_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_d_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_e_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_f_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_g_2_0925_comparison.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_2_backtest.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_2_extend_results.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_2_long_leaders_backtest.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_2_relaxed_0925_backtest.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_2_unused_slots_data.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_2_unused_slots_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_3_backtest.py` | retain | Keep existing archive verification/export CLI behavior; use separate G3 dated/production entrypoint. |
| `fno_v13_v10_g_3l_capital_replay.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_g_backtest.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_daily_replay.py` | refactor | Extract profile-neutral snapshot/feature/coverage helpers; preserve historical G select_day_orders and production policy. |
| `fno_v13_v10_g_expansion_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_identity.py` | refactor | Reuse pure ID algorithm with independent G3 version/schema; preserve historical G IDs. |
| `fno_v13_v10_g_live.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_live_config.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_live_kite_session.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_options_backtest.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_chart.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_config.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_data.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_execution.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_fetch.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_fixed_profile.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_html.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_one_lot_execution.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_paper.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_projection.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_options_signals.py` | review/refactor | If existing options follower is in migration scope, parameterize equity-source profile/IDs/output roots; retain options execution economics and PAPER-only authority. |
| `fno_v13_v10_g_paper.py` | refactor | Extract config-independent locking/capacity/expiry core; keep G compatibility wrapper; use G3-specific contract for new book. |
| `fno_v13_v10_g_policy.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_projection_chart.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_g_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v10_g_selection.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_staged_replay.py` | retain | Preserve G semantics, profile identity and historical compatibility; production G3 uses independent adapter/core; shared pure helpers may be extracted under regression coverage. |
| `fno_v13_v10_g_two_bar.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v10_research.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `fno_v13_v5_derivative_data.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v6_portfolio_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v9_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v13_v9_data.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v5_hybrid_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v5_hybrid_optimize.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v5_live.py` | refactor | Accept immutable profile for selection, timing, stop policy, IDs, readiness, paper helpers and telemetry; preserve G branch/restart behavior. |
| `fno_v5_live_config.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v6_corrected_backtest.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v6_live.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v6_live_config.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `fno_v6_live_kite_session.py` | refactor | Profile-driven worker entrypoint, validation, tags and roots; retain quantity-one real-order guard and reconciliation. |
| `nse_market_calendar.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `research/__init__.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `research/g2_session_forensic_audit.py` | retain | Research/baseline dependency and evidence; preserve published G/G2 artifacts and behavior. Do not use research runners as live engines. |
| `research/g3_dated_replay.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_daywise_backtest.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_daywise_report.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_entry_preview.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_extend_results.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_freeze.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_followthrough.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_metrics.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_monitor.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_report.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_selective.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_selective_features.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_selective_report.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_pullback_tuning.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_results_report.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_sep24_raw_pool.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `research/g3_six_confirmation_variants.py` | retain | Keep frozen research/provenance and optional variants or observer analysis separate from production. Reuse reviewed pure helpers through a G3 analysis adapter; no implicit retraining or trading authority. |
| `tests/test_fno_v13_v10_d.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `tests/test_fno_v13_v10_g.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `tests/test_fno_v13_v10_g_expansion.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `tests/test_fno_v13_v10_g_morning.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `tests/test_fno_v13_v10_g_two_bar.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `tools/correct_v13_v10_g_cutoff_bundle.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/extend_v13_v10_g2_dashboard_history.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/extend_v13_v10_g_dated_results.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/refresh_v13_v10_g_research_source.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/v13_execution_research.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/v13_prospective_shadow.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/v13_shadow_automation.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `tools/v13_strategy_research.py` | refactor | Add explicit G3 source/profile/report outputs and truthful archive/new-session status while retaining old G output readers. |
| `wait_for_data_backtesting_ready.py` | retain | Shared historical/data/execution ancestor required by dependency closure; preserve math/data semantics and import contract. |
| `bat/migrate_backtesting_task_to_v13_v10_g.ps1` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/migrate_fno_v6_tasks_to_v13_v10_g.ps1` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_backtesting_result_v13_v10_g_1620.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_confirmation_1min.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_equity_1min_feed.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_live_kite_qty1.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_live_long.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_live_short.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_net_result.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_options_live_long.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_options_live_short.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_options_net_result.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_options_trade_logger.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_scanner_5min.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_fno_v13_v10_g_trade_logger.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_v13_shadow_prepare.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_v13_shadow_seal.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/run_v13_strategy_research_refresh.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/schedule_backtesting_result_v13_v10_g_weekday.bat` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/schedule_fno_v13_v10_g_options_weekday.ps1` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `bat/schedule_v13_shadow_weekday.ps1` | retain/add G3 sibling | Preserve historical G launcher; plan separate G3 launch/schedule definitions and later controlled cutover after approval. No tasks changed now. |
| `tests/test_backtesting_result_v13_v10_g_daily.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_backtesting_result_v13_v10_g_launcher.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_correct_v13_v10_g_cutoff_bundle.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_dashboard_fno_v13_v10_g.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_dashboard_fno_v13_v10_g_improvement_views.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_dashboard_fno_v13_v10_g_observability_views.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_dashboard_fno_v13_v10_g_options.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_dashboard_fno_v13_v10_g_research_views.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_extend_v13_v10_g_dated_results.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_equity_fetch_1min.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_oi_hybrid_data.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_2.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_2_0925_comparison.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_2_long_leaders.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_2_relaxed_0925.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_2_unused_slots.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_backtest_cli.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_daily_replay.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_expansion.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_launchers.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_live_execution.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_live_strategy.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_morning.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_options_backtest.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_options_data.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_options_execution.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_options_paper.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_options_projection.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_options_signals.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_paper.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_pipeline.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_production_replay.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_projection_chart.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_promotion.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_promotion_consumers.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_staged_runtime.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v13_v10_g_two_bar.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v5_live.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v6_live.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_fno_v6_live_kite_session.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_dated_replay.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_daywise_backtest.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_extend_results.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_pullback_followthrough.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_pullback_metrics.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_pullback_monitor.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_pullback_selective.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_pullback_selective_features.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_g3_pullback_tuning.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `tests/test_refresh_v13_v10_g_research_source.py` | retain/extend | Keep G/G2/G3 historical regression coverage; add independent G3 production cross-mode cases and shared-profile regressions. Do not rewrite frozen expected baselines to make migration pass. |
| `fno_v13_v10_g_3_strategy.py` | new proposed | Pure immutable G3 production contract and candidate decision core; explicit side thresholds, strict09:25 policy, normalized effective exits, stable ranking and semantic fingerprint. |
| `fno_v13_v10_g_3_live_config.py` | new proposed | Adapt pure G3 contract to shared runtime interface; separate G3 runtime identity, roots, flags and validation. |
| `fno_v13_v10_g_3_live.py` | new proposed | Canonical G3 scanner/confirmation/equity PAPER entrypoint with explicit profile. |
| `fno_v13_v10_g_3_live_kite_session.py` | new proposed | Canonical quantity-one G3 LIVE coordinator wrapper; retain broker safeguards. |
| `fno_v13_v10_g_3_daily_replay.py` | new proposed | Independent G3 daily reconstruction and prefix-causal selection using shared data helpers; source snapshot, coverage, decisions, selected/nofill ledger and manifests. |
| `backtesting_result_v13_v10_g_3_daily.py` | new proposed | Daily G3 publish/analysis coordinator with explicit IST date, immutable run bundle and truthful latest status. |
| `fno_v13_v10_g_3_analysis.py` | new proposed | Observer/report adapter for accepted G3 results and optional locked pullback inference; future outcome evaluation separate and no trading authority. |
| `tests/test_fno_v13_v10_g_3_strategy.py` | new proposed | Exact14-setup, boundary, strict09:25, causal volume/time, reranking and immutable configuration contract tests. |
| `tests/test_fno_v13_v10_g_3_cross_mode.py` | new proposed | Same-data live/PAPER/replay selection and policy parity; documented fill-clock differences, state isolation and G regressions. |
| `tests/test_fno_v13_v10_g_3_runtime.py` | new proposed | Readiness,10-minute expiry, staged stop, capacity/locks, restart/broker reconciliation and quantity-one guards with fakes only. |
| `tests/test_fno_v13_v10_g_3_daily.py` | new proposed | Completed-session snapshots, nofill/zero-trade/missing-data days, provenance publication and observer-only outputs. |

Only these audit artifacts were written. No implementation, activation, broker call, trading test, scheduled task mutation or backtest was performed.
