# Plan: replace active V13-V10-G with V13-V10-G-3

**Status (12 October 2026): G-3 migration and independent backtesting through 9 October completed. Both dashboards restarted and verified. Deployment is in PAPER validation mode; LIVE remains disabled.**

**Subsequent sizing amendment (12 October 2026):** the user selected ₹10,000 margin capital × 5 for G3 LIVE equity execution. See the [separate LIVE execution policy record](V13_V10_G3_LIVE_SIZING_2026-10-12.md). One-share references in the original plan below describe the migration baseline; accepted decisions, PAPER/backtests and historical provenance remain unchanged.

Prepared 11 October 2026 for the project at `C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2` and runtime data at `C:/TradingData/eqidv2`.

The objective is to make **G-3 the active strategy for signal generation, equity paper trading, equity live execution, scheduled backtesting, dashboard monitoring and analysis**. G and G-2 historical results remain correctly labelled and available for comparison. The migration should use one explicit G-3 decision contract across modes, with execution differences recorded honestly.

The companion [file manifest](V13_V10_G3_MIGRATION_FILE_MANIFEST_2026-10-11.md) inventories individual existing files, planned additions, scheduled tasks and runtime directories. Inclusion in that manifest means the file is covered by the migration review; it does not mean every file should be edited.

## Implementation record — 12 October 2026

The sections below this record retain the reviewed migration plan and pre-migration audit. This record describes the deployed state.

- Canonical contract: `fno_v13_v10_g_3_contract.py` and `configs/strategies/v13_v10_g_3_production_v1.json`; accepted variant `G3_W1_V1p1`, fingerprint `9761be9583ef63f4627bad43d3b67f6c5b12480e09a5d853325fe2b994e82275`.
- Active deployment: `configs/strategies/active_profile.json`, profile `V13_V10_G_3`, stage `PAPER_VALIDATION`, `live_activation_authorized=false`. Selecting a profile cannot grant LIVE authority.
- Shared runtime: explicit G3 wrappers and profile-aware scanner, causal confirmation, ranking, staged stop, paper portfolio, equity qty1 and options-PAPER adapters. Historic G and shared predecessor dependencies remain intact.
- Independent replay: `fno_v13_replay_source.py`, `fno_v13_v10_g_3_daily_replay.py`, `backtesting_result_v13_v10_g_3_daily.py`. These use immutable raw snapshots or hash-verified pre-selection feature/path bundles. Completed G trades are not selection or execution inputs. Source eligibility is disclosed separately from morning decision rules.
- Publication: immutable `production/runs` and `history/runs` generations under `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3`, with hash-bound `latest/current_run.json` and `latest/current_history.json`. Failed days cannot replace successful evidence. A successful future daily run incrementally rebuilds cumulative history.
- Main dashboard, Flow, monitor detail, cockpit, AI API and metrics readers route active operations to G3. Stable legacy card IDs remain compatible with saved dashboard pins. History remains labelled G-3, G-2, G. Flow defaults to a verified G3 history package and shows excluded dates and failed newer attempts explicitly.
- Analysis: G3 baseline, setup/stock/side attribution, decision-time regime buckets, selection funnel and chronological historical review use the canonical G3 ledger. G3 execution-realism diagnostics use G3 PAPER inputs. Prediction models, prospective shadow evidence and execution calibration remain pending; old G model evidence is not reused as G3 validation.
- Scheduler: 13 old G tasks disabled; 12 G3 input/PAPER/results/backtest tasks enabled; G3 qty1 LIVE task disabled. Task XML backups and rollback plan are in `.codex_tmp/g3_task_rollback_20261012_retry`. No task was started by migration. The first apply stopped before registration because the old XML omitted `Settings/Enabled`; the corrected helper validated all definitions before the successful retry.
- Both dashboards restarted, with authenticated HTTP checks passing on ports 8787 and 8790; the existing public tunnel was preserved. Unauthenticated requests return 401.

Operational commands from the project directory:

```powershell
# Readiness only: no workers, broker calls, task mutation or arming.
python fno_v13_v10_g_3_preflight.py

# An explicit closed session; automatically updates established cumulative history.
python backtesting_result_v13_v10_g_3_daily.py --date 2026-10-09

# Full available history, with unavailable sessions explicitly excluded.
python backtesting_result_v13_v10_g_3_daily.py --history-through 2026-10-09 --jobs 2

# Regenerate the active profile's read-only diagnostics.
python tools/v13_execution_research.py
python tools/v13_strategy_research.py
```

`fno_v13_v10_g_3_backtest.py` retains its frozen-archive verification/export meaning. It is not the production daily runner. Do not use the old G auto-arming BAT for preflight. LIVE rollout still requires three reconciled G3 PAPER sessions, broker/open-order review, separate LIVE approval and dated fingerprint-bound controls. No credentials, frozen reports or historical ledgers were archived or deleted.

Verification: 144 combined dashboard/contract/runtime tests, 85 API/execution/installer-guard tests and 14 producer tests passed after the final integration changes. Earlier focused checks include exact 88-selection parity across 5,622 sealed candidates, 241 runtime/legacy tests plus 11 subtests, 135 PowerShell mock assertions and in-memory validation of all 13 real task XMLs, 123 legacy dashboard tests and 29 JavaScript tests. Counts overlap across focused and combined runs and should not be added together. The final 548-file dependency review found no remaining concrete routing gaps; four legacy scheduling installers now refuse to recreate G tasks while the persisted active profile is G3.

### Recomputed backtest through 9 October 2026

| Measure | Result |
| --- | ---: |
| Covered period | 29 July–9 October 2026 |
| Usable sessions | 49 |
| Selected orders | 108 |
| Filled portfolio trades | 99 |
| Wins / losses | 68 / 31 |
| Gross P&L | INR 276,914.67 |
| Modeled costs | INR 24,750.00 |
| Net P&L after modeled costs | INR 252,164.67 |
| Win rate | 68.69% |
| Profit factor | 2.7662 |
| Maximum daily-close drawdown | INR 14,142.14 |
| Round-trip cost estimate | 5 basis points |

The 9 October daily replay verified 213/213 eligible stocks and produced zero selected orders and zero fills. That valid zero-trade session remains in the cumulative denominator. Missing source sessions are excluded explicitly: 24 and 25 August (near-month source availability) and 14 September (missing exact 09:20 NIFTY context). Stock-level OI exclusions within otherwise usable sessions are listed in the immutable coverage evidence.

The new history contains an independently reconstructed 1 October session: 212/213 stocks eligible, one MCX `0951_SHORT` fill, net INR 4,400. The archived 46-session and 48-session packages excluded that date. Reconciliation therefore compares their exact calendars separately and reports the additional day, rather than asserting that the 49-session total equals either older package. These are historical modeled fills; prospective G3 PAPER and LIVE outcomes remain separate evidence.

The exact 46-session comparison passed for all 102 selected orders / 93 fills; the extended 48-session comparison passed for all 98 fills. Matched entry/exit prices, timestamps and per-trade net P&L have zero measured differences. The complete candidate decisions were recomputed from pre-selection features; prior G trade results were not used as execution inputs. The retained 38-session sealed source bundle and 11 independently reconstructed raw sessions have separate coverage provenance. Raw coverage records nine excluded stock-days across seven otherwise usable dates.

Final canonical history: `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/history/runs/2026-10-09/20261012T004438234536_52798a14`, manifest SHA-256 `55e4f930697f609a1d8e4262b1e22fa0b2b47e8f0a125ea6b54149807bf247eb`. The second immutable history generation exercised incremental publication while preserving the first generation and the daily run. Human-readable summary: `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/latest/latest_history.md`.

The execution and strategy-analysis commands completed against this exact manifest. The timestamp audit found zero violations and no missing timestamps across 108 selected rows. G3 PAPER calibration has zero observed completed orders; its status is `AWAITING_G3_PAPER_CALIBRATION`, with path stresses `NOT_RUN`. Prediction/shadow reports remain `AWAITING_G3_PROSPECTIVE_EVIDENCE`; they do not imply completed validation.

Both dashboard listeners were restarted at 00:45 IST on 12 October. Authenticated local and public HTTP checks passed; unauthenticated local access returned 401. Flow defaults to the verified 49-session G3 history and reconciles trades, net/gross P&L, costs and daily-close drawdown. The main backtesting card reads the verified daily package, and its scheduled task resolves to `EQIDV2_backtesting_result_v13_v10_g_3_1620`. The existing public tunnel was preserved. No trading worker, broker request or LIVE arm operation was started during the restart.

Final evidence is saved under `.codex_tmp/g3_migration_plan_20261011/`: `production_backtest_evidence.json`, `implementation_files.json`, `final_dependency_audit.json`, `deployed_tasks_audit.json`, `final_dashboard_restart.json` and `running_dashboard_checks.json`. The implementation file manifest records 88 concrete reviewed files in addition to the original 548-file dependency inventory. No archive operation was performed.

## 1. What the audit established

| Finding | Consequence for the plan |
| --- | --- |
| `fno_v13_v10_g_live.py` selects the G profile and delegates to `fno_v5_live.py`; the G Kite wrapper delegates to `fno_v6_live_kite_session.py`. | A change to the G wrapper alone cannot migrate the actual scanner, confirmation, order lifecycle, stop management or recovery logic. |
| `fno_live_profile.py` currently accepts G for the V6 generation. Shared runtime branches use G-specific policy, identity and configuration checks. | Add an explicit G-3 profile and pass a profile contract through shared code; preserve the existing G behavior for rollback and historical tests. |
| `fno_v13_v10_g_3_backtest.py` defaults to verifying/exporting a frozen archive. | It cannot replace the weekday daily backtest task. A real G-3 daily producer and publisher are required. |
| `research/g3_dated_replay.py` consumes a completed G daily feature ledger and sealed snapshot; `tools/run_g3_dated_once.py` waits for that source. | Decouple source preparation from G trading decisions before retiring the active G daily producer. |
| Current production G includes a relaxed 09:25 LONG policy from 6 October. Accepted G-3 does not include that relaxation. | Copying the current G runtime and only reducing the confirmation threshold would create a different strategy. |
| The G-3 frozen config's donor `setup_rules` retain older stops; effective exits are in `exit.setups`. | Normalize configuration with explicit field precedence. Do not accidentally use donor stops or G-only reward/risk validation. |
| Main dashboard identities, data paths, cards, logs, task/restart actions and analysis defaults still target G. | Producers and every active consumer must switch together. A label change is insufficient. |
| Dashboard Flow already offers G-3, G-2, G, but discovers files by CSV presence and run-name ordering. | Add verified active/history pointers so an incomplete run, research export or one-day replay cannot replace the intended default history. |
| Options paper trading consumes G equity orders with exact identity/fingerprint checks. | Cover the follower's source routing and attribution explicitly; preserve its separate execution rules and PAPER mode. |
| Legacy V5/V6 and older G predecessors remain real dependencies. | Keep them in the dependency map and regression checks; versioned filenames are not evidence that files are unused. |

Evidence: `fno_live_profile.py`, `fno_v13_v10_g_live.py`, `fno_v13_v10_g_live_kite_session.py`, `fno_v5_live.py`, `fno_v6_live_kite_session.py`, `fno_v13_v10_g_policy.py`, `fno_v13_v10_g_3_backtest.py`, `research/g3_freeze.py`, `research/g3_dated_replay.py`, `log_dashboard_server.py`, `dashboard_flow.py`, and the configured scheduler actions.

## 2. Proposed target: accepted G-3, with explicit deployment boundaries

Use **`G3_W1_V1p1`**, sourced from:

`C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1`

The frozen research identity is `V13-V10-G-3-FROZEN-LONG110-NEXTMINUTE-V1`. Create a separately validated production identity/profile, proposed as `V13_V10_G_3` / `V13-V10-G-3-PRODUCTION-V1`, referencing the frozen baseline hash. Keep the archive's `execution_authority=False` and frozen metadata unchanged. A production profile must earn its own validation and activation state.

| Rule or boundary | Proposed G-3 behavior |
| --- | --- |
| Universe and source prices | Retain the accepted dated stock-future OI/equity-price universe and dated NIFTY-future gate. Validate universe, instruments, calendar, timezone and source provenance. |
| Setup family | Preserve all 14 accepted setup slots, side/time rules, quotas, F-core-first selection and G SHORT expansion. Export these explicitly into the normalized production contract. |
| Confirmation clock | Exactly the next completed one-minute candle after the five-minute signal. Preserve the causal data cutoff. A late-arriving exact candle is handled under a defined freshness/deadline rule; it does not permit using a +2/+3 candle instead. |
| Confirmation volume | LONG ratio at least **1.10**; SHORT at least **1.20**. Implement explicit thresholds, not the research shim that rescales LONG volume ratios. |
| Confirmation shape | Preserve side, OHLC validity, nonzero range, close-through-signal-price and setup-specific body/wick conditions. The volume denominator excludes the current candle. |
| 09:25 LONG | Use accepted strict G-3 conditions, including EMA ordering; exclude the later G relaxed-slot addition. Any future hybrid needs a different identity and evidence. |
| Ranking | Re-evaluate and rank the entire eligible pool. Lowering the LONG threshold may replace selections; it is not simply an append-only list of additional trades. |
| Entry and expiry | Stop trigger at confirmation high for LONG / low for SHORT, retained ten-minute trigger window, explicit tick rounding and order expiry. |
| Stops and targets | Initial stop **1.25%**, tightened once to **1.00% after 120 minutes**. Use effective `exit.setups` targets; preserve no-partial/no-breakeven behavior. G-3 rules apply to the G-3 profile rather than G's 6 October promotion date. |
| Session exit | Preserve the accepted **15:15 IST** time exit and calendar/session handling. |
| Research/PAPER sizing | Preserve the accepted reference capital of INR 100,000 per filled entry, 5x target exposure and INR 1,000,000 portfolio capital, with its existing occupancy rules. Confirm effective PAPER configuration during implementation. |
| LIVE sizing | Preserve the current **quantity-one** deployment constraint, broker safeguards and account limits. This plan does not propose increasing live capital or quantity. |
| Costs | Preserve the archived five-basis-point round-trip estimate where applicable. Keep modeled costs separate from actual broker charges. |
| Data quality | Retain the operational requirement for 20 valid prior completed one-minute observations; verify the accepted book satisfies it. Research's mathematical minimum of five observations must not silently weaken production readiness. |
| Pullback analysis | Keep observer warnings, eventual-outcome labels and followthrough analysis outside entry/exit authority. They do not modify G-3 orders or stops. |
| Options | Migrate the existing PAPER follower's equity-source identity and reporting after parity checks. Do not infer new option rules or enable options LIVE from an equity-strategy migration. |

There are two important limits to cross-mode equality:

- Bar replay starts the staged-stop clock from the entry candle's end; LIVE/PAPER use actual fill time. Keep these semantics explicit and test boundary behavior. An OHLC bar cannot establish the exact tick sequence or actual broker fill.
- Historical full-day coverage is an evidence-quality requirement. Morning live selection may only use information available then. Later missing bars or future pullback outcomes must never become live entry filters.

The frozen acceptance book contains **46 sessions, 102 selected orders and 93 executed trades** through 7 October, with 1 October excluded. The later Flow history through 9 October contains **48 sessions and 98 executed trades**. These are distinct validation packages; do not replace one set of acceptance counts with the other. Neither is an untouched out-of-sample performance test.

## 3. Target structure

Use a pure, versioned strategy contract and decision layer, with separate adapters for input collection and execution:

`verified market inputs -> canonical G-3 features/selection -> versioned order intent -> PAPER simulator / LIVE broker coordinator / historical simulator -> versioned ledger -> dashboard and analysis`

The decision layer should be import-safe: no directory creation, environment mutation, network access, broker calls or process startup on import. Avoid global monkeypatches or altering G configuration in place. Shared legacy functions may be parameterized, while historical G defaults remain unchanged.

Every signal, order, run and analysis package should carry the profile, strategy version, config fingerprint, schema version, source/cutoff identity, session date, mode and run ID. Validate these at producer/consumer boundaries. A G success record must not count as a healthy G-3 component. Unknown or inconsistent identities should produce a visible blocked/mismatch state.

Keep three distinct concepts: **promoted active strategy**, **latest complete daily run**, and **accepted cumulative history**. Research previews, archive exports and partial runs must have separate run kinds.

## 4. Implementation phases and acceptance gates

### Phase 0 — establish the migration baseline

1. Confirm the target assumptions in section 2 and record all 14 effective setup rules and their source hashes.
2. Snapshot source/config fingerprints, current task actions/triggers, active registry/paths, dashboard mappings and current runtime-state inventory. Preserve existing uncommitted work, especially `v14/` and its tests, in a separate change boundary.
3. Record baseline test results without fixing unrelated failures under the migration label.
4. Diagnose existing nonzero scheduled-task results before evaluating G-3 readiness. The 11 October read-only snapshot showed result `2` for several G scanner/feed/confirmation/PAPER/logger tasks; G quantity-one LIVE and the daily G backtest showed result `0`. A task being enabled does not prove healthy operation.
5. Inventory all entry routes, including canonical G launchers, V6/V11 compatibility wrappers, preopen autofix, supervisor recovery, dashboard restart buttons and one-off jobs.

**Gate:** reviewed contract, named file/task/data inventory, protected historical sources and a reproducible rollback snapshot. Planning approval alone does not arm live trading.

### Phase 1 — implement and verify the canonical G-3 contract

1. Add the G-3 production profile, config normalizer and pure confirmation/selection/stop-policy adapters. Explicitly resolve donor setup metadata versus effective exit metadata.
2. Preserve strict 09:25 behavior, side-specific confirmation thresholds, stable ranking/quota rules and G-3 staged stops.
3. Extend profile routing and strategy identity handling. Make shared V5/V6 branches use explicit profile capabilities rather than assuming every modern profile is G.
4. Preserve signal-ID determinism while assigning a distinct G-3 namespace. Separate display labels, profile identity and broker-safe order tags.
5. Add boundary tests for 1.10/1.20, +1-minute timing, 09:25 relaxation exclusion, quota reranking, stop clocks, effective targets, input freshness and malformed/missing data.

**Gate:** full candidate/selection parity on available accepted inputs, including all 102 selected orders and nine unfilled orders, plus execution/accounting parity for the 93 fills. If the archived candidate universe is incomplete, report that limit and reconstruct a verified source before claiming full selector equivalence. Verifying the frozen export alone is insufficient.

### Phase 2 — build an independent G-3 daily backtest and publisher

1. Extract strategy-neutral source validation, sealed snapshot preparation and feature-ledger generation from the current G daily flow. G-3 must not require a successful G trade replay as its source.
2. Add a real date-aware G-3 replay and daily runner. Keep the existing frozen G-3 CLI's archive/export semantics intact; do not silently redefine it as production.
3. Produce immutable dated runs, decision/rejection ledgers, selected orders, executed trades, daily/monthly summaries, input manifests, code/config fingerprints, data-coverage reports and validation results.
4. Create independent G-3 `status`, `latest`, `reports`, verification and heartbeat outputs. Publish current pointers atomically only after every required validation succeeds.
5. Distinguish `RUNNING`, complete success, blocked input, failure, true zero-trade day and excluded/missing session. Keep a last-success record with its actual date when a later run fails.
6. Maintain an accepted cumulative-history pointer separately from daily results. Appending a newly verified day must preserve prior history and deduplicate by date/run identity.
7. Validate archived 46-session parity and separately the retained 48-session extension through 9 October. Preserve the documented 1 October gap; missing evidence is not a zero-trade session.

**Gate:** the new runner handles a valid unseen date, a legitimate no-trade day and a blocked-data day without depending on G publication. Its ledgers reconcile and its failure paths cannot promote partial results.

### Phase 3 — route PAPER and downstream consumers to G-3

1. Add G-3 scanner, one-minute feed/confirmation, LONG/SHORT PAPER, trade logger and net-result launchers. Preserve their actual modes: existing `live_long`/`live_short` G filenames currently launch PAPER workers.
2. Use the shared decision contract with G-3-specific runtime roots, locks, snapshot manifests, heartbeat/status, identity/fingerprint checks and ledger writers.
3. Ensure restart/recovery, expiry, duplicate suppression, trade IDs, fill timestamps, staged stops, costs and end-of-day accounting all retain the same G-3 identity.
4. Adapt the options PAPER follower's source profile/root and ledger attribution. Keep option instrument choice, lot/cash rules, entry/exit logic and PAPER-only protection separately versioned and unchanged unless separately specified.
5. Run G-3 in an isolated shadow/PAPER cohort and compare decisions with the canonical replay on the same observable inputs. Record real-time latency, late data and fill-model differences instead of masking them as parity.

**Gate:** clean complete-session PAPER evidence and exercised restart/duplicate/stale-input failure cases. A proposed minimum is three complete trading sessions, extended until the required cases have evidence; calendar duration alone is not sufficient. Synthetic fixtures should cover rare cases that did not occur naturally.

### Phase 4 — migrate both dashboards and analysis

1. Main dashboard: switch active profile identity, cards, expected versions, roots, reports, logs, task names, heartbeats, freshness thresholds, timeline sources and restart allowlists together.
2. Preserve legacy API routes only as documented compatibility adapters; return truthful strategy/mode metadata. Ensure a legacy route cannot restart G while the active dashboard presents G-3.
3. Flow: retain **G-3, G-2, G** order and G-3 default; use verified production/history manifests instead of choosing by folder name alone. Label current daily, cumulative history and archived/research evidence distinctly. Never silently substitute G when active G-3 data is unavailable.
4. Preserve Flow's existing visual design, top-of-main-dashboard link, new-tab behavior, phone access, authentication, responsive layout and read-only endpoints. Avoid coupling this strategy migration to a visual redesign.
5. Update analysis adapters, registries, source discovery, feature ledgers, reconciliation, profitability, strategy/execution research, improvement views, shadow preparation/sealing and observability labels for G-3.
6. Create new G-3 analysis/calibration cohorts. Old G models, projections or static P&L charts must not be relabelled as G-3 results. Expose insufficient evidence instead of reusing an incompatible model silently.
7. Keep historical G/G-2 reports and all frozen G-3 research reproducible. Pullback observer features and future outcome labels remain separate from execution.

**Gate:** dashboard/API fixtures, run-selection tests, reconciliation tests and browser checks for active, stale, failed, blocked, historical, PAPER and disarmed LIVE states pass. Filtered metrics, charts and CSV exports must agree on the same ledger and calendar. Authentication and public relative links remain intact.

### Phase 5 — prepare scheduler and live cutover

1. Create a dry-run migration command that prints exact old/new task actions, profiles, roots and rollback actions. Export the existing relevant task definitions before changing them; keep any credential-bearing task data local.
2. Register G-3 tasks initially disabled. Build a separate readiness-only route for disarmed checks. The existing quantity-one BAT unconditionally passes `--auto-arm` and rejects extra arguments, so it must never be used as a preflight by appending an assumed disarm flag. The new preflight must be unable to arm, place orders or spawn trading workers. Preserve trading calendars, trigger order, start delays, retry policy and working directories for the separately approved activation route.
3. Update preopen healthcheck/autofix, supervisor process matching, log/status naming and dashboard restart actions so recovery cannot revive the old G entry path after cutover.
4. Replace the recurring G daily backtest only after Phase 2 removes its source dependency. Cover the research refresh and shadow finalization commands chained after it.
5. Reconcile G open/pending broker orders and positions. Prefer an end-of-session transition with no old pending entries/open positions. If any remain, keep their G ownership and management until an explicit transition resolves them; do not relabel their stops or simply kill their manager.
6. Preserve the existing quantity-one LIVE guard, authentication/token handling, kill switch, broker reconciliation, timeout and emergency-exit behavior. Do not copy an old G arm file into G-3.
7. Use one account-level real-entry ownership guard so G and G-3 cannot independently submit duplicate live entries. Any temporary exit-only manager for old positions must be explicitly scoped.

**Gate:** approved implementation, successful technical/PAPER gates, known broker state and explicit authorization for G-3 live activation. The audit found the next regular G tasks scheduled for Monday 12 October at 09:15 IST, and the daily backtest at 16:20; the existing G-3 one-off has no next run. This plan does not change those schedules or promise a Monday promotion.

### Phase 6 — controlled activation, monitoring and rollback readiness

1. During the approved maintenance window, disable old G new-entry authority, switch the active registry/task actions, verify G-3 fingerprints end to end and confirm dashboard routing.
2. Use the PAPER services and separate readiness-only preflight to verify freshness, broker/session health and the active identity before the explicitly approved LIVE arm step. Do not invoke an auto-arming launcher for this check.
3. Observe the first live session's signal-to-order trace, rejection reasons, actual fills, stop adjustments, duplicate guards, time exits and ledger reconciliation. Keep quantity one.
4. Publish a migration evidence report listing exact changed files, task definitions, profile hashes, tests, accepted deviations, activation time and rollback state.

**Gate:** one active real-entry owner, no version/path leakage, complete current-day reconciliation and operational acceptance. Historical backtest profit alone does not satisfy this gate.

## 5. Concrete file work packages

The companion manifest expands these groups into individual paths and proposed actions.

| Work package | Principal existing files | Planned action |
| --- | --- | --- |
| Profile/config/identity | `fno_live_profile.py`; `fno_v13_v10_g_live_config.py`; `fno_v13_v10_g_policy.py`; `fno_v13_v10_g_selection.py`; `fno_v13_v10_g_identity.py` | Add a normalized G-3 contract and profile; parameterize shared interfaces without changing historical G defaults. |
| Signal and PAPER engines | `fno_v5_live.py`; `fno_v6_live.py`; `fno_v6_live_config.py`; `fno_v13_v10_g_live.py`; `fno_v13_v10_g_paper.py`; `fno_equity_fetch_1min.py` | Route profile/config explicitly; cover candidate/confirmation checks, trade state, locks, stops, feed freshness and loggers. |
| Broker execution | `fno_v6_live_kite_session.py`; `fno_v13_v10_g_live_kite_session.py`; shared broker/reconciliation helpers identified in the manifest | Add G-3 coordinator/wiring; preserve quantity-one safeguards and reconciliation; isolate state and arming. |
| Replay/publication | `fno_v13_v10_g_daily_replay.py`; `fno_v13_v10_g_staged_replay.py`; `backtesting_result_v13_v10_g_daily.py`; data readiness/verification helpers | Reuse strategy-neutral source/publisher components and add independent G-3 production entrypoints. |
| Frozen reference | `fno_v13_v10_g_3_backtest.py`; `research/g3_freeze.py`; `research/g3_six_confirmation_variants.py`; `research/g3_dated_replay.py`; G-3 report/observer modules | Preserve archived behavior; use as evidence/reference. Extract reviewed logic rather than execute research scripts as live workers. |
| Options PAPER | `fno_v13_v10_g_options_config.py`; `fno_v13_v10_g_options_paper.py`; `fno_v13_v10_g_options_signals.py`; options data/execution/report modules | Make the source equity profile configurable, maintain independent option rules and explicit PAPER identity. |
| Main dashboard | `log_dashboard_server.py`; `dashboard_fno_monitor.js`; `fno_eq_id_monitor_detail.py`; `dashboard_ops_state.js` | Switch all active paths/identities/actions together; validate freshness and mode. |
| Dashboard Flow | `dashboard_flow.py`, `.js`, `.html`; `dashboard_flow_core.js`; `dashboard_flow_compare.js`; `dashboard_flow_chart.js`; `tools/run_dashboard_flow.py` | Add verified run-kind/current selection; preserve layout, filters, comparisons, exports and routing. CSS remains a regression surface unless a needed status display requires an edit. |
| Analysis/platform | `ai_platform/adapters/`; `ai_platform/api/`; `ai_platform/observability/`; `tools/v13_*`; profile/source registries and observability config | Add G-3 identity/source/cohort support; review every inventoried consumer, preserving schema/security helpers where compatible. |
| Scheduling/operations | G BAT/PS1 files; V6/V11 compatibility launchers; `preopen_session_healthcheck.py`; `preopen_session_autofix.py`; supervision and authentication/export helpers | New G-3 launch/schedule wiring and recovery paths; protect common infrastructure and credentials. |
| Reports/docs/tests | operational Markdown, historical web reports, projections, G/G2/G3 and shared-runtime tests, browser/JS tests | Update active runbooks, add G-3 tests/fixtures and reporting; preserve original historical reports and unrelated V14 work. |

Proposed new filenames are listed explicitly in the manifest. They are implementation proposals, not files already created or a requirement to duplicate shared logic.

## 6. Runtime data and history policy

- Keep existing `fno_oi/v13_v10_g_live`, legacy V6 controls, G/G-2 research bundles, G daily snapshots and frozen G-3 files at their current paths. Some older output folders are current input dependencies.
- Proposed active G-3 equity runtime root: `C:/TradingData/eqidv2/fno_oi/v13_v10_g_3_live/`, with separate signals, PAPER state, LIVE state, locks, reports, status and session manifests. Exact subpaths must be centralized in the production profile.
- Proposed G-3 controls should have their own profile identity and authorization state, with the existing global emergency stop continuing to dominate. Migrate control readers and dashboard actions together.
- Extend `C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/` with `production/runs/<date>/<run-id>/`, `history/runs/<through-date>/<run-id>/`, `status.json`, `latest/`, versioned `reports/<date>/<run-id>/` and verification outputs. Existing `runs/` historical packages remain unchanged and are identified by run kind. Shared `runtime_status` and configured log directories stay outside this publication root and use the runtime path helper.
- Create versioned G-3 options PAPER and analysis/research output roots if those consumers are migrated. Never pool G and G-3 live/PAPER rows into one unattributed ledger.
- Preserve raw market data, instrument masters, token caches, API text files, authentication scripts, Kite exports, logs and dependencies. No credential rotation, copying credentials into reports, OS ACL change, folder relocation or archiving is part of this migration plan.
- The earlier archive-candidate document remains a separate proposal awaiting future approval. Migration dependencies must be rechecked against it before any later archive operation.

## 7. Required verification matrix

| Area | Required evidence |
| --- | --- |
| Config normalization | All 14 setups match effective accepted rules; effective exit precedence is tested; wrong config/hash/profile rejects cleanly. |
| Decision parity | Complete candidate decisions, core/expansion ranking, quotas, selected orders, rejects and nofills compared by stable identity, not only totals. |
| Threshold/timing | LONG below/at/above 1.10, SHORT below/at/above 1.20; exact +1 candle; duplicate/late/missing candles; no current-volume contamination; no relaxed-slot leakage. |
| Execution | Trigger expiry, gaps, tick rounding, partial broker fills, stop/target ambiguity, staged-stop boundaries, time exit and recovery are tested against each mode's stated observable semantics. |
| Capital/accounting | Capacity conflicts, simultaneous slots, released capital, costs, gross/net P&L, daily/monthly totals and recorded calendars reconcile. LIVE quantity-one differences remain explicit. |
| Restart/safety | No duplicate entry after process restart; single real-entry owner; wrong-day/fingerprint/arm state blocks entry; kill switch and old-position management remain effective. |
| Daily publication | Independent raw-source preparation; same-date rerun/idempotency; atomic publish; stale/partial/missing data cannot become success or zero-trade output. |
| Dashboard/API | Active G-3 cannot display old G status as healthy; restart action resolves G-3; run kinds/defaults correct; auth and path allowlists unchanged. |
| Flow | G-3/G-2/G order, historical selection, common-calendar comparison, missing dates, ledger filters, charts and CSV export remain consistent. |
| Analysis/options | Fingerprints and source equity IDs agree; no cross-version training/calibration leakage; observer outcomes cannot place/modify orders; options remains PAPER. |
| Regression | Historical G/G-2/frozen G-3 hash/behavior checks; shared V5/V6 tests; unrelated dashboard strategies and existing V14 work retain their behavior. |
| Operations | Dry-run/export/apply/rollback task mapping verified; readiness-only checks cannot auto-arm; preopen/autofix/supervisor cannot resurrect the wrong profile; nonzero task results are explained against each launcher's timing and exit-code contract. |

Run targeted tests per work package, then the required integration suite once the combined changes are ready. Use temporary data roots and fake brokers for automated tests. No strategy import or test should accidentally use real order authority.

## 8. Rollback procedure

Rollback is a controlled restoration of entry authority and routing, not deletion of G-3 files or a blind process restart.

1. Disarm G-3 new entries and preserve all G-3 logs, state, task exports and manifests.
2. Query/reconcile outstanding broker orders and positions. Cancel/finish/manage them through the owning strategy's approved controls; do not abandon exit management or restore G on top of unknown positions.
3. Restore the exported G task actions, approved active profile and dashboard mappings after the ownership condition is satisfied. Restore only the relevant integration changes, preserving unrelated work.
4. Verify G identity, data readiness, task health, control state and broker reconciliation before any approved re-arm.
5. Keep failed G-3 evidence as a labelled run for diagnosis. Historical G/G-2 and frozen G-3 sources should not require restoration because they were never overwritten.

Triggers include unexplained decision differences, wrong-version data accepted as G-3, duplicate live entry authority, stale/invalid input admitted, stop/expiry regressions, accounting discrepancies or broken broker reconciliation. During an incident, preserving correct exit management takes precedence over rapidly restarting old entry workers.

## 9. Review decisions and completion definition

The proposed defaults for review are: exact accepted `G3_W1_V1p1`; strict 09:25 LONG; +1-minute confirmation only; pullbacks remain analysis; existing live quantity one; operational 20-bar readiness; options source migration remains PAPER; historical G/G-2 and all frozen evidence stay intact; phased PAPER validation before separately authorized live activation.

Implementation is complete when the canonical G-3 logic drives every approved active producer and consumer, the daily backtest is independent, current pointers/status/analysis are truthful, both dashboards use the right version, recovery and scheduler paths cannot revert silently, all verification gates pass and the documented rollback works. A changed dashboard title, a successful frozen export or matching aggregate P&L is not sufficient.

## 10. Audit scope and limitations

This plan is based on current source text, Python import relationships, launcher/config references, small run metadata, existing provenance inventories and a sanitized read-only Windows task snapshot. No engine was imported or executed to determine the plan, and no backtest, broker action, scheduler change or service restart was performed.

The source inventory includes direct references, transitive dependencies and reverse consumers. Dynamic environment values, external executables, broker state and future file/task changes require a fresh pre-cutover check. The manifest identifies review/preserve surfaces as well as expected edits; it is not an instruction to rewrite every listed file. The audit describes observed coverage rather than claiming static analysis can prove every possible runtime dependency.
