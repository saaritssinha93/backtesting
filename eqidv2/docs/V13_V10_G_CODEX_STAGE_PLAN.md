**V13-V10-G: stage-wise implementation using the current Codex setup**

Prepared: 17 September 2026. Status: implementation roadmap; all build stages below are proposed. Repository exploration, the architecture plan, and local CLI help/version checks have been completed. No chatbot, API service, scheduled agent, repair job, or model has been deployed by this planning work.

This is the current delivery roadmap. It supersedes the stage order and default AI-runtime assumption in the [earlier architecture plan](V13_V10_G_AI_PLATFORM_PLAN.md). That document remains the detailed reference for source contracts, API schemas, execution isolation, and strategy validation. Where the two differ, follow this roadmap's Codex-first feasibility checks and stage dependencies.

**A. The outcome we are building.** Use this Codex workspace to implement a local FastAPI service, reliable data tools, a dashboard assistant, reproducible research jobs, and eventually bounded specialist agents. The first useful release explains readiness, trade decisions, options rejections, and results using actual recorded evidence. Later stages coordinate experiments and selected data repairs.

There are three separate choices: using Codex to write and test the software; trying Codex as the local assistant's execution engine; and selecting a runtime suitable for continued use. The first is available now. The second needs a pilot. The third depends on authentication, quota, latency, isolation, and reliability results. FastAPI and the data tools remain useful regardless of the chosen AI backend.

Local help checks found Codex CLI `0.154.0-alpha.6.2`, noninteractive execution, JSON/schema output options, and App Server. These checks do not establish the current account's billing entitlement or prove sandbox enforcement. Official documentation supports scripted execution and saved authentication, but subscription access remains subject to the applicable account limits. See [noninteractive mode](https://learn.chatgpt.com/docs/non-interactive-mode) and [authentication](https://learn.chatgpt.com/docs/auth).

Codex App Server is an optional lab prototype only in this roadmap: current documentation marks the command/WebSocket transport experimental and unsupported for production workloads. It is not a required component or an execution dependency. See [App Server documentation](https://learn.chatgpt.com/docs/app-server).

**B. Delivery map and release boundaries.** Complete a stage's acceptance checks before enabling the dependent capability. A failed AI-runtime pilot does not block deterministic APIs, reconciliation, or ordinary report generation.

| Stage | Deliverable | Depends on | What becomes usable |
|---|---|---|---|
| 0 | Baseline inventory and Codex feasibility checklist | Existing repository | Agreed sources, identities, boundaries and build environment |
| 1 | Pure readers, schemas and evidence snapshots | 0 | Reliable structured facts without an LLM |
| 2 | Authenticated read-only FastAPI | 1 | Status, rules, traces, trades and result APIs |
| 3 | Bounded Codex adapter pilot with process supervision | 1, 2 | On-demand explanations over sanitized evidence |
| 4 | Local dashboard Assistant panel | 2; 3 for AI answers | First user-facing MVP; deterministic fallback available |
| 5 | Reconciliation and prospective evidence collection | 1, 2 | Daily comparisons and a trustworthy research dataset |
| 6 | Durable isolated research job service | 2, 5 | Reproducible backtests, comparisons and job tracking |
| 7 | Operations, research and review specialist workflows | 3, 5, 6 | Coordinated multi-agent investigations |
| 8 | Optional bounded data-repair actions | 5, 6 and action-specific checks | Approved or standing-policy repairs with verification |
| 9 | Reliability hardening and expanded local rollout | Each enabled capability | Supported operating routine, monitoring, backup and rollback |
| 10 | Optional predictive research and forward shadow tests | 5, 6; enough valid data | Evidence-based assessment of strategy challengers |

First MVP: stages 0-4. Research release: add 5-7. Operational automation: add 8 only for selected actions. Stage 9 hardens whatever is enabled; authentication, isolation, and rollback checks apply from the first exposure. Stage 10 has an independent market-data and evidence timeline.

**C. Architecture and ownership.** Preserve the existing dashboard on port 8787. Propose a separate loopback API on 8788, subject to checking the port. Codex invocations run behind a backend adapter and an owned-process supervisor. Heavy research uses a separate persistent worker.

| Layer | Owns | Does not own |
|---|---|---|
| Dashboard / Assistant panel | User context, answers, evidence and job progress | Model credentials or broker execution |
| FastAPI | Authentication, validation, source authorization, tools and jobs | Starting trading workers during API startup |
| Deterministic services | Freshness, decision traces, accounting and comparisons | Inventing missing evidence |
| Codex adapter | Bounded requests, approved context, structured explanations | Arbitrary access to the whole trading workspace |
| Specialist workflows | Investigation and research coordination | Risk-limit changes or autonomous strategy deployment |
| Research worker | Registered jobs and validated isolated artifacts | Production state or live order execution |
| Existing engines | Fetching, signal rules, sizing, order state and exits | Depending on chatbot availability |

Use a feature-flagged, route-allowlisted proxy for same-origin UI integration only after the public-tunnel boundary is verified. Otherwise serve the first assistant UI separately on authenticated loopback. A local tunnel's loopback peer address does not prove that the user is local. Do not automatically expose new assistant routes through the existing public link.

Codex is the implementation assistant. The user supplies business choices when they cannot be inferred, such as external spending limits or a newly requested operating scope. Existing authorization should carry forward; stage gates are checks, not repetitive permission prompts. This document itself requests planning, not deployment or changes to trading behavior.

**Stage 0. Establish the baseline and verify the build path.** Estimated effort: 1-2 engineering days. Goal: start with known identities, ownership and constraints.

Work:

- [ ] Record the relevant Python, Codex and dependency versions without changing global installations.
- [ ] Check whether a usable Git repository exists; otherwise establish a reviewed source-snapshot/versioning approach without moving production files.
- [ ] Inventory the producers, consumers, existing scheduled tasks, ports and report locations needed for G.
- [ ] Record equity LIVE quantity-one, equity PAPER, one-lot options PAPER, and three-lot historical options as separate profiles.
- [ ] Record the live 20-prior-minute versus research min5 confirmation policy difference.
- [ ] Record active configuration and source hashes; label reused-history results as historical references.
- [ ] Identify which modules have import-time writes and which entry points can overwrite canonical `latest` output.
- [ ] Inspect supported authentication/account status through documented interfaces; never print, copy into prompts, or inspect token contents.
- [ ] Define the permitted source roots, read-only MVP scope, pilot usage limit and fallback behavior.
- [ ] Record the existing dashboard/tunnel exposure and decide the initial authenticated local UI boundary.

Deliverables: proposed `docs/ai_platform/BASELINE.md`, `source_registry.json`, `profile_registry.json`, an import/write-path audit, and a sanitized fixture list. These are future build outputs, not files created by this roadmap.

Acceptance: each displayed mode has an authoritative source and profile; current service processes are distinguishable from proposed ones; no secret values appear in fixtures or documentation; the pilot has a clear pass/fail checklist. No assertion of unlimited or free unattended Codex access is made.

Example demonstration: explain, from source references, why an options worker named `live_long` is PAPER and why its one-lot results cannot be merged with the historical three-lot study.

Rollback: remove only the new inventory/build-environment artifacts if needed. No running service needs to change in this stage.

**Stage 1. Build trustworthy data readers.** Estimated effort: 3-4 days. Goal: produce correct facts before introducing model explanations.

Work:

- [ ] Implement pure adapters for runtime health, fetch readiness, strategy metadata, decision evidence, equity ledgers, options state and completed research results.
- [ ] Read status/report files directly through adapters; do not import runnable trading modules into the API.
- [ ] Normalize source IDs, session date, timezone, event time, source availability/observation time, profile, mode and configuration hash.
- [ ] Preserve COMPLETE, PARTIAL, STALE, UNAVAILABLE and integrity/schema failures as explicit states; distinguish a valid zero-trade session from missing data.
- [ ] Build coherent multi-file snapshots using source generation/hash checks and bounded retries.
- [ ] Resolve historical explanations against evidence available at the decision cutoff, not a later repaired marker.
- [ ] Implement deterministic counts, fees/P&L totals and capital-normalized diagnostics with explicit units and rounding.
- [ ] Add fixtures for normal, incomplete, stale, malformed, conflicting-generation and unresolved-exit cases.

Deliverables: `ai_platform/schemas/`, `adapters/`, `services/`, and sanitized fixture-based contract tests. Runtime metadata belongs under `C:/TradingData/eqidv2/ai_platform/`, outside the OneDrive workspace.

Acceptance: golden financial fixtures reconcile in the documented currency precision; no cross-mode aggregation is implicit; missing values remain unavailable; readers do not mutate canonical files or call brokers. Unsupported schemas fail explicitly.

Example demonstration: an EOD file marked RUNNING yields an incomplete/current status, not yesterday's result relabeled as today's. A missing option square-off quote produces UNRESOLVED, not a guessed close price.

Rollback: disable the adapter package. Existing producers and reports remain authoritative.

**Stage 2. Expose an authenticated read-only FastAPI service.** Estimated effort: 3-4 days. Goal: one stable interface for both a future chatbot and ordinary dashboard features.

Work:

- [ ] Create a separate pinned environment and application factory with no trading startup hooks.
- [ ] Add assistant authentication, server-owned principal/capability mapping, bounded requests and source authorization.
- [ ] Implement health, readiness, strategy, signal trace, trades, result summary and registered evidence routes.
- [ ] Use stable error codes and response envelopes; paginate and bound dates, symbols and payloads.
- [ ] Cache slow scheduler enumeration and large summaries; do not rescan the history for each request.
- [ ] Redact operational errors and source excerpts before exposing them to clients.
- [ ] Restrict documentation and sensitive endpoints to authenticated access.
- [ ] Verify the API cannot forward calls to legacy restart/kill routes or accept arbitrary filesystem paths.

Minimum routes: `GET /api/v1/health/live`, `/health/ready`, `/readiness`, `/strategies/v13-v10-g`, `/signals`, `/signals/{id}/trace`, `/trades`, `/results/summary`, and `/evidence/{source_id}`; assistant session endpoints are included. The detailed parameter/error contract is in the architecture plan.

Acceptance: fixture parity, authentication, source access, invalid input and path-boundary checks pass. Cached reads target p95 below one second on this machine, subject to measurement. API restart cannot spawn scanners or duplicate writers. Stopping the API leaves the existing dashboard and trading processes functional.

Example demonstration: request one specific signal trace and receive its recorded gates, observed timestamps, profile and source references as JSON. A legacy dashboard token cannot grant research/operator capabilities.

Rollback: stop only the identified new API process and disable any new proxy flag.

**Stage 3. Pilot Codex as a bounded explanation engine.** Estimated effort: 2-3 days. Goal: prove that the installed Codex can produce useful answers under acceptable limits before embedding it in the dashboard.

Work:

- [ ] Implement a backend interface such as `explain(request, evidence_bundle)`, independent of model vendor or transport.
- [ ] Begin with a fixed Codex executable and argument array, prompt via stdin, schema-validated final output and a dedicated working directory; never use `shell=True` or interpolate a user command.
- [ ] Give the model sanitized, bounded evidence bundles from stage 1/2. The initial version need not give Codex unrestricted API discovery or repository access.
- [ ] Test the installed read-only sandbox plus effective OS file, process, network, plugin, MCP and hook boundaries. Read-only alone does not prevent reading credentials or calling an external service.
- [ ] Use a restricted execution identity/configuration and approved data access. If containment cannot be demonstrated, keep explanations in this supervised Codex conversation and ship the deterministic API/UI while revising the adapter.
- [ ] Let Codex manage its supported authentication internally. Do not pass an auth file or secrets as model context, nor give the agent unnecessary broker/network access.
- [ ] Add a turn supervisor now: request ID, owned process creation identity, one active invocation initially, bounded queue, timeout/cancel, bounded output and interrupted/error states.
- [ ] Parse usage where available; handle login expiry, rate limits, quota exhaustion and process failures explicitly.
- [ ] Validate source IDs and critical numbers in the answer against the evidence bundle. Generate numerical tables from structured data where possible.
- [ ] Benchmark a small fixed set of pilot questions; separate prompt-development examples from held-out evaluation cases.

Deliverables: `assistant/backends/codex_exec.py`, backend interface, evidence-bundle builder, answer schema, turn supervisor and pilot report. Names are proposed. SDK adoption can follow if it improves workflow management; App Server stays lab-only under current support limitations.

Acceptance: a bounded invocation starts and terminates correctly; prohibited write/read/network/tool attempts are denied in the test environment; allowed evidence remains accessible; no authentication content reaches prompts/logs; usage failures return a usable error/factual fallback. A schema-valid answer still fails if its evidence or numerical claims are wrong.

Pin the executable version, effective authentication mode and runtime configuration as a tested unit at this stage. The currently observed executable is an alpha build. An executable upgrade, authentication/configuration change, or newly enabled plugin/MCP/hook invalidates the corresponding containment result and requires focused isolation/evaluation checks before the adapter is re-enabled. Stage 9 packages the already-tested configuration; it does not first establish this constraint.

Auth decision: the supported saved-login path may use account allowance subject to its limits. An API-key route is separately billable. Never silently switch to API billing when a subscription quota is exhausted. If the account route is unsuitable, record the finding and keep the deterministic release usable.

Example demonstration: the same evidence bundle yields a source-backed readiness explanation; an injected sentence in a log asking the agent to read credentials is ignored and cannot broaden its capabilities. Cancellation terminates only the owned Codex task.

Rollback: disable `AI_CODEX_ENABLED`; retain APIs, data views and manually generated summaries. An ongoing conversation here is not a persistent production service.

**Stage 4. Add the dashboard Assistant panel.** Estimated effort: 3-4 days. Goal: a usable local MVP.

Work:

- [ ] Add visible date, asset, mode and profile selectors, plus Readiness, Explain signal, Explain options rejection, and Summarize session shortcuts.
- [ ] Add chat turn creation/status routes backed by the stage 3 supervisor; use polling initially.
- [ ] Display factual answers with as-of time, completeness, profile and clickable authorized source excerpts.
- [ ] Ask for missing context only when it materially changes the answer; never merge modes to avoid a clarification.
- [ ] Show tool/invocation progress, cancellation and failure states without freezing existing cards.
- [ ] Provide deterministic results when Codex is unavailable or disabled.
- [ ] Introduce the same-origin allowlisted proxy only after the local/public routing boundary is verified; otherwise use the separate loopback UI.
- [ ] Test desktop/mobile layout, keyboard access, error messages, escaping and browser-write protections.

Deliverables: Assistant UI, authenticated narrow proxy where eligible, turn API, evidence viewer, and a 40-60-question versioned evaluation suite.

Acceptance: zero critical financial/profile errors in the release evaluation; all evaluated factual claims have real returned sources; missing evidence is acknowledged; target at least 95% correct context/tool selection. The UI distinguishes busy, failed, incomplete and complete. Normal-answer latency is benchmarked with a proposed target of 15 seconds, not assumed from CLI presence.

MVP demonstrations: the five questions from the earlier plan work end to end. Disabling Codex leaves readiness, ledgers, evidence and the existing trading dashboard usable.

Rollback: turn off the Assistant/proxy feature flag. No market-data or order-state migration is required.

**Stage 5. Reconcile execution and collect prospective evidence.** Estimated effort: 3-4 days for the initial implementation; collection then continues. Goal: explain differences and prepare a valid research dataset.

Work:

- [ ] Match signals, equity orders/fills, options eligibility/fills and exits by stable identities, session and profile.
- [ ] Resolve the documented preference for an actual LIVE equity fill over PAPER fallback; detect duplicate identities without deleting source evidence. Pending equity signals cannot initiate options entries, and the current options three-minute entry deadline remains explicit.
- [ ] Classify differences by source availability, config, rank/quota, confirmation, trigger/expiry, capacity, quote guards, latency, fees, slippage and exits.
- [ ] Show quantity-one equity LIVE, exposure-sized equity PAPER and one-lot options PAPER separately; label the three-lot historical study as a different reference.
- [ ] Distinguish the live 20-observation confirmation policy from research min5 when attributing mismatches.
- [ ] Create session briefs after source completion and data validation, with deduplicated incident summaries.
- [ ] Archive the eligible candidate universe and stage-by-stage decisions, including rejections and missing-input cases.
- [ ] Capture decision-time quote/contract metadata when available, and store post-entry labels/outcomes separately from causal features.
- [ ] Record unresolved/censored trades and uncovered contracts instead of assigning zero outcomes.

Deliverables: deterministic comparison service, matched and unmatched ledgers, attribution report, prospective schema and data-quality coverage report. Read-only collection may start earlier once its adapter contract is proven; adding producer instrumentation must not degrade trading deadlines.

Acceptance: selected known sessions reconcile to source ledgers; every mismatch is explained or explicitly unknown; costs/capital denominators are stated; later repairs cannot rewrite the original observation history. A complete no-trade day is distinguishable from an incomplete day. A fixture containing closed wins/losses, fees, an open option and an unresolved exit must reconcile reserved/released cash without treating open exposure as closed P&L. Daily realized drawdown and intraday mark-to-market drawdown are separate measurements, with unavailable marks disclosed.

Example demonstration: an equity fill followed by an options quote-window rejection is reported as a recorded execution exclusion. The system does not invent a historical option fill or treat the missed trade as a verified loss/profit.

Rollback: disable new collectors/summaries and retain existing immutable evidence. Preserve already collected research records.

**Stage 6. Build the durable research job service.** Estimated effort: 4-6 days. Goal: run controlled, reproducible experiments while preserving production state.

Work:

- [ ] Implement local SQLite job metadata, a separate Windows worker, exclusive worker identity, leases/heartbeat and fencing.
- [ ] Register only audited job types such as isolated replay, compare runs, data audit, and build research report.
- [ ] Validate an experiment specification: hypothesis, baseline, exact changes, source coverage, train/validation/test windows, execution/cost/capital assumptions, trial budget and selection rules.
- [ ] Freeze code/dependencies and materialize immutable data/config inputs before launch. A hash manifest alone is insufficient if the subprocess reads mutable files.
- [ ] Audit every imported/write path; verify a runner honors isolated roots and cannot overwrite production `latest` files.
- [ ] Give each attempt its own directory and publish only the current validated attempt's manifest.
- [ ] Add idempotent submission, owned-process cancellation, bounded concurrency/resources, timeouts and interrupted/orphan recovery.
- [ ] Keep one heavy job initially, normally outside market hours until load measurements justify more.
- [ ] Verify accounting, coverage, required artifacts and hashes after execution; zero process exit alone cannot mean research success.

Deliverables: job schema, registry, worker, `POST /research/jobs`, status/cancel endpoints, immutable experiment artifacts and a dashboard job card.

Acceptance: duplicate submission cannot start duplicate work; retries cannot use stale outputs; worker restart and API restart preserve correct state; cancellation cannot touch trading processes; denied output roots fail. A known baseline is reproducible from the frozen inputs and code.

Example demonstration: submit an isolated comparison, cancel it, retry it, and recover after a simulated worker interruption. Only a verified completed attempt is published, and canonical source hashes remain unchanged.

Rollback: disable new submissions, drain/cancel owned jobs and stop only the new worker. Keep metadata and artifact evidence for diagnosis.

**Stage 7. Introduce specialist agent workflows.** Estimated effort: 2-4 days after the tool/job foundation works. Goal: use multiple agents only where separation and parallel work improve the result.

Start with three roles behind one chatbot:

| Role | Allowed work | Output |
|---|---|---|
| Operations / trade analyst | Read health, evidence, traces and comparisons | Recorded causes, discrepancies, proposed next checks |
| Research agent | Draft validated experiment specs, submit user-requested permitted jobs, interpret results | Hypothesis, experiment references and results |
| Review agent | Inspect underlying artifacts and declared criteria in an independent context | Errors, limitations and evidence-based recommendation |

The application coordinates these roles. A dedicated conversational coordinator model is optional. Roles can run sequentially in one bounded worker; three roles do not require three simultaneous persistent processes or three subscriptions.

Extend the evidence-only pilot through a host-controlled tool loop: Codex returns a schema-validated proposed tool request; the backend checks the authenticated principal, role, arguments, budget and preconditions; the backend invokes the allowlisted service; sanitized results return as evidence for the next bounded step. This does not grant Codex unrestricted shell or network access. The review role has read/review capabilities and cannot submit research jobs or repairs; test that boundary explicitly.

Work:

- [ ] Route simple questions to one agent/tool path; avoid invoking the whole team for every question.
- [ ] Give each task a shared snapshot/profile identity, role-specific tool permissions and a common usage budget.
- [ ] Parallelize independent data and execution investigations only after load checks; run dependent experiments after their inputs are resolved.
- [ ] Give the reviewer the specification and raw artifacts, not only the research agent's conclusion.
- [ ] Add conflict handling, duplicate-job suppression, tool-step limits and clear termination conditions.
- [ ] Validate the final combined answer against sources and deterministic checks. Agent agreement is not statistical evidence.
- [ ] Compare answer quality, latency and usage with the single-agent baseline on matched tasks.

Acceptance: a multi-step investigation is demonstrably useful on the evaluation set; all roles preserve mode/profile/date; no permission is broadened through handoffs; a single failed specialist produces a partial report rather than fabricated agreement. Disable unnecessary delegation if its overhead adds no measured value.

Example demonstration: investigate options underperformance. Operations checks data quality; analysis attributes fill/exit effects; research proposes one bounded test; review identifies reused-history or unmatched-profile issues before a conclusion is presented.

Rollback: route everything back to the single assistant and deterministic tools; retain research job capability.

**Stage 8. Enable selected data-repair actions.** Optional, estimated 3-5 days for the first small action set. Goal: reduce repetitive operational work without creating competing production writers.

Start with an isolated historical-data audit/backfill workflow. Add canonical publishing or non-trading worker recovery only after separate action-specific tests.

Work:

- [ ] Produce a concrete plan containing exact resources, dates/contracts, preconditions, destination, limits, expiry and plan hash.
- [ ] Map each action to a narrow operator capability or an existing scoped standing authorization; the model cannot authorize itself.
- [ ] Revalidate preconditions before launch and acquire resource/producer locks.
- [ ] Invoke the existing audited fetcher/repair mechanism with its existing network retry policy; bound orchestration retries separately.
- [ ] Validate post-action coverage, metadata and integrity; preserve a partial outcome when history cannot be obtained.
- [ ] For canonical publication, require a tested single-writer publish/rollback procedure. An isolated cache result is not automatically production-ready.
- [ ] Record action results and notify in the dashboard; external messaging remains a separately authorized integration.

Deliverables: action registry, scoped plan/execution endpoints, resource locks, post-action validators, audit and rollback evidence.

Acceptance: stale plans are rejected; duplicates cannot spawn another canonical producer; allowed scope cannot expand through model input; unavailable historical data is never synthesized. Live executor startup, broker order placement/cancellation and risk/config edits are absent from the new agent action registry.

Example demonstration: repair a bounded isolated cache gap, verify the actual recovered bars, and report remaining gaps without weakening readiness thresholds.

Rollback: disable operational actions independently of read/chat/research features; restore only affected canonical artifacts through the action's tested rollback if publication occurred.

**Stage 9. Harden and complete local rollout.** Estimated 2-3 engineering days plus observed market sessions. Goal: a maintainable service with measured behavior. Security and exposure checks already apply at stages 2-4; this stage does not defer them.

Work:

- [ ] Pin the validated runtime versions and launch each new process with its own supervised, hidden Windows startup mechanism.
- [ ] Rehearse model/auth/quota failure, API crash, worker crash, disk exhaustion, concurrent-file changes and duplicate-worker launch.
- [ ] Add request/job metrics, latency/error reporting, queue depth, source freshness and usage summaries.
- [ ] Set independent flags for panel, Codex, research jobs, specialists, repairs and shadow models.
- [ ] Verify consistent SQLite backup/restore, migration compatibility and retention policy.
- [ ] Observe complete preopen, market, square-off and EOD cycles without missed existing deadlines attributable to the platform.
- [ ] Write a short operator runbook for login expiry, unavailable answers, stalled jobs, backup restore and rollback.
- [ ] Measure investigation time and unresolved discrepancies against the pre-build baseline.

Deliverables: validated launch definitions, operational metrics, backup/restore evidence, soak report and runbook.

Acceptance: deterministic dashboard/engine behavior survives all relevant new-service failures; no duplicate market-data or live execution processes are introduced; an operator can disable any new feature and recover jobs without touching unrelated processes. Observe several complete market sessions, with duration increased if failures occur. This is reliability evidence, not strategy-profitability evidence.

Rollback: disable exposure/features, stop new submissions, drain/cancel owned work, stop only identified platform processes, and restore compatible UI/config/metadata versions. Preserve audit and experiment artifacts.

**Stage 10. Research strategy improvements in shadow mode.** Optional; estimate 10-20 engineering days after data readiness, with prospective collection and forward validation taking additional time. Goal: determine whether a challenger adds value to the existing strategy.

Work:

- [ ] Freeze current G and the relevant execution profile as baseline; register each hypothesis and small parameter set before evaluation.
- [ ] Start with execution-delay, slippage/fees, one-lot options spread/quote-age and confirmation-policy attribution studies.
- [ ] Progress to simple regime filters, candidate meta-labels or ranking only when the candidate coverage and independent-session evidence are sufficient.
- [ ] Review the existing meta-label scaffold for G-specific feature/label compatibility; do not assume it is already integrated.
- [ ] Separate train, validation and untouched forward sessions; fit transformations/calibration only on training data; purge overlapping outcome windows where applicable.
- [ ] Keep decision-time features separate from post-entry labels. Group uncertainty by sessions; many candidates on one day are not independent market histories.
- [ ] Compare net expectancy, drawdown, opportunity retention, fill rate, costs, capital usage and concentration under realistic execution stress.
- [ ] Record every attempted variant and preserve insufficient-evidence results.
- [ ] Emit shadow scores and hypothetical decisions separately; do not let them alter real orders while unvalidated.

Deliverables: versioned candidate dataset, experiment registry, matched baseline/challenger ledgers, uncertainty/robustness report and forward shadow evaluation.

Acceptance for trading promotion is separate from implementing shadow scoring. Require causal data integrity, predeclared out-of-sample effect/risk criteria, execution realism, distributed evidence, forward paper consistency, versioned configuration and a rollback target. A separate requested deployment is needed to change trading behavior. If evidence is inconclusive, remain in shadow.

The retained equity study has 66 executions across 31 previously reviewed sessions. The historical options study has 20 executions, a different three-lot fill model and missing history. These observations are useful for development but cannot become untouched evidence through a new train/test label. References: [equity study](../V13_V10_G_IMPLEMENTATION_STATUS.md), [current options profile](../OPTIONS_V13_V10_G_PAPER_TRADING.md), and [options study limitations](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl30_target40p4/V13_V10_G_OPTIONS_FIXED_PROFILE_RESULTS.md).

Example demonstration: a promising historical challenger remains marked exploratory when the holdout was reused or live execution assumptions differ. The reviewer can return INSUFFICIENT_EVIDENCE without blocking the platform's operational use.

Rollback: disable shadow collection/scoring or revert its version; the baseline strategy continues unchanged unless a separately authorized promotion previously occurred.

**D. Budgets and cost decisions.** Building in this Codex workspace avoids a mandatory external contractor invoice; Codex usage, review time and testing remain real costs. No outsourced labour price is a required project fee.

The local deterministic API/database/worker design has no mandatory new cloud hosting subscription. Existing-machine capacity, electricity, backups and any new market-data requirements must still be measured. Codex subscription-backed runtime usage is conditional on supported account access and limits; do not promise zero incremental cost or unlimited use. A Codex CLI process using a hosted model is not offline local inference.

During stage 3, measure usage and latency over a fixed question set, with one active invocation and a bounded backlog. At stage 7, apply one total budget across all specialist calls. If supported account usage information is unavailable, cap requests/concurrency/time and report that precise currency spend is unknown; do not manufacture an API-dollar estimate for subscription credits.

If a separately billed API backend is selected, retain the earlier proposed initial allowance of Rs3,000/month as a configurable planning cap, not a vendor guarantee. Enforce application admission limits with headroom for in-flight calls and reconcile provider usage. Do not activate that paid fallback automatically after Codex quota exhaustion. Detailed current pricing should be checked when that backend is selected.

**E. Scheduling and dependencies.** The stage estimates total 12-17 engineering days for stages 0-4. Stages 0-9 total 26-39 days if the optional repair stage is included. These are planning allowances for one substantially full-time implementer using Codex, not a claim that coding agents will run continuously or a delivery guarantee. Re-estimate after baseline inventory and the runtime pilot; account access, source irregularities and machine capacity can alter the schedule.

Allow roughly 3-4 working weeks for the first MVP and 6-10 calendar weeks for the broader platform including integration and observation. Useful pieces can be delivered much earlier: verified data tools after stage 1 and usable read APIs after stage 2. Market observation and stage 10 validation cannot be compressed merely by adding coding agents.

Independent work can proceed in parallel after contracts stabilize: fixtures/adapters, authentication/API, and dashboard UI mocks. Stage 5 collection design starts during stage 1. Code-generation parallelism does not authorize concurrent changes to the same shared file or running competing production jobs.

Old-to-new mapping: earlier P0 maps to stages 0-1; P1 to 2; P2 to 3-4; P3 to 5; P4 to 6; multiple-agent delivery is now explicit in 7; P5 maps to 8; rollout is explicit in 9; P6 maps to 10.

**F. Progress tracking and the first build instruction.** During implementation, maintain a stage tracker with status, artifact/commit references, checks run, results, remaining issues and rollback state. Suggested status values: NOT_STARTED, IN_PROGRESS, READY_FOR_REVIEW, VERIFIED, ENABLED, or BLOCKED. Keep software completion, service enablement and strategy promotion distinct.

Each stage handoff should state what changed, why, how it was verified, any unresolved limitation, and the next dependency. Passing checks allows continued work within the user's authorized scope; it does not by itself broaden permissions to broker execution, external spending or strategy deployment.

The first concrete implementation batch is stage 0 followed by stage 1: establish the baseline/profile registry, collect sanitized fixtures, implement pure readers and prove their accounting/timestamp/mode behavior. Stage 2 then exposes those proven functions. Codex integration begins only after the facts it will explain are reliable.

Initial task specification for a future implementation turn:

> Implement stages 0 and 1 of this roadmap in isolated new files. Inventory the relevant sources and profiles; build pure read adapters and sanitized contract fixtures; verify exact accounting, evidence cutoff and mode separation. Record completed checks and remaining gaps. Do not start trading jobs, alter active strategy settings, read credential contents or deploy the chatbot as part of this batch.

The plan is complete as a roadmap. Implementation stages remain proposed until an implementation task is started.
