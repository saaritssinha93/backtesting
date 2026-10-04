**V13-V10-G: FastAPI, chatbot, agentic workflows, and strategy research implementation plan**

Prepared: 17 September 2026. Status: proposed implementation plan; this document does not deploy services or change trading behavior. Scope: the local EQIDV2 dashboard, equity G workflow, options paper workflow, data operations, and research. Estimates assume one engineer working substantially full time, with an AI coding assistant and access to the existing machine.

Current delivery roadmap: [Stage-wise implementation using the current Codex setup](V13_V10_G_CODEX_STAGE_PLAN.md). Follow that roadmap for stage order, estimates and AI-runtime selection; it supersedes those assumptions below. This document remains the detailed architecture reference. The revised path pilots bounded Codex execution before choosing a runtime; a separately billed model API is optional, and Codex App Server is restricted to a lab prototype under its current support status.

**1. Outcome and implementation order.** Build a local assistant that can explain system readiness, trace trade decisions, reconcile execution results, and coordinate reproducible research. Use the existing trading and fetching code as the source of behavior. Add a FastAPI service, a grounded chatbot, a persistent job worker, and a small set of bounded agent workflows.

The delivery order is: source contracts and evidence -> read-only API -> dashboard chatbot -> reconciliation and monitoring -> isolated research jobs -> selected operational actions -> predictive-model experiments after sufficient prospective data exists.

The first useful release must answer these questions accurately:

- Is the G pipeline ready for the requested session, and which dependency is blocking it?
- Why was a stock selected, rejected, or left without an entry?
- Why did an equity fill produce, or fail to produce, an options entry?
- What did equity LIVE, equity PAPER, options PAPER, and historical replay each do?
- Which figures are final, provisional, incomplete, or unavailable, and which evidence supports them?

Measure operational gains separately from strategy gains. Faster diagnosis, correct explanations, and reproducible experiments are platform outcomes. Higher net expectancy or lower drawdown are research outcomes that must be demonstrated on new evidence.

**2. Current system and facts the implementation must preserve.** These findings come from the repository and saved reports reviewed on the preparation date. The dashboard URL returned HTTP 401 during direct inspection; its routes and behavior were inspected in source. No claim is made that every dashboard card or process was healthy at that instant.

| Area | Existing behavior or evidence | Consequence for this plan |
|---|---|---|
| Dashboard | `log_dashboard_server.py` serves 8787 through `ThreadingHTTPServer`; routes include snapshot, log, restart, and kill | Extend incrementally; preserve existing operating controls |
| Equity G | Equity price/volume/indicators plus futures OI; five-minute gates, exact one-minute confirmation, ranking, quotas, breakout entry | Explanations must follow the actual decision chain |
| G selection | Retains F core selections, then fills vacant quota with the selected SHORT price-threshold relaxation | A new ranking model would be a strategy change requiring research |
| Live/research confirmation | Live requires 20 prior one-minute observations; the research denominator accepts min5 | Reconciliation must label this deliberate policy difference |
| Options runtime | One-lot PAPER buying of ATM CE for filled equity LONG and ATM PE for filled equity SHORT; LIVE equity fill preferred, PAPER fallback | A worker name containing `live` must not be interpreted as real options trading |
| Options exits | Current documented paper profile uses 30% premium stop, 40.4% target, and 15:15 IST square-off | Resolve the active profile fingerprint at read time; do not infer it from a report name |
| Historical options study | Three lots, five-minute execution proxy, 20 completed trades; missing history and reconstructed metadata are disclosed | Keep it separate from one-lot guarded-quote paper observations |
| Equity reference | 66 executions over 31 sessions in the retained historical study, with reused history | This is an exploratory reference, not a fresh test or today's result |
| Operations | Scheduled tasks, retry/backoff, coverage gates, health checks, and EOD validation already exist | AI diagnoses and coordinates these mechanisms |
| Evidence | Append-only decision evidence records observation time and payload hashes | Use this for deadline-specific explanations |

The active historical G reference leaves the morning-slot and two-bar extensions disabled after failed quality criteria. Their implementation in the repository does not imply deployment or promotion.

Primary local references: [G configuration](../fno_v13_v10_g_live_config.py), [G implementation/evidence notes](../V13_V10_G_IMPLEMENTATION_STATUS.md), [options runtime notes](../OPTIONS_V13_V10_G_PAPER_TRADING.md), [evidence archive](../fno_live_evidence.py), and [daily replay](../backtesting_result_v13_v10_g_daily.py).

The historical options limitations are recorded in the [fixed-profile report](C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl30_target40p4/V13_V10_G_OPTIONS_FIXED_PROFILE_RESULTS.md). Forty triggered source orders lack August-expiry option metadata/history. Missing data must remain missing in every API, explanation, and experiment.

**3. Architecture and deployment boundary.** Keep the existing dashboard on 127.0.0.1:8787. Propose 127.0.0.1:8788 for a separate API service after checking availability. The model is accessed by the local backend; the model provider does not need an inbound connection to the local dashboard.

| Component | Responsibility | Runs or stores data where |
|---|---|---|
| Existing dashboard | Existing cards and controls; new Assistant panel | Existing 8787 process |
| Narrow dashboard proxy | Forward only enumerated assistant API routes after user authentication | Existing dashboard, behind a feature flag |
| FastAPI application | Validate requests, authenticate callers, query services, expose jobs/chat | Separate loopback process on proposed 8788 |
| Read adapters | Normalize status, evidence, CSV/JSON/Parquet records and reports | API process, with bounded reads and caching |
| Deterministic analytics | P&L, costs, comparisons, freshness and decision traces | Shared service modules; expensive work in worker |
| Chat orchestration | Retrieve facts, request allowed tools, validate outputs, cite sources | API-side application code with provider adapter |
| Job worker | Claim and execute registered research/repair jobs | Separate Windows process; one heavy job initially |
| Platform metadata | Job state, source registry, chat references, audit events | Local SQLite under `C:/TradingData/eqidv2/ai_platform/` |
| Research artifacts | Immutable inputs/manifests, outputs, validation and reports | Per-job directories under the same runtime root |
| Existing trading/fetching engines | Market-data collection, deterministic entry/exit, orders and current supervision | Existing processes and scheduled tasks |

The browser uses the same origin for the assistant panel and a dedicated authenticated assistant session. The proxy has an exact route/method allowlist, bounded bodies, timeouts, and a dedicated server credential for the sidecar. It must not become a general URL proxy or forward trading-control endpoints. The sidecar validates that credential and derives the user's role from a verified server mapping, never a browser-supplied role header. Possession of the legacy dashboard shared/query token must not grant researcher/operator privileges or become the sidecar credential. MVP can use polling for chat/job progress; streaming is optional after the proxy behavior is tested.

Use one API process and one separate worker initially. Keep scheduling and compute execution outside API startup/lifespan hooks so an API reload cannot start another scanner, fetcher, or executor. The chatbot and API being unavailable must not stop the trading processes.

FastAPI provides typed validation and OpenAPI documentation; application code must implement the authorization and data semantics. See [FastAPI features](https://fastapi.tiangolo.com/features/). Heavy research work belongs in a separate worker, consistent with the [background-task guidance](https://fastapi.tiangolo.com/tutorial/background-tasks/#caveat).

**4. Proposed repository and runtime layout.** This is a proposed layout, not a list of files already created.

```text
ai_platform/
  app.py                      API application factory; no trading side effects
  settings.py                 validated settings and allowed runtime roots
  schemas/                    response, signal, trade, job, chat contracts
  api/                        health, readiness, signals, trades, chat, jobs
  adapters/                   pure readers for existing artifacts
  services/                   freshness, traces, reconciliation, analytics
  knowledge/                  approved document index and retrieval
  assistant/                  model provider, tool registry, answer checks
  jobs/                       registry, queue, worker, leases, cancellation
  policy/                     capabilities, limits, scoped standing rules
  storage/                    metadata schema, migrations, repositories
  audit/                      redaction and append-only event emission
  evals/                      curated questions and expected evidence
configs/
  ai_platform.example.toml    documented settings; no secrets
tests/ai_platform/            contract, integration, recovery and eval checks
bat/
  run_ai_platform_api.bat     proposed hidden supervised service launcher
  run_ai_platform_worker.bat  proposed worker launcher
docs/
  V13_V10_G_AI_PLATFORM_PLAN.md
```

```text
C:/TradingData/eqidv2/ai_platform/
  metadata/platform.sqlite3
  knowledge/index/
  cache/
  audit/
  jobs/<job_id>/
    request.json
    inputs/manifest.json
    logs/
    outputs/
    validation.json
    report.md
  backups/
```

Keep the metadata database, locks, and active job artifacts outside the OneDrive-synced repository, matching the intent of [runtime path handling](../eqidv2_runtime_paths.py). Use a dedicated Python environment and pin dependencies after compatibility checks with the installed interpreter. Initial choices: FastAPI, Uvicorn, Pydantic, the selected model SDK, existing compatible data libraries, and SQLite. Use lexical document search first; add embeddings only if retrieval evaluation demonstrates a need. Postgres or a distributed queue can follow measured multi-machine requirements.

**5. Source adapters and evidence contracts.** Build pure readers rather than importing runnable strategy modules into the API. Some existing imports create directories or establish execution profiles. Audit imports and any proposed reusable functions before calling them.

| Adapter | Source family | Normalized output |
|---|---|---|
| Runtime health | Status/heartbeat files and cached scheduler snapshots | Process state, last observation, blocker, session identity |
| Data readiness | Fetch markers, quality reports, dated instrument metadata | Expected/received coverage, cutoff, errors, freshness |
| G strategy | Attested active configuration and strategy payload | Setup definitions, thresholds, policy differences, fingerprint |
| Signal evidence | Original decision envelopes and selected/pending state | Ordered gate decisions with observation timestamps |
| Equity trades | PAPER and quantity-one LIVE ledgers/state | Orders, fills, quantities, realized/unrealized result |
| Options trades | One-lot paper state and quote decision records | Parent equity source, contract, quote checks, entry/exit status |
| Historical results | Run manifests, validation files, completed reports | Modeled metrics and coverage with run/profile identity |
| Knowledge documents | Explicitly registered code/docs/research descriptions | Versioned passages with source references |

Each adapter has a declared schema version and fixture examples for complete, stale, missing, malformed, and changed-schema inputs. If an input contract changes, return an explicit unsupported/incomplete state rather than guessing column meanings.

Every data response includes these semantics:

| Field | Meaning |
|---|---|
| `schema_version` | Version of the normalized API response contract |
| `session_date`, `timezone` | Requested trading session and Asia/Kolkata |
| `as_of`, `snapshot_id` | When the answer's data snapshot was assembled |
| `data_cutoff`, `source_observed_at` | Market information cutoff and when the consumer actually saw it |
| `strategy_version`, `config_hash`, `profile_id` | Exact strategy and execution profile |
| `asset_class` | EQUITY or OPTION |
| `execution_mode` | LIVE, PAPER, or SIMULATED |
| `run_kind` | LIVE_SESSION, PAPER_SESSION, HISTORICAL_REPLAY, or RESEARCH |
| `run_id`, `signal_id`, `order_id`, `parent_signal_id` | Applicable stable identities |
| `quality_state`, `issues` | COMPLETE, PARTIAL, STALE, UNAVAILABLE, INTEGRITY_FAILED, or SCHEMA_UNSUPPORTED |
| `sources` | Opaque source IDs, version/hash, observed time, evidence kind |

Keep per-record mode and profile even when displaying a comparison. An overall result cannot silently combine live and paper capital or realized and modeled P&L. Record source equity mode on each option trade. Resolve the effective active configuration for the session, not merely the newest file modification time.

For multiple files, build a source-version manifest; verify relevant generations/hashes remained consistent during the read. Retry a bounded number of times or return PARTIAL when concurrent updates prevent a coherent snapshot. A mutable latest marker is not proof of what was available at an earlier deadline. Preserve event time, publication/availability time, and ingestion/observation time separately.

Return monetary amounts as decimal strings or integer paise with explicit currency; state whether percentages use percent units or fractions. Declare the capital denominator, lot size, leverage, fees, slippage and drawdown method for every performance comparison. Missing values are null with reasons, never fabricated zeroes.

Existing per-stage freshness and market-calendar policies are authoritative. Before market open, during trading, after close, and on holidays require different interpretations. An alive process, fresh data, and strategy readiness are distinct facts. Cache slow scheduler enumeration and large file summaries; do not enumerate scheduled tasks or scan the full history on every chat tool request.

**6. API contract.** The endpoints below are proposed new versioned routes. Existing `/api/*` routes remain separately implemented until any deliberate later migration.

| Method and route | Inputs and result | Earliest phase |
|---|---|---|
| `POST /api/v1/auth/session` | Authenticate assistant principal; issue protected session cookie | P1 |
| `DELETE /api/v1/auth/session` | Revoke the assistant session | P1 |
| `GET /api/v1/auth/me` | Effective principal and capabilities | P1 |
| `GET /api/v1/health/live` | Process liveness only; no sensitive inventory | P1 |
| `GET /api/v1/health/ready` | API storage/adapters readiness, distinct from market readiness | P1 |
| `GET /api/v1/readiness` | Session date -> dependency states and blockers | P1 |
| `GET /api/v1/strategies/v13-v10-g` | Session/profile -> attested rules and source references | P1 |
| `GET /api/v1/signals` | Date, side, symbol, setup, profile; bounded pagination | P1 |
| `GET /api/v1/signals/{signal_id}/trace` | Session/run context -> recorded decision stages | P1 |
| `GET /api/v1/trades` | Session, instrument, mode, profile, status -> typed records | P1 |
| `GET /api/v1/results/summary` | Exact run/profile/period -> deterministic metrics and completeness | P1 |
| `GET /api/v1/evidence/{source_id}` | Authorized source ID -> redacted registered excerpt | P1 |
| `POST /api/v1/chat/turns` | Conversation, question, selected context -> turn ID/status | P2 |
| `GET /api/v1/chat/turns/{turn_id}` | Answer, evidence, tool status or interruption/error | P2 |
| `POST /api/v1/comparisons` | Explicit runs/modes -> cached comparison or queued job | P3/P4 |
| `POST /api/v1/research/jobs` | Validated experiment specification -> 202 and job ID | P4 |
| `GET /api/v1/jobs/{job_id}` | State, progress, heartbeat, artifacts and validation | P4 |
| `POST /api/v1/jobs/{job_id}/cancel` | Cooperative cancellation of that job only | P4 |
| `POST /api/v1/operations/plans` | Requested repair -> scoped action plan, no execution | P5 |
| `POST /api/v1/operations/plans/{plan_id}/execute` | Authorized plan hash -> bounded job | P5 |

Reject unknown fields where appropriate, unsupported profile/mode combinations, invalid dates, and oversized requests. Use bounded time ranges and cursor pagination. Do not accept a raw filesystem path, shell command, Python snippet, or unrestricted SQL query as an API argument. A source ID resolves through a server-owned registry and allowed roots.

Errors have stable codes, a request ID, retryability, and a useful explanation. Examples: `SOURCE_STALE`, `EVIDENCE_NOT_ARCHIVED`, `PROFILE_MISMATCH`, `INCOMPLETE_OPTION_HISTORY`, `JOB_ALREADY_EXISTS`, `ACTION_NOT_PERMITTED`, and `MODEL_UNAVAILABLE`. Application failures must not return HTTP success with invented data. API documentation is authenticated except any deliberately minimal liveness endpoint.

**7. Chatbot behavior and tool design.** The first chatbot is an evidence-backed interface to deterministic services. It receives the selected date, mode, and profile from the UI and must state any additional assumption it makes. If a missing date/profile would materially change the answer, it asks a short clarification instead of merging incompatible results.

Initial tools: `get_pipeline_health`, `get_data_coverage`, `get_strategy_profile`, `list_signals`, `explain_signal`, `list_trades`, `get_result_summary`, and `search_approved_docs`. Add `compare_runs` once reconciliation is validated. Each tool has a narrow JSON schema, time/resource limits, role checks, and a source-bearing response.

The flow is: user question -> context resolution -> permitted tool selection -> server validation -> deterministic retrieval/calculation -> model explanation -> citation and numeric-field checks -> displayed answer with evidence. OpenAI's Responses API function calling supports this application-executed tool pattern; see the [official guide](https://developers.openai.com/api/docs/guides/function-calling). Keep provider access behind an interface so provider/model selection can follow measured evaluation rather than becoming embedded throughout the codebase.

Knowledge retrieval indexes only approved strategy documents, selected code descriptions, and registered research manifests. It excludes broker/API credential files, browser profiles, unrelated personal files, raw environment dumps, and unrestricted logs. Initially use a local document manifest and lexical search with version filters. Structured numeric questions use typed query tools, not document embeddings.

Every answer distinguishes recorded fact, calculation, inference, and proposed experiment. Source links open authorized excerpts in the dashboard. Validate that cited IDs were returned for that turn and that critical amounts/counts match tool results; generate financial tables directly from structured output where feasible. If a reason was not recorded, say so. A reconstructed explanation must be labeled reconstructed and cannot claim decision-time certainty.

Example question: "Why was there no option after this equity entry?" Retrieve the equity fill and source mode, frozen contract selection, option entry window, quote guard outcomes and paper state. Report the recorded rejection or unresolved condition; do not infer a profitable missed trade from a later candle.

Example question: "Are we ready today?" Present the current dependency status and oldest relevant observation. Show which stage is blocked and its upstream cause. Do not equate a green process heartbeat with valid trade inputs.

If the model is unavailable, deterministic readiness, ledgers, reports and ordinary dashboard controls continue to work. Offer a factual fallback response assembled from tool output. Conversation history is contextual memory, not authoritative market data. A new question about current state refreshes its sources.

**8. Agent workflows.** Start with one orchestrator and a small tool registry. The roles below can share infrastructure; separate model services or agent-to-agent conversations are unnecessary for the first release.

| Workflow | Trigger | Permitted sequence | Output and stop condition |
|---|---|---|---|
| Readiness analyst | User request or existing preopen check completion | Read health and data coverage; traverse recorded dependencies; explain blockers | One report per meaningful state change; stop after bounded reads |
| Data investigator | QC failure, stale source, missing contract history | Inspect registered QC reports and evidence; classify; create repair plan | Exact affected dates/contracts/files and unresolved causes |
| Trade explainer | User opens a signal/trade | Fetch trace and execution state; compare recorded gate outcomes | Source-backed explanation or explicit evidence gap |
| Reconciliation analyst | Completed EOD artifacts or user request | Match identities; classify mismatches; compute scoped deltas | Verified comparison with coverage and unresolved exceptions |
| Research coordinator | User asks for an experiment | Build experiment spec; validate permitted parameters/budget; submit isolated job; compare completed results | Reproducible report; no active-config edit |
| Operations coordinator | Enabled repair policy and qualifying incident | Revalidate plan; acquire locks; invoke registered action; verify result | Success, partial success, or escalation after retry budget |

Use deduplication, cooldowns, maximum tool steps, deadlines and cost caps. Suggested initial limits to benchmark: 8 tool calls and a 60-second wall-time budget per interactive investigation; at most one recovery retry for a registered action unless its underlying fetcher already owns retries. Long jobs return a job ID and complete asynchronously. Count workflow retries separately from existing network retries to prevent multiplying attempts.

Autonomous monitoring should be triggered by state changes or scheduled summaries, not by an LLM call for every tick. Fixed threshold checks stay in code; the model explains incidents. Dashboard notifications are part of scope. External messages require a separately configured destination and user-authorized channel.

**9. Permissions and action policy.** Enforce permissions in backend code for every tool and endpoint. The model cannot grant itself a capability. Initial deployment enables reads and disables operational writes until their individual tests pass.

| Action | Initial behavior | Later permitted behavior |
|---|---|---|
| Read registered data, explain, compare | Allowed for authenticated viewer | Same, within query/resource limits |
| Submit isolated allowed research | Disabled until P4 | Research role; user's requested experiment within its validated budget can run |
| Backfill isolated study cache | Plan only | Operator/standing policy scoped to dates, contracts, limits, and destination |
| Repair canonical production data | Plan only | Separately tested publish procedure with exclusive writer lock and integrity validation |
| Restart a non-trading worker | Existing manual workflow | Optional narrowly registered recovery after orphan/duplicate checks |
| Restart/arm live executor, place/cancel broker orders | Not exposed to chatbot or new agents | Outside this plan; existing deterministic/manual controls remain authoritative |
| Change active strategy, risk, sizing, SL/target | Produce research proposal only | Separate reviewed deployment after research gates |
| Run arbitrary shell, install code, read secrets | Not available as model tools | Not part of this platform |

A repair plan records action ID, inputs, affected resources, output root, estimated work, preconditions, expiry, and plan hash. Execution rechecks current preconditions and the caller's capabilities. Standing authorization can cover routine actions within fixed limits; do not require repetitive approval for every already-authorized retry. Expired or materially changed plans require a new plan. This document specifies future product behavior and does not execute or authorize any live action now.

Keep broker credentials accessible only to existing trusted execution/data processes. The model receives redacted tool results. Put any model key and sidecar service credential in server-managed secret settings, never browser JavaScript, URLs, tracked files or conversational context. Configure authenticated sessions, CSRF/origin protection for browser writes, restrictive cross-origin behavior, and HTTPS before remote access. Public-link/tunnel ingress must deny the assistant UI/API routes until remote assistant access is deliberately enabled and tested. A loopback peer address is not proof of local user access when a local tunnel forwards requests. If the current tunnel cannot enforce this routing boundary, serve the first assistant UI separately on the authenticated loopback sidecar until the boundary is implemented. No legacy token is accepted as an assistant write credential.

Retrieved documents and logs are data, not instructions. Test prompt-injection attempts in those sources, path traversal, forged source IDs, and requests to call existing kill/restart routes. Keep an audit trail of user identity, tool/action, validated arguments, policy decision, input/output references and status; do not log credentials or hidden model reasoning.

**10. Persistent jobs and Windows operations.** Use a separate worker with a SQLite-backed queue on the local disk for the initial single-machine deployment. Transactions and constraints govern job submission and claiming. Enable a tested durability/backup configuration; a raw copy of a live WAL database is not the backup procedure.

Job states: `QUEUED -> VALIDATING -> RUNNING -> VERIFYING -> SUCCEEDED`, with explicit `BLOCKED`, `FAILED`, `CANCEL_REQUESTED`, `CANCELLED`, and `INTERRUPTED` branches. Partial results remain marked incomplete. Model chat turns use a separate lightweight lifecycle; interrupted requests can be retried without blocking heavy work.

Each job stores the requesting principal, typed specification, idempotency key, code version/hash, data/config manifest, expected output root, timestamps, worker lease/heartbeat, process identity, progress and validation result. Repeated submission with the same key and same content returns the original job. Reusing a key with different content is a conflict.

Before launch, materialize the exact data/config inputs into an immutable snapshot and execute a frozen code revision or sanitized source snapshot with recorded dependency versions. A manifest alone does not freeze files or imports in the actively edited workspace. Verify the copied inputs against their expected hashes and ensure the registered runner resolves all sources to that snapshot. Block the job if this cannot be established. Avoid writable hardlinks to mutable production files, and retain sufficient inputs or a durable immutable source reference for reproduction.

Run only registered job types with an argument array and `shell=False`. Validate numeric ranges, dates, profile IDs, allowed source roots, and output destinations. Sanitize the child environment and bound CPU, memory, duration, disk usage, and concurrent requests. Start with one heavy research job and avoid its expensive stages during market hours until resource measurements establish a safe budget.

Do not assume passing `--output-root` makes a legacy script isolated: inspect all writes, imported side effects, environment-based roots, and latest-report updates. A job is eligible only after a temporary-root test proves that it cannot write canonical production state. Register wrappers where needed.

Use leases plus a fencing token to prevent stale workers from publishing success. For a lost worker, check the recorded process creation identity and output state before retry; PID alone is insufficient. Unknown/orphaned processes put the job into `INTERRUPTED` until reconciled. Do not claim exactly-once execution; design idempotent work and a single validated publish step. Cancellation targets only the recorded child process tree after identity verification, never generic trading processes.

Write artifacts to per-job staging locations such as `jobs/<job_id>/attempt_<n>/`, validate them, then publish a manifest atomically. Tie the fencing token to that attempt; only the current validated attempt may become the published job result. A nonzero exit code is failure; zero exit alone is not proof of valid results. Check required files, coverage, accounting, hashes and experiment-specific invariants. Retain failed-job evidence and isolate retries from earlier partial output. Use hidden Windows launches and dedicated service/task identities where practical; each worker launcher has an exclusive instance lock.

**11. Reconciliation and analytics.** This is the bridge between operational improvement and strategy research. Start with a small set of completed sessions and produce a deterministic report before asking a model to summarize it.

Match by session, strategy fingerprint, signal ID, setup and instrument. Preserve parent equity signal and fill identity for options. Distinguish missing identities from true unmatched trades; never use symbol alone to merge records. Report raw price/fill differences, quantity differences and after-cost results separately.

Classify mismatches into: source-data availability, contract mapping, configuration policy, signal selection/rank/quota, one-minute confirmation, entry deadline/trigger, capacity/cash, spread/liquidity/quote freshness, partial/unfilled order, slippage, fee model, exit semantics, and unknown. Reconcile one stage at a time; an earlier mismatch can explain later differences.

Compare one-lot options paper with a replay configured for that same profile whenever possible. The saved three-lot study remains a separately labeled historical reference. Equity quantity-one LIVE and exposure-sized PAPER require explicit quantity/capital normalization; show actual rupees and normalized diagnostics without implying equal deployable returns.

Track realized P&L, unrealized exposure, charges, entry/exit lag, fill rate, rejection counts and unresolved positions. Report both daily realized drawdown and intraday mark-to-market drawdown only where the required marks exist; otherwise the latter is unavailable. No daily loss explanation can silently assume a missing square-off price.

**12. Research program and data collection.** Freeze the current G rules and execution assumptions as the baseline, and begin prospective collection immediately during implementation. Preserve all eligible candidates and rejections, not only completed winners and losers.

Candidate fields should include session/slot, symbol, dated futures/option identity where applicable, side/setup, strategy/config hash, core/expansion classification, native rank and quota, equity EMA relationships, directional price change, futures OI change, volume ratios and denominators, one-minute body/wick/direction, NIFTY gate input, each recorded gate result, event/available/observed timestamps, missing-data flags and raw source references. Preserve pre-selection candidate coverage so absence can be distinguished from rejection.

Execution fields add trigger/expiry, observed equity fill, options source preference, strike/expiry/lot/tick metadata and observation time, best bid/ask and depth when actually captured, spread, quote age, guards, fees, capital reservations, exits and unresolved states. Store post-entry outcomes and labels separately from decision-time feature snapshots; a later options quote cannot become an earlier equity selection feature. Capture unavailable fields as unavailable. Do not retroactively manufacture historical order-book depth, IV or Greeks. Any future IV/Greek computation needs its own versioned inputs and causal availability audit.

Prioritize these experiments:

| Priority | Hypothesis | Required evidence | Comparison |
|---|---|---|---|
| R1 | Execution/data delays account for part of live-versus-replay divergence | Matched decision and fill timestamps, source coverage | Same signals and policies; attribution before changing thresholds |
| R2 | A tighter options spread/freshness/entry-delay rule improves after-cost results | Prospective guarded quotes, rejected entries and executable paths | Current one-lot policy against a small predeclared challenger set |
| R3 | Some setup/regime combinations have persistently different expectancy | Sufficient independent sessions and causal regime features | Frozen baseline versus simple interpretable filter |
| R4 | Alternative vacant-quota ranking improves results | Complete eligible candidate set and forward paths | Preserve G core-first/quota semantics unless explicitly testing a separate strategy |
| R5 | Simpler exit or sizing alternatives improve robustness | Valid path data, costs and portfolio replay | Small registered alternatives; drawdown and capital usage included |
| R6 | A calibrated meta-label adds value beyond simple rules | Adequate candidate/event labels across unseen sessions | Simple model versus baseline and simple filter |

Treat R1/R2 as early data/diagnostic work. R3-R6 need broader prospective evidence. Previously reviewed histories can be used for development and debugging, but relabeling their dates does not create an untouched test. Thousands of candidate rows on the same few sessions are not thousands of independent market observations.

The displayed 20-trade options study has 15 time exits and five target exits, with no stop exits. It therefore does not establish the 30% stop's behavior through adverse regimes. Measure loss paths and execution stress prospectively before drawing conclusions about that stop. For capacity, one executable lot does not establish that three or more lots would fill at the same price; historical bar volume alone cannot demonstrate order-book capacity.

Each experiment specification records: question and rationale; baseline code/config hashes; asset/execution profile; source manifest and coverage; candidate population; feature/label definitions and availability; training/validation/final-test windows; exact permitted parameter variants; fee/slippage/liquidity/capacity assumptions; comparison metrics; compute/trial limits; and predeclared decision rules. Store all variants, including failures. An AI-generated specification is validated before execution.

Use time-ordered walk-forward development with preprocessing and calibration fitted only on training data. Purge overlapping event labels across boundaries and use an embargo when the feature/label construction requires it. Group dependence by session and consider symbol clustering in uncertainty estimates. Reserve a later untouched evaluation or prospective shadow period; once results inform a change, that period is development history for the next attempt.

Evaluate after-cost net expectancy, profit factor with sample counts, drawdown, opportunity retention, exposure, fill rate, turnover, capital usage, and concentration by setup/month/symbol. Use day-block uncertainty estimates where sample size supports them; explicitly report insufficient evidence otherwise. Stress measured execution costs and adverse scenarios, explain bar-path ambiguity, and replay capacity causally. Profit factor or win rate alone is not a promotion objective.

Repeated testing increases false-discovery risk; the experiment registry and trial budget are part of validation, not administrative extras. See [Bailey and coauthors on backtest overfitting](https://www.davidhbailey.com/dhbpapers/overfit-tools-at.pdf).

The existing [meta-label training scaffold](../eqidv2_meta_train_walkforward.py) has time-ordered evaluation and overlap handling. It is not verified integrated with G; its current feature and label semantics need review. Its existing minimum-sample guards are implementation checks, not proof that reaching those counts makes a G model reliable.

**13. Strategy promotion criteria.** A successful research job produces a candidate for review, not a live strategy edit. Specify numeric acceptance limits in the experiment contract before looking at evaluation results. Choose effect size, uncertainty, drawdown and opportunity tolerances based on the actual profile and data; do not inherit historical PF/win-rate targets blindly.

All promotion prerequisites must hold:

- Data integrity and decision-time causality checks pass, with coverage limitations disclosed.
- Baseline and challenger use comparable execution, cost, sizing and capital assumptions.
- Improvement meets the predeclared after-cost effect and uncertainty criteria on untouched/prospective evidence.
- Drawdown, concentration and opportunity loss remain inside predeclared tolerances.
- Results survive the specified cost/liquidity stress and are not solely one symbol/day/setup windfall.
- Shadow output is reproducible and operationally stable; forward paper behavior matches the intended profile.
- Code/configuration is versioned, reviewed and independently replayable; a rollback configuration exists.

Keep `INSUFFICIENT_EVIDENCE` as a valid conclusion. There is no universal fixed trade count or calendar duration that guarantees robustness. Shadow scoring may be enabled for collection before predictive value is established; changing the actual trading decisions requires the complete promotion gate and a separate deployment decision.

**14. Dashboard experience.** Add an Assistant panel behind a feature flag. Retain the existing cards and direct operating controls. Give users date, asset, execution mode and profile filters that remain visible throughout the conversation.

Suggested prompt shortcuts: Check readiness; Explain a signal; Explain an options rejection; Compare execution; Summarize session; Plan an experiment. Answers show data timestamp, completeness, relevant profile and compact evidence links. Numerical tables come from the backend. A job card shows queued/running/verifying/completed state, progress, cancel control where permitted, and report links.

A proposed repair appears as a concrete plan with affected resources and expected result. An experiment appears with baseline, dates, parameter changes, execution assumptions and budget. Show when the user's request or a standing policy already authorizes execution; reserve additional confirmation for actions beyond that scope. Missing evidence appears as a first-class outcome with the next useful diagnostic step.

UI text should explain trading/operational meaning. Implementation details such as internal queue leases remain in diagnostics. On narrow screens, preserve mode/profile and timestamps before decorative charts. Use keyboard navigation and make status distinguishable without color alone.

**15. Delivery phases, dependencies and estimates.** These are planning estimates rather than delivery promises. Re-estimate after P0. Engineering completion and forward-market validation use different clocks.

| Phase | Work and deliverables | Acceptance gate | Effort estimate |
|---|---|---|---|
| P0: inventory/contracts | Map relevant producers/consumers; freeze profile inventory; collect sanitized fixtures; define source schemas, policies and evaluation set | Mode identities and sources verified; imports/write paths audited; auth design decided | 3-4 engineering days |
| P1: read-only API | Separate environment/service; adapters; deterministic analytics; health/readiness/trace/trade APIs; source registry and auth | Fixture parity; malformed/stale data handled; no canonical writes or broker calls | 4-6 days |
| P2: dashboard chatbot | Panel/proxy; provider adapter; retrieval/tools; evidence/number checks; cost limits; factual fallback | Curated evaluation passes; mode/citation isolation; provider outage leaves dashboard usable | 5-7 days |
| P3: reconciliation/monitoring | Completed-session comparison; discrepancy classification; incident grouping; daily summary | Matched sample sessions reconcile; incomplete cases labeled; event deduplication works | 4-6 days |
| P4: research jobs | Persistent queue/worker; typed experiment contract; isolated wrappers; manifests; cancellation/recovery | Restart, duplicate, timeout and output-isolation tests pass; reports reproduce | 5-7 days |
| P5: bounded operations | Repair planning/execution policies; exact resources/locks; post-action validation; scope/expiry controls | Every enabled action has tested preconditions, failure behavior and audit | 4-6 days |
| P6: predictive research | G-specific dataset, simple baselines, temporal validation, shadow scores and research report | Complete strategy promotion criteria or explicit insufficient evidence | 10-20 engineering days after data readiness; collection duration separate |

P0-P2 gives the first chatbot release in roughly 3-4 working weeks. P0-P5 totals 25-36 engineering days; allow roughly 6-9 calendar weeks including integration/soak time for one substantially full-time engineer. Historical irregularities, hardware limits and permissions can extend this. P6 is optional and not on a promised profitability schedule.

Dependency sequence: P0 -> P1 -> P2; P3 relies on P1 and can partially overlap P2; P4 relies on source/identity and isolation contracts; P5 relies on P4 and action-specific testing. Prospective data capture design starts in P0 and collection should begin as early as validated instrumentation permits.

**16. Concrete backlog and reviewable changes.** Deliver small increments with tests matched to their actual risk. Avoid one large rewrite.

| Item | Implementation unit | Completion evidence |
|---|---|---|
| B01 | Inventory and source/profile registry | Reviewed source map; known one-lot/three-lot and confirmation differences captured |
| B02 | Schemas and sanitized fixture pack | Complete/stale/missing/conflicting inputs validate as intended |
| B03 | Pure adapters and source-version snapshots | No imports of executable runners; coherent snapshot/partial behavior tested |
| B04 | Read APIs and authentication | OpenAPI contract, bounded queries, authorization and profile-isolation checks |
| B05 | Source viewer and deterministic decision trace | Exact evidence references for selected/rejected/unfilled examples |
| B06 | Knowledge retrieval and model tool loop | Grounded answers with measured retrieval/tool performance |
| B07 | Dashboard panel/proxy and fallbacks | Desktop/mobile interaction checks and model/API failure behavior |
| B08 | Session comparison engine | Known arithmetic and mismatch fixtures reconcile |
| B09 | Incident grouping and daily summaries | No repeated alerts for unchanged conditions; final/provisional labels correct |
| B10 | Job DB/worker/recovery | Duplicate submission, worker crash, cancellation and fencing checks |
| B11 | Isolated research adapters and reports | Baseline replay reproducible in a temporary root; production files untouched |
| B12 | Bounded operational action registry | One enabled action at a time with verified before/after state |
| B13 | Prospective candidate/execution dataset | Availability timestamps, eligibility denominator and coverage audits |
| B14 | Simple research challengers/shadow scoring | Registered experiment results and explicit promotion decision |

Assign an implementation owner and reviewer to each item. Source/adapter and dashboard work can run in parallel once schemas are stable. Strategy-validation review should be independent of the agent that proposed the experiment where practical.

**17. Validation and acceptance measurements.** Establish baseline performance during P0 and measure the same workloads after each release. The numerical targets below are proposed engineering acceptance targets, not claims about current performance.

| Measure | Initial target or gate |
|---|---|
| Golden financial fixtures | Exact reconciliation in minor currency units, using documented rounding |
| Mode/profile handling | Zero unlabelled cross-mode merges in the curated suite |
| Evidence references | All factual result claims in evaluated answers map to returned sources; nonexistent citations fail |
| Tool selection | At least 95% correct tool/context selection on the curated MVP set; zero critical financial/mode errors |
| Missing/stale data | All curated cases return explicit incompleteness; no fabricated price/P&L |
| Authorization | Every forbidden endpoint/tool/path attempt denied in negative tests |
| Read responsiveness | Target p95 under 1 second for cached reads on the local machine; measure realistic payloads |
| Chat response | Target normal factual answers within 15 seconds; show progress/fallback on longer work |
| Resource isolation | No missed live deadlines or duplicate producers attributable to platform load during soak |
| Diagnosis efficiency | Measure time for five recurring incidents before/after; initial improvement objective 50%, subject to baseline |
| Job durability | Restart/duplicate/cancel scenarios retain correct identity and never publish unverified success |
| Trading effect | No claim until the separately defined research promotion criteria pass |

Create a curated evaluation set initially around 40-60 realistic questions: readiness and holidays; exact rule explanations; raw/confirmation/quota rejection; no-trigger cases; live/paper/profile confusion; options quote/contract/window failures; partial jobs; historical-versus-today questions; misleading report text; and unsupported requests. Reserve evaluation cases from prompt tuning and add newly observed failure cases with versioned expectations.

Test source parsing, accounting, gate ordering, unit conventions, stable matching, document version selection, idempotency, process recovery and path boundaries. Use integration fixtures for EOD RUNNING reports, option square-off UNRESOLVED, concurrent file updates, schema drift and stale metadata. Test model timeout, invalid tool arguments, invented citations and prompt injection. Use the same sources/mode for metric comparisons.

Operational soak is a separate gate: several complete market sessions for the read-only release, including preopen, active trading, square-off and EOD, with deliberate simulated API/model failures outside execution-critical paths. This checks reliability; it does not establish strategy profitability.

**18. Rollout and rollback.** Develop on isolated fixtures first. Bring up the loopback API with authentication, then read real files under bounded queries. Enable the dashboard panel for the local user only. Add reconciliation, jobs and individual operations behind separate flags after their gates pass.

Suggested flags: `AI_PANEL_ENABLED`, `AI_CHAT_ENABLED`, `AI_RESEARCH_JOBS_ENABLED`, `AI_OPERATIONS_ENABLED`, and `AI_SHADOW_MODEL_ENABLED`. Effective capabilities are the intersection of deployment flags, authenticated user role, standing policy and action preconditions. A flag alone does not authorize a live action.

Keep versioned environment dependencies and database migrations. Back up metadata through a consistent SQLite backup mechanism and test restoration. Record service/task definitions before changing them. Choose and document retention during P0: suggested start is 30 days for redacted chat text, 90 days for platform diagnostics, and retention of all experiment manifests/results and decision evidence needed for comparisons. Never delete existing strategy evidence to satisfy a new chat retention rule. Alert on disk budget before cleanup.

Rollback sequence: disable the panel/chat flag; prevent new submissions; cancel or drain owned research jobs; stop only the identified API/worker services; restore the previous proxy/UI/config and, if necessary, a compatible metadata backup. Existing dashboard routes, trading state, scanners and execution processes continue independently. Preserve audit/artifact records for diagnosis.

Rehearse recovery from API crash, worker crash, model outage, corrupt input, database contention, disk exhaustion and accidental duplicate worker launch. Canonical data repair requires its own staged publish and rollback procedure; it cannot reuse a generic file-overwrite tool.

**19. Costs and operating budget.** Do not assume a model subscription covers this application's API usage. Select a provider/model and verify current account pricing/limits before implementation. No fixed token price or trading improvement is assumed here.

Track model input/output tokens, tool calls, latency, retries and estimated spend per turn/job. Estimate monthly model usage as average turns/day times trading/usage days times measured cost/turn, plus separately budgeted automated summaries. Add local CPU/RAM/disk, storage retention and engineering/maintenance costs. Cache deterministic results and retrieve small relevant excerpts; avoid sending full market histories or logs to the model.

Use a tested economical model for ordinary tool routing if it meets the evaluation gates, with an explicit escalation path for complex research explanations. Compare models on this project's questions before choosing. Enforce per-request and daily spend caps; hitting a cap disables model generation while deterministic APIs remain available. A local model is a later alternative if privacy, connectivity or measured costs justify its hardware and maintenance demands.

**20. Defaults and decisions to confirm during implementation.** These defaults make the plan actionable without blocking planning on optional preferences.

| Decision | Proposed default | Revisit when |
|---|---|---|
| Product scope | Local dashboard assistant first | Remote or multi-user access is requested |
| API integration | Loopback sidecar with narrow same-origin proxy | A full dashboard/backend migration is independently justified |
| User roles | Viewer, researcher, operator; map one current user initially | More users or unattended operations are added |
| Model access | Provider adapter and hosted function-calling model, chosen through evaluation | Account availability, data handling or costs require an alternative |
| Storage | Existing market files plus SQLite platform metadata | Multi-host writers or measured contention demand a different design |
| Retrieval | Approved docs and lexical search | Measured recall gaps justify embeddings |
| Jobs | Single Windows worker, isolated output, one heavy job | Verified capacity permits more concurrency |
| Autonomy | Read-only first; requested isolated research next | An action-specific standing policy and tests are complete |
| Strategy | Current G baseline preserved; models shadow first | Promotion criteria pass and a deployment is requested |

Before connecting a hosted model, document which redacted content leaves the machine and verify the provider's applicable data-handling settings. Before scheduling unattended repairs, define exact actions, scope, time windows, retry/cost limits and notification preferences. These are implementation decisions, not reasons to delay source contracts, adapters or the read-only API.

**21. First implementation sprint.** Start with B01-B05: map authoritative files and profile identities; collect sanitized fixtures for a normal day, a data failure, a rejected signal, an equity fill with no option, and an incomplete result; implement pure adapters and typed contracts; expose authenticated readiness, strategy, trace, trade and summary routes; compare outputs with their original sources. Completion means the future chatbot has reliable tools to call, with no model dependency and no trading side effects.

The next increment adds the Assistant panel and model tool loop against those validated APIs. Its demo should answer the five questions in section 1, show the relevant evidence, and remain useful when an input is missing or the model is offline. Research and recovery capabilities follow their own phase gates.
