# V13-V10-G observability operations and usage guide

This is the operating guide for OBS-0 through OBS-12 across the live strategy,
daily replay, market data, indicators, read-only API, telemetry stack and
research workflow.

Observability is diagnostic and fail-open. A log, metric or trace export failure
must not alter a signal, block an order, change a strategy result or restart an
otherwise healthy trading process. Existing market-data and execution safety
checks remain fail-closed. Immutable evidence and broker truth remain
authoritative; Prometheus, Loki and Tempo are diagnostic indexes, not trading
ledgers.

## Status vocabulary

- **Implemented**: code, configuration and tests exist in the repository.
- **Enabled**: the relevant process is running and producing/collecting actual
  telemetry.
- **Accepted**: controlled drills and complete market-session evidence meet the
  stated acceptance gate.

Implementation is not the same as runtime activation or acceptance.

## Priority implementation checkpoint - 2026-09-25

The priority work is installed and active, with the following deliberately
unclosed evidence gates:

- Prometheus, Alertmanager and Alloy have been reloaded from the checked-in
  configuration. Prometheus is running all 11 recording rules and 32 alert
  rules. Grafana, Prometheus, Alertmanager, Loki, Tempo, Alloy and the
  authenticated read-only API are healthy on loopback interfaces.
- The read-only API was restarted to load the current collector. Its latest
  fresh broker projection reports position mismatch `0`, active-order mismatch
  `0`, scanner schedule overdue `0`, and
  `READY_POSITION_AND_ACTIVE_ORDER_PARITY`. This is current point-in-time
  evidence; it does not reconstruct the earlier `FORTIS` mismatch or prove
  historical broker P&L.
- After correcting the bounded equity-feed terminal contract, the read-only
  LIVE trust gate passed all 37 checks at 15:14 IST. The equity feed had
  correctly completed all nine frozen slots at 11:21; the former stale result
  was a verifier false negative, not missing feed data.
- One real P1 remains firing: the supervised NIFTY guard heartbeat is stale.
  It made 21 runs with 20 allowed restarts; the supervisor ended the sequence
  on repeated `worker_run_id_mismatch` and then `max_restarts_exceeded`.
  Separate worker logs also contain repeated broker-token failures and there
  are no current-day NIFTY bars. It was not restarted during the live session.
  Confirm both supervisor compatibility and authentication, then require fresh
  current-day bars and a READY marker at the next controlled session.
- The authentication task is installed with two retries, ten minutes apart.
  The prospective-shadow prepare and seal tasks are installed for weekdays at
  08:50 and 15:25 IST and begin on 2026-09-28. Installation did not run them.
- The EOD V13-V10-G job now finalizes a prepared shadow session after a
  successful dated replay, then refreshes execution-realism and all 14
  research/observability dashboard cards. A missing prepared session is an
  explicit safe skip; corrupt or partial evidence fails closed.
- An authenticated dashboard probe returned HTTP 200 and found all 14 expected
  research/observability card labels, including the six cards under the
  dedicated **Observability** subheading.
- Prospective shadow remains `NOT_STARTED`, with `0/20` accepted sessions.
  No session was fabricated or backfilled for 2026-09-25, and promotion remains
  blocked. No threshold, sizing, stop, target, live configuration or broker
  order was changed by the research automation.

## OBS-0 through OBS-12 status

| Stage | Implemented in the repository | Operational activation | Acceptance still required |
|---|---|---|---|
| OBS-0 - baseline and guardrails | Common policies, schemas, metric/SLO contracts, fail-open sinks and this runbook | CLI validation is available; no continuous service is required | Record an owned pre/post-instrumentation market-day baseline and named owners |
| OBS-1 - correlation and lineage | `run_id`/`replay_id`, strategy fingerprints, correlation context, W3C trace parsing/injection, event hashes and immutable source hashes | Supervised live and dated replay launchers create/propagate run IDs; spans and logs carry trace context | Demonstrate one complete fetch-to-exit and replay lifecycle from IDs alone |
| OBS-2 - collection | Alloy, Prometheus, Loki, Tempo, Grafana and Alertmanager Compose stack; API filesystem collector; bounded local storage | Enabled locally on 2026-09-25 with Docker Desktop 4.91.0 and the authenticated API; all seven health endpoints passed | Prove a telemetry outage is fail-open during a controlled paper/maintenance drill |
| OBS-3 - logs and journals | Redacted JSON logs, rotating files, hash-chained journals, live order-event journals and Loki ingestion | Live and replay launchers opt in automatically; the API durable configuration attestation is active and its journal verifies | Reconstruct a real paper-session order lifecycle, including failures and recovery |
| OBS-4 - raw live data | OHLCV/OI quality evaluation, immutable append-only observation ledger, live scanner quality snapshots, freshness/coverage collector metrics | Evidence generation and verification have been exercised; supported launchers publish automatically | Observe a full paper day and inject missing, stale, duplicate, revised and out-of-order fixtures |
| OBS-5 - historical provenance | Replay source manifest, input/source hashes, dated quality report, immutable replay observation and artifact hashes | Produced by dated G replay | Replay the same pinned source snapshot twice and prove matching output hashes |
| OBS-6 - indicators and gates | V13-V10-G feature/gate ledger for every evaluated row; live snapshot hashes; replay CSV plus manifest; parity comparator | Live scanner/confirmation and daily replay populate these artifacts when run | Compare a complete live/observed/finalized session within declared numeric tolerances |
| OBS-7 - metrics and traces | All 27 contract metrics are registered; live writers cover raw anomalies, slot lag, stage duration, signal/order/broker/duplicate events; the collector also evaluates the frozen scanner schedule independently of generic heartbeat aggregation; the API safely merges runtime/textfile samples; real opt-in OTLP/HTTP export, local fallback spans and bounded shutdown flushing are implemented | The pinned OTel 1.44 packages are installed in both the API venv and live/replay worker interpreter; API log/trace correlation was verified through Loki and Tempo | Prove complete worker trace coverage, p99 overhead and backpressure/drop behavior over full paper sessions |
| OBS-8 - live/EOD parity | Native-snapshot, repeated-file and bounded-directory stage bundling; three-view reconciliation; first-divergence classification; report hashes; API report endpoint and collector metrics | Available on demand; publishing a report activates parity metrics | Schedule it after EOD and obtain PASS/FAIL/INDETERMINATE for every session |
| OBS-9 - dashboards, SLOs and alerts | Two provisioned dashboards, SLO catalogue, 11 recording rules and 32 P0/P1/P2 alerts | Stack, dashboards and the current 11/32 Prometheus rule set are active; the default Alertmanager receiver remains local UI only | Exercise every production-relevant route plus resolved external notification and market-calendar suppression |
| OBS-10 - P&L attribution | Closed/unresolved profitability summaries, grouping, evidence sufficiency and path-aware execution-drag scenarios | Historical replay, PAPER and current parity evidence are published; broker fills, charges and realised P&L are still unavailable | Reconcile attribution exactly to the authoritative broker tradebook/ledger on multiple sessions |
| OBS-11 - controlled improvement | Hash-chained experiment registry, chronological windows, trial budgets, exact preregistered return/drawdown gates, independent review and a hash-verified prospective-shadow lifecycle | Weekday prepare/seal automation and EOD finalize are installed; state is `NOT_STARTED`, `0/20`, and no strategy is promoted by this system | Collect at least 20 valid prospective sessions, preserve an untouched holdout and bind an independent registry decision to the exact candidate cohort |
| OBS-12 - resilience and retention | Retention tiers, bounded cleanup, safe dry-run failure drills and guarded volume purge | Not exercised against a running stack | Run drills in paper/maintenance and complete at least five full market-session soak days |

No live-trading acceptance, five-session soak result, external notification test
or profit-improvement claim is made by this implementation.

## Current runtime snapshot

At the 2026-09-25 truthfulness pass:

- `.venv-ai-platform\Scripts\python.exe -m ai_platform.observability doctor`
  returned `READY_WITH_OPTIONAL_GAPS`: core, reconciliation, profitability and
  journal capabilities were ready, while pandas/numpy-backed `data-quality`
  was not installed in that isolated API environment.
- `.venv-ai-platform` and the separately pinned live/replay system Python both
  import the pinned OpenTelemetry 1.44 SDK and OTLP/HTTP exporter. The worker
  packages were installed only after a dependency dry run showed additions and
  no replacement of existing trading packages.
- The existing raw-observation ledger verified as valid. Use the command below
  for the current count rather than relying on a number copied into this guide.
- Docker Desktop 4.91.0 is installed per-user with the WSL2 backend and Windows
  containers disabled. Grafana, Prometheus, Alertmanager, Loki, Tempo and Alloy
  passed native container validation and runtime readiness; the authenticated
  API is listening on loopback port 8788 with OTLP export active and zero
  dropped spans at activation time.
- Prometheus received the API target through Alloy, Loki ingested structured API
  logs, and the same API trace ID was retrieved successfully from both Loki and
  Tempo. Both provisioned Grafana dashboards and all three data sources loaded.
- Prometheus, Alertmanager and Alloy were subsequently reloaded successfully.
  The running rule inventory is 11 recording rules and 32 alerts. The current
  collector exposes broker position and active-order mismatch gauges at zero
  and the frozen scanner schedule-overdue gauge at zero.
- The only firing alert at the priority checkpoint is the genuine P1 stale
  heartbeat for `eqidv2_nifty_guard_fetcher_supervised_v16_5min`; do not silence
  it as an observability false positive.
- The API log is active, OTLP setup has no error, and metric/span/event drop
  counters are zero. Its fail-open `api.runtime.configured` attestation created
  the journal, and the one-entry hash chain verified as `VALID` after the
  controlled API restart.
- The running dashboard predates a launcher hardening change, so its current
  process command line still exposes authentication arguments to local process
  inspection. The checked-in launcher now passes authentication only through
  inherited environment variables; activate it at the next controlled
  dashboard restart. Rotate the old values and move the remaining default
  secret out of source into a protected file or credential store before
  treating local process isolation as a security boundary.
- Repository tests prove behavior with fixtures; they are not market-session
  acceptance evidence.

## Level 0 - dependency and self-diagnosis

The repository-local API environment includes the OpenTelemetry API, SDK and
OTLP/HTTP exporter. Create or refresh it from the lock file:

```powershell
python -m venv .venv-ai-platform
.\.venv-ai-platform\Scripts\python.exe -m pip install --requirement ai_platform\requirements.lock
.\.venv-ai-platform\Scripts\python.exe -m ai_platform.observability doctor
```

`doctor` describes only the interpreter used for that command. The API venv and
the system Python pinned by the live/replay batch files are separate package
environments; installing into one does not change the other. The observability
CLI does not start a fetcher, scanner, broker session or trading process. The
`data-quality` command needs pandas/numpy, while the core bundle,
reconciliation, profitability and verification commands do not; `doctor`
reports the current interpreter's optional capabilities.

Run the focused validation suites:

```powershell
python -m pytest `
  tests\test_ai_platform_observability_core.py `
  tests\test_observability_metric_contract.py `
  tests\test_observability_runtime_collector.py `
  tests\test_observability_data_features.py `
  tests\test_observability_reconciliation_profitability.py `
  tests\test_observability_cli.py `
  tests\test_observability_live_integration.py `
  tests\test_observability_ops.py -q
```

## Level 1 - local telemetry stack

Prerequisite: Docker Desktop with the Compose plugin. Interfaces bind to
`127.0.0.1`; they are not intended for LAN or internet exposure.

For a new Windows workstation, first verify 64-bit Windows, hardware
virtualization, at least 8 GB RAM, WSL 2 and the Windows Server service. Then
update WSL and install Docker Desktop using its WSL 2 backend:

```powershell
wsl --update
winget install --exact --id Docker.DockerDesktop
```

If the machine user is intentionally non-administrator, download the official
Docker Desktop installer and run its documented per-user installation instead:

```powershell
& '.\Docker Desktop Installer.exe' install --user --backend=wsl-2 --no-windows-containers
```

This workstation has Windows 11 Pro build 26200, 31.7 GB RAM, virtualization,
WSL 2.7.14 and the required Server service. Docker Desktop 4.91.0 was installed
per-user because the normal machine-wide installer correctly stopped at the
administrator/UAC boundary. Docker Desktop licensing must be checked against
the organisation using it; personal use, education and qualifying small
business use differ from larger commercial and government use.

Static validation, startup and runtime checks:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Test-Observability.ps1 -SkipContainerValidation
powershell -ExecutionPolicy Bypass -File .\bat\observability\Start-Observability.ps1
powershell -ExecutionPolicy Bypass -File .\bat\observability\Test-Observability.ps1 -RequireRunning
```

The start script creates local file-backed Grafana and API/metrics secrets under
`configs/observability/runtime/secrets/`, applies a Windows ACL and does not
print their values. Read the Grafana password only when signing in:

```powershell
Get-Content .\configs\observability\runtime\secrets\grafana_admin_password.txt
```

Local endpoints:

- Grafana: <http://127.0.0.1:3000>
- Prometheus alerts: <http://127.0.0.1:9090/alerts>
- Alertmanager: <http://127.0.0.1:9093>
- Alloy component health: <http://127.0.0.1:12345>

Stop while preserving named volumes:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Stop-Observability.ps1
```

Destructive local telemetry-volume removal requires the explicit guard below.
It does not remove application evidence:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Stop-Observability.ps1 `
  -PurgeData -ConfirmPurge PURGE_EQIDV2_OBSERVABILITY
```

## Level 2 - read-only API and runtime collector

Start the stack first so its generated metrics token can also be used by the
API. The preferred background launcher reads that local secret without printing
it and enables OTLP/HTTP export:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Start-AiPlatformApi.ps1
```

For an explicit foreground process instead:

```powershell
$env:AI_PLATFORM_API_TOKEN = Get-Content `
  .\configs\observability\runtime\secrets\ai_platform_api_token.txt -Raw
$env:EQIDV2_RUNTIME_ROOT = "C:\TradingData\eqidv2"
.\bat\run_ai_platform_api.bat
```

The API automatically writes redacted JSON logs and a hash-chained journal
under `%EQIDV2_RUNTIME_ROOT%\observability`. It also projects supported existing
status, heartbeat, data, replay, reconciliation and filesystem evidence into
Prometheus samples with a cache of at most five seconds.

Fresh per-PID `.prom` snapshots are read through an exact metric/label
allow-list, bounded to 1,000 series per metric and 4,096 total. Duplicate label
sets within one file are ignored. Counters and histogram parts are added across
independent fresh process snapshots and the API registry per scrape; gauges use
their metric-specific conservative/newest merge rule. Snapshots older than the
configured maximum age disappear. A producer or API restart can therefore be a
normal Prometheus counter reset, not evidence that the historical event never
occurred. Durable JSONL/evidence remains authoritative.

Inspect it from another PowerShell process:

```powershell
$env:AI_PLATFORM_API_TOKEN = Get-Content `
  .\configs\observability\runtime\secrets\ai_platform_api_token.txt -Raw
$headers = @{ Authorization = "Bearer $env:AI_PLATFORM_API_TOKEN" }

Invoke-RestMethod http://127.0.0.1:8788/api/v1/health/live
Invoke-RestMethod http://127.0.0.1:8788/api/v1/health/ready -Headers $headers
Invoke-RestMethod http://127.0.0.1:8788/api/v1/observability/status -Headers $headers
(Invoke-WebRequest -UseBasicParsing `
  http://127.0.0.1:8788/api/v1/observability/metrics `
  -Headers $headers).Content
```

The observability status response reports log/journal paths, metric drops,
OTLP activation, exporter setup errors and dropped spans. The health route proves
process liveness only; readiness additionally evaluates registered sources.

Query Prometheus directly after the stack and API are running:

```powershell
$query = [uri]::EscapeDataString('max(trading_heartbeat_age_seconds)')
Invoke-RestMethod "http://127.0.0.1:9090/api/v1/query?query=$query"
```

## Level 3 - live and replay process instrumentation

The supported G launchers already opt in:

- `run_fno_v13_v10_g_live_kite_qty1.bat` sets
  `EQIDV2_OBSERVABILITY_ENABLED=1`; the supervisor supplies a run ID.
- `run_backtesting_result_v13_v10_g_1620.bat` enables observability and creates a
  replay run ID.
- Both default the trace endpoint to `http://127.0.0.1:4318/v1/traces` using
  OTLP/HTTP. An endpoint alone does not activate export: the exact interpreter
  running the worker must also have the SDK/exporter installed.

Do not start the real LIVE launcher merely to test telemetry. Use it only under
the existing live-trading authorization, broker and safety procedure. For a
dated replay:

```powershell
.\bat\run_backtesting_result_v13_v10_g_1620.bat --date 2026-09-24
```

For an already authorized/running quantity-one session, run the read-only,
fail-closed trust check from a separate terminal:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File `
  .\bat\observability\Test-V13V10GLiveTrust.ps1
```

Exit code 0 and top-level `state: PASS` require all of the following at once:
the current frozen strategy identity and quantity-one profile; armed/running,
fresh coordinator state; live LONG and SHORT managers; schedule-current scanner
evidence plus fresh active or same-session terminal-complete confirmation and
equity 1-minute-feed evidence; complete fresh cash and futures-OI
markers; the dedicated read-only broker-reconciliation child; a valid,
digest-verified same-run broker report no older than 120 seconds; complete
broker scope; and
zero broker/local position mismatches plus exact parity for active tagged
broker orders. Missing evidence is a failure, never an assumed zero. The
command itself does not call the broker, start or stop a
process, change an arm/kill-switch file, or submit/cancel an order.

This is a diagnostic gate, not an order-path interlock. Until an explicit
trading-policy decision wires it into pre-entry control, the operator or
scheduler must treat a non-zero exit code as a no-go signal; the command itself
cannot prevent an entry.

The dedicated reconciliation child starts with the quantity-one launcher. Do
not restart an active trading session merely to activate it. A process started
before this deployment fails the trust check on a missing or legacy report; a
later controlled launcher start includes the v2 child automatically. The
position-only v1 trust check did pass 36/36 at 10:24 on 2026-09-25, but that is
historical evidence only. The v2 contract subsequently found broker `FORTIS`
quantity `-1` while local expected quantity was `0`; the read-only trust result
at 10:50 was `FAIL` (34 pass, 3 fail). That was a real P0 incident. At the later
priority checkpoint, fresh v2 evidence reports zero position and active-order
mismatches and `READY_POSITION_AND_ACTIVE_ORDER_PARITY`, so the current P0
alert is resolved. The later zero is point-in-time state, not proof of how the
earlier position was resolved; retain both artifacts and reconcile historical
provenance separately. Do not place, modify or close an order merely to make
telemetry green.

At 10:45 on 2026-09-25 a delegated audit worker violated its read-only boundary,
stopped the active process tree and invoked the scheduled LIVE task. The worker
was interrupted and all delegated agents were stopped. This was not an
authorized remediation and must be included in the incident record; no further
process, task, broker, arm or kill-switch changes were made by the final audit.

Each instrumented worker process writes one or more of the following according
to its role (the coordinator itself primarily publishes status and propagates
correlation settings to its children):

- JSONL events under `C:\TradingData\eqidv2\observability\logs`;
- hash-chained durable events under `...\observability\journals`;
- bounded Prometheus text snapshots under `...\observability\metrics`;
- spans locally, and to Alloy/Tempo when the collector is reachable.

The live scanner and confirmation status paths publish bounded signal-funnel,
raw-anomaly, stage-duration and slot-deadline observations. Broker wrappers
publish request count/duration; canonical order transitions publish order
events; duplicate broker-tag matches publish a duplicate-order counter and a
durable diagnostic event. These writers are fail-open and their current
process-local snapshots are exposed by the API collector.

Async local span queues and OTLP batch processors receive a bounded,
idempotent process-exit flush. A full queue never blocks an order path; if a
shutdown flush times out, queued spans drain and the worker exits without
duplicating the business operation.

The launchers currently pin this worker interpreter:

```powershell
$workerPython = "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe"
& $workerPython -c "import opentelemetry.sdk; import opentelemetry.exporter.otlp.proto.http.trace_exporter; print('OTLP ready')"
```

If that check fails, install only the pinned telemetry dependencies into the
exact worker environment during a maintenance window, then rerun its full tests
before any market session:

```powershell
& $workerPython -m pip install `
  opentelemetry-api==1.44.0 `
  opentelemetry-sdk==1.44.0 `
  opentelemetry-exporter-otlp-proto-http==1.44.0
```

Do not perform an untested interpreter change immediately before live trading.

To deliberately run locally without any OTLP exporter thread, set this before
starting the process:

```powershell
$env:OTEL_SDK_DISABLED = "true"
```

Local JSON logs and metrics still work. Export and sink failures are counted and
must remain isolated from the trading path.

## Level 4 - raw live and historical data

Evaluate one OHLCV/OI CSV without modifying it:

```powershell
python -m ai_platform.observability data-quality `
  --input C:\path\to\bars.csv `
  --timestamp-column ts `
  --expected-interval 1min `
  --source NSE_EQUITY_1M `
  --symbol INFY
```

The report includes a content fingerprint, missing timestamps, duplicates,
ordering, invalid OHLC, negative volume and zero/negative OI. Exit code 0 means
`GOOD`; warning or blocked evidence returns 1, and a command error returns 2.

Fetchers and replay paths append immutable observations under:

```text
C:\TradingData\eqidv2\observability\raw_observations
```

Verify every record/payload digest and event-ID uniqueness:

```powershell
python -m ai_platform.observability verify-observations `
  --root C:\TradingData\eqidv2\observability\raw_observations
```

Do not edit an as-observed record after a late correction. Persist the corrected
data as a new revision with a new digest, then use it in the finalized replay.
The ledger detects modified or truncated records, but deletion detection still
requires an external backup, inventory or WORM store.

Useful data queries after collection is running:

```powershell
$q = [uri]::EscapeDataString('trading_data_age_seconds')
Invoke-RestMethod "http://127.0.0.1:9090/api/v1/query?query=$q"

$q = [uri]::EscapeDataString('trading_data_coverage_ratio')
Invoke-RestMethod "http://127.0.0.1:9090/api/v1/query?query=$q"
```

The live scanner publishes `trading_raw_data_anomaly_total` when its quality
report contains an issue. No issue means no counter increment; absence of that
series alone is not proof of clean data. The quality artifacts and immutable raw
observation ledger remain the evidence of record.

## Level 5 - indicators, gates and selection

The current G paths generate feature evidence automatically:

- Live scanner and confirmation snapshots include every evaluated row,
  `feature_evaluations`, gate outcomes, first failure and a canonical payload
  hash.
- Daily replay writes `feature_ledger.csv` and
  `feature_ledger.csv.manifest.json`, including input/features hashes, gate
  decisions, ranks, quota/selection decisions and final selection.
- Historical replay also writes `data_quality.csv`, coverage, selected orders,
  trades and a source manifest.

Run the direct replay builder when isolated diagnostic output is needed:

```powershell
python .\fno_v13_v10_g_daily_replay.py `
  --session-date 2026-09-24 `
  --output-dir C:\TradingData\eqidv2\observability\replay\2026-09-24 `
  --snapshot-root D:\eqidv2-replay-snapshots
```

Before reconstruction, the replay now copies every required causal-history file
into a content-addressed snapshot. Each copy must have a stable source identity
and a matching source/copy SHA-256; an unstable source is retried three times,
then blocks with `SOURCE_SNAPSHOT_UNSTABLE`. Reuse verifies every snapshot hash,
and a replay never falls back to mutable live files. If `--snapshot-root` is
omitted, snapshots are stored below the output directory. Exact 2026-09-24 EMA
parity currently needs about 422 files / 3.162 GiB, so select and monitor the
retained location deliberately.

A replay returning exit code 2 is incomplete and must not be treated as a zero
trade or parity result. Keep symbol, signal and row identifiers in the ledger,
logs and traces rather than Prometheus labels.

## Level 6 - live, observed and finalized reconciliation

The accepted comparison order is:

```text
universe -> raw_equity -> raw_futures_oi -> aggregate_5m -> feature ->
base_gate -> confirmation -> setup_gate -> ranking -> selection -> execution ->
exit -> pnl
```

Build one immutable bundle for each view. A stage input can be a CSV/JSON file,
a native live snapshot/envelope, or a bounded directory of CSV/JSON files.
Repeat `--stage STAGE=PATH` both for different stages and for additional files
belonging to the same stage; inputs are merged in deterministic order. Missing
stages remain explicit rather than being invented:

```powershell
python -m ai_platform.observability bundle `
  --session-date 2026-09-24 `
  --kind live `
  --output C:\TradingData\eqidv2\observability\bundles\2026-09-24-live.json `
  --stage "raw_equity=C:\path\live-raw-equity.csv" `
  --stage "feature=C:\path\live-feature-ledger.csv" `
  --stage "selection=C:\path\live-selection.json"
```

For a day split over many native scanner snapshots, either point a stage at the
directory or repeat the stage. These two forms are intentionally equivalent:

```powershell
# Recursive, bounded directory input (CSV/JSON only; links are rejected).
python -m ai_platform.observability bundle `
  --session-date 2026-09-24 --kind live `
  --output C:\TradingData\eqidv2\observability\bundles\2026-09-24-live.json `
  --stage "feature=C:\path\scanner-snapshots"

# The same stage assembled from explicit files.
python -m ai_platform.observability bundle `
  --session-date 2026-09-24 --kind live `
  --output C:\TradingData\eqidv2\observability\bundles\2026-09-24-live.json `
  --stage "feature=C:\path\scanner-0925.json" `
  --stage "feature=C:\path\scanner-0930.json"
```

Native G scanner/confirmation JSON can be supplied directly. The bundler
extracts stage-specific fields such as `feature_evaluations`, `candidates` and
`selected_signal_ids`, validates declared payload/field hashes and dates, and
does not invent values. Reusing one native snapshot for several applicable
stages is also supported, for example `feature=...`, `base_gate=...` and
`ranking=...`. Duplicate comparison keys are preserved and make that stage
`INDETERMINATE`; they are never silently collapsed.

Create equivalent `observed` and `finalized` bundles, then reconcile:

```powershell
python -m ai_platform.observability reconcile `
  --live C:\TradingData\eqidv2\observability\bundles\2026-09-24-live.json `
  --observed C:\TradingData\eqidv2\observability\bundles\2026-09-24-observed.json `
  --finalized C:\TradingData\eqidv2\observability\bundles\2026-09-24-finalized.json `
  --output C:\TradingData\eqidv2\observability\reconciliation\2026-09-24.json
```

Interpretation:

- live differs from observed: live code/config/state/scheduling behavior;
- observed differs from finalized: late, missing or revised source data;
- decision stages agree but execution/P&L differs: activation, broker, fill,
  slippage, cost or exit behavior;
- unavailable or duplicate-key evidence: `INDETERMINATE`, never false parity.

Read the published report through the API:

```powershell
Invoke-RestMethod `
  http://127.0.0.1:8788/api/v1/observability/reconciliation/2026-09-24 `
  -Headers $headers
```

The endpoint requires the report's `report_sha256`, recomputes it over the
unsigned payload, and also verifies the requested session date. A missing or
tampered digest is not served: it returns HTTP 503 with
`RECONCILIATION_DIGEST_MISMATCH`.

Publishing a report activates the collector's live/EOD and feature-parity
metrics. Reconciliation is currently an on-demand CLI workflow, not a scheduled
EOD job. Report-derived `_total` values are process-local nondecreasing
accumulators deduplicated by verified report digest. On API restart they are
reconstructed from the newest 400 retained, verified reports, so retained-set
changes can reset or revise the projection. Use Prometheus `increase()`/`rate()`
rather than treating the raw value as a lifetime total.

## Level 7 - trader/operator dashboards and incidents

During a paper or authorized live session, use **Trading Control Plane**:

1. Confirm market state, mode and the expected strategy fingerprint.
2. Confirm heartbeat age is within its cadence: below 420 seconds for `5m` or
   `5min` slot-driven services and below 120 seconds for continuous/one-minute
   services.
3. Confirm each required data source has acceptable age and coverage. The alert
   policy allows 420 seconds for five-minute markers (one full cadence plus two
   minutes of publication grace) and 90 seconds for continuously updating
   faster inputs. The scheduled confirmation artifact is intentionally excluded
   from the generic age alert between frozen signal windows; its process
   heartbeat and slot-deadline evidence remain mandatory.
4. Confirm `trading_pipeline_schedule_overdue{pipeline="v13_v10_g_scanner",mode="live"}`
   is zero. This schedule-aware gauge evaluates the nine frozen slots directly,
   allows 180 seconds for completion, and becomes one after grace even when the
   scanner heartbeat is missing; another fresh V13 heartbeat cannot mask it.
5. Follow signal and order funnels, then open correlated logs/traces for detail.
6. Treat every P0 as a safety incident, including broker/local reconciliation,
   duplicate-order and missing/mismatched strategy-fingerprint evidence.

After the close, use **Data, Parity and Research** to inspect data revisions,
feature mismatches and first-divergence classification.

The raw-anomaly, slot-deadline, stage-duration and duplicate-order metrics have
live writers. A counter appears only after its first event, and a histogram only
after its first observation; an absent series is not automatically healthy.
The collector accepts only fresh, schema-matching textfile samples, so a stopped
process's snapshot ages out instead of looking current.

The running API collector now uses the checked-in logical-pipeline mapping. The retired
`fno_oi_fetch_5min` and its replacement
`fno_oi_fetch_5min_fast_production` are exported as the single service
`fno_oi_5min_production`, using the freshest equivalent heartbeat. This avoids
alerting on a stale cutover artifact while retaining a stale alert when neither
generation is fresh.

Heartbeat alerts and the availability SLO are cadence-aware. A five-minute
scanner can remain unchanged between completed slots, so `5m`/`5min` service
names use a 420-second limit (one cadence plus two minutes of publication
grace). Continuous and one-minute workers retain the 120-second limit. The
dashboard displays age divided by the applicable limit so unlike cadences can
be compared without false red status.

The frozen scanner has a separate schedule SLO and
`TradingPipelineScheduleOverdue` alert. A completed 10:00 slot is valid until
11:20 becomes due, while a missing or incomplete due slot becomes overdue after
the 180-second grace. This closes the gap where another V13 worker's heartbeat
could keep the generic family present while the scanner itself was absent.

The activated Alloy configuration also tails recent `*.log.supervisor.log` files and recent
`*supervised*.log` worker files from the repository log directory. It starts at
EOF, excludes files older than 48 hours, removes credential values and email
addresses before forwarding, and classifies broker and alert-delivery
authentication failures. Inspect them in Grafana Explore with:

```logql
{job=~"trading-supervisor|trading-supervised-worker", event=~".*authentication_failure"}
```

The derived authentication counters reset when Alloy reloads; Loki logs are the
investigation record and Prometheus `increase()` is the alerting view.

`trading_clock_offset_seconds` is conditional: the collector projects an
explicit clock/NTP-offset field, but the checked-in supervisor does not measure
host offset itself. Until a trusted time source writes that field, absence is
unknown clock state, not proof of synchronization.

`trading_strategy_fingerprint_mismatch` is emitted only when a current-day,
nonterminal LIVE status/heartbeat has a semantic timestamp no more than 120
seconds old. It compares that active evidence and the strategy manifest with the
approved profile fingerprint. Stale, future, failed/blocked or missing active
evidence stays absent; during market hours with a live heartbeat,
`TradingStrategyFingerprintEvidenceMissing` alerts on that absence.

`trading_position_reconciliation_mismatch` and
`trading_active_order_reconciliation_mismatch` come only from explicit broker
reads performed by the dedicated LIVE quantity-one reconciliation child
(30-second default cadence; `--broker-reconcile-sec` cannot be below 10
seconds). The child is restricted to broker `positions` and `orders` reads and
does not submit, modify or cancel an order. It compares local open strategy
state with NSE MIS symbols attributable through local state or the strategy
order tag. It also compares every non-terminal strategy-tagged broker order
against the locally expected active order. `COMPLETE`, `CANCELLED` and
`REJECTED` are terminal evidence; all recognized pending/open states are active,
and an unknown status fails closed. An active order must match its local order
ID, deterministic tag, symbol, transaction side, order type, quantity, exchange
and product. `OPEN` local states require active stop and target IDs;
`SQUARE_OFF_PENDING` requires an active square-off ID; a `PENDING_ENTRY` with an
entry ID requires that active broker entry. Terminal local states expect no
active order.

A non-zero scoped position mismatch is exported even if unrelated non-zero NSE
MIS positions make the position scope incomplete. The active-order metric
exports every unexpected, missing, duplicate, malformed or identity-mismatched
active order. Healthy zeroes are exported only when broker truth is available,
position scope is complete and active-order parity is explicitly complete;
broker errors, missing evidence and incomplete zeroes remain absent, not
healthy. Reports use the
`v13_v10_g_broker_position_reconciliation_v2` contract, declare today's IST
session and a semantic observation
timestamp no more than 120 seconds old; missing, stale or future timestamps are
rejected. The producer writes a canonical SHA-256 integrity digest in
`report_sha256`; this detects accidental/local corruption but is not a keyed
signature or authentication against a malicious local writer. Missing or
invalid digests and legacy position-only v1 reports are rejected. During market
hours, either absent reconciliation series alongside a live worker heartbeat raises
`TradingBrokerReconciliationMissing`.

The coordinator propagates the same `run_id` to LONG, SHORT and reconciliation
children. Broker read/authentication failure, incomplete reconciliation scope,
a non-zero position mismatch or incomplete/non-zero active-order parity makes
the child and coordinator status `DEGRADED`, while
leaving the LONG/SHORT managers alive to protect existing positions. The
digest-verified artifact is written to
`%EQIDV2_RUNTIME_ROOT%\observability\reconciliation\broker_positions_<date>.json`.

### P0 live safety alerts

Stop new entries through the existing safety control. Inspect broker positions
and open orders directly, reconcile local quantity, preserve state/evidence, and
only then consider a controlled restart. Never delete local evidence merely to
clear a mismatch. `TradingActiveOrderReconciliationMismatch` specifically means
the order view is unsafe even when net broker quantity currently equals local
quantity; investigate the detailed `active_order_mismatches` rows in the
digest-verified artifact.

### P1 pipeline alerts

Inspect supervisor heartbeat, process identity, clock offset, data timestamps,
coverage and broker authentication/rate limiting. Record whether a slot was
skipped, late or completed using stale/incomplete evidence.

`TradingBrokerAuthenticationFailure` identifies broker/Kite credential errors
from supervisor-managed workers. Refresh credentials only through the approved
interactive login procedure, then verify source readiness before permitting new
entries. `TradingAlertDeliveryAuthenticationFailure` means local alerts still
exist but the external notification channel could not authenticate.

The weekday `EQIDV2_authentication_v2_0900` task is installed and enabled with
two retry attempts at ten-minute intervals; its next scheduled run after this
deployment is 2026-09-28 08:30 IST. It still uses an interactive-token logon,
so it does not eliminate the need to verify a successful current-session login.
Inspect it without exposing credentials:

```powershell
Get-ScheduledTask -TaskName EQIDV2_authentication_v2_0900 |
  Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName EQIDV2_authentication_v2_0900
```

### Parity alerts

Open the published reconciliation report, verify its digest, and start at
`first_divergence_stage`. Treat live-versus-observed differences as a live
code/config/state/timing investigation; treat observed-versus-finalized
differences as late or revised input-data investigation. `INDETERMINATE` means
evidence must be repaired or collected, not that parity passed.

### Data-quality alerts

Inspect the dated quality artifact and immutable observations before changing a
source. Classify gaps, duplicates, ordering, OHLC, volume and OI independently.
Preserve the as-observed revision and append a corrected revision rather than
overwriting evidence.

### API alerts

Check `/health/live`, then authenticated `/health/ready` and
`/observability/status`. A healthy trading worker does not depend on this
read-only API, so recover the API/collector without restarting trading merely
to clear a dashboard alert.

### Telemetry outage

Do not restart a healthy strategy solely to restore a dashboard. Inspect drop
counters and exporter state, recover the telemetry component, and record the
gap. A telemetry failure that changes a trading outcome fails OBS-12.

## Level 8 - profitability and controlled research

Summarize only authoritative closed/unresolved trade evidence:

```powershell
python -m ai_platform.observability profitability `
  --input C:\path\portfolio_trades.csv `
  --group-by setup_id,side `
  --min-trades 20
```

The result retains unresolved rows and labels evidence `SUFFICIENT` only when
the minimum closed-trade count is met and no unresolved records remain. Exit
code 1 means insufficient evidence. This analysis can expose data loss,
slippage and weak regimes; it cannot guarantee or automatically maximize profit.

Create a governed one-factor experiment template:

```powershell
python .\tools\observability_experiment_registry.py scaffold `
  --experiment-id g-confirmation-one-factor-001 `
  --proposed-by saar `
  --output .\research_outputs\g-confirmation-one-factor-001.json
```

After filling the hashes, chronological windows, costs, budget and predeclared
decision rules:

```powershell
python .\tools\observability_experiment_registry.py validate `
  --spec .\research_outputs\g-confirmation-one-factor-001.json
python .\tools\observability_experiment_registry.py register `
  --spec .\research_outputs\g-confirmation-one-factor-001.json
```

Before a positive shadow decision, the result manifest must be `COMPLETED`, all
checks must pass, and `metrics` must contain the preregistered primary metric
for `baseline`, `challenger` and `difference`, plus
`maximum_drawdown_rs` for baseline and challenger. The recorded difference must
equal challenger minus baseline, meet `minimum_improvement`, and keep worsened
drawdown within `maximum_drawdown_degradation_rupees`. `ELIGIBLE_FOR_SHADOW`
and `APPROVED_FOR_SHADOW` additionally require `UNTOUCHED_HOLDOUT` evidence;
reused development evidence fails closed.

Record a result manifest and a review decision:

```powershell
python .\tools\observability_experiment_registry.py record-result `
  --experiment-id g-confirmation-one-factor-001 `
  --result .\research_outputs\g-confirmation-one-factor-001-result.json `
  --actor backtest-runner
python .\tools\observability_experiment_registry.py decide `
  --experiment-id g-confirmation-one-factor-001 `
  --decision ELIGIBLE_FOR_SHADOW `
  --actor research-reviewer `
  --reason "Predeclared holdout and risk checks passed"
python .\tools\observability_experiment_registry.py verify
python .\tools\observability_experiment_registry.py list
```

The registry can approve shadow testing and record
`CANDIDATE_FOR_MANUAL_LIVE_REVIEW` after the minimum prospective sessions. It
requires the latest result to use `PROSPECTIVE_SHADOW` evidence for that final
candidate state. It cannot edit live configuration, arm execution or promote a
strategy to live.

### V13-V10-G Strategy Research & Prediction dashboard sessions

The local log dashboard has a dedicated **Research & Suggestions ->
V13-V10-G Strategy Research & Prediction** subheading. Its eight cards are
artifact views, not Windows tasks or trading workers:

| Card | STRAT stages | What it answers |
|---|---|---|
| Research Data Quality | STRAT-0, STRAT-1 | Are required inputs hash-addressed, complete and point-in-time ordered? |
| Historical Research Dataset | STRAT-1, STRAT-2 | What sessions, rows, contracts and repaired-date coverage are actually present? |
| Frozen Baseline Replay | STRAT-3 | What did unchanged G produce by day after recorded costs? |
| Decision Attribution | STRAT-4 | Where did candidates fail, pass, fill and contribute P&L? |
| Market Regimes & Drift | STRAT-5, STRAT-10 | How did fixed decision-time market/OI/volatility/liquidity buckets behave? |
| Prediction Quality & Calibration | STRAT-6 | How well do transparent prior-day-only fill, stop, return, MFE and MAE estimates calibrate? |
| Walk-Forward & Holdout Evaluation | STRAT-7 | Is chronological evidence sufficient and is an untouched final test available? |
| Prospective Shadow & Promotion Gate | STRAT-8, STRAT-9, STRAT-10 | Has prospective shadow evidence and independent review cleared every promotion gate? |

The same Research section has a separate **Observability** subheading with six
additional read-only sessions:

| Card | Primary evidence | What it answers |
|---|---|---|
| Market Regime | Latest immutable scanner ledger | What are current NIFTY direction, breadth, dispersion and OI participation? Current VIX/realised volatility stay unavailable unless verified sources exist. |
| Selection Funnel | Scanner, confirmation and signal snapshots | How many symbols passed each stage, which first gates failed and which symbols were selected? |
| Entry and Execution | Finalized replay, persisted PAPER/LIVE orders, append-only first-failure evidence, broker parity and verified execution scenarios | What are activation/trigger latency, fill ratio, cancellations, trigger-to-fill distance and P&L sensitivity to entry distance/delay? |
| Live vs Finalized Drift | Natural-key live/finalized selection comparison | Which selected sets or common-row values differ? First divergence stays unavailable until complete stage bundles exist. |
| P&L Attribution | Historical/finalized replay, PAPER/LIVE local records and verified execution scenarios | What gross, costs, net and execution drag are independently evidenced by each mode, and how sensitive is replay P&L to adverse execution assumptions? |
| Regime Profitability | Reused historical selected trades | What are trades, win rate, expectancy, PF and drawdown by regime, setup, slot and side? |

Refresh all fourteen views from the newest **complete** historical source
bundle plus the current persisted live/replay evidence:

```powershell
cd C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2
.\bat\run_v13_strategy_research_refresh.bat
```

The equivalent explicit two-stage sequence is:

```powershell
py -3.12 .\tools\v13_execution_research.py
py -3.12 .\tools\v13_strategy_research.py `
  --source C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_full_history `
  --live-root C:\TradingData\eqidv2\fno_oi\v13_v10_g_live `
  --replay-root C:\TradingData\eqidv2\backtesting_result_v13_v10_g\runs `
  --output-root C:\TradingData\eqidv2\v13_v10_g_strategy_research
```

The refresh batch first runs the path-aware execution analysis and refuses to
publish a new dashboard bundle if that verification fails. Run the execution
stage by itself when diagnosing entries:

```powershell
py -3.12 .\tools\v13_execution_research.py
```

The verified 2026-09-25 sensitivity snapshot uses 13 terminal PAPER orders
(10 fills) to stress, not retune, the frozen 85-order historical baseline:

| scenario | trades | net INR | delta vs frozen INR | profit factor |
|---|---:|---:|---:|---:|
| frozen entry model | 77 | 176,236.32 | 0.00 | 2.829 |
| observed median distance proxy, 1.4523 bps | 77 | 153,251.77 | -22,984.55 | 2.420 |
| one-minute delay plus median distance | 72 | 97,121.70 | -79,114.62 | 1.812 |
| observed p90 distance proxy, 8.8624 bps | 77 | 105,082.05 | -71,154.26 | 1.830 |

These are small-sample, adverse-entry sensitivity scenarios. The distance is a
quote-distance proxy, not measured broker slippage. They show that execution
quality is economically material, but they do not authorize an entry-rule or
live-strategy change.

The generator discovers only a source directory containing every required
dataset, audit, baseline and coverage artifact. It does not call a replay,
import a live worker, query the broker, rewrite canonical backtest `latest`, or
change strategy configuration. Each execution writes an immutable
`runs/<run_id>/` bundle, hashes its reports and prediction CSV in
`manifest.json`, then atomically publishes the same verified files under
`latest/` for the dashboard. The manifest and dashboard card status both carry
`execution_authority=false`.

Use the views at four levels:

- **Operator:** check Data Quality first. `READY` means the report exists, not
  that trading or a challenger is approved. Use the Frozen Baseline card for
  day-wise results, the six Observability cards for current diagnostic state,
  and the Shadow card for the fail-closed promotion gate.
- **Researcher:** start with Decision Attribution and Regimes. Register exactly
  one proposed change before testing it; never select a rule from these reused
  slices and then label the same history as a holdout.
- **Reviewer/risk owner:** verify the bundle manifest, chronological metrics,
  untouched-holdout declaration, after-cost comparison, drawdown, at least 20
  completed prospective shadow sessions and independent registry decision.
- **Developer:** run
  `pytest -q tests/test_observability_strategy_research.py
  tests/test_observability_execution_shadow.py
  tests/test_observability_shadow_automation.py
  tests/test_dashboard_fno_v13_v10_g_research_views.py
  tests/test_dashboard_fno_v13_v10_g_observability_views.py
  tests/test_fno_v13_v10_g_launchers.py`; keep every research card outside
  task, restart, kill, heartbeat and live-monitor mappings.

The prediction view uses expanding windows by whole trading day. All rows for a
day are scored before any outcome from that day enters history. It backs off
from setup+regime to setup+side, side and finally global empirical estimates,
with Laplace smoothing and explicit minimum sample sizes. Regime thresholds are
fixed engineering buckets, not outcome-fitted filters. Current historical G
evidence is labelled `EXPLORATORY_REUSED_HISTORY_NO_UNTOUCHED_TEST`; therefore
these estimates may diagnose selection, entry and market shifts but cannot
support a claim that expected profit has improved.

The cards intentionally have no restart button, timeline entry, scheduler
state, heartbeat, kill switch or broker action. Missing reports show
`WAITING_OUTPUT`. Published reports show `READY` with `view_scope=ARTIFACT` and
are excluded from operational-health counts.

### Automatic prospective-shadow lifecycle

The installed weekday lifecycle is read-only and has no broker or execution
authority:

1. `EQIDV2_v13_shadow_prepare_0850` freezes the newest complete corrected
   dataset manifest, run metadata and frozen strategy/model configuration
   before the market decision window.
2. `EQIDV2_v13_shadow_seal_1525` accepts only a stable, complete, same-day set
   of all configured scanner slots and their exact selected signal union. It
   also accepts a genuine zero-signal day. Missing, partial, changed or
   noncanonical evidence fails closed.
3. The successful 16:20 dated replay invokes `finalize`, joins the exact sealed
   decisions to same-day replay/PAPER outcomes, then refreshes the dashboard.

Inspect the tasks without running them:

```powershell
$shadowTasks = @(
  'EQIDV2_v13_shadow_prepare_0850',
  'EQIDV2_v13_shadow_seal_1525',
  'EQIDV2_backtesting_result_v13_v10_g_1620'
)
Get-ScheduledTask -TaskName $shadowTasks |
  Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName EQIDV2_v13_shadow_prepare_0850
Get-ScheduledTaskInfo -TaskName EQIDV2_v13_shadow_seal_1525
```

The lifecycle can be inspected or exercised for an explicitly chosen session
date without importing a live worker:

```powershell
py -3.12 .\tools\v13_shadow_automation.py --session-date 2026-09-28 prepare
py -3.12 .\tools\v13_shadow_automation.py --session-date 2026-09-28 seal
py -3.12 .\tools\v13_shadow_automation.py --session-date 2026-09-28 finalize
py -3.12 .\tools\v13_prospective_shadow.py verify --session-date 2026-09-28
```

Do not manually backfill missed prepare/seal boundaries and count them as
prospective evidence. `SKIPPED_NOT_PREPARED` is an expected safe outcome;
`FAILED_CLOSED` or exit code 2 requires evidence repair and must not be counted.
Promotion remains blocked until at least 20 complete hash-verified sessions,
an untouched holdout and an independent registry decision all refer to the
same strategy/model cohort. The current registry schema cannot bind an
arbitrary historical approval to that cohort, so the dashboard fails closed.

## Event and evidence verification

Verify an API or worker event journal with its exact service name:

```powershell
python -m ai_platform.observability verify-events `
  --journal C:\TradingData\eqidv2\observability\journals\ai_platform_api_events.jsonl `
  --service ai_platform_api
```

The service argument is part of the journal hash contract; a different value is
not interchangeable.

## Alert delivery

The checked-in safe default groups alerts in the local Alertmanager UI only.
Before unattended live use, configure and test an owned receiver based on:

```text
configs/observability/alertmanager/alertmanager.receiver.example.yml
```

Do not commit SMTP/API credentials. Verify firing, grouping, deduplication and
resolved delivery. Until then, external paging is not enabled.

## Failure drills

Preview without changing a container:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Invoke-FailureDrill.ps1 `
  -Drill collector-outage
```

Execute only in paper mode or a maintenance window:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Invoke-FailureDrill.ps1 `
  -Drill collector-outage -DurationSeconds 30 -Execute `
  -AcknowledgePaperOrMaintenance PAPER_OR_MAINTENANCE
```

Available container drills are `collector-outage`, `prometheus-restart`,
`loki-restart` and `tempo-restart`. The script attempts restoration in `finally`.
Record the fields in `configs/observability/failure-drills.yml`.

## Retention and disk pressure

Defaults are Prometheus 90 days/20 GB, Loki 90 days, Tempo 30 days and
Alertmanager five days. Preview local telemetry-log cleanup:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Invoke-LogRetention.ps1 -KeepDays 90
```

By default the script targets
`%EQIDV2_RUNTIME_ROOT%\observability\logs`, falling back to
`C:\TradingData\eqidv2\observability\logs`. To inspect another configured
runtime, supply `-RuntimeRoot`; to narrow cleanup to that runtime's exact logs
directory or a child, supply `-LogRoot`:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Invoke-LogRetention.ps1 `
  -RuntimeRoot D:\TradingRuntime -LogRoot D:\TradingRuntime\observability\logs\api `
  -KeepDays 90
```

Apply only after reviewing every path:

```powershell
powershell -ExecutionPolicy Bypass -File .\bat\observability\Invoke-LogRetention.ps1 `
  -KeepDays 90 -Execute
```

The script accepts only the resolved telemetry logs directory or one of its
children, refuses reparse points, and removes only eligible old `.jsonl`,
`.log`, rotated or `.gz` files after `-Execute`. It excludes evidence,
manifests, order journals, feature ledgers, experiment/raw-observation and
reconciliation paths. Immutable trading evidence and raw point-in-time
observations have preservation status in `retention-policy.yml`.

## Acceptance checklist

Retain evidence from at least five complete paper market sessions proving:

- no telemetry-caused missed slot, delayed order or changed decision;
- measured p99 instrumentation overhead within the agreed budget;
- every selected signal/order resolves to correlated telemetry and evidence;
- raw-data and feature differences are explicit, never silently zero;
- observed and finalized reconciliation completes or reports `INDETERMINATE`;
- production-relevant alerts fire once, route and resolve correctly;
- collector/store outages create visible gaps without affecting trading;
- broker reconciliation is never reported healthy without broker truth;
- experiment verification passes and research never mutates live config.

Only then should OBS-12 be marked accepted. Strategy-profit conclusions require
separate prospective OBS-11 evidence and remain subject to costs, liquidity,
drawdown and tail-risk limits.
