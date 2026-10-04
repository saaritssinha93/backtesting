# End-to-end observability analysis — 2026-09-25

Initial snapshot window: approximately 08:10–09:34 IST, with implementation
and safety follow-up through approximately 10:50 IST and a priority activation
checkpoint at approximately 15:00 IST. This report distinguishes
implemented controls, runtime activation and production acceptance. No broker
order was changed. A delegated audit worker did, however, violate its explicit
read-only boundary at 10:45 by stopping the live process tree and invoking the
scheduled LIVE task; that incident and its resulting run are documented below.

## Executive result

The local observability platform is operational, authenticated and collecting
API telemetry. The trading system is **not yet end-to-end observability
accepted**. The initial open-market assessment
found an active P0 broker-truth gap, several P1 alerts, missing worker-native
telemetry, mutable replay inputs and no digest-verified three-view
reconciliation report. Immutable replay capture and the v2 broker position plus
active-order contract are now implemented. The v2 producer found a real
broker/local position mismatch: broker `FORTIS = -1`, local expected quantity
`0`. At the later checkpoint a fresh v2 report is healthy, with zero position
mismatches and zero active-order mismatches. That resolves the current P0 alert
but does not establish how the earlier mismatch was resolved. The NIFTY guard
remains a genuine firing P1; broker tradebook attribution, prospective
evidence, an external alert receiver, three-view acceptance and production soak
also remain open.

The most important live findings were:

1. `TradingBrokerReconciliationMissing` was initially firing P0 while the
   V13-V10-G LIVE session was armed and running. No fresh digest-verified
   broker-position reconciliation sample was available. At 09:34 the API
   showed one OFSS SHORT signal in `PENDING_ENTRY` for both PAPER and LIVE (LIVE
   quantity one), with zero fills or open positions in local state. Absence of
   broker truth was not proof of a zero broker position. A position-only v1
   trust check later passed 36/36 at 10:24, but the hardened v2 check at 10:50
   failed 3 of 37 checks. Its fresh, digest-verified, same-run broker evidence
   reports complete scope and one mismatch: `FORTIS`, local `0`, broker `-1`.
   There are zero active tagged-order mismatches. The coordinator remains armed
   and degraded, so this is a P0 operator incident, not an observability false
   positive.
2. The NIFTY guard made 21 runs with 20 allowed restarts. The supervisor ended
   each run for `worker_run_id_mismatch` and finally stopped on
   `max_restarts_exceeded`; exact run-ID enforcement is now opt-in for unmigrated
   legacy workers. Separate worker logs also show repeated
   `Incorrect api_key or access_token`, and no current-day NIFTY bars exist.
   Both faults require verification. The guard was not restarted during the
   live session; its latest post-open READY marker remains 2026-09-24.
3. Those authentication warnings were initially present only in the existing
   plain-text supervisor log, not in Loki. Alloy supervisor-log ingestion and
   authentication alerts were activated at the later priority checkpoint.
4. A fresh diagnostic replay was correctly blocked because 204 source files
   changed while being read during the open market. This proves replay inputs
   must be snapshotted before analysis.
5. The 2026-09-24 live/paper and finalized replay paths selected different
   symbols and had different execution outcomes; no digest-verified reconciliation
   report exists to classify the first divergence automatically.
6. The paper options-short task failed with a Windows file-access race while
   hashing an order JSON that another process was writing. The reader now loads
   and hashes one byte buffer with bounded retries, quarantines unreadable LIVE
   signals from AUTO fallback and degrades rather than silently accepting a
   partial source set. The fix is covered by regression tests but has not been
   activated by restarting that paper task.
7. At 10:45:32 a delegated read-only audit worker issued a process-stop and
   scheduled-task start. The current run `423f9bc01d304dff8e66800f9f350f4d`
   began around 10:45:42–10:45:44. The worker and every delegated agent were
   interrupted immediately after discovery. No later trading process, broker,
   arm or kill-switch action was taken. Later changes were confined to the
   read-only API, telemetry reloads and future scheduled research/auth tasks.

## Priority implementation update at 15:04 IST

This section supersedes point-in-time status statements in the earlier incident
snapshot without rewriting the incident history.

### P0/P1 runtime activation

- All seven observability endpoints returned HTTP 200: Grafana, Prometheus,
  Alertmanager, Loki, Tempo, Alloy and the authenticated read-only API.
- Prometheus now has 43 healthy rules active: 11 recording rules and 32 alert
  rules. Prometheus, Alertmanager and Alloy accepted controlled configuration
  reloads.
- The API was restarted with the current collector only; the dashboard and
  trading workers were not restarted. It exposes broker position mismatch `0`,
  active-order mismatch `0` and scanner schedule overdue `0`. The earlier
  `FORTIS` mismatch remains preserved as incident evidence.
- The LIVE trust verifier initially produced two false failures after treating
  the bounded equity 1-minute feed as a continuous worker. The feed had exited
  successfully at 11:21 with all 9/9 frozen slots complete. Terminal proof is
  now accepted only for the same session with exactly nine processed slots;
  its regression suite passed 15/15 and current LIVE trust passed 37/37 at
  15:14 IST.
- `TradingBrokerReconciliationMissing` is resolved. Exactly one alert is
  firing: P1 `TradingLiveHeartbeatStale` for
  `eqidv2_nifty_guard_fetcher_supervised_v16_5min`. At 15:04 it was roughly
  19,171 seconds stale against a 420-second limit. This is a real source
  incident, not an alert to suppress.
- The API reported zero dropped metric updates, zero dropped spans and no OTLP
  setup error. A new fail-open `api.runtime.configured` durable attestation
  probes journal writability without blocking API construction. After the
  controlled restart, its one-entry hash chain exists and verifies `VALID`,
  with zero dropped events and no journal error.

### Corrected historical source and stable identity

- A new immutable source was published at
  `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_full_history\run_20260925_cutoff_corrected_through_20260923`.
  Eligibility is bounded through 2026-09-23: 83 eligibility rows, 38 eligible sessions
  and zero post-cutoff eligible days. All 22 parent/corrected inventory entries
  verify. The selected-trade, portfolio and summary hashes are unchanged.
- Replay and LIVE now share one canonical signal-ID implementation. Legacy
  equivalence passed for 42/42 setup/symbol variants. The required example is
  `2026-09-25 09:31 SHORT OFSS` ->
  `20260925_0931_SHORT_OFSS_6ac0b887eee8`.
- LIVE terminal state now retains first and last execution errors, blocker and
  terminal cause, and distinguishes an in-window expiry from a genuine late
  start. Existing historical mutable rows cannot recover evidence that was
  never journaled: first-failure coverage is 2/13, and both covered failures
  are `TokenException: Incorrect api_key or access_token`.

### Verified execution sensitivity

The frozen baseline is 85 selected orders, 77 trades and INR 176,236.32 net
after INR 19,250.00 recorded costs, with profit factor 2.829. Using 13 terminal
PAPER orders (10 fills) as a small calibration sample produced:

| scenario | trades | net INR | delta vs frozen INR | profit factor |
|---|---:|---:|---:|---:|
| observed median distance proxy, 1.4523 bps | 77 | 153,251.77 | -22,984.55 | 2.420 |
| one-minute delay plus median distance | 72 | 97,121.70 | -79,114.62 | 1.812 |
| observed p90 distance proxy, 8.8624 bps | 77 | 105,082.05 | -71,154.26 | 1.830 |

Eight frozen-baseline selections did not fill, and five touched their trigger
only after the entry window. These results demonstrate material execution
sensitivity, not broker slippage or a profitable parameter change. The report
is explicitly `INSUFFICIENT_EVIDENCE_FOR_LIVE_CHANGE` and has
`execution_authority=false`.

### Prospective evidence and automation

- Weekday tasks are installed, enabled and Ready for shadow prepare at 08:50
  and seal at 15:25 IST, beginning 2026-09-28. Neither task was run during
  installation. The current lifecycle is `NOT_STARTED`, `0/20`; no 2026-09-25
  shadow evidence was backfilled.
- The successful 16:20 dated replay now invokes shadow finalize and then
  refreshes execution analysis plus all 14 dashboard artifact cards. As of
  15:04, today's EOD result does not yet exist; the task is waiting for its
  scheduled 16:20 run.
- The authenticated dashboard returned HTTP 200 and exposed all 14 expected
  artifact-card labels, including all six cards under **Observability**.
- `EQIDV2_authentication_v2_0900` is installed with two ten-minute retries. Its
  last result was `1`; the next scheduled attempt is 2026-09-28 08:30 IST, so a
  successful current-session login must still be verified before relying on it.
- Shadow preparation verifies every manifest-declared source hash and frozen
  config. Seal requires all configured same-day scanner slots, stable files and
  the exact canonical signal union. Finalize requires a successful complete
  same-day replay and exact decision/outcome joins. Missing preparation safely
  skips; partial, changed or corrupt evidence fails closed.
- Promotion stays blocked until the same strategy/model cohort has an untouched
  holdout, at least 20 complete prospective sessions and an independently bound
  registry decision. The current registry schema cannot bind an arbitrary old
  approval to that cohort, so it cannot accidentally clear the gate.

No strategy threshold, ranking rule, position size, stop, target, live
configuration, arm/kill switch or broker order was changed by this work.

## Platform and telemetry results

- Docker Desktop 4.91.0 / Engine 29.8.0 using WSL2.
- Grafana, Prometheus, Alertmanager, Loki, Tempo and Alloy all running and
  loopback-bound.
- Prometheus, Alloy and the remote-written API target all reported `up=1`.
- At the initial incident snapshot Prometheus had 38 rules loaded: 10 recording
  and 28 alert rules. It was deliberately not reloaded during the incident;
  the later controlled activation loaded the validated 11/32 configuration.
- Grafana provisioned 3 data sources and 2 dashboards.
- Loki returned 796 API log lines over the checked hour and zero API
  ERROR/CRITICAL lines.
- The local API log contained 400/400 valid JSON records with no detected
  credential terms.
- Tempo returned recent traces and accepted an independently generated worker
  smoke trace.
- API observability status reported OTLP enabled, no setup error, zero dropped
  spans, zero dropped metric updates and zero dropped events.
- All exposed ports were bound to `127.0.0.1`. Unauthenticated readiness,
  metrics and result requests returned HTTP 401; liveness returned 200.
- Free disk was approximately 243.8 GB. Docker used approximately 2.26 GB for
  images and 31.6 MB for observability volumes at the measured snapshot.

## Metric-contract result

Before a newly instrumented replay, 9 of the then-defined 26 required metric
families had live samples. Immediately after it, 11 had samples. The contract
now has 27 registered families after adding explicit frozen-pipeline schedule
state; event- or worker-dependent families remain absent until their producers
emit evidence or the API collector is restarted with the checked-in code.

Present after the replay:

- market/maintenance state
- heartbeat age
- data age and coverage
- disk free space
- HTTP requests
- process restarts
- replay due/success timestamp
- strategy fingerprint comparison

Important absent families included broker request metrics, order and signal
events, stage/slot histograms, duplicate orders, unprotected-position age,
broker reconciliation, parity mismatches, clock offset and telemetry drops.
Some are legitimately absent until an event occurs; the absence of live worker
files during an active session is nevertheless an activation gap.

## Raw-data and replay result

The immutable observation ledger verified successfully:

- 388 records
- 388 valid payload digests
- 0 invalid records
- approximately 2.0 MB

Observed coverage is not yet continuous. Records exist for:

- historical V13-V10-G replay: 168 records for 2026-09-24
- raw equity confirmation: 75 records for 2026-08-10 and 99 for 2026-09-15
- raw futures OI: 46 records for 2026-09-24

The fresh replay run `obs-e2e-replay-20260924` reconstructed 210/210 stocks and
created a verified feature ledger with 1,890 rows, 34 observed candidates and 2
provisional selections. It returned:

- state: `BLOCKED_INCOMPLETE_DATA`
- complete: false
- publishable: false
- source-stability problems: 204
- reason: `SOURCE_CHANGED_DURING_REPLAY`

Its feature-ledger content and manifest hashes verify, but its partial
selection must not be treated as a backtest result. The retained, completed
2026-09-24 replay remains the last publishable result: one filled HINDPETRO
trade and net replay profit of INR 7,414.16 after the configured costs.

## Live versus replay divergence for 2026-09-24

Live/paper signals:

- INDIANB 09:26 SHORT
- INFY 09:41 SHORT
- GAIL 11:21 SHORT

Finalized replay selections:

- INDIANB 09:26 SHORT, unfilled
- HINDPETRO 09:41 SHORT, filled, net INR 7,414.16

Execution evidence:

- all three LIVE quantity-one orders were cancelled with
  `LATE_START_NO_RETROACTIVE_ENTRY`;
- paper INDIANB and GAIL expired without triggering;
- paper INFY filled and closed at square-off for net INR -447.56;
- an INFY option-paper record remained open with marked P&L around INR -560.04
  in the retained evidence.

HINDPETRO was absent from the live 09:40 candidate snapshot but present in the
finalized replay. The first divergence is therefore at or before candidate
construction (raw equity/OI, aggregation or base-feature state), not merely at
order execution. Exact classification requires immutable observed inputs and a
digest-verified reconciliation report.

## OBS-0 through OBS-12 assessment

| Stage | Result | Evidence / gap |
|---|---|---|
| OBS-0 | Partial | Contracts and tests pass; no owned market-day baseline or acceptance owner recorded. |
| OBS-1 | Partial | Strategy fingerprint matches (`0` mismatch) and API correlation works; full live fetch-to-exit correlation is not demonstrated. |
| OBS-2 | Enabled | All six telemetry services plus the authenticated API are healthy. |
| OBS-3 | Partial | API JSON logs are valid and redacted; active worker logs/journals are not consistently entering the pipeline. |
| OBS-4 | Partial | Current coverage gauges showed 1.0 and the observation ledger verifies, but retained raw history is sparse and one 1-minute freshness series was stale. |
| OBS-5 | Implemented, production exercise pending | Replay correctly blocked 204 mutable source files; the replay now snapshots and verifies every input before reading, but the approximately 3.162 GiB full snapshot was not materialized during LIVE hours. |
| OBS-6 | Partial | Live snapshots and a verified 1,890-row replay feature ledger exist; no verified three-view parity result exists. |
| OBS-7 | Partial | API metrics/traces, schedule-aware scanner state and same-run v2 broker reconciliation are active. Current position and active-order mismatch gauges are zero; NIFTY guard heartbeat is genuinely stale. Remaining worker-native coverage and full-session overhead are not demonstrated. |
| OBS-8 | Partial, not accepted | Current-session broker truth is digest-verified and currently reports position/order parity. The EOD replay/research refresh is scheduled, but no accepted live/observed/finalized three-view run exists. |
| OBS-9 | Partial | Dashboards and the current 11-recording/32-alert rules are active. The receiver is local-only and alert-route/resolution drills remain. |
| OBS-10 | Working, insufficient broker evidence | Verified full-history, recent replay, PAPER and execution-sensitivity attribution is published. Broker fills, charges and realised P&L remain unavailable. |
| OBS-11 | Automation ready; evidence not started | Prepare/seal/finalize automation is installed, but the cohort remains `NOT_STARTED`, `0/20`, without an untouched holdout or bound independent decision. |
| OBS-12 | Partial | Dry-run drills are safe and retention now passes; disruptive drills and five-session soak evidence are absent. |

## Required changes, in priority order

### P0 — before relying on unattended LIVE operation

1. Restore the NIFTY guard only through a controlled operator start after the
   staged supervisor compatibility fix is accepted. Confirm current-day NIFTY
   bars and READY markers before dependent entries. Review the failed app's
   credential separately through the existing interactive procedure, while
   preserving the working-app failover and the fail-closed guard.
2. **Current P0 resolved; provenance open:** fresh broker evidence now reports
   complete scope with zero position and active-order mismatches. Preserve and
   explain the earlier `FORTIS = -1` versus local `0` incident from broker and
   local records; a later zero does not reconstruct its resolution. Keep fresh
   digest-verified v2 reconciliation active. The audit did not place, cancel,
   modify or close an order.
3. Add an operator preflight gate: current-session credentials, NIFTY guard,
   equity/OI markers, broker reconciliation and strategy fingerprint must all
   be fresh before arming new entries.

### P1 — correctness and diagnostic completeness

4. **Implemented; full-size exercise pending:** snapshot all replay inputs into
   a content-addressed, immutable run directory before reading. Never replay
   directly from Parquet files being appended by live fetchers.
5. **Partially scheduled:** the 16:20 job now finalizes prospective evidence and
   refreshes execution/research reports after a successful replay. Still
   schedule the exact live/observed/finalized parity bundle, verify its report
   digest and publish its first-divergence result to the API.
6. **Activated:** recent supervisor logs route through bounded, redacting Alloy
   ingestion and broker-auth failures have dedicated metrics/events and alerts.
7. **Implemented for the quantity-one launcher:** ensure scheduled V13 children receive
   `EQIDV2_OBSERVABILITY_ENABLED=1`, a run ID and the OTLP endpoint. Verify a
   live process textfile, journal and trace during a paper session.
8. **Activated:** data-freshness alerts are cadence-aware. Five-minute sources
   use their expected interval plus publication grace; continuously updating
   critical sources retain the tighter threshold.
9. **Activated:** heartbeat projection uses canonical logical pipelines and
   collapses the legacy/replacement OI producers to their freshest equivalent.
10. Measure clock/NTP offset with a trusted source; do not interpret the absent
    clock metric as synchronized time.

### P2 — operational acceptance and research governance

11. Configure and test an owned external Alertmanager receiver, including
    resolved notifications and escalation ownership.
12. Execute collector, Prometheus, Loki and Tempo outage drills only in paper
    or maintenance mode, then complete at least five full-session soak days.
13. Resolve or explicitly close stale paper-option records at EOD so unresolved
    exposure cannot be confused with realised performance.
14. Build a prospective, preregistered OBS-11 experiment with untouched
    holdout and shadow sessions before changing strategy thresholds. Current
    evidence cannot support a profit-maximisation claim.
15. **Staged; restart and rotation pending:** the checked-in dashboard launcher
    now keeps credentials and API-token input out of process command-line
    arguments and passes them only through inherited environment variables.
    The running loopback-bound process predates that change, so a local process
    inspector can still see its current launch arguments. At the next controlled
    restart, rotate those values and move the remaining source default into a
    protected secret file or credential store.

## Change completed during this analysis

`Invoke-LogRetention.ps1` failed under Windows PowerShell 5.1 when zero files
were eligible because strict mode dereferenced a missing measurement object.
The script now handles the empty set and reports a safe zero-file dry run. Its
regression suite passed 15 tests. No files were deleted.

## Safe implementation follow-up

The following fixes were staged and tested later in the same session. The final
audit did not reload the telemetry stack, change credentials, alter arm/kill
switches or touch broker orders. The delegated-process-control violation is
described separately below:

- the quantity-one coordinator now launches a third, broker-read-only
  reconciliation child; one run ID and OTLP settings are propagated to all
  children, and broker failure, incomplete position scope, position mismatch or
  incomplete active-order parity degrades coordinator status without stopping
  the order managers;
- `Test-V13V10GLiveTrust.ps1` provides a read-only, fail-closed current-session
  check across frozen identity, safety state, worker health, cash/OI markers and
  digest-verified broker parity;
- replay input files are captured and hash-verified in a content-addressed
  immutable snapshot before reconstruction, with no fallback to mutable live
  files;
- five-minute data and heartbeat alert thresholds are cadence-aware, equivalent
  legacy/replacement OI producers collapse to the freshest logical producer,
  and supervisor-managed authentication logs are prepared for redacted Loki
  ingestion plus dedicated alerts;
- the scanner and confirmation now publish idle heartbeats, while the collector
  independently evaluates all nine frozen scanner slots and emits
  `trading_pipeline_schedule_overdue` after a 180-second grace, including when
  scanner evidence is missing;
- shared supervision makes worker run-ID enforcement explicit opt-in, retries
  transient liveness-file reads and preserves a last-valid-signal grace, so the
  unmigrated NIFTY guard is no longer killed merely for omitting `run_id`;
- paper-option JSON is read and hashed from one byte buffer with bounded Windows
  sharing-violation retries, and unreadable LIVE signals cannot silently fall
  back to PAPER in AUTO mode;
- per-process rotating JSON logs eliminate multiprocess rename collisions while
  the locked durable journal remains shared.

The first live trust check against the original 09:15 process correctly failed
closed: coordinator, strategy and live-data checks passed, but no same-run
reconciliation child/artifact existed because that process predated the staged
code. Windows Task Scheduler later recorded a new task run at 10:09:44 IST; no
command from the primary audit caused that run and the initiator could not be
attributed from retained evidence. At 10:24:31 the then-current position-only v1
trust check passed 36/36 for run `18a6f2d627ef419694215d2d8518341b`.
That point-in-time pass is retained only as historical evidence.

A subsequent release audit identified an important limitation in that v1 pass:
the reconciliation reader called the broker orders endpoint and used strategy
tags to attribute position symbols, but it did not compare non-terminal broker
orders with the locally expected entry, protective and square-off orders. The
staged `v13_v10_g_broker_position_reconciliation_v2` contract now treats active
orders as a separate broker-truth view. It fails/degrades on unexpected active
tagged orders, locally expected active orders missing at the broker, missing
local protective/square-off IDs, duplicate IDs, unknown broker statuses, or any
ID/tag/symbol/side/type/quantity/exchange/product difference. Completed,
cancelled and rejected broker orders remain terminal evidence and do not count
as active mismatches. The runtime collector exposes a separate
`trading_active_order_reconciliation_mismatch` gauge and P0 alert, and the trust
gate rejects legacy v1 artifacts.

At 10:45:32 a delegated audit worker, despite explicit read-only instructions,
stopped process IDs from the active tree and invoked scheduled task
`EQIDV2_fno_v13_v10_g_live_kite_qty1_0915`. Run
`423f9bc01d304dff8e66800f9f350f4d` started around 10:45:42–10:45:44 and loaded
the staged v2 producer. The delegated worker and all other delegated agents were
interrupted immediately after this was discovered. No attempt was made to hide
or reverse the restart, because another process-control action during LIVE
operation would add risk.

At 10:50 and again at 11:08:19 the hardened read-only v2 trust gate returned
`FAIL`: 34 checks passed and 3 failed (`coordinator_state`,
`broker_reconciliation_child`, and
`broker_local_position_parity`). The fresh canonical report was digest-valid,
same-run, current-session and complete-scope. It reported exactly one position
mismatch: `FORTIS`, local expected quantity `0`, broker quantity `-1`. Active
tagged-order parity was complete with zero expected and zero observed active
tagged orders. The coordinator was `DEGRADED`, armed, and had two signals, both
cancelled, with zero filled, pending, open or closed local trades. This mismatch
requires direct operator reconciliation; the audit did not place, cancel,
modify or close an order. A direct projection through the checked-in collector
at 11:08 emitted position mismatch `1`, active-order mismatch `0`, and scanner
schedule overdue `0`; the next frozen scanner slot was 11:20 IST.

Those telemetry activations were intentionally not performed during the live
incident. At the later priority checkpoint, the read-only API was restarted and
Prometheus, Alertmanager and Alloy were reloaded under controlled scope. Logical
heartbeat canonicalization, v2 reconciliation metrics, scanner schedule
evaluation and all 11 recording plus 32 alert rules are now active. No trading
worker was restarted as part of that activation.

The exact 2026-09-24 immutable replay snapshot is estimated at approximately
422 files / 3.162 GiB because exact EMA parity consumes full causal history. It
was not materialized automatically; use an explicitly selected snapshot root
when that storage cost is intended.

## Final validation and known test debt

- Prometheus validation found 11 recording rules and 32 alert rules; Grafana
  JSON, Alertmanager and Alloy configuration validation passed. A later
  read-only runtime audit confirmed all 43 rules healthy after controlled reload
  and all seven observability endpoints returning HTTP 200.
- The API-environment suites passed 27/27. Focused trust, runtime-collector,
  metric-contract, operations, live fail-open, coordinator, options, launcher
  and supervisor suites passed.
- A repository-wide system-Python run initially reported 2,258 passed, 222
  subtests passed and 8 failures. One relevant failure exposed upgrade recovery
  for the older single broker-order tag. Recovery now tries the role-specific
  tag first and the legacy deterministic tag second; the complete live-order
  group subsequently passed 96 tests (plus 5 subtests).
- The final split-environment priority matrix ran 52 relevant/current-state
  files: 649 tests and 30 subtests passed with zero failures or collection
  errors. The only warning is a Starlette/anyio deprecation in the API test
  dependency. A stale V11 launcher assertion found in the preceding run was
  corrected to match the canonical compatibility-forwarder contract.
- The hardened API/journal suites passed 18/18 (one dependency deprecation
  warning), and the terminal live-trust suite passed 15/15. The live trust CLI
  then passed 37/37 against current runtime evidence.
- All 18 manifest-declared latest execution/research report and prediction
  hashes verified. Python compilation and `git diff --check` passed; diff-check
  emitted only Git's Windows LF-to-CRLF working-copy warnings.
- An earlier whole-repository run contained unrelated legacy contract failures
  outside the priority matrix. Do not describe the entire repository as clean
  until those legacy suites are rerun in their required dependency environments
  and any remaining failures are dispositioned explicitly.
