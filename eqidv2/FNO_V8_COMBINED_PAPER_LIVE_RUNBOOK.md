# FnO V8-Combined Paper Shadow - Operations Runbook

## Status and safety boundary

This wiring stages one independent **paper-only** V8-Combined shadow session.
It does not replace, edit, or import the V6 live runtime. The scheduled task is
created **Disabled**, the V8 dashboard card has no Restart control, and the task
is excluded from pre-open autofix.

The session contract is:

| Item | Value |
|---|---|
| Card/session ID | `fno_v8_combined_paper` |
| Scheduled task | `EQIDV2_fno_v8_combined_paper_0915` |
| Entry point | `fno_v8_combined_paper_session.py run` |
| Runner | `bat/run_fno_v8_combined_paper_session.bat` |
| Execution-mode environment | `FNO_V8_COMBINED_EXECUTION_MODE=PAPER` |
| Log | `logs/fno_v8_combined_paper.log` |
| Runtime status | `C:\TradingData\eqidv2\runtime_status\fno_v8_combined_paper.status` |
| Runtime heartbeat | `C:\TradingData\eqidv2\runtime_status\fno_v8_combined_paper.heartbeat` |
| Latest report | `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_v8_combined_paper.md` |

The paper session keeps all strategy state under the frozen independent root
`C:\TradingData\eqidv2\fno_oi\v8_combined_paper_v1\`. It must not write to
`v6_live`, V6 signals/orders, the V6 candidate-feed marker namespace, or V8
research/cache namespaces.

## What "all 8 apps" means

`app1` through `app8` are eight configured Kite API credential pairs. They are
a market-data capacity pool; they are not eight V8 strategy processes, eight
portfolios, or authorization to place eight copies of an order.

The existing system already uses the pool in three relevant places:

- the shared futures 5-minute fetcher partitions contracts across usable apps;
- the global incremental equity 1-minute fetcher round-robins across all usable
  apps and writes `C:\TradingData\eqidv2\stocks_indicators_1min_eq`;
- the V6 candidate-only 1-minute feed authenticates up to eight apps and
  partitions its candidates among them.

V8 intentionally has a stricter contract than those existing pool users. Its
dedicated market-data adapter must discover and authenticate exactly the
ordered roster `app1` through `app8` before it fetches any candidate candle. A
missing, expired, misnamed, or failed credential makes the V8 paper session
fail closed; it does not shrink the pool or silently fall back to fewer apps.

For every completed minute, V8 normalizes the immutable candidate set, sorts it
by symbol and instrument token, and assigns it deterministically by
round-robin position across ordered `app1` through `app8`. An app can therefore
receive zero candidates when the set is small, but it must still authenticate.
The adapter fetches exact completed NSE equity 1-minute candles only, records
per-app assignment/outcome evidence, places no orders, and publishes snapshots
only under the separate V8 root above.

Do not launch a second writer against `fno_oi\raw_equity_1m` or point this
adapter there: the existing V6 writer performs a read/merge/atomic replace
without a cross-process lock. The V8 adapter has per-process pacing, not a
shared cross-process Kite limiter, so the operator must account for API
contention while V6 and V8 both use the same eight credentials.

## Exact 5-minute to 1-minute causal flow

V8 independently evaluates the complete current mapped 208-stock universe.
V6 candidate rows are never a filter or V8 authority. For each signal slot,
the session must finish this causal sequence before the first S+1 decision:

1. Starting at `S+3 seconds`, poll the exact current slot under activation and
   deadline gates. When the final cash marker appears (normally near `S+17`),
   prewarm strict causal cash features for all 208 mapped stocks from
   `stocks_indicators_5min_eq_live`. Bind every source byte, the current
   universe, exact regular-session grid and prefix hash; require real
   `source_1m_count==5`, clean lineage and valid OHLCV geometry.
2. When the final futures marker appears (normally near `S+46`), prove for
   every mapped stock future that the unique exact `S` row has immediate sorted
   predecessor exactly `S-5`. Both rows must be valid, positive and identity-
   consistent for symbol, token, expiry, tick, lot and contract month. Bind the
   full source-file hash/size, universe and cash marker.
3. Join the sealed cash prewarm and exact-OI proof in memory for all 208 names,
   then apply every frozen V8 LONG/SHORT gate and rank. This independent table
   is the sole candidate authority.
4. Directly audit the union of independently V8-eligible cash names through
   Kite: one five-minute range request per name, deterministically partitioned
   over `app1` through `app8`. Require exactly five unique real minute starts
   `S-5` through `S-1`, and require their aggregate to match the sealed cash
   signal OHLCV.
5. Reapply exact predicates/ranks into separate LONG/SHORT books. Recheck both
   activation keys and the clock before evidence publication, fsync the direct
   audit and books, then recheck again immediately before the first reducer
   mutation. Both gates must be earlier than `S+1+3 seconds`; crossing either
   leaves zero registered candidates and forbids retrospective entry.
6. Register the independently derived books and checkpoint the reducer before
   polling the finalized V6 scanner snapshot. Validate and archive that scanner
   snapshot only as a post-registration session-validity diagnostic: it is
   required to publish by `S+1+3 seconds`, but it never delays candidate
   registration, filters candidates, becomes V8 authority, or removes any
   independently eligible V8 name. A late/missing diagnostic invalidates the
   PAPER session rather than enabling a retrospective entry.
7. Process subsequent exact same-session completed one-minute batches in time
   order through one global PAPER reducer. Recheck activation every completed
   minute and require the exact 15:30 close for a valid session.

All source, proof, timing, candidate and state-transition artifacts live under
the isolated V8 session directory. They include fetch/audit/archive/decision
timestamps and hashes needed to demonstrate that a decision preceded S+1.

## Existing V6 process boundary

V6 has seven scheduled cards: six roles in `fno_v5_live.py` plus its separate
durable candidate 1-minute feed. The cutover script changes only these six
downstream tasks:

1. `EQIDV2_fno_v6_equity_1min_feed_0919`
2. `EQIDV2_fno_v6_confirmation_1min_0919`
3. `EQIDV2_fno_v6_live_long_0920`
4. `EQIDV2_fno_v6_live_short_0920`
5. `EQIDV2_fno_v6_trade_logger_0920`
6. `EQIDV2_fno_v6_net_result_0920`

It intentionally keeps the shared upstream tasks and
`EQIDV2_fno_v6_scanner_5min_0918` enabled. The `0918`, `0919`, and `0920`
suffixes are legacy names; their installed trigger is 09:15.

## Stage the disabled task

Run this once from an elevated PowerShell prompt:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\bat\schedule_fno_v8_combined_paper_disabled.ps1
```

The installer constructs a dedicated hardened settings object with `-Disable`
and registers the task non-runnable from its first observable state. It does
not call the common hardener, because that helper enables a task and could race
with catch-up execution. `StartWhenAvailable` is deliberately false, so a
later enable cannot replay a missed prior trigger. It then verifies `Disabled`
and not `Running`. The
installer does not start the session and does not alter any V6 task.

Verify the staged state:

```powershell
Get-ScheduledTask -TaskName EQIDV2_fno_v8_combined_paper_0915 |
  Select-Object TaskName, State, @{Name='Enabled';Expression={$_.Settings.Enabled}}, Actions
```

Expected: `State=Disabled` and `Enabled=False`. The dashboard shows the card in
the FnO section as disabled/staged. There is intentionally no card-level
Restart button, and dashboard **Restart All** cannot start it.

The frozen scheduler definition is one enabled weekly trigger at exactly
09:15 Monday-Friday, one action with no arguments or working directory,
principal `Saarit` / `Interactive` / `Limited`, `StartWhenAvailable=False`,
`AllowDemandStart=False`, and no automatic restart (`RestartCount=0`). Both
the installer and cutover script verify the full definition; `-StartTime`
values other than `09:15` are rejected. This prevents dashboard, autofix, or
manual demand-start paths from replaying the PAPER session outside its sole
scheduled trigger.

## Required dashboard maintenance reload

The dashboard process must have loaded the reviewed source that blocks restart
and PID-kill actions for Disabled scheduled tasks. An older long-running
dashboard is not made safe merely because `log_dashboard_server.py` changed on
disk. Before minting a V8 permit, use a maintenance window with no dashboard
restart action in flight and reload only the local dashboard server while
keeping its public-link process:

```powershell
cmd /c .\bat\run_log_dashboard_restart_keep_url.bat
Start-Sleep -Seconds 10
python -c "import json; import fno_v8_combined_paper_control as c; print(json.dumps(c.require_dashboard_runtime_identity(), indent=2))"
```

The canonical `127.0.0.1:8787` server publishes a five-second heartbeat at
`C:\TradingData\eqidv2\runtime_status\log_dashboard_server.runtime.json`,
binding its PID, start time, endpoint and loaded source SHA-256. A missing,
stale, alternate-port or source-mismatched identity blocks V8 preflight before
credential discovery and before any scheduler mutation. Do not approve or
cut over V8 until this command succeeds.

## Pre-cutover checks

Begin and finish the cutover preflight before **09:13 IST**, leaving a
two-minute safety margin ahead of the 09:15 trigger. Neither V6 downstream nor
V8 may be running.
First run the automated tests:

```powershell
python -m pytest -q tests/test_fno_v8_combined_paper_ops.py
python -m pytest -q tests/test_fno_v8_combined_best_per_leg_backtest.py
python -m pytest -q tests/test_fno_v8_windowed_1m_entry_backtest.py
```

Then confirm:

- `fno_v8_combined_paper_session.py` exists and its own tests pass;
- the V8 task is Disabled and points to the approved paper runner;
- all six V6 downstream tasks are Enabled but not Running;
- the V6 5-minute scanner is Enabled but not Running;
- V8 has no LIVE order-placement path;
- V8 report/status/heartbeat/artifact paths do not overlap V6.

## Approve and arm one paper session

Approval is deliberately split into two control actions and a separate
scheduler cutover. Replace the example date, operator and reason below. The
first command creates an immutable one-date permit but leaves the kill switch
engaged:

```powershell
python .\fno_v8_combined_paper_control.py approve `
  --session-date 2026-08-24 `
  --approved-by "Saarit" `
  --reason "Reviewed prospective V8 paper shadow" `
  --phrase "I APPROVE ONE SESSION OF FNO V8-COMBINED PAPER ONLY"
```

Copy the returned `permit_id`, then explicitly disengage the PAPER kill switch:

```powershell
python .\fno_v8_combined_paper_control.py disarm `
  --session-date 2026-08-24 `
  --permit-id "<PERMIT_ID>" `
  --actor "Saarit" `
  --reason "Begin approved paper observation"
```

Before any scheduler state changes, verify the exact runtime bundle, today's
permit and kill switch, and all eight authenticated Kite apps:

```powershell
python .\fno_v8_combined_paper_session.py preflight `
  --require-activation --authenticate-apps
```

The permit expires after that session and cannot be reused. Source-code or
strategy-policy drift also invalidates it. Neither control command starts a
process or changes Task Scheduler.

This is intentionally a **one-session** switch. After the paper session, V8
must be returned to Disabled and the six V6 downstream definitions restored
before the next trading day. Otherwise the expired V8 permit would block V8
while the persistent scheduler state still kept V6 disabled.

## Approved scheduler cutover

The switch script refuses to mutate anything unless both the `-Execute` switch
and the exact case-sensitive approval phrase are supplied:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\bat\switch_fno_v6_1m_to_v8_paper_after_approval.ps1 `
  -Execute `
  -ApprovalPhrase I_APPROVE_FNO_V6_1M_TO_V8_COMBINED_PAPER
```

The scheduler approval phrase is intentionally different from the session
permit phrase above. The script validates every task and runner, reruns the
activation/eight-app preflight, then rechecks the clock and every V6/V8 task
state immediately before its first mutation. If the preflight reaches 09:13,
or any task starts or changes state during authentication, the cutover aborts
with V6 unchanged. Otherwise it first disables the five V6 consumer/output
tasks, enables and verifies V8, and only then disables the V6 one-minute feed.
This ordering prevents pre-open autofix from observing both pipeline markers
disabled. The V6 scanner remains enabled. The script does **not** run any task;
the new state applies at the next scheduled trigger.

The enabled V8 task is also the scheduler-backed cutover-mode marker used by
the pre-open healthcheck. While that mode is selected, the intentionally
disabled V6 one-minute feed is not an autofix target. The healthcheck instead
requires today's valid V8 permit and disengaged bound kill switch. If those
are missing or expired, it reports a failure with no automatic start action;
it never launches the V6 BAT behind an enabled V8 pipeline.
It also requires the scanner Enabled and all six V6 downstream definitions
Disabled and idle. Any partial/re-enabled V6 state is a non-autofixable
coherence failure, preventing an unnoticed V6/V8 overlap.
After a two-minute startup grace, it additionally requires the V8 task to be
Running with a fresh same-session PAPER heartbeat. If V8 exits at startup,
health remains FAIL and no automatic V6/V8 restart or fallback is attempted.

If any mutation or verification fails, it restores the original enabled/
disabled state of all seven tasks it could have changed. A rollback error is
reported explicitly and must be resolved in Task Scheduler before market open.

## Recovery boundary

Do not manually toggle the V8 or V6 scheduler definitions. Scheduler recovery
is permitted only through the guarded restore script below and only before
**08:55 IST**, before the pre-open autofix loop begins. At or after 08:55, do
not change either pipeline's task definitions: V6 uses
`StartWhenAvailable=True`, and a manual enable after its missed trigger can
launch it immediately. A partial toggle can also make autofix launch the V6
feed behind V8.

After a controlled pre-open recovery, verify all task states and inspect:

- `C:\TradingData\eqidv2\runtime_status\fno_v6_*.status`;
- `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_v6_*.md`;
- `logs\fno_v8_combined_paper.log` for any attempted V8 start.

For an in-session PAPER intervention, engage the kill switch first. Revoke the
permit as well when the session must remain off:

```powershell
python .\fno_v8_combined_paper_control.py kill `
  --session-date 2026-08-24 `
  --actor "Saarit" `
  --reason "Operator stopped the paper observation"

python .\fno_v8_combined_paper_control.py revoke `
  --session-date 2026-08-24 `
  --actor "Saarit" `
  --reason "Paper session revoked"
```

On its next completed-minute control check, the reducer cancels pending PAPER
orders and resolves only accepted modeled-open positions at that exact
completed candle. This is an intervention close, not a broker fill. If the
required candle is missing or invalid, the session remains unresolved and
data-incomplete; it must not report zero P&L or a valid completion.

## Required next-session pre-open restore

Restore the default scheduler state before **08:55 IST** on the next intended
V6 trading day, while V8 and all six V6 downstream roles are idle. Do not run
this restoration after the 09:15 trigger has been missed: the installed V6
tasks use `StartWhenAvailable=True`, so enabling them after the close can cause
an immediate catch-up launch rather than a harmless definition-only change.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\bat\restore_fno_v6_1m_after_v8_paper.ps1 `
  -Execute `
  -ApprovalPhrase I_RESTORE_FNO_V6_1M_AND_DISABLE_V8_PAPER
```

The script disables V8 first, enables exactly the six V6 downstream task
definitions, verifies the V6 scanner remains enabled, and starts or ends no
process. It refuses at or after 08:55, any non-idle managed task, and any
partial scheduler mode; it rolls scheduler states back if a mutation fails.

## Promotion boundary

This is a prospective research shadow, not a LIVE promotion. V8-Combined was
selected as a research configuration and needs new-session shadow evidence.
Do not add it to dashboard restart/autofix sets, add LIVE credentials/arming,
or disable the V6 scanner unless a separate reviewed production change
explicitly authorizes those actions.
