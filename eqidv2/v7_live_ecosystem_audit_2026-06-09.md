# V7 ID Live Trading Ecosystem Audit

Audit date: 2026-06-09  
Session observed: live market hours, around 10:25 to 10:32 IST  
Scope: V7 ID 5-minute signal discovery, 1-minute entry engine, live signal writer, paper/live executors, dashboard, scheduled jobs, supporting data feeds, research jobs, and runtime process health.

## A. Executive Summary

The V7 ID live ecosystem is not live-safe today. The live executor task is currently disabled, and it should remain disabled until the P0 issues below are fixed and verified in a full live-paper replay or next-session dry run.

The signal discovery layer is alive and publishing candidates. The 5-minute feed is alive, but it is often close to the timing budget. The paper executor is alive and managing existing paper trades. The broken part of the production flow is the handoff from discovery to tradable signal CSVs: the entry engine runtime is stale/crashing and has not been delivering fresh signals even though discovery produced candidates later in the session.

There are also serious production hygiene risks: the public dashboard is reachable through a Cloudflare quick tunnel with weak credentials currently accepted by the running process, a heavy backtesting/data-moving task is mis-scheduled during market hours, replay mode can write into production candidate paths, and old multiprocessing workers are still orphaned from prior days.

Top verdict:

| Area | Status | Production meaning |
| --- | --- | --- |
| Live executor | Disabled | Good. Do not enable yet. |
| Paper executor | Running | Useful for observation, but not proof of live readiness. |
| 5-minute feed | Running, WARN latency | Functional but timing margin is thin. |
| Signal discovery | Running | Produces candidates, but Tier123 can skip when feed/scan lag is high. |
| Entry engine | Broken/stale runtime | Main blocker. Fresh discovery candidates are not reaching signal CSVs. |
| Dashboard | Public and weakly protected | Immediate security and control-plane risk. |
| Scheduler hygiene | Unsafe | Heavy data/backtesting task runs during live hours. |
| Replay safety | Unsafe | Replay can contaminate live candidate files. |

## B. Architecture And Dependency Map

Current V7 ID production path:

```text
Kite auth / token export
        |
        v
5-minute live data feed
        |
        v
signal discovery v7 5min ID
        |
        v
latest candidate JSON/CSV
        |
        v
1-minute entry engine v5 ID
        |
        v
eqidv2_live_signal_writer.py
        |
        v
live signal CSVs, long and short
        |
        +--> paper executor, PAPER_TRADE_TRUE
        |
        +--> live executor, PAPER_TRADE_FALSE, currently disabled
```

Important side systems:

| System | Relationship to live path |
| --- | --- |
| Dashboard server | Control and observability plane. Can trigger kill switches. |
| Cloudflare public link | Exposes dashboard outside localhost. |
| Nifty guard | Market guard input. Indirect risk control dependency. |
| V7 research layer | Monitoring/research. Reads live artifacts, does not write live signals. |
| V7 pre-momentum analyst | Monitoring/research. Reads audits and writes suggestions. |
| Daily live V7 research session | Monitoring/research/reporting. |
| Backtesting/data-moving jobs | Should be after-market only. Currently mis-scheduled into live hours. |
| Replay batch | Should be isolated. Currently writes production candidate paths. |

## C. Active Jobs And Purpose

Observed scheduled V7-related tasks:

| Task | Observed state | Purpose | Notes |
| --- | --- | --- | --- |
| `EQIDV2_signal_discovery_v7_5mins_ID` | Running | Persistent V7 ID 5-minute scanner | Main candidate producer. |
| `EQIDV2_entry_engine_1min_v5_ID` | Running according to scheduler | Converts candidates to executable 1-minute signals | Runtime is stale/crashing. |
| `EQIDV2_paper_trade_id_5min_v7_0900` | Running | Paper executor for V7 ID signals | Managing paper open state. |
| `EQIDV2_live_trade_id_5min_v7_0900` | Disabled | Real Kite executor | Keep disabled. |
| `EQIDV2_eod_5mins_data_0900` | Running | Live 5-minute data feed | Current feed state was WARN but usable. |
| `EQIDV2_log_dashboard_start_0855` | Running | Dashboard control plane | Public tunnel active. Weak current credentials. |
| `EQIDV2_kite_export_start_0915` | Running | Kite/token/export support | Direct dependency for broker connectivity. |
| `EQIDV2_nifty_guard_fetch_v16_5min_0915` | Running | Nifty guard input | Indirect risk control. |
| `EQIDV2_v7_pre_momentum_filter_analyst_0917` | Running | Research/monitoring | Reads audits, writes suggestions. |
| `EQIDV2_v7_research_layer_0917` | Running | Research/ops snapshots | Reads live artifacts, should alert harder on stale entry state. |
| `EQIDV2_daily_live_v7_research_0917` | Running | Periodic research reporting | Indirect monitoring. |
| `EQIDV2_data_for_backtesting_1545` | Running during live hours | Moving/backtesting data | Mis-scheduled. Major interference risk. |

Selected scheduler result codes observed:

| Task | Last run | Result | Interpretation |
| --- | --- | --- | --- |
| `EQIDV2_entry_engine_1min_v5_ID` | 2026-06-09 10:25:45 | `267009` / `0x41301` | Running. Does not imply healthy. |
| `EQIDV2_paper_trade_id_5min_v7_0900` | 2026-06-09 09:00:00 | `267009` / `0x41301` | Running. |
| `EQIDV2_live_trade_id_5min_v7_0900` | 2026-05-22 09:00:00 | `267014` / `0x41306` | Disabled/terminated historical state. |
| `EQIDV2_signal_discovery_v7_5mins_ID` | 2026-06-09 09:00:00 | `2147946720` / `0x800710E0` | Needs scheduler interpretation, but process is active. |

## D. Direct And Indirect Relation To Live Trading

Direct trading path:

| Component | Directness | Why it matters |
| --- | --- | --- |
| 5-minute feed | Direct | Discovery depends on fresh 5-minute bars. |
| Signal discovery | Direct | Creates candidate tickers and side selection. |
| Entry engine | Direct | Decides executable entry rows and writes signal handoff. |
| Live signal writer | Direct | Enforces freshness contract and writes signal CSVs consumed by executors. |
| Paper executor | Direct for paper only | Validates handoff behavior and manages paper positions. |
| Live executor | Direct for real orders | Disabled now; unsafe to enable until parity and control risks are fixed. |

Indirect but important:

| Component | Directness | Why it matters |
| --- | --- | --- |
| Dashboard | Control plane | Can operate kill switches and observe system state. If exposed weakly, it becomes a trading control risk. |
| Cloudflare tunnel | Access plane | Exposes dashboard publicly. Must not use weak/default secrets. |
| Research layer | Monitoring | Should detect stale handoffs and timing degradation. Currently insufficiently forceful. |
| Backtesting/moving-files task | Interference risk | Should not run during live; heavy CPU/disk/data writes can starve live components. |
| Replay batch | Contamination risk | Can write into production candidate files. |
| Old scanner/live batch files | Confusion risk | Disabled, but incompatible settings can be accidentally revived. |

## E. Timing And Latency Bottlenecks

The system is operating with thin timing margins.

Observed 5-minute feed state at slot 10:25:

| Metric | Value |
| --- | --- |
| Feed slot | 10:25 |
| Updated at | 10:25:44 |
| Total elapsed | About 42.0 seconds |
| SLA state | WARN |
| Overall state | WARN |
| Verification failures | 0 |
| Partition symbols | About 1254 total |

Observed discovery at slot 10:25:

| Metric | Value |
| --- | --- |
| Feed ready wait | About 45 seconds after slot |
| Main scan elapsed | About 16 seconds |
| Candidate publish time | About T+61 seconds |
| Candidates | 2 short, 0 long |
| Tier123 overlay | Skipped because scan start lag was about 45.2s, above 40s cap |

This means the handoff can easily publish around T+60 to T+73. The live signal contract allows a 30-second max detection lag from the intended T+1 entry reference. That leaves only roughly 17 to 30 seconds of practical margin before the entry signal becomes stale.

Current latency risk:

| Bottleneck | Risk |
| --- | --- |
| 5-minute feed often WARN around 40-50 seconds | Discovery starts late, Tier123 can skip, entry window tightens. |
| Discovery scan about 16 seconds | Reasonable by itself, but risky after slow feed. |
| Entry engine waits up to 30 seconds for candidates | Good design, but current runtime is crashing/stale. |
| Heavy backtesting/data-moving task running during live | Can compete for CPU, disk, and data directories. |
| Old orphan worker processes | Adds process table and memory noise; may indicate cleanup bugs. |

## F. Freshness And Stale Data Risks

Critical freshness issue: entry engine output is stale.

Evidence:

| Artifact | Observed state |
| --- | --- |
| `logs\eqidv2_entry_engine_1min_v5_id_2026-06-09.log` | Repeated `AttributeError: 'NoneType' object has no attribute 'get'` in `_build_entry_rows`. |
| Entry worker PID | Spawned at about 10:26:39. |
| Entry engine source file | Modified at about 10:26:50, after the worker spawned. |
| `latest_summary.json` | Stuck around slot 09:40 while discovery had later candidates. |
| Live short signal CSV | Only signals at 09:31 and 09:36. |
| Discovery latest candidates | Slot 10:25 had two short candidates. |

The file on disk now contains a partial fix for missing `entry_bar`, using `signal_close` as fallback when the T+1 1-minute bar is not available. But the running process likely loaded the old code before that fix and needs a controlled restart.

Other stale/freshness risks:

| Risk | Detail |
| --- | --- |
| Entry status reports wrong script | Runtime status says `eqidv2_live_combined_analyser_csv_v15.py` because the entry BAT does not set `EQIDV2_RUNTIME_SCRIPT_NAME`. |
| Non-atomic latest writes | Entry engine and research layer still use direct `to_csv` / `write_text` in several latest paths. |
| Research layer JSON decode error | Indicates a reader may observe a blank or partially written file. |
| Universe mismatch | Scanner observed around 1280 symbols while feed partitions wrote about 1253/1254. |
| Replay writes live candidate files | Replay can overwrite or append to production latest/daily candidate artifacts. |

## G. Logic And Parameter Issues

Important logic/parameter findings:

| Area | Current behavior | Issue |
| --- | --- | --- |
| Entry reference | Current file can fall back to `signal_close` when no T+1 1-minute bar exists | Good direction, but the running worker likely has old crashing code. |
| Entry freshness | Writer rejects rows if lag is negative or greater than 30 seconds | Good, but this makes feed/discovery latency margin tight. |
| Signal contract | `MAX_LAG_SEC=30`, `ENTRY_LAG_MIN=1` | Contract is clear, but comments still mention older T+1 to T+1:15 language. |
| Same ticker/slot dedupe | Entry engine drops duplicates by `bar_time_ist,ticker`, ignoring side | Probably intentional to prevent same ticker double orders, but must be explicit in docs/dashboard. |
| Short focus | `SHORT_FOCUS=1` | Long stream is effectively disabled; dashboard/reporting should state this clearly. |
| Live quantity | Live BAT sets `EQIDV7_ID_5MIN_FORCE_ENTRY_QUANTITY=1` | Live is not equivalent to paper/backtest sizing. |
| Config attestation | Live BAT sets `EQIDV2_CONFIG_CHECK_BYPASS=1` | Removes the value of strict paper/live parity checks. |
| Paper vs live risk gates | Paper has daily loss brake/time stop/session cap; live differs | Live behavior is not a faithful production mirror. |
| Old disabled stack | Older V7 scanner BATs exist with incompatible lag settings | Keep retired and clearly labeled to avoid accidental scheduling. |

## H. Monitoring Gaps

The dashboard and research jobs are useful, but they are not yet strong enough for production live trading.

Observed gaps:

| Gap | Why it matters |
| --- | --- |
| Scheduler state can show "running" while process is unhealthy | Entry engine was running but stale/crashing. |
| Entry stale condition did not become a hard red alert | Discovery produced candidates while entry latest summary stayed old. |
| Dashboard accepted weak current token | A control plane must be treated as a security-sensitive service. |
| Dashboard logs token query strings | Tokens in URLs leak into logs and browser history. |
| Dashboard can still start in `NO AUTH` mode from code | BAT helps, but server should enforce this itself. |
| No single health object for the full V7 path | Health is scattered across logs, JSONs, CSVs, and scheduler state. |
| Orphan workers not surfaced | 88 old child processes should be a red operational indicator. |
| Heavy maintenance during live not alerted | The data/backtesting task should have been flagged immediately. |
| Universe mismatch not prominent | Feed universe and scanner universe must be aligned or explicitly quarantined. |

## I. Reliability Risks

Reliability risks ranked by blast radius:

| Risk | Consequence |
| --- | --- |
| Entry engine stale/crashing | No fresh signals reach executors, or stale rows are written after recovery. |
| Weak public dashboard auth | Unauthorized control-plane access, including kill switch endpoints. |
| Mis-scheduled data/backtesting task | Live latency spikes, file contention, partial writes, and CPU/disk contention. |
| Replay contamination | Historical replay can pollute live latest/daily candidate files. |
| Live executor parity bypass | Real orders can run with unverified config and quantity mismatch. |
| Non-atomic writes | Readers can see blank/partial files, causing crashes or stale decisions. |
| Orphan child processes | Process leaks, handle leaks, false health signals, resource pressure. |
| Thin timing budget | A modest feed delay can cause missed entries or overlay skips. |
| Cloudflare quick tunnel | Public URL can change, has no production uptime model, and expands attack surface. |

## J. Exact Recommended Fixes

P0 fixes to do before enabling real live trading:

1. Restart and verify the entry engine.
   - Stop `EQIDV2_entry_engine_1min_v5_ID`.
   - Confirm no old worker remains.
   - Start it from current source after the `entry_bar is None` fallback.
   - Verify `latest_summary.json` advances to the next live slot.
   - Verify new discovery candidates create either accepted entry rows or explicit rejection rows.
   - Add a regression test for missing T+1 1-minute bar fallback.

2. Harden dashboard authentication immediately.
   - Kill the currently running dashboard process that accepts `eqidv2` as username/password/token.
   - Rotate the Cloudflare quick tunnel URL.
   - Use high-entropy password and token only.
   - Remove hard-coded weak defaults from BAT files.
   - Make `log_dashboard_server.py` refuse startup when no auth is configured.
   - Stop logging tokens in query strings, or move tokens to headers only.

3. Disable and recreate the backtesting/data-moving task.
   - Disable `EQIDV2_data_for_backtesting_1545` immediately.
   - Recreate it as a true 15:45 after-market task.
   - Remove live-hour repetition.
   - Add a scheduler audit that fails if maintenance jobs run between 09:15 and 15:35 IST.

4. Isolate replay output.
   - Change replay to write to `signal_discovery_v7_5mins_ID_replay` or a timestamped replay root.
   - Add a `--replay-output-root` or `--dry-run` flag.
   - Make production latest/daily candidate paths read-only for replay by default.

5. Remove live executor parity bypasses before enabling live.
   - Remove `EQIDV2_CONFIG_CHECK_BYPASS=1`.
   - Remove or explicitly document `EQIDV7_ID_5MIN_FORCE_ENTRY_QUANTITY=1`.
   - Align live and paper risk gates, or generate an attestation explaining each intentional difference.
   - Run paper/live config diff as a mandatory preflight.

6. Clean orphan worker processes.
   - Terminate old orphan `multiprocessing.spawn` children from June 5 and June 8.
   - Add parent-existence checks and shutdown cleanup.
   - Add dashboard alert when orphan worker count is greater than zero.

## K. P0 / P1 / P2 Ranking

P0 - Block live trading:

| Item | Why P0 |
| --- | --- |
| Entry engine runtime stale/crashing | Direct signal handoff is broken. |
| Dashboard public weak auth | Control plane exposed with weak credentials. |
| Backtesting/data-moving task running live | Can interfere with live feed/entry and writes. |
| Replay writes production paths | Can contaminate live candidate handoff. |
| Live executor bypass/quantity mismatch | Real orders would not match tested/paper assumptions. |
| Orphan process cleanup | Indicates lifecycle leaks and resource/control risk. |

P1 - Fix before sustained unattended paper/live:

| Item | Why P1 |
| --- | --- |
| Thin latency margin | Feed/discovery frequently consume most of freshness budget. |
| Universe mismatch | Scanner and feed do not clearly operate over identical symbol set. |
| Wrong entry runtime script name | Monitoring points to the wrong process/script. |
| Non-atomic latest writes | Causes reader crashes and false stale states. |
| Research layer weak stale escalation | It noticed stale data but did not make it a hard stop. |
| Paper/live risk gate differences | Paper does not fully predict live behavior. |
| Long/short mode visibility | `SHORT_FOCUS=1` needs to be explicit in every monitor. |
| Retired incompatible BATs | Accidental revival can reintroduce old lag/logic. |

P2 - Cleanup and polish:

| Item | Why P2 |
| --- | --- |
| Stale comments around T+1/T+1:15 | Confuses operators and future audits. |
| Stale audit label `missing_1min_entry_bar` | No longer always a rejection after fallback. |
| Cloudflare quick tunnel | Usable for personal monitoring, weak for production ops. |
| Test tooling missing | `pytest` is not installed, so regression tests cannot run locally. |
| Dashboard URL/token hygiene | Move away from URL query tokens. |

## L. Quick Wins

Highest leverage changes that can be done quickly:

1. Restart entry engine and verify a fresh slot advances end to end.
2. Disable `EQIDV2_data_for_backtesting_1545` during live hours.
3. Restart dashboard with rotated high-entropy credentials and a fresh tunnel.
4. Remove `EQIDV2_CONFIG_CHECK_BYPASS=1` from the live executor BAT.
5. Set `EQIDV2_RUNTIME_SCRIPT_NAME=eqidv2_entry_engine_1min_v5_id.py` in the entry engine BAT.
6. Add a dashboard red alert when entry latest slot is older than discovery latest slot.
7. Add an atomic write helper to entry engine latest CSV/JSON outputs.
8. Rename or move replay BATs so they cannot be run accidentally against live paths.

## M. Structural Improvements

Recommended structural direction:

| Improvement | Benefit |
| --- | --- |
| Single V7 health aggregator JSON | One dashboard/API object can decide green/yellow/red for the whole path. |
| Explicit stage contracts | Each stage publishes slot, source slot, publish time, data age, counts, and rejects. |
| Atomic writes everywhere for latest artifacts | Prevents partial reads and JSON decode errors. |
| Scheduler safety audit | Detects live-hour maintenance jobs and disabled/renamed production tasks. |
| Process supervisor with stale-output checks | Running process must prove fresh output, not just process existence. |
| Paper/live config attestation | Blocks live start if paper/live mismatch is unapproved. |
| Replay sandboxing | Historical tests cannot mutate production latest/daily artifacts. |
| Latency SLO tracking | Use P50/P95/P99 feed, discovery, entry, and executor timings. |
| Universe manifest | Feed, scanner, ranker, and executor use the same dated manifest or explicit quarantine list. |

## N. Suggested Improved Monitor Format

Create one V7 live monitor object, refreshed every few seconds:

```json
{
  "as_of_ist": "2026-06-09 10:26:05",
  "market_session": "OPEN",
  "live_executor_enabled": false,
  "dashboard_auth": {
    "mode": "basic_and_token",
    "weak_default_detected": false,
    "public_tunnel": true
  },
  "slot": {
    "current_5m": "10:25",
    "expected_entry_time": "10:26",
    "max_signal_deadline": "10:26:30"
  },
  "feed": {
    "slot": "10:25",
    "state": "WARN",
    "elapsed_sec": 42.0,
    "updated_lag_sec": 44,
    "expected_symbols": 1280,
    "written_symbols": 1254,
    "verification_failed_count": 0
  },
  "discovery": {
    "slot": "10:25",
    "publish_delay_sec": 61,
    "scan_elapsed_sec": 16,
    "final_long": 0,
    "final_short": 2,
    "tier123_state": "skipped",
    "tier123_skip_reason": "scan_start_lag_gt_40"
  },
  "entry_engine": {
    "slot": "10:25",
    "latest_summary_age_sec": 20,
    "candidate_count": 2,
    "rows_selected": 0,
    "rows_written": 0,
    "freshness_rejected": 0,
    "last_exception": null
  },
  "executors": {
    "paper": {
      "state": "RUNNING",
      "open_positions": 1,
      "signals_today": 2,
      "latest_signal_time": "09:36"
    },
    "live": {
      "state": "DISABLED",
      "config_attested": false,
      "force_quantity": 1
    }
  },
  "system": {
    "orphan_worker_count": 0,
    "live_hour_maintenance_jobs": [],
    "stale_artifacts": []
  },
  "overall": {
    "state": "RED",
    "reasons": [
      "entry_engine_stale",
      "live_executor_config_not_attested"
    ]
  }
}
```

Suggested red/yellow rules:

| Rule | Severity |
| --- | --- |
| Entry slot older than discovery slot | RED |
| Entry latest summary older than one slot | RED |
| Any repeated entry exception | RED |
| Dashboard weak/default credential detected | RED |
| Live-hour maintenance task running | RED |
| Replay writing production path | RED |
| Feed elapsed greater than 60 seconds | RED |
| Feed WARN for 3 consecutive slots | YELLOW escalating to RED |
| Discovery publish delay greater than 75 seconds | YELLOW |
| Tier123 skipped due to scan lag | YELLOW |
| Orphan workers greater than zero | YELLOW, RED if persistent |
| Universe mismatch greater than zero | YELLOW, RED if symbols are tradable universe members |

## O. Specific Code, File, And Function-Level Changes

Recommended edits:

| File | Function/area | Change |
| --- | --- | --- |
| `bat\run_eqidv2_entry_engine_1min_v5_id.bat` | Environment | Set `EQIDV2_RUNTIME_SCRIPT_NAME=eqidv2_entry_engine_1min_v5_id.py`. |
| `eqidv2_entry_engine_1min_v5_id.py` | `_build_entry_rows` | Keep the `entry_bar is None` fallback and add test coverage so it never regresses. |
| `eqidv2_entry_engine_1min_v5_id.py` | latest writes | Use atomic temp-write-and-replace for `latest_summary.json`, `latest_entry_engine_rows.csv`, and audit latest files. |
| `eqidv2_entry_engine_1min_v5_id.py` | `_entry_reject_audit` | Rename or split `missing_1min_entry_bar` now that missing bar can fall back to signal close. |
| `eqidv2_entry_engine_1min_v5_id.py` | comments | Update stale T+1 to T+1:15 comments to the current 30-second contract. |
| `eqidv2_signal_discovery_v7_5min_id_persistent.py` | replay branch | Add isolated replay output root and prevent writes to production latest/daily files. |
| `bat\run_eqidv2_signal_discovery_v7_replay_today.bat` | replay default | Point to replay-only root and add a clear safety banner. |
| `log_dashboard_server.py` | startup | Refuse startup if neither strong basic auth nor strong API token is configured. |
| `log_dashboard_server.py` | request logging | Redact `token=` query parameters from logs. Prefer header token auth. |
| `bat\run_log_dashboard_server.bat` | defaults | Remove hard-coded weak defaults. Require externally supplied secrets. |
| `bat\run_log_dashboard_public_link_scheduled.bat` | tunnel | Refuse to start when current secret is missing or matches known weak defaults. |
| `bat\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat` | live config | Remove `EQIDV2_CONFIG_CHECK_BYPASS=1`. |
| `bat\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat` | quantity | Remove `EQIDV7_ID_5MIN_FORCE_ENTRY_QUANTITY=1` or mark it as an explicit tiny-live mode. |
| `avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py` | preflight | Refuse live start if config attestation is bypassed outside a named emergency flag. |
| `log_dashboard_server.py` | V7 monitor | Add full V7 stage health, stale entry detection, orphan worker count, and live-hour maintenance detection. |
| Scheduler BATs | task creation | Recreate `EQIDV2_data_for_backtesting_1545` to run at 15:45 only, after market. |
| Tests | regression suite | Add/enable tests for feed gate, entry fallback, freshness reject, dashboard auth, kill switch scopes, and paper/live config diff. |

Recommended operational commands or actions:

| Action | Expected outcome |
| --- | --- |
| Disable `EQIDV2_data_for_backtesting_1545` now | Removes heavy live-hour interference. |
| Restart entry engine from current source | Fresh slots should advance beyond 09:40. |
| Restart dashboard with rotated token/password | `token=eqidv2` should return 401. New secret should return 200. |
| Kill old orphan spawn workers | Process table should only show current-day children under current parent. |
| Run py_compile and pytest | Compile must pass; pytest currently needs installation. |

## P. Final Checklist

Do not enable the live executor until every P0 item is complete:

| Check | Required state |
| --- | --- |
| Live executor remains disabled | Yes, until all checks below pass. |
| Entry engine advances to current slot | Required. |
| Entry engine has no repeated exceptions for at least one full session | Required. |
| Discovery candidate slot and entry summary slot stay aligned | Required. |
| Signal writer freshness rejects are understood and visible | Required. |
| Paper executor receives fresh signals after discovery publishes candidates | Required. |
| Dashboard no longer accepts weak/default credentials | Required. |
| Public tunnel secret rotated and token not logged | Required. |
| Backtesting/data-moving job removed from live hours | Required. |
| Replay cannot write production latest/daily candidate paths | Required. |
| Live executor config attestation enabled | Required. |
| Live quantity sizing matches approved paper/live plan | Required. |
| Orphan workers cleaned and monitored | Required. |
| Feed/scanner universe mismatch resolved or quarantined | Required. |
| `pytest` installed and focused tests passing | Required before code-change confidence. |

Verification performed during audit:

| Check | Result |
| --- | --- |
| Key Python module compile check | Passed. |
| Focused pytest run | Could not run because `pytest` is not installed. |
| Live scheduler/process/log inspection | Completed. |
| Signal discovery latest artifacts inspected | Completed. |
| Entry engine latest artifacts and logs inspected | Completed. |
| Dashboard process/log/public URL inspected | Completed. |
| Scheduler trigger inspection for maintenance task | Completed. |

Final recommendation: treat the current system as a live-paper observation environment only. Keep real trading disabled until the entry handoff, dashboard security, schedule hygiene, replay isolation, and live executor parity controls are fixed and verified.
