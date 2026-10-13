# G3 production V3: 09:20 LONG promotion

The selected historical 09:20 LONG configuration is now part of the shared
`V13-V10-G-3-PRODUCTION-V3` decision contract used by the main daily backtest,
PAPER and the LIVE rule book. The existing fifteen setups retain their rules.
09:20 SHORT remains disabled.

Strategy fingerprint: `0119d67996b0f801dcfe55057d76e82482d10bec40898dab017262b09506a811`.

## Promoted configuration

| Setting | Value |
| --- | --- |
| Signal / exact confirmation | 09:20 / 09:21 IST |
| Cash price movement | First completed 09:15–09:20 bar close/open return >= 0.30% |
| OI increase | >= 0.10%, <= 1.00%, positive and increasing |
| Five-minute volume ratio | >= 3.00 |
| EMA alignment | EMA9 > EMA20 > EMA50 |
| Confirmation | Green candle, close above signal close |
| Confirmation body / range | >= 0.75 |
| Upper wick / range | <= 0.35 |
| One-minute volume ratio | >= 1.80; prior observed minutes, current candle excluded |
| Selection | Highest liquidity; maximum one entry for this setup/day |
| Entry trigger / expiry | Exact confirmation high / 09:31 IST |
| Target / initial stop | 3.00% / 1.25% |
| Stop tightening / square-off | 1.00% after 120 minutes / 15:15 IST |
| PAPER / backtest allocation | Rs100,000 margin x 5 = Rs500,000 exposure |
| LIVE allocation | Rs10,000 margin x 5 = Rs50,000 exposure; whole shares |

The 09:16-to-09:20 OI comparison is four minutes, not an adjacent five-minute
change. Only the new opening setup uses this basis. Regular market bars,
stored indicators and the later setups' calculations are not overwritten.

## Inputs and exclusions

Historical replay snapshots the same dated futures contract's verified
09:15–09:16 minute from `dashboard_flow_observer/historical/<date>/`, alongside
cash minutes, futures bars and the dated universe. Recovered evidence is
labelled retrospective; it does not prove live availability.

Runtime first accepts an exact minute acquired before 09:20. Otherwise it can
use the previously approved provisional full quote: exchange time
09:16:00–09:16:15, received within 15 seconds and before 09:20. The completed
09:20 futures bar must also have a verified receipt no later than evaluation.
The source, actual interval, evidence hash and availability accompany decisions.

A stock with missing, zero, stale, synthetic or mismatched opening OI is
excluded from **09:20 only**, and the exclusion is reported. It may qualify
at a later slot with valid inputs. No proxy OI is inserted. If no stock has
valid opening inputs, that slot/daily verification blocks rather than claiming
a valid zero-trade opening. Other existing market-data completeness gates remain.

The October 12 source has seven opening exclusions: BAJAJHLDNG, BANDHANBNK,
BOSCHLTD, GODFRYPHLP, LICHSGFIN, PRESTIGE and UNITDSPR. ALKEM separately lacks
its required 09:20 futures bar and is excluded by the existing stock-day policy.

## Sessions and schedules

| Existing scheduled task/session | Schedule and effect |
| --- | --- |
| `EQIDV2_dashboard_flow_observer_0855` | Weekdays 08:55; waits for authentication/universe; captures opening quotes through 09:21 |
| `EQIDV2_fno_v13_v10_g_3_scanner_5min_0915` | Starts 09:15; now includes the 09:20 signal |
| `EQIDV2_fno_v13_v10_g_3_equity_1min_feed_0915` | Starts 09:15; derives the new exact 09:21 confirmation from the shared clocks |
| `EQIDV2_fno_v13_v10_g_3_confirmation_1min_0915` | Starts 09:15; applies the 1.80 volume threshold at 09:21 |
| G3 `live_long`, `live_short`, trade logger and net-result tasks | Start 09:15; these existing scheduled execution workers are PAPER |
| G3 options PAPER tasks | Existing equity-fill consumer; receives any eligible new LONG fill through the shared source |
| `EQIDV2_backtesting_result_v13_v10_g_3_1620` | 16:20; recovers opening minutes, then runs native G3 V3 daily replay and analysis |
| `EQIDV2_dashboard_flow_observer_history_1700` | 17:00 standalone recovery retry |
| `EQIDV2_fno_v13_v10_g_3_live_kite_qty1_0915` | Remains disabled; its ordinary launcher is readiness-only |

Existing tasks already point to these launchers, so no duplicate scheduled
tasks are created. The shared configuration is loaded by each fresh scheduled
process. The schedule was checked on October 13; enabled tasks have their next
run that morning/afternoon and continue on weekdays, including October 14.
Workers were not launched into an overnight trading session.

**Broker execution is not armed.** The deployment remains `PAPER_VALIDATION`
with `live_activation_authorized=false`. Existing paper-session, reconciliation,
acknowledgement, deployment and dated fingerprint arming requirements still apply.
Preparing the LIVE code/configuration does not authorize or submit an order.

## File map

| Area | Files |
| --- | --- |
| Shared decision rules | `fno_v13_v10_g_3_contract.py`, `fno_v13_v10_g_3_live_config.py`, `fno_v13_v10_g_3_policy.py` |
| Deployment specification | `configs/strategies/v13_v10_g_3_production_v3.json`, `configs/strategies/active_profile.json` |
| Opening input validation | `fno_v13_v10_g_3_opening_inputs.py`; existing pure comparisons in `fno_v13_v10_g_3_opening_oi.py` |
| Native daily/history replay | `fno_v13_replay_source.py`, `fno_v13_v10_g_3_daily_replay.py`, `backtesting_result_v13_v10_g_3_daily.py` |
| Live/PAPER scanner and execution | `fno_v5_live.py`; existing G3 `live.py`, `paper.py`, `live_kite_session.py` wrappers inherit the new contract |
| Confirmation feed | Existing `fno_equity_fetch_1min.py` derives clocks from the G3 configuration |
| Daily launcher | `bat/run_backtesting_result_v13_v10_g_3_1620.bat`; recovery precedes replay, duplicate sidecar removed |
| Other launchers | Existing `bat/run_fno_v13_v10_g_3_*.bat` already select G3; no copied numeric rules required |
| Dashboard and evidence | `log_dashboard_server.py`, `fno_eq_id_monitor_detail.py`, `dashboard_flow.py`, `dashboard_flow_observer_sessions.py`, `ai_platform/observability/feature_ledger.py` |
| Local readiness | `fno_v13_v10_g_3_preflight.py`, existing `bat/preflight_fno_v13_v10_g_3.ps1` |
| Historical research isolation | `research/g3_v2_contract.py`, updated imports in `g3_0920_strategy.py`, `g3_0920_backtest.py`, `g3_0920_long_current.py`, `g3_0950_long_backtest.py` |
| Duplicate prevention | `research/g3_0920_long_daily.py` rejects already-promoted V3 daily inputs |
| Original study code evidence | Copies in `research/archived_g3_v2/`; original sealed run artifacts remain intact |

The original 49-session historical comparison remains a separate saved study.
V1/V2 results remain readable without relabelling them V3. Current V3 daily
history starts a separate revision cohort. Full native history rebuilding
falls back to raw dated inputs when an older sealed feature pool lacks the
09:20 evidence; it cannot silently omit the new setup.

## Validation and operations

The native October 12 replay completed successfully over 213 dated-universe
stocks (212 under the existing stock-day eligibility policy). It selected
POLYCAB at 09:20 and retained DELHIVERY at 09:25. Two fills, one win, one loss:
POLYCAB -Rs725.74 and DELHIVERY +Rs4,600.00, total **+Rs3,874.26**. Execution
prices, stops, targets and P&L match the prior independent daily comparison.
These figures use the PAPER/backtest allocation above, not LIVE allocation.

The main production result is
`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\production\runs\2026-10-12\20261013T011201132377_8407b0c0`.
The current daily/history pointers and execution/strategy analysis were
refreshed. Both dashboard servers were restarted and authenticated HTTP checks
confirmed production V3 and the preserved 49-session historical comparison.
The existing public tunnel was preserved.

The focused migration suite passed 268 tests, with additional targeted checks
for the historical fallback and runtime session date. The separate optional
AI platform API suite could not collect because FastAPI is absent in this
Python environment; the running main dashboard and Flow HTTP checks passed.
Local preflight, task schedules, parity checks, HTTP checks and JUnit results
are saved under
`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\readiness\2026-10-13`.

Offline tests cover threshold boundaries, unchanged older setups, receipt
timing, same-contract identity, quote provenance, missing-data exclusions,
scanner-to-confirmation selection, PAPER entry, LIVE sizing without submission,
snapshot integrity, revision isolation and dashboard visibility.

Read-only local check: `python fno_v13_v10_g_3_preflight.py`.
Daily manual launcher: `bat\run_backtesting_result_v13_v10_g_3_1620.bat --date YYYY-MM-DD`
after the session closes. This recovers opening evidence before replay.

The historical win rate is research evidence on a small selected sample, not
a runtime eligibility check or a promise about the next trading session.
