# V13-V10-G dashboard, paper trading and live execution

**Policy update effective 2026-10-06:** All dated equity scanner, confirmation,
paper and quantity-one live sessions adopt the G-2 09:25 LONG relaxation and
1.25% to 1.00% stop tightening after 120 minutes. See
[the promotion record](V13_V10_G_IMPLEMENTATION_STATUS.md). The tables below
describe the original baseline; their targets remain active, but their fixed
equity stops are superseded for sessions on/after October 6. Scheduled launchers
already reference the updated workspace and require no additional flag.

Updated: 2026-09-14. The current dashboard sessions, launchers, logs and scheduled-task names use V13-V10-G. Existing V6 launcher filenames are compatibility forwarders to the canonical G launchers. Internal V6 feed-generation and acknowledgement names remain compatibility contracts; strategy evidence and reports use G's isolated location. The separate V10/V11/V12 shared paper session is unchanged.

## Active strategy and source

- Backtesting engine: [fno_v13_v10_g_backtest.py](fno_v13_v10_g_backtest.py).
- Runtime adapter: [fno_v13_v10_g_live_config.py](fno_v13_v10_g_live_config.py).
- Frozen strategy version: `FNO_V13_V10_G_RETAINED_20260914`.
- Profile selector in all eight launchers: `FNO_V6_STRATEGY_PROFILE=V13_V10_G`.
- Pinned configuration: `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion/frozen_config.json`.
- Configuration SHA-256: `d8bcae37d7725279f8ac6e5c4c96a44e8a43f41b5e42d413b167fd806b52a127`.

The adapter checks the pinned configuration and selected ledger identity. Rejected extra morning slots and two-candle continuation are disabled. The retained G preserves F's first-choice candidates and admits newly eligible SHORT candidates into vacant setup slots after reducing the SHORT price-move thresholds by 35%. Existing setup quotas remain fixed.

The pinned exploratory replay contains 73 selected orders, 66 executions, 43 wins and 23 losses across 31 sessions: 65.15% win rate and PF 3.4303. These are historical research results; they are not today's paper or broker results.

## Dashboard connections

The repeated scanner in the request is represented by one card. The durable one-minute feed is also retained, giving eleven distinct cards.

| Canonical card ID | G role | Output below `C:/TradingData/eqidv2/fno_oi/` |
|---|---|---|
| `fno_v13_v10_g_scanner_5min` | Equity 5m + futures OI candidate scan | `latest/latest_fno_v13_v10_g_scanner_5min.md` |
| `fno_v13_v10_g_equity_1min_feed` | Durable, completed candidate 1m evidence | `latest/latest_fno_v13_v10_g_equity_1min_feed.md` |
| `fno_v13_v10_g_confirmation_1min` | Exact next-minute confirmation and ranked selection | `latest/latest_fno_v13_v10_g_confirmation_1min.md` |
| `fno_v13_v10_g_live_long` | LONG paper entries | `latest/latest_fno_v13_v10_g_live_long.md` |
| `fno_v13_v10_g_live_short` | SHORT paper entries | `latest/latest_fno_v13_v10_g_live_short.md` |
| `fno_v13_v10_g_trade_logger` | Consolidated paper trade log | `latest/latest_fno_v13_v10_g_trade_logger.md` |
| `fno_v13_v10_g_net_result` | Paper realized/unrealized net result | `latest/latest_fno_v13_v10_g_net_result.md` |
| `live_signals_csv_fno_id_v13_v10_g_short` | Live SHORT entry sheet | `v13_v10_g_live/live_kite/signals_<date>_fno_id_v13_v10_g_short.csv` |
| `live_signals_csv_fno_id_v13_v10_g_long` | Live LONG entry sheet | `v13_v10_g_live/live_kite/signals_<date>_fno_id_v13_v10_g_long.csv` |
| `live_kite_trades_csv_fno_id_v13_v10_g` | Actual quantity-one broker trade ledger | `v13_v10_g_live/live_kite/live_trades_<date>_fno_id_v13_v10_g.csv` |
| `kite_trade_fno_id_v13_v10_g` | Live coordinator log | Repository `logs/fno_v13_v10_g_live_kite_qty1_<date>.log` |

The seven paper/feed cards use `run_fno_v13_v10_g_<role>.bat`, G status/heartbeat filenames and matching G restart connections. Their scheduled-task names are `EQIDV2_fno_v13_v10_g_<role>_0915`, all at 09:15 IST on weekdays. The four live views share `fno_v13_v10_g_live_kite_qty1`, its canonical launcher, and its existing supervisor protections. Dashboard live-order restart controls have not been added.

The old eight `run_fno_v6_*.bat` paths forward arguments and exit codes to their G equivalents. They do not own separate workers or logs. `bat/schedule_fno_oi_weekday.ps1` installs the seven G paper/feed tasks and retires the seven V6 task aliases. `bat/schedule_fno_v13_v10_g_live_kite_qty1_weekday.ps1` installs only the canonical G broker task and refuses migration while its legacy task is running. The old V6 broker installer forwards to this canonical installer. Installers do not start the tasks or alter live-arm/kill-switch state.

All G scanner, confirmation, signal and order state lives under `fno_oi/v13_v10_g_live/`. The paper CSV is `consolidated/fno_v13_v10_g_trades_<date>.csv` inside this root. Old `fno_oi/v6_live/` strategy evidence is not consumed as G evidence.

The dashboard checks the reported strategy version. Old V6 status is shown as an identity mismatch; absent G evidence is shown as awaiting G runtime. A renamed card therefore does not establish that an old worker is executing G.

## Selection, confirmation and exits

The strategy has fourteen direction-specific setups across nine five-minute signal times. All times are completed-candle ends in IST. SL and target percentages are inherited from the pinned G/B exit table.

| Signal end | Confirmation end | Side | SL % | Target % |
|---|---|---|---:|---:|
| 09:25 | 09:26 | LONG | 0.60 | 0.97 |
| 09:25 | 09:26 | SHORT | 0.60 | 2.67 |
| 09:30 | 09:31 | LONG | 0.85 | 2.00 |
| 09:30 | 09:31 | SHORT | 0.89 | 2.00 |
| 09:35 | 09:36 | LONG | 0.60 | 1.73 |
| 09:35 | 09:36 | SHORT | 0.82 | 1.23 |
| 09:40 | 09:41 | LONG | 0.60 | 3.00 |
| 09:40 | 09:41 | SHORT | 0.60 | 2.43 |
| 09:45 | 09:46 | LONG | 0.82 | 1.23 |
| 09:45 | 09:46 | SHORT | 0.82 | 1.23 |
| 09:50 | 09:51 | SHORT | 0.60 | 0.93 |
| 09:55 | 09:56 | LONG | 0.60 | 0.90 |
| 10:00 | 10:01 | LONG | 0.60 | 3.00 |
| 11:20 | 11:21 | SHORT | 0.62 | 2.00 |

Five-minute candidates require the directional EMA9/EMA20/EMA50 stack, positive futures OI growth, the raw OI floor of 0.05%, maximum OI change of 1%, raw directional price move of at least 0.10%, and five-minute volume ratio of at least 0.80. Each setup then imposes its own stricter price, OI, volume and candle thresholds. The five-minute volume filters are preserved.

The exact S+1 confirmation candle must move in the trade direction and close beyond the signal close. Its body and relevant wick must pass the selected setup's requirements. Confirmation volume must be at least 1.20 times the mean of the previous twenty observed completed one-minute candles. The confirmation candle and future bars are excluded from the denominator.

The historical calculation permits a five-observation minimum at the start of available history. The paper/live producer requires all twenty prior candles so a short download cannot masquerade as complete history: it requests seven days, expands to thirty-five days if needed, and rejects the candidate if the warmup remains insufficient. This conservative operational guard is fingerprinted; it can reject a newly listed or long-suspended stock that the historical minimum-five rule would admit. Only regular-session candles ending 09:16 through 15:30 are used. The frozen 31-session selection parity test still matches all 73 selected orders exactly.

The 09:25 SHORT setup additionally requires the dated near-month NIFTY futures 09:20 candle's open-to-close return to be at most -0.05%. Missing or ambiguous NIFTY evidence blocks that setup; it is not treated as neutral cash-index context.

Two deadlines are deliberately separate:

- Confirmation publication grace: 90 seconds after the exact confirmation candle closes. A late, newly reconstructed confirmation cannot be admitted as timely evidence.
- Trigger/fill expiry: ten minutes after that confirmation close. For the 11:20 signal, confirmation is 11:21 and trigger expiry is 11:31. The live CSV field `activation_deadline_ist`, displayed as `entry_expires_at_ist`, carries this trigger deadline.

Entries cannot fill on their own confirmation candle. Full exits use the frozen stop and target, with no partial exits or break-even rule. Square-off is 15:15 IST. Actual broker fills, tick rounding and charges are recorded separately from the backtest's modeled execution.

## Paper sizing and the existing live pilot

Paper trading uses ₹1,00,000 allocated per trade with modeled 5× exposure, approximately ₹5,00,000 position value before integer quantity rounding. The shared LONG/SHORT paper capital book is ₹10,00,000, with no separate three-position cap. Both sides draw from the same capacity calculation.

The existing live pilot remains fixed at one share per executed trade. It does not deploy ₹1,00,000 per broker order. Its signal sheet includes both the fixed execution quantity and strategy-sized quantity for comparison. Scaling the live pilot to portfolio-sized orders is a separate operational change.

The original live control files remain authoritative:

- `C:/TradingData/eqidv2/fno_oi/v6_live/live_arm.json`.
- `C:/TradingData/eqidv2/fno_oi/v6_live/kill_switch.json`.

Live execution requires the existing acknowledgement, an enabled arm file dated to the session, an arm `strategy_fingerprint` equal to the current G fingerprint, and an inactive kill switch. The canonical quantity-one launcher now refreshes that dated G arm record automatically on every trading-session start. The kill switch remains authoritative and can still prevent entries or square off managed positions. A legacy V6 arm file does not implicitly arm G. The live supervisor reads G open-position state from `v13_v10_g_live/live_kite/open_positions_<date>.json`.

## Readiness and monitoring

This command validates the pinned configuration and ledger and publishes the live CSV/status views without starting live workers, authenticating a broker client, submitting orders or changing the arm/kill files:

```powershell
$env:FNO_V6_STRATEGY_PROFILE = "V13_V10_G"
python fno_v13_v10_g_live_kite_session.py --readiness-only --session-date 2026-09-14
```

The command writes local readiness artifacts; it is not a trading run. It reports `workers_started=false` and `execution_enabled=false`. Existing order-state files, if present, are exported rather than erased. The broker BAT intentionally rejects caller-supplied arguments, so use the Python command directly for this readiness check.

The dashboard monitor covers all nine G signal/confirmation pairs. To keep refresh work bounded, its one-minute grid includes entry windows plus actual later entry/exit event minutes, including 15:15 exits when present; quiet intermediate minutes are omitted. The separate shared V10/V11/V12 five-minute windows remain 09:25–09:45. Per-minute books use timestamped realized events; no historical MTM is fabricated from the current ledger.

2026-09-14 is Ganesh Chaturthi in the repository's reviewed NSE calendar. Regular selection and execution are not expected today. The integration work placed no real orders. Readiness output is not evidence of a trading-day paper or broker fill.

## Session rename verification

Completed on 2026-09-14: all eleven dashboard session/view IDs, eight launchers, task mappings, status/heartbeat files, log names and current CSV exports use canonical `V13-V10-G` / `v13_v10_g` names. The eight Windows scheduled tasks were migrated to `EQIDV2_fno_v13_v10_g_<role>_0915`. Every replacement is enabled and reports its next run as **2026-09-15 09:15:00 IST**. The old V6 task registrations were removed after verifying their replacements, so they cannot run twice. Old entry-point aliases and historical files remain available for compatibility.

The one-time migration exported the original task XML, registered replacements disabled, retired the original schedules, verified replacement enablement/next runs, then removed the old registrations. Original task settings, principal, weekday triggers, wake settings and duplicate-instance policy were preserved. Its first trigger was explicitly set to the future 2026-09-15 09:15 boundary, preventing catch-up starts during migration. No trading workers were started.

Task backups and machine-readable verification are in `C:/TradingData/eqidv2/fno_oi/v13_v10_g_live/session_rename_20260914/`: eight original XML files, `renamed_tasks.json`, and `dashboard_verification.json`.

The dashboard was restarted through its existing supervisor. The running source SHA matches the edited file. Authenticated HTML/snapshot responses returned HTTP 200; all eleven canonical views exist, show strategy identity `MATCH`, and link to the renamed tasks with tomorrow's 09:15 run. No old V6 cards remain. The snapshot took approximately 0.21 seconds. The seven paper/feed cards have fresh holiday statuses; four broker views show `READY_DISARMED`.

Combined rename, strategy, execution, feed, dashboard, launcher and pre-open regression verification: **270 tests plus 16 subtests passed**. G's strategy fingerprint remains `a41f11737885e13c3cbe5c3310eed59d17bfab0da70784752fdee41e56c67f8f`; the existing order identities, quantity-one pilot, data/schema transport, acknowledgement and arm/kill controls were preserved.

Tomorrow, Tuesday 2026-09-15, is a trading day in the configured calendar. Required existing dependencies were verified enabled with valid action paths and these next runs:

| Time IST | Scheduled component |
|---|---|
| 08:30 | Authentication refresh |
| 08:50 | FnO universe |
| 08:55 | Dashboard/public-link start |
| 09:00 | Equity five-minute data |
| 09:05 | Futures/OI fast production feed |
| 09:15 | Feature ranker and all eight G tasks |

The tasks use the existing interactive Windows user session: keep the machine available and the user signed in. The scheduler check does not establish tomorrow's broker authentication or data availability. G's first signal is the completed 09:25 candle, followed by 09:26 confirmation. The live coordinator remains gated by the existing same-day arm requirements and fixed one-share limit; renaming did not arm live trading.

Launcher verification covers canonical G identities, fixed live quantity-one acknowledgement/supervisor guards, legacy argument/exit-code forwarding, exactly seven G paper/feed task definitions at 09:15, retirement of old task aliases, and PowerShell syntax parsing without executing installers.

## Initial strategy-migration validation record

The daily backtesting card was subsequently migrated to **Backtesting result v13-v10-G**, running only the requested day's retained G strategy. Its canonical task is `EQIDV2_backtesting_result_v13_v10_g_1620`, enabled for September 15 at 16:20 IST. The dashboard was reloaded again and all eleven paper/live identities remained MATCH. See [the daily backtest implementation and verification record](V13_V10_G_DAILY_BACKTEST.md).

The following record describes verification before the session-name migration:

- Combined runtime, adapter, capital, coordinator, feed, calendar and dashboard verification: 227 tests plus 16 subtests passed.
- Historical selection parity: all 73 selected orders across the frozen 31 sessions match exactly. This does not imply quote-based paper fills or broker fills reproduce historical one-minute execution prices.
- Fake-broker checks cover recovery without duplicate entry/bracket orders, target/stop exits, entry expiry, fill during cancellation, kill/square-off and worker supervision. End-to-end temporary-data checks cover scanner, durable volume evidence, immutable marker, read-only confirmation, signal publication and paper state.
- Shared paper capital checks include competing LONG/SHORT processes, ten admitted allocations, the eleventh rejected, and capital released after exit. Expired pending orders cancel before broker authentication or quote acquisition.
- The seven paper/feed roles published fresh `SKIPPED_NON_TRADING_DAY` reports for 2026-09-14. The live readiness command prepared all three CSVs and `READY_DISARMED` state without starting workers, contacting brokers, or changing the arm/kill files. The runner log records this readiness check.
- The dashboard alone was reloaded through its existing supervisor. Its running source SHA was checked against the edited file. Authenticated `/` and `/api/snapshot` returned HTTP 200 in approximately 0.04 and 0.22 seconds; all eleven requested views exist with G strategy identity `MATCH`. Log/report endpoints and CSV projections were checked.
- Visual browser verification could not complete: Edge returned `net::ERR_BLOCKED_BY_CLIENT` for the local dashboard, and the in-app browser is unavailable. No browser security or dashboard authentication settings were changed.

No real orders were placed during this migration. A fresh disarmed readiness result is shown separately from any earlier failed supervisor run, whose status and reason remain in the dashboard details. Trading-day paper fills and broker executions have not occurred as part of this holiday verification.
