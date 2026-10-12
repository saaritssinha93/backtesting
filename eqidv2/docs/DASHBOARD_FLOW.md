# Dashboard Flow

Active G3 backtesting contract (12 October 2026): the default is the complete, hash-verified G-3 historical backtest referenced by `backtesting_result_v13_v10_g_3/latest/current_backtest.json`, if available. Otherwise Dashboard Flow retains the verified cumulative production history referenced by `current_history.json`. An invalid current backtest cannot silently become a successful result; the live G-3 contract and production pointers stay separate. Historical G-3, G-2, G remain selectable in decreasing version order. Excluded source sessions and newer failed attempts are shown explicitly. Refresh follows the newest verified generation; historical selections retain their saved identity. See the [implementation record](V13_V10_G3_MIGRATION_PLAN_2026-10-11.md#implementation-record--12-october-2026).

Dashboard Flow is a separate visual workspace for retained **V13-V10-G, G-2 and G-3 backtesting**. It reads saved CSV evidence, offers full-history and daily replay selection, and presents performance and trade details. It does not place trades or modify the source results.


## Live flow and five-minute session review

Open **Live flow** in the top navigation, or append `#live` to the authenticated Dashboard Flow URL. This read-only workspace uses the same authentication on ports 8787 and 8790. It reads local files only and has no order or strategy-control action.

- Select a retained session and any five-minute boundary from **09:15 to 15:30 IST**. The default slot is the latest snapshot with a reliable cash price; OI-only slots remain selectable. **Auto refresh** is on by default and reads saved data every 30 seconds while this view is active. **Follow latest** separately advances the selected slot. Reviewing an earlier slot or historical date keeps that selection fixed while automatic data refresh continues; Auto refresh can be switched off explicitly. Future slots and source gaps remain explicit.
- LONG and SHORT show up to **15 stocks each**. Price change and five-minute change sort descending for LONG and ascending for SHORT. Absolute OI sorts descending on both sides; positive OI-change ranking includes only OI increases and also sorts descending on both sides. The direction uses the selected price reference, not the sign of OI.
- The default price reference is a verified previous-session close. The exact-date broker daily close in `fno_oi/raw_equity_daily` takes priority over legacy daily data and a valid final intraday candle. Missing verified baselines exclude stocks from strict LONG/SHORT rankings. The **last real prior-session price proxy** is available only by explicit selection; the view never automatically switches to it. The prior session uses the shared NSE calendar, not the last date saved on disk, and exchange holidays are excluded from the session selector.
- **Open gap %** in both direction tables is `(session open / verified previous-session close - 1) * 100`. Positive is gap up, negative is gap down. It stays fixed throughout the session and remains unavailable if either input is missing, even in proxy comparison mode. The live open is reconstructed from the completed 09:20 candle. For a completed session (after 16:00 IST), a validated same-date daily open can recover a missing opening reference without creating five-minute candles. A later intraday candle never substitutes for the opening bar. Stock details show the close date, close source and open source. The established historical LTIM-to-LTM rename is handled explicitly.
- Five-minute price and OI changes use adjacent completed boundaries. OI belongs to the dated near-month futures contract, not cash shares; its percentage change compares the same contract. The 09:15 opening point is reconstructed from the first completed candle's open and is labelled accordingly. It is not a real-time opening tick.
- Select a stock to inspect its path through the selected time and source metrics. **Strategy decisions / misses** shows recorded G-3 base filters, published candidates, later confirmation outcomes and saved PAPER/LIVE or verified backtest evidence. Market rankings are descriptive, not recomputed entry decisions.
- A base-pass row absent from published candidates is labelled as such. It is **not proof of a missed executable trade**. In particular, the preserved October 12 09:25 and 09:30 packets contain 5 and 15 base-pass candidates respectively but zero publications; the timestamp incident is documented in `G3_ISSUES_FIXED_2026-10-12.md`.

The October 12 scanner and one-minute confirmation packets were recorded under G-3 production V1 before the V2 promotion. Live flow accepts only that pinned historical V1 identity or the current V2 identity, checks each feature row against its enclosing packet, and labels recorded V1 decisions separately from the hash-verified V2 retrospective backtest. At 09:50, the V1 scanner published five LONG candidates and its 09:51 confirmation selected none. The Flow Map's 09:50 LONG / 0.75% result is a historical simulation using rules now configured in V2; its fills are not PAPER or broker orders. The deployment remains in PAPER validation and LIVE order execution is disabled.

The authenticated endpoint is `GET /api/dashboard-flow/live?date=YYYY-MM-DD`; omitting the date selects today in IST. Concurrent requests for the same session share one load, with up to four responses cached for at most 20 seconds. Automatic refresh polls every 30 seconds independently of Follow latest. First reads on a busy server can take up to 90 seconds; failures preserve the last successful view, and an initial failure pauses automatic retries until Refresh. Dates are validated and callers cannot supply source paths. The adapter reads equity indicator/candle directories, `fno_oi/raw_contracts_5m`, dated near-month universes, `fno_oi/v13_v10_g_3_live` scanner/confirmation/order evidence, and verified G3 historical packages. Source paths and coverage are exposed in the view. Historical market reconstruction is kept separate from as-recorded strategy decisions; missing data never creates a zero return, signal, order or execution. Historical G, G-2 and G-3 performance tabs remain separate.

Implementation: `dashboard_flow_live.py` joins the market adapter (`dashboard_flow_live_data.py`) with recorded evidence (`dashboard_flow_live_evidence.py`); `dashboard_flow_live.js` and `.css` render the session workspace; `dashboard_flow_live_cache.py` coalesces simultaneous reads. Opening `#live` loads market data directly and defers the historical backtest request until a research tab is selected. The main server serves the same assets and authenticated API for both dashboard instances.

### Daily close recovery and maintenance — 12 October 2026

The audit found 1,041 stale legacy daily cash files (1,019 ended on 15 May), while Flow's retained sessions began on 27 May. On 9 October, all 213 current stocks' saved one-minute data stopped at the 15:15 candle end; 15:20, 15:25 and 15:30 live five-minute placeholders were gap-filled. A direct broker minute request reproduced the missing tail, while the broker's daily endpoint supplied the dated closing price. The old verifier could pass 360/375 minutes (96% coverage) without the close, and the collector treated the failed final-candle check as non-fatal. The exact reason the broker minute response lacks that tail is not established.

`dashboard_flow_daily_backfill.py` collects completed daily OHLC only, using saved NSE equity mappings and credentials. It writes validated source-labelled rows atomically into a separate reporting dataset; it does not rewrite trading indicators, historical fills, or execution evidence. Initial recovery covered 26 May–9 October for all 213 current stocks. Dry run is the default; `--fetch` makes read-only historical data calls. The default end date is the latest NSE session finalized before 16:00 IST. Existing validated sessions are reused, missing sessions remain explicit, requests are paced, retries are bounded, and audit JSON contains outcomes without credentials.

```powershell
python dashboard_flow_daily_backfill.py --latest-completed
python dashboard_flow_daily_backfill.py --fetch --latest-completed
```

The Windows task `EQIDV2_Dashboard_Flow_Daily_Prices` runs `bat/run_dashboard_flow_daily.ps1` at **09:10 and 16:05 local/IST**. It runs as the logged-in user, catches missed starts when available, and ignores overlapping launches. Valid broker credentials are required; failures are recorded in `logs/dashboard_flow_daily_YYYY-MM-DD.log` and daily audit JSON. Daily data updates invalidate Flow's file cache and appear on automatic refresh.

The full-session verifier now requires an actual valid closing row separately from overall bar coverage. The one-minute collector and its scheduler reject a synthetic or missing final candle, apply bounded EOD recovery, and return failure if the close is still missing. Partial live sessions retain their prior behavior. Recovering daily closes makes gap % and previous-close comparisons available; it does **not** repair missing five-minute price/OI observations. Historical Flow warns about those remaining gaps. G3's 15:15 square-off and its separate strategy replay coverage contract are unchanged.

Final reference audit: **95/95 selectable trading sessions** have a validated prior-session daily close for every stock in their saved/reconstructed universe; **94/94 completed historical sessions** also have daily opening references. Today has verified closes and observed completed opening candles for **213/213 stocks**. Historical DALBHARAT was recovered using its retained 21 August equity mapping. LTIM's stale cash files now fall back to the verified canonical LTM history as well as its daily references. The 14 September exchange holiday is excluded. Examples for 12 October: RELIANCE open 1,179.30 / prior daily close 1,170.30 = **+0.7690%**; 360ONE open 1,040 / prior daily close 1,048 = **−0.7634%**. No prior-real-price proxy supplies either gap.

The per-session evidence and audit method are saved in [reference coverage audit](../.codex_tmp/live_flow_20261012/reference_coverage_final_audit.md) and its adjacent `daily_reference_coverage_after_backfill.json`. The broker fetch audits are under `C:/TradingData/eqidv2/fno_oi/raw_equity_daily/audits/`.

## Strict intraday checks and recovery outcome (12 October 2026)

The cash verifier now requires the exact completed timestamp grid: 375 one-minute ends from 09:16 through 15:30, or 75 five-minute ends from 09:20 through 15:30. The optional 09:15 opening snapshot is not a completed candle. During today's session, only completed timestamps are due. A missing, duplicate, off-grid, non-finite, invalid-OHLCV or explicitly synthetic row prevents a complete result. Declared bad quality and proxy/fill sources are rejected. OI end-of-day QC likewise requires all 75 valid, unique bars of the correct dated contract and returns a nonzero exit status for incomplete data. An explicit diagnostic 80% threshold cannot turn a partial OI run into success.

Both regular one-minute generation and `fno_equity_1m_backfill.py` preserve quality flags and provenance on unrecovered rows. Only an exact validated broker replacement can clear an old synthetic flag. Repairs preserve the original backup, validate staged serialization before replacement, and report remaining missing minutes as partial. Historical OI audits use the dated universe and write dated reports instead of replacing today's runtime status or latest reports. Dashboard Flow applies the corresponding finite-price, quality, duplicate and exact-slot checks when reading data.

The read-only audit covers all **95 exchange sessions from 27 May through 12 October**, and 215 cash symbols across changing memberships. The 94 completed sessions contain **19,962 stock/session combinations**. All have complete cash minutes and reconstructed five-minute prices through **15:15** after the explicit LTIM/LTM alias is applied. Full-session cash coverage is complete for **9,895** combinations; the other **10,067** lack the final 15 minutes (151,005 minute bars, or 30,201 five-minute bars). This is not a certification of indicator warmup, futures OI or historical strategy execution.

At the audit's 13:04 IST snapshot, today's cash feed covered **213/213 stocks through 12:55**. OI covered 9,371 of 9,372 expected stock/slot observations: ALKEM's 09:20 bar was absent. Today's historical one-minute directory is an end-of-day output; its missing current-day rows do not mean the separate live five-minute feed has stopped.

OI still has historical gaps, including a broad 21 August afternoon gap and an 18 September missing slot. **56 of the 95 sessions lack a saved dated universe** and use a disclosed registry reconstruction; these early OI contract assignments cannot be certified as the actual near-month membership used on that date. The audit records that limitation instead of treating reconstructed membership as authoritative.

Eight bounded broker probes were run and their returned bars saved separately. None recovered the requested missing timestamps. RELIANCE on 9 October and 360ONE on 3 August still ended at 15:15 on both minute and five-minute endpoints. ALKEM's 12 October 09:20 OI bar and SAIL's 8 October opening OI bars were not returned. Requests for the expired August/September RELIANCE contracts raised broker `InputException`. This establishes current recovery limitations; it does not establish why the original broker response lacked those bars. No probe rows were promoted into strategy data, and no historical results were regenerated from unchanged inputs.

Detailed coverage and exact missing windows:

- `C:/TradingData/eqidv2/fno_oi/data_quality/intraday_20261012_final/report.md`
- `C:/TradingData/eqidv2/fno_oi/data_quality/intraday_20261012_final/symbol_session_coverage.csv`
- `C:/TradingData/eqidv2/fno_oi/data_quality/intraday_20261012_verified/recovery_probe/probe_results.json`
- `C:/TradingData/eqidv2/fno_oi/data_quality/intraday_20261012_verified/recovery_probe/cash5m_probe_results.json`

Reproduce a read-only audit in a **new** output directory:

```powershell
python tools/audit_dashboard_flow_intraday.py --start 2026-05-27 --end 2026-10-12 --output C:/TradingData/eqidv2/fno_oi/data_quality/new_audit
```

Existing scheduled tasks pick up these fixes on their next launches: OI QC at 15:40, cash end-of-day collection/verification at 15:45, and daily reference refresh at 16:05 (also 09:10). Remaining unavailable broker bars now keep completeness checks in a failed/partial state. No trading worker, order setting, or recorded strategy decision is changed by these reporting/data-quality repairs.

Validation after the final changes: **427 targeted Python tests and 29 dashboard JavaScript tests passed**. Both dashboard servers were restarted and authenticated API checks confirmed 213/213 prices, previous closes, opening gaps and OI at 09:30 for today's view on both ports; the 9 October historical view retained its three missing late cash slots. An additional broader pipeline run exposed two pre-existing test failures in `tests/test_fno_oi_pipeline.py`: assertions still require retired V6 task names although the unchanged HEAD configuration already selects G3. These two legacy test expectations were not changed as part of this data-quality repair. Independent metadata validation is saved alongside the final audit as `metadata_validation.json`; newly rejected metadata patterns were absent from the retained data, so the added checks do not change the reported coverage counts.

## Open it

Double-click `bat/run_dashboard_flow.bat`. This starts an isolated local session on port **8790** and opens the authenticated page in your default browser. Keep its console window open; press Ctrl+C to stop it. The launcher does not restart or replace the existing dashboard on port 8787.

From a terminal in the repository:

```powershell
python tools/run_dashboard_flow.py
```

If that port is occupied, use `python tools/run_dashboard_flow.py --port 8791`. Python must be available on PATH. The launcher binds only to `127.0.0.1`, uses the existing `LOG_DASH_*` authentication environment when configured, and otherwise generates a temporary session token in memory. It does not print the token or write a credential file. Its POST endpoints are disabled.

The main dashboard at **`http://127.0.0.1:8787/`** also serves **`/dashboard-flow`**. Its green **Dashboard Flow ↗** link is the first action in the top header (and spans the first action row on mobile), and opens Flow in a new tab while keeping the main dashboard open. Flow's labelled **Main dashboard** link returns to the main dashboard. Both pages use the same authentication. The JSON data endpoint is `GET /api/dashboard-flow`; an optional `run` parameter selects an ID returned in the response's `runs` list, while `strategy=V13-V10-G-3`, `V13-V10-G-2`, or `V13-V10-G` selects that family's latest eligible run.

## Source data and metric definitions

`dashboard_flow.py` discovers the three strategy families separately beneath these directories under `C:/TradingData/eqidv2`:

- `fno_oi/strategy_research/v13_v10_g_full_history/run_*/g_backtest/`
- `backtesting_result_v13_v10_g/runs/<date>/<run>/daily_results.csv`
- `fno_oi/strategy_research/v13_corrected_v10_g_2/run_*/daily_results.csv`
- `backtesting_result_v13_v10_g_3/runs/<date>/<run>/daily_results.csv`
- `backtesting_result_v13_v10_g_3/latest/current_history.json` and `current_run.json`, after verifying their production manifests and artifact hashes
- `backtesting_result_v13_v10_g_3/latest/current_backtest.json`, after verifying the regular historical G-3 backtest manifest, configuration, daily totals, and trade ledger
- `fno_oi/strategy_research/v13_v10_g_3_0950_long_backtesting/run_*/`, after verifying its backtest-only manifest and artifact hashes

The selector lists strategy families in descending version order: **G-3, G-2, G**. A complete, hash-verified regular G-3 backtest referenced by `current_backtest.json` is the default historical backtest; the verified production history remains separately selectable and becomes the default if the current backtest is missing or fails verification. An isolated research run cannot become the default. Within G, full-history backtests take priority over individual daily replays. **Follow latest run for this strategy** is on by default: selecting a family or refreshing reselects its newest eligible run, and the page checks for new results every minute while open. Selecting an older run pauses that automatic following until the checkbox is enabled again. The October 12 snapshot is:

| Strategy | Sessions | Executed trades | Net P&L (INR) | Latest run |
| --- | ---: | ---: | ---: | --- |
| G-3 current 09:50 LONG backtest, target 0.75% | 49 | 112 | 286,682.16 | Through October 12; October 1 excluded |
| G-3 production history, separately selectable | 50 | 100 | 256,764.67 | Through October 12 |
| G-2 | 50 | 97 | 247,664.67 | `run_20261012_staged125_to100_120m_through_20261012` |
| G | 50 | 97 | 212,598.10 | `run_20261012_through_20261012_dated_extension` |

The default `fno_v13_v10_g_3_backtest.py` run now uses the verified **09:50 LONG · Target 0.75%** profile and publishes `current_backtest.json` only after complete artifact verification. The 0.75% run covers 49 sessions through October 12, with 112 filled trades, 80 wins, and ₹286,682.16 modeled net P&L. Its 09:50 setup retains the 1.25% initial stop, tightened to 1.00% after 120 minutes; the other G-3 setups are unchanged. The saved backtest has no execution authority. Publishing that result did not itself alter the live contract or production-history pointer; the separate G-3 V2 promotion configured the same rule for PAPER, daily backtesting, and the LIVE rule book. October 1 remains excluded because its immutable source was incomplete. Earlier isolated 0.75% and 1.00% studies remain separately selectable. The current run's daily and trade ledgers are read only after their hashes and saved summary reconcile.

The G and G-2 archives now extend through October 12 using their frozen selections and exits. The October 12 sealed replay checked all 213 dated FnO stocks and included 212. ALKEM was excluded because its genuine 09:20 futures-OI bar was unavailable; it was not treated as a zero-signal stock. The retained G selector chose one DELHIVERY 09:25 LONG order, which reached its target at 09:30 for ₹4,600 modeled net P&L under both G and G-2 exits. All earlier archived trade rows remain unchanged. These dated extensions do not claim a regenerated full historical feature dataset. The separately verified G-3 production history reaches October 12 and includes October 1; its coverage report identifies three other excluded source sessions. The 09:50 LONG research variant uses a different 49-session source calendar that excludes October 1, so its totals should not be read as a direct 50-session production comparison.

The shared cash-session verifier still reports a 15:30 gap: all 213 stocks lack genuine one-minute candles after 15:15, and the final three five-minute bars are gap-filled. A direct broker refetch for a sample stock returned the same 15:15 cutoff. G's replay ends at 15:15, so its fresh strategy-specific gate verifies all genuine 09:16–15:15 cash minutes and then lets the sealed replay check futures OI, warmup, selection and execution paths. This does not certify a complete 15:30 cash session. The ALKEM futures-OI exclusion remains explicit in the dated proof and daily G report.

Each strategy retains its frozen configuration, and historical trade/daily results were checked against the prior runs. Individual daily G replays remain selectable separately because their strategy revisions can differ; production replay totals are not used as a substitute for the retained G rules. Archived runs retain their original date ranges. The three families have separate version buttons and are never combined. The run picker initially shows the latest run; enable **Include archived runs & daily replays** to inspect older evidence. Friendly labels show coverage dates instead of folder names.

For full-history G runs, daily results are derived from executed portfolio trades and the saved explicit eligible-session calendar, retaining sessions with no trades. For daily replays, values come from saved daily results. Each flow line sums a stock's executed net P&L within the selected session window; it does not represent a funded account balance. Maximum drawdown is the largest decline in the run's total cumulative net P&L at day end, expressed as a positive rupee amount. It is not intraday drawdown or a percentage return.

The trade ledger reads `trades_with_pullbacks.csv`, or `portfolio_trades.csv` when present. Portfolio-rejected and unfilled orders are excluded, and portfolio P&L takes precedence over raw candidate P&L. Profit factor is positive **net trade P&L** divided by the magnitude of negative **net trade P&L**, only when the executed ledger reconciles to the daily totals. An undefined ratio, missing ledger, or non-finite source value displays as unavailable instead of a fabricated value. Run IDs are resolved against discovery; API callers cannot supply arbitrary filesystem paths.

## Explore the flow

The page follows the reference video's mint/rose connected-map structure: ranked entry groups, stock dispersion, cumulative stock paths, an outcome spine, profitable/losing card clusters, and a stock-detail overlay. The left groups use saved setup IDs or the entry slot/direction encoded in the trade ID. They are not sector classifications.

Use stock/setup search, long/short filters, and 20/40/all-session ranges to narrow the map. Session replay steps through historical sessions. **Filtered selection** is the default summary scope: summary metrics, ledger and CSV export all follow those same filters, including the replay endpoint. Choose **Entire run** to keep the summary and ledger on the full recorded run while exploring the map. Ledger search returns to Filtered selection and shares the map search. Filtered cumulative results and drawdown begin at zero for the selected recorded-session window. Missing or incomplete ledger metrics remain unavailable.

The **Compare** tab compares the latest verified G-3, G-2 and G backtests. It defaults to the intersection of their actual recorded session dates, and also offers full-history comparison. Tables show net/gross P&L, costs, trades, win rate, day-end drawdown and monthly results, alongside cumulative curves. Missing dates are explicitly listed, rather than filled with fabricated zero results. The earlier isolated G-3 studies remain individually selectable for inspection.

The compact layout retains the reference video's flow-map structure, with readable chart labels and responsive panels. The light/dark preference is shared with the main dashboard and synchronized across tabs. All rendering is local and dependency-free.

The main dashboard supports pausing its automatic refresh and preserves log reading position, open details, controls, focus and text selection during updates. Pinned and Problems First ordering are retained inside grouped sections. A failed request leaves the previous successful content visible with a warning. The consolidated status row distinguishes processes from views and labels preview row counts explicitly; freshness warnings only apply to currently eligible monitored outputs.

## Validation

### Live flow reading workspace (October 12, 2026)

Open **Live flow**, then use **Analysis view** above the rankings. All six views share the selected session, five-minute cutoff, reference and symbol search:

| View | Purpose |
| --- | --- |
| Comparison desk | LONG / SHORT tables with larger aligned figures, sticky headers, frozen stock names and separate price / futures metric labels. Adapts to stock cards on phones. |
| Table + inspector | Full-width rankings, with a stock trace alongside on wide screens and a floating inspector on smaller screens. Selecting a stock does not scroll away from the table. |
| Session heatmap | Current top-15 stocks across saved slots through the selected cutoff. Signed values and shading show changes; missing cells are hatched. Selecting a cell changes the cutoff and opens that stock. |
| Rank movement | Current and previous ranks, entrants, departures and direction changes against the immediately preceding five-minute slot. Missing observations and contract changes are labelled unavailable. |
| Price / OI map | Five-minute price change versus five-minute OI change for all matching directional stocks with both observations, including OI decreases. Points and the stock list open the same inspector. Colour follows the selected price reference, not the five-minute price sign. |
| Stock cards | Touch-friendly metrics with BOTH / LONG / SHORT controls and stock inspection. |

**Row order: held** preserves the order of surviving top-15 members in tables and cards as data refreshes. Newly eligible members append; stocks leaving the top 15 disappear. Displayed rank numbers remain current. Changing the session, ranking metric, reference or search establishes a separate order. Toggle back to ranked to restore automatic sorting. Heatmap, movement and scatter use their own natural ordering.

**Compact rows** reduces spacing. Auto refresh continues on its existing 30-second schedule while active and visible, preserving the chosen view, stock, table scroll and expanded observations. Same-slot numeric corrections receive a brief outline; reduced-motion preferences disable that animation. Follow latest still independently controls whether the selected time advances. Close the inspector with its close button or Escape.

Rank changes are relative to the selected metric and search universe. A move from rank 18 to 12 is a six-place improvement and an entry into the top 15. A stock absent from the preceding snapshot has an unavailable baseline, not an invented entrant rank. Both OI rankings remain highest first; positive OI ranking still excludes nonpositive changes. Opening gap always uses the verified close, including in proxy mode. Historical heatmaps show the selected top-15 cohort's earlier observations, not a claim that those stocks were already top-ranked earlier. These views describe market observations; recorded strategy decisions remain separate below.

Implementation: `dashboard_flow_live.js` and `dashboard_flow_live.css`. This UI change does not alter collectors, broker calls, strategy rules, order sizing, saved observations or backtest results. Regression coverage includes ranking movement, contract changes, missing values, cutoff preservation, row holding, six-view rendering and existing refresh behavior in `tests/dashboard_flow_live.test.cjs`. Browser QA uses live API data and checks desktop / phone layouts, stock selection, refresh scroll retention and historical replay.

```powershell
python -m pytest tests/test_dashboard_flow.py tests/test_dashboard_flow_routes.py tests/test_dashboard_ops_state.py -q
python -m pytest tests/test_dashboard_flow_live.py tests/test_dashboard_flow_live_data.py tests/test_dashboard_flow_live_evidence.py tests/test_dashboard_flow_live_cache.py -q
node --test tests/dashboard_flow_core.test.cjs tests/test_dashboard_flow_compare.cjs tests/dashboard_flow_live.test.cjs tests/dashboard_flow_dom.test.cjs
```

The data tests cover totals, trade-based profit factor, daily drawdown, execution filtering, default run selection, unknown IDs, empty data, non-finite values, and incomplete-ledger reporting. Route tests cover authentication and the separate Flow response. The historical CSV adapter uses the Python standard library. Live flow uses the project's existing pandas and PyArrow installation.

## G-3 opening observations and recovery

Opening observations are shared reporting evidence for V13-V10-G-3 backtest analysis, LIVE and PAPER analysis. They are displayed alongside the recorded decisions in Live flow. They do not become entry or exit signals and do not rewrite recorded trades or backtest P&L. The existing 09:15-15:30 candle collection and indicators retain their inputs.

| Dashboard session | Schedule, IST | Purpose |
| --- | --- | --- |
| G3 LIVE / PAPER opening-data observer | Weekdays 08:55-09:21 | One shared worker polls the selected app's full quotes every five seconds. It waits for the existing authentication task and exact dated universe, and rejects a start at or after 09:15. |
| G3 BACKTEST opening-data recovery | Weekdays 17:00 | Recover the latest completed session's 09:15-09:20 minute intervals into isolated reporting storage, after the production/EOD window. |

The dedicated task installer checks the Windows timezone is IST. Both tasks run hidden under the logged-in Windows account, skip overlapping runs and do not catch up a missed start automatically. Keep the computer awake and the account signed in. The existing LIVE order task's enable/disable state is not changed. Market holidays are checked in the workers.

All new writes are under `C:\TradingData\eqidv2\dashboard_flow_observer\`: `raw` for timestamped quotes, `historical` for recovered minute evidence, `universe` for the observer's dated membership copy, `status`, `coverage`, `logs`, and `state` for worker state and task backups. `config.json` requires `opening_enabled: true` for dashboard consumption; missing or malformed configuration disables the overlay. Dashboard reads never trigger broker requests. Broker credentials are read from the existing selected app files without modifying or logging them.

| Field | Meaning and availability |
| --- | --- |
| Price 5m % at 09:15 | Unavailable: no preceding regular-session five-minute interval exists. Separate pre-open price change is shown only with valid observations around both 09:10 and 09:15. |
| Opening OI | Same-contract quote observed and received by 09:15, with exchange timestamp no more than 60 seconds old. Its actual timestamp and age are shown. A late observation cannot be backdated. |
| OI day % | Current OI versus the verified previous session's closing OI of the same contract. A missing closing baseline remains unavailable. |
| Displayed OI Δ 5m % at 09:20 | Uses the same-contract validated 09:16 first-minute OI when available, or a fresh live quote near 09:16 with a **provisional quote** label. Its actual interval is about four minutes, so the source and interval accompany the value. With neither baseline it remains missing. An exact 09:15 snapshot is kept as a separate opening comparison. |
| First-minute historical OI | Separate retrospective context from the minute beginning 09:15, available after 09:16 and first visible on the five-minute timeline at 09:20. Once the 09:20 slot is available, the 09:15 stock row and inspector also link to the recovered value with a retrospective label for historical review. It never fills exact 09:15 OI. |
| OI since 09:16 % at 09:20 | The same first-bar OI comparison displayed in **OI Δ 5m %** at 09:20. A validated first-minute candle gives an exact four-minute interval; a timely quote uses its true exchange time and is labelled provisional. The archived candle's acquisition time determines whether its value was actually available by 09:20; later recovery remains retrospective research evidence. |

The tables and stock cards include OI day %. Stock inspection shows opening timestamps, pre-open comparisons and the separate historical first-minute value. The API also exposes an independent `opening_first_minutes` list for validated dated contracts even when their regular 09:20 row is missing. Its OHLC values are futures prices, not the dashboard's equity cash price. The opening status strip reports coverage and missing evidence. Missing values remain missing; collection failures, synthetic data and contract changes do not create zeroes or proxy baselines.

Stock inspection inserts a distinct **09:16 recovered futures minute** beneath 09:15 when a validated first-minute archive exists for that stock and date. Its OI and, when the verified same-contract prior-session baseline exists, OI day % are shown. Cash Price/PDC and regular five-minute changes stay blank for that minute; its futures close is labelled separately below the table. At 09:20, **OI since 09:16 %** and displayed **OI Δ 5m %** show the same validated comparison against the completed near-month futures five-minute OI. A recovered 09:16 minute produces a four-minute interval; a live quote near 09:16 uses its actual quote-to-09:20 interval and carries a visible **provisional quote** label. The API records whether minute evidence was acquired by 09:20; later recovery is retrospective research evidence. A future live setup must also verify when the completed 09:20 futures bar arrived before using the feature. **OI day %** still uses verified previous-session, same-contract OI. The active G-3 setup rules still begin at 09:25. The live quote collector's quality rules and all-universe coverage artifact are documented in [Dashboard Flow live 09:16 quote](DASHBOARD_FLOW_LIVE_0916_QUOTE.md).

Implementation files: `dashboard_flow_observer_store.py`, `dashboard_flow_observer.py`, `dashboard_flow_observer_history.py`, `dashboard_flow_opening_data.py`, `fno_v13_v10_g_3_opening_oi.py` (pure comparison helper, reusable for future G-3 setup research), `dashboard_flow_observer_sessions.py`, the existing `dashboard_flow_live_data.py` / `dashboard_flow_live.py` read adapters, `dashboard_flow_live.js` / `.css`, and the main dashboard's session registry in `log_dashboard_server.py`. Dedicated launchers and installer are in `bat/run_dashboard_flow_observer*.ps1` and `bat/schedule_dashboard_flow_observer.ps1`. The local audit is `tools/audit_dashboard_flow_observer.py`.

```powershell
# Default commands show a plan and make no broker requests.
python dashboard_flow_observer.py
python dashboard_flow_observer_history.py --from-date 2026-10-09 --to-date 2026-10-12
powershell -NoProfile -File bat/schedule_dashboard_flow_observer.ps1 -Plan

# Explicit historical recovery: writes only isolated observer evidence.
# Broker recovery is blocked during 08:30-17:00 on trading days.
python dashboard_flow_observer_history.py --from-date 2026-10-09 --to-date 2026-10-12 --import-local --fetch
python tools/audit_dashboard_flow_observer.py --date 2026-10-12 --save

# Install/remove only the two dedicated observer tasks.
powershell -NoProfile -File bat/schedule_dashboard_flow_observer.ps1 -Apply
powershell -NoProfile -File bat/schedule_dashboard_flow_observer.ps1 -Remove
```

Rollback: set `opening_enabled` to `false`, then remove the two dedicated tasks if desired. Existing observer evidence can remain for audit; no production history restore is required. A scheduled worker already running must finish before task replacement/removal. The observer will first collect a real opening after deployment at the next eligible market session; automated tests and historical recovery do not establish live-opening coverage in advance.

Broker semantics were checked against the official [full quote documentation](https://kite.trade/docs/connect/v3/market-quotes/) and [historical candle documentation](https://kite.trade/docs/connect/v3/historical/). Recovery availability depends on retained contracts and the broker's returned observations; exact historical opening snapshots cannot be manufactured from minute candles.

Deployment verified on 12 October 2026 after market hours: both tasks are Ready, with next runs on 13 October at 08:55 and 17:00 IST. Dashboard reporting consumption is enabled. Both dashboard ports (8787 and 8790) were restarted and authenticated API checks confirmed the two reporting sessions and the same recovered coverage. The first real opening observation remains pending the next eligible session.

Initial broker recovery covers **12 October only**: **211/213** contracts have the first-minute record; **205/213** have all six requested minute records. A second request for the eight incomplete contracts returned the same gaps. ALKEM and GODFRYPHLP lack the first-minute record; BAJAJHLDNG, BANDHANBNK, BOSCHLTD, LICHSGFIN, PRESTIGE and UNITDSPR have other missing minutes. These records are not forward-filled. Earlier sessions can be processed with the date-range recovery command; this deployment does not claim the entire historical archive was backfilled. Expired contracts require a usable local minute archive or a currently verified contract/token pair before broker recovery is attempted.

Both APIs show zero historical first-minute values at 09:15 and 211 at 09:20. Five-second polling will normally supply a recent as-of opening observation, not an exact timestamp at the boundary; the separately labelled opening OI comparison is therefore expected to be more widely available than regular 09:20 OI-five-minute change.

Validation: **182 Python tests passed, one Windows symlink-privilege test skipped, and 42 JavaScript tests passed**. Browser checks passed across six views, desktop and 320/390/736/1024-pixel layouts, historical replay, refresh retention and keyboard dismissal, with no browser errors. All 100 pre-existing task definitions and 18 protected code/config/data hashes matched their pre-deployment snapshots. The LIVE Kite order task remained disabled. API evidence is saved in `outputs/opening-deployment-qa.json`; coverage details are under the isolated observer's `coverage` directory.

### Recovery across older sessions

For a large date range, use the separate range runner. It reads each contract archive once and groups missing dates into broker requests spanning at most 30 calendar days. It uses the same isolated output format and worker status, so recovered records appear through Dashboard Flow's normal refresh.

```powershell
# Plan only: no writes or broker calls.
python tools/backfill_dashboard_flow_openings.py --from-date 2026-07-28 --to-date 2026-10-12

# Import genuine saved minute evidence, then request recoverable gaps.
python tools/backfill_dashboard_flow_openings.py --from-date 2026-07-28 --to-date 2026-10-12 --import-local --fetch
```

The read-only `dashboard_flow_observer_archives.py` adapter includes known futures-minute archives and retained V5/V6 research caches. It checks contract and expiry identities, timestamp aliases and minute boundaries; conflicting and synthetic observations are excluded. Those research folders supply market observations only, not strategy decisions. Existing valid observer minutes are preserved, including when a newer source disagrees. Disagreements with already saved minutes are recorded; conflicting local observations remain missing.

When a dated universe is missing, the observer's 2026 fallback requires the expected near-month expiry using the reviewed trading calendar and [NSE stock-futures expiry convention](https://www.nseindia.com/static/products-services/equity-derivatives-individual-securities). A surviving later-month contract cannot replace a missing near-month contract. Reconstructed membership remains labelled as reconstructed. This guard excludes July 28 when no July contract identity exists, and prevents later-month listings being mixed into earlier membership. Captured dated universes and production strategy inputs remain unchanged.

The range coverage file is `C:\TradingData\eqidv2\dashboard_flow_observer\coverage\history_2026-07-28_2026-10-12.json`. It records every requested calendar date, closed-market dates, unavailable identities, per-session coverage, source paths and missing minutes. Re-running the command can recover newly available evidence without replacing already valid saved minutes.

The requested July 28-October 12 recovery finished on October 12 at 18:51 IST. All **10,904 identified stock-session combinations** were processed across 52 dates, with July 28 recorded separately as an unavailable 53rd trading date. The run added **22,769 minute records** using local archives and **213 bounded broker requests**. There are now **24,030 validated opening-minute records**, **4,007 stock-sessions with the first-minute OI record**, and **3,964 with all six requested minutes**. These are counts of stock/date combinations, not distinct stocks. The overall status is **PARTIAL**.

| Requested period | Identified stock-sessions | First-minute record available | All six minutes available |
| --- | ---: | ---: | ---: |
| July 29-31 | 624 | 621 | 615 |
| August | 4,376 | 1,671 | 1,660 |
| September | 4,413 | 233 | 229 |
| October 1-12 | 1,491 | 1,482 | 1,460 |

July 28 lacks the required July futures identity. Many August 12-September 29 observations are missing from local archives; their expired contract tokens are no longer verifiable against the current broker instrument master. They were not queried under another contract's identity. Sparse retained research caches supplied some observations within that interval. Separately, 38 recent stock-sessions still lack one or more opening minutes after a successful broker response. Missing observations were not manufactured, and October 12's previously valid records were preserved.

Final validation re-read all saved outputs and found no invalid output records. **1,874 original parquet files retained their size and modification timestamps**, and **477 archive file hashes were unchanged**. The range/adapter/observer regression run passed **88 tests**. The compact verification summary is `coverage/verified_range_2026-07-28_2026-10-12.json`; authenticated dashboard sample checks are in `coverage/http_range_2026-07-28_2026-10-12.json`, both beneath the isolated observer root. Production candles, indicators, strategy rules, schedules and historical trade results were not regenerated by this recovery.
