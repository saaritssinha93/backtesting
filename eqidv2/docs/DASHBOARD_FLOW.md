# Dashboard Flow

Dashboard Flow is a separate visual workspace for retained **V13-V10-G, G-2 and G-3 backtesting**. It reads saved CSV evidence, offers full-history and daily replay selection, and presents performance and trade details. It does not place trades or modify the source results.

## Open it

Double-click `bat/run_dashboard_flow.bat`. This starts an isolated local session on port **8790** and opens the authenticated page in your default browser. Keep its console window open; press Ctrl+C to stop it. The launcher does not restart or replace the existing dashboard on port 8787.

From a terminal in the repository:

```powershell
python tools/run_dashboard_flow.py
```

If that port is occupied, use `python tools/run_dashboard_flow.py --port 8791`. Python must be available on PATH. The launcher binds only to `127.0.0.1`, uses the existing `LOG_DASH_*` authentication environment when configured, and otherwise generates a temporary session token in memory. It does not print the token or write a credential file. Its POST endpoints are disabled.

The main dashboard at **`http://127.0.0.1:8787/`** also serves **`/dashboard-flow`**. Its green **Dashboard Flow ↗** link is the first action in the top header (and spans the first action row on mobile), and opens Flow in a new tab while keeping the main dashboard open. Flow's labelled **Main dashboard** link returns to the main dashboard. Both pages use the same authentication. The JSON data endpoint is `GET /api/dashboard-flow`; an optional `run` parameter selects an ID returned in the response's `runs` list.

## Source data and metric definitions

`dashboard_flow.py` discovers the three strategy families separately beneath these directories under `C:/TradingData/eqidv2`:

- `fno_oi/strategy_research/v13_v10_g_full_history/run_*/g_backtest/`
- `backtesting_result_v13_v10_g/runs/<date>/<run>/daily_results.csv`
- `fno_oi/strategy_research/v13_corrected_v10_g_2/run_*/daily_results.csv`
- `backtesting_result_v13_v10_g_3/runs/<date>/<run>/daily_results.csv`

The selector lists strategy families in descending version order: **G-3, G-2, G**. The newest retained G-3 run is the default; if that family is unavailable, the newest run from the next available family is selected. Within G, full-history backtests take priority over individual daily replays. The latest runs cover July 29 through **October 9, 2026**:

| Strategy | Sessions | Executed trades | Net P&L (INR) | Latest run |
| --- | ---: | ---: | ---: | --- |
| G-3 | 48 | 98 | 247,764.67 | `20261010T_through_20261009_pullbacks` |
| G-2 | 49 | 96 | 243,064.67 | `run_20261010_staged125_to100_120m_through_20261009` |
| G | 49 | 96 | 207,998.10 | `run_20261010_through_20261009_dated_extension` |

The October 9 extension uses completed, sealed 213-stock evidence and independently reruns the retained frozen selection rules, which select zero orders. G preserves its archived October 8 ledger and adds this verified session. G-2 uses the completed causal dataset through October 8 plus the independently verified October 9 session. Their provenance explicitly distinguishes the dated extension from a regenerated full feature dataset. G-3 appends completed dated replays and applies its existing observer models without refitting. Its original October 1 source gap remains excluded, explaining the different session count.

Each strategy retains its frozen configuration, and historical trade/daily results were checked against the prior runs. Individual daily G replays remain selectable separately because their strategy revisions can differ; production replay totals are not used as a substitute for the retained G rules. Archived runs retain their original date ranges. The three families have separate version buttons and are never combined. The run picker initially shows the latest run; enable **Include archived runs & daily replays** to inspect older evidence. Friendly labels show coverage dates instead of folder names.

For full-history G runs, daily results are derived from executed portfolio trades and the saved explicit eligible-session calendar, retaining sessions with no trades. For daily replays, values come from saved daily results. Each flow line sums a stock's executed net P&L within the selected session window; it does not represent a funded account balance. Maximum drawdown is the largest decline in the run's total cumulative net P&L at day end, expressed as a positive rupee amount. It is not intraday drawdown or a percentage return.

The trade ledger reads `trades_with_pullbacks.csv`, or `portfolio_trades.csv` when present. Portfolio-rejected and unfilled orders are excluded, and portfolio P&L takes precedence over raw candidate P&L. Profit factor is positive **net trade P&L** divided by the magnitude of negative **net trade P&L**, only when the executed ledger reconciles to the daily totals. An undefined ratio, missing ledger, or non-finite source value displays as unavailable instead of a fabricated value. Run IDs are resolved against discovery; API callers cannot supply arbitrary filesystem paths.

## Explore the flow

The page follows the reference video's mint/rose connected-map structure: ranked entry groups, stock dispersion, cumulative stock paths, an outcome spine, profitable/losing card clusters, and a stock-detail overlay. The left groups use saved setup IDs or the entry slot/direction encoded in the trade ID. They are not sector classifications.

Use stock/setup search, long/short filters, and 20/40/all-session ranges to narrow the map. Session replay steps through historical sessions. **Filtered selection** is the default summary scope: summary metrics, ledger and CSV export all follow those same filters, including the replay endpoint. Choose **Entire run** to keep the summary and ledger on the full recorded run while exploring the map. Ledger search returns to Filtered selection and shares the map search. Filtered cumulative results and drawdown begin at zero for the selected recorded-session window. Missing or incomplete ledger metrics remain unavailable.

The **Compare** tab compares the latest G-3, G-2 and G runs. It defaults to the intersection of their actual recorded session dates, and also offers full-history comparison. Tables show net/gross P&L, costs, trades, win rate, day-end drawdown and monthly results, alongside cumulative curves. Missing dates are explicitly listed, rather than filled with fabricated zero results. For the current latest runs, the common calendar contains 48 sessions; full histories contain 48/49/49 sessions.

The compact layout retains the reference video's flow-map structure, with readable chart labels and responsive panels. The light/dark preference is shared with the main dashboard and synchronized across tabs. All rendering is local and dependency-free.

The main dashboard supports pausing its automatic refresh and preserves log reading position, open details, controls, focus and text selection during updates. Pinned and Problems First ordering are retained inside grouped sections. A failed request leaves the previous successful content visible with a warning. The consolidated status row distinguishes processes from views and labels preview row counts explicitly; freshness warnings only apply to currently eligible monitored outputs.

## Validation

```powershell
python -m pytest tests/test_dashboard_flow.py tests/test_dashboard_flow_routes.py tests/test_dashboard_ops_state.py -q
node --test tests/dashboard_flow_core.test.cjs tests/test_dashboard_flow_compare.cjs
```

The data tests cover totals, trade-based profit factor, daily drawdown, execution filtering, default run selection, unknown IDs, empty data, non-finite values, and incomplete-ledger reporting. Route tests cover authentication and the separate Flow response. The adapter uses the Python standard library and does not require a package installation.
