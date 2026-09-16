# G options projections in the local report

Run these commands from the repository root. The active backtest and paper-trading configuration is a 30% premium stop and 40.4% premium target. Archived projection inputs below retain the verified historical three-lot, 12.5% / 25% ledger so past results remain reproducible.

```powershell
python -B fno_v13_v10_g_options_projection.py
python -B fno_v13_v10_g_options_html.py --html-file C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/V13_V10_G_INTERACTIVE_BACKTEST.html --payload C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/options_3lots_5min_20260914_sl12p5_target25/one_year_scenarios/projection_payload.json
```

Open the existing report and choose **Options results** or **Options projections**. Direct anchors are `#options-history` and `#options-projections`. The stock `#projections` section also links to the options model.

The injector owns only `V13_V10_G_OPTIONS` marker blocks. Re-running it replaces those additions without duplicating them, and checks that all original stock bytes remain unchanged. An original stock HTML backup and SHA-256 integration manifest are saved beside the projection output. After rebuilding the stock HTML with its own builder, run the options injector again.

`options.html`, `options.css` and `options.js` define the added sections. Charts, payload, CSV exports and SVG exports are embedded, with no external script or chart-library dependency. Controls cover position sizing, payoff retention, equity/profit/return measures and conditional percentile bands. The monthly lot chart and sizing comparison show how the policies differ. Stock controls and their data remain independent.

All three policies start at three lots and share the same sampled days. Fixed sizing keeps three lots throughout. Equity-based monthly sizing uses a fractional budget equal to the smaller of `3 * path month-opening equity / projection-opening equity` and `previous fractional budget * 1.10`; executable lots are rounded down. Carrying the fractional budget allows eventual growth from three to four lots. The 10% cap applies to that budget; a discrete whole-lot increase can exceed 10%. Equity declines can reduce size immediately.

The requested profit-based monthly increase starts at three lots in M1. At each later month opening, a profitable preceding model month permits an increase of `1 + floor(max(0, cumulative future net profit) / 500000)` lots. Thus the increment starts at one, becomes two at Rs5 lakh cumulative future profit, three at Rs10 lakh, and continues in Rs5 lakh steps. A flat or losing preceding month holds the prior lot count. Historical profit is excluded from the milestone calculation. The Rs5 lakh step is an explicit illustrative assumption, not an optimized threshold. This policy does not reduce size after losses; cash admission still applies to every trade.

The scenario engine uses each path's realized month-opening equity, never a future value or an average across paths, and holds its chosen lot count for 21 sessions. Every order must pay its full premium and entry fees from available cash. Unaffordable orders are skipped entirely. Fees are recomputed on actual whole-lot quantities, retaining fixed per-order brokerage. It uses 13 source sessions including verified no-entry days, and excludes earlier unavailable August-expiry history. It retains original five-minute fill prices and observed exit times. Payoff stress changes saved positive gross P&L and recomputes sale fees; it is not a new option-price-path backtest. The three stressed cases add 10 bps on buy and sell premium as separate costs. All projected months contain 21 assumed sessions and have no forecast calendar dates.

These projections reuse a small, previously inspected sample of 20 trades. Conditional P10–P90 ranges and simulated loss frequencies do not measure future market probabilities. Historical contract lot sizes and premium prices are reused as templates; larger orders have not been validated against historical volume or bid/ask depth, and their additional market impact is not modeled. Daily realized drawdown excludes intraday mark-to-market losses.

Focused model checks:

```powershell
python -B -m pytest tests/test_fno_v13_v10_g_options_projection.py -q
python -B .codex_tmp/options_projection/audit_projection_model.py
python -B .codex_tmp/options_projection/audit_monthly_resizing.py
python -B .codex_tmp/options_projection/check_browser.py
```

The independent artifact audits recompute fees, cash funding and sizing chronology, plus annual/daily/monthly values. The browser check requires Selenium and Chrome and exercises all sizing/scenario/measure combinations, bands, stock controls, downloads, themes and mobile layouts. Audit outputs are stored with the model and in `.codex_tmp/options_projection`. The previous fixed-only delivery is retained under `one_year_scenarios/fixed_only_snapshot_before_monthly` for comparison.
