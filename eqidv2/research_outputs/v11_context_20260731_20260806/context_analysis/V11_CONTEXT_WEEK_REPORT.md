# V11 one-week Market/Sector Context study

Evaluation window: **2026-07-31 to 2026-08-06**.

The authoritative V11 replay candidate population, entry engine, exits, costs,
and P&L are frozen. Context is attached after resolution and never generates
or removes a trade in the baseline artifact.

## Baseline (LONG-only primary study)

- Trades: 9
- Net P&L: Rs -2,572.56
- Profit factor: 0.268
- Win rate: 11.11%
- Average P&L/trade: Rs -285.84
- Trade-sequence max drawdown: Rs -3,085.21
- Frozen all-side appendix: 26 trades, Rs -3,045.37 net, PF 0.440

## Context coverage

- V11 configured universe: 1280 tickers
- Universe files with data: 1268
- Static sector map: 117 tickers
- Mapped available tickers: 110
- Sector map coverage vs full V11 universe: 8.59%
- Bank Nifty source available: False
- Midcap source available: False
- Mean fresh market breadth coverage: 96.37% (minimum 95.78%)
- Nifty source ready at candidate timestamps: 96.71%
- Full requested context complete: 0.00% (Bank Nifty/Midcap absent and sector map incomplete)
- Numerical model candidates: 191

| Stage | Rows | Market context | Sector snapshot | Sector mapped |
|---|---:|---:|---:|---:|
| pre_dedupe_live_candidates | 3099 | 100.00% | 14.55% | 14.55% |
| selected_strategy_signals | 26 | 100.00% | 0.00% | 0.00% |
| resolved_trades_all_sides | 26 | 100.00% | 0.00% | 0.00% |
| resolved_trades_long_primary | 9 | 100.00% | 0.00% | 0.00% |

## LONG setup results (unchanged baseline trades)

| Setup | Side | Trades | Net Rs | PF | Win % | Avg Rs |
|---|---|---:|---:|---:|---:|---:|
| G_HIGHER_HIGH_BREAK | LONG | 6 | -1,324.36 | 0.416 | 16.67 | -220.73 |
| L_DOUBLE_BOTTOM_VWAP | LONG | 3 | -1,248.21 | 0.000 | 0.00 | -416.07 |

## Exploratory context findings

The following are diagnostics, not approved setup filters. The descriptive
feature table uses the same one-week outcomes, so it is in-sample. The shadow
table fixes direction/threshold on the first three sessions and applies it to
the final two, but the sample remains extremely small and many features are tried.

Strongest one-week descriptive associations:

| Feature | Valid n | Coverage | Spearman(P&L) | High-low avg Rs |
|---|---:|---:|---:|---:|
| mce_pct_above_ema20 | 9 | 100.00% | 0.750 | 470.26 |
| mce_fraction_above_ema20 | 9 | 100.00% | 0.750 | 470.26 |
| mce_fraction_above_vwap | 9 | 100.00% | 0.733 | 426.97 |
| mce_pct_above_vwap | 9 | 100.00% | 0.733 | 426.97 |
| mce_sector_positive_share | 9 | 100.00% | -0.723 | -664.65 |
| mce_market_breadth | 9 | 100.00% | 0.667 | 428.64 |
| mce_pct_above_ema50 | 9 | 100.00% | 0.633 | 428.64 |
| mce_fraction_above_ema50 | 9 | 100.00% | 0.633 | 428.64 |
| mce_sector_rank_turnover_mean | 9 | 100.00% | 0.567 | 373.09 |
| mce_cross_sectional_return_dispersion | 9 | 100.00% | -0.550 | -494.87 |

Chronological two-session shadow results (exploratory):

| Side | Feature | Direction | Holdout n | Kept | Baseline avg | Selected avg | Delta |
|---|---|---|---:|---:|---:|---:|---:|
| LONG | mce_fraction_above_ema20 | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_fraction_new_intraday_highs | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_pct_above_ema20 | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_combined_index_trend_score | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_nifty_trend_score | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_pct_new_intraday_highs | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_risk_on_off_score | HIGH | 3 | 1 | -513.20 | -340.52 | 172.68 |
| LONG | mce_sector_laggard_strength | HIGH | 3 | 2 | -513.20 | -448.49 | 64.71 |
| LONG | mce_decline_count | HIGH | 3 | 3 | -513.20 | -513.20 | 0.00 |
| LONG | mce_down_volume | HIGH | 3 | 3 | -513.20 | -513.20 | 0.00 |

## Decision

This run creates a context-enriched, model-ready V11 research artifact but
does **not** authorize a live gate or claim improved profitability. Promotion
requires a longer pre-period, purged walk-forward testing, multiple-testing
control, a complete point-in-time NSE sector master, and live shadow parity.
Bank Nifty and Midcap scores remain missing because those index bars are absent;
they are never imputed to zero or relabelled from an equity basket.

Shadow split: `{"sessions": ["2026-07-31", "2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"], "discovery_sessions": ["2026-07-31", "2026-08-03", "2026-08-04"], "holdout_sessions": ["2026-08-05", "2026-08-06"], "warning": "exploratory five-session shadow study; not promotion evidence"}`
