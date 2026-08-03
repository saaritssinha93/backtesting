# V7 Paper-Live vs V11 Backtest Parity Report

Dates: 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02, 2026-07-03, 2026-07-06, 2026-07-07

## Executive Summary

Verdict: FAIL

- Live source: `C:\TradingData\eqidv2\live_signals` paper trade/signal CSVs
- V11 source: `C:\TradingData\eqidv2\backtesting_result_v11\YYYY-MM-DD` daily live-parity outputs
- Live paper rows: 41 total, 39 filled trades, 2 nonfilled/skipped/rejected rows
- Trade match rate: 15.38% of live trades, 8.00% of V11 trades
- Signal match rate: 17.07% of live signals, 9.33% of V11 signals
- Live total net P&L: Rs -3,737.65
- V11 modeled total net P&L: Rs -12,927.95
- Total net P&L divergence: -245.88%
- Daily P&L correlation: 0.660

V11 was run in `live_parity` mode with `selected_strategy_profile=final_setup_conf`, so it replayed live JSON candidate/gate snapshots rather than recomputing gate state from future data. V11 raw P&L is price-only in this path; this report uses recomputed statutory costs and V7-style exit slippage for V11 modeled P&L.

## Inputs Used

| date | live_filled_rows | live_nonfilled_rows | live_trades | live_signals_long | live_signals_short | v11_dir |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | 29 | 2 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-06-29_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-06-29_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29 |
| 2026-06-30 | 7 | 0 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-30_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-06-30_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-06-30_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-30 |
| 2026-07-01 | 1 | 0 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-07-01_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-01_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-01_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-07-01 |
| 2026-07-02 | 0 | 0 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-07-02_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-02_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-02_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-07-02 |
| 2026-07-03 | 0 | 0 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-07-03_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-03_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-03_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-07-03 |
| 2026-07-06 | 0 | 0 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-07-06_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-06_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-06_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-07-06 |
| 2026-07-07 | 2 | 0 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-07-07_id_5min_v7.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-07_id_5min_v7_long.csv | C:\TradingData\eqidv2\live_signals\signals_2026-07-07_id_5min_v7_short.csv | C:\TradingData\eqidv2\backtesting_result_v11\2026-07-07 |

## Config And Input Alignment

- Dates: matched to the last seven NSE sessions discovered from V7 logs.
- Universe: both V7 scanner and V11 live-parity path route through `candidate_scan.v2._load_universe()` for the main V7 universe; Tier123 add-on may use its own futures fallback in V11 internals.
- 5-minute bar source: V7 live feed root is `stocks_indicators_5min_eq_live`; V11 backtest root is `stocks_indicators_5min_eq_live2`, while live-parity signals come from archived live JSON snapshots.
- Session: market window 09:15-15:30 IST; entry window 09:30-14:30 IST; unresolved exits close at 15:20 IST.
- Gate state: V11 `inputs.txt` for every date shows `mode=live_parity`, so gate/qualification state came from live-day JSON snapshots.
- V7 scanner config wrapper: `bat\run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`.
- V7 paper executor config wrapper: `bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat`.
- V11 entry point: `avwap_5min_ID_v11_backtesting.py`; daily wrapper used here: `backtesting_result_v11_daily.py`.
- V11 run command used per day: `python -u backtesting_result_v11_daily.py --date YYYY-MM-DD --selected-strategy-profile final_setup_conf`.
- Cost model: `nse_intraday_costs.py`; universe route: `candidate_scan.v2._load_universe()` from `filtered_stocks_MIS.py`.
- Scanner config drift: all trading gate/filter env vars match; only live scheduling/feed wait worker knobs are absent in V11 (`SCAN_WORKERS`, `TIER123_SCAN_WORKERS`, `PARALLEL_SCAN_BRANCHES`, feed-gate delay/poll/failure budget, `POST_SLOT_DELAY_SEC`, `TIER123_LATEST_START_LAG_SEC`).
- Execution config drift: V7 paper uses actual LTP polling, entry retry/slip gates, daily loss brake, capacity gates, C_OR time stop/session cap, 5-second LTP exits, statutory costs; V11 uses 1-minute OHLC resolution, no portfolio state, raw zero-cost P&L.

## Config Diff Table

| field | V7 paper-live | V11 backtest | impact |
| --- | --- | --- | --- |
| Trading gate/filter env vars | matched wrapper values | matched wrapper values | No drift found |
| SCAN_WORKERS / TIER123_SCAN_WORKERS | 24 / 24 | not set in scheduled wrapper | Scheduling only |
| PARALLEL_SCAN_BRANCHES | 1 | not set | Scheduling only |
| TIER123_LATEST_START_LAG_SEC | 40 | not set | Feed timing |
| FEED_GATE_MAX_VERIFICATION_FAILURES | 5 | not set | Feed timing |
| FEED_GATE_MIN_DELAY_SEC / POLL_SEC | 1 / 0.5 | not set | Feed timing |
| POST_SLOT_DELAY_SEC | 75 | not set | Feed timing |
| Entry fill source | actual ltp_on_signal | ltp_on_signal_1m_open approximation | High |
| Entry slippage | 5 bps adverse | 5 bps adverse | Matched |
| Exit slippage | 5 bps adverse except target | raw live-parity output has none; reconciler models it | High |
| Statutory costs | nse_intraday_costs.py columns | raw v6_cost_rs=0; reconciler recomputes | High |
| Entry slip retry gate | 0.3% max slip, wait 300s, poll 2s | not modeled | High |
| Daily loss brake | Rs 10,000 | not modeled | Medium |
| Capacity gates | max concurrent/open 100/100, capital 2,000,000 | no live capacity state | Medium |
| C_OR setup execution controls | 30m time stop/session cap 50 | no setup-specific live time stop in resolver | High |
| Exit resolver | 5-second LTP polling | 1-minute OHLC | High |
| Gate state source | live-day gate state | live JSON snapshots in live_parity | Matched |

## Daily Aggregate

| date | live_signals | v11_signals | live_trades | v11_trades | live_gross_pnl | v11_gross_pnl_model | live_costs | v11_costs_model | live_net_pnl | v11_net_pnl_model | net_pnl_diff |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | 31 | 20 | 29 | 20 | -3067.99 | -4100.60 | 1281.86 | 1647.53 | -4349.85 | -5748.13 | -1398.28 |
| 2026-06-30 | 7 | 12 | 7 | 12 | -255.43 | -3598.70 | 192.97 | 987.51 | -448.40 | -4586.21 | -4137.81 |
| 2026-07-01 | 1 | 4 | 1 | 4 | 1375.00 | 2292.79 | 54.05 | 330.12 | 1320.95 | 1962.67 | 641.72 |
| 2026-07-02 | 0 | 10 | 0 | 10 | 0.00 | 587.29 | 0.00 | 825.94 | 0.00 | -238.65 | -238.65 |
| 2026-07-03 | 0 | 9 | 0 | 9 | 0.00 | 3511.09 | 0.00 | 742.94 | 0.00 | 2768.15 | 2768.15 |
| 2026-07-06 | 0 | 5 | 0 | 5 | 0.00 | -1926.63 | 0.00 | 411.96 | 0.00 | -2338.59 | -2338.59 |
| 2026-07-07 | 2 | 15 | 2 | 15 | -205.85 | -3510.14 | 54.50 | 1237.04 | -260.35 | -4747.18 | -4486.83 |

## Per-Day Per-Setup Aggregate

| side | date | setup | signals | trades | win_rate_pct | avg_win | avg_loss | gross_pnl | costs | net_pnl |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| LIVE | 2026-06-29 | A_MOD_BREAK_C1_LOW | 3 | 3 | 33.33 | 219.93 | -183.77 | -72.88 | 74.73 | -147.61 |
| LIVE | 2026-06-29 | C_OR_BREAKDOWN | 12 | 11 | 45.45 | 196.68 | -212.45 | 26.77 | 318.06 | -291.29 |
| LIVE | 2026-06-29 | E_VWAP_LOSE_EARLY_SHORT | 3 | 3 | 33.33 | 315.14 | -174.19 | 77.56 | 110.80 | -33.24 |
| LIVE | 2026-06-29 | G_LOWER_LOW_BREAK | 1 | 1 | 0.00 | 0.00 | -175.98 | -149.82 | 26.16 | -175.98 |
| LIVE | 2026-06-29 | L_DOUBLE_BOTTOM_VWAP | 3 | 3 | 0.00 | 0.00 | -415.88 | -1074.75 | 172.90 | -1247.65 |
| LIVE | 2026-06-29 | L_PRESSURE_BURST_VWAP | 9 | 8 | 12.50 | 784.27 | -462.62 | -1874.87 | 579.21 | -2454.08 |
| LIVE | 2026-06-30 | A_MOD_BREAK_C1_LOW | 3 | 3 | 0.00 | 0.00 | -236.99 | -631.83 | 79.14 | -710.97 |
| LIVE | 2026-06-30 | B_HUGE_RED_FAILED_BOUNCE | 2 | 2 | 0.00 | 0.00 | -145.73 | -232.66 | 58.80 | -291.46 |
| LIVE | 2026-06-30 | C_OR_BREAKDOWN | 1 | 1 | 100.00 | 86.13 | 0.00 | 115.14 | 29.01 | 86.13 |
| LIVE | 2026-06-30 | D_EMA20_REJECTION | 1 | 1 | 100.00 | 467.90 | 0.00 | 493.92 | 26.02 | 467.90 |
| LIVE | 2026-07-01 | E_ORB_BREAKOUT_LONG | 1 | 1 | 100.00 | 1320.95 | 0.00 | 1375.00 | 54.05 | 1320.95 |
| LIVE | 2026-07-07 | A_MOD_BREAK_C1_LOW | 1 | 1 | 0.00 | 0.00 | -295.44 | -270.35 | 25.09 | -295.44 |
| LIVE | 2026-07-07 | C_OR_BREAKDOWN | 1 | 1 | 100.00 | 35.09 | 0.00 | 64.50 | 29.41 | 35.09 |
| V11_MODELED | 2026-06-29 | A_MOD_BREAK_C1_LOW | 3 | 3 | 0.00 | 0.00 | -543.49 | -1384.40 | 246.06 | -1630.46 |
| V11_MODELED | 2026-06-29 | B_HUGE_RED_FAILED_BOUNCE | 1 | 1 | 0.00 | 0.00 | -1018.54 | -936.32 | 82.22 | -1018.54 |
| V11_MODELED | 2026-06-29 | C_OR_BREAKDOWN | 13 | 13 | 38.46 | 304.10 | -672.75 | -2790.68 | 1070.76 | -3861.44 |
| V11_MODELED | 2026-06-29 | G_LOWER_LOW_BREAK | 1 | 1 | 0.00 | 0.00 | -932.21 | -849.56 | 82.65 | -932.21 |
| V11_MODELED | 2026-06-29 | L_DOUBLE_BOTTOM_VWAP | 2 | 2 | 50.00 | 1914.73 | -220.21 | 1860.36 | 165.84 | 1694.52 |
| V11_MODELED | 2026-06-30 | A_MOD_BREAK_C1_LOW | 3 | 3 | 33.33 | 901.23 | -1226.48 | -1304.38 | 247.35 | -1551.73 |
| V11_MODELED | 2026-06-30 | B_HUGE_RED_FAILED_BOUNCE | 1 | 1 | 0.00 | 0.00 | -1028.93 | -946.27 | 82.66 | -1028.93 |
| V11_MODELED | 2026-06-30 | G_HIGHER_HIGH_BREAK | 7 | 7 | 28.57 | 1202.75 | -699.32 | -516.05 | 575.05 | -1091.10 |
| V11_MODELED | 2026-06-30 | L_DOUBLE_BOTTOM_VWAP | 1 | 1 | 0.00 | 0.00 | -914.44 | -832.00 | 82.44 | -914.44 |
| V11_MODELED | 2026-07-01 | E_ORB_BREAKOUT_LONG | 1 | 1 | 100.00 | 2666.51 | 0.00 | 2750.00 | 83.49 | 2666.51 |
| V11_MODELED | 2026-07-01 | G_HIGHER_HIGH_BREAK | 3 | 3 | 33.33 | 1911.12 | -1307.48 | -457.21 | 246.63 | -703.84 |
| V11_MODELED | 2026-07-02 | A_MOD_BREAK_C1_LOW | 4 | 4 | 25.00 | 43.46 | -837.49 | -2139.38 | 329.63 | -2469.01 |
| V11_MODELED | 2026-07-02 | B_HUGE_RED_FAILED_BOUNCE | 1 | 1 | 100.00 | 211.75 | 0.00 | 294.30 | 82.55 | 211.75 |
| V11_MODELED | 2026-07-02 | G_HIGHER_HIGH_BREAK | 3 | 3 | 100.00 | 858.43 | 0.00 | 2823.86 | 248.57 | 2575.29 |
| V11_MODELED | 2026-07-02 | L_DOUBLE_BOTTOM_VWAP | 2 | 2 | 0.00 | 0.00 | -278.35 | -391.49 | 165.21 | -556.70 |
| V11_MODELED | 2026-07-03 | A_MOD_BREAK_C1_LOW | 5 | 5 | 80.00 | 591.73 | -414.52 | 2364.26 | 411.87 | 1952.39 |
| V11_MODELED | 2026-07-03 | C_OR_BREAKDOWN | 2 | 2 | 50.00 | 59.93 | -579.77 | -354.58 | 165.26 | -519.84 |
| V11_MODELED | 2026-07-03 | E_ORB_BREAKOUT_LONG | 1 | 1 | 100.00 | 2668.71 | 0.00 | 2752.20 | 83.49 | 2668.71 |
| V11_MODELED | 2026-07-03 | G_HIGHER_HIGH_BREAK | 1 | 1 | 0.00 | 0.00 | -1333.12 | -1250.79 | 82.33 | -1333.12 |
| V11_MODELED | 2026-07-06 | L_DOUBLE_BOTTOM_VWAP | 5 | 5 | 40.00 | 120.44 | -859.82 | -1926.63 | 411.96 | -2338.59 |
| V11_MODELED | 2026-07-07 | A_MOD_BREAK_C1_LOW | 3 | 3 | 0.00 | 0.00 | -981.46 | -2696.31 | 248.08 | -2944.39 |
| V11_MODELED | 2026-07-07 | A_PULLBACK_C2_THEN_BREAK_C2_LOW | 2 | 2 | 50.00 | 749.23 | -1319.71 | -405.60 | 164.88 | -570.48 |
| V11_MODELED | 2026-07-07 | C_OR_BREAKDOWN | 3 | 3 | 0.00 | 0.00 | -1027.16 | -2833.86 | 247.61 | -3081.47 |
| V11_MODELED | 2026-07-07 | E_ORB_BREAKOUT_LONG | 1 | 1 | 100.00 | 2655.53 | 0.00 | 2738.85 | 83.32 | 2655.53 |
| V11_MODELED | 2026-07-07 | L_DOUBLE_BOTTOM_VWAP | 6 | 6 | 16.67 | 555.60 | -272.40 | -313.22 | 493.16 | -806.38 |

## Trade Buckets

| bucket | rows |
| --- | --- |
| MATCHED | 6 |
| LIVE_ONLY | 33 |
| BACKTEST_ONLY | 69 |
| LIVE_NONFILLED_SKIPPED_OR_REJECTED | 2 |

## Live Nonfilled/Skipped/Rejection Rows

| date | symbol | side | setup | entry_time | exit_reason | qty | net_pnl | source_file |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | RALLIS | SHORT | C_OR_BREAKDOWN | 2026-06-29 14:26:30+0530 | ENTRY_SKIPPED_STALE_SIGNAL | 124 | 0.00 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | SWSOLAR | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 14:26:30+0530 | ENTRY_SKIPPED_STALE_SIGNAL | 292 | 0.00 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |

## Matched Trade Tolerance Failures

| date | symbol | side | setup | entry_delta_min | exit_delta_min | entry_price_diff | exit_price_diff | net_pnl_diff | net_pnl_tol | exit_reason_match | net_pnl_within_tol |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | BRIGADE | SHORT | C_OR_BREAKDOWN | 0.48 | 0.02 | -0.35 | -0.35 | -742.13 | 99.84 | True | False |
| 2026-06-29 | ICRA | SHORT | A_MOD_BREAK_C1_LOW | 0.48 | 0.03 | 1.00 | 0.00 | -163.07 | 94.74 | True | False |
| 2026-06-29 | RPTECH | SHORT | G_LOWER_LOW_BREAK | 0.45 | 13.03 | 0.30 | 2.10 | -756.23 | 99.74 | False | False |
| 2026-06-30 | MARKSANS | SHORT | B_HUGE_RED_FAILED_BOUNCE | 0.35 | 0.77 | -0.11 | -0.11 | -738.52 | 99.77 | True | False |
| 2026-06-30 | NMDC | SHORT | A_MOD_BREAK_C1_LOW | 0.47 | 0.72 | 0.00 | 0.00 | -913.39 | 99.91 | True | False |
| 2026-07-01 | FUSION | LONG | E_ORB_BREAKOUT_LONG | 0.18 | 0.03 | 0.00 | 0.00 | 1345.56 | 100.05 | True | False |

## Live-Only Sample

| date | symbol | side | setup | entry_time | exit_reason | net_pnl | source_file |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | BIRLACORPN | SHORT | E_VWAP_LOSE_EARLY_SHORT | 2026-06-29 09:51:26+0530 | SL | -54.43 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | JUSTDIAL | SHORT | E_VWAP_LOSE_EARLY_SHORT | 2026-06-29 09:51:26+0530 | TARGET | 315.14 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | KERNEX | SHORT | E_VWAP_LOSE_EARLY_SHORT | 2026-06-29 09:51:26+0530 | SL | -293.95 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | GODFRYPHLP | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 11:01:25+0530 | SL | -565.66 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | IEX | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 11:01:25+0530 | EOD_CLOSE | -118.57 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | PVRINOX | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:11:24+0530 | EOD_CLOSE | 217.06 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | IFBIND | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 11:16:24+0530 | SL | -572.01 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | LODHA | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:36:29+0530 | EOD_CLOSE | -19.94 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | AFFLE | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 12:26:27+0530 | SL | -605.24 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | TATACONSUM | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:26:27+0530 | SL | -305.15 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | AVANTIFEED | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 12:31:25+0530 | TARGET | 219.93 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | BRITANNIA | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:31:25+0530 | EOD_CLOSE | 207.18 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | MINDACORP | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 12:31:25+0530 | TARGET | 784.27 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | DEEPINDS | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:41:28+0530 | SL | -290.96 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | MARUTI | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:46:25+0530 | EOD_CLOSE | 60.60 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | MEDPLUS | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:46:25+0530 | EOD_CLOSE | 266.70 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | GRASIM | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:56:29+0530 | EOD_CLOSE | -75.16 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | HYUNDAI | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:16:28+0530 | EOD_CLOSE | 231.88 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | PARADEEP | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 13:21:25+0530 | EOD_CLOSE | -194.39 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |
| 2026-06-29 | TENNIND | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 13:26:27+0530 | SL | -310.11 | C:\TradingData\eqidv2\live_signals\paper_trades_2026-06-29_id_5min_v7.csv |

## Backtest-Only Sample

| date | symbol | side | setup | entry_time | exit_reason | net_pnl | source_file |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | BPCL | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:26:00+0530 | EOD_CLOSE | -66.06 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | JKLAKSHMI | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 11:31:00+0530 | SL | -1230.10 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | CHOLAFIN | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:36:00+0530 | EOD_CLOSE | 165.01 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | SUMICHEM | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:36:00+0530 | EOD_CLOSE | 371.05 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | INDIAMART | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:46:00+0530 | SL | -1025.77 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | HDBFS | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:56:00+0530 | SL | -1033.49 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | BBTC | SHORT | B_HUGE_RED_FAILED_BOUNCE | 2026-06-29 12:06:00+0530 | SL | -1018.54 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | BLUESTONE | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 12:11:00+0530 | EOD_CLOSE | -220.21 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | CAMLINFINE | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:11:00+0530 | EOD_CLOSE | 788.36 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | DEVYANI | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:11:00+0530 | EOD_CLOSE | -136.45 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | BATAINDIA | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:16:00+0530 | EOD_CLOSE | -25.34 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | NAVKARCORP | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:16:00+0530 | EOD_CLOSE | 94.24 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | TMB | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:51:00+0530 | SL | -1032.10 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | ULTRACEMCO | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:51:00+0530 | EOD_CLOSE | 101.86 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | HARIOMPIPE | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 13:56:00+0530 | EOD_CLOSE | -179.85 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | YATRA | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 14:01:00+0530 | TARGET | 1914.73 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-29 | RALLIS | SHORT | C_OR_BREAKDOWN | 2026-06-29 14:26:00+0530 | SL | -1030.07 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-29\trades.csv |
| 2026-06-30 | BERGEPAINT | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 11:16:00+0530 | SL | -1326.67 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-30\trades.csv |
| 2026-06-30 | RANEHOLDIN | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-30 11:16:00+0530 | TARGET | 901.23 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-30\trades.csv |
| 2026-06-30 | ABDL | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 11:56:00+0530 | EOD_CLOSE | -91.41 | C:\TradingData\eqidv2\backtesting_result_v11\2026-06-30\trades.csv |

## Ranked Root Causes

| rank_order | finding | evidence_count | sample_trade | evidence | fix |
| --- | --- | --- | --- | --- | --- |
| 1 | Live trade missing from V11 selected signal set | 33 | 2026-06-29 BIRLACORPN SHORT E_VWAP_LOSE_EARLY_SHORT | live entry=2026-06-29 09:51:26+0530, live net=-54.43. | Inspect V11 selected_strategy_rejects and live JSON for this signal. |
| 2 | 5-minute data roots differ by design | 1 |  | V7 live feed root is C:\TradingData\eqidv2\stocks_indicators_5min_eq_live; V11 backtest root is C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2. | For strict OHLC parity, archive per-slot live bars or diff symbol/day OHLC before comparing signals. |
| 3 | Exit reason mismatch | 1 | 2026-06-29 RPTECH SHORT G_LOWER_LOW_BREAK | live=EOD_CLOSE vs v11=SL. | Compare 5-second live LTP path against 1-minute OHLC resolver for the named trades. |
| 4 | Cross-sectional and gate state are replayed from live JSON in this run | 1 |  | All V11 inputs.txt files show mode=live_parity and live_candidate_json_dir under signal_discovery_v7_5mins_ID. | Keep using live_parity for EOD parity; avoid historical_full_day for gate-state assertions. |
| 5 | V11 backtest-only trade absent from live signal CSV | 69 | 2026-06-29 BPCL SHORT C_OR_BREAKDOWN | v11 entry=2026-06-29 11:26:00+0530, v11 modeled net=-66.06. | Check live executor skips/brakes if live signal exists; otherwise compare V11 selected profile against live signal writer. |
| 6 | Execution price mismatch beyond modeled slippage | 2 | 2026-06-29 BRIGADE SHORT C_OR_BREAKDOWN | entry_diff=-0.35, exit_diff=-0.35. | Model V7 paper's actual LTP-at-signal and exit slippage, or store the LTP tick used by V7 for replay. |
| 6 | Matched-trade net P&L outside 10 bps notional tolerance | 6 | 2026-06-29 BRIGADE SHORT C_OR_BREAKDOWN | net_diff=-742.13, tolerance=99.84. | Prioritize entry/exit fill model parity before judging setup edge. |
| 7 | Cost model mismatch in raw V11 live-parity output | 6 | 2026-06-29 BRIGADE SHORT C_OR_BREAKDOWN | V11 reported_costs=0.0 while modeled_costs=82.69; live_costs=29.31. | Keep V11 raw output, but use reconciliation costed columns for parity until live_parity resolver writes statutory costs. |
| 8 | No systemic timezone offset detected by keying in IST and allowing one 5-minute bar | 6 |  | The reconciler parses all timestamps to Asia/Kolkata and matches on entry_bar. | Keep explicit timezone normalization in daily parity. |
| 9 | Logic drift remains between live executor and V11 resolver | 1 |  | V7 has entry retry/slip gate, portfolio brakes, C_OR time stop/session cap, and 5-second LTP exits; V11 live_parity uses deterministic selected signals and 1-minute OHLC resolver. | Move shared execution-resolution rules into a common pure module used by both paper executor and V11. |

## Root Cause Checks In Requested Order

| check_order | area | status | sample | evidence | fix |
| --- | --- | --- | --- | --- | --- |
| 1 | Config/parameter drift | PARTIAL_FAIL | 2026-06-29 BRIGADE SHORT C_OR_BREAKDOWN | Wrapper diff found scanner/gate/filter values aligned; execution settings drift materially. Matched trades still miss 10 bps P&L tolerance. | Make V11 live-parity execution options mirror V7 paper executor before comparing setup edge. |
| 2 | Data mismatch | LIKELY | 2026-06-29 BPCL SHORT C_OR_BREAKDOWN | 5-minute roots differ and 69 V11 trades are absent from live paper fills; OHLC checksums were not available in the V7 CSVs. | Archive or checksum per-slot live OHLC and diff it against the V11 root before signal comparison. |
| 3 | Signal timing/lookahead | PARTIAL_FAIL | 2026-06-29 RPTECH SHORT G_LOWER_LOW_BREAK | Matched entries are within one bar, but V7 fills from live LTP seconds after signal while V11 resolves on 1-minute OHLC; RPTECH shows an exit-reason/timing mismatch. | Replay completed-bar decision time and next-bar/next-tick fill rules identically in both paths. |
| 4 | Cross-sectional RS universe mismatch | POSSIBLE | 2026-06-29 BPCL SHORT C_OR_BREAKDOWN | V11 live_parity replays live JSON snapshots, reducing future-data risk, but V11 selected signals still diverge sharply from live signal CSVs. | Persist per-slot live universe/feed coverage and RS ranks, then compare rank rows symbol by symbol. |
| 5 | Gate/qualification tracker state | MOSTLY_ALIGNED | 2026-06-29 BIRLACORPN SHORT E_VWAP_LOSE_EARLY_SHORT | Every V11 inputs.txt reports live_parity mode; still, 33 filled live trades are missing from V11 selected trades. | For each live-only row, join against V11 selected_strategy_rejects and archived JSON gate fields. |
| 6 | Execution reality | FAIL | 2026-06-29 RALLIS SHORT C_OR_BREAKDOWN | V7 paper produced 2 nonfilled/skipped rows and matched fills have price/P&L failures under the current model. | Store and replay V7 paper's exact entry/exit LTP ticks, stale-signal skips, retry gates, and portfolio brakes. |
| 7 | Cost model mismatch | FAIL | 2026-06-29 BRIGADE SHORT C_OR_BREAKDOWN | Raw V11 live-parity trades report zero costs while V7 paper rows include statutory costs; reconciler recomputes V11 costs for comparison. | Write statutory cost columns from V11 live-parity resolver using nse_intraday_costs.py. |
| 8 | Timezone/bar indexing | PASS_WITH_RESIDUAL | 2026-06-29 BRIGADE SHORT C_OR_BREAKDOWN | All matching keys are normalized to Asia/Kolkata and allow one 5-minute bar; no systemic one-bar offset appeared among matched trades. | Keep explicit IST parsing and include entry_bar in all daily parity outputs. |
| 9 | Logic drift | FAIL | 2026-06-29 RPTECH SHORT G_LOWER_LOW_BREAK | V7 paper executor has retry/slip gates, C_OR time stop/session cap, portfolio brakes, 5-second LTP exits, and statutory costs; V11 uses deterministic selected signals and a 1-minute resolver. | Extract shared pure execution-resolution logic used by both V7 paper and V11 backtest. |

## Fixes Ranked By Impact Vs Effort

| priority | fix | impact | effort |
| --- | --- | --- | --- |
| 1 | Make V11 live_parity resolver emit statutory costs and V7-style exit slippage columns | High | Low |
| 2 | Archive the exact V7 entry LTP and exit LTP ticks used by paper executor, then replay those in parity | High | Medium |
| 3 | Extract shared execution rules for entry retry/slip gate, C_OR time stop, portfolio brakes, and exit slippage into one pure module | High | Medium |
| 4 | Store per-slot live universe/feed coverage and OHLC checksum so RS/data mismatches are directly provable | Medium | Medium |
| 5 | Add a scheduled EOD call to `python reconcile.py --run-v11 --dates YYYY-MM-DD` after the 16:00 V11 job | Medium | Low |

## Output Files

- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\raw_live_paper_trades.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\raw_live_signals.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\raw_v11_trades.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\raw_v11_signals.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\matched_trades.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\live_only_trades.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\backtest_only_trades.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\live_nonfilled_rows.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\matched_signals.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\live_only_signals.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\backtest_only_signals.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\daily_aggregate.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\per_setup_aggregate.csv`
- `C:\TradingData\eqidv2\backtesting_result_v11\seven_day_reconcile\root_causes.csv`
