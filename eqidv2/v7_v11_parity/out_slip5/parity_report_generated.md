# V7-live vs V11-backtest — Parity Reconciliation (auto-generated tables)

Sessions: 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02, 2026-07-03, 2026-07-06, 2026-07-07  
Verdict: **FAIL**

## Per-day

| date | live_signals | bt_signals | live_trades | bt_trades | matched | live_only | live_stale | bt_only | live_net_stat_rs | bt_net_stat_rs | live_recorded_net_rs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | 31 | 20 | 29 | 20 | 3 | 26 | 2 | 17 | -4349.8 | -6792.9 | -4349.8 |
| 2026-06-30 | 7 | 12 | 7 | 12 | 2 | 5 | 0 | 10 | -448.4 | -5231.5 | -448.4 |
| 2026-07-01 | 1 | 4 | 1 | 4 | 1 | 0 | 0 | 3 | 1321.0 | 1657.9 | 1321.0 |
| 2026-07-02 | 0 | 10 | 0 | 10 | 0 | 0 | 0 | 10 | 0 | -792.6 | 0 |
| 2026-07-03 | 0 | 9 | 0 | 9 | 0 | 0 | 0 | 9 | 0 | 2174.0 | 0 |
| 2026-07-06 | 0 | 5 | 0 | 5 | 0 | 0 | 0 | 5 | 0 | -2587.3 | 0 |
| 2026-07-07 | 2 | 16 | 2 | 16 | 0 | 2 | 0 | 16 | -260.3 | -6920.3 | -260.4 |

## Signal reconciliation

| date | live_signals | bt_signals | matched | live_only | bt_only |
| --- | --- | --- | --- | --- | --- |
| 2026-06-29 | 31 | 20 | 4 | 27 | 16 |
| 2026-06-30 | 7 | 12 | 2 | 5 | 10 |
| 2026-07-01 | 1 | 4 | 1 | 0 | 3 |
| 2026-07-02 | 0 | 10 | 0 | 0 | 10 |
| 2026-07-03 | 0 | 9 | 0 | 0 | 9 |
| 2026-07-06 | 0 | 5 | 0 | 0 | 5 |
| 2026-07-07 | 2 | 16 | 0 | 2 | 16 |

## Per-setup (ranked by unmatched)

| setup | live_n | bt_n | matched | live_only | bt_only | live_net | bt_net |
| --- | --- | --- | --- | --- | --- | --- | --- |
| C_OR_BREAKDOWN | 13 | 18 | 1 | 12 | 17 | -170.1 | -8359.0 |
| A_MOD_BREAK_C1_LOW | 7 | 18 | 2 | 5 | 16 | -1154.0 | -7688.6 |
| L_DOUBLE_BOTTOM_VWAP | 3 | 16 | 0 | 3 | 16 | -1247.6 | -3765.3 |
| G_HIGHER_HIGH_BREAK | 0 | 14 | 0 | 0 | 14 | 0.0 | -1348.9 |
| L_PRESSURE_BURST_VWAP | 8 | 0 | 0 | 8 | 0 | -2454.1 | 0.0 |
| B_HUGE_RED_FAILED_BOUNCE | 2 | 3 | 1 | 1 | 2 | -291.5 | -1986.8 |
| E_VWAP_LOSE_EARLY_SHORT | 3 | 0 | 0 | 3 | 0 | -33.2 | 0.0 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 0 | 3 | 0 | 0 | 3 | 0.0 | -2049.7 |
| E_ORB_BREAKOUT_LONG | 1 | 3 | 1 | 0 | 2 | 1321.0 | 7687.0 |
| D_EMA20_REJECTION | 1 | 0 | 0 | 1 | 0 | 467.9 | 0.0 |
| G_LOWER_LOW_BREAK | 1 | 1 | 1 | 0 | 0 | -176.0 | -981.4 |

## Root-cause tally

| cause | count |
| --- | --- |
| backtest_only | 37 |
| live_only_real | 33 |
| backtest_only_live_zero_day | 17 |
| raw_pre_gate_readmit_bug | 16 |
| live_stale_skip | 2 |

## Matched trades (entry/exit/net diffs)

| date | ticker | side | setup | bar_dt_min | live_entry | bt_entry | entry_bps | live_exit | bt_exit | exit_bps | live_outcome | bt_outcome | live_net_stat | bt_net_stat | net_bps_of_notional | live_slippage_vs_model_rs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | BRIGADE | SHORT | C_OR_BREAKDOWN | 0.0 | 499.55 | 499.2 | 7.0 | 504.3 | 503.7 | 11.9 | SL | SL | -290.6 | -1083.0 | 288.4 | 0.0 |
| 2026-06-29 | ICRA | SHORT | A_MOD_BREAK_C1_LOW | 0.0 | 5262.37 | 5263.37 | 1.9 | 5271.13 | 5268.5 | 5.0 | EOD_CLOSE | EOD_CLOSE | -57.4 | -267.9 | 100.0 | -0.0 |
| 2026-06-29 | RPTECH | SHORT | G_LOWER_LOW_BREAK | 0.0 | 744.03 | 744.33 | 4.0 | 748.57 | 750.29 | 23.0 | EOD_CLOSE | SL | -176.0 | -981.4 | 328.0 | -0.0 |
| 2026-06-30 | NMDC | SHORT | A_MOD_BREAK_C1_LOW | 0.0 | 83.96 | 83.96 | 0.0 | 84.92 | 84.88 | 4.7 | SL | SL | -311.7 | -1278.0 | 387.5 | 0.0 |
| 2026-06-30 | MARKSANS | SHORT | B_HUGE_RED_FAILED_BOUNCE | 0.0 | 264.76 | 264.65 | 4.2 | 267.27 | 267.03 | 9.0 | SL | SL | -290.4 | -1080.1 | 286.8 | 0.0 |
| 2026-07-01 | FUSION | LONG | E_ORB_BREAKOUT_LONG | 0.0 | 200.1 | 200.1 | 0.0 | 205.6 | 205.59999999999997 | 0.0 | TARGET | TARGET | 1321.0 | 2565.1 | 248.7 | -0.0 |

## Sample LIVE-ONLY (up to 15)

| date | ticker | side | setup | signal_bar | outcome | _cause | _note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | KERNEX | SHORT | E_VWAP_LOSE_EARLY_SHORT | 2026-06-29 09:50:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | GODFRYPHLP | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 11:00:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | IFBIND | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 11:15:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | JUSTDIAL | SHORT | E_VWAP_LOSE_EARLY_SHORT | 2026-06-29 09:50:00 | TARGET | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | MINDACORP | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 12:30:00 | TARGET | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | TATACONSUM | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:25:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | AFFLE | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 12:25:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | BIRLACORPN | SHORT | E_VWAP_LOSE_EARLY_SHORT | 2026-06-29 09:50:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | POKARNA | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 13:40:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | DEEPINDS | SHORT | C_OR_BREAKDOWN | 2026-06-29 12:40:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | PANACEABIO | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 13:45:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | AVANTIFEED | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 12:30:00 | TARGET | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | TENNIND | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 13:25:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | AWL | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:30:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |
| 2026-06-29 | RICOAUTO | LONG | L_PRESSURE_BURST_VWAP | 2026-06-29 14:05:00 | SL | live_only_real | Live took it; backtest produced no matching signal — investigate feed/universe/gate divergence for this (ticker, setup, bar). |

## Sample BACKTEST-ONLY (up to 25)

| date | ticker | side | setup | signal_bar | outcome | bt_gross_rs | _cause | _note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-06-29 | BPCL | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:25:00 | EOD_CLOSE | 66.404052734375 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | JKLAKSHMI | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 11:30:00 | SL | -1097.0399999999954 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | CHOLAFIN | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:35:00 | EOD_CLOSE | 297.3572656249944 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | SUMICHEM | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:35:00 | EOD_CLOSE | 502.89856567383026 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | INDIAMART | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:45:00 | SL | -893.3599999999797 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | HDBFS | SHORT | C_OR_BREAKDOWN | 2026-06-29 11:55:00 | SL | -900.0900000000069 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | BBTC | SHORT | B_HUGE_RED_FAILED_BOUNCE | 2026-06-29 12:05:00 | SL | -886.3999999999942 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | BLUESTONE | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 12:10:00 | EOD_CLOSE | -87.41545898437585 | raw_pre_gate_readmit_bug | RAW_PRE_GATE readmit setup: v11 readmits it from the full ranked frame, but the live scanner v8-drops it before readmission — known live-emission bug. |
| 2026-06-29 | CAMLINFINE | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:10:00 | EOD_CLOSE | 916.8027978515564 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | DEVYANI | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:10:00 | EOD_CLOSE | -0.0027374267603619273 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | BATAINDIA | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:15:00 | EOD_CLOSE | 107.25174560546225 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | NAVKARCORP | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:15:00 | EOD_CLOSE | 223.43744293212836 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | TMB | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:50:00 | SL | -898.7200000000081 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | ULTRACEMCO | SHORT | C_OR_BREAKDOWN | 2026-06-29 13:50:00 | EOD_CLOSE | 226.55999999999767 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | HARIOMPIPE | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-29 13:55:00 | EOD_CLOSE | -48.63687499999651 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-29 | YATRA | LONG | L_DOUBLE_BOTTOM_VWAP | 2026-06-29 14:00:00 | TARGET | 1998.00000000001 | raw_pre_gate_readmit_bug | RAW_PRE_GATE readmit setup: v11 readmits it from the full ranked frame, but the live scanner v8-drops it before readmission — known live-emission bug. |
| 2026-06-29 | RALLIS | SHORT | C_OR_BREAKDOWN | 2026-06-29 14:25:00 | SL | -898.0 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | BERGEPAINT | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 11:15:00 | SL | -1195.099999999993 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | RANEHOLDIN | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-30 11:15:00 | TARGET | 983.25 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | ABDL | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 11:55:00 | EOD_CLOSE | 40.23363769531318 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | HIRECT | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 12:00:00 | EOD_CLOSE | 1046.612124023447 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | PRAJIND | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 12:00:00 | SL | -1197.0599999999974 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | VOLTAMP | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 12:20:00 | EOD_CLOSE | 1619.0999999999967 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | PENIND | LONG | G_HIGHER_HIGH_BREAK | 2026-06-30 12:30:00 | EOD_CLOSE | -496.34182495117255 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
| 2026-06-30 | XPROINDIA | SHORT | A_MOD_BREAK_C1_LOW | 2026-06-30 12:40:00 | SL | -1094.8200000000052 | backtest_only | Backtest signalled; live did not take it — check late-detection drop, position/slot cap, max-entry-slip gate, or dedupe. |
