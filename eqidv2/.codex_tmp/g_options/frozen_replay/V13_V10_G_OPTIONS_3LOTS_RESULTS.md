# V13-V10-G ATM CE/PE options: three lots, five-minute replay

**Closed modeled net: Rs99,673.96; 20 closed trades; 0 entered trades with unresolved exits.** Source G has 73 selected orders across 31 observed sessions. Actual mapped option history covers 10 sessions (2026-08-26 to 2026-09-11).

**Provisional paper-test rule: 17.5% premium stop and 22.5% premium target, all three lots.** Do not promote the train-selected challenger: it underperformed the predeclared baseline on later dates. Keep the predeclared baseline for further paper testing; this small reused sample does not establish an optimal live stop/target.

This is an options adaptation of the retained G signals. It is not the stock result multiplied by three, and it is not a full-history or one-year options result. The source G configuration was already selected on this history; the chronological options evaluation is not an untouched test of the complete strategy.

## Execution rules

- Preserve G five-minute selection, one-minute confirmation and subsequent underlying trigger. Seven untriggered orders remain unfilled.
- Buy ATM CE for LONG and ATM PE for SHORT. Choose the closest strike to the exact completed underlying minute close at option entry; ties go to the lower strike. Freeze the contract through exit.
- Enter at the first five-minute bar open at or after the underlying trigger minute has completed. Equal-boundary entry assumes zero additional latency; no enclosing-bar open is used. All timestamps are IST.
- Buy exactly three exchange lots. No fractional lots, fivefold leverage, premium compounding or automatic resizing. All three lots use one stop and one target; manually scheduled exit is 15:15 open.
- Check protective orders on every five-minute candle. Open gaps are resolved first, stops gap through at the adverse open, and bars touching both levels use stop first. Target is a resting limit; its modeled fill is at its tick-rounded limit.
- Require three-lot quantity to be at most 10% of the previous completed option candle volume. Realized entry/exit candle capacity breaches are flagged, not used to select winners. This is an OHLC fill model, not proof of available bid/ask depth.
- Entry bars must print at least the entire three-lot quantity; exits lacking the required total printed volume remain unresolved (an entry-bar exit requires both buy and sell quantities). Held-path gaps also leave an entered trade unresolved with premium reserved and fees recorded. Unresolved trades are penalized as total premium losses during parameter selection.
- Reserve actual premium plus entry fees from Rs1,500,000 cash. Reuse proceeds only when the exit is observable; intrabar exits release cash at bar end. No separate stock margin allocation is applied.
- Base adverse market-fill slippage: 10 bps per side. Costs include Rs20 per executed order, sell STT, exchange fees, SEBI, GST, stamp duty and IPFT using the [Zerodha schedule checked September 14, 2026](https://zerodha.com/charges). Tax rounding and historical rate changes beyond the frozen schedule are not reconstructed.

## How stops and targets were selected

The predeclared baseline is 17.5% premium SL / 22.5% premium target. 19 fixed pairs were compared. Training ends 2026-09-02; later option-data sessions are evaluated after freezing the profile. Training-only score is mean net return on entry premium minus one standard error. At least eight entered training trades over four days are required for a global selection. A CE/PE-specific or individual five-minute setup override needs twelve training trades over four days; sparse groups inherit the global profile or baseline. The grid file includes descriptive full-sample rankings, which must not be read as prospective performance. The main trade/bar ledgers always show the predeclared baseline; separate challenger files preserve the training-selected experiment.

Training selected 25% SL / 40% target. Later-date net was Rs-4,373.08 versus Rs13,274.21 for the baseline. Do not promote the train-selected challenger: it underperformed the predeclared baseline on later dates. Keep the predeclared baseline for further paper testing; this small reused sample does not establish an optimal live stop/target.

| setup_id   | option_type   |   stop_pct |   target_pct | selection_scope      |   setup_training_trades |   closed |   wins |    net_pnl |
|:-----------|:--------------|-----------:|-------------:|:---------------------|------------------------:|---------:|-------:|-----------:|
| 0926_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       2 |        3 |      3 |  41,648.89 |
| 0926_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       1 |        4 |      4 |  44,642.39 |
| 0931_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       0 |        0 |      0 |       0.00 |
| 0931_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       0 |        2 |      1 |  -4,877.82 |
| 0936_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       0 |        0 |      0 |       0.00 |
| 0936_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       0 |        0 |      0 |       0.00 |
| 0941_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       1 |        2 |      1 | -13,694.41 |
| 0941_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       1 |        1 |      1 |  12,240.44 |
| 0946_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       0 |        0 |      0 |       0.00 |
| 0946_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       1 |        1 |      0 |    -724.67 |
| 0951_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       1 |        2 |      1 |   1,082.45 |
| 0956_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       2 |        2 |      2 |  43,940.54 |
| 1001_LONG  | CE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       2 |        3 |      1 | -24,583.85 |
| 1121_SHORT | PE            |      17.50 |        22.50 | PREDECLARED_BASELINE |                       0 |        0 |      0 |       0.00 |

## Results and chronological evaluation

| profile                      | period                   |   closed |   unresolved |   win_rate_pct |   profit_factor |    net_pnl |   daily_realized_drawdown |
|:-----------------------------|:-------------------------|---------:|-------------:|---------------:|----------------:|-----------:|--------------------------:|
| BASELINE_17.5_SL_22.5_TARGET | ALL_AVAILABLE            |       20 |            0 |          70.00 |            2.48 |  99,673.96 |                  5,639.82 |
| BASELINE_17.5_SL_22.5_TARGET | TRAIN                    |       11 |            0 |          63.64 |            3.54 |  86,399.75 |                      0.00 |
| BASELINE_17.5_SL_22.5_TARGET | CHRONOLOGICAL_EVALUATION |        9 |            0 |          77.78 |            1.40 |  13,274.21 |                  5,639.82 |
| BASELINE_17.5_SL_22.5_TARGET | ASOF_METADATA_ONLY       |       11 |            0 |          81.82 |            2.04 |  34,652.16 |                  5,639.82 |
| TRAIN_SELECTED_CHALLENGER    | ALL_AVAILABLE            |       20 |            0 |          65.00 |            3.94 | 162,287.24 |                 31,420.53 |
| TRAIN_SELECTED_CHALLENGER    | TRAIN                    |       11 |            0 |          63.64 |           15.20 | 166,660.32 |                      0.00 |
| TRAIN_SELECTED_CHALLENGER    | CHRONOLOGICAL_EVALUATION |        9 |            0 |          66.67 |            0.90 |  -4,373.08 |                 31,420.53 |
| TRAIN_SELECTED_CHALLENGER    | ASOF_METADATA_ONLY       |       11 |            0 |          72.73 |            1.46 |  19,976.64 |                 31,420.53 |

Peak reserved premium plus entry fees: Rs322,919.75. Capital rejections: 0. Unresolved premium and fees still reserved: Rs0.00. Closed P&L plus full loss of unresolved premiums: Rs99,673.96. Drawdown above uses daily realized closes; it is not intraday mark-to-market drawdown.

## Monthly results

| month   |   attempts |   entered |   closed |   unresolved |   wins |   losses |   win_rate_pct |   profit_factor |   net_pnl |   gross_pnl |    costs |   unresolved_entry_costs |   net_pnl_full_premium_loss_bound |   daily_realized_drawdown |   capital_rejections |   entry_capacity_breaches |   exit_capacity_breaches |
|:--------|-----------:|----------:|---------:|-------------:|-------:|---------:|---------------:|----------------:|----------:|------------:|---------:|-------------------------:|----------------------------------:|--------------------------:|---------------------:|--------------------------:|-------------------------:|
| 2026-07 |          5 |         0 |        0 |            0 |      0 |        0 |         nan    |          nan    |      0.00 |        0.00 |     0.00 |                     0.00 |                              0.00 |                      0.00 |                    0 |                         0 |                        0 |
| 2026-08 |         50 |         9 |        9 |            0 |      5 |        4 |          55.56 |            2.91 | 65,021.81 |   67,239.75 | 2,217.94 |                     0.00 |                         65,021.81 |                      0.00 |                    0 |                         1 |                        3 |
| 2026-09 |         18 |        11 |       11 |            0 |      9 |        2 |          81.82 |            2.04 | 34,652.16 |   36,735.00 | 2,082.84 |                     0.00 |                         34,652.16 |                  5,639.82 |                    0 |                         1 |                        3 |

## Coverage and excluded orders

| reason                          |   orders |
|:--------------------------------|---------:|
| MISSING_MONTHLY_OPTION_METADATA |       40 |
| UNDERLYING_UNFILLED             |        7 |
| PREVIOUS_BAR_MISSING            |        3 |
| PREVIOUS_VOLUME_INSUFFICIENT    |        3 |

| mapping_status                  |   orders |
|:--------------------------------|---------:|
| MAPPED_CAUSAL                   |       13 |
| MAPPED_RETROSPECTIVE_METADATA   |       13 |
| MISSING_MONTHLY_OPTION_METADATA |       40 |
| UNDERLYING_UNFILLED             |        7 |

Historical dated instrument masters are preferred. MAPPED_RETROSPECTIVE_METADATA means the same required monthly expiry was reconstructed from a later snapshot; listing/strike-universe and lot-size history are not independently proved for those dates. These rows are explicitly separated from ASOF_METADATA_ONLY above. Missing expired options are not replaced by September contracts. [Kite documents that expired option history is unavailable](https://kite.trade/forum/discussion/3493/historical-data-for-expired-f-o-tokens). A zero in an uncovered session means no measurable options P&L, not a verified no-trade day.

## Execution sensitivity with the baseline fixed

|   slippage_bps |   previous_volume_participation |   closed |   unresolved |   win_rate_pct |    net_pnl |   entry_capacity_breaches |   exit_capacity_breaches |
|---------------:|--------------------------------:|---------:|-------------:|---------------:|-----------:|--------------------------:|-------------------------:|
|          10.00 |                            0.10 |    20.00 |         0.00 |          70.00 |  99,673.96 |                      2.00 |                     6.00 |
|          10.00 |                            0.25 |    21.00 |         0.00 |          66.67 |  92,298.34 |                      2.00 |                     4.00 |
|          10.00 |                            1.00 |    23.00 |         0.00 |          69.57 | 110,369.21 |                      0.00 |                     0.00 |
|          25.00 |                            0.10 |    20.00 |         0.00 |          70.00 |  98,820.59 |                      2.00 |                     6.00 |
|          25.00 |                            0.25 |    21.00 |         0.00 |          66.67 |  91,444.96 |                      2.00 |                     4.00 |
|          25.00 |                            1.00 |    23.00 |         0.00 |          69.57 | 109,422.18 |                      0.00 |                     0.00 |
|          50.00 |                            0.10 |    20.00 |         0.00 |          65.00 |  96,961.37 |                      2.00 |                     6.00 |
|          50.00 |                            0.25 |    21.00 |         0.00 |          61.90 |  89,480.70 |                      2.00 |                     3.00 |
|          50.00 |                            1.00 |    23.00 |         0.00 |          65.22 | 107,289.31 |                      0.00 |                     0.00 |

## Trade ledger

| day        | setup_id   | option_symbol         |   lot_size |   quantity | entry_ts                  |   entry_price |   stop_price |   target_price | exit_ts                   |   exit_price | reason         |    net_pnl | status   |
|:-----------|:-----------|:----------------------|-----------:|-----------:|:--------------------------|--------------:|-------------:|---------------:|:--------------------------|-------------:|:---------------|-----------:|:---------|
| 2026-08-26 | 0956_LONG  | HINDZINC26SEP620CE    |   1,225.00 |   3,675.00 | 2026-08-26 10:00:00+05:30 |         23.45 |        19.35 |          28.75 | 2026-08-26 14:40:00+05:30 |        28.75 | TARGET         |  19,188.58 | CLOSED   |
| 2026-08-26 | 1001_LONG  | KOTAKBANK26SEP410CE   |   2,000.00 |   6,000.00 | 2026-08-26 10:05:00+05:30 |          9.20 |         7.60 |          11.30 | 2026-08-26 10:40:00+05:30 |        11.30 | TARGET         |  12,397.73 | CLOSED   |
| 2026-08-27 | 0941_LONG  | AMBER26SEP7700CE      |     100.00 |     300.00 | 2026-08-27 09:45:00+05:30 |        298.30 |       246.10 |         365.45 | 2026-08-27 12:05:00+05:30 |       245.85 | STOP           | -15,964.15 | CLOSED   |
| 2026-08-27 | 0946_SHORT | IDFCFIRSTB26SEP84PE   |   9,275.00 |  27,825.00 | 2026-08-27 09:50:00+05:30 |          1.85 |         1.53 |           2.27 | 2026-08-27 15:15:00+05:30 |         1.83 | TIME_EXIT_1515 |    -724.67 | CLOSED   |
| 2026-08-27 | 0951_SHORT | RECLTD26SEP320PE      |   1,575.00 |   4,725.00 | 2026-08-27 09:55:00+05:30 |          7.25 |         6.00 |           8.90 | 2026-08-27 15:15:00+05:30 |         6.70 | TIME_EXIT_1515 |  -2,722.18 | CLOSED   |
| 2026-08-27 | 0956_LONG  | KALYANKJIL26SEP620CE  |   1,350.00 |   4,050.00 | 2026-08-27 10:00:00+05:30 |         27.35 |        22.60 |          33.55 | 2026-08-27 13:20:00+05:30 |        33.55 | TARGET         |  24,751.96 | CLOSED   |
| 2026-08-28 | 0926_LONG  | COFORGE26SEP1960CE    |     475.00 |   1,425.00 | 2026-08-28 09:30:00+05:30 |         76.35 |        63.00 |          93.55 | 2026-08-28 12:30:00+05:30 |        93.55 | TARGET         |  24,157.78 | CLOSED   |
| 2026-08-28 | 1001_LONG  | SAGILITY26SEP46CE     |  12,000.00 |  36,000.00 | 2026-08-28 10:05:00+05:30 |          2.28 |         1.89 |           2.80 | 2026-08-28 13:00:00+05:30 |         1.88 | STOP           | -14,614.15 | CLOSED   |
| 2026-08-31 | 0926_SHORT | KAYNES26SEP3800PE     |     150.00 |     450.00 | 2026-08-31 09:30:00+05:30 |        185.80 |       153.30 |         227.65 | 2026-08-31 10:55:00+05:30 |       227.65 | TARGET         |  18,550.90 | CLOSED   |
| 2026-09-01 | 0926_LONG  | RELIANCE26SEP1300CE   |     500.00 |   1,500.00 | 2026-09-01 09:30:00+05:30 |         27.40 |        22.65 |          33.60 | 2026-09-01 11:35:00+05:30 |        33.60 | TARGET         |   9,137.50 | CLOSED   |
| 2026-09-02 | 0941_SHORT | HEROMOTOCO26SEP5300PE |     150.00 |     450.00 | 2026-09-02 09:45:00+05:30 |        122.75 |       101.30 |         150.40 | 2026-09-02 10:15:00+05:30 |       150.40 | TARGET         |  12,240.44 | CLOSED   |
| 2026-09-07 | 0926_SHORT | MPHASIS26SEP2400PE    |     275.00 |     825.00 | 2026-09-07 09:30:00+05:30 |         77.40 |        63.90 |          94.85 | 2026-09-07 13:05:00+05:30 |        94.85 | TARGET         |  14,170.01 | CLOSED   |
| 2026-09-07 | 0926_SHORT | PERSISTENT26SEP5600PE |     125.00 |     375.00 | 2026-09-07 09:30:00+05:30 |        191.45 |       157.95 |         234.55 | 2026-09-07 15:15:00+05:30 |       192.80 | TIME_EXIT_1515 |     287.86 | CLOSED   |
| 2026-09-07 | 0941_LONG  | SUPREMEIND26SEP3550CE |     175.00 |     525.00 | 2026-09-07 09:45:00+05:30 |        117.15 |        96.65 |         143.55 | 2026-09-07 15:15:00+05:30 |       121.85 | TIME_EXIT_1515 |   2,269.74 | CLOSED   |
| 2026-09-07 | 1001_LONG  | SOLARINDS26SEP22000CE |      50.00 |     150.00 | 2026-09-07 10:05:00+05:30 |        836.85 |       690.45 |       1,025.15 | 2026-09-07 13:15:00+05:30 |       689.75 | STOP           | -22,367.44 | CLOSED   |
| 2026-09-09 | 0926_LONG  | COALINDIA26SEP430CE   |   1,350.00 |   4,050.00 | 2026-09-09 09:30:00+05:30 |          9.15 |         7.55 |          11.25 | 2026-09-09 09:45:00+05:30 |        11.25 | TARGET         |   8,353.61 | CLOSED   |
| 2026-09-09 | 0931_SHORT | PERSISTENT26SEP5400PE |     125.00 |     375.00 | 2026-09-09 09:35:00+05:30 |        163.80 |       135.15 |         200.70 | 2026-09-09 10:10:00+05:30 |       135.00 | STOP           | -10,972.09 | CLOSED   |
| 2026-09-09 | 0951_SHORT | HDFCBANK26SEP690PE    |     650.00 |   1,950.00 | 2026-09-09 09:55:00+05:30 |          8.80 |         7.30 |          10.80 | 2026-09-09 15:05:00+05:30 |        10.80 | TARGET         |   3,804.63 | CLOSED   |
| 2026-09-10 | 0931_SHORT | HAL26SEP4950PE        |     150.00 |     450.00 | 2026-09-10 09:35:00+05:30 |         95.15 |        78.50 |         116.60 | 2026-09-10 15:15:00+05:30 |       109.05 | TIME_EXIT_1515 |   6,094.27 | CLOSED   |
| 2026-09-11 | 0926_SHORT | DELHIVERY26SEP440PE   |   2,075.00 |   6,225.00 | 2026-09-11 09:30:00+05:30 |          8.40 |         6.95 |          10.30 | 2026-09-11 09:40:00+05:30 |        10.30 | TARGET         |  11,633.61 | CLOSED   |

## Files and replay

- `options_trades.csv`: every original order, exact contract/quantity/entry/SL/target/exit/costs/status.
- `options_5min_bar_audit.csv`: each monitored candle with protective levels and execution decisions.
- `option_mapping_and_coverage.csv`, `missing_option_fetch_plan.csv`: mapping evidence and missing history.
- `options_daily.csv`, `options_monthly.csv`, `setup_sl_target_results.csv`: daily, monthly and setup results.
- `stop_target_grid.csv`, `selected_option_profile.json`, `comparison_summary.csv`: parameter study and baseline rules.
- `challenger_options_trades.csv`, `challenger_5min_bar_audit.csv`, `train_selected_challenger_profile.json`: the training-selected experiment.
- `premium_cash_events.csv`, `execution_sensitivity.csv`, `manifest.json`: accounting, stresses and source hashes.

Replay the hashed inputs: `python -B fno_v13_v10_g_options_backtest.py --frozen-input "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\.codex_tmp\g_options\frozen_replay\frozen_input" --output-dir "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\.codex_tmp\g_options\frozen_replay_replay"`.

No live strategy, task scheduler or brokerage order configuration was changed.
