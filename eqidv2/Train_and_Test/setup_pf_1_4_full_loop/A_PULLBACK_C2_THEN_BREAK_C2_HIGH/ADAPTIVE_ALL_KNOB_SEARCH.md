# Adaptive All-Knob Search - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

## Status
- Passing candidates requiring approval: 0
- Approximate candidates generated: 1200
- Exact candidates evaluated: 312
- TEST runs allowed by train-side gate: 300
- Output CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\adaptive_all_knob_iterations.csv`
- Approx CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\adaptive_all_knob_approx_candidates.csv`
- Passing JSON: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_HIGH\adaptive_all_knob_passing_candidates.json`

## Data Windows
- FIT: 2026-03-02..2026-04-23 (34 sessions)
- VAL: 2026-04-24..2026-05-29 (24 sessions)
- TRAIN: 2026-03-02..2026-05-29 (58 sessions)
- TEST: 2026-06-01..2026-07-02 (19 sessions)
- Pool rows after 1-minute attach: 6703

## Method
- Dynamic numeric feature discovery was used instead of the older hand-written uppercase indicator list.
- Result/leaky columns were excluded: resolved exits, PnL, outcomes, costs, v6/v7/v8 resolution fields, entry/exit timestamps and prices.
- Approximate discovery used the repo dedupe and cached 1-minute resolution, then exact validation used `setup_train_test.eval_family` and per-trade details.
- TEST was run only after controlled train-side behavior or a separately labeled hot-train overfit probe.
- Passing still requires TRAIN PF 1.30..1.80, TEST PF > 1.40, positive TRAIN/TEST net, minimum trades/days/symbols, domination checks, and controlled average loss.

## Dynamically Tested Numeric Columns
atr_pct, body_pct, close_loc, lower_wick_pct, market_abs_ret_pct, market_ret_pct, notional, quality_score, rs_pct, signal_close, signal_high, signal_low, signal_minute, signal_open, signal_range_pct, signal_volume, upper_wick_pct, vol_ratio, vwap_dist_atr, wick_skew_pct

## Numeric Feature Diagnostics
Columns with too few valid TRAIN values were not swept as honest train-time filters.
```json
[
  {
    "column": "atr_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5469,
    "reason": "selected"
  },
  {
    "column": "body_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 4235,
    "reason": "selected"
  },
  {
    "column": "close_loc",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 3450,
    "reason": "selected"
  },
  {
    "column": "lower_wick_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 3432,
    "reason": "selected"
  },
  {
    "column": "market_abs_ret_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 1185,
    "reason": "selected"
  },
  {
    "column": "market_ret_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 1200,
    "reason": "selected"
  },
  {
    "column": "notional",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 4637,
    "reason": "selected"
  },
  {
    "column": "quality_score",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5355,
    "reason": "selected"
  },
  {
    "column": "rs_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5469,
    "reason": "selected"
  },
  {
    "column": "signal_close",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5147,
    "reason": "selected"
  },
  {
    "column": "signal_high",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5035,
    "reason": "selected"
  },
  {
    "column": "signal_low",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5129,
    "reason": "selected"
  },
  {
    "column": "signal_minute",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 44,
    "reason": "selected"
  },
  {
    "column": "signal_open",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5165,
    "reason": "selected"
  },
  {
    "column": "signal_range_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5462,
    "reason": "selected"
  },
  {
    "column": "signal_volume",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5153,
    "reason": "selected"
  },
  {
    "column": "upper_wick_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 4075,
    "reason": "selected"
  },
  {
    "column": "vol_ratio",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5469,
    "reason": "selected"
  },
  {
    "column": "vwap_dist_atr",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 5469,
    "reason": "selected"
  },
  {
    "column": "wick_skew_pct",
    "selected": true,
    "valid_train_rows": 5469,
    "nunique": 4935,
    "reason": "selected"
  },
  {
    "column": "_basis",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "adx",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "bar_time_ist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "candidate_schema_version",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "capital_per_trade_rs",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "created_at_ist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "diagnostics_json",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "ema20_slope",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "entry_price_v6",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "entry_time_v6",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "leverage",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "lower_wick_price_pct",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "macd_hist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "macd_hist_delta",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "notional_exposure_rs",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "pnl",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "quantity",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "ranker_model",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "ranker_score",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "research_shadow_action",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "research_shadow_reason",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "research_shadow_status",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "research_shadow_version",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "rsi",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "rsi3max",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "scan_slot_ist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "score",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "signal_bar_time_ist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "signal_datetime",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "signal_entry_datetime_ist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "signal_time_ist",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "signal_time_v8",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "source_quality_score",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "source_setup",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "stock_ret",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "v11_exit_override_applied",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "v11_exit_rule_source",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "v11_selected_strategy_profile",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  },
  {
    "column": "v11_source_day",
    "selected": false,
    "valid_train_rows": 0,
    "nunique": 0,
    "reason": "all_non_numeric_or_nan"
  }
]
```

## Context Inventory
```json
{
  "exit_scan_count": 18,
  "kept_exits": [
    [
      0.7,
      0.9
    ],
    [
      0.85,
      1.8
    ],
    [
      0.85,
      2.0
    ],
    [
      0.9,
      2.0
    ],
    [
      1.0,
      1.5
    ],
    [
      1.0,
      2.0
    ],
    [
      1.1,
      2.0
    ],
    [
      1.1,
      2.5
    ],
    [
      1.2,
      2.0
    ],
    [
      1.2,
      2.5
    ],
    [
      1.4,
      2.5
    ]
  ],
  "guard_scan_count": 264,
  "selected_context_count": 66,
  "top_exit_rows": [
    {
      "guard": {},
      "sl": 1.2,
      "tgt": 2.0,
      "score": -1.6128955555555557,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4545,
        "net_pnl": -993469.01,
        "avg_loss": -934.4,
        "avg_win": 935.22,
        "win_rate": 31.23,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0021,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3321,
        "net_pnl": -921917.76,
        "avg_loss": -944.2,
        "avg_win": 886.86,
        "win_rate": 26.12,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0039,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.4017,
        "net_pnl": -1915386.77,
        "avg_loss": -938.6,
        "avg_win": 917.39,
        "win_rate": 29.13,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0014,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 1.1,
      "tgt": 2.0,
      "score": -1.6173155555555554,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4539,
        "net_pnl": -979456.8,
        "avg_loss": -914.13,
        "avg_win": 933.56,
        "win_rate": 30.77,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0022,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3303,
        "net_pnl": -912094.58,
        "avg_loss": -924.64,
        "avg_win": 889.12,
        "win_rate": 25.57,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0039,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.4006,
        "net_pnl": -1891551.38,
        "avg_loss": -918.64,
        "avg_win": 917.24,
        "win_rate": 28.63,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0014,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 1.4,
      "tgt": 2.5,
      "score": -1.6223155555555557,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4703,
        "net_pnl": -986083.65,
        "avg_loss": -960.13,
        "avg_win": 978.33,
        "win_rate": 31.58,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0026,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3227,
        "net_pnl": -971962.54,
        "avg_loss": -978.19,
        "avg_win": 904.39,
        "win_rate": 25.87,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0049,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.4061,
        "net_pnl": -1958046.19,
        "avg_loss": -967.91,
        "avg_win": 951.43,
        "win_rate": 29.23,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0017,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 1.0,
      "tgt": 2.0,
      "score": -1.6233805555555558,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4487,
        "net_pnl": -976693.13,
        "avg_loss": -891.57,
        "avg_win": 938.43,
        "win_rate": 29.89,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0022,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3292,
        "net_pnl": -894160.66,
        "avg_loss": -898.87,
        "avg_win": 884.81,
        "win_rate": 25.06,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.004,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3974,
        "net_pnl": -1870853.79,
        "avg_loss": -894.69,
        "avg_win": 918.63,
        "win_rate": 27.9,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0014,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.9,
      "tgt": 2.0,
      "score": -1.6253405555555553,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4356,
        "net_pnl": -991732.89,
        "avg_loss": -868.14,
        "avg_win": 944.91,
        "win_rate": 28.58,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0023,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3325,
        "net_pnl": -863370.94,
        "avg_loss": -865.81,
        "avg_win": 886.92,
        "win_rate": 24.51,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0041,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3919,
        "net_pnl": -1855103.83,
        "avg_loss": -867.15,
        "avg_win": 923.19,
        "win_rate": 26.91,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0015,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 1.2,
      "tgt": 2.5,
      "score": -1.6254005555555557,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4639,
        "net_pnl": -983281.4,
        "avg_loss": -932.99,
        "avg_win": 980.39,
        "win_rate": 30.63,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0027,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3232,
        "net_pnl": -945912.99,
        "avg_loss": -944.36,
        "avg_win": 905.3,
        "win_rate": 25.21,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.005,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.4031,
        "net_pnl": -1929194.39,
        "avg_loss": -937.88,
        "avg_win": 952.98,
        "win_rate": 28.4,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0017,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.85,
      "tgt": 2.0,
      "score": -1.6318605555555554,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4343,
        "net_pnl": -982897.78,
        "avg_loss": -851.31,
        "avg_win": 951.6,
        "win_rate": 27.98,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0023,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.33,
        "net_pnl": -855669.01,
        "avg_loss": -848.05,
        "avg_win": 891.12,
        "win_rate": 23.9,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0042,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3901,
        "net_pnl": -1838566.79,
        "avg_loss": -849.93,
        "avg_win": 929.0,
        "win_rate": 26.3,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0015,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 1.1,
      "tgt": 2.5,
      "score": -1.6330255555555557,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.461,
        "net_pnl": -974044.91,
        "avg_loss": -912.77,
        "avg_win": 975.68,
        "win_rate": 30.13,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0027,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3208,
        "net_pnl": -936070.15,
        "avg_loss": -924.36,
        "avg_win": 906.06,
        "win_rate": 24.66,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0051,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.4004,
        "net_pnl": -1910115.06,
        "avg_loss": -917.75,
        "avg_win": 950.37,
        "win_rate": 27.88,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0018,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.85,
      "tgt": 1.8,
      "score": -1.6467155555555557,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4347,
        "net_pnl": -976328.32,
        "avg_loss": -852.53,
        "avg_win": 929.32,
        "win_rate": 28.51,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0021,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3231,
        "net_pnl": -862268.58,
        "avg_loss": -849.27,
        "avg_win": 859.37,
        "win_rate": 24.2,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0038,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3874,
        "net_pnl": -1838596.9,
        "avg_loss": -851.14,
        "avg_win": 903.29,
        "win_rate": 26.74,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0013,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 1.0,
      "tgt": 1.5,
      "score": -1.6667005555555554,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4515,
        "net_pnl": -951707.34,
        "avg_loss": -894.38,
        "avg_win": 876.28,
        "win_rate": 31.55,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0016,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3088,
        "net_pnl": -909816.29,
        "avg_loss": -899.76,
        "avg_win": 787.86,
        "win_rate": 26.07,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0031,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.39,
        "net_pnl": -1861523.63,
        "avg_loss": -896.7,
        "avg_win": 843.92,
        "win_rate": 29.3,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0011,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.7,
      "tgt": 1.8,
      "score": -1.6854955555555557,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4059,
        "net_pnl": -988414.09,
        "avg_loss": -789.62,
        "avg_win": 928.89,
        "win_rate": 25.65,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0023,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3155,
        "net_pnl": -819077.95,
        "avg_loss": -780.05,
        "avg_win": 848.35,
        "win_rate": 22.49,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0041,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3681,
        "net_pnl": -1807492.05,
        "avg_loss": -785.58,
        "avg_win": 898.31,
        "win_rate": 24.35,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0015,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.7,
      "tgt": 2.5,
      "score": -1.6859755555555558,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.4121,
        "net_pnl": -990042.85,
        "avg_loss": -787.72,
        "avg_win": 997.27,
        "win_rate": 24.56,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0033,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.3129,
        "net_pnl": -831487.38,
        "avg_loss": -778.26,
        "avg_win": 893.19,
        "win_rate": 21.42,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.006,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3707,
        "net_pnl": -1821530.23,
        "avg_loss": -783.74,
        "avg_win": 957.87,
        "win_rate": 23.27,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0021,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.6,
      "tgt": 1.8,
      "score": -1.7462405555555556,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.3803,
        "net_pnl": -1001682.88,
        "avg_loss": -741.8,
        "avg_win": 938.47,
        "win_rate": 23.11,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0025,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.2972,
        "net_pnl": -812394.5,
        "avg_loss": -734.35,
        "avg_win": 848.1,
        "win_rate": 20.46,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0046,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3456,
        "net_pnl": -1814077.39,
        "avg_loss": -738.68,
        "avg_win": 903.94,
        "win_rate": 22.02,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0016,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.7,
      "tgt": 0.9,
      "score": -1.8087205555555559,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.3663,
        "net_pnl": -971495.75,
        "avg_loss": -805.66,
        "avg_win": 603.29,
        "win_rate": 32.85,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0015,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.2744,
        "net_pnl": -821115.49,
        "avg_loss": -792.51,
        "avg_win": 563.69,
        "win_rate": 27.84,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0021,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3273,
        "net_pnl": -1792611.24,
        "avg_loss": -800.02,
        "avg_win": 588.57,
        "win_rate": 30.79,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.001,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {},
      "sl": 0.5,
      "tgt": 2.5,
      "score": -1.8371905555555554,
      "FIT": {
        "trades": 2834,
        "profit_factor": 0.3374,
        "net_pnl": -1042920.43,
        "avg_loss": -681.03,
        "avg_win": 1015.19,
        "win_rate": 18.45,
        "n_days": 31,
        "n_symbols": 923,
        "top_trade_gross_profit_share": 0.0043,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 1979,
        "profit_factor": 0.2713,
        "net_pnl": -806649.97,
        "avg_loss": -673.37,
        "avg_win": 896.64,
        "win_rate": 16.93,
        "n_days": 21,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0075,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 4813,
        "profit_factor": 0.3101,
        "net_pnl": -1849570.41,
        "avg_loss": -677.85,
        "avg_win": 968.9,
        "win_rate": 17.83,
        "n_days": 52,
        "n_symbols": 1017,
        "top_trade_gross_profit_share": 0.0027,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    }
  ],
  "top_context_rows": [
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 0.85,
      "tgt": 1.8,
      "score": -1.3968555555555557,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4833,
        "net_pnl": -133635.43,
        "avg_loss": -983.44,
        "avg_win": 1190.56,
        "win_rate": 28.53,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.0125,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.4193,
        "net_pnl": -112372.45,
        "avg_loss": -925.86,
        "avg_win": 1014.15,
        "win_rate": 27.68,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0193,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4559,
        "net_pnl": -246007.87,
        "avg_loss": -957.94,
        "avg_win": 1114.27,
        "win_rate": 28.16,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0076,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 0.85,
      "tgt": 2.0,
      "score": -1.418915555555556,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4845,
        "net_pnl": -134477.92,
        "avg_loss": -980.71,
        "avg_win": 1239.12,
        "win_rate": 27.72,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.014,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.4089,
        "net_pnl": -115778.45,
        "avg_loss": -923.86,
        "avg_win": 1039.99,
        "win_rate": 26.64,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0221,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4521,
        "net_pnl": -250256.37,
        "avg_loss": -955.49,
        "avg_win": 1153.46,
        "win_rate": 27.25,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0086,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 1.1,
      "tgt": 2.5,
      "score": -1.4275705555555556,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4697,
        "net_pnl": -150215.96,
        "avg_loss": -1085.32,
        "avg_win": 1243.49,
        "win_rate": 29.08,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.017,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.4108,
        "net_pnl": -130153.91,
        "avg_loss": -1051.81,
        "avg_win": 1148.44,
        "win_rate": 27.34,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.025,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4439,
        "net_pnl": -280369.87,
        "avg_loss": -1070.38,
        "avg_win": 1203.12,
        "win_rate": 28.31,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0101,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 1.4,
      "tgt": 2.5,
      "score": -1.4310505555555557,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4858,
        "net_pnl": -153672.81,
        "avg_loss": -1190.78,
        "avg_win": 1241.14,
        "win_rate": 31.79,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.0156,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.4041,
        "net_pnl": -142186.68,
        "avg_loss": -1163.86,
        "avg_win": 1147.67,
        "win_rate": 29.07,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0235,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4495,
        "net_pnl": -295859.49,
        "avg_loss": -1178.68,
        "avg_win": 1202.08,
        "win_rate": 30.59,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0094,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 0.9,
      "tgt": 2.0,
      "score": -1.4342955555555559,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.492,
        "net_pnl": -133425.3,
        "avg_loss": -994.86,
        "avg_win": 1242.48,
        "win_rate": 28.26,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.0137,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.3996,
        "net_pnl": -120541.77,
        "avg_loss": -951.54,
        "avg_win": 1028.64,
        "win_rate": 26.99,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.022,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.452,
        "net_pnl": -253967.07,
        "avg_loss": -975.62,
        "avg_win": 1150.83,
        "win_rate": 27.7,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0084,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 1.1,
      "tgt": 2.0,
      "score": -1.440405555555556,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4819,
        "net_pnl": -143988.78,
        "avg_loss": -1089.77,
        "avg_win": 1184.98,
        "win_rate": 30.71,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.0132,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.4009,
        "net_pnl": -130247.24,
        "avg_loss": -1055.29,
        "avg_win": 1049.91,
        "win_rate": 28.72,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0203,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4463,
        "net_pnl": -274236.02,
        "avg_loss": -1074.36,
        "avg_win": 1127.78,
        "win_rate": 29.83,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.008,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 0.9,
      "tgt": 2.0,
      "score": -1.4455855555555557,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5236,
        "net_pnl": -239525.17,
        "avg_loss": -966.88,
        "avg_win": 1149.58,
        "win_rate": 30.57,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0067,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3858,
        "net_pnl": -255266.9,
        "avg_loss": -955.4,
        "avg_win": 1061.79,
        "win_rate": 25.77,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.011,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4612,
        "net_pnl": -494792.07,
        "avg_loss": -961.65,
        "avg_win": 1114.69,
        "win_rate": 28.46,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0042,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 1.2,
      "tgt": 2.5,
      "score": -1.4509405555555555,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4652,
        "net_pnl": -156228.43,
        "avg_loss": -1132.36,
        "avg_win": 1235.64,
        "win_rate": 29.89,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.0167,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.4021,
        "net_pnl": -138279.38,
        "avg_loss": -1106.56,
        "avg_win": 1162.39,
        "win_rate": 27.68,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0244,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4373,
        "net_pnl": -294507.81,
        "avg_loss": -1120.81,
        "avg_win": 1204.8,
        "win_rate": 28.92,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0099,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 0.85,
      "tgt": 2.0,
      "score": -1.4533705555555554,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5172,
        "net_pnl": -240035.57,
        "avg_loss": -945.16,
        "avg_win": 1153.01,
        "win_rate": 29.77,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0069,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3843,
        "net_pnl": -251723.48,
        "avg_loss": -929.14,
        "avg_win": 1076.01,
        "win_rate": 24.91,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.0112,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4572,
        "net_pnl": -491759.05,
        "avg_loss": -937.87,
        "avg_win": 1122.54,
        "win_rate": 27.64,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0043,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 0.85,
      "tgt": 1.8,
      "score": -1.4551405555555554,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5122,
        "net_pnl": -241306.69,
        "avg_loss": -947.64,
        "avg_win": 1116.13,
        "win_rate": 30.31,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0062,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3851,
        "net_pnl": -249443.92,
        "avg_loss": -932.53,
        "avg_win": 1034.48,
        "win_rate": 25.77,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.01,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4549,
        "net_pnl": -490750.6,
        "avg_loss": -940.77,
        "avg_win": 1083.51,
        "win_rate": 28.31,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0038,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 1.0,
      "tgt": 2.0,
      "score": -1.460980555555556,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4812,
        "net_pnl": -141063.87,
        "avg_loss": -1045.75,
        "avg_win": 1211.39,
        "win_rate": 29.35,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.0135,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.3917,
        "net_pnl": -128475.93,
        "avg_loss": -1010.63,
        "avg_win": 1034.33,
        "win_rate": 27.68,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0213,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.4421,
        "net_pnl": -269539.81,
        "avg_loss": -1030.1,
        "avg_win": 1136.05,
        "win_rate": 28.61,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0083,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 1.4,
      "tgt": 2.5,
      "score": -1.4633855555555555,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5682,
        "net_pnl": -236365.76,
        "avg_loss": -1131.0,
        "avg_win": 1173.73,
        "win_rate": 35.38,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0073,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3664,
        "net_pnl": -308752.37,
        "avg_loss": -1149.35,
        "avg_win": 1102.29,
        "win_rate": 27.65,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.0127,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4732,
        "net_pnl": -545118.14,
        "avg_loss": -1139.57,
        "avg_win": 1146.62,
        "win_rate": 31.99,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0046,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:30"
      },
      "sl": 0.9,
      "tgt": 2.0,
      "score": -1.4634755555555556,
      "FIT": {
        "trades": 1218,
        "profit_factor": 0.5335,
        "net_pnl": -369636.24,
        "avg_loss": -943.34,
        "avg_win": 1118.44,
        "win_rate": 31.03,
        "n_days": 29,
        "n_symbols": 679,
        "top_trade_gross_profit_share": 0.0042,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 879,
        "profit_factor": 0.3723,
        "net_pnl": -385247.06,
        "avg_loss": -931.29,
        "avg_win": 1038.52,
        "win_rate": 25.03,
        "n_days": 20,
        "n_symbols": 572,
        "top_trade_gross_profit_share": 0.0077,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 2097,
        "profit_factor": 0.4631,
        "net_pnl": -754883.3,
        "avg_loss": -938.04,
        "avg_win": 1089.04,
        "win_rate": 28.52,
        "n_days": 49,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0027,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "min_slot": "09:45",
        "max_slot": "12:30"
      },
      "sl": 0.9,
      "tgt": 2.0,
      "score": -1.4634755555555556,
      "FIT": {
        "trades": 1218,
        "profit_factor": 0.5335,
        "net_pnl": -369636.24,
        "avg_loss": -943.34,
        "avg_win": 1118.44,
        "win_rate": 31.03,
        "n_days": 29,
        "n_symbols": 679,
        "top_trade_gross_profit_share": 0.0042,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 879,
        "profit_factor": 0.3723,
        "net_pnl": -385247.06,
        "avg_loss": -931.29,
        "avg_win": 1038.52,
        "win_rate": 25.03,
        "n_days": 20,
        "n_symbols": 572,
        "top_trade_gross_profit_share": 0.0077,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 2097,
        "profit_factor": 0.4631,
        "net_pnl": -754883.3,
        "avg_loss": -938.04,
        "avg_win": 1089.04,
        "win_rate": 28.52,
        "n_days": 49,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0027,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 1.2,
      "tgt": 2.5,
      "score": -1.4658855555555554,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5312,
        "net_pnl": -256007.91,
        "avg_loss": -1087.78,
        "avg_win": 1174.32,
        "win_rate": 32.98,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0078,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3754,
        "net_pnl": -291427.94,
        "avg_loss": -1090.15,
        "avg_win": 1108.59,
        "win_rate": 26.96,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.0129,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4594,
        "net_pnl": -547435.86,
        "avg_loss": -1088.87,
        "avg_win": 1148.68,
        "win_rate": 30.34,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0049,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 1.2,
      "tgt": 2.0,
      "score": -1.4678155555555557,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5212,
        "net_pnl": -259172.34,
        "avg_loss": -1095.77,
        "avg_win": 1106.43,
        "win_rate": 34.05,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0063,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3776,
        "net_pnl": -286713.88,
        "avg_loss": -1094.25,
        "avg_win": 1054.33,
        "win_rate": 28.16,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.0102,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4552,
        "net_pnl": -545886.22,
        "avg_loss": -1095.07,
        "avg_win": 1085.96,
        "win_rate": 31.46,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0039,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "11:30"
      },
      "sl": 1.2,
      "tgt": 2.0,
      "score": -1.468605555555556,
      "FIT": {
        "trades": 368,
        "profit_factor": 0.4757,
        "net_pnl": -150201.34,
        "avg_loss": -1136.79,
        "avg_win": 1174.74,
        "win_rate": 31.52,
        "n_days": 26,
        "n_symbols": 303,
        "top_trade_gross_profit_share": 0.013,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 289,
        "profit_factor": 0.3907,
        "net_pnl": -138673.92,
        "avg_loss": -1110.15,
        "avg_win": 1058.41,
        "win_rate": 29.07,
        "n_days": 17,
        "n_symbols": 251,
        "top_trade_gross_profit_share": 0.0199,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 657,
        "profit_factor": 0.438,
        "net_pnl": -288875.26,
        "avg_loss": -1124.84,
        "avg_win": 1125.88,
        "win_rate": 30.44,
        "n_days": 43,
        "n_symbols": 479,
        "top_trade_gross_profit_share": 0.0078,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 1.1,
      "tgt": 2.5,
      "score": -1.4728105555555557,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.541,
        "net_pnl": -243047.7,
        "avg_loss": -1046.39,
        "avg_win": 1178.71,
        "win_rate": 32.44,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0079,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3697,
        "net_pnl": -288786.74,
        "avg_loss": -1053.27,
        "avg_win": 1121.77,
        "win_rate": 25.77,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.0134,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4615,
        "net_pnl": -531834.44,
        "avg_loss": -1049.57,
        "avg_win": 1156.89,
        "win_rate": 29.51,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.005,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:00"
      },
      "sl": 1.1,
      "tgt": 2.0,
      "score": -1.4735105555555559,
      "FIT": {
        "trades": 749,
        "profit_factor": 0.5315,
        "net_pnl": -245912.21,
        "avg_loss": -1054.06,
        "avg_win": 1111.58,
        "win_rate": 33.51,
        "n_days": 28,
        "n_symbols": 509,
        "top_trade_gross_profit_share": 0.0063,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 586,
        "profit_factor": 0.3722,
        "net_pnl": -284108.49,
        "avg_loss": -1057.4,
        "avg_win": 1066.19,
        "win_rate": 26.96,
        "n_days": 18,
        "n_symbols": 435,
        "top_trade_gross_profit_share": 0.0105,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 1335,
        "profit_factor": 0.4578,
        "net_pnl": -530020.7,
        "avg_loss": -1055.6,
        "avg_win": 1094.05,
        "win_rate": 30.64,
        "n_days": 46,
        "n_symbols": 714,
        "top_trade_gross_profit_share": 0.0039,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    },
    {
      "guard": {
        "max_slot": "12:30"
      },
      "sl": 1.2,
      "tgt": 2.0,
      "score": -1.4748705555555555,
      "FIT": {
        "trades": 1218,
        "profit_factor": 0.5455,
        "net_pnl": -379317.29,
        "avg_loss": -1045.86,
        "avg_win": 1084.0,
        "win_rate": 34.48,
        "n_days": 29,
        "n_symbols": 679,
        "top_trade_gross_profit_share": 0.0039,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "VAL": {
        "trades": 879,
        "profit_factor": 0.3646,
        "net_pnl": -428110.64,
        "avg_loss": -1049.52,
        "avg_win": 1036.64,
        "win_rate": 26.96,
        "n_days": 20,
        "n_symbols": 572,
        "top_trade_gross_profit_share": 0.0072,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      },
      "TRAIN": {
        "trades": 2097,
        "profit_factor": 0.4647,
        "net_pnl": -807427.93,
        "avg_loss": -1047.49,
        "avg_win": 1066.91,
        "win_rate": 31.33,
        "n_days": 49,
        "n_symbols": 852,
        "top_trade_gross_profit_share": 0.0025,
        "top_day_net_share": null,
        "top_symbol_net_share": null
      }
    }
  ]
}
```

## Term Inventory Summary
- Contexts with term generation: 66
- Numeric columns: 20
- Pre-momentum seed limit: 50

## Exact Train-Gate Counts
```json
{
  "controlled_train_gate": 113,
  "hot_train_probe_overfit_risk": 187,
  "train_n<20; fit_n<5; val_n<5; train_pf_outside_1.30_1.80; val_pf<1.05": 1,
  "train_n<20": 2,
  "train_n<20; val_n<5; train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; fit_net<0": 1,
  "train_n<20; fit_n<5; val_n<5; train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05": 4,
  "train_n<20; train_pf_outside_1.30_1.80; val_pf<1.05; val_net<0": 1,
  "fit_pf<1.05": 1,
  "fit_pf<1.05; fit_net<0": 2
}
```

## Exact Failure Counts
```json
{
  "TEST PF <= 1.40; TEST net <= 0; TEST PF <= 1.40": 14,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST PF <= 1.40": 23,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40": 7,
  "TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40": 20,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40": 119,
  "TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40": 65,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST trades < 5; TEST PF <= 1.40": 11,
  "TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40": 10,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST trades < 5; TEST domination: top_trade_share>0.35; TEST days < 3; TEST avg loss worse than Rs 1,250; TEST PF <= 1.40": 3,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40": 6,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST trades < 5; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40": 4,
  "TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; top_symbol_share>0.4; TEST PF <= 1.40": 2,
  "train_n<20; fit_n<5; val_n<5; train_pf_outside_1.30_1.80; val_pf<1.05": 1,
  "train_n<20": 2,
  "train_n<20; val_n<5; train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; fit_net<0": 1,
  "train_n<20; fit_n<5; val_n<5; train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05": 4,
  "train_n<20; train_pf_outside_1.30_1.80; val_pf<1.05; val_net<0": 1,
  "fit_pf<1.05": 1,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST days < 3; TEST PF <= 1.40": 1,
  "TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST days < 3; TEST PF <= 1.40": 2,
  "fit_pf<1.05; fit_net<0": 2,
  "TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST trades < 5; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40": 13
}
```

## Top Exact Candidates By TRAIN PF
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795._regime0.0: TRAIN n=1 PF=inf net=Rs 231; FIT PF=inf VAL PF=0.0 TEST PF=None; failure=train_n<20; fit_n<5; val_n<5; train_pf_outside_1.30_1.80; val_pf<1.05
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795.: TRAIN n=38 PF=2.0729 net=Rs 12,843; FIT PF=2.1957 VAL PF=1.9421 TEST PF=0.5613; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795._maxpos10: TRAIN n=38 PF=2.0729 net=Rs 12,843; FIT PF=2.1957 VAL PF=1.9421 TEST PF=0.5613; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795._maxpos15: TRAIN n=38 PF=2.0729 net=Rs 12,843; FIT PF=2.1957 VAL PF=1.9421 TEST PF=0.5613; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795._dloss3000: TRAIN n=38 PF=2.0729 net=Rs 12,843; FIT PF=2.1957 VAL PF=1.9421 TEST PF=0.5613; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795._dloss5000: TRAIN n=38 PF=2.0729 net=Rs 12,843; FIT PF=2.1957 VAL PF=1.9421 TEST PF=0.5613; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_atr_pct>=0.003349_mask_signal_minute>=795._dloss7500: TRAIN n=38 PF=2.0729 net=Rs 12,843; FIT PF=2.1957 VAL PF=1.9421 TEST PF=0.5613; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._premom_pre3_close_pos<=0_regime0.15: TRAIN n=28 PF=1.9784 net=Rs 7,318; FIT PF=1.9617 VAL PF=1.9828 TEST PF=0.633; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST days < 3; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_signal_minute>=795._premom_pre2_mom_r<=0.287: TRAIN n=84 PF=1.9746 net=Rs 22,787; FIT PF=2.0193 VAL PF=1.8817 TEST PF=0.5607; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_signal_minute>=795._premom_pre2_mom_r<=0.287_maxpos15: TRAIN n=84 PF=1.9746 net=Rs 22,787; FIT PF=2.0193 VAL PF=1.8817 TEST PF=0.5607; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_signal_minute>=795._premom_pre2_mom_r<=0.287_dloss3000: TRAIN n=84 PF=1.9746 net=Rs 22,787; FIT PF=2.0193 VAL PF=1.8817 TEST PF=0.5607; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_signal_minute>=795._premom_pre2_mom_r<=0.287_dloss5000: TRAIN n=84 PF=1.9746 net=Rs 22,787; FIT PF=2.0193 VAL PF=1.8817 TEST PF=0.5607; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_signal_minute>=795._premom_pre2_mom_r<=0.287_dloss7500: TRAIN n=84 PF=1.9746 net=Rs 22,787; FIT PF=2.0193 VAL PF=1.8817 TEST PF=0.5607; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=0.0_mask_signal_minute>=795._premom_pre2_mom_r<=0.287_maxpos10: TRAIN n=80 PF=1.8699 net=Rs 20,339; FIT PF=1.8642 VAL PF=1.8817 TEST PF=0.5607; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_13_30_mask_quality_score<=22.7_mask_atr_pct>=0.00303_mask_market_ret_pct<=-0._premom_pre5_body_sum_r<=: TRAIN n=42 PF=1.861 net=Rs 12,574; FIT PF=1.8461 VAL PF=1.871 TEST PF=0.2121; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_13_30_mask_quality_score<=22.7_mask_atr_pct>=0.00303_mask_market_ret_pct<=-0._premom_pre5_body_sum_r_maxpos10: TRAIN n=42 PF=1.861 net=Rs 12,574; FIT PF=1.8461 VAL PF=1.871 TEST PF=0.2121; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_13_30_mask_quality_score<=22.7_mask_atr_pct>=0.00303_mask_market_ret_pct<=-0._premom_pre5_body_sum_r_maxpos15: TRAIN n=42 PF=1.861 net=Rs 12,574; FIT PF=1.8461 VAL PF=1.871 TEST PF=0.2121; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_13_30_mask_quality_score<=22.7_mask_atr_pct>=0.00303_mask_market_ret_pct<=-0._premom_pre5_body_sum_r_dloss3000: TRAIN n=42 PF=1.861 net=Rs 12,574; FIT PF=1.8461 VAL PF=1.871 TEST PF=0.2121; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_13_30_mask_quality_score<=22.7_mask_atr_pct>=0.00303_mask_market_ret_pct<=-0._premom_pre5_body_sum_r_dloss5000: TRAIN n=42 PF=1.861 net=Rs 12,574; FIT PF=1.8461 VAL PF=1.871 TEST PF=0.2121; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_13_30_mask_quality_score<=22.7_mask_atr_pct>=0.00303_mask_market_ret_pct<=-0._premom_pre5_body_sum_r_dloss7500: TRAIN n=42 PF=1.861 net=Rs 12,574; FIT PF=1.8461 VAL PF=1.871 TEST PF=0.2121; failure=TRAIN PF outside 1.30..1.80; TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40

## Controlled TRAIN PF Candidates
- adaptive_sl0.85_t2.0_max_slot_12_30_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66_premom_pre5_range_r<=1.4: TRAIN n=25 PF=1.6311 net=Rs 6,811; TEST n=8 PF=0.8611 net=Rs -598; failure=TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_12_30_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66_premom_pre5_range_r<=1.5: TRAIN n=25 PF=1.6311 net=Rs 6,811; TEST n=8 PF=0.8611 net=Rs -598; failure=TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_12_30_min_slot_09_45_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66_premom_pre5_range_r<=1.4: TRAIN n=25 PF=1.6311 net=Rs 6,811; TEST n=8 PF=0.8611 net=Rs -598; failure=TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_12_30_min_slot_09_45_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66_premom_pre5_range_r<=1.5: TRAIN n=25 PF=1.6311 net=Rs 6,811; TEST n=8 PF=0.8611 net=Rs -598; failure=TEST PF <= 1.40; TEST net <= 0; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_12_30_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66: TRAIN n=27 PF=1.6284 net=Rs 7,462; TEST n=9 PF=0.6885 net=Rs -1,677; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.85_t2.0_max_slot_12_30_min_slot_09_45_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66: TRAIN n=27 PF=1.6284 net=Rs 7,462; TEST n=9 PF=0.6885 net=Rs -1,677; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._premom_sig5_rsi_dir<=72._regime0.15: TRAIN n=29 PF=1.7339 net=Rs 6,157; TEST n=19 PF=0.678 net=Rs -3,287; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST days < 3; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0_premom_sig5_rsi_dir<_regime0.15: TRAIN n=29 PF=1.7339 net=Rs 6,157; TEST n=19 PF=0.678 net=Rs -3,287; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST days < 3; TEST PF <= 1.40
- adaptive_sl0.9_t2.0_max_slot_12_30_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66: TRAIN n=27 PF=1.6897 net=Rs 7,893; TEST n=9 PF=0.6581 net=Rs -1,925; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl0.9_t2.0_max_slot_12_30_min_slot_09_45_mask_upper_wick_pct>=0.2_mask_vwap_dist_atr<=0.66: TRAIN n=27 PF=1.6897 net=Rs 7,893; TEST n=9 PF=0.6581 net=Rs -1,925; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST domination: top_trade_share>0.35; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795.: TRAIN n=94 PF=1.5503 net=Rs 17,509; TEST n=37 PF=0.6149 net=Rs -6,710; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0: TRAIN n=94 PF=1.5503 net=Rs 17,509; TEST n=37 PF=0.6149 net=Rs -6,710; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0: TRAIN n=94 PF=1.5503 net=Rs 17,509; TEST n=37 PF=0.6149 net=Rs -6,710; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.0_t2.0_max_slot_13_30_mask_signal_minute>=795._mask_regime!=BULL_mask_market_ret_pct<=-0.: TRAIN n=94 PF=1.5503 net=Rs 17,509; TEST n=37 PF=0.6149 net=Rs -6,710; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._premom_sig5_rsi_dir<=72._maxpos10: TRAIN n=73 PF=1.7272 net=Rs 16,373; TEST n=35 PF=0.6126 net=Rs -6,642; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0_premom_sig5_rsi_dir<_maxpos10: TRAIN n=73 PF=1.7272 net=Rs 16,373; TEST n=35 PF=0.6126 net=Rs -6,642; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795.: TRAIN n=94 PF=1.6898 net=Rs 20,483; TEST n=37 PF=0.6026 net=Rs -7,067; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0: TRAIN n=94 PF=1.6898 net=Rs 20,483; TEST n=37 PF=0.6026 net=Rs -7,067; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0: TRAIN n=94 PF=1.6898 net=Rs 20,483; TEST n=37 PF=0.6026 net=Rs -7,067; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40
- adaptive_sl1.2_t2.0_max_slot_13_30_mask_market_ret_pct<=-0._mask_signal_minute>=795._mask_market_ret_pct<=0.0_mask_market_ret_pct<=0.0: TRAIN n=94 PF=1.6898 net=Rs 20,483; TEST n=37 PF=0.6026 net=Rs -7,067; failure=TEST PF <= 1.40; TEST net <= 0; TRAIN domination: top_day_share>0.4; TEST PF <= 1.40

## Passing Candidates
No adaptive candidate passed all approval gates.
## Approval Note
No final setup config was edited by this script. Any candidate above is research-only until the user explicitly approves it.
