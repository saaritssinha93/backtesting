# BEST_LONG_SETUP_RECOMMENDATION

## Verdict: REJECT FOR PROMOTION; WATCH ONLY AS RESEARCH

**DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL**

- best setup name: `FAST_MOMENTUM_LONG_LONG_VOLUME_EXPANSION_BREAKOUT`
- family: LONG_VOLUME_EXPANSION_BREAKOUT
- rule id: LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5
- candidate config path: `Train_and_Test/long_setup_discovery_from_raw_data/candidates/FAST_MOMENTUM_LONG_LONG_VOLUME_EXPANSION_BREAKOUT_best_research_config.json`
- train trades file: `Train_and_Test/long_setup_discovery_from_raw_data/results/best_train_trades.csv`
- test trades file: `Train_and_Test/long_setup_discovery_from_raw_data/results/best_test_trades.csv`
- reason/verdict detail: TRAIN near-miss PF < 0.8; TRAIN PF < 1.05; TEST PF < 1.40; TRAIN win rate < 52%; TEST win rate < 52%

## Exact Entry Logic
- Relative-volume expansion breaks prior 5-bar high.
- indicator values: vol_ratio >= 2, RSI >= 48, ADX >= 12
- non-indicator rules: close above prior 5-bar high; not overextended from VWAP
- pre-momentum filter: Volume rising into the breakout and candle closes in top 35% of its range.
- guards: {"guard_id": "g_top3_slot", "min_slot": 2, "max_slot": 60, "top_n_per_slot": 3, "max_per_symbol_day": 1, "cooldown_after_sl_bars": 0}

## Exact Exit Logic
- SL 0.75% / target 1% / time exit after 9 5-minute bars.
- breakeven_after_pct: None
- trailing_after_pct: None; trailing_gap_pct: None
- Intrabar: chronological 1-minute OHLC after next 1-minute open; same 1-minute target/SL touch is SL-first.

## Metrics Net Of Costs
- FIT: trades 1011, PF 0.2721, net Rs -375624.53, WR 24.23%
- VAL: trades 2008, PF 0.2329, net Rs -833833.17, WR 22.41%
- TRAIN: trades 3019, PF 0.2455, net Rs -1209457.71, WR 23.02%
- TEST: trades 1443, PF 0.2424, net Rs -586237.06, WR 22.52%

## Stability
- TRAIN day concentration: 9.99; symbol concentration: 9.99; trades/day 100.63
- TEST day concentration: 9.99; symbol concentration: 9.99; trades/day 144.3
- 5-minute conflict count TRAIN/TEST: 8 / 0
- same-1-minute tie count TRAIN/TEST: 1 / 0

## Why It May Work
- It requires immediate pressure/structure before entry and a short holding window, matching the tight +0.75% target.
- It uses 1-minute path order instead of optimistic 5-minute OHLC assumptions.

## Why It May Fail
- A 0.75% target leaves little room for statutory costs plus 15 bps/leg slippage.
- The setup can degrade quickly if volume/slot quality changes, so paper-watch validation is required before any promotion.
- If TEST PF is below target or dominated by one day/symbol, it must remain rejected.

## Candidate Config Block
```json
{
  "setup_name": "FAST_MOMENTUM_LONG_LONG_VOLUME_EXPANSION_BREAKOUT",
  "version": "candidate_001",
  "side": "LONG",
  "source": "raw_5m_signals_with_1m_intrabar_exit",
  "family": "LONG_VOLUME_EXPANSION_BREAKOUT",
  "rule_id": "LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5",
  "entry_logic": "Relative-volume expansion breaks prior 5-bar high.",
  "indicator_values": [
    "vol_ratio >= 2, RSI >= 48, ADX >= 12"
  ],
  "non_indicator_rules": [
    "close above prior 5-bar high",
    "not overextended from VWAP"
  ],
  "pre_momentum_filter": "Volume rising into the breakout and candle closes in top 35% of its range.",
  "guards": {
    "guard_id": "g_top3_slot",
    "min_slot": 2,
    "max_slot": 60,
    "top_n_per_slot": 3,
    "max_per_symbol_day": 1,
    "cooldown_after_sl_bars": 0
  },
  "exit_logic": {
    "exit_id": "sl0.75_tgt1_tb9",
    "sl_pct": 0.75,
    "target_pct": 1.0,
    "time_bars": 9,
    "breakeven_after_pct": null,
    "trailing_after_pct": null,
    "trailing_gap_pct": null
  },
  "intrabar_resolution": "Use chronological 1-minute OHLC after next 1-minute open. If target and SL hit in the same 1-minute bar, assume SL first.",
  "cost_model": {
    "notional_rs": 100000.0,
    "slippage_bps_per_leg": 15.0,
    "statutory_costs": "nse_intraday_costs.CostConfig 2026-06"
  },
  "metrics": {
    "fit": {
      "trades": 1011,
      "wins": 245,
      "losses": 766,
      "win_rate_pct": 24.23,
      "gross_profit": 140386.76,
      "gross_loss": -516011.29,
      "net_pnl": -375624.53,
      "net_pf": 0.2721,
      "avg_win": 573.01,
      "avg_loss": -673.64,
      "expectancy": -371.54,
      "max_drawdown": -377249.35,
      "avg_holding_min": 30.99,
      "sl_cnt": 345,
      "target_cnt": 158,
      "time_exit_cnt": 508,
      "eod_exit_cnt": 0,
      "breakeven_cnt": 0,
      "trail_cnt": 0,
      "five_min_conflict_cnt": 4,
      "same_1m_tie_cnt": 1,
      "trades_per_day": 67.4,
      "top_trade_gross_profit_share": 0.005,
      "top_day_net_share": 9.99,
      "top_symbol_net_share": 9.99,
      "daywise": [],
      "symbolwise": [],
      "timewise": []
    },
    "val": {
      "trades": 2008,
      "wins": 450,
      "losses": 1558,
      "win_rate_pct": 22.41,
      "gross_profit": 253101.26,
      "gross_loss": -1086934.44,
      "net_pnl": -833833.17,
      "net_pf": 0.2329,
      "avg_win": 562.45,
      "avg_loss": -697.65,
      "expectancy": -415.26,
      "max_drawdown": -834019.17,
      "avg_holding_min": 31.11,
      "sl_cnt": 721,
      "target_cnt": 288,
      "time_exit_cnt": 999,
      "eod_exit_cnt": 0,
      "breakeven_cnt": 0,
      "trail_cnt": 0,
      "five_min_conflict_cnt": 4,
      "same_1m_tie_cnt": 0,
      "trades_per_day": 133.87,
      "top_trade_gross_profit_share": 0.003,
      "top_day_net_share": 9.99,
      "top_symbol_net_share": 9.99,
      "daywise": [],
      "symbolwise": [],
      "timewise": []
    },
    "train": {
      "trades": 3019,
      "wins": 695,
      "losses": 2324,
      "win_rate_pct": 23.02,
      "gross_profit": 393488.02,
      "gross_loss": -1602945.72,
      "net_pnl": -1209457.71,
      "net_pf": 0.2455,
      "avg_win": 566.17,
      "avg_loss": -689.74,
      "expectancy": -400.62,
      "max_drawdown": -1211845.3,
      "avg_holding_min": 31.07,
      "sl_cnt": 1066,
      "target_cnt": 446,
      "time_exit_cnt": 1507,
      "eod_exit_cnt": 0,
      "breakeven_cnt": 0,
      "trail_cnt": 0,
      "five_min_conflict_cnt": 8,
      "same_1m_tie_cnt": 1,
      "trades_per_day": 100.63,
      "top_trade_gross_profit_share": 0.002,
      "top_day_net_share": 9.99,
      "top_symbol_net_share": 9.99,
      "daywise": [
        {
          "date": "2026-04-30",
          "trades": 23,
          "net_pnl": -2019.1,
          "pf": 0.733
        },
        {
          "date": "2026-05-04",
          "trades": 39,
          "net_pnl": -20026.24,
          "pf": 0.199
        },
        {
          "date": "2026-05-05",
          "trades": 26,
          "net_pnl": -14325.83,
          "pf": 0.137
        },
        {
          "date": "2026-05-06",
          "trades": 39,
          "net_pnl": -11835.52,
          "pf": 0.349
        },
        {
          "date": "2026-05-07",
          "trades": 49,
          "net_pnl": -15956.94,
          "pf": 0.34
        },
        {
          "date": "2026-05-08",
          "trades": 41,
          "net_pnl": -17596.75,
          "pf": 0.261
        },
        {
          "date": "2026-05-11",
          "trades": 29,
          "net_pnl": -8515.75,
          "pf": 0.393
        },
        {
          "date": "2026-05-12",
          "trades": 10,
          "net_pnl": -5777.89,
          "pf": 0.142
        },
        {
          "date": "2026-05-13",
          "trades": 87,
          "net_pnl": -33598.38,
          "pf": 0.231
        },
        {
          "date": "2026-05-14",
          "trades": 107,
          "net_pnl": -43223.54,
          "pf": 0.227
        },
        {
          "date": "2026-05-15",
          "trades": 89,
          "net_pnl": -41174.39,
          "pf": 0.197
        },
        {
          "date": "2026-05-18",
          "trades": 95,
          "net_pnl": -21669.52,
          "pf": 0.408
        },
        {
          "date": "2026-05-19",
          "trades": 119,
          "net_pnl": -38330.88,
          "pf": 0.31
        },
        {
          "date": "2026-05-20",
          "trades": 130,
          "net_pnl": -48104.97,
          "pf": 0.278
        },
        {
          "date": "2026-05-21",
          "trades": 128,
          "net_pnl": -53468.85,
          "pf": 0.24
        },
        {
          "date": "2026-05-22",
          "trades": 150,
          "net_pnl": -60386.6,
          "pf": 0.258
        },
        {
          "date": "2026-05-25",
          "trades": 147,
          "net_pnl": -55436.77,
          "pf": 0.25
        },
        {
          "date": "2026-05-26",
          "trades": 147,
          "net_pnl": -58179.91,
          "pf": 0.252
        },
        {
          "date": "2026-05-27",
          "trades": 148,
          "net_pnl": -67118.13,
          "pf": 0.179
        },
        {
          "date": "2026-05-29",
          "trades": 124,
          "net_pnl": -51052.94,
          "pf": 0.243
        },
        {
          "date": "2026-06-01",
          "trades": 88,
          "net_pnl": -46869.23,
          "pf": 0.133
        },
        {
          "date": "2026-06-02",
          "trades": 116,
          "net_pnl": -28194.17,
          "pf": 0.427
        },
        {
          "date": "2026-06-03",
          "trades": 131,
          "net_pnl": -44542.5,
          "pf": 0.31
        },
        {
          "date": "2026-06-04",
          "trades": 140,
          "net_pnl": -61162.82,
          "pf": 0.223
        },
        {
          "date": "2026-06-05",
          "trades": 132,
          "net_pnl": -70457.75,
          "pf": 0.112
        },
        {
          "date": "2026-06-08",
          "trades": 123,
          "net_pnl": -58749.17,
          "pf": 0.197
        },
        {
          "date": "2026-06-09",
          "trades": 149,
          "net_pnl": -52099.76,
          "pf": 0.246
        },
        {
          "date": "2026-06-10",
          "trades": 147,
          "net_pnl": -61912.62,
          "pf": 0.261
        },
        {
          "date": "2026-06-11",
          "trades": 117,
          "net_pnl": -55157.14,
          "pf": 0.197
        },
        {
          "date": "2026-06-12",
          "trades": 149,
          "net_pnl": -62513.67,
          "pf": 0.256
        }
      ],
      "symbolwise": [
        {
          "symbol": "FEDFINA",
          "trades": 4,
          "net_pnl": 1883.75,
          "pf": Infinity
        },
        {
          "symbol": "RANEHOLDIN",
          "trades": 3,
          "net_pnl": 1867.41,
          "pf": Infinity
        },
        {
          "symbol": "RICOAUTO",
          "trades": 2,
          "net_pnl": 1530.5,
          "pf": Infinity
        },
        {
          "symbol": "RHIM",
          "trades": 2,
          "net_pnl": 1530.33,
          "pf": Infinity
        },
        {
          "symbol": "NEPHROPLUS",
          "trades": 2,
          "net_pnl": 1526.8,
          "pf": Infinity
        },
        {
          "symbol": "EXPLEOSOL",
          "trades": 2,
          "net_pnl": 1524.51,
          "pf": Infinity
        },
        {
          "symbol": "VTL",
          "trades": 2,
          "net_pnl": 1524.3,
          "pf": Infinity
        },
        {
          "symbol": "PAYTM",
          "trades": 2,
          "net_pnl": 1519.12,
          "pf": Infinity
        },
        {
          "symbol": "POCL",
          "trades": 2,
          "net_pnl": 1518.49,
          "pf": Infinity
        },
        {
          "symbol": "INDOTHAI",
          "trades": 3,
          "net_pnl": 1462.89,
          "pf": 23.387
        },
        {
          "symbol": "MANINFRA",
          "trades": 7,
          "net_pnl": 1385.68,
          "pf": 1.828
        },
        {
          "symbol": "AVANTEL",
          "trades": 5,
          "net_pnl": 1372.73,
          "pf": 2.4
        },
        {
          "symbol": "GMRP&UI",
          "trades": 3,
          "net_pnl": 1346.48,
          "pf": 8.323
        },
        {
          "symbol": "MAITHANALL",
          "trades": 4,
          "net_pnl": 1308.11,
          "pf": 2.336
        },
        {
          "symbol": "STARHEALTH",
          "trades": 5,
          "net_pnl": 1296.6,
          "pf": 2.307
        },
        {
          "symbol": "EMIL",
          "trades": 3,
          "net_pnl": 1245.65,
          "pf": 5.389
        },
        {
          "symbol": "MANKIND",
          "trades": 3,
          "net_pnl": 1227.36,
          "pf": Infinity
        },
        {
          "symbol": "WINDLAS",
          "trades": 7,
          "net_pnl": 1197.8,
          "pf": 1.861
        },
        {
          "symbol": "ATALREAL",
          "trades": 4,
          "net_pnl": 1166.86,
          "pf": 3.807
        },
        {
          "symbol": "ANUP",
          "trades": 5,
          "net_pnl": 1134.28,
          "pf": 3.007
        }
      ],
      "timewise": [
        {
          "hour": "09",
          "trades": 141,
          "net_pnl": -55934.57,
          "pf": 0.322
        },
        {
          "hour": "10",
          "trades": 598,
          "net_pnl": -237937.16,
          "pf": 0.287
        },
        {
          "hour": "11",
          "trades": 660,
          "net_pnl": -275220.13,
          "pf": 0.222
        },
        {
          "hour": "12",
          "trades": 700,
          "net_pnl": -289239.85,
          "pf": 0.223
        },
        {
          "hour": "13",
          "trades": 634,
          "net_pnl": -241715.58,
          "pf": 0.235
        },
        {
          "hour": "14",
          "trades": 286,
          "net_pnl": -109410.42,
          "pf": 0.245
        }
      ]
    },
    "test": {
      "trades": 1443,
      "wins": 325,
      "losses": 1118,
      "win_rate_pct": 22.52,
      "gross_profit": 187544.6,
      "gross_loss": -773781.66,
      "net_pnl": -586237.06,
      "net_pf": 0.2424,
      "avg_win": 577.06,
      "avg_loss": -692.11,
      "expectancy": -406.26,
      "max_drawdown": -586036.86,
      "avg_holding_min": 30.05,
      "sl_cnt": 534,
      "target_cnt": 216,
      "time_exit_cnt": 693,
      "eod_exit_cnt": 0,
      "breakeven_cnt": 0,
      "trail_cnt": 0,
      "five_min_conflict_cnt": 0,
      "same_1m_tie_cnt": 0,
      "trades_per_day": 144.3,
      "top_trade_gross_profit_share": 0.004,
      "top_day_net_share": 9.99,
      "top_symbol_net_share": 9.99,
      "daywise": [
        {
          "date": "2026-06-15",
          "trades": 139,
          "net_pnl": -74021.96,
          "pf": 0.139
        },
        {
          "date": "2026-06-16",
          "trades": 137,
          "net_pnl": -42234.2,
          "pf": 0.368
        },
        {
          "date": "2026-06-17",
          "trades": 150,
          "net_pnl": -68388.71,
          "pf": 0.185
        },
        {
          "date": "2026-06-18",
          "trades": 161,
          "net_pnl": -51555.94,
          "pf": 0.348
        },
        {
          "date": "2026-06-19",
          "trades": 131,
          "net_pnl": -52613.14,
          "pf": 0.263
        },
        {
          "date": "2026-06-22",
          "trades": 133,
          "net_pnl": -43945.28,
          "pf": 0.299
        },
        {
          "date": "2026-06-23",
          "trades": 133,
          "net_pnl": -62916.79,
          "pf": 0.187
        },
        {
          "date": "2026-06-24",
          "trades": 156,
          "net_pnl": -61573.41,
          "pf": 0.235
        },
        {
          "date": "2026-06-25",
          "trades": 150,
          "net_pnl": -55825.45,
          "pf": 0.271
        },
        {
          "date": "2026-06-29",
          "trades": 153,
          "net_pnl": -73162.18,
          "pf": 0.183
        }
      ],
      "symbolwise": [
        {
          "symbol": "PDSL",
          "trades": 4,
          "net_pnl": 2173.91,
          "pf": 19.377
        },
        {
          "symbol": "ACE",
          "trades": 3,
          "net_pnl": 1553.84,
          "pf": Infinity
        },
        {
          "symbol": "NCLIND",
          "trades": 2,
          "net_pnl": 1530.23,
          "pf": Infinity
        },
        {
          "symbol": "ELLEN",
          "trades": 2,
          "net_pnl": 1530.02,
          "pf": Infinity
        },
        {
          "symbol": "KKCL",
          "trades": 2,
          "net_pnl": 1529.38,
          "pf": Infinity
        },
        {
          "symbol": "HIKAL",
          "trades": 2,
          "net_pnl": 1528.84,
          "pf": Infinity
        },
        {
          "symbol": "M&MFIN",
          "trades": 2,
          "net_pnl": 1527.63,
          "pf": Infinity
        },
        {
          "symbol": "MANGLMCEM",
          "trades": 2,
          "net_pnl": 1521.3,
          "pf": Infinity
        },
        {
          "symbol": "ANANDRATHI",
          "trades": 2,
          "net_pnl": 1519.51,
          "pf": Infinity
        },
        {
          "symbol": "ORIENTHOT",
          "trades": 3,
          "net_pnl": 1278.43,
          "pf": 6.097
        },
        {
          "symbol": "GOKEX",
          "trades": 2,
          "net_pnl": 1238.74,
          "pf": Infinity
        },
        {
          "symbol": "KSCL",
          "trades": 3,
          "net_pnl": 1112.21,
          "pf": 3.686
        },
        {
          "symbol": "JINDALSAW",
          "trades": 3,
          "net_pnl": 941.77,
          "pf": 5.875
        },
        {
          "symbol": "ARVIND",
          "trades": 2,
          "net_pnl": 919.42,
          "pf": Infinity
        },
        {
          "symbol": "EASEMYTRIP",
          "trades": 1,
          "net_pnl": 766.41,
          "pf": Infinity
        },
        {
          "symbol": "ACI",
          "trades": 3,
          "net_pnl": 765.97,
          "pf": 2.003
        },
        {
          "symbol": "BHARATCOAL",
          "trades": 1,
          "net_pnl": 765.51,
          "pf": Infinity
        },
        {
          "symbol": "RUSHIL",
          "trades": 1,
          "net_pnl": 765.48,
          "pf": Infinity
        },
        {
          "symbol": "IREDA",
          "trades": 1,
          "net_pnl": 765.41,
          "pf": Infinity
        },
        {
          "symbol": "ESAFSFB",
          "trades": 1,
          "net_pnl": 765.41,
          "pf": Infinity
        }
      ],
      "timewise": [
        {
          "hour": "09",
          "trades": 60,
          "net_pnl": -28579.2,
          "pf": 0.233
        },
        {
          "hour": "10",
          "trades": 295,
          "net_pnl": -125715.84,
          "pf": 0.264
        },
        {
          "hour": "11",
          "trades": 327,
          "net_pnl": -145775.41,
          "pf": 0.216
        },
        {
          "hour": "12",
          "trades": 318,
          "net_pnl": -121677.75,
          "pf": 0.25
        },
        {
          "hour": "13",
          "trades": 306,
          "net_pnl": -115286.89,
          "pf": 0.228
        },
        {
          "hour": "14",
          "trades": 137,
          "net_pnl": -49201.97,
          "pf": 0.278
        }
      ]
    },
    "reject_reasons": [
      "TRAIN near-miss PF < 0.8",
      "TRAIN PF < 1.05",
      "TEST PF < 1.40",
      "TRAIN win rate < 52%",
      "TEST win rate < 52%"
    ]
  },
  "final_config_block_requires_user_approval": {
    "WARNING": "DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL",
    "example_only": {
      "status": "WATCH_OR_PAPER_ONLY",
      "side": "LONG",
      "sl_pct": 0.75,
      "target_pct": 1.0,
      "time_exit_bars": 9,
      "rule_id": "LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5",
      "guards": {
        "guard_id": "g_top3_slot",
        "min_slot": 2,
        "max_slot": 60,
        "top_n_per_slot": 3,
        "max_per_symbol_day": 1,
        "cooldown_after_sl_bars": 0
      }
    }
  }
}
```

## Final Config Block That Would Need Approval
```python
# DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL
'FAST_MOMENTUM_LONG_LONG_VOLUME_EXPANSION_BREAKOUT': {
    'status': 'WATCH_OR_PAPER_ONLY',
    'side': 'LONG',
    'sl_pct': 0.75,
    'target_pct': 1.0,
    'time_exit_bars': 9,
    'rule_id': 'LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5',
    'guard': {'guard_id': 'g_top3_slot', 'min_slot': 2, 'max_slot': 60, 'top_n_per_slot': 3, 'max_per_symbol_day': 1, 'cooldown_after_sl_bars': 0},
}
```