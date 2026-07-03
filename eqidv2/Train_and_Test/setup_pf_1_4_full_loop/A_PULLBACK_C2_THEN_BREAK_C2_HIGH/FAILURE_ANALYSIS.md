# Failure Analysis - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

## Baseline TRAIN
- losing trade classifications: {'SL_hit': 1495, 'weak_volume_vs_setup_median': 888, 'high_volatility_risk': 458, 'overextended_above_vwap': 858, 'low_volatility_noise': 462, 'time_or_EOD_exit': 316, 'edge_time_window_issue': 196}
```json
{
  "worst_trade_date": [
    {
      "trade_date": "2026-05-15",
      "size": 72,
      "sum": -44816.4
    },
    {
      "trade_date": "2026-04-29",
      "size": 73,
      "sum": -39570.52
    },
    {
      "trade_date": "2026-05-13",
      "size": 66,
      "sum": -36201.97
    },
    {
      "trade_date": "2026-04-10",
      "size": 60,
      "sum": -35234.47
    },
    {
      "trade_date": "2026-04-23",
      "size": 63,
      "sum": -32865.21
    },
    {
      "trade_date": "2026-05-22",
      "size": 70,
      "sum": -32841.31
    },
    {
      "trade_date": "2026-03-25",
      "size": 59,
      "sum": -32603.48
    },
    {
      "trade_date": "2026-04-01",
      "size": 65,
      "sum": -32245.41
    },
    {
      "trade_date": "2026-04-20",
      "size": 70,
      "sum": -32205.21
    },
    {
      "trade_date": "2026-04-28",
      "size": 58,
      "sum": -31154.91
    }
  ],
  "best_trade_date": [
    {
      "trade_date": "2026-05-20",
      "size": 44,
      "sum": -9700.3
    },
    {
      "trade_date": "2026-04-22",
      "size": 50,
      "sum": -8547.27
    },
    {
      "trade_date": "2026-03-13",
      "size": 9,
      "sum": -8277.37
    },
    {
      "trade_date": "2026-05-18",
      "size": 44,
      "sum": -8195.07
    },
    {
      "trade_date": "2026-03-05",
      "size": 23,
      "sum": -7033.98
    },
    {
      "trade_date": "2026-03-17",
      "size": 40,
      "sum": -6460.38
    },
    {
      "trade_date": "2026-03-06",
      "size": 7,
      "sum": -1130.38
    },
    {
      "trade_date": "2026-03-19",
      "size": 1,
      "sum": 665.44
    },
    {
      "trade_date": "2026-04-02",
      "size": 64,
      "sum": 1476.9
    },
    {
      "trade_date": "2026-05-06",
      "size": 22,
      "sum": 2940.9
    }
  ],
  "worst_ticker": [
    {
      "ticker": "SOBHA",
      "size": 6,
      "sum": -5558.16
    },
    {
      "ticker": "ALKYLAMINE",
      "size": 6,
      "sum": -5550.71
    },
    {
      "ticker": "BBL",
      "size": 6,
      "sum": -5501.85
    },
    {
      "ticker": "DIVISLAB",
      "size": 6,
      "sum": -5041.75
    },
    {
      "ticker": "THOMASCOOK",
      "size": 7,
      "sum": -4927.14
    },
    {
      "ticker": "CSBBANK",
      "size": 7,
      "sum": -4919.6
    },
    {
      "ticker": "ASTRAMICRO",
      "size": 6,
      "sum": -4873.39
    },
    {
      "ticker": "STARHEALTH",
      "size": 7,
      "sum": -4687.64
    },
    {
      "ticker": "INFOBEAN",
      "size": 5,
      "sum": -4661.32
    },
    {
      "ticker": "SAPPHIRE",
      "size": 5,
      "sum": -4659.4
    }
  ],
  "best_ticker": [
    {
      "ticker": "TITAGARH",
      "size": 3,
      "sum": 1988.23
    },
    {
      "ticker": "PREMEXPLN",
      "size": 3,
      "sum": 1990.93
    },
    {
      "ticker": "SAATVIKGL",
      "size": 3,
      "sum": 1994.73
    },
    {
      "ticker": "KPIGREEN",
      "size": 3,
      "sum": 1994.98
    },
    {
      "ticker": "ACI",
      "size": 3,
      "sum": 1995.87
    },
    {
      "ticker": "PFOCUS",
      "size": 3,
      "sum": 1997.19
    },
    {
      "ticker": "PANACEABIO",
      "size": 3,
      "sum": 1997.28
    },
    {
      "ticker": "ASHOKLEY",
      "size": 3,
      "sum": 1999.16
    },
    {
      "ticker": "GANECOS",
      "size": 4,
      "sum": 2652.83
    },
    {
      "ticker": "VMART",
      "size": 4,
      "sum": 2659.71
    }
  ],
  "time_window": [
    {
      "hour": "10:00",
      "size": 16,
      "sum": -3718.26
    },
    {
      "hour": "11:00",
      "size": 969,
      "sum": -380375.04
    },
    {
      "hour": "12:00",
      "size": 766,
      "sum": -295461.63
    },
    {
      "hour": "13:00",
      "size": 555,
      "sum": -206080.35
    },
    {
      "hour": "14:00",
      "size": 340,
      "sum": -110720.62
    }
  ]
}
```

## Baseline TEST
- losing trade classifications: {'SL_hit': 422, 'high_volatility_risk': 128, 'weak_volume_vs_setup_median': 265, 'low_volatility_noise': 139, 'time_or_EOD_exit': 118, 'overextended_above_vwap': 172, 'edge_time_window_issue': 60}
```json
{
  "worst_trade_date": [
    {
      "trade_date": "2026-06-08",
      "size": 53,
      "sum": -40593.61
    },
    {
      "trade_date": "2026-07-01",
      "size": 69,
      "sum": -39292.39
    },
    {
      "trade_date": "2026-06-10",
      "size": 54,
      "sum": -36742.8
    },
    {
      "trade_date": "2026-06-25",
      "size": 65,
      "sum": -35960.26
    },
    {
      "trade_date": "2026-06-11",
      "size": 58,
      "sum": -24493.18
    },
    {
      "trade_date": "2026-06-30",
      "size": 60,
      "sum": -23658.17
    },
    {
      "trade_date": "2026-06-22",
      "size": 42,
      "sum": -23411.75
    },
    {
      "trade_date": "2026-06-16",
      "size": 35,
      "sum": -15914.73
    },
    {
      "trade_date": "2026-06-04",
      "size": 43,
      "sum": -15147.98
    },
    {
      "trade_date": "2026-06-09",
      "size": 60,
      "sum": -15045.16
    }
  ],
  "best_trade_date": [
    {
      "trade_date": "2026-06-04",
      "size": 43,
      "sum": -15147.98
    },
    {
      "trade_date": "2026-06-09",
      "size": 60,
      "sum": -15045.16
    },
    {
      "trade_date": "2026-06-15",
      "size": 27,
      "sum": -14802.76
    },
    {
      "trade_date": "2026-06-24",
      "size": 34,
      "sum": -11797.32
    },
    {
      "trade_date": "2026-06-05",
      "size": 6,
      "sum": -5565.49
    },
    {
      "trade_date": "2026-06-02",
      "size": 49,
      "sum": -5454.23
    },
    {
      "trade_date": "2026-06-29",
      "size": 13,
      "sum": -4621.16
    },
    {
      "trade_date": "2026-06-03",
      "size": 42,
      "sum": -4023.43
    },
    {
      "trade_date": "2026-06-01",
      "size": 2,
      "sum": -780.79
    },
    {
      "trade_date": "2026-06-12",
      "size": 30,
      "sum": 233.49
    }
  ],
  "worst_ticker": [
    {
      "ticker": "CEMPRO",
      "size": 4,
      "sum": -3704.21
    },
    {
      "ticker": "KERNEX",
      "size": 4,
      "sum": -3699.27
    },
    {
      "ticker": "SYRMA",
      "size": 5,
      "sum": -3055.16
    },
    {
      "ticker": "SIEMENS",
      "size": 4,
      "sum": -2905.91
    },
    {
      "ticker": "GKENERGY",
      "size": 3,
      "sum": -2796.5
    },
    {
      "ticker": "SWSOLAR",
      "size": 3,
      "sum": -2795.35
    },
    {
      "ticker": "SANGHVIMOV",
      "size": 3,
      "sum": -2792.58
    },
    {
      "ticker": "SGMART",
      "size": 3,
      "sum": -2791.62
    },
    {
      "ticker": "ATGL",
      "size": 3,
      "sum": -2785.72
    },
    {
      "ticker": "AUROPHARMA",
      "size": 3,
      "sum": -2778.36
    }
  ],
  "best_ticker": [
    {
      "ticker": "BERGEPAINT",
      "size": 3,
      "sum": 896.47
    },
    {
      "ticker": "AEGISLOG",
      "size": 2,
      "sum": 1323.18
    },
    {
      "ticker": "JSLL",
      "size": 2,
      "sum": 1328.04
    },
    {
      "ticker": "LICHSGFIN",
      "size": 2,
      "sum": 1328.47
    },
    {
      "ticker": "GABRIEL",
      "size": 2,
      "sum": 1329.22
    },
    {
      "ticker": "ADFFOODS",
      "size": 2,
      "sum": 1330.95
    },
    {
      "ticker": "JINDALSAW",
      "size": 2,
      "sum": 1331.72
    },
    {
      "ticker": "IGIL",
      "size": 2,
      "sum": 1331.94
    },
    {
      "ticker": "AJMERA",
      "size": 2,
      "sum": 1333.02
    },
    {
      "ticker": "PNGJL",
      "size": 3,
      "sum": 1994.65
    }
  ],
  "time_window": [
    {
      "hour": "10:00",
      "size": 10,
      "sum": -5602.12
    },
    {
      "hour": "11:00",
      "size": 265,
      "sum": -116706.0
    },
    {
      "hour": "12:00",
      "size": 187,
      "sum": -93151.23
    },
    {
      "hour": "13:00",
      "size": 192,
      "sum": -74795.45
    },
    {
      "hour": "14:00",
      "size": 88,
      "sum": -26816.92
    }
  ]
}
```

## Rejected Candidate Failure Classes
```json
{
  "train_net<=0; train_pf_outside_1.30_1.80; fit_pf<1.05; val_pf<1.05; fit_net<0; val_net<0": 196
}
```

## Notes
- SL/target behavior was tracked via SL/TARGET/EOD counts per iteration.
- Fake breakout/reversal risk was proxied by poor close location, weak body, overextended VWAP distance, and SL-hit clusters.
- Volume, volatility, and trend weakness were checked through vol_ratio, atr_pct, vwap_dist_atr, ADX/pre-momentum sweeps where available.
