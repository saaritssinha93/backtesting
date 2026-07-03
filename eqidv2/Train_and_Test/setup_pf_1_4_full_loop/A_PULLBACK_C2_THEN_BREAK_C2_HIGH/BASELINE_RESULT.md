# Baseline Result - A_PULLBACK_C2_THEN_BREAK_C2_HIGH

## Current Rules
- setup name: A_PULLBACK_C2_THEN_BREAK_C2_HIGH
- side: LONG
- config source: raw scanner in avwap_5min_ID_v2_backtesting.py / all_setups_catalog.py; not active in FINAL_SETUP_CONF.
- structural idea: After a 2-bar down-pullback, hold VWAP and break prior high.
- current entry trigger: long_struct, close above VWAP, close above previous bar high, previous close below previous-2 close, vol_ratio >= 1.4, regime != BEAR.
- current indicator rules: VWAP hold, vol_ratio >= 1.4, regime filter; ATR participates through common range/liquidity prep and exit simulation.
- current non-indicator rules: bullish candle structure via long_struct, break above previous high, pullback condition on prior close versus previous-2 close.
- current pre-momentum rules: none.
- current filters: none beyond raw detection/common v11 candidate scanning.
- current guards: default repo entry window/dedupe only; no setup-specific guard.
- current SL/target: 0.70/0.90.
- current exit logic: fixed SL/target/EOD resolved on 1-minute OHLC by repo exit resolver.
- current time windows: repo default 09:30..14:30 candidate entry behavior after scan.
- current portfolio limits: repo evaluator default max_positions=20, daily_loss_rs=0.

## Exact Sessions
- FIT: 2026-03-02..2026-04-23 (34)
- VAL: 2026-04-24..2026-05-29 (24)
- TRAIN: 2026-03-02..2026-05-29 (58)
- TEST: 2026-06-01..2026-07-02 (19)

## Baseline Metrics
- FIT: trades=1553, wins=522, losses=1031, win_rate=33.61%, PF=0.377, net=Rs -537,333, avg_win=Rs 623, avg_loss=Rs -837, SL/TGT/EOD=863/471/219, top_trade/day/symbol=0.0021/None/None
- VAL: trades=1093, wins=313, losses=780, win_rate=28.64%, PF=0.2901, net=Rs -459,023, avg_win=Rs 599, avg_loss=Rs -829, SL/TGT/EOD=632/268/193, top_trade/day/symbol=0.0036/None/None
- Full TRAIN: trades=2646, wins=835, losses=1811, win_rate=31.56%, PF=0.3398, net=Rs -996,356, avg_win=Rs 614, avg_loss=Rs -833, SL/TGT/EOD=1495/739/412, top_trade/day/symbol=0.0013/None/None
- TEST: trades=742, wins=202, losses=540, win_rate=27.22%, PF=0.2768, net=Rs -317,072, avg_win=Rs 601, avg_loss=Rs -812, SL/TGT/EOD=422/174/146, top_trade/day/symbol=0.0055/None/None

## Initial Diagnosis
- TRAIN PF is below the target band; needs structural filtering or exit improvement.
- TEST PF does not clear 1.40; baseline is not acceptable.
