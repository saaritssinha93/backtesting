# A_PULLBACK_C2_THEN_BREAK_C2_LOW (SHORT) - BASELINE_RESULT

Generated 2026-07-02.

## Current Rules

- Logic: after a 2-bar up-pullback in a non-bull regime, price loses VWAP and breaks the prior bar low on volume.
- Entry trigger: 5-minute signal, next 1-minute open entry.
- Detection: close<open, close_loc<=0.40, close<VWAP, close<prev_bar_low, prev_close>prev2_close, vol_ratio>=1.4, regime!=BULL.
- Current filters: quality_score>=123.7606
- Current pre-momentum: sig5_adx_calc>=21.4683
- Current guards: {}
- Current SL/target: 1.2 / 1.5
- Exit logic: 1-minute SL / target / EOD resolve to 15:20 IST, net of statutory NSE intraday costs plus slippage.
- Config source: root `final_setup_conf.py`, active `FINAL_SETUP_CONF` block.

## Sessions

- FIT: 2026-03-02..2026-04-27 (32 setup-candidate sessions)
- VAL: 2026-04-28..2026-05-29 (21 setup-candidate sessions)
- TRAIN: 2026-03-02..2026-05-29 (53 setup-candidate sessions)
- TEST: 2026-06-01..2026-06-24 (17 setup-candidate sessions)

## Baseline FIT Metrics

| metric | value |
|---|---|
| trades | 119 |
| wins | 35 |
| losses | 84 |
| win rate | 29.4% |
| gross profit | Rs35,429 |
| gross loss | Rs95,494 |
| net PnL | Rs-60,065 |
| Profit Factor | 0.371 |
| average win | Rs1,012 |
| average loss | Rs-1,137 |
| avg win / avg loss ratio | 0.89 |
| max drawdown | Rs-62,071 |
| SL / target / EOD exits | 59 / 23 / 37 |
| trades per day | 4.96 |
| top trade gross-profit share | 0.036 |
| top day net share | 9.99 |
| top symbol net share | 9.99 |
| day-block p | 0.9999 |
| top day | 2026-03-05: Rs2,512 |
| top symbol | WEL: Rs1,268 |

## Baseline VAL Metrics

| metric | value |
|---|---|
| trades | 119 |
| wins | 52 |
| losses | 67 |
| win rate | 43.7% |
| gross profit | Rs54,947 |
| gross loss | Rs72,537 |
| net PnL | Rs-17,589 |
| Profit Factor | 0.758 |
| average win | Rs1,057 |
| average loss | Rs-1,083 |
| avg win / avg loss ratio | 0.976 |
| max drawdown | Rs-28,872 |
| SL / target / EOD exits | 44 / 39 / 36 |
| trades per day | 5.95 |
| top trade gross-profit share | 0.023 |
| top day net share | 9.99 |
| top symbol net share | 9.99 |
| day-block p | 0.855 |
| top day | 2026-05-29: Rs5,939 |
| top symbol | FLAIR: Rs2,377 |

## Baseline TRAIN Metrics

| metric | value |
|---|---|
| trades | 238 |
| wins | 87 |
| losses | 151 |
| win rate | 36.6% |
| gross profit | Rs90,377 |
| gross loss | Rs168,031 |
| net PnL | Rs-77,654 |
| Profit Factor | 0.538 |
| average win | Rs1,039 |
| average loss | Rs-1,113 |
| avg win / avg loss ratio | 0.934 |
| max drawdown | Rs-89,518 |
| SL / target / EOD exits | 103 / 62 / 73 |
| trades per day | 5.41 |
| top trade gross-profit share | 0.014 |
| top day net share | 9.99 |
| top symbol net share | 9.99 |
| day-block p | 0.9986 |
| top day | 2026-05-29: Rs5,939 |
| top symbol | FLAIR: Rs2,377 |

## Baseline TEST Metrics

| metric | value |
|---|---|
| trades | 64 |
| wins | 32 |
| losses | 32 |
| win rate | 50.0% |
| gross profit | Rs29,829 |
| gross loss | Rs33,272 |
| net PnL | Rs-3,443 |
| Profit Factor | 0.897 |
| average win | Rs932 |
| average loss | Rs-1,040 |
| avg win / avg loss ratio | 0.896 |
| max drawdown | Rs-17,488 |
| SL / target / EOD exits | 19 / 19 / 26 |
| trades per day | 4.27 |
| top trade gross-profit share | 0.042 |
| top day net share | 9.99 |
| top symbol net share | 9.99 |
| day-block p | 0.6012 |
| top day | 2026-06-01: Rs11,096 |
| top symbol | NEPHROPLUS: Rs1,268 |

## Initial Diagnosis

- Baseline TRAIN PF 0.538 with 238 trades.
- Baseline TEST PF 0.897 with 64 trades.
- Optimization continues only from FIT/VAL evidence; TEST is used for final validation of train-side candidates.