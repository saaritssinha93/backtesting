# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 1.0% / Tgt 1.5%
- **Baseline mask_terms:** `[['regime', '!=', 'BULL']]`
- **Baseline pre_momentum_terms:** `[]`
- **Baseline entry_guards:** `{}`
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); statutory NSE costs; entry = next 1-min open after the 5-min signal + 15 bps/leg slippage.

## Sessions (exact)

- **FIT** 2026-03-04..2026-04-27 (31 sessions — first 60% of TRAIN)
- **VAL** 2026-04-28..2026-05-29 (20 sessions — last 40% of TRAIN)
- **TRAIN** 2026-03-04..2026-05-29 (51 sessions)
- **TEST** 2026-06-01..2026-07-01 (22 sessions): 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-29, 2026-06-30, 2026-07-01

## Baseline FIT metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 416 |
| net PF | 0.519 |
| net PnL | Rs-132,941 |
| win rate | 34.1% |
| wins / losses | 142 / 274 |
| avg win / avg loss | Rs1,009 / Rs-1,008 |
| avgW/avgL ratio | 1.0 |
| gross profit / loss | Rs143,250 / Rs276,192 |
| max drawdown | Rs-133,463 |
| SL / TGT / EOD exits | 190 / 103 / 123 |
| target-fill rate | 24.8% |
| trades/day | 15.41 |
| days / symbols | 27 / 345 |
| top-trade gross share | 0.011 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs10,090 |
| top symbol | FEDERALBNK: Rs2,530 |

## Baseline VAL metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 327 |
| net PF | 0.458 |
| net PnL | Rs-114,650 |
| win rate | 31.8% |
| wins / losses | 104 / 223 |
| avg win / avg loss | Rs932 / Rs-949 |
| avgW/avgL ratio | 0.98 |
| gross profit / loss | Rs96,942 / Rs211,593 |
| max drawdown | Rs-115,271 |
| SL / TGT / EOD exits | 136 / 67 / 124 |
| target-fill rate | 20.5% |
| trades/day | 17.21 |
| days / symbols | 19 / 275 |
| top-trade gross share | 0.013 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-05-06: Rs3,973 |
| top symbol | ACE: Rs2,095 |

## Baseline TRAIN metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 743 |
| net PF | 0.492 |
| net PnL | Rs-247,592 |
| win rate | 33.1% |
| wins / losses | 246 / 497 |
| avg win / avg loss | Rs976 / Rs-981 |
| avgW/avgL ratio | 0.99 |
| gross profit / loss | Rs240,192 / Rs487,784 |
| max drawdown | Rs-249,855 |
| SL / TGT / EOD exits | 326 / 170 / 247 |
| target-fill rate | 22.9% |
| trades/day | 16.15 |
| days / symbols | 46 / 514 |
| top-trade gross share | 0.007 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs10,090 |
| top symbol | DBREALTY: Rs2,532 |

## Baseline TEST metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 293 |
| net PF | 0.475 |
| net PnL | Rs-91,606 |
| win rate | 34.5% |
| wins / losses | 101 / 192 |
| avg win / avg loss | Rs820 / Rs-908 |
| avgW/avgL ratio | 0.9 |
| gross profit / loss | Rs82,791 / Rs174,397 |
| max drawdown | Rs-99,086 |
| SL / TGT / EOD exits | 113 / 53 / 127 |
| target-fill rate | 18.1% |
| trades/day | 14.65 |
| days / symbols | 20 / 254 |
| top-trade gross share | 0.015 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9998 |
| top day | 2026-06-03: Rs6,128 |
| top symbol | PWL: Rs2,531 |

## Initial diagnosis

- Baseline TRAIN PF 0.492 (n=743) / TEST PF 0.475 (n=293) vs goal TRAIN [1.30,1.80] / TEST >1.40.
- Baseline FIT PF 0.519 vs VAL PF 0.458 — stable halves.
- Exit mix TRAIN SL/TGT/EOD = 326/170/247; avgW/avgL = Rs976/Rs-981.
- See FAILURE_ANALYSIS.md for loser classification.