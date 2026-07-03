# B_HUGE_RED_FAILED_BOUNCE (SHORT) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** FINAL_SETUP_CONF (active)
- **Baseline exit:** SL 0.9% / Tgt 1.25%
- **Baseline mask_terms:** `[]`
- **Baseline pre_momentum_terms:** `[['pre3_close_pos', '<=', 0.581797], ['sig5_rsi_dir', '<=', 64.104659], ['pre5_mom_r', '<=', 0.284145]]`
- **Baseline entry_guards:** `{}`
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); statutory NSE costs; entry = next 1-min open after the 5-min signal + 15 bps/leg slippage.

## Sessions (exact)

- **FIT** 2026-03-02..2026-04-27 (32 sessions — first 60% of TRAIN)
- **VAL** 2026-04-28..2026-05-29 (21 sessions — last 40% of TRAIN)
- **TRAIN** 2026-03-02..2026-05-29 (53 sessions)
- **TEST** 2026-06-01..2026-06-30 (20 sessions): 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-29, 2026-06-30

## Baseline FIT metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 22 |
| net PF | 0.848 |
| net PnL | Rs-1,301 |
| win rate | 50.0% |
| wins / losses | 11 / 11 |
| avg win / avg loss | Rs660 / Rs-778 |
| avgW/avgL ratio | 0.85 |
| gross profit / loss | Rs7,259 / Rs8,560 |
| max drawdown | Rs-3,993 |
| SL / TGT / EOD exits | 6 / 6 / 10 |
| target-fill rate | 27.3% |
| trades/day | 2.2 |
| days / symbols | 10 / 22 |
| top-trade gross share | 0.14 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.6214 |
| top day | 2026-04-09: Rs1,949 |
| top symbol | POLYMED: Rs1,018 |

## Baseline VAL metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 26 |
| net PF | 0.621 |
| net PnL | Rs-4,499 |
| win rate | 42.3% |
| wins / losses | 11 / 15 |
| avg win / avg loss | Rs671 / Rs-792 |
| avgW/avgL ratio | 0.85 |
| gross profit / loss | Rs7,383 / Rs11,882 |
| max drawdown | Rs-7,326 |
| SL / TGT / EOD exits | 8 / 5 / 13 |
| target-fill rate | 19.2% |
| trades/day | 2.36 |
| days / symbols | 11 / 25 |
| top-trade gross share | 0.138 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.8506 |
| top day | 2026-04-28: Rs1,601 |
| top symbol | SWANCORP: Rs1,413 |

## Baseline TRAIN metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 48 |
| net PF | 0.716 |
| net PnL | Rs-5,800 |
| win rate | 45.8% |
| wins / losses | 22 / 26 |
| avg win / avg loss | Rs666 / Rs-786 |
| avgW/avgL ratio | 0.85 |
| gross profit / loss | Rs14,642 / Rs20,442 |
| max drawdown | Rs-8,899 |
| SL / TGT / EOD exits | 14 / 11 / 23 |
| target-fill rate | 22.9% |
| trades/day | 2.29 |
| days / symbols | 21 / 47 |
| top-trade gross share | 0.07 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.8313 |
| top day | 2026-04-09: Rs1,949 |
| top symbol | SWANCORP: Rs1,413 |

## Baseline TEST metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 41 |
| net PF | 0.72 |
| net PnL | Rs-4,573 |
| win rate | 34.1% |
| wins / losses | 14 / 27 |
| avg win / avg loss | Rs841 / Rs-605 |
| avgW/avgL ratio | 1.39 |
| gross profit / loss | Rs11,768 / Rs16,340 |
| max drawdown | Rs-6,342 |
| SL / TGT / EOD exits | 11 / 11 / 19 |
| target-fill rate | 26.8% |
| trades/day | 2.28 |
| days / symbols | 18 / 41 |
| top-trade gross share | 0.086 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.8738 |
| top day | 2026-06-10: Rs1,012 |
| top symbol | JNKINDIA: Rs1,018 |

## Initial diagnosis

- Baseline TRAIN PF 0.716 (n=48) / TEST PF 0.72 (n=41) vs goal TRAIN [1.30,1.80] / TEST >1.40.
- Baseline FIT PF 0.848 vs VAL PF 0.621 — stable halves.
- Exit mix TRAIN SL/TGT/EOD = 14/11/23; avgW/avgL = Rs666/Rs-786.
- See FAILURE_ANALYSIS.md for loser classification.