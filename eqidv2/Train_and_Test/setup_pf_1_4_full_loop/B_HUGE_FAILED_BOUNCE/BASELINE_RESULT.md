# B_HUGE_FAILED_BOUNCE (SHORT) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** default exits 0.70/1.25 (no conf entry — raw detection baseline)
- **Baseline exit:** SL 0.7% / Tgt 1.25%
- **Baseline mask_terms:** `[]`
- **Baseline pre_momentum_terms:** `[]`
- **Baseline entry_guards:** `{}`
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); statutory NSE costs; entry = next 1-min open after the 5-min signal + 15 bps/leg slippage.

## Sessions (exact)

- **FIT** 2026-03-02..2026-04-24 (35 sessions — first 60% of TRAIN)
- **VAL** 2026-04-27..2026-05-29 (23 sessions — last 40% of TRAIN)
- **TRAIN** 2026-03-02..2026-05-29 (58 sessions)
- **TEST** 2026-06-01..2026-07-01 (22 sessions): 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-29, 2026-06-30, 2026-07-01

## Baseline FIT metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 972 |
| net PF | 0.344 |
| net PnL | Rs-379,145 |
| win rate | 27.3% |
| wins / losses | 265 / 707 |
| avg win / avg loss | Rs749 / Rs-817 |
| avgW/avgL ratio | 0.92 |
| gross profit / loss | Rs198,593 / Rs577,738 |
| max drawdown | Rs-380,419 |
| SL / TGT / EOD exits | 570 / 159 / 243 |
| target-fill rate | 16.4% |
| trades/day | 27.77 |
| days / symbols | 35 / 587 |
| top-trade gross share | 0.005 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-09: Rs4,123 |
| top symbol | GOCOLORS: Rs2,116 |

## Baseline VAL metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 656 |
| net PF | 0.422 |
| net PnL | Rs-203,928 |
| win rate | 31.1% |
| wins / losses | 204 / 452 |
| avg win / avg loss | Rs729 / Rs-780 |
| avgW/avgL ratio | 0.93 |
| gross profit / loss | Rs148,671 / Rs352,599 |
| max drawdown | Rs-203,777 |
| SL / TGT / EOD exits | 335 / 114 / 207 |
| target-fill rate | 17.4% |
| trades/day | 28.52 |
| days / symbols | 23 / 480 |
| top-trade gross share | 0.007 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-29: Rs3,174 |
| top symbol | EMMVEE: Rs2,033 |

## Baseline TRAIN metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 1628 |
| net PF | 0.373 |
| net PnL | Rs-583,073 |
| win rate | 28.8% |
| wins / losses | 469 / 1159 |
| avg win / avg loss | Rs740 / Rs-803 |
| avgW/avgL ratio | 0.92 |
| gross profit / loss | Rs347,264 / Rs930,337 |
| max drawdown | Rs-584,347 |
| SL / TGT / EOD exits | 905 / 273 / 450 |
| target-fill rate | 16.8% |
| trades/day | 28.07 |
| days / symbols | 58 / 792 |
| top-trade gross share | 0.003 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-09: Rs4,123 |
| top symbol | CCL: Rs3,027 |

## Baseline TEST metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 673 |
| net PF | 0.503 |
| net PnL | Rs-174,139 |
| win rate | 34.5% |
| wins / losses | 232 / 441 |
| avg win / avg loss | Rs760 / Rs-795 |
| avgW/avgL ratio | 0.96 |
| gross profit / loss | Rs176,421 / Rs350,560 |
| max drawdown | Rs-179,538 |
| SL / TGT / EOD exits | 344 / 146 / 183 |
| target-fill rate | 21.7% |
| trades/day | 30.59 |
| days / symbols | 22 / 492 |
| top-trade gross share | 0.006 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-10: Rs10,493 |
| top symbol | BAYERCROP: Rs2,361 |

## Initial diagnosis

- Baseline TRAIN PF 0.373 (n=1628) / TEST PF 0.503 (n=673) vs goal TRAIN [1.30,1.80] / TEST >1.40.
- Baseline FIT PF 0.344 vs VAL PF 0.422 — stable halves.
- Exit mix TRAIN SL/TGT/EOD = 905/273/450; avgW/avgL = Rs740/Rs-803.
- See FAILURE_ANALYSIS.md for loser classification.