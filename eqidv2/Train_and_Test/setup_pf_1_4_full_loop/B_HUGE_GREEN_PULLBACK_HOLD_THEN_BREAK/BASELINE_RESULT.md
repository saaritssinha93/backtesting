# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** default exits 0.70/1.25 (no conf entry — raw detection baseline)
- **Baseline exit:** SL 0.7% / Tgt 1.25%
- **Baseline mask_terms:** `[]`
- **Baseline pre_momentum_terms:** `[]`
- **Baseline entry_guards:** `{}`
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); statutory NSE costs; entry = next 1-min open after the 5-min signal + 15 bps/leg slippage.

## Sessions (exact)

- **FIT** 2026-03-02..2026-04-27 (23 sessions — first 60% of TRAIN)
- **VAL** 2026-04-28..2026-05-29 (15 sessions — last 40% of TRAIN)
- **TRAIN** 2026-03-02..2026-05-29 (38 sessions)
- **TEST** 2026-06-01..2026-06-30 (13 sessions): 2026-06-01, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-09, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-18, 2026-06-19, 2026-06-23, 2026-06-29, 2026-06-30

## Baseline FIT metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 349 |
| net PF | 0.544 |
| net PnL | Rs-88,219 |
| win rate | 35.5% |
| wins / losses | 124 / 225 |
| avg win / avg loss | Rs848 / Rs-859 |
| avgW/avgL ratio | 0.99 |
| gross profit / loss | Rs105,139 / Rs193,357 |
| max drawdown | Rs-91,654 |
| SL / TGT / EOD exits | 201 / 91 / 57 |
| target-fill rate | 26.1% |
| trades/day | 15.17 |
| days / symbols | 23 / 278 |
| top-trade gross share | 0.012 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-16: Rs2,748 |
| top symbol | LUMAXTECH: Rs2,386 |

## Baseline VAL metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 201 |
| net PF | 0.415 |
| net PnL | Rs-72,983 |
| win rate | 28.9% |
| wins / losses | 58 / 143 |
| avg win / avg loss | Rs891 / Rs-872 |
| avgW/avgL ratio | 1.02 |
| gross profit / loss | Rs51,705 / Rs124,688 |
| max drawdown | Rs-72,292 |
| SL / TGT / EOD exits | 130 / 49 / 22 |
| target-fill rate | 24.4% |
| trades/day | 13.4 |
| days / symbols | 15 / 188 |
| top-trade gross share | 0.02 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9998 |
| top day | 2026-05-14: Rs1,232 |
| top symbol | FLUOROCHEM: Rs1,067 |

## Baseline TRAIN metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 550 |
| net PF | 0.493 |
| net PnL | Rs-161,202 |
| win rate | 33.1% |
| wins / losses | 182 / 368 |
| avg win / avg loss | Rs862 / Rs-864 |
| avgW/avgL ratio | 1.0 |
| gross profit / loss | Rs156,843 / Rs318,045 |
| max drawdown | Rs-160,273 |
| SL / TGT / EOD exits | 331 / 140 / 79 |
| target-fill rate | 25.5% |
| trades/day | 14.47 |
| days / symbols | 38 / 406 |
| top-trade gross share | 0.008 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-16: Rs2,748 |
| top symbol | LUMAXTECH: Rs2,386 |

## Baseline TEST metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 164 |
| net PF | 0.355 |
| net PnL | Rs-63,249 |
| win rate | 26.8% |
| wins / losses | 44 / 120 |
| avg win / avg loss | Rs793 / Rs-818 |
| avgW/avgL ratio | 0.97 |
| gross profit / loss | Rs34,881 / Rs98,129 |
| max drawdown | Rs-62,317 |
| SL / TGT / EOD exits | 93 / 31 / 40 |
| target-fill rate | 18.9% |
| trades/day | 12.62 |
| days / symbols | 13 / 150 |
| top-trade gross share | 0.029 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9999 |
| top day | 2026-06-04: Rs1,014 |
| top symbol | SHRIRAMPPS: Rs1,016 |

## Initial diagnosis

- Baseline TRAIN PF 0.493 (n=550) / TEST PF 0.355 (n=164) vs goal TRAIN [1.30,1.80] / TEST >1.40.
- Baseline FIT PF 0.544 vs VAL PF 0.415 — stable halves.
- Exit mix TRAIN SL/TGT/EOD = 331/140/79; avgW/avgL = Rs862/Rs-864.
- See FAILURE_ANALYSIS.md for loser classification.