# A_MOD_BREAK_C1_LOW (SHORT) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** FINAL_SETUP_CONF (active)
- **Baseline exit:** SL 1.1% / Tgt 1.0%
- **Baseline mask_terms:** `[['vol_ratio', '>=', 1.955814]]`
- **Baseline pre_momentum_terms:** `[['pre5_mom_r', '>=', 0.425861], ['pre3_range_r', '<=', 0.202087]]`
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
| trades | 109 |
| net PF | 0.428 |
| net PnL | Rs-36,988 |
| win rate | 39.4% |
| wins / losses | 43 / 66 |
| avg win / avg loss | Rs642 / Rs-979 |
| avgW/avgL ratio | 0.66 |
| gross profit / loss | Rs27,624 / Rs64,612 |
| max drawdown | Rs-38,519 |
| SL / TGT / EOD exits | 40 / 34 / 35 |
| target-fill rate | 31.2% |
| trades/day | 3.89 |
| days / symbols | 28 / 101 |
| top-trade gross share | 0.028 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-09: Rs2,296 |
| top symbol | ORISSAMINE: Rs1,501 |

## Baseline VAL metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 55 |
| net PF | 0.842 |
| net PnL | Rs-3,906 |
| win rate | 56.4% |
| wins / losses | 31 / 24 |
| avg win / avg loss | Rs671 / Rs-1,029 |
| avgW/avgL ratio | 0.65 |
| gross profit / loss | Rs20,791 / Rs24,697 |
| max drawdown | Rs-13,649 |
| SL / TGT / EOD exits | 16 / 25 / 14 |
| target-fill rate | 45.5% |
| trades/day | 4.23 |
| days / symbols | 13 / 52 |
| top-trade gross share | 0.037 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.6658 |
| top day | 2026-05-11: Rs1,553 |
| top symbol | ADVAIT: Rs1,516 |

## Baseline TRAIN metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 164 |
| net PF | 0.542 |
| net PnL | Rs-40,894 |
| win rate | 45.1% |
| wins / losses | 74 / 90 |
| avg win / avg loss | Rs654 / Rs-992 |
| avgW/avgL ratio | 0.66 |
| gross profit / loss | Rs48,415 / Rs89,310 |
| max drawdown | Rs-49,396 |
| SL / TGT / EOD exits | 56 / 59 / 49 |
| target-fill rate | 36.0% |
| trades/day | 4.0 |
| days / symbols | 41 / 146 |
| top-trade gross share | 0.016 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9992 |
| top day | 2026-04-09: Rs2,296 |
| top symbol | ADVAIT: Rs1,516 |

## Baseline TEST metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 36 |
| net PF | 0.337 |
| net PnL | Rs-14,426 |
| win rate | 36.1% |
| wins / losses | 13 / 23 |
| avg win / avg loss | Rs564 / Rs-946 |
| avgW/avgL ratio | 0.6 |
| gross profit / loss | Rs7,326 / Rs21,752 |
| max drawdown | Rs-17,078 |
| SL / TGT / EOD exits | 13 / 8 / 15 |
| target-fill rate | 22.2% |
| trades/day | 2.25 |
| days / symbols | 16 / 36 |
| top-trade gross share | 0.105 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.994 |
| top day | 2026-06-01: Rs1,985 |
| top symbol | SONACOMS: Rs768 |

## Initial diagnosis

- Baseline TRAIN PF 0.542 (n=164) / TEST PF 0.337 (n=36) vs goal TRAIN [1.30,1.80] / TEST >1.40.
- Baseline FIT PF 0.428 vs VAL PF 0.842 — stable halves.
- Exit mix TRAIN SL/TGT/EOD = 56/59/49; avgW/avgL = Rs654/Rs-992.
- See FAILURE_ANALYSIS.md for loser classification.