# B_AVWAP_RECLAIM_REVERSAL (LONG) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.7% / Tgt 1.5%
- **Baseline mask_terms:** `[['vwap_dist_atr', '<=', 1.0]]`
- **Baseline pre_momentum_terms:** `[]`
- **Baseline entry_guards:** `{}`
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); statutory NSE costs; entry = next 1-min open after the 5-min signal + 15 bps/leg slippage.

## Sessions (exact)

- **FIT** 2026-03-04..2026-04-23 (31 sessions — first 60% of TRAIN)
- **VAL** 2026-04-27..2026-05-29 (21 sessions — last 40% of TRAIN)
- **TRAIN** 2026-03-04..2026-05-29 (52 sessions)
- **TEST** 2026-06-01..2026-07-01 (22 sessions): 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19, 2026-06-22, 2026-06-23, 2026-06-24, 2026-06-25, 2026-06-29, 2026-06-30, 2026-07-01

## Baseline FIT metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 1008 |
| net PF | 0.399 |
| net PnL | Rs-362,847 |
| win rate | 26.3% |
| wins / losses | 265 / 743 |
| avg win / avg loss | Rs908 / Rs-812 |
| avgW/avgL ratio | 1.12 |
| gross profit / loss | Rs240,526 / Rs603,373 |
| max drawdown | Rs-361,915 |
| SL / TGT / EOD exits | 588 / 157 / 263 |
| target-fill rate | 15.6% |
| trades/day | 32.52 |
| days / symbols | 31 / 624 |
| top-trade gross share | 0.005 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-03-24: Rs11,063 |
| top symbol | GMRP&UI: Rs2,533 |

## Baseline VAL metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 763 |
| net PF | 0.297 |
| net PnL | Rs-331,541 |
| win rate | 21.8% |
| wins / losses | 166 / 597 |
| avg win / avg loss | Rs844 / Rs-790 |
| avgW/avgL ratio | 1.07 |
| gross profit / loss | Rs140,058 / Rs471,599 |
| max drawdown | Rs-330,610 |
| SL / TGT / EOD exits | 447 / 88 / 228 |
| target-fill rate | 11.5% |
| trades/day | 36.33 |
| days / symbols | 21 / 528 |
| top-trade gross share | 0.009 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-05-06: Rs1,413 |
| top symbol | AVANTEL: Rs2,532 |

## Baseline TRAIN metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 1771 |
| net PF | 0.354 |
| net PnL | Rs-694,388 |
| win rate | 24.3% |
| wins / losses | 431 / 1340 |
| avg win / avg loss | Rs883 / Rs-802 |
| avgW/avgL ratio | 1.1 |
| gross profit / loss | Rs380,583 / Rs1,074,972 |
| max drawdown | Rs-693,456 |
| SL / TGT / EOD exits | 1035 / 245 / 491 |
| target-fill rate | 13.8% |
| trades/day | 34.06 |
| days / symbols | 52 / 825 |
| top-trade gross share | 0.003 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-03-24: Rs11,063 |
| top symbol | SHADOWFAX: Rs3,000 |

## Baseline TEST metrics (@15 bps/leg, statutory costs)

| metric | value |
|---|---|
| trades | 730 |
| net PF | 0.334 |
| net PnL | Rs-298,849 |
| win rate | 24.1% |
| wins / losses | 176 / 554 |
| avg win / avg loss | Rs850 / Rs-809 |
| avgW/avgL ratio | 1.05 |
| gross profit / loss | Rs149,537 / Rs448,386 |
| max drawdown | Rs-304,605 |
| SL / TGT / EOD exits | 431 / 92 / 207 |
| target-fill rate | 12.6% |
| trades/day | 33.18 |
| days / symbols | 22 / 506 |
| top-trade gross share | 0.008 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-12: Rs17,611 |
| top symbol | HIRECT: Rs2,505 |

## Initial diagnosis

- Baseline TRAIN PF 0.354 (n=1771) / TEST PF 0.334 (n=730) vs goal TRAIN [1.30,1.80] / TEST >1.40.
- Baseline FIT PF 0.399 vs VAL PF 0.297 — stable halves.
- Exit mix TRAIN SL/TGT/EOD = 1035/245/491; avgW/avgL = Rs883/Rs-802.
- See FAILURE_ANALYSIS.md for loser classification.