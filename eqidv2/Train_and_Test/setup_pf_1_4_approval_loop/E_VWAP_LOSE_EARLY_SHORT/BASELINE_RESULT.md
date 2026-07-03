# E_VWAP_LOSE_EARLY_SHORT (SHORT) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.7% / Tgt 1.0%
- **Baseline mask_terms:** [vol_ratio>=1.8, vol_ratio<=3.2]
- **Baseline pre_momentum_terms:** [] (none)
- **Baseline entry_guards:** {'min_slot': '09:45'}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 0 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-09 (14 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-09
- **TEST**  2026-06-10..2026-06-16 (5 sessions): 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-15, 2026-06-16

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 29 |
| net PF | 0.51 |
| net PnL | Rs-7,450 |
| win rate | 37.9% |
| wins / losses | 11 / 18 |
| avg win / avg loss | Rs704 / Rs-844 |
| gross profit / loss | Rs7,745 / Rs15,195 |
| max drawdown | Rs-8,530 |
| SL / TGT / EOD exits | 16 / 10 / 3 |
| trades/day | 2.07 |
| days / symbols | 14 / 28 |
| top-trade gross share | 0.099 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9667 |
| top day | 2026-05-21: Rs2,253 |
| top symbol | BLUESTARCO: Rs767 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 15 |
| net PF | 0.332 |
| net PnL | Rs-4,807 |
| win rate | 26.7% |
| wins / losses | 4 / 11 |
| avg win / avg loss | Rs598 / Rs-654 |
| gross profit / loss | Rs2,392 / Rs7,199 |
| max drawdown | Rs-5,574 |
| SL / TGT / EOD exits | 7 / 3 / 5 |
| trades/day | 3.75 |
| days / symbols | 4 / 15 |
| top-trade gross share | 0.321 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-10: Rs-165 |
| top symbol | VIKRAMSOLR: Rs767 |

## Initial diagnosis

- Baseline TRAIN PF 0.51 / TEST PF 0.332 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 11 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.