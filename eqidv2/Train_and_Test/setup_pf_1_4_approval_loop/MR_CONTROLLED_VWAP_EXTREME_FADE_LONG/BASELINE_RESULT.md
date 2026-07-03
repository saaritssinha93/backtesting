# MR_CONTROLLED_VWAP_EXTREME_FADE_LONG (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.7% / Tgt 1.25%
- **Baseline mask_terms:** [] (none)
- **Baseline pre_momentum_terms:** [] (none)
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-12-31) had only 0 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-02-01..2026-05-19 (43 sessions) = FIT 2026-02-01..2026-03-16 + VAL 2026-03-17..2026-05-19
- **TEST**  2026-05-21..2026-05-29 (5 sessions): 2026-05-21, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 94 |
| net PF | 0.401 |
| net PnL | Rs-27,231 |
| win rate | 30.9% |
| wins / losses | 29 / 65 |
| avg win / avg loss | Rs628 / Rs-699 |
| gross profit / loss | Rs18,222 / Rs45,453 |
| max drawdown | Rs-28,818 |
| SL / TGT / EOD exits | 37 / 11 / 46 |
| trades/day | 2.19 |
| days / symbols | 43 / 77 |
| top-trade gross share | 0.056 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9999 |
| top day | 2026-02-02: Rs2,116 |
| top symbol | MOTHERSON: Rs1,016 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 26 |
| net PF | 0.117 |
| net PnL | Rs-15,648 |
| win rate | 15.4% |
| wins / losses | 4 / 22 |
| avg win / avg loss | Rs517 / Rs-805 |
| gross profit / loss | Rs2,067 / Rs17,715 |
| max drawdown | Rs-14,716 |
| SL / TGT / EOD exits | 17 / 2 / 7 |
| trades/day | 5.2 |
| days / symbols | 5 / 26 |
| top-trade gross share | 0.492 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9623 |
| top day | 2026-05-27: Rs1,493 |
| top symbol | INOXWIND: Rs1,017 |

## Initial diagnosis

- Baseline TRAIN PF 0.401 / TEST PF 0.117 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 5 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.