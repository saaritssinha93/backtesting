# L_BB_SQUEEZE_LONG (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.7% / Tgt 1.25%
- **Baseline mask_terms:** [] (none)
- **Baseline pre_momentum_terms:** [] (none)
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-12-31) had only 0 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-02-01..2026-05-11 (59 sessions) = FIT 2026-02-01..2026-03-13 + VAL 2026-03-16..2026-05-11
- **TEST**  2026-05-12..2026-05-18 (5 sessions): 2026-05-12, 2026-05-13, 2026-05-14, 2026-05-15, 2026-05-18

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 374 |
| net PF | 0.417 |
| net PnL | Rs-137,777 |
| win rate | 28.1% |
| wins / losses | 105 / 269 |
| avg win / avg loss | Rs939 / Rs-879 |
| gross profit / loss | Rs98,635 / Rs236,412 |
| max drawdown | Rs-137,946 |
| SL / TGT / EOD exits | 246 / 94 / 34 |
| trades/day | 6.34 |
| days / symbols | 59 / 296 |
| top-trade gross share | 0.01 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs7,504 |
| top symbol | GICRE: Rs2,030 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 14 |
| net PF | 1.124 |
| net PnL | Rs708 |
| win rate | 50.0% |
| wins / losses | 7 / 7 |
| avg win / avg loss | Rs916 / Rs-815 |
| gross profit / loss | Rs6,412 / Rs5,704 |
| max drawdown | Rs-2,442 |
| SL / TGT / EOD exits | 6 / 6 / 2 |
| trades/day | 2.8 |
| days / symbols | 5 / 13 |
| top-trade gross share | 0.159 |
| top-day net share | 2.672 |
| top-symbol net share | 1.435 |
| day-block p | 0.4295 |
| top day | 2026-05-15: Rs1,893 |
| top symbol | WEBELSOLAR: Rs1,016 |

## Initial diagnosis

- Baseline TRAIN PF 0.417 / TEST PF 1.124 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 7 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.