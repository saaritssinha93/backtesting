# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 1.0% / Tgt 1.5%
- **Baseline mask_terms:** [regime!=BULL]
- **Baseline pre_momentum_terms:** [] (none)
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-11 (17 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-11
- **TEST**  2026-06-12..2026-06-24 (5 sessions): 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 162 |
| net PF | 0.573 |
| net PnL | Rs-39,189 |
| win rate | 36.4% |
| wins / losses | 59 / 103 |
| avg win / avg loss | Rs892 / Rs-892 |
| gross profit / loss | Rs52,640 / Rs91,830 |
| max drawdown | Rs-39,191 |
| SL / TGT / EOD exits | 55 / 37 / 70 |
| trades/day | 10.8 |
| days / symbols | 15 / 143 |
| top-trade gross share | 0.024 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.97 |
| top day | 2026-06-03: Rs7,255 |
| top symbol | ADANIPOWER: Rs1,267 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 61 |
| net PF | 1.381 |
| net PnL | Rs8,620 |
| win rate | 50.8% |
| wins / losses | 31 / 30 |
| avg win / avg loss | Rs1,009 / Rs-755 |
| gross profit / loss | Rs31,264 / Rs22,645 |
| max drawdown | Rs-10,309 |
| SL / TGT / EOD exits | 11 / 20 / 30 |
| trades/day | 12.2 |
| days / symbols | 5 / 60 |
| top-trade gross share | 0.04 |
| top-day net share | 1.288 |
| top-symbol net share | 0.147 |
| day-block p | 0.2597 |
| top day | 2026-06-12: Rs11,100 |
| top symbol | AEQUS: Rs1,266 |

## Initial diagnosis

- Baseline TRAIN PF 0.573 / TEST PF 1.381 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 18 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.