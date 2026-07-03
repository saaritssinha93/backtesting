# L_PRESSURE_BURST_VWAP (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.7% / Tgt 1.25%
- **Baseline mask_terms:** [quality_score<=25.0]
- **Baseline pre_momentum_terms:** [pre1_adx>=44.0]
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-11 (18 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-11
- **TEST**  2026-06-12..2026-06-24 (5 sessions): 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 64 |
| net PF | 0.359 |
| net PnL | Rs-21,221 |
| win rate | 25.0% |
| wins / losses | 16 / 48 |
| avg win / avg loss | Rs744 / Rs-690 |
| gross profit / loss | Rs11,910 / Rs33,131 |
| max drawdown | Rs-22,075 |
| SL / TGT / EOD exits | 30 / 10 / 24 |
| trades/day | 3.76 |
| days / symbols | 17 / 63 |
| top-trade gross share | 0.085 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-05-21: Rs530 |
| top symbol | SURYAROSNI: Rs1,015 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 22 |
| net PF | 0.439 |
| net PnL | Rs-5,725 |
| win rate | 31.8% |
| wins / losses | 7 / 15 |
| avg win / avg loss | Rs640 / Rs-681 |
| gross profit / loss | Rs4,483 / Rs10,208 |
| max drawdown | Rs-7,752 |
| SL / TGT / EOD exits | 9 / 3 / 10 |
| trades/day | 4.4 |
| days / symbols | 5 / 21 |
| top-trade gross share | 0.226 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.8525 |
| top day | 2026-06-12: Rs2,027 |
| top symbol | INDIACEM: Rs1,015 |

## Initial diagnosis

- Baseline TRAIN PF 0.359 / TEST PF 0.439 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 39 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.