# G_HIGHER_HIGH_BREAK (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.9% / Tgt 2.5%
- **Baseline mask_terms:** [] (none)
- **Baseline pre_momentum_terms:** [pre2_mom_r>=0.55, sig5_adx_calc>=26.0]
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-09 (11 sessions) = FIT 2026-05-18..2026-05-27 + VAL 2026-05-29..2026-06-09
- **TEST**  2026-06-10..2026-06-24 (5 sessions): 2026-06-10, 2026-06-12, 2026-06-15, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 1 |
| net PF | inf |
| net PnL | Rs815 |
| win rate | 100.0% |
| wins / losses | 1 / 0 |
| avg win / avg loss | Rs815 / Rs0 |
| gross profit / loss | Rs815 / Rs-0 |
| max drawdown | Rs0 |
| SL / TGT / EOD exits | 0 / 0 / 1 |
| target-fill rate | 0.0% |
| trades/day | 1.0 |
| days / symbols | 1 / 1 |
| top-trade gross share | 1.0 |
| top-day net share | 1.0 |
| top-symbol net share | 1.0 |
| day-block p | None |
| top day | 2026-06-03: Rs815 |
| top symbol | ISGEC: Rs815 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 3 |
| net PF | 0.0 |
| net PnL | Rs-2,885 |
| win rate | 0.0% |
| wins / losses | 0 / 3 |
| avg win / avg loss | Rs0 / Rs-962 |
| gross profit / loss | Rs0 / Rs2,885 |
| max drawdown | Rs-2,248 |
| SL / TGT / EOD exits | 2 / 0 / 1 |
| target-fill rate | 0.0% |
| trades/day | 1.0 |
| days / symbols | 3 / 3 |
| top-trade gross share | 9.99 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-12: Rs-637 |
| top symbol | RICOAUTO: Rs-637 |

## Initial diagnosis

- Baseline TRAIN PF inf / TEST PF 0.0 (target: TRAIN PF >= 1.30, TEST PF >= 1.40 with day-block p <= 0.10).
- TEST sample is 12 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.