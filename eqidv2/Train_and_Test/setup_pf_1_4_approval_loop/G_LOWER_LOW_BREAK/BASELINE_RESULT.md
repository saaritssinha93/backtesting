# G_LOWER_LOW_BREAK (SHORT) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** FINAL_SETUP_CONF (active)
- **Baseline exit:** SL 0.8% / Tgt 0.8%
- **Baseline mask_terms:** [vol_ratio>=4.129044, quality_score>=76.444124]
- **Baseline pre_momentum_terms:** [sig5_rsi_dir>=68.747209]
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-10 (16 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-10
- **TEST**  2026-06-12..2026-06-24 (5 sessions): 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 7 |
| net PF | 0.975 |
| net PnL | Rs-43 |
| win rate | 42.9% |
| wins / losses | 3 / 4 |
| avg win / avg loss | Rs567 / Rs-436 |
| gross profit / loss | Rs1,700 / Rs1,743 |
| max drawdown | Rs-1,410 |
| SL / TGT / EOD exits | 1 / 3 / 3 |
| trades/day | 1.4 |
| days / symbols | 5 / 6 |
| top-trade gross share | 0.334 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.5202 |
| top day | 2026-05-18: Rs568 |
| top symbol | SPORTKING: Rs567 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 0 |
| net PF | 0.0 |
| net PnL | Rs0 |
| win rate | 0.0% |
| wins / losses | 0 / 0 |
| avg win / avg loss | Rs0 / Rs0 |
| gross profit / loss | Rs0 / Rs0 |
| max drawdown | Rs0 |
| SL / TGT / EOD exits | 0 / 0 / 0 |
| trades/day | 0.0 |
| days / symbols | 0 / 0 |
| top-trade gross share | None |
| top-day net share | None |
| top-symbol net share | None |
| day-block p | None |
| top day | None |
| top symbol | None |

## Initial diagnosis

- Baseline TRAIN PF 0.975 / TEST PF 0.0 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 5 trades over 2 day(s) — thin June data is the binding constraint on OOS confidence.