# B_HUGE_RED_FAILED_BOUNCE (SHORT) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** FINAL_SETUP_CONF (active)
- **Baseline exit:** SL 0.9% / Tgt 1.25%
- **Baseline mask_terms:** [] (none)
- **Baseline pre_momentum_terms:** [pre3_close_pos<=0.581797, sig5_rsi_dir<=64.104659, pre5_mom_r<=0.284145]
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-10 (16 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-10
- **TEST**  2026-06-12..2026-06-24 (5 sessions): 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 17 |
| net PF | 1.012 |
| net PnL | Rs71 |
| win rate | 47.1% |
| wins / losses | 8 / 9 |
| avg win / avg loss | Rs747 / Rs-656 |
| gross profit / loss | Rs5,974 / Rs5,903 |
| max drawdown | Rs-2,603 |
| SL / TGT / EOD exits | 0 / 0 / 0 |
| target-fill rate | 0.0% |
| trades/day | 1.31 |
| days / symbols | 13 / 17 |
| top-trade gross share | 0.17 |
| top-day net share | 14.264 |
| top-symbol net share | 14.264 |
| day-block p | 0.4924 |
| top day | 2026-06-08 00:00:00: Rs1,018 |
| top symbol | JNKINDIA: Rs1,018 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 3 |
| net PF | 0.0 |
| net PnL | Rs-2,311 |
| win rate | 0.0% |
| wins / losses | 0 / 3 |
| avg win / avg loss | Rs0 / Rs-770 |
| gross profit / loss | Rs0 / Rs2,311 |
| max drawdown | Rs-1,229 |
| SL / TGT / EOD exits | 0 / 0 / 0 |
| target-fill rate | 0.0% |
| trades/day | 1.0 |
| days / symbols | 3 / 3 |
| top-trade gross share | 9.99 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-15 00:00:00: Rs-101 |
| top symbol | JYOTHYLAB: Rs-101 |

## Initial diagnosis

- Baseline TRAIN PF 1.012 / TEST PF 0.0 (target: TRAIN PF >= 1.30, TEST PF >= 1.40 with day-block p <= 0.10).
- TEST sample is 6 trades over 3 day(s) — thin June data is the binding constraint on OOS confidence.