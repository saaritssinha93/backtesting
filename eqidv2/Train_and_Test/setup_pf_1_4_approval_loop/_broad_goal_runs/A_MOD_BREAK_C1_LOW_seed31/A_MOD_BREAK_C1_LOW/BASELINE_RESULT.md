# A_MOD_BREAK_C1_LOW (SHORT) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** FINAL_SETUP_CONF (active)
- **Baseline exit:** SL 1.1% / Tgt 1.0%
- **Baseline mask_terms:** [vol_ratio>=1.955814]
- **Baseline pre_momentum_terms:** [pre5_mom_r>=0.425861, pre3_range_r<=0.202087]
- **Baseline entry_guards:** {}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-10 (16 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-10
- **TEST**  2026-06-12..2026-06-24 (5 sessions): 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 16 |
| net PF | 0.738 |
| net PnL | Rs-1,802 |
| win rate | 50.0% |
| wins / losses | 8 / 8 |
| avg win / avg loss | Rs634 / Rs-859 |
| gross profit / loss | Rs5,071 / Rs6,873 |
| max drawdown | Rs-3,716 |
| SL / TGT / EOD exits | 0 / 0 / 0 |
| target-fill rate | 0.0% |
| trades/day | 1.78 |
| days / symbols | 9 / 16 |
| top-trade gross share | 0.151 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.6716 |
| top day | 2026-06-08 00:00:00: Rs805 |
| top symbol | GKENERGY: Rs767 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 10 |
| net PF | 0.532 |
| net PnL | Rs-2,352 |
| win rate | 50.0% |
| wins / losses | 5 / 5 |
| avg win / avg loss | Rs535 / Rs-1,006 |
| gross profit / loss | Rs2,677 / Rs5,029 |
| max drawdown | Rs-3,462 |
| SL / TGT / EOD exits | 0 / 0 / 0 |
| target-fill rate | 0.0% |
| trades/day | 2.5 |
| days / symbols | 4 / 10 |
| top-trade gross share | 0.287 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9552 |
| top day | 2026-06-15 00:00:00: Rs342 |
| top symbol | SONACOMS: Rs768 |

## Initial diagnosis

- Baseline TRAIN PF 0.738 / TEST PF 0.532 (target: TRAIN PF >= 1.30, TEST PF >= 1.40 with day-block p <= 0.10).
- TEST sample is 10 trades over 4 day(s) — thin June data is the binding constraint on OOS confidence.