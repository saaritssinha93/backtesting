# L_DOUBLE_BOTTOM_VWAP (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** FINAL_SETUP_CONF (active)
- **Baseline exit:** SL 0.9% / Tgt 2.0%
- **Baseline mask_terms:** [] (none)
- **Baseline pre_momentum_terms:** [sig5_rsi_dir>=60.101595]
- **Baseline entry_guards:** {'min_slot': '10:00', 'max_slot': '11:30', 'top_n': 1}
- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; entry = next 1-min open after the 5-min signal + slippage.

## Sessions (exact)

- calendar TEST (>= 2026-06-20) had only 2 session(s) -> FELL BACK to the last 5 available sessions.
- **TRAIN** 2026-05-18..2026-06-11 (18 sessions) = FIT 2026-05-18..2026-05-29 + VAL 2026-06-01..2026-06-11
- **TEST**  2026-06-12..2026-06-24 (5 sessions): 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-22, 2026-06-24

## Baseline TRAIN metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 7 |
| net PF | 1.773 |
| net PnL | Rs2,364 |
| win rate | 57.1% |
| wins / losses | 4 / 3 |
| avg win / avg loss | Rs1,355 / Rs-1,019 |
| gross profit / loss | Rs5,421 / Rs3,057 |
| max drawdown | Rs-3,057 |
| SL / TGT / EOD exits | 2 / 2 / 3 |
| target-fill rate | 28.6% |
| trades/day | 1.4 |
| days / symbols | 5 / 7 |
| top-trade gross share | 0.326 |
| top-day net share | 0.8 |
| top-symbol net share | 0.747 |
| day-block p | 0.2147 |
| top day | 2026-05-21: Rs1,891 |
| top symbol | FLAIR: Rs1,766 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 3 |
| net PF | 2.467 |
| net PnL | Rs1,657 |
| win rate | 66.7% |
| wins / losses | 2 / 1 |
| avg win / avg loss | Rs1,393 / Rs-1,129 |
| gross profit / loss | Rs2,786 / Rs1,129 |
| max drawdown | Rs-1,129 |
| SL / TGT / EOD exits | 1 / 1 / 1 |
| target-fill rate | 33.3% |
| trades/day | 3.0 |
| days / symbols | 1 / 3 |
| top-trade gross share | 0.63 |
| top-day net share | 1.0 |
| top-symbol net share | 1.059 |
| day-block p | None |
| top day | 2026-06-12: Rs1,657 |
| top symbol | SENORES: Rs1,755 |

## Initial diagnosis

- Baseline TRAIN PF 1.773 / TEST PF 2.467 (target: TRAIN PF >= 1.30, TEST PF >= 1.40 with day-block p <= 0.10).
- TEST sample is 4 trades over 3 day(s) — thin June data is the binding constraint on OOS confidence.