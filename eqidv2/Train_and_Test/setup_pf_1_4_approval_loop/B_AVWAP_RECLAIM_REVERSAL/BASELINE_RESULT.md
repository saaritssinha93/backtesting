# B_AVWAP_RECLAIM_REVERSAL (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

- **Config source:** RESEARCH_WATCH_CONF (disabled)
- **Baseline exit:** SL 0.7% / Tgt 1.5%
- **Baseline mask_terms:** [vwap_dist_atr<=1.0]
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
| trades | 514 |
| net PF | 0.287 |
| net PnL | Rs-229,353 |
| win rate | 21.6% |
| wins / losses | 111 / 403 |
| avg win / avg loss | Rs831 / Rs-798 |
| gross profit / loss | Rs92,210 / Rs321,563 |
| max drawdown | Rs-230,037 |
| SL / TGT / EOD exits | 306 / 57 / 151 |
| trades/day | 30.24 |
| days / symbols | 17 / 396 |
| top-trade gross share | 0.014 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-02: Rs7,853 |
| top symbol | VINCOFE: Rs2,532 |

## Baseline TEST metrics (@15 bps/leg)

| metric | value |
|---|---|
| trades | 147 |
| net PF | 0.463 |
| net PnL | Rs-42,054 |
| win rate | 30.6% |
| wins / losses | 45 / 102 |
| avg win / avg loss | Rs804 / Rs-767 |
| gross profit / loss | Rs36,194 / Rs78,247 |
| max drawdown | Rs-59,214 |
| SL / TGT / EOD exits | 71 / 22 / 54 |
| trades/day | 29.4 |
| days / symbols | 5 / 141 |
| top-trade gross share | 0.035 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 0.9446 |
| top day | 2026-06-12: Rs15,943 |
| top symbol | JAICORPLTD: Rs1,266 |

## Initial diagnosis

- Baseline TRAIN PF 0.287 / TEST PF 0.463 (target: TRAIN in [1.30,1.70], TEST > 1.40).
- TEST sample is 1 trades over 1 day(s) — thin June data is the binding constraint on OOS confidence.