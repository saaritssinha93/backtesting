# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — BASELINE_RESULT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Current rules (config of record)

- **Config source:** NOT in final_setup_conf.py — unpromoted catalog setup; baseline = raw detector + production exit, plus the live-overlay OR-gate variant.
- **Detector (5-min):** moderate-impulse bar, long structure, above session VWAP, `close_loc >= 0.75`, `close > prev_bar_high`, `rs_pct > 0.00`, `vol_ratio >= 1.4` (avwap_5min_ID_v2_backtesting.py:704-711; catalog min qs 6.8).
- **Structural note:** the shared candidate scan keeps ONE candidate per (ticker, bar) by quality score with alphabetical tie-break, so A_MOD_BREAK_C1_HIGH shadows this setup outside BEAR regime — 96.8% of this setup's rows are BEAR-regime days (it is effectively a bear-day continuation LONG).
- **Pre-momentum:** none. **Filters:** live overlay OR-gate (`signal_range_pct >= 2.2` OR `notional <= Rs100k`). **Guards:** none.
- **SL/Target:** 0.70% / 1.50% (v6.SETUP_EXIT_RULES). **Exit:** first-touch SL/TARGET on 1-min bars else EOD 15:20 IST.
- **Costs:** statutory NSE intraday + 15 bps/leg adverse slippage; entry = next 1-min open after the 5-min signal.

## Sessions (exact)

- **TRAIN** 2026-03-02..2026-05-29 (58 sessions) — requested 2026-03-01..2026-05-30
- **FIT** 2026-03-02..2026-04-24 (35 sessions, first 60% of TRAIN)
- **VAL** 2026-04-27..2026-05-29 (23 sessions, last 40% of TRAIN)
- **TEST** 2026-06-01..2026-07-01 (21 sessions) — requested 2026-06-01..2026-07-02; 2026-07-02 excluded (1-min exit data truncated ~09:30), 2026-06-26 has no 5-min data.
- Pool: 7354 raw rows over 80 sessions (2026-03-02..2026-07-02).

## raw_detector

cfg: SL 0.7 / Tgt 1.5, mask=[], premom=[], guard=None, or_gate=False

### raw_detector — FIT

| metric | value |
|---|---|
| trades | 1159 |
| net PF | 0.328 |
| net PnL | Rs-501,880 |
| win rate | 24.0% |
| wins / losses | 278 / 881 |
| avg win / avg loss | Rs880 / Rs-847 |
| gross profit / loss | Rs244,731 / Rs746,611 |
| max drawdown | Rs-500,985 |
| SL / TGT / EOD exits | 764 / 159 / 236 |
| target-fill rate | 13.7% |
| trades/day | 33.11 |
| days / symbols | 35 / 677 |
| top-trade gross share | 0.005 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs19,801 |
| top symbol | LLOYDSME: Rs3,415 |

### raw_detector — VAL

| metric | value |
|---|---|
| trades | 728 |
| net PF | 0.296 |
| net PnL | Rs-344,227 |
| win rate | 20.3% |
| wins / losses | 148 / 580 |
| avg win / avg loss | Rs979 / Rs-843 |
| gross profit / loss | Rs144,892 / Rs489,118 |
| max drawdown | Rs-344,561 |
| SL / TGT / EOD exits | 495 / 101 / 132 |
| target-fill rate | 13.9% |
| trades/day | 31.65 |
| days / symbols | 23 / 511 |
| top-trade gross share | 0.009 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-05-18: Rs330 |
| top symbol | SENCO: Rs2,531 |

### raw_detector — TRAIN

| metric | value |
|---|---|
| trades | 1887 |
| net PF | 0.315 |
| net PnL | Rs-846,107 |
| win rate | 22.6% |
| wins / losses | 426 / 1461 |
| avg win / avg loss | Rs915 / Rs-846 |
| gross profit / loss | Rs389,623 / Rs1,235,730 |
| max drawdown | Rs-845,212 |
| SL / TGT / EOD exits | 1259 / 260 / 368 |
| target-fill rate | 13.8% |
| trades/day | 32.53 |
| days / symbols | 58 / 832 |
| top-trade gross share | 0.003 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs19,801 |
| top symbol | CAMLINFINE: Rs2,959 |

### raw_detector — TEST

| metric | value |
|---|---|
| trades | 613 |
| net PF | 0.252 |
| net PnL | Rs-315,822 |
| win rate | 18.6% |
| wins / losses | 114 / 499 |
| avg win / avg loss | Rs933 / Rs-846 |
| gross profit / loss | Rs106,405 / Rs422,226 |
| max drawdown | Rs-315,223 |
| SL / TGT / EOD exits | 424 / 73 / 116 |
| target-fill rate | 11.9% |
| trades/day | 29.19 |
| days / symbols | 21 / 464 |
| top-trade gross share | 0.012 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-09: Rs2,827 |
| top symbol | AXISCADES: Rs3,748 |

## baseline_live_overlay

cfg: SL 0.7 / Tgt 1.5, mask=[], premom=[], guard=None, or_gate=True

### baseline_live_overlay — FIT

| metric | value |
|---|---|
| trades | 1003 |
| net PF | 0.337 |
| net PnL | Rs-414,157 |
| win rate | 24.6% |
| wins / losses | 247 / 756 |
| avg win / avg loss | Rs853 / Rs-827 |
| gross profit / loss | Rs210,776 / Rs624,933 |
| max drawdown | Rs-414,638 |
| SL / TGT / EOD exits | 629 / 136 / 238 |
| target-fill rate | 13.6% |
| trades/day | 28.66 |
| days / symbols | 35 / 559 |
| top-trade gross share | 0.006 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs12,335 |
| top symbol | ZYDUSWELL: Rs5,048 |

### baseline_live_overlay — VAL

| metric | value |
|---|---|
| trades | 618 |
| net PF | 0.276 |
| net PnL | Rs-294,604 |
| win rate | 19.1% |
| wins / losses | 118 / 500 |
| avg win / avg loss | Rs953 / Rs-814 |
| gross profit / loss | Rs112,506 / Rs407,110 |
| max drawdown | Rs-293,674 |
| SL / TGT / EOD exits | 402 / 77 / 139 |
| target-fill rate | 12.5% |
| trades/day | 26.87 |
| days / symbols | 23 / 427 |
| top-trade gross share | 0.011 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-05-18: Rs1,262 |
| top symbol | MOLDTKPAC: Rs2,518 |

### baseline_live_overlay — TRAIN

| metric | value |
|---|---|
| trades | 1621 |
| net PF | 0.313 |
| net PnL | Rs-708,761 |
| win rate | 22.5% |
| wins / losses | 365 / 1256 |
| avg win / avg loss | Rs886 / Rs-822 |
| gross profit / loss | Rs323,282 / Rs1,032,043 |
| max drawdown | Rs-707,867 |
| SL / TGT / EOD exits | 1031 / 213 / 377 |
| target-fill rate | 13.1% |
| trades/day | 27.95 |
| days / symbols | 58 / 701 |
| top-trade gross share | 0.004 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-04-02: Rs12,335 |
| top symbol | ZYDUSWELL: Rs4,117 |

### baseline_live_overlay — TEST

| metric | value |
|---|---|
| trades | 530 |
| net PF | 0.256 |
| net PnL | Rs-262,232 |
| win rate | 19.1% |
| wins / losses | 101 / 429 |
| avg win / avg loss | Rs893 / Rs-822 |
| gross profit / loss | Rs90,230 / Rs352,462 |
| max drawdown | Rs-261,312 |
| SL / TGT / EOD exits | 350 / 61 / 119 |
| target-fill rate | 11.5% |
| trades/day | 26.5 |
| days / symbols | 20 / 413 |
| top-trade gross share | 0.014 |
| top-day net share | 9.99 |
| top-symbol net share | 9.99 |
| day-block p | 1.0 |
| top day | 2026-06-09: Rs1,057 |
| top symbol | AXISCADES: Rs3,748 |

## Initial diagnosis

- Raw detector is a heavy net loser at production exits: TRAIN PF 0.315 (n=1,887, -Rs846k, win 22.6%), TEST PF 0.252 (n=613, -Rs316k). FIT/VAL/TRAIN/TEST all consistent (PF 0.25-0.34) - the base is uniformly negative, not regime-lucky.
- Exit mix at SL0.70/T1.50: 67% SL, 14% target, 19% EOD - the 0.70% stop is hit far too often for a 1.50% target on BEAR-day longs; risk/reward never realizes.
- The live overlay OR-gate (signal_range_pct>=2.2 OR notional<=100k) removes ~14% of trades but leaves PF unchanged (0.313/0.256) - the current production gate does NOT rescue this setup.
- ~29-33 trades/day is enormous churn; the setup fires almost exclusively on BEAR days (96.8% of rows) because A_MOD_BREAK_C1_HIGH takes the non-BEAR bars in the same-bar collapse - this is a counter-trend long by construction.
- Optimization must therefore find a small, defensible pocket (strong-confirmation subset + time guard + top-N cap + exit retune) rather than trim a marginal book; trade count will fall dramatically and that is acceptable if the pocket is stable across FIT/VAL and TEST.