# Two Brand-New Setups (GAP / POWER_HOUR) — Honest Diagnosis (v11)
*Designed + tested 2026-06-13 through the same pipeline as the book (structural scan -> clean train/test
-> aggressive search + full anti-overfit battery). Engines: new2_setups_scan_v11.py,
new_setups_salvage_search_v11.py --phase new2. NET of cost. Train 2025-11..2026-04 / test 2026-05..06.*

## Verdict: BOTH REJECT (day-concentration). 0 of 2 promote.

These were proposed as mechanistically-sound, literature-backed ideas distinct from the 10-setup book.
Both fired huge populations and BOTH failed the anti-overfit battery — not on sample, but because their
test-window "edge" is concentrated on 1-2 days. **The battery did exactly its job.** No honest config exists,
and the only configs with high PF are crash-day artifacts. **Not salvageable** (unlike the L/N sample-rejects):
the samples are already huge (5k+), and a 60k-random search found NO day-spread gate — loosening cannot
manufacture day-spread.

| Setup | side | n (tr/te) | ungated tr/te | best gated config | why REJECT |
|---|---|---|---|---|---|
| GAP_UP_HOLD_BREAK | LONG | 5494 (5243/251) | 0.64 / 0.63 (−Rs363k) | stock_ret≥1.29 & adx≤26 & quality≥92, 1.1/1.5: test 3.27 | **86% of test profit = ONE day (2026-05-13)**; top-2 days 112%; all PF≥2 configs top1day 80–167%, test spans only 3–5 days |
| POWER_HOUR_LAGGARD_BREAKDOWN | SHORT | 6059 (5791/268) | 0.57 / 1.45 (−Rs228k) | signal_minute≤825 & market_ret≤−0.8, 1.1/1.5: test 53 | **100% of test profit = ONE day (2026-05-12)** — the SAME crash day that trapped S_MACD_HIST_FLIP; the market_ret gate literally selects that down day |

### GAP_UP_HOLD_BREAK (gap-and-go long)
Mechanism (gap holds OR, then breaks) is real, but **ungated it is a −Rs363k chase** and the gated wins do
not spread: the best non-single-day gate yields 14 test trades over 4 days with **86% from 2026-05-13** and
the other days flat/negative. The robustness-first ranking surfaced only day-concentrated configs
(top1day 80–167%). The `adx≤26` (low-trend) + `stock_ret≥1.29` (already up) gate is the same low-ADX /
don't-overextend shape seen elsewhere, but here it does not generalise across days. **REJECT.**

### POWER_HOUR_LAGGARD_BREAKDOWN (late-day laggard short)
Ungated is slightly test-positive (1.45 over 11 days) but train-negative (0.57). EVERY train-PF≥2 config
collapses to a **single test day (d1, top1day 100%)** and requires `market_ret_pct ≤ −0.6…−1.0` — i.e. it
selects a down-market day. That day is **2026-05-12**, the documented crash-day artifact (see S_MACD_HIST_FLIP
in the book's research-watch). The "edge" is one day of broad-market capitulation, not a repeatable laggard
signal. **REJECT.**

## Honesty note
This is the correct, expected outcome for most novel ideas (in the prior round, 1 of 4 cleanly passed). The
failure modes are exactly the ones flagged at design time: gap-and-go as a late chase, power-hour shorts
concentrating on one crash day. Forcing acceptance would require selecting the May-12/13 days — the dishonest
move the day-concentration metric (top1day) exists to prevent. **Nothing added to final_setup_conf.py or the
candidate config.** Artifacts: new2_setups_standalone_trades.csv, proposals/new2_setups_search_results.csv.
