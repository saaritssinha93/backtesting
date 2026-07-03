# P_PDH_BREAK_RETEST_LONG — FAILURE_ANALYSIS

## 1. Losing-trade classification (baseline, 15 bps/leg)

**TRAIN (n=36):** outcomes **20 SL / 10 TARGET / 6 EOD**, win 31%, avg_win **+343**, avg_loss **−633**, net −12,055.
**TEST (n=9):** outcomes **4 SL / 5 TARGET**, win 56%, avg_win **+364**, avg_loss **−674**, net −876.

Dominant failure modes:
- **SL too tight / target too small relative to cost** — the 0.50/0.60 scalp: avg_loss is ~**1.9×** avg_win *even though SL (0.50) < target (0.60)*. Slippage (15 bps × 2 legs = 30 bps) + statutory costs + the +3 bps SL penalty make a "small" stop cost more than a "big" target pays. A 31% win rate then guarantees a loss.
- **fake breakout / failed retest** — 56% of TRAIN trades hit SL: the previous-day-high "retest" frequently fails to resume (price breaks PDH, retests, then rolls over), i.e. the entry has no positive expectancy.
- **death-by-cost over-firing** — ungated the setup fires ~14 trades/session; at PF 0.3 each trade is a small negative-EV cost event.

## 2. Worst days / symbols

| | TRAIN worst days | TEST worst days |
|---|---|---|
| | 2026-05-14 (−2,376), 2026-04-20 (−2,193), 2026-04-06 (−1,461) | 2026-05-26 (−730), 2026-05-22 (−367), 2026-05-29 (−351) |

Worst symbols (baseline): TRAIN ANGELONE/DLF/RECLTD (−732 each, all full SL); TEST SAMMAANCAP/AUBANK/SIEMENS (−718…−732). Best symbols barely reach +363…+366 (a single capped target). The **loss tail is far heavier than the win tail** — structural negative skew.

## 3. Exit behaviour across the search

- **Widening the target helps but cannot rescue it:** ungated PF rises 0.19 → 0.37 from Tgt 0.60 → 2.50; baseline-gated 0.24 → 1.28. The cost drag is amortised over larger moves, but the win rate is too low for PF to clear 1.30 at any realistic SL/target.
- **Tightening the SL** worsens it (more fake-retest stop-outs); **widening the SL** beyond ~1.0 only adds EOD bleed without enough extra targets.
- **Time-of-day:** a morning-only window (≤11:30) lifts TRAIN PF to ~1.32 but on only 10 trades and a 3-trade TEST — not a window edge, a sample artifact.

## 4. Pre-momentum / volume / volatility / trend issues

- **Pre-momentum gate (the conf "edge"):** `score≥75 & range_r≥0.50` cuts 386 → 36 trades but only lifts PF to ~1.0; it does not isolate a positive-EV subset, it just shrinks the sample.
- **Volume:** the only filter that reaches TRAIN PF ≥ 2.0 is `vol_ratio ≥ 7.12` (extreme climax) on 21 trades — overfit; TEST 0.84/n=3.
- **Quality score:** `quality_score ≥ median` gives TRAIN 4.22 on **9** trades — textbook overfit; TEST rides one day (domday 21×).
- **Trend / regime:** `regime_align` (don't-fight-tape) and `sig5_adx≥25` do not help — `sig5_adx≥25` alone keeps 222 trades at PF 0.34.

## 5. Live / backtest reconciliation
- This honest April–May backtest (TRAIN PF 0.24 / TEST PF 0.68) **independently agrees** with the live-paper June result (40 trades, PF 0.25, −Rs14,497, win 25%). The setup loses in *both* windows and *both* engines. The original promotion evidence ("train 2.39 / test 6.88") came from a different, smaller tier123 probe pool on corrected-VWAP at lower assumed cost and did not survive contact with realistic cost + an honest out-of-sample window — the classic small-sample / cost-omission overfit that this loop is designed to catch.

## 6. Bottom line
The previous-day-high break-retest signal, as captured in this pool, has **no exploitable directional edge** net of realistic cost. Every route to TRAIN PF ≥ 1.30 requires extreme selectivity (n ≤ 20) that is overfit and fails OOS or rides a single day. **No tradeable candidate exists.**
