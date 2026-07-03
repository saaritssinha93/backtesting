# A_MOD_BREAK_C1_LOW (SHORT) — FROM_SCRATCH_LOGIC_REVIEW

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## 1. What is this setup trying to capture?
A momentum-continuation short: a moderate red impulse 5-min bar that closes below the PREVIOUS bar's low while under session VWAP, in a non-bull tape — the idea is that broken micro-support + selling impulse continues down for at least ~1%.

## 2. Why should it work theoretically?
Intraday breakdown continuation is a real phenomenon when (a) the broken level matters, (b) participation is real (volume), and (c) there is room to fall. It monetises trend-day persistence and stop-cascades under the prior bar's low.

## 3. Why did the earlier optimization fail?
Because it FILTERED a population whose per-trade movement cannot pay the cost stack. The TRAIN-book 1-min study (n=11733) shows the median trade's MAX favorable excursion is only 0.472% (p40 0.31%, p60 0.661%) while median adverse excursion is 0.823% and the round-trip cost is ~0.30% (15 bps/leg both ways + statutory). The production 1.0% target sits beyond the p60 of what trades EVER achieve; favorable-first happens only 48.9% of the time; median EOD drift is -0.12%. No filter can fix a population whose median best-case move is smaller than cost+noise.

## 4. Are the current entry rules logically weak?
Yes — three ways. (i) The broken level is just the PRIOR BAR's low (a 5-minute micro-level), not a structural level (day low / OR low / multi-bar low), so most breaks are noise. (ii) The entry chases: it fills at the next 1-min open AFTER a 0.6-2.2 ATR impulse has already run — buying the extension, which is where mean-reversion bites (median MAE 0.823% against). (iii) Three incidental gates (ADX>=19.12, RSI>=23.22, atr_pct<=0.63%) restrict it to LOW-volatility names — precisely the names with the least room to fall (median MFE ~0.47%).

## 5. Are the current filters blocking winners or allowing losers?
Both. The atr_pct<=0.63% gate removes the high-energy names where a 1% move is possible, while vol_ratio>=1.5 alone admits thousands of noise breaks. Phase-2 proved NO slice of the gated population reaches PF 1.0 on both FIT and VAL (846 scans).

## 6. Are SL/target mismatched with actual 1-minute movement?
Severely. SL 1.10 vs median MAE 0.823% means ~half of trades nearly stop; target 1.00 vs median MFE 0.472% means most trades CANNOT reach it (TRAIN target-fill was 36.0%). The geometry is inverted R:R after costs.

## 7. Are exits too early/late/tight/wide?
The EOD 15:20 forced exit accounts for ~30% of trades; those bleed the -0.12% median drift. MFE-derived tight targets (0.3-0.66%) fill often but cannot cover 0.30% costs; wide targets never fill. There is no exit setting that rescues the geometry (phase-1 swept 49 exit pairs).

## 8. Are signals coming in bad time windows?
The baseline book loses in EVERY signal hour (hourly PF 0.44-0.65); late-morning (11:00) and 13:00 blocks are worst. Morning restriction reduces losses but never flips the sign.

## 9. Are some symbols/days/regimes destroying the edge?
No single destroyer: losses are uniform across days and symbols (that is what makes it structural). BEAR-regime days lose least (best 2-term pocket PF ~0.93) but still lose.

## 10. Is the current pool correctly recreated?
Yes — 4 deterministic sources, cross-verified identical row sets on shared dates, 53 TRAIN + 20 TEST sessions, 100% feature coverage; plus a from-raw re-detection that reproduces and widens the scanner universe (146k events vs its 25k).

## 11. Any lookahead, leakage, or unrealistic exits?
None found: signals use bar-close information only; entry is the NEXT 1-min open + adverse slippage; exits walk 1-min OHLC to 15:20; thresholds come from TRAIN-only quantiles; the MFE/MAE study uses TRAIN only; TEST was scored once per finalist (budget-capped).

## 12. Should the setup be redesigned while keeping the core idea?
It WAS — six redesigns were built and tested from raw data (see REDESIGNED_SETUP_IDEAS.md): fresh-session-low continuation, 2-bar persistence, deep-flow break, first-event-of-day morning, NIFTY-aligned, and retest-reject entry. Results are in ITERATION_LOG.md / CANDIDATE_CONFIGS.md.