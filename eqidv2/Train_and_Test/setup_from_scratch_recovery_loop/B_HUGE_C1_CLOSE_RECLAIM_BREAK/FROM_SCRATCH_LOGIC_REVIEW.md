# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — FROM_SCRATCH_LOGIC_REVIEW

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

**1. What is this setup trying to capture?** momentum continuation — break of a prior HUGE GREEN bar's high in a non-bear regime.

**2. Why should it work theoretically?** A huge/structural 5-min event implies institutional participation; the follow-through (or its failure) should have short-horizon drift beyond noise.

**3. Why did earlier optimization fail?** Rounds 1-3 (~1,200+ configs) proved the detection is a high-frequency net loser at statutory+15bps and that mask-space pockets don't carry OOS. Cost anatomy on the broad TRAIN book: gross(0bps) PF 1.073 -> net@5bps 0.72 -> net@15bps 0.472 — a thin gross edge exists but costs consume it; execution quality matters most.

**4. Are the entry rules logically weak?** The signal fires at the close of an extended bar and buys/sells the NEXT 1-min open — the worst price of the sequence. Retest-depth data: within 30 min a 0.3-ATR pullback fills 84.6% of the time (0.6 ATR: 75.4%), so limit entries are mechanically feasible — F2 tests whether they help or adversely select.

**5. Are filters blocking winners / allowing losers?** See WINNER_LOSER_STUDY.md (FIT-only): the top separation features feed F3 directly.

**6. Are SL/target mismatched with actual 1-min movement?** MFE/MAE medians at 60 min: MFE 0.372% vs MAE -0.678%; only 43.0% of trades ever see +0.5% in the first hour — wide targets are structurally optimistic for most rows.

**7. Are exits too early/late/tight/wide?** F1 answers empirically (BE/trail/time grid). Baseline books are SL+EOD heavy with avgW~avgL — the classic no-edge shape.

**8. Bad time windows?** Hour table in WINNER_LOSER_STUDY.md; F4 tests the coarse windows.

**9. Symbols/days/regimes destroying the edge?** Domination metrics in every confirmation (caps trade 0.35 / day 0.40 / sym 0.40); worst-day/symbol tables in WINNER_LOSER_STUDY.md.

**10. Pool correctly recreated?** Yes — verified recreation for the mandated windows (POOL_RECREATION_REPORT.md lineage); 2026-07-02 excluded (1-min EOD sync incomplete).

**11. Lookahead/leakage/unrealistic exits?** Entries next-1-min-open +15bps adverse; exits first-touch pessimistic (same-bar SL before TGT; BE/trail effective next bar; resolver validated 300/300 vs production); thresholds from FIT/TRAIN only; TEST evaluated once per family.

**12. Should the setup be redesigned within the same idea?** That is this loop: F1 exit engineering, F2 retest entries, F3 FIT-mined confirmations, F4 windows, F5 fade (diagnostics: best fade TRAIN PF 0.465).
