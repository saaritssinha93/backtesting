# A_MOD_BREAK_C1_HIGH — From-Scratch Logic Review

_Generated 2026-07-03. Recovery-loop Stage 0. Evidence sources: campaigns 1-2 under
`Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/` (~2,970 configs) + this loop's
1-minute path study._

## 1. What is this setup trying to capture?

Intraday momentum continuation: a *moderate-impulse* 5-min bar (0.60–2.20 ATR — deliberately not
climactic) closes above the prior bar's high, above session VWAP, with volume confirmation
(vol_ratio ≥ 1.5), positive relative strength (rs_pct > 0.05) and a non-BEAR tape. The theory:
controlled breaks by relatively strong names above VWAP mark institutional continuation, not
exhaustion — ride the next leg.

## 2. Why should this setup work theoretically?

- Above-VWAP + RS-positive filters align the trade with the day's dominant flow.
- "Moderate" impulse avoids buying blow-off bars (the huge-bar family is handled elsewhere).
- Prior-bar-high break is the minimal continuation trigger → early entry, good location…
  *if* follow-through exists.

## 3. Why did the earlier optimization fail?

Measured, not guessed:
- Raw expression PF 0.22 TRAIN / 0.18 TEST over 74 sessions, 3.5k/1.4k trades.
- 69% of trades die at a 0.70% SL; realized avg loss (−927) > avg win (+761).
- The break-candle **chase entry buys the top of a 5-min thrust and pays ~30bps round-trip**;
  median follow-through cannot cover it.
- Every filter family (40+ true indicator/premom/structural features) removes trades roughly
  proportionally from winners and losers — conditional information ≈ 0 at the signal bar.
- Optuna's best configs on the cleanest sub-pool converge to the **unmasked base** — i.e., no
  gate improves the book; the entry expression itself is broken.

## 4. Are the current entry rules logically weak?

Yes — three concrete weaknesses:
1. **Any** prior-bar-high break qualifies (95 emissions/day) — most are mid-range noise, not
   structure. Requiring a genuine 20-bar high (`is_20bar_high`) was the only VAL-stable term.
2. Entry at next-1m-open after the signal bar = buying immediately after a completed upswing;
   MAE study (this loop) quantifies how much better entries are available minutes later.
3. Multiple same-day re-fires per name churn the book (first-per-day dedupe alone doubled PF).

## 5. Are the current filters blocking winners or allowing losers?

The production gate (`rs_pct≥2 & atr_pct≤0.006 & ≤11:10`) does BOTH badly: it sits inside the
worst ATR quintile band (0.0043–0.0059 → PF 0.187), points at the worst time-of-day per-trade,
and its rs_pct term carries no information (flat quintiles). It cut 98% of trades and still lost
(TRAIN PF 0.315 / TEST 0.216).

## 6. Are SL/target values mismatched with actual 1-minute movement?

Yes. 0.70% SL / 1.00% target produces 69-79% SL-rate — the SL sits inside normal 1-min noise of
these names (ATR ~0.4-0.6% per 5-min bar). The MFE/MAE study in `WINNER_LOSER_STUDY.md` measures
the actual excursion geometry and drives exits from data this time.

## 7. Are exits too early, too late, too tight, or too wide?

Too tight on the loss side, too small on the win side relative to costs. EOD exits were the
least-bad outcome class in campaign 1 — evidence the underlying drift (if any) needs time, not a
1.0% cap. Trailing/breakeven/time exits were untestable in the tt harness; the path engine in
this loop tests them.

## 8. Are signals coming in bad time windows?

Detector fires mostly ≥11:00. Guard `max_slot 11:05` was the single most VAL-stable knob
(morning subset PF ~0.35 vs 0.22 base) — but per-trade PF was *least bad* late (750-810 min).
Interpretation: mornings are better per-book (fewer, cleaner), afternoons better per-trade but
crowded. The redesign tests open-session and mid-session as separate regimes.

## 9. Are some symbols/days/regimes destroying the edge?

No concentration: worst symbols only −Rs6-7.5k each; losses diffuse across days; day-block p=1.0.
regime==BULL is *worse* than !=BEAR (chases already-run tape). This is a uniform negative drift,
not an event artifact — which is why filtering cannot fix it and entry redesign might.

## 10. Is the current pool correctly recreated?

Yes — 26,277 rows / 74 sessions (2026-03-04..07-01), rebuilt from master + fresh v11 raw scans
with the mid-June gap regeneration; 05-28 & 06-26 raw-store holes documented. Copied with
provenance into `pools/pool_base/` (see `POOL_RECREATION_REPORT.md`).

## 11. Is there any lookahead, leakage, or unrealistic exit behavior?

Audited: features at signal-bar close only; thresholds from FIT quantiles; TEST evaluated once
per confirmed candidate. Fill = next-1m-open + 15bps adverse; exits pessimistic (same-bar SL
priority) + exit-leg slippage; statutory cost model. The path engine in this loop is validated
against the canonical `setup_train_test` resolver before use (see `paths/validation.json`).
One residual idealization: limit-fill retest entries assume a resting order fills when price
trades through the level — conservative modeling (fill exactly at level + slippage), flagged in
candidate risk notes.

## 12. Should the setup be redesigned while keeping the same core idea?

Yes — that is this loop. The core idea (relative-strength continuation above VWAP) survives; the
expression (chase the break candle with a tight bracket) is what fails. Redesign directions, in
priority order (each grounded in a measured failure):

| # | redesign | attacks which failure |
|---|---|---|
| R1 | **first-per-day + genuine 20-bar-high** base (carried from campaign 2) | churn + noise breaks |
| R2 | **confirmation entry**: enter only when price takes out the signal-bar high within K minutes (stop-buy) | fake breaks; enters only on proof of follow-through |
| R3 | **retest-limit entry**: resting limit at/near the breakout level after the signal | buying the top; captures the pullback fill instead |
| R4 | **MFE/MAE-derived brackets**: SL/target set from measured excursion geometry | noise-level SL, cost-dominated target |
| R5 | **time/EOD/breakeven/trailing exits** via the 1-min path engine | bracket rigidity; EOD was least-bad |
| R6 | **two-stage day-regime split**: open-session vs mid-session rules | time-window mismatch |
| R7 | simpler/stricter single-knob variants of each of the above | overfit guard |
