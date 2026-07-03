# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — FROM_SCRATCH_LOGIC_REVIEW

_Generated 2026-07-03. Research-only; no live trades; final_setup_conf.py untouched._

## 1. What is this setup trying to capture?

A **trend-continuation entry**: a 5-minute bar of *moderate* impulse (range 0.60–2.20 ATR —
not a blow-off, not noise) that closes in the top 25% of its range (`close_loc >= 0.75`),
breaks the **prior bar's high**, sits **above session VWAP**, shows **positive relative
strength** vs the market, on **>= 1.4x relative volume**. The theory: a stock already in
control of buyers, making a controlled new micro-high on volume, tends to continue — you
join the move on the next tick, risking a fraction of ATR.

## 2. Why should this setup work theoretically?

Continuation-on-strength is one of the two canonical intraday edges (with failed-move
reversal). The card's ingredients are the right ones: VWAP side = intraday control; RS = the
stock leads the tape; close-near-high = no seller response into the bar close; moderate
impulse = the move is fresh rather than climactic; volume = participation. **Its natural
habitat is a flat-to-positive tape** — continuation longs on a falling market are fighting
index beta.

## 3. Why did the earlier optimization fail? (evidence, not opinion)

Two campaigns (1,673 logged iterations over 59 features, exits, guards, premom, TPE) failed
because **the pool itself was structurally mis-sampled**, not because filters were missed:

* `avwap_5min_ID_v7_candidate_scan._dedupe_candidate_frame` keeps ONE candidate per
  (ticker, bar), tie-broken by quality then **alphabetical setup name**. `A_MOD_BREAK_C1_HIGH`
  (same bar pattern, requires `regime != BEAR`) alphabetically precedes and quality-ties this
  setup, so it absorbs every non-BEAR bar. What reached this setup's pool was **96.8%
  BEAR-regime rows** — precisely the tape where continuation longs cannot work.
  Baseline PF 0.315 with 67% SL-outs is what index beta against a falling market looks like.
* The earlier campaigns could only re-slice that bear-day residue. The best any filter stack
  could do was FIT/VAL PF ~0.56/0.59; the single TRAIN-band pocket (n=20) collapsed on TEST
  (n=3, PF 0.31) — a thin artifact, correctly rejected.

## 4. Are the current entry rules logically weak?

The *card conditions* are sound. The **detection context** is weak:
* the v2 scan loop starts at bar index `VWAP_LOOKBACK=20` → **first scanned bar ≈ 10:55 IST**.
  The morning continuation window (09:30–10:55), where breakout follow-through is
  strongest, is never scanned. Confirmed empirically: zero signals exist before ~10:00 in the
  pool; trade mass sits 11:00–13:00 (lunch chop).
* no freshness condition: the bar can be the 5th consecutive break (chasing).

## 5. Are the current filters blocking winners or allowing losers?

Both, in the worst combination: the **collapse filter blocks the winners' regime**
(non-BEAR bars go to the sibling setup) while the detector itself **allows losers**
(bear-day counter-trend longs, late-morning chop entries, already-extended bars —
loser medians showed HIGHER rs_pct/quality than winners). The live overlay OR-gate
(signal_range_pct >= 2.2 OR notional <= 100k) removed 14% of trades with zero PF gain.

## 6. Are SL/target values mismatched with actual 1-minute movement?

At SL 0.70% / Tgt 1.50%: 67% of trades die on SL (avg ~45 min to stop), only 14% reach
target. The full 49-combo exit grid was negative everywhere — on the bear-residue pool no
bracket works, which means the mismatch was **entry distribution, not exit numbers**.
The redesigned pool gets a fresh MFE/MAE study from 1-min paths to set brackets from data.

## 7. Are exits too early, too late, too tight, or too wide?

On the old pool: irrelevant — every bracket lost. Structure of the losses (SL-heavy, EOD
~19% with 152-bar holds) says entries were immediately underwater, not that winners were
cut. Re-answered for the redesigned pool after the MFE/MAE study.

## 8. Are signals coming in bad time windows?

Yes, by construction: nothing before ~10:55 (scan-start artifact), mass in 11:00–13:00.
Every hour bucket was negative on the old pool, so no window rescued it; the redesign
opens the 10:00–10:55 window (earliest where the causal 20-bar volume mean exists).

## 9. Are some symbols/days/regimes destroying the edge?

Regime is the whole story: BEAR n=1,692 PF 0.31; BULL n=120 PF 0.26 (a 3% contaminated
remnant); the only positive slice (TREND, PF 1.33) had n=18. Losses were spread across
days/symbols (no single-name artifact) — it is a regime-composition problem, not an
outlier problem.

## 10. Is the current pool correctly recreated?

Yes — and that is exactly how the defect was found: the recreated pool (master + gap-fill +
fresh tail, basis-validated within 1–4 rows/day of independent scans) faithfully reproduces
what production feeds this setup. *Correct recreation of a mis-sampled universe.* The
recovery pool is therefore built by a new scanner (`scripts/scan_redesigned_pool.py`) that
re-detects the card conditions directly from 5-min OHLCV, uncollapsed, all regimes,
from ~10:00 IST.

## 11. Is there any lookahead, leakage, or unrealistic exit behavior?

None found, and the redesign preserves that: session VWAP is the causal cumulative;
vol_ratio uses the SHIFTED 20-bar mean; prev-day levels only; regime/market return is the
last known market bar <= signal time; entry is the NEXT 1-min open + 15 bps/leg slippage;
exits are first-touch SL/target on 1-min bars else EOD 15:20; statutory NSE costs.
One data-integrity exclusion: 2026-07-02 has truncated 1-min data (~09:30) → excluded from
TEST; 2026-06-26 has no 5-min data.

## 12. Should the setup be redesigned while keeping the same core idea?

Yes — that is this campaign. Same card intent, redesigned detection:
* **uncollapsed**: every qualifying bar emits, all regimes — the pattern finally gets tested
  in its natural habitat (BULL/NEUTRAL/TREND tape);
* **earlier window**: scan from ~10:00 instead of 10:55;
* **two-stage variants** as structural flags: `x_first_break_of_day` (trade only the first
  break, not the 5th), `x_fresh_break` (prior bar had NOT already broken — anti-chase),
  `x_prev_pullback` (pullback-then-break two-stage logic);
* regime now a *searchable* dimension instead of a hidden allocator;
* exits re-derived from MFE/MAE measured on 1-min paths;
* deployment path if a candidate passes: flag-gated detector extension (S9/DOC5D pattern),
  NOT an edit to the collapse (which would perturb sibling setups).

## New logic directions tested (see REDESIGNED_SETUP_IDEAS.md)

R1 uncollapsed card baseline; R2 non-BEAR only; R3 first-break-of-day; R4 fresh-break
(anti-chase); R5 pullback-then-break; R6 morning window (10:00–11:30); R7 aligned-regime
(BULL/TREND) + volume thrust; R8 quality-ranked top-N per slot; combinations thereof, plus
full indicator/premom/guard/exit sweeps on the winning skeleton.
