# DOC5D_AVWAP_RECLAIM_LONG (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only. NO live trades. NO `final_setup_conf.py` edits._

## What this setup is

**Doc source:** `~/Downloads/5min_long_setups.md` → **Setup D — AVWAP Reclaim (lean / aggressive)**.
A stock trading *below* session VWAP reclaims it from below on an up-bar — "catching the turn"
into a trend. Deliberately earlier / riskier / looser-RS than the A pullback or B breakout.

**Config source:** no entry in `final_setup_conf.py` (this is a NEW research detector, not a book
setup). Baseline = **raw detection only**, i.e. the standalone scan `scan_doc5_long_setups.py`
`DOC5D` branch with the doc-suggested exit and no extra mask / pre-momentum / guard.

### Current rules (raw detection — `reclaim_session_vwap_from_below_long`)
- **Non-indicator / price-action:** `prev_close ≤ prev_VWAP` AND `close > VWAP` (fresh reclaim);
  `close > prev_close` (up close); `close > open` AND `close_loc ≥ 0.60` (strong close);
  skip climax bars `range ≤ 2.75×ATR`.
- **Indicators:** session VWAP, ATR, `vol_ratio ≥ 1.3`; VWAP slope (ATR-normalised, 5-bar) turning
  up (or NaN early).
- **RS / regime (proxies):** `rs_pct > 0.15` (stock_ret − index_ret); `regime ≠ BEAR` and
  `market_ret ≥ −0.35`.
- **Shared gates:** `close ≥ Rs 80`, day-value ≥ Rs 2 cr, 5-min traded-value floor.
- **Time guard:** signal minute 09:45–13:00 IST.
- **Pre-momentum:** none in the raw baseline.
- **Filters (mask):** none in the raw baseline.
- **Guards:** default live 09:30–14:30 window + one-ticker-per-day dedupe.
- **Exit:** SL 0.70% / Tgt 1.25% (doc default); entry = next 1-min open + slippage; resolve
  SL/Tgt/EOD on 1-min OHLC to 15:20 IST; net of NSE intraday costs.

## Sessions (exact — inferred from the DOC5D pool)

- Pool: `Train_and_Test/doc5_long_setups/pool/historical_all_available_pre_dedupe_live_candidates.csv`
  (161 DOC5D rows over 47 sessions, 2026-04-01…06-30).
- **Split rule:** TRAIN from 2026-05-18; TEST from 2026-06-20 (calendar; 5 sessions ≥ min 4, no fallback needed).
- **TRAIN** 2026-05-18…2026-06-19 (19 sessions) = **FIT** 2026-05-18…2026-06-02 (9) + **VAL** 2026-06-04…2026-06-19 (10).
  - FIT: 05-18, 05-19, 05-20, 05-22, 05-25, 05-26, 05-27, 05-29, 06-02.
  - VAL: 06-04, 06-09, 06-10, 06-11, 06-12, 06-15, 06-16, 06-17, 06-18, 06-19.
- **TEST** 2026-06-22…2026-06-30 (5 sessions): 06-22, 06-23, 06-24, 06-25, 06-30.

## Baseline metrics (raw detection, SL 0.70 / Tgt 1.25, one-per-day dedupe)

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| **FIT**   | n=31 PF=**0.175** net=−Rs16,515 win=19.4% SL/TGT/EOD=19/2/10 tgt-fill=6.5% | n=31 PF=0.479 net=−Rs7,457 |
| **VAL**   | n=35 PF=**0.143** net=−Rs19,411 win=22.9% SL/TGT/EOD=24/2/9 tgt-fill=5.7% | n=35 PF=0.366 net=−Rs10,928 |
| **TRAIN** | n=66 PF=**0.158** net=−Rs35,926 win=21.2% SL/TGT/EOD=43/4/19 tgt-fill=6.1% tpd=3.47 | n=66 PF=0.417 net=−Rs18,385 |
| **TEST**  | n=19 PF=**0.339** net=−Rs7,821 win=21.1% SL/TGT/EOD=11/4/4 tgt-fill=21.1% tpd=3.80 | n=19 PF=0.462 net=−Rs5,245 |

_TRAIN avg win / avg loss = Rs481 / −Rs820; bars-held avg ≈ 118. TEST avg win / avg loss = Rs1,005 / −Rs789._

## Initial diagnosis

- The raw archetype is a **heavy net loser on the recent window** — TRAIN PF 0.158, TEST PF 0.339,
  and it is a loser at 5 bps too (TRAIN 0.417 / TEST 0.462), so this is not a cost artifact.
- The failure signature is structural: **win rate ≈ 21%** with a **65% stop-out rate** (43/66 TRAIN
  SL) and only **4–6% target-fill**. The "fresh reclaim from below" is buying turns that immediately
  fail (whipsaw back under VWAP) on late-May/June tape. The average win (Rs481) does not cover the
  average loss (Rs820).
- Nearly every session is red (TRAIN best day only +Rs280; **every** TEST day negative), so the loss
  is broad-based, not one bad day — there is no clean day/symbol to gate around.
- To reach TRAIN PF ≥ 1.30 the loop would have to lift PF by ~8× off a 21%-win base, which is only
  possible by cutting to a tiny high-quality pocket — exactly the overfit trap the band gate is
  designed to catch. **Prior:** the wider-window doc5 run (TRAIN 04-01…05-29) also rejected DOC5D
  (best TRAIN 1.25 / TEST 0.47), and memory `project_5min_long_setups_doc_2026_07_01` records
  0/4 doc archetypes carrying June OOS edge.

**Target for a pass:** TRAIN PF ∈ [1.30, 1.70] with n ≥ 20, TEST PF > 1.40 with n ≥ 6, day-block
p ≤ 0.10, no single trade/day/symbol dominating. See ITERATION_LOG / PARAMETER_SWEEP_SUMMARY for
the search; APPROVAL_REQUIRED_FINAL_RECOMMENDATION for the verdict.
