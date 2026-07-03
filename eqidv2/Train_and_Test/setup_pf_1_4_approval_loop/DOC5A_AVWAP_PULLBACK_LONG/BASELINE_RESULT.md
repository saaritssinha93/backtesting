# DOC5A_AVWAP_PULLBACK_LONG (LONG) — BASELINE_RESULT

_Generated 2026-07-01. Research-only; NO live trades; NO final_setup_conf.py edits._

## Setup
- **Name / side:** `DOC5A_AVWAP_PULLBACK_LONG` / LONG
- **Origin:** Setup A ("AVWAP Trend Pullback") from `~/Downloads/5min_long_setups.md`, built as a new
  distinct detector by `Train_and_Test/doc5_long_setups/scan_doc5_long_setups.py` (standalone raw-5min
  scan; NOT wired into the live v2/v11 engine).
- **Config source:** none in `final_setup_conf.py` — this is a research detector, so the baseline is the
  raw detection at the doc-suggested default exit (no tuned config of record).

## Current rules (as detected)
- **Context (indicator + non-indicator):** established uptrend `close>EMA20 & EMA20>=EMA50`; rising
  session VWAP `slope_n >= 0.05` (ATR-normalised over 5 bars); `close>VWAP`; not over-extended
  `vwap_dist_atr <= 1.5`.
- **Pullback (non-indicator):** a low within the last 4 bars tagged value (`min(low) <= VWAP + 0.10*ATR`).
- **Trigger:** `close>close[t-1]` AND `close>VWAP` (closing back up off the dip); `close>open` and
  `close_loc>=0.60`.
- **Filters:** `rs_pct>0.30` (RS proxy = return-vs-NIFTYBEES), `vol_ratio>=1.2`, liquidity
  `close>=80 & day_value_so_far>=Rs2cr`, regime `!= BEAR & market_ret>-0.35`, skip climax bar `range<=2.75*ATR`.
- **Pre-momentum:** none in the raw detector (added only by the search).
- **Guards:** time window 09:45–14:00 only.
- **SL / target (baseline):** 0.70% / 1.25%.
- **Exit logic:** repo model — fill at next 1-min open after the 5-min signal + slippage; resolve on
  1-min OHLC to 15:20 IST (TARGET / SL / EOD); net of NSE intraday costs @ 15 bps/leg.

## Exact sessions (from the DOC5A pool)
- **FIT**   2026-05-18 … 2026-06-02 (10 sessions)
- **VAL**   2026-06-04 … 2026-06-19 (11 sessions)
- **TRAIN** 2026-05-18 … 2026-06-19 (21 sessions) = FIT + VAL
- **TEST**  2026-06-22 … 2026-06-30 (6 sessions)

## Baseline metrics (raw default cfg, SL 0.70 / Tgt 1.25, no mask/premom/guard, @15 bps/leg)

| window | n | PF | net PnL | win% | avgW / avgL | SL/TGT/EOD | tgt% | trades/day | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| FIT   | 89  | 0.258 | Rs-39,569 | 22.5 | 687 / -773 | 51/10/28 | 11.2 | 8.9  | Rs-43,091 |
| VAL   | 119 | 0.267 | Rs-53,593 | 23.5 | 697 / -803 | 70/17/32 | 14.3 | 10.8 | Rs-55,013 |
| TRAIN | 208 | 0.263 | Rs-93,163 | 23.1 | 693 / -790 | 121/27/60 | 13.0 | 9.9  | Rs-97,004 |
| TEST  | 58  | 0.290 | Rs-27,367 | 20.7 | 930 / -838 | 39/11/8  | 19.0 | 9.7  | Rs-28,509 |

_(reproduce: `py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5A_AVWAP_PULLBACK_LONG/scripts/eval_baseline.py`)_

## Initial diagnosis
- **No directional edge.** Win rate ~23% with avgW ≈ avgL ⇒ deeply negative expectancy (PF ~0.26)
  identically across FIT / VAL / TRAIN / TEST — i.e. not a regime artifact, a structural loser.
- **Target rarely fills** (tgt% 11–19%); SL count dominates (121 SL vs 27 targets in TRAIN). The 1.25%
  target is too far for the move this "pullback" produces; the 0.70% SL is hit first.
- **Over-fires:** ~10 trades/day across the F&O universe — far above the 6/day cap. Any positive config
  must gate hard, raising overfit risk.
- **Prior expectation for the search:** to reach PF ≥ 1.30 from a raw PF of 0.26 requires removing ~80% of
  trades — almost certainly overfit. The pf-band search (700 trials) confirms this: max achievable
  min(FIT,VAL) PF = 0.449; 0 configs reached the band. See PARAMETER_SWEEP_SUMMARY.md / FAILURE_ANALYSIS.md.
