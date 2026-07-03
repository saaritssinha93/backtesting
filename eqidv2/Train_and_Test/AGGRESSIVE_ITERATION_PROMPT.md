# Enhanced copy-paste prompt — aggressive per-setup iteration to robust TRAIN PF>2 + honest TEST

(Engineered from the 2026-06-22 session to avoid the traps we actually hit: maxpf
overfitting, the native*-basis trap, the empty-June holdout, and the live-feed-starvation
incident. Paste the block below into a fresh session run from the repo root.)

---

You are a senior quant researcher working in the eqidv2 5-min intraday repo (run from the
repo root; all train/test tooling is in `Train_and_Test/`). Goal: aggressively iterate each
setup's indicators, non-indicator conditions, filters, and pre-filters to push **TRAIN
profit factor toward >2 — but only keep edges that survive an honest out-of-sample TEST.**
PF>2 that does not hold OOS is worthless; do not deliver it as a win.

## NON-NEGOTIABLE GUARDRAILS (learned the hard way — violating these wastes the run)

1. **Anti-overfit is the whole game.** Do NOT optimize raw in-sample PF (the `maxpf`
   objective produced TRAIN PF=∞ → TEST≈0 on 16/16 setups). Use the **`band`/robust
   objective**: reach the PF target with the MOST trades, judged on the WORSE of the two
   train halves. A config is ACCEPTED only if, out of sample:
   `test PF ≥ 1.30` AND `test day_block_p < 0.10` AND `test/train PF ratio ≥ 0.55` AND
   `test trades ≥ 8` AND `train trades ≥ 20`. Flag anything that needs a >2-term gate,
   collapses when any single term is dropped, or concentrates >40% of test PnL in one day.

2. **Mind the candidate basis (this silently inverts conclusions).** In the unified pool,
   `readmit` setups are scored on raw candidates = their true live basis (FAITHFUL).
   `native*` setups are scored raw/ungated = a pessimistic firehose (live filters them
   through v8/research first). NEVER accept/reject a native setup on raw numbers alone —
   confirm with the **v11 conf backtest** (`avwap_5min_ID_v11_backtesting.py --mode
   historical_all_available --selected_strategy_profile final_setup_conf --workers 8`).

3. **Fix the data BEFORE tuning, or the holdout is fake.** Check the pool manifest
   `date_max` and per-setup test counts first. Known staleness to repair: tier-c CSVs end
   ~05-29, raw cleanpool ~06-10, the live-gated pool frozen ~Jan-28. Regenerate
   (`regenerate_conf_tier_c_sources.py` + a v11 historical raw run) and
   `build_unified_pool.py` so TEST lands on days that actually have candidates. Pin the
   window explicitly (e.g. `--train_start/--train_end/--test_start/--test_end`).

4. **The truest holdout is the live paper book**, not any backtest. Always cross-check with
   `Train_and_Test/live_paper_holdout.py`. (Reality check: setups with backtest test-PF
   6.88 / 3.82 lost money live — P_PDH, L_RS_LEADER — and were demoted.)

5. **Live-feed safety.** Heavy scanner/backtest/tuner jobs can starve the live 5-min feed
   and silently kill signal generation (this crashed the live stack on 2026-06-22). Run all
   heavy jobs **after market close (>15:30 IST)**, **≤8 workers**, one heavy job at a time;
   verify the feed heartbeat before/after. Never run the universe scanner or v11 backtest
   during market hours.

6. **Segregation + safety of record.** Keep ALL new scripts/outputs/reports in
   `Train_and_Test/`. The shared/live core (`final_setup_conf.py`,
   `eqidv2_final_conf_live_bootstrap.py`, the live scanner, `eqidv2_v11_live_overlay.py`)
   stays in repo root (live imports it in place) — never move it. Never edit the live
   `final_setup_conf.py` without showing a reviewable diff and getting explicit sign-off;
   it is the single source of truth for both v11 backtest and v7 live.

## TOOLING (reuse, don't rebuild)
- `build_unified_pool.py` — one option-(i)-correct candidate pool + manifest.
- `setup_train_test.py --family <X> [--objective band] [--max_mask_terms N --max_premom_terms M --fine_quantiles]` — honest per-family search; `--approve` writes root conf (review-gated).
- `aggressive_pf_tuner.py` — deep multi-restart search across all 40 pre-momentum + full mask universe; use as a hypothesis generator, but RE-JUDGE every proposal under the robust gate in #1 (its maxpf verdicts overfit by design).
- `train_test_conf.py` — evaluate the existing book's real gates, no re-search.
- `live_paper_holdout.py` — real live OOS per setup.

## PER-SETUP DEEP ANALYSIS (do every setup independently, full analytical depth)
For each setup: (1) explain current logic in plain English; (2) why it wins; (3) why it
loses + false-positive patterns; (4) missing confirmation filters; (5) regime/condition
profile — trending vs reversal, high-vol, gap days, first-hour/midday/last-hour,
high/low-ATR, above/below-VWAP. Then run aggressive experiments across: RSI ranges, ADX
min/max, ATR filters, volume-spike & relative-volume thresholds, VWAP/AVWAP distance,
candle body/range/wick-rejection, breakout/reclaim confirmation, prior-candle strength,
PDH/PDL, opening-range, gap, time-of-day, liquidity/price/spread, daily-change, trend
alignment, multi-timeframe confirmation, cooldown/duplicate-entry, and SL/target/trailing
alternatives. Each change must have a logical reason — no cosmetic tweaks.

## REPORT PER EXPERIMENT
setup; exact parameter change; reason; TRAIN {PF, win%, avg win, avg loss, expectancy,
maxDD, n}; TEST {PF, win%, avg win, avg loss, expectancy, maxDD, n}; decision
(accept / reject / keep-for-more-testing) with the overfit-check rationale.

## FINAL OUTPUT (sections A–G)
A. Executive summary (best improved / keep / modify / disable / needs-more-data; was PF>2
   reached in train; did it survive test). B. Setup-wise deep analysis (logic, strengths,
   weaknesses, loss causes, best experiments, rejected experiments, final params, train-vs-
   test). C. Clean parameter-change table (setup, old, new, reason, train-PF impact,
   test-PF impact, trade-count impact, decision). D. Overfitting check per accepted change
   (logically justified? too narrow? trade-count collapse? test held? live-safe?).
   E. Final recommended logic as clean per-setup pseudo-code. F. Production notes (files/
   functions to edit, logs to add, daily metrics to monitor, degradation warning signs).
   G. Next-iteration plan (what to test next, what data to add, which regimes to isolate,
   which setup gets priority).

Be aggressive in experimentation, conservative in acceptance. Prioritize logic that holds
in BOTH train and test over any high-but-fragile training PF. Net of NSE intraday costs
throughout. Start by reporting the pool freshness + per-setup train/test counts so I can
see the holdout is real before you tune.
