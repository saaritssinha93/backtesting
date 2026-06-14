# New Setup Research — Two High-Conviction Candidates (v11)
*Analyst design note. Generated 2026-06-12. STATUS: designed + engine built — NOT yet run
(deferred per user + market-hours feed protection). Validate before any promotion.*

> Goal: one NEW **long** and one NEW **short** intraday (5-min) setup, each built on a real
> microstructure edge **not already in the A–T catalog**, combining the best indicators +
> non-indicators + filters, and wired to use the two ingredients that won repeatedly across the
> A–T sweep: a **pre-entry momentum/ADX gate** (the single most reliable edge-maker — it salvaged
> G, L_DOUBLE_BOTTOM, T_SHORT, and is the core of D) and a **day-concentration-safe** structure
> (so the edge is a daily edge, not a single-crash-day artifact like the S_MACD trap).

These are **hypotheses with strong priors**, NOT confirmed edges. "Highly profitable" can only be
claimed after the same anti-overfit validation that gated everything else (see §4). I will promote
to `final_setup_conf.py` ONLY if a candidate clears: train PF ≥ 2, test PF ≥ 1.3, day-block p < 0.10,
contiguous/monotonic sensitivity, both train halves positive, ≥70% months positive, top1day ≤ ~50%.

---

## 1. NEW LONG — `L_RS_LEADER_VWAP_HOLD`  (RS-Leader VWAP-Hold Continuation)

### Edge thesis (why it should pay)
Intraday, institutions accumulate their highest-conviction names and **defend VWAP** (their average
fill). A stock that is a clear **relative-strength leader** (rising while the index is flat/red),
pulls back to *test* VWAP, **holds it** (buyers step in at the institutional cost line), and resumes
up — is front-running the VWAP defense in a leader. This is the *continuation* analogue of a reclaim:
the stock never loses VWAP; it dips into it and bounces.

**Why it is NEW (vs catalog):**
- `B_AVWAP_RECLAIM_REVERSAL` reclaims VWAP **from below** (a reversal). This **holds from above** (a
  continuation) — opposite structure.
- `D_EMA20_BOUNCE` bounces off EMA20; this defends **VWAP** *and* requires **RS-leadership**.
- **No catalog setup uses relative-strength leadership (rs_pct) as the PRIMARY trigger.** Here it is
  the first gate, not a side-filter.

### Detection (5-min signal bar) — indicators + non-indicators
| Class | Condition | Rationale |
|---|---|---|
| Leadership (non-ind) | `rs_pct ≥ +0.75` **and** `stock_ret_from_open ≥ +0.30%` | a genuine leader, outperforming NIFTY |
| Uptrend stack (ind) | `close > EMA20`, `EMA20 ≥ EMA50`, `EMA20_slope_3 > 0` | established short-term uptrend |
| VWAP test+hold (struct) | `low ≤ VWAP + 0.30·ATR` **and** `close > VWAP` | wicked into VWAP, closed back above (defense) |
| Resumption (price action) | `close > open`, `close_loc ≥ 0.60`, `close > prev_bar_high` | strong green bar resuming up |
| Participation (non-ind) | `vol_ratio ≥ 1.3` | buyers showed up on the bounce |
| Trend quality (ind) | `ADX ≥ 20`, `50 ≤ RSI ≤ 72` | trending, bullish but not blow-off |
| Regime (non-ind) | `regime ∈ {BULL, TREND, NEUTRAL}` | don't buy leaders into a BEAR tape |
| Time (non-ind) | `09:45 ≤ signal ≤ 14:00` | skip open noise; room to EOD |
| Liquidity (common gate) | price ≥ 80, 5-min traded-value floor, ADV floor | tradeable names only |

### Pre-entry gate (1-min, TO BE TUNED — prior from the winning pattern)
`pre2_mom_r ≥ ~0.30` (genuine 1-min momentum into entry) **and** `sig5_adx_calc ≥ ~24` (trend
confirmed). The search will tune exact thresholds; mechanism = momentum-confirmed leader continuation.

### Exit (TO BE SWEPT — prior): **0.80 SL / 1.50–2.00 target** — let leaders run (momentum long).

---

## 2. NEW SHORT — `S_UPTHRUST_TRAP_FADE`  (Failed-High Upthrust / Bull-Trap Distribution)

### Edge thesis (why it should pay)
A failed breakout at a prior high **on a volume spike** with immediate **wick rejection** (close back
below the broken level) is a Wyckoff **upthrust / bull-trap**: breakout buyers are trapped and become
forced sellers, while the volume reveals **distribution** (size selling into the breakout). The trapped
longs' stops fuel the move down — historically one of the highest-*win* intraday shorts.

**Why it is NEW (vs catalog):**
- `B_FAILED_BREAKOUT_REVERSAL` keys only on the **opening-range** high; this is **any N-bar/swing high**.
- `S_DOUBLE_TOP_VWAP` needs **two equal highs**; this is a **single upthrust bar**.
- The unique signature = **new-high break + volume spike + long upper wick + close-back-below + RSI/MACD
  rollover** (the full distribution fingerprint). No catalog setup combines all of these.

### Detection (5-min signal bar) — SKELETON (loosened) + enriched features
**Design note:** the first cut ANDed 12 conditions and fired **0 candidates in 25 tickers** (e.g.,
`close < VWAP` on a bar that just made a new 10-bar high is near-contradictory). Fixed to a
**structural skeleton** (fires a workable population) with the softer discriminators **enriched onto
each candidate row** (`rsi`, `rsi3max`, `macd_hist`, `macd_hist_delta`, `upper_wick_pct`, `ema20_slope`,
`stock_ret`, …) so the robustness-first search tunes them as gates rather than hard-coding them.
After the fix: **699 short candidates / 25 tickers** (630 train / 69 test) — workable.

**Detection skeleton (hard conditions):**
| Class | Condition | Rationale |
|---|---|---|
| Break & fail (struct) | `high ≥ rolling-10-bar-high[prior]` **and** `close < that level` | broke the high, closed back below |
| Rejection (price action) | `upper_wick_pct ≥ 0.30`, `close_loc ≤ 0.45`, `close < open` | upper wick + weak red close |
| Participation (non-ind) | `vol_ratio ≥ 1.5` | distribution volume |
| Laggard (non-ind) | `rs_pct ≤ +0.50` | not a strong leader |
| Regime (non-ind) | `regime ∈ {BEAR, TREND, NEUTRAL}` | don't fight a strong BULL up-tape |
| Time (non-ind) | `09:45 ≤ signal ≤ 14:30` | |
| Liquidity (common gate) | price ≥ 80, traded-value floor, ADV floor | |

**Enriched tunable features (the search gates on these):** `rsi3max ≥ ~65 & rsi < 60` (overbought
rollover), `macd_hist_delta < 0` (MACD decel), `vwap_dist_atr ≤ ~0` (losing VWAP), tighter
`upper_wick_pct`/`vol_ratio`, plus the 1-min pre-entry momentum/ADX gate.

### Pre-entry gate (1-min, TO BE TUNED)
A downward-thrust / weak-entry confirmation: e.g., `pre3_close_pos ≤ ~0.4` (entering on the lows) +
`sig5_adx_calc ≥ ~24`. The search tunes direction/threshold.

### Exit (TO BE SWEPT — prior; shorts revert fast): **0.80 SL / 1.00–1.25 target** (tight).

---

## 3. Indicator / non-indicator coverage (the "best of" mix)
- **Indicators used:** VWAP, EMA20/50 (+ slope), ATR, ADX, RSI (level + 3-bar max), MACD-Hist
  (deceleration), Bollinger-adjacent via wick%/range, plus the 1-min pre-entry momentum/ADX gate.
- **Non-indicators used:** relative strength (rs_pct, the *primary* long trigger), volume ratio,
  upper-wick %, N-bar/swing high levels, intraday return from open, market regime, time-of-day,
  liquidity floors.
- Each setup is a **confluence** (structure + indicator confirmation + non-indicator filter + gate),
  not a single trigger — which is what separated the keepers from the rejects in the A–T sweep.

## 4. Validation plan (identical discipline to the A–T sweep)
1. **Scan** the 5-min universe → candidates for both setups (`new_setups_scan_v11.py`, mode `scan`).
2. **Build** entry→EOD 1-min paths, NET of `nse_intraday_costs`.
3. **Aggressive + robustness-first search** (greedy + 2-term exhaustive + 40–60k random + forced
   momentum/ADX grid, exit co-optimized) to tune the pre-entry gate + exit.
4. **Anti-overfit battery:** train (Nov 2025–Apr 2026) / test (May–Jun) split; **day-block bootstrap**
   (p < 0.10); **threshold-sensitivity** (must be contiguous/monotonic, not a knife-edge); **both train
   halves** positive; **monthly** ≥70% positive; **day-concentration** (`top1day ≤ ~50%` — the S_MACD
   guard); **term drop-out** (no fragile multi-term overfit, the PRESSURE guard).
5. **Promote** to `final_setup_conf.py` ONLY on a clean pass; otherwise honest reject + research-watch.

## 5. Execution note (why not run now)
Deferred per user instruction AND to protect the live v7 5-min feed (heavy scans during market hours
starve the 8-partition feed → silent signal death — see `project_v7_feed_starvation_failure_mode`).
**Recommended run window: after 15:30 IST (post-close)**, workers ≤ 8.
`new_setups_scan_v11.py` is built and ready; **dry-run 1 day first** (`--dry-run`) to confirm the
detections fire before the full scan. Also still queued (separately): the OOS-only portfolio run and
wiring `final_setup_conf.py` into the v11 backtester.
