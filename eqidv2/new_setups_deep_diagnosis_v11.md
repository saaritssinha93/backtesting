# New Setups (L / S / N) — Deep Diagnosis (v11)
*Aggressive-iteration + anti-overfit sweep, same standard as the A–T families. Generated 2026-06-12.*

> Four candidate setups run through the **identical** robustness-first pipeline used for the
> `final_setup_conf` book: clean-pool candidates → entry→EOD 1-min paths + pre-entry-momentum
> features → greedy + exhaustive 2-term + 60k random search (exit co-optimized) → anti-overfit
> battery (day-block bootstrap, day-concentration, train-halves, threshold sensitivity, term
> drop-out, monthly). **Train 2025-11-01..2026-04-30 / test 2026-05-01..2026-06-10. NET of cost.**
>
> Engines: `new_setups_scan_v11.py` (L/S structural scan), `new_setups_overlay_gen_v11.py` (N_
> clean-pool overlay relabel), `new_setups_search_v11.py` (search), `new_setups_validate_passers.py`
> (battery). Cache: `v11_newsetups_paths_cache.pkl`.

---

## 1. Executive summary — the new setups contribute **1** setup

| Setup | side | source | n (tr/te) | ungated tr/te | Verdict |
|---|---|---|---|---|---|
| **S_UPTHRUST_TRAP_FADE** | SHORT | structural scan | 2510 (1999/511 → 33/12 gated) | 0.55 / 0.68 (−Rs350k) | **PROMOTE** — momentum/rollover gate, train 1.67 / test 2.76, **p 0.001**, clean ACCEPT_GATE pass |
| N_MORNING_ZERO_WICK_SHORT | SHORT | clean-pool overlay | 672 (582/90 → 36/8 gated) | 1.07 / 0.73 (+Rs6k) | reject → research-watch — exit-fragile, knife-edge rs_pct, 331-config multiple-testing, E_ORB churn source |
| L_RS_LEADER_VWAP_HOLD | LONG | structural scan | 236 (231/**5**) | — | reject — **test-starved** (5 test < 8); no qualifying configs |
| N_HIGH_RS_EMA_BOUNCE_LONG | LONG | clean-pool overlay | **12** (10/2) | — | reject — **too sparse** (insufficient paths); confirms prior "WAIT" verdict |

Both LONGs reject on **sample** (the May–Jun test window is short and both long detections are
selective). Both SHORTs have workable populations; only **S_UPTHRUST_TRAP_FADE** survives the
anti-overfit battery.

### 1.1 🟢 The keeper — S_UPTHRUST_TRAP_FADE × momentum/rollover gate
> **`ema20_slope ≤ −0.0059` AND `pre2_mom_r ≥ 0.128` AND `rsi3max ≥ 50.66`**, exit **0.70 / 0.80**
> TRAIN **1.67** [h1 1.48 / h2 2.08] · TEST **2.76** (n12, d6, **p 0.001**) · full 1.90, +Rs 7,316
> **top1day 34% · all 3 terms load-bearing · significant at 3 of 5 exits (the 0.80-target family)**

Mechanism: a failed-high upthrust / bull-trap short pays only when the stock is **already rolling
over** — EMA20 slope down (`≤−0.0059`), a genuine recent down-thrust into the trade (`pre2_mom_r≥0.128`,
risk-normalised), and the failed high came off a **local RSI peak** (`rsi3max≥50.66` = it trapped
late longs). The tight 0.80 target scalps the distribution leg. Ungated the setup is a −Rs350k churn
loser; the gate creates the entire edge.

---

## 2. Setup definitions
- **L_RS_LEADER_VWAP_HOLD** (LONG, structural): RS-leader VWAP test-and-hold continuation —
  `rs_pct≥0.75 & stock_ret≥0.30 & close>EMA20≥EMA50 & EMA20_slope>0 & low≤VWAP+0.30ATR & close>VWAP &
  close>open & close_loc≥0.60 & close>prev_high & vol_ratio≥1.3 & ADX≥20 & 50≤RSI≤72 & regime≠BEAR`.
- **S_UPTHRUST_TRAP_FADE** (SHORT, structural): failed-high upthrust skeleton — `high≥10-bar-high &
  close<level & upper_wick≥0.30 & close_loc≤0.45 & close<open & vol_ratio≥1.5 & rs_pct≤0.50 &
  regime≠BULL`, with RSI/MACD/wick/EMA-slope ENRICHED for the search to gate.
- **N_HIGH_RS_EMA_BOUNCE_LONG** (LONG, overlay): `D_EMA20_BOUNCE` LONG & `body_pct≥0.60` & `rs_pct≥4.0`.
- **N_MORNING_ZERO_WICK_SHORT** (SHORT, overlay): `{S_BB_SQUEEZE_SHORT, E_ORB_BREAKOUT_SHORT,
  D_EMA20_REJECTION, E_VWAP_BAND_FADE}` SHORT & 10:01–11:30 & `lower_wick≤0.01%` & `quality_score≤100`.

> **Provenance note.** L/S come from the standalone structural scan of the live 5-min data
> (`stocks_indicators_5min_eq_live2`, span 2025-06 → 2026-06-12). The two N_ setups are **relabels of
> existing-setup candidates from the clean pool** (`outputs_ID_v11_cleanpool` raw_candidates,
> Nov 2025 → Jun 2026) — per the user's chosen methodology. The scanner ALSO emits a structural
> *approximation* of N_; those rows were **dropped** so the two methodologies never mix.

---

## 3. S_UPTHRUST_TRAP_FADE — the keeper (full battery)
Ungated: train 0.55 / test 0.68, −Rs350,555 (heavy churn loser). The 3-term gate transforms it.

- **Term drop-out — all three load-bearing** (exit 0.7/0.8): full gate train 1.67; drop `ema20_slope`
  → 0.53 (floods 558 train rows), drop `pre2_mom_r` → 1.05, drop `rsi3max` → 0.99. No single term
  carries it; the edge is the **conjunction**.
- **Threshold sensitivity — monotone, not a knife-edge:** `pre2_mom_r` rises cleanly 0.08→0.20
  (train 1.15→1.83, test 1.59→3.26); `rsi3max` rises 45→53 (train 0.91→2.16); `ema20_slope` is flat
  across −0.003…−0.012 (the other two terms already imply a steep down-slope). No cliff.
- **Exit-robust at the 0.80 target:** 0.7/0.8 (test 2.76, **p 0.001**, top1d 34%), 0.9/0.8 (test 2.23,
  p 0.009, top1d 40%), 1.1/0.8 (test 3.92, p 0.017, top1d 50%) — significant at 3 exits, all 0.80-target.
  Widening to 1.0 (0.7/1.0 p 0.104) or tightening the stop to 0.5 (p 0.189) weakens it → the edge is a
  **0.80-target scalp**.
- **Both train halves positive** at every exit (h1 1.36–1.75 / h2 1.55–2.08).
- **Day-spread good:** top1day 34–50% — not a single-day artifact.
- **Monthly:** 7/11 months positive (64%) — below the 70% guideline, BUT every losing month is n≤3
  (Jul n2, Nov n1, Jan n3, Mar n3); all larger-sample months (Aug n9, Sep n5, Feb n5, May n12) positive.
- **Clean ACCEPT_GATE pass** at 0.7/0.8: train 1.67∈[1.5,2.0] ✓ · test 2.76≥1.30 ✓ · p 0.001<0.10 ✓ ·
  test/train ratio 1.65≥0.55 ✓ · test n 12≥8 ✓. (Stronger numeric pass than several current book members.)
- **Caveats:** (1) test = 12 trades over **6 days, all May** (no gated June signals) — thin, single-month
  OOS; (2) the scanner's **train history is longer than the standard window** (it spans 2025-06→2026-04,
  not Nov→Apr) because L/S use the live 5-min feed, not the clean pool; (3) the gate features
  `ema20_slope` & `rsi3max` are **scanner-enriched** — to wire into v11/live they must be enriched onto
  every candidate row (same kind of wiring/coverage caveat as L_DOUBLE_BOTTOM's raw-pool note).
- **Verdict: PROMOTE as STRONG PROBATION** (with the thin-May-test + enrichment-wiring caveats).

## 4. N_MORNING_ZERO_WICK_SHORT — reject → research-watch
Best config: `rs_pct≥−0.34 & sig5_adx_calc≥21.4 & pre2_mom_r≥0.17`, exit 1.1/2.0 → train 2.13
[2.13/2.13] / test 2.37 (n8, **p 0.097**), monthly 6/8 (75%). Looks decent but **fails the bar**:
- **Multiple-testing:** the search returned **331** train-PF≥2 configs on 672 candidates — a p≈0.09 hit
  is expected by chance. Both passing exits have train PF (2.02–2.13) **over the 2.0 overfit ceiling**.
- **Exit-fragile:** significant at only 2 of 5 exits (1.1/2.0 p 0.097, 1.1/1.25 p 0.091); the other
  three **collapse** in test (0.7/1.5 → 0.86; 0.9/1.5 → 1.13, **top1day 366%**; 0.9/2.0 → 1.27,
  **top1day 225%**) — i.e. a single day carries those.
- **Knife-edge `rs_pct`:** loosening to ≥−0.60 → test 0.86 (fails); tightening to ≥−0.22 → test n=1.
  The OOS pocket is one narrow band. `sig5_adx_calc` is weak (drop-out test stays fine without it).
- **Churn source:** 605/672 candidates are `E_ORB_BREAKOUT_SHORT`, the documented cost sink
  (`final_setup_conf` research-watch: E_ORB best-found train 1.04/test 0.94). Prior overlay research
  already **REJECTED** this setup after June reversed it.
- **Verdict: REJECT** (record in research-watch with best-found config + re-validation trigger).

## 5. L_RS_LEADER_VWAP_HOLD — reject (test-starved)
236 candidates but only **5 in the test window** (vs 231 train). The detection (`rs_pct≥0.75`
leadership + 11 ANDed conditions) is ~100× more selective than S_UPTHRUST. Below `MIN_TEST=8` →
**no qualifying configs**. Cannot be validated honestly on this OOS window. Not a verdict on the
idea — a verdict on sample. Re-run when more forward data accrues.

## 6. N_HIGH_RS_EMA_BOUNCE_LONG — reject (sparse)
Only **12** deduped candidates (10 train / 2 test) on the clean pool — the `rs_pct≥4.0` filter on
`D_EMA20_BOUNCE` is brutal (its 25-train prior result came from a *different, larger* probe pool).
Below `MIN_TRADES=25` → **insufficient paths**. This independently reproduces the earlier
`V11_NEW_SETUP_RESEARCH_2026-06-12` verdict: **WAIT for more data**.

## 7. Family scorecard
**New setups contribute 1 setup** — `S_UPTHRUST_TRAP_FADE` (momentum/rollover gate, STRONG PROBATION,
clean ACCEPT_GATE pass; thin-May-test + enrichment-wiring caveats). The recurring cross-family
discriminators did the work again: **term drop-out** (proved S_UPTHRUST's 3-term conjunction is real,
exposed N_MORNING's weak adx term), **day-concentration** (killed 3 of N_MORNING's exits), and
**multiple-testing awareness** (331 N_MORNING configs). If promoted, the book would be **10 active
+ 12 research-watch**. **Promotion requires user review** (see `new_setups_v11_candidate_config.json`).

---

## 8. HONEST SALVAGE of the 3 rejects (round 2)
*User: "use full power research and all possible ways to accept it honestly." Same acceptance bar — no
goalpost move. The diagnosis: the 3 rejects were starved/poisoned by a PRE-IMPOSED constraint, not by a
missing edge. Honest fix = remove the bad constraint, let the search find the real gate, re-run the FULL
battery. Engines: `l_rs_leader_loose_scan_v11.py`, `new_setups_salvage_gen_v11.py`,
`new_setups_salvage_search_v11.py`, `new_setups_validate_salvage.py`.*

**Result: all 3 rejects SALVAGE to CONDITIONAL ACCEPT (probation)** — each survives the full battery, but
each winning gate differs from the original name/thesis, so each must be **HONESTLY RELABELLED**.

| Reject | what starved it | honest fix | salvaged config | verdict |
|---|---|---|---|---|
| L_RS_LEADER_VWAP_HOLD | tight 11-AND skeleton (rs≥0.75…) → 5 test | loosen skeleton, search the gate | `stock_ret≤0.97 & atr_pct≥0.0022 & lower_wick≥0.059`, 1.1/2.0 → train **1.89** [1.84/1.99] / test **2.77** (n12, **p0.055**), 3/5 exits, **83% months**, top1d 55%, **train in-band** | **CONDITIONAL ACCEPT** |
| N_HIGH_RS_EMA_BOUNCE_LONG | `rs≥4.0` at the ~99th pct → 12 total | drop hard rs filter, full D_EMA20_BOUNCE pool, search | `sig5_adx_calc≤14.2 & close_loc≤0.95 & rs≥0.20`, 0.5/1.25 → train **1.87** / test **3.11** (n8, **p0.070**), 4/5 exits, **100% months**, top1d 42% | **CONDITIONAL ACCEPT** |
| N_MORNING_ZERO_WICK_SHORT | 90% from E_ORB churn sink | exclude E_ORB, search the non-churn pool | `sig5_adx_calc≥21.5 & signal_minute≥710 & atr_pct≥0.0021`, 0.9/2.0 → train **2.74** [2.72/2.77] / test **4.19** (n8, **p0.064**), **5/5 exits**, **86% months**, top1d 50% | **CONDITIONAL ACCEPT** |

### 8.1 The honest relabels (what the data actually supports)
- **L_RS_LEADER → "DON'T-CHASE VWAP-DIP CONTINUATION".** Drop-out: `stock_ret≤0.97` is THE edge (drop→
  train 1.09/test 0.68 collapse) — buying the leader BEFORE it over-extends, not the RS leadership itself
  (rs_pct isn't in the winning gate). `lower_wick≥0.059` DOES capture the original VWAP-test dip-and-recover.
  All 3 terms monotone (not knife-edge); train PF in [1.5,2.0] at every passing exit (cleanest of the three).
  CAVEAT: Feb-2026 was a real losing month (pf 0.03, −Rs5,341); test n12/6d; significant only at wide targets.
- **N_HIGH_RS → "LOW-ADX MEAN-REVERSION BOUNCE".** Drop-out: `sig5_adx_calc≤14.2` is THE edge (drop→0.65);
  `rs_pct` is the WEAKEST term (drop→1.87). So it is a D_EMA20 bounce that pays in QUIET/ranging (low-ADX)
  conditions, NOT a high-RS leader. 100% months positive, 4/5 exits. CAVEAT: the ADX gate is a CLIFF
  (≤14→test 2.82 but ≤17→test 0.99) — a low-ADX regime boundary, mechanically meaningful but sharp;
  train PF >2.0 at the high-target exit (use the in-band 0.5/1.25); test n8/5d.
- **N_MORNING → "LATE-MORNING NON-CHURN ZERO-WICK SHORT".** Drop-out: `signal_minute≥710` is load-bearing
  (drop→train 1.64) — the edge lives in the 11:50-12:00 slot, NOT the morning. `atr_pct` is droppable
  (2-term core = adx & late-slot). 5/5 exits significant, 86% months, even halves. CAVEAT: requires
  EXCLUDING E_ORB; train PF >2.0 (like G/L/T in the book); test n8/6d; it is a late-morning, not a morning, short.
- **N_MORNING_ZERO_WICK_SHORT (strict morning, non-E_ORB, n112): STILL REJECT** — every config day-concentrated
  (top1day 104–229%), p>0.20. The morning-window thesis genuinely has no edge; only the late slot does.

### 8.2 Honest framing
These are real, battery-surviving edges — but they are **PROBATION-grade with the same caveats as the rest
of the book** (thin tests n8–12, train-over-band for the two N_, the N_HIGH_RS ADX cliff, the L Feb loser),
AND **two of three only pass under a different name than requested** (a low-ADX bounce and a late-morning
short, not a "high-RS leader" and a "morning" short). The salvage did NOT manufacture an edge by loosening
the *validation*; it found edges by removing a wrong *pre-filter* and then applying the identical battery —
the strict-morning variant still fails, which proves the bar still bites. **Promotion is the user's call.**
