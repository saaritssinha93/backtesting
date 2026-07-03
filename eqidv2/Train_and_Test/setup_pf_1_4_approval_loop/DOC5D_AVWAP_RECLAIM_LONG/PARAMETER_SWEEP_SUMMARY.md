# DOC5D_AVWAP_RECLAIM_LONG (LONG) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-01. Research-only. Optimizer: Optuna TPE (installed)._

> **This file = the 15-bps track on the ORIGINAL doc5 pool (tune the fixed detector).**
> The subsequent **detector-reinvention track @ 5 bps** (redesigned rules + full-universe scan) is in
> **[REINVENTION_RESULTS.md](REINVENTION_RESULTS.md)**. Both tracks REJECT; see
> APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md.

Search machinery: `setup_pf_1_4_approval_loop/_engine/pf_band_fitval_loop.py` — optimises ONLY on
FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) − λ·|FIT_PF−VAL_PF|` (reward tents at
PF 1.70, penalises overshoot), confirms the single best config on full TRAIN, scores TEST once.
Quantile thresholds are drawn from **TRAIN only** (never TEST). Costs modelled at 15 bps/leg
(primary) and 5 bps/leg (cross-check). Entry = next 1-min open; exits resolved on 1-min OHLC to EOD.

**5 Optuna studies × 500 trials = 2,500 evaluated configs.** Studies varied term budget, gap
penalty, and seed to give the search maximum room:

| Study | mask×pm | λ | seed | best FIT/VAL score | full-TRAIN n/PF | TEST n/PF | verdict |
|---|---|---|---|---|---|---|---|
| canonical | 1×1 | 0.80 | 7 | 0.748 | 26 / **0.767** | 5 / 0.00 | REJECT |
| m2p1_s7 | 2×1 | 0.80 | 7 | 0.907 | 14 / 1.629 | 4 / 0.00 | REJECT |
| m1p2_s11 | 1×2 | 0.80 | 11 | 0.743 | 13 / 0.769 | 2 / 0.42 | REJECT |
| m2p2_s23 | 2×2 | 0.80 | 23 | 0.481 | 16 / 0.581 | 3 / 0.30 | REJECT |
| m2p1_gl04_s41 | 2×1 | 0.40 | 41 | 1.021 | 16 / 1.086 | 3 / 0.00 | REJECT |

**Zero of 2,500 trials** produced FIT_PF ≥ 1.30 **and** VAL_PF ≥ 1.30 with ≥ 6 trades in each half.
No config is even positive on both TRAIN halves. Sweep artifacts: `_sweeps/<study>/DOC5D_AVWAP_RECLAIM_LONG/`.

---

## Knobs iterated and the best stable range found

### 1. Indicator-based mask filters (feature `op` quantile-threshold, `≥`/`≤`)
Searchable TRAIN features: `quality_score, ranker_score, rs_pct, vol_ratio, atr_pct, vwap_dist_atr,
signal_range_pct` (+ pre-momentum below).

| Feature | Range tested (TRAIN q0.1–q0.9) | Best behaviour | Verdict |
|---|---|---|---|
| `quality_score` | ≥ q0.1…q0.9 | strongest single lever: `≥78.05` (~q0.5) → TRAIN n=26 PF 0.767; tighter `≥97` → n=11 PF 1.1 | best-of-a-bad-lot; still < band, collapses OOS |
| `ranker_score` | ≥ q0.7…q0.9 | `≥84.8` → n=13 PF 0.77; `≥87.8` → n≈11 | over-tightens, OOS collapse |
| `vol_ratio` / `atr_pct` / `signal_range_pct` | full grid, `≥`/`≤` | never lifted min(FIT,VAL) PF above ~0.8 | rejected — no edge |
| `vwap_dist_atr` | `≤` grid (near-VWAP) | no material PF lift | rejected |
| `rs_pct` | `≥` grid (stronger leader) | no material PF lift | rejected |

### 2. Non-indicator price-action mask filters
Searchable: `body_pct, close_loc, upper_wick_pct, lower_wick_pct, wick_skew_pct`.

| Feature | Range tested | Best behaviour | Verdict |
|---|---|---|---|
| `close_loc` | `≥0.60…0.95` | `≥0.895` (very strong close) → n=16 PF 1.086 but dayDom 3.38 | overfit pocket, TEST 0.00 |
| `lower_wick_pct` | `≤` grid | `≤0.038` (no lower tail) paired w/ quality → n=14 PF 1.629 | overfit pocket (n=14), TEST 0.00 |
| `body_pct` | `≥0.78` | n=16 PF 0.581 | rejected |
| `upper_wick_pct` / `wick_skew_pct` | full grid | no lift | rejected |

### 3. Pre-momentum filters
Searchable: `pre1_adx, pre3_close_pos, pre3_range_r, pre5_mom_r, pre_entry_momentum_score,
sig5_adx_calc, sig5_rsi_dir, sig5_vol_ratio20`.

| Feature | Range tested | Best behaviour | Verdict |
|---|---|---|---|
| `sig5_vol_ratio20` | `≥1.68…2.65` | appears in 2-term pockets (n=16) | contributes only inside overfit pockets |
| `pre5_mom_r` | `≥−0.03…` | marginal | rejected |
| `pre1_adx`, `sig5_adx_calc`, `pre_entry_momentum_score`, `sig5_rsi_dir`, `pre3_*` | full grid | none lifted both FIT & VAL into band | rejected |

### 4. Guards
`min_slot ∈ {09:30,09:45,10:00,10:30,11:00}`, `max_slot ∈ {12:00,12:30,13:00,14:00,14:30}`,
`top_n ∈ {0,1,2,3}`, `max_positions ∈ {10,20}`, `daily_loss_rs ∈ {0, 4000}`.
- Best trials favoured `top_n=2` and `daily_loss_rs=4000` (a portfolio circuit-breaker on a
  losing book) + wide time window (09:30–13:00). These *reduce the loss* but never create an edge —
  the daily-loss stop is cosmetic on a PF-0.16 base.
- Tightening `min_slot`→10:00 (skip open) or shrinking `max_slot` did not open a positive window.

### 5. Exit / SL / target (grid SL {0.50,0.70,0.85,1.00,1.10,1.20,1.50} × Tgt {0.60,0.80,1.00,1.25,1.50,2.00,2.50})
- The doc default **0.70/1.25** is the single worst region: 65% stop-out, 6% target-fill on TRAIN.
- The band search drifted to **wide SL + wide target (SL 1.10–1.50 / Tgt 2.00–2.50)** to survive the
  21%-win base — top-50 canonical trials *all* converged on SL 1.1 / Tgt 2.5. Even there, best
  full-TRAIN PF is 0.767. Wide brackets convert SL exits into EOD exits (SL/TGT/EOD shift) but the
  book stays net-negative.
- Small SL / small target (0.85/0.80, 1.0/0.8) tested in the 2-term sweeps → n=16 PF 0.58–1.09,
  still short of band and OOS-negative.

---

## Best stable range per knob (what "survives" on FIT/VAL, still not enough)

- **exit:** SL 1.1 / Tgt 2.5 (only region that keeps min(FIT,VAL) PF ≈ 0.77; nothing reaches band).
- **mask:** `quality_score ≥ ~78` (median split) — the single least-bad filter, PF 0.767 @ n=26.
- **guard:** `top_n=2`, `daily_loss_rs=4000`, window 09:30–13:00 (loss-limiting, not edge-creating).
- **pre-momentum:** none stable — every pm term only helps inside sub-16-trade overfit pockets.

## Rejected ranges / overfit-risk values (do NOT trust)

- Any config with **TRAIN n < 20** (13–16-trade pockets): `quality_score≥78 & lower_wick≤0.038`
  (PF 1.63), `close_loc≥0.895 & sig5_vol20≥2.65` (PF 1.086). All have dayDom/symDom ≥ 0.71 (or 9.99)
  and TEST PF 0.00 — **classic single-pocket overfit**, exactly the "force TRAIN PF up ⇒ OOS
  collapse" wall.
- `quality_score ≥ q0.9` / `ranker_score ≥ q0.9`: cuts to n≈11, PF still ~1.1 — noise.
- The 5-bps view lifts every window's PF (TRAIN 0.16→0.42, TEST 0.34→0.46) but **none crosses 1.0** —
  the setup is not a cost-model victim; the edge simply is not there on this window.

**Conclusion:** no knob or combination lifts TRAIN into [1.30,1.70] while keeping n ≥ 20 and a
non-negative TEST. The band-vs-count relationship is a hard wall: **n ≥ 20 ⇒ PF ≤ 0.77;
PF ≥ 1.30 ⇒ n ≤ 16, concentrated, TEST → 0.** No candidate qualifies. See CANDIDATE_CONFIGS.md
(empty) and APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md (NO).
