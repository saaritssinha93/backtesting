# C* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 days, ~1,200-ticker NSE universe). Generated 2026-06-11.*

> Method: C_OR_BREAKOUT is overlay-admitted (absent from the `profile=none` clean pool), so
> it was diagnosed from the **raw 5-minute detections** (12,664) in the clean-pool
> per-day `raw_candidates.csv`, with the vwap/atr entry guards applied (vwap_dist_atr≥2.0,
> atr_pct≤0.010), **sampled to 1,500** to bound compute, resolved at the fixed production
> exit (1.20/1.50) on 1-minute data, **net of statutory NSE cost**, with MAE/MFE and the
> pre-entry momentum gate evaluated on/off. C_OR_BREAKDOWN (24,714 raw) is a permanent
> SHADOW/blocked setup, assessed under its mirror guard. Engine: `validate_C_setups_filters.py`.

---

## 1. Executive summary

| Setup | Status | Diagnosis |
|---|---|---|
| **C_OR_BREAKOUT** (LONG) | **DEAD by bug, no edge underneath** | (a) zero live entries — detection window and entry-guard window are disjoint; (b) even on its real window it is a net loser (PF 0.7–0.8); (c) the pre-momentum gate is a no-op; (d) the 1.20/1.50 exit is badly mismatched to its MFE. |
| **C_OR_BREAKDOWN** (SHORT) | **Shadow-block justified** | net loser even gated (PF 0.72/0.92); keep blocked. |

**Three headline findings:**

1. **🔴 C_OR_BREAKOUT produces ZERO live entries — root-caused (the unresolved "zero-entry-row" bug, punchlist P2-26).** Its raw detections only begin at **10:55** (the v2 scan loop starts at bar index 20 = `VWAP_LOOKBACK`, ~100 min after the 09:15 open; min observed signal-minute = 655) and run to 14:30. The live entry guard requires **09:55–10:40** (`ENTRY_C_OR_BREAKOUT_MIN/MAX_SIGNAL_TIME`). **The two windows do not overlap → every C_OR_BREAKOUT signal is rejected by the time guard.** The stale comment that justified the guard ("Morning window 09:55-10:40: WR 71.2%, PF 1.71") predates the current `_eq_live2` OR/scan timing.

2. **The bug accidentally protected the book.** On its *real* firing window (with the vwap/atr guards), C_OR_BREAKOUT is a **net loser at every time window** (all-day PF 0.80/0.69; best cell 12:30–13:30 only reaches PF 0.98, still < 1). Enabling it by "fixing" the window — without an exit/edge redesign — would simply turn on losses.

3. **The pre-entry momentum gate is a no-op for C_OR_BREAKOUT.** Within the guard population: premom-ON PF 0.81/0.71, premom-OFF 0.80/0.69, premom-REJECTED 0.80/0.68. It splits the trades ~50/50 and **both halves lose identically** — the gate removes no bad-trade cluster. (Tested on/off explicitly, per request.)

**Cost basis:** net of NSE intraday cost. **Exit mode:** fixed 1.20/1.50 (C_OR_BREAKOUT), 0.70/1.30 (C_OR_BREAKDOWN). **Sample:** 1,500/setup of the guard population (representative; full guard pops are 10,960 / 22,048).

---

## 2. Current setup definitions
(Detection in `avwap_5min_ID_v2_backtesting.py` L709/716; exits in `avwap_5min_ID_v6_backtesting.py` L55/56; entry guards + pre-momentum + shadow-block in `avwap_5min_ID_v11_backtesting.py`.)

- **C_OR_BREAKOUT** (LONG, `opening_range_breakout`): `or_high` finite, long_struct (close>open & close_loc≥0.60), `close > VWAP`, `close > or_high`, `rs_pct > 0`, `vol_ratio ≥ 1.5`, regime≠BEAR. **Live entry guards:** signal 09:55–10:40, `vwap_dist_atr ≥ 2.0`, `atr_pct ≤ 0.010`. **Pre-momentum gate:** `sig5_adx_calc≥25, sig5_rsi_dir≥60, sig5_vol_ratio20≥1.5, pre2_mom_r≥−0.05`. Exit 1.20/1.50. v11 mask: "broad C_OR_BREAKOUT kept after holdout".
- **C_OR_BREAKDOWN** (SHORT, `opening_range_breakdown`): mirror — `or_low` finite, short_struct, `close < VWAP`, `close < or_low`, `rs_pct < 0.10`, `vol_ratio ≥ 1.5`, regime≠BULL. Exit 0.70/1.30. **Permanently shadow-blocked** (`ENTRY_SHADOW_SETUPS`).

---

## 3. Current train/test results

**C_OR_BREAKOUT — guard population, fixed 1.20/1.50, NET:**
| Window | TRAIN n / PF | TEST n / PF / win | net |
|---|---|---|---|
| Guard pop (all-day) | 1126 / 0.80 | 374 / 0.69 / 42% | −₹158,172 |
| **LIVE 09:55–10:40 (the guard)** | **0** | **0** | **0 — the bug** |
| corrected 10:55–12:00 | 306 / 0.65 | 110 / 0.54 / 40% | −₹94,424 |
| corrected 12:00–13:30 | 458 / 0.92 | 156 / 0.72 / 44% | −₹37,549 |
| late >13:30 | 401 / 0.82 | 123 / 0.81 / 41% | −₹35,033 |

**Pre-momentum ON vs OFF (within guard pop):**
| | TRAIN PF | TEST PF |
|---|---|---|
| premom OFF (all) | 0.80 | 0.69 |
| premom ON (pass) | 0.81 | 0.71 |
| premom REJECTED (what PM drops) | 0.80 | 0.68 |

→ **No separation. The gate is useless.**

**Time-of-day edge map (guard pop):** 10:55–11:30 PF 0.55 · 11:30–12:00 0.70 · 12:00–12:30 0.76 · **12:30–13:30 0.98 (win 45.8%, best)** · >13:30 0.81. Every window loses.

**C_OR_BREAKDOWN (mirror guard):** all-day PF 0.72/0.92; +market-falling 0.80/1.05; +regime-BEAR 0.80/1.02. Net loser; block justified.

---

## 4. Diagnosis

### 4.1 C_OR_BREAKOUT
**A. Entry-logic quality.** Intends: opening-range breakout (momentum continuation as price clears the OR high). In practice it is a **late, all-day break of a morning range** — the OR high is established over the first ~hour, and `close > or_high` then triggers from 10:55 onward, mostly **midday/afternoon**. So it is not a "morning" setup at all; the morning entry guard is a category error.
- **The decisive defect is the window contradiction** (§1.1) — it makes the setup unexecutable.
- **Beneath that, no edge:** PF 0.7–0.8 net on every window, win 42%, **mfe_R ≈ 0.6** (trades rarely reach even 0.6R favorable). The breakout doesn't follow through — by the time the morning OR high is cleared midday, the move is mature and mean-reverts.
- **vwap_dist_atr ≥ 2.0 guard:** barely filters (86% of detections already sit ≥2 ATR above VWAP) — i.e. these breakouts are *intrinsically* extended, which is *why* they fade. Like the B_AVWAP finding, "extended" is a tell for failure.
- **Exit mismatch:** SL 1.20% (wide) + Tgt 1.50% (≈1.25 R:R) on trades with mfe_R 0.6 → target unreachable, wide stop bleeds, lots of EOD-at-loss. immediate-fail only ~1% (slow fades, not fast stops).

**Verdict:** No tradeable edge as defined. The window bug should be *documented*, but the setup should **not** be enabled without a full redesign (window + exit + an extension cap), and even then the base rate (42% win, fading breakouts) is discouraging.

### 4.2 C_OR_BREAKDOWN
A SHORT opening-range breakdown — structurally the same late-break-into-a-mature-move problem, plus Indian-equity upside bias. Net loser even gated (PF 0.72 train). The `+market falling` / `+regime BEAR` filters lift *test* to ~1.0 but **train stays 0.80** — no stable edge. **The shadow-block is correct; keep it.**

---

## 5. Gate ladder — marginal value of each gate (the with/without test)
| Gate added | Effect |
|---|---|
| vwap_dist_atr ≥ 2.0 | near no-op (86% already pass); selects *extended* breakouts that fade |
| atr_pct ≤ 0.010 | liquidity/vol sanity; keeps most |
| **time 09:55–10:40** | **removes 100% (disjoint from detections) — the bug** |
| **pre-momentum gate** | **no-op — both halves lose identically** |
| corrected time windows | best (12:30–13:30) still PF 0.98 < 1 |

**Not one gate turns C_OR_BREAKOUT positive.** The problem is the setup's base rate, not the gating.

---

## 6. Exit review
mfe_R ≈ 0.6, immediate-fail ~1%, win 42% → the **1.20/1.50 exit is wrong**: target too ambitious, stop too wide, trades fade slowly. The single most worthwhile *experiment* (not a fix) is a **tight exit re-resolve** (e.g. SL 0.70 / Tgt 0.70–0.80, or a VWAP-loss / time stop) to see if a quick-scalp exit on the midday window salvages anything. But given a 42% win rate on a fading-breakout base, expectation is low. **Do not assume an exit tweak rescues it.**

---

## 7. Recommended changes
- **C_OR_BREAKOUT:** (1) **Document the window bug** and decide deliberately — either keep it disabled (status quo, accidentally protective) or, if pursued as research, redesign all three of {window→12:00–13:30, exit→tight, add an extension cap to reject the most-extended breakouts}. (2) **Remove the pre-momentum gate** (no-op). (3) Do **not** simply correct the time window — that enables a net loser.
- **C_OR_BREAKDOWN:** keep shadow-blocked.

---

## 8. Anti-overfit warnings
- The `+market falling` / `+regime BEAR` test-PF≈1.0 for C_OR_BREAKDOWN is **train-weak (0.80)** — a regime artifact, not edge. Do not act on it.
- C_OR_BREAKOUT's best window (12:30–13:30, PF 0.98) is still **< 1 and net-negative** — not a candidate.
- Sample is 1,500/setup (representative); the conclusions are about base-rate, which large samples make robust.

---

## 9. What NOT to do (yet)
- **Do not** "fix" the C_OR_BREAKOUT entry-guard window to enable it — it is a net loser on its real window.
- **Do not** keep the pre-momentum gate on C_OR_BREAKOUT — it does nothing.
- **Do not** unblock C_OR_BREAKDOWN — block is justified.
- **Do not** trust the C_OR_BREAKDOWN market-falling test bump — train contradicts it.

---

## 10. Final recommended next experiments (low priority — the family has no clear edge)
1. **[research]** Re-resolve C_OR_BREAKOUT (guard pop, 12:00–13:30) at tight exits (0.70/0.70, 0.70/0.80) + a VWAP-loss stop, to test the exit-mismatch hypothesis. *Expectation: marginal at best.*
2. **[research]** Add an extension cap (`vwap_dist_atr ≤ 3` or a from-OR-high distance cap) to reject the most-extended breakouts; re-test.
3. **[housekeeping]** File the window-bug root-cause against punchlist P2-26 so the stale 09:55–10:40 guard is corrected or the setup is formally retired.

**Net:** the C family contributes **nothing tradeable** today. C_OR_BREAKOUT is dead-by-bug and edge-less; C_OR_BREAKDOWN is correctly blocked. Nothing from C goes into `final_setup_conf.py`.

---

## 10b. Deep out-of-the-box iteration (exhaustive) — and a caught mirage

A second, harder pass (`C_setups_iterate.py`, `C_setups_rs_breakout.py`) tested far beyond the live config:

- **Full exit grid** — 7 SL × 7 Tgt × 4 time-stops, on **both** the original LONG **and** the contrarian **SHORT-the-failed-breakout fade**: **392 configurations**. Best `min(train_pf, test_pf)` across all = **0.74**. Nothing clears PF 1.0 on either side; day-block p ≈ 0.95–0.99 (significantly negative). The breakout neither continues (long) nor cleanly reverses (short) — it chops (mfe_R ≈ 0.6).
- **12 sub-populations** (regime, time, rs_pct, vol_ratio, vwap_dist, market): only **market-falling** (`market_ret ≤ −0.20`) looked positive — TRAIN PF 1.14 / **TEST PF 1.29 on n=25** — a relative-strength-breakout hypothesis (stock breaks OR high while NIFTY is red = genuine RS).
- **Focused RS-breakout sweep** (full population, market threshold × exit grid): the hypothesis **did not hold**. On the full sample (test n=171–334, not 25), **TEST PF = 0.76–0.87 at every threshold/exit** (TRAIN ~1.0–1.14). The 1.29 was a 25-trade small-sample mirage; corrected, there is **no OOS edge**.

**Conclusion of the deep search:** across ~400 exit configs, both sides, 12 sub-populations, and a focused RS sweep, **no C_OR_BREAKOUT configuration is positive in both train and test.** The setup has no robust edge. (Artifacts: `C_OR_BREAKOUT_exit_sweep.csv`, `C_OR_BREAKOUT_subpop.csv`, `C_OR_BREAKOUT_rs_breakout_sweep.csv`.)

## 11–14. Config changes / what-not-to-change / data gaps / validation
- **Config:** none to production. Candidate ideas in `C_setups_v11_candidate_config.json` are all `rejected_or_research` — no probation candidate is strong enough.
- **Data gap:** prev-bar/OR-distance and gap context not in the pre-dedupe features; the exit re-resolve (experiment 1) needs a fresh 1-minute walk.
- **Validation:** any future C candidate must clear the same gate as B (purged WF + day-block bootstrap, OOS net PF≥1.3) — none currently approaches it.
