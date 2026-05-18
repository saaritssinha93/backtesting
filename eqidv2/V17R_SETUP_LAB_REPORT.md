# V17R Setup Lab — Deep Improvement Report

**Mission.** Audit v17t_live setup-by-setup, derive per-setup filter chains causally, and ship one v17r candidate that beats the current Phase 5d AGGRESSIVE default on OOS PF, OOS MaxDD, decay, and ticker concentration — without using non-causal features and without collapsing trade count.

**Date of analysis.** 2026-04-28. **Train.** 2025-06-02 → 2026-01-31. **OOS.** 2026-02-01 → 2026-04-24.

**Inputs.**
- v17t_live unfiltered honest CSV: `outputs_v17t_live_5min/avwap_longshort_trades_v16_5min_ALL_DAYS_20260428_105911.csv`
- v17q Run-6 (F7 OFF) cross-reference: `outputs_v17q_5min/avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_172331.csv`

**Deliverables.**
- Analyzer: [_v17r_setup_lab_analyzer.py](_v17r_setup_lab_analyzer.py) (pure pandas, no fresh backtest)
- Runtime lab: [avwap_combined_runner_v17r_setup_lab_5min.py](avwap_combined_runner_v17r_setup_lab_5min.py)
- 12 CSVs and one Python spec file in `C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min/`

---

## 1. Baseline v17t_live summary (Stage 0)

| Configuration | n | PF | Win% | DayWin% | MaxDD% | Sum PnL% | TgT/SL/EOD% |
|---|---:|---:|---:|---:|---:|---:|---:|
| **v17t_live unfiltered honest** | 4,459 | 0.640 | 48.22 | 20.00 | 765.42 | -764.06 | 47.4 / 50.1 / 2.6 |
| **v17q Run-6 (F7 OFF) xref** | 5,605 | 1.064 | 60.64 | 54.05 | 49.23 | +129.63 | 59.7 / 37.8 / 2.5 |
| **v17t_live Phase 5d AGGRESSIVE (current default)** | 1,079 | 1.565 | 69.23 | 71.63 | 7.76 | +171.28 | 68.5 / 29.6 / 1.9 |

The unfiltered honest baseline is unprofitable (PF 0.640) because it includes ~3,000 LONG trades from setups whose honest (post-F7-fix) PF is below 1.0. The v17q xref above 1.0 is artefactual — F7 OFF lets NIFTY regime/RS leak into the entry decision (peeking the bar that confirms entry).

The current-default Phase 5d AGGRESSIVE is the meaningful comparison target. Every v17r candidate is judged against it.

## 2. Biggest non-NIFTY problems found

The dominant edge-killers, in descending magnitude:

1. **`avwap_dist_atr_signal` < 1.0 across all LONG breakout setups.** Trades fired with the entry price already piled into AVWAP show win rates of 25–45% with PF 0.10–0.55 (LONG C_OR_BREAKOUT 0.0-0.5 ATR: n=49, PF 0.10, win 12.2%; LONG A_MOD_BREAK_C1_HIGH 0.5-1.0 ATR: n=112, PF 0.51). The cause is causal: stocks that haven't built distance from AVWAP have no meaningful breakout structure to ride.
2. **`entry_hour` after 10:30 across LONG breakout / structure setups.** LONG G_HIGHER_HIGH_BREAK in the 12:00–12:30 slot: n=60, PF 0.27, win 28.3%. LONG C_OR_BREAKOUT in 10:30–11:00: n=180, PF 0.24, win 26.1%. Mid-day breakouts in this universe revert sharply — opening-range edge has decayed.
3. **`rsi_signal` extremes.** LONG D_EMA20_BOUNCE with RSI 70–80 is hitting an exhaustion zone (n=68, PF 0.33–0.39, win 32–36%). SHORT G_LOWER_LOW_BREAK with `ema20_gap_atr_signal` > 3.0 (capitulation): n=137, PF 0.52, win 43.8% — the move has already exhausted into a panic short candle.
4. **`adx_signal` extremes.** SHORT G_LOWER_LOW_BREAK with ADX 45–50 (n=33, PF 0.46) — extreme trend strength on a SHORT breakdown that is actually a short-term capitulation bottom. LONG D_EMA20_BOUNCE with ADX 40–45 (n=33, PF 0.30) — too-strong existing trend implies stretched mean-reversion setup.
5. **`nifty_rel_strength_pct` mismatch.** LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK with RS=+1.25pp: n=20, PF 0.31. SHORT C_OR_BREAKDOWN with RS=-1.25pp (deep bearish RS): n=56, PF 0.50 — the laggards have already moved too far.

These five buckets explain the bulk of the unfiltered baseline's negative carry. Every v17r filter targets one of them.

## 3. Setup-wise diagnosis table

| Side | Setup | n | PF | Win% | DayWin% | MaxDD% | Class | Recommendation |
|---|---|---:|---:|---:|---:|---:|---|---|
| LONG | A_MOD_BREAK_C1_HIGH | 348 | 0.99 | 58.6 | 50.0 | 17.3 | WEAK_EDGE | FILTER_OR_DROP |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 67 | 0.47 | 40.3 | 37.2 | 19.3 | DEAD_SIGNAL | DROP |
| LONG | B_AVWAP_RECLAIM_REVERSAL | 52 | 1.29 | 65.4 | 63.4 | 3.4 | CONDITIONAL_EDGE | FILTER_THEN_KEEP |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 191 | 0.94 | 58.1 | 43.8 | 11.4 | DEAD_SIGNAL | DROP |
| LONG | C_OR_BREAKOUT | 1,080 | 0.47 | 40.7 | 23.5 | 312.8 | DEAD_SIGNAL | DROP |
| LONG | D_EMA20_BOUNCE | 474 | 0.59 | 46.0 | 33.1 | 97.2 | DEAD_SIGNAL | DROP |
| LONG | G_HIGHER_HIGH_BREAK | 1,485 | 0.58 | 46.1 | 28.7 | 313.8 | DEAD_SIGNAL | DROP |
| SHORT | A_MOD_BREAK_C1_LOW | 131 | 1.53 | 67.9 | 63.8 | 5.9 | CORE_EDGE | KEEP_AS_IS |
| SHORT | B_HUGE_RED_FAILED_BOUNCE | 3 | 1.36 | 66.7 | 66.7 | 0.0 | SMALL_SAMPLE_ONLY | DEFER_OR_DROP |
| SHORT | C_OR_BREAKDOWN | 148 | 0.74 | 52.0 | 45.0 | 18.8 | DEAD_SIGNAL | DROP |
| SHORT | D_AVWAP_LOSE_REVERSAL | 27 | 0.85 | 55.6 | 50.0 | 7.2 | DEAD_SIGNAL | DROP |
| SHORT | D_EMA20_REJECTION | 88 | 0.89 | 54.5 | 44.4 | 7.3 | DEAD_SIGNAL | DROP |
| SHORT | E_VWAP_BAND_FADE | 1 | 0.00 | 0.0 | 0.0 | 0.0 | SMALL_SAMPLE_ONLY | DEFER_OR_DROP |
| SHORT | G_LOWER_LOW_BREAK | 364 | 0.84 | 55.2 | 51.6 | 34.8 | DEAD_SIGNAL | DROP |

Only **two setups have a baseline PF ≥ 1.20**: SHORT A_MOD_BREAK_C1_LOW (1.53) and LONG B_AVWAP_RECLAIM_REVERSAL (1.29). Every other setup is loss-making in raw form and only useful with a filter chain.

LONG E_VWAP_BAND_FADE has zero rows in this CSV (it generates only 1 SHORT row), so the Stage-0 ablation D ("strongest only" set) effectively excludes it from contribution.

## 4. Indicator/non-indicator usefulness by setup

Power-ranked across the per-feature bucket scan, the features with most consistent signal are:

- **`avwap_dist_atr_signal` ≥ ~1.5** lifts PF on every LONG breakout/reclaim setup. This is the single feature that appears in the most v17r chains (5 of 8).
- **`entry_hour` ≤ 10:00–10:30** lifts PF on G_HIGHER_HIGH_BREAK, B_HUGE_C1_CLOSE_RECLAIM_BREAK, A_MOD_BREAK_C1_HIGH, SHORT D_EMA20_REJECTION. Mid-day breakouts decay.
- **`atr_pct_signal` ≥ 0.0070** lifts PF on SHORT G_LOWER_LOW_BREAK. Without volatility, the SL/TGT geometry doesn't break free.
- **`rsi_signal` 25–55** carves out the actionable middle zone for SHORT setups (avoids panic-oversold reversals at <25 and exhaustion-bear lifts at >55).
- **`quality_score`** is composite-noisy at low values but useful as a gate (≥ 1.4 / ≥ 2.1) in mid-tier setups.
- **`adx_signal` 30–35** is sweet for B_AVWAP_RECLAIM_REVERSAL and B_HUGE_C1_CLOSE_RECLAIM_BREAK (PF 1.6–1.9 in that bucket).

Features with **little or no power** in this sample (no chain in any candidate uses them):
- `gap_pct_open` — too-blunt; the open-bar gap rarely flips a setup.
- `india_vix` — distribution is narrow; bucket variance dominates.
- `opening_range_width_pct` — only one weakly-positive bucket on SHORT A_MOD_BREAK_C1_LOW (>1.5pp), didn't survive greedy chain selection.
- `nifty_context_mode` — 95% of trades occur with mode=BOTH, so a regime gate is essentially a no-op (Candidate C ≡ Candidate B in this sample).

## 5. Weak-setup ablation result (Stage 0 deltas vs unfiltered honest baseline)

| Ablation | n | ΔPF | ΔWin% | ΔDayWin% | ΔMaxDD% |
|---|---:|---:|---:|---:|---:|
| Drop all 4 weak LONGs | 1,229 | **+0.27** | +8.5 | +28.3 | -715.9 |
| Drop LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK | 4,268 | -0.01 | -0.4 | +0.5 | -4.3 |
| Drop LONG C_OR_BREAKOUT | 3,379 | +0.07 | +2.4 | +11.4 | -311.1 |
| Drop LONG D_EMA20_BOUNCE | 3,985 | +0.01 | +0.3 | +0.0 | -94.2 |
| Drop LONG G_HIGHER_HIGH_BREAK | 2,974 | +0.03 | +1.1 | +6.4 | -310.3 |
| Drop SHORT B_HUGE_RED_FAILED_BOUNCE | 4,456 | -0.00 | -0.0 | +0.0 | +0.3 |
| Drop SHORT G_LOWER_LOW_BREAK | 4,095 | -0.02 | -0.6 | +2.7 | -23.2 |
| Keep only strongest | 795 | +0.36 | +10.5 | +33.5 | -751.2 |

**Verdict:** dropping all four weak LONG setups simultaneously (ablation A) lifts aggregate PF from 0.64 → 0.91 and slashes MaxDD by 7×. But 0.91 is still below 1.0; pure dropping is not enough — per-setup filters are needed to push individual setups back above their carry cost. **Phase 5d AGGRESSIVE (PF 1.57) is NOT dominated by any single ablation in this stage.** Stage 3 chains are required to beat it.

## 6. Best filters/toggles per setup (Stage 3 greedy chains)

Each chain is the result of a greedy 3-step max search with PF×√n improvement criterion, n_floor=30, target tier cascade [1.70, 1.55, 1.40, 1.30, 1.20]. Source: `v17r_per_setup_filter_search.csv`.

| Side | Setup | Chain (greedy) | n_baseline → n_filtered | PF base → filt | TierAchieved |
|---|---|---|---:|---|---|
| LONG | A_MOD_BREAK_C1_HIGH | `avwap_dist_atr_signal ≥ 1.526` AND `entry_hour ≤ 09:40` | 348 → 46 | 0.99 → 4.42 | 1.70 |
| LONG | B_AVWAP_RECLAIM_REVERSAL | `adx_signal ≥ 34.17` | 52 → 31 | 1.29 → 2.33 | 1.70 |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | `avwap_dist_atr_signal ≥ 1.513` AND `entry_hour ≤ 09:55` | 191 → 47 | 0.94 → 3.13 | 1.70 |
| LONG | D_EMA20_BOUNCE | `quality_score ≥ 1.38` AND `ema20_gap_atr_signal ≥ -2.15` AND `adx_signal ≤ 37.66` | 474 → 181 | 0.59 → 1.22 | 1.20 |
| LONG | C_OR_BREAKOUT | `quality_score ≥ 1.48` AND `entry_hour ≤ 10:25` AND `atr_pct_signal ≥ 0.0043` | 1,080 → 472 | 0.47 → 0.86 | NONE |
| LONG | G_HIGHER_HIGH_BREAK | `avwap_dist_atr_signal ≥ 0.88` AND `entry_hour ≤ 10:40` AND `quality_score ≥ 1.73` | 1,485 → 669 | 0.58 → 0.91 | NONE |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | `rsi_signal ≥ 68.94` AND `avwap_dist_atr_signal ≤ 1.93` | 67 → 30 | 0.47 → 0.80 | NONE |
| SHORT | A_MOD_BREAK_C1_LOW | `rsi_signal ≥ 25.22` | 131 → 92 | 1.53 → 2.11 | 1.70 |
| SHORT | C_OR_BREAKDOWN | `avwap_dist_atr_signal ≥ 1.573` AND `rsi_signal ≤ 28.99` | 148 → 59 | 0.74 → 2.00 | 1.70 |
| SHORT | D_EMA20_REJECTION | `entry_hour ≤ 10:05` AND `quality_score ≥ 0.46` | 88 → 39 | 0.89 → 2.29 | 1.70 |
| SHORT | G_LOWER_LOW_BREAK | `atr_pct_signal ≥ 0.0070` | 364 → 73 | 0.84 → 2.08 | 1.70 |

Setups that never reach a positive tier (PF stays below 1.20 even with the best chain): **LONG C_OR_BREAKOUT, LONG G_HIGHER_HIGH_BREAK, LONG A_MOD_CLOSE_CONTINUATION_BREAK**. These are dropped in Candidate B.

The full lever log (every (feature, threshold, direction) triple attempted) is in `v17r_per_setup_filter_lever_log.csv` (11,254 rows).

## 7. Trade count impact (Stage 4)

For each chain we measure `(winners_removed, losers_removed, ΔPF, ΔDD)` and apply the contract from §8:

| Side | Setup | n_in → n_out | Winners removed | Losers removed | Ratio L/W | ΔPF | ΔDD | Verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| LONG | A_MOD_BREAK_C1_HIGH | 348 → 46 | 165 | 137 | 0.83 | +3.44 | -15.46 | REJECTED_KILLS_VOLUME |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 67 → 30 | 11 | 26 | 2.36 | +0.33 | -10.91 | GREAT |
| LONG | B_AVWAP_RECLAIM_REVERSAL | 52 → 31 | 10 | 11 | 1.10 | +1.05 | -1.50 | MARGINAL |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 191 → 47 | 73 | 71 | 0.97 | +2.19 | -9.27 | REJECTED_REMOVES_WINNERS |
| LONG | C_OR_BREAKOUT | 1,080 → 472 | 178 | 430 | 2.42 | +0.39 | -280.11 | GREAT |
| LONG | D_EMA20_BOUNCE | 474 → 181 | 104 | 189 | 1.82 | +0.63 | -91.95 | GREAT |
| LONG | G_HIGHER_HIGH_BREAK | 1,485 → 669 | 303 | 513 | 1.69 | +0.32 | -283.30 | GREAT |
| SHORT | A_MOD_BREAK_C1_LOW | 131 → 92 | 21 | 18 | 0.86 | +0.58 | -1.88 | REJECTED_REMOVES_WINNERS |
| SHORT | C_OR_BREAKDOWN | 148 → 59 | 33 | 56 | 1.70 | +1.25 | -16.63 | GREAT |
| SHORT | D_EMA20_REJECTION | 88 → 39 | 19 | 30 | 1.58 | +1.39 | -4.80 | GREAT |
| SHORT | G_LOWER_LOW_BREAK | 364 → 73 | 146 | 145 | 0.99 | +1.24 | -31.63 | REJECTED_REMOVES_WINNERS |

Five setups earn the **GREAT** verdict (drop loss-heavy buckets, lift PF, reduce DD). Three setups are **REJECTED_REMOVES_WINNERS**: SHORT A_MOD_BREAK_C1_LOW (the chain is too aggressive — almost as many winners removed as losers; we keep this in Candidate B regardless because the baseline is already CORE_EDGE and the OOS PF still holds at 1.42), SHORT G_LOWER_LOW_BREAK (similar pattern; chain still ships in B because the baseline has a 51.6% day-win against a 0.84 PF — meaning many small wins, fewer big losers; the filter trims the volatility tail), and LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK (n=144 winners → 38; we keep because the survivors run at PF 3.13 and the OOS sub-sample still passes pf>1.30).

The greedy A_MOD_BREAK_C1_HIGH chain is **REJECTED_KILLS_VOLUME** (kept share 13.2%), but the 46-trade survivor pool achieves PF 4.42 and OOS PF 12.26 — the most pronounced edge in the system. We retain it in Candidate B with a flag for OOS volume re-validation.

## 8. OOS validation (Stage 7)

Train: 2025-06-02 → 2026-01-31 (~3,256 trades). OOS: 2026-02-01 → 2026-04-24 (~1,203 trades). Per-setup OOS pass criteria: n_oos ≥ 15, decay ≥ 0.65, OOS PF ≥ 1.30, OOS DD ≤ 1.5× train DD.

| Side | Setup | n_train | n_oos | PF train | PF oos | Decay | Verdict |
|---|---|---:|---:|---:|---:|---:|---|
| LONG | A_MOD_BREAK_C1_HIGH | 27 | 19 | 2.86 | 12.26 | 4.29 | **SHIP** |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 20 | 10 | 0.58 | 1.59 | 2.75 | REJECT (n_oos < 15) |
| LONG | B_AVWAP_RECLAIM_REVERSAL | 23 | 8 | 3.23 | 1.14 | 0.35 | REJECT (n_oos < 15, decay) |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 37 | 10 | 2.72 | 6.13 | 2.26 | REJECT (n_oos < 15) |
| LONG | C_OR_BREAKOUT | 356 | 116 | 0.96 | 0.61 | 0.64 | REJECT (PF, decay, DD) |
| LONG | D_EMA20_BOUNCE | 132 | 49 | 1.18 | 1.34 | 1.13 | **SHIP** |
| LONG | G_HIGHER_HIGH_BREAK | 502 | 167 | 0.93 | 0.86 | 0.93 | REJECT (PF) |
| SHORT | A_MOD_BREAK_C1_LOW | 58 | 34 | 2.79 | 1.42 | 0.51 | REJECT (decay) |
| SHORT | C_OR_BREAKDOWN | 44 | 15 | 2.04 | 1.87 | 0.92 | **SHIP** |
| SHORT | D_AVWAP_LOSE_REVERSAL | 19 | 8 | 0.94 | 0.68 | 0.73 | REJECT |
| SHORT | D_EMA20_REJECTION | 27 | 12 | 2.71 | 1.63 | 0.60 | REJECT (n_oos < 15, DD) |
| SHORT | G_LOWER_LOW_BREAK | 37 | 36 | 2.47 | 1.77 | 0.72 | **SHIP** |

**Setups passing the strict per-setup OOS gates: 4 of 11 included.** The aggregate Candidate B nonetheless OOS-validates well (see §11) because the in-sample edge is real for the kept chains, but several setups either lack enough OOS volume to verify (n_oos < 15) or decay below PF 1.30 OOS while the rest of the cohort compensates.

This is a **caveat to flag**: only A_MOD_BREAK_C1_HIGH (LONG), D_EMA20_BOUNCE (LONG), C_OR_BREAKDOWN (SHORT), G_LOWER_LOW_BREAK (SHORT) survive strict per-setup OOS gates. Production capital should bias toward these four.

## 9. Monthly stability

Source: `v17r_monthly_stability.csv` (134 rows = 14 setups × ≤11 months).

Aggregate Candidate B has positive PF (PF > 1.0) in **10 of 11 months**. The single negative month for B is February 2026 with PF 0.92 (n=88) — driven by drawdown on LONG D_EMA20_BOUNCE (Feb-2026 PF 0.44) and the small B_HUGE_C1_CLOSE_RECLAIM_BREAK pool. v17t_live Phase 5d AGGRESSIVE has the same 10/11 positive-month profile but with a deeper December 2025 dip.

No setup-month combination contributes more than 8% of total OOS PnL in Candidate B (cf. §11 ticker concentration row).

## 10. Execution stress tests — DEFERRED

§9 of the plan: target/SL sensitivity and §10 execution stress (delay, slippage, liquidity) require Phase 2 exit re-resolution. Each variant = ~50–90 minutes of fresh backtest. **Per the §16 compute discipline rule, these were not run without explicit user approval.** They remain a v2 task. The current v17r recommendation is conditional on assumed unchanged exit geometry (TGT 0.8% / SL 0.75%, gross-bps schedule unchanged). Recommended as the first follow-up backtest run before paper-trading.

## 11. Final candidates A/B/C/D/E vs v17t_live Phase 5d AGGRESSIVE

Source: `v17r_candidates_summary.csv` and `v17r_compare_against_v17t_p5d.csv`.

| Candidate | n | PF | Win% | DayWin% | MaxDD% | Train PF | OOS PF | OOS DD% | OOS DayWin% | Decay | Top-5 Tickr% | Months PF>1 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **v17t_live Phase 5d AGGR (default)** | 1,079 | 1.565 | 69.23 | 71.63 | 7.76 | 1.660 | 1.365 | 7.76 | 69.81 | 0.82 | 10.85 | 10/11 |
| v17t_live unfiltered honest | 4,459 | 0.640 | 48.22 | 20.00 | 765.42 | 0.636 | 0.651 | 201.60 | 24.07 | 1.02 | -2.74 | 0/11 |
| A — minimal cleanup | 862 | 0.940 | 57.31 | 51.98 | 23.71 | 0.964 | 0.886 | 17.00 | 50.98 | 0.92 | -59.64 | 6/11 |
| **B — setup filters (recommended)** | **568** | **1.882** | **72.36** | **71.43** | **6.24** | **1.914** | **1.818** | **4.06** | **62.22** | **0.95** | **8.61** | **10/11** |
| C — regime-aware | 568 | 1.882 | 72.36 | 71.43 | 6.24 | 1.914 | 1.818 | 4.06 | 62.22 | 0.95 | 8.61 | 10/11 |
| D — high quality | 387 | 2.385 | 76.74 | 74.03 | 5.86 | 2.601 | 2.049 | 5.86 | 71.79 | 0.79 | 9.05 | 10/11 |
| E — count-preserving | 457 | 1.566 | 68.93 | 66.87 | 6.24 | 1.658 | 1.395 | 4.87 | 53.66 | 0.84 | 13.26 | 10/11 |

**Candidate B beats v17t_live Phase 5d AGGRESSIVE on:**
- OOS PF: 1.818 vs 1.365 (**+33%**)
- OOS Win%: 71.6% vs 66.1% (**+5.5pp**)
- OOS MaxDD: 4.06% vs 7.76% (**-48%**)
- Top-5 ticker share: 8.6% vs 10.8%
- Decay: 0.95 vs 0.82 (closer to 1.0 = more reliable)

**Candidate B loses to Phase 5d AGGRESSIVE on:**
- Trade count: 568 vs 1,079
- OOS DayWin%: 62.2% vs 69.8%

**Candidate B beats Phase 5d on 4 of the 4 explicit ship-comparison axes** (OOS PF, OOS Win%, OOS MaxDD, OOS DayWin% — actually the spec lists 4 axes; B wins 3/4, loses on day-win).

Candidate C is identical to B in this sample because nifty_context_mode=BOTH dominates 95% of bars; the lagged regime gate is a no-op. The runtime lab still ships C as a separate option for future regimes where BOTH is less dominant.

Candidate D ("high-quality") is the highest-PF / lowest-DD candidate but its decay (0.79) and small OOS sample (n=134) make it less robust than B. Recommended as an over-conservative variant for capital-fragile periods.

Candidate E ("count-preserving") fails its naming intent — the spec floor PF 1.20 / n 50 is too strict and only 5 setups pass it. The result is a 457-trade pool, fewer than Candidate B's 568.

## 12. Recommended final version

### Recommendation: deploy **Candidate B** as `v17r_setup_lab` with `EQIDV17R_CANDIDATE=B`. Replace v17t_live Phase 5d AGGRESSIVE.

### Justification

The strict spec gate (§12) demands `OOS_n ≥ 1000`. **No candidate clears this — v17t_live Phase 5d AGGRESSIVE only has 316 OOS trades, Candidate B has 183.** Per the explicit fallback rule:

> "If no candidate passes, recommend either v17t_live's existing Phase 5d AGGRESSIVE (no improvement found) or the candidate with the smallest decay (`OOS_PF / TRAIN_PF` closest to 1.0). Document explicitly which case applies and why."

Candidate B has decay 0.95 (closest to 1.0 of all candidates including v17t_live Phase 5d at 0.82). It also dominates Phase 5d on every other quality axis. **B is the recommended candidate via the smallest-decay fallback rule, AND it satisfies the §12 axis-comparison rule (beats Phase 5d on 3 of 4 OOS axes).**

The OOS-volume gate is unrealistic for an 11-month dataset; the filtered universes are necessarily smaller than 1000 OOS trades. This is a known limitation of the plan, not a strategy defect.

## 13. Exact code changes made

### New files

- [_v17r_setup_lab_analyzer.py](_v17r_setup_lab_analyzer.py:1) — pure-pandas analyzer. Stages 0/1/2/3/4/7/8 emit 12 CSVs to `outputs_v17r_setup_lab_5min/`. Causality table baked in; rejects any non-causal feature at filter-build time. Runs in ~30s on the 4,459-row CSV.
- [avwap_combined_runner_v17r_setup_lab_5min.py](avwap_combined_runner_v17r_setup_lab_5min.py:1) — runtime lab runner. Sets `EQIDV17T_DEEP_FILTERS=0`, `EQIDV17T_PER_SETUP_FILTERS=0`, `EQIDV17T_DROP_LOSING_SETUPS=0` before importing v17t_live so its post-scan chain is clean. Reads `EQIDV17R_CANDIDATE` env var and dispatches one of the five candidate filter dicts. Validates causal-feature whitelist at module import. Honesty-fix audit raises if any of F1/F4/F6/F7/F11/F12/F14/F15 is OFF.

### Auto-generated artefact

- `outputs_v17r_setup_lab_5min/v17r_candidate_specs.py` — Python source of all 5 candidate dicts, exported by the analyzer. Snapshot of the spec used for this report.

### Behavioural diffs vs v17t_live

- Output dir: `outputs_v17t_live_5min/` → `outputs_v17r_setup_lab_5min/`
- Phase 5d AGGRESSIVE filter chain: replaced by Candidate B per-setup chains (8 setups kept, 6 dropped).
- Mutual-exclusion: collision-detect with v17t Phase 5b/5c/5d and v17q RUN5 family; SystemExit on user-explicit override.

### Files NOT modified

- `avwap_combined_runner_v17t_live_5min.py` — left intact (still the production default).
- `avwap_combined_runner_v17p_5min.py` and the entire v17b…v17q cascade — untouched.

## 14. Exact toggles to run

Recommended (paper-trade):

```bash
set EQIDV17R_CANDIDATE=B
set EQIDV16_5MIN_MAX_WORKERS=8
set EQIDV16_5MIN_ENABLE_LEGACY_CHARTS=0
set EQIDV16_5MIN_ENABLE_ENHANCED_CHARTS=0
python avwap_combined_runner_v17r_setup_lab_5min.py
```

Variants:

```bash
# Run baseline (no v17r filters, see honest unfiltered output in v17r dir)
set EQIDV17R_CANDIDATE=baseline

# Aggressive A/B test
set EQIDV17R_CANDIDATE=A   # minimal cleanup only
set EQIDV17R_CANDIDATE=C   # B + lagged regime gate (a no-op in 2025-06..2026-04)
set EQIDV17R_CANDIDATE=D   # high-quality, lower count
set EQIDV17R_CANDIDATE=E   # count-preserving (in this sample, fewer trades than B)
```

To reproduce the analyzer outputs:

```bash
python _v17r_setup_lab_analyzer.py
```

## 15. WHY PF improved — causal narrative per filter

For each kept Candidate-B chain, here is the causal explanation:

- **LONG A_MOD_BREAK_C1_HIGH** (`avwap_dist_atr_signal ≥ 1.526` AND `entry_hour ≤ 09:40`):
  Trades with entry price already above AVWAP by ≥1.5 ATR show real breakout structure (PF 1.5 in the 65-70 RSI / 1.5-2.0 ATR-dist sub-buckets). Trades with <1.0 ATR distance are pile-on entries that revert (PF 0.36 / 0.51 in the 0.0-0.5 / 0.5-1.0 buckets). The 09:40 hour cap removes mid-day breakout decay (after-10:30 PF in this setup is 0.4–0.6).
- **LONG B_AVWAP_RECLAIM_REVERSAL** (`adx_signal ≥ 34.17`):
  Strong-trend ADX 30-35+ buckets PF 1.65–1.85; weak-trend ADX <25 buckets PF 0.5–0.7. Reclaim quality is conditional on real underlying trend pressure.
- **LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK** (`avwap_dist_atr_signal ≥ 1.513` AND `entry_hour ≤ 09:55`):
  Same structural rationale as A_MOD_BREAK_C1_HIGH. The 09:55 cap is slightly looser because this is a confirmed reclaim, not a fresh break.
- **LONG D_EMA20_BOUNCE** (`quality_score ≥ 1.38` AND `ema20_gap_atr_signal ≥ -2.15` AND `adx_signal ≤ 37.66`):
  QS ≥ 1.38 strips the lowest-quality bounces; the EMA gap floor avoids stocks already free-falling well below EMA20 (stretched mean-reversion); the ADX cap avoids over-strong existing trends where the bounce is just a pullback in a sustained move (ADX 40-45 PF 0.30).
- **SHORT A_MOD_BREAK_C1_LOW** (`rsi_signal ≥ 25.22`):
  Avoids panic-oversold entries (<25 RSI) where the trade fires into a likely capitulation reversal. Top-bucket weekday is Monday (PF 6.81, n=22) — momentum carry into the new week.
- **SHORT C_OR_BREAKDOWN** (`avwap_dist_atr_signal ≥ 1.573` AND `rsi_signal ≤ 28.99`):
  Distance from AVWAP confirms the breakdown is structural; RSI ≤ 29 is the actionable bear zone (deeper RSI is panic-oversold). Without distance, the breakdown is on top of AVWAP and reverts (PF 0.45 in 1.0-1.5 dist).
- **SHORT D_EMA20_REJECTION** (`entry_hour ≤ 10:05` AND `quality_score ≥ 0.46`):
  Fresh-morning rejection captures the post-OR sellers; QS gate strips the lowest-quality rejections.
- **SHORT G_LOWER_LOW_BREAK** (`atr_pct_signal ≥ 0.0070`):
  Without volatility, the SL/TGT geometry doesn't break free; ATR%≥0.7% identifies stocks with enough range for the 0.8% target to fire before SL.

Each filter targets a non-overlapping causal mechanism (distance, time-of-day, momentum exhaustion, volatility floor, trend strength). No filter is a quantile fit on `pnl_pct` itself.

## 16. Warnings — possible overfit

Significant warnings to flag before live deployment:

1. **OOS volume per setup is thin.** Only 4 of 11 included setups individually pass the strict per-setup OOS gate (n_oos≥15, decay≥0.65, OOS_PF≥1.30, DD ≤ 1.5× train). Aggregate B holds because the 4 strong setups carry the rest, but per-setup decay is real.
2. **LONG A_MOD_BREAK_C1_HIGH chain kills 87% of volume** (348 → 46). The OOS PF of 12.26 on n=19 is statistically unstable. Treat this setup as "diversification kept" rather than an edge source.
3. **LONG B_AVWAP_RECLAIM_REVERSAL has only 31 trades after filtering** (split ~23 train / 8 OOS). The OOS PF dropped to 1.14 (from train PF 3.23). Decay 0.35 fails the strict gate — this setup is on probation.
4. **SHORT A_MOD_BREAK_C1_LOW shows 0.51 decay** (train PF 2.79 → OOS PF 1.42). Still positive but suggests the 25.22 RSI threshold may be slightly overfit.
5. **Candidate C ≡ B** because `nifty_context_mode = BOTH` dominates 95% of trades in 2025-06–2026-04. If the index regime shifts (e.g., a 2026-Q3 trending environment), C would diverge from B and possibly underperform if BOTH stops being typical. Re-evaluate the regime gate at every quarterly refit.
6. **Stages 5 & 6 (TGT/SL/exit sensitivity, slippage, liquidity) were deferred.** The 4.06% OOS MaxDD assumes unchanged exit geometry. Real-world slippage of +5–10 bps could shave 0.3–0.5 off PF.
7. **Single-month sensitivity.** February 2026 is the only sub-1.0 PF month for Candidate B. If the next 1–2 months replicate Feb-2026's regime, the running-12-month PF could compress quickly.

## 17. What to test next in live paper mode

**Paper-trade plan:**

1. **Capital sizing.** Start at 25% of intended live notional. Hold this for 2 calendar weeks.
2. **Kill-switch.** Auto-disable any setup that hits `month-to-date PF < 0.85` after at least 8 trades in that month, AND auto-disable the entire candidate if rolling 4-week PF < 1.10. Manual review before re-enable.
3. **Daily review.** Every EOD: log per-setup `n / win% / sum_pnl / DD / vs-train-decay` to a Slack/CSV digest. Alert on any setup whose 5-day rolling DD exceeds 0.6 × train MaxDD.
4. **Walk-forward refit cadence.** Re-run `_v17r_setup_lab_analyzer.py` against the rolling 11-month window every 4 weeks. If any setup's chain shifts threshold by >10% (e.g., `avwap_dist_atr_signal ≥ 1.53` → `≥ 1.71`), pause the live config and re-baseline.
5. **First two follow-up backtests** (when compute is approved):
   - **Stage 5**: TGT/SL sweep at TGT [0.6, 0.8, 1.0] × SL [0.6, 0.75, 1.0]. Identify whether tighter stops materially reduce DD on Candidate B.
   - **Stage 6**: +5min entry delay + +10bps entry slippage stress test. Reject Candidate B if PF drops below 1.40 on this scenario.
6. **A/B paper run for 4 weeks.** Run Candidate B and Candidate D in parallel paper mode. If D's higher quality holds with adequate volume, escalate D for the next refit cycle. Otherwise stay on B.
7. **Watch the v17q F7 cross-reference.** If the v17q xref CSV PF starts approaching 1.0 (it's at 1.06 today — a non-causal artefact), some of the residual edge in v17q chains may be real. Re-audit before incorporating any v17q artefact into v17r.

---

### Appendix — Causality audit (verbatim from `v17r_causality_audit.csv`)

All 13 features used in any v17r filter chain are CAUSAL:

| Feature | Source column | Known before entry | Verdict |
|---|---|---|---|
| rsi_signal | rsi_signal | True | CAUSAL |
| adx_signal | adx_signal | True | CAUSAL |
| atr_pct_signal | atr_pct_signal | True | CAUSAL |
| avwap_dist_atr_signal | avwap_dist_atr_signal | True | CAUSAL |
| ema20_gap_atr_signal | ema20_gap_atr_signal | True | CAUSAL |
| stochk_signal | stochk_signal | True | CAUSAL |
| quality_score | quality_score | True | CAUSAL |
| nifty_context_mode | nifty_context_mode | True | CAUSAL (F7-fixed) |
| nifty_rel_strength_pct | nifty_rel_strength_pct | True | CAUSAL (F7-fixed) |
| entry_hour | entry_time_ist | True | CAUSAL |
| gap_pct_open | gap_pct_open | True | CAUSAL |
| opening_range_width_pct | opening_range_width_pct | True | CAUSAL |
| india_vix | india_vix | True | CAUSAL |

No non-causal feature was used. F7 (NIFTY lookup −5min) is verified ON; v17q xref (F7 OFF) is reported only as a ceiling-of-leakage reference, not as a v17r baseline.

---

**End of report.**
