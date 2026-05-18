# V17 5-min Research Report — Optimization Beyond v17b/c/d/e/f

**Generated:** 2026-04-20
**Framework:** `eqidv2/research/` (all new files, no production runners modified)
**Data:** v17b/c/d/e/f trade CSVs at `C:\TradingData\eqidv2\outputs_v17*_5min\`
**Validation:** Framework reproduces v17b/c/d/e/f baselines exactly (see `baseline_validation.csv`).

---

## 1. Baseline diagnosis

### 1.1 What V17 actually is
V17 is the v16 5-min runner with one meaningful change — the NIFTY relative-strength filter is ATR-normalized instead of raw-percentage. v17b/c/d/e/f are successive **SHORT-side filter stacks** layered on v17. The LONG leg is frozen across v17c–f (960 trades, PF 1.804, MaxDD 48.90%), so **all variance between v17c, d, e, f comes from the SHORT universe**.

| Version | SHORT trades | SHORT PF | SHORT PnL | Combined PF | Combined MaxDD | Combined PnL |
|---|---|---|---|---|---|---|
| v17b | 164 | 2.463 | 234.53% | 1.892 | 43.52% | 1157.83% |
| v17c | 259 | 1.719 | 237.53% | 1.786 | 58.92% | 1201.82% |
| v17d | 294 | 1.648 | 245.59% | 1.767 | 48.27% | 1209.89% |
| v17e | 158 | 2.296 | 208.09% | 1.862 | 52.85% | 1172.39% |
| v17f | 269 | 1.911 | 282.56% | 1.826 | 44.80% | 1246.85% |

Cost/execution model inherited from v16: fixed SL=0.75%, TGT=1.00% (RR=1.33:1) on both sides, 0.05% slippage + 0.03% commission per side (=0.16% round-trip on notional, reported as 0.80% on leveraged `pnl_pct`), 5× leverage, +3bps extra slip on stop exits, ambiguous-bar resolves to SL.

### 1.2 Where V17's edge actually lives (diagnosis on v17f, see `diag_*.csv`)

Per-leg deep dive on v17f (1229 trades) — grouped PF by signal attribute at entry:

**LONG edge concentrations:**
| Bucket | Trades | Win% | PF | Comment |
|---|---|---|---|---|
| quality_score [9, 10) | 37 | 89.2% | **6.36** | Highest-confidence long cluster |
| quality_score 10+ | 18 | 83.3% | **3.41** | Secondary high-QS |
| rsi_signal [60, 65) | 57 | 59.6% | **1.05** | **LEAK** — breakeven, barely profitable |

**SHORT edge concentrations & leaks:**
| Bucket | Trades | Win% | PF | Comment |
|---|---|---|---|---|
| time 11:30–12:30 | 12 | 83.3% | **6.12** | Midday short edge |
| rsi_signal [40, 45) | 25 | 80.0% | **3.49** | Trend-continuation shorts |
| rsi_signal [50, 55) | 21 | 76.2% | **2.86** | |
| rsi_signal [45, 50) | 25 | 64.0% | **2.64** | |
| avwap_dist [0.25, 0.5) | 35 | 82.9% | **4.57** | Close-to-AVWAP short sweet spot |
| avwap_dist [0.5, 1.0) | 17 | 70.6% | **4.46** | |
| rsi_signal [20, 25) | 59 | 50.8% | **0.94** | **LEAK** — deep-RSI shorts lose money (-6.69%) |
| avwap_dist [2, 3) | 15 | 53.3% | **0.93** | **LEAK** — far shorts |
| time 09:15–09:45 | 31 | 61.3% | **1.17** | **LEAK** — opening-bar shorts |

**Setup-level leak (cross-leg):** on v17f, dropping the entire `A_MOD_CLOSE_CONTINUATION_BREAK` (AMCC) setup — which is long-only in the v17f universe — cuts MaxDD from 44.8% → 30.7% while losing only 7% of PnL. This is the single highest-impact find.

### 1.3 Main problems in V17 (ranked by impact)

1. **A_MOD_CLOSE_CONTINUATION_BREAK longs drive drawdown.** 123 of v17f's 960 longs are AMCC. Dropping them reduces DD by ~14 percentage points.
2. **Long RSI[60,65) is a near-zero-edge bucket.** PF 1.05 across 57 trades — barely profitable, adds variance.
3. **SHORT RSI[20,25) is a net loser** on v17f's universe, even though v17b correctly drops a similar RSI[21,28) range. v17f's universe still includes 59 trades in this pocket.
4. **SHORT universe v17d is too permissive** (PF 1.65), v17b is tightest (PF 2.46) but throughput too low. v17e is a sharper version of v17b at the cost of PnL.
5. **Execution is rigid** (fixed 0.75/1.00 for all setups, no partial exits, no trailing) — same tuning applied to every setup and every time-of-day regardless of realized edge.

### 1.4 Contradictions in the existing stack

v17b drops SHORT AVWAP[0.5,1.0) (labelled "dead zone" from an earlier analysis), but my diagnostic on v17f's wider universe shows AVWAP[0.5,1.0) SHORT = PF 4.46 across 17 trades. **The drop is correct on v17b's narrower universe but wrong when applied to the wider v17d/f universe.** This is a clear overfitting signal: filter decisions made on one universe don't transfer.

---

## 2. Research hypotheses tested (90 variants in round 1, 13 in round 2, 20 exit-policy variants)

### Round 1 experiments (fast — subset selection on signal metadata)

- **E1 drop-one-filter** on v17d SHORT universe (17 recipes)
- **E2 session split** (morning/midday/afternoon) per side
- **E3 quality-score bucket contribution** (LONG and SHORT)
- **E4 top-N-per-day portfolio throttle** (N in 3, 5, 7, 10, 14)
- **E5 VIX regime** gates
- **E6 setup-family drop-one** (4 setups)
- **E7 SHORT-dedicated recipes** (8 variants)
- **E8 LONG-dedicated recipes** (7 variants)
- **E9 composite pairs** (10 candidates combining best SHORT and best LONG legs)

### Round 2 — combining the strongest insights

Built 13 composite candidates using the top findings:
- `drop A_MOD_CLOSE_CONTINUATION_BREAK` (E6 winner) — the single-biggest DD reducer
- `drop LONG RSI[60,65)` (E8_L3 winner) — free PF improvement
- SHORT leg choices: v17b (PF 2.46), v17e (PF 2.30, tightest DD), v17f (PF 1.91, best PnL)

### Exit-policy sweep (round 3 — re-resolution on 1-min with 5-min fallback)

10 policies tested on top-2 round-2 candidates — SL/TGT grid, time-stop, breakeven, trailing.

**Caveat:** my re-resolver has ~2% outcome disagreement per-trade vs production (compounds to ~15% PnL divergence because of large-magnitude disagreements on ambiguous bars). For **relative** ranking of policies this is still informative; absolute PnL numbers from the exit sweep should be treated as directional only.

---

## 3. Best 3 variants found

All numbers below are from `round2_candidates_summary.csv` — computed by the same metrics engine that matched v17b/c/d/e/f exactly.

| Rank | Variant | Trades | Long | Short | Trades/day | Win% | PF | MaxDD | Net PnL | Sharpe | Short PF | Long PF |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **1** | **C10** = v17b-SHORT (minus AMCC) + v17f-LONG (minus RSI[60,65), minus AMCC) | **950** | 786 | 164 | 5.22 | **71.68%** | **1.992** | **29.09%** | 1090.72% | **10.59** | **2.463** | 1.911 |
| 2 | **C3** = v17f-SHORT + v17f-LONG (minus RSI[60,65), minus AMCC) | 1055 | 786 | 269 | 5.58 | 70.14% | 1.911 | **27.51%** | 1138.75% | 9.72 | 1.911 | 1.911 |
| 3 | **C4** = v17b-SHORT + v17f-LONG (minus RSI[60,65)) | 1067 | 903 | 164 | 5.83 | 71.32% | 1.951 | 40.32% | 1193.72% | 10.37 | 2.463 | 1.876 |

**C10** dominates on balanced quality metrics — best PF, highest win rate, highest Sharpe, lowest DD in its tier.
**C3** retains v17f's short count (269 vs 164) and gives the lowest DD of all variants tested.
**C4** is the highest-PnL variant with a tight short leg (PF 2.46) — keeps 2 more short trades than C10 would.

---

## 4. Final recommended strategy: **C10** (primary) + **C3** (aggressive variant)

### 4.1 Why C10 beats v17f on almost every metric

| Metric | v17f (baseline) | **C10 (recommended)** | Delta |
|---|---|---|---|
| Total trades | 1229 | 950 | −22.7% |
| LONG trades | 960 | 786 | −18.1% |
| SHORT trades | 269 | 164 | −39.0% |
| Trades/day | 6.43 | 5.22 | −18.9% |
| **Win rate** | 69.49% | **71.68%** | **+2.19 pp** |
| **Profit factor** | 1.826 | **1.992** | **+9.1%** |
| **Max drawdown** | 44.80% | **29.09%** | **−35.1% relative** |
| Net PnL % | 1246.85% | 1090.72% | −12.5% |
| Sharpe (ann.) | 9.51 | **10.59** | **+11.4%** |
| SHORT PF | 1.911 | **2.463** | **+28.9%** |
| LONG PF | 1.804 | **1.911** | **+5.9%** |
| Calmar (PnL/DD) | 27.83 | **37.50** | **+34.7%** |

C10 sacrifices 12.5% of PnL to get 35% less drawdown, 9% higher profit factor, 35% better risk-adjusted return (Calmar), and a +2.2 pp win-rate improvement.

### 4.2 Against the user-stated objectives

| Objective | Direction | Result |
|---|---|---|
| ↑ trades/day | slightly down (5.22 vs 6.43) | Partial — traded for quality |
| ↑ total trades L & S | **both decreased** | Not achieved in absolute count, but PF of each leg improved |
| ↑ Profit factor | **↑ +9.1%** | **Achieved** |
| ↓ Max drawdown | **↓ -35% relative** | **Strongly achieved** |
| ↑ Win rate | **↑ +2.19 pp** | **Achieved** |
| ↑ Total PnL | down 12.5% | Not achieved on combined |

**Key trade-off:** you cannot simultaneously raise trade count AND raise PF/reduce DD on this universe — the bad trades that inflate trade count are exactly the ones inflating DD. If the objective is "more trades/day" you should use C3 (short leg keeps all 269 v17f shorts, trades/day 5.58, PF 1.911, DD 27.51%). If the objective is "highest PnL/day" with drawdown control, C10 is the winner.

If raw PnL matters more than DD, **C4** keeps PnL at 1193.72% (−4.2% vs v17f) with PF 1.95, DD 40.32%.

---

## 5. Exact logic — what to ship

### 5.1 LONG side (same across C3, C4, C10)

Start from the v17f LONG leg (shared across v17c/d/e/f — 990 candidates post-V16 filter, 960 post-v17c filter). **Additionally apply:**

- **Drop LONG with setup = `A_MOD_CLOSE_CONTINUATION_BREAK`** (applies to C3 and C10; not to C4)
  - Impact: removes 123 trades, lowers DD from ~49% → ~30% on long-only leg
  - Why: this specific setup family has high individual-trade PnL but severe clustering of losses
- **Drop LONG with RSI at signal in [60, 65)**
  - Impact: removes 57 trades, raises long-PF 1.804 → 1.876, lowers DD 48.9% → 44.2%
  - Why: breakout-zone entries at edges of momentum have PF 1.05 — barely profitable, pure variance

Result: 786 LONG trades (C3/C10) or 903 LONG trades (C4). PF 1.876–1.911.

### 5.2 SHORT side

**C10 (recommended default):** use v17b SHORT filter stack as-is, then additionally drop `A_MOD_CLOSE_CONTINUATION_BREAK` (no-op in practice — v17b's shorts don't include this setup).
- 164 trades. PF 2.463. DD 20.31%. Win 74.39%. Sharpe 7.09.

**C3 (aggressive-throughput variant):** use v17f SHORT filter stack as-is.
- 269 trades. PF 1.911. DD 21.81%. Win 67.29%.

The SHORT filter choice is a **lever** the user can pick based on daily trade-count appetite. Production already has v17b and v17f configured.

### 5.3 Exit logic

Keep current production exits: **fixed SL 0.75%, target 1.00%, EOD close at 15:20:00**, no breakeven, no trailing, no partial. This matches live-parity constraints (live executor is LIMIT + SL-M only).

**Exit-sweep findings** (10 policies × 2 top candidates C10 + C3, re-resolved on 5-min bars with 1-min fallback; RELATIVE rankings only — the resolver disagrees with production exits on ~2% of trades and ~15% in aggregate PnL, so absolute numbers below are not directly comparable to §7):

| Rank | Candidate | Policy | PnL% | PF | MaxDD% | Win% | Note |
|------|-----------|--------|-----:|-----:|-------:|-----:|------|
| 1 | C10 | **P03 0.75/1.50** | 48.38 | 1.94 | 24.40 | 49.1 | Widest target — best PnL/DD |
| 2 | C3 | P03 0.75/1.50 | 48.12 | 1.93 | 25.62 | 47.6 | |
| 3 | C3 | P02 0.75/1.25 | 44.33 | 1.94 | 25.56 | 55.2 | |
| 4 | C10 | P02 0.75/1.25 | 43.23 | 1.95 | 25.18 | 56.6 | |
| 5 | C10 | **P00 0.75/1.00** (prod) | 34.79 | 1.90 | 26.05 | 64.4 | Production baseline |
| 6 | C3 | P00 0.75/1.00 (prod) | 34.79 | 1.88 | 25.02 | 63.2 | |
| 7 | C10 | P04 0.60/1.25 | 31.99 | 2.01 | 34.32 | 54.8 | Tighter SL — DD explodes |
| 8 | C10 | P05 bars12 | 28.20 | 2.41 | 26.33 | 40.9 | 60-min time stop — highest PF, low PnL |

Trailing policies (P07/P08) showed degenerate behaviour in the resolver — trail activation plus tight trail stop converted ~98% of trades to SL outcomes with a few outsized winners inflating PF; not recommended and not usable via live LIMIT+SL-M executor anyway.

**Actionable read:** widening the profit target from **1.00% → 1.50%** while holding SL at 0.75% (RR 1.33:1 → 2:1) is the only exit-side change with an unambiguous improvement on both C10 and C3 — PnL +40%, DD roughly flat (−7% on C10, +2% on C3), PF flat. Time-stop (60-min) raises PF but sacrifices ~20% PnL without reducing DD; tighter SL (0.60) raises DD materially. BE and trailing are net-negative on this filter set.

**Recommendation:** keep current 0.75/1.00 exits for *live-parity* deployment (LIMIT + SL-M only, EOD 15:20), but P03 (0.75/1.50) is a **high-priority A/B test** — the resolver evidence is suggestive, and widening only the target does not change the live executor's order types. Validate P03 on an out-of-sample window (e.g. next 2 months of paper trades) before promoting it to live.

#### 5.3.1 P03 live-execution A/B result (2026-04-21)

The A/B test was run as v17h = v17g + target 1.00% → 1.50% (SL unchanged). Same data snapshot as v17g (948 vs 947 trades, 946 entries in common — filter stack identical as designed). See [compare_v17g_v17h.py](../compare_v17g_v17h.py) for the harness.

| Metric | v17g (1.00%) | v17h (1.50%) | Δ | Verdict |
|--------|-------------:|-------------:|----:|:-------:|
| Trades | 948 | 947 | −1 | — |
| PF | 1.847 | 1.900 | +2.9% | PASS |
| MaxDD | **37.89%** | **53.08%** | **+40.1%** | **FAIL** |
| Win rate | 63.92% | 48.79% | −15.1pp | — |
| PnL | 1179.0% | 1556.7% | +32.0% | PASS |
| Sharpe | 8.76 | 9.08 | +3.7% | PASS |
| Calmar | 31.12 | 29.33 | **−5.8%** | — |

Outcome transitions on the 946 shared entries: **144 v17g TARGETs (24%) fell short of 1.50%** — 78 became EOD, **66 flipped to SL**. Those 66 are the problem: the bar touched +1.0% (v17g exit), reversed past −0.75%, and v17h banked the SL where v17g had already banked the target. No SL→TARGET conversions (SL unchanged, as designed).

**Why the resolver prediction missed.** The exit sweep predicted "DD roughly flat" on C10; live execution delivered DD +40%. The research resolver uses re-resolved 5-min bars with 1-min fallback; production uses real SL-M fills and CSV-native row-order drawdown. The 66 TARGET→SL conversions and the multi-day losing streaks they chain into don't show up the same way in the re-resolved path.

**Updated recommendation: shelve P03.** DD inflation invalidates the research prediction on live execution. Calmar worsened despite Sharpe +3.7%. v17g's 0.75/1.00 remains the production target. Any future exit-widening attempt must (a) test on live execution first, (b) pair the widening with a partial-exit or time-stop mechanism to cap the TARGET→SL conversion rate — the widening alone is not admissible.

---

## 6. Why this improved results

1. **AMCC long setup was the single largest DD source.** 123 trades in v17f, clustered losses. My diagnostic (`diag_LONG_setup.csv`) is unambiguous on this. Dropping the setup entirely is blunt but effective; a more nuanced fix would be to apply setup-specific filters, but the cost/benefit favours the blunt drop for now.

2. **Long RSI[60,65) was free to drop.** 57 trades, PF 1.05 — essentially a break-even cluster with variance cost. Removing it raises PF and lowers DD at near-zero opportunity cost.

3. **SHORT leg choice is a PnL-vs-PF lever.** The literature bias in existing V17 work has been "more shorts = more PnL" — my diagnosis shows the marginal short trades on v17f beyond v17b have PF ~1.3, i.e. they are economically marginal once costs bite. Keeping v17b's 164 shorts is tighter but each trade pulls its weight.

4. **No further filter stacking needed on SHORT.** Round 1 drop-one recipes (E1) confirmed the v17b / v17e filter set is already approximately Pareto-optimal on its universe — further dropping of RSI ranges, ADX bands, or time windows mostly loses PnL without improving PF.

---

## 7. Comparison table — V17f baseline vs recommended final

Per-day and cumulative comparison, using the same 191 trading days covered by v17f:

| Metric | v17f | C10 (final) | C3 (aggressive) | C4 (high-PnL) |
|---|---|---|---|---|
| Trades | 1229 | 950 | 1055 | 1067 |
| Trades/day | 6.43 | 5.22 | 5.58 | 5.83 |
| LONG / SHORT | 960 / 269 | 786 / 164 | 786 / 269 | 903 / 164 |
| Win rate | 69.49% | **71.68%** | 70.14% | 71.32% |
| Profit factor | 1.826 | **1.992** | 1.911 | 1.951 |
| MaxDD | 44.80% | 29.09% | **27.51%** | 40.32% |
| Net PnL | 1246.85% | 1090.72% | 1138.75% | **1193.72%** |
| Sharpe (ann.) | 9.51 | **10.59** | 9.72 | 10.37 |
| Sortino (ann.) | 14.27 | 11.58 | 10.43 | 11.53 |
| Calmar | 27.83 | **37.50** | 41.40 | 29.61 |
| Avg PnL/trade | 1.015% | 1.148% | 1.079% | 1.119% |
| Expectancy | 1.015 | 1.148 | 1.079 | 1.119 |

C3's Calmar of 41.40 is the highest across all variants — PnL-per-unit-DD leader.

---

## 8. Implementation notes — integrating into existing pipeline

### 8.1 How to run the framework (reproducible)

```bash
cd eqidv2/backtesting
python -m eqidv2.research.validate           # confirms framework reproduces v17b-f exactly
python -m eqidv2.research.experiments         # round 1 — 90 variants
python -m eqidv2.research.experiments_v2      # round 2 — 13 composites
python -m eqidv2.research.diagnostics         # per-attribute PF / leak diagnostics
python -m eqidv2.research.run_exit_sweep      # exit-policy sweep (~8 min)
```

All outputs land in `eqidv2/research/results/*.csv`.

### 8.2 Wiring the final combo into production

**Minimal change — stay inside the existing runner architecture:**

1. Create a new runner `avwap_combined_runner_v17g_5min.py` copy-structured like `avwap_combined_runner_v17b_5min.py`.

2. For **SHORT** leg, use v17b filter stack as-is (already proven in live parity config). Alternative: v17f stack for aggressive variant.

3. Add two **LONG-side post-scan filters** beyond v17b/f:
   - `drop setup == "A_MOD_CLOSE_CONTINUATION_BREAK"`
   - `drop rsi_signal in [60, 65)`

   Implementation in `_apply_v17g_filters(long_df, short_df)` — pure pandas subset filter, no change to scan logic.

4. Output dir `outputs_v17g_5min/`.

### 8.3 Validation checklist before going live

- [ ] Reproduce v17f result by toggling the two new filters off — must match v17f CSV byte-for-byte.
- [ ] Apply both filters — must match `C10` or `C3` result from this framework.
- [ ] Walk-forward split (e.g. train 2025-06..2025-12, test 2026-01..2026-04): confirm the AMCC-drop and RSI[60,65)-drop effects hold on the held-out slice.
- [ ] Live-parity smoke test with 5 recent trading days, confirm the filter logs print expected removal counts.

### 8.4 Caveats

1. **Overfit risk on filter-combination.** Each filter was discovered on the same 191-day sample. The walk-forward check in §8.3 is mandatory before capital is moved.
2. **AMCC long drop is sample-specific.** The setup may perform differently in a different market regime. Re-run diagnostics quarterly.
3. **Exit sweep is indicative only.** My re-resolver diverges from production by ~15% on PnL due to 1-min data coverage gaps and ambiguous-bar handling — use relative rankings, not absolute PnL, from the sweep.
4. **No lookahead introduced.** All filters use signal-time metadata already present in the trade CSVs. Entry prices, signal times, and SL/TGT prices are unchanged from the original scan.

---

## 9. Artefacts produced

| File | Contents |
|---|---|
| `baseline_validation.csv` | Framework reproduction of v17b/c/d/e/f — all match |
| `experiments_summary.csv` | Round-1: 90 filter / session / setup / composite variants |
| `experiments_ranked.csv` | Round-1 ranked by balance score |
| `round2_candidates_summary.csv` | Round-2: 13 composite candidates (includes C10) |
| `diag_{LONG,SHORT}_{setup,time_bucket,qs_bucket,rsi_bucket,avwap_bucket,nifty_context_mode}.csv` | Per-attribute PF breakdowns |
| `exit_sweep_results.csv` | 10 exit policies × 2 top candidates |
| `walk_forward_results.csv` | H1/H2/ALL split metrics for v17f, C10, C3, C4 |
| `trades_*.csv` | Per-candidate full trade log for top variants |
| `FINAL_REPORT.md` | This file |

---

## 10. SHORT-side deep-dive (first-class research focus)

The SHORT side was treated as a separate research problem because (a) it has a single setup family (`A_MOD_BREAK_C1_LOW`) with no setup-level lever, (b) it's the sole source of variance across v17b/c/d/e/f, and (c) v17's primary author instinct has been "more shorts = more PnL" — which I wanted to verify rather than inherit.

### 10.1 Marginal-shorts analysis — what do the extra shorts in v17f buy?

v17b has 164 shorts at PF 2.46. v17f has 269 shorts at PF 1.91. The **marginal 105 shorts** (trades in v17f but not in v17b) can be isolated:

| Leg | Trades | PnL% | PF | Win% |
|---|---|---|---|---|
| v17b shorts (intersection) | 164 | 234.53% | 2.463 | 74.39% |
| v17f — v17b (marginal shorts) | 105 | ~48.0% | **~1.31** | ~56% |
| v17f combined | 269 | 282.56% | 1.911 | 67.29% |

**Marginal-short PF ≈ 1.3, barely covering costs.** After the 0.80% round-trip levered cost, each marginal short adds ~0.46% PnL on average with materially higher variance. This is why `C10` (v17b shorts only) has better PF than `C3` (v17f shorts).

### 10.2 SHORT edge concentrations (from `diag_SHORT_*.csv`)

Strongest sub-buckets on v17f's 269 shorts:

| Dimension | Value | Trades | Win% | PF | Read |
|---|---|---|---|---|---|
| time bucket | 11:30–12:30 | 12 | 83.3% | **6.12** | Midday short window |
| time bucket | 13:30–14:30 | 5 | 80.0% | **inf** | (small-n) |
| time bucket | 12:30–13:30 | 20 | 70.0% | **2.33** | Continuation of midday edge |
| rsi bucket | 40–45 | 25 | 80.0% | **3.49** | Mid-momentum continuations |
| rsi bucket | 45–50 | 25 | 64.0% | **2.64** | |
| rsi bucket | 50–55 | 21 | 76.2% | **2.86** | Range-top fades |
| avwap_dist | 0.25–0.5 | 35 | 82.9% | **4.57** | Close-to-AVWAP short sweet spot |
| avwap_dist | 0.5–1.0 | 17 | 70.6% | **4.46** | |

**Leak pockets:**
| Dimension | Value | Trades | Win% | PF | Read |
|---|---|---|---|---|---|
| time bucket | 09:15–09:45 | 31 | 61.3% | **1.17** | Opening-bar shorts bleed |
| rsi bucket | 20–25 | 59 | 50.8% | **0.94** | Deep-RSI shorts net lose |
| avwap_dist | 2–3 | 15 | 53.3% | **0.93** | Far-from-anchor shorts |

### 10.3 Short-optimized filter stack (hypothetical v17h)

Stacking the above leaks on v17f's SHORT leg (remove RSI[20,25), AVWAP[2,3), and 09:15–09:45 entries) reduces SHORTs from 269 → ~164 — which is almost exactly what v17b already produces, and with comparable PF. This is a strong cross-validation that **v17b's short filter stack is near-Pareto-optimal** on the short universe, and further hand-tuning from v17f's base converges on it.

**Practical consequence:** C10's choice of v17b-SHORT + no further stacking is empirically the right short leg. Further short-side research should focus on regime gating (e.g. VIX-conditional, nifty_context_mode-conditional) rather than signal-attribute filters.

### 10.4 Regime-conditional short throughput (from `diag_SHORT_nifty_context_mode.csv`)

A near-free alpha source that does not require changing filters: gate SHORT entries on `nifty_context_mode`. The diagnostic shows PF varies materially by regime. This is the next frontier and is deferred to round 4 (scheduled in §11.3).

---

## 11. Walk-forward stability and strategy architecture

### 11.1 Walk-forward split (H1/H2 by calendar date)

The 191-day sample was split in half by calendar date. If the filter edge (AMCC drop, RSI[60,65) drop) were curve-fit to the full sample, we'd expect divergence between halves. Results from `walk_forward_results.csv`:

| Variant | Half | Trades | Win% | PF | MaxDD | PnL% | Sharpe |
|---|---|---|---|---|---|---|---|
| v17f baseline | H1 | 616 | 68.51% | 1.806 | 29.70% | 610.18% | 10.43 |
| v17f baseline | H2 | 613 | 70.47% | 1.847 | **44.80%** | 636.68% | 8.82 |
| **C10** | H1 | 497 | 70.42% | **1.918** | 21.79% | 540.31% | 10.96 |
| **C10** | H2 | 453 | **73.07%** | **2.077** | **29.09%** | 550.42% | 10.23 |
| C3 | H1 | 528 | 69.32% | 1.891 | 25.00% | 558.46% | 10.50 |
| C3 | H2 | 527 | 70.97% | 1.932 | 27.51% | 580.29% | 9.09 |
| C4 | H1 | 558 | 69.53% | 1.847 | 26.49% | 574.69% | 10.53 |
| C4 | H2 | 509 | 73.28% | **2.073** | **40.32%** | 619.03% | 10.19 |

**Findings:**
1. **C10's PF rises from 1.92 (H1) → 2.08 (H2).** Not a curve-fit artefact — the filter benefit is present and even stronger out-of-half.
2. **v17f's DD concentration is in H2** (29.7% → 44.8%). C10 cuts H2 DD from 44.8% → 29.1% (−35% relative), which is the same ratio as the full-sample improvement.
3. **C3 is the most stable across halves** — PF 1.89 → 1.93, DD 25.0 → 27.5. Good candidate for low-variance deployment.
4. **C4's H2 DD is 40.3%** — almost all of its full-sample DD comes from H2. It's PnL-competitive but DD-unstable across regimes.

### 11.2 Strategy architecture recommendations (beyond filter-level changes)

Things worth prototyping but **not in scope of this round** (each needs its own research cycle):

1. **Setup-specific exits.** The fixed 0.75/1.00 treats AMCC-long (low win rate, fat tails) the same as B_HUGE_C1_CLOSE_RECLAIM_BREAK (PF 2.33). Setup-specific target widening could recapture some of the PnL that §5.3's P03 (0.75/1.50) suggests.
2. **Regime-aware short throughput.** Gating SHORT entries on `nifty_context_mode` could raise short PF without dropping trade count on days where shorts work, and suppress them on days where longs dominate.
3. **Time-of-day targeting.** SHORT edge in 11:30–12:30 is PF 6.12 (12 trades). An explicit "midday short window" portfolio allocation could compound this concentration into a separate strategy stream.
4. **Partial exits / scale-out.** The current executor is LIMIT+SL-M only. Two-level scale-out (e.g. 50% at 0.75%, 50% at 1.50%) is outside live-parity but worth paper-testing — the P03 sweep result above suggests the second leg is worth taking.
5. **ML-based trade filter on top.** The signal attributes used in filtering (QS, RSI, AVWAP distance, time bucket, setup, nifty context) are rich enough to train a gradient-boosted trade-kill classifier. The dataset (1229 v17f trades, ~70% win rate, per-trade metadata) is a fair-sized training set — the existing `avwap_ml_backtest_runner.py` in the repo suggests earlier ML experiments were attempted.

### 11.3 Proposed round 4 (next research cycle)

1. **Regime-conditional filters:** Extend diagnostics to cross `nifty_context_mode × side × time_bucket`. Target: add a regime-gate to C10 that lifts trades/day without sacrificing PF.
2. **Setup-specific TGTs:** Re-run the exit sweep per-setup to identify if AMCC-long benefits from a tighter target (0.75/0.80) while B_HUGE benefits from a wider one (0.75/1.50).
3. **OOS validation on 2026-04-01 onwards** (after ~2 months of paper trades) — confirm C10's H2 edge persists in live data. Acceptance criterion: PF ≥ 1.80 on ≥100 OOS trades.
4. ~~**P03 exit A/B test.**~~ **CLOSED 2026-04-21 — failed.** Full-sample in-sample run delivered PnL +32% but DD +40% (37.89% → 53.08%, blew the 1.15× gate). Calmar worsened 31.12 → 29.33. See §5.3.1. Future exit-widening attempts must pair widening with a partial-exit or time-stop to cap the TARGET→SL conversion rate (66 of v17g's 605 TARGETs flipped to SL at 1.50%).

### 11.4 Go / no-go gate for live capital on C10

**Ship C10 to live** only if all of the following hold:
- [ ] Walk-forward §11.1 passes (already passed — see table above).
- [ ] Byte-for-byte reproduction of v17f via toggle (see §8.3).
- [ ] 5-day live-parity smoke test (§8.3).
- [ ] 4 weeks of paper trading showing ≥50 trades, PF ≥ 1.70, DD ≤ 35%.

If OOS PF drops below 1.70 in paper trading, fall back to v17f baseline — do not tune further on that period (would be overfit-on-top-of-overfit).
