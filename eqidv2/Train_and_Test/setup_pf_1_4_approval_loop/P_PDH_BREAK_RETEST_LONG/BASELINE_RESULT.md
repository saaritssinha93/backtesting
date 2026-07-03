# P_PDH_BREAK_RETEST_LONG — BASELINE_RESULT

**Setup:** `P_PDH_BREAK_RETEST_LONG`  **Side:** LONG
**Status in book:** DEMOTED 2026-06-22 (`enabled=False`), parked in `RESEARCH_WATCH` / live-survival demotion. NOT traded live.
**Scope of this work:** research-only, under `Train_and_Test/setup_pf_1_4_approval_loop/`. No edits to `final_setup_conf.py` (root or Train_and_Test copy). No live execution.

---

## 1. Current rules (the idea)

- **Logic:** price breaks the **previous-day high (PDH)**, pulls back and **retests** it, then resumes up — a Tier-3 breakout-retest continuation long. (`reason_tag = pdh_break_retest_long`)
- **Detection source:** `research_v11_tier123_new_setups.py` (Tier 3), corrected session VWAP/regime. Scanner-source / readmitted; the **gate is the edge** (ungated reference is a −Rs173k loser).

## 2. Current config (config source: `final_setup_conf.py` → `P_PDH_BREAK_RETEST_LONG`)

| Knob | Value |
|---|---|
| SL / Target | **0.50 / 0.60** (`exit_alt` on file: 0.50 / 0.80) |
| mask_terms (filter) | `body_pct <= 0.749993` |
| pre_momentum_terms (gate) | `pre_entry_momentum_score >= 75.071712` **AND** `pre3_range_r >= 0.499787` |
| pre_momentum_missing_action | `block` |
| entry_guards | none |
| entry model | next 1-min open after the 5-min signal + paper slippage |
| exit model | resolve SL / Target / EOD on 1-min OHLC to 15:20 IST |

## 3. Data / windows

The task's requested calendar windows (TRAIN `2026-05-18`→pre-test, TEST `2026-06-20`→latest)
are **not available**: P_PDH is demoted, so the conf/fresh pools no longer scan it, and the only
substantial P_PDH candidate pool (`outputs_ID_v11_unified_pool`, 2,498 rows / 215 sessions)
**ends 2026-05-29**. Per the task's fallback rule, the **nearest-available completed sessions** are used and printed:

| Window | Sessions | Range |
|---|---:|---|
| FIT   | 13 | 2026-04-01 .. 2026-04-22 |
| VAL   | 14 | 2026-04-23 .. 2026-05-15 |
| **TRAIN** (FIT+VAL) | 27 | 2026-04-01 .. 2026-05-15 |
| **TEST** (held out) | 9 | 2026-05-18 .. 2026-05-29 |

- Pool used: `Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/pool/` (P_PDH-only slice of the unified pool, built once by `scripts/build_pool.py`).
- Cost basis: repo statutory NSE intraday cost model + **15 bps/leg** adverse slippage on entry and exit (the harness realism default). 5 bps paper is reported as a sensitivity.
- Pipeline: `setup_train_test.py` (entry → 1-min resolve → cost → family dedupe (1 ticker/day) → pre-momentum → mask → portfolio overlay).

## 4. Baseline metrics (net of cost, 15 bps/leg)

| Window | Trades | PF | Net Rs | Win % |
|---|---:|---:|---:|---:|
| TRAIN (04-01..05-15) | 36 | **0.238** | −12,055 | ~31% |
| TEST  (05-18..05-29) | 9  | **0.675** | −876 | ~56% |

(Per-trade detail saved to `baseline_train_trades.csv` / `baseline_test_trades.csv`.)

## 5. Initial diagnosis

- The demoted config is a **clear loser in this honest backtest too** — TRAIN PF 0.24, TEST PF 0.68 — independently corroborating the live-paper failure (PF 0.25, −Rs14,497 over 40 trades).
- **Root cause = the 0.50/0.60 scalp dies by cost.** A 0.60% gross target against 15 bps/leg slippage (30 bps round-trip) **plus** statutory costs leaves almost no edge; one SL wipes several targets. The card's own note: "over-fires ~13×/day on a 0.50/0.60 scalp = death-by-cost."
- The file's own hint (`exit_alt` 0.50/0.80, "wider target") and the live over-firing both point the optimization at: **(a) widen the target / fix the R:R so winners pay for costs, (b) gate harder for selectivity so it is not a high-frequency cost sink.**
- TRAIN baseline trade count (36) is already thin after the tight premom block; the search must keep a meaningful count while lifting PF into the [1.30, 1.70] band.

**Optimization plan:** sweep exit SL/target first (biggest lever vs the cost problem), then add the single most robust selectivity filter/gate, validated on FIT/VAL, confirmed on full TRAIN (band 1.30–1.70), and judged once on TEST (>1.40).
