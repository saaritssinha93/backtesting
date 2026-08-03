# V7-live vs V11-backtest — Parity Reconciliation Report

**Window:** last 7 NSE sessions, 2026-06-29 → 2026-07-07 (IST).
**Generated:** 2026-07-07, evening (post-close).
**Live side:** paper track (`paper_trades_<date>_id_5min_v7.csv`). **Backtest side:** v11 `live_parity` + `final_setup_conf`, per-day.
**Reconciler:** [`v7_v11_parity/reconcile.py`](reconcile.py) → outputs in [`v7_v11_parity/out/`](out/).

---

## 0. Verdict — **FAIL** (not at parity)

| Metric | Value | Tolerance | Result |
|---|---|---|---|
| Trade match rate | **5.5%** (6 of 115 union) | ≥ 90% | ❌ |
| Signal match rate | **6.4%** | ≥ 90% | ❌ |
| Net-P&L divergence (statutory, both sides) | **−192.8%** (live −3,738 → bt −10,942) | ≤ 25% | ❌ |
| Matched trades within 10 bps net-of-notional | **0 of 6** | all | ❌ |
| Daily net-P&L correlation | 0.581 | — | weak |

**Headline:** the live executor and the V11 backtest are **taking almost entirely different trades** — this is *not* a fill-price/cost discrepancy on shared trades. The live executor realizes only a small, latency-filtered subset of the book that the strategy (backtested on **live's own scanner output**) would take. On three of seven sessions (07-02, 07-03, 07-06) the live executor emitted **zero** trades while the backtest found 24, despite the live 5-min scanner accepting hundreds of candidates those days.

> **Read this before "fixing" anything:** over this window **both sides lost money** — live −3,738 vs backtest −10,942 (statutory; −18,493 with 5 bps/leg slippage). The live emission gap *reduced* realized losses here. The parity defects below are real and should be fixed for **correctness and predictability**, but closing them would have made live lose **more** over these particular 7 sessions. The strategy book itself was net-negative this week on both sides.

---

## 1. Scope & input alignment (STEP 0–1)

### 1.1 The 7 sessions and live files used

| Date | live paper_trades | live signals L/S | live real trades | backtest trades | note |
|---|---|---|---|---|---|
| 2026-06-29 | ✅ 31 rows | 12 / 19 | 29 (+2 stale) | 20 | both sides active |
| 2026-06-30 | ✅ 7 | – / 7 | 7 | 12 | short-only live |
| 2026-07-01 | ✅ 1 | 1 / – | 1 | 4 | long-only live |
| 2026-07-02 | — | – / – | **0** | 10 | live executor emitted nothing |
| 2026-07-03 | — | – / – | **0** | 9 | live executor emitted nothing |
| 2026-07-06 | — | – / – | **0** | 5 | live executor emitted nothing |
| 2026-07-07 | ✅ 2 | – / 2 | 2 | 16 | today (complete; EOD close 15:20) |
| **Total** | | | **39** | **76** | |

### 1.2 Both sides aligned on

- **Same dates / session window** 09:15–15:30 IST; timestamps IST (mixed `+05:30`/`+0530` offsets normalized in the reconciler).
- **Same config file:** V7-live (`eqidv2_final_conf_live_bootstrap.py`) and V11 (`avwap_5min_ID_v11_backtesting.py::_activate_final_setup_conf`) both `import final_setup_conf` from repo root (19-setup enabled book). No two-config drift — this is the V7/V11 **ID path** (no two-session RS shortlist, so cross-sectional RS-rank divergence does not apply).
- **Same candidate source:** the backtest ran `mode=live_parity`, consuming the **live scanner's own JSON snapshots** (`signal_discovery_v7_5mins_ID/json/candidate_tickers_YYYYMMDD_HHMM.json`). Confirmed for all 7 days including the three zero-live days. This is the strongest possible parity basis — the backtest is *not* independently reconstructing signals from parquet.
- **Same statutory cost model** (`nse_intraday_costs.py`, rates_as_of 2026-06, ~5.3 bps of turnover). The reconciler applies it identically to both sides; backtest `trades.csv` is price-only (`v6_cost_rs=0`), so costs were recomputed. Validated against a live row (KERNEX: 36.57 total_cost, 5.31 bps).
- **07-07 methodology parity:** the pre-existing 07-07 backtest dir had run raw (`profile=none`); it was re-run at **8 workers** (per the machine's 8-worker cap) into `backtesting_result_v11/2026-07-07_conf_parity/`, and its `inputs.txt` diffs **identical** (except date) to the six good days.

### 1.3 Config-as-of (point-in-time book)

`final_setup_conf.py` was committed 2026-07-03 (inside the window); `S9_MIDDAY_LOSE` & `E_ORB_BREAKOUT_LONG` enabled 2026-06-30, `DOC5D_AVWAP_RECLAIM_LONG` 2026-07-01. The reconciler flags any backtest trade for a setup **before** its live-enable date as `config_as_of_drift`. **Result: 0 such rows** — the current-book backtest introduced no anachronistic setups on the earlier days. Config-as-of is therefore **not** a contaminating factor here.

---

## 2. Aggregate results (STEP 4)

### 2.1 Per-day

| Date | live trades | bt trades | matched | live-only | bt-only | live net (₹) | bt net (₹) |
|---|--:|--:|--:|--:|--:|--:|--:|
| 06-29 | 29 | 20 | 3 | 26 | 17 | −4,349.8 | −4,809.4 |
| 06-30 | 7 | 12 | 2 | 5 | 10 | −448.4 | −4,043.5 |
| 07-01 | 1 | 4 | 1 | 0 | 3 | +1,321.0 | +2,055.4 |
| 07-02 | 0 | 10 | 0 | 0 | 10 | 0.0 | +204.7 |
| 07-03 | 0 | 9 | 0 | 0 | 9 | 0.0 | +3,069.3 |
| 07-06 | 0 | 5 | 0 | 0 | 5 | 0.0 | −2,090.7 |
| 07-07 | 2 | 16 | 0 | 2 | 16 | −260.3 | −5,328.0 |
| **Σ** | **39** | **76** | **6** | **33** | **70** | **−3,737.5** | **−10,942.2** |

Note 06-29: live took **more** trades than the backtest (29 vs 20) — the divergence is **bidirectional**, not "backtest always over-fires."

### 2.2 Per-setup (ranked by unmatched) — the core signal

| setup | live n | bt n | matched | live-only | bt-only | interpretation |
|---|--:|--:|--:|--:|--:|---|
| C_OR_BREAKDOWN | 13 | 18 | **1** | 12 | 17 | same setup, **different tickers** (selection divergence) |
| A_MOD_BREAK_C1_LOW | 7 | 18 | 2 | 5 | 16 | backtest fires ~2.5× more of the same setup |
| L_DOUBLE_BOTTOM_VWAP | 3 | 16 | 0 | 3 | 16 | **RAW_PRE_GATE readmit** over-fires in bt |
| G_HIGHER_HIGH_BREAK | **0** | 14 | 0 | 0 | 14 | **live never emits it** — gate mismatch |
| L_PRESSURE_BURST_VWAP | 8 | **0** | 0 | 8 | 0 | opposite: **live emits, bt never readmits** |
| E_VWAP_LOSE_EARLY_SHORT | 3 | 0 | 0 | 3 | 0 | live emits, bt drops |
| E_ORB_BREAKOUT_LONG | 1 | 3 | **1** | 0 | 2 | FUSION 07-01 matched cleanly ✅ |
| B_HUGE_RED_FAILED_BOUNCE | 2 | 3 | 1 | 1 | 2 | |
| D_EMA20_REJECTION | 1 | 0 | 0 | 1 | 0 | |
| G_LOWER_LOW_BREAK | 1 | 1 | 1 | 0 | 0 | matched (RPTECH, exit-path diff) |

---

## 3. Trade-level reconciliation (STEP 3)

### 3.1 MATCHED (6) — where both sides took the same (ticker, side, setup, 5-min bar)

Signal bars aligned exactly on all 6 (`bar_dt_min=0`). Entry slippage is **small** (0–7 bps), consistent with the live LTP-fill vs backtest next-1-min-open model. But **all 6 breach the 10-bps net-of-notional tolerance**, driven by exit-path and small compounding fill diffs:

| date | ticker | setup | entry bps | exit bps | live outcome | bt outcome | note |
|---|---|---|--:|--:|---|---|---|
| 06-29 | BRIGADE | C_OR_BREAKDOWN | 7.0 | 11.9 | SL | SL | same path, minor slip |
| 06-29 | ICRA | A_MOD_BREAK_C1_LOW | 1.9 | 5.0 | EOD_CLOSE | EOD | same path |
| 06-29 | **RPTECH** | G_LOWER_LOW_BREAK | 4.0 | **23.0** | **EOD_CLOSE** | **SL** | **exit-path divergence** — bt stopped out (750.29), live rode to EOD (748.57) |
| 06-30 | NMDC | A_MOD_BREAK_C1_LOW | 0.0 | 4.7 | SL | SL | clean |
| 06-30 | MARKSANS | B_HUGE_RED_FAILED_BOUNCE | 4.2 | 9.0 | SL | SL | clean |
| 07-01 | FUSION | E_ORB_BREAKOUT_LONG | 0.0 | 0.0 | TARGET | TARGET | near-perfect ✅ |

The **exit slippage asymmetry** is real: the backtest fills exits at the exact SL/target level (0 bps), while live charges ~5 bps on stop-outs. This shows as 5–23 bps exit divergence. It is a genuine but **minor** contributor (only 6 matched trades).

### 3.2 LIVE-ONLY real (33) + stale-skips (2)

Live took these; the backtest produced no matching signal. Dominated by **06-29** (26): `L_PRESSURE_BURST_VWAP`×8, `C_OR_BREAKDOWN`×10, `L_DOUBLE_BOTTOM_VWAP`×3, `E_VWAP_LOSE_EARLY_SHORT`×3, `A_MOD`×2. Plus 06-30 (5) and 07-07 (2). The 2 stale-skips (`ENTRY_SKIPPED_STALE_SIGNAL`, RALLIS/SWSOLAR 06-29) are live-only latency artifacts — expected, never executable.

### 3.3 BACKTEST-ONLY (70)

Backtest signalled; live didn't. Tagged causes: `backtest_only` 37, `backtest_only_live_zero_day` 17, `raw_pre_gate_readmit_bug` 16. Per-day×setup in [`out/parity_backtest_only.csv`](out/parity_backtest_only.csv). The 24 backtest trades on 07-02/03/06 (against 0 live) are the largest single bloc.

---

## 4. Root-cause analysis, ranked by evidence & impact (STEP 5)

### RC-1 — Entry-engine **handoff freshness race** (DOMINANT). *Impact: very high · Effort: medium*

The live 5-min scanner is healthy and productive, but the separate **1-min entry engine** starves. It reads candidates from `signal_discovery_v7_5mins_ID/latest/latest_candidate_tickers.json` under a **30-second** handoff deadline (`max_signal_handoff_lag_sec=30.0`), while the scanner writes that pointer at **slot+45–60 s** (the documented feed-race). When the pointer is stale, the entry engine builds **zero entry rows**.

**Evidence — 07-03 entry-engine log** (`logs/eqidv2_entry_engine_1min_v5_id_2026-07-03.log`):
```
candidate_source_path = ...\signal_discovery_v7_5mins_ID\latest\latest_candidate_tickers.json
max_signal_handoff_lag_sec: 30.0
[PROGRESS] raw_entry_rows=0 slot=14:55
[PROGRESS] entry_rows=0 phase=FRESHNESS_GATE slot=14:55
```
**Evidence — 07-03 live scanner funnel** (aggregated from `candidate_tickers_audit_2026-07-03.jsonl`, 61 slots): 3,726 firehose → 1,855 raw → 472 `final_setup_conf_accepted` → **429 written / 371 overlay-selected**. The scanner accepted hundreds; the entry engine emitted **zero**; no `signals_2026-07-03_*.csv` or `paper_trades` file exists. The backtest replayed the same per-slot JSON (no 30 s deadline) and produced 9 tradeable signals (`live_parity_pipeline_stats.csv`: `entry_engine_signals=9`).

**Explains:** all 24 zero-day backtest-only trades (07-02/03/06), and much of the general under-emission on active days (the entry engine only catches the handoff-lucky slots → a *different, smaller* subset than the backtest's pool selection — e.g. C_OR_BREAKDOWN 06-29: ~12 candidates each side, only 1 shared).

### RC-2 — `G_HIGHER_HIGH_BREAK` **gate mismatch** (live never emits). *Impact: medium-high · Effort: low*

Backtest fired `G_HIGHER_HIGH_BREAK` **14×** across 06-30/07-01/07-02/07-03; live emitted it **0×** all week. The live detector's G_HIGHER entry gate does not match the promoted conf gate (`pre2_mom_r≥0.55 & adx≥26`) — corroborated by the standing note that the live runners use the wrong G_HIGHER gate. This is a clean, isolated per-setup config divergence (not the handoff race — live emitted other setups fine on those days).

### RC-3 — **RAW_PRE_GATE readmit pool** populated inconsistently. *Impact: medium · Effort: medium*

`L_DOUBLE_BOTTOM_VWAP`: backtest **16** / live **3**. `L_PRESSURE_BURST_VWAP`: live **8** / backtest **0** — an *asymmetry in opposite directions*. These setups are readmitted from the full firehose pool (`raw_all_setup_candidates`), which the live emission path and the v11 replay populate differently. The live scanner's per-slot `raw_all_setup_candidate_count` is the pool that readmit/overlay draw from; when it is short-populated in live emission, readmit setups drop — while the backtest readmits from the full ranked frame. (Matches the per-slot raw_candidates population defect.)

### RC-4 — Same-setup **selection/ranking/cap divergence**. *Impact: medium · Effort: follows RC-1*

Even where both sides emit a setup, the realized tickers differ (C_OR_BREAKDOWN 06-29: 1/13 live and 1/18 bt overlap). Root: RC-1 (only handoff-lucky slots survive live) compounded by live-only caps not modeled in the backtest (20-position cap, ₹10k daily-loss brake, 0.3% max-entry-slip gate, `EQIDV2_LATE_DETECTION_MAX_LAG_SEC=30`) and `entry_guards.top_n` not being enforced on either side.

### RC-5 — **Exit-fill slippage** on matched trades. *Impact: low · Effort: low*

Backtest exits at exact SL/target (0 bps); live charges ~5 bps on stops → 5–23 bps exit divergence, and occasional exit-path flips (RPTECH: bt SL vs live EOD). Modeling 5 bps/leg on the backtest **widens** total divergence to −394.8% (bt net −18,493) — i.e. this makes the backtest look worse, confirming slippage is *not* what explains the gap.

### RC-6 through RC-9 — checked, **not** primary drivers

- **Cost model** (RC/STT/GST/etc.): identical statutory code both sides; validated to the rupee. Not a divergence source.
- **Data / bar source:** matched-trade entry diffs are 0–7 bps → 5-min/1-min bars agree closely; no corporate-action/symbol-change artifacts seen. `uncovered_fallback_rejected_by_universe` ≈ 750–820/day shows a live-universe exclusion, but it affects *fallback* candidates, not the conf book here.
- **Signal timing / lookahead:** backtest enters at the **next 1-min open after** the 5-min signal bar — no lookahead. The only timing issue is RC-1's handoff race.
- **Gate/qualification state:** `DRY_RUN`/`INSUFFICIENT_HISTORY` → constant across the window; per-day snapshots exist and don't move. Not a factor.
- **Timezone / bar indexing:** all 6 matched bars aligned exactly (`bar_dt_min=0`); no off-by-one.
- **Config-as-of:** 0 anachronistic backtest rows (§1.3).

---

## 5. Fixes, ranked by impact ÷ effort

| # | Fix | Impact | Effort | Notes |
|---|---|---|---|---|
| 1 | **Close the handoff race (RC-1).** Raise `max_signal_handoff_lag_sec` 30→~75 s to cover scanner write latency, **or** have the entry engine consume the per-slot snapshot (`candidate_tickers_YYYYMMDD_HHMM.json`) instead of only `latest/`, **or** gate the entry engine on scanner feed-completion (the mechanism already exists in discovery). | ★★★★★ | ★★★ | Recovers the 24 zero-day trades + most active-day under-emission. **Predictability win — but net-negative over *this* window; validate on positive windows before enabling in anger.** |
| 2 | **Align live `G_HIGHER_HIGH_BREAK` gate to the conf** (`pre2_mom_r≥0.55 & adx≥26`). | ★★★★ | ★ | Isolated one-setup config edit; unlocks 14 trades/week. Backtest-verify first (G_HIGHER bt net was ≈ flat, +37). |
| 3 | **Fix RAW_PRE_GATE readmit pool population (RC-3)** so `L_DOUBLE_BOTTOM_VWAP` / `L_PRESSURE_BURST_VWAP` readmit identically live & backtest. | ★★★ | ★★★ | Removes the largest remaining per-setup asymmetry. |
| 4 | **Model live caps in the backtest** (20-position, ₹10k brake, 0.3% max-slip, late-detection 30 s) as an optional overlay, so the backtest's *realized* book matches live once RC-1 is fixed. | ★★★ | ★★ | Turns the backtest into a faithful realized-book predictor. |
| 5 | **Model 5 bps/leg exit slippage in the backtest** for honest net P&L. | ★ | ★ | Correctness, not gap-closing (worsens bt PnL). `reconcile.py --slippage-bps 5`. |
| 6 | **Sync the stale `Train_and_Test/final_setup_conf.py` mirror** (old G_HIGHER exits, missing DOC5D) to prevent cwd-dependent config drift; satisfies the qualification Q6 attestation. | ★★ | ★ | Housekeeping; unrelated to this window's gap but a latent parity hazard. |

---

## 6. How to reproduce / run daily (STEP 6)

```bash
# from repo root
python v7_v11_parity/reconcile.py --last-n 7 \
  --bt-override 2026-07-07=C:\TradingData\eqidv2\backtesting_result_v11\2026-07-07_conf_parity

# honest exit-slippage variant
python v7_v11_parity/reconcile.py --last-n 7 --slippage-bps 5 --out v7_v11_parity/out_slip5
```

`reconcile.py` is **standalone** (no live/production imports; statutory costs embedded & validated) and **read-only** (writes only under `--out`). It auto-discovers the last N sessions from the live paper-execution logs, matches on `(date, ticker, side, setup, 5-min signal bar)` within a 1-bar tolerance, buckets MATCHED / LIVE-ONLY / BACKTEST-ONLY (+ stale-skips), root-cause-tags every unmatched row, and emits the tables below plus a PASS/FAIL exit code.

### Deliverables in [`v7_v11_parity/`](.)
- [`reconcile.py`](reconcile.py) — reusable daily EOD parity checker.
- [`parity_report.md`](parity_report.md) — this report.
- [`out/parity_per_day.csv`](out/parity_per_day.csv), [`out/parity_per_setup.csv`](out/parity_per_setup.csv), [`out/parity_matched_trades.csv`](out/parity_matched_trades.csv), [`out/parity_live_only.csv`](out/parity_live_only.csv), [`out/parity_backtest_only.csv`](out/parity_backtest_only.csv), [`out/parity_signal_reconciliation.csv`](out/parity_signal_reconciliation.csv), [`out/parity_summary.json`](out/parity_summary.json), [`out/parity_report_generated.md`](out/parity_report_generated.md).

---

## 7. Caveats & what was *not* modified

- **No live/production code was changed.** The only backtest run was a non-destructive re-run of **07-07** into a new `_conf_parity` dir (8 workers) to make today conf-matched; the six historical days reused their existing (today-regenerated, live-matched) dirs.
- **Point-in-time universe** (V7 punchlist P1-8) remains unresolved system-wide; `live_parity` sidesteps most of it by replaying live's own snapshots, but a survivorship-biased universe still underlies any historical reconstruction.
- The **backtest is a strategy-intent oracle, not a live-execution oracle** — it deliberately omits live's caps/latency (RC-4). Once RC-1/RC-4 are addressed, the backtest can be made to predict the *realized* book, not just the *intended* book.
- **The strategy was net-negative on both sides this week.** Parity fixes improve fidelity and predictability; they do not, on this sample, improve P&L.
