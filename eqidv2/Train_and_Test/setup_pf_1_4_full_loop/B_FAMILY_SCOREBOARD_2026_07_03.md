# B-family full-loop campaign — SCOREBOARD (2026-07-03, updated after Rounds 2–3)

> **FINAL: 0 / 5 pass after three escalating rounds.** Round 1 = repo-schema search (~700 configs/setup). Round 2 = ENRICHED feature space (~38 point-in-time indicator/price-action features joined from the 5-min feed at the signal bar; 3 mask terms; 800 TPE trials + ~380 sweeps/setup; artifacts in each `<SETUP>/round2/`). Round 3 = disciplined local refinement of the two near-band anchors with one TEST shot each (`<SETUP>/round3*`). Verdict unchanged everywhere; details below and in per-setup `round2/ROUND2_RESULTS.md`.

## Round 2–3 addendum (enriched space: RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/pressure/vol-z/ROC/W%R + gap/day/OR/prev-day geometry + prev-candle structure)

| Setup | R2 best (full TRAIN) | R2/R3 best TEST seen | Final verdict |
|---|---|---|---|
| B_AVWAP_RECLAIM_REVERSAL | PF 1.101 (n=46, day-dom 1.37) | (not unlocked) | **REJECT** |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | PF 0.99 (n=131–220 tier) | (not unlocked) | **REJECT** |
| B_HUGE_RED_FAILED_BOUNCE | PF 0.99 (n=79) | (not unlocked) | **REJECT — demote signal stands** |
| B_HUGE_FAILED_BOUNCE | R3 refined: PF 1.735 (n=27, dom-clean, dbp 0.08) | **TEST n=17 PF 0.746 (neg), robustness failed** | **REJECT** (train-fit, no OOS carry) |
| B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK | PF 1.704 (n=39) / R3: 1.89–1.95 (n=33) | TEST n=14 PF 1.084 (+Rs758, dom-heavy); R3 no TRAIN survivor (day-dom 0.47–0.55 structural) | **REJECT** (day-concentration is structural) |

**Round-2/3 conclusions.** (1) The enriched indicator space measurably helps the two huge-green-bar LONG variants (GREEN went from TEST-negative to TEST break-even) but nothing reaches TEST PF 1.40 with clean concentration. (2) GREEN's profits live in 1–2 big trending mornings — no guard combination (top_n/max_positions/daily-loss/slots) dissolves the day-dominance without killing the sample. (3) B_HUGE_FAILED_BOUNCE's refined afternoon-weakness short (regime≠BULL + below-EMA20 + weak pre-close + expanding pre-range, 12:00–14:00, top-1) is the family's most coherent config on TRAIN (PF 1.74, dom-clean) and still loses OOS — treat any future "huge-bar" candidate with June+ OOS suspicion. (4) Enriched-mask candidates would additionally need a conf-gate extension to read indicator columns at apply time (same live 5-min feed, small change — moot while nothing passes).

---

# Round 1 scoreboard (original)

_Research-only. No live trades. `final_setup_conf.py` untouched. Windows: TRAIN 2026-03-01..2026-05-30, TEST 2026-06-01..2026-07-02 (07-02 excluded everywhere: its EOD 1-min sync had not run — exits unresolvable). Costs: statutory NSE + 15 bps/leg slippage; entry = next 1-min open; exits on 1-min OHLC to 15:20 IST. Protocol: FIT (60%)/VAL (40%) band search (tent at PF 1.80) → full-TRAIN confirm → TEST once per in-band finalist → rescue loop. Gates: TRAIN PF [1.30,1.80] & n≥20, TEST PF>1.40 & n≥5, positive net both, trade≤35% gross / day≤40% / sym≤40% net, day-block p≤0.10, neighborhood + dropout robustness._

## Verdicts — 0 / 5 pass

| # | Setup | Side | Pool basis | Baseline TRAIN / TEST | Best found (full TRAIN) | Best TEST seen | Verdict |
|---|---|---|---|---|---|---|---|
| 1 | B_AVWAP_RECLAIM_REVERSAL | LONG | production raw, 6,965 rows | PF 0.354 (n=1771, −Rs694k) / PF 0.334 (n=730) | PF 1.043 (n=55, rescue premom-off) — never in band | (not unlocked) | **REJECT** |
| 2 | B_HUGE_C1_CLOSE_RECLAIM_BREAK | LONG | production raw, 2,673 rows | PF 0.492 (n=743, −Rs248k) / PF 0.475 (n=293) | PF 1.698 (n=21, in band, day-dom 0.86) | n=3, PF 0.166 | **REJECT** |
| 3 | B_HUGE_RED_FAILED_BOUNCE | SHORT | production raw, 1,998 rows | **PF 0.716 (n=48, −Rs5.8k) / PF 0.720 (n=41, −Rs4.6k) — this is the ACTIVE conf gate** | PF 0.799 (n=23) — never in band | (not unlocked) | **REJECT + demote-signal for the active book** |
| 4 | B_HUGE_FAILED_BOUNCE | SHORT | as-promoted research scan, 2,932 rows | PF 0.373 (n=1628, −Rs583k) / PF 0.503 (n=673) | PF 1.043 (n=26, sym-dom 3.98) | (not unlocked) | **REJECT** |
| 5 | B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK | LONG | as-promoted research scan, 772 rows | PF 0.493 (n=550, −Rs161k) / PF 0.355 (n=164) | PF 1.53 (n=26, clean TRAIN dom) | n=8, PF 0.524 (also 1.411/n=36 → TEST n=11 PF 0.249) | **REJECT** (train-fit / test-collapse) |

Per setup: ~150–168 single-knob sweeps + 500 Optuna-TPE FIT/VAL trials + ~45–50 rescue iterations (≈700 configs each; full logs in each folder's `iteration_log.csv` / `trials.csv` / 8 campaign reports).

## Structural findings

1. **Every B-family detector is a high-frequency net loser ungated** (13–34 trades/day, PF 0.35–0.50 at realistic costs). The family needs ~90% trade suppression to break even, and the surviving pockets are too thin (n≈21–37 over 3 months) to separate edge from luck — every in-band TRAIN pocket failed TEST or a domination/robustness gate.
2. **B_HUGE_RED_FAILED_BOUNCE (ACTIVE in live conf) failed re-validation**: its own promoted 3-term pre-momentum gate is net-negative on BOTH the mandated TRAIN (PF 0.716) and TEST (PF 0.720) windows. The 2026-06-13 promotion numbers (train 2.90 / test 3.49) came from a shorter, favorable split. Consistent with the 2026-07-01 PF-band campaign demote-candidates finding. **Recommend user review for demotion — conf untouched by this campaign.**
3. **Catalog-detector shadowing (new plumbing discovery)**: `B_HUGE_FAILED_BOUNCE` and `B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK` can NEVER fire in the production pipeline: (a) `candidate_scan` drops setups without a `v6.SETUP_EXIT_RULES` entry; (b) `_dedupe_candidate_frame` keeps one label per (ticker, candle) by quality then ALPHABETICAL name — GREEN always loses to `B_HUGE_C1_CLOSE_RECLAIM_BREAK`. Pools were built with a research scan on the "as-promoted" universe (production allowlist + the 2 targets, production collapse unchanged): `B_HUGE_FAILED_BOUNCE/scripts/research_scan_catalog.py` → `B_HUGE_FAILED_BOUNCE/pools/_research_scan_catalog_20260301_20260701/` (collapsed: 2,932 / 772 rows; pre-collapse diagnostics: 4,314 / 6,498).
4. **2026-07-02 excluded from TEST everywhere**: 5-min feed complete but 1-min EOD sync had not run (rows stop 09:30) — SL/target exits unresolvable. Actual TEST = 2026-06-01..2026-07-01.

## Rerun commands (per setup, from repo root)

```
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\<SETUP>\scripts\recreate_pool.py
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\<SETUP>\scripts\run_full_loop.py --trials 500 --time_budget_min 60 --seed 7
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\<SETUP>\scripts\write_reports.py
# catalog-pool scan (shared by setups 4+5, post-market only):
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_FAILED_BOUNCE\scripts\research_scan_catalog.py
```

> **DO NOT MOVE ANYTHING TO FINAL CONFIG — no candidate passed; nothing is proposed for promotion.**
