# S* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 days). Generated 2026-06-12.*

> Only **2 S\* setups have a population**: S_BB_SQUEEZE_SHORT (gated clean pool 196/16) and
> S_MACD_HIST_FLIP (raw pre-gate pool 4808/297; gated out). The other S\* shorts
> (S_MACD_BEAR_VWAP, S_TREND_REJECT, S_MFI_OVERBOUGHT_FAIL, S_CCI_EXTREME_FLIP,
> S_DOUBLE_TOP_VWAP, S_PRESSURE_DUMP_VWAP …) emit **nothing** — gated behind
> `ENABLE_NOISY_ADVANCED_SHORTS`. Engine: `S_iterate.py` (greedy + 2-term + 40k random,
> robustness-first objective, day-block bootstrap on finalists). NET of cost.

---

## 1. Executive summary — S contributes 0 setups

| Setup | pool (tr/te) | baseline train/test | Verdict |
|---|---|---|---|
| S_BB_SQUEEZE_SHORT (SHORT) | clean 196/16 | 0.94 / 0.98 (−Rs6k) | **REJECT** — sample-capped, no significant edge |
| S_MACD_HIST_FLIP (SHORT) | raw 1500/297 | 0.83 / 1.14 (−Rs73k) | **REJECT** — gaudy PF but single-day artifact |

Both fail. **The headline lesson: S_MACD_HIST_FLIP produced spectacular-looking numbers (train PF
4–5, test PF 5–10) that are entirely a day-concentration mirage** — the day-block bootstrap and a
day-share check caught it. This is exactly the false positive the anti-overfit discipline exists for.

---

## 2. S_BB_SQUEEZE_SHORT — REJECT (sample-capped)
Short counterpart of the rejected L_BB_SQUEEZE_LONG. Baseline near-breakeven (train 0.94 / test 0.98,
−Rs6k) at exit 1.00/1.50.
- **train-PF-max configs overfit:** `body_pct≤0.94 & vol_ratio≥2.32` → train 2.08 [2.36/1.90] but **test 0.58** (p0.85). Test collapses.
- **robustness-first:** all-period-positive 3–4 term greedy configs (best `vol_ratio≥2.32 & pre1_adx≤38.4 & vwap_dist_atr≤−1.81 & pre3_close_pos≥0.93`, exit 1.1/1.5 → train 1.91 [1.88/1.94], test 2.66, minpf 1.88) — but **p 0.146** (not significant), 3–4 terms (overfit risk), and test cut to **n=8** of only 16 clean-pool test trades.
- **Verdict:** no significant edge; fundamentally capped by the tiny clean-pool test sample. Reject.

## 3. S_MACD_HIST_FLIP — REJECT (single-day artifact)
MACD-hist negative flip below VWAP. Baseline train 0.83 / test 1.14, **−Rs73k** (heavy loser).
The search found 67 train-PF≥2 configs with eye-popping PFs — but **not one reaches p<0.10** (best 0.12–0.18):

| config | train [h1/h2] | test (n,days) | day-block p |
|---|---|---|---|
| pre3_close_pos≤0.355 & vwap_dist_atr≤−3.39 & regime==BEAR & pre1_adx≥31.6 (0.7/2.5) | 3.97 [3.92/4.03] | 10.74 (14) | **0.29** |
| vwap_dist_atr≤−4.498 & pre2_mom_r≤−0.029 (0.9/1.0) — *cleanest 2-term* | 2.07 [2.22/1.98] | 5.51 (14, **5 days**) | **0.175** |

**Day-concentration check (cleanest config), decisive:**
- TEST: 14 trades across **5 days**; **top-2-day share = 112%** → a single day **2026-05-12 made Rs 7,233 of the Rs 7,043 total** test PnL. Remove that one crash day → net loser in test.
- Monthly is similarly day-thin (most months 4–7 distinct days).

The shared pattern across all top configs — `vwap_dist_atr ≤ −3.4 to −4.5` (entry 4+ ATR *below*
VWAP) + `regime==BEAR` — is a **falling-knife short on already-broken stocks in bear tape**. It
prints huge PF on the few crash days and nothing otherwise. **A tail-event lottery, not a daily
edge.** Reject (and raw-pool gated besides).

---

## 4. Why the day-block bootstrap mattered here
A naive train/test PF view would have *promoted* S_MACD_HIST_FLIP (train 4–5, test 5–10 looks
elite). The day-clustered bootstrap (resampling DAYS, not trades) returned p≥0.12 despite PF>5 —
the signal that the profit lives in a handful of days. The follow-up day-share check confirmed a
single day carried the entire OOS result. **This is the canonical day-concentration trap, caught.**

## 5. Recommended changes
- **Reject both.** Add nothing to the active book. S_BB_SQUEEZE_SHORT and S_MACD_HIST_FLIP recorded
  in `RESEARCH_WATCH_CONF` (enabled=False) with findings + revalidation triggers.
- S_MACD_HIST_FLIP: do NOT promote — it is a bear-crash-day artifact.

## 6. Family scorecard
**S contributes 0 setups.** Active book unchanged at 8. Remaining families: T*/MR*.
