# D* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 days, ~1,200-ticker NSE universe). Generated 2026-06-11.*

> Method: D_EMA20_BOUNCE (555) and D_EMA20_REJECTION (486) read from the admitted
> pre-dedupe clean pool; D_AVWAP_LOSE_REVERSAL (only ~18 admitted) read from RAW
> detections (sampled 1,500). Resolved at fixed production exits on 1-minute data,
> NET of NSE cost, with MAE/MFE and the per-setup pre-entry momentum gate evaluated
> ON/OFF. Engine: `validate_D_setups_filters.py`.

---

## 1. Executive summary

| Setup | TRAIN PF | TEST PF | Pre-momentum | Verdict |
|---|---|---|---|---|
| **D_EMA20_REJECTION** (SHORT) | 0.71 → **1.35*** | 1.20 → **4.07*** | **HELPS — it IS the edge** | **PROBATION candidate** |
| D_EMA20_BOUNCE (LONG) | 0.58 | 0.56 | **HURTS** (remove) | reject (only TREND cell positive) |
| D_AVWAP_LOSE_REVERSAL (SHORT) | 0.69 | 0.76 | n/a | reject (structural loser) |

\* with the pre-momentum gate ON (full-sample PF 1.35, n=57/36 days, day-block p 0.179).

**Headline finding (and it directly answers the with/without-pre-momentum question):**
the pre-entry momentum gate has **opposite effects on the two EMA setups** —
- It **HURTS the LONG** (`D_EMA20_BOUNCE`): premom-ON 0.54/0.45 vs premom-OFF 0.58/0.56; what it *rejects* (0.61/0.66) is better than what it keeps. The gate is counterproductive.
- It **MAKES the SHORT** (`D_EMA20_REJECTION`): premom-ON flips train **0.71→1.23** and test **1.20→4.07**; full-sample **PF 1.35** (57 trades). The gate (`pre10_mom_r≤0.16, pre5_mom_r≥0.12, sig5_adx≥20`) is mechanically the edge — it demands *recent downward momentum + an ADX trend*, i.e. a genuine trend-rejection short rather than a random EMA touch.

So D_EMA20_REJECTION is the one D setup worth carrying (probation); the LONG bounce and the AVWAP-lose short have no robust edge.

---

## 2. Setup definitions
(Detection `avwap_5min_ID_v2_backtesting.py` L688/724/731; exits `..._v6` L57-59; PM gates `..._v11` L337-344.)

- **D_EMA20_BOUNCE** (LONG, `ema20_trend_bounce`): near_ema20 (|close−EMA20|≤0.35·ATR), long_struct (close>open & close_loc≥0.60), uptrend_stack (close>EMA20 & EMA20≥EMA50), rs_pct>−0.05, vol_ratio≥1.3, regime≠BEAR. Exit 0.70/1.50. PM gate `pre3_range_r≥0.2923 & pre_entry_momentum_score≤78.3448`. Prod mask `(vol_ratio≤1.60 OR vwap_dist_atr≥−0.39) AND signal_minute≤705`.
- **D_EMA20_REJECTION** (SHORT, `ema20_trend_rejection`): near_ema20, short_struct (close<open & close_loc≤0.40), downtrend_stack (close<EMA20 & EMA20≤EMA50), rs_pct<0.10, vol_ratio≥1.3, regime≠BULL. Exit 0.75/1.30. PM gate `pre10_mom_r≤0.1566 & pre5_mom_r≥0.1249 & sig5_adx_calc≥20`. Prod mask `body_pct≥0.89 AND ranker_score≥0.39` + residual late overlay.
- **D_AVWAP_LOSE_REVERSAL** (SHORT, `lose_session_vwap_from_above`): short_struct, prev_close>prev_VWAP, close<VWAP, rs_pct<0.15, vol_ratio≥1.4, regime≠BULL. Exit 1.00/1.50. (Bearish mirror of B_AVWAP_RECLAIM_REVERSAL.)

---

## 3. Train/test results (fixed exits, NET)

| Setup | TRAIN n / PF / win | TEST n / PF / win | mfe_R | immFail |
|---|---|---|---|---|
| D_EMA20_BOUNCE | 443 / 0.58 / 32% | 112 / 0.56 / 32% | 0.74–0.90 | 1.6–1.8% |
| D_EMA20_REJECTION (all) | 410 / 0.71 / 39% | 76 / 1.20 / 47% | 0.77–0.86 | 0.5–2.6% |
| **D_EMA20_REJECTION (premom ON)** | **52 / 1.23** | **5 / 4.07 / 80%** | — | — |
| D_EMA20_REJECTION (premom ON, full sample) | **57 trades / 36 days / PF 1.35 / win 53% / net +₹6,711 / day-block p 0.179** | | | |
| D_AVWAP_LOSE_REVERSAL | 1241 / 0.69 / 40% | 259 / 0.76 / 44% | 0.56–0.61 | 0.5–1.2% |

---

## 4. Diagnosis

### 4.1 D_EMA20_REJECTION (SHORT) — the keeper
Captures a trend-rejection short: price touches a falling EMA20 (downtrend stack) and rejects. **Without the pre-momentum gate it's a coin-flip loser (PF 0.71 train)** — many EMA touches are not real rejections. **The pre-momentum gate is what separates real rejections** (recent down-momentum `pre5_mom_r≥0.12`, contained `pre10_mom_r≤0.16`, trending `sig5_adx≥20`) from noise, lifting PF to 1.35. mfe_R ~0.8 with 53% win supports the 1.30 target; the 0.75 stop is reasonable (immFail ~0.5–2.6%).
- **Drop the production `body_pct≥0.89 & ranker_score≥0.39` mask** — it over-tightens to **n=6** (useless). The pre-momentum gate alone is the right filter.
- **Caveat:** day-block p 0.179 (not significant at 0.10); 57 trades / 36 days = modest. PROBATION, not size-up.

### 4.2 D_EMA20_BOUNCE (LONG) — reject
The bullish mirror, but a net loser (PF 0.56–0.58, 32% win). The bounce mostly fails (mfe_R 0.74–0.90 but low win). **Its pre-momentum gate actively HURTS** (premom-ON 0.45 test). Only the **TREND-regime** sub-cell is positive (n=37, PF 1.28) but on **11 days only** (day-block p 0.332) — too concentrated to trust. **No robust edge.** If anything, remove its pre-momentum gate; do not promote.

### 4.3 D_AVWAP_LOSE_REVERSAL (SHORT) — reject
Bearish VWAP-lose short. Net loser at every bucket (PF ~0.7, win 40–44%, mfe_R 0.56 — doesn't run). Unlike its long mirror B_AVWAP (which un-inverted to a near-VWAP edge), the SHORT version finds nothing — Indian-equity upside bias + shorting a fresh down-move into bounce risk. Only ~18 admitted live anyway (v8 gate already filters it). **Keep off.**

---

## 5. Pre-momentum ON/OFF (the explicit with/without test)
| Setup | premom OFF | premom ON | what PM rejects |
|---|---|---|---|
| D_EMA20_BOUNCE | 0.58 / 0.56 | 0.54 / 0.45 | 0.61 / 0.66 ← *better than kept* |
| D_EMA20_REJECTION | 0.71 / 1.20 | **1.23 / 4.07** | 0.64 / 1.10 |

**Lesson:** a momentum gate that confirms *trend continuation in the trade's direction* helps the SHORT-rejection but hurts the LONG-bounce (where it filters out the very pullback-then-resume trades the bounce wants). Pre-momentum gates are **setup-directional**, not universal.

---

## 6. Exit review
mfe_R ≈ 0.6 (D_AVWAP) → 0.9 (D_EMA20_BOUNCE). D_EMA20_REJECTION's 0.75/1.30 fits (53% win, mfe_R 0.8). No exit change recommended; the leverage was the pre-momentum gate, not the exit.

---

## 7. Recommended changes
- **D_EMA20_REJECTION (probation candidate):** keep the pre-momentum gate (it's the edge), **drop the `body≥0.89 & ranker≥0.39` mask** (over-tight). Exit 0.75/1.30. PF 1.35, p 0.179, n=57.
- **D_EMA20_BOUNCE:** do not promote; **remove its counterproductive pre-momentum gate**; at most run `regime==TREND` as a low-conviction probe.
- **D_AVWAP_LOSE_REVERSAL:** keep effectively off (v8 gate already admits only ~18; no edge).

---

## 8. Anti-overfit warnings
- D_EMA20_REJECTION's edge rests on **57 trades / 36 days, day-block p 0.179 (not significant)** — directional, probation only.
- D_EMA20_BOUNCE TREND cell (37 trades, **11 days**) is too day-concentrated — likely a few good days, not an edge.
- Stacking the production mask onto D_EMA20_REJECTION (→ n=6) is exactly the over-fit trap to avoid.

---

## 9. What NOT to do
- Don't keep the pre-momentum gate on **D_EMA20_BOUNCE** (it hurts).
- Don't add the `body≥0.89 & ranker≥0.39` mask to D_EMA20_REJECTION (collapses the sample).
- Don't promote D_EMA20_BOUNCE or D_AVWAP_LOSE_REVERSAL.
- Don't size up D_EMA20_REJECTION (p 0.179, n=57).

## 10. Next experiments / validation
1. Re-confirm D_EMA20_REJECTION (premom-gated, no extra mask) under purged walk-forward + day-block bootstrap on a longer history before sizing.
2. (Low priority) D_EMA20_BOUNCE: test a *reversed* momentum condition (pullback-friendly) since the trend-continuation gate hurts it.
3. Family scorecard: **D contributes one probation candidate — D_EMA20_REJECTION (premom-gated).**
