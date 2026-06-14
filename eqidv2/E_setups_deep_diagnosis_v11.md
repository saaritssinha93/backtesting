# E* Setup Family — Deep Diagnosis (v11)
*Research period: 2025-11-03 → 2026-06-10 (148 days, ~1,200-ticker NSE universe). Generated 2026-06-11.*

> Of the 15 E* setups, only **4 have a population** (the other 11 are EARLY-mode/blocked → zero
> candidates). Read from the admitted pre-dedupe clean pool (E_ORB_BREAKOUT_SHORT sampled to 1,500),
> resolved at fixed production exits on 1-minute data, NET of NSE cost, with MAE/MFE and the per-setup
> pre-momentum gate ON/OFF. Engine: `validate_E_setups_filters.py`.

---

## 1. Executive summary

| Setup | TRAIN PF | TEST PF | Verdict |
|---|---|---|---|
| **E_VWAP_LOSE_EARLY_SHORT** (SHORT) | 0.82 → **2.27*** | 1.18 → **2.05*** | **STRONGEST candidate found (any family)** |
| E_ORB_BREAKOUT_SHORT (SHORT) | 0.90 | 0.74 | reject (the big churn/cost loser, confirmed) |
| E_ORB_BREAKOUT_LONG (LONG) | 0.69 | 0.73 | reject (22% immediate-fail, chasing) |
| E_VWAP_BAND_FADE (SHORT) | 0.67 | 0.78 | reject (doesn't run, mfe_R 0.6) |

\* with **vol_ratio ∈ [2, 3]** (see §1.1). The full E_VWAP_LOSE_EARLY_SHORT is a *loser* (PF 0.86); the edge lives entirely in a volume band.

### 1.1 🟢 The headline — E_VWAP_LOSE_EARLY_SHORT × vol_ratio [2,3]
| Cell | TRAIN n/PF | TEST n/PF/win | full PF | **day-block p** |
|---|---|---|---|---|
| ALL volumes | 324 / 0.82 | 56 / 1.18 / 52% | 0.86 | 0.890 (loser) |
| **vol_ratio 2–3** | **37 / 2.27** | **7 / 2.05 / 71%** | **2.24** | **0.008** ✅ |
| vol_ratio 1.8–3.2 (wider) | 47 / 2.06 | 9 / 1.76 / 67% | 2.01 | **0.004** ✅ |
| + premom ON | 25 / 1.39 | 5 / 0.88 | 1.31 | 0.243 (premom HURTS) |

**This is the first setup across A/B/C/D/E to clear PF > 2 in BOTH train and test AND be day-block significant** (p 0.004–0.008). Mechanism: the VWAP-lose short only follows through on **moderate-conviction volume (2–3× SMA)** — *institutional* selling losing VWAP. Below 2× = no conviction (fails); above ~3–5× = **exhaustion/climax that bounces** (fails, PF 0.74). The full setup bled because it traded every volume; the band isolates the real edge. **The pre-momentum gate HURTS it** (drop it).

---

## 2. Setup definitions (the 4 with data)
- **E_VWAP_LOSE_EARLY_SHORT** (SHORT, `early_vwap_lose_break_prev_low`, EARLY engine): common_short, `prev_close≥prev_VWAP` (was above VWAP), `close<VWAP` (loses it), `close<prev_low`, `close_loc≤0.35`, `rs_pct≤−0.10`, `vwap_dist_atr≥−1.80`; early gate `rs_pct≥−1.20 & close_loc≥0.08 & atr_pct≤0.008`; entry guard ≥09:45. Exit 0.70/1.00. PM gate `sig5_vol_ratio20≥1.56 & pre3_body_sum_r≤0.797`.
- **E_ORB_BREAKOUT_SHORT** (SHORT): OR-low breakdown. Exit 0.80/1.20 (prod override 0.80/1.50). PM `pre10_dir_count≥5 & pre5_vol_ratio20≥1.66`.
- **E_ORB_BREAKOUT_LONG** (LONG): OR-high breakout. Exit 0.80/1.20. PM `pre15_vol_ratio20≤1.08 & pre1_adx≥42.3`. Prod mask notional≥100k.
- **E_VWAP_BAND_FADE** (SHORT): VWAP-band fade, tight 0.70/0.60 exit. No PM gate.

---

## 3. Train/test results (fixed exits, NET)

| Setup | TRAIN n/PF/win | TEST n/PF/win | mfe_R | immFail | dbp |
|---|---|---|---|---|---|
| E_ORB_BREAKOUT_LONG | 477 / 0.69 / 35% | 147 / 0.73 / 37% | 0.9 | **22%** | 0.92 |
| E_ORB_BREAKOUT_SHORT | 1240 / 0.90 / 43% | 259 / 0.74 / 43% | 0.87 | 2–3% | 0.97 |
| E_VWAP_BAND_FADE | 415 / 0.67 / 48% | 40 / 0.78 / 50% | 0.60 | 2% | 0.83 |
| E_VWAP_LOSE_EARLY_SHORT (all) | 324 / 0.82 / 43% | 56 / 1.18 / 52% | 0.82 | 5–9% | 0.89 |
| **E_VWAP_LOSE_EARLY_SHORT (vol 2–3)** | **37 / 2.27** | **7 / 2.05 / 71%** | — | — | **0.008** |

---

## 4. Diagnosis

### 4.1 E_VWAP_LOSE_EARLY_SHORT — the keeper (volume-banded)
A VWAP-failure short. As traded (all volumes) it's a coin-flip loser; the edge is concentrated in the **vol_ratio 2–3** band (PF 2.24, p 0.008). Supporting cells: regime BEAR 1.02, market_ret≤−0.5 PF 1.16, vwap_dist −3..−1 PF 1.02. The single decisive variable is **volume conviction** (2–3×), not regime/RS. **Pre-momentum gate is counterproductive** (drop it). The 0.70/1.00 exit is fine (52–71% win in the band).

### 4.2 E_ORB_BREAKOUT_SHORT — reject (the big churn loser, confirmed)
1,240 train trades, PF 0.90 → test 0.74. Every regime/market/vol bucket is sub-1.0 (best 0.93–0.94). The pre-momentum gate **hurts test** (ON 0.56 vs OFF 0.74; what it rejects is better, 0.88). This is the heavy net-loss cost-sink from the earlier portfolio analysis — confirmed: no edge, no rescue.

### 4.3 E_ORB_BREAKOUT_LONG — reject
PF 0.69/0.73, **22% immediate-fail** (chasing OR-high breakouts that reverse fast). Premom: ON 0.61/1.19 (train worse). Only TREND regime breakeven (PF 1.0, n=27). No edge.

### 4.4 E_VWAP_BAND_FADE — reject
PF 0.67/0.78, **mfe_R 0.60** — at a 0.60% target, trades barely reach it; the fade doesn't run. Best cell regime-BULL 0.94 / market>0.5 1.07 (n=73) — marginal. No edge.

---

## 5. Pre-momentum ON/OFF (the explicit with/without test)
| Setup | OFF | ON | what PM drops |
|---|---|---|---|
| E_ORB_BREAKOUT_LONG | 0.69/0.73 | 0.61/1.19 | 0.71/0.63 |
| E_ORB_BREAKOUT_SHORT | 0.90/0.74 | 0.94/**0.56** | 0.85/**0.88** ← better |
| E_VWAP_LOSE_EARLY_SHORT | 0.82/1.18 | 0.73/1.34 | 1.19/0.99 |

**Consistent with D's lesson:** these momentum-confirmation gates do not help the E shorts and actively hurt E_ORB_BREAKOUT_SHORT. For E_VWAP_LOSE_EARLY_SHORT the *volume band*, not the PM gate, is the edge — and the PM gate dilutes it. **Recommend dropping the PM gate on the E_VWAP_LOSE band.**

---

## 6. Exit review
E_VWAP_LOSE (vol 2–3): 0.70/1.00 fits (71% test win). E_VWAP_BAND_FADE 0.60 target unreachable (mfe_R 0.6). E_ORB_LONG 22% immediate-fail → entry timing problem, not exit. No exit changes recommended beyond the existing.

---

## 7. Recommended changes
- **E_VWAP_LOSE_EARLY_SHORT (PROMOTE-grade candidate):** add **`vol_ratio ∈ [2.0, 3.0]`** (conservative; PF 2.24, p 0.008) or `[1.8, 3.2]` (more trades, p 0.004), **drop the pre-momentum gate**, keep exit 0.70/1.00 and the ≥09:45 entry guard. The full unbanded setup should NOT trade (it's the churn loser).
- **E_ORB_BREAKOUT_SHORT / LONG, E_VWAP_BAND_FADE:** reject / keep off — no edge.

---

## 8. Anti-overfit warnings
- The vol_ratio 2–3 cell is **n=44 (37/7)** — but the **day-block p=0.008** (and 0.004 for the wider band) is genuinely significant across 30+ days, and the relationship is **monotonic and mechanical** (quiet→fails, moderate→works, climax→fails), which is far more trustworthy than a flat-PF threshold. Still: test n=7 → adopt as strong probation, confirm on more history before sizing.
- Prefer the **wider [1.8, 3.2] band** to reduce threshold-overfit (47/9 trades, still p 0.004).
- This edge came from *restricting* a net-loser to its core sub-population — exactly the right move (not stacking filters).

---

## 9. What NOT to do
- Don't trade the **unbanded** E_VWAP_LOSE_EARLY_SHORT (PF 0.86 churn loser).
- Don't keep its pre-momentum gate (it dilutes the band edge).
- Don't promote E_ORB_BREAKOUT_SHORT/LONG or E_VWAP_BAND_FADE.
- Don't size up on n=44.

## 10. Next experiments / validation
1. Re-confirm **E_VWAP_LOSE_EARLY_SHORT × vol_ratio[1.8,3.2]** under purged walk-forward + day-block bootstrap on a longer history before sizing.
2. Family scorecard: **E contributes one strong candidate — E_VWAP_LOSE_EARLY_SHORT (volume-banded), the best train+test edge found so far (PF >2 both periods, p 0.008).**
