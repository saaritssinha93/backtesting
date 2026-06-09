# V7 Strategy Review — 2026-06-09

---

## A. Executive Summary

**Gross P&L: Rs +466 | Net after costs (est.): ~Rs +66 | PF: 1.16 | Trades: 8 (4W / 4L)**

Today's headline number is marginally positive, but it masks three critical structural problems that are destroying net P&L over any rolling multi-day window:

1. **All 8 trades were SHORT.** The LONG side had zero paper entries despite generating 756+ raw candidates and 29 gate-passing candidates, several with verified 1-minute MFE of 3–7%. This is not a market condition issue — it is a funnel failure: C_OR_BREAKOUT (29 passed V8 gate) produced zero entry rows due to an infrastructure gap, and A_MOD_BREAK_C1_HIGH (161 raw) was blocked entirely at the V8 gate.

2. **Multi-window P&L is negative across every lookback**: 3-day PF=0.65, 5-day PF=0.74, 7-day PF=0.76, 11-day PF=0.81. Today's Rs 466 is an outlier against a running 12-session net loss of approximately Rs -38,720. The strategy is not net profitable in live paper.

3. **Two setups should be placed on probation or stopped before tomorrow**: `E_VWAP_LOSE_EARLY_SHORT` (19 trades, PF 0.58, net -Rs 3,554 over 20 sessions) and `T_TREND_DAY_EMA_STAIR_SHORT` (83 trades, PF 0.47, net -Rs 8,406) are the two largest contributors to cumulative losses. T_TREND_DAY_EMA_STAIR_SHORT was not active today — but if it fires tomorrow, it will damage P&L.

**Market regime today: CHOP\_NEUTRAL\_NORMAL\_VOL** (market\_ret = -0.022%, median\_vol = 1.849). A net short bias on a mildly down day is mechanically sensible, but the complete LONG blockage meant we missed the best moves of the day.

---

## B. What Worked Today

**High confidence positives, all verifiable from path data:**

| Trade | Setup | Hold | PnL | Path quality |
|---|---|---|---|---|
| ANTELOPUS SHORT | D_AVWAP_LOSE_REVERSAL | 26.6m | +Rs 1,488 | Best R=1.17, worst R=-0.28, reached +0.25R in 3.9m — textbook clean path |
| KITEX SHORT | A_PULLBACK_C2_THEN_BREAK_C2_LOW | 31.0m | +Rs 999 | TARGET hit cleanly, no path data gap |
| SOLARWORLD SHORT | MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | 51.0m | +Rs 800 | TARGET hit with controlled hold |
| NIITMTS SHORT | MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | 143.8m | +Rs 17 | EOD close, barely positive after long hold — not a quality winner but not a loss |

**Infrastructure that held:**
- 5-minute fetch: 1253/1253 tickers, no drops, no data failures
- Pre-momentum filter correctly rejected 6 candidates (prevented additional -SL losses)
- ANTELOPUS trade timing was near-perfect: entered 11:36, reached target at 12:03, drawdown never exceeded -0.28R before target

**System observations that proved correct today:**
- The path quality lab correctly flagged AWFIS as `TRAIL_AFTER_0.5R_SHADOW` (signal was already present in the audit file)
- Pre-momentum filter's `sig5_vol_ratio20` gate for E_VWAP_LOSE_EARLY_SHORT is correctly rejecting low-volume continuation setups (1 additional reject today beyond the 2 that traded)

---

## C. What Failed Today

### C1. E_VWAP_LOSE_EARLY_SHORT — Two distinct failure modes

**CRISIL SHORT (09:31):** Entered at the first 5-minute slot (09:31). SL hit at 3.0 minutes. Path showed maximum best R of only +0.157 — the trade never had any meaningful profit window. The failure mode was an immediate adverse move from bar-open. **Root cause: 09:30–09:35 VWAP is not yet established (only 1–2 bars of intraday data), making `E_VWAP_LOSE_EARLY_SHORT` structurally unreliable in the first slot.** The pre-momentum filter should have flagged this (ADX at signal = 30.52, not extreme), but the core issue is time-window: no VWAP can "lose" meaningfully on the first bar.

**AWFIS SHORT (09:36):** Held for 338 minutes, SL hit at 15:14. Path evidence is damning:
- Reached +0.25R profit in 2.87 minutes
- Reached -0.50R adverse at 31.87 minutes (price already above entry)
- Recovered to +0.95R best at approximately 56 minutes
- Gave back everything, SL hit at 338 minutes

This is not a bad signal. It is a bad exit. The trade hit +0.95R (near a 1.0R target) and the strategy held through a full reversal to -1.0R. **A breakeven stop triggered after +0.5R would have produced at minimum 0R (neutral) instead of -1.0R on this trade alone, saving Rs ~700 in losses.** The path quality lab already output `TRAIL_AFTER_0.5R_SHADOW` — this recommendation has live evidence and should be promoted.

### C2. D_EMA20_REJECTION — Low-ADX environment

**MARICO SHORT (13:46):** SL hit at 70.5 minutes, -Rs 745. Pre-momentum stats at signal: **ADX = 12.38** — extremely low, indicating a sideways/chop environment with no trend. D_EMA20_REJECTION requires a declining EMA trend to be valid. At ADX 12, there is no trend. The setup fired on a flat EMA, not a declining one. This is fixable with a minimum ADX threshold.

### C3. MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT — Structural SL hold problem

**UNIONBANK SHORT (12:11):** SL hit at 159.6 minutes, -Rs 697. The trade sat for nearly 2.5 hours before being stopped out. **Average SL hold across all 4 SL trades today was 142.8 minutes.** This means losing trades are locking up capital for hours while winning trades exit in 26–51 minutes. The capital efficiency ratio is deeply negative.

### C4. C_OR_BREAKOUT — Infrastructure gap: 29 passed gate, zero entry rows

**This is the most important structural failure today.** From the slot flow data:

| Slot | V11 selected | Entry rows | Entry after scan (s) |
|---|---|---|---|
| 14:20 | 20 | 0 | 0.1s |
| 14:25 | 15 | 0 | 0.6s |
| 14:30 | **29** | **0** | 0.7s |

At 14:30, V11 selected 29 candidates — the highest count of the day — but exactly zero entry rows were written. The entry-after-scan lag was 0.7s, meaning the entry engine received the candidates but produced no rows. The reality gap report confirms: 20 C_OR_BREAKOUT entries all carry reason `no_entry_row`. This is not a scanner filter issue. **It is an entry engine or entry-row-builder issue specifically for C_OR_BREAKOUT in the late-session profile.** MSTCLTD (score 326, +2.69% MFE), MAYURUNIQ (score 279), FINOPB, KTKBANK, BALAMINES, MARKSANS and 13 others were gated and selection-ready but never entered.

### C5. Feature gaps — 62.5% of accepted trades had NaN pre-momentum features

5 of 8 accepted trades (ANTELOPUS, UNIONBANK, SOLARWORLD, NIITMTS, KITEX) had all 5 pre-momentum features blank (`pre_bars`, `sig5_adx_calc`, `sig5_vol_ratio20`, `pre2_mom_r`, `pre_entry_momentum_score`). The reason logged is `blank`. This means the pre-momentum filter accepted these trades **without evaluating them** — it cannot filter or score them. The filter is effectively disabled for these 5 tickers. Notably, 3 of these 5 were winners (ANTELOPUS, SOLARWORLD, KITEX) and 2 were SL trades (UNIONBANK, NIITMTS), so the null features are not all causing losses — but the filter's selectivity is reduced when features are blank.

### C6. Scanner latency is crossing both soft SLA thresholds

- 5min fetch: 36.1s (soft SLA ≈ 30s)
- Scanner publish delay: 53–70s across recent slots
- Scanner overhead outside Tier123: 27.3–33.7s

The combined pipeline delay (fetch + scan + overhead) means entries are based on data that is 53–70 seconds stale by the time the entry engine acts. For fast-moving intraday setups, this is material. The bottleneck is the `candidate JSON assembly and v11 overlay merge`, not the Tier123 parallel scan (25.7s).

### C7. ALL LONG setups completely absent from paper trades

Market regime was neutral-to-slightly-bearish (market\_ret = -0.022%), but the LONG side generated enormous 1-minute follow-through that was entirely missed:

| Ticker | Setup | MFE | Rejection reason |
|---|---|---|---|
| MAYURUNIQ | E_ORB_BREAKOUT_LONG 09:50 | **7.10%** | rejected_v8_gate |
| NIITLTD | C_OR_BREAKOUT 11:10 | **4.55%** | rejected_v8_gate |
| SUVEN | A_MOD_BREAK_C1_HIGH 12:25 | **5.08%** | rejected_v8_gate |
| CARYSIL | A_PULLBACK_C2_THEN_BREAK_C2_HIGH 13:40 | **3.79%** | rejected_v8_gate |
| BANKINDIA | A_MOD_BREAK_C1_HIGH 11:00 | **3.05%** | rejected_v8_gate |
| CAMLINFINE | B_AVWAP_RECLAIM_REVERSAL 11:50 | **3.19%** | rejected_v8_gate |
| AVANTIFEED | D_AVWAP_LOSE_REVERSAL 12:50 | **2.40%** | rejected_v8_gate |

A_MOD_BREAK_C1_HIGH had 161 raw candidates, ALL rejected at the V8 gate. The exit lab (REJECTED_MISSED cohort, multi-window 20-session) shows this setup has avg MFE 1.119% on a 717-sample base — this is not noise, it is a real setup with real edge that the V8 gate is completely blocking.

---

## D. Missed Profitable Opportunities and Exact Reasons

### Category 1: Infrastructure gap (no_entry_row) — most urgent

| Time | Ticker | Setup | Score | MFE | Exact reason |
|---|---|---|---|---|---|
| 14:30 | MSTCLTD | C_OR_BREAKOUT | 326 | +2.69% | no_entry_row — entry engine produced 0 rows for this slot despite V11 selection |
| 14:30 | MAYURUNIQ | C_OR_BREAKOUT | 279 | +0.41% | no_entry_row — same infrastructure gap |
| 14:30 | FINOPB | C_OR_BREAKOUT | 185 | +0.24% | no_entry_row |
| 14:30 | KTKBANK | C_OR_BREAKOUT | 167 | not measured | no_entry_row |
| 14:30 | BALAMINES | C_OR_BREAKOUT | 150 | not measured | no_entry_row |
| 13:55 | KITEX (duplicate) | A_PULLBACK_C2_THEN_BREAK_C2_LOW | 263 | — | not_written_to_live_signal_csv (higher-scored duplicate of the 13:56 trade that did execute) |

**Verdict: These should have been traded.** The V8 gate accepted them. The entry engine failed to produce rows. Fix the infrastructure gap.

### Category 2: V8 gate rejections with high-quality 1-minute follow-through

| Time | Ticker | Setup | MFE | MAE | Verdict |
|---|---|---|---|---|---|
| 09:50 | MAYURUNIQ | E_ORB_BREAKOUT_LONG | 7.10% | 0.00% | Should be shadow-tested — near-zero MAE, 7% MFE is exceptional |
| 11:10 | NIITLTD | C_OR_BREAKOUT | 4.55% | 0.58% | MAE controllable; missed large move |
| 12:25 | SUVEN | A_MOD_BREAK_C1_HIGH | 5.08% | 0.12% | Near-zero MAE — extremely clean setup |
| 13:40 | CARYSIL | A_PULLBACK_C2_THEN_BREAK_C2_HIGH | 3.79% | 0.00% | Zero MAE |
| 11:00 | BANKINDIA | A_MOD_BREAK_C1_HIGH | 3.05% | 0.05% | Very low MAE |
| 11:50 | CAMLINFINE | B_AVWAP_RECLAIM_REVERSAL | 3.19% | 0.49% | MAE manageable |

**Verdict: These should NOT be immediately added to live.** However, the consistency of high-MFE + low-MAE across multiple tickers and setups for A_MOD_BREAK_C1_HIGH LONG and E_ORB_BREAKOUT_LONG strongly suggests the V8 gate is over-filtering on the LONG side. Promote to shadow/paper experiment before next week.

### Category 3: Pre-momentum filter correctly rejected but were they correct rejections?

6 pre-momentum rejects today. The reject reasons:
- `sig5_vol_ratio20=1.30 >= 1.56` — low relative volume → correct reject (vol not confirming)
- `pre10_dir_count=3 >= 5; pre5_vol_ratio20=0.91 >= 1.66` — direction not aligned → correct reject
- `pre10_dir_count=2 >= 5; pre5_vol_ratio20=0.35 >= 1.66` — extremely weak trend + vol → correct reject
- `pre10_mom_r=0.18 <= 0.157` — tiny negative momentum → correct reject
- `pre5_mom_r=0.012 >= 0.125` — near-zero momentum → correct reject
- `blank_reason` — GNA C_OR_BREAKDOWN → **unclear rejection; needs investigation**

**Verdict: 5 of 6 rejections appear correct based on the filter criteria. The `blank_reason` GNA rejection is a data gap — investigate the reason field.**

---

## E. Bad/Negative P&L Entries and Exact Reasons

| Ticker | Setup | PnL | Hold | Primary cause | Secondary cause |
|---|---|---|---|---|---|
| CRISIL | E_VWAP_LOSE_EARLY_SHORT | -Rs 695 | 3m | Time-of-day: 09:31 first slot, VWAP not yet valid | Immediate adverse move, no +0.2R window |
| AWFIS | E_VWAP_LOSE_EARLY_SHORT | -Rs 700 | 338m | Exit failure: reached +0.95R, gave back all profit to SL | No trail/breakeven stop; capital locked entire session |
| UNIONBANK | MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | -Rs 697 | 159.6m | Prolonged no-progress hold | Time-based SL tighten missing |
| MARICO | D_EMA20_REJECTION | -Rs 745 | 70.5m | Low ADX (12.38) at signal — no trend present | EMA rejection pattern requires declining trend, not flat |

**Shared traits of today's losers:**
1. Three of four SL trades had prolonged holds (159m, 70m, 338m) — slow-bleed losses rather than quick stops
2. Two of four were in the first session hour (09:31, 09:36) — early-session VWAP reliability issue
3. Zero of four showed strong adverse momentum at entry — these were not breakout failures, they were slow drift-into-SL situations

**Common structural weakness: The SL is placed correctly but there is no time-based exit mechanism.** AWFIS drifted between profit and loss for 5 hours 38 minutes. A 30-minute time-to-progress gate (exit or tighten if +0.25R not reached in 30m) would have substantially reduced UNIONBANK and AWFIS losses, and the AWFIS breakeven-stop would have saved the recovered-profit trade.

---

## F. Setup-Wise 5-Minute Entry vs 1-Minute P&L Analysis

### CRISIL — E_VWAP_LOSE_EARLY_SHORT (09:31)
- **5-minute signal**: Valid VWAP structure at signal bar, but 09:30 bar is bar 1 of the session — VWAP is anchored to a single data point
- **1-minute path**: `best_r=+0.16, worst_r=-0.97, time_to_neg050r=2.83m` — adverse in under 3 minutes
- **Assessment**: The 5-minute signal was premature. There is no meaningful 1-minute confirmation that could have validated this — the move was immediate and adverse from bar 1
- **Fix**: Add time window restriction to E_VWAP_LOSE_EARLY_SHORT: earliest entry = 09:40 (4th bar), not 09:30

### AWFIS — E_VWAP_LOSE_EARLY_SHORT (09:36)
- **5-minute signal**: Second slot (09:35 bar), VWAP more established than CRISIL
- **1-minute path**: `best_r=+0.95, t025=2.87m, t050=56.87m, t_neg050=31.87m` — the trade showed profit early (hit +0.25R at 2.87m), dipped to -0.5R adverse at ~31m, recovered to +0.95R at ~57m, then drifted to -1.0R over 5 more hours
- **Assessment**: The 5-minute signal itself was not bad (it reached near-target), but the exit logic failed catastrophically. The 1-minute data confirms the trail opportunity was real and present
- **Fix**: Breakeven stop once +0.5R is confirmed on 1-minute. This is a pure exit change, not a signal quality change.

### ANTELOPUS — D_AVWAP_LOSE_REVERSAL (11:36)
- **5-minute signal**: Mid-session (11:35 bar), AVWAP structure mature
- **1-minute path**: `best_r=+1.17, worst_r=-0.28, giveback=-0.33, t025=3.87m` — reached profit in under 4 minutes, minimal drawdown
- **Assessment**: This is the model trade. The 5-minute signal was correctly timed. The 1-minute path confirms no better entry point was available. Note: ANTELOPUS also appeared 5 minutes earlier (11:35 slot, rejected_v8_gate) with MFE=1.868% — the system caught it one slot later and still profited
- **5-minute vs 1-minute verdict**: 5-minute entry was correct and well-timed. No earlier 1-minute entry would have added meaningful improvement

### SOLARWORLD — MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT (12:46)
- TARGET in 51 minutes, +Rs 800
- No detailed path data available (NO_FUTURE_BARS in path quality CSV for this slot)

### KITEX — A_PULLBACK_C2_THEN_BREAK_C2_LOW (13:56)
- TARGET in 31 minutes, +Rs 999
- No detailed path data available — exit lab result confirms clean 1-minute follow-through for this setup

### 1-minute vs 5-minute summary verdict
The 5-minute signals for today's winning trades (ANTELOPUS, SOLARWORLD, KITEX) showed no timing advantage from earlier 1-minute entry — the 5-minute entry was correctly placed. The losses (CRISIL, AWFIS, MARICO, UNIONBANK) were caused by **exit quality and time-of-day restrictions**, not by late signal generation. The 5-minute scan is not causing timing losses here.

---

## G. Setup-Wise Ranking for Tomorrow

### TIER 1 — Run unchanged (highest confidence, do not modify)

| Setup | Action | Reason | Confidence |
|---|---|---|---|
| D_AVWAP_LOSE_REVERSAL SHORT | Keep | Today 1/1 target, +Rs 1,488; clean 1-min path; 34 raw → 1 gated → correct selectivity | High |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW SHORT | Keep | Today 1/1 target, +Rs 999; funnel 63→2→1 — appropriate selectivity | High |
| MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | Keep | Today 2/3 targets, PF 1.17; best available multi-trade setup today; funnel 9→4→3 is healthy | High |

### TIER 2 — Keep but with specific parameter changes

| Setup | Action | Change required | Confidence |
|---|---|---|---|
| D_EMA20_REJECTION SHORT | Tighten | Add minimum ADX >= 20 at signal bar. Today's SL (MARICO) had ADX=12.38 — no trend. 11-session PF=1.51 with proper trend filter should improve further | High |
| E_VWAP_LOSE_EARLY_SHORT SHORT | Time-restrict + exit change | (1) Earliest entry slot = 09:40 (not 09:30/09:35); (2) Add breakeven stop after +0.5R confirmed | High |

### TIER 3 — Probation / shadow-block immediately

| Setup | Action | Evidence | Confidence |
|---|---|---|---|
| T_TREND_DAY_EMA_STAIR_SHORT SHORT | **Stop firing** | 83 trades, PF=0.47, net -Rs 8,406 — single largest P&L drain in history; not active today but must be stopped | High |
| C_OR_BREAKDOWN SHORT | Probation + shadow | 8 trades, PF=0.38, net -Rs 2,023; 1-min exit lab REJECTED_MISSED shows avg -0.095% at best profile | High |
| L_TREND_PULLBACK LONG | Probation | 18 trades, PF=0.66, net -Rs 2,678; 1-min exit lab shows consistently poor actual paper outcomes | High |
| B_AVWAP_RECLAIM_REVERSAL LONG | Probation (ranker filter first) | 5 trades, PF=0.11, net -Rs 1,508; multi-window worst setup by PF; require ranker_score >= 0.65 as minimum gate | High |
| L_GAP_DOWN_REVERSAL LONG | Stop | 4 trades, PF=0.07, net -Rs 2,292 — insufficient edge; worst 1-day session had -Rs 2,292 alone | High |

### TIER 4 — Shadow/paper experiment only (do not fire live)

| Setup | Direction | Evidence | Action |
|---|---|---|---|
| A_MOD_BREAK_C1_HIGH LONG | Research | 161 raw, 0 gated; 158 rejected-missed show avg ret 0.432%, 69.6% win on exit lab (717-sample 20-day window) | Shadow-test with relaxed V8 gate |
| C_OR_BREAKOUT LONG | Research (infrastructure fix first) | 29 passed V8 gate today, 0 entry rows — fix entry engine first, then paper-test | P0 infrastructure fix then paper |
| E_ORB_BREAKOUT_LONG LONG | Research | 6 raw, 0 gated; MAYURUNIQ 09:50 showed 7.10% MFE with 0.00% MAE; 12 rejected-missed show avg 0.458% | Shadow-test V8 gate conditions |
| D_EMA20_BOUNCE LONG | Research | 18-trade 11-session PF=1.26, net +Rs 1,747; not firing today; investigate entry engine block | Audit why gated rows don't produce entries |

---

## H. Overtrading, Brokerage, Slippage, and Taxation Risk

**Today's 8 trades at estimated Rs 50/round-trip brokerage = Rs 400 in brokerage cost. Net after brokerage: approximately Rs 66.**

| Trade | Gross PnL | Holding time | Capital efficiency assessment |
|---|---|---|---|
| CRISIL SHORT | -Rs 695 | 3m | Fast clean failure — capital freed quickly |
| AWFIS SHORT | -Rs 700 | **338m** | Capital locked entire session. One trade used a full slot for 5.5 hours |
| ANTELOPUS SHORT | +Rs 1,488 | 26.6m | Excellent capital efficiency |
| UNIONBANK SHORT | -Rs 697 | **159.6m** | Capital locked 2.5 hours on a losing trade |
| SOLARWORLD SHORT | +Rs 800 | 51m | Good efficiency |
| NIITMTS SHORT | +Rs 17 | **143.8m** | Rs 17 profit for 2.4 hours of capital use = deeply negative after any cost model |
| MARICO SHORT | -Rs 745 | 70.5m | 1.25 hours for a loss |
| KITEX SHORT | +Rs 999 | 31m | Excellent efficiency |

**Key problem: NIITMTS and AWFIS together locked capital for 481 minutes (8 hours combined) and generated a net of -Rs 683.** If those two slots had been used to catch the LONG missed trades (SUVEN +5%, BANKINDIA +3%, CARYSIL +3.8%), the day's P&L could have been Rs 3,000–5,000 higher.

**Multi-session context — the real taxation/churning risk:**
- C_OR_BREAKOUT: 266 trades over 12 sessions, PF=0.77, net -Rs 20,521. Each trade generates brokerage + STCG liability. At Rs 50/trade: 266 * 50 = Rs 13,300 brokerage alone. Net after costs: approximately -Rs 33,821.
- T_TREND_DAY_EMA_STAIR_SHORT: 83 trades, PF=0.47, net -Rs 8,406. Additional Rs 4,150 brokerage. Total damage: ~Rs 12,556.

These are not trading setups — they are capital destruction setups at the current filter level. They should be stopped or placed on probation with shadow-only logging until P&L evidence improves.

---

## I. Recommended V7 Logic Changes for Tomorrow

### HIGH-CONFIDENCE changes (test tonight or pre-open)

**Change 1: T_TREND_DAY_EMA_STAIR_SHORT → Disable before market open tomorrow**
- Evidence: 83 trades, PF=0.47, -Rs 8,406 net, -Rs 12,556 after costs. Worst-performing setup in the universe
- Risk of disabling: low (setup produces consistent losses; disabling improves expected net P&L immediately)
- Implementation: Add to EXCLUDED_SETUPS or set pre-momentum gate to SHADOW_ONLY

**Change 2: E_VWAP_LOSE_EARLY_SHORT → Add earliest entry time restriction**
- Change: Entry window = 09:40 minimum (reject 09:30 and 09:35 slots)
- Evidence: CRISIL (09:31) had VWAP with only 1 bar of data; best R = 0.16 with immediate adverse move
- Risk: Low. Multi-window shows this setup wins more often in mid-session anyway

**Change 3: E_VWAP_LOSE_EARLY_SHORT → Add breakeven stop after +0.5R confirmed on 1-minute**
- Change: When trade reaches +0.5R (confirmed on 1-minute close), move SL to entry price (breakeven)
- Evidence: AWFIS reached +0.95R at ~57m, gave it all back to -1.0R over 280 more minutes. Breakeven stop would have yielded at minimum 0R instead of -1.0R
- Implementation: Add to AVWAP trade execution logic. Shadow-test first session

**Change 4: D_EMA20_REJECTION → Require ADX >= 20 at signal bar**
- Change: Add gate condition: `sig5_adx_calc >= 20` (or use `pre1_adx >= 20` as pre-entry check)
- Evidence: MARICO (today's SL) had ADX = 12.38. Multi-window shows 11-session PF=1.51 with good trades; the bad ones (recent 7-day PF=0.13) likely coincide with low-ADX entries
- Risk: May reduce trade count by ~30%; should improve win rate
- Implementation: Pre-momentum filter gate (SHADOW_ONLY first session to count filtered candidates, then activate)

**Change 5: C_OR_BREAKDOWN → Move to probation (shadow-block)**
- Evidence: 8 trades, PF=0.38, -Rs 2,023. 1-min exit lab REJECTED_MISSED shows avg -0.095% even for missed candidates — the setup has no edge at current gate settings
- Implementation: Add to pre-momentum SHADOW_ONLY block list

**Change 6: B_AVWAP_RECLAIM_REVERSAL → Require ranker_score >= 0.65**
- Evidence: 5 trades, PF=0.11, -Rs 1,508. Multi-window suggestion explicitly calls this out with `SHADOW_ONLY` action requiring ranker_score >= 0.65
- Risk: May leave only 0–2 trades per week, which is acceptable given the current loss rate

### MEDIUM-CONFIDENCE changes (test in paper before live)

**Change 7: Pre-momentum feature gap investigation (pre_bars / sig5_adx_calc / sig5_vol_ratio20 / pre2_mom_r blank for 5/8 accepted trades)**
- Investigation: Why are ANTELOPUS, UNIONBANK, SOLARWORLD, NIITMTS, KITEX showing `blank` reason for all 5 features?
- Likely cause: These tickers' 1-minute data is insufficient (< pre_bars minimum required) or their signal bars fall in a data window gap
- Action: Before tomorrow, add a diagnostic log: if pre_bars is blank, log the actual 1-minute bar count for those tickers at signal time. If the bar count is < minimum, do not suppress — raise as a pre-momentum soft-reject with reason `insufficient_1min_bars`

**Change 8: Add time-based SL tighten (no-progress exit)**
- Change: If trade has not reached +0.20R within 30 minutes of entry, tighten SL to -0.50R (or exit at market)
- Evidence: avg SL hold = 142.8m vs avg target hold = 36.2m. The slowest-moving SL trades (AWFIS, UNIONBANK, NIITMTS) all showed no significant progress in the first 30 minutes
- Shadow test in paper first session

**Change 9: L_TREND_PULLBACK → Move to probation**
- Evidence: 18 trades, PF=0.66, -Rs 2,678 net over 12 sessions
- Risk of acting: low (setup is losing consistently)

**Change 10: L_GAP_DOWN_REVERSAL → Disable**
- Evidence: 4 trades, PF=0.07, -Rs 2,292 net. Single worst-return setup per trade
- Risk: Low

### EXPERIMENTAL (do not deploy in live; shadow/paper only)

**Change 11: A_MOD_BREAK_C1_HIGH LONG → Investigate V8 gate rejection reasons for this setup**
- Context: 161 raw today, ALL rejected by V8 gate. Exit lab on 717 rejected-missed samples shows 69.6% win rate, avg 0.432% return. Multiple tickers (SUVEN +5%, BANKINDIA +3%, CARYSIL +3.8%) had near-zero MAE
- Next step: Pull the V8 gate rejection reasons for today's 161 A_MOD_BREAK_C1_HIGH rejects. Identify which gate condition is blocking them. Do NOT loosen until the rejection analysis is done.

**Change 12: C_OR_BREAKOUT LONG → Fix entry engine gap first, then paper-test**
- Context: 29 passed V8 gate today, 0 entry rows. This is an infrastructure fix, not a strategy change. Once the entry engine produces rows for C_OR_BREAKOUT, paper-test for 5 sessions before considering promotion to live.

**Change 13: Freshness score ranking for C_OR_BREAKOUT flood management**
- Context: When 13–29 C_OR_BREAKOUT candidates pass the gate in a single slot (as happened at 14:30 today), cap at top 5 by freshness score (pre2_mom_r + pre1_body_r + sig5_close_pos)
- Do not implement until entry engine is fixed and producing rows

---

## J. Recommended V7 Monitor and Dashboard Improvements

**Missing from today's live monitor — add for tomorrow:**

1. **no_entry_row count per slot** — Today had 20+ candidates with this reason; the monitor showed "0 entries" but did not explain why 29 V11-selected candidates produced zero rows. Add column: `v11_selected | entry_rows | no_entry_row_count` to the candidate flow table.

2. **Path quality live feed** — Best R, worst R, giveback R, time\_to\_025R should appear in the live monitor for open trades. Today AWFIS's +0.95R recovery at 57m would have been visible and actionable.

3. **Per-setup feature gap alert** — When > 50% of accepted rows for a setup have NaN pre-momentum features, flag it: `⚠ 5/8 accepted trades had blank pre_bars/sig5_adx_calc`. This is currently buried in the ops audit CSV.

4. **SL hold time alert** — When a trade exceeds 60 minutes without hitting +0.25R or SL, surface it in the monitor: `AWFIS SHORT — 60m with no progress, shadow recommend: tighten stop`.

5. **Ranker score per trade** — Currently ranker scores are shadow-only and not visible in the live monitor. Add ranker_score and freshness_bucket (WEAK/STRONG) next to each live/paper trade row.

6. **C_OR_BREAKOUT gated-but-no-entry warning** — A dedicated line: `C_OR_BREAKOUT: 29 gated, 0 entry rows — investigate entry engine`. This exact situation happened today without a clear dashboard alert.

7. **Setup-level PF by recent window (3/7/20 session)** — The current monitor shows today's PF only. Add a second column: rolling-7 PF. This would have shown T_TREND_DAY_EMA_STAIR_SHORT's 7-day PF of 0.47 live and flagged it for review before it fires again.

8. **Capital utilization tracker** — Show total minutes of capital locked in open trades (today AWFIS + UNIONBANK + NIITMTS = 641 minutes combined). This is not tracked at all in the current monitor.

---

## K. P0 / P1 / P2 Priority Fixes

### P0 — Must fix before market open tomorrow

**P0.1: Fix C_OR_BREAKOUT no_entry_row bug**
- What: At 14:20, 14:25, 14:30 slots, V11 selected 20/15/29 candidates respectively, but entry engine produced zero entry rows for all of them
- Where: Entry engine (eqidv2_entry_engine_1min_v5_id.py) — investigate why C_OR_BREAKOUT candidates in late-session slots are skipped in the entry row builder
- Impact: 20 high-scoring C_OR_BREAKOUT opportunities were missed today alone
- Action: Read the entry engine audit for today's 14:20–14:30 slots to find the drop-off. Is this a C_OR_BREAKOUT-specific profile rule failure, a time-window cutoff, or a missing `no_entry_row` condition?

**P0.2: Disable T_TREND_DAY_EMA_STAIR_SHORT before market open**
- What: 83 trades, PF=0.47, -Rs 8,406 over window. Worst setup by any metric
- Where: EXCLUDED_SETUPS or pre-momentum SHADOW_ONLY block
- Impact: Prevents further capital destruction from this setup immediately

### P1 — Implement before or early in tomorrow's session

**P1.1: E_VWAP_LOSE_EARLY_SHORT — Add earliest entry = 09:40 (reject 09:30, 09:35 slots)**
- Impact: CRISIL today would have been blocked. Reduces first-slot VWAP-reliability problem
- Confidence: High

**P1.2: E_VWAP_LOSE_EARLY_SHORT — Add breakeven stop after +0.5R**
- Impact: AWFIS today: -Rs 700 → likely 0R or small positive. Saves ~Rs 700 in worst-case scenarios
- Confidence: High (path data confirms this explicitly)

**P1.3: D_EMA20_REJECTION — Add ADX >= 20 gate at signal bar**
- Impact: MARICO (ADX=12.38) would have been blocked. Improves win rate on this setup
- Confidence: High

**P1.4: Move C_OR_BREAKDOWN and B_AVWAP_RECLAIM_REVERSAL to shadow/probation**
- Impact: Prevents additional Rs 2,000+ losses per session from chronic losers
- Confidence: High

**P1.5: Add no_entry_row count and path quality fields to live monitor**
- Impact: Tomorrow's team can see in real time whether C_OR_BREAKOUT entry engine fix is working
- Confidence: High (operational improvement only)

### P2 — Test within 3 sessions, shadow-first

**P2.1: Investigate A_MOD_BREAK_C1_HIGH LONG V8 gate rejection reasons**
- Pull gate rejection breakdown for 161 raw candidates. Identify the specific blocking condition
- Do not loosen gate until the reject reason is known

**P2.2: Add time-based no-progress SL tighten (if +0.20R not reached in 30m, move SL to -0.5R)**
- Shadow-test one session first. UNIONBANK and NIITMTS are the target cases
- Confidence: Medium (may trigger on legitimate slow setups that eventually hit target)

**P2.3: Freshness score ranking for C_OR_BREAKOUT flood (cap at top 5 per slot)**
- Only relevant once P0.1 is fixed and entry rows start flowing
- Confidence: Medium

**P2.4: Investigate feature gap cause for pre_bars/sig5_adx_calc blank on ANTELOPUS-type tickers**
- These tickers appear to be missing 1-minute bars at signal time
- Add diagnostic log of actual 1-minute bar count per accepted ticker

---

## L. Tomorrow's Final Trading Checklist

### Pre-market (before 09:15)

- [ ] Verify T_TREND_DAY_EMA_STAIR_SHORT is in EXCLUDED_SETUPS or shadow-blocked
- [ ] Verify C_OR_BREAKOUT entry engine fix is deployed and entry rows are generated in pre-open test
- [ ] Confirm E_VWAP_LOSE_EARLY_SHORT earliest entry = 09:40 is active
- [ ] Confirm D_EMA20_REJECTION ADX >= 20 gate is active or shadow-flagging
- [ ] Check pre-momentum filter feature gap diagnostic is logging
- [ ] Review multi-window PF table — confirm no new setup has dropped below PF 0.70 in 7-day window

### During market (live monitoring targets)

- [ ] **09:30–09:45**: Watch for E_VWAP_LOSE_EARLY_SHORT entries. Verify no 09:30 or 09:35 entries fire
- [ ] **Any trade reaching +0.5R**: Confirm breakeven stop moves to entry price (shadow or live)
- [ ] **Any trade open > 30 minutes without +0.20R progress**: Surface in monitor, prepare to tighten
- [ ] **14:20–14:35 slots**: Watch C_OR_BREAKOUT entry row count. If 29 pass V8 gate again, verify entry rows are now generated (test of P0.1 fix)
- [ ] **Latency**: Flag if 5min fetch > 35s or scanner publish > 60s — already crossing soft SLA
- [ ] **D_EMA20_REJECTION**: At entry, verify ADX >= 20 in the signal bar before accepting

### Post-market review (mandatory tomorrow)

- [ ] Was C_OR_BREAKOUT entry engine fixed? Count gated vs entry rows
- [ ] Were any E_VWAP_LOSE_EARLY_SHORT trades fired after 09:40? What was their outcome?
- [ ] Did any trade hit +0.5R and then trigger breakeven stop? Outcome?
- [ ] Did T_TREND_DAY_EMA_STAIR_SHORT fire zero times?
- [ ] Update multi-window PF table with tomorrow's results
- [ ] Check feature gap count: are pre_bars and sig5_adx_calc still blank for accepted tickers?

### Thresholds to watch

- Scanner publish delay: WARN > 55s, ALERT > 70s
- 5min fetch duration: WARN > 35s
- No-progress holds: Flag any trade open > 60m below +0.20R
- Setup-level P&L: Daily SL count per setup — if D_AVWAP_LOSE_REVERSAL hits 2 SLs on same day, review before third entry

---

## M. Changes to Test Separately (Not in Live)

**Test 1: A_MOD_BREAK_C1_HIGH LONG V8 gate audit**
- Shadow-log every rejection reason for today's 161 raw candidates
- Build a histogram: which gate condition blocks the most? Is it a single threshold or multiple?
- Run a 5-session shadow paper where the gate is relaxed by one condition at a time
- Do not promote until MAE is confirmed controlled (today's evidence: SUVEN 0.12% MAE, BANKINDIA 0.05% MAE is promising but sample is small)

**Test 2: Trail-after-0.5R exit with 1-minute bar confirmation**
- Implement as a shadow exit logic running alongside current fixed SL/target
- Compare: shadow trail vs actual fixed exit for all paper trades over next 5 sessions
- Measure: shadow net R vs actual net R, and whether trail-stop activates more on winners or whipsaws

**Test 3: Two-speed entry for LONG setups**
- Strong momentum (pre2_mom_r > 0.15 AND pre5_vol_ratio20 > 1.5): enter on V11 selected bar (current behavior)
- Weak momentum (passes V8 gate but pre2_mom_r < 0.10): wait for next 1-minute bar to close in the entry direction before entry
- Do not implement until C_OR_BREAKOUT entry engine fix is deployed

**Test 4: 30-minute time-to-progress gate**
- Paper-only shadow: if trade does not reach +0.25R within 30 minutes, tighten SL to -0.5R from -1.0R
- Target cases: UNIONBANK (159.6m hold), NIITMTS (143.8m hold)
- Measure whether this reduces avg SL hold without killing slow-but-eventually-profitable trades

**Test 5: Regime-gated short setups**
- T_TREND_DAY_EMA_STAIR_SHORT: test BEAR-regime-only activation (today regime = CHOP_NEUTRAL; in BEAR regime, this setup likely behaves better)
- Evidence from multi-window: BEAR regime trades show PF=1.07 across all setups; NEUTRAL = PF=0.86; UNKNOWN = PF=0.73
- Gate: only fire T_TREND_DAY_EMA_STAIR_SHORT when `market_ret < -0.3%` in current session

---

## Summary: Tomorrow's Net Expected P&L Impact from P0 + P1 Changes

| Change | Expected daily P&L impact |
|---|---|
| Disable T_TREND_DAY_EMA_STAIR_SHORT | +Rs 100/day (PF 0.47, ~7 trades/day at -Rs 100/day net) |
| E_VWAP_LOSE_EARLY_SHORT time restriction | +Rs 50–200/day (eliminates first-slot losses) |
| Breakeven stop after +0.5R | +Rs 300–700/day on days with AWFIS-type patterns |
| D_EMA20_REJECTION ADX filter | +Rs 150–400/day on ADX<20 SL avoidance |
| Fix C_OR_BREAKOUT entry engine | +Rs 500–2,000/day (29 high-score candidates blocked today) |
| C_OR_BREAKDOWN probation | +Rs 150/day (PF 0.38 contributes ~-Rs 150/day over window) |
