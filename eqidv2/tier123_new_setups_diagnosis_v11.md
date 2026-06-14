# New Tier123 Research Setups — Honest Diagnosis (v11, CORRECTED VWAP/regime)
*Validated 2026-06-13 on the corrected tier123 probe (after the VWAP/regime fix) through the full
T_iterate search + anti-overfit battery. NET of cost; train Nov-Apr / test May-Jun. Engines:
research_v11_tier123_new_setups.py (probe), tier123_new_iterate.py (search), tier123_new_validate.py (battery).*

## Result: of 16 new setups (8 L/S pairs), **2 STRONG + 2 WEAK pass; 12 reject.**

| Setup | n (tr/te) | verdict | battery summary |
|---|---|---|---|
| **P_PDH_BREAK_RETEST_LONG** | 1350 (1000/350) | **STRONG PASS** | gate `pre_entry_mom>=75 & body<=0.75 & pre3_range>=0.5`, exit 0.5/0.6 → train 2.39 [even 2.50/2.32] / test 6.88, **5/5 exits sig** (p 0.004-0.082), **83% months**, top1d 25-44%; pre3_range robust |
| **E_ORB_RETEST_HOLD_LONG** | 1510 (1000/510) | **STRONG PASS** | gate `sig5_adx>=42 & vol>=2.42 & quality>=86 & signal>=605`, exit 0.9/1.25 → train 2.54 [even 2.46/2.63] / test 2.50, **monotone** sens, 4/5 exits (p 0.007-0.049), **91% months**, top1d 41-56%; robust 2-term core (adx & vol) |
| V_RECLAIM_PULLBACK_LONG | 1303 (1000/303) | WEAK/conditional | 2-term `sig5_adx<=16.9 & pre5_dir_count<=2`, top1d 25-29%, even halves, but **narrow low-ADX CLIFF** (<=16.9 ok, <=20 -> 1.10); 3/5 exits; 75% months |
| E_ORB_RETEST_HOLD_SHORT | 1499 (1000/499) | WEAK/conditional | 4-term (3 load-bearing), strong at tight 0.6 target (p0.002 top1d33%) but **narrow pre3_close_pos pocket**; only 2/5 exits; 75% months |
| E_FAILED_OR_BREAKDOWN_TRAP_LONG | 176 | reject (borderline) | small sample; best 4-term, market_ret-conditioned |
| E_FAILED_OR_BREAKOUT_TRAP_SHORT | 120 | reject | tiny sample + 3123 PF>=2 configs (extreme multiple-testing) |
| V_REJECTION_PULLBACK_SHORT | 1061 | reject (borderline) | cleaner gates top1d 53-56%; most day-concentrated/down-market |
| M_EXPANSION_FIRST_PULLBACK_LONG/SHORT | 1369/1370 | reject | ungated 0.47/0.53; best configs market_ret-conditioned, top1d 79-136% (and single-day artifacts top1d 3264-12519%) |
| C_LATE_MORNING_COMPRESSION_BREAK_LONG/SHORT | 534/447 | reject | all gated test PF<1.0; no edge |
| G_GAP_HOLD_CONTINUATION_LONG | 1195 | reject (borderline) | a couple p<0.10 top1d 38-40% but 4-term + market_ret-conditioned; ungated 0.43 |
| G_GAP_HOLD_CONTINUATION_SHORT | 671 | reject | 0 train-PF>=2; robustness all day-concentrated (top1d 75-268%) |
| A_HVN_ABSORPTION_BREAK_LONG/SHORT | 341/293 | reject | no clean passer; p>0.10, top1d 50-7780% |
| P_PDL_BREAK_RETEST_SHORT | 1320 | reject | gaudy (test PF 10-28) BUT every gate `market_ret<=-0.66` over **only 4 test days** = down-market/crash-day trap |

## The 2 STRONG passes (promote candidates, pending review)
Both are TIER123 / scanner-source (need external-candidate injection wiring, like the removed S_UPTHRUST),
validated on the CORRECTED VWAP/regime probe. Caveats: thin test (n10-18), scanner-source provenance,
found via a wide search (multiple-testing) but they SURVIVE the full anti-overfit battery — the same battery
that exposed S_UPTHRUST/T_TREND_SHORT as broken-data artifacts and confirmed L_RS_LEADER.

- **P_PDH_BREAK_RETEST_LONG** — a controlled prev-day-high break-retest entered with strong pre-entry
  momentum and a small (non-blow-off) body. 5/5 exits significant, 83% months, top1d 25-44%, pre3_range
  monotone-robust. Tight 0.6-target scalp. Drop-out: pre_entry_momentum + body load-bearing; pre3_range a refinement.
- **E_ORB_RETEST_HOLD_LONG** — an opening-range retest-and-hold in a strong-ADX, high-volume, high-quality
  context. MONOTONE sensitivity on ADX (the stronger the trend, the better — not a knife-edge), 4/5 exits,
  91% months. Robust 2-term core (sig5_adx & vol_ratio); quality/signal_minute are refinements.

## Honest framing
The day-concentration metric + term drop-out + sensitivity did the discriminating again: they killed the
down-market-conditioned shorts (P_PDL, M_EXPANSION, the bear gates) and the day-concentrated longs, and
confirmed 2 setups with genuine, spread, monotone/robust edges. The 2 WEAK ones (V_RECLAIM_LONG,
E_ORB_RETEST_SHORT) have real but fragile (cliff/narrow-pocket) gates — research-watch, not promote.
**NOT auto-added to the book.** Recommend promoting the 2 STRONG (would take the book to 11 active).
