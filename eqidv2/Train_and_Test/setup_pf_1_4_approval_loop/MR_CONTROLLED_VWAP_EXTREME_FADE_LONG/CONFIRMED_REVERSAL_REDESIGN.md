# MR_CONTROLLED_VWAP_EXTREME_FADE_LONG - Confirmed Reversal Redesign

Research-only run. No final config edit and no live execution.

## Change Tested
- Delayed entry until post-signal confirmation instead of buying the first next-minute open.
- Confirmation variants: break above signal high within 10/15m, optional higher-low path, optional two green 1m bars, and next-5m follow-through.
- Additional simple filters tested: market not weak, RS not deeply negative, no BEAR regime, volume/panic filters, close/body quality, premomentum, time/top-N guards.

## Split
- FIT: 2025-06-03..2025-11-25 (56 sessions)
- VAL: 2025-11-27..2026-03-30 (57 sessions)
- TRAIN: 2025-06-03..2026-03-30 (113 sessions)
- TEST: 2026-04-01..2026-05-29 (22 sessions)

## Entry Mode Survival
- `base_next_1m`: 310 candidate entries
- `confirm_break_high_10m`: 184 candidate entries
- `confirm_break_high_10m_higherlow`: 173 candidate entries
- `confirm_break_high_10m_higherlow_twogreen`: 162 candidate entries
- `confirm_break_high_15m_higherlow`: 182 candidate entries
- `confirm_next5m_follow`: 83 candidate entries
- `confirm_next5m_follow_higherlow`: 82 candidate entries

## Baseline
- Config: `base_next_1m SL=0.7 TGT=0.8 mask=[-] premom=[-] guard={}`
- TRAIN: n=240 PF=0.2789 net=Rs-75,681 win=28.33% t/s/e=44/74/122 dbp=1.0
- TEST: n=70 PF=0.2461 net=Rs-28,660 win=27.14% t/s/e=16/32/22 dbp=0.9998

## Best Overall Search Row
- Config: `base_next_1m SL=1.2 TGT=1.25 mask=[close_loc>=0.6] premom=[sig5_adx_calc>=20.0] guard={"max_slot": "12:00"}`
- FIT: n=11 PF=0.7844 net=Rs-684 win=45.45% t/s/e=0/0/0 dbp=0.6306
- VAL: n=12 PF=1.742 net=Rs2,917 win=75.0% t/s/e=0/0/0 dbp=0.185
- TRAIN: n=23 PF=1.3144 net=Rs2,233 win=60.87% t/s/e=6/2/15 dbp=0.28
- TEST: n=14 PF=0.4048 net=Rs-6,245 win=35.71% t/s/e=3/6/5 dbp=0.8386
- Status: TEST fail

## Best Confirmed-Entry Row
- Config: `confirm_break_high_10m_higherlow SL=1.0 TGT=1.5 mask=[rs_pct>=-1.0] premom=[sig5_adx_calc>=20.0] guard={"max_slot": "13:00", "top_n": 1}`
- FIT: n=8 PF=1.0473 net=Rs117 win=50.0% t/s/e=0/0/0 dbp=0.4831
- VAL: n=9 PF=1.4142 net=Rs861 win=33.33% t/s/e=0/0/0 dbp=0.3552
- TRAIN: n=17 PF=1.215 net=Rs978 win=41.18% t/s/e=3/1/13 dbp=0.3858
- TEST: not run
- Status: TRAIN out of band

## Passing Candidates
- None passed TRAIN PF 1.30-1.70 + TEST PF > 1.40 + dominance checks.

## Recommendation
NO APPROVAL CANDIDATE

DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES