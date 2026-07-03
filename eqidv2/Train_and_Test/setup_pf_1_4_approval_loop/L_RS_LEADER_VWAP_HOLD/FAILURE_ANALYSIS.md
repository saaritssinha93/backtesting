# FAILURE_ANALYSIS - L_RS_LEADER_VWAP_HOLD

Primary analyzed config: iter 71 (TRAIN PF too low).
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", "<=", 21.924144]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- TRAIN: n=39 PF=0.9645 net=Rs-598 win=48.72% t/s/e=9/13/17 dom=0.076/9.99/9.99
- TEST: not run

## Losing Trade / Structure Notes
- The baseline is too sparse to diagnose: one TRAIN target, one TRAIN SL, one TEST SL.
- Meaningful-trade variants fail because the broad RS-leader VWAP-hold signal does not follow through after costs.
- Failures classify mainly as TRAIN PF too low, too few trades for the baseline, and one day/symbol dominance for tiny subsets.

## Worst TRAIN Days
- 2026-04-23: n=3 net=Rs-2,477
- 2026-04-09: n=3 net=Rs-1,684
- 2026-04-27: n=3 net=Rs-1,517
- 2026-03-25: n=1 net=Rs-933
- 2026-04-21: n=1 net=Rs-932
- 2026-05-05: n=1 net=Rs-931
- 2026-05-04: n=1 net=Rs-927
- 2026-05-13: n=1 net=Rs-927
- 2026-04-20: n=1 net=Rs-732
- 2026-03-24: n=2 net=Rs-672

## Worst TRAIN Symbols
- NHPC: n=1 net=Rs-933
- ADANIENT: n=1 net=Rs-932
- SYNGENE: n=1 net=Rs-932
- ADANIENSOL: n=1 net=Rs-931
- TATAPOWER: n=1 net=Rs-931
- CAMS: n=1 net=Rs-929
- GLENMARK: n=1 net=Rs-928
- JSWSTEEL: n=1 net=Rs-927
- HDFCAMC: n=1 net=Rs-927
- INDUSINDBK: n=1 net=Rs-927

## Time Windows
- 10:00 n=8 net=Rs-1,981
- 11:00 n=7 net=Rs-1,241
- 12:00 n=12 net=Rs686
- 13:00 n=11 net=Rs1,320
- 14:00 n=1 net=Rs618

## Failure Classes Seen
- TRAIN PF too low for all meaningful-trade variants.
- Too few trades for the original card gate.
- One trade/day/symbol dominance for any tiny in-band pocket.
- Known live/backtest mismatch: setup was demoted after live paper PF 0.15.
