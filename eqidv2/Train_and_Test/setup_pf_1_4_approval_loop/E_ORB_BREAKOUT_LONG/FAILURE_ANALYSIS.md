# E_ORB_BREAKOUT_LONG — FAILURE_ANALYSIS

Loss structure of the baseline (card) config — the live-faithful behaviour the search must beat.

### TRAIN baseline (29 trades, 21 losers, net Rs-9,402)
- outcome split: {'SL': 21, 'TARGET': 8}
- losers by hour: {'09': 11, '10': 10}
- worst days: 2026-05-22:Rs-1,864, 2026-06-18:Rs-1,863, 2026-06-04:Rs-1,859, 2026-05-25:Rs-1,859
- worst symbols: TMB(n1/Rs-932), TBOTEK(n1/Rs-932), DAMCAPITAL(n1/Rs-932), SUNTECK(n1/Rs-932), SOLARWORLD(n1/Rs-932), STCINDIA(n1/Rs-932)
- best symbols: IOLCP(n1/Rs1,265), JSLL(n1/Rs1,266), THOMASCOOK(n1/Rs1,266), JGCHEM(n1/Rs1,266)

### TEST baseline (6 trades, 4 losers, net Rs-1,189)
- outcome split: {'SL': 4, 'TARGET': 2}
- losers by hour: {'09': 1, '10': 3}
- worst days: 2026-06-22:Rs-926, 2026-06-24:Rs-599, 2026-06-23:Rs337
- worst symbols: HUBTOWN(n1/Rs-932), ICIL(n1/Rs-930), COHANCE(n1/Rs-929), CLEAN(n1/Rs-926), ENTERO(n1/Rs1,263), OMINFRAL(n1/Rs1,266)
- best symbols: COHANCE(n1/Rs-929), CLEAN(n1/Rs-926), ENTERO(n1/Rs1,263), OMINFRAL(n1/Rs1,266)

## Notes
- SL vs target vs EOD mix above shows whether the exit is too tight/wide.
- A few dominant losing symbols/days = idiosyncratic, not a systematic edge failure.
- TEST has only 3 sessions → day-level conclusions are not robust.