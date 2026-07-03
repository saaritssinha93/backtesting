# FAILURE_ANALYSIS — B_AVWAP_CONFIRMED_RECLAIM_LONG

Loss diagnosis for **best greedy config (no candidate passed)** (full-TRAIN book, 5 bps/leg, net of cost).

- trades=18 win=66.67% PF=3.0087 net=Rs6,810
- outcome split: TARGET=8  SL=1  EOD/time=9
- avg win=Rs850  avg loss=Rs-565  maxDD=Rs-1,129
- gross profit=Rs10,201  gross loss=Rs-3,390

## Worst days
- 2026-06-18: n=1 net=Rs-1,129 PF=0.0
- 2026-05-19: n=1 net=Rs-593 PF=0.0
- 2026-06-15: n=2 net=Rs-306 PF=0.551
- 2026-05-26: n=1 net=Rs-202 PF=0.0
- 2026-06-04: n=1 net=Rs-90 PF=0.0
- 2026-05-27: n=1 net=Rs309 PF=inf

## Worst symbols
- VBL: n=1 net=Rs-1,129
- APLAPOLLO: n=1 net=Rs-695
- SHREECEM: n=1 net=Rs-682
- NTPC: n=1 net=Rs-593
- AUBANK: n=1 net=Rs-202
- VEDL: n=1 net=Rs2
- DMART: n=1 net=Rs598
- ASTRAL: n=2 net=Rs685

## Time-of-day (entry hour)
- 10:00  n=4 net=Rs1,931 PF=22.4
- 11:00  n=6 net=Rs-1,607 PF=0.481
- 12:00  n=3 net=Rs3,347 PF=inf
- 13:00  n=5 net=Rs3,140 PF=16.564

## Notes
- SL share = 6.0% of exits; target share = 44.0%. A high SL share with low target share = fake reclaim / no follow-through (raw double-bottom signal is being faded). Pre-momentum / volume gates aim to remove the no-follow-through subset; the ablation log shows whether any group recovered a band-PF edge.