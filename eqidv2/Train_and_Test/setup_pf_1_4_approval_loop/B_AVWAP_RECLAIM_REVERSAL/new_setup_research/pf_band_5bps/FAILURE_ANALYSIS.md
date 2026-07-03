# FAILURE_ANALYSIS — B_AVWAP_CONFIRMED_RECLAIM_LONG

Loss diagnosis for **best greedy config (no candidate passed)** (full-TRAIN book, 5 bps/leg, net of cost).

- trades=13 win=69.23% PF=2.9453 net=Rs5,266
- outcome split: TARGET=6  SL=1  EOD/time=6
- avg win=Rs886  avg loss=Rs-677  maxDD=Rs-1,129
- gross profit=Rs7,973  gross loss=Rs-2,707

## Worst days
- 2026-06-18: n=1 net=Rs-1,129 PF=0.0
- 2026-06-15: n=2 net=Rs-306 PF=0.551
- 2026-05-26: n=1 net=Rs-202 PF=0.0
- 2026-06-16: n=2 net=Rs-97 PF=0.86
- 2026-05-27: n=1 net=Rs309 PF=inf
- 2026-06-02: n=1 net=Rs1,115 PF=inf

## Worst symbols
- VBL: n=1 net=Rs-1,129
- APLAPOLLO: n=1 net=Rs-695
- SHREECEM: n=1 net=Rs-682
- AUBANK: n=1 net=Rs-202
- DMART: n=1 net=Rs598
- ASTRAL: n=2 net=Rs685
- DLF: n=1 net=Rs1,114
- ANGELONE: n=1 net=Rs1,115

## Time-of-day (entry hour)
- 10:00  n=3 net=Rs2,021 PF=inf
- 11:00  n=5 net=Rs-1,014 PF=0.595
- 12:00  n=2 net=Rs2,231 PF=inf
- 13:00  n=3 net=Rs2,027 PF=11.05

## Notes
- SL share = 8.0% of exits; target share = 46.0%. A high SL share with low target share = fake reclaim / no follow-through (raw double-bottom signal is being faded). Pre-momentum / volume gates aim to remove the no-follow-through subset; the ablation log shows whether any group recovered a band-PF edge.