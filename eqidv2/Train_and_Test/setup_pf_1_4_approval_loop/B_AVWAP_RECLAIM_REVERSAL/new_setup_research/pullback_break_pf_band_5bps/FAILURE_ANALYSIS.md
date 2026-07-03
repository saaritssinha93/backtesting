# FAILURE_ANALYSIS — B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG

Loss diagnosis for **best greedy config (no candidate passed)** (full-TRAIN book, 5 bps/leg, net of cost).

- trades=34 win=70.59% PF=2.8202 net=Rs11,828
- outcome split: TARGET=12  SL=2  EOD/time=20
- avg win=Rs764  avg loss=Rs-650  maxDD=Rs-1,917
- gross profit=Rs18,327  gross loss=Rs-6,498

## Worst days
- 2026-05-29: n=1 net=Rs-1,431 PF=0.0
- 2026-06-19: n=3 net=Rs-1,011 PF=0.051
- 2026-05-19: n=1 net=Rs-593 PF=0.0
- 2026-06-18: n=2 net=Rs-325 PF=0.773
- 2026-06-11: n=1 net=Rs-313 PF=0.0
- 2026-06-15: n=2 net=Rs-306 PF=0.551

## Worst symbols
- AUROPHARMA: n=1 net=Rs-1,431
- VBL: n=1 net=Rs-1,427
- INDIANB: n=1 net=Rs-792
- APLAPOLLO: n=1 net=Rs-695
- SHREECEM: n=1 net=Rs-682
- NTPC: n=1 net=Rs-593
- FORTIS: n=1 net=Rs-313
- AUBANK: n=1 net=Rs-202

## Time-of-day (entry hour)
- 10:00  n=5 net=Rs3,044 PF=34.732
- 11:00  n=12 net=Rs-518 PF=0.893
- 12:00  n=10 net=Rs7,359 PF=24.49
- 13:00  n=7 net=Rs1,944 PF=2.535

## Notes
- SL share = 6.0% of exits; target share = 35.0%. A high SL share with low target share = fake reclaim / no follow-through (raw double-bottom signal is being faded). Pre-momentum / volume gates aim to remove the no-follow-through subset; the ablation log shows whether any group recovered a band-PF edge.