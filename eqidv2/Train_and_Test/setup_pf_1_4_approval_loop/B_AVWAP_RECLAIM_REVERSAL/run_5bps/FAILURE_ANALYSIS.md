# FAILURE_ANALYSIS — B_AVWAP_RECLAIM_REVERSAL

Loss diagnosis for **best greedy config (no candidate passed)** (full-TRAIN book, 15 bps/leg, net of cost).

- trades=14 win=71.43% PF=1.7154 net=Rs2,663
- outcome split: TARGET=3  SL=4  EOD/time=7
- avg win=Rs639  avg loss=Rs-931  maxDD=Rs-1,862
- gross profit=Rs6,386  gross loss=Rs-3,723

## Worst days
- 2026-06-10: n=2 net=Rs-1,862 PF=0.0
- 2026-05-26: n=2 net=Rs-1,860 PF=0.0
- 2026-06-03: n=1 net=Rs146 PF=inf
- 2026-06-08: n=1 net=Rs296 PF=inf
- 2026-06-04: n=1 net=Rs337 PF=inf
- 2026-05-27: n=2 net=Rs460 PF=inf

## Worst symbols
- PRECAM: n=1 net=Rs-932
- DCBBANK: n=1 net=Rs-932
- SHANTIGOLD: n=1 net=Rs-930
- VTL: n=1 net=Rs-928
- MAHSEAMLES: n=1 net=Rs146
- SHRINGARMS: n=1 net=Rs168
- SMLMAH: n=1 net=Rs196
- ICIL: n=1 net=Rs264

## Time-of-day (entry hour)
- 11:00  n=1 net=Rs-932 PF=0.0
- 12:00  n=8 net=Rs3,454 PF=4.705
- 13:00  n=4 net=Rs-196 PF=0.894
- 14:00  n=1 net=Rs337 PF=inf

## Notes
- SL share = 29.0% of exits; target share = 21.0%. A high SL share with low target share = fake reclaim / no follow-through (raw double-bottom signal is being faded). Pre-momentum / volume gates aim to remove the no-follow-through subset; the ablation log shows whether any group recovered a band-PF edge.