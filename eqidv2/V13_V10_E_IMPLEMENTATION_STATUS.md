# V13-v10-E corrected implementation

V13-v10-E combines the exact frozen V13-v10-B setup-specific SL and target table with the causal completed one-minute confirmation-volume ratio filter used in V13-v10-D.

- Confirmation volume ratio minimum: 1.20, applied before native per-setup ranking.
- Exit table: unchanged from V13-v10-B, including its 3.00% target cap.
- Entry: next-minute breakout trigger with ten-minute expiry.
- Exit handling: full position only, no partial exit and no break-even stop move; 15:15 square-off.

Run `python -B fno_v13_v10_e_research.py` to rebuild the frozen artifacts and detailed daywise result.

## Corrected result

| Period | Selected | Executed | Wins-losses | Win rate | PF | Modeled net profit | Daily-close DD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full 31 sessions | 59 | 47 | 33-14 | 70.21% | 4.9058 | Rs 161,336.10 | Rs 7,313.06 |
| July 29-31 | 5 | 4 | 4-0 | 100.00% | infinity | Rs 26,243.15 | Rs 0.00 |
| August | 40 | 35 | 23-12 | 65.71% | 4.2185 | Rs 114,056.97 | Rs 7,313.06 |
| September 1-11 | 14 | 8 | 6-2 | 75.00% | 4.5839 | Rs 21,035.97 | Rs 5,869.65 |

At 9bps round-trip cost, the full result is Rs 151,936.10 with PF 4.4447. The September result is Rs 19,435.97 with PF 4.1000.

The standalone CLI replay matches the research ledger across all 59 selected orders with zero P&L difference. The B control replay also matches the prior frozen B ledger exactly.
