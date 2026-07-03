# S9_MIDDAY_LOSE (SHORT) — last 3 months

Setup `S9_MIDDAY_LOSE` SHORT, bracket **x_125_250** (SL 1.25% / target 2.50%, 1:2), morning-only ≤11:00, mask `mom3_pct≥0.1 & atr_pct≥0.3`, no-overlap per symbol, ≤20 concurrent.
Window **2026-03-30 .. 2026-06-29** (60 resolvable sessions). Note: 2026-04-30–2026-06-15 was the S9 TRAIN (in-sample); 2026-06-15–2026-06-29 = TEST (OOS); **before 2026-04-30 = extra OOS never seen by the search**. Net of statutory cost + slippage.

## Overall (primary bracket x_125_250)
| cost | trades | PF | win% | net Rs | exp/tr | avg win | avg loss | tgt/sl/eod | dayDom | symDom | topTr | maxDD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 bps | 238 | 1.117 | 51.68 | 14,837 | 62.3 | 1148.0 | -1098.9 | 24/80/134 | 0.111 | 0.033 | 0.017 | 28,942 |
| 5 bps | 238 | 1.005 | 47.9 | 674 | 2.8 | 1176.2 | -1076.0 | 24/80/134 | 0.114 | 0.033 | 0.017 | 32,308 |
| 15 bps | 238 | 0.708 | 42.44 | -46,419 | -195.0 | 1115.7 | -1161.4 | 24/80/134 | 0.121 | 0.036 | 0.019 | 53,829 |
| 0 bps (statutory only) | 238 | 1.199 | 52.94 | 24,231 | 101.8 | — | — | — | — | — | — | — |

_True price-path (0 cost incl. no statutory): net Rs 43,820, PF 1.39._

## Month-by-month (primary bracket, 5 bps/leg)
| month | sessions | trades | PF | win% | net Rs | exp/tr | in-sample? |
|---|---:|---:|---:|---:|---:|---:|---|
| 2026-03 | 1 | 3 | 0.968 | 66.7 | -45 | -15.1 | extra OOS |
| 2026-04 | 20 | 88 | 0.649 | 37.5 | -20,116 | -228.6 | extra OOS |
| 2026-05 | 19 | 71 | 1.446 | 53.5 | 15,181 | 213.8 | TRAIN/in-sample |
| 2026-06 | 20 | 76 | 1.139 | 53.9 | 5,654 | 74.4 | TRAIN/in-sample |

## Bracket robustness over the 3 months (5 bps/leg)
| bracket | trades | PF | win% | net Rs |
|---|---:|---:|---:|---:|
| x_075_075 | 239 | 0.854 | 57.32 | -12,874 |
| x_100_200 | 239 | 1.066 | 47.28 | 8,029 |
| x_125_250 | 238 | 1.005 | 47.9 | 674 |
| x_100_300 | 239 | 1.05 | 46.03 | 6,163 |

## Day-wise net (5 bps) — worst 5 / best 5
```
WORST: 2026-06-02:-9,823(n10) | 2026-04-02:-8,532(n6) | 2026-05-18:-5,958(n5) | 2026-04-08:-5,634(n8) | 2026-04-16:-5,617(n7)
BEST : 2026-05-29:6,443(n4) | 2026-04-29:7,133(n8) | 2026-06-25:7,947(n6) | 2026-06-08:13,647(n11) | 2026-05-12:15,262(n8)
```

Profitable days: **26/56**. Best single day = **11%** of gross profit (concentration risk).

## Per-symbol net (5 bps) — worst 5 / best 5
```
WORST: KEI:-4,879(n5) | MEESHO:-3,778(n4) | MFSL:-2,932(n3) | BBOX:-2,860(n2) | IIFL:-2,859(n2)
BEST : APOLLO:2,953(n2) | UNIONBANK:3,087(n2) | FORCEMOT:3,634(n2) | LENSKART:4,144(n2) | KAYNES:4,481(n2)
```

## Read
- Over the full 3 months (60 sessions): **238 trades, PF 1.005, win 47.9%, net Rs 674 @5 bps** (≈4.0 trades/day).
- Still **not a fast scalp** — ~56% exit at EOD, avg hold 222.4 min.
- **WATCH / research only — DO NOT PROMOTE WITHOUT APPROVAL.** final_setup_conf.py untouched.