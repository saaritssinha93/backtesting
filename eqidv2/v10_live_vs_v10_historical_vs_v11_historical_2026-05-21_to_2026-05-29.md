# Three-way comparison: 2026-05-21 to 2026-05-29

## Sources

- `v10_live_replay`: actual v7 live paper replay from `C:\TradingData\eqidv2\outputs_ID_v10_5min\trades.csv`.
- `v10_historical`: v10 historical regenerated backtest from `C:\TradingData\eqidv2\outputs_ID_v10_historical_2026-05-01_to_2026-05-29\trades.csv`; includes modeled costs in net PnL.
- `v11_historical`: v11 full historical regenerated backtest from `C:\TradingData\eqidv2\outputs_ID_v11_5min\trades.csv`; no modeled costs in this output.
- Date filter applied to all sources: `2026-05-21` through `2026-05-29`.

## Overall

| Metric | v10 live replay | v10 historical | v11 historical |
| --- | --- | --- | --- |
| Trades | 82 | 162 | 163 |
| Wins / Losses / Flats | 34 / 48 / 0 | 69 / 93 / 0 | 72 / 91 / 0 |
| Win rate | 41.46% | 42.59% | 44.17% |
| Target / SL / EOD | 23 / 38 / 21 | 28 / 62 / 72 | 23 / 69 / 71 |
| Profit factor | 0.964 | 0.773 | 0.987 |
| Gross PnL | Rs -1,200.39 | Rs 6,561.83 | Rs -701.35 |
| Costs | Rs 0.00 | Rs 13,890.00 | Rs 0.00 |
| Net PnL | Rs -1,200.39 | Rs -7,328.17 | Rs -701.35 |
| Avg trade | Rs -14.64 | Rs -45.24 | Rs -4.30 |
| Median trade | Rs -507.70 | Rs -88.78 | Rs -129.60 |
| LONG trades / PnL | 67 / Rs -5,205.53 | 82 / Rs -11,295.70 | 82 / Rs -14,922.99 |
| SHORT trades / PnL | 15 / Rs 4,005.14 | 80 / Rs 3,967.54 | 81 / Rs 14,221.64 |

## Deltas

| Comparison | Trade Delta | Net PnL Delta | Win-rate Delta | PF Delta | LONG Delta | SHORT Delta |
| --- | --- | --- | --- | --- | --- | --- |
| v10_historical vs v10_live_replay | 80 | Rs -6,127.78 | 1.13% | -0.191 | 15 / Rs -6,090.17 | 65 / Rs -37.61 |
| v11_historical vs v10_live_replay | 81 | Rs 499.04 | 2.71% | 0.023 | 15 / Rs -9,717.46 | 66 / Rs 10,216.49 |
| v11_historical vs v10_historical | 1 | Rs 6,626.82 | 1.58% | 0.214 | 0 / Rs -3,627.29 | 1 / Rs 10,254.10 |

## Daily

| Date | v10 live Trades | v10 live Net | v10 live PF | v10 hist Trades | v10 hist Net | v10 hist PF | v11 Trades | v11 Net | v11 PF | v11-v10hist Net |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-05-21 | 0 | Rs 0.00 | 0.000 | 10 | Rs -85.69 | 0.931 | 10 | Rs 1,114.59 | 1.669 | Rs 1,200.27 |
| 2026-05-22 | 15 | Rs -3,628.24 | 0.458 | 37 | Rs -3,887.02 | 0.501 | 38 | Rs -3,059.58 | 0.760 | Rs 827.44 |
| 2026-05-23 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | Rs 0.00 |
| 2026-05-24 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | Rs 0.00 |
| 2026-05-25 | 14 | Rs -2,546.11 | 0.604 | 32 | Rs -3,751.78 | 0.473 | 32 | Rs -5,053.66 | 0.571 | Rs -1,301.88 |
| 2026-05-26 | 16 | Rs -1,175.54 | 0.837 | 25 | Rs -598.85 | 0.883 | 25 | Rs 2,502.16 | 1.309 | Rs 3,101.01 |
| 2026-05-27 | 25 | Rs 6,587.59 | 1.959 | 40 | Rs -1,371.49 | 0.828 | 40 | Rs -1,682.87 | 0.878 | Rs -311.39 |
| 2026-05-28 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | Rs 0.00 |
| 2026-05-29 | 12 | Rs -438.09 | 0.926 | 18 | Rs 2,366.66 | 1.777 | 18 | Rs 5,478.02 | 1.983 | Rs 3,111.36 |

## Side

| Side | v10 live Trades | v10 live Net | v10 live PF | v10 hist Trades | v10 hist Net | v10 hist PF | v11 Trades | v11 Net | v11 PF | v11-v10hist Net |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| LONG | 67 | Rs -5,205.53 | 0.825 | 82 | Rs -11,295.70 | 0.440 | 82 | Rs -14,922.99 | 0.553 | Rs -3,627.29 |
| SHORT | 15 | Rs 4,005.14 | 2.185 | 80 | Rs 3,967.54 | 1.327 | 81 | Rs 14,221.64 | 1.699 | Rs 10,254.10 |

## Setup

| Setup | v10 live Trades | v10 live Net | v10 live PF | v10 hist Trades | v10 hist Net | v10 hist PF | v11 Trades | v11 Net | v11 PF | v11-v10hist Net |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SHORT D_EMA20_REJECTION | 15 | Rs 4,005.14 | 2.185 | 80 | Rs 3,967.54 | 1.327 | 81 | Rs 14,221.64 | 1.699 | Rs 10,254.10 |
| LONG B_AVWAP_RECLAIM_REVERSAL | 5 | Rs -1,473.65 | 0.128 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | Rs 0.00 |
| LONG G_HIGHER_HIGH_BREAK | 22 | Rs 11.42 | 1.001 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | Rs 0.00 |
| LONG L_TREND_PULLBACK | 18 | Rs -2,645.90 | 0.666 | 0 | Rs 0.00 | 0.000 | 0 | Rs 0.00 | 0.000 | Rs 0.00 |
| LONG D_EMA20_BOUNCE | 22 | Rs -1,097.40 | 0.887 | 82 | Rs -11,295.70 | 0.440 | 82 | Rs -14,922.99 | 0.553 | Rs -3,627.29 |

## Entry Window

| Window | v10 live Trades | v10 live Net | v10 live PF | v10 hist Trades | v10 hist Net | v10 hist PF | v11 Trades | v11 Net | v11 PF | v11-v10hist Net |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11:01-12:00 | 37 | Rs 2,055.93 | 1.141 | 38 | Rs -101.67 | 0.988 | 38 | Rs -1,172.50 | 0.922 | Rs -1,070.82 |
| 12:01-13:00 | 28 | Rs -3,477.96 | 0.711 | 46 | Rs 1,852.93 | 1.234 | 46 | Rs 6,559.13 | 1.457 | Rs 4,706.20 |
| 13:01-14:00 | 11 | Rs -730.06 | 0.833 | 50 | Rs -7,811.47 | 0.344 | 50 | Rs -8,044.66 | 0.551 | Rs -233.19 |
| 14:01-15:00 | 6 | Rs 951.70 | 1.455 | 28 | Rs -1,267.96 | 0.705 | 29 | Rs 1,956.68 | 1.311 | Rs 3,224.63 |

## Pairwise Trade Overlap

| A | B | Exact common | A-only exact | B-only exact | Loose common | A-only loose | B-only loose |
| --- | --- | --- | --- | --- | --- | --- | --- |
| v10_live_replay | v10_historical | 12 | 70 | 150 | 38 | 44 | 124 |
| v10_live_replay | v11_historical | 12 | 70 | 151 | 38 | 44 | 125 |
| v10_historical | v11_historical | 162 | 0 | 1 | 162 | 0 | 1 |

## v10 Historical vs v11 Exact Common Trades

| Date | Signal | Ticker | Side | Setup | v10 hist outcome | v10 hist PnL | v11 outcome | v11 PnL | Delta |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-05-27 | 12:55 | KINGFA | LONG | D_EMA20_BOUNCE | TARGET | Rs 670.00 | SL | Rs -698.40 | Rs -1,368.40 |
| 2026-05-25 | 11:45 | FDC | SHORT | D_EMA20_REJECTION | TARGET | Rs 570.00 | SL | Rs -747.96 | Rs -1,317.96 |
| 2026-05-29 | 11:15 | GALAPREC | SHORT | D_EMA20_REJECTION | TARGET | Rs 570.00 | SL | Rs -745.18 | Rs -1,315.18 |
| 2026-05-27 | 11:20 | UNOMINDA | LONG | D_EMA20_BOUNCE | EOD | Rs 280.45 | SL | Rs -692.56 | Rs -973.01 |
| 2026-05-22 | 12:20 | TATACOMM | LONG | D_EMA20_BOUNCE | EOD | Rs 117.00 | SL | Rs -698.19 | Rs -815.19 |
| 2026-05-27 | 11:50 | 63MOONS | SHORT | D_EMA20_REJECTION | TARGET | Rs 570.00 | EOD | Rs -200.66 | Rs -770.66 |
| 2026-05-25 | 12:45 | TMCV | LONG | D_EMA20_BOUNCE | EOD | Rs -212.34 | SL | Rs -700.56 | Rs -488.22 |
| 2026-05-22 | 12:05 | MRF | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -946.67 | Rs -476.67 |
| 2026-05-26 | 12:45 | LEMONTREE | LONG | D_EMA20_BOUNCE | EOD | Rs -364.40 | SL | Rs -695.36 | Rs -330.96 |
| 2026-05-21 | 12:25 | FUSION | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -751.95 | Rs -281.95 |
| 2026-05-27 | 13:00 | SATIN | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -750.32 | Rs -280.32 |
| 2026-05-22 | 13:15 | EPL | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -750.26 | Rs -280.26 |
| 2026-05-27 | 13:50 | WEBELSOLAR | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -749.70 | Rs -279.70 |
| 2026-05-26 | 14:30 | LANDMARK | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -749.55 | Rs -279.55 |
| 2026-05-22 | 12:30 | PPLPHARMA | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -749.32 | Rs -279.32 |
| 2026-05-22 | 12:05 | TENNIND | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -749.28 | Rs -279.28 |
| 2026-05-27 | 13:05 | EUREKAFORB | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -749.28 | Rs -279.28 |
| 2026-05-26 | 12:40 | MHRIL | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -749.10 | Rs -279.10 |
| 2026-05-29 | 13:40 | SUNTECK | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -748.98 | Rs -278.98 |
| 2026-05-29 | 11:30 | KRISHANA | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -748.88 | Rs -278.88 |
| 2026-05-25 | 13:50 | JAINREC | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -748.51 | Rs -278.51 |
| 2026-05-27 | 11:45 | SEAMECLTD | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -748.34 | Rs -278.34 |
| 2026-05-21 | 13:00 | ARVIND | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -748.25 | Rs -278.25 |
| 2026-05-29 | 11:15 | TARSONS | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -747.32 | Rs -277.32 |
| 2026-05-29 | 11:25 | AARTIPHARM | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -746.90 | Rs -276.90 |
| 2026-05-29 | 13:15 | MONARCH | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -746.35 | Rs -276.35 |
| 2026-05-27 | 13:10 | KIMS | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -746.20 | Rs -276.20 |
| 2026-05-27 | 12:50 | LUMAXIND | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -746.13 | Rs -276.13 |
| 2026-05-25 | 13:30 | ANTHEM | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -744.90 | Rs -274.90 |
| 2026-05-25 | 13:10 | GOODLUCK | SHORT | D_EMA20_REJECTION | SL | Rs -470.00 | SL | Rs -741.30 | Rs -271.30 |
| 2026-05-27 | 14:25 | GHCLTEXTIL | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -703.61 | Rs -258.61 |
| 2026-05-26 | 12:45 | IGL | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -702.10 | Rs -257.10 |
| 2026-05-26 | 11:40 | J&KBANK | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -700.92 | Rs -255.92 |
| 2026-05-25 | 13:55 | JYOTICNC | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -700.04 | Rs -255.04 |
| 2026-05-27 | 11:50 | MARINE | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -700.00 | Rs -255.00 |
| 2026-05-22 | 11:55 | EMSLIMITED | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -699.96 | Rs -254.96 |
| 2026-05-25 | 14:00 | SUNTECK | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -699.78 | Rs -254.78 |
| 2026-05-25 | 12:55 | IGIL | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -699.60 | Rs -254.60 |
| 2026-05-22 | 13:40 | DAMCAPITAL | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -699.20 | Rs -254.20 |
| 2026-05-27 | 11:35 | TIRUMALCHM | LONG | D_EMA20_BOUNCE | SL | Rs -445.00 | SL | Rs -699.13 | Rs -254.13 |

## Biggest v11-only Winners vs v10 Historical

| Date | Signal | Ticker | Side | Setup | Outcome | PnL |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-05-22 | 14:00 | KERNEX | SHORT | D_EMA20_REJECTION | EOD | Rs 621.96 |

## Worst v10 Historical-only Trades Missing/Changed in v11

_No rows._
