# G3 daily top-two movers: selection and missed-signal audit

**Coverage:** 64 complete sealed G3 sessions, 2026-07-29 through 2026-11-03. Each day examines the two largest qualifying moves in each direction. There are 255 stock/day/direction cases; a direction with fewer than two stocks moving at least 3% has fewer rows.

**Mover definition:** First available 09:16 IST one-minute open to the day's highest high for LONG or lowest low for SHORT, threshold 3%. First 3% and extreme times are hindsight diagnostics. G3 uses only its completed signal bar and next-minute confirmation; the latest signal is 13:30.

**Selection meaning:** Trade = portfolio-executed entry; Order only = selected but its trigger was not hit or portfolio capital was unavailable; Missed = no order selected in that direction. One stock can have multiple slot orders. The reason column names the closest enabled slot by fewest failed gates, or a disabled slot that produced a native signal. Other slots can have different failures.

**One tested change:** V20 caps each enabled slot's minimum next-minute confirmation candle body ratio at 0.30. A slot already requiring 0.30 or less is unchanged. All other V19 filters, enabled slots, no-quota policy and portfolio rules stay fixed. Daily high/low rank is never used in selecting G3 orders. The 0.30 threshold was examined against these dates, so this is an in-sample test.

## Full-history replay

| Metric | V19 no slot cap | V20 body minimum cap 0.30 |
|:---|---:|---:|
| Selected orders | 631 | 743 |
| Executed trades | 515 | 600 |
| Win rate | 71.46% | 67.17% |
| Net P&L (Rs) | 1,004,970.75 | 935,134.80 |
| Daily-close drawdown (Rs) | 28,166.32 | 29,325.35 |

V19 selected **50** of 255 top-two cases and executed **44**. V20 selected **57** and executed **50**. V20 newly selected **7** cases. The full-history net P&L change is Rs -69,835.95.

The report covers the stocks with complete minute data in the sealed G3 historical sources. It does not cover every NSE security. Some dates have a dated/research cohort that differs from complete minute coverage; those source limits are recorded in `source_provenance.json`. The previously chosen slot allowlist and this body threshold were both inspected using historical outcomes. Full-day extreme ranking cannot guarantee a causal entry or a profitable fill.

[Full audit CSV](top2_audit.csv) | [V19 trade ledger](v19_trades.csv) | [V20 trade ledger](v20_trades.csv) | [Daywise P&L](daily_comparison.csv)

## Daily top-two audit

### 29 Jul 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | KAYNES | +12.66% | 09:59 | 13:05 | Trade | Trade | - |
| LONG | 2 | KPITTECH | +8.63% | 09:31 | 13:40 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:00 |
| SHORT | 1 | PHOENIXLTD | -4.72% | 09:18 | 09:58 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:45 |
| SHORT | 2 | ADANIPORTS | -4.48% | 11:41 | 13:44 | Trade | Trade | - |

### 30 Jul 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SONACOMS | +6.71% | 09:52 | 15:12 | Missed | Missed | 0921_LONG (09:20): slot 5m volume <3.00, 1m body 0.12 <0.75, 1m volume <1.80 |
| LONG | 2 | SWIGGY | +4.50% | 14:57 | 15:03 | Missed | Missed | No native signal; closest 11:10: 5m volume |
| SHORT | 1 | KPITTECH | -4.89% | 10:21 | 14:19 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:55, 12:15, 13:15, 13:20 |
| SHORT | 2 | TIINDIA | -4.76% | 10:38 | 14:15 | Missed | Missed | 0936_SHORT (09:35): 1m body 0.21 <0.40, 1m wick >0.60, 1m volume <1.20 |

### 31 Jul 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | KEI | +5.99% | 09:49 | 10:57 | Missed | Missed | 1041_LONG (10:40): slot 5m volume <1.50 |
| LONG | 2 | KAYNES | +5.90% | 09:57 | 12:20 | Trade | Trade | - |
| SHORT | 1 | MANKIND | -6.99% | 09:16 | 11:25 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:10 |
| SHORT | 2 | INDUSTOWER | -5.95% | 10:16 | 10:19 | Missed | Missed | No native signal; closest 10:10: OI rise/range |

### 03 Aug 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | APLAPOLLO | +7.56% | 11:04 | 12:30 | Trade | Order only | - |
| LONG | 2 | LTM | +7.31% | 11:38 | 13:30 | Trade | Trade | - |
| SHORT | 1 | MUTHOOTFIN | -7.13% | 09:32 | 09:32 | Missed | Missed | No native signal; closest 09:35: OI rise/range |
| SHORT | 2 | GAIL | -5.46% | 09:17 | 10:13 | Missed | Missed | 1036_SHORT (10:35): slot 5m volume <1.00, 1m body 0.10 <0.30 |

### 04 Aug 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | GODFRYPHLP | +7.11% | 09:21 | 09:25 | Trade | Trade | - |
| LONG | 2 | KEI | +5.94% | 09:21 | 15:13 | Missed | Missed | 1036_LONG (10:35): 1m body 0.08 <0.50 |
| SHORT | 1 | UPL | -6.23% | 09:17 | 15:04 | Trade | Trade | - |
| SHORT | 2 | NESTLEIND | -4.90% | 13:37 | 13:48 | Missed | Missed | No native signal; closest 09:25: EMA stack, directional price |

### 05 Aug 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | PNBHOUSING | +7.83% | 09:16 | 10:09 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:40, 09:45, 12:20 |
| LONG | 2 | BOSCHLTD | +5.42% | 10:22 | 11:04 | Trade | Trade | - |
| SHORT | 1 | NYKAA | -5.83% | 09:18 | 10:41 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50%, 1m volume <1.20 |
| SHORT | 2 | SOLARINDS | -5.23% | 15:14 | 15:14 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:50 |

### 06 Aug 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MAZDOCK | +8.13% | 11:32 | 12:50 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:45 |
| LONG | 2 | GVT&D | +7.28% | 09:25 | 15:14 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| SHORT | 1 | BLUESTARCO | -6.56% | 14:30 | 15:10 | Missed | Missed | No native signal; closest 10:10: exact 1m continuation |
| SHORT | 2 | POLICYBZR | -5.20% | 13:32 | 14:54 | Missed | Missed | No native signal; closest 12:10: EMA stack |

### 07 Aug 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MOTHERSON | +9.43% | 09:16 | 14:32 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:00, 10:25, 11:00, 11:20 |
| LONG | 2 | AUROPHARMA | +5.21% | 10:41 | 15:11 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:30, 11:15, 12:45 |
| SHORT | 1 | CROMPTON | -8.04% | 09:17 | 14:55 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50% |
| SHORT | 2 | BLUESTARCO | -4.27% | 11:15 | 11:59 | Missed | Missed | 1121_SHORT (11:20): 1m volume <1.20 |

### 10 Aug 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | PAYTM | +10.62% | 09:40 | 15:11 | Trade | Trade | - |
| LONG | 2 | KAYNES | +8.80% | 09:18 | 09:39 | Missed | Missed | No native signal; closest 09:20: EMA stack, OI rise/range |
| SHORT | 1 | BHARATFORG | -9.77% | 13:57 | 14:51 | Missed | Missed | No native signal; closest 10:55: EMA stack, exact 1m continuation |
| SHORT | 2 | PFC | -7.01% | 10:38 | 15:11 | Trade | Trade | - |

### 11 Aug 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | POLICYBZR | +7.84% | 09:59 | 13:57 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:20, 12:50, 12:55, 13:00 |
| LONG | 2 | ZYDUSLIFE | +7.69% | 11:52 | 14:56 | Missed | Missed | 1036_LONG (10:35): EMA20 extension |
| SHORT | 1 | ADANIGREEN | -4.31% | 09:25 | 15:13 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| SHORT | 2 | POWERINDIA | -4.14% | 09:46 | 15:10 | Trade | Trade | - |

### 12 Aug 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | NATIONALUM | +5.95% | 09:16 | 09:31 | Missed | Missed | 1231_LONG (12:30): slot 5m volume <1.30, 1m body 0.50 <0.50, 1m wick >0.50 |
| LONG | 2 | IDEA | +5.88% | 09:50 | 09:57 | Missed | Missed | No native signal; closest 09:55: OI rise/range |
| SHORT | 1 | PIIND | -9.02% | 09:30 | 14:14 | Missed | Missed | 0951_SHORT (09:50): slot 5m volume <1.00 |
| SHORT | 2 | FORTIS | -6.00% | 09:38 | 15:01 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50%, 1m volume <1.20 |

### 13 Aug 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SOLARINDS | +8.26% | 10:18 | 15:07 | Missed | Missed | No native signal; closest 09:25: exact 1m continuation |
| LONG | 2 | ASTRAL | +6.74% | 09:17 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:50 |
| SHORT | 1 | FORCEMOT | -5.00% | 12:34 | 12:59 | Trade | Trade | - |
| SHORT | 2 | GMRAIRPORT | -4.25% | 09:17 | 12:34 | Missed | Missed | No native signal; closest 09:30: EMA stack |

### 14 Aug 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | JUBLFOOD | +5.18% | 09:19 | 09:30 | Missed | Missed | 0951_LONG (09:50): slot 5m volume <1.30, 1m volume <0.60 |
| LONG | 2 | IDEA | +4.88% | 10:11 | 15:13 | Trade | Trade | - |
| SHORT | 1 | SAIL | -5.51% | 10:34 | 10:36 | Missed | Missed | No native signal; closest 10:30: OI rise/range |
| SHORT | 2 | ALKEM | -4.73% | 14:12 | 14:20 | Missed | Missed | No native signal; closest 10:15: directional price |

### 17 Aug 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MOTILALOFS | +6.68% | 09:47 | 14:18 | Missed | Missed | 0926_LONG (09:25): slot OI <0.10%, 1m body 0.49 <0.60 |
| LONG | 2 | INOXWIND | +5.94% | 13:05 | 14:43 | Missed | Missed | Native signal only in disabled G3 slot(s): 13:00 |
| SHORT | 1 | VOLTAS | -5.49% | 09:31 | 15:13 | Missed | Missed | 1121_SHORT (11:20): 1m volume <1.20 |
| SHORT | 2 | SUNPHARMA | -3.32% | 15:02 | 15:15 | Missed | Missed | No native signal; closest 09:50: exact 1m continuation |

### 18 Aug 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | TIINDIA | +8.46% | 09:28 | 15:13 | Trade | Trade | - |
| LONG | 2 | IDEA | +4.84% | 13:18 | 14:31 | Missed | Missed | Native signal only in disabled G3 slot(s): 13:10 |
| SHORT | 1 | SBICARD | -4.37% | 09:59 | 15:15 | Missed | Missed | 0936_SHORT (09:35): slot price <0.33%, slot OI <0.50%, 1m body 0.29 <0.40, 1m volume <1.20 |
| SHORT | 2 | INOXWIND | -4.24% | 10:48 | 14:37 | Missed | Missed | No native signal; closest 09:25: EMA stack |

### 19 Aug 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | KAYNES | +5.01% | 10:00 | 15:13 | Missed | Missed | No native signal; closest 11:10: OI rise/range |
| LONG | 2 | PAYTM | +3.47% | 10:14 | 10:18 | Missed | Trade | 1016_LONG (10:15): 1m body 0.45 <0.50 |
| SHORT | 1 | POWERINDIA | -4.96% | 09:26 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:05 |
| SHORT | 2 | LTF | -4.36% | 09:26 | 11:12 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:00 |

### 20 Aug 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MOTILALOFS | +4.50% | 09:57 | 10:56 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:35, 10:55 |
| LONG | 2 | SBICARD | +4.40% | 10:10 | 11:03 | Missed | Missed | 0926_LONG (09:25): slot price <0.30%, 1m volume <1.10 |
| SHORT | 1 | BDL | -3.58% | 10:26 | 15:06 | Missed | Missed | No native signal; closest 10:10: OI rise/range |
| SHORT | 2 | RECLTD | -3.11% | 10:28 | 12:35 | Missed | Missed | No native signal; closest 09:30: OI rise/range |

### 21 Aug 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | POWERGRID | +4.29% | 10:49 | 11:09 | Missed | Missed | No native signal; closest 10:35: OI rise/range |
| LONG | 2 | CDSL | +4.15% | 11:51 | 12:12 | Missed | Missed | No native signal; closest 10:35: OI rise/range |
| SHORT | 1 | KEI | -3.64% | 11:59 | 15:10 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| SHORT | 2 | TIINDIA | -3.56% | 15:06 | 15:06 | Missed | Missed | 1121_SHORT (11:20): slot 5m volume <1.00, 1m volume <1.20 |

### 26 Aug 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SAGILITY | +6.47% | 09:21 | 09:24 | Missed | Missed | No native signal; closest 09:25: OI rise/range |
| LONG | 2 | SAIL | +5.67% | 14:35 | 15:11 | Missed | Missed | 1231_LONG (12:30): slot 5m volume <1.30 |
| SHORT | 1 | VBL | -6.18% | 09:20 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:45 |
| SHORT | 2 | PREMIERENE | -5.24% | 10:20 | 14:46 | Trade | Trade | - |

### 27 Aug 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | BHEL | +5.34% | 09:40 | 10:52 | Trade | Trade | - |
| LONG | 2 | KALYANKJIL | +4.15% | 13:23 | 14:28 | Trade | Trade | - |
| SHORT | 1 | MANAPPURAM | -5.31% | 10:29 | 14:45 | Trade | Trade | - |
| SHORT | 2 | MUTHOOTFIN | -4.36% | 12:44 | 13:39 | Missed | Missed | 1046_SHORT (10:45): 1m volume <0.80 |

### 28 Aug 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ATHERENERG | +6.98% | 09:16 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:15 |
| LONG | 2 | COFORGE | +5.30% | 09:32 | 13:59 | Trade | Trade | - |
| SHORT | 1 | TVSMOTOR | -5.63% | 11:30 | 12:32 | Trade | Trade | - |
| SHORT | 2 | HAVELLS | -4.56% | 13:59 | 14:09 | Missed | Missed | No native signal; closest 11:20: OI rise/range, directional price |

### 31 Aug 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SBICARD | +5.62% | 09:34 | 15:02 | Missed | Missed | No native signal; closest 09:20: EMA stack |
| LONG | 2 | ASTRAL | +4.01% | 14:54 | 15:13 | Missed | Missed | No native signal; closest 09:50: OI rise/range |
| SHORT | 1 | ADANIENSOL | -9.06% | 12:58 | 15:06 | Missed | Missed | 0926_SHORT (09:25): slot price <0.13%, 1m directional wick <0.20, 1m volume <1.20 |
| SHORT | 2 | KAYNES | -7.56% | 09:26 | 15:13 | Missed | Missed | 0926_SHORT (09:25): 1m directional wick <0.20 |

### 01 Sep 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ADANIGREEN | +4.20% | 09:23 | 10:54 | Missed | Missed | No native signal; closest 09:25: EMA stack, OI rise/range |
| LONG | 2 | MPHASIS | +4.14% | 11:56 | 13:47 | Missed | Missed | 1206_LONG (12:05): 1m body 0.47 <0.70, 1m wick >0.50 |
| SHORT | 1 | KALYANKJIL | -6.63% | 09:38 | 14:26 | Missed | Missed | 0931_SHORT (09:30): slot OI <0.12% |
| SHORT | 2 | NESTLEIND | -5.20% | 09:16 | 15:14 | Missed | Missed | No native signal; closest 12:10: OI rise/range, directional price |

### 02 Sep 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | JSWENERGY | +5.00% | 10:00 | 13:14 | Missed | Missed | 1041_LONG (10:40): 1m body 0.35 <0.50, 1m volume <0.60 |
| LONG | 2 | ADANIGREEN | +4.74% | 10:04 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:10 |
| SHORT | 1 | HEROMOTOCO | -5.66% | 09:48 | 11:48 | Missed | Missed | 0931_SHORT (09:30): 1m volume <1.20 |
| SHORT | 2 | ATHERENERG | -4.70% | 14:51 | 15:14 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:50, 13:25 |

### 03 Sep 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MAHABANK | +5.05% | 09:38 | 13:20 | Missed | Missed | 1016_LONG (10:15): slot 5m volume <1.30 |
| LONG | 2 | SOLARINDS | +4.89% | 09:17 | 11:06 | Missed | Missed | 0926_LONG (09:25): 1m volume <1.10 |
| SHORT | 1 | MCX | -4.34% | 09:25 | 13:24 | Missed | Trade | 1016_SHORT (10:15): 1m body 0.38 <0.40 |
| SHORT | 2 | APLAPOLLO | -4.19% | 10:45 | 13:57 | Order only | Order only | SOURCE_UNFILLED |

### 04 Sep 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | APLAPOLLO | +5.87% | 13:38 | 15:12 | Missed | Missed | 1216_LONG (12:15): slot 5m volume <1.30, 1m body 0.05 <0.50, 1m wick >0.50 |
| LONG | 2 | SWIGGY | +5.61% | 09:39 | 14:42 | Missed | Missed | 0926_LONG (09:25): 1m body 0.23 <0.60, 1m wick >0.60, 1m volume <1.10 |
| SHORT | 1 | ATHERENERG | -5.05% | 10:32 | 13:34 | Trade | Trade | - |
| SHORT | 2 | SRF | -3.76% | 09:26 | 09:27 | Missed | Missed | No native signal; closest 09:25: 09:25 Nifty short gate |

### 07 Sep 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ZYDUSLIFE | +4.38% | 11:26 | 11:33 | Missed | Missed | No native signal; closest 09:20: EMA stack, OI rise/range |
| LONG | 2 | KAYNES | +4.07% | 09:59 | 10:06 | Missed | Missed | No native signal; closest 09:20: OI rise/range |
| SHORT | 1 | SAIL | -4.92% | 09:24 | 13:14 | Missed | Missed | No native signal; closest 09:25: OI rise/range |
| SHORT | 2 | GODREJPROP | -4.59% | 11:14 | 11:21 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:10, 11:15 |

### 08 Sep 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | KALYANKJIL | +4.72% | 11:33 | 15:14 | Trade | Trade | - |
| LONG | 2 | LAURUSLABS | +4.34% | 11:15 | 14:50 | Missed | Missed | 0926_LONG (09:25): slot price <0.30%, slot OI <0.10%, 1m volume <1.10 |
| SHORT | 1 | BLUESTARCO | -3.21% | 09:43 | 09:43 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50% |
| SHORT | 2 | BPCL | -3.12% | 13:52 | 13:54 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:00 |

### 09 Sep 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ADANIENT | +6.15% | 09:44 | 10:52 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:20, 10:25 |
| LONG | 2 | PAYTM | +4.88% | 09:27 | 14:28 | Missed | Missed | 0921_LONG (09:20): 1m volume <1.80 |
| SHORT | 1 | HDFCLIFE | -4.64% | 10:00 | 12:35 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:55, 12:20 |
| SHORT | 2 | LODHA | -3.53% | 14:42 | 15:12 | Missed | Missed | No native signal; closest 09:50: OI rise/range, 5m volume |

### 10 Sep 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | FORCEMOT | +5.28% | 09:56 | 14:01 | Trade | Trade | - |
| LONG | 2 | MANAPPURAM | +4.92% | 12:46 | 15:15 | Missed | Missed | No native signal; closest 09:50: OI rise/range |
| SHORT | 1 | IDEA | -5.61% | 10:50 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:05 |
| SHORT | 2 | CAMS | -5.25% | 09:16 | 09:16 | Missed | Missed | No native signal; closest 09:30: directional price |

### 11 Sep 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | YESBANK | +7.74% | 12:50 | 13:50 | Missed | Missed | No native signal; closest 11:10: OI rise/range |
| LONG | 2 | PAYTM | +5.44% | 10:05 | 15:13 | Trade | Trade | - |
| SHORT | 1 | COCHINSHIP | -9.14% | 09:17 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:40, 10:40, 12:40 |
| SHORT | 2 | GODREJPROP | -7.33% | 09:19 | 13:57 | Trade | Trade | - |

### 15 Sep 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MPHASIS | +4.59% | 09:16 | 09:30 | Missed | Missed | 1046_LONG (10:45): slot 5m volume <1.50, 1m body 0.36 <0.50 |
| LONG | 2 | KPITTECH | +3.74% | 09:35 | 10:42 | Missed | Missed | 1216_LONG (12:15): 1m volume <1.00 |
| SHORT | 1 | SOLARINDS | -14.58% | 09:18 | 15:13 | Missed | Missed | 1056_SHORT (10:55): 1m volume <0.80 |
| SHORT | 2 | GVT&D | -7.27% | 10:15 | 15:05 | Missed | Missed | 0951_SHORT (09:50): 1m volume <1.20 |

### 16 Sep 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | PATANJALI | +8.36% | 10:17 | 14:07 | Missed | Missed | 1331_LONG (13:30): slot 5m volume <1.40, 1m body 0.36 <0.55 |
| LONG | 2 | POLICYBZR | +6.16% | 12:38 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 13:15 |
| SHORT | 1 | PREMIERENE | -5.56% | 09:33 | 12:52 | Missed | Missed | No native signal; closest 09:30: OI rise/range |
| SHORT | 2 | PAYTM | -5.40% | 09:18 | 09:46 | Missed | Missed | No native signal; closest 09:30: EMA stack, OI rise/range |

### 17 Sep 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | TMPV | +6.25% | 14:40 | 14:52 | Missed | Missed | No native signal; closest 09:25: EMA stack, exact 1m continuation |
| LONG | 2 | MFSL | +6.04% | 10:02 | 12:58 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:25, 11:45 |
| SHORT | 1 | POLICYBZR | -6.57% | 09:18 | 09:36 | Missed | Missed | 1016_SHORT (10:15): 1m volume <1.20 |
| SHORT | 2 | MAZDOCK | -3.25% | 13:39 | 13:40 | Missed | Missed | Native signal only in disabled G3 slot(s): 13:30 |

### 18 Sep 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ADANIGREEN | +5.00% | 12:55 | 13:26 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:45 |
| LONG | 2 | RVNL | +4.34% | 13:02 | 15:00 | Missed | Missed | No native signal; closest 09:20: OI rise/range, exact 1m continuation |
| SHORT | 1 | KPITTECH | -3.60% | 14:04 | 14:32 | Missed | Missed | 0951_SHORT (09:50): slot price <0.13% |
| SHORT | 2 | TCS | -3.49% | 13:17 | 15:06 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:00, 12:20, 13:15 |

### 21 Sep 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | PATANJALI | +8.52% | 09:18 | 13:58 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:30, 09:35, 09:40, 10:10, 11:05, 13:10 |
| LONG | 2 | MANKIND | +6.02% | 09:41 | 12:21 | Missed | Missed | 1141_LONG (11:40): slot 5m volume <1.30 |
| SHORT | 1 | OFSS | -7.53% | 09:30 | 15:10 | Missed | Trade | 0931_SHORT (09:30): 1m body 0.35 <0.40 |
| SHORT | 2 | BSE | -5.31% | 12:22 | 13:51 | Missed | Missed | 1031_SHORT (10:30): 1m volume <1.20 |

### 22 Sep 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | RBLBANK | +4.97% | 13:41 | 15:13 | Trade | Trade | - |
| LONG | 2 | PATANJALI | +4.77% | 09:42 | 10:08 | Trade | Trade | - |
| SHORT | 1 | KAYNES | -4.83% | 09:16 | 14:11 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:25, 12:45, 13:15, 13:20 |
| SHORT | 2 | ATHERENERG | -4.71% | 11:26 | 15:13 | Trade | Trade | - |

### 23 Sep 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SAIL | +6.42% | 10:36 | 13:33 | Trade | Trade | - |
| LONG | 2 | POLICYBZR | +5.83% | 13:15 | 14:54 | Trade | Trade | - |
| SHORT | 1 | INOXWIND | -4.41% | 10:27 | 15:06 | Missed | Missed | No native signal; closest 09:30: OI rise/range |
| SHORT | 2 | ATHERENERG | -3.91% | 09:23 | 10:08 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:30, 12:00, 12:40 |

### 24 Sep 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ICICIGI | +7.61% | 09:25 | 13:20 | Missed | Missed | 0951_LONG (09:50): 1m body 0.13 <0.50 |
| LONG | 2 | CHOLAFIN | +5.03% | 09:24 | 10:05 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| SHORT | 1 | POLICYBZR | -26.69% | 09:31 | 14:53 | Missed | Missed | No native signal; closest 09:35: exact 1m continuation |
| SHORT | 2 | BAJAJHLDNG | -4.32% | 14:21 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 12:15 |

### 25 Sep 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | POLICYBZR | +8.77% | 09:16 | 09:22 | Missed | Missed | No native signal; closest 09:20: EMA stack, OI rise/range |
| LONG | 2 | MFSL | +5.31% | 09:17 | 11:20 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:45 |
| SHORT | 1 | FORTIS | -5.26% | 10:01 | 15:09 | Trade | Trade | - |
| SHORT | 2 | MCX | -4.43% | 10:48 | 15:13 | Missed | Missed | 1301_SHORT (13:00): 1m volume <0.80 |

### 28 Sep 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | IREDA | +3.65% | 12:27 | 12:35 | Missed | Missed | No native signal; closest 12:30: OI rise/range |
| LONG | 2 | DIXON | +3.31% | 12:37 | 12:43 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| SHORT | 1 | BANKINDIA | -5.96% | 10:18 | 15:13 | Trade | Trade | - |
| SHORT | 2 | YESBANK | -5.67% | 09:22 | 15:00 | Missed | Missed | 0926_SHORT (09:25): 1m body 0.33 <0.40, 1m directional wick <0.20 |

### 29 Sep 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MANKIND | +5.01% | 11:05 | 14:36 | Order only | Order only | SOURCE_UNFILLED |
| LONG | 2 | PNBHOUSING | +4.17% | 11:13 | 11:33 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:25 |
| SHORT | 1 | POLICYBZR | -7.32% | 09:23 | 10:16 | Missed | Missed | No native signal; closest 09:25: OI rise/range |
| SHORT | 2 | PREMIERENE | -5.78% | 12:28 | 15:08 | Missed | Missed | 1036_SHORT (10:35): slot 5m volume <1.00, EMA20 extension |

### 30 Sep 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | RVNL | +5.51% | 11:43 | 11:58 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:20, 11:45, 11:55 |
| LONG | 2 | PRESTIGE | +4.77% | 12:24 | 14:07 | Missed | Missed | No native signal; closest 10:45: exact 1m continuation |
| SHORT | 1 | FORTIS | -7.29% | 09:38 | 14:44 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50% |
| SHORT | 2 | APOLLOHOSP | -6.16% | 09:34 | 15:13 | Missed | Missed | 1031_SHORT (10:30): 1m body 0.22 <0.40, 1m wick >0.60, 1m volume <1.20 |

### 05 Oct 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | KALYANKJIL | +5.62% | 09:43 | 14:28 | Missed | Missed | 0951_LONG (09:50): 1m body 0.38 <0.50, 1m wick >0.50 |
| LONG | 2 | ETERNAL | +4.50% | 10:58 | 13:46 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:00 |
| SHORT | 1 | DMART | -6.91% | 09:17 | 15:13 | Missed | Missed | 1011_SHORT (10:10): 1m volume <1.20 |
| SHORT | 2 | UPL | -4.70% | 12:40 | 14:09 | Trade | Trade | - |

### 06 Oct 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ATHERENERG | +10.09% | 15:13 | 15:13 | Missed | Missed | No native signal; closest 09:20: exact 1m continuation |
| LONG | 2 | PGEL | +9.08% | 10:00 | 14:18 | Missed | Missed | 0956_LONG (09:55): 1m body 0.14 <0.40, 1m wick >0.60, 1m volume <1.10 |
| SHORT | 1 | JINDALSTEL | -5.06% | 09:18 | 09:21 | Missed | Missed | No native signal; closest 09:30: OI rise/range |
| SHORT | 2 | PHOENIXLTD | -4.57% | 11:17 | 12:57 | Trade | Trade | - |

### 07 Oct 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | KALYANKJIL | +6.17% | 09:31 | 12:24 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:20 |
| LONG | 2 | PAYTM | +5.19% | 09:28 | 10:21 | Missed | Missed | 0951_LONG (09:50): 1m body 0.17 <0.50 |
| SHORT | 1 | NATIONALUM | -4.80% | 09:38 | 15:13 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50% |
| SHORT | 2 | TIINDIA | -4.65% | 11:33 | 13:26 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:45, 13:05 |

### 08 Oct 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | LICHSGFIN | +5.34% | 09:26 | 09:37 | Missed | Trade | 0926_LONG (09:25): 1m body 0.39 <0.60 |
| SHORT | 1 | ADANIGREEN | -10.03% | 09:45 | 12:40 | Trade | Trade | - |
| SHORT | 2 | ADANIENT | -7.04% | 09:45 | 14:05 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:00 |

### 09 Oct 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | COLPAL | +7.64% | 09:17 | 10:15 | Missed | Trade | 0951_LONG (09:50): 1m body 0.31 <0.50 |
| LONG | 2 | INOXWIND | +6.29% | 10:12 | 15:13 | Missed | Missed | No native signal; closest 09:50: EMA stack, OI rise/range |
| SHORT | 1 | SIEMENS | -4.53% | 09:21 | 09:33 | Missed | Missed | 0936_SHORT (09:35): slot OI <0.50%, 1m volume <1.20 |
| SHORT | 2 | ENRIN | -4.39% | 09:20 | 09:42 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:40 |

### 12 Oct 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | UPL | +6.18% | 13:19 | 14:50 | Missed | Missed | 1216_LONG (12:15): 1m volume <1.00 |
| LONG | 2 | DELHIVERY | +5.87% | 09:26 | 10:39 | Trade | Trade | - |
| SHORT | 1 | ITC | -3.84% | 11:23 | 14:01 | Missed | Missed | Native signal only in disabled G3 slot(s): 11:15 |
| SHORT | 2 | TECHM | -3.70% | 09:22 | 09:27 | Missed | Missed | No native signal; closest 09:50: directional price |

### 13 Oct 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | BANDHANBNK | +6.79% | 09:17 | 15:13 | Missed | Missed | No native signal; closest 09:20: OI rise/range |
| LONG | 2 | SHRIRAMFIN | +6.63% | 14:04 | 14:51 | Missed | Missed | No native signal; closest 09:25: EMA stack, directional price |
| SHORT | 1 | PAYTM | -6.07% | 12:42 | 13:19 | Trade | Trade | - |
| SHORT | 2 | RBLBANK | -4.65% | 09:17 | 13:08 | Missed | Missed | 0936_SHORT (09:35): 1m volume <1.20 |

### 14 Oct 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | TATAELXSI | +7.01% | 09:16 | 09:47 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:45 |
| LONG | 2 | JSWENERGY | +6.36% | 09:37 | 13:48 | Missed | Missed | 1231_LONG (12:30): slot 5m volume <1.30, 1m body 0.27 <0.50 |
| SHORT | 1 | LICHSGFIN | -5.03% | 09:23 | 15:13 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:55 |
| SHORT | 2 | POLYCAB | -4.74% | 09:21 | 10:08 | Missed | Missed | No native signal; closest 09:25: OI rise/range |

### 15 Oct 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | HDFCAMC | +5.57% | 13:36 | 14:44 | Missed | Missed | Native signal only in disabled G3 slot(s): 10:55 |
| LONG | 2 | HINDPETRO | +5.03% | 11:04 | 13:02 | Order only | Order only | SOURCE_UNFILLED |
| SHORT | 1 | YESBANK | -20.19% | 14:05 | 15:03 | Missed | Missed | No native signal; closest 09:30: OI rise/range |
| SHORT | 2 | NESTLEIND | -8.66% | 12:26 | 12:29 | Missed | Missed | No native signal; closest 13:00: 5m volume |

### 16 Oct 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | WIPRO | +9.10% | 09:18 | 15:13 | Missed | Trade | 0926_LONG (09:25): 1m body 0.56 <0.60 |
| LONG | 2 | KPITTECH | +5.51% | 09:40 | 15:10 | Missed | Missed | No native signal; closest 11:35: 5m volume |
| SHORT | 1 | LTF | -6.63% | 09:16 | 09:32 | Missed | Missed | No native signal; closest 09:35: directional price |
| SHORT | 2 | CROMPTON | -5.94% | 13:42 | 13:58 | Missed | Missed | No native signal; closest 13:00: OI rise/range |

### 19 Oct 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | FEDERALBNK | +6.12% | 10:45 | 12:03 | Trade | Trade | - |
| LONG | 2 | UNIONBANK | +5.84% | 12:05 | 14:46 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:40, 10:55 |
| SHORT | 1 | CROMPTON | -5.40% | 09:30 | 14:27 | Missed | Missed | No native signal; closest 10:10: 5m volume, directional price |
| SHORT | 2 | UJJIVANSFB | -5.00% | 09:20 | 09:27 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:20 |

### 21 Oct 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SBILIFE | +3.66% | 09:35 | 11:06 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| LONG | 2 | KALYANKJIL | +3.21% | 09:35 | 10:03 | Missed | Missed | 0926_LONG (09:25): 1m body 0.32 <0.60, 1m wick >0.60 |
| SHORT | 1 | VMM | -8.76% | 13:03 | 13:08 | Order only | Order only | INSUFFICIENT_PORTFOLIO_CAPITAL |
| SHORT | 2 | ABB | -7.50% | 09:16 | 13:42 | Trade | Trade | - |

### 22 Oct 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | BAJAJHLDNG | +4.96% | 09:40 | 09:57 | Trade | Trade | - |
| LONG | 2 | KAYNES | +4.84% | 09:49 | 15:04 | Missed | Missed | No native signal; closest 09:50: OI rise/range |
| SHORT | 1 | KEI | -8.66% | 09:16 | 10:17 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:40, 10:00 |
| SHORT | 2 | SWIGGY | -5.80% | 09:17 | 15:06 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:20, 10:40, 11:45 |

### 23 Oct 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ETERNAL | +3.89% | 13:04 | 15:13 | Missed | Missed | No native signal; closest 09:20: EMA stack, exact 1m continuation |
| LONG | 2 | SHREECEM | +3.66% | 13:40 | 13:44 | Missed | Missed | No native signal; closest 09:25: EMA stack |
| SHORT | 1 | SWIGGY | -14.64% | 09:17 | 10:50 | Missed | Missed | 1211_SHORT (12:10): slot 5m volume <1.00 |
| SHORT | 2 | INFY | -12.55% | 10:36 | 10:39 | Missed | Missed | No native signal; closest 09:35: EMA stack |

### 26 Oct 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ATHERENERG | +18.85% | 12:33 | 14:55 | Missed | Missed | 1231_LONG (12:30): slot 5m volume <1.30, 1m body 0.35 <0.50 |
| LONG | 2 | SWIGGY | +8.84% | 09:34 | 13:12 | Missed | Missed | No native signal; closest 09:20: EMA stack |
| SHORT | 1 | UJJIVANSFB | -5.49% | 09:19 | 09:26 | Missed | Missed | No native signal; closest 09:25: exact 1m continuation |
| SHORT | 2 | IDFCFIRSTB | -4.62% | 09:26 | 09:42 | Missed | Missed | 0951_SHORT (09:50): 1m volume <1.20 |

### 27 Oct 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | PGEL | +7.68% | 10:11 | 12:09 | Missed | Trade | 0926_LONG (09:25): 1m body 0.37 <0.60 |
| LONG | 2 | KAYNES | +7.66% | 13:55 | 13:55 | Missed | Missed | No native signal; closest 09:25: OI rise/range, directional price |
| SHORT | 1 | MAZDOCK | -10.71% | 14:47 | 14:49 | Missed | Missed | No native signal; closest 09:25: EMA stack, OI rise/range, directional price |
| SHORT | 2 | AMBUJACEM | -4.90% | 10:21 | 14:24 | Missed | Missed | No native signal; closest 09:25: EMA stack, OI rise/range |

### 28 Oct 2026 (Wed)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | PATANJALI | +10.44% | 14:26 | 15:15 | Missed | Missed | No native signal; closest 12:15: EMA stack |
| LONG | 2 | KPITTECH | +8.95% | 12:56 | 13:21 | Missed | Missed | No native signal; closest 09:20: EMA stack, directional price |
| SHORT | 1 | KAYNES | -6.04% | 09:42 | 15:13 | Missed | Missed | 1031_SHORT (10:30): slot 5m volume <1.00 |
| SHORT | 2 | NAUKRI | -5.76% | 14:01 | 14:09 | Missed | Missed | No native signal; closest 09:30: EMA stack, exact 1m continuation |

### 29 Oct 2026 (Thu)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | SYRMA | +7.80% | 09:24 | 13:04 | Missed | Missed | Native signal only in disabled G3 slot(s): 13:00 |
| LONG | 2 | ADANIENT | +5.61% | 10:47 | 15:15 | Missed | Missed | Native signal only in disabled G3 slot(s): 09:30 |
| SHORT | 1 | PNBHOUSING | -6.79% | 09:18 | 11:56 | Order only | Order only | INSUFFICIENT_PORTFOLIO_CAPITAL |
| SHORT | 2 | HEROMOTOCO | -4.43% | 11:09 | 14:58 | Trade | Trade | - |

### 30 Oct 2026 (Fri)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | ATHERENERG | +7.18% | 11:45 | 15:13 | Missed | Missed | 0921_LONG (09:20): slot OI <0.10%, 1m body 0.17 <0.75, 1m volume <1.80 |
| LONG | 2 | LAURUSLABS | +6.29% | 09:21 | 14:24 | Missed | Missed | 0921_LONG (09:20): 1m volume <1.80 |
| SHORT | 1 | SUZLON | -8.58% | 09:19 | 15:13 | Trade | Trade | - |
| SHORT | 2 | INOXWIND | -7.08% | 09:27 | 15:13 | Order only | Order only | SOURCE_UNFILLED |

### 02 Nov 2026 (Mon)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | INOXWIND | +12.83% | 09:21 | 11:31 | Missed | Missed | 1136_LONG (11:35): 1m volume <1.00 |
| LONG | 2 | DIXON | +12.73% | 09:16 | 12:29 | Missed | Missed | 1231_LONG (12:30): slot 5m volume <1.30, 1m body 0.26 <0.50, 1m wick >0.50 |
| SHORT | 1 | POLICYBZR | -9.03% | 10:05 | 15:14 | Trade | Trade | - |
| SHORT | 2 | SYRMA | -7.46% | 09:17 | 13:16 | Missed | Missed | No native signal; closest 09:30: OI rise/range |

### 03 Nov 2026 (Tue)

| Side | Rank | Stock | Move | First 3% | Extreme | V19 | V20 | V19 reason if missed/unfilled |
|:---|---:|:---|---:|:---:|:---:|:---|:---|:---|
| LONG | 1 | MCX | +7.87% | 12:10 | 13:42 | Trade | Trade | - |
| LONG | 2 | ANGELONE | +7.63% | 09:39 | 13:56 | Missed | Missed | 1036_LONG (10:35): 1m volume <0.60 |
| SHORT | 1 | LAURUSLABS | -4.16% | 10:11 | 11:39 | Missed | Missed | 1016_SHORT (10:15): slot 5m volume <1.00 |
| SHORT | 2 | NATIONALUM | -4.04% | 11:57 | 12:34 | Missed | Missed | No native signal; closest 09:30: EMA stack |
