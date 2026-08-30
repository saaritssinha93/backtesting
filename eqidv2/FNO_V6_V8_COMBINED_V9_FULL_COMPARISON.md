# FNO V6 vs V8-Combined vs V9-Honest — full evidence comparison

Generated 2026-08-21 IST.

## Executive answer

- **V6 has the strongest historical profitability diagnostic** on the common
  40-session window: PF 2.009 and +72.04 percentage points at 15 bps, versus
  V8-Combined PF 1.892 and +60.16 points.
- **V8-Combined has the better risk/final-segment diagnostic** on that window: lower
  drawdown (6.00 versus 7.10 points), more fills (184 versus 176), and higher
  final-10-session PF (1.494 versus 1.356).
- **V9-Honest has the most fail-closed outcome reporting, not a newly superior
  strategy or a flawless protocol.** None of the four proposed 09:50/09:55 legs
  passed TRAIN, so all four remain disabled. The active V9 book is exactly the
  ten-leg V8-Combined book. Its disclosed stability-block implementation
  discrepancy still requires correction before another selection run.
- An actual V9 replay on the same 40-session, 208-symbol, 15 bps comparison
  contract reproduced V8-Combined exactly: 890 candidates, 184 fills,
  93 wins / 91 losses, PF 1.892, +60.162 points and 6.001 points drawdown.
- On V9's separate 35-symbol, 46-session panel, the V9 launcher and an
  independent V8-Combined replay are exactly equal: 57 fills, 49.12% wins,
  diagnostic PF 1.485, +10.852 points, and 7.919 points drawdown. That equality
  is expected because V9 enabled no new leg.
- **No version is ready for live production.** V6's legacy full-history reports
  are invalidated by cross-session path leakage; V8-Combined and V9 correctly
  suppress official headline performance because their source contracts are
  incomplete.

The practical conclusion is: retain V6 only as a profitability benchmark;
continue V8-Combined through the V9 launcher in paper/shadow mode if further
evidence is desired; do not enable 09:50 or 09:55.

## How to read the numbers

`Net points` is the additive sum of independent trade returns in percentage
points. It is not compounded account return. `Proxy P&L` sizes each filled cash
equity position at approximately Rs50,000; it is not futures-lot P&L and is not
a capital-constrained portfolio return. PF is gross positive return divided by
absolute gross negative return.

There are three distinct evidence sets below. They must not be merged:

1. a common 40-session V6 versus V8-Combined diagnostic, with an actual
   same-input V9 active-book parity replay;
2. V6's longer 57-session corrected evidence, explicitly split between its
   older published aggregate and a later diagnostic reconstruction, plus its
   separately invalidated legacy reports; and
3. V9's 35-symbol, 46-session parity replay plus its TRAIN-only 09:50/09:55
   optimization.

## 1. Common 40-session comparison: V6 versus V8-Combined

### Comparison contract

| Item | Contract |
|---|---|
| Sessions | 40 official NSE F&O sessions, 2026-06-24 through 2026-08-19 |
| TRAIN | 30 sessions, 2026-06-24 through 2026-08-05 |
| Final replay segment | 10 sessions, 2026-08-06 through 2026-08-19 |
| Universe | Fixed later-dated mapped universe of 208 stocks |
| Frozen source fingerprint | `6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc` |
| OI source | Static August futures; not a historically rolling near-month contract |
| Execution prices | NSE cash-equity one-minute bars |
| 15 bps comparison treatment | V6 is deterministically re-costed from 5 to 15 bps without changing its selected fills; V8-Combined natively runs at 15 bps cost and 0 bps extra slippage |
| Important non-parity | V6 keeps legacy exact-trigger/longer-lived independent orders; V8 models finite windows, adverse gaps and a global ledger |

The same dates, source snapshot and displayed 15 bps cost make this the fairest
available comparison, but only V8-Combined is a native 15 bps engine replay.
V6's row is a cost sensitivity on its already selected legacy fills: the extra
10 bps changes net outcomes and classifications but does not rerun selection,
entry, expiry or fill mechanics. Cost equalization therefore does not equalize
execution. This is a diagnostic comparison, not exact implementation parity.

### Overall result at 15 bps

| Metric | V6 strict | V8-Combined | Better diagnostic |
|---|---:|---:|---|
| Candidates | Not retained as a comparable count | 890 | Not comparable |
| Closed fills | 176 | **184** | V8-Combined |
| Fills/session | 4.40 | **4.60** | V8-Combined |
| Wins / losses | 90 / 86 | 93 / 91 | Different fill counts |
| Win rate | **51.14%** | 50.54% | V6 |
| PF | **2.009** | 1.892 | V6 |
| Net points | **+72.04** | +60.16 | V6 |
| Sizing-proxy net P&L | **+Rs36,233.84** | +Rs29,890.12 | V6 |
| Maximum cumulative daily drawdown | 7.101 points | **6.001 points** | V8-Combined |
| Positive / negative / flat days | 24 / 15 / 1 | **28 / 12 / 0** | V8-Combined |
| Official promotion-grade headline | No | No | Neither |

Exact V6 values before display rounding are PF 2.009307, +72.038992 points,
Rs36,233.84 proxy P&L, and 7.100871 points drawdown. Exact V8-Combined values
are PF 1.891554, +60.1622 points, Rs29,890.12 proxy P&L, and 6.0012 points
drawdown.

### Actual V9 parity replay on the same 40-session contract

After the comparison was assembled, V9-Honest was independently run on the
same dates, 208 symbols, frozen source, 15 bps cost, zero extra slippage,
split date and last-real-bar policy as V8-Combined. The complete audit matched:

| Metric | V8-Combined | V9-Honest active book |
|---|---:|---:|
| Candidates | 890 | 890 |
| Closed fills | 184 | 184 |
| Wins / losses | 93 / 91 | 93 / 91 |
| Win rate | 50.54% | 50.54% |
| PF | 1.891554 | 1.891554 |
| Net points | +60.1622 | +60.1622 |
| Proxy P&L | +Rs29,890.12 | +Rs29,890.12 |
| Max DD | 6.0012 | 6.0012 |
| TRAIN | 153 fills, PF 1.982878, +53.9330 | Same |
| Final 10 | 31 fills, PF 1.494080, +6.2291 | Same |

This is behavioral parity, not a new V9 performance gain: the active setup
payloads are byte-for-byte identical under setup-book SHA-256
`ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675`,
and all four proposed 09:50/09:55 legs remain disabled. A row-level audit found
the same 890 candidate IDs and no difference in any common behavioral field;
only the expected launcher identity fields differ. Both rows retain the same
incomplete-source and last-real-bar limitations, so their official headline
fields are `N/A`.

### TRAIN versus final 10 sessions at 15 bps

| Strategy | TRAIN fills | TRAIN W-L | TRAIN PF | TRAIN net | Final-10 fills | Final-10 W-L | Final-10 PF | Final-10 net |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| V6 | 149 | 79-70 | **2.159** | **+67.31** | 27 | 11-16 | 1.356 | +4.73 |
| V8-Combined | **153** | 79-74 | 1.983 | +53.93 | **31** | 14-17 | **1.494** | **+6.23** |

The final segment is only ten sessions, and both strategies had already been
developed after observing related history. It is a stability diagnostic, not a
clean untouched production test. V8-Combined's 1.494 is also fractionally below
the requested PF 1.50 floor.

### Common 5 bps sensitivity

| Metric | V6 | V8-Combined | Better diagnostic |
|---|---:|---:|---|
| Closed fills | 176 | **184** | V8-Combined |
| Wins / losses | 93 / 83 | 96 / 88 | — |
| Win rate | **52.84%** | 52.17% | V6 |
| PF | **2.425** | 2.341 | V6 |
| Net points | **+89.64** | +78.56 | V6 |
| Sizing-proxy net P&L | **+Rs44,768** | +Rs38,788 | V6 |
| Maximum cumulative daily drawdown | 5.60 | **4.02** | V8-Combined |
| TRAIN PF | **2.606** | 2.455 | V6 |
| Final-10 PF | 1.633 | **1.848** | V8-Combined |

V6 wins full-window PF and net at both cost levels. V8-Combined trades slightly
more often, has lower drawdown, and has the stronger last-ten-session PF.
At 5 bps, V8-Combined's TRAIN segment was 81-72 (52.94% wins), while its final
segment was 15-16 (48.39% wins).

## 2. Full V6 evidence

### What V6 does

V6 first applies its five-minute price, futures-OI and cash-volume filters. For
the exact `S+1` one-minute confirmation candle it then requires:

- LONG: bullish candle, close above the five-minute signal close, setup-specific
  minimum body ratio and maximum adverse upper wick;
- SHORT: bearish candle, close below the five-minute signal close,
  setup-specific minimum body ratio and maximum adverse lower wick; and
- a later one-minute candle to break the confirmation high for LONG or low for
  SHORT; the confirmation candle cannot fill its own order.

The legacy execution assumes exact trigger-price fills, no finite entry expiry,
stop-first handling when both brackets touch in one bar, independent trades and
no shared capital ledger. Those assumptions are less conservative than V8's.

### V6 per-leg result on the common 40-session window at 15 bps

| Leg | Fills | Wins | Win % | PF | Net points |
|---|---:|---:|---:|---:|---:|
| 09:25 LONG | 18 | 8 | 44.44% | 2.463 | +8.808 |
| 09:25 SHORT | 39 | 22 | 56.41% | 2.171 | +17.917 |
| 09:30 LONG | 11 | 6 | 54.55% | 3.117 | +9.152 |
| 09:30 SHORT | 22 | 11 | 50.00% | 1.553 | +5.363 |
| 09:35 LONG | 19 | 9 | 47.37% | 1.053 | +0.507 |
| 09:35 SHORT | 10 | 8 | 80.00% | 7.111 | +14.055 |
| 09:40 LONG | 22 | 8 | 36.36% | 1.676 | +5.663 |
| 09:40 SHORT | 19 | 9 | 47.37% | 1.422 | +4.500 |
| 09:45 LONG | 7 | 3 | 42.86% | 1.392 | +1.366 |
| 09:45 SHORT | 9 | 6 | 66.67% | 4.046 | +4.709 |

The V6 total at 15 bps is 176 fills, 90 wins and 86 losses. As with V8,
several attractive leg PF values are based on only 7-10 fills and should not be
treated as independently validated edges.

### V6 LONG/SHORT result on the common 40-session window at 15 bps

| Side | Fills | W-L | Win % | PF | Net points | Proxy P&L | Max DD |
|---|---:|---:|---:|---:|---:|---:|---:|
| LONG | 77 | 34-43 | 44.16% | 1.800 | +25.495 | +Rs12,537.07 | 5.222 |
| SHORT | 99 | 56-43 | 56.57% | 2.178 | +46.544 | +Rs23,696.76 | 5.281 |
| **Total** | **176** | **90-86** | **51.14%** | **2.009** | **+72.039** | **+Rs36,233.84** | **7.101** |

The side drawdowns are calculated independently; the total drawdown comes
from the combined daily sequence and is therefore not the sum or maximum of
the two side-only values.

### Corrected same-session 57-session evidence: two distinct source lineages

This is the longest corrected same-session V6 evidence currently documented,
but it is **not one unified immutable run**. Two source lineages must be kept
separate:

1. The originally documented aggregate replay removed overnight leakage and
   used the older frozen V7 physical snapshot at
   `C:\TradingData\eqidv2\fno_oi\strategy_research\_source_snapshots_v7_high_low_breakout_v1\snapshot_20260818T145532763230+0530_4cdqg0jp\manifest.json`,
   fingerprint
   `4be7500f183d8bf9b23c23d43d13a976ffd645f5d2ad982fc4b48e64c8007bd7`.
2. The detailed win/loss and rupee fields were reconstructed later from the V8
   snapshot at
   `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\snapshots\snapshot_20260820T124734626995+0530_mnofor_c\manifest.json`,
   fingerprint
   `6734204d53315d386a2c3949f01b272d4399a8a1d3b44b9cfd556a1b859190cc`,
   with corrected broad-cache input fingerprint
   `3feaa392006b3aa493bd12bfb0d7dcdfec93ff19643b0e61c9b1a7c8a945c079`.

### Published aggregate and later reconstruction check

| Aggregate | Older `4be7500f...` published replay | Later `6734204d...` reconstruction | Relationship |
|---|---:|---:|---|
| Window | 2026-05-27 through 2026-08-17 | Same | Same displayed dates |
| Sessions | 57 | 57 | Exact aggregate match |
| Five-minute candidates | 1,347 | 1,347 | Exact aggregate match |
| Orders | 222 | 222 | Exact aggregate match |
| Closed fills | 221 | 221 | Exact aggregate match |
| Fills/session | 3.88 | 3.88 | Derived from matching totals |
| Fill rate | 99.5% | 99.5% | Derived from matching totals |
| Cost | 5 bps round trip | 5 bps round trip | Same displayed economics |
| Trade PF | 2.171 | 2.171 | Match to published precision |
| Net points | +99.17 | +99.169 | Match to published precision |
| Maximum cumulative daily drawdown | 5.60 points | 5.601 points | Match to published precision |

A direct source audit compared all 416 mapped file pairs over this date window.
All 208 futures five-minute slices were identical, and all 208 cash one-minute
slices had identical timestamps and raw OHLCV, which are the fields this
strategy consumes. Some persisted cash indicator columns differed, but the
builder does not use them; it reconstructs its features from raw OHLCV. Thus
the effective strategy inputs are proven equal for these dates. The original
corrected trade artifact was not retained, so literal output-row hashes still
cannot be compared.

### Details available only from the later reconstruction

| Item | Later `6734204d...` diagnostic reconstruction |
|---|---:|
| Wins / losses | 112 / 109 |
| Win rate | 50.68% |
| Sizing-proxy P&L | Approximately +Rs49,744 |
| Immutable standalone V6 run provenance | None |
| Status | Reproducible ad-hoc diagnostic reconstruction, not a canonical run |

The 112-109 count, win rate, rupee proxy, and reconstructed daily-state counts
were calculated in the later replay; they were not fields stored in the older
report. They are reproducible derived detail on strategy-equivalent source
inputs, not a hash comparison to an old trade artifact. There is no separately
sealed V6 run provenance for the later reconstruction.

### Older published descriptive periods

| Descriptive period | Sessions | Fills | PF | Net points |
|---|---:|---:|---:|---:|
| Through 2026-07-16 | 35 | 130 | 2.315 | +65.86 |
| 2026-07-17 through 2026-08-03 | 12 | 63 | 2.305 | +28.23 |
| 2026-08-04 through 2026-08-17 | 10 | 28 | 1.392 | +5.07 |

These older-snapshot periods are descriptive, not clean
TRAIN/VALIDATION/TEST. The V6 setup book was selected using history through
2026-08-11, so part of every displayed period is in-sample or
selection-contaminated.

### Invalidated V6 legacy/full-history artifacts

There is no valid canonical "full-history V6" headline at present. The two
published longer-history artifacts below are retained only for auditability and
must not be used to compare or select a strategy.

| V6 artifact | Sessions | Orders/fills | Trade PF | Day PF | Net points | Validity |
|---|---:|---:|---:|---:|---:|---|
| Current-source report | 53 | 210 / 209 | 2.811 | 6.062 | +146.711 | **Invalid:** forward paths could include the next session |
| Protected legacy selection | 53 | 206 / 205 | 2.796 | 5.968 | +144.003 | **Invalid:** same path defect; original source state cannot be recovered |

The path audit removed 60,997 next-session rows from 1,347 five-minute-qualified
candidate paths. A trigger, target, stop or fallback close could therefore occur
on the next trading day in these legacy artifacts. Provenance hashes preserve
what ran; they do not repair that logic error.

### V6 honesty verdict

V6 is the strongest profitability benchmark in the available corrected
diagnostics, but it is not the most defensible production engine. Its setup book
is historically selected, its exact-trigger/no-expiry model is optimistic, and
its canonical full-history outputs are invalid. The 57-session corrected result
is two-lineage research evidence whose matching rounded aggregates do not prove
trade-row identity; it is not deployable proof.

## 3. Full authenticated V8-Combined result (40 sessions)

`Full V8-Combined` in this report means the complete authenticated independent
V8-Combined launcher run over its frozen 40-session contract, 2026-06-24 through
2026-08-19. It does **not** mean the separate 57-session V8 B0 full-history
study, and it does not imply that every raw historical date available on disk
was included.

### Book construction

V8-Combined is an independent, hash-pinned launcher using the diagnostic
TRAIN-comparison choice for each five-minute slot-side leg. Most choices are a
TRAIN winner or a configuration common to strict and retuned books. The
09:40 SHORT exception is V8-Strict only because it was less weak than the
retuned alternative; it still had negative TRAIN expectancy and did not
qualify. The mapping is therefore a frozen diagnostic hybrid, not ten validated
leg winners. Its literal setup book SHA-256 is
`ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675`.

| Five-minute slot | LONG source | SHORT source |
|---|---|---|
| 09:25 | Retuned V8 | Retuned V8 |
| 09:30 | Common | Retuned V8 |
| 09:35 | Common | Common |
| 09:40 | Common | V8-Strict, less weak but unqualified |
| 09:45 | Common | Common |

`Common` means the strict and retuned books use the same leg configuration.

### Literal per-leg configuration

`Px`, `OI`, and `Vol` are the five-minute directional thresholds. `Body` is the
one-minute body/range minimum, and `Wick` is the maximum adverse-wick/range
ratio.

| Leg | Source | Cap / picker | Px / OI / Vol | Body / Wick | SL / target | One-minute policy |
|---|---|---|---|---|---|---|
| 09:25 LONG | Retuned | 4 / max move | .30 / .10 / 3.0 | .00 / .50 | .40% / 1.00% | Through S+3, raw trigger, no midpoint/CLV |
| 09:25 SHORT | Retuned | 4 / max move | .20 / .10 / 1.5 | .60 / .60 | .50% / 3.00% | Through S+3, 2 bps buffer; minimum traded value Rs25m |
| 09:30 LONG | Common | 1 / max move | .65 / .10 / 1.0 | .50 / .50 | 1.00% / 2.50% | S+1, raw trigger |
| 09:30 SHORT | Retuned | 4 / max volume | .20 / 1.00 / 1.0 | .45 / .30 | 1.00% / 4.00% | Through S+3, midpoint and CLV .50; minimum value Rs25m |
| 09:35 LONG | Common | 1 / max liquidity | .20 / .10 / 1.0 | .60 / .50 | 1.00% / 2.50% | S+1, raw trigger |
| 09:35 SHORT | Common | 2 / max liquidity | .50 / 1.00 / 1.0 | .40 / .50 | 1.00% / 3.00% | S+1, raw trigger |
| 09:40 LONG | Common | 1 / max liquidity | .20 / .10 / 2.0 | .50 / .50 | .50% / 2.50% | S+1, raw trigger |
| 09:40 SHORT | V8-Strict, unqualified | 1 / max move | .20 / .10 / 1.0 | .40 / .50 | 1.00% / 3.00% | S+1, raw trigger; selected only as less weak |
| 09:45 LONG | Common | 1 / max move | .65 / .10 / 1.0 | .40 / .50 | 1.00% / 3.00% | S+1, raw trigger |
| 09:45 SHORT | Common | 1 / max volume | .20 / .75 / 1.0 | .40 / .30 | 1.00% / 2.00% | S+1, raw trigger |

### Overall 40-session diagnostic

| Metric | V8-Combined |
|---|---:|
| Window | 2026-06-24 through 2026-08-19 |
| Sessions | 40 |
| Cost / extra slippage | 15 / 0 bps |
| EOD policy | Last-real-bar sensitivity at declared 15:30 |
| Candidates | 890 |
| Closed fills | 184 |
| Fills/session | 4.60 |
| Wins / losses / flat | 93 / 91 / 0 |
| Win rate | 50.54% |
| Diagnostic PF | **1.892** |
| Diagnostic net points | **+60.16** |
| Sizing-proxy net P&L | **+Rs29,890.12** |
| Maximum cumulative daily drawdown | **6.00 points** |
| Positive / negative / flat days | 28 / 12 / 0 |
| TRAIN, 30 sessions | 153 fills, 79-74, PF 1.983, +53.93 points |
| Final 10 sessions | 31 fills, 14-17, PF 1.494, +6.23 points |
| Official headline | **N/A — source incomplete and EOD sensitivity used** |

### LONG/SHORT result

| Side | Candidates | Fills | W-L | Win % | PF | Net points | Proxy P&L |
|---|---:|---:|---:|---:|---:|---:|---:|
| LONG | 391 | 87 | 46-41 | 52.87% | 1.894 | +24.934 | +Rs12,057.11 |
| SHORT | 499 | 97 | 47-50 | 48.45% | 1.890 | +35.229 | +Rs17,833.01 |
| **Total** | **890** | **184** | **93-91** | **50.54%** | **1.892** | **+60.162** | **+Rs29,890.12** |

### Per-leg result

| Leg | Candidates | Fills | W-L | Win % | PF | Net points | Proxy P&L |
|---|---:|---:|---:|---:|---:|---:|---:|
| 09:25 LONG | 84 | 45 | 25-20 | 55.56% | 1.910 | +10.085 | +Rs4,878.16 |
| 09:25 SHORT | 198 | 51 | 22-29 | 43.14% | 1.942 | +17.256 | +Rs8,537.95 |
| 09:30 LONG | 50 | 8 | 5-3 | 62.50% | 3.904 | +8.250 | +Rs4,068.06 |
| 09:30 SHORT | 72 | 21 | 11-10 | 52.38% | 1.515 | +5.387 | +Rs2,801.12 |
| 09:35 LONG | 159 | 17 | 9-8 | 52.94% | 1.103 | +0.847 | +Rs456.83 |
| 09:35 SHORT | 29 | 6 | 4-2 | 66.67% | 4.941 | +9.084 | +Rs4,541.96 |
| 09:40 LONG | 78 | 13 | 5-8 | 38.46% | 2.099 | +4.948 | +Rs2,271.16 |
| 09:40 SHORT | 137 | 14 | 6-8 | 42.86% | 1.129 | +1.086 | +Rs802.74 |
| 09:45 LONG | 20 | 4 | 2-2 | 50.00% | 1.672 | +0.803 | +Rs382.90 |
| 09:45 SHORT | 63 | 5 | 4-1 | 80.00% | 19.872 | +2.416 | +Rs1,149.24 |

The high PF values in 09:35 SHORT and 09:45 SHORT rest on only six and five
fills. They are not stable estimates. The weak legs are also visible:
09:35 LONG has PF 1.103 and 09:40 SHORT PF 1.129. In the per-leg
TRAIN/final-segment audit, 09:30 SHORT won TRAIN but lost its only final-segment
trade, while several later legs had no final-segment fills.

### Direct V6 versus V8-Combined per-leg comparison at 15 bps

This is the leg-level view of the same common 40-session window. It remains a
diagnostic rather than execution parity because the two engines admit, fill and
expire orders differently.

| Leg | V6 fills | V6 win % | V6 PF | V6 net | V8-C fills | V8-C win % | V8-C PF | V8-C net |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:25 LONG | 18 | 44.44% | **2.463** | +8.808 | **45** | **55.56%** | 1.910 | **+10.085** |
| 09:25 SHORT | 39 | **56.41%** | **2.171** | **+17.917** | **51** | 43.14% | 1.942 | +17.256 |
| 09:30 LONG | **11** | 54.55% | 3.117 | **+9.152** | 8 | **62.50%** | **3.904** | +8.250 |
| 09:30 SHORT | **22** | 50.00% | **1.553** | +5.363 | 21 | **52.38%** | 1.515 | **+5.387** |
| 09:35 LONG | **19** | 47.37% | 1.053 | +0.507 | 17 | **52.94%** | **1.103** | **+0.847** |
| 09:35 SHORT | **10** | **80.00%** | **7.111** | **+14.055** | 6 | 66.67% | 4.941 | +9.084 |
| 09:40 LONG | **22** | 36.36% | 1.676 | **+5.663** | 13 | **38.46%** | **2.099** | +4.948 |
| 09:40 SHORT | **19** | **47.37%** | **1.422** | **+4.500** | 14 | 42.86% | 1.129 | +1.086 |
| 09:45 LONG | **7** | 42.86% | 1.392 | **+1.366** | 4 | **50.00%** | **1.672** | +0.803 |
| 09:45 SHORT | **9** | 66.67% | 4.046 | **+4.709** | 5 | **80.00%** | **19.872** | +2.416 |

V6 contributes more net in seven of ten legs; V8-Combined contributes more net
in 09:25 LONG, 09:30 SHORT and 09:35 LONG. This arithmetic does not establish
which leg should be transplanted: global duplicate/capital interactions mean a
hybrid must be replayed as a whole rather than estimated by summing rows.

### Candidate outcome funnel

| Terminal state | Count |
|---|---:|
| No confirmation | 542 |
| Window expired | 61 |
| Post-confirmation cancelled | 58 |
| Pre-confirmation invalidated | 23 |
| Duplicate rejected | 22 |
| Stopped | 81 |
| Targeted | 48 |
| Square-off | 55 |
| **Total candidates** | **890** |

### Coverage and validity

| Coverage item | Result |
|---|---:|
| Expected symbol-sessions | 8,320 |
| Complete symbol-sessions | 4,782 (57.48%) |
| Incomplete symbol-sessions | 3,538 (42.52%) |
| Symbol-sessions with exact 15:30 bar | 5,616 (67.50%) |
| Symbols affected by at least one incomplete session | 208 / 208 |

V8 correctly sets official PF, return, P&L and drawdown to `N/A`. The numeric
figures above are `diagnostic_closed_trade_metrics` under
`LAST_REAL_BAR_SENSITIVITY`. The hybrid mapping was selected on the first 30
sessions, and the final-ten result has only 31 fills. V8-Combined is therefore a
paper/shadow candidate, not a production configuration.

## 4. V9 09:50/09:55 honest optimization

### Frozen search design

| Item | V9 contract |
|---|---|
| Proposed legs | 09:50 LONG, 09:50 SHORT, 09:55 LONG, 09:55 SHORT |
| Search breadth | 48 preregistered configurations per leg; 192 visible leg hypotheses |
| Selection | Each slot-side leg independently; no pooling of LONG and SHORT |
| TRAIN | 2026-05-27 through 2026-07-09, 30 sessions |
| VALIDATION | 2026-07-10 through 2026-07-23, 10 sessions; locked unless TRAIN passed |
| TEST | 2026-07-24 through 2026-07-31, 6 sessions; locked unless prior stages passed |
| Diagnostic panel | 35 TRAIN-availability-selected symbols out of 208 |
| Base economics | 15 bps cost + 1 bp adverse slippage |
| Stress economics | 20 bps cost + 2 bps adverse slippage |
| Entry capacity | Maximum two entries per leg/day |
| Confirmation window | Through minute 4 using the V8 same-session state machine |

Each leg needed at least 40 TRAIN fills, 15 active days, PF at least 1.50,
positive net, robust PF excluding the best day at least 1.20, top-day share no
more than 25%, sufficient positive time blocks, stress PF at least 1.00,
positive stress net, and zero incomplete/unresolved selected trades.

### TRAIN result: no leg qualified

The max-trade row is the configuration with the largest observed sample. The
highest-PF row is shown only to expose how misleading sparse PF can be; it is
not an eligible winner.

| Leg | Diagnostic choice | Fills | Active days | W-L | Win % | PF | Net points | Proxy P&L | Max DD |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:50 LONG | Max-trade | 7 | 6 | 2-5 | 28.57% | 0.987 | -0.075 | +Rs80.41 | 3.460 |
| 09:50 LONG | Highest finite PF | 4 | 4 | 2-2 | 50.00% | 2.468 | +3.383 | +Rs1,768.41 | 1.154 |
| 09:50 SHORT | Max-trade / highest finite PF | 6 | 4 | 0-6 | 0.00% | 0.000 | -3.986 | -Rs1,879.87 | 3.986 |
| 09:55 LONG | Max-trade / highest finite PF | 3 | 3 | 2-1 | 66.67% | 1.458 | +0.529 | +Rs113.51 | 1.155 |
| 09:55 SHORT | Max-trade | 3 | 3 | 1-2 | 33.33% | 1.020 | +0.026 | +Rs18.40 | 1.318 |
| 09:55 SHORT | Highest finite PF | 2 | 2 | 1-1 | 50.00% | 3.014 | +0.469 | +Rs235.61 | 0.233 |

The 09:50 LONG max-trade row has -0.075 equal-weight net points but +Rs80.41
proxy P&L. This is not an arithmetic contradiction: the points column weights
each trade equally as a percentage return, whereas the rupee proxy uses
approximately Rs50,000 positions with integer quantity rounding and therefore
different per-trade weights. Neither measure passes the evidence gates.

| Leg / choice | Robust PF ex-best day | Stress PF | Stress net | Top-day share | Positive implemented thirds | Why it failed |
|---|---:|---:|---:|---:|---:|---|
| 09:50 LONG max-trade | 0.493 | 0.929 | -0.426 | 50.01% | 1 / 3 | Fills, days, PF, robustness, concentration, blocks and stress |
| 09:50 LONG highest PF | 1.233 | 2.323 | +3.182 | 50.02% | 2 / 3 | Only four fills/four days and excessive concentration |
| 09:50 SHORT max-trade | 0.000 | 0.000 | -4.297 | N/A | 0 / 3 | No wins; failed all substantive evidence gates |
| 09:55 LONG max-trade | 0.131 | 1.293 | +0.353 | 91.04% | 2 / 3 | Fills, days, base PF, robustness and concentration |
| 09:55 SHORT max-trade | 0.000 | 0.913 | -0.123 | 100.00% | 1 / 3 | Fills, days, base PF, robustness, concentration, blocks and stress |
| 09:55 SHORT highest PF | 0.000 | 2.170 | +0.344 | 100.00% | 1 / 3 | Only two fills/two days, no ex-best-day edge and full concentration |

The maximum samples were only 7, 6, 3 and 3 fills, versus 40 required. The PF
2.468 and 3.014 rows are based on four and two trades and cannot support an
edge claim. No joint extended-slot portfolio was formed, and VALIDATION and
TEST trade outcomes stayed locked.

### Exact leg status

| Leg | TRAIN-qualified config | VALIDATION | TEST | V9 status |
|---|---|---|---|---|
| 09:50 LONG | None | Locked/not run | Locked/not run | Disabled |
| 09:50 SHORT | None | Locked/not run | Locked/not run | Disabled |
| 09:55 LONG | None | Locked/not run | Locked/not run | Disabled |
| 09:55 SHORT | None | Locked/not run | Locked/not run | Disabled |

### Methodology discrepancy disclosed by the audit

The intended TRAIN stability gate was four positive non-overlapping
five-session blocks out of six. The sealed code instead used three contiguous
ten-session blocks and required two positive blocks. It would also have applied
extra stress, block and concentration gates in VALIDATION/TEST beyond the
previously stated later-stage protocol.

This defect does not change the rejection: every leg already failed the
40-fill and 15-active-day minimums, so no later-stage outcomes were opened.
Before any rerun capable of unlocking VALIDATION, the block definition and
later-stage guard set must be corrected and frozen.

## 5. V9 historical-data repair outcome

### Full 46-session source audit

| Audit measure | Result |
|---|---:|
| Sessions | 46 |
| Mapped symbols | 208 |
| Expected symbol-sessions | 9,568 |
| Complete symbol-sessions | 5,986 (62.56%) |
| Incomplete symbol-sessions | 3,582 (37.44%) |
| Expected bars | 4,305,600 |
| Valid bars | 4,252,937 |
| Missing bars | 43,703 |
| Invalid bars | 8,960 |
| Suspect flat/zero-volume synthetic cash rows | 6,426 |
| Repair targets | 52,663 |
| Headline source complete | **No** |

| Provider evidence state | Targets | Share | Interpretation |
|---|---:|---:|---|
| Exact-timestamp API `CANDLE` response | 7,192 | 13.66% | A returned row, not automatically a valid repair |
| Invalid API data | 2,539 | 4.82% | Response failed the exact validity contract |
| Repeated verified no candle | 42,932 | 81.52% | Three observations found no usable candle; not valid exchange coverage |

All 6,426 returned cash rows remained flat OHLC with zero volume. Strict
publication rejected them, so **no sealed repaired full-universe snapshot was
created**. Repair policy was subsequently hardened to reject these rows as
`suspect_api_flat_zero_volume`.

For the narrower 09:50/09:55 entry-time contract, 8,277 of 9,568 full-universe
symbol-sessions were complete and 1,291 remained incomplete. The optimizer then
used 35 symbols selected by TRAIN source availability. Their narrow grids were
complete within each split, but availability-based membership and absent legacy
row-lineage flags still prevent a full-universe or unbiased claim.

## 6. Final V9 active-book parity result

V9's independent launcher contains the literal V8-Combined ten-leg book and
four authenticated disabled records for 09:50/09:55. It imports the neutral V8
engine, uses its own cache/run/provenance namespace, and does not import the V6,
V7, V8-Combined launcher, optimizer or repair module.

This parity replay is on a **different population/window** from the common
40-session table: 35 symbols, 46 sessions from 2026-05-27 through 2026-07-31,
15 bps cost, 1 bp slippage and exact 15:30 square-off. It is shown separately
and must not be compared numerically as if the inputs matched the 40-session
V6/V8 result.

### V9 equals V8-Combined on identical inputs

| Metric | V9-Honest launcher | Independent V8-Combined parity replay |
|---|---:|---:|
| Sessions / symbols | 46 / 35 | 46 / 35 |
| Candidates | 223 | 223 |
| Closed fills | 57 | 57 |
| Fills/session | 1.24 | 1.24 |
| Wins / losses / flat | 28 / 29 / 0 | 28 / 29 / 0 |
| Win rate | 49.12% | 49.12% |
| Diagnostic PF | 1.485 | 1.485 |
| Diagnostic net points | +10.852 | +10.852 |
| Sizing-proxy net P&L | +Rs5,674.04 | +Rs5,674.04 |
| Maximum cumulative daily drawdown | 7.919 | 7.919 |
| Positive / negative / flat days | 14 / 14 / 18 | 14 / 14 / 18 |
| First 30 sessions | 31 fills, 16-15, 51.61% wins, PF 1.643, +8.383 points | Same |
| Later 16 sessions | 26 fills, 12-14, 46.15% wins, PF 1.264, +2.469 points | Same |
| Official headline | **N/A** | **N/A** |

### V9 active-book per-leg diagnostic on the 35-symbol panel

| Active leg | Candidates | Fills | W-L | Win % | PF | Net points | Proxy P&L |
|---|---:|---:|---:|---:|---:|---:|---:|
| 09:25 LONG | 13 | 8 | 4-4 | 50.00% | 1.525 | +1.164 | +Rs570.20 |
| 09:25 SHORT | 70 | 24 | 13-11 | 54.17% | 2.436 | +9.831 | +Rs4,855.04 |
| 09:30 LONG | 10 | 3 | 1-2 | 33.33% | 1.006 | +0.010 | -Rs50.60 |
| 09:30 SHORT | 23 | 7 | 1-6 | 14.29% | 0.657 | -2.005 | -Rs814.32 |
| 09:35 LONG | 26 | 4 | 2-2 | 50.00% | 0.684 | -0.514 | -Rs231.14 |
| 09:35 SHORT | 6 | 1 | 0-1 | 0.00% | 0.000 | -1.152 | -Rs565.64 |
| 09:40 LONG | 10 | 0 | 0-0 | N/A | N/A | 0.000 | Rs0.00 |
| 09:40 SHORT | 38 | 5 | 3-2 | 60.00% | 1.346 | +0.797 | +Rs580.62 |
| 09:45 LONG | 7 | 1 | 1-0 | 100.00% | Infinite | +1.609 | +Rs785.37 |
| 09:45 SHORT | 20 | 4 | 3-1 | 75.00% | 2.618 | +1.113 | +Rs544.53 |

The rupee proxy and equal-weight net points can differ in sign for a tiny leg
(for example 09:30 LONG) because quantity rounding and per-trade price weights
are different from an equal-weight percentage-point sum.

The active baseline's broader early-slot source contract found 152 incomplete
symbol-sessions out of 1,610 panel symbol-sessions. The official headline is
therefore `N/A`; the numbers above are closed-trade diagnostics. The panel is
also availability selected and lacks certified point-in-time lineage.

## 7. Honesty comparison

| Question | V6 | V8-Combined | V9-Honest |
|---|---|---|---|
| Same-session paths | Corrected only in ad-hoc replay; legacy canonical reports failed | Yes | Yes, via neutral V8 engine |
| Finite entry windows | No in legacy definition | Yes | Yes |
| Adverse gap fill and brackets from modeled fill | No | Yes | Yes |
| Global duplicate/capital ledger | No; independent trades | Yes, conservative no-backfill overlay | Yes, same active engine |
| Missing-source headline gate | Legacy engine did not fail closed | Yes; headline suppressed | Yes; headline suppressed |
| Chronological selection discipline | Historical setup selection contaminates displayed periods | Hybrid legs chosen on 30-session TRAIN; 10-session diagnostic shown | Later stages stayed locked because TRAIN failed |
| Full point-in-time universe / rolling OI | No | No | No |
| Certified row lineage | No | No | No |
| Official promotion-grade performance | None | None | None |
| Honest interpretation | Strongest profitability benchmark, not deployment proof | Better execution realism and final-ten/DD profile; paper/shadow only | Most fail-closed outcome reporting: admits no new edge and leaves all late legs disabled; protocol conformance still needs repair |

### Why V9 is the most fail-closed result

V9 does not have the highest PF. Among these artifacts it has the most
fail-closed outcome reporting because it refuses to manufacture a winner from
two to seven trades, requires each side to stand on its own, locks later
outcomes when TRAIN fails, preserves disabled-leg evidence, and suppresses
headline metrics when source coverage is incomplete. It also discloses the
stability-block implementation discrepancy rather than hiding it. That
discrepancy means V9 must not be described as the cleanest or most conformant
optimization methodology until the stated and implemented protocols match.

This honesty does **not** make the 1.485 diagnostic PF promotion-grade, and it
does not improve V8-Combined's active trading behavior. With all four new legs
disabled, V9 is a more auditable, fail-closed **research** wrapper around the
same active book; it is not inherently safe, validated or live-ready.

## 8. Production and research decision

| Decision | Result |
|---|---|
| Produce V6 live from these results | **No** — legacy canonical history is invalid and corrected results are ad-hoc/in-sample |
| Produce V8-Combined live | **No** — 42.52% source-contract incompleteness, PF 1.494 in only 31 final-segment fills, no prospective qualification |
| Enable 09:50 LONG/SHORT | **No** — neither leg passed TRAIN |
| Enable 09:55 LONG/SHORT | **No** — neither leg passed TRAIN |
| Continue paper/shadow | Use the independent V9 launcher with its unchanged V8-Combined active book and all four late legs disabled |
| Profitability benchmark | Keep corrected V6 as the benchmark, not as a production claim |

Before any live decision, the evidence needs a point-in-time F&O universe,
historically rolling near-month OI, a sealed full-universe exact-grid snapshot
with certified row lineage, one preregistered rerun with the block protocol
fixed, one-shot validation/test for only genuinely TRAIN-qualified legs, and at
least 20 new prospective sessions and 100 prospective fills.

## 9. Primary artifacts

| Artifact | Identifier |
|---|---|
| Common 40-session comparison | `FNO_V6_V7_V8_LAST_40_SESSIONS_TO_20260819.md` |
| V6 corrected/legacy audit | `FNO_V6_V7_BACKTEST_STRATEGY_AND_RESULTS.md` |
| V8-Combined result | `FNO_V8_COMBINED_BEST_PER_LEG_RESULTS.md` |
| V8-Combined validated run | `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_combined_best_per_leg_v1\runs\fno_v8_vc_20260820T174309351502+0530_af9cdf2ca31b` |
| V8-Combined run fingerprint | `af9cdf2ca31b830de32a3640bcf8fa0e4bb98c60da18c262ecfdd70da041ec53` |
| V9 optimization report | `FNO_V9_0950_0955_HONEST_OPTIMIZATION_RESULTS.md` |
| V9 sealed search | `C:\TradingData\eqidv2\fno_oi\strategy_research\v9_0950_0955_honest_v1\optimizer_runs\search_20260820T235156342242+0530_335057a1588c` |
| V9 search fingerprint | `335057a1588c66762c1d20fceb8690ae132db7308aa100174e8ad0554f02b0c7` |
| V9 35-symbol / 46-session parity run | `C:\TradingData\eqidv2\fno_oi\strategy_research\v9_honest_v8_combined_v1\runs\fno_v8_vh_20260821T001048354219+0530_d147ed2fc985` |
| V9 35-symbol parity fingerprint | `d147ed2fc985be6c466bc7e1194519461864976737ce1c5533abfe140129013b` |
| V9 actual common-40-session parity run | `C:\TradingData\eqidv2\fno_oi\strategy_research\v9_honest_v8_combined_v1\runs\fno_v8_vh_20260821T081627628253+0530_fc9bde5b2e14` |
| V9 common-40 parity fingerprint | `fc9bde5b2e1426a7bda5df7350bb9d8b8b50bb72d009fbe72da585159c4d4f08` |
| Repair audit | `C:\TradingData\eqidv2\fno_oi\historical_repair\v9_honest_v1\audits\audit_20260820T230716495187+0530_c8870913ee7a_4b5f1159\manifest.json` |
| Provider evidence | `C:\TradingData\eqidv2\fno_oi\historical_repair\v9_honest_v1\evidence\evidence_20260820T230741905042+0530_c8870913ee7a_d528ef60\manifest.json` |
