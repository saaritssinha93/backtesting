# FnO relaxed shadow replay — 2026-08-18

Snapshot: **2026-08-18 12:26:23 IST**  
Execution: **counterfactual/shadow only; no live orders or canonical signals created**

## Relaxed policy

- 5-minute directional move: at least **0.10%**
- Futures OI increase: at least **0.05%**
- 5-minute volume ratio: at least **0.90**
- 1-minute candle: matching colour only; no close-vs-5-minute-close breakout requirement
- 1-minute body/range: at least **0.20**
- Side-adverse wick/range: at most **0.80**
- Maximum entries: **5 per side per slot**
- Position sizing: `floor(₹50,000 / entry)`
- Cost model: **5 bps round trip**
- Existing slot-specific stop/target percentages retained

P&L conventions: STOP and TARGET rows show realized counterfactual net P&L at the bracket level. RUNNING rows show net mark-to-market at the snapshot LTP. PENDING rows would have zero P&L until filled.

Headline and slot totals use unrounded calculations; entry rows are rounded to paise, so manually summing displayed rows can differ by a few paise.

## Summary

| Model | Entries | Target | Stop | Running | Net P&L |
|---|---:|---:|---:|---:|---:|
| Full 5-minute forced entry | 61 | 3 | 18 | 40 | **-₹2,327.50** |
| Relaxed 1-minute trigger entry | 25 | 2 | 8 | 15 | **+₹1,961.92** |
| Relaxed 1-minute excluding stale 09:30 | 23 | 1 | 8 | 14 | **+₹1,117.70** |

### Slot totals

| Slot | 5m entries | 5m net | Relaxed 1m entries | Relaxed 1m net |
|---|---:|---:|---:|---:|
| 09:25 | 14 | -₹2,154.53 | 7 | -₹2,197.56 |
| 09:30 | 15 | -₹2,222.22 | 2 | +₹844.21 |
| 09:35 | 13 | -₹832.41 | 5 | +₹750.73 |
| 09:40 | 9 | +₹1,419.97 | 6 | +₹2,517.03 |
| 09:45 | 10 | +₹1,461.70 | 5 | +₹47.50 |

## Full 5-minute forced entries

These are all 61 scanner occurrences, entered immediately at the corresponding 5-minute signal close. This deliberately bypasses the 1-minute confirmation.

| Slot | Side | Symbol | Entry | Status | Exit/current mark | Net P&L |
|---|---|---|---:|---|---:|---:|
| 09:25 | LONG | OIL | 482.60 | RUNNING | 480.50 | -₹241.15 |
| 09:25 | LONG | ONGC | 240.37 | STOP | 239.17 | -₹274.60 |
| 09:25 | LONG | SWIGGY | 276.50 | STOP | 275.10 | -₹276.88 |
| 09:25 | LONG | TVSMOTOR | 4,388.30 | STOP | 4,366.40 | -₹265.04 |
| 09:25 | SHORT | ABCAPITAL | 396.35 | RUNNING | 398.65 | -₹314.77 |
| 09:25 | SHORT | BHARTIARTL | 1,941.60 | RUNNING | 1,942.20 | -₹39.27 |
| 09:25 | SHORT | BSE | 3,248.30 | STOP | 3,272.70 | -₹390.36 |
| 09:25 | SHORT | HAL | 5,077.00 | STOP | 5,115.10 | -₹365.75 |
| 09:25 | SHORT | KAYNES | 3,691.20 | RUNNING | 3,659.00 | +₹394.61 |
| 09:25 | SHORT | OFSS | 11,738.00 | STOP | 11,826.00 | -₹375.48 |
| 09:25 | SHORT | PAYTM | 1,563.20 | STOP | 1,574.90 | -₹386.93 |
| 09:25 | SHORT | TMPV | 330.30 | RUNNING | 324.95 | +₹782.91 |
| 09:25 | SHORT | TRENT | 2,940.60 | STOP | 2,962.70 | -₹400.70 |
| 09:25 | SHORT | VOLTAS | 1,257.90 | RUNNING | 1,257.30 | -₹1.13 |
| 09:30 | LONG | BLUESTARCO | 1,508.30 | RUNNING | 1,502.40 | -₹219.59 |
| 09:30 | LONG | DIVISLAB | 8,591.00 | RUNNING | 8,588.00 | -₹36.48 |
| 09:30 | LONG | OIL | 484.00 | RUNNING | 480.50 | -₹385.43 |
| 09:30 | LONG | ONGC | 240.84 | RUNNING | 239.07 | -₹391.32 |
| 09:30 | LONG | PGEL | 607.70 | STOP | 601.60 | -₹525.12 |
| 09:30 | LONG | SONACOMS | 819.70 | STOP | 811.50 | -₹516.59 |
| 09:30 | LONG | TIINDIA | 2,844.30 | TARGET | 2,915.40 | +₹1,184.52 |
| 09:30 | LONG | TVSMOTOR | 4,404.80 | STOP | 4,360.80 | -₹508.23 |
| 09:30 | SHORT | ABCAPITAL | 395.40 | RUNNING | 398.65 | -₹434.41 |
| 09:30 | SHORT | ADANIENSOL | 1,593.80 | STOP | 1,609.70 | -₹517.60 |
| 09:30 | SHORT | BRITANNIA | 5,520.00 | RUNNING | 5,544.00 | -₹240.84 |
| 09:30 | SHORT | COLPAL | 1,914.40 | RUNNING | 1,917.00 | -₹92.49 |
| 09:30 | SHORT | INDUSTOWER | 374.30 | RUNNING | 372.70 | +₹187.91 |
| 09:30 | SHORT | KPITTECH | 602.30 | RUNNING | 598.00 | +₹331.90 |
| 09:30 | SHORT | WIPRO | 179.21 | RUNNING | 179.33 | -₹58.48 |
| 09:35 | LONG | NATIONALUM | 389.20 | RUNNING | 389.45 | +₹7.09 |
| 09:35 | LONG | OIL | 484.85 | RUNNING | 480.50 | -₹473.02 |
| 09:35 | LONG | PREMIERENE | 1,024.80 | STOP | 1,014.60 | -₹514.20 |
| 09:35 | SHORT | ABCAPITAL | 394.95 | RUNNING | 398.65 | -₹491.08 |
| 09:35 | SHORT | ASIANPAINT | 2,654.00 | RUNNING | 2,615.00 | +₹678.11 |
| 09:35 | SHORT | BDL | 1,374.10 | RUNNING | 1,378.50 | -₹183.13 |
| 09:35 | SHORT | COLPAL | 1,907.80 | RUNNING | 1,917.00 | -₹264.00 |
| 09:35 | SHORT | DLF | 660.20 | STOP | 666.80 | -₹519.76 |
| 09:35 | SHORT | DRREDDY | 1,183.90 | RUNNING | 1,186.00 | -₹113.06 |
| 09:35 | SHORT | LICHSGFIN | 497.10 | RUNNING | 496.90 | -₹4.85 |
| 09:35 | SHORT | NESTLEIND | 1,471.80 | RUNNING | 1,471.70 | -₹20.98 |
| 09:35 | SHORT | SBICARD | 628.65 | RUNNING | 623.60 | +₹374.12 |
| 09:35 | SHORT | TMPV | 329.70 | RUNNING | 324.95 | +₹692.36 |
| 09:40 | LONG | COALINDIA | 412.00 | STOP | 409.95 | -₹272.98 |
| 09:40 | LONG | TIINDIA | 2,871.10 | TARGET | 2,942.90 | +₹1,196.20 |
| 09:40 | SHORT | DLF | 659.50 | STOP | 666.10 | -₹519.73 |
| 09:40 | SHORT | FORTIS | 914.55 | RUNNING | 909.75 | +₹234.51 |
| 09:40 | SHORT | GVT&D | 4,268.00 | RUNNING | 4,220.30 | +₹501.23 |
| 09:40 | SHORT | PATANJALI | 351.40 | RUNNING | 347.75 | +₹493.35 |
| 09:40 | SHORT | PAYTM | 1,556.70 | STOP | 1,572.30 | -₹524.11 |
| 09:40 | SHORT | SBICARD | 628.00 | RUNNING | 623.60 | +₹322.79 |
| 09:40 | SHORT | SHRIRAMFIN | 1,113.10 | RUNNING | 1,112.80 | -₹11.29 |
| 09:45 | LONG | DIVISLAB | 8,625.00 | RUNNING | 8,588.00 | -₹206.56 |
| 09:45 | LONG | TIINDIA | 2,891.40 | TARGET | 2,978.10 | +₹1,449.32 |
| 09:45 | SHORT | CROMPTON | 243.70 | RUNNING | 243.50 | +₹16.02 |
| 09:45 | SHORT | IRFC | 86.50 | RUNNING | 86.44 | +₹9.68 |
| 09:45 | SHORT | MPHASIS | 2,472.50 | RUNNING | 2,476.90 | -₹112.73 |
| 09:45 | SHORT | NAM-INDIA | 1,169.30 | STOP | 1,181.00 | -₹515.96 |
| 09:45 | SHORT | NBCC | 88.72 | RUNNING | 88.46 | +₹121.41 |
| 09:45 | SHORT | PAGEIND | 37,085.00 | RUNNING | 36,695.00 | +₹371.46 |
| 09:45 | SHORT | RVNL | 225.80 | RUNNING | 226.08 | -₹86.83 |
| 09:45 | SHORT | TMPV | 327.85 | RUNNING | 324.95 | +₹415.88 |

## Relaxed 1-minute trigger entries

These entries first pass the relaxed 5-minute and 1-minute rules, then use the confirmation high for LONG or low for SHORT as the stop-entry trigger. All 25 triggers traded afterward.

| Slot | Side | Symbol | Trigger entry | Fill minute | Status | Exit/current mark | Net P&L |
|---|---|---|---:|---|---|---:|---:|
| 09:25 | LONG | ONGC | 240.70 | 09:26 | STOP | 239.50 | -₹273.31 |
| 09:25 | LONG | OIL | 483.90 | 09:26 | STOP | 481.50 | -₹272.12 |
| 09:25 | SHORT | PAYTM | 1,560.00 | 09:26 | STOP | 1,571.70 | -₹399.36 |
| 09:25 | SHORT | ABCAPITAL | 395.55 | 09:26 | STOP | 398.50 | -₹396.62 |
| 09:25 | SHORT | TRENT | 2,935.10 | 09:26 | STOP | 2,957.10 | -₹398.95 |
| 09:25 | SHORT | BHARTIARTL | 1,939.90 | 09:26 | RUNNING | 1,942.20 | -₹81.75 |
| 09:25 | SHORT | OFSS | 11,723.00 | 09:26 | STOP | 11,811.00 | -₹375.45 |
| 09:30 | LONG | TIINDIA | 2,850.00 | 09:37 | TARGET | 2,921.20 | +₹1,186.17 |
| 09:30 | SHORT | COLPAL | 1,904.80 | 09:31 | RUNNING | 1,917.00 | -₹341.96 |
| 09:35 | LONG | OIL | 485.50 | 09:36 | STOP | 480.65 | -₹519.46 |
| 09:35 | SHORT | COLPAL | 1,904.60 | 09:36 | RUNNING | 1,917.00 | -₹347.16 |
| 09:35 | SHORT | TMPV | 329.20 | 09:36 | RUNNING | 324.95 | +₹616.90 |
| 09:35 | SHORT | ASIANPAINT | 2,652.00 | 09:38 | RUNNING | 2,615.00 | +₹642.13 |
| 09:35 | SHORT | SBICARD | 628.45 | 09:36 | RUNNING | 623.60 | +₹358.33 |
| 09:40 | LONG | TIINDIA | 2,884.30 | 09:42 | TARGET | 2,956.40 | +₹1,201.18 |
| 09:40 | SHORT | GVT&D | 4,263.50 | 09:41 | RUNNING | 4,220.30 | +₹451.75 |
| 09:40 | SHORT | FORTIS | 913.70 | 09:41 | RUNNING | 909.75 | +₹188.63 |
| 09:40 | SHORT | PATANJALI | 351.10 | 09:46 | RUNNING | 347.75 | +₹450.77 |
| 09:40 | SHORT | SHRIRAMFIN | 1,111.00 | 09:51 | RUNNING | 1,112.80 | -₹106.00 |
| 09:40 | SHORT | SBICARD | 628.10 | 09:41 | RUNNING | 623.60 | +₹330.69 |
| 09:45 | SHORT | TMPV | 327.65 | 09:50 | RUNNING | 324.95 | +₹385.50 |
| 09:45 | SHORT | NAM-INDIA | 1,166.70 | 09:46 | STOP | 1,178.40 | -₹515.90 |
| 09:45 | SHORT | MPHASIS | 2,470.30 | 09:46 | RUNNING | 2,476.90 | -₹156.70 |
| 09:45 | SHORT | IRFC | 86.48 | 09:47 | RUNNING | 86.44 | -₹1.87 |
| 09:45 | SHORT | PAGEIND | 37,050.00 | 09:50 | RUNNING | 36,695.00 | +₹336.48 |

## Data and causality notes

- Scanner inputs: immutable V6 scanner snapshots for 09:25–09:45.
- Confirmation inputs: exact durable completed 1-minute bars where available.
- The canonical 09:30 confirmation remained stale-blocked. Its two relaxed entries are **counterfactual only** and are excluded from the causal subtotal.
- Four non-selected 09:30 confirmation bars required later read-only broker reconstruction; none of those four became a relaxed 1-minute entry.
- Minute paths and 46/46 LTPs were retrieved read-only. This report does not alter live configuration, fingerprints, markers, signals, or orders.
- The relaxed thresholds are fitted from today’s discussion and have not been validated over the frozen historical backtest. They must not be promoted without a separate versioned backtest and parity review.
