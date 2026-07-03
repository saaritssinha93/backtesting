# A_MOD_BREAK_C1_LOW (SHORT) — REDESIGNED_SETUP_IDEAS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

All variants keep the core intent (impulse continuation short through the prior bar's low, below session VWAP) and are generated from raw 5-min OHLCV by `scripts/redesign_scan.py` (CORE: red bar, close_loc<=0.40, range 0.60-2.20x ATR, close<prev low, close<session VWAP, vol_ratio>=1.5, bar>=3, 09:30-15:00).

## RX2_ALL

- **logic:** CORE re-detection, incidental gates removed
- **why it makes sense:** frees ADX/RSI/atr_pct so high-energy names enter; tests whether the scanner's gates were hiding the edge
- **rows (FIT/VAL/TRAIN/TEST):** 11998/7952/19950/7985
- **best ungated exit (FIT/VAL band score):** [1.0, 1.5]

## RX2_FRESHLOW

- **logic:** CORE + bar makes a NEW session low
- **why it makes sense:** continuation only when the break creates fresh discovery — removes mid-range noise breaks
- **rows (FIT/VAL/TRAIN/TEST):** 3460/2599/6059/2615
- **best ungated exit (FIT/VAL band score):** [1.2, 2.0]

## RX2_CONFIRM2

- **logic:** CORE + previous bar was also a red prior-low break
- **why it makes sense:** 2-bar persistence = real flow, not a one-bar flush
- **rows (FIT/VAL/TRAIN/TEST):** 4286/2533/6819/2514
- **best ungated exit (FIT/VAL band score):** [1.0, 1.5]

## RX2_DEEP

- **logic:** CORE + close >= 0.35 ATR below the broken level
- **why it makes sense:** requires the break to travel — filters marginal ticks through the level
- **rows (FIT/VAL/TRAIN/TEST):** 10741/7151/17892/7168
- **best ungated exit (FIT/VAL band score):** [1.0, 1.5]

## RX2_FIRST_MORN

- **logic:** first CORE event of the symbol-day, <= 12:00
- **why it makes sense:** the first break is the informative one; morning has the follow-through
- **rows (FIT/VAL/TRAIN/TEST):** 7584/5093/12677/5045
- **best ungated exit (FIT/VAL band score):** [1.2, 2.0]

## RX2_MKT

- **logic:** CORE + NIFTY50 below its 5-min EMA20
- **why it makes sense:** don't fight the tape — shorts only when the index itself is weak
- **rows (FIT/VAL/TRAIN/TEST):** 7980/5265/13245/4873
- **best ungated exit (FIT/VAL band score):** [1.2, 2.0]

## RETEST

- **logic:** break -> pullback to the broken level within 4 bars -> red rejection
- **why it makes sense:** enters HIGHER on the retest, fixing the chase-the-extension entry (better R:R geometry)
- **rows (FIT/VAL/TRAIN/TEST):** 8061/5914/13975/5991
- **best ungated exit (FIT/VAL band score):** [1.5, 2.5]
