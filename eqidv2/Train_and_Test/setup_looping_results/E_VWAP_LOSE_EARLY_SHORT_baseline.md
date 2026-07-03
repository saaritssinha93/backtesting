# E_VWAP_LOSE_EARLY_SHORT Baseline

## 2026-06-29 Six-Week Rerun

Status before this rerun: parked by `_LIVE_SURVIVAL_DEMOTION_2026_06_29`. The source setup card previously still said "active/strongest"; that has now been corrected to rejected/parked.

Pinned split inferred from available pool coverage:

| Period | Dates | Raw pool rows | Entry rows |
|---|---|---:|---:|
| TRAIN | 2026-04-27..2026-06-05 | 265 | 265 |
| TEST | 2026-06-08..2026-06-12 | 60 | 60 |

Available pool coverage is 2025-06-02 through 2026-06-24. The weeks ending 2026-06-19 and 2026-06-26 are partial, so 2026-06-08..2026-06-12 is the latest completed available TEST week; the TRAIN window is the 6 immediately preceding completed weeks.

Baseline config evaluated:

| Field | Value |
|---|---|
| Side | SHORT |
| Detection | `early_vwap_lose_break_prev_low` |
| Mask filters | `vol_ratio >= 1.8` and `vol_ratio <= 3.2` |
| Pre-momentum gates | none |
| Entry guards | `min_slot=09:45` |
| Exit | SL 0.70 / Target 1.00 |
| Cost realism | `setup_train_test.py` default 15 bps per leg + statutory intraday costs |

### Six-Week Baseline Metrics

| Period | Trades | Win % | Gross profit | Gross loss | Net PnL | PF | Avg win | Avg loss | Max DD | Day block p | Outcomes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| TRAIN | 54 | 29.63 | 11,570 | 31,976 | -20,407 | 0.362 | 723 | -841 | -21,771 | 0.9999 | SL 34, TARGET 15, EOD 5 |
| TEST | 10 | 40.00 | 3,058 | 4,756 | -1,698 | 0.643 | 765 | -793 | -2,296 | 1.0000 | SL 5, TARGET 4, EOD 1 |

### Six-Week Rejection / Filter Counts

| Period | Entry rows | After guard | After pre-mom | After dedupe | After mask | Resolved trades |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 265 | 196 | 196 | 192 | 54 | 54 |
| TEST | 60 | 38 | 38 | 38 | 10 | 10 |

### Six-Week Day-Wise Results

TRAIN:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-04-27 | 2 | -167 |
| 2026-04-29 | 1 | -918 |
| 2026-04-30 | 4 | -2,941 |
| 2026-05-04 | 1 | -835 |
| 2026-05-05 | 1 | -927 |
| 2026-05-06 | 2 | -1,853 |
| 2026-05-07 | 3 | -199 |
| 2026-05-08 | 7 | -3,114 |
| 2026-05-11 | 2 | -1,843 |
| 2026-05-12 | 2 | -165 |
| 2026-05-13 | 1 | -929 |
| 2026-05-14 | 4 | -324 |
| 2026-05-18 | 1 | -241 |
| 2026-05-19 | 1 | -931 |
| 2026-05-21 | 3 | 2,253 |
| 2026-05-25 | 3 | -1,086 |
| 2026-05-26 | 3 | -1,709 |
| 2026-05-27 | 2 | -1,061 |
| 2026-05-29 | 3 | 606 |
| 2026-06-01 | 1 | -927 |
| 2026-06-02 | 2 | -1,860 |
| 2026-06-03 | 1 | -912 |
| 2026-06-04 | 1 | -924 |
| 2026-06-05 | 3 | 599 |

TEST:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-06-08 | 1 | -932 |
| 2026-06-09 | 4 | -325 |
| 2026-06-10 | 2 | -165 |
| 2026-06-11 | 3 | -276 |

### Six-Week Symbol-Wise Notes

Worst TRAIN symbols: FINEORG -1,856 across 2 trades; ZFCVINDIA -1,076 across 2 trades; NUVOCO -932; JKIL -932; LUMAXIND -932.

Best TRAIN symbols: INTERARCH 767; UTTAMSUGAR 767; BLUESTARCO 767; EUREKAFORB 766; ARTEMISMED 766.

Worst TEST symbols: ORIENTCEM -932; PRICOLLTD -932; POONAWALLA -931; RALLIS -931; TBOTEK -924.

Best TEST symbols: SHAREINDIA 767; GODFRYPHLP 767; ANANDRATHI 763; PNBHOUSING 762.

### Six-Week Failure Modes

- Bad follow-through / fake break: most signals still failed to continue after losing VWAP and prior low.
- Tight SL / volatility noise: TRAIN had 34 SL exits out of 54 trades; TEST had 5 SL exits out of 10.
- Weak volume edge: the documented `1.8..3.2` volume band did not create positive expectancy over 6 TRAIN weeks.
- Day fragility: TRAIN was negative on most active days, and TEST was negative on every active day.
- Symbol-specific weakness was broad rather than concentrated in one name, so pruning single symbols does not rescue the setup.

Full per-trade detail is in `E_VWAP_LOSE_EARLY_SHORT_6wk_loop_details.json`, iteration `0`.

---

## Prior Thin 2wk/1wk Baseline

Status before the prior thin loop: parked by `_LIVE_SURVIVAL_DEMOTION_2026_06_29`. At that time the source setup card still said "active", but the current imported `FINAL_SETUP_CONF` removed E into `RESEARCH_WATCH_CONF`.

Pinned split:

| Period | Dates | Raw pool rows | Entry rows |
|---|---|---:|---:|
| TRAIN | 2026-05-25..2026-06-05 | 111 | 111 |
| TEST | 2026-06-08..2026-06-12 | 60 | 60 |

Later pool weeks ending 2026-06-19 and 2026-06-26 are partial, so 2026-06-08..2026-06-12 is the latest completed available TEST week.

Baseline config evaluated:

| Field | Value |
|---|---|
| Side | SHORT |
| Detection | `early_vwap_lose_break_prev_low` |
| Mask filters | `vol_ratio >= 1.8` and `vol_ratio <= 3.2` |
| Pre-momentum gates | none |
| Entry guards | `min_slot=09:45` |
| Exit | SL 0.70 / Target 1.00 |
| Cost realism | `setup_train_test.py` default 15 bps per leg + statutory intraday costs |

## Baseline Metrics

| Period | Trades | Win % | Gross profit | Gross loss | Net PnL | PF | Avg win | Avg loss | Max DD | Day block p | Outcomes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| TRAIN | 19 | 31.58 | 3,963 | 11,237 | -7,273 | 0.353 | 661 | -864 | -7,872 | 0.9962 | SL 12, TARGET 5, EOD 2 |
| TEST | 10 | 40.00 | 3,058 | 4,756 | -1,698 | 0.643 | 765 | -793 | -2,296 | 1.0000 | SL 5, TARGET 4, EOD 1 |

## Rejection / Filter Counts

| Period | Entry rows | After guard | After pre-mom | After dedupe | After mask | Resolved trades |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 111 | 78 | 78 | 77 | 19 | 19 |
| TEST | 60 | 38 | 38 | 38 | 10 | 10 |

## Day-Wise Results

TRAIN:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-05-25 | 3 | -1,086 |
| 2026-05-26 | 3 | -1,709 |
| 2026-05-27 | 2 | -1,061 |
| 2026-05-29 | 3 | 606 |
| 2026-06-01 | 1 | -927 |
| 2026-06-02 | 2 | -1,860 |
| 2026-06-03 | 1 | -912 |
| 2026-06-04 | 1 | -924 |
| 2026-06-05 | 3 | 599 |

TEST:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-06-08 | 1 | -932 |
| 2026-06-09 | 4 | -325 |
| 2026-06-10 | 2 | -165 |
| 2026-06-11 | 3 | -276 |

## Symbol-Wise Notes

Worst TRAIN symbols: FINEORG -1,856 across 2 trades; LUMAXIND -932; BLSE -931; REDINGTON -931; HBLENGINE -929.

Best TRAIN symbols: BLUESTARCO 767; TALBROAUTO 766; SYNGENE 765; STAR 760; JLHL 758.

Worst TEST symbols: ORIENTCEM -932; PRICOLLTD -932; POONAWALLA -931; RALLIS -931; TBOTEK -924.

Best TEST symbols: SHAREINDIA 767; GODFRYPHLP 767; ANANDRATHI 763; PNBHOUSING 762.

## Failure Modes

- Weak volume edge: the documented band did not create positive expectancy on TRAIN or TEST.
- Tight SL / volatility noise: TRAIN had 12 SL exits out of 19 trades; TEST had 5 SL exits out of 10.
- Fake break / poor follow-through: 2 TRAIN and 1 TEST trades still faded to EOD rather than resolving cleanly.
- Day fragility: TRAIN was negative on 7 of 9 trading days; TEST was negative on all 4 active trading days.
- Live contradiction: existing live-paper evidence already showed E as a loser, and this replay does not rebut it.

Full per-trade detail is in `E_VWAP_LOSE_EARLY_SHORT_loop_details.json`, iteration `0`.
