# CANDIDATE_CONFIGS — fast-momentum LONG (~0.75% symmetric) candidates

Pass gate = TRAIN trades≥60 & TRAIN PF≥1.15 & TEST trades≥20 & **TEST PF≥1.3** & dayDom≤0.5 & symDom≤0.4 & topTradeShare≤0.25.
Primary cost = 5 bps/leg slippage + statutory NSE; 15 bps/leg shown as stress.

**No candidate cleared the TEST gate.** See search_summary.json for the best per-family configs and ITERATION_LOG.md for why. Closest near-misses:

## near-miss F2_PRESSURE_BURST (LONG Pressure Burst Breakout)
```json
{
  "family": "F2_PRESSURE_BURST",
  "bracket": "b_075_100",
  "slip_bps": 5.0,
  "min_minute": null,
  "max_minute": 720,
  "top_n": 1,
  "rank_feat": "atr_pct",
  "max_per_sym_day": null,
  "max_book_concurrent": 20,
  "mask": [
    [
      "atr_pct",
      ">=",
      0.45
    ],
    [
      "vwap_dist_atr",
      "<=",
      3.0
    ],
    [
      "adx",
      ">=",
      25
    ]
  ]
}
```
- TRAIN @5bps: n=773 PF=0.765 net=Rs-88,906 win=46.7% exp=Rs-115.0/tr tgt/sl/eod/time=358/408/7/0 tpd=25.77 dayDom=0.021 symDom=0.011 topTr=0.003 hold=39.2m maxDD=Rs98,315 tie=0.0%
- TEST  @5bps: n=209 PF=0.739 net=Rs-27,001 win=45.45% exp=Rs-129.2/tr tgt/sl/eod/time=95/110/4/0 tpd=20.9 dayDom=0.083 symDom=0.053 topTr=0.011 hold=46.9m maxDD=Rs34,814 tie=0.0%
- TEST  @15bps: n=209 PF=0.457 net=Rs-68,439 win=45.45% exp=Rs-327.5/tr tgt/sl/eod/time=95/110/4/0 tpd=20.9 dayDom=0.023 symDom=0.053 topTr=0.011 hold=46.9m maxDD=Rs73,473 tie=0.0%

## near-miss F7_TREND_CONT (LONG EMA/VWAP Trend Continuation)
```json
{
  "family": "F7_TREND_CONT",
  "bracket": "b_075_100",
  "slip_bps": 5.0,
  "min_minute": null,
  "max_minute": 690,
  "top_n": null,
  "rank_feat": "atr_pct",
  "max_per_sym_day": null,
  "max_book_concurrent": 20,
  "mask": [
    [
      "vwap_dist_atr",
      "<=",
      2.0
    ],
    [
      "atr_pct",
      ">=",
      0.45
    ],
    [
      "mom3_pct",
      ">=",
      0.1
    ]
  ]
}
```
- TRAIN @5bps: n=750 PF=0.708 net=Rs-109,750 win=44.27% exp=Rs-146.3/tr tgt/sl/eod/time=328/403/19/0 tpd=25.0 dayDom=0.04 symDom=0.02 topTr=0.003 hold=42.5m maxDD=Rs110,876 tie=0.0%
- TEST  @5bps: n=181 PF=0.739 net=Rs-23,336 win=45.3% exp=Rs-128.9/tr tgt/sl/eod/time=82/96/3/0 tpd=18.1 dayDom=0.04 symDom=0.049 topTr=0.013 hold=45.6m maxDD=Rs35,252 tie=0.0%
- TEST  @15bps: n=181 PF=0.456 net=Rs-59,235 win=45.3% exp=Rs-327.3/tr tgt/sl/eod/time=82/96/3/0 tpd=18.1 dayDom=0.0 symDom=0.049 topTr=0.013 hold=45.6m maxDD=Rs64,068 tie=0.0%

## near-miss F5_PULLBACK_CONT (LONG Pullback Continuation)
```json
{
  "family": "F5_PULLBACK_CONT",
  "bracket": "b_075_100",
  "slip_bps": 5.0,
  "min_minute": null,
  "max_minute": 690,
  "top_n": 1,
  "rank_feat": "atr_pct",
  "max_per_sym_day": 2,
  "max_book_concurrent": 20,
  "mask": [
    [
      "mom3_pct",
      ">=",
      0.1
    ],
    [
      "atr_pct",
      ">=",
      0.35
    ],
    [
      "rsi",
      "<=",
      80
    ]
  ]
}
```
- TRAIN @5bps: n=644 PF=0.823 net=Rs-53,787 win=48.29% exp=Rs-83.5/tr tgt/sl/eod/time=309/327/8/0 tpd=21.47 dayDom=0.037 symDom=0.028 topTr=0.003 hold=42.6m maxDD=Rs63,328 tie=0.0%
- TEST  @5bps: n=205 PF=0.729 net=Rs-27,507 win=45.37% exp=Rs-134.2/tr tgt/sl/eod/time=91/108/6/0 tpd=20.5 dayDom=0.06 symDom=0.054 topTr=0.011 hold=56.1m maxDD=Rs34,809 tie=0.0%
- TEST  @15bps: n=205 PF=0.449 net=Rs-68,135 win=44.88% exp=Rs-332.4/tr tgt/sl/eod/time=91/108/6/0 tpd=20.5 dayDom=0.011 symDom=0.047 topTr=0.011 hold=56.1m maxDD=Rs70,511 tie=0.0%

## near-miss F8_OPENING_STRENGTH (LONG Opening Strength Continuation)
```json
{
  "family": "F8_OPENING_STRENGTH",
  "bracket": "b_075_100",
  "slip_bps": 5.0,
  "min_minute": null,
  "max_minute": null,
  "top_n": null,
  "rank_feat": "atr_pct",
  "max_per_sym_day": 2,
  "max_book_concurrent": 20,
  "mask": [
    [
      "vwap_dist_atr",
      "<=",
      2.0
    ],
    [
      "atr_pct",
      ">=",
      0.45
    ],
    [
      "mom2_pct",
      ">=",
      0.0
    ],
    [
      "body_frac",
      ">=",
      0.5
    ]
  ]
}
```
- TRAIN @5bps: n=455 PF=0.647 net=Rs-83,445 win=41.76% exp=Rs-183.4/tr tgt/sl/eod/time=189/253/13/0 tpd=15.17 dayDom=0.058 symDom=0.031 topTr=0.005 hold=55.3m maxDD=Rs83,206 tie=0.0%
- TEST  @5bps: n=102 PF=0.694 net=Rs-15,986 win=44.12% exp=Rs-156.7/tr tgt/sl/eod/time=45/55/2/0 tpd=10.2 dayDom=0.08 symDom=0.045 topTr=0.023 hold=55.7m maxDD=Rs16,076 tie=0.0%
- TEST  @15bps: n=102 PF=0.429 net=Rs-36,274 win=44.12% exp=Rs-355.6/tr tgt/sl/eod/time=45/55/2/0 tpd=10.2 dayDom=0.034 symDom=0.045 topTr=0.023 hold=55.7m maxDD=Rs35,729 tie=0.0%

## near-miss F6_VOLUME_EXPANSION (LONG Volume Expansion Breakout)
```json
{
  "family": "F6_VOLUME_EXPANSION",
  "bracket": "b_075_100",
  "slip_bps": 5.0,
  "min_minute": null,
  "max_minute": 660,
  "top_n": null,
  "rank_feat": "atr_pct",
  "max_per_sym_day": 3,
  "max_book_concurrent": 20,
  "mask": [
    [
      "atr_pct",
      ">=",
      0.25
    ],
    [
      "adx",
      ">=",
      20
    ],
    [
      "vwap_dist_atr",
      "<=",
      3.0
    ],
    [
      "close_loc",
      ">=",
      0.6
    ]
  ]
}
```
- TRAIN @5bps: n=950 PF=0.675 net=Rs-153,746 win=43.68% exp=Rs-161.8/tr tgt/sl/eod/time=388/498/64/0 tpd=31.67 dayDom=0.027 symDom=0.017 topTr=0.003 hold=80.2m maxDD=Rs153,630 tie=0.0%
- TEST  @5bps: n=328 PF=0.676 net=Rs-52,770 win=43.29% exp=Rs-160.9/tr tgt/sl/eod/time=132/171/25/0 tpd=32.8 dayDom=0.023 symDom=0.02 topTr=0.01 hold=91.5m maxDD=Rs57,497 tie=0.0%
- TEST  @15bps: n=328 PF=0.411 net=Rs-117,776 win=42.07% exp=Rs-359.1/tr tgt/sl/eod/time=132/171/25/0 tpd=32.8 dayDom=0.0 symDom=0.02 topTr=0.01 hold=91.5m maxDD=Rs118,094 tie=0.0%
