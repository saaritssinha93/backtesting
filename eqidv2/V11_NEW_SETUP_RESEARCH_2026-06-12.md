# V11 New Setup Research - 2026-06-12

## Decision

No new long/short pair cleared the full promotion standard.

- Production changes to v7 live: **none**
- Production changes to v11 backtesting profiles: **none**
- One long overlay remains a **research probation** candidate.
- The strongest apparent new short failed badly in the fresh June forward window and is rejected.

Profitability is not guaranteed. All reported P&L is from historical simulation with next-1-minute entry, 1-minute exits, and the repository's cost model.

## Best Long Candidate

### `N_HIGH_RS_EMA_BOUNCE_LONG`

Underlying structure:

1. Existing `D_EMA20_BOUNCE` long detection.
2. Signal candle `body_pct >= 0.60`.
3. Intraday relative strength `rs_pct >= 4.0`.
4. One candidate per ticker per day.
5. Selected exit: `0.90%` stop / `1.50%` target.

Results:

| Window | Trades | Net PF | Net P&L |
|---|---:|---:|---:|
| Train through 2026-01-31 | 25 | 2.03 | Rs 7,307 |
| Validation 2026-02-01 to 2026-03-31 | 8 | 1.31 | Rs 1,124 |
| Holdout 2026-04-01 to 2026-05-29 | 21 | 1.45 | Rs 3,192 |
| Fresh forward 2026-06-01 to 2026-06-10 | 1 | infinity | Rs 1,499 |

Why it is not promoted:

- The fresh forward window has only one trade.
- April was exceptionally strong, while May had PF `0.21` and lost Rs 5,615.
- The sample is too sparse and monthly stability is not established.

Decision: **WAIT FOR MORE FORWARD DATA**.

## Best Short Candidate

### `N_MORNING_ZERO_WICK_SHORT`

Underlying structure:

1. A bearish candidate from `S_BB_SQUEEZE_SHORT`, `E_ORB_BREAKOUT_SHORT`, `D_EMA20_REJECTION`, or `E_VWAP_BAND_FADE`.
2. Signal between `10:01` and `11:30` IST.
3. Lower wick no more than `0.01%` of signal close.
4. `quality_score <= 100`.
5. One candidate per ticker per day.
6. Development-selected exit: `1.00%` stop / `2.00%` target.

Results:

| Window | Trades | Net PF | Net P&L |
|---|---:|---:|---:|
| Train through 2026-01-31 | 78 | 1.80 | Rs 18,600 |
| Validation 2026-02-01 to 2026-03-31 | 17 | 1.61 | Rs 4,838 |
| Holdout 2026-04-01 to 2026-05-29 | 17 | 1.84 | Rs 4,997 |
| Fresh forward 2026-06-01 to 2026-06-10 | 21 | 0.32 | **-Rs 8,200** |

Why it failed:

- June immediately reversed the historical result.
- Twenty of the 21 June candidates came from `E_ORB_BREAKOUT_SHORT`, already diagnosed as a churn/cost sink.
- Selecting a post-June filter to remove those losses would be forward-window overfitting.

Decision: **REJECT**.

## Other Hypotheses Tested

### Market-relative response

- Long: stock holds during a NIFTY pullback, then breaks its recent range.
- Short: stock fails during a NIFTY bounce, then breaks its recent range.
- Result: both failed the training edge gate after costs.

### Late-day intraday momentum

- Uses first-half-hour direction and relative strength, midday retention, VWAP/EMA alignment, then a late range break.
- Long full PF: approximately `0.64`.
- Short full PF: approximately `0.69`.
- Result: rejected on raw performance.

## Existing Setups Remain Better

The current researched book already contains stronger validated candidates, notably:

- Long: `G_HIGHER_HIGH_BREAK`, `L_DOUBLE_BOTTOM_VWAP`
- Short: `T_TREND_DAY_EMA_STAIR_SHORT`, `E_VWAP_LOSE_EARLY_SHORT`

Those are preferable to promoting a weaker setup merely to satisfy a request for something new.

## Research Artifacts

- `research_v11_market_relative_response.py`
- `research_v11_late_day_intraday_momentum.py`
- `research_v11_new_overlay_forward_test.py`
- `C:\TradingData\eqidv2\outputs_ID_v11_market_relative_response`
- `C:\TradingData\eqidv2\outputs_ID_v11_late_day_intraday_momentum`
- `C:\TradingData\eqidv2\outputs_ID_v11_new_overlay_forward_test`

## Primary Research Used

- Gao, Han, Li, and Zhou, *Intraday Momentum: The First Half-Hour Return Predicts the Last Half-Hour Return*: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2552752
- Zarattini, Barbon, and Aziz, *A Profitable Day Trading Strategy for the U.S. Equity Market*: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4729284
- Stefan Nagel, *Evaporating Liquidity*: https://www.nber.org/papers/w17653
- Campbell, Ramadorai, and Vuolteenaho, *Caught on Tape: Institutional Order Flow and Stock Returns*: https://www.nber.org/papers/w11439
- Park and Irwin, *The Profitability of Technical Analysis: A Review*: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=603481

The papers motivate hypotheses; they do not establish that those hypotheses transfer to NSE single-stock intraday execution. The local forward tests are the governing evidence.
