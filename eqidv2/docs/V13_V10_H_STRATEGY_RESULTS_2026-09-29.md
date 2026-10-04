# V13-V10-H strategy backtest through 28 September 2026

## Scope and source

This report uses cash-equity entry and exit prices with mapped futures OI. It
does not simulate option premiums or futures-contract execution. The source is
an immutable snapshot of futures files taken on 29 September at about 09:22
IST, paired with completed equity minute history through 28 September. The
source contains **41 eligible sessions** from 29 July through 28 September.

The 15–28 September window has **10 completed sessions**, each with 210
observed equities and 15,120 completed five-minute equity bars. All 6,465
forward paths in the source path-quality audit are complete. The new G replay
reconciled every overlapping selection and execution against the frozen G
bundle through 23 September: state, entry/exit time and price, costs, and P&L.
The full new bundle is [here](C:/TradingData/eqidv2/v13_v10_h_research/source_build_20260928_snapshot_final/bundle_manifest.json).

Both published runs have `COMPLETE` manifests, verified artifact hashes and
`execution_authority: false`:

- [Standard H sizing report](C:/TradingData/eqidv2/v13_v10_h_research/runs/20260929_risk3000_through_20260928_final/REPORT.md)
- [Fixed strategy trial report](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/REPORT.md)
- [Machine-readable trial results](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/results.json)

## Full-history results

G's original legacy model earned **₹191,300.47**. With H's common whole-share
and tick execution assumptions, the same G signals earned **₹189,879.77**.
The ₹1,420.71 difference is an execution-model effect. The original H
`risk_3000` sizing experiment earned **₹164,729.15** on the common model:
₹25,150.61 less than matched G. It selected the same 91 orders and executed
the same 82 trades.

Each row below changes one research rule. Stop trials use the INR 3,000
planned-risk G reference of ₹164,729.15; all other rules use the ₹189,879.77
shared-model G reference. Currency is modeled net P&L after the declared
flat 5-bps round-trip cost proxy.

| Rule | Executed | Net P&L | Difference vs matched G | Daily-close DD | Minute-close DD | 15–28 Sep net |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| G, shared model | 82 | ₹189,879.77 | Control | ₹16,271.10 | ₹18,492.99 | ₹10,330.23 |
| H risk 3000 | 82 | ₹164,729.15 | −₹25,150.61 vs shared G | ₹14,989.26 | ₹17,113.70 | ₹5,338.16 |
| EMA9 extension ≤ 1 ATR | 5 | ₹35,371.68 | −₹154,508.09 | ₹0.00 | ₹5,029.52 | ₹0.00 |
| VWAP extension ≤ 1 ATR | 7 | ₹34,227.94 | −₹155,651.83 | ₹3,239.37 | ₹8,302.69 | ₹0.00 |
| ATR stop, risk matched | 82 | ₹121,401.25 | −₹43,327.90 | ₹19,162.39 | ₹21,095.03 | −₹302.51 |
| Structural stop, risk matched | 82 | ₹110,750.25 | −₹53,978.90 | ₹9,696.04 | ₹13,668.87 | ₹17,199.36 |
| Breakout retest entry | 48 | ₹124,814.92 | −₹65,064.85 | ₹11,671.21 | ₹18,384.07 | ₹11,651.15 |
| Failed-breakdown exit | 82 | ₹112,512.17 | −₹77,367.60 | ₹19,445.89 | ₹19,610.52 | −₹937.27 |
| Observed-universe breadth | 51 | ₹61,402.24 | −₹128,477.53 | ₹15,129.07 | ₹20,063.49 | ₹2,755.33 |
| Stock vs NIFTY 5m relative strength > 0 | 82 | ₹189,879.77 | ₹0.00 | ₹16,271.10 | ₹18,492.99 | ₹10,330.23 |

The relative-strength rule made **no selection change**: all 91 G orders
already passed its simple directional comparison. The EMA9 and VWAP rules
retained only six and eight orders respectively and removed every selected
order in the recent two-week window. Their zero recent P&L reflects no
executed trades. Sector-cap results require a complete dated classification
map; the VIX trial requires time-stamped historical quotes available by each
decision. Neither input was present, so neither trial has a performance claim.

The standard H report includes the declared model, extra adverse 5 bps on
both sides and one minute of activation delay. Both stress tests kept the
`risk_3000` sizing delta negative. Each strategy trial also records these
stresses and exploratory chronological folds in its own directory.

## Daywise result: 15–28 September

The period is the 14 calendar dates ending with the latest completed session
on 28 September. `G trades` counts executed trades. Stop rows below use their
equal-risk control for comparison, so their rupee amounts should not be
treated as identical-position counterfactuals to `G shared`.

| Day | G trades | G shared net | H risk 3000 | Structural stop | Retest entry |
| --- | ---: | ---: | ---: | ---: | ---: |
| 15 Sep | 4 | ₹11,733.31 | ₹9,319.90 | ₹15,650.81 | ₹8,556.92 |
| 16 Sep | 1 | −₹3,247.46 | −₹2,993.47 | −₹2,993.64 | ₹0.00 |
| 17 Sep | 1 | −₹3,248.75 | −₹2,999.62 | −₹2,995.15 | ₹0.00 |
| 18 Sep | 3 | −₹9,774.89 | −₹8,996.18 | −₹3,707.26 | −₹6,577.15 |
| 21 Sep | 0 | ₹0.00 | ₹0.00 | ₹0.00 | ₹0.00 |
| 22 Sep | 0 | ₹0.00 | ₹0.00 | ₹0.00 | ₹0.00 |
| 23 Sep | 0 | ₹0.00 | ₹0.00 | ₹0.00 | ₹0.00 |
| 24 Sep | 1 | ₹7,412.75 | ₹6,790.63 | ₹6,131.60 | ₹0.00 |
| 25 Sep | 2 | ₹14,092.43 | ₹10,215.05 | ₹11,109.96 | ₹9,671.38 |
| 28 Sep | 2 | −₹6,637.15 | −₹5,998.15 | −₹5,996.96 | ₹0.00 |
| **Total** | **14** | **₹10,330.23** | **₹5,338.16** | **₹17,199.36** | **₹11,651.15** |

See the [full daywise comparison](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/last_two_weeks_daily_comparison.csv) for every trial.
The structural stop improves this short window by **₹11,861.20 versus its
equal-risk reference**, while losing **₹53,978.90** across all 41 sessions.
Recent performance alone does not support adoption.

## Stock entries in those two weeks

These are the common-model G orders. All times are IST, and each P&L includes
modeled 5-bps round-trip cost. The [stock-by-stock comparison](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/last_two_weeks_stock_comparison.csv)
adds each H rule's status, entry, exit and P&L beside these rows.

| Day | Stock | Side | Entry → exit IST | Outcome | G net |
| --- | --- | --- | --- | --- | ---: |
| 15 Sep | IDEA | Short | 09:29 ₹14.75 → 09:40 ₹14.85 | Stop | −₹3,639.80 |
| 15 Sep | ABB | Short | 09:32 ₹7,113.50 → 15:15 ₹7,015.00 | Time exit | ₹6,646.03 |
| 15 Sep | APOLLOHOSP | Short | 09:42 ₹8,853.00 → 15:15 ₹8,770.00 | Time exit | ₹4,400.12 |
| 15 Sep | POWERINDIA | Short | 09:52 ₹30,730.00 → 10:15 ₹30,444.20 | Target | ₹4,326.96 |
| 16 Sep | MUTHOOTFIN | Long | 09:57 ₹2,784.50 → 10:05 ₹2,767.75 | Stop | −₹3,247.46 |
| 17 Sep | ATHERENERG | Short | 09:52 ₹1,531.00 → 10:02 ₹1,540.20 | Stop | −₹3,248.75 |
| 18 Sep | HINDZINC | Long | 09:37 ₹586.90 → 12:54 ₹583.35 | Stop | −₹3,270.78 |
| 18 Sep | HINDZINC | Long | 09:42 ₹590.90 → 09:53 ₹587.35 | Stop | −₹3,253.25 |
| 18 Sep | AMBUJACEM | Long | 09:58 ₹391.40 → 10:40 ₹389.05 | Stop | −₹3,250.86 |
| 23 Sep | 360ONE | Long | No fill | Trigger not reached | ₹0.00 |
| 24 Sep | INDIANB | Short | No fill | Trigger not reached | ₹0.00 |
| 24 Sep | HINDPETRO | Short | 09:46 ₹355.55 → 15:15 ₹350.10 | Time exit | ₹7,412.75 |
| 25 Sep | OFSS | Short | 09:35 ₹10,576.00 → 10:09 ₹10,364.45 | Target | ₹9,694.31 |
| 25 Sep | FORTIS | Short | 09:53 ₹860.20 → 10:01 ₹852.20 | Target | ₹4,398.11 |
| 28 Sep | NATIONALUM | Short | 09:27 ₹344.90 → 09:29 ₹347.00 | Stop | −₹3,292.78 |
| 28 Sep | VEDL | Short | 09:27 ₹258.50 → 09:47 ₹260.10 | Stop | −₹3,344.37 |

Of 16 selected orders, 14 executed: six winners earned ₹36,878.28 and eight
losers lost ₹26,548.05. Two orders remained unfilled.

## Minute-level check of 28 September

The minute files contain every modeled minute from fill through exit, with
OHLC, share quantity and marked P&L. Start with [G's minute paths](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/g_shared/last_two_weeks_stock_minutes.csv),
then compare the [ATR stop](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/stop_atr/last_two_weeks_stock_minutes.csv),
[structural stop](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/stop_structure/last_two_weeks_stock_minutes.csv)
and [early invalidation exit](C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/20260929_strategy_through_20260928_final/failed_breakdown_exit/last_two_weeks_stock_minutes.csv).

- NATIONALUM entered in the 09:27 one-minute bar at ₹344.90. The 09:28 bar
  reached ₹346.20, then the 09:29 bar reached ₹347.50, crossing G's modeled
  stop and exiting at ₹347.00. The structural-stop trial stayed open until
  10:31, then also stopped; it did not rescue the trade. The ATR stop exited
  at ₹346.30 in the 09:29 bar and reduced this one loss under the equal-risk
  sizing comparison.
- VEDL entered in the 09:27 bar at ₹258.50. Its 09:30 bar reached ₹259.50;
  G held, and the 09:47 bar reached ₹260.50, crossing the modeled ₹260.10
  exit. The ATR stop exited at ₹259.40 at 09:30. A completed-minute reclaim
  exit covered at the next open, ₹259.75 at 09:40. Both saved money on this
  trade, while reducing net profit over the full sample.
- Both retest-entry trials stayed unfilled on 28 September. Both EMA9 and
  VWAP extension filters rejected the two stocks. Their entry distances
  were 7.04 and 5.34 ATR from EMA9, and 3.40 and 2.44 ATR from VWAP,
  respectively, for NATIONALUM and VEDL.

One-minute OHLC cannot reveal the sequence of trades inside a minute. The
simulator applies stop-first resolution where stop and target touch in one
bar. Historical G's original execution model earned −₹6,500 on the two
28 September trades, versus −₹6,637.15 under the common model. Neither is
the one-share broker LIVE result or the dashboard's PAPER result.

## Decision and limits

No tested rule establishes a full-history profit improvement over its
matched G control. The structural stop has lower daily-close drawdown and a
slightly higher win rate (52/82), while giving up nearly ₹54,000 net on the
full sample. Retest entry improves recent net by ₹1,320.92 versus shared G
but misses 34 executed G trades and loses ₹65,064.85 over all sessions.
The broad extension filters discard many profitable continuation entries.

The dataset is complete for the recorded 210-symbol, ten-session recent
window, but historical raw arrival times and independently verified dated
roster membership remain unavailable. The common execution model assumes
whole-share fills, a uniform ₹0.05 tick, flat 5-bps costs and no order-book
depth or partial fills. These results reuse known historical data and have
no untouched prospective sample. The code reports `promotion_eligible: false`.

The next useful research work is to collect a prospective H decision journal
and verified dated sector/VIX inputs. Any new extension threshold should be
registered before future outcomes are known. Repeated threshold fitting on
this same sample risks backtest overfitting, as described in the
[Bailey et al. research paper](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf).
