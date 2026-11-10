# G3 V21 entry-check combination audit through 4 November 2026

Backtest-only replay of all 16 fixed combinations of four previously computed G7 V4 entry checks on V21's 73 setup pairs. The baseline first applies the V21 39-new-pair cap-one / completed-5-minute-volume gate; the 34 original pairs retain cap three. Each G7 check is applied only where its source policy defines it. Rejected leaders are not replaced. The same native entries, targets, stops, costs, paths and fixed per-entry sizing are replayed with unlimited portfolio capital.

Gate codes: **N** = dated Nifty both-counter veto; **L** = G7 V4 extreme / efficiency / weak-RSI veto; **A** = G7 named setup ATR maximum; **S** = G7 named setup OI or traded-value minimum. **none** is the V21 baseline. **N+L+A+S** is the current all-G7 entry-check research variant.

## All 65 sessions

| Gates | Orders | Trades | Losses | Win rate | Mean trade (Rs) | Median trade (Rs) | Net P&L (Rs) | Losing days | Daily-close DD (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 1092 | 961 | 323 | 66.39% | 1,299.21 | 2,470.69 | 1,248,545.54 | 16 | 40,721.16 |
| N | 922 | 819 | 274 | 66.54% | 1,336.37 | 2,457.56 | 1,094,487.14 | 16 | 39,221.16 |
| L | 1076 | 945 | 319 | 66.24% | 1,298.52 | 2,457.56 | 1,227,105.74 | 15 | 42,749.18 |
| A | 1053 | 925 | 301 | 67.46% | 1,407.46 | 2,550.00 | 1,301,901.63 | 15 | 40,721.16 |
| S | 1087 | 956 | 319 | 66.63% | 1,321.41 | 2,528.43 | 1,263,266.56 | 16 | 40,721.16 |
| N+L | 910 | 807 | 271 | 66.42% | 1,341.42 | 2,457.56 | 1,082,522.19 | 16 | 35,384.36 |
| N+A | 888 | 788 | 254 | 67.77% | 1,455.29 | 2,550.00 | 1,146,767.97 | 16 | 39,221.16 |
| N+S | 917 | 814 | 270 | 66.83% | 1,362.66 | 2,508.81 | 1,109,208.16 | 16 | 39,221.16 |
| L+A | 1037 | 909 | 297 | 67.33% | 1,408.65 | 2,550.00 | 1,280,461.83 | 14 | 36,954.03 |
| L+S | 1071 | 940 | 315 | 66.49% | 1,321.09 | 2,508.81 | 1,241,826.76 | 15 | 42,749.18 |
| A+S | 1048 | 920 | 297 | 67.72% | 1,431.11 | 2,550.00 | 1,316,622.66 | 15 | 40,721.16 |
| N+L+A | 876 | 776 | 251 | 67.65% | 1,462.38 | 2,550.00 | 1,134,803.02 | 16 | 35,384.36 |
| N+L+S | 905 | 802 | 267 | 66.71% | 1,368.13 | 2,508.81 | 1,097,243.22 | 16 | 35,384.36 |
| N+A+S | 883 | 783 | 250 | 68.07% | 1,483.38 | 2,550.00 | 1,161,488.99 | 16 | 39,221.16 |
| L+A+S | 1032 | 904 | 293 | 67.59% | 1,432.72 | 2,550.00 | 1,295,182.86 | 14 | 36,954.03 |
| N+L+A+S | 871 | 771 | 247 | 67.96% | 1,490.95 | 2,550.00 | 1,149,524.05 | 16 | 35,384.36 |

## Tradeoffs visible in the saved history

- Compared with V21 baseline, **A+S** changes 961 to 920 fills, 323 to 297 losses, win rate 66.39% to 67.72%, mean trade Rs 1,299.21 to Rs 1,431.11, median Rs 2,470.69 to Rs 2,550.00, and total net Rs 1,248,545.54 to Rs 1,316,622.66.
- Compared with **N+L+A+S**, A+S yields Rs 167,098.61 more total net, but 50 more losing trades; its mean trade changes Rs 1,490.95 to Rs 1,431.11. These objectives trade off.

## Later six observed sessions

The first 59 sessions run through 27 October; the later six are 28 October through 4 November. They have already been observed and are a sensitivity check, not a prospective test.

| Gates | Early 59 trades | Early net (Rs) | Later 6 trades | Later losses | Later win rate | Later mean trade (Rs) | Later median trade (Rs) | Later net (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 853 | 1,218,435.96 | 108 | 48 | 55.56% | 278.79 | 818.54 | 30,109.58 |
| A+S | 819 | 1,277,967.93 | 101 | 44 | 56.44% | 382.72 | 1,209.55 | 38,654.73 |
| N+L+A+S | 687 | 1,107,993.97 | 84 | 36 | 57.14% | 494.41 | 1,503.47 | 41,530.08 |

## Separate exploratory CHOP14 check

This is a **new, exploratory round-number** trend filter, not a locked G7 threshold. It starts with all four G7 entry checks and vetoes a selected candidate when its confirmation-minute CHOP14 exceeds 50. Missing CHOP14 passes through; it is not inferred from later bars. The 50 threshold was checked once here, without a sweep. The locked G7 low-efficiency veto does not use CHOP14; a separate unselected G7 research candidate uses a different CHOP threshold.

Among the 871 all-G7 selected orders, 90 lack a ready CHOP14 value; 81 of those filled. The extra gate vetoes 69 selected orders.

| Policy | Trades | Losses | Win rate | Mean trade (Rs) | Median trade (Rs) | Net P&L (Rs) | Daily-close DD (Rs) | Later six net (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| Existing all G7 | 771 | 247 | 67.96% | 1,490.95 | 2,550.00 | 1,149,524.05 | 35,384.36 | 41,530.08 |
| All G7 + CHOP14 <=50 | 710 | 222 | 68.73% | 1,595.57 | 2,647.86 | 1,132,853.26 | 31,763.81 | 43,346.80 |
| All G7 + noon CHOP14 <=50 | 727 | 227 | 68.78% | 1,585.94 | 2,658.83 | 1,152,981.80 | 24,615.85 | 42,291.96 |

The global extra CHOP check can improve win rate, loss count and per-trade profit while reducing total net P&L, so it does not dominate all G7. The noon-only variant applies the same CHOP check only from 12:00 IST onward. **Its clock condition was chosen after inspecting history; its full, early and later results are in-sample and do not validate the rule. It is not recommended for live activation.** The noon rule changes P&L on 37 of 65 days: 17 improve and 20 worsen. Its paired daily difference has standard deviation Rs 2,700.01 versus a total net gain of Rs 3,457.75. Only 2 filled trades are vetoed in the later six sessions.

[Its period results](exploratory_overall.csv) | [day-wise results](exploratory_daily.csv) | [per-order decisions](exploratory_admission_audit.csv).

## Method and limits

The 65 sealed sessions span 2026-07-29 to 2026-11-04. After-cost trade mean and median use **executed trades**, not selected orders. Daily-close drawdown uses realized daily P&L; it does not measure intraday equity lows. All 16 combinations share the same historical observations, and inspecting their outcomes can overfit. No combination is a verified live improvement. The G7 historical slot allowlist, two-loss lock and profit-protection exits are outside this entry-check comparison.

Existing baseline and all-G7 arms reproduce their saved 65-day metrics after verifying both earlier result manifests. Admission uses only saved pre-entry gate decisions; no P&L field is used by the filter.

[All period results](overall.csv) | [Day-wise results](daily.csv) | [Per-order decisions](admission_audit.csv) | [Configuration](config.json) | [Provenance](provenance.json)
