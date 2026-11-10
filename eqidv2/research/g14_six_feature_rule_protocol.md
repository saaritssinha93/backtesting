# V14 six-field morning-entry experiment — frozen protocol

Frozen on 2026-11-03 **before examining the results of these candidate rules**. This is a research protocol, not a claim that any rule will reach a 70% win rate. Apply the same four variants to LONG and SHORT in all six slots: 09:20, 09:25, 09:30, 09:35, 09:40 and 09:45. Do not tune a separate threshold for each of the 12 slot/side cells after inspecting their outcomes.

## Six inputs and their clocks

At the **completed** five-minute signal, let `s = +1` for LONG and `s = -1` for SHORT. All percentages below are in percentage points: `0.20` means **0.20%**, not 20%.

| Input | Exact definition at signal time |
| --- | --- |
| Open gap % `G` | `100 × (today's 09:15 cash open / previous completed cash session's official NSE EQ close − 1)`. |
| PDC % `P` | `100 × (completed signal cash close / that same prior-session close − 1)`. This defines “PDC %” as current price relative to previous-day close. |
| Price 5m % `M` | `100 × (completed signal cash close / preceding completed five-minute cash close − 1)`. At 09:20 only, use 09:15 cash open as the opening-bar anchor and label it **opening-bar move**, because no earlier completed five-minute bar exists. |
| OI `Q` | The positive current OI for the stock's **same dated futures contract** at the completed signal. Use it as a validity check; raw OI levels are not comparable across different stocks and do not get an absolute cutoff. |
| OI Δ 5m % `D` | `100 × (Q_signal / Q_previous_completed_bar − 1)` for 09:25 onward. At 09:20, the available historical comparison is 09:16→09:20, **four minutes**, and must be labelled as such in reports. |
| OI day % `H` | `100 × (Q_signal / Q_09:20 − 1)` using the same dated contract's first completed 09:20 OI. It is exactly zero at 09:20 and is partly correlated with `D` later. |

Use the prior-session close only if the vendor/source value, symbol mapping, date and availability before today's open have been audited. The raw candidate file's `prev_close` means **previous five-minute close**, so substituting it for the prior **day's** close would corrupt both `G` and `P`. For OI, never compare different expiries/contracts. Missing or non-positive required values fail the candidate; no forward fill from a later candle. The 09:20 four-minute OI comparison is an explicit exception to the name “OI Δ 5m %”; also publish results with 09:20 excluded as a sensitivity check if the exact five-minute definition is mandatory.

Define directional fields `g = s × G`, `p = s × P`, `m = s × M`. OI changes `D` and `H` retain their actual signs; do **not** reverse them for SHORT. OI is participation, not a directional vote by itself.

## Four predeclared selection variants

All thresholds are inclusive. Every variant first requires valid as-of values of all six inputs, a completed signal, a valid same-contract OI history, and `Q > 0`. It then applies the conditions below, with no other five-minute or one-minute **admission** indicators. These thresholds are hypotheses, not values discovered by optimizing the final result.

| ID | Rule | Rationale to test |
| --- | --- | --- |
| `A_direction_only` | `p ≥ 0.20`, `m ≥ 0.10`, `D ≥ 0`, `H ≥ 0`; no gap-sign restriction | Simple directional participation control. |
| `B_with_gap` | All A conditions plus `g ≥ 0` | Test whether avoiding opposite opening gaps improves trade quality. |
| `C_three_paths_OR` | `D ≥ 0`, `H ≥ 0`, and **one complete price path**: continuation `g ≥ 0.20, p ≥ 0.25, m ≥ 0.10`; flat gap `abs(G) < 0.20, p ≥ 0.25, m ≥ 0.10`; or reversal `g ≤ -0.20, p ≥ 0.05, m ≥ 0.10` | Test structured OR across meaningful setups. OR does not mean that any single weak indicator may admit a trade. |
| `D_strict_OR` | Same three paths as C, but each path needs `m ≥ 0.15`, continuation/flat `p ≥ 0.40`, reversal `p ≥ 0.20`, plus `D ≥ 0.05` and `H ≥ 0.10` **after 09:20**. For 09:20 only, `H ≥ 0` because it is identically zero. | One predeclared stricter-neighbour sensitivity, rather than an open-ended grid search. |

Each path is mirrored for SHORT by the `s` multiplication. The absolute open gap only distinguishes the flat-gap path. These are **four variants total**, each evaluated unchanged across 12 slot/side cells. The existing V14/G7 V4 Lock2 run is a separate external comparator and is not counted as a candidate variant.

If more than one candidate competes for a preserved slot/side quota, use a fixed six-field-only score (larger wins):

```
score = 0.45·clip(p/1.00, 0, 1)
      + 0.35·clip(m/0.50, 0, 1)
      + 0.10·clip(D/0.50, 0, 1)
      + 0.10·clip(H/1.00, 0, 1)
```

Resolve exact score ties by canonical stock symbol. This prevents a hidden EMA, volume, Nifty, wick or former G7 score from silently deciding the admitted trades. `Q` is checked for validity rather than ranked across stocks. Keep existing position limits, exits, stops, targets, loss-day guard and portfolio accounting identical across variants. One-minute observations may establish a time-valid execution/confirmation and fill, but they cannot be a separate direction, EMA, volume, body or wick selection gate in this six-field experiment. Report any unavoidable legacy gate separately if it cannot be removed without changing the backtest engine.

## Honest replay and evaluation

1. Reconstruct candidates from **all eligible stocks at each of the six signal times**, not just trades chosen by G7/V14. Verify complete daily stock and minute-candle coverage, symbol/contract joins, prior-close provenance, and timestamps. A rule that admits previously rejected stocks needs their complete subsequent minute path for stops/targets. Record missing-data exclusions per slot and side.
2. Reject any signal whose entry or fill predates the completed candle/confirmation. Preserve the existing stop-first same-minute ambiguity rule, realistic lot sizes/capital limits, position conflicts and fixed exits. Do not infer a favorable fill from the completed candle's high/low.
3. Freeze the candidate rules above and the July–September 2026 development / October–November 2, 2026 chronological assessment split. If the actual available session range differs, use the same **September 30 calendar boundary**, report the number of sessions in each side, and never select by assessment results. Prior G7/V14 strategy and exits have already been studied on parts of this history, so this is a chronological assessment, **not** a completely untouched independent holdout.
4. Select **at most one** candidate using development data only: require at least **30 executed trades**, development win rate ≥70%, positive development net P&L at **9 bps round-trip costs**, and no worse development maximum drawdown than the existing V14 comparator. Among eligible candidates pick highest 9 bps net P&L; break ties by lower maximum drawdown, then rule ID. If none qualify, explicitly say **no variant qualified**. Still publish every variant's assessment results, without choosing a retrospective winner.
5. Show both development and assessment results for all four variants and the comparator at **5, 9 and 15 bps round-trip costs**. Metrics: candidate count, executed trade count, LONG/SHORT and each slot counts, win rate, net ₹ and return per unit capital, average win/loss, profit factor, losing days, worst day, maximum drawdown, fills rejected by limits, newly admitted winners/losers, and profitable trades the new rule omitted. Publish day-wise and trade-wise files. Report `n` next to every percentage and preferably a Wilson 95% interval; a tiny cell with >70% observed wins is not proof of a 70% underlying win probability.
6. Make the adoption decision from the **assessment** results after the predeclared development selection, with no further threshold edits: 70% assessment win rate alone is insufficient if net after costs, downside, or losing days worsen. If assessment misses the goal, report that honestly and retain the V14 comparator for live/paper decisions. Any further idea becomes a **new frozen experiment** tested on future sessions.

The absolute OI field and opening OI day change contain little incremental cross-stock information under these definitions. The six inputs also omit news, liquidity, volatility and market direction by design. Consequently, no rule can guarantee a profitable day or a 70% out-of-sample win rate; fewer selected trades can mechanically lift the displayed win rate while reducing net profit.

## Pre-run pool coverage audit

The archived raw pool has 63 dates from 2026-07-29 through 2026-11-02 and 77,812 stock/signal rows in these six slots **before** testing both sides or applying these rules. A typical slot has 208–216 stock rows per day. Across all dates, the 09:20 slot has 13,256 rows but only 6,548 non-null archived `oi_change_pct` values (49.4%); 13,123 have a non-null opening price move. The four most recent archived days have no 09:40/09:45 raw rows; separate expanded-slot snapshots must be joined and clock-audited. These are input-coverage facts, not rule outcomes. Publish the final feature-valid denominator and excluded counts for every slot and day so that a high win rate cannot be caused by silently dropping half the opening pool.

The completed, expanded six-feature source contains **79,536 stock/signal candidates**, of which **69,806** pass its as-of feature/integrity audit. Each stock/signal has a direction derived from its completed five-minute price move (`LONG`, `SHORT` or `NONE`), so the candidate count is not doubled simply because both side rules are evaluated. This candidate direction assignment must be disclosed separately from the six-field threshold gates.
