# V13-V10-H research backtest

H is a separate, offline cash-equity research backtest derived from retained G.
Mapped futures provide OI; this is not an options-premium or futures-contract
execution simulator. It does not place orders, change G, update schedulers, or
promote itself to live trading.

## Run

From the project directory:

```powershell
py -3.12 fno_v13_v10_h_backtest.py --experiment risk_3000
py -3.12 fno_v13_v10_h_backtest.py --experiment control
py -3.12 fno_v13_v10_h_backtest.py --experiment strategy_trials
```

Python 3.12 and the project's existing NumPy, pandas, pyarrow and pytest
environment are sufficient; H does not need Docker, a broker login or a new
service. The default source is the checksum-inventoried, cutoff-corrected G
bundle through **2026-09-23**, not an automatically refreshed live dataset.

Each command prints a new immutable run directory under
`C:/TradingData/eqidv2/v13_v10_h_research/runs/`. Read `REPORT.md` first.
Only a run with a final `manifest.json` containing `state: COMPLETE` is a
published result. Existing run IDs are refused, and source artifacts are
hashed before and after execution. Failed runs may retain diagnostic files;
they must not be mistaken for completed reports.

```powershell
py -3.12 fno_v13_v10_h_backtest.py --help
py -3.12 fno_v13_v10_h_backtest.py --source-run C:/path/to/checksummed/G/bundle --output-root C:/path/to/H/research
py -3.12 -m pytest -q tests/test_fno_v13_v10_h.py
```

Do not point `--source-run` at raw/live data. The source needs the same declared
bundle format as the default: dataset/features/paths, G ledgers, metadata,
retained config hash and an artifact inventory. Unsupported source contracts
fail rather than silently falling back to a different G version.

## Three distinct comparisons

1. **Exact G legacy replay:** reuses G's original selector, exits, execution and
   capital model. Ordered selections, fills, timestamps, prices, per-trade net
   P&L and costs must match the saved source. This preserves the historical G
   result, including its original assumptions.
2. **G on the H shared execution model:** same selected signals and exit
   percentages, but whole shares, declared tick rounding and the new execution
   assumptions below. Changes versus exact legacy G are model effects, not
   strategy improvement.
3. **H on the same shared model:** only sizing changes. `risk_3000` budgets up
   to INR 3,000 of planned stop loss plus flat transaction-cost proxy per trade,
   subject to G's INR 500,000 notional-per-entry ceiling. No settings are
   selected by historical profit. `control` disables this sizing change and
   requires the two shared-model ledgers to be exactly identical.

The INR 3,000 budget is one predefined research hypothesis, not a recommended
live risk level. Signals, selection quotas, ranking, stops and targets remain
G's retained definitions. Repeated runs do not create independent evidence.

## Execution assumptions

- Capital: INR 1,000,000 total, INR 100,000 per-entry capital ceiling, 5x
  assumed leverage, non-compounded. Both comparison arms use the same caps.
- Default uniform tick: INR 0.05, **not verified symbol/date tick metadata**.
  `--tick-size` changes a simulation assumption, not an exchange rule.
- Float32 storage noise within approximately half an ULP of a tick is snapped
  to that tick before touch checks. Genuine off-grid prices are not blindly
  rounded to the nearest tick; trigger/exit rounding remains directional.
- Flat default round-trip cost: 5 bps of entry notional. This is not an
  itemized broker/tax reconciliation. No borrowing, impact or depth model.
- Entry requires a touch after activation and no later than confirmation+10
  minutes. A one-minute delay removes the first minute; it does not extend
  the absolute deadline.
- Long entries round upward, short entries downward; adverse market exit
  rounding and gap-stop fills apply symmetrically. Stop-first resolves OHLC
  ambiguity, including the entry bar. No partial-fill/queue evidence exists.
- Both stop and target offsets are measured from the simulated fill, as in
  the G model. Explicit exit slippage treats bracket triggers as market exits.
- Capital released by an exit cannot fund another entry in the same minute,
  because intrabar ordering cannot be established from one-minute bars.
- MTM drawdown uses minute closes, not tick-level worst drawdown. Half the
  flat fee accrues at entry and half at exit. Gap losses may exceed risk budget.

Every run includes the declared model, an extra adverse 5 bps on each side,
and an extra one-minute activation delay. These are sensitivity scenarios,
not observed broker estimates. Entry and exit costs/slippage can also be
declared with CLI flags; no optimizer or multi-rule combination is provided.

## Artifacts

| Artifact | Purpose |
| --- | --- |
| `REPORT.md` | Three-way aggregate results, sensitivities, day-wise matched results, limitations and stage status |
| `experiment.json` | Hypothesis and model frozen before H outcomes; code/config/source identities and future-review requirements |
| `manifest.json` | Final completion marker and hashes of all run artifacts |
| `comparison.json` | Machine-readable metrics, baseline parity and blocked promotion gates |
| `g_exact_legacy.parquet` | Recomputed, unchanged G control ledger |
| `decision_audit.parquet` | Available candidates, upstream reasons, G-specific margins, ranks, selection and G/H execution states |
| `coverage.parquet` | Every source-eligibility date and setup, exclusions, observed counts and explicit evidence gaps |
| `market_context.parquet` | Observation-only backward-looking ATR, realized volatility, same-clock relative volume, VWAP extension, OI acceleration and observed-universe breadth |
| `trade_attribution.parquet` | Sizing-only quantity and P&L differences, keyed by signal/setup |
| `execution_model_attribution.parquet` | Legacy G versus shared-model G differences, separate from H's sizing effect |
| `validation.json` | Chronological whole-day diagnostics and exploratory day-block bootstrap |
| `<scenario>/g_trades.parquet`, `h_trades.parquet` | Paired trade-level accounting and execution states |
| `<scenario>/daily_comparison.parquet` | All declared included sessions, including zero-trade days |
| `<scenario>/g_minute_equity.parquet`, `h_minute_equity.parquet` | Minute-close MTM curves |

`DATA_MISSING`, `GATE_FAILED`, `RANKED_OUT` and `SELECTED` are distinct. The
audit never calls a failed filter a rank loss. Positive gate margins mean a
minimum gate passes (wick margin uses maximum minus observed). Existing
upstream selection fields remain available alongside the new `h_` fields.
Rank is native G rank, including its core-first preference; it is not a new
prediction score or a measurement of cross-snapshot rank stability.

Market context is recorded but cannot affect this sizing-only challenger.
ATR is a simple rolling mean of 14 five-minute true ranges; realized volatility
is the unannualized standard deviation of 20 five-minute log returns. Same-clock
volume divides by the mean of the preceding 20 observed sessions at that clock,
with at least five prior observations. OI acceleration requires contiguous
same-day/same-contract bars. Breadth is only the observed equity universe,
not verified full-exchange breadth. Missing VIX, sector, spread, quote age and
correlation exposure are null with `UNAVAILABLE`, never zero.

## Plan status and remaining work

| Plan stage | Implemented now | Remaining evidence/work |
| --- | --- | --- |
| G-0 | Frozen-source verification, per-trade exact G replay, unique runs and code/config identities | Future changes must retain the parity tests |
| G-1 | Audit all source dates/setups, missing inputs, exclusions and zero-trade days | Exact dated roster membership, raw arrival versions and complete warm-up-count proof; historical data backfill is not performed |
| G-2 | Shared whole-share/tick model, gap/ambiguity/absolute-expiry tests, cost and delay stresses, minute MTM | Verified ticks, itemized fees, real quotes/depth/partial fills and broker reconciliation |
| G-3 | Unified available-candidate attribution and numerical gate margins | First live/finalized divergence and rank stability require paired stage snapshots |
| G-4 | Causal derived context in observation mode | VIX/sector/quote/correlation datasets and original availability-time evidence |
| G-5 | One fixed sizing trial and explicit control; no optimization | Other indicator/ranking/entry/exit hypotheses need separate registered implementations |
| G-6 | Whole-day expanding diagnostic blocks (20 prior sessions, next five sessions), exploratory block bootstrap | Untouched future evidence and adequate samples; no model fitting/predictive skill claim |
| G-7 | Frozen experiment identity and review criteria | Paired H prospective collector, future sessions, independent manual approval; existing G shadow is not H evidence |

Optional `--holdout-start YYYY-MM-DD --holdout-end YYYY-MM-DD` records a
strictly future reservation in the run contract. It does **not** collect data,
prove untouchedness, enforce a global research registry or enable live
promotion. With no dates, the report states `NOT_RESERVED`. A subsequent
prospective collector must bind decisions to this exact code/config hash and
pre-session freeze before those sessions can count. Twenty sessions alone do
not establish statistical reliability. Promotion is always false in this CLI.

## Interpretation

A lower drawdown caused by smaller positions is not automatically better
selection or a higher-profit strategy. Compare H only with G on the same shared
model for the sizing effect; use exact legacy G to verify historical fidelity.
The reused historical result cannot justify replacing live G. A failed
hypothesis is a valid research result; do not tune the budget to reverse it.

## Fixed strategy trials

`strategy_trials` is a separate research run under
`C:/TradingData/eqidv2/v13_v10_h_research/strategy_trials/`. It compares each
single-rule change with a matched reference: an EMA9 or VWAP extension filter
at one completed five-minute ATR, a one-ATR stop, a signal-candle structural
stop, a completed-minute retest entry, a confirmation-extreme reclaim exit,
and observed-universe breadth. Filtered slots are not refilled. The two stop
trials use the same INR 3,000 planned-risk budget as their G reference; other
trials use the shared-model G sizing. Missing ATR retains the G rule and is
marked in `decisions.parquet`.

An optional sector cap needs a complete dated `day,tradingsymbol,sector` CSV:

```powershell
py -3.12 fno_v13_v10_h_backtest.py --experiment strategy_trials --sector-map C:/path/to/dated_sectors.csv
```

Relative strength runs only when each exact NIFTY futures bar comes from a
source matching the dataset's recorded hash. The optional VIX regime trial
requires `--vix-data` with `observed_at,available_at,india_vix` and explicit
timezone offsets. It tests a fixed VIX-below-20 hypothesis using only the
latest same-session value available by each decision. Unavailable trials are recorded
in the run and do not produce invented outcomes.

Each trial folder contains `trades.parquet`, `daily.csv`,
`last_two_weeks_stock_entries.csv` and
`last_two_weeks_stock_minutes.csv`. The latter records each executed stock's
one-minute OHLC, position size and marked P&L through the modeled exit.
`paired_daily.csv`, `trade_attribution.csv` and `decisions.parquet` show
the comparison, each stock's entry/exit changes, and filtered
signals. `results.json` identifies which sessions in the 14-calendar-day
window actually exist in the sealed source. Before 16:00 IST the window ends
on the previous calendar day. Historical trials are exploratory;
the procedure does not choose a combined or live strategy.

The root `last_two_weeks_daily_comparison.csv` and
`last_two_weeks_stock_comparison.csv` put all trials beside their G controls;
each trial's minute CSV holds the detailed path for its own executed stocks.

To extend the source beyond its current cutoff, build a new V13-v9 dataset in
a new directory, then run `tools/build_v13_v10_h_source.py` with the prior G
bundle as `--old-bundle`. The builder seals its own checksum inventory only
after verifying every overlapping G selection and result.
