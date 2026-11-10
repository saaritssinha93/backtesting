# Data freshness, earlier CVD experiments, and news feasibility

Checked 3 November 2026. This audit inspected saved source files and public primary documentation. Its numerical results are from existing completed experiments; it does not present a new news backtest or text-model validation. Machine-readable metrics, source paths and hashes are in [data_audit.json](data_audit.json).

## Newer completed market data

The G3 V18 and four CVD reports all stop on 30 October, with 62 represented sessions beginning 29 July. Their statements that no sealed November 2 source exists are now stale: a later completed G3 V17 run is available at:

`C:/TradingData/eqidv2/g3_g7_v4_through_20261102/g3_daily_20261102`

The run has `complete=true`, `state=SUCCESS`, and strategy version `V13-V10-G-3-BACKTEST-SLOT-WIN70-V17`. Its November 2 results at the saved 5 bps cost are 9 selected orders, 7 executed trades, 3 wins, 4 losses and ₹269.3223 net. The source contains the full feature ledger and selected candidate signal file, plus a sealed one-minute cash/futures/opening-evidence snapshot. All eight artifacts listed in its run manifest and the snapshot-manifest SHA-256 matched during this audit. This verifies the listed files' integrity; it does not by itself prove every raw input was available to a live strategy at its historical decision time.

The corresponding newer G3/G7 comparison is:

`C:/TradingData/eqidv2/g3_g7_v4_through_20261102/comparison_20261102_verified`

Its provenance lists 63 represented sessions through November 2. `research/g3_g7_v4_through_20261102.py` implements the extension. No completed November 3 G3 production run was found in the inspected production-run directory. November 3 must not be silently counted as a completed zero-trade day.

**November 2 coverage is restricted.** The declared universe has 218 stocks, of which 215 are included. KARURVYSYA, KAYNES and SAIL are excluded for the whole stock-day because of required futures OI gaps. The source labels this `AFTER_CLOSE_STOCK_DAY_INPUT_ELIGIBILITY` and explicitly says it is not a live causal gate. A new experiment must inherit and disclose this restriction or rebuild an independently defined causal universe. It must not use the completed flag as proof of full-universe live comparability.

Additionally, COROMANDEL, IEX, MAHABANK, OBEROIRLTY, SHREECEM, UJJIVANSFB and UNOMINDA are excluded only from the 09:20 signal for missing/invalid opening OI evidence. The source records no OI proxy. October 28 remains the earlier restricted 215/218-stock session. The historical slot policy was selected through October 27, and later sessions were already viewed in prior research; chronological evaluation on these dates is still retrospective research.

## What the CVD studies actually tested

All four families use candle-derived volume pressure. They do not contain observed buyer-initiated and seller-initiated traded volume. In particular, dividing volume into calculated buy and sell components does not recover exchange aggressor classifications, institutional orders or hidden liquidity.

The common one-minute calculation is:

```text
delta_i = volume_i * (2 * close_i - high_i - low_i) / (high_i - low_i)
delta_i = 0 for a flat candle
P_t = sum(delta over latest 15 completed minutes) / sum(volume over those minutes)
P_previous = the corresponding ratio over the preceding nonoverlapping 15 minutes
A_t = P_t - P_previous
directional P = side_sign * P_t; directional A = side_sign * A_t
side_sign = +1 for LONG, -1 for SHORT
```

The one-minute versions require 30 consecutive valid session minutes for acceleration, first available at the 09:45 signal clock. Timestamp conversion adds one minute to Kite's minute-start labels. Five-minute features have their own five-minute-start to bar-end conversion. No formula should mix those clocks.

| Family | Exact treatment | Readiness / replacement behavior | Saved finding |
|---|---|---|---|
| Kite five-minute proxy | Session sum and latest three-bar pressure; positive signed session pressure, recent pressure, both, and a body/range sensitivity | Common three-bar warmup, first signal 09:30; all gates before native ranking, replacement allowed; compare with the covered control | None passed every frozen October comparison with matched coverage. |
| Advanced one-minute CVD | Native picker divided by eligible group's positive maximum, plus `0.10 * side_sign * A/2`; second arm multiplies adjustment by `clip(RVOL20-1,0,1)` | First 09:45; unavailable features or incomplete competitor coverage leave whole group's original ranking; core and extra quota boundaries retained | Zero groups changed selections for either ranking arm. Identical profit is therefore a no-effect finding. |
| Strict one-minute CVD | Aligned `P>=0 AND A>=0`; strict `P>=0.10 AND A>=0.05`; strict+volume also `RVOL20>=1.20` | Missing required features reject; matched CVD-ready/RVOL-ready controls isolate loss of opening trades and warmup | Stricter gates reduced drawdown in some comparisons but removed substantial profit; no arm passed all frozen October criteria. |
| Relaxed one-minute CVD | Six fixed AND/OR/tolerance rules below | Native opening candidates retained before 09:45; exact 30-minute readiness required afterward; before-rank filtering permits replacements | No relaxed rule improved October profit over its availability-matched control. Prior-month selection retained the control for September and October. |

The existing RVOL20 is the current 15-minute volume divided by the mean of the same clock window across exactly 20 strictly prior observed stock sessions. It is not a median five-minute RVOL and is not an exchange-calendar denominator. Missing historical clocks disable the reading; source days with no observations are absent from the disclosed stock-session calendar. The advanced ranking's acceleration is divided by two, while the strict and relaxed threshold rules use the raw difference. Reusing the wrong scale would change the experiment.

The relaxed rules use signed P and A:

| Rule | Eligibility after warmup |
|---|---|
| Relaxed AND | `P >= 0 AND A >= -0.05` |
| Either nonnegative | `P >= 0 OR A >= 0` |
| Either soft | `P >= -0.05 OR A >= -0.05` |
| Opposing-pressure veto | `P > -0.10 OR A > -0.05` |
| Strong opposing-pressure veto | `P > -0.15 OR A > -0.10` |
| Pressure tolerance | `P >= -0.10` |

The two opposition vetoes reject only when both measures breach their opposing thresholds. No OR branch can waive data readiness. Native G3 eligibility still runs, so these studies do **not** test relaxing original EMA, OI, body or volume gates.

The source code and methodologies are under `g3_v18_20261102/cvd_kite_proxy_20261102`, `cvd_advanced_20261102`, `cvd_strict_20261102` and `cvd_relaxed_20261102`. Each family's `results_20261102/summary.json` is separately hashed in the audit JSON. Across the four reports there are 23 named rows including reused controls: 15 CVD treatment variants and eight control rows. This is existing research, additional to the current lab's newly attempted experiments.

## Existing October CVD results

These are rounded saved results at 5 bps over the same 19 represented October sessions. DD is daily realized drawdown, not intraday open-position drawdown. Compare each treatment to its specified readiness control before attributing an effect to CVD.

| Experiment / variant | Trades | Win % | Net ₹ | Daily DD ₹ | Correct reference |
|---|---:|---:|---:|---:|---|
| Original G3 | 112 | 66.96 | 165,581 | 25,645 | — |
| Five-minute covered control | 106 | 66.98 | 144,377 | 25,645 | Original for coverage cost |
| Five-minute session CVD | 96 | 67.71 | 133,806 | 20,500 | Covered control |
| Five-minute recent-15 CVD | 101 | 68.32 | 144,149 | 29,145 | Covered control |
| Five-minute both | 92 | 69.57 | 137,728 | 22,645 | Covered control |
| Five-minute body sensitivity | 94 | 64.89 | 114,698 | 21,355 | Covered control |
| Advanced acceleration rank | 112 | 66.96 | 165,581 | 25,645 | Original; no substitution |
| Advanced acceleration × RVOL rank | 112 | 66.96 | 165,581 | 25,645 | Original; no substitution |
| CVD-ready control | 101 | 69.31 | 165,398 | 19,145 | Original for warmup cost |
| RVOL-ready control | 101 | 69.31 | 165,398 | 19,145 | Original for warmup cost |
| Aligned CVD gate | 76 | 69.74 | 94,933 | 16,000 | CVD-ready |
| Strict CVD gate | 61 | 70.49 | 88,017 | 11,102 | CVD-ready |
| Strict CVD + RVOL | 46 | 67.39 | 46,391 | 14,750 | RVOL-ready |
| Relaxed availability control | 112 | 66.96 | 165,581 | 25,645 | Original; same selections |
| Relaxed AND | 87 | 65.52 | 93,703 | 22,500 | Relaxed availability |
| Either nonnegative | 97 | 68.04 | 130,862 | 22,500 | Relaxed availability |
| Either soft | 101 | 66.34 | 122,681 | 22,500 | Relaxed availability |
| Opposition veto | 105 | 66.67 | 128,853 | 25,645 | Relaxed availability |
| Strong opposition veto | 108 | 67.59 | 141,837 | 25,645 | Relaxed availability |
| Pressure tolerance | 105 | 66.67 | 128,853 | 25,645 | Relaxed availability |

The relaxed control has three losing October days. Relaxed AND has seven; either-nonnegative, either-soft, opposition-veto and pressure-tolerance each have six; strong-opposition-veto has three. The saved chronological calibration chooses `available_control` for both September and October because no rule passed the predeclared prior-period constraints. All history was already viewed, so this is not independent model selection evidence.

## News and earnings data: what is available

No timestamped announcement/news/event corpus was found by the scoped filename inventory of `configs`, `research`, `outputs`, `g3_v18_20261102`, or event/news-related top-level directories under `C:/TradingData/eqidv2`. This is an inventory result, not a claim that no external vendor can provide the data. The current price/OI ledgers cannot tell us whether a stock had no material news or whether news coverage is unknown.

Public primary sources demonstrate that announcements and scheduled-event records exist for the research period. They do not establish complete, revision-preserving coverage for every historical F&O member, or the time this trading system could actually have received the text. An example is the [NSE-hosted Swiggy October filing](https://nsearchives.nseindia.com/corporate/ixbrl/INTEGRATED_FILING_INDAS_203206_22102026184058_iXBRL_WEB.html). Its presence and dated filename are not proof of contemporaneous ingestion. A [Swiggy issuer archive](https://www.swiggy.com/corporate/governance/corporate-announcements/) also lists July/August notices and results, but a present-day archive alone cannot supply the missing receipt history.

| Source | Verified useful capability | Timing and price evidence | Decision |
|---|---|---|---|
| NSE announcement website | Symbol, subject, attachment and broadcast/received/dissemination information; date filtering includes custom range and CSV export | An archive can support reconstruction after completeness and revision checks; it does not record our historic arrival time | Suitable acquisition candidate; coverage for this full study is unverified. [Official page](https://www.nseindia.com/companies-listing/corporate-filings-announcements) |
| NSE board-meeting page | Meeting purpose, scheduled date and broadcast timestamp | The meeting date and the date the schedule became public are separate fields | Suitable for scheduled-results flags, using only notices known before decision time. [Official board meetings](https://www.nseindia.com/companies-listing/corporate-filings-board-meetings) |
| NSE online corporate feed | Event-driven corporate/quick-result packets | Official page lists ₹10,60,000 domestic for corporate data and dedicated leased-line delivery; connectivity, contract scope, taxes and current quote need verification | Technically relevant; purchase/integration not justified by existing performance evidence. [NSE products](https://www.nseindia.com/static/market-data/corporate-data-subscription) |
| NSE EOD corporate feed | Corporate/quick-result files and attachments via SFTP | Official page lists ₹5,00,000 domestic per annum and daily availability after 20:00 IST | Can serve next-session context. Same-day intraday use requires a separately justified earlier public event timestamp and explicit modeled receipt assumption, not EOD availability. [NSE products](https://www.nseindia.com/static/market-data/corporate-data-subscription) |
| NSE RSS | Latest corporate information updates | NSE discloses that reader/provider delays can occur; no deterministic latency bound or guaranteed historical replay was established | Useful prospective capture option if terms permit; record first seen time. [NSE RSS documentation](https://www.nseindia.com/static/rss-feed) |
| BSE corporate data | Official data portal advertises corporate API covering disclosures, financials, announcements/actions | Direct portal and announcement-page access returned HTTP403 in this audit; primary search index exposed product description | Product exists; exact historical revisions, latency SLA and full-period completeness were not verified. [Official data portal](https://marketdata.bseindia.com/) |
| BSE historical tariff | Corporate announcements, results calendar and historical corporate-data products | Located tariff is explicitly February2025: announcements ₹9 lakh continuous/₹5 lakh EOD annually, results calendar ₹3 lakh, taxes extra | Indicative old pricing only; do not present this as a November2026 binding quote. [BSE tariff](https://www.bseindia.com/downloads1/Information_Products_Pricing_Sheet.pdf) |

The NSE online schema includes sequence number, symbol, description, details, attachment and an announcement date with minute precision (`DDMMYYYYHHmm`). That precision matters for a one-minute strategy: an event somewhere in 10:10 must not be assumed known at 10:10:00. Packet arrival and processing completion should supply the decision clock. The EOD schema has a similar announcement-time field but a different delivery time. Neither field establishes this system's historical receipt. [Online schema](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Download_Technical_Specification_Corporate_Data.pdf), [EOD schema](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/EOD_DATA-Corporate_EODv1.2.pdf)

The online specification also distinguishes announcement/quick-result packets from structured company-results and distribution-schedule data delivered at end of day. Buying an online corporate product does not make every financial field contemporaneously available. The linked specification is version 2.4 dated December 19, 2024; product-page prices were rechecked during this audit, while historical tariff dates remain explicit. [NSE technical specification](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Download_Technical_Specification_Corporate_Data.pdf)

NSE's usage policy makes intended use, handling and redistribution dependent on the applicable agreement. It does not grant unrestricted commercial reuse merely because a file is viewable publicly. The lab neither purchased a subscription nor automated bulk extraction. Any acquisition should establish research/non-display use and retention rights under the chosen product or vendor agreement. [NSE data policy](https://www.nseindia.com/static/market-data/nse-data-policy)

The BSE February2025 tariff explicitly separates member-website redistribution fees from the corporate-feed fees. A ₹75,000 member-website entry is not the continuous corporate-announcement feed price. No full-period BSE quote, access contract or source delivery history was available locally.

## Required event schema and leakage controls

Record one append-only version per source event with:

- `source`, `event_id`, `revision_id`, original attachment/text hash and source URL.
- ISIN, exchange symbol, dated symbol mapping and F&O membership at the decision date; retain both exchanges' identifiers when deduplicating.
- Event category and materiality confidence, with `unknown` available; original raw text is retained.
- `exchange_received_at`, `exchange_disseminated_at`, `vendor_received_at` where provided, `our_first_seen_at`, `processing_finished_at` and `retrieved_at_now` as distinct fields.
- `scheduled_event_at` for announced future results/meetings, with notice publication history and cancellation/rescheduling versions.
- `available_at` and the explicit rule producing it; real capture uses actual receipt/processing, reconstructed research uses documented dissemination plus conservative modeled delay.
- Coverage status per source, symbol and interval: `observed_event`, `confirmed_no_event_in_complete_interval`, or `coverage_unknown`. A missing row alone is not confirmed no news.

For a replayed decision at t, require `available_at <= t` and only text versions available by t. With minute-only exchange timestamps, use a conservative end-of-minute availability plus processing/latency assumption. Do not assign a later revised PDF to its original notice time. Do not use a result period-end date, meeting date or attachment filename as the dissemination timestamp. Scheduled earnings on a future date become known when the schedule notice is received, not when a later calendar is downloaded.

An event initially obtained through an EOD product can be safely used after that product's receipt for the next session. A reconstructed same-day event study is feasible only if its public dissemination history and content version are independently verified and latency is explicitly modeled. It cannot be labeled a replay of actual live receipt. Test at least several plausible delays before trusting a one-minute response.

Deduplicate repeated NSE/BSE/issuer copies by source IDs and content hashes while preserving the earliest independently verified available version. Entity matching must handle renamed symbols and avoid news about a similarly named company. Corporate actions need separate price-adjustment and eligibility treatment; adding a sentiment score does not repair a split-induced apparent return.

## Which news experiments are feasible now?

| Requested comparison | Status | Reason |
|---|---|---|
| Event flags as diagnostics for every historical candidate | Not run | Complete dated-universe event corpus with coverage states is missing. Selected web examples are not representative coverage. |
| Predeclared material-news pause | Not run | Cannot determine historical event availability for all candidates without inventing latency/absence. |
| Event-aware ranking or sizing | Not run | Same missing as-of corpus; attaching news only to known losers would bias selection. |
| FinGPT or alternative event classifier | Not run | No exact archived text corpus, manually labeled Indian-stock validation sample, versioned model inference or usable as-of labels were available. |

These are explicit data limitations, not evidence that news cannot help. A text classifier may organize announcements once the corpus exists. Generic sentiment is not a validated trade-direction signal; a favorable result headline can still fall short of market expectations. The earliest useful target is event type/materiality, with abstention, rather than positive/negative trading advice.

The smallest useful next news experiment is a dated, coverage-audited scheduled-results flag. Acquire the actual schedule notices for every included stock/session over a small fixed evaluation period, preserve their earlier broadcasts and revisions, and manually inspect a fixed sample selected without seeing returns. Then compare unchanged trading plus diagnostic tags, a fixed event pause, and one bounded ranking penalty against the same control. Freeze the pause and penalty before inspecting that period's P&L. If only reconstructed timestamps are available, report separate latency scenarios instead of claiming measured receipt.

For future classifier validation, have reviewers label source text for results/guidance, corporate actions, financing, legal/regulatory, routine administrative and uncertain categories; retain disagreements. Measure event-category precision/recall, material-event false negatives, abstention and performance by category. A profitable backtest cannot replace checking whether the model read the announcement correctly.

## Research consequences

The new November 2 sealed source can extend the market-data study by one session, with its restrictions disclosed. The older CVD evidence should be used as prior negative/no-effect evidence rather than rerunning the same gate as if it were new. New candle-pressure features must demonstrate value beyond body/wick/volume information, and a weak weight that changes no selection cannot demonstrate an improved strategy.

The new lab additionally found that October 28, 29, 30 and November 2 ledgers contained only the 28 active G3 signal clocks. Seven missing donor clocks have now been reconstructed from their hash-verified sealed snapshots for the `all51_slots` ablation only. All original rows and stock cohorts remain intact. The [supplement methodology and coverage proof](expanded_slots/README.md) documents 6,034 additional stock-clock rows, exact OI timing, seeded native EMA arithmetic and parity across 23,274 existing non-opening rows. This supplies the full donor-clock pool for the retained cohorts; it does not erase their after-close universe restrictions.

News is currently **TEST NEXT / INSUFFICIENT DATA FOR P&L**. The defensible immediate work is validating available price/volume/OI selection variants with the native replay engine, while establishing a timestamped event corpus before making any news-performance claim.
