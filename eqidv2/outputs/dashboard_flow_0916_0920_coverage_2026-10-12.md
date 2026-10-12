# Dashboard Flow opening OI coverage through 12 Oct 2026

At 09:20, the displayed **OI Δ 5m %** and **OI since 09:16 %** use the same validated, same-contract first-bar OI comparison. A genuine 09:15–09:16 futures minute gives a four-minute 09:16→09:20 comparison. A future live session can use a timely full quote near 09:16 as a clearly labelled **provisional quote** baseline over its actual quote-to-09:20 interval. Later slots retain adjacent five-minute OI changes. **OI day %** continues to use verified previous-session OI.

| Historical dated-universe audit | Stock-date rows |
| --- | ---: |
| 39 trading sessions, 10 Aug–12 Oct 2026 | 8,194 |
| Valid genuine 09:16 OI and completed 09:20 OI | 2,284 |
| No genuine first-minute OI observation | 5,841 |
| First-minute OI is zero and cannot be a percentage denominator | 69 |

The API equality check covered five representative dates: 10 Aug, 31 Aug, 30 Sep, 9 Oct and 12 Oct. All **1,032 valid displayed rows** had equal `oi_change_pct`, `oi_change5m_pct`, and `opening_context.oi_change_since_0916_pct`; no mismatch was found. This was a sample of the dashboard loader, while the source-coverage count above covers all 39 trading sessions.

There is no saved dated contract universe for 28 Jul–7 Aug, so the dashboard cannot safely assert an all-stock, same-contract result for those dates. A universe file exists for the 14 Sep market holiday and was excluded from the trading-session count. On 12–14 Aug, the historical observer archives contain no first-minute candles because the old contract tokens could not be verified against the current broker instrument list. Existing local minute archives do not supply those missing 09:16 values; a 09:20 five-minute bar cannot reconstruct them. Missing percentages remain blank.

One 10 Aug record for cash symbol **LTIM** was stored under its futures underlying **LTM**. The dashboard now accepts that explicit alias only when the dated contract is exactly an LTM monthly future, recovering its genuine 09:16 OI; arbitrary symbol mismatches remain rejected.

The scheduled live observer starts at 08:55 IST and runs to 09:21. For each dated-universe stock it will save either a qualifying 09:16 full quote with source/timestamps or an explicit missing reason. The scheduled task is enabled for 13 Oct 2026; the new provisional quote path has passed synthetic tests but has not yet been observed through a live market opening. A completed 09:20 bar's receipt time must be verified separately before any future 09:20 trading setup uses this research feature. The active G-3 strategy setup times and order logic were not changed.
