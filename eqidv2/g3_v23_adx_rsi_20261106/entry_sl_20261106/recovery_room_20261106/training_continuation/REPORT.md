# Native-stop continuation: first44 sessions only

This diagnostic inspects prices after the baseline had already stopped out. It does not alter entries or create a rescue rule. Only sessions through5October2026 are inspected; no validation or final-seven outcomes select a threshold.

| Native exit | Stopped trades | Later target touch | Later target close | Later close above cost break-even | Median minutes to target (upper bound) | Median later adverse move |
|:--|--:|--:|--:|--:|--:|--:|
| all_native_stops | 44 | 2 | 2 | 6 | 150.0 | 1.528% |
| STOP | 25 | 2 | 2 | 5 | 150.0 | 1.852% |
| TIGHTENED_STOP | 19 | 0 | 0 | 1 | nan | 1.351% |

The continuation excludes the native exit bar, whose remainder cannot be ordered from one-minute OHLC. Target touch time is reported as a one-minute interval. Adverse excursion through the first target bar is an upper bound: its high/low may occur after the target touch. Barrier-order flags apply only to the remaining full bars; an excluded native exit bar may already have crossed a wider stop.

The four planned full-entry replays were declared before this diagnostic: original1.25%→1%after120minutes, constant1.25%,1.25%→1%after240minutes, and constant1.50%. They must be replayed for every entry with correct stop-first and gap handling. These diagnostic recoveries must not select individual trades or be counted as profitable rescues.

[Summary](summary.csv) · [Every stopped trade](native_stop_continuation.csv) · [Configuration](config.json)
