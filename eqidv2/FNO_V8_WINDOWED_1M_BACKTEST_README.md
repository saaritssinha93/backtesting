# FNO V8 Windowed One-Minute Backtest

## What was implemented

The independent backtest is in:

`fno_v8_windowed_1m_entry_backtest.py`

It does not import the V6/V7 strategy modules, their caches, the legacy sweep
builder, the legacy replay engine, or the live configuration. It owns:

- a literal, hash-locked copy of the ten V6 five-minute setup legs;
- exact one-minute-to-five-minute cash aggregation and futures-OI joining;
- a hash-locked 2026 regular-session calendar from NSE F&O and cash-market
  circulars, including the 15-Jan holiday amendment and the full regular
  Union Budget session on Sunday 1-Feb;
- timestamped, same-session OHLCV paths;
- the sequential V8 confirmation/order state machine;
- adverse gap entry, tick rounding, actual-fill brackets, and stop-first
  same-bar treatment;
- setup-cap reassignment during the S+1..S+5 window;
- a global pending-margin/concurrency/duplicate-symbol ledger with a
  conservative no-backfill rule after a portfolio rejection;
- V8-only cache, run, report, event, diagnostic-breakdown, and provenance
  artifacts.

## Implemented entry variants

- `B0`: strict confirmation at S+1 only, raw high/low break, expiry S+5.
- `B1`: first strict confirmation during S+1..S+4, raw break.
- `B2`: B1 with a 2-bps trigger buffer.
- `B3`: B1 with a 5-bps trigger buffer.
- `B4`: B2 with five-minute-midpoint pre-confirmation invalidation.
- `B5`: B4 with directional close-location of at least 0.75.

For all variants, the confirmation candle cannot fill itself. A pending order
may fill only on a later one-minute bar and must fill by S+5. A completed close
back through the five-minute signal close cancels an unfilled order.

## Important research status

V8 is independent and runnable, but it is deliberately marked **not promotion
eligible**. The available historical inputs do not yet provide:

- daily point-in-time F&O universe membership for the full history;
- point-in-time rolling near-month futures OI;
- certified one-minute row-lineage flags in the legacy cash files;
- complete 15:30 bars for every recent session;
- prospective evidence of at least 20 new sessions and 100 fills.

The current prototype therefore executes NSE cash equity and uses a static
26AUG future only for OI. That is useful for entry-logic research, not a claim
of historical rolling-futures performance.

## Commands

Every run requires an explicit physical source-snapshot manifest. There is no
silent V6/V7 cache or live-directory fallback.

The requested date window must fall inside 2026, the year covered by the
embedded exchange calendar. Expected sessions are generated from that frozen
calendar—not inferred from available price files. Every selected symbol must
have the exact 09:16–15:30 cash one-minute grid and the six required futures-OI
signal slots on every expected session. A market-wide missing day, an off-grid
row, invalid OHLCV/OI, or an unexpected holiday session makes the headline
fail closed.

Build a cache:

```powershell
python fno_v8_windowed_1m_entry_backtest.py build-cache `
  --source-snapshot <manifest.json> `
  --from-day 2026-05-27 --through-day 2026-07-31
```

Run B4 at the conservative 15-bps cost assumption:

```powershell
python fno_v8_windowed_1m_entry_backtest.py run `
  --source-snapshot <manifest.json> `
  --from-day 2026-05-27 --through-day 2026-07-31 `
  --variant B4 --cost-bps 15 `
  --eod-policy EXACT_SQUARE_OFF --square-off 15:30
```

Run the fixed two-day source/chronology smoke test:

```powershell
python fno_v8_windowed_1m_entry_backtest.py smoke `
  --source-snapshot <manifest.json>
```

Validate an immutable run:

```powershell
python fno_v8_windowed_1m_entry_backtest.py validate `
  --provenance <run-directory>\provenance.json
```

`LAST_REAL_BAR_SENSITIVITY` is available only as an explicitly named
sensitivity. It must not be presented as an exact 15:30 result.

## Current research boundary

The runnable registry currently covers B0 through B5. B6 one-minute-volume
and B7 market/sector-context variants are deliberately not filled in with
uncertified inputs. No previous-ten-bar ratios, sector attribution, or
post-hoc liquidity/OI/volume/volatility buckets are fabricated.

The candidate/order audit is self-contained for B0-B5: it includes the full
five-minute OHLCV/EMA/OI context, picker rank and setup cap, every attempted
confirmation candle with ordered rejection codes and morphology, trigger and
entry timing, execution economics, position notional, and portfolio usage.
Post-fill MFE/MAE are emitted as side-normalized one-minute-OHLC lower and
upper bounds. Entry/exit-bar extremes contribute only to the upper bound
unless the position is known to span that whole bar; ambiguity flags preserve
the limitation instead of inventing an intrabar price order.

Each run also writes `diagnostic_breakdowns.csv` for side, setup, signal slot,
confirmation minute, entry minute, buffer, symbol, official-calendar
five-session block, and gap-fill status. The Markdown report carries compact
versions of those tables. The breakdown artifact and all diagnostic
conventions are versioned, fingerprinted, hashed, and provenance-validated.

## Artifact isolation

V8 writes only under:

- `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1`
- `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_v8_windowed_1m_research.md`

Each cache key includes the V8 source-code hash, setup hash, data/path policy,
universe hashes, source-snapshot fingerprint, source-inventory fingerprint,
official-calendar hash and expected sessions, date window, and selected symbol
set. Each run additionally fingerprints the variant, timing, cost, slippage,
EOD, and portfolio policies.

## Next research sequence

1. Obtain daily point-in-time universes and rolling near-month futures/OI.
2. Repair or refetch exact 15:30 cash rows and certify one-minute lineage.
3. Rebuild B0 on the clean data contract.
4. Compare B1 windows separately for LONG and SHORT.
5. Test buffers 0/2/5 bps at 15-bps cost.
6. Add midpoint, close-location, volume, and range filters one at a time.
7. Freeze the winning rule and begin prospective shadow collection.
8. Consider promotion only after every criterion in the research plan passes.
