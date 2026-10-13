# Options V13-V10-G-3 Paper Net Result

- Session date (IST): `2026-10-13`
- Mode: **PAPER**
- Mapping: equity LONG -> ATM CE BUY; equity SHORT -> ATM PE BUY
- Size: **exactly one exchange lot per filled equity entry**
- Premium stop / target: `30.0%` / `40.4%`; square-off `15:15`
- Full-premium account: `Rs 1,500,000.00`; adverse fill slippage: `10 bps`
- Trades: `5`; open: `4`; closed: `1`; net: `Rs 7,053.59`

| Equity | Side | Option | Qty | Equity fill time | Option fill time | Entry | Stop | Target | Exit | Status | Net P&L |
|---|---|---|---:|---|---|---:|---:|---:|---:|---|---:|
| GVT&D | SHORT | GVT&D26OCT4100PE | 125 | 2026-10-13T09:41:53+05:30 | 2026-10-13T09:41:53+05:30 | 102.45 | 71.75 | 143.85 | 0.00 | OPEN | -27.66 |
| INFY | LONG | INFY26OCT1040CE | 400 | 2026-10-13T09:47:25+05:30 | 2026-10-13T09:47:26+05:30 | 34.50 | 24.15 | 48.45 | 0.00 | OPEN | 818.35 |
| HCLTECH | LONG | HCLTECH26OCT1280CE | 400 | 2026-10-13T10:06:11+05:30 | 2026-10-13T10:06:13+05:30 | 25.40 | 17.80 | 35.70 | 35.85 | CLOSED | 4100.68 |
| LICI | LONG | LICI26OCT395CE | 1400 | 2026-10-13T10:16:13+05:30 | 2026-10-13T10:16:14+05:30 | 6.65 | 4.70 | 9.35 | 0.00 | OPEN | -1396.72 |
| ADANIPOWER | SHORT | ADANIPOWER26OCT185PE | 3550 | 2026-10-13T10:16:08+05:30 | 2026-10-13T10:16:08+05:30 | 5.15 | 3.61 | 7.24 | 0.00 | OPEN | 3558.93 |

Paper safeguards require the exact G strategy fingerprint, an actual filled equity state, a same-day NFO master, deterministic monthly ATM mapping, one-lot quantity, fresh two-sided NFO depth, bounded spread, traded volume, full-premium cash, atomic state writes, and credential failover. No broker order method is called.
