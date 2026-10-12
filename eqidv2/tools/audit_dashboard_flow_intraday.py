"""Read-only, exact-slot audit of retained Dashboard Flow cash and OI inputs.

Reads each source file once, writes only a new audit directory. Missing broker
bars remain missing. Reconstructed historical universe membership is labelled.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard_flow_live_data import _cash_valid, _cash_sources, _universe, _contract_stem
from nse_market_calendar import market_closed_reason

FLAGS = ('gap_filled', 'is_gap_filled', 'opening_snapshot', 'provisional_stale',
         'is_synthetic', 'synthetic', 'forward_filled', 'is_filled', 'interpolated',
         'is_interpolated', 'synthetic_repair', 'repaired_synthetic')


def _ist(values):
    values = pd.to_datetime(values, errors='coerce')
    return values.dt.tz_localize('Asia/Kolkata') if values.dt.tz is None else values.dt.tz_convert('Asia/Kolkata')


def valid_rows(frame, *, minute=False, oi=False):
    """Reject duplicates and flagged/nonfinite bars; do not manufacture slots."""
    if frame.empty:
        return frame.copy()
    cols = ['open', 'high', 'low', 'close', 'volume'] + (['oi'] if oi else [])
    numeric = frame.reindex(columns=cols).apply(pd.to_numeric, errors='coerce')
    good = pd.Series(np.isfinite(numeric.to_numpy(dtype=float)).all(axis=1), index=frame.index)
    good &= numeric[['open', 'high', 'low', 'close']].gt(0).all(axis=1)
    good &= numeric.volume.ge(0) & numeric.high.ge(numeric[['open', 'close']].max(axis=1))
    good &= numeric.low.le(numeric[['open', 'close']].min(axis=1))
    good &= ~frame.ts.duplicated(keep=False) & frame.ts.notna()
    for flag in FLAGS:
        if flag in frame:
            values = frame[flag].fillna('').astype(str).str.strip().str.lower()
            good &= values.isin(['', '0', '0.0', 'false', 'no', 'none'])
    if 'quality_state' in frame:
        good &= frame.quality_state.fillna('' if oi else 'VALID').astype(str).str.upper().eq('VALID')
    if 'source' in frame:
        good &= ~frame.source.fillna('').astype(str).str.upper().str.contains('SYNTHETIC|GAP.?FILL|FORWARD.?FILL|INTERPOLAT|PROXY|CARRY.?FORWARD', regex=True)
    if oi:
        good &= numeric.oi.ge(0)
        if 'candle_start' in frame:
            good &= frame.ts.sub(_ist(frame.candle_start)).eq(pd.Timedelta(minutes=5))
    clean = frame.loc[good].copy()
    if not oi:
        clean = _cash_valid(clean, minute=minute)
    return clean


def windows(values, step):
    values = sorted(values)
    ranges = []
    for value in values:
        if ranges and value == ranges[-1][1] + step:
            ranges[-1][1] = value
        else:
            ranges.append([value, value])
    clock = lambda value: f'{value // 60:02}:{value % 60:02}'
    return ';'.join(clock(a) if a == b else f'{clock(a)}-{clock(b)}' for a, b in ranges)


def scan_file(path, start, end, *, minute=False, oi=False):
    if not path.is_file():
        return {}, 'MISSING_FILE'
    try:
        schema = pq.ParquetFile(path).schema_arrow
        timestamp = next(c for c in ('date', 'timestamp', 'datetime') if c in schema.names)
        cols = [c for c in (timestamp, 'candle_start', 'open', 'high', 'low', 'close', 'volume', 'oi',
                            'source_1m_count', 'quality_state', 'source', *FLAGS) if c in schema.names]
        lo, hi = pd.Timestamp(start), pd.Timestamp(end + timedelta(days=1))
        if getattr(schema.field(timestamp).type, 'tz', None):
            lo, hi = lo.tz_localize('Asia/Kolkata'), hi.tz_localize('Asia/Kolkata')
        frame = pd.read_parquet(path, columns=list(dict.fromkeys(cols)), filters=[(timestamp, '>=', lo), (timestamp, '<', hi)])
        frame['ts'] = _ist(frame[timestamp])
        frame = frame.sort_values('ts').reset_index(drop=True)
        clean = valid_rows(frame, minute=minute, oi=oi)
        groups = {day: part for day, part in clean.groupby(clean.ts.dt.date)}
        result = {}
        for day, part in frame.groupby(frame.ts.dt.date):
            valid = groups.get(day, clean.iloc[:0])
            stamps = valid.ts
            # Offset includes seconds so off-grid seconds cannot pass as a minute.
            offsets = (stamps - stamps.dt.normalize()).dt.total_seconds() / 60
            slots = set(int(v) for v in offsets if v == int(v))
            result[day] = dict(slots=slots, rows=len(part), rejected=len(part)-len(valid),
                               duplicates=int(part.ts.duplicated(keep=False).sum()))
        return result, 'READ'
    except Exception as exc:
        return {}, 'READ_ERROR_' + type(exc).__name__


def audit(root, start, end, output, *, now=None):
    now = pd.Timestamp.now(tz='Asia/Kolkata') if now is None else pd.Timestamp(now)
    now = now.tz_localize('Asia/Kolkata') if now.tzinfo is None else now.tz_convert('Asia/Kolkata')
    days = [t.date() for t in pd.date_range(start, end) if not market_closed_reason(t.date()) and t.date() <= now.date()]
    universes = {day: _universe(root, day) for day in days}
    symbols = sorted({item['symbol'] for rows, _ in universes.values() for item in rows})
    memberships = {symbol: {day: item['contract'] for day, (rows, _) in universes.items() for item in rows if item['symbol'] == symbol} for symbol in symbols}
    output.mkdir(parents=True, exist_ok=False)
    file_errors, detail = [], []

    def inspect(symbol):
        records, errors = [], []
        sources = []
        for path, label, minute in _cash_sources(root, symbol):
            rows, status = scan_file(path, start, end, minute=minute)
            sources.append((label, minute, rows, str(path)))
            if status.startswith('READ_ERROR'):
                errors.append(dict(path=str(path), status=status))
        contracts = {}
        for contract in set(memberships[symbol].values()):
            path = root / 'fno_oi/raw_contracts_5m' / (_contract_stem(contract) + '_5minute.parquet')
            contracts[contract], status = scan_file(path, start, end, oi=True)
            if status.startswith('READ_ERROR'):
                errors.append(dict(path=str(path), status=status))
        for day, contract in memberships[symbol].items():
            # Give the live collector one full slot grace at the time of audit.
            cutoff = 930 if day < now.date() else min(930, (now.hour*60+now.minute)//5*5-5)
            expected5 = set(range(560, cutoff+1, 5))
            expected1 = set(range(556, cutoff+1))
            cash5, cash1 = set(), set()
            for label, minute, data, source_path in sources:
                row = data.get(day, {})
                slots = row.get('slots', set())
                expected = expected1 if minute else expected5
                accepted = slots & expected
                missing = expected - accepted
                records.append(dict(day=str(day), symbol=symbol, contract=contract, source=label,
                    source_path=source_path,
                    universe_source=universes[day][1], expected=len(expected), valid=len(accepted),
                    missing=len(missing), missing_ranges=windows(missing, 1 if minute else 5),
                    critical_missing=len({x for x in missing if x <= 915}), rejected=row.get('rejected', 0),
                    duplicates=row.get('duplicates', 0)))
                if minute:
                    cash1 |= accepted
                    cash5 |= {slot for slot in expected5 if set(range(slot-4,slot+1)) <= slots}
                else:
                    cash5 |= accepted
            oirow = contracts[contract].get(day, {})
            oislots = oirow.get('slots', set()) & expected5
            for label, slots, expected in [('EFFECTIVE_CASH_1M', cash1, expected1), ('EFFECTIVE_CASH_5M', cash5, expected5), ('EXACT_CONTRACT_OI_5M', oislots, expected5)]:
                missing = expected - slots
                records.append(dict(day=str(day), symbol=symbol, contract=contract, source=label,
                    universe_source=universes[day][1], expected=len(expected), valid=len(slots),
                    missing=len(missing), missing_ranges=windows(missing, 1 if label.endswith('_1M') else 5),
                    critical_missing=len({x for x in missing if x <= 915}),
                    rejected=oirow.get('rejected',0) if label.startswith('EXACT') else 0,
                    duplicates=oirow.get('duplicates',0) if label.startswith('EXACT') else 0))
        return records, errors

    with ThreadPoolExecutor(max_workers=2) as pool:
        for index, (records, errors) in enumerate(pool.map(inspect, symbols), 1):
            detail.extend(records)
            file_errors.extend(errors)
            if index % 25 == 0:
                print(f'Audited {index}/{len(symbols)} stocks', flush=True)
    frame = pd.DataFrame(detail)
    frame.to_csv(output / 'symbol_session_coverage.csv', index=False)
    summaries = []
    for (day, source), part in frame.groupby(['day', 'source']):
        # Alias fallback files are retained separately in CSV. Summary coverage
        # uses only union-derived EFFECTIVE series or unique OI contracts;
        # raw sources are explicitly file counts, never stock counts.
        summaries.append(dict(day=day, source=source, stocks=len(part), expected=int(part.expected.sum()),
            count_unit='stock' if source.startswith(('EFFECTIVE_', 'EXACT_')) else 'source_file',
            valid=int(part.valid.sum()), missing=int(part.missing.sum()),
            complete_stocks=int(part.missing.eq(0).sum()), critical_missing=int(part.critical_missing.sum()),
            critical_complete_stocks=int(part.critical_missing.eq(0).sum())))
    report = dict(schema='flow_intraday_quality_audit_v1', as_of=now.isoformat(), start=str(start), end=str(end),
        sessions=len(days), symbols=len(symbols), data_root=str(root),
        universe_sources=dict(Counter(kind for _, kind in universes.values())),
        caveats=['Current day cutoff gives collector one completed five-minute slot grace.',
                 'Today historical cash stores are EOD outputs, not live-feed failures.',
                 'Raw source summaries count files; alias fallback files are separate CSV rows.',
                 'Registry-reconstructed membership is not proof of the original dated universe.',
                 'Critical window means market open through15:15; indicator warmup is not certified.',
                 'This audit does not alter raw data, strategy evidence, orders or backtest results.'],
        file_errors=file_errors, sessions_by_source=summaries)
    (output/'summary.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    lines = ['# Intraday data quality audit', '', f'As of {now.isoformat()}. Range {start} to {end}.', '',
             f'{len(days)} exchange sessions; {len(symbols)} distinct cash symbols.', '',
             'Exact completed slots only. Daily reference coverage is checked separately.', '',
             '| Session | Source | Complete stocks | Total stocks | Missing bars | Missing through15:15 |',
             '|---|---|---:|---:|---:|---:|']
    for row in summaries:
        if row['source'] in ('EFFECTIVE_CASH_5M','EXACT_CONTRACT_OI_5M'):
            lines.append(f"| {row['day']} | {row['source']} | {row['complete_stocks']} | {row['stocks']} | {row['missing']} | {row['critical_missing']} |")
    lines.extend(['', *report['caveats'], '', 'See symbol_session_coverage.csv for exact missing windows and rejected-row counts.'])
    (output/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='sessions_by_source'}, indent=2), flush=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root', type=Path, default=Path('C:/TradingData/eqidv2'))
    parser.add_argument('--start', type=date.fromisoformat, default=date(2026,5,27))
    parser.add_argument('--end', type=date.fromisoformat, default=date.today())
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    audit(args.data_root, args.start, args.end, args.output)
