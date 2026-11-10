"""Read-only Kite native five-minute history for the isolated CVD study.

Kite dates are candle starts. Stored ``bar_end`` is start plus five minutes.
Every run freezes its completed-bar cutoff and records historical membership.
Only files below this new experiment directory may be written.
"""
from __future__ import annotations

import sys
sys.dont_write_bytecode = True

import argparse
import contextlib
from datetime import date, datetime, timedelta
import hashlib
import io
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = Path(__file__).resolve().parent
DATA = Path('C:/TradingData/eqidv2')
IST = 'Asia/Kolkata'
FIELDS = ['open', 'high', 'low', 'close', 'volume']
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked_output(path: Path) -> Path:
    out = path.resolve()
    if out == PACKAGE or not out.is_relative_to(PACKAGE):
        raise ValueError('Data output must be a child of the isolated CVD package')
    return out


def dump(path: Path, value: dict) -> None:
    checked_output(path)
    path.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8')


def windows(start: date, end: date, max_days: int = 60) -> list[tuple[date, date]]:
    if start > end or max_days < 1 or max_days > 60:
        raise ValueError('Invalid request range or chunk size')
    result = []
    while start <= end:
        stop = min(end, start + timedelta(days=max_days - 1))
        result.append((start, stop))
        start = stop + timedelta(days=1)
    return result


def cutoff_ist(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError('Cutoff must include timezone')
    return stamp.tz_convert(IST)


def normalize(rows: list[dict], symbol: str, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Validate genuine native candles; discard still-open/out-of-session rows."""
    cutoff = pd.Timestamp(cutoff)
    if cutoff.tzinfo is None:
        raise ValueError('Cutoff must include timezone')
    cols = ['symbol', 'bar_end', *FIELDS]
    if not rows:
        return pd.DataFrame(columns=cols)
    frame = pd.DataFrame(rows)
    if not {'date', *FIELDS}.issubset(frame):
        raise ValueError('Provider candle schema is incomplete')
    # Kite historical timestamps carry a timezone; reject ambiguous naive data.
    if any(pd.Timestamp(value).tzinfo is None for value in frame.date):
        raise ValueError('Provider timestamp is naive')
    frame['bar_end'] = pd.to_datetime(frame.date, utc=True).dt.tz_convert(IST) + pd.Timedelta(minutes=5)
    clock = frame.bar_end.dt.hour * 60 + frame.bar_end.dt.minute
    frame = frame.loc[(clock >= 560) & (clock <= 930) & frame.bar_end.le(cutoff)].copy()
    if frame.empty:
        return pd.DataFrame(columns=cols)
    if (frame.bar_end.dt.minute.mod(5).ne(0) | frame.bar_end.dt.second.ne(0)).any():
        raise ValueError('Provider timestamps are off the five-minute grid')
    if frame.bar_end.duplicated().any():
        raise ValueError('Provider timestamps are duplicated')
    values = frame[FIELDS].apply(pd.to_numeric, errors='coerce')
    if not np.isfinite(values.to_numpy(float)).all():
        raise ValueError('Nonfinite provider candle value')
    if (values[['open', 'high', 'low', 'close']] <= 0).any().any() or (values.volume < 0).any():
        raise ValueError('Invalid provider candle prices or volume')
    if ((values.high < values[['open', 'close', 'low']].max(axis=1)) |
        (values.low > values[['open', 'close', 'high']].min(axis=1))).any():
        raise ValueError('Inconsistent provider OHLC')
    frame[FIELDS] = values
    frame['symbol'] = symbol
    return frame[cols].sort_values('bar_end').reset_index(drop=True)


def membership(start: str, through: str) -> tuple[pd.DataFrame, dict[str, set[int]], list[dict]]:
    """Dated membership where available; explicit archived cohorts before that."""
    from g3_v18_20261102 import data
    frames, tokens, sources = [], {}, []
    represented = set()
    for path in sorted((DATA / 'fno_oi/universe').glob('near_month_*.parquet')):
        day = path.stem.removeprefix('near_month_')
        if not start <= day <= through:
            continue
        blob = path.read_bytes()
        universe = pd.read_parquet(io.BytesIO(blob))
        stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)].copy()
        field = 'equity_symbol' if 'equity_symbol' in stocks else 'underlying'
        symbols = sorted(stocks[field].dropna().astype(str).unique())
        frames.append(pd.DataFrame(dict(session_date=day, symbol=symbols,
            universe_basis='DATED_FNO_STOCK_UNIVERSE', source_path=str(path))))
        represented.add(day)
        sources.append(dict(path=str(path), sha256=hashlib.sha256(blob).hexdigest(), session_date=day))
        for row in stocks.itertuples():
            if hasattr(row, 'equity_instrument_token') and pd.notna(row.equity_instrument_token):
                tokens.setdefault(str(getattr(row, field)), set()).add(int(row.equity_instrument_token))
    for day in data.available_days(through):
        if day < start:
            continue
        path = data.CACHE / day / 'features.parquet'
        if not path.is_file():
            if day not in represented:
                raise ValueError(f'Missing cohort mapping for sealed session {day}')
            continue
        proof_path = path.parent / 'provenance.json'
        expected = json.loads(proof_path.read_text(encoding='utf-8'))['features_sha256']
        blob = path.read_bytes()
        if hashlib.sha256(blob).hexdigest() != expected:
            raise ValueError(f'Archived cohort hash mismatch for {day}')
        archived = set(pd.read_parquet(io.BytesIO(blob), columns=['tradingsymbol']).tradingsymbol.astype(str))
        if day in represented:
            dated = set(pd.concat(frames, ignore_index=True).query('session_date == @day').symbol)
            missing = sorted(archived - dated)
            if missing:
                frames.append(pd.DataFrame(dict(session_date=day, symbol=missing,
                    universe_basis='ARCHIVED_G3_COHORT_EXTRA_TO_DATED_UNIVERSE', source_path=str(path))))
        else:
            frames.append(pd.DataFrame(dict(session_date=day, symbol=sorted(archived),
                universe_basis='ARCHIVED_AFTER_DATED_G3_COHORT_NOT_POINT_IN_TIME', source_path=str(path))))
        sources.append(dict(path=str(path), sha256=expected, session_date=day, role='ARCHIVED_COHORT'))
    if not frames:
        raise ValueError('No G3 universe membership found')
    result = pd.concat(frames, ignore_index=True).drop_duplicates(['session_date', 'symbol'])
    return result.sort_values(['session_date', 'symbol']).reset_index(drop=True), tokens, sources


def make_client():
    # Existing login helper reads local credential files. Never expose profile or errors.
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            from kite_vix_fetch_5m_15m import setup_kite_session
            return setup_kite_session()
    except Exception as exc:
        raise RuntimeError(f'Kite login unavailable ({type(exc).__name__}); refresh local access token') from None


class Reader:
    def __init__(self, kite, pause: float = 0.70):
        if pause < 0.5:
            raise ValueError('Historical calls must be at most two per second')
        self.kite, self.pause, self.last = kite, pause, 0.0

    def fetch(self, token: int, start: datetime, end: datetime) -> list[dict]:
        for attempt in range(3):
            time.sleep(max(0.0, self.pause - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            try:
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    return self.kite.historical_data(token, start, end, '5minute', continuous=False, oi=False)
            except Exception as exc:
                if type(exc).__name__ in ('TokenException', 'PermissionException'):
                    raise RuntimeError(f'Kite authentication/permission failure: {type(exc).__name__}') from None
                if attempt == 2:
                    raise RuntimeError(f'Kite historical request failed: {type(exc).__name__}') from None
                time.sleep(3.0 * (attempt + 1))
        raise AssertionError('Unreachable')


def resolve_token(symbol: str, tokens: dict[str, int], history: set[int]) -> tuple[int | None, str | None, str]:
    """Keep archived labels when a unique dated equity token has been renamed."""
    direct = tokens.get(symbol)
    if direct is not None:
        if history and history != {direct}:
            return None, None, 'CONFLICTING_DATED_EQUITY_TOKEN'
        return direct, symbol, 'CURRENT_SYMBOL_AND_DATED_TOKEN_AGREE' if history else 'CURRENT_SYMBOL_MAPPING_RETROSPECTIVE'
    if len(history) == 1:
        old_token = next(iter(history))
        names = [name for name, token in tokens.items() if token == old_token]
        if len(names) == 1:
            return old_token, names[0], 'DATED_EQUITY_TOKEN_CURRENT_PROVIDER_SYMBOL_ALIAS'
    return None, None, 'INSTRUMENT_TOKEN_MISSING_OR_AMBIGUOUS'


def run(output: Path, start: str = '2026-07-29', through: str = '2026-11-02',
        max_symbols: int | None = None) -> dict:
    from g3_v18_20261102.run import guard_writes
    guard_writes(PACKAGE)
    output = checked_output(output)
    output.mkdir(parents=True, exist_ok=True)
    member, token_history, sources = membership(start, through)
    symbols = sorted(member.symbol.unique())
    if max_symbols:
        symbols = symbols[:max_symbols]
        member = member.loc[member.symbol.isin(symbols)].copy()
    plan_path = output / 'fetch_plan.json'
    if plan_path.exists():
        plan = json.loads(plan_path.read_text(encoding='utf-8'))
        if plan['start'] != start or plan['through'] != through or plan['symbols'] != symbols:
            raise ValueError('Existing fetch plan differs; use a fresh output directory')
        cutoff = cutoff_ist(plan['completed_bar_cutoff_ist'])
    else:
        captured = pd.Timestamp.now(tz=IST)
        cutoff = min((captured - pd.Timedelta(seconds=5)).floor('5min'), pd.Timestamp(f'{through} 15:30', tz=IST))
        if cutoff.date() < date.fromisoformat(start):
            raise ValueError('Requested history lies in the future')
        plan = dict(start=start, through=through, symbols=symbols, captured_at_ist=str(captured),
            completed_bar_cutoff_ist=str(cutoff), interval='5minute', chunk_calendar_days=60,
            interval_semantics='Kite start plus 5 minutes gives completed bar_end',
            requests_planned=len(symbols)*len(windows(date.fromisoformat(start), cutoff.date())),
            scope='HISTORICAL_RESEARCH_ONLY', execution_authority=False,
            data_kind='OHLCV_FOR_ESTIMATED_CVD_NOT_AGGRESSOR_DELTA', universe_sources=sources)
        dump(plan_path, plan)
    member.to_parquet(output / 'universe_membership.parquet', index=False)
    member.to_csv(output / 'universe_membership.csv', index=False)
    kite = make_client()
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            instruments = kite.instruments('NSE')
    except Exception as exc:
        raise RuntimeError(f'Kite instrument lookup failed: {type(exc).__name__}') from None
    tokens = {str(r['tradingsymbol']): int(r['instrument_token']) for r in instruments
              if r.get('segment') == 'NSE' and r.get('instrument_type') == 'EQ'}
    raw_dir = output / 'native_5m_chunks'
    raw_dir.mkdir(exist_ok=True)
    reader = Reader(kite)
    records, pieces = [], []
    request_windows = windows(date.fromisoformat(start), cutoff.date())
    print(f'KITE_FETCH symbols={len(symbols)} chunks={len(request_windows)} requests={plan["requests_planned"]} cutoff={cutoff}', flush=True)
    for number, symbol in enumerate(symbols, 1):
        token, provider_symbol, mapping_basis = resolve_token(symbol, tokens, token_history.get(symbol, set()))
        if token is None:
            records.append(dict(symbol=symbol, status=mapping_basis))
            continue
        for left, right in request_windows:
            query_start = pd.Timestamp(f'{left} 09:15', tz=IST)
            query_end = min(pd.Timestamp(f'{right} 15:30', tz=IST), cutoff)
            chunk = raw_dir / f'{symbol}_{left}_{right}.parquet'
            receipt_path = chunk.with_suffix('.json')
            rec = dict(symbol=symbol, instrument_token=token, request_start_ist=str(query_start),
                request_end_ist=str(query_end), interval='5minute', provider='KITE_HISTORICAL', path=str(chunk),
                provider_symbol=provider_symbol, identity_mapping_basis=mapping_basis)
            try:
                if chunk.exists() and receipt_path.exists():
                    old = json.loads(receipt_path.read_text(encoding='utf-8'))
                    if old['sha256'] != sha(chunk) or old['request_end_ist'] != str(query_end):
                        raise ValueError('Cached fetch receipt mismatch')
                    frame = pd.read_parquet(chunk)
                    rec.update(old)
                    rec['cache_reused'] = True
                else:
                    rows = reader.fetch(token, query_start.to_pydatetime(), query_end.to_pydatetime())
                    frame = normalize(rows, symbol, cutoff)
                    frame = frame.loc[frame.bar_end.ge(query_start + pd.Timedelta(minutes=5)) & frame.bar_end.le(query_end)].copy()
                    frame.to_parquet(chunk, index=False)
                    rec.update(status='FETCHED', rows=len(frame), sha256=sha(chunk), acquired_at_ist=str(pd.Timestamp.now(tz=IST)), cache_reused=False)
                    dump(receipt_path, rec)
                pieces.append(frame)
            except Exception as exc:
                rec.update(status='FAILED', error_type=type(exc).__name__)
                # Sanitized messages contain no provider payload or credentials.
                if isinstance(exc, RuntimeError):
                    rec['reason'] = str(exc)
                if 'authentication/permission' in str(exc):
                    records.append(rec)
                    dump(output / 'fetch_status.json', dict(complete=False, records=records))
                    raise
            records.append(rec)
        if number % 10 == 0 or number == len(symbols):
            print(f'KITE_FETCH {number}/{len(symbols)} chunks_ok={sum(r.get("status")=="FETCHED" for r in records)} failed={sum(r.get("status")=="FAILED" for r in records)}', flush=True)
            dump(output / 'fetch_status.json', dict(complete=False, records=records))
    if not pieces:
        raise ValueError('No valid Kite five-minute history fetched')
    bars = pd.concat(pieces, ignore_index=True).sort_values(['symbol', 'bar_end']).reset_index(drop=True)
    if bars.duplicated(['symbol', 'bar_end']).any():
        raise ValueError('Chunk consolidation produced duplicate identities')
    bars['session_date'] = bars.bar_end.dt.strftime('%Y-%m-%d')
    bars['provider'] = 'KITE_HISTORICAL_NATIVE_5MIN'
    bars['is_g3_member_on_date'] = pd.MultiIndex.from_frame(bars[['session_date', 'symbol']]).isin(
        pd.MultiIndex.from_frame(member[['session_date', 'symbol']]))
    bars.to_parquet(output / 'bars_5m.parquet', index=False)
    coverage = []
    by_identity = {key: frame for key, frame in bars.groupby(['session_date', 'symbol'], sort=False)}
    observed_dates = set(bars.session_date.unique())
    for row in member.itertuples(index=False):
        expected_end = min(pd.Timestamp(f'{row.session_date} 15:30', tz=IST), cutoff)
        expected = pd.date_range(pd.Timestamp(f'{row.session_date} 09:20', tz=IST), expected_end, freq='5min')
        frame = by_identity.get((row.session_date, row.symbol))
        actual = pd.DatetimeIndex([]) if frame is None else pd.DatetimeIndex(frame.bar_end)
        missing = expected.difference(actual)
        strategy_expected = expected[expected <= pd.Timestamp(f'{row.session_date} 13:30', tz=IST)]
        strategy_missing = strategy_expected.difference(actual)
        coverage.append(dict(session_date=row.session_date, symbol=row.symbol, universe_basis=row.universe_basis,
            expected_bars=len(expected), observed_bars=len(actual), missing_bars=len(missing),
            complete=not len(missing), first_missing=str(missing[0]) if len(missing) else None,
            expected_strategy_bars=len(strategy_expected), missing_strategy_bars=len(strategy_missing),
            strategy_window_complete=not len(strategy_missing),
            any_symbol_observed_on_date=row.session_date in observed_dates))
    cov = pd.DataFrame(coverage)
    cov.to_csv(output / 'coverage.csv', index=False)
    summary = dict(complete=all(r.get('status') == 'FETCHED' for r in records),
        completion_meaning='ALL_REQUESTS_SUCCEEDED; inspect per-session coverage separately',
        symbols_requested=len(symbols), symbols_observed=int(bars.symbol.nunique()), bars=len(bars),
        first_bar_ist=str(bars.bar_end.min()), last_bar_ist=str(bars.bar_end.max()),
        completed_bar_cutoff_ist=str(cutoff), requests_planned=plan['requests_planned'],
        failed_chunks=sum(r.get('status') != 'FETCHED' for r in records),
        member_stock_days=len(cov), complete_member_stock_days=int(cov.complete.sum()),
        missing_member_bars=int(cov.missing_bars.sum()),
        strategy_window_complete_stock_days=int(cov.strategy_window_complete.sum()),
        missing_strategy_window_bars=int(cov.missing_strategy_bars.sum()),
        membership_dates_without_any_provider_bars=sorted(set(member.session_date)-observed_dates),
        aliases=list({r['symbol']:dict(symbol=r['symbol'], provider_symbol=r['provider_symbol'], instrument_token=r['instrument_token'],
            mapping_basis=r['identity_mapping_basis']) for r in records
            if r.get('provider_symbol') and r['symbol'] != r['provider_symbol']}.values()),
        source_disclosure='All candles were retrieved retrospectively from Kite. Early archived cohorts and current instrument mapping are not fully point-in-time.',
        bars_path=str(output / 'bars_5m.parquet'), bars_sha256=sha(output / 'bars_5m.parquet'),
        membership_path=str(output / 'universe_membership.parquet'), membership_sha256=sha(output / 'universe_membership.parquet'),
        records=records)
    dump(output / 'fetch_manifest.json', summary)
    dump(output / 'fetch_status.json', dict(operation_finished=True, complete=summary['complete'],
        summary={k:v for k,v in summary.items() if k != 'records'}))
    print(json.dumps({k:v for k,v in summary.items() if k != 'records'}, indent=2), flush=True)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=PACKAGE/'kite_data_20261102')
    parser.add_argument('--start', default='2026-07-29')
    parser.add_argument('--through', default='2026-11-02')
    parser.add_argument('--max-symbols', type=int)
    args = parser.parse_args()
    run(args.output, args.start, args.through, args.max_symbols)
