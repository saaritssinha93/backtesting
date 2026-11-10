"""Isolated, read-only-Kite data collector for the 8 November 2026 session.

Broker calls are limited to profile and historical_data. Production caches,
calendars, signal markers and broker orders are never written by this module.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import re
from threading import Lock
import time
from typing import Any
import uuid

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

IST = 'Asia/Kolkata'
DAY = date(2026, 11, 8)
FRIDAY = date(2026, 11, 6)
REPO = Path(__file__).resolve().parents[1]
ALLOWED_ROOT = REPO / 'outputs/muhurat_20261108/data'
SOURCE_ROOT = Path('C:/TradingData/eqidv2')
UNIVERSE_SOURCE = SOURCE_ROOT / 'fno_oi/universe/near_month_2026-11-06.parquet'
OPEN = pd.Timestamp('2026-11-08 14:20', tz=IST)
CASH_CLOSE = pd.Timestamp('2026-11-08 15:15', tz=IST)
FUTURES_CLOSE = pd.Timestamp('2026-11-08 15:40', tz=IST)
OHLCV = ['open', 'high', 'low', 'close', 'volume']
BOOTSTRAP_VERSION = 'friday_ohlc_verified_canonical_contract_names_v2'


def stamp(value: Any) -> pd.Timestamp:
    value = pd.Timestamp(value)
    return value.tz_localize(IST) if value.tzinfo is None else value.tz_convert(IST)


def dated_now(value: Any) -> pd.Timestamp:
    result = stamp(value)
    if pd.isna(result) or result.date() != DAY:
        raise ValueError('This collector is valid only on2026-11-08; no recurring override')
    return result


def output_path(root: Path | str) -> Path:
    root = Path(root).resolve()
    if not root.is_relative_to(ALLOWED_ROOT.resolve()):
        raise ValueError('Collector writes must stay in the isolated Muhurat data directory')
    return root


def symbol_name(symbol: str) -> str:
    value = str(symbol).strip().upper()
    if value in {'.', '..'} or not re.fullmatch(r'[A-Z0-9&._-]+', value):
        raise ValueError('Invalid instrument filename')
    return value


def source_contract_stem(symbol: str) -> str:
    """Match the production reader's encoded filenames, including M&M/GVT&D."""
    value = symbol_name(symbol)
    stem = re.sub(r'[^A-Z0-9._-]+', '_', value).strip('._') or 'CONTRACT'
    return stem if stem == value else stem + '_' + hashlib.sha1(value.encode('utf-8')).hexdigest()[:8]


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.' + path.name + '.' + uuid.uuid4().hex + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n', encoding='utf-8')
    os.replace(temporary, path)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name('.' + path.name + '.' + uuid.uuid4().hex + '.tmp')
    frame.to_parquet(temporary, index=False)
    os.replace(temporary, path)


def _valid_prices(frame: pd.DataFrame, *, require_oi: bool = False) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    columns = OHLCV + (['oi'] if require_oi else [])
    for key in columns:
        out[key] = pd.to_numeric(out.get(key, np.nan), errors='coerce')
    good = np.isfinite(out[columns]).all(axis=1)
    good &= out[['open', 'high', 'low', 'close']].gt(0).all(axis=1)
    good &= out.high.ge(out[['open', 'close', 'low']].max(axis=1))
    good &= out.low.le(out[['open', 'close']].min(axis=1)) & out.volume.ge(0)
    if require_oi:
        good &= out.oi.gt(0)
    for flag in ('gap_filled', 'is_synthetic', 'opening_snapshot', 'provisional_stale'):
        if flag in out:
            flagged = out[flag].astype(str).str.strip().str.lower().isin(['true', 'yes', 'on', '1', '1.0'])
            good &= ~flagged
    if 'quality_state' in out:
        good &= out.quality_state.fillna('VALID').astype(str).str.upper().eq('VALID')
    return out.loc[good].copy()


def normalize_current(records, now_ist, *, interval_minutes: int, futures: bool = False) -> pd.DataFrame:
    """Convert actual Kite candle-start labels to completed candle-end labels."""
    now = dated_now(now_ist)
    if interval_minutes not in (1, 5) or (not futures and interval_minutes != 1):
        raise ValueError('Expected equity minute or futures minute/five-minute data')
    rows = []
    limit = min((now - pd.Timedelta(seconds=3)).floor('min'), FUTURES_CLOSE if futures else CASH_CLOSE)
    for record in records:
        try:
            start = stamp(record['date'])
        except (KeyError, ValueError, TypeError):
            continue
        end = start + pd.Timedelta(minutes=interval_minutes)
        if start.date() != DAY or start < OPEN or end > limit:
            continue
        if start.second or start.microsecond or (start - OPEN).total_seconds() % (interval_minutes * 60):
            continue
        rows.append({**{key: record.get(key) for key in OHLCV + ['oi']},
                     'candle_start': start, 'timestamp': end, 'ts': end, 'date': end,
                     'quality_state': 'VALID', 'source': 'kite_historical_muhurat',
                     'gap_filled': 0, 'is_synthetic': False})
    if not rows:
        return pd.DataFrame()
    frame = _valid_prices(pd.DataFrame(rows), require_oi=futures)
    return frame.sort_values('ts').drop_duplicates('ts', keep='last').reset_index(drop=True)


def aggregate_five_minute(frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate only five distinct real minute bars; never forward-fill."""
    if frame.empty:
        return pd.DataFrame()
    minute = _valid_prices(frame).sort_values('ts').drop_duplicates('ts', keep='last').copy()
    if minute.empty:
        return pd.DataFrame()
    minute['ts'] = pd.to_datetime(minute.ts, utc=True).dt.tz_convert(IST)
    minute = minute.loc[minute.ts.eq(minute.ts.dt.floor('min'))].copy()
    # Both09:15 regular and14:20 Muhurat opens lie on five-minute boundaries.
    minute['slot_end'] = minute.ts.dt.ceil('5min')
    grouped = minute.groupby('slot_end').agg(open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'), volume=('volume', 'sum'),
        source_1m_count=('ts', 'size'), first=('ts', 'first'), last=('ts', 'last')).reset_index()
    good = grouped.source_1m_count.eq(5) & grouped['last'].eq(grouped.slot_end)
    good &= grouped['first'].eq(grouped.slot_end - pd.Timedelta(minutes=4))
    out = grouped.loc[good].drop(columns=['first', 'last']).copy()
    out['ts'] = out.slot_end
    out['date'] = out.ts
    out['timestamp'] = out.ts
    out['candle_start'] = out.ts - pd.Timedelta(minutes=5)
    out['quality_state'] = 'VALID'
    out['gap_filled'] = 0
    out['opening_snapshot'] = False
    out['provisional_stale'] = 0
    return out.drop(columns='slot_end').reset_index(drop=True)


def _merge(path: Path, incoming: pd.DataFrame) -> pd.DataFrame:
    old = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    if incoming.empty:
        return old
    combined = pd.concat([old, incoming], ignore_index=True) if not old.empty else incoming.copy()
    combined['ts'] = pd.to_datetime(combined.ts, utc=True).dt.tz_convert(IST)
    combined = combined.sort_values('ts').drop_duplicates('ts', keep='last').reset_index(drop=True)
    _atomic_parquet(path, combined)
    return combined


def _read_history(path: Path, *, futures: bool = False) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    available = set(pq.ParquetFile(path).schema.names)
    key = 'timestamp' if futures else 'date'
    if not set([key] + OHLCV).issubset(available):
        return pd.DataFrame()
    extra = ['oi'] if futures else []
    optional = ['gap_filled', 'is_synthetic', 'opening_snapshot', 'provisional_stale',
                'quality_state', 'source_1m_count', 'underlying', 'tradingsymbol', 'expiry',
                'instrument_token', 'candle_start', 'source']
    columns = list(dict.fromkeys([key] + OHLCV + extra + [x for x in optional if x in available]))
    start = pd.Timestamp('2026-11-02', tz=IST)
    stop = pd.Timestamp('2026-11-07', tz=IST)
    frame = pd.read_parquet(path, columns=columns, filters=[(key, '>=', start), (key, '<', stop)])
    if frame.empty:
        return frame
    frame['ts'] = pd.to_datetime(frame[key], utc=True).dt.tz_convert(IST)
    frame['date'] = frame.ts
    frame['timestamp'] = frame.ts
    frame = _valid_prices(frame, require_oi=futures)
    if 'source_1m_count' in frame:
        frame = frame.loc[frame.source_1m_count.isna() | frame.source_1m_count.eq(5)]
    return frame.sort_values('ts').drop_duplicates('ts', keep='last').reset_index(drop=True)


@dataclass
class Lane:
    name: str
    client: Any
    pace_seconds: float = .36
    last_call: float = 0.
    lock: Lock = field(default_factory=Lock)
    auth_failed: bool = False

    def historical(self, token, start, end, *, interval, oi):
        with self.lock:
            time.sleep(max(0., self.pace_seconds - (time.monotonic() - self.last_call)))
            self.last_call = time.monotonic()
            return self.client.historical_data(int(token), start.to_pydatetime(), end.to_pydatetime(),
                                               interval=interval, continuous=False, oi=oi)


class Collector:
    def __init__(self, output_root=ALLOWED_ROOT, *, credential_loader=None, client_factory=None,
                 universe_source=UNIVERSE_SOURCE, source_root=SOURCE_ROOT, pace_seconds=.36,
                 max_cycle_seconds=20.):
        self.root = output_path(output_root)
        self.source_root = Path(source_root)
        self.universe_source = Path(universe_source)
        self.credential_loader = credential_loader
        self.client_factory = client_factory
        self.pace_seconds = max(.36, float(pace_seconds))
        self.max_cycle_seconds = float(max_cycle_seconds)
        if not np.isfinite(self.max_cycle_seconds) or self.max_cycle_seconds <= 0:
            raise ValueError('Collection cycle budget must be positive and finite')
        self.lanes = []
        self.last_health = None
        self.universe = None
        self.cache_errors = {}
        self._next_instrument = 0

    def healthcheck(self):
        """Reload token files every invocation; never retain a failed login pool."""
        import fno_oi_common as common
        loader = self.credential_loader or common.discover_kite_credentials
        factory = self.client_factory or common.make_kite_client
        self.lanes = []
        try:
            credentials = loader()
        except Exception as exc:
            result = dict(state='AUTH_REQUIRED', healthy_apps=0, apps=[], error_type=type(exc).__name__)
            self.last_health = result
            return result

        def probe(credential):
            try:
                client = factory(credential, timeout_sec=8.)
                client.profile()
                return Lane(credential.app_name, client, self.pace_seconds), dict(app=credential.app_name, authenticated=True)
            except Exception as exc:
                return None, dict(app=credential.app_name, authenticated=False, error_type=type(exc).__name__)
        with ThreadPoolExecutor(max_workers=max(1, min(8, len(credentials)))) as pool:
            outcomes = list(pool.map(probe, credentials))
        self.lanes = [lane for lane, _ in outcomes if lane is not None]
        self.last_health = dict(state='READY' if self.lanes else 'AUTH_REQUIRED', healthy_apps=len(self.lanes),
                                apps=[status for _, status in outcomes])
        return self.last_health.copy()

    def _initialize(self):
        if self.universe is not None:
            return
        universe = pd.read_parquet(self.universe_source)
        required = {'underlying', 'tradingsymbol', 'instrument_token', 'expiry', 'is_index_future',
                    'equity_symbol', 'equity_instrument_token'}
        if not required.issubset(universe):
            raise ValueError('Friday universe schema is incomplete')
        expiry = pd.to_datetime(universe.expiry, errors='coerce')
        if expiry.isna().any() or expiry.dt.date.le(DAY).any() or universe.tradingsymbol.duplicated().any():
            raise ValueError('Frozen Friday contract universe is invalid or expired')
        for name in universe.tradingsymbol:
            symbol_name(name)
        stocks = universe.loc[~universe.is_index_future.astype(bool)]
        if stocks.equity_symbol.isna().any() or pd.to_numeric(stocks.equity_instrument_token).le(0).any():
            raise ValueError('Invalid stock/equity mapping')
        self.root.mkdir(parents=True, exist_ok=True)
        marker = self.root / 'bootstrap_manifest.json'
        if marker.exists():
            saved = json.loads(marker.read_text())
            if saved['universe_sha256'] != hashlib.sha256(self.universe_source.read_bytes()).hexdigest():
                raise ValueError('Friday universe changed after data initialization')
            if saved.get('bootstrap_version') == BOOTSTRAP_VERSION:
                self.universe = universe
                return
        _atomic_parquet(self.root / 'universe.parquet', universe)

        def warm(row):
            contract = symbol_name(row.tradingsymbol)
            errors = []

            def read(source_path, *, futures=False):
                try:
                    return _read_history(source_path, futures=futures)
                except Exception as exc:
                    errors.append(dict(file=source_path.name, error_type=type(exc).__name__))
                    return pd.DataFrame()

            future = read(self.source_root / f'fno_oi/raw_contracts_5m/{source_contract_stem(contract)}_5minute.parquet', futures=True)
            _merge(self.root / f'futures_5m/{contract}.parquet', future)
            result = dict(contract=contract, futures_history_rows=len(future), errors=errors)
            if not row.is_index_future:
                symbol = symbol_name(row.equity_symbol)
                minute = read(self.source_root / f'stocks_indicators_1min_eq/{symbol}_stocks_indicators_1min.parquet')
                five = read(self.source_root / f'stocks_indicators_5min_eq_live2/{symbol}_stocks_indicators_5min.parquet')
                _merge(self.root / f'equity_1m/{symbol}.parquet', minute)
                _merge(self.root / f'equity_5m_history/{symbol}.parquet', five)
                result.update(symbol=symbol, equity_history_rows=len(minute), equity_5m_history_rows=len(five))
            return result
        def safe_warm(row):
            try:
                return warm(row)
            except Exception as exc:
                return dict(contract=row.tradingsymbol, symbol=row.equity_symbol,
                    futures_history_rows=0, equity_history_rows=0, equity_5m_history_rows=0,
                    errors=[dict(error_type=type(exc).__name__, phase='ISOLATED_CACHE_BOOTSTRAP')])
        with ThreadPoolExecutor(max_workers=8) as pool:
            coverage = list(pool.map(safe_warm, universe.itertuples(index=False)))
        _atomic_json(marker, dict(bootstrap_version=BOOTSTRAP_VERSION, session_date=str(DAY), source_date=str(FRIDAY),
            universe_source=str(self.universe_source), universe_sha256=hashlib.sha256(self.universe_source.read_bytes()).hexdigest(),
            source_contracts=len(universe), source_stocks=len(stocks), coverage=coverage,
            note='Read-only prior-weekday warm-up; no Nov8 candle fabricated from Friday'))
        self.universe = universe

    def _last(self, kind, name):
        path = self.root / kind / (symbol_name(name) + '.parquet')
        if not path.exists():
            return None
        frame = pd.read_parquet(path, columns=['ts'])
        if frame.empty:
            return None
        return stamp(frame.ts.max())

    def _needed_start(self, kind, name, end, minutes):
        """Repair the earliest absent completed bar, including interior gaps."""
        expected = pd.date_range(OPEN + pd.Timedelta(minutes=minutes), end, freq=f'{minutes}min')
        if not len(expected):
            return None
        path = self.root / kind / (symbol_name(name) + '.parquet')
        observed = pd.DatetimeIndex([], tz=IST)
        if path.exists():
            try:
                observed = pd.DatetimeIndex(pd.to_datetime(pd.read_parquet(path, columns=['ts']).ts, utc=True)).tz_convert(IST)
                self.cache_errors.pop(str(path.relative_to(self.root)), None)
            except Exception as exc:
                self.cache_errors[str(path.relative_to(self.root))] = type(exc).__name__
        missing = expected.difference(observed)
        return missing[0] - pd.Timedelta(minutes=minutes) if len(missing) else None

    def _coverage(self, cash_end):
        stocks = self.universe.loc[~self.universe.is_index_future.astype(bool)]
        available, exact = 0, 0
        for symbol in stocks.equity_symbol:
            path = self.root / f'equity_1m/{symbol_name(symbol)}.parquet'
            if not path.exists():
                continue
            try:
                stamps = pd.to_datetime(pd.read_parquet(path, columns=['ts']).ts, utc=True).dt.tz_convert(IST)
                available += int(stamps.dt.date.eq(DAY).any())
                exact += int(cash_end > OPEN and stamps.eq(cash_end).any())
            except Exception as exc:
                self.cache_errors[str(path.relative_to(self.root))] = type(exc).__name__
        opening_count = sum(self._opening_complete(row) for row in stocks.itertuples(index=False))
        return dict(expected_equities=len(stocks), expected_symbols=len(stocks),
            equities_available=available, current_minute_covered_symbols=exact,
            opening_oi_covered_symbols=opening_count,
            cache_errors=[dict(file=name, error_type=error) for name, error in sorted(self.cache_errors.items())])

    def _opening_complete(self, row):
        path = self.root / f'opening_oi/{symbol_name(row.tradingsymbol)}.json'
        if not path.exists():
            return False
        try:
            record = json.loads(path.read_text())
            good = (stamp(record['candle_start']) == OPEN and stamp(record['candle_end']) == OPEN + pd.Timedelta(minutes=1)
                    and record['futures_tradingsymbol'] == row.tradingsymbol and float(record['oi']) > 0)
            if good:
                self.cache_errors.pop(str(path.relative_to(self.root)), None)
            return good
        except Exception as exc:
            self.cache_errors[str(path.relative_to(self.root))] = type(exc).__name__
            return False

    def collect(self, now_ist):
        now = dated_now(now_ist)
        self._initialize()
        if self.last_health is None:
            self.healthcheck()
        if not self.lanes:
            status = dict(self.last_health or {}, session_date=str(DAY), asof=now.isoformat(),
                          execution_authority=False, collector='DATA_ONLY', **self._coverage(min(now.floor('min'), CASH_CLOSE)))
            _atomic_json(self.root / 'status.json', status)
            return status
        completed = (now - pd.Timedelta(seconds=3)).floor('min')
        cash_end, futures_end = min(completed, CASH_CLOSE), min(completed.floor('5min'), FUTURES_CLOSE)
        if completed < OPEN + pd.Timedelta(minutes=1):
            status = dict(state='WAITING_SESSION', healthy_apps=len(self.lanes), session_date=str(DAY),
                          asof=now.isoformat(), cash_first_completed='14:21', execution_authority=False,
                          **self._coverage(OPEN))
            _atomic_json(self.root / 'status.json', status)
            return status
        # Keep each instrument's required requests together. NIFTY is needed
        # for the opening context; rotate the rest to avoid alphabetic starvation
        # when only one or two authenticated apps are available.
        rows = list(self.universe.itertuples(index=False))
        priority = [row for row in rows if row.underlying == 'NIFTY']
        regular = [row for row in rows if row.underlying != 'NIFTY']
        cursor = self._next_instrument % max(1, len(regular))
        rotated = regular[cursor:] + regular[:cursor]
        positions = {row.tradingsymbol: index for index, row in enumerate(regular)}
        jobs, groups = [], []
        for row in priority + rotated:
            instrument_jobs = []
            if not row.is_index_future:
                name = symbol_name(row.equity_symbol)
                start = self._needed_start('equity_1m', name, cash_end, 1)
                if start is not None:
                    instrument_jobs.append(('equity', row, start, cash_end))
            start = self._needed_start('futures_5m', row.tradingsymbol, futures_end, 5)
            if start is not None:
                instrument_jobs.append(('future', row, start, futures_end))
            if not self._opening_complete(row):
                instrument_jobs.append(('opening', row, OPEN, OPEN + pd.Timedelta(minutes=1)))
            if instrument_jobs:
                groups.append(instrument_jobs)
                jobs.extend(instrument_jobs)

        cycle_started = time.monotonic()
        deadline = cycle_started + self.max_cycle_seconds
        started_groups = []
        started_lock = Lock()

        def deferred(lane, job):
            kind, row, _, _ = job
            return dict(kind=kind, symbol=row.equity_symbol if kind == 'equity' else row.tradingsymbol,
                        app=lane.name, state='DEFERRED', rows=0, reason='CYCLE_TIME_BUDGET')

        def perform(lane, job):
            kind, row, start, end = job
            symbol = row.equity_symbol if kind == 'equity' else row.tradingsymbol
            token = row.equity_instrument_token if kind == 'equity' else row.instrument_token
            interval = '5minute' if kind == 'future' else 'minute'
            error = None
            if time.monotonic() >= deadline:
                return deferred(lane, job)
            if lane.auth_failed:
                return dict(kind=kind, symbol=symbol, app=lane.name, state='FETCH_ERROR',
                            rows=0, error_type='TokenException')
            for attempt in range(2):
                if time.monotonic() >= deadline:
                    return deferred(lane, job)
                try:
                    records = lane.historical(token, start, end, interval=interval, oi=kind != 'equity')
                    frame = normalize_current(records, now, interval_minutes=5 if kind == 'future' else 1,
                                              futures=kind != 'equity')
                    if kind == 'opening':
                        exact = frame.loc[frame.ts.eq(OPEN + pd.Timedelta(minutes=1))] if not frame.empty else frame
                        if exact.empty:
                            return dict(kind=kind, symbol=symbol, state='MISSING_OPENING_OI', rows=0)
                        value = exact.iloc[0]
                        _atomic_json(self.root / f'opening_oi/{symbol_name(symbol)}.json',
                            dict(underlying=row.underlying, symbol=row.equity_symbol if not row.is_index_future else row.underlying,
                                 futures_tradingsymbol=row.tradingsymbol, futurecontract=row.tradingsymbol,
                                 instrument_token=int(row.instrument_token), oi=float(value.oi),
                                 candle_start=OPEN.isoformat(), candle_end=(OPEN + pd.Timedelta(minutes=1)).isoformat(),
                                 timestamp=(OPEN + pd.Timedelta(minutes=1)).isoformat(),
                                 request_asof=now.isoformat(), observed_at=max(now, pd.Timestamp.now(tz=IST)).isoformat(),
                                 source='kite_historical_actual_opening_1minute'))
                    else:
                        if not frame.empty and kind == 'future':
                            for key in ('underlying', 'tradingsymbol', 'instrument_token', 'expiry', 'contract_month',
                                        'lot_size', 'tick_size', 'is_index_future'):
                                if hasattr(row, key):
                                    frame[key] = getattr(row, key)
                        cache_kind = 'equity_1m' if kind == 'equity' else 'futures_5m'
                        _merge(self.root / cache_kind / (symbol_name(symbol) + '.parquet'), frame)
                        missing = self._needed_start(cache_kind, symbol, end, 1 if kind == 'equity' else 5)
                        if missing is not None:
                            return dict(kind=kind, symbol=symbol, state='PARTIAL_DATA', rows=len(frame),
                                        earliest_missing_start=missing.isoformat())
                    return dict(kind=kind, symbol=symbol, state='WRITTEN' if len(frame) else 'NO_COMPLETED_DATA', rows=len(frame))
                except Exception as exc:
                    error = type(exc).__name__
                    if error == 'TokenException':
                        lane.auth_failed = True
                        break
                    if attempt == 0:
                        time.sleep(.5)
            return dict(kind=kind, symbol=symbol, app=lane.name, state='FETCH_ERROR', rows=0, error_type=error)

        def partition(index):
            outcomes = []
            for group_index in range(index, len(groups), len(self.lanes)):
                group = groups[group_index]
                if time.monotonic() < deadline:
                    with started_lock:
                        started_groups.append((group_index, group[0][1].tradingsymbol))
                outcomes.extend(perform(self.lanes[index], job) for job in group)
            return outcomes
        with ThreadPoolExecutor(max_workers=len(self.lanes)) as pool:
            outcomes = [item for partition_out in pool.map(partition, range(len(self.lanes))) for item in partition_out]
        ordinary_started = [(index, symbol) for index, symbol in started_groups if symbol in positions]
        if ordinary_started:
            _, last_symbol = max(ordinary_started)
            self._next_instrument = (positions[last_symbol] + 1) % len(regular)
        failed_apps = {lane.name for lane in self.lanes if lane.auth_failed}
        if failed_apps:
            self.lanes = [lane for lane in self.lanes if not lane.auth_failed]
            failed_keys = {(item['kind'], item['symbol']) for item in outcomes if item.get('error_type') == 'TokenException'}
            retries = [job for job in jobs if (job[0], job[1].equity_symbol if job[0] == 'equity' else job[1].tradingsymbol) in failed_keys]
            if self.lanes and retries:
                def retry_partition(index):
                    return [perform(self.lanes[index], job) for job in retries[index::len(self.lanes)]]
                with ThreadPoolExecutor(max_workers=len(self.lanes)) as pool:
                    retried = [item for items in pool.map(retry_partition, range(len(self.lanes))) for item in items]
                outcomes = [item for item in outcomes if (item['kind'], item['symbol']) not in failed_keys] + retried
                failed_apps.update(lane.name for lane in self.lanes if lane.auth_failed)
                self.lanes = [lane for lane in self.lanes if not lane.auth_failed]
            old_apps = (self.last_health or {}).get('apps', [])
            self.last_health = dict(state='READY' if self.lanes else 'AUTH_REQUIRED', healthy_apps=len(self.lanes),
                needs_recheck=True, apps=[dict(item, authenticated=False, error_type='TokenException')
                                         if item['app'] in failed_apps else item for item in old_apps])
        status = dict(state='PARTIAL' if any(item['state'] != 'WRITTEN' for item in outcomes) else 'COMPLETE',
            session_date=str(DAY), asof=now.isoformat(), cash_completed_through=str(cash_end),
            futures_completed_through=str(futures_end), healthy_apps=len(self.lanes),
            jobs=len(jobs), outcomes=outcomes, execution_authority=False, collector='DATA_ONLY',
            collection_budget_seconds=self.max_cycle_seconds,
            collection_elapsed_seconds=time.monotonic() - cycle_started,
            deferred_jobs=sum(item['state'] == 'DEFERRED' for item in outcomes),
            next_instrument_index=self._next_instrument,
            **self._coverage(cash_end))
        _atomic_json(self.root / 'status.json', status)
        return status

    def load_frames(self):
        self._initialize()
        return load_frames(self.root)


def load_frames(output_root=ALLOWED_ROOT):
    root = output_path(output_root)
    result = dict(universe=pd.read_parquet(root / 'universe.parquet'), equity_1m={}, equity_5m={}, futures_5m={}, opening_oi={}, data_errors=[])
    for kind in ('equity_1m', 'futures_5m'):
        for path in (root / kind).glob('*.parquet'):
            try:
                frame = pd.read_parquet(path)
                frame['ts'] = pd.to_datetime(frame.ts, utc=True).dt.tz_convert(IST)
                result[kind][path.stem] = _valid_prices(frame, require_oi=kind == 'futures_5m')
            except Exception as exc:
                result['data_errors'].append(dict(file=str(path.relative_to(root)), error_type=type(exc).__name__))
    for row in result['universe'].itertuples(index=False):
        if not row.is_index_future:
            symbol = symbol_name(row.equity_symbol)
            history_path = root / f'equity_5m_history/{symbol}.parquet'
            try:
                history = pd.read_parquet(history_path) if history_path.exists() else pd.DataFrame()
            except Exception as exc:
                history = pd.DataFrame()
                result['data_errors'].append(dict(file=str(history_path.relative_to(root)), error_type=type(exc).__name__))
            try:
                five = aggregate_five_minute(result['equity_1m'].get(symbol, pd.DataFrame()))
                combined = pd.concat([history, five], ignore_index=True) if not history.empty else five
                if not combined.empty:
                    result['equity_5m'][symbol] = combined.sort_values('ts').drop_duplicates('ts', keep='last').reset_index(drop=True)
            except Exception as exc:
                result['data_errors'].append(dict(symbol=symbol, phase='AGGREGATION', error_type=type(exc).__name__))
        path = root / f'opening_oi/{symbol_name(row.tradingsymbol)}.json'
        if path.exists():
            try:
                record = json.loads(path.read_text())
                key = row.equity_symbol if not row.is_index_future else row.underlying
                result['opening_oi'][key] = record
            except Exception as exc:
                result['data_errors'].append(dict(file=str(path.relative_to(root)), error_type=type(exc).__name__))
    return result


def collect(now_ist, output_root=ALLOWED_ROOT, **kwargs):
    return Collector(output_root, **kwargs).collect(now_ist)
