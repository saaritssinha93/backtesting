from pathlib import Path
from types import SimpleNamespace
import json

import numpy as np
import pandas as pd
import pytest

from . import data


def candle(start, *, oi=1000., **values):
    return dict(date=start, open=100., high=101., low=99., close=100.5,
                volume=100., oi=oi, **values)


def test_actual_start_labels_become_completed_end_labels():
    rows = [candle('2026-11-08 14:20+05:30'), candle('2026-11-08 14:21+05:30')]
    frame = data.normalize_current(rows, '2026-11-08 14:21:03+05:30', interval_minutes=1)
    assert frame.ts.tolist() == [pd.Timestamp('2026-11-08 14:21', tz=data.IST)]
    assert frame.candle_start.iloc[0] == data.OPEN


def test_completion_buffer_prevents_early_publication():
    frame = data.normalize_current([candle(data.OPEN)], '2026-11-08 14:21:02+05:30', interval_minutes=1)
    assert frame.empty


def test_future_five_minute_extended_session_preserved_cash_capped():
    frame = data.normalize_current([candle('2026-11-08 15:35+05:30')],
        '2026-11-08 15:40:03+05:30', interval_minutes=5, futures=True)
    assert frame.ts.iloc[0] == data.FUTURES_CLOSE
    cash = data.normalize_current([candle('2026-11-08 15:15+05:30')],
        '2026-11-08 15:40:03+05:30', interval_minutes=1)
    assert cash.empty


def test_reject_wrong_day_preopen_invalid_ohlc_and_missing_oi():
    records = [candle('2026-11-06 14:20+05:30'), candle('2026-11-08 14:19+05:30'),
               candle(data.OPEN, oi=np.nan)]
    invalid = candle(data.OPEN)
    invalid['low'] = 102
    records.append(invalid)
    assert data.normalize_current(records, '2026-11-08 14:25:03+05:30', interval_minutes=1, futures=True).empty


@pytest.mark.parametrize('when', ['2026-11-07 14:21+05:30', '2026-11-09 14:21+05:30'])
def test_date_guard_expires(when):
    with pytest.raises(ValueError, match='only on2026-11-08'):
        data.normalize_current([], when, interval_minutes=1)


def minutes():
    return data.normalize_current([candle(data.OPEN + pd.Timedelta(minutes=i)) for i in range(5)],
                                   '2026-11-08 14:25:03+05:30', interval_minutes=1)


def test_aggregate_exact_five_and_preserve_actual_clock():
    result = data.aggregate_five_minute(minutes())
    assert len(result) == 1
    assert result.ts.iloc[0] == data.OPEN + pd.Timedelta(minutes=5)
    assert result.source_1m_count.iloc[0] == 5
    assert result.volume.iloc[0] == 500


def test_missing_duplicate_or_synthetic_bar_cannot_fill_five():
    frame = minutes().drop(index=2)
    assert data.aggregate_five_minute(frame).empty
    assert data.aggregate_five_minute(pd.concat([frame, frame.iloc[:1]])).empty
    frame = minutes()
    frame.loc[2, 'is_synthetic'] = True
    assert data.aggregate_five_minute(frame).empty


def test_fractional_minute_cannot_complete_five():
    frame = minutes()
    frame.loc[2, 'ts'] += pd.Timedelta(seconds=20)
    assert data.aggregate_five_minute(frame).empty


def test_output_cannot_touch_shared_live_runtime(tmp_path):
    with pytest.raises(ValueError, match='isolated'):
        data.Collector('C:/TradingData/eqidv2')
    with pytest.raises(ValueError):
        data.symbol_name('../live')


def test_canonical_source_contract_filename_for_ampersands():
    from fno_oi_common import safe_contract_stem
    for symbol in ('GVT&D26NOVFUT', 'M&M26NOVFUT', 'NIFTY26NOVFUT'):
        assert data.source_contract_stem(symbol) == safe_contract_stem(symbol)


@pytest.fixture
def collector(tmp_path, monkeypatch):
    output = tmp_path / 'isolated'
    monkeypatch.setattr(data, 'ALLOWED_ROOT', output)
    source = tmp_path / 'source'
    universe = tmp_path / 'universe.parquet'
    pd.DataFrame([dict(underlying='ABC', tradingsymbol='ABC26NOVFUT', instrument_token=222,
        expiry=pd.Timestamp('2026-11-23'), is_index_future=False, equity_symbol='ABC',
        equity_instrument_token=111, lot_size=100, contract_month='2026-11')]).to_parquet(universe)
    credentials = [SimpleNamespace(app_name='app1')]

    class Client:
        def __init__(self):
            self.calls = []
            self.invalid = False

        def profile(self):
            if self.invalid:
                raise RuntimeError('private diagnostic must not appear')
            return {}

        def historical_data(self, token, start, end, *, interval, continuous, oi):
            self.calls.append((token, start, end, interval, continuous, oi))
            rows = [candle(data.OPEN + pd.Timedelta(minutes=i)) for i in range(5)]
            return rows[:1] if interval == '5minute' else rows

        def __getattr__(self, name):
            raise AssertionError('Unexpected broker capability: ' + name)

    client = Client()
    instance = data.Collector(output, credential_loader=lambda: credentials,
        client_factory=lambda *a, **k: client, universe_source=universe, source_root=source)
    monkeypatch.setattr(data.time, 'sleep', lambda _: None)
    return instance, client


def test_health_reload_drops_failed_pool_and_redacts_error(collector):
    instance, client = collector
    assert instance.healthcheck()['healthy_apps'] == 1
    client.invalid = True
    status = instance.healthcheck()
    assert status['healthy_apps'] == 0
    assert 'private diagnostic' not in json.dumps(status)
    client.invalid = False
    assert instance.healthcheck()['healthy_apps'] == 1


def test_premarket_health_only_no_historical_calls(collector):
    instance, client = collector
    status = instance.collect('2026-11-08 14:20:59+05:30')
    assert status['state'] == 'WAITING_SESSION'
    assert not client.calls


def test_completed_collection_actual_opening_oi_and_reusable_cache(collector):
    instance, client = collector
    status = instance.collect('2026-11-08 14:25:03+05:30')
    assert status['state'] == 'COMPLETE'
    assert len(client.calls) == 3
    frames = instance.load_frames()
    assert len(frames['equity_1m']['ABC']) == 5
    assert len(frames['equity_5m']['ABC']) == 1
    opening = frames['opening_oi']['ABC']
    assert opening['oi'] == 1000
    assert pd.Timestamp(opening['candle_end']) == data.OPEN + pd.Timedelta(minutes=1)
    assert opening['futurecontract'] == 'ABC26NOVFUT'
    assert instance.collect('2026-11-08 14:25:04+05:30')['jobs'] == 0
    assert len(client.calls) == 3


def test_missing_interior_minute_is_refetched(collector):
    instance, client = collector
    instance.collect('2026-11-08 14:25:03+05:30')
    path = instance.root / 'equity_1m/ABC.parquet'
    frame = pd.read_parquet(path).drop(index=2)
    frame.to_parquet(path,index=False)
    status = instance.collect('2026-11-08 14:25:04+05:30')
    assert status['jobs'] == 1
    assert len(pd.read_parquet(path)) == 5
    assert pd.Timestamp(client.calls[-1][1]) == data.OPEN + pd.Timedelta(minutes=2)


def test_expired_contract_refused_before_any_fetch(collector):
    instance, client = collector
    universe = pd.read_parquet(instance.universe_source)
    universe['expiry'] = pd.Timestamp('2026-11-07')
    universe.to_parquet(instance.universe_source,index=False)
    with pytest.raises(ValueError, match='expired'):
        instance.collect('2026-11-08 14:25:03+05:30')
    assert not client.calls


def test_bad_cache_does_not_block_other_frames_or_raise_global(collector):
    instance, client = collector
    instance.collect('2026-11-08 14:25:03+05:30')
    (instance.root / 'equity_1m/BROKEN.parquet').write_bytes(b'not parquet')
    frames = instance.load_frames()
    assert 'ABC' in frames['equity_1m']
    assert 'BROKEN' not in frames['equity_1m']
    assert frames['data_errors']
    (instance.root / 'equity_1m/ABC.parquet').write_bytes(b'bad existing cache')
    status = instance.collect('2026-11-08 14:25:04+05:30')
    assert status['state'] == 'PARTIAL'
    assert status['equities_available'] == 0
    assert status['cache_errors']
    assert 'ABC26NOVFUT' in instance.load_frames()['futures_5m']


def test_corrupt_opening_record_refetched(collector):
    instance, client = collector
    instance.collect('2026-11-08 14:25:03+05:30')
    (instance.root / 'opening_oi/ABC26NOVFUT.json').write_text('broken')
    status = instance.collect('2026-11-08 14:25:04+05:30')
    assert status['jobs'] == 1
    assert instance.load_frames()['opening_oi']['ABC']['oi'] == 1000


def test_failed_app_does_not_remove_healthy_app(collector):
    instance, client = collector

    class TokenException(Exception):
        pass

    class FailingClient:
        def profile(self):
            return {}

        def historical_data(self, *args, **kwargs):
            raise TokenException('private')

    bad = FailingClient()
    credentials = [SimpleNamespace(app_name='app1'), SimpleNamespace(app_name='app2')]
    instance.credential_loader = lambda: credentials
    instance.client_factory = lambda credential, **kw: bad if credential.app_name == 'app1' else client
    status = instance.collect('2026-11-08 14:25:03+05:30')
    assert status['healthy_apps'] == 1
    assert status['state'] == 'COMPLETE'
    assert [lane.name for lane in instance.lanes] == ['app2']
    assert instance.last_health['needs_recheck']
    assert len(instance.load_frames()['equity_1m']['ABC']) == 5


def test_cycle_budget_defers_work_and_rotates_coherent_instrument_groups(collector, monkeypatch):
    instance, client = collector
    original = pd.read_parquet(instance.universe_source).iloc[0].to_dict()
    rows = []
    for index, symbol in enumerate(('ABC', 'DEF', 'GHI')):
        rows.append(dict(original, underlying=symbol, tradingsymbol=symbol + '26NOVFUT',
                         equity_symbol=symbol, equity_instrument_token=111 + index,
                         instrument_token=222 + index))
    rows.append(dict(original, underlying='NIFTY', tradingsymbol='NIFTY26NOVFUT',
                     is_index_future=True, equity_symbol=None, equity_instrument_token=np.nan,
                     instrument_token=900))
    pd.DataFrame(rows).to_parquet(instance.universe_source, index=False)
    instance.max_cycle_seconds = 6.5
    clock = [100.]
    monkeypatch.setattr(data.time, 'monotonic', lambda: clock[0])
    fetch = client.historical_data

    def delayed(*args, **kwargs):
        clock[0] += 1.
        return fetch(*args, **kwargs)

    client.historical_data = delayed
    first = instance.collect('2026-11-08 14:25:03+05:30')
    assert first['state'] == 'PARTIAL'
    assert first['collection_elapsed_seconds'] == 7.
    assert first['deferred_jobs'] == 4
    assert [call[0] for call in client.calls[:2]] == [900, 900]
    frames = instance.load_frames()
    assert 'ABC' in frames['equity_1m'] and 'ABC' in frames['opening_oi']
    assert 'ABC26NOVFUT' in frames['futures_5m']
    assert 'GHI' not in frames['equity_1m']
    calls_before = len(client.calls)
    second = instance.collect('2026-11-08 14:25:04+05:30')
    assert client.calls[calls_before][0] == 113  # rotating continuation starts with GHI
    assert second['state'] == 'COMPLETE'
    assert second['deferred_jobs'] == 0
    assert set(instance.load_frames()['opening_oi']) == {'ABC', 'DEF', 'GHI', 'NIFTY'}


def test_cycle_deadline_prevents_timeout_retry_and_queued_calls(collector, monkeypatch):
    instance, client = collector
    instance.max_cycle_seconds = 1.
    clock = [100.]
    monkeypatch.setattr(data.time, 'monotonic', lambda: clock[0])
    attempts = []

    def timeout(*args, **kwargs):
        attempts.append(args)
        clock[0] += 8.
        raise TimeoutError('private network details')

    client.historical_data = timeout
    status = instance.collect('2026-11-08 14:25:03+05:30')
    assert len(attempts) == 1
    assert status['collection_elapsed_seconds'] == 8.
    assert status['deferred_jobs'] == 3
    assert status['healthy_apps'] == 1
    assert status['state'] == 'PARTIAL'
    assert 'private network details' not in json.dumps(status)
