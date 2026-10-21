from datetime import datetime, timedelta
import json

import pytest
from g3_trade_monitor_market_data import IST, MarketDataFeed, completed_bars, quote_rows

NOW = datetime(2026,10,14,12,0,tzinfo=IST)


class QuotesOnly:
    def __init__(self): self.calls=[]
    def quote(self, keys):
        self.calls.append(('quote',keys))
        return {'NSE:M&M':dict(last_price=100., instrument_token=123,
            timestamp=NOW,last_trade_time=NOW-timedelta(seconds=1))}
    def historical_data(self,*args,**kwargs):
        self.calls.append(('history',args,kwargs))
        return [dict(date=NOW-timedelta(minutes=i),open=100,high=101,low=99,close=100,volume=100) for i in (0,1,2)]


def snapshot():
    return dict(session_date=NOW.date().isoformat(),trades=[dict(symbol='M&M',mode='PAPER',status='OPEN',entry_at_ist=(NOW-timedelta(minutes=10)).isoformat())])


def test_only_read_endpoints_and_real_completed_minute_data(tmp_path):
    client=QuotesOnly()
    feed=MarketDataFeed(tmp_path,client=client,sleeper=lambda _:None,clock=lambda:NOW)
    result=feed.refresh(snapshot(),NOW)
    assert result['state']=='READY'
    quote=json.loads((tmp_path/'quotes/latest.json').read_text())['quotes'][0]
    assert quote['symbol']=='M&M' and quote['last_trade_time'] != quote['received_at']
    cache=json.loads(next((tmp_path/'minutes'/str(NOW.date())).glob('*.json')).read_text())
    assert len(cache['bars'])==2 and cache['bars'][-1]['timestamp']==NOW.isoformat()
    feed.refresh(snapshot(),NOW+timedelta(seconds=15))
    assert [call[0] for call in client.calls]==['quote','history','quote']


def test_closed_pending_wrong_day_do_not_call_broker(tmp_path):
    client=QuotesOnly(); feed=MarketDataFeed(tmp_path,client=client,clock=lambda:NOW)
    for status in ('CLOSED','PENDING_ENTRY'):
        value=snapshot();value['trades'][0]['status']=status
        assert feed.refresh(value,NOW)['state']=='NO_OPEN_POSITIONS'
    value=snapshot();value['session_date']='2026-10-13'
    assert feed.refresh(value,NOW)['state']=='OUTSIDE_CURRENT_MARKET_SESSION'
    assert client.calls==[]


def test_prior_day_or_future_last_trade_is_not_fresh():
    client=QuotesOnly();payload=client.quote([])
    for timestamp in (NOW-timedelta(days=1),NOW+timedelta(seconds=1)):
        payload['NSE:M&M']['last_trade_time']=timestamp
        assert quote_rows(payload,['M&M'],NOW)==[]


def test_invalid_candle_and_auth_failure_are_not_hidden(tmp_path):
    with pytest.raises(ValueError):
        completed_bars([dict(date=NOW-timedelta(minutes=1),open=100,high=99,low=98,close=100,volume=1)],NOW.date(),NOW)
    class Bad:
        def quote(self, keys): raise RuntimeError('sensitive-token-must-not-escape')
    feed=MarketDataFeed(tmp_path,client=Bad(),clock=lambda:NOW)
    result=feed.refresh(snapshot(),NOW)
    assert result['state']=='MARKET_DATA_UNAVAILABLE'
    assert 'sensitive-token' not in json.dumps(result)
    assert feed.refresh(snapshot(),NOW+timedelta(seconds=15))['state']=='WAITING_MARKET_DATA_RETRY'


def test_new_entry_without_a_completed_bar_is_warming_up(tmp_path):
    class EmptyHistory(QuotesOnly):
        def historical_data(self, *args, **kwargs):
            self.calls.append(('history', args, kwargs))
            return []

    value = snapshot()
    value['trades'][0]['entry_at_ist'] = (NOW-timedelta(seconds=30)).isoformat()
    result = MarketDataFeed(tmp_path, client=EmptyHistory(), sleeper=lambda _: None,
                            clock=lambda: NOW).refresh(value, NOW)
    assert result['state'] == 'READY'
    assert result['warming_up_symbols'] == ['M&M']
    assert result['errors'] == []
