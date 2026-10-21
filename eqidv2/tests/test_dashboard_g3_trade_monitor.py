"""G3 monitoring is visible, authenticated, advisory-only and safe to refresh."""
from collections import OrderedDict
from datetime import datetime
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
from types import ModuleType, SimpleNamespace
import sys

import pytest

import dashboard_g3_trade_monitor as adapter
import log_dashboard_server as dashboard
from tests.test_dashboard_cockpit_routes import Handler


@pytest.fixture(autouse=True)
def reset_cache(monkeypatch):
    monkeypatch.setattr(dashboard, '_DASH_READ_CACHE', OrderedDict())
    monkeypatch.setattr(dashboard, '_DASH_READ_INFLIGHT', {})


@pytest.mark.parametrize('path', ['/api/g3-trade-monitor', '/api/g3-trade-monitor/backtest',
    '/g3-trade-monitor-backtest/report.html'])
def test_authentication_precedes_all_monitor_reads(monkeypatch, path):
    calls = []
    monkeypatch.setattr(adapter, 'load_snapshot', lambda *a: calls.append(a))
    monkeypatch.setattr(adapter, 'load_backtest_payload', lambda *a: calls.append(a))
    monkeypatch.setattr(adapter, 'backtest_artifact', lambda *a: calls.append(a))
    h = Handler(path, authorized=False)
    h.do_GET()
    assert h.status == 401 and calls == []


def test_snapshot_route_accepts_token_auth_and_uses_selected_date(monkeypatch):
    calls = []
    monkeypatch.setattr(adapter, 'load_snapshot', lambda day: calls.append(day) or {'session_date': day})
    h = Handler('/api/g3-trade-monitor?token=monitor-test&date=2026-10-13', authorized=False)
    h.server.api_token = 'monitor-test'
    h.do_GET()
    assert h.status == 200
    assert calls == ['2026-10-13']
    assert json.loads(h.wfile.getvalue())['session_date'] == '2026-10-13'
    assert h.response_headers['Cache-Control'] == 'no-store'


@pytest.mark.parametrize('query', ['date=2026-13-01', 'date=2099-01-01', 'date=../../secrets',
    'date=2026-10-13&date=2026-10-12', 'date=2026-1-1'])
def test_invalid_date_is_rejected_before_read(monkeypatch, query):
    calls = []
    monkeypatch.setattr(adapter, 'load_snapshot', lambda day: calls.append(day))
    h = Handler('/api/g3-trade-monitor?' + query)
    h.do_GET()
    assert h.status == 400 and not calls


def test_backend_failures_do_not_expose_paths_or_credentials(monkeypatch):
    def broken(day):
        raise RuntimeError(r'C:\private\token.json secret=not-for-client')
    monkeypatch.setattr(adapter, 'load_snapshot', broken)
    h = Handler('/api/g3-trade-monitor?date=2026-10-13')
    h.do_GET()
    assert h.status == 503
    assert 'private' not in h.wfile.getvalue().decode()
    assert 'not-for-client' not in h.wfile.getvalue().decode()


def test_current_day_reads_atomic_snapshot_while_past_day_uses_readonly_builder(monkeypatch):
    calls = []
    backend = ModuleType('g3_trade_monitor')
    def snapshot(day, **kwargs):
        return dict(session_date=day.isoformat(), read_only=True, advisory_only=True,
                    execution_authority=False, trades=[], pending=[])
    backend.load_latest = lambda **kw: calls.append(('latest', kw['day'])) or snapshot(**kw)
    backend.build_snapshot = lambda day, **kw: calls.append(('history', day)) or snapshot(day, **kw)
    monkeypatch.setitem(sys.modules, 'g3_trade_monitor', backend)
    now = datetime(2026, 10, 14, 11, 0, tzinfo=adapter.IST)
    adapter.load_snapshot('2026-10-14', now=now)
    adapter.load_snapshot('2026-10-13', now=now)
    assert [(kind, str(day)) for kind, day in calls] == [('latest', '2026-10-14'), ('history', '2026-10-13')]


def test_snapshot_projection_keeps_nulls_but_removes_internal_evidence(monkeypatch):
    backend = ModuleType('g3_trade_monitor')
    backend.load_latest = lambda **kw: dict(session_date='2026-10-14', read_only=True,
        advisory_only=True, execution_authority=False, secret_token='secret',
        trades=[dict(symbol='AAA', mode='PAPER', mark_price=None, metrics=dict(mfe_pct=float('nan')),
                     source_path=r'C:\private\file', entry_price=100.)],
        pending=[dict(symbol='PENDING', mode='LIVE', status='TRIGGER_PENDING', access_token='secret')])
    monkeypatch.setitem(sys.modules, 'g3_trade_monitor', backend)
    result = adapter.load_snapshot('2026-10-14', now=datetime(2026, 10, 14, 11, tzinfo=adapter.IST))
    assert result['trades'][0]['mark_price'] is None
    assert result['trades'][0]['metrics']['mfe_pct'] is None
    assert 'secret' not in json.dumps(result) and 'private' not in json.dumps(result)
    assert len(result['pending']) == 1


@pytest.mark.parametrize('name', ['../secret.json', 'C:/secret.json', 'ledgers/../../token.json', 'unknown.csv'])
def test_report_artifact_names_are_allowlisted_before_loading(monkeypatch, name):
    monkeypatch.setattr(adapter, '_backtest', lambda: pytest.fail('must reject before loading'))
    with pytest.raises(ValueError):
        adapter.backtest_artifact(name)


def test_report_links_keep_authentication_and_cannot_run_script(monkeypatch):
    monkeypatch.setattr(adapter, 'backtest_artifact', lambda name: (
        b'<html><a href="daily_comparison.csv">CSV</a></html>', 'text/html; charset=utf-8'))
    h = Handler('/g3-trade-monitor-backtest/report.html?token=monitor-test', authorized=False)
    h.server.api_token = 'monitor-test'
    h.do_GET()
    assert h.status == 200
    assert '/g3-trade-monitor-backtest/daily_comparison.csv?token=monitor-test' in h.wfile.getvalue().decode()
    assert "default-src 'none'" in h.response_headers['Content-Security-Policy']


def test_verified_downloads_support_fixed_nested_ledgers_and_redact_json_paths(monkeypatch, tmp_path):
    directory = tmp_path / 'runs' / 'run'
    (directory / 'ledgers').mkdir(parents=True)
    (directory / 'ledgers/baseline.csv').write_text('day,net_pnl_rs\n2026-10-13,123\n')
    (directory / 'manifest.json').write_text(json.dumps(dict(files={'ledgers/baseline.csv': 'digest'},
        source={'root': r'C:\private\data'}, code_sha256={r'C:\private\source.py': 'digest'})))
    monkeypatch.setattr(adapter, 'runtime_dir', lambda _: tmp_path)
    monkeypatch.setattr(adapter, '_backtest', lambda: dict(run_dir=str(directory),
        manifest={'files': {'ledgers/baseline.csv': 'digest'}}))
    body, mime = adapter.backtest_artifact('ledgers/baseline.csv')
    assert b'123' in body and mime.startswith('text/csv')
    body, _ = adapter.backtest_artifact('manifest.json')
    assert b'private' not in body and b'digest' in body


def test_main_dashboard_embeds_visible_tab_group_cards_and_no_execution_buttons():
    h = Handler('/')
    h._send_html()
    html = h.wfile.getvalue().decode()
    assert 'data-terminal-view="g3monitor"' in html
    assert 'G3 Trade Monitor ·15s' in html and 'G3 Monitor Backtest ·1m' in html
    assert 'key: "g3-trade-monitoring"' in html
    assert 'G3TradeMonitor.mount' in html
    assert '__G3_TRADE_MONITOR_' not in html
    script = (Path(dashboard.__file__).parent / 'dashboard_g3_trade_monitor.js').read_text(encoding='utf-8')
    for forbidden in ('/api/restart', '/api/kill', '/orders', 'method: \'POST\'', 'method: "POST"'):
        assert forbidden not in script
    node = shutil.which('node')
    if node:
        scripts = re.findall(r'<script\b[^>]*>(.*?)</script>', html, flags=re.S | re.I)
        checked = subprocess.run([node, '--check'], input='\n'.join(scripts), capture_output=True, encoding='utf-8')
        assert checked.returncode == 0, checked.stderr


def test_monitor_css_uses_the_shared_light_and_dark_terminal_palette():
    css = (Path(dashboard.__file__).parent / 'dashboard_g3_trade_monitor.css').read_text(encoding='utf-8')
    for undefined in ('--terminal-text', '--terminal-border', '--terminal-accent'):
        assert undefined not in css
    for shared in ('--terminal-ink', '--terminal-panel', '--terminal-bg', '--terminal-muted',
                   '--terminal-line', '--terminal-indigo'):
        assert shared in css
    assert 'body[data-theme="dark"] .g3tm-panel' in css
    assert 'color-scheme: dark' in css
    assert 'body[data-theme="dark"] .g3tm-direction-favorable' in css
    assert '.g3tm-heading h2' in css and '.g3tm-book h3' in css


def test_reporting_cards_remain_visible_when_worker_has_no_snapshot(monkeypatch):
    monkeypatch.setattr(adapter, 'load_snapshot', lambda: (_ for _ in ()).throw(FileNotFoundError()))
    items = adapter.session_items()
    assert [item['id'] for item in items] == ['g3_trade_monitor', 'g3_trade_monitor_backtest']
    assert all(item['card_kind'] == 'view' and item['status']['reporting_only'] == 'true' for item in items)
    assert items[0]['status']['status'] == 'WAITING'


def test_monitor_schedule_restarts_after_an_unexpected_worker_failure():
    source = (Path(dashboard.__file__).parent / 'bat' / 'schedule_g3_trade_monitor.ps1').read_text(encoding='utf-8')
    assert '-RestartCount 3' in source
    assert '-RestartInterval (New-TimeSpan -Minutes 1)' in source
    assert "RestartCount -ne 3" in source
    assert "RestartInterval -ne 'PT1M'" in source


def _browser(scenario):
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is required for monitor UI tests')
    source = (Path(dashboard.__file__).parent / 'dashboard_g3_trade_monitor.js').read_text(encoding='utf-8')
    scaffold = r'''
    global.window={location:{origin:'http://127.0.0.1:8787'}};
    const visibility={};global.document={hidden:false,addEventListener:(n,f)=>visibility[n]=f,removeEventListener:()=>{}};
    const intervals=[];global.setInterval=(fn,ms)=>{intervals.push({fn,ms});return intervals.length;};global.clearInterval=()=>{};
    const flush=()=>new Promise(resolve=>setImmediate(resolve));
    function host(){const zones=Object.fromEntries(['status','feed','warnings','trades','research'].map(k=>[k,{innerHTML:'',textContent:''}]));return {zones,innerHTML:'',isConnected:true,listeners:{},querySelector(s){return zones[s.match(/data-zone="([^"]+)"/)[1]];},addEventListener(n,f){this.listeners[n]=f;}};}
    const payload=day=>({session_date:day,read_only:true,advisory_only:true,execution_authority:false,state:'READY',generated_at_ist:'2026-10-14T10:01:00+05:30',modes:{PAPER:{open:1,closed:0,pending:1},LIVE:{open:0,closed:1,pending:0,no_trades_reason:'LIVE is disarmed'}},trades:[{id:'paper',symbol:'AAA<script>alert(1)</script>',mode:'PAPER',side:'LONG',status:'OPEN',entry_price:100,mark_price:null,mark_state:'NO_QUOTE',metrics:{mfe_pct:null},reasons:['<img src=x onerror=bad()>']},{id:'live',symbol:'LIVESTOCK',mode:'LIVE',side:'SHORT',status:'CLOSED',entry_price:100,mark_price:99,mark_state:'STALE',recorded_net_pnl_rs:100,metrics:{gross_unrealized_pnl_rs:null},reasons:['Recorded exit']}],pending:[{symbol:'PENDING_ONLY',id:'pending',mode:'PAPER',status:'TRIGGER_PENDING'}],warnings:[]});
    '''
    run = subprocess.run([node, '-'], input=scaffold + source + '\n(async()=>{' + scenario + '\n})().catch(e=>{console.error(e);process.exitCode=1;});',
                         capture_output=True, encoding='utf-8', timeout=30)
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)


def test_ui_separates_modes_pending_and_missing_price_and_escapes_evidence():
    result = _browser(r'''
      global.fetch=async(url)=>({ok:true,json:async()=>url.pathname.endsWith('/backtest')?{state:'NO_VERIFIED_REPORT'}:payload(url.searchParams.get('date'))});
      const h=host(),ui=window.G3TradeMonitor.mount(h,{});ui.setActive(true);await flush();await flush();
      const paper=h.zones.trades.innerHTML;
      h.listeners.change({target:{dataset:{filter:'mode'},value:'LIVE'}});h.listeners.change({target:{dataset:{filter:'status'},value:'CLOSED'}});
      const live=h.zones.trades.innerHTML;ui.destroy();process.stdout.write(JSON.stringify({paper,live,interval:intervals[0].ms}));
    ''')
    assert result['interval'] == 15000
    assert '&lt;script&gt;' in result['paper'] and '<script>' not in result['paper']
    assert '&lt;img' in result['paper'] and '<img' not in result['paper']
    assert 'NO_QUOTE' in result['paper'] and 'Unavailable' in result['paper']
    assert 'Pending signals · 1 · not executed trades' in result['paper']
    assert 'LIVESTOCK' not in result['paper']
    assert 'LIVESTOCK' in result['live'] and 'STALE' in result['live']
    assert 'PENDING_ONLY' not in result['live']


def test_ui_refresh_does_not_overlap_and_stops_hidden_then_recovers():
    result = _browser(r'''
      const calls=[];let resolveRequest;
      global.fetch=(url,options)=>{if(url.pathname.endsWith('/backtest'))return Promise.resolve({ok:true,json:async()=>({state:'NO_VERIFIED_REPORT'})});calls.push({url:String(url),options});return new Promise(resolve=>resolveRequest=()=>resolve({ok:true,json:async()=>payload(url.searchParams.get('date'))}));};
      const h=host(),ui=window.G3TradeMonitor.mount(h,{});ui.setActive(true);intervals[0].fn();intervals[0].fn();const active=calls.length;
      document.hidden=true;visibility.visibilitychange();intervals[0].fn();const hidden=calls.length,aborted=calls[0].options.signal.aborted;
      resolveRequest();await flush();await flush();document.hidden=false;visibility.visibilitychange();const recovered=calls.length;
      resolveRequest();await flush();await flush();ui.destroy();process.stdout.write(JSON.stringify({active,hidden,aborted,recovered}));
    ''')
    assert result == dict(active=1, hidden=1, aborted=True, recovered=2)


def test_ui_rejects_wrong_session_and_readwrite_payload():
    result = _browser(r'''
      global.fetch=async(url)=>({ok:true,json:async()=>url.pathname.endsWith('/backtest')?{state:'NO_VERIFIED_REPORT'}:{...payload('1999-01-01'),execution_authority:true}});
      const h=host(),ui=window.G3TradeMonitor.mount(h,{});ui.setActive(true);await flush();await flush();ui.destroy();process.stdout.write(JSON.stringify({status:h.zones.status.textContent,rows:h.zones.trades.innerHTML}));
    ''')
    assert 'mismatch' in result['status'] and 'AAA' not in result['rows']


def test_ui_includes_pending_square_off_as_open_and_suppresses_stale_flags():
    result = _browser(r'''
      const data=payload('2026-10-14');data.trades[0].status='SQUARE_OFF_PENDING';
      data.trades[0].advisory_flags=['EXIT_REVIEW'];data.snapshot_stale=true;
      const rows=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      const absent=window.G3TradeMonitor.tradeMarkup({...data,state:'NO_SNAPSHOT'},'PAPER','OPEN','');
      process.stdout.write(JSON.stringify({rows,absent}));
    ''')
    assert 'SQUARE_OFF_PENDING' in result['rows'] and 'EXIT_REVIEW' not in result['rows']
    assert 'Suppressed for stale' in result['rows']
    assert 'Unavailable%' not in result['rows']
    assert 'trade counts and results are unavailable' in result['absent']
    assert 'AAA' not in result['absent']


def test_ui_hidden_tab_aborts_research_and_recovers_without_late_render():
    result = _browser(r'''
      const calls=[];let finish;
      global.fetch=(url,options)=>{
        if(!url.pathname.endsWith('/backtest'))return Promise.resolve({ok:true,json:async()=>payload(url.searchParams.get('date'))});
        calls.push(options);return new Promise(resolve=>finish=()=>resolve({ok:true,json:async()=>({state:'VERIFIED',read_only:true,execution_authority:false,summary:{},policy_results:[{policy:'LATE'}]})}));};
      const h=host(),ui=window.G3TradeMonitor.mount(h,{});ui.setActive(true);await flush();await flush();
      document.hidden=true;visibility.visibilitychange();const aborted=calls[0].signal.aborted;
      finish();await flush();await flush();const hidden=h.zones.research.innerHTML;
      document.hidden=false;visibility.visibilitychange();await flush();await flush();const recovered=calls.length;
      finish();await flush();await flush();
      ui.destroy();process.stdout.write(JSON.stringify({aborted,hidden,recovered}));
    ''')
    assert result['aborted'] and result['recovered'] == 2
    assert 'LATE' not in result['hidden']


def test_book_summary_has_separate_direction_coverage_pnl_and_stale_guard():
    result = _browser(r'''
      const data=payload('2026-10-14');data.modes.PAPER={open:2,closed:1,pending:0,open_count:2,eligible_count:2,overall_direction:'FAVORABLE',overall_reason:'All open positions eligible',attention_count:1,gross_marked_pnl_rs:1200,gross_marked_pnl_coverage_count:2,open_direction_counts:{FAVORABLE:2,ADVERSE:0,MIXED:0,WARMING_UP:0,STALE:0,NO_DATA:0}};
      const ready=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      data.modes.PAPER.eligible_count=1;const incomplete=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      data.snapshot_stale=true;const stale=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      process.stdout.write(JSON.stringify({ready,incomplete,stale}));
    ''')
    assert 'Overall open-position direction' in result['ready']
    assert 'g3tm-direction-favorable' in result['ready']
    assert '2 / 2' in result['ready'] and '1,200.00' in result['ready']
    assert 'Positions needing attention' in result['ready']
    assert result['ready'].index('g3tm-book-summary') < result['ready'].index('g3tm-scroll')
    assert 'INSUFFICIENT DATA' in result['incomplete']
    assert '1,200.00' not in result['stale']
    assert 'Snapshot is stale' in result['stale']


def test_attention_card_lists_open_stock_names_and_flag_types_safely():
    result = _browser(r'''
      const data=payload('2026-10-14');
      data.modes.PAPER={open:2,closed:1,pending:0,open_count:2,eligible_count:2,overall_direction:'MIXED',overall_reason:'Mixed book',attention_count:2,gross_marked_pnl_rs:100,gross_marked_pnl_coverage_count:2,open_direction_counts:{FAVORABLE:0,ADVERSE:0,MIXED:2,WARMING_UP:0,STALE:0,NO_DATA:0}};
      data.trades=[
        {symbol:'M&M<script>',mode:'PAPER',side:'LONG',status:'OPEN',entry_price:100,advisory_flags:['TREND_FAILURE'],metrics:{},reasons:[]},
        {symbol:'PNB',mode:'PAPER',side:'SHORT',status:'SQUARE_OFF_PENDING',entry_price:100,advisory_flags:['PROFIT_GIVEBACK'],metrics:{},reasons:[]},
        {symbol:'OLDCO',mode:'PAPER',side:'LONG',status:'CLOSED',entry_price:100,advisory_flags:['TREND_FAILURE'],metrics:{},reasons:[]}
      ];
      const ready=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      data.snapshot_stale=true;const stale=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      data.snapshot_stale=false;data.historical=true;const historical=window.G3TradeMonitor.tradeMarkup(data,'PAPER','OPEN','');
      process.stdout.write(JSON.stringify({ready,stale,historical}));
    ''')
    assert 'M&amp;M&lt;script&gt;' in result['ready'] and 'TREND FAILURE' in result['ready']
    assert 'PNB' in result['ready'] and 'PROFIT GIVEBACK' in result['ready']
    assert 'OLDCO' not in result['ready']
    assert '<script>' not in result['ready']
    assert 'Current flagged stock names are unavailable' in result['stale']
    assert 'Historical observations do not establish a current overall direction' in result['historical']


def test_panel_explains_every_supported_advisory_flag_and_direction_states():
    result = _browser(r'''
      const h=host(),ui=window.G3TradeMonitor.mount(h,{});const markup=h.innerHTML;ui.destroy();
      process.stdout.write(JSON.stringify({markup}));
    ''')['markup']
    assert 'Advisory flag meanings' in result
    assert 'TREND FAILURE' in result and 'three adverse closes' in result
    assert 'signed 5-minute momentum at or below' in result and '0.15%' in result
    assert 'signed move from entry at or below' in result and '0.30%' in result
    assert 'PROFIT GIVEBACK' in result and 'at least 0.50%' in result
    assert 'at least 0.30 percentage points' in result
    assert 'direction/evidence states, not advisory flags' in result


def test_market_feed_partial_state_and_warnings_are_visible_and_escaped():
    result = _browser(r'''
      const data=payload('2026-10-14');data.market_data_feed={state:'PARTIAL_MARKET_DATA',quotes:1,symbols:2,generated_at_ist:'2026-10-14T12:00:00+05:30',errors:['A&B:<img src=x onerror=bad()>'],warming_up_symbols:['WAITING']};
      const html=window.G3TradeMonitor.healthMarkup(data);process.stdout.write(JSON.stringify({html}));
    ''')
    assert 'PARTIAL_MARKET_DATA' in result['html'] and 'Quotes 1 / 2' in result['html']
    assert 'Minute data warming up: WAITING' in result['html']
    assert 'Feed warnings:' in result['html']
    assert '&lt;img' in result['html'] and '<img' not in result['html']
