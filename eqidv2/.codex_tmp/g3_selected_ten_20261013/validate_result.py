"""Independent verification of published G3 results and retained live config."""
import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import numpy as np
import pandas as pd
from research.g3_backtest_profile import contract, ADDED_IDS
from research.g3_current_history import verified_package
import backtesting_result_v13_v10_g_3_daily as publisher
import dashboard_flow

HERE = Path(__file__).resolve().parent
DATA = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
def read(path): return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def close(a,b): assert np.allclose(a,b,atol=1e-6,rtol=0,equal_nan=True), (a,b)

pointer = read(DATA / 'latest/current_backtest.json')
run = DATA / pointer['run_dir']
assert sha(run / 'manifest.json') == pointer['manifest_sha256']
summary, manifest = verified_package(run, DATA)
for name, digest in manifest['files'].items(): assert sha(run/name) == digest
for name, digest in read(HERE/'before/production_hashes.json').items(): assert sha(ROOT/name) == digest, name
ledger = pd.read_csv(run/'portfolio_trades.csv')
daily = pd.read_csv(run/'daily_results.csv')
setups = pd.read_csv(run/'setup_results.csv')
monthly = pd.read_csv(run/'monthly_results.csv')
periods = pd.read_csv(run/'period_results.csv')
assert len(setups) == 32 and set(setups.loc[setups.added].setup_id) == ADDED_IDS
assert len(daily) == 50 and daily.day.is_unique and '2026-10-01' not in set(daily.day)
assert ledger.signal_id.is_unique and ledger.strategy_version.eq(contract.STRATEGY_VERSION).all()
executed = ledger.loc[ledger.portfolio_executed]
assert len(executed)==257 and executed.portfolio_net_profit_rupees.gt(0).sum()==192
close(executed.portfolio_net_profit_rupees, executed.net_return_pct/100*500000.)
close(executed.portfolio_cost_rupees, 250.)
for frame in (daily, monthly, setups):
    col = 'net_pnl' if frame is daily else 'net_pnl_rs'
    close(frame[col].sum(), executed.portfolio_net_profit_rupees.sum())
close(daily.net_pnl, ledger.groupby('day').portfolio_net_profit_rupees.sum().reindex(daily.day,fill_value=0))
curve = np.r_[0., daily.net_pnl.cumsum().to_numpy()]
close((np.maximum.accumulate(curve)-curve).max(), summary['current_g3']['max_drawdown_rs'])
for row in contract.ACTIVE_SETUPS:
    assert ledger.loc[ledger.setup_id.eq(row.setup_id)].native_target_pct.eq(row.target_pct).all()
added = ledger.loc[ledger.setup_id.isin(ADDED_IDS)]
assert added.loc[added.filled].initial_stop_pct.eq(1.25).all()
assert added.tightened_stop_pct.eq(1.).all()
assert not added.duplicated(['day','setup_id']).any()
assert setups.loc[setups.added].win_rate_pct.ge(70).all()
# Compare the old 50-session G3 portfolio by economic identity, independent of
# the new canonical version/fingerprint used for this replay.
old_pointer = read(HERE/'before/current_backtest.json')
old = pd.read_csv(DATA/old_pointer['run_dir']/'portfolio_trades.csv')
keys=['day','setup_id','tradingsymbol']
left = ledger.loc[~ledger.setup_id.isin(ADDED_IDS)].set_index(keys).sort_index()
right = old.set_index(keys).sort_index()
assert left.index.equals(right.index)
for field in ('portfolio_net_profit_rupees','entry_price','exit_price','native_target_pct'):
    close(left[field],right[field])
assert left.portfolio_executed.equals(right.portfolio_executed)
for role,name in [('production_daily','current_run.json'),('cumulative_history','current_history.json')]:
    directory, member = publisher._read_verified_pointer(DATA,name,role)
    assert member['strategy_version']==contract.STRATEGY_VERSION
    assert member['metrics']['trades']==6 and member['metrics']['wins']==4
    close(member['metrics']['net_profit_rupees'],daily.iloc[-1].net_pnl)
flow = dashboard_flow.load_flow_data(strategy='V13-V10-G-3')
assert flow['selected_run']['active_setup_count']==32
assert flow['selected_run']['strategy_matches_backtest'] and not flow['selected_run']['strategy_matches_active']
assert flow['summary']['trades']==257 and flow['summary']['sessions']==50
close(flow['summary']['net_pnl'],summary['current_g3']['net_pnl_rs'])
daily_runs = [r for r in flow['runs'] if r.get('role')=='production_daily' and r.get('strategy_matches_backtest')]
assert len(daily_runs)==1
day_flow=dashboard_flow.load_flow_data(run_id=daily_runs[0]['id'])
assert day_flow['summary']['trades']==6
class Links(HTMLParser):
    def handle_starttag(self,tag,attrs):
        if tag=='a': assert (run/dict(attrs)['href']).is_file()
report=(run/'report.html').read_text(encoding='utf-8')
Links().feed(report)
assert '32 setups' in report and '743,590.44' in report and '\ufffd' not in report
receipt=dict(status='PASS',run=str(run),manifest_sha256=pointer['manifest_sha256'],
    sessions=50,active_setups=32,added_setups=10,executed_trades=257,wins=192,losses=65,
    net_pnl_rs=summary['current_g3']['net_pnl_rs'],win_rate_pct=summary['current_g3']['win_rate_pct'],
    max_daily_drawdown_rs=summary['current_g3']['max_drawdown_rs'],
    added_fills=int(added.portfolio_executed.sum()),added_net_pnl_rs=float(added.portfolio_net_profit_rupees.sum()),
    production_files_unchanged=True,existing_g3_trades_unchanged=True,
    historical_daily_and_dashboard_totals_reconciled=True,
    daily_run_id=daily_runs[0]['id'],dashboard_run_id=flow['selected_run']['id'])
(HERE/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
print(monthly[['month','executed_trades','wins','win_rate_pct','net_pnl_rs']].to_string(index=False))
print(periods[['period','scope','executed_trades','win_rate_pct','net_pnl_rs']].to_string(index=False))
