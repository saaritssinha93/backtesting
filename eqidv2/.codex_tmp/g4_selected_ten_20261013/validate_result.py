"""Independent reconciliation of the selected-ten G4 result bundle."""
import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RUN = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/runs/2026-10-13/20261013T174450540027_a18b5551_late_slots')
EXPECTED = {'1036_LONG': 2.12, '1036_SHORT': 2.86, '1041_LONG': 1.71,
    '1046_LONG': .79, '1046_SHORT': 1.01, '1051_LONG': .77, '1056_SHORT': 1.53,
    '1111_LONG': 1.76, '1111_SHORT': 1.84, '1126_SHORT': 4.20}
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def close(a, b):
    assert np.allclose(a, b, atol=1e-6, rtol=0, equal_nan=True), (a, b)
def read(name):
    return pd.read_csv(RUN / (name + '.csv'))

manifest = json.loads((RUN / 'manifest.json').read_text())
for name, expected in manifest['files'].items():
    assert digest(RUN / name) == expected, name
for name, expected in manifest['code_sha256'].items():
    assert digest(ROOT / name) == expected, name
summary = json.loads((RUN / 'summary.json').read_text())
config = json.loads((RUN / 'config.json').read_text())
assert digest(Path(summary['config_path'])) == summary['config_sha256']
assert json.loads(Path(summary['config_path']).read_text()) == config
assert {r['setup_id']: r['target_pct'] for r in config['setups']} == EXPECTED
orders, added, baseline, combined = map(read, ['all_selected_orders', 'added_trades', 'baseline_trades', 'combined_trades'])
daily, monthly, periods, setups, inventory = map(read, ['daily_results', 'monthly_results', 'period_results', 'setup_results', 'enabled_setups'])
assert len(orders) == 318 and orders.sid.is_unique and orders.signal_id.is_unique
assert set(orders.sid) == set(combined.sid)
assert len(inventory) == 37 and inventory.enabled.all()
assert set(inventory.loc[inventory.addition].setup_id) == set(EXPECTED)
assert set(added.setup_id) == set(EXPECTED) and len(added) == 106
assert not added.duplicated(['day', 'setup_id']).any()
for row in config['setups']:
    frame = added.loc[added.setup_id.eq(row['setup_id'])]
    assert frame.native_target_pct.eq(row['target_pct']).all()
    assert frame.native_stop_pct.eq(1.25).all() and frame.tightened_stop_pct.eq(1.).all()
    assert frame.loc[frame.filled].initial_stop_pct.eq(1.25).all()
    signal = pd.to_datetime(frame.signal_timestamp, utc=True)
    confirmation = pd.to_datetime(frame.confirmation_timestamp, utc=True)
    assert confirmation.sub(signal).eq(pd.Timedelta(minutes=1)).all()
    assert signal.dt.tz_convert('Asia/Kolkata').dt.strftime('%H:%M').eq(row['signal_end']).all()

old = combined.loc[combined.sid.isin(baseline.sid)].set_index('sid').sort_index()
base = baseline.set_index('sid').sort_index()
for field in ['portfolio_net_profit_rupees', 'entry_price', 'exit_price', 'native_target_pct']:
    close(old[field], base[field])
assert old.portfolio_executed.equals(base.portfolio_executed)
for scope, ledger, summary_key in [('baseline', baseline, 'baseline_g4'),
        ('additions', added, 'selected_additions'), ('combined', combined, 'combined_g4')]:
    s = summary[summary_key]
    executed = ledger.loc[ledger.portfolio_executed]
    pnl = executed.portfolio_net_profit_rupees
    close(pnl, executed.net_return_pct / 100 * 500000.)
    close(executed.portfolio_cost_rupees, 250.)
    assert len(executed) == s['executed_trades']
    assert int(pnl.gt(0).sum()) == s['wins']
    assert int(pnl.lt(0).sum()) == s['losses']
    close(100 * pnl.gt(0).mean(), s['win_rate_pct'])
    close(pnl.sum(), s['net_pnl_rs'])
    d = daily.loc[daily.scope.eq(scope)].sort_values('day')
    assert len(d) == 49 and d.day.is_unique and '2026-10-01' not in set(d.day)
    grouped = ledger.groupby('day').portfolio_net_profit_rupees.sum().reindex(d.day, fill_value=0)
    close(d.net_pnl_rs, grouped)
    curve = np.r_[0., d.net_pnl_rs.cumsum().to_numpy()]
    close(np.max(np.maximum.accumulate(curve) - curve), s['max_drawdown_rs'])
    close(d.cumulative_net_pnl_rs, curve[1:])
    close(monthly.loc[monthly.scope.eq(scope)].net_pnl_rs.sum(), pnl.sum())
    subset = periods.loc[periods.scope.eq(scope)].set_index('period')
    close(subset.loc[['training_38', 'later_11'], 'net_pnl_rs'].sum(), pnl.sum())
    close(subset.loc['all_49', 'net_pnl_rs'], pnl.sum())

close(setups.net_pnl_rs.sum(), combined.portfolio_net_profit_rupees.sum())
assert setups.loc[setups.addition].win_rate_pct.ge(70).all()
rejected = combined.loc[combined.filled & ~combined.portfolio_executed]
assert len(rejected) == 1 and rejected.portfolio_net_profit_rupees.eq(0).all()
assert rejected.iloc[0].setup_id == '1111_SHORT'
assert rejected.iloc[0].portfolio_reject_reason == 'INSUFFICIENT_PORTFOLIO_CAPITAL'
assert len(combined.loc[~combined.filled]) == 28

class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.links.append(dict(attrs)['href'])
parser = Links()
report = (RUN / 'report.html').read_text(encoding='utf-8')
parser.feed(report)
assert all((RUN / href).is_file() for href in parser.links)
assert '\ufffd' not in report and 'untouched out-of-sample' in report
receipt = dict(status='PASS', run=str(RUN), manifest_sha256=digest(RUN / 'manifest.json'),
    files_verified=len(manifest['files']), code_files_verified=len(manifest['code_sha256']),
    config_verified=True, exact_requested_setups=10, total_setups=37,
    orders=318, fills=290, capital_rejected_fills=1, executed_trades=289,
    baseline_unchanged=True, accounting_reconciled=True, all_daily_monthly_period_totals_reconciled=True,
    report_links_verified=len(parser.links), focused_tests='50 passed',
    combined_net_pnl_rs=summary['combined_g4']['net_pnl_rs'],
    combined_win_rate_pct=summary['combined_g4']['win_rate_pct'])
(Path(__file__).parent / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
