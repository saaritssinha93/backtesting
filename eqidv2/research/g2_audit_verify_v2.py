"""Read-only independent reconciliation of the completed October 9 research report."""
from __future__ import annotations

import argparse
import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

import numpy as np
import pandas as pd


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []
        self.ids = set()
        self.tables = 0

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if 'href' in a:
            self.hrefs.append(a['href'])
        if 'id' in a:
            self.ids.add(a['id'])
        self.tables += tag == 'table'


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def verify(root):
    root = Path(root).resolve()
    read = lambda name: pd.read_csv(root/name, low_memory=False)
    obj = lambda name: json.loads((root/name).read_text(encoding='utf-8'))
    m = read('movement/universe_movements.csv')
    summary = obj('movement/summary.json')
    up = read('movement/gainers_ge_2pct.csv')
    down = read('movement/decliners_le_minus2pct.csv')
    assert len(m) == 213 and m.symbol.is_unique
    for price, ret in [('official_high','peak_pct'),('official_low','trough_pct'),('official_close','close_return_pct')]:
        assert np.allclose((m[price]/m.official_previous_close-1)*100,m[ret],atol=1e-9)
    assert set(up.symbol) == set(m.loc[m.peak_pct.ge(2),'symbol']) and len(up) == 105
    assert set(down.symbol) == set(m.loc[m.trough_pct.le(-2),'symbol']) and len(down) == 15
    assert set(up.symbol) & set(down.symbol) == {'ATHERENERG','BHEL'}
    assert abs(m.close_return_pct.mean()-summary['equal_weight_official_close_to_close_return_pct']) < 1e-9
    for group in summary['groups'].values():
        assert abs(group['count']/len(m)*100-group['pct_of_usable']) < 1e-9
    for fname,metric,asc in [('top10_long.csv','peak_pct',False),('top10_short.csv','trough_pct',True)]:
        assert read('movement/'+fname).symbol.tolist() == m.sort_values(metric,ascending=asc).head(10).symbol.tolist()
    assert m.frozen_1m_present_expected_bars.eq(360).all()
    assert m.current_5m_valid_bars.eq(72).all()
    assert m.frozen_1m_valid_bars.sum() == 76559
    assert m.frozen_1m_zero_volume_count.sum() == 121
    assert m.official_previous_close_matches.all() and m.cas_final_matches_official_close.all()
    assert m.frozen_current_1m_file_hash_match.all()

    slots = read('selection/all_stock_slot_audit.csv')
    gates = read('selection/all_indicator_checks.csv')
    assert len(slots) == 2982 and len(gates) == 62835
    assert not slots.duplicated(['symbol','setup_id']).any()
    assert not gates.duplicated(['symbol','setup_id','gate']).any()
    assert slots.groupby('symbol').size().eq(14).all()
    assert slots.price_oi_usable.all()
    assert not slots.selected.any() and not slots.filled.any()
    assert slots.five_minute_pass.sum() == 10
    assert set(slots.symbol) == set(gates.symbol) == set(m.symbol)
    assert len(read('qualifying_stock_slot_audit.csv')) == 118*14
    cf = read('selection/top20_slot_counterfactuals.csv')
    earliest = read('selection/top20_earliest_counterfactual_entries.csv')
    policies = read('selection/counterfactual_session_policy_comparison.csv')
    assert len(cf) == 144 and len(policies) == 20 and len(earliest) == 13
    assert earliest.symbol.is_unique and earliest.filled.all() and earliest.portfolio_executed.all()
    assert (pd.to_datetime(earliest.entry_ts) > pd.to_datetime(earliest.confirmation_time)).all()
    assert (pd.to_datetime(earliest.exit_ts) >= pd.to_datetime(earliest.entry_ts)).all()
    assert policies.policy_id.is_unique
    assert [int(policies.net_pnl.gt(0).sum()),int(policies.net_pnl.lt(0).sum()),int(policies.net_pnl.eq(0).sum())] == [6,13,1]

    history = read('history/comparison.csv')
    daily = read('history/daywise.csv')
    changes = read('history/added_removed_trades.csv')
    assert len(history) == 5 and history.sessions.eq(48).all()
    baseline = history.loc[history.variant.eq('frozen_g2')].iloc[0]
    assert baseline.trades == 95 and abs(baseline.net_pnl-238664.666174602) < 1e-6
    for row in history.itertuples():
        days = daily.loc[daily.variant.eq(row.variant)]
        assert len(days) == 48 and days.date.is_unique
        assert abs(days.net_pnl.sum()-row.net_pnl) < 1e-6
        assert days.trades.sum() == row.trades
        assert abs(row.net_pnl-baseline.net_pnl-row.net_delta_vs_baseline) < 1e-6
        ch = changes.loc[changes.variant.eq(row.variant)]
        added = ch.loc[ch.change.eq('ADDED'),'portfolio_net_profit_rupees'].sum()
        removed = ch.loc[ch.change.eq('REMOVED'),'portfolio_net_profit_rupees'].sum()
        assert abs(added-removed-row.net_delta_vs_baseline) < 1e-6

    manifest = obj('report_manifest.json')
    assert manifest['all_baselines_unchanged']
    for p in manifest['protected_baselines']:
        assert digest(Path(p['path'])) == p['before'] == p['after']
    for p in manifest['artifacts']:
        assert digest(root/p['path']) == p['sha256'], p['path']
    assert digest(root/'report.html') == manifest['report_sha256']
    local_links = 0
    pages = [root/'report.html',*sorted((root/'stocks').glob('*.html'))]
    assert len(pages) == 214
    for path in pages:
        doc = path.read_text(encoding='utf-8')
        assert '<meta charset="utf-8">' in doc
        assert '\ufffd' not in doc and '\u00e2\u20ac' not in doc
        parser = Links(); parser.feed(doc)
        assert parser.tables >= 3
        for href in parser.hrefs:
            url = urlsplit(href)
            if url.scheme or url.netloc:
                continue
            if not url.path:
                assert url.fragment in parser.ids, (path,href)
            else:
                assert (path.parent/unquote(url.path)).is_file(), (path,href)
            local_links += 1
    print(json.dumps(dict(status='PASS',dated_stocks=213,stock_pages=213,html_pages_checked=len(pages),
        local_links_checked=local_links,stock_setup_rows=len(slots),gate_rows=len(gates),
        historical_policies=5,historical_sessions=48,all_artifact_hashes_verified=True,
        protected_baselines_unchanged=True),indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output-root',type=Path,required=True)
    verify(p.parse_args().output_root)
