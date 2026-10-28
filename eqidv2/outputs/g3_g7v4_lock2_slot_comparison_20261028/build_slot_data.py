"""Read-only reconciliation and presentation of two sealed trade ledgers."""
from __future__ import annotations

import argparse
import hashlib
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g7_entry_quality_20261026 import verify_members
from research.g8_exit_engine import _flags


OUT = Path(__file__).resolve().parent
G3_ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
G7 = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/practical_research/runs/2026-10-27/20261027T185748273567_five_fixed_tests')
G7_SHA = 'daa7440e6c2e0a8d60a6bc89841da802d59639aa1c2f43f78a371622ca43f56b'
READ_COLUMNS = ['sid', 'day', 'setup_id', 'side', 'confirmation_ts', 'filled',
                'portfolio_executed', 'portfolio_gross_profit_rupees',
                'portfolio_cost_rupees', 'portfolio_net_profit_rupees']
FIELDS = ['selected', 'native_fills', 'trades', 'wins', 'losses', 'win_rate_pct',
          'gross_rs', 'cost_rs', 'net_rs', 'losing_amount_rs', 'profit_factor']


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def metrics(group: pd.DataFrame) -> dict:
    executed = group.loc[group.portfolio_executed]
    pnl = executed.portfolio_net_profit_rupees
    pos = float(pnl.loc[pnl.gt(1e-8)].sum())
    neg = float(-pnl.loc[pnl.lt(-1e-8)].sum())
    result = dict(selected=int(len(group)), native_fills=int(group.filled.sum()),
                  trades=int(len(executed)), wins=int(pnl.gt(1e-8).sum()),
                  losses=int(pnl.lt(-1e-8).sum()),
                  win_rate_pct=float(100 * pnl.gt(1e-8).sum() / len(executed)) if len(executed) else 0.,
                  gross_rs=float(executed.portfolio_gross_profit_rupees.sum()),
                  cost_rs=float(executed.portfolio_cost_rupees.sum()),
                  net_rs=float(pnl.sum()), losing_amount_rs=neg,
                  profit_factor=pos / neg if neg else None)
    assert result['trades'] == result['wins'] + result['losses']
    assert np.isclose(result['gross_rs'] - result['cost_rs'], result['net_rs'], atol=1e-6)
    return result


def checked_book(path: Path, days: list[str]) -> pd.DataFrame:
    book = pd.read_csv(path, usecols=READ_COLUMNS, low_memory=False)
    for name in ('filled', 'portfolio_executed'):
        book[name] = _flags(book[name], name)
    if book.sid.duplicated().any() or not set(book.day).issubset(days):
        raise ValueError('Invalid ledger identity/calendar')
    if (book.portfolio_executed & ~book.filled).any():
        raise ValueError('Executed order lacks native fill')
    if not book.setup_id.astype(str).str.fullmatch(r'\d{4}_(LONG|SHORT)').all():
        raise ValueError('Unknown setup clock/side')
    if not book.setup_id.str.rsplit('_', n=1).str[-1].eq(book.side).all():
        raise ValueError('Setup side mismatch')
    if not pd.to_datetime(book.confirmation_ts).dt.strftime('%H%M').eq(book.setup_id.str[:4]).all():
        raise ValueError('Setup clock differs from confirmation timestamp')
    book['slot'] = book.setup_id.str[:4].str[:2] + ':' + book.setup_id.str[:4].str[2:]
    money = book[['portfolio_gross_profit_rupees', 'portfolio_cost_rupees',
                  'portfolio_net_profit_rupees']].apply(pd.to_numeric, errors='raise')
    if not np.isfinite(money).all().all():
        raise ValueError('Nonfinite ledger P&L')
    if not np.allclose(money.iloc[:, 0] - money.iloc[:, 1], money.iloc[:, 2], atol=1e-6):
        raise ValueError('Trade P&L not reconciled')
    if not np.allclose(money.loc[~book.portfolio_executed], 0., atol=1e-8):
        raise ValueError('Rejected orders contribute P&L')
    return book


def comparison(g3: pd.DataFrame, lock: pd.DataFrame, key: str, keys: list[str]):
    left = g3.groupby(key, sort=False)
    right = lock.groupby(key, sort=False)
    rows = []
    for item in keys:
        a = metrics(left.get_group(item) if item in left.groups else g3.iloc[:0])
        b = metrics(right.get_group(item) if item in right.groups else lock.iloc[:0])
        row = {key: item, **{'g3_' + k: a[k] for k in FIELDS},
               **{'g7v4_lock2_' + k: b[k] for k in FIELDS},
               'net_difference_g3_minus_lock2_rs': a['net_rs'] - b['net_rs']}
        rows.append(row)
    for strategy, frame in (('g3', g3), ('g7v4_lock2', lock)):
        expected = metrics(frame)
        for field in ('selected', 'native_fills', 'trades', 'wins', 'losses', 'gross_rs',
                      'cost_rs', 'net_rs', 'losing_amount_rs'):
            actual = sum(r[strategy + '_' + field] for r in rows)
            if not np.isclose(actual, expected[field], atol=1e-6, rtol=0):
                raise ValueError(f'{strategy} {key} {field} does not sum to source ledger')
    return rows


def prepare():
    pointer = read_json(G3_ROOT / 'latest/current_backtest.json')
    g3_dir = G3_ROOT / pointer['run_dir']
    verify_members(g3_dir, pointer['manifest_sha256'])
    verify_members(G7, G7_SHA)
    g3_summary = read_json(g3_dir / 'summary.json')
    g7_summary = read_json(G7 / 'summary.json')
    config = read_json(g3_dir / 'config.json')
    days = g7_summary['periods']['all']
    if (g3_summary['days'] != days or len(days) != 59
            or g3_summary['strategy_version'] != 'V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V14'
            or len(config['active_setups']) != 51 or config['cost_bps'] != 5
            or config['portfolio_capital_rupees'] != 1_000_000
            or config['capital_per_entry_rupees'] != 100_000 or config['leverage_factor'] != 5
            or g7_summary['selected_candidate'] is not None
            or g7_summary['default_changed'] is not False):
        raise ValueError('Source identity/period drift')
    g3_path = g3_dir / 'portfolio_trades.csv'
    lock_path = G7 / 'loss_lock2/portfolio_trades.csv'
    g3 = checked_book(g3_path, days)
    lock = checked_book(lock_path, days)
    slots = sorted(set(g3.slot) | set(lock.slot))
    setups = sorted(set(g3.setup_id) | set(lock.setup_id))
    if len(slots) != 35 or len(setups) != 51:
        raise ValueError('Unexpected 35-slot/51-setup inventory')
    periods = {'overall': days, 'october': [d for d in days if d.startswith('2026-10')]}
    tables = {}
    totals = {}
    for period, calendar in periods.items():
        a, b = (frame.loc[frame.day.isin(calendar)] for frame in (g3, lock))
        tables[f'slot_time_{period}.csv'] = comparison(a, b, 'slot', slots)
        tables[f'setup_side_{period}.csv'] = comparison(a, b, 'setup_id', setups)
        totals[period] = {'G3': metrics(a), 'G7 V4 two-loss lock': metrics(b)}
    daywise = []
    for day in periods['october']:
        a, b = (frame.loc[frame.day.eq(day)] for frame in (g3, lock))
        dayrows = comparison(a, b, 'slot', slots)
        daywise.extend([{'day': day, **row} for row in dayrows])
    tables['slot_daywise_october.csv'] = daywise
    for period, expected in [('overall', g3_summary), ('october', read_json(OUT.parent / 'g7_v4_practical_review_20261028/g3_reference.json')['october'])]:
        wanted = expected['current_g3'] if period == 'overall' else expected
        if not np.isclose(totals[period]['G3']['net_rs'], wanted['net_pnl_rs'], atol=1e-6):
            raise ValueError('G3 published summary differs')
    study_all = pd.read_csv(G7 / 'comparison.csv')
    for period in periods:
        row = study_all.loc[study_all.policy.eq('loss_lock2') & study_all.period.eq('all' if period == 'overall' else period)]
        if len(row) != 1 or not np.isclose(totals[period]['G7 V4 two-loss lock']['net_rs'], row.net_pnl_rs.iloc[0], atol=1e-6):
            raise ValueError('G7 lock summary differs')
    provenance = dict(periods=periods, source_g3=str(g3_dir), source_g7v4_research=str(G7),
                      g3_manifest_sha256=pointer['manifest_sha256'], g7_manifest_sha256=G7_SHA,
                      g3_ledger_sha256=sha(g3_path), g7_lock_ledger_sha256=sha(lock_path),
                      fixed_capital_per_entry_rs=100000, leverage=5,
                      portfolio_capital_rs=1000000, modeled_roundtrip_cost_bps=5,
                      calendar_matched=True, slot_definition='Original setup confirmation clock; actual fill may occur later',
                      g3_execution='Published fractional-share sizing and original admission timing',
                      g7_lock_execution='Conservative entry-minute capacity, whole-share sizing, two realized-loss admission lock',
                      g7_lock_research_only=True, selected_g7_candidate=None, totals=totals)
    build = OUT / '.build'
    build.mkdir(parents=True, exist_ok=True)
    (build / 'tables.json').write_text(json.dumps(tables, indent=2, allow_nan=False), encoding='utf-8')
    (OUT / 'source_provenance.json').write_text(json.dumps(provenance, indent=2, allow_nan=False), encoding='utf-8')
    print('PREPARED', len(slots), 'slots', len(setups), 'setups', len(days), 'days')
    for period, pair in totals.items():
        print(period, {k: (v['trades'], v['net_rs']) for k, v in pair.items()})


def finalize():
    tables = read_json(OUT / '.build/tables.json')
    prov = read_json(OUT / 'source_provenance.json')
    for filename, rows in tables.items():
        path = OUT / filename
        frame = pd.read_csv(path)
        if len(frame) != len(rows) or list(frame) != list(rows[0]):
            raise ValueError('Artifact CSV shape differs')
        for field in frame:
            if field in ('slot', 'setup_id', 'day'):
                if frame[field].astype(str).tolist() != [str(row[field]) for row in rows]:
                    raise ValueError('Artifact key differs')
            else:
                observed = pd.to_numeric(frame[field], errors='coerce')
                expected = pd.Series([row[field] for row in rows], dtype=float)
                if not np.allclose(observed, expected, atol=1e-6, rtol=0, equal_nan=True):
                    raise ValueError('Artifact value differs: ' + filename + '/' + field)
        prov.setdefault('output_sha256', {})[filename] = sha(path)
    (OUT / 'source_provenance.json').write_text(json.dumps(prov, indent=2, allow_nan=False), encoding='utf-8')

    def table(name, cols):
        frame = pd.read_csv(OUT / name)[cols].copy()
        return '<div class="scroll">' + frame.to_html(index=False, float_format=lambda x: f'{x:,.2f}', border=0) + '</div>'

    cols = ['slot', 'g3_trades', 'g3_wins', 'g3_losses', 'g3_net_rs',
            'g7v4_lock2_trades', 'g7v4_lock2_wins', 'g7v4_lock2_losses',
            'g7v4_lock2_net_rs', 'net_difference_g3_minus_lock2_rs']
    setup_cols = ['setup_id', *cols[1:]]
    totals = prov['totals']
    head = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 and G7 V4 two-loss lock: slot results</title><style>body{max-width:1700px;margin:30px auto;padding:0 20px;color:#20344c;font:15px/1.5 system-ui}.scroll{overflow:auto}table{border-collapse:collapse;font-size:13px}th,td{padding:7px 10px;border-bottom:1px solid #ced9e3;text-align:right}th{background:#eaf2f8;position:sticky;top:0}a{color:#1a5d94}.box{background:#eef5fa;border-left:5px solid #3977a0;padding:15px}</style></head><body><h1>G3 and G7 V4 two-loss lock: slot results</h1>'''
    overview = '<p class="box">Same 59 available dates, 29 July–27 October 2026; 35 confirmation slots and 51 time-and-direction setups. Original 5 bps modeled round-trip cost. G3 is the published V14 backtest. The G7 V4 two-loss lock is an unselected research experiment.</p>'
    rows = []
    for period, pair in totals.items():
        for name, m in pair.items():
            rows.append(dict(period=period, strategy=name, trades=m['trades'], wins=m['wins'], losses=m['losses'], net_rs=m['net_rs'], losing_amount_rs=m['losing_amount_rs']))
    overview += '<h2>Totals</h2><div class="scroll">' + pd.DataFrame(rows).to_html(index=False, float_format=lambda x:f'{x:,.2f}', border=0) + '</div>'
    overview += '<p>Slot labels denote the completed confirmation minute in the setup ID, not the eventual fill minute. Each table includes all configured slots, including slots with zero executed trades in the period. Net differences subtract the G7 lock result from G3; these are descriptive comparisons between different entry and exit rules. G3 uses fractional sizing and its original admission convention; the G7 lock uses whole shares and reserves a slot at the entry minute open.</p>'
    body = ''
    for period in ('overall', 'october'):
        body += f'<h2>{html.escape(period.title())}: by confirmation time</h2>' + table(f'slot_time_{period}.csv', cols)
        body += f'<h2>{html.escape(period.title())}: time and direction</h2>' + table(f'setup_side_{period}.csv', setup_cols)
    body += '<p>The two-loss rule blocks new admissions after two realized losing exits. It changes which later opportunities get capital; these slot results are from the full portfolio replay and should not be read as independent standalone slot strategies. Prior research reused these dates, so the best-looking slot is not a validated new filter.</p>'
    links = '<h2>Download data</h2><ul>' + ''.join(f'<li><a href="{name}">{name}</a></li>' for name in [*tables, 'source_provenance.json']) + '</ul>'
    (OUT / 'report.html').write_text(head + overview + body + links + '</body></html>', encoding='utf-8')
    print('VERIFIED OUTPUTS', len(tables), 'CSV files and report')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=('prepare', 'finalize'))
    args = parser.parse_args()
    prepare() if args.phase == 'prepare' else finalize()
