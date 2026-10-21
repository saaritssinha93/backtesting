import csv
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

ROOT = Path(r'C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
TODAY = ROOT / 'production/runs/2026-10-15/20261015T170545010772_39928700/portfolio_trades.csv'
HISTORY = ROOT / 'backtests/runs/2026-10-15/20261015T171145325060_264a0ea2_current_full_history/portfolio_trades.csv'

def read(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        r['pnl'] = float(r['portfolio_net_profit_rupees'] or 0)
        r['executed'] = r['portfolio_executed'].lower() == 'true'
        r['decision'] = datetime.fromisoformat(r['confirmation_timestamp'] or r['confirmation_ts'])
        r['entry'] = datetime.fromisoformat(r['entry_ts']) if r['entry_ts'] else None
        r['exit'] = datetime.fromisoformat(r['exit_execution_ts'] or r['exit_ts']) if r['exit_ts'] else None
    return rows

def label(r):
    return f"{r['setup_id']} {r['tradingsymbol']} {r['side']}"

def day_case(rows, threshold, time_key='decision'):
    # Fixed-source ledger overlay: retain open positions, no re-ranking,
    # no reinstatement of source portfolio rejects. Latch stop for the day.
    accepted, skipped, audit, closed = [], [], [], set()
    realized = 0.
    latched = False
    trigger_at = None
    for r in sorted((r for r in rows if r['executed']), key=lambda x: (x[time_key], x['signal_id'])):
        # Group equal-timestamp closes; exit must be strictly earlier than decision.
        eligible = defaultdict(list)
        for a in accepted:
            if a['signal_id'] not in closed and a['exit'] < r[time_key]:
                eligible[a['exit']].append(a)
        for ts, exiting in sorted(eligible.items()):
            realized += sum(a['pnl'] for a in exiting)
            closed.update(a['signal_id'] for a in exiting)
            if threshold is not None and realized <= -threshold + 1e-7:
                if not latched:
                    trigger_at = ts.isoformat()
                latched = True
        (skipped if latched else accepted).append(r)
        audit.append({'trade': label(r), 'decision_time': r[time_key].isoformat(), 'realized_before': realized, 'action': 'SKIP' if latched else 'KEEP'})
    return {'net': sum(r['pnl'] for r in accepted), 'trades': len(accepted), 'wins': sum(r['pnl'] > 0 for r in accepted), 'losses': sum(r['pnl'] < 0 for r in accepted), 'stop_exits': sum('STOP' in r['exit_reason'] for r in accepted), 'trigger_at': trigger_at, 'kept': [label(r) for r in accepted], 'skipped': [label(r) for r in skipped], 'audit': audit}

def metrics(rows, threshold):
    by_day = defaultdict(list)
    for r in rows:
        by_day[r['day']].append(r)
    days = {d: day_case(rr, threshold) for d, rr in sorted(by_day.items())}
    total = {k: sum(d[k] for d in days.values()) for k in ('net', 'trades', 'wins', 'losses', 'stop_exits')}
    total['losing_days'] = sum(d['net'] < -1e-7 for d in days.values())
    total['win_rate_pct'] = total['wins'] / total['trades'] * 100
    total['worst_day'] = min((d['net'], date) for date, d in days.items())
    total['days_with_skips'] = [date for date,d in days.items() if d['skipped']]
    eq = peak = mdd = 0.
    for d in days.values():
        eq += d['net']
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    total['daily_close_max_drawdown'] = mdd
    return {'metrics': total, 'days': days}

today, history = read(TODAY), read(HISTORY)
result = {
    'sources': {'today': str(TODAY), 'history': str(HISTORY)},
    'policy': 'Latched new-entry rejection after realized net <= threshold. Read only exits strictly before confirmation decision. Keep existing positions; all costs retained; no backfill of original portfolio rejections.',
    'today_cases': {str(t): day_case(today, t) for t in (None, 6500, 13000)},
    'today_entry_time_cases': {str(t): day_case(today, t, 'entry') for t in (None, 6500, 13000)},
    'history_cases': {str(t): metrics(history, t) for t in (None, 6500, 13000)},
    'uniform_half_size': {'today_net': sum(r['pnl'] for r in today if r['executed']) / 2, 'history_net': sum(r['pnl'] for r in history if r['executed']) / 2, 'assumption': 'Linear proportional costs and exposures, unchanged fills, no share rounding or minimum brokerage effects.'},
    'today_trades': [{k: r[k] for k in ('setup_id','tradingsymbol','side','confirmation_timestamp','entry_ts','exit_ts','exit_reason','pnl','mfe_pct','mae_pct','target_pct','stop_pct')} for r in today if r['executed']],
}
out = Path(__file__).with_name('risk_review.json')
out.write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps({'today_cases': result['today_cases'], 'history_metrics': {k:v['metrics'] for k,v in result['history_cases'].items()}, 'uniform_half_size': result['uniform_half_size']}, indent=2))
