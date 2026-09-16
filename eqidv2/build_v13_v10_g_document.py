"""Assemble a verified, self-contained G documentation snapshot; never execute trading."""
from __future__ import annotations
import hashlib
import json
import math
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import fno_v13_v10_g_backtest as g

ROOT = Path(__file__).resolve().parent
SITE = ROOT / 'docs/v13_v10_g_web'
CAPITAL = Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g/run_20260914_capital_3l_portfolio_15l')
PROJECTION = CAPITAL / 'one_year_scenarios'
RETAINED = CAPITAL.parent / 'run_20260914_opportunity_expansion'

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def js_safe(value):
    if isinstance(value, dict): return {str(k): js_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [js_safe(v) for v in value]
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.bool_,)): return bool(value)
    if isinstance(value, (float, np.floating)):
        return None if math.isnan(value) else ('Infinity' if value > 0 else '-Infinity') if math.isinf(value) else float(value)
    return value

def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))

def read_csv(path):
    return js_safe(pd.read_csv(path, float_precision='round_trip').to_dict('records'))

def flatten(data, group, prefix=''):
    rows=[]
    for key, value in data.items():
        name=f'{prefix}.{key}' if prefix else key
        if isinstance(value, dict): rows.extend(flatten(value, group, name))
        else: rows.append(dict(group=group, parameter=name, value=value, status='Saved value'))
    return rows

def main():
    provenance=[]
    for label, folder, manifest_name in [('Retained G', RETAINED, 'research_manifest.json'),
                                          ('Capital replay', CAPITAL, 'manifest.json'),
                                          ('Projection', PROJECTION, 'manifest.json')]:
        manifest=read_json(folder/manifest_name)
        for name, expected in manifest['artifacts'].items():
            path=folder/name.replace('\\','/')
            if not path.is_file() or sha(path)!=expected: raise RuntimeError(f'Artifact drift: {path}')
            provenance.append(dict(group=label,file=name.replace('\\','/'),sha256=expected,status='SHA256 verified'))
    source_files=['fno_v13_v10_g_backtest.py','fno_v13_v10_f_backtest.py','fno_v13_v9_backtest.py',
        'fno_v13_corrected_v5_backtest.py','fno_v13_v6_portfolio_backtest.py','fno_v13_v10_b_backtest.py',
        'fno_v13_v10_g_3l_capital_replay.py','fno_v13_v10_g_projection_chart.py','fno_v13_v10_g_live_config.py',
        'fno_v13_v10_g_daily_replay.py','backtesting_result_v13_v10_g_daily.py','build_v13_v10_g_document.py']
    for file in source_files:
        provenance.append(dict(group='Current source snapshot',file=file,sha256=sha(ROOT/file),status='Current hash recorded'))
    if sha(ROOT/'fno_v13_v10_g_projection_chart.py')!=read_json(PROJECTION/'manifest.json')['code_sha256']:
        raise RuntimeError('Projection source differs from saved projection manifest')
    settings=read_json(RETAINED/'frozen_config.json')
    v9config=read_json(g.DEFAULT_SOURCE/'frozen_config.json')
    configurations={'Retained G':settings,'Underlying V9':v9config,'Capital replay':read_json(CAPITAL/'configuration.json'),
                    'Projection':read_json(PROJECTION/'projection_methodology.json')}
    change=g.SelectionChange(**settings['selection_change'])
    setups=[]
    for original in g.v9.v5.profile_setups(g.v9.v5.PROFILES['higher_frequency']):
        core, expanded=g.setup_pair(original,change)
        item=asdict(expanded)
        item['setup_id']=expanded.setup_id
        item['f_price_change_pct']=core.price_change_pct
        item['f_oi_change_pct']=core.oi_change_pct
        item['exit']=settings['exit']['setups'].get(expanded.setup_id,settings['exit']['default'])
        item['reward_risk']=item['exit']['target_pct']/item['exit']['stop_pct']
        item['native_profile_exit_note']='Native stop/target fields in setup profile are overridden by the frozen B exit table shown here.'
        setups.append(item)
    daily=read_csv(PROJECTION/'historical_daily_returns.csv')
    trades=read_csv(CAPITAL/'result/portfolio_trades.csv')
    assert len(daily)==31 and len(trades)==73 and sum(t['portfolio_executed'] is True for t in trades)==66
    assert abs(sum(d['net_profit_rupees'] for d in daily)-536625.556950759)<1e-6
    assert not settings.get('morning_slots',False) and not settings.get('two_bar_continuation',False)
    summary=read_json(CAPITAL/'result/summary.json')
    notes={key:read_json(ROOT/f'.codex_tmp/v13g_doc_{key}.json') for key in ['strategy','results','architecture']}
    parameters=[]
    for group, cfg in configurations.items(): parameters.extend(flatten(cfg,group))
    for item in notes['strategy']['global_parameters']:
        parameters.append(dict(group='Verified rule catalog',parameter=item['name'],value=item.get('value'),
            status=('Active' if item.get('active') else 'Inactive / context')+'; '+item.get('unit','')+'; '+item.get('note','')))
    for setup in setups:
        for field in ['signal_end','confirmation_end','price_change_pct','oi_change_pct','volume_ratio','body_ratio','max_wick_ratio','max_entries','picker']:
            if field in setup: parameters.append(dict(group='Resolved setup',parameter=f"{setup['setup_id']}.{field}",value=setup[field],status='Active setup rule'))
    for name in ['morning_slots','two_bar_continuation']:
        parameters.append(dict(group='Retained G',parameter=name,value=False,status='Absent flag defaults to disabled'))
    field_rows=[]
    for name in trades[0]:
        group='Signal / selection'
        if name.startswith('v9_5m'): group='Five-minute feature'
        elif name.startswith('v9_1m'): group='One-minute feature'
        elif name.startswith('portfolio_'): group='Portfolio / capital accounting'
        elif name.startswith('v10_g_two_bar') or name=='v10_g_morning_slot': group='Optional / inactive extension evidence'
        elif name.startswith(('entry_','exit_')) or name in ('mfe_pct','mae_pct','filled','holding_minutes','same_bar_ambiguous'): group='Execution path'
        elif 'return' in name or 'profit' in name or 'cost' in name: group='Return / rupee accounting'
        values=[t[name] for t in trades if t[name] is not None]
        field_rows.append(dict(field=name,group=group,example=values[0] if values else None,
                              observed_non_null=len(values),total_rows=len(trades)))
    data=dict(title='V13–V10–G interactive backtest reference',generated_at=datetime.now().astimezone().isoformat(),
        initial=1500000.,projection_opening=float(daily[-1]['closing_equity']),summary=summary,
        daily=daily,trades=trades,configurations=configurations,setups=setups,parameters=parameters,
        historical_months=read_csv(PROJECTION/'historical_monthly_results.csv'),
        setup_results=read_csv(CAPITAL/'setup_summary.csv'),exit_results=read_csv(CAPITAL/'exit_reason_summary.csv'),
        annual=read_csv(PROJECTION/'sizing_comparison_summary.csv'),
        curves=read_csv(PROJECTION/'sizing_comparison_daily_curves.csv'),
        monthly=read_csv(PROJECTION/'monthly_scenario_projections.csv'),notes=notes,
        provenance=provenance,fields=field_rows,source_columns=list(trades[0]),
        limitations=configurations['Projection']['limitations'])
    SITE.mkdir(parents=True,exist_ok=True)
    (SITE/'data.json').write_text(json.dumps(js_safe(data),ensure_ascii=False,separators=(',',':'),allow_nan=False),encoding='utf-8')
    print(f'Embedded data: {len(trades)} orders, {len(daily)} dates, {len(parameters)} parameters, {len(provenance)} provenance rows.')

if __name__=='__main__': main()
