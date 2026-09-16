from dataclasses import replace
from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fno_v5_hybrid_backtest as replay
import fno_v13_corrected_v2_backtest as v2
import fno_v13_corrected_v3_backtest as v3
import fno_v13_v3_scaleout_sweep as sx
import fno_v13_v3_exit_sweep as ux


def load():
    eligibility, calendar, _, regimes, _ = v3._load_eligibility(False, 0.99)
    eligibility = eligibility.loc[eligibility.eligible]
    by_month = {}
    for r in eligibility.to_dict('records'):
        if r['required_contract'] in regimes:
            by_month.setdefault(r['required_contract'], []).append(r['day'])
    parts = []
    for month in sorted(by_month, key=lambda m: calendar[m]):
        sig, paths, _ = v3._load_or_build_regime(month, regimes[month], by_month[month], square_off='1530', max_forward_bars=400, rebuild=False)
        parts.append((sig, paths))
    signals, paths = v3.v6.concat_regimes(parts)
    ctx = v3.load_nifty_first_bar_context(signals.contract_month.unique())
    ann = v3.annotate_nifty_gate(signals, ctx)
    ann = ann.loc[ann.nifty_first_bar_gate_pass]
    ann = v2.apply_policy(ann, v2.POLICIES[v3.BASE_POLICY_NAME])
    return ann, paths, sorted(set(signals.day))


def setup(slot, side, *, price=.2, oi=.1, volume=1., body=.4, wick=.5, stop=1., target=3.):
    base = v2._modal_long_setup(slot)
    return replace(base, side=side, price_change_pct=price, oi_change_pct=oi,
                   volume_ratio=volume, body_ratio=body, max_wick_ratio=wick,
                   stop_pct=stop, target_pct=target, source_version='PROBE')


def stats(audit, days):
    return sx.metrics(audit, days)


signals, paths, days = load()
base_orders = ux.selected_orders(signals, v3.load_nifty_first_bar_context(signals.contract_month.unique())) if False else None
parts=[]
for s in v3.active_setups():
    z=replay.select_setup_rows(signals,s).copy()
    if len(z):
        z['setup_id']=s.setup_id; z['native_stop_pct']=s.stop_pct; z['native_target_pct']=s.target_pct; parts.append(z)
base_orders=pd.concat(parts,ignore_index=True)
base=sx.simulate_scaleout(base_orders,paths,initial_stop_pct=1.5,t1_pct=1.05,partial_pct=.2,runner_target_pct=2.6,runner_stop='BREAKEVEN',cost_bps=5)
rows=[]
train=[d for d in days if d <= date(2026,8,13)]
val=[d for d in days if date(2026,8,14) <= d <= date(2026,8,21)]
test=[d for d in days if d >= date(2026,8,26)]
for hh in range(9,12):
    for mm in range(0,60,5):
        n=hh*100+mm
        if n < 950 or n > 1130 or n in {955,1000}: continue
        slot=f'{hh:02d}:{mm:02d}'
        for side in ('LONG','SHORT'):
            s=setup(slot,side)
            extra=replay.select_setup_rows(signals,s).copy()
            if extra.empty: continue
            extra['setup_id']=s.setup_id;extra['native_stop_pct']=s.stop_pct;extra['native_target_pct']=s.target_pct
            standalone=sx.simulate_scaleout(extra,paths,initial_stop_pct=1.5,t1_pct=1.05,partial_pct=.2,runner_target_pct=2.6,runner_stop='BREAKEVEN',cost_bps=5)
            combined=pd.concat([base_orders,extra],ignore_index=True)
            ca=sx.simulate_scaleout(combined,paths,initial_stop_pct=1.5,t1_pct=1.05,partial_pct=.2,runner_target_pct=2.6,runner_stop='BREAKEVEN',cost_bps=5)
            r={'slot':slot,'side':side,**{f'extra_{k}':v for k,v in stats(standalone,days).items()},**{f'combined_{k}':v for k,v in stats(ca,days).items()}}
            for name,pdays in [('train',train),('val',val),('test',test)]:
                m=sx.period_metrics(standalone,pdays,name)
                r.update(m)
            rows.append(r)
out=pd.DataFrame(rows).sort_values(['test_pf','val_pf','train_pf'],ascending=False)
print('BASE',stats(base,days))
print(out[['slot','side','extra_fills','extra_win_rate_pct','extra_t1_hit_rate_pct','extra_profit_factor','extra_net_pct','train_fills','train_pf','train_net_pct','val_fills','val_pf','val_net_pct','test_fills','test_pf','test_net_pct','combined_fills','combined_profit_factor','combined_net_pct']].to_string(index=False))
