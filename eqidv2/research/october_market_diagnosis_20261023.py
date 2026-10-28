"""Read-only retrospective month/regime diagnosis; never changes strategy inputs."""
from pathlib import Path
import json
import hashlib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT/'outputs/october_regime_diagnosis_20261023/market'
G6 = ROOT/'outputs/g6_market_regime_20261022_noon_v2'
G8 = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_8')
FEATURES = G8/'exit_regime_research/2026-10-22/20261023T111837751869_g8_exit_regimes/features.csv'

def output(df, name):
    df.to_csv(OUT/name, index=False)

def monthly_means(f, cols, by='month'):
    records = []
    for month, p in f.groupby(by):
        for col in cols:
            x = pd.to_numeric(p[col], errors='coerce').dropna().astype(float)
            records.append(dict(month=month, parameter=col, n=len(x), mean=x.mean(), median=x.median(), q25=x.quantile(.25), q75=x.quantile(.75)))
    return pd.DataFrame(records)

def result(g):
    p = g.portfolio_net_profit_rupees
    return dict(trades=len(g), days=g.day.nunique(), wins=int(p.gt(0).sum()), losses=int(p.lt(0).sum()), win_rate_pct=p.gt(0).mean()*100,
        pnl_rs=p.sum(), expectancy_rs=p.mean(), avg_win_rs=p[p.gt(0)].mean(), avg_loss_rs=p[p.lt(0)].mean(), target_rate_pct=g.exit_reason.str.contains('TARGET').mean()*100,
        sl_rate_pct=g.exit_reason.str.contains('STOP').mean()*100)

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    paths = [G6/'market_slots.csv',G6/'trade_features.csv',FEATURES,G8/'inputs/baseline_v2/baseline.csv',G8/'inputs/india_vix_5minute_2026-07-27_to_2026-10-22.csv']
    proof = {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    m = pd.read_csv(paths[0])
    # Exclude partial Oct22 noon study. All sessions contribute the same 51 clock positions.
    m = m.loc[m.day.le('2026-10-21')].copy()
    m['month'] = m.day.str[:7]
    m['abs_session_return_pct'] = m.session_return_pct.abs()
    m['chop30_flag'] = m.efficiency30.lt(.25).where(m.efficiency30.notna())
    m['large_crosses_flag'] = m.vwap_crosses30.ge(3).where(m.vwap_crosses30.notna())
    m['high_rv_flag'] = m.rv_relative20.gt(1.25).where(m.rv_relative20.notna())
    cols=['rv_pct','range_pct','efficiency30','chop30_flag','large_crosses_flag','rv_relative20','high_rv_flag','abs_session_return_pct','breadth_open_pct','breadth_change15_pp','dispersion_pct','volume_relative20']
    output(monthly_means(m,cols), 'nifty_equal_clock_monthly.csv')
    output(monthly_means(m.loc[m.clock.eq('13:30')],cols), 'nifty_1330_monthly.csv')
    output(m.groupby('day',as_index=False)[cols].mean(), 'nifty_daily_equal_clock.csv')
    coverage=m.groupby('month').agg(days=('day','nunique'),clock_rows=('day','size'),index_valid=('index_complete','sum'),breadth_valid=('breadth_complete','sum'),rv20_valid=('rv_relative20','count'),breadth_change_valid=('breadth_change15_pp','count')).reset_index()
    output(coverage,'market_coverage.csv')
    complete_days=m.groupby('day').index_complete.sum()
    complete=m.loc[m.day.isin(complete_days[complete_days.eq(51)].index)]
    output(monthly_means(complete,cols),'nifty_complete_session_sensitivity.csv')
    v = pd.read_csv(paths[4])
    v['date'] = pd.to_datetime(v.date,utc=True).dt.tz_convert('Asia/Kolkata')
    v['available_at'] = v.date+pd.Timedelta(minutes=5)
    v['day']=v.date.dt.strftime('%Y-%m-%d')
    v['clock']=v.available_at.dt.strftime('%H:%M')
    v=v.loc[v.day.isin(m.day.unique()) & v.clock.between('09:20','13:30')].copy()
    v['month']=v.day.str[:7]
    v['vix_30_pct']=v.groupby('day').close.pct_change(6)*100
    v['vix_session_pct']=(v.close/v.groupby('day').open.transform('first')-1)*100
    v['vix_abs_30_pct']=v.vix_30_pct.abs()
    output(monthly_means(v,['close','vix_30_pct','vix_abs_30_pct','vix_session_pct']),'vix_equal_clock_monthly.csv')
    g6 = pd.read_csv(paths[1])
    g6=g6.loc[g6.portfolio_executed.eq(True)].copy()
    g6['month']=g6.day.str[:7]
    g6['period']=np.where(g6.month.eq('2026-10'),'October','Jul-Sep')
    sign=g6.side.map({'LONG':1,'SHORT':-1})
    g6['signed_nifty15']=sign*g6.return15_pct
    g6['signed_breadth15']=sign*g6.breadth_change15_pp
    for col in ['efficiency30','rv_relative20','vwap_crosses30','signed_nifty15','signed_breadth15']:
        edges={'efficiency30':[-np.inf,.25,.5,np.inf], 'rv_relative20':[-np.inf,.75,1.25,1.5,np.inf], 'vwap_crosses30':[-np.inf,1,2,np.inf], 'signed_nifty15':[-np.inf,-.1,0,.1,np.inf], 'signed_breadth15':[-np.inf,-10,0,10,np.inf]}[col]
        g6['bin']=pd.cut(g6[col],edges).astype(str)
        rows=[]
        for (period,bin_),g in g6.groupby(['period','bin']):rows.append(dict(parameter=col,period=period,bin=bin_,**result(g)))
        output(pd.DataFrame(rows),f'g6_{col}_bins.csv')
    f=pd.read_csv(paths[2])
    b=pd.read_csv(paths[3],low_memory=False)
    cols_b=['sid','day','portfolio_executed','portfolio_net_profit_rupees','exit_reason','entry_price','confirmation_close','stop_pct','target_pct','mfe_pct','mae_pct','holding_minutes','v9_5m_distance_vwap_pct','v9_1m_extension_atr','research_extension_atr']
    b=b[[c for c in cols_b if c in b]]
    f=f.merge(b.drop(columns='day'),on='sid',validate='one_to_one')
    f=f.loc[f.portfolio_executed.eq(True)].copy()
    f['month']=f.day.str[:7]
    f['period']=np.where(f.month.eq('2026-10'),'October','Jul-Sep')
    f['stock_atr_pct']=f.stock_prior14_atr/f.confirmation_close*100
    f['target_atr_ratio']=f.baseline_target_pct/f.stock_atr_pct
    f['stop_atr_ratio']=f.baseline_stop_pct/f.stock_atr_pct
    f['signed_stock_session_pct']=f.stock_session_return_pct*f.side_sign
    f['signed_stock5_pct']=f.stock5_return_pct*f.side_sign
    f['signed_vwap_distance_pct']=f.v9_5m_distance_vwap_pct*f.side_sign
    f['hostile_vix30']=((f.side_sign*f.vix_30_pct>=2)&(f.side_sign*f.vix_session_pct>=2)).astype(int)
    cols_f=['vix_level','vix_30_pct','vix_session_pct','vix_level_ratio','nifty_both_aligned','signed_nifty_slot','signed_nifty_session','confirmation_range_atr','confirmation_close_position','confirmation_body_direction','signal_move_abs_pct','volume_log','signal_body_ratio','stock_relative5_pct','stock_relative_session_pct','signed_stock5_pct','signed_stock_session_pct','stock_atr_pct','target_atr_ratio','stop_atr_ratio','signed_vwap_distance_pct','hostile_vix30','baseline_stop_pct','baseline_target_pct','mfe_pct','mae_pct','holding_minutes']
    output(monthly_means(f,cols_f),'g7v2_executed_feature_monthly.csv')
    bin_specs={'confirmation_range_atr':[-np.inf,1,2,3,np.inf], 'target_atr_ratio':[-np.inf,3,5,8,np.inf], 'stock_relative5_pct':[-np.inf,0,.25,.5,np.inf], 'signed_stock_session_pct':[-np.inf,0,1,2,np.inf], 'signed_vwap_distance_pct':[-np.inf,0,.5,1,np.inf], 'vix_level':[-np.inf,10,12,15,np.inf], 'signed_nifty_slot':[-np.inf,0,.05,.1,np.inf], 'nifty_both_aligned':[-np.inf,0,1], 'hostile_vix30':[-np.inf,0,1]}
    bins=[]
    for col,edges in bin_specs.items():
        f['bin']=pd.cut(f[col],edges).astype(str)
        for (period,bin_),g in f.groupby(['period','bin']): bins.append(dict(parameter=col,period=period,bin=bin_,**result(g)))
    output(pd.DataFrame(bins),'g7v2_executed_feature_bins.csv')
    output(pd.DataFrame([dict(month=k,**result(p)) for k,p in f.groupby('month')]),'g7v2_monthly_result.csv')
    output(pd.DataFrame([dict(month=k,side=side,**result(p)) for (k,side),p in f.groupby(['month','side'])]),'g7v2_monthly_side_result.csv')
    sidefeatures=[]
    for side,part in f.groupby('side'):
        tmp=monthly_means(part,cols_f)
        tmp['side']=side
        sidefeatures.append(tmp)
    output(pd.concat(sidefeatures),'g7v2_side_feature_monthly.csv')
    sidebins=[]
    for feature,edges in {'stock_atr_pct':[0,.1,.15,.2,np.inf], 'vix_level':[0,12,14,16,np.inf], 'nifty_both_aligned':[-np.inf,0,1]}.items():
        f['bin']=pd.cut(f[feature],edges).astype(str)
        for (month,side,bin_),part in f.groupby(['month','side','bin']):
            sidebins.append(dict(month=month,side=side,parameter=feature,bin=bin_,**result(part)))
    output(pd.DataFrame(sidebins),'g7v2_side_feature_bins_monthly.csv')
    output(f.drop(columns='bin'),'g7v2_executed_enriched.csv')
    # Fixed previously studied causal vetoes: report observed avoided losers AND foregone winners.
    rows=[]
    for period,g in g6.groupby('period'):
        for col in [c for c in g if c.startswith('veto_')]:
            p=g.loc[g[col].eq(True)]
            pnl=p.portfolio_net_profit_rupees
            rows.append(dict(period=period,rule=col,trades=len(p),losers_avoided=int(pnl.lt(0).sum()),winners_excluded=int(pnl.gt(0).sum()),loss_value_avoided=float(-pnl[pnl.lt(0)].sum()),winner_value_excluded=float(pnl[pnl.gt(0)].sum()),net_delta_without_reallocation=float(-pnl.sum())))
    output(pd.DataFrame(rows),'g6_existing_veto_month_split.csv')
    # Full saved policy books already include portfolio reallocation.  Do not
    # mistake the static removed-trade ablation above for a portfolio replay.
    books={p.parent.name:pd.read_csv(p) for p in (G6/'policies').glob('*/trades.csv')}
    full=[]
    reconciliation=[]
    for period,mask in [('Jul-Sep',lambda x:x.day.lt('2026-10-01')),('October',lambda x:x.day.ge('2026-10-01'))]:
        baseline=books['baseline']
        baseline=baseline.loc[mask(baseline)&baseline.portfolio_executed.eq(True)].set_index('sid')
        baseline_pnl=baseline.portfolio_net_profit_rupees.sum()
        days=sorted(books['baseline'].loc[mask(books['baseline']),'day'].unique())
        baseline_daily=baseline.groupby('day').portfolio_net_profit_rupees.sum().reindex(days,fill_value=0)
        for policy,book in books.items():
            selected=book.loc[mask(book)&book.portfolio_executed.eq(True)].set_index('sid')
            removed=baseline.loc[~baseline.index.isin(selected.index)]
            added=selected.loc[~selected.index.isin(baseline.index)]
            common=baseline.index.intersection(selected.index)
            common_delta=(selected.loc[common,'portfolio_net_profit_rupees']-baseline.loc[common,'portfolio_net_profit_rupees']).sum()
            delta=selected.portfolio_net_profit_rupees.sum()-baseline_pnl
            saved_losses=-removed.loc[removed.portfolio_net_profit_rupees.lt(0),'portfolio_net_profit_rupees'].sum()
            lost_winners=removed.loc[removed.portfolio_net_profit_rupees.gt(0),'portfolio_net_profit_rupees'].sum()
            added_pnl=added.portfolio_net_profit_rupees.sum()
            identity=saved_losses-lost_winners+added_pnl+common_delta
            if not np.isclose(delta,identity,rtol=0,atol=1e-6):
                raise ValueError('Saved-book PnL delta failed reconciliation')
            daily=selected.groupby('day').portfolio_net_profit_rupees.sum().reindex(days,fill_value=0)
            full.append(dict(period=period,policy=policy,sessions=len(days),**result(selected),delta_net_rs=delta,
                delta_trades=len(selected)-len(baseline),delta_win_rate_pp=selected.portfolio_net_profit_rupees.gt(0).mean()*100-baseline.portfolio_net_profit_rupees.gt(0).mean()*100,
                losing_days=int(daily.lt(0).sum()),delta_losing_days=int(daily.lt(0).sum()-baseline_daily.lt(0).sum()),
                losers_avoided=int(removed.portfolio_net_profit_rupees.lt(0).sum()),winners_excluded=int(removed.portfolio_net_profit_rupees.gt(0).sum()),
                loss_value_avoided_rs=saved_losses,winner_value_excluded_rs=lost_winners,newly_allocated_trades=len(added),newly_allocated_pnl_rs=added_pnl,
                common_trade_pnl_delta_rs=common_delta,reconciled_delta_rs=identity,reconciliation_error_rs=delta-identity))
            for kind,part in [('removed',removed),('newly_allocated',added)]:
                for sid,row in part.iterrows():reconciliation.append(dict(period=period,policy=policy,change=kind,sid=sid,day=row.day,setup_id=row.setup_id,tradingsymbol=row.tradingsymbol,pnl_rs=row.portfolio_net_profit_rupees))
    output(pd.DataFrame(full),'g6_full_policy_month_split.csv')
    output(pd.DataFrame(reconciliation),'g6_full_policy_changed_trades.csv')
    proof.update({str(G6/'policies'/name/'trades.csv'):hashlib.sha256((G6/'policies'/name/'trades.csv').read_bytes()).hexdigest() for name in books})
    meta=dict(provenance=proof,market_days=sorted(m.day.unique()),trade_days=sorted(f.day.unique()),incomplete_nifty_session_rows=complete_days[complete_days.lt(51)].to_dict(),caveats=['Historical periods repeatedly used in tuning: retrospective, not untouched validation.','Market summary uses fixed 09:20-13:30 clock range and excludes partial Oct22; trade features through Oct22.','Nifty/stock historical arrival latency cannot be established for every repaired archive.','VIX 30 minute references fall back to session at early slots as specified by existing study.','ATR uses 14 completed same-session one-minute bars before confirmation; 49 of 571 orders lack early history, never future filled.','Feature bins descriptive associations, not causal attribution; fixed-rule avoided-PnL excludes new allocations.','MFE/MAE are retrospective path outcomes, not allowed as entry features.','Breadth archived coverage incomplete and changes across months.','Inherited v9_5m_distance_vwap_pct has no October values and an implausible July aggregate: its descriptive export is NOT usable for a cross-month inference or entry rule without rebuilding.'])
    (OUT/'manifest.json').write_text(json.dumps(meta,indent=2))
    print(coverage.to_string(index=False))
    print(pd.read_csv(OUT/'g7v2_monthly_result.csv').to_string(index=False))
    print('OUTPUT',OUT)

if __name__=='__main__':main()
