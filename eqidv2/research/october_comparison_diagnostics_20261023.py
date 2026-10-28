"""Read-only October diagnosis: same-version books, stratification and paths.

Writes analytical outputs only. Post-entry excursions/returns are explanatory
outcomes and must never be used as entry-time predictors.
"""
from pathlib import Path
import hashlib
import html
import json

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs/october_regime_diagnosis_20261023'
G8 = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_8')
RUN = G8 / 'daily_comparison/2026-10-23/20261023T125505562512_g8_baseline_vs_shock'
SEED = 20261023


def load_books():
    books = [pd.read_csv(OUT / 'normalized_executed_books_g3_g7.csv', low_memory=False)]
    for folder, model in [('baseline', 'G8'), ('shock', 'G8shock')]:
        b = pd.read_csv(RUN / folder / 'combined_portfolio_trades.csv', low_memory=False)
        assert b.portfolio_executed.isin([True, False]).all()
        b = b.loc[b.portfolio_executed].copy()
        b['model'] = model
        books.append(b)
    x = pd.concat(books, ignore_index=True).copy()
    x['month'] = x.day.str[:7]
    x['win'] = x.portfolio_net_profit_rupees.gt(1e-8)
    x['loss'] = x.portfolio_net_profit_rupees.lt(-1e-8)
    assert not x.duplicated(['model', 'day', 'setup_id', 'tradingsymbol', 'side']).any()
    assert (x.groupby('model').portfolio_net_profit_rupees.count() == x.groupby('model').size()).all()
    return x


def summaries(x, days):
    rows = []
    for (model, month), b in x.groupby(['model', 'month']):
        p = b.portfolio_net_profit_rupees
        gain, loss = p[p.gt(0)].sum(), -p[p.lt(0)].sum()
        avg_win, avg_loss = p[p.gt(0)].mean(), -p[p.lt(0)].mean()
        calendar = [d for d in days if d.startswith(month)]
        daily = b.groupby('day').portfolio_net_profit_rupees.sum().reindex(calendar, fill_value=0)
        rows.append(dict(model=model, month=month, sessions=len(calendar), trades=len(b),
            wins=int(b.win.sum()), losses=int(b.loss.sum()), win_rate_pct=100*b.win.mean(),
            mean_daily_win_rate_pct=100*b.groupby('day').win.mean().mean(),
            net_pnl_rs=p.sum(), net_per_session_rs=p.sum()/len(calendar),
            net_return_on_10lakh_capital_pct=p.sum()/1e6*100,
            profit_factor=gain/loss if loss else np.nan,
            average_win_rs=avg_win, average_loss_abs_rs=avg_loss,
            break_even_win_rate_pct=100*avg_loss/(avg_win+avg_loss),
            stop_loss_pct=100*(b.exit_reason.str.contains('STOP') & b.loss).mean(),
            full_target_exit_pct=100*(b.exit_reason.eq('TARGET') | b.exit_reason.str.startswith('T1_THEN_FULL_TARGET')).mean(),
            two_stage_first_target_hit_pct=100*b.exit_reason.str.startswith('T1_THEN_').mean() if model=='G7V3' else np.nan,
            losing_days=int(daily.lt(0).sum()), worst_day_rs=daily.min()))
    return pd.DataFrame(rows)


def decompose(x):
    details, decompositions = [], []
    for model, b in x.groupby('model'):
        old = b.loc[b.month.isin(['2026-08', '2026-09'])]
        new = b.loc[b.month.eq('2026-10')]
        for dimension in ['side', 'setup_id']:
            a = old.groupby(dimension).agg(n=('win', 'size'), wr=('win', 'mean'))
            z = new.groupby(dimension).agg(n=('win', 'size'), wr=('win', 'mean'))
            table = a.join(z, lsuffix='_old', rsuffix='_oct', how='outer')
            table['n_old'] = table.n_old.fillna(0)
            table['n_oct'] = table.n_oct.fillna(0)
            missing_prior = int(table.loc[table.n_old.eq(0), 'n_oct'].sum())
            # Disclose any unsupported prior group; no silent zero probability.
            table['wr_old'] = table.wr_old.fillna(old.win.mean())
            table['wr_oct'] = table.wr_oct.fillna(table.wr_old)
            table['w_old'] = table.n_old/len(old)
            table['w_oct'] = table.n_oct/len(new)
            table['mix_pp'] = 100*(table.w_oct-table.w_old)*table.wr_old
            table['within_pp'] = 100*table.w_oct*(table.wr_oct-table.wr_old)
            gap = 100*(new.win.mean()-old.win.mean())
            assert np.isclose(table.mix_pp.sum()+table.within_pp.sum(), gap)
            decompositions.append(dict(model=model, dimension=dimension,
                prior_trades=len(old), october_trades=len(new),
                prior_win_rate_pct=100*old.win.mean(), october_win_rate_pct=100*new.win.mean(),
                change_pp=gap, composition_effect_pp=table.mix_pp.sum(),
                within_group_effect_pp=table.within_pp.sum(),
                october_trades_without_prior_group=missing_prior))
            table = table.reset_index().rename(columns={dimension:'group'})
            table.insert(0, 'dimension', dimension)
            table.insert(0, 'model', model)
            details.append(table)
    return pd.DataFrame(decompositions), pd.concat(details, ignore_index=True)


def bootstrap(x, days):
    rng = np.random.default_rng(SEED)
    old_days = [d for d in days if d[:7] in ['2026-08','2026-09']]
    new_days = [d for d in days if d[:7]=='2026-10']
    old_sample = rng.integers(len(old_days), size=(20000,len(old_days)))
    new_sample = rng.integers(len(new_days), size=(20000,len(new_days)))
    out=[]
    for model,b in x.groupby('model'):
        daily = b.groupby('day').agg(wins=('win','sum'),n=('win','size'))
        a=daily.reindex(old_days,fill_value=0).to_numpy()
        z=daily.reindex(new_days,fill_value=0).to_numpy()
        av=a[old_sample].sum(axis=1); zv=z[new_sample].sum(axis=1)
        difference=100*(zv[:,0]/zv[:,1]-av[:,0]/av[:,1])
        out.append(dict(model=model, difference_pp=100*(z[:,0].sum()/z[:,1].sum()-a[:,0].sum()/a[:,1].sum()),
            day_bootstrap_p025_pp=np.quantile(difference,.025),day_bootstrap_p975_pp=np.quantile(difference,.975),
            draws=20000,seed=SEED,independent_validation=False))
    return pd.DataFrame(out)


def overlap(x):
    rows=[]
    oct_=x[x.month.eq('2026-10')]
    key=['day','setup_id','tradingsymbol','side']
    baseline=oct_[oct_.model.eq('G4')].set_index(key)
    for name,b in oct_.groupby('model'):
        b=b.set_index(key)
        shared=b.index.intersection(baseline.index)
        rows.append(dict(model=name,october_trades=len(b),shared_G4_trade_identities=len(shared),
            pct_shared_with_G4=100*len(shared)/len(b),
            shared_outcome_sign_agreement_pct=100*(b.loc[shared,'win']==baseline.loc[shared,'win']).mean()))
    return pd.DataFrame(rows)


def path_diagnostics(x):
    b=x.loc[x.model.eq('G8') & x.day.le('2026-10-22')].copy()
    cache=G8/'inputs/baseline_v2'
    manifest=json.loads((cache/'manifest.json').read_text())
    raw_path=cache/'minute_paths.npz'
    assert hashlib.sha256(raw_path.read_bytes()).hexdigest()==manifest['files']['minute_paths.npz']
    records=[]
    with np.load(raw_path,allow_pickle=False) as paths:
        for r in b.itertuples():
            sid=int(r.sid); index=int(r.entry_path_index); sign=1 if r.side=='LONG' else -1
            stamps=paths[f'{sid}_timestamp_ns']; close=paths[f'{sid}_close']
            assert stamps[index]==pd.Timestamp(r.entry_ts).value
            row=dict(day=r.day,month=r.month,side=r.side,setup_id=r.setup_id,
                     tradingsymbol=r.tradingsymbol,net_win=bool(r.win))
            for minutes in [1,5,15]:
                j=index+minutes
                assert j < len(stamps) and stamps[j]-stamps[index]==minutes*60_000_000_000
                row[f'return_after_{minutes}m_pct']=sign*(close[j]/r.entry_price-1)*100
            records.append(row)
    f=pd.DataFrame(records)
    rows=[]
    for (month,side),part in f.groupby(['month','side']):
        v=dict(month=month,side=side,trades=len(part),
               positive5_then_negative15_pct=100*((part.return_after_5m_pct>0)&(part.return_after_15m_pct<0)).mean())
        for minutes in [1,5,15]:
            val=part[f'return_after_{minutes}m_pct']
            v[f'negative_after_{minutes}m_pct']=100*val.lt(0).mean()
            v[f'mean_return_after_{minutes}m_pct']=val.mean()
        rows.append(v)
    return f,pd.DataFrame(rows)


def main():
    OUT.mkdir(exist_ok=True,parents=True)
    x=load_books()
    days=pd.read_csv(OUT/'common_calendar_g3_g7.csv').day.tolist()
    summary=summaries(x,days)
    decomposition,detail=decompose(x)
    frame,paths=path_diagnostics(x)
    outputs={'monthly_all_models.csv':summary,'win_rate_decomposition.csv':decomposition,
        'slot_side_contributions.csv':detail,'win_rate_uncertainty.csv':bootstrap(x,days),
        'october_shared_entries.csv':overlap(x),'postentry_path_observations.csv':frame,
        'postentry_path_monthly.csv':paths}
    for name,value in outputs.items():value.to_csv(OUT/name,index=False)
    write_report(summary, decomposition, detail, outputs['win_rate_uncertainty.csv'])
    print(summary.loc[summary.month.ne('2026-07'),['model','month','trades','win_rate_pct','net_pnl_rs','losing_days']].round(2).to_string(index=False))
    print(decomposition.loc[decomposition.model.isin(['G4','G8'])].round(3).to_string(index=False))
    print(paths.round(3).to_string(index=False))


def write_report(summary, decomposition, detail, intervals):
    def table(frame):
        return '<div class="table">'+frame.to_html(index=False,escape=True,
            float_format=lambda v:f'{v:,.2f}')+'</div>'
    rates=summary.pivot(index='model',columns='month',values='win_rate_pct').reset_index()
    oct_=summary.loc[summary.month.eq('2026-10'),['model','trades','wins','net_pnl_rs','losing_days']]
    headline=rates.merge(oct_,on='model').drop(columns='2026-07').rename(columns={
        'model':'Strategy','2026-08':'August win %','2026-09':'September win %',
        '2026-10':'October win %','trades':'October trades','wins':'October wins',
        'net_pnl_rs':'October net INR','losing_days':'October losing days'})
    worst=detail.loc[detail.model.eq('G8')&detail.dimension.eq('setup_id')].sort_values('within_pp').head(8)
    worst=worst[['group','n_old','wr_old','n_oct','wr_oct','within_pp']].copy()
    worst[['wr_old','wr_oct']]*=100
    params=[
        ('Volatility relative to the clock','RV(t) / median(RV at same clock over 20 strictly previous sessions)',
         'Cumulative square root of summed squared 5-minute log returns, percent; match available timestamps.',
         'October RV is higher. A blanket >1.5 veto reduced October G6 profit.'),
        ('Stock noise and stop cushion','ATR14_prior / confirmation_price * 100; stop_pct / ATR_pct',
         'Use completed same-session 1-minute bars before confirmation. Missing warmup stays missing.',
         'Short-side ATR rose 25% from September to October. Losses also worsened within ATR bands.'),
        ('Trend efficiency','abs(P_t - P_t-30m) / sum(abs(completed 5m price changes))',
         'Only completed bars at/before decision time; zero denominator is unavailable.',
         'Average efficiency is almost unchanged. A blanket <0.25 veto was unprofitable.'),
        ('Direction and relative strength','side_sign * Nifty_return_5m; side_sign * session_return; side_sign * (stock_return_5m - Nifty_return_5m)',
         'Identical completed endpoints and the dated Nifty futures contract.',
         'Nifty alignment is stronger in October; direction alone did not prevent the decline.'),
        ('VIX level and change','VIX / median(prior 20 session closes); 100*(VIX_t/VIX_t-30m - 1)',
         '5-minute candle close is available at candle start plus 5 minutes; reference uses up to 20 prior session closes with minimum 10 readiness; early fallback must be explicit.',
         'Current adopted G8 is a 30-minute lookback on 5-minute candles, not a 5-minute lookback.'),
        ('Conditional exit expectancy','p_target*mean_target_return + p_stop*mean_stop_return + p_time*mean_time_return - costs',
         'Estimate using earlier sessions only, pool sparse slots toward the side/regime average, include gaps.',
         'Prefer probability of target before stop plus time exits over optimizing win rate alone.'),
        ('Rolling deterioration','Observed wins - sum(prior-model win probabilities); realized net return per unit of initial risk',
         'Monitor by day blocks and use earlier sessions to calibrate alarm thresholds.',
         'Do not fit a separate October switch or use future MFE/MAE as predictors.'),
    ]
    p=pd.DataFrame(params,columns=['Parameter','Calculation','Availability constraint','Finding / use'])
    p.to_csv(OUT/'analytical_parameter_plan.csv',index=False)
    pieces=[
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>October strategy comparison</title>',
        '<style>body{font:15px/1.55 system-ui,sans-serif;color:#25344a;max-width:1350px;margin:36px auto;padding:0 24px}h1{font-size:28px}h2{font-size:20px;margin-top:30px}.table{overflow:auto}table{border-collapse:collapse;font-size:13px}td,th{padding:8px 11px;border-bottom:1px solid #dce2e9;text-align:right}th{background:#edf1f6}td:first-child,th:first-child{text-align:left}p{max-width:1100px}a{color:#1658a8}code{background:#edf1f6;padding:2px 4px}.scope{background:#f1f4f8;padding:14px 18px}</style>',
        '<h1>Why October win rates are lower</h1><p>Comparison through 23 October 2026. The decline is real in these saved simulations: it reflects weaker sustained follow-through and more adverse movement, particularly in shorts. Higher volatility is a measurable association, but neither higher VIX, trend disagreement, nor immediate reversal alone explains it.</p>',
        '<p class="scope">Same-version ledgers, executed trades only, win = net P&amp;L above zero. Available history: 29 July–23 October; 57 sessions (July 3, August 19, September 21, October 14). October is partial. 1 October is missing from sealed inputs and is excluded, never counted as a zero day. Monetary results use the existing ₹100,000 allocation per trade, 5× exposure, ₹1,000,000 portfolio and 5bps round-trip modeled cost. They are not the ₹10,000 LIVE sizing policy.</p>',
        '<h2>Monthly comparison</h2><p>Month columns below are win rates in percent. July appears in the CSV but is omitted here because three sessions are too few for a useful monthly reference.</p>',table(headline),
        '<p>Higher win rate does not automatically mean higher profits. October G7 V3 wins 68.75% but earns ₹111,238.59; G8 wins 62.96% and earns ₹154,863.64. G8 has 2 losing days out of 14 in October versus 3 out of 19 in August, so lower trade win rate does not imply the highest losing-day frequency.</p>',
        '<h2>What changed, and what did not</h2>',
        '<p><b>1. Volatility increased.</b> On the matched 09:20–13:30 Nifty clock sample through 21 October, mean realized volatility rises from 0.3248% in September to 0.5192% in October (+59.8%), and mean session-so-far range rises from 0.4851% to 0.9386% (+93.5%). Mean trend efficiency is 0.3973 versus 0.3970: a general claim of greater chop is unsupported. This market sample has 12 October sessions, whereas the final results above have 14.</p>',
        '<p>For executed G8 trades through 23 October, mean VIX at entry is 12.09 in September and 14.92 in October. Trade-signed Nifty slot alignment increases from +0.0286% to +0.0580%, so index alignment was stronger at entry. Short-side pre-entry stock ATR in the history through 22 October rises from 0.1192% to 0.1490% (+25%) while the initial stop stays 1.20%. A fixed stop therefore represents fewer units of current stock noise. This does not by itself prove that widening stops would help.</p>',
        '<p><b>2. Follow-through weakened after entry.</b> G7 V2 stop-loss exits rise from 7.19% to 18.75%, target exits fall from 38.13% to 32.14%, and the win rate of time-exit trades falls from 78.95% to 58.18%. G8 mean favorable excursion falls from 0.975% to 0.892%, while adverse excursion magnitude rises from 0.438% to 0.541%. These excursions are post-entry outcomes through the actual exit, including the inherited OHLC-bar ambiguity conventions; they cannot be used as entry-time predictors.</p>',
        '<p>The separate minute-path check through 22 October does not show a broad increase in immediate reversal: G8 shorts negative five minutes after entry are 45.26% in September versus 44.74% in October. Mean signed 15-minute short return declines from +0.093% to +0.053%. The problem is therefore broader than removing trades that turn red immediately. This path check follows stock prices through each stated horizon even if the original exit occurred earlier, and is diagnostic only.</p>',
        '<p><b>3. Shorts deteriorated within comparable groups.</b> G8 short win rate falls 85.26%→62.35%; long win rate falls 72.73%→65.22%. Even within low ATR bands the short win rate declines, so simply moving all high-volatility trades out would not remove the whole problem.</p>',
        '<p><b>4. The side mix does not explain the decline.</b> Relative to August–September, G8 drops 16.77 percentage points. October’s different long/short weights would improve win rate by 0.68 points at the earlier success rates; the change within sides contributes −17.46 points. Exact-slot decomposition is exploratory because many slots are sparse and one October trade has no prior matching setup.</p>',
        table(decomposition.loc[decomposition.dimension.eq('side'),['model','prior_win_rate_pct','october_win_rate_pct','change_pp','composition_effect_pp','within_group_effect_pp']]),
        '<h2>Where the decline is concentrated</h2><p>G8 setup-level contributions below compare October to August–September. Setup names use the confirmation clock. These are descriptions, not rules selected for deletion. For example, 13:21 SHORT went from 10 wins in 11 trades to 1 in 6, but six October trades cannot justify a permanent slot ban.</p>',table(worst),
        '<h2>Controls against misleading conclusions</h2>',
        '<p>Every October G6, G7 V2/V3, and G8 executed trade identity is also in G4; these strategies share entry ancestry. Their repeated decline is correlated evidence. Saved source records explicitly mark historical targets as selected after inspection and independent holdout qualification as false. G8 examined 230 initial combinations, 93 later choices plus sensitivity checks, and 27 exit variants. Earlier months are an optimistic benchmark, and October is not an untouched whole-strategy test.</p>',
        '<p>Some historical opening-OI and stock indicator coverage differs by month. The inherited saved VWAP/range columns are absent or unreliable in parts of October, so the market comparisons use separately reconstructed price features. A day-block bootstrap gives a descriptive interval for the change, but does not remove parameter-selection bias or dependence across days.</p>',table(intervals[['model','difference_pp','day_bootstrap_p025_pp','day_bootstrap_p975_pp']]),
        '<h2>Can a numerical rule improve it?</h2>',
        '<p>Yes, the conditions can be measured, but a profitable threshold has not been established. The existing G6 full portfolio replays through 21 October show that rejecting realized volatility above 1.5× the prior same-clock median reduces October profit by ₹55,431.57 and increases losing days from 2 to 4. Rejecting efficiency below 0.25 reduces profit by ₹42,066.41. Breadth opposition helps October by ₹5,750.23 but costs ₹16,698.04 in July–September.</p>',
        '<p>The already tested G8 shock rule raises October wins from 68 to 72 out of 108 and adds ₹7,521.43, but loses ₹13,825 in August and ₹14,458.74 in September. Its trigger is absolute 5-minute VIX change ≥1%, or trade-signed Nifty slot change ≤−0.05% while session change ≥+0.10%. Its action is initial SL 1.05% (0.875×1.20%) and half the original target, with existing timed tightening retained. It remains exploratory; prior automated development/validation retained original exits.</p>',
        table(p),
        '<p>A practical next experiment would use a few shared volatility/trend regimes with partial pooling across sparse slot/side groups. Estimate target-before-stop, stop-first and time-exit probabilities from earlier days, optimize net expectancy after costs, and retain a candidate only when later sequential test blocks preserve profit and reduce losses. Include a baseline fallback when samples or input history are insufficient. Do not optimize an October-specific calendar filter, fit 51 independent parameter sets on 14 sessions, or call a retrospective improvement a validated edge. Freeze any selected candidate for genuinely new data.</p>',
        '<h2>Files and definitions</h2><p><a href="monthly_all_models.csv">All monthly metrics</a> · <a href="analytical_parameter_plan.csv">Numerical parameter plan</a> · <a href="slot_side_contributions.csv">Slot/side attribution</a> · <a href="postentry_path_monthly.csv">Post-entry path diagnostics</a> · <a href="market/g6_full_policy_month_split.csv">Full portfolio filter checks</a> · <a href="book_source_provenance.csv">Source ledgers and configuration hashes</a> · <a href="g8/research_audit.json">G8 provenance and research history</a></p>',
        '<p>Win rates are trade-weighted, not averages of daily percentages. Mean daily win rates, average gains/losses, net profit per session, net return on fixed portfolio capital, profit factor and losing-day counts are in the monthly CSV. Stop-loss rates require both a STOP exit label and negative net P&amp;L; profitable G7 V3 runner stops are not losses. Full-target exit rates include G7 V3 full-runner targets; its separate two-stage first-target-hit rate includes subsequent profit-lock stops and time exits.</p>',
        '<p>Reference: <a href="https://www.nseindia.com/static/products-services/indices-indiavix-index">NSE India VIX definition</a> describes 30-calendar-day expected volatility, not a guaranteed intraday direction or stop distance. <a href="https://escholarship.org/uc/item/4w1110bb">Bailey et al., The Probability of Backtest Overfitting</a> explains the risks of selecting strategies through repeated backtests.</p>',
        '<p>Analysis only. No trading configurations, saved result books, approval controls, or publication pointers were changed.</p></html>',
    ]
    (OUT/'report.html').write_text('\n'.join(pieces),encoding='utf-8')
    sources=[OUT/'normalized_executed_books_g3_g7.csv', OUT/'common_calendar_g3_g7.csv',
        RUN/'baseline/combined_portfolio_trades.csv',RUN/'shock/combined_portfolio_trades.csv',
        OUT/'market/manifest.json',OUT/'g8/research_audit.json']
    manifest=dict(scope='READ_ONLY_RETROSPECTIVE_DIAGNOSTICS',through='2026-10-23',
        postentry_paths_through='2026-10-22',market_metrics_through='2026-10-21',
        independent_holdout=False,bootstrap_unit='session',bootstrap_seed=SEED,
        bootstrap_draws=20000,code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        inputs={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        outputs={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir()
                 if p.is_file() and p.name!='analysis_manifest.json'})
    (OUT/'analysis_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')


if __name__=='__main__':main()
