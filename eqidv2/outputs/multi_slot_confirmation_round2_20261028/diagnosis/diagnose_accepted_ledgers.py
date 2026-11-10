"""Descriptive accepted-ledger audit; no rule selection or threshold tuning."""
from pathlib import Path
import hashlib
import json
import math

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
BASE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-28/20261028T143059782924_f86f1197_0926_short_wick020_lock2')
SETUPS = ['0931_LONG','0931_SHORT','1001_LONG','1051_LONG','1111_SHORT','1146_SHORT','1226_SHORT','1231_SHORT','1301_SHORT','1321_SHORT']

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def flag(series):
    out = series.astype('string').fillna('False').str.lower()
    assert out.isin(['true','false']).all(), out.unique()
    return out.eq('true')

def clean(value):
    if isinstance(value, dict): return {k: clean(v) for k,v in value.items()}
    if isinstance(value, list): return [clean(v) for v in value]
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating, float)) and not math.isfinite(value): return None
    return value

def wilson(w,n):
    if not n: return np.nan
    z=1.959963984540054
    p=w/n
    return 100*(p+z*z/(2*n)-z*math.sqrt(p*(1-p)/n+z*z/(4*n*n)))/(1+z*z/n)

def stats(frame):
    loss=frame.loc[frame.net.lt(0)]
    wins=frame.loc[frame.net.gt(0)]
    result=dict(trades=len(frame),wins=len(wins),losses=len(loss),flats=int(frame.net.eq(0).sum()),
        win_rate_pct=100*len(wins)/len(frame) if len(frame) else np.nan,
        wilson_95_lower_pct=wilson(len(wins),len(frame)),
        gross_wins=int(frame.gross.gt(0).sum()),gross_losses=int(frame.gross.lt(0).sum()),gross_flats=int(frame.gross.eq(0).sum()),
        gross_winners_turned_net_losses=int((frame.gross.gt(0)&frame.net.lt(0)).sum()),
        gross_pnl_rs=float(frame.gross.sum()),cost_rs=float(frame.cost.sum()),net_pnl_rs=float(frame.net.sum()),
        loss_amount_rs=float(-loss.net.sum()),win_amount_rs=float(wins.net.sum()),
        mean_win_rs=float(wins.net.mean()),mean_loss_rs=float(loss.net.mean()),
        mean_hold_min=float(frame.holding_minutes.mean()),median_hold_min=float(frame.holding_minutes.median()),
        losses_median_hold_min=float(loss.holding_minutes.median()),wins_median_hold_min=float(wins.holding_minutes.median()),
        losses_hold_le_5min=int(loss.holding_minutes.le(5).sum()),losses_hold_le_15min=int(loss.holding_minutes.le(15).sum()),
        losses_hold_le_30min=int(loss.holding_minutes.le(30).sum()),
        loss_median_mfe_pct=float(loss.mfe_pct.median()),loss_median_mae_pct=float(loss.mae_pct.median()),
        win_median_mfe_pct=float(wins.mfe_pct.median()),win_median_mae_pct=float(wins.mae_pct.median()),
        loss_mfe_available=int(loss.mfe_pct.notna().sum()),loss_mfe_lt_0p6=int(loss.mfe_pct.lt(.6).sum()),
        loss_ohlc_mfe_ge_0p6=int(loss.mfe_pct.ge(.6).sum()),
        entry_gap_count=int(frame.entry_gap_through.sum()),loss_entry_gap_count=int(loss.entry_gap_through.sum()),
        exit_gap_count=int(frame.exit_gap_through.sum()),loss_exit_gap_count=int(loss.exit_gap_through.sum()),
        ambiguous_bar_count=int(frame.same_bar_ambiguous.sum()),loss_ambiguous_bar_count=int(loss.same_bar_ambiguous.sum()),
        max_entry_overshoot_bps=float(frame.entry_overshoot_bps.max()),
        first_trade_day=None if frame.empty else frame.day.min(),last_trade_day=None if frame.empty else frame.day.max())
    if 'protection_armed_path_index' in frame:
        result.update(protection_armed_trades=int(frame.protection_armed_path_index.notna().sum()),
            protection_armed_losses=int(loss.protection_armed_path_index.notna().sum()),
            protection_applied_trades=int(frame.protection_applied.sum()),
            protection_applied_losses=int(loss.protection_applied.sum()))
    return result

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    summaries=[]; exits=[]; daily=[]; allrows=[]; model=[]; source=[]
    for strategy, filename in [('G3','g3_reference_portfolio_trades.csv'),('G7_V4_LOCK2','portfolio_trades.csv')]:
        path=BASE/filename
        data=pd.read_csv(path,low_memory=False)
        data.day=data.day.astype(str).str[:10]
        assert data.day.max()=='2026-10-27'
        sessions=sorted(data.day.unique())
        assert len(sessions)==59
        source.append(dict(strategy=strategy,path=str(path),sha256=digest(path),rows=len(data),first_session=sessions[0],last_session=sessions[-1],sessions=len(sessions)))
        for col in ['portfolio_executed','filled','entry_gap_through','exit_gap_through','same_bar_ambiguous']:
            data[col]=flag(data[col])
        if 'protection_applied' in data: data['protection_applied']=flag(data.protection_applied)
        raw=data.loc[data.setup_id.isin(SETUPS)].copy()
        frame=raw.loc[raw.portfolio_executed].copy()
        assert frame.filled.all()
        assert not frame[['day','setup_id','tradingsymbol','entry_ts']].duplicated().any()
        frame['net']=pd.to_numeric(frame.portfolio_net_profit_rupees,errors='raise')
        frame['gross']=pd.to_numeric(frame.portfolio_gross_profit_rupees,errors='raise')
        frame['cost']=pd.to_numeric(frame.portfolio_cost_rupees,errors='raise')
        assert np.allclose(frame.gross-frame.cost,frame.net,rtol=0,atol=1e-7)
        assert np.allclose(frame.portfolio_trade_exposure_rupees*frame.gross_return_pct/100,frame.gross,rtol=0,atol=1e-7)
        assert np.allclose(frame.portfolio_trade_exposure_rupees*frame.cost_pct/100,frame.cost,rtol=0,atol=1e-7)
        frame['strategy']=strategy
        frame['net_outcome']=np.where(frame.net.gt(0),'WIN',np.where(frame.net.lt(0),'LOSS','FLAT'))
        periods={'all':sessions,'pre_october':[x for x in sessions if x<'2026-10-01'],'october':[x for x in sessions if x.startswith('2026-10')],
                 'development':sessions[:35],'later':sessions[35:]}
        for period,days in periods.items():
            for setup in ['ALL_REQUESTED']+SETUPS:
                subset=frame.loc[frame.day.isin(days)&(True if setup=='ALL_REQUESTED' else frame.setup_id.eq(setup))]
                summaries.append(dict(strategy=strategy,setup_id=setup,period=period,calendar_sessions=len(days),calendar_first=days[0],calendar_last=days[-1],**stats(subset)))
                for reason,part in subset.groupby('exit_reason'):
                    exits.append(dict(strategy=strategy,setup_id=setup,period=period,exit_reason=reason,**stats(part)))
        for day in sessions:
            daily.append(dict(strategy=strategy,day=day,**stats(frame.loc[frame.day.eq(day)])))
        values={c:sorted(frame[c].dropna().unique().tolist()) for c in ['capital_model','capital_per_entry_rupees','leverage_factor','exposure_per_entry_rupees','portfolio_trade_capital_rupees','portfolio_trade_exposure_rupees','cost_pct','initial_stop_pct','native_stop_pct','native_target_pct']}
        model.append(dict(strategy=strategy,requested_selected_rows=len(raw),requested_executed_rows=len(frame),portfolio_status_counts=raw.portfolio_status.value_counts().to_dict(),observed_values=values))
        allrows.append(frame)
    summary=pd.DataFrame(summaries)
    summary.to_csv(OUT/'slot_period_diagnostics.csv',index=False)
    pd.DataFrame(exits).to_csv(OUT/'exit_reason_diagnostics.csv',index=False)
    pd.DataFrame(daily).to_csv(OUT/'daywise_requested_slots.csv',index=False)
    trades=pd.concat(allrows,ignore_index=True)
    wanted=['strategy','day','setup_id','tradingsymbol','net_outcome','entry_ts','entry_price','exit_ts','exit_price','exit_reason','holding_minutes','gross','cost','net','gross_return_pct','net_return_pct','mfe_pct','mae_pct','entry_gap_through','entry_overshoot_bps','exit_gap_through','exit_gap_bps','same_bar_ambiguous','protection_armed_path_index','protection_effective_path_index','protection_applied','portfolio_trade_capital_rupees','portfolio_trade_exposure_rupees','leverage_factor','cost_pct']
    trades[[c for c in wanted if c in trades]].to_csv(OUT/'trade_diagnostics.csv',index=False)
    doc=dict(scope='Descriptive accepted baseline diagnostics only; no rule or threshold is selected from these results.',data_end='2026-10-27',october_28='Excluded from this frozen comparable baseline.',sources=source,exposure_model=model,
        definitions=dict(winner='portfolio_net_profit_rupees > 0; every executed trade in the denominator, including flats.',loss='portfolio_net_profit_rupees < 0',
            holding_time='Existing one-minute simulator clock: entry candle end to reported exit clock; actual intraminute fill time is unknown. A long holding duration does not establish when an adverse reversal began.',
            mfe='Ledger OHLC excursion from the entry bar through exit: intrabar/close exits include the full exit-bar high/low; open exits exclude the exit bar. The entry candle can include pre-entry prices, and the exit candle can include post-exit prices. No later bars through square-off are included after an earlier exit. MFE >=0.6 is an OHLC envelope observation, not proof of an executable gain or a completed close.',
            g7_armed='Non-null protection_armed_path_index establishes a full post-entry completed close reached +0.6% before exit; protection effective from next bar. Target and stop checks precede arming at the same close.',
            g3_close_threshold='G3 accepted ledger has no protection arming field; a completed-close +0.6% count is not inferred from MFE.',
            inference_limit='All comparisons are descriptive hindsight, not causal explanations or estimates of an intervention effect.',
            quantity='G3 uses continuous fixed exposure. G7 uses integer cash quantity, so observed trade exposure is typically slightly below the intended INR500000.'),
        accounting_checks=dict(gross_minus_cost_equals_net=True,exposure_times_return_equals_gross=True,exposure_times_cost_rate_equals_cost=True,unique_trade_keys=True,only_filled_executed_rows=True),
        overall=summary.loc[summary.setup_id.eq('ALL_REQUESTED')].to_dict('records'))
    (OUT/'diagnosis.json').write_text(json.dumps(clean(doc),indent=2,allow_nan=False),encoding='utf-8')
    manifest={p.name:digest(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='artifact_manifest.json'}
    (OUT/'artifact_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(summary.loc[summary.setup_id.eq('ALL_REQUESTED'),['strategy','period','trades','wins','losses','gross_wins','gross_winners_turned_net_losses','net_pnl_rs','losses_median_hold_min','losses_hold_le_5min','losses_hold_le_15min','losses_hold_le_30min','loss_median_mfe_pct','loss_ohlc_mfe_ge_0p6','loss_entry_gap_count','loss_exit_gap_count','loss_ambiguous_bar_count','protection_armed_losses']].to_string(index=False))
    print(pd.DataFrame(exits).query("setup_id=='ALL_REQUESTED' and period=='all'")[['strategy','exit_reason','trades','wins','losses','net_pnl_rs']].to_string(index=False))

if __name__=='__main__': main()
