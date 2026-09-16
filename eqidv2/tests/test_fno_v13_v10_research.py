import pandas as pd

import fno_v13_v10_research as r


def test_higher_headline_win_rate_cannot_hide_cost_stress_failure():
    m=dict(trades=25,wins=22,losses=3,breakeven=0,win_rate_pct=88.,profit_factor=3.,
           net_profit_rupees=10000.,daily_close_drawdown_rupees=1000.)
    stats={(p,c):dict(m) for p in ['TRAIN','VALIDATION'] for c in [5,9]}
    control={key:dict(m,win_rate_pct=75.) for key in stats}
    stats[('VALIDATION',9)]['win_rate_pct']=68.
    row=r.assessed(r.grid()[0],stats,control)
    assert not row['passes_objective']
    assert 'VALIDATION_9bps_WIN_RATE_NOT_HIGHER' in row['reasons']


def candidate(i,stop,target,win=85.,passes=True):
    return dict(candidate_id=i,policy='SINGLE',partial_pct=1.,runner_stop='INITIAL',
        initial_stop_pct=stop,first_target_pct=target,runner_target_pct=target,
        passes_objective=passes,minimum_split_win_pct=win,minimum_split_pf=2.5,development_net=50000.,
        TRAIN_5bps_net_profit_rupees=30000.,VALIDATION_5bps_net_profit_rupees=20000.)


def test_supported_grid_cell_beats_isolated_higher_win_setting():
    frame=pd.DataFrame([candidate(0,1.,1.),candidate(1,.95,1.),candidate(2,1.05,1.),
                        candidate(3,1.5,2.,win=99.)])
    chosen,status,_=r.choose(frame)
    assert chosen['candidate_id']==0
    assert chosen['adjacent_passing_cells']==2
    assert 'NEIGHBOR_SUPPORT' in status


def test_no_pass_is_explicitly_experimental():
    chosen,status,_=r.choose(pd.DataFrame([candidate(0,1.,1.,passes=False)]))
    assert chosen['candidate_id']==0
    assert status=='EXPERIMENTAL_BEST_AVAILABLE_OBJECTIVE_NOT_MET'


def test_later_results_are_excluded_from_both_development_splits():
    days=[pd.Timestamp(d).date() for d in ['2026-08-13','2026-08-14','2026-08-26','2026-08-27','2026-09-11']]
    split=r.periods(days)
    assert set(split['TRAIN']+split['VALIDATION']).isdisjoint(split['LATER_PREVIOUSLY_SEEN'])
