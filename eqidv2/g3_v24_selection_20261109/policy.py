"""Predeclared ranking ablations; no outcome fields or new entry vetoes."""
import hashlib
import json

import numpy as np
import pandas as pd

from g3_v21_expanded_20261104 import selection
from g3_v21_expanded_20261104.refinement_20261104.strict_new_pairs_20261104 import policy as strict

POLICIES = ('baseline_v23', 'stretch_rank', 'structure_rank', 'combined_rank', 'combined_volume_rank')


def configuration():
    return dict(version='G3-V24-SELECTION-20261109', scope='BACKTEST_ONLY', execution_authority=False,
        policies=list(POLICIES), original_pairs=34, original_cap=3, new_pairs=39, new_cap=1,
        core_first=True, postrank_gates_unchanged=True, rejected_leader_replacement=False,
        rank_population='Full G3-filter-passing pool before slot quota, within each day/setup/core tier',
        stretch='Smaller max(0,directional distance of confirmation close from 1m EMA20 / ATR14)',
        structure='Completed confirmation beyond prior same-day extreme preferred; otherwise more room in ATR',
        combined='Equal average of within-tier percentile ranks of stretch and structure',
        combined_volume='Equal average of stretch, structure and descending existing confirmation RVOL ranks',
        tie_break='Original donor rank then SID', missing='Entire day/setup uses original ranking if any candidate lacks required features',
        thresholds_optimized=False, hypothesis_source='Nov9 retrospective inspection; no unseen holdout claimed')


def fingerprint():
    return hashlib.sha256(json.dumps(configuration(),sort_keys=True).encode()).hexdigest()


def select(pool, name):
    """Rank eligible candidates, cap, then retain the existing new-pair volume veto."""
    if name not in POLICIES:
        raise ValueError('Unknown ranking policy')
    required={'sid','day','setup_id','v24_core_pass','v9_rank_in_setup_day','volume_ratio'}
    if not required.issubset(pool):
        raise ValueError('Incomplete candidate pool')
    if pool.sid.duplicated().any():
        raise ValueError('Duplicate SID')
    frame=pool.copy()
    frame['v24_rank_score']=0.
    frame['v24_missing_fallback']=False
    frame['v24_quota_selected']=False
    frame['v24_rank']=0
    if frame.empty:
        return frame.copy(),frame
    if 'v9_filter_pass' in frame and not frame.v9_filter_pass.eq(True).all():
        raise ValueError('Only G3-filter-passing candidates can be ranked')
    for (_,setup),group in frame.groupby(['day','setup_id'],sort=True):
        if setup not in selection.SPECS:
            raise ValueError('Unknown setup')
        fallback=False
        if name!='baseline_v23':
            fields=['v24_extension_atr','v24_room_atr','v24_close_breakout']
            if name=='combined_volume_rank': fields+=['v9_1m_volume_ratio']
            fallback=(not set(fields+['v24_feature_ready']).issubset(group)
                      or not group.get('v24_feature_ready',pd.Series(False,index=group.index)).eq(True).all()
                      or not np.isfinite(group[fields].astype(float).to_numpy()).all())
        frame.loc[group.index,'v24_missing_fallback']=fallback
        if name!='baseline_v23' and not fallback:
            for _,tier in group.groupby('v24_core_pass',sort=False):
                stretch=tier.v24_extension_atr.clip(lower=0).rank(method='average',pct=True)
                # Tuple order avoids a fitted resistance-distance threshold.
                tuples=[(not bool(r.v24_close_breakout),-float(r.v24_room_atr) if not r.v24_close_breakout else 0.)
                        for r in tier.itertuples()]
                levels={value:i for i,value in enumerate(sorted(set(tuples)))}
                structure=pd.Series([levels[x] for x in tuples],index=tier.index).rank(method='average',pct=True)
                if name=='stretch_rank': score=stretch
                elif name=='structure_rank': score=structure
                elif name=='combined_rank': score=(stretch+structure)/2
                else:
                    volume=tier.v9_1m_volume_ratio.rank(method='average',pct=True,ascending=False)
                    score=(stretch+structure+volume)/3
                frame.loc[tier.index,'v24_rank_score']=score
        ranked=frame.loc[group.index].sort_values(
            ['v24_core_pass','v24_rank_score','v9_rank_in_setup_day','sid'],
            ascending=[False,True,True,True],kind='stable')
        cap=1 if setup in strict.NEW_IDS else 3
        frame.loc[ranked.index,'v24_rank']=range(1,len(ranked)+1)
        frame.loc[ranked.index[:cap],'v24_quota_selected']=True
    minimum=frame.setup_id.map({k:s.volume_ratio for k,s in selection.SPECS.items()})
    frame['v24_volume_gate_allowed']=(~frame.setup_id.isin(strict.NEW_IDS)
        | frame.volume_ratio.ge(minimum+strict.VOLUME_INCREMENT-1e-12))
    selected=frame.loc[frame.v24_quota_selected & frame.v24_volume_gate_allowed].copy()
    selected['v9_selected']=True
    selected['v10_g_f_core']=selected.v24_core_pass
    selected['v24_policy']=name
    return selected,frame
