"""Training-only excursion study and causal entry-time stop features.

Only the first44 chronological sessions supply outcome labels for calibration.
Confirmation ATR/OHLC for every fill are immutable input features, not labels.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.loss_control_20261106 import inputs
from g3_v23_adx_rsi_20261106.run import sha
from g3_v21_expanded_20261104.refinement_20261104 import run as native_source
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import stock_features

PACKAGE = Path(__file__).resolve().parent
OUTPUT = PACKAGE / 'analysis_source'
CONF_FIELDS = ['confirmation_open','confirmation_high','confirmation_low','confirmation_close']
NATIVE_FIELDS = ['sid','day','tradingsymbol','side','confirmation_ts','v9_1m_atr14',*CONF_FIELDS]


def ceil_step(value, step):
    return float(np.ceil((float(value)-1e-12)/step)*step)


def excursions(path, entry_index, exit_index, entry, exit_price, is_long):
    """Bounds, not asserted exact intrabar MAE/MFE.

    The safe bound uses bars wholly between entry/exit bars and the known exit
    price. The upper bound also includes entry/exit extrema, which can precede
    entry or follow exit. Never use these outcome fields as live features.
    """
    if not 0 <= entry_index <= exit_index < len(path['close']):
        raise ValueError('Invalid excursion interval')
    sign = 1 if is_long else -1
    actual = sign * (float(exit_price)/entry - 1) * 100
    adverse_key, favorable_key = ('low','high') if is_long else ('high','low')
    result = {}
    for label, sl in [('lower',slice(entry_index+1,exit_index)),
                      ('upper',slice(entry_index,exit_index+1))]:
        adverse = -sign * (np.asarray(path[adverse_key][sl],float)/entry-1)*100
        favorable = sign * (np.asarray(path[favorable_key][sl],float)/entry-1)*100
        result['mae_'+label+'_pct'] = float(max([0.,-actual,*adverse]))
        result['mfe_'+label+'_pct'] = float(max([0.,actual,*favorable]))
    return result


def entry_features(data):
    trades = data['trades']
    wanted = set(trades.sid)
    audit = data['audit'].set_index('sid')
    old_proof = json.loads((inputs.OLD/'provenance.json').read_text())
    for name, expected in old_proof['checkpoint_sha256'].items():
        if sha(native_source.CHECKPOINTS/name) != expected:
            raise ValueError('Native checkpoint fingerprint changed: '+name)
    _, old, _, _ = native_source._load_books()
    old = old.loc[old.sid.isin(wanted),NATIVE_FIELDS].copy()
    old['feature_source_kind'] = 'HASH_VERIFIED_NATIVE_CHECKPOINT'
    newest = pd.read_csv(inputs.LATEST/'expanded_orders.csv')
    newest = newest.loc[newest.sid.isin(wanted),NATIVE_FIELDS].copy()
    newest['feature_source_kind'] = 'HASH_VERIFIED_NOV5_NATIVE_SOURCE'
    completed = pd.concat([old,newest],ignore_index=True)
    missing = trades.loc[~trades.sid.isin(completed.sid)].copy()
    if len(missing)!=16 or not missing.day.eq('2026-11-04').all():
        raise ValueError('Expected exactly sixteen Nov4 entry feature records')
    day_proof = old_proof['days']['2026-11-04']
    feature_path = Path(day_proof['feature_path'])
    if sha(feature_path) != day_proof['features_sha256']:
        raise ValueError('Nov4 sealed feature ledger changed')
    raw = pd.read_csv(feature_path)
    raw['_confirmation_utc'] = pd.to_datetime(raw.confirmation_ts,utc=True)
    new_rows, source_proofs = [], []
    for row in missing.itertuples(index=False):
        confirmation = pd.Timestamp(audit.loc[row.sid,'confirmation_ts'])
        found = raw.loc[raw.tradingsymbol.eq(row.tradingsymbol)
                        & raw._confirmation_utc.eq(confirmation.tz_convert('UTC'))]
        common = dict(sid=int(row.sid),day=row.day,tradingsymbol=row.tradingsymbol,
                      side=row.side,confirmation_ts=str(confirmation))
        if len(found)==1:
            observed = found.iloc[0]
            record = dict(common,v9_1m_atr14=float(observed.v9_1m_atr14),
                          **{f:float(observed[f]) for f in CONF_FIELDS},
                          feature_source_kind='HASH_VERIFIED_NOV4_FEATURE_LEDGER')
        elif len(found)==0:
            path, expected, proof = stock_features._source_for(row.day,row.tradingsymbol)
            frame, proof = stock_features._prepared_frame(path,[common],proof)
            indexes = frame.index[frame.ts.eq(confirmation)]
            if len(indexes)!=1:
                raise ValueError('Missing exact completed Nov4 confirmation')
            at = int(indexes[0]); bar=frame.iloc[at]
            components = stock_features._extension_series(frame)
            record = dict(common,v9_1m_atr14=float(components.atr14.iloc[at]),
                          **{'confirmation_'+key:float(bar[key]) for key in ('open','high','low','close')},
                          feature_source_kind='HASH_VERIFIED_NOV4_RAW_CONFIRMATION_PREFIX')
            source_proofs.append(proof)
        else:
            raise ValueError('Duplicated Nov4 confirmation feature')
        new_rows.append(record)
    features=pd.concat([completed,pd.DataFrame(new_rows)],ignore_index=True)
    features['day']=features.day.astype(str).str[:10]
    if not features.sid.is_unique or set(features.sid)!=wanted:
        raise ValueError('Entry feature inventory changed')
    features=features.merge(trades[['sid','entry_price','entry_ts','setup_id']],on='sid',validate='one_to_one')
    for field in ['day','tradingsymbol','side']:
        if not features.set_index('sid')[field].sort_index().eq(trades.set_index('sid')[field].sort_index()).all():
            raise ValueError('Entry feature identity mismatch: '+field)
    confirmation=pd.to_datetime(features.confirmation_ts,utc=True)
    if not confirmation.eq(pd.to_datetime(features.sid.map(audit.confirmation_ts),utc=True)).all():
        raise ValueError('Entry feature confirmation timestamp changed')
    if not confirmation.le(pd.to_datetime(features.entry_ts,utc=True)).all():
        raise ValueError('Feature is not known by entry')
    fields=['v9_1m_atr14',*CONF_FIELDS,'entry_price']
    values=features[fields].to_numpy(float)
    if not np.isfinite(values).all() or not (values>0).all():
        raise ValueError('Nonfinite/nonpositive entry risk feature')
    features['atr14_1m']=features.v9_1m_atr14
    features['atr14_pct']=features.atr14_1m/features.entry_price*100
    features['feature_available_at']=features.confirmation_ts.astype(str)
    sign=np.where(features.side.eq('LONG'),1.,-1.)
    edge=np.where(features.side.eq('LONG'),features.confirmation_low,features.confirmation_high)
    features['structure_edge_price']=edge
    features['structure_distance_pct']=sign*(features.entry_price-edge)/features.entry_price*100
    if not features.structure_distance_pct.ge(0).all():
        raise ValueError('Entry is on wrong side of known confirmation edge')
    proof=dict(checkpoint_sha256=old_proof['checkpoint_sha256'],nov4_feature_ledger=str(feature_path),
               nov4_features_sha256=day_proof['features_sha256'],nov4_raw_stock_proofs=source_proofs)
    return features.sort_values('sid').reset_index(drop=True),proof


def training_excursions(data):
    fit_days=data['days'][:44]
    if fit_days[-1]!='2026-10-05':
        raise ValueError('Pinned first44 calibration inventory changed')
    meta=data['parity'].set_index('sid'); rows=[]
    for row in data['trades'].loc[data['trades'].day.isin(fit_days)].itertuples(index=False):
        m=meta.loc[row.sid]
        values=excursions(data['paths'][int(row.sid)],int(m.entry_index),int(m.exit_index),
                           float(row.entry_price),float(row.exit_price),row.side=='LONG')
        rows.append(dict(sid=int(row.sid),day=row.day,side=row.side,
                         baseline_winner=bool(row.portfolio_net_profit_rupees>0),
                         baseline_net_pnl_rs=float(row.portfolio_net_profit_rupees),**values))
    return pd.DataFrame(rows)


def fit_models(features, excursions_frame, fit_days):
    labels=excursions_frame.copy()
    if not labels.day.isin(fit_days).all() or labels.sid.duplicated().any():
        raise ValueError('Calibration labels extend beyond the first44 sessions')
    train=features.loc[features.day.isin(fit_days)].merge(
        labels[['sid','baseline_winner','mae_lower_pct','mae_upper_pct']],on='sid',validate='one_to_one')
    if len(train)!=len(labels) or train.baseline_winner.sum()<40:
        raise ValueError('Insufficient or mismatched training winners')
    winners=train.loc[train.baseline_winner]
    median_atr=float(train.atr14_pct.median())
    train['volatility_bin']=np.where(train.atr14_pct.le(median_atr),'low','high')
    q95=float(winners.mae_upper_pct.quantile(.95))
    q975=float(winners.mae_upper_pct.quantile(.975))
    multiplier=float((winners.mae_upper_pct/winners.atr14_pct).quantile(.95))
    bins={}
    for label in ['low','high']:
        part=train.loc[train.volatility_bin.eq(label)&train.baseline_winner]
        q=float(part.mae_upper_pct.quantile(.95)) if len(part)>=40 else q95
        bins[label]=dict(winner_samples=len(part),mae_upper_q95_pct=q,
                         stop_pct=float(np.clip(ceil_step(q,.05),.50,1.25)),
                         fallback_global=len(part)<40)
    return dict(fit_days=fit_days,fit_sessions=len(fit_days),fit_trades=len(train),
        fit_winners=int(train.baseline_winner.sum()),fit_losers=int((~train.baseline_winner).sum()),
        quantile_definition='Pandas linear quantile of native winner MAE upper bound; entry/exit bar chronology uncertain.',
        winner_q95_pct=q95,winner_q975_pct=q975,
        fixed_q95_stop_pct=float(np.clip(ceil_step(q95,.05),.50,1.25)),
        fixed_q975_stop_pct=float(np.clip(ceil_step(q975,.05),.50,1.25)),
        volatility_boundary_atr_pct=median_atr,volatility_bins=bins,
        atr_multiplier_raw_q95=multiplier,atr_multiplier_q95=ceil_step(multiplier,.25),
        atr_stop_floor_pct=.50,atr_stop_cap_pct=1.25,
        all_history_previously_observed=True,
        calibration_labels_only_first44=True,no_threshold_fit_from_next15_or_last7=True)


def run(output=OUTPUT):
    output=Path(output).resolve()
    if output.exists() or output==PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh analysis source folder inside isolated entry_sl study')
    data=inputs.load()
    features,proof=entry_features(data)
    labels=training_excursions(data)
    fit=fit_models(features,labels,data['days'][:44])
    features['volatility_bin']=np.where(features.atr14_pct.le(fit['volatility_boundary_atr_pct']),'low','high')
    quantiles=labels.groupby('baseline_winner')[['mae_lower_pct','mae_upper_pct','mfe_lower_pct','mfe_upper_pct']].quantile([.50,.75,.90,.95,.975,.99]).reset_index().rename(columns={'level_1':'quantile'})
    output.mkdir(parents=True)
    features.to_csv(output/'entry_features.csv',index=False)
    labels.to_csv(output/'training_excursions.csv',index=False)
    quantiles.to_csv(output/'training_quantiles.csv',index=False)
    (output/'fit.json').write_text(json.dumps(fit,indent=2)+'\n')
    coverage=dict(entries=len(features),finite_positive_atr=int(features.atr14_pct.gt(0).sum()),
                  source_kind_counts=features.feature_source_kind.value_counts().to_dict(),
                  all_features_available_at_confirmation=True,all_confirmation_times_at_or_before_entry=True,
                  atr_definition='One-minute simple rolling14 true range, inherited from native v9_1m_atr14; raw Nov4 clocks use the same G3/G7 formula. ATR is in price units; atr14_pct divides by actual entry price.',
                  structure_definition='LONG: completed confirmation low; SHORT: completed confirmation high. Distance measured from actual native fill, available when fill occurs.',
                  fit_sessions=data['days'][:44],validation_sessions=data['days'][44:59],final_sessions=data['days'][59:])
    (output/'coverage.json').write_text(json.dumps(coverage,indent=2)+'\n')
    lines=['# Entry-relative stop data study','',
        'Only the first44 sessions (29 July–5 October2026) supply outcomes for this calibration. '
        'The next15 and final7 sessions are excluded from fitting. All history was previously observed '
        'in other research, so this is retrospective, not an untouched holdout.', '',
        f'Training sample: {fit["fit_trades"]} trades, {fit["fit_winners"]} native winners and {fit["fit_losers"]} losers.', '',
        'Native winner adverse excursion q95 is %.6f%%, q97.5 is %.6f%%. Upward0.05%% rounding gives %.2f%% and %.2f%% fixed entry stop hypotheses.'%(fit['winner_q95_pct'],fit['winner_q975_pct'],fit['fixed_q95_stop_pct'],fit['fixed_q975_stop_pct']), '',
        'The training ATR median is %.8f%% of entry. Below/above this boundary, rounded winnerq95 stops are %.2f%% / %.2f%%. Training winnerMAE/ATRq95 is %.6f, rounded upward to %.2f ATR, with0.50%% floor/1.25%% cap.'%(fit['volatility_boundary_atr_pct'],fit['volatility_bins']['low']['stop_pct'],fit['volatility_bins']['high']['stop_pct'],fit['atr_multiplier_raw_q95'],fit['atr_multiplier_q95']), '',
        'These are hypotheses, not optimized profitable rules. They must be replayed against every baseline entry. Winner labels are used only to fit prior calibration quantiles; no current-trade outcome determines its stop.', '',
        '## Excursion bounds','',
        'Exact intrabar MAE/MFE cannot be recovered from one-minute OHLC. The lower bound uses wholly post-entry/pre-exit bars plus the known exit price. The upper bound also includes entry/exit bar extremes; those extremes may precede entry or follow exit. Calibration uses the conservative upper bound to avoid understating the space a winner needed. Native stops censor loser excursions; no claim is made about their unrestricted future path.', '',
        '## Entry features','',coverage['atr_definition'], '',coverage['structure_definition'], '',
        'All790 entries have finitepositiveATR and completed confirmationOHLC.755 records come from verified native checkpoints,19 from the sealed Nov5 native source,9 from the Nov4 sealed feature ledger and7 from hashverified Nov4 stock prefixes.', '',
        '[Training quantiles](training_quantiles.csv) · [Training excursion rows](training_excursions.csv) · [All entry-time features](entry_features.csv) · [Frozen fit](fit.json) · [Coverage](coverage.json)','']
    (output/'STUDY.md').write_text('\n'.join(lines),encoding='utf-8')
    provenance=dict(source_manifests=data['source_manifests'],feature_source_proof=proof,
        code_sha256={str(p):sha(p) for p in [Path(__file__),Path(inputs.__file__),Path(stock_features.__file__),Path(native_source.__file__)]})
    (output/'provenance.json').write_text(json.dumps(provenance,indent=2,default=str)+'\n')
    (output/'manifest.json').write_text(json.dumps({p.name:sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(json.dumps(fit,indent=2),flush=True)
    return fit


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,default=OUTPUT)
    run(p.parse_args().output)
