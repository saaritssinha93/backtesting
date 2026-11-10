"""Frozen entry paths, confirmed features and the user-adopted1.50% benchmark."""
from pathlib import Path
import json
import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.loss_control_20261106 import inputs
from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest
from g3_v23_adx_rsi_20261106 import run as original
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries
from g3_v23_adx_rsi_20261106.entry_sl_20261106.calibration import _validate_features

ROOT=Path(__file__).resolve().parents[1]
BENCHMARK=ROOT/'entry_sl_20261106/recovery_room_20261106/results_through_20261105'
FEATURES=ROOT/'entry_sl_20261106/analysis_source'


def adaptive_risk(features):
    structural=_validate_features(features)
    distance=np.clip(structural+.25*features.atr14_pct.to_numpy(float),.4,1.)
    return pd.Series(np.where(structural>0,distance,1.5),index=features.index)


def load():
    data=inputs.load()
    for folder in (BENCHMARK,FEATURES):
        verify_manifest(folder)
        for name,expected in json.loads((folder/'provenance.json').read_text()).get('code_sha256',{}).items():
            if original.sha(name)!=expected:
                raise ValueError('Source code changed: '+name)
        data['source_manifests'][str(folder)]=original.sha(folder/'manifest.json')
    baseline=pd.read_csv(BENCHMARK/'constant_1p50_trades.csv')
    assert_entries(data['trades'],baseline)
    features=pd.read_csv(FEATURES/'entry_features.csv')
    assert_entries(baseline,features)
    _validate_features(features)
    features['adaptive_initial_stop_pct']=adaptive_risk(features)
    if len(baseline)!=790 or baseline.sid.duplicated().any() or not np.isclose(
            baseline.portfolio_net_profit_rupees.sum(),1220580.5488569569,atol=1e-5,rtol=0):
        raise ValueError('User-adopted fixed1.50% benchmark changed')
    data['trades']=baseline
    data['entry_features']=features
    return data
