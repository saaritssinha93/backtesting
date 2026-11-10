from pathlib import Path
import hashlib,json
import pandas as pd
import numpy as np

BASE=Path(__file__).resolve().parents[1]
OUT=BASE/'late'
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def load(p): return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def run():
    manifest=load(OUT/'artifact_manifest.json')
    assert all(sha(OUT/p)==h for p,h in manifest['files'].items())
    provenance=load(OUT/'source_provenance.json')
    assert all(sha(p)==h for p,h in provenance['inputs_and_code'].items())
    features=load(BASE/'feature_manifest.json')
    assert all(sha(BASE/p)==h for p,h in features['files'].items())
    parity=load(OUT/'baseline_reproduction.json')
    assert all(x['passed'] for x in parity)
    setup=pd.read_csv(OUT/'variant_setup_metrics.csv')
    portfolio=pd.read_csv(OUT/'variant_portfolio_metrics.csv')
    decisions=pd.read_csv(OUT/'development_selection.csv')
    assert len(setup)==4*2*9*3*7
    assert len(portfolio)==len(setup)
    assert len(decisions)==4*2*8
    index=['setup_id','strategy','variant','cost_bps','period']
    assert not setup[index].duplicated().any()
    assert not portfolio[index].duplicated().any()
    assert (setup.trades==setup.wins+setup.losses+setup.flats).all()
    assert np.allclose(setup.win_rate_pct,setup.wins/setup.trades*100,equal_nan=True)
    dev=setup.query("cost_bps==5 and period=='development'").set_index(['setup_id','strategy','variant'])
    devp=portfolio.query("cost_bps==5 and period=='development'").set_index(['setup_id','strategy','variant'])
    checks=0
    for row in decisions.itertuples():
        key=(row.setup_id,row.strategy,row.variant)
        r=dev.loc[key]; b=dev.loc[(row.setup_id,row.strategy,'baseline')]
        rp=devp.loc[key]; bp=devp.loc[(row.setup_id,row.strategy,'baseline')]
        eligible=(r.trades>=8 and r.win_rate_pct>75 and r.net_pnl_rs>=b.net_pnl_rs-1e-6 and r.negative_trade_sum_abs_rs<b.negative_trade_sum_abs_rs-1e-6 and rp.net_pnl_rs>=bp.net_pnl_rs-1e-6)
        assert bool(eligible)==bool(row.eligible)
        checks+=1
    selected=[]
    for key, rows in decisions.groupby(['setup_id','strategy']):
        eligible=rows.loc[rows.eligible].sort_values(['development_setup_net_pnl_rs','development_losses','variant_order'],ascending=[False,True,True])
        actual=rows.loc[rows.selected_from_development,'variant'].tolist()
        expect=eligible.variant.head(1).tolist()
        assert actual==expect,(key,actual,expect)
        selected.append(dict(setup_id=key[0],strategy=key[1],selected=actual))
    permission=pd.read_csv(OUT/'g7_candidate_features.csv')
    for col in ['feature_available_at','indicator_available_at']:
        assert (pd.to_datetime(permission[col],utc=True)<=pd.to_datetime(permission.confirmation_ts,utc=True)).all()
    assert permission.sid.is_unique
    report=dict(passed=True,artifact_hashes=len(manifest['files']),provenance_hashes=len(provenance['inputs_and_code']),feature_hashes=len(features['files']),baseline_parity_checks=len(parity),setup_metric_rows=len(setup),portfolio_metric_rows=len(portfolio),development_gates_recomputed=checks,selection_tie_breaks_recomputed=len(selected),g7_permission_rows=len(permission),causal_permission_timestamp_checks=2*len(permission),selected=selected)
    (BASE/'shared/late_independent_audit.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2))
if __name__=='__main__':run()
