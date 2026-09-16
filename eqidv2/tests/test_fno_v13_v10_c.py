import pytest

import fno_v13_v10_c_backtest as c


@pytest.mark.parametrize('pair,expected',[({'stop_pct':.6,'target_pct':3.},{'stop_pct':.6,'target_pct':1.2}),
    ({'stop_pct':.85,'target_pct':2.},{'stop_pct':.85,'target_pct':1.7}),
    ({'stop_pct':.89,'target_pct':2.},{'stop_pct':.89,'target_pct':1.78}),
    ({'stop_pct':.82,'target_pct':1.23},{'stop_pct':.82,'target_pct':1.64}),
    ({'stop_pct':.62,'target_pct':2.},{'stop_pct':.62,'target_pct':1.24})])
def test_target_is_twice_unchanged_stop(pair,expected):
    assert c.adjusted_pair(pair)==expected


def test_complete_transform_preserves_every_stop_and_sets_exact_two_to_one():
    source={'default':{'stop_pct':.82,'target_pct':1.23},'setups':{
        '0926_LONG':{'stop_pct':.6,'target_pct':.97},'0931_SHORT':{'stop_pct':.89,'target_pct':2.}}}
    result=c.transform(source)
    assert result['version']=='V13-v10-C'
    assert not result['partial_exits'] and not result['breakeven_stop']
    assert result['default']['stop_pct']==source['default']['stop_pct']
    for name,pair in result['setups'].items():
        assert pair['stop_pct']==source['setups'][name]['stop_pct']
        assert pair['target_pct']==pytest.approx(2*pair['stop_pct'])
        c.b.validate_pair(pair)
