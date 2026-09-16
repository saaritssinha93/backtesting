import pytest

import fno_v13_v10_b_backtest as b


@pytest.mark.parametrize('old,expected', [
    ({'stop_pct':.21,'target_pct':.34}, {'stop_pct':.6,'target_pct':.97}),
    ({'stop_pct':.45,'target_pct':2.}, {'stop_pct':.6,'target_pct':2.67}),
    ({'stop_pct':.39,'target_pct':1.58}, {'stop_pct':.6,'target_pct':2.43}),
    ({'stop_pct':.27,'target_pct':.42}, {'stop_pct':.6,'target_pct':.93}),
    ({'stop_pct':.16,'target_pct':.24}, {'stop_pct':.6,'target_pct':.9}),
    ({'stop_pct':.6,'target_pct':1.73}, {'stop_pct':.6,'target_pct':1.73}),
    ({'stop_pct':.82,'target_pct':1.23}, {'stop_pct':.82,'target_pct':1.23}),
])
def test_requested_pair_transformation(old, expected):
    assert b.adjusted_pair(old) == expected


def test_transformed_config_has_full_exit_valid_pairs_and_minimum_stop():
    source = {'default': {'stop_pct':.4,'target_pct':.8},
              'setups': {'0926_LONG': {'stop_pct':.2,'target_pct':.3},
                         '0931_LONG': {'stop_pct':.8,'target_pct':1.2}}}
    result = b.transform(source)
    assert result['version'] == 'V13-v10-B'
    assert not result['partial_exits'] and not result['breakeven_stop']
    for pair in [result['default'], *result['setups'].values()]:
        assert pair['stop_pct'] >= .6
        assert pair['target_pct'] <= 3
        assert pair['target_pct'] / pair['stop_pct'] >= 1.5 - 1e-12
        b.validate_pair(pair)


@pytest.mark.parametrize('pair', [
    {'stop_pct':.59,'target_pct':1.}, {'stop_pct':1.51,'target_pct':3.},
    {'stop_pct':.6,'target_pct':3.01}, {'stop_pct':.6,'target_pct':.89},
])
def test_v10_b_contract_rejects_out_of_bounds(pair):
    with pytest.raises(ValueError):
        b.validate_pair(pair)
