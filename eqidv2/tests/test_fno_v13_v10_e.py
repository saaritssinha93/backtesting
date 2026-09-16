import copy

import pytest

import fno_v13_v10_e_backtest as e


def source_exit():
    return {
        'version': 'V13-v10-B',
        'default': {'stop_pct': .82, 'target_pct': 1.23},
        'setups': {
            '0926_LONG': {'stop_pct': .6, 'target_pct': .97},
            '0941_LONG': {'stop_pct': .6, 'target_pct': 3.0},
        },
        'partial_exits': False,
        'breakeven_stop': False,
    }


def test_e_preserves_b_exits_and_adds_volume_filter():
    original = source_exit()
    snapshot = copy.deepcopy(original)
    result = e.config(original, 1.2)
    assert original == snapshot
    assert result['version'] == 'V13-v10-E'
    assert result['source_version'] == 'V13-v10-B'
    assert result['minimum_confirmation_1m_volume_ratio'] == 1.2
    assert result['exit'] == original
    assert result['exit']['setups']['0941_LONG']['target_pct'] == 3.0
    assert result['entry_expiry_minutes'] == 10


def test_e_does_not_force_b_targets_to_twice_stop():
    result = e.config(source_exit(), 1.2)
    pair = result['exit']['setups']['0941_LONG']
    assert pair['target_pct'] != 2 * pair['stop_pct']


def test_e_rejects_invalid_volume_threshold():
    with pytest.raises(ValueError, match='volume ratio'):
        e.config(source_exit(), 0)


def test_e_rejects_non_ten_minute_entry_expiry_during_evaluation():
    value = e.config(source_exit())
    value['entry_expiry_minutes'] = 5
    with pytest.raises(ValueError, match='ten-minute'):
        e.evaluate({}, value)
