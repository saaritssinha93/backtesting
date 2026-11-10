"""Guard the source-extension boundaries rather than historical P&L targets."""
from pathlib import Path
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from g3_v23_adx_rsi_20261106.through_20261105 import build_nov5 as builder


def fixture_source():
    stamp = pd.to_datetime([builder.DAY + ' ' + clock for clock in builder.source.CLOCKS]).tz_localize('Asia/Kolkata')
    raw = pd.DataFrame(dict(day=builder.DAY, tradingsymbol='TEST', signal_ts=stamp))
    minutes = pd.DataFrame(dict(ts=pd.date_range(builder.DAY+' 09:16', builder.DAY+' 15:15', freq='min', tz='Asia/Kolkata'),
                                tradingsymbol='TEST', open=100., high=100., low=100., close=100., volume=10))
    proof = dict(loader_source='HASH_VERIFIED_SEALED_DAILY_FEATURE_LEDGER_AND_RAW_SNAPSHOT',
                 v21_expansion=dict(missing_append_rows=0, through_clock='14:00'))
    for name in ('run_manifest', 'source_manifest', 'snapshot_manifest'):
        proof[name] = str(Path(name))
        proof[name+'_sha256'] = 'testdigest'
    return raw, minutes, proof


def test_full_source_and_empty_candidate_day_are_distinct():
    raw, minutes, proof = fixture_source()
    with patch.object(builder, 'sha', return_value='testdigest'):
        result = builder.validate_source(raw, minutes, proof)
    assert result['complete_session'] is True
    assert result['minute_rows'] == 360
    audit, indicator_proof = builder.feature_audit(pd.DataFrame())
    assert audit.empty and 'all_g7_allowed' in audit and indicator_proof['stock'] == []
    assert builder.v23.masks(audit)['baseline_v21_g7'].empty


def test_missing_minute_does_not_become_a_complete_day():
    raw, minutes, proof = fixture_source()
    with patch.object(builder, 'sha', return_value='testdigest'), pytest.raises(ValueError, match='Incomplete/duplicate execution minutes'):
        builder.validate_source(raw, minutes.iloc[:-1], proof)


def test_incomplete_last_slot_and_changed_source_are_rejected():
    raw, minutes, proof = fixture_source()
    with patch.object(builder, 'sha', return_value='testdigest'), pytest.raises(ValueError, match='Incomplete feature clock grid'):
        builder.validate_source(raw.iloc[:-1], minutes, proof)
    with patch.object(builder, 'sha', return_value='changed'), pytest.raises(ValueError, match='Sealed source proof changed'):
        builder.validate_source(raw, minutes, proof)


def test_output_cannot_escape_or_overwrite(tmp_path):
    with pytest.raises(ValueError, match='fresh isolated'):
        builder.run(tmp_path)
    with pytest.raises(ValueError, match='fresh isolated'):
        builder.run(builder.PACKAGE)


def test_native_date_objects_join_the_serialized_feature_audit():
    stamp = builder.DAY + 'T11:01:00+05:30'
    gate = pd.DataFrame([dict(sid=65000001, day=date.fromisoformat(builder.DAY),
                             setup_id='1101_LONG', tradingsymbol='TEST', side='LONG',
                             confirmation_ts=stamp, all_g7_entry_allowed=True,
                             rsi14=60., signed_rsi_delta3=1., rsi_ready=True,
                             feature_available_at=stamp)])
    adx = gate[['sid', 'day', 'setup_id', 'tradingsymbol', 'side', 'confirmation_ts']].copy()
    adx['day'] = builder.DAY
    for key,value in dict(adx14=25., plus_di14=30., minus_di14=10., adx_delta3=1.,
                          adx_ready=True, adx_delta3_ready=True, adx_available_at=stamp,
                          adx_session_bars=106, adx_stock_source_sha256='digest').items():
        adx[key] = value
    with patch.object(builder,'build_stock_audit',return_value=(pd.DataFrame(), [])), \
         patch.object(builder,'build_nifty_audit',return_value=(pd.DataFrame(), [])), \
         patch.object(builder.g7,'decisions',return_value=gate), \
         patch.object(builder,'build_adx_audit',return_value=(adx, [])):
        result,_ = builder.feature_audit(pd.DataFrame([dict(sid=65000001)]))
    assert result.day.iloc[0] == builder.DAY
    assert bool(result.all_g7_allowed.iloc[0]) is True
