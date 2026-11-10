"""Freeze causal six-feature morning candidates for research-only G14 replay.

The selection feature vector is derived at the completed 5-minute signal. The
next-minute confirmation OHLC is carried solely for later execution replay and
is never part of ``integrity_ok`` or any of the six selection features.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / 'g3_indicator_lab_20261103/inputs'
EXPANDED = ROOT / 'g3_indicator_lab_20261103/expanded_slots'
CONFIG = ROOT / 'configs/strategies/v13_v10_g_14_six_feature_v2.json'
SOURCE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/six_feature_source')
SLOTS = ('0920', '0925', '0930', '0935', '0940', '0945')
FEATURES = ('open_gap_pct', 'pdc_pct', 'price_5m_pct', 'oi', 'oi_delta_5m_pct', 'oi_day_pct')


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str, allow_nan=False) + '\n', encoding='utf-8')


def slot_values(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(':', '', regex=False).str.zfill(4)


def config_setups() -> dict[tuple[str, str], dict]:
    source = json.loads(CONFIG.read_text(encoding='utf-8'))
    result = {}
    for setup in source['active_setups']:
        slot = setup['signal_end'].replace(':', '')
        side = setup['side']
        if slot not in SLOTS or side not in ('LONG', 'SHORT'):
            raise ValueError(f'Unexpected active setup {slot}/{side}')
        key = slot, side
        if key in result:
            raise ValueError(f'Duplicate setup {key}')
        result[key] = setup
    if len(result) != 12:
        raise ValueError('The source V14 setup inventory must have exactly 12 entries')
    return result


def load_day(day: str) -> tuple[pd.DataFrame, list[dict]]:
    raw_path = INPUTS / day / 'raw.parquet'
    raw = pd.read_parquet(raw_path)
    raw['slot'] = slot_values(raw['hhmm'])
    raw = raw.loc[raw['slot'].isin(SLOTS)].copy()
    raw['source_kind'] = 'FROZEN_RAW'
    proofs = [dict(day=day, kind='FROZEN_RAW', path=str(raw_path), sha256=sha256(raw_path))]
    expected_cohort = set(raw.loc[raw.slot.eq('0920'), 'tradingsymbol'])
    if not expected_cohort:
        raise ValueError(f'Missing opening cohort on {day}')

    # Four late snapshot dates lack 09:40/09:45 in the native raw frame. Their
    # independently archived, exact 5m supplements fill those holes only.
    absent_slots = [slot for slot in SLOTS if not raw.slot.eq(slot).any()]
    if absent_slots:
        supplement_path = EXPANDED / f'{day}.parquet'
        if not supplement_path.is_file():
            raise ValueError(f'Missing frozen expanded slots for {day}: {absent_slots}')
        supplement = pd.read_parquet(supplement_path)
        supplement['slot'] = slot_values(supplement['hhmm'])
        supplement = supplement.loc[supplement.slot.isin(absent_slots)].copy()
        supplement['source_kind'] = 'FROZEN_EXPANDED'
        proofs.append(dict(day=day, kind='FROZEN_EXPANDED', path=str(supplement_path), sha256=sha256(supplement_path), slots=absent_slots))
        raw = pd.concat([raw, supplement], ignore_index=True, sort=False)
    observed = raw.groupby(['tradingsymbol', 'slot'], observed=True).size()
    if not observed.eq(1).all() or len(observed) != len(expected_cohort) * 6:
        raise ValueError(f'Incomplete or duplicate candidate grid on {day}; observed {len(observed)}')
    if set(raw.tradingsymbol) != expected_cohort:
        raise ValueError(f'Supplement stock cohort differs from opening on {day}')
    raw['hhmm'] = raw['slot']
    return raw, proofs


def add_reason(frame: pd.DataFrame, mask, reason: str) -> None:
    bad = np.asarray(mask, dtype=bool)
    frame.loc[bad, 'integrity_reason'] += reason + ';'


def prepare(output: Path = SOURCE) -> dict:
    output = output.resolve()
    if output != SOURCE.resolve():
        raise ValueError('Only the isolated G14 six-feature source directory is supported')
    pdc_path = output / 'pdc.csv'
    source_manifest_path = output / 'source_manifest.json'
    if not pdc_path.is_file() or not source_manifest_path.is_file():
        raise ValueError('Verified official NSE PDC bundle is required first')
    frozen_path = INPUTS / 'manifest.json'
    days = json.loads(frozen_path.read_text(encoding='utf-8'))['days']
    if len(days) != 63 or days != sorted(set(days)):
        raise ValueError('Frozen 63-day signal calendar changed')
    pdc = pd.read_csv(pdc_path, dtype={'day':str,'tradingsymbol':str,'prior_day':str,'source_sha256':str})
    required = {'day','tradingsymbol','pdc','prior_day','source_sha256'}
    if required - set(pdc) or pdc.duplicated(['day','tradingsymbol']).any():
        raise ValueError('PDC CSV schema or uniqueness changed')
    official = json.loads(source_manifest_path.read_text(encoding='utf-8'))
    verified = {row['day']:row['sha256'] for row in official['fetches'] if row['status'] == 'VERIFIED_OFFICIAL_EQ'}
    seen = pdc[pdc.pdc.notna()].copy()
    if ((seen.prior_day >= seen.day) | seen.prior_day.isna() |
        seen.apply(lambda r: verified.get(r.prior_day) != r.source_sha256, axis=1)).any():
        raise ValueError('PDC previous-session or official ZIP hash integrity failed')
    setups = config_setups()
    all_frames = []
    raw_proofs = []
    per_day_counts = []
    output_dir = output / 'candidates_by_day'
    output_dir.mkdir(exist_ok=True)
    for day in days:
        frame, proofs = load_day(day)
        raw_proofs.extend(proofs)
        frame['day'] = day
        frame['tradingsymbol'] = frame.tradingsymbol.astype(str)
        frame['slot'] = frame.slot.astype(str)
        frame['source_sid'] = frame['sid'] if 'sid' in frame else np.nan
        frame = frame.merge(pdc[list(required)], on=['day','tradingsymbol'], how='left', validate='many_to_one')
        opening = frame.loc[frame.slot.eq('0920'), ['tradingsymbol','open','oi','contract_month','futures_tradingsymbol']].copy()
        opening = opening.rename(columns={'open':'cash_open_0915','oi':'opening_oi_0920',
            'contract_month':'opening_contract_month','futures_tradingsymbol':'opening_futures_symbol'})
        frame = frame.merge(opening, on='tradingsymbol', how='left', validate='many_to_one')
        for col in ('open','close','prev_close','oi','prev_oi','pdc','cash_open_0915','opening_oi_0920',
                    'confirmation_high','confirmation_low'):
            frame[col] = pd.to_numeric(frame[col], errors='coerce')
        first = frame.slot.eq('0920')
        with np.errstate(divide='ignore', invalid='ignore'):
            frame['open_gap_pct'] = 100.0 * (frame.cash_open_0915 / frame.pdc - 1.0)
            frame['pdc_pct'] = 100.0 * (frame.close / frame.pdc - 1.0)
            base_price = frame.prev_close.where(~first, frame.cash_open_0915)
            frame['price_5m_pct'] = 100.0 * (frame.close / base_price - 1.0)
            frame['oi_delta_5m_pct'] = 100.0 * (frame.oi / frame.prev_oi - 1.0)
            frame['oi_day_pct'] = 100.0 * (frame.oi / frame.opening_oi_0920 - 1.0)
        frame['oi_delta_window'] = np.where(first, 'OPEN_4M_0916_TO_0920', 'EXACT_PRIOR_5M')
        frame = frame.sort_values(['tradingsymbol','slot'], kind='stable').reset_index(drop=True)
        first = frame.slot.eq('0920')
        base_price = frame.prev_close.where(~first, frame.cash_open_0915)
        prior_observed_oi = frame.groupby('tradingsymbol', sort=False).oi.shift(1)
        prior_observed_close = frame.groupby('tradingsymbol', sort=False).close.shift(1)
        signal_ts = pd.to_datetime(frame.signal_ts, utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
        confirm_ts = pd.to_datetime(frame.confirmation_ts, utc=True, errors='coerce').dt.tz_convert('Asia/Kolkata')
        expected_signal = pd.to_datetime(day + ' ' + frame.slot.str.slice(0,2) + ':' + frame.slot.str.slice(2,4)).dt.tz_localize('Asia/Kolkata')
        frame['integrity_reason'] = ''
        add_reason(frame, signal_ts.ne(expected_signal), 'SIGNAL_TIMESTAMP')
        add_reason(frame, confirm_ts.ne(expected_signal + pd.Timedelta(minutes=1)), 'CONFIRMATION_TIMESTAMP_METADATA')
        add_reason(frame, frame.pdc.isna() | frame.pdc.le(0), 'OFFICIAL_PDC_MISSING')
        add_reason(frame, frame.cash_open_0915.isna() | frame.cash_open_0915.le(0), 'OPENING_CASH_OPEN_MISSING')
        add_reason(frame, frame.close.isna() | frame.close.le(0) | base_price.isna() | base_price.le(0), 'SIGNAL_PRICE_INVALID')
        add_reason(frame, frame.oi.isna() | frame.oi.le(0) | frame.prev_oi.isna() | frame.prev_oi.le(0), 'SIGNAL_OI_INVALID')
        add_reason(frame, frame.opening_oi_0920.isna() | frame.opening_oi_0920.le(0), 'OPENING_OI_BASELINE_MISSING')
        add_reason(frame, frame.contract_month.ne(frame.opening_contract_month) |
            frame.futures_tradingsymbol.ne(frame.opening_futures_symbol), 'OI_CONTRACT_CHANGED')
        add_reason(frame, ~first & ~np.isclose(frame.prev_oi, prior_observed_oi, rtol=0, atol=1e-6, equal_nan=False), 'PRIOR_OI_BAR_MISMATCH')
        add_reason(frame, ~first & ~np.isclose(frame.prev_close, prior_observed_close, rtol=0, atol=0.02, equal_nan=False), 'PRIOR_PRICE_BAR_MISMATCH')
        if 'supplement_exact_prior_oi_bar' in frame:
            expanded_bad = frame.source_kind.eq('FROZEN_EXPANDED') & frame.supplement_exact_prior_oi_bar.ne(True)
            add_reason(frame, expanded_bad, 'SUPPLEMENT_PRIOR_OI_NOT_EXACT')
        if 'opening_0920_status' in frame:
            add_reason(frame, first & frame.opening_0920_status.ne('VALID'), 'OPENING_4M_STATUS_INVALID')
        if 'source_1m_count' in frame:
            add_reason(frame, frame.source_1m_count.ne(5), 'CASH_BAR_NOT_FIVE_MINUTES')
        if 'gap_filled' in frame:
            add_reason(frame, pd.to_numeric(frame.gap_filled, errors='coerce').fillna(0).gt(0), 'CASH_BAR_GAP_FILLED')
        if 'provisional_stale' in frame:
            add_reason(frame, pd.to_numeric(frame.provisional_stale, errors='coerce').fillna(0).gt(0), 'SIGNAL_STALE')
        add_reason(frame, ~frame.price_source.astype(str).str.startswith('NSE_EQUITY_1M'), 'NON_CASH_PRICE_SOURCE')
        add_reason(frame, frame.oi_source.ne('NFO_FUTURE'), 'NON_FUTURES_OI_SOURCE')
        finite_features = np.isfinite(frame[list(FEATURES)].to_numpy(dtype=float)).all(axis=1)
        add_reason(frame, ~finite_features, 'SIX_FEATURE_NONFINITE')
        frame['side'] = np.select([frame.price_5m_pct.gt(0),frame.price_5m_pct.lt(0)], ['LONG','SHORT'], default='NONE')
        frame['direction'] = frame.side
        add_reason(frame, frame.side.eq('NONE'), 'ZERO_PRICE_MOVE')
        frame['setup_id'] = [setups.get((slot,side),{}).get('setup_id') for slot,side in zip(frame.slot,frame.side)]
        frame['native_stop_pct'] = [setups.get((slot,side),{}).get('stop_pct') for slot,side in zip(frame.slot,frame.side)]
        frame['native_target_pct'] = [setups.get((slot,side),{}).get('target_pct') for slot,side in zip(frame.slot,frame.side)]
        frame['integrity_ok'] = frame.integrity_reason.eq('')
        # Confirmation is future to the feature clock. It cannot filter the
        # ranked signal candidates. Replay can evaluate it one minute later.
        frame['execution_ready'] = (np.isfinite(frame.confirmation_high) &
            np.isfinite(frame.confirmation_low) & frame.confirmation_high.ge(frame.confirmation_low) &
            frame.confirmation_low.gt(0))
        frame['trigger'] = np.where(frame.side.eq('LONG'), frame.confirmation_high,
            np.where(frame.side.eq('SHORT'), frame.confirmation_low, np.nan))
        frame['signal_ts'] = signal_ts
        frame['confirmation_ts'] = confirm_ts
        if 'v9_exact_confirmation_present' in frame:
            frame['execution_ready'] &= frame.v9_exact_confirmation_present.eq(True)
        per_day_counts.append(dict(day=day, rows=len(frame), valid=int(frame.integrity_ok.sum()),
            execution_ready=int(frame.execution_ready.sum()), opening_4m_proxy_rows=int(first.sum()),
            expanded_rows=int(frame.source_kind.eq('FROZEN_EXPANDED').sum())))
        all_frames.append(frame)
    full = pd.concat(all_frames, ignore_index=True, sort=False)
    full = full.sort_values(['day','tradingsymbol','slot'], kind='stable').reset_index(drop=True)
    full['sid'] = np.arange(1, len(full)+1, dtype=np.int64)
    if full.duplicated(['day','tradingsymbol','slot']).any() or full.sid.duplicated().any():
        raise ValueError('Candidate key uniqueness failed')
    for day, piece in full.groupby('day', sort=True):
        piece.to_parquet(output_dir / f'{day}.parquet', index=False, compression='zstd')
    full_path = output / 'candidates.parquet'
    full.to_parquet(full_path, index=False, compression='zstd')
    reason_counts = full.integrity_reason.str.rstrip(';').str.split(';').explode().value_counts().to_dict()
    reason_counts.pop('', None)
    proof = dict(scope='RESEARCH_ONLY', execution_authority=False, days=len(days), candidates=len(full),
        integrity_valid=int(full.integrity_ok.sum()), execution_ready=int(full.execution_ready.sum()),
        features=list(FEATURES), feature_rule='All six features are as-of completed 5m signal; 09:20 OI delta is the archived 09:16→09:20 4m proxy.',
        oi_day_baseline='First completed 09:20 OI of the same futures contract; 09:20 OI-day=0; missing baseline fails closed.',
        pdc_rule='Previous completed NSE official EQ ClsPric. Never current-day final PrvsClsgPric.',
        selection_integrity_excludes_future_confirmation=True,
        source_input_manifest_sha256=sha256(frozen_path), source_pdc_csv_sha256=sha256(pdc_path),
        source_pdc_manifest_sha256=sha256(source_manifest_path), source_v14_config_sha256=sha256(CONFIG),
        source_files=raw_proofs, by_day=per_day_counts, integrity_reasons=reason_counts,
        candidates_sha256=sha256(full_path), per_day_sha256={p.name:sha256(p) for p in sorted(output_dir.glob('*.parquet'))})
    dump_json(output / 'candidate_manifest.json', proof)
    dump_json(output / 'manifest.json', proof)
    return proof


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=SOURCE)
    args = parser.parse_args()
    report = prepare(args.output)
    print(json.dumps({k:report[k] for k in ('days','candidates','integrity_valid','execution_ready','integrity_reasons')}, indent=2))
