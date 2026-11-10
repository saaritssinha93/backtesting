"""Isolated, date-bound Muhurat PAPER simulation and after-session replay.

No broker client, shared live runtime, order executor or shared runtime writer
is imported. Actual exchange times stay in evidence and ledger records. Only
the pure donor admission call receives explicitly recorded logical clocks.
"""
from __future__ import annotations

import copy
from dataclasses import asdict
from datetime import date
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_3_contract as donor
import fno_v13_v10_g_3_cutoff_production as friday

DAY = date(2026, 11, 8)
IST = 'Asia/Kolkata'
OPEN = '14:20'
SQUARE_OFF = '15:10'
SCHEMA = 'muhurat_20261108_paper_v1'
MINUTE = pd.Timedelta(minutes=1)
# User-approved mapping: actual 14:25-15:05 borrows 09:25-10:05 in order.
SHIFT = pd.Timedelta(minutes=300)


def stamp(value):
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError('A timezone-aware timestamp is required')
    return result.tz_convert(IST)


def at(clock):
    return pd.Timestamp(f'{DAY} {clock}', tz=IST)


def _json(value):
    if isinstance(value, dict):
        return {str(k): _json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(v) for v in value]
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    if isinstance(value, (np.integer, np.bool_)):
        return value.item()
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(value) else None
    if value is pd.NaT or value is pd.NA:
        return None
    return value


def build_plan():
    """Freeze all18 special pairs from the61-rule donor book, without edits."""
    pairs = []
    active = {s.setup_id: s for s in friday.ACTIVE_SETUPS}
    for number in range(9):
        signal = at('14:25') + number * 5 * MINUTE
        logical = (signal - SHIFT).strftime('%H:%M')
        for side in ('LONG', 'SHORT'):
            eligible = [s for s in donor.ACTIVE_SETUPS if s.side == side and s.signal_end <= logical]
            spec = max(eligible, key=lambda s: s.signal_end)
            # Retain the exact Friday active setup when this logical pair was
            # active. Otherwise explicitly enable its existing donor rule.
            same_clock = spec.signal_end == logical
            inherited = same_clock and spec.setup_id in active
            if inherited:
                spec = active[spec.setup_id]
            pairs.append(dict(actual_signal_end=signal.strftime('%H:%M'),
                actual_confirmation_end=(signal + MINUTE).strftime('%H:%M'),
                actual_setup_id=f'{(signal+MINUTE).strftime("%H%M")}_{side}',
                logical_signal_end=logical, donor_setup_id=spec.setup_id,
                enabled_friday=inherited,
                admission_policy='FRIDAY_PRODUCTION_PURE_FILTERS_AND_RANKING' if inherited else 'ORIGINAL_61_RULE_DONOR_PURE_FILTERS_AND_RANKING',
                mapping='INHERITED_ACTIVE_FRIDAY' if inherited else
                        'ENABLED_EXISTING_DONOR' if same_clock else 'NEAREST_EARLIER_SAME_SIDE_DONOR',
                donor=asdict(spec),
                minimum_confirmation_volume=donor.confirmation_volume_minimum(side, setup_id=spec.setup_id)))
    value = dict(schema=SCHEMA, session_date=str(DAY), mode='PAPER', execution_authority=False,
        expires_after=str(DAY), source_profile=friday.STRATEGY_VERSION,
        source_profile_fingerprint=friday.strategy_fingerprint(), donor_fingerprint=donor.strategy_fingerprint(),
        source_code_sha256={str(Path(m.__file__).resolve()): hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
                            for m in (donor, friday)},
        open=OPEN, first_minute_oi_end='14:21', square_off=SQUARE_OFF,
        broker_cash_fno_mis_cutoff='15:12', signal_clocks=[(at('14:25')+i*5*MINUTE).strftime('%H:%M') for i in range(9)],
        pairs=pairs, inherited_pairs=sum(p['enabled_friday'] for p in pairs), total_pairs=18,
        initial_stop_pct=donor.INITIAL_STOP_PCT, native_targets_retained=True,
        tighten_after_minutes=donor.TIGHTEN_AFTER_MINUTES,
        note='The short session ends before the normal120-minute stop tightening can activate. V23 breakeven is not promoted into this Friday-rule simulation.',
        capital_per_entry_rs=donor.CAPITAL_PER_ENTRY_RS, leverage=donor.LEVERAGE,
        portfolio_capital_rs=donor.PORTFOLIO_CAPITAL_RS, one_open_position_per_symbol=True,
        cost_bps=donor.ROUND_TRIP_COST_BPS, entry_expiry_minutes=donor.ENTRY_EXPIRY_MINUTES,
        publication_grace_seconds=90, completed_boundary_buffer_seconds=3,
        paper_fill_rule='First eligible full one-minute bar starts at/after ceiling(actual first_seen). No retrospective new fills; missing entry bars are unresolved.',
        replay_fill_rule='First full bar starts at confirmation close; available historical source only. Replay is not prospective PAPER.',
        warmup='Earlier actual sessions are used only for causal indicator and volume warm-up; no pre-open current-day samples or manufactured candles.')
    value['fingerprint'] = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return value


def _frame(raw, asof, fields):
    if raw is None or not isinstance(raw, pd.DataFrame) or raw.empty:
        return pd.DataFrame(columns=['ts', *fields])
    out = raw.copy()
    col = 'ts' if 'ts' in out else 'date' if 'date' in out else 'timestamp'
    out['ts'] = pd.to_datetime(out[col], errors='coerce', utc=True).dt.tz_convert(IST)
    out = out.loc[out.ts.notna() & out.ts.le(asof)].copy()
    current = out.ts.dt.date.eq(DAY)
    out = out.loc[~current | out.ts.gt(at(OPEN))].copy()
    for field in fields:
        if field not in out:
            return pd.DataFrame(columns=['ts', *fields])
        out[field] = pd.to_numeric(out[field], errors='coerce')
    valid = np.isfinite(out[list(fields)]).all(axis=1)
    if {'open', 'high', 'low', 'close'}.issubset(fields):
        valid &= out[['open', 'high', 'low', 'close']].gt(0).all(axis=1)
        valid &= out.high.ge(out[['open', 'close']].max(axis=1)) & out.low.le(out[['open', 'close']].min(axis=1))
    if 'volume' in fields:
        valid &= out.volume.ge(0)
    for flag in ('gap_filled', 'opening_snapshot', 'provisional_stale'):
        if flag in out:
            valid &= ~out[flag].astype(str).str.lower().isin(['true', '1', '1.0', 'yes'])
    out = out.loc[valid].sort_values('ts')
    # Conflicting duplicate source bars are missing evidence, not last-write wins.
    dup = out.ts.duplicated(keep=False)
    conflicts = set()
    for ts, group in out.loc[dup].groupby('ts'):
        if len(group[list(fields)].drop_duplicates()) != 1:
            conflicts.add(ts)
    return out.loc[~out.ts.isin(conflicts)].drop_duplicates('ts').reset_index(drop=True)


def _features(minute):
    """Exact five-minute groups and causal prefixes, retaining actual clocks."""
    if minute.empty:
        return pd.DataFrame(), minute
    frame = minute.copy()
    frame['end'] = frame.ts.dt.ceil('5min')
    five = frame.groupby('end', as_index=False).agg(open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'), volume=('volume', 'sum'),
        count=('ts', 'size'), first=('ts', 'first'), last=('ts', 'last'))
    five = five.loc[five['count'].eq(5) & five['first'].eq(five.end-4*MINUTE) & five['last'].eq(five.end)].copy()
    for span in (9, 20, 50):
        five['ema'+str(span)] = five.close.ewm(span=span, adjust=False).mean()
    five['history_bars'] = np.arange(1, len(five)+1)
    five['prev_close'] = five.close.shift(1)
    five['price_change_pct'] = (five.close/five.prev_close-1)*100
    five['volume_ratio'] = five.volume/five.volume.shift(1).rolling(20, min_periods=5).mean()
    five['traded_value'] = five.close*five.volume
    frame['v9_1m_volume_ratio'] = frame.volume/frame.volume.shift(1).rolling(20, min_periods=5).mean()
    return five.set_index('end'), frame.set_index('ts')


def _universe_rows(frames):
    table = frames.get('universe', pd.DataFrame())
    if not isinstance(table, pd.DataFrame) or table.empty:
        return [], None
    rows, nifty = [], None
    for row in table.to_dict('records'):
        underlying = str(row.get('underlying', row.get('equity_symbol', ''))).upper()
        future = str(row.get('futures_tradingsymbol', row.get('tradingsymbol', '')))
        is_index = str(row.get('is_index_future', False)).lower() in ('true', '1', '1.0')
        if underlying == 'NIFTY':
            nifty = future
        if not is_index:
            symbol = str(row.get('equity_symbol', underlying))
            if symbol and future:
                rows.append(dict(symbol=symbol, future=future))
    if len({r['symbol'] for r in rows}) != len(rows):
        raise ValueError('Duplicate universe equity symbols')
    return rows, nifty


def _opening_oi(frames, symbol, future):
    record = frames.get('opening_oi', {}).get(symbol, frames.get('opening_oi', {}).get(future))
    if not isinstance(record, dict):
        return math.nan
    try:
        end = stamp(record.get('candle_end', record.get('timestamp')))
        start = stamp(record.get('candle_start'))
        contract = record.get('futures_tradingsymbol', record.get('contract', future))
        value = float(record['oi'])
        if start != at(OPEN) or end != at('14:21') or contract != future or value <= 0:
            return math.nan
        return value if math.isfinite(value) else math.nan
    except (ValueError, TypeError, KeyError):
        return math.nan


def _prepare(frames, asof):
    rows, nifty = _universe_rows(frames)
    cache = {}
    for row in rows:
        symbol, future = row['symbol'], row['future']
        try:
            minute = _frame(frames.get('equity_1m', {}).get(symbol), asof, ('open', 'high', 'low', 'close', 'volume'))
            five, minute = _features(minute)
            fut = _frame(frames.get('futures_5m', {}).get(future), asof, ('open', 'high', 'low', 'close', 'oi')).set_index('ts')
            cache[symbol] = dict(minute=minute, five=five, future=fut, contract=future,
                                 opening_oi=_opening_oi(frames, symbol, future))
        except (KeyError, ValueError, TypeError) as exc:
            cache[symbol] = dict(minute=pd.DataFrame(),five=pd.DataFrame(),future=pd.DataFrame(),contract=future,
                source_error=f'{type(exc).__name__}: {exc}',opening_oi=math.nan)
    nifty_return = math.nan
    if nifty:
        try:
            nf = _frame(frames.get('futures_5m', {}).get(nifty), asof, ('open', 'high', 'low', 'close', 'oi')).set_index('ts')
            if at('14:25') in nf.index:
                bar = nf.loc[at('14:25')]
                nifty_return = (float(bar.close)/float(bar.open)-1)*100
        except (KeyError, ValueError, TypeError):
            pass  # Missing NIFTY context blocks only its explicitly gated shorts.
    return cache, nifty_return


def _candidate(symbol, source, pair, nifty_return):
    if source.get('source_error'):
        return None, 'MALFORMED_STOCK_SOURCE'
    signal, confirmation = at(pair['actual_signal_end']), at(pair['actual_confirmation_end'])
    five, minutes, future = source['five'], source['minute'], source['future']
    if signal not in five.index or confirmation not in minutes.index:
        return None, 'MISSING_EXACT_CASH_SIGNAL_OR_CONFIRMATION'
    if signal not in future.index:
        return None, 'MISSING_EXACT_FUTURES_OI'
    bar, confirm, oi_bar = five.loc[signal], minutes.loc[confirmation], future.loc[signal]
    if int(bar.history_bars) < 50:
        return None, 'INSUFFICIENT_CAUSAL_EMA50_WARMUP'
    if signal == at('14:25'):
        prev_oi = source['opening_oi']
        price_change = (float(bar.close)/float(bar.open)-1)*100
    else:
        prior = signal-5*MINUTE
        if prior not in future.index or prior not in five.index:
            return None, 'MISSING_EXACT_PREVIOUS_FIVE_MINUTE'
        prev_oi = float(future.loc[prior].oi)
        price_change = (float(bar.close)/float(five.loc[prior].close)-1)*100
    oi = float(oi_bar.oi)
    if not all(math.isfinite(x) and x > 0 for x in (oi, prev_oi)):
        return None, 'MISSING_OR_INVALID_OPENING_OR_PREVIOUS_OI'
    o, hi, lo, close = [float(confirm[k]) for k in ('open', 'high', 'low', 'close')]
    if hi <= lo:
        return None, 'FLAT_CONFIRMATION'
    spec = donor.SetupSpec(**pair['donor'])
    logical_signal, logical_confirmation = at(spec.signal_end), at(spec.confirmation_end)
    side = spec.side
    if side=='SHORT' and spec.signal_end in donor.NIFTY_SHORT_SIGNAL_ENDS and not math.isfinite(nifty_return):
        return None, 'MISSING_REQUIRED_NIFTY_CONTEXT'
    candidate = dict(tradingsymbol=symbol, futures_tradingsymbol=source['contract'], side=side,
        signal_timestamp=logical_signal.isoformat(), confirmation_timestamp=logical_confirmation.isoformat(),
        v9_1m_feature_ts=logical_confirmation.isoformat(), v9_5m_feature_ts=logical_signal.isoformat(),
        v9_feature_available_ts=logical_confirmation.isoformat(), signal_end=spec.signal_end,
        signal_close=float(bar.close), ema9=float(bar.ema9), ema20=float(bar.ema20), ema50=float(bar.ema50),
        price_change_pct=price_change, oi=oi, prev_oi=prev_oi, oi_change_pct=(oi/prev_oi-1)*100,
        volume_ratio=float(bar.volume_ratio), traded_value=float(bar.traded_value),
        confirm_open=o, confirm_high=hi, confirm_low=lo, confirm_close=close,
        body_ratio=abs(close-o)/(hi-lo), wick_ratio=(hi-max(o,close) if side=='LONG' else min(o,close)-lo)/(hi-lo),
        v9_1m_volume_ratio=float(confirm.v9_1m_volume_ratio), source_1m_count=5,
        v9_exact_confirmation_present=True, nifty_first_bar_return_pct=nifty_return,
        trigger=hi if side=='LONG' else lo, opening_0920_status='VALID',
        price_change_basis='09:15_CASH_OPEN_TO_09:20_CASH_CLOSE',
        oi_change_source='FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M',
        actual_signal_timestamp=signal.isoformat(), actual_confirmation_timestamp=confirmation.isoformat(),
        actual_opening_price_basis='14:20_CASH_OPEN_TO_14:25_CASH_CLOSE', actual_opening_oi_basis='14:21_TO_14:25_FUTURES',
        actual_setup_id=pair['actual_setup_id'], donor_setup_id=pair['donor_setup_id'])
    policy = friday if pair['enabled_friday'] else donor
    passed = policy.passes_selected_filters(candidate, spec, session_date=DAY)
    return candidate, 'ELIGIBLE' if passed else 'DONOR_FILTER_REJECTED'


def _decision(cache, nifty_return, pair, observed, mode):
    audit, candidates, healthy = [], [], 0
    for symbol, source in cache.items():
        row, reason = _candidate(symbol, source, pair, nifty_return)
        healthy += row is not None
        audit_row=_json(row) if row is not None else {}
        audit_row.update(actual_setup_id=pair['actual_setup_id'], signal_end=pair['actual_signal_end'],
            confirmation_end=pair['actual_confirmation_end'], side=pair['donor']['side'], tradingsymbol=symbol,
                donor_setup_id=pair['donor_setup_id'], first_seen=observed.isoformat(), reason=reason)
        audit.append(audit_row)
        if reason == 'ELIGIBLE':
            candidates.append(row)
    policy = friday if pair['enabled_friday'] else donor
    selected = policy.rank_candidates(candidates, donor.SetupSpec(**pair['donor']), session_date=DAY)
    chosen = {r['tradingsymbol'] for r in selected}
    for row in audit:
        if row['reason'] == 'ELIGIBLE':
            row['reason'] = 'SELECTED' if row['tradingsymbol'] in chosen else 'SETUP_QUOTA'
    return dict(status='SEALED', healthy_symbols=healthy, first_seen=observed.isoformat(),
                audit=audit, candidates=[_json(r) for r in selected])


def _new_order(candidate, pair, observed, mode):
    confirm = at(pair['actual_confirmation_end'])
    first = observed.ceil('min') if mode == 'paper' else confirm
    identity = f'MUHURAT20261108_{pair["actual_setup_id"]}_{candidate["tradingsymbol"]}'
    return dict(signal_id=identity, session_date=str(DAY), mode=mode.upper(),
        actual_setup_id=pair['actual_setup_id'], donor_setup_id=pair['donor_setup_id'],
        tradingsymbol=candidate['tradingsymbol'], side=pair['donor']['side'],
        signal_ts=at(pair['actual_signal_end']).isoformat(), confirmation_ts=confirm.isoformat(),
        first_seen=observed.isoformat(), earliest_entry_bar_start=first.isoformat(),
        expiry_ts=min(confirm+donor.ENTRY_EXPIRY_MINUTES*MINUTE, at(SQUARE_OFF)).isoformat(),
        trigger_price=float(candidate['trigger']), initial_stop_pct=pair['donor']['stop_pct'],
        target_pct=pair['donor']['target_pct'], status='PENDING_ENTRY', filled=False,
        last_processed_bar_end=first.isoformat(), capital_rs=donor.CAPITAL_PER_ENTRY_RS,
        quantity=0, net_pnl_rs=0., data_incomplete=False)


def _close(order, price, when, reason):
    sign = 1 if order['side']=='LONG' else -1
    cost = order['quantity']*order['entry_price']*donor.ROUND_TRIP_COST_BPS/10000
    order.update(status='CLOSED', exit_price=float(price), exit_ts=when.isoformat(), exit_reason=reason,
        net_pnl_rs=sign*(price-order['entry_price'])*order['quantity']-cost,
        costs_rs=cost, data_incomplete=False)


def _advance(order, bar, end, all_orders):
    """Process one completed actual bar; never use quote/broker execution."""
    start = end-MINUTE
    if order['status'] not in ('PENDING_ENTRY', 'OPEN'):
        return
    if order['status']=='PENDING_ENTRY':
        if start < stamp(order['earliest_entry_bar_start']):
            return
        if start >= stamp(order['expiry_ts']) or end > at(SQUARE_OFF):
            order['status']='NO_FILL_EXPIRED'
            return
    if bar is None:
        order['data_incomplete']=True
        if order['status']=='PENDING_ENTRY':
            order.update(status='UNRESOLVED_ENTRY_DATA', status_reason='MISSING_COMPLETED_ENTRY_BAR_NO_RETROSPECTIVE_FILL')
        return
    op, hi, lo, close = [float(bar[k]) for k in ('open','high','low','close')]
    entered=False
    long=order['side']=='LONG'
    if order['status']=='PENDING_ENTRY':
        trigger=order['trigger_price']
        hit=hi>=trigger if long else lo<=trigger
        if hit:
            # A position exiting intrabar remains reserved for this bar: OHLC
            # cannot prove its exit happened before the new trigger. An exact
            # adverse-open exit can release capacity at that known open.
            opened=[x for x in all_orders.values() if x['signal_id']!=order['signal_id']
                and (x['status']=='OPEN' or (x.get('filled') and x.get('exit_ts')
                     and stamp(x['exit_ts'])>start))]
            conflict=any(x['tradingsymbol']==order['tradingsymbol'] for x in opened)
            capital=sum(x['capital_rs'] for x in opened)+order['capital_rs']
            if conflict or capital>donor.PORTFOLIO_CAPITAL_RS+1e-9:
                order.update(status='BLOCKED_PORTFOLIO',status_reason='SAME_SYMBOL_OPEN' if conflict else 'CAPITAL_LIMIT')
                return
            price=max(op,trigger) if long else min(op,trigger)
            quantity=int(math.floor(donor.CAPITAL_PER_ENTRY_RS*donor.LEVERAGE/price))
            if quantity<=0:
                order['status']='BLOCKED_QUANTITY'
                return
            sign=1 if long else -1
            at_open=op>=trigger if long else op<=trigger
            order.update(status='OPEN',filled=True,entry_price=price,quantity=quantity,
                entry_ts=(start if at_open else end).isoformat(),entry_bar_end=end.isoformat(),
                entry_time_precision='BAR_OPEN' if at_open else 'INTRABAR_RECORDED_AT_BAR_END',
                stop_price=price*(1-sign*order['initial_stop_pct']/100),
                target_price=price*(1+sign*order['target_pct']/100))
            entered=True
    if order['status']=='OPEN':
        stop,target=order['stop_price'],order['target_price']
        stop_hit=lo<=stop if long else hi>=stop
        target_hit=hi>=target if long else lo<=target
        gap=not entered and (op<=stop if long else op>=stop)
        if stop_hit:
            _close(order,op if gap else stop,start if gap else end,'STOP')
            order['same_bar_stop_target_ambiguous']=bool(target_hit and not gap)
        elif target_hit:
            _close(order,target,end,'TARGET')
        elif end==at(SQUARE_OFF):
            _close(order,close,end,'TIME_EXIT_1510')
        else:
            sign=1 if long else -1
            cost=order['quantity']*order['entry_price']*donor.ROUND_TRIP_COST_BPS/10000
            order.update(last_price=close,last_price_ts=end.isoformat(),
                unrealized_net_pnl_rs=sign*(close-order['entry_price'])*order['quantity']-cost,
                data_incomplete=False)
    order['last_processed_bar_end']=end.isoformat()


def _process(state, cache, asof, watermark=None):
    due=min(asof.floor('min'),at(SQUARE_OFF),stamp(watermark) if watermark is not None else asof.floor('min'))
    previous=stamp(state['processed_through'])
    orders=state['orders']
    def process_bar(order,end):
        minute=cache.get(order['tradingsymbol'],{}).get('minute',pd.DataFrame())
        bar=minute.loc[end] if end in minute.index else None
        latest=minute.index.max() if len(minute) else None
        # A just-ended bar may not have been requested or published yet. Its
        # absence is not proof of an internal source gap. Keep the standing
        # simulated order waiting; its decision time remains immutable.
        if bar is None and (latest is None or latest<end) and asof<end+pd.Timedelta(seconds=90):
            order.update(waiting_for_data=True,waiting_bar_end=end.isoformat())
            return False
        was_filled=order['filled']
        _advance(order,bar,end,orders)
        order['waiting_for_data']=False
        if not was_filled and order['filled']:
            order['fill_observed_at']=asof.isoformat()
            order['fill_data_delay_seconds']=max(0.,(asof-end).total_seconds())
        return bar is not None

    # Completed data can arrive after its request cycle. Only orders already
    # admitted before the eligible bar can catch up; decisions are never added
    # retrospectively, and internal missing entry bars are unresolved.
    for order in orders.values():
        if order['status'] not in ('PENDING_ENTRY','OPEN'):
            continue
        end=stamp(order['last_processed_bar_end'])+MINUTE
        while end<=min(previous,due):
            if not process_bar(order,end):
                break
            if order['status'] not in ('PENDING_ENTRY','OPEN'):
                break
            end+=MINUTE
    end=previous+MINUTE
    while end<=due:
        ordered=sorted(orders.values(),key=lambda o:(o['status']!='OPEN',o['first_seen'],o['signal_id']))
        for order in ordered:
            if order['status'] not in ('PENDING_ENTRY','OPEN'):
                continue
            if end<=stamp(order['last_processed_bar_end']):
                continue
            if stamp(order['last_processed_bar_end'])!=end-MINUTE:
                continue
            process_bar(order,end)
        end+=MINUTE
    state['processed_through']=max(previous,due).isoformat()
    for order in orders.values():
        if (order['status']=='PENDING_ENTRY' and asof>=stamp(order['expiry_ts'])
                and stamp(order['last_processed_bar_end'])>=stamp(order['expiry_ts'])):
            order['status']='NO_FILL_EXPIRED'


def evaluate(frames, asof, *, mode='paper', state=None):
    """Return DataFrame audit/candidates/trades/slots plus JSON summary/state."""
    observed=stamp(asof)
    if observed.date()!=DAY or mode not in ('paper','replay'):
        raise ValueError('Only PAPER/replay for2026-11-08 is supported; LIVE is unavailable')
    plan=build_plan()
    if state is None or mode=='replay':
        state=dict(schema=SCHEMA,fingerprint=plan['fingerprint'],session_date=str(DAY),mode=mode,
            decisions={},orders={},processed_through=at(OPEN).isoformat(),last_asof=at('00:00').isoformat())
    else:
        state=copy.deepcopy(state)
        if any(state.get(k)!=v for k,v in dict(schema=SCHEMA,fingerprint=plan['fingerprint'],session_date=str(DAY),mode=mode).items()):
            raise ValueError('Persisted PAPER state has a different date, mode or rule fingerprint')
        if observed<stamp(state['last_asof']):
            raise ValueError('PAPER clock cannot move backwards')
    watermark=frames.get('source_completed_through') if mode=='paper' else None
    if watermark is not None and stamp(watermark).date()!=DAY:
        raise ValueError('Source watermark escaped the special session date')
    cache,nifty=_prepare(frames,min(observed,stamp(watermark)) if watermark is not None else observed)
    for pair in plan['pairs']:
        key=pair['actual_setup_id']
        if key in state['decisions']:
            continue
        confirm=at(pair['actual_confirmation_end'])
        ready=confirm+pd.Timedelta(seconds=3 if mode=='paper' else 0)
        if observed<ready:
            continue
        if mode=='paper' and observed>confirm+pd.Timedelta(seconds=90):
            state['decisions'][key]=dict(status='MISSED_PUBLICATION_WINDOW',first_seen=observed.isoformat(),
                healthy_symbols=0,audit=[],candidates=[])
            continue
        first_seen=observed if mode=='paper' else confirm
        decision=_decision(cache,nifty,pair,first_seen,mode)
        if not decision['healthy_symbols'] and mode=='paper':
            continue
        if not decision['healthy_symbols']:
            decision['status']='SKIPPED_MISSING_SOURCE'
        state['decisions'][key]=decision
        for candidate in decision['candidates']:
            order=_new_order(candidate,pair,first_seen,mode)
            if order['signal_id'] in state['orders']:
                raise ValueError('Duplicate special-session signal identity')
            state['orders'][order['signal_id']]=order
    _process(state,cache,observed,watermark=watermark)
    state['last_asof']=observed.isoformat()
    audit=[row for d in state['decisions'].values() for row in d['audit']]
    candidates=[row for d in state['decisions'].values() for row in d['candidates']]
    trades=list(state['orders'].values())
    closed=[x for x in trades if x['status']=='CLOSED']
    filled=[x for x in trades if x['filled']]
    opened=[x for x in trades if x['status']=='OPEN']
    unresolved=[x for x in trades if x.get('data_incomplete')]
    unsealed=sum(d['status']!='SEALED' for d in state['decisions'].values())
    pending=[x for x in trades if x['status']=='PENDING_ENTRY']
    excluded_pairs=sum(max(0,len(cache)-d['healthy_symbols']) for d in state['decisions'].values())
    summary=dict(session_date=str(DAY),asof=observed.isoformat(),mode=mode.upper(),execution_authority=False,
        source_profile=plan['source_profile'],fingerprint=plan['fingerprint'],universe_stocks=len(cache),
        selected_orders=len(trades),filled_trades=len(filled),closed_trades=len(closed),open_positions=len(opened),
        wins=sum(x['net_pnl_rs']>1e-7 for x in closed),losses=sum(x['net_pnl_rs']<-1e-7 for x in closed),
        flat_trades=sum(abs(x['net_pnl_rs'])<=1e-7 for x in closed),
        net_pnl_rs=sum(x['net_pnl_rs'] for x in closed),
        mtm_pnl_rs=sum(x['net_pnl_rs'] for x in closed)+sum(x.get('unrealized_net_pnl_rs',0) for x in opened),
        gross_loss_rs=-sum(min(x['net_pnl_rs'],0) for x in closed),data_incomplete_orders=len(unresolved),
        excluded_stock_pair_count=excluded_pairs, source_partial=bool(excluded_pairs or unsealed),
        data_complete=len(state['decisions'])==18 and not excluded_pairs and not unsealed and not unresolved,
        nifty_opening_context_available=bool(math.isfinite(nifty)),coverage_policy='PER_STOCK_VALID_DATA_CONTINUES',
        decided_pairs=len(state['decisions']),missing_or_missed_pairs=unsealed,
        completed_slots=sum(all(f'{(at(c)+MINUTE).strftime("%H%M")}_{s}' in state['decisions'] for s in ('LONG','SHORT')) for c in plan['signal_clocks']),
        complete=observed>=at(SQUARE_OFF) and len(state['decisions'])==18 and not unsealed and not opened and not unresolved and not pending,
        result_kind='PROSPECTIVE_PAPER_COMPLETED_CANDLE_SIMULATION' if mode=='paper' else 'AFTER_SESSION_REPLAY',
        paper_replay_parity_claimed=False)
    slots=[]
    for clock in plan['signal_clocks']:
        ds=[state['decisions'].get(f'{(at(clock)+MINUTE).strftime("%H%M")}_{s}') for s in ('LONG','SHORT')]
        status='PENDING' if observed<at(clock)+MINUTE else 'WAITING_FOR_SOURCE'
        if all(ds):
            status='SEALED' if all(d['status']=='SEALED' for d in ds) else 'PARTIAL_OR_MISSED'
        slots.append(dict(signal_end=clock,confirmation_end=(at(clock)+MINUTE).strftime('%H:%M'),status=status,
            healthy_symbols=max([d['healthy_symbols'] for d in ds if d] or [0]),
            selected_count=sum(len(d['candidates']) for d in ds if d)))
    return dict(audit=pd.DataFrame(audit),candidates=pd.DataFrame(candidates),trades=pd.DataFrame(trades),
        slots=pd.DataFrame(slots),summary=_json(summary),state=_json(state),plan=plan)
