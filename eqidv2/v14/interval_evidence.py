"""Prove unchanged execution through gaps using independent native 5m bounds.

This is an opt-in historical evidence policy, never a candle reconstruction.
The caller must verify each broker snapshot against its fetch manifest before
passing envelopes. Selection is fixed before assessment. Every missing minute
before resolution needs a strict no-touch bound; a possible touch stays unknown.
Only real 1m bars determine fills, stops, targets and square-off timestamps.
"""
from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace

import numpy as np
import pandas as pd

from . import baseline
from ._baseline import execution
from .path_evidence import FIELDS, TERMINAL_REASONS, _hash_prefix, _stamp, _value
from .path_evidence import assess as assess_prefix

EVIDENCE_VERSION = "V14_INDEPENDENT_5M_NO_TOUCH_V1"
NATIVE_SOURCE = "KITE_HISTORICAL_5MINUTE"


def assess(row, path: dict, envelopes: pd.DataFrame) -> dict:
    """Return the ordinary proof shape, with real sparse bars on success.

    Envelopes require end-labelled ``ts``, OHLCV, matching ``symbol``,
    ``source=KITE_HISTORICAL_5MINUTE``, ``independent_interval=True``,
    ``candle_minutes=5``, ``price_scale_compatible=True`` and the native payload's
    hexadecimal ``source_hash``. The compatibility flag must come from a full-day
    broker1m/canonical1m overlap check, allowing only storage-roundoff differences.
    A 5m bound may certify an omitted minute only; it never supplies a fill.
    Native ``_entry`` and ``staged_exit`` determine observed candidate events.
    Entry expiry uses elapsed wall-clock minutes, not compressed array offsets.
    """
    original = assess_prefix(row, path)
    if original["resolved"]:
        return original
    details = dict(original["evidence"], version=EVIDENCE_VERSION,
        independent_interval_evidence=True, synthetic_bars=False,
        envelope_proofs=[], missing_minutes_certified=0, mfe_mae_exact=True,
        path_index_basis="OBSERVED_MINUTES_ONLY")
    empty = {name: np.array([], dtype=np.int64 if name == "timestamp_ns" else float) for name in FIELDS}

    def result(ok, reason, real=empty):
        copied = {name: np.array(real[name], copy=True) for name in FIELDS}
        evidence = dict(details, required_minutes=len(copied["timestamp_ns"]), prefix_sha256=_hash_prefix(copied))
        if len(copied["timestamp_ns"]):
            evidence.update(prefix_start=_stamp(copied["timestamp_ns"][0]), prefix_end=_stamp(copied["timestamp_ns"][-1]))
        return dict(resolved=bool(ok), reason=reason, path=copied, evidence=evidence)

    try:
        confirmation = execution._to_ist_timestamp(_value(row, "confirmation_ts"))
        trigger = float(_value(row, "trigger"))
        side, setup_id = _value(row, "side"), _value(row, "setup_id")
        symbol = _value(row, "tradingsymbol", _value(row, "symbol"))
        if (pd.isna(confirmation) or confirmation.value % execution.MINUTE_NS or side not in {"LONG", "SHORT"}
                or not isinstance(setup_id, str) or not isinstance(symbol, str) or not np.isfinite(trigger) or trigger <= 0):
            return result(False, "INVALID_SELECTED_ORDER")
        if not isinstance(path, Mapping) or set(FIELDS).difference(path):
            return result(False, "MISSING_PATH_FIELDS")
        arrays = {name: np.asarray(path[name]) for name in FIELDS}
        if any(v.ndim != 1 for v in arrays.values()) or len({len(v) for v in arrays.values()}) != 1:
            return result(False, "INVALID_PATH_ARRAY_SHAPE")
        if not np.issubdtype(arrays["timestamp_ns"].dtype, np.signedinteger):
            return result(False, "TIMESTAMPS_MUST_BE_INTEGER_NANOSECONDS")
        arrays = {name: value.astype(np.int64 if name == "timestamp_ns" else float, copy=False) for name,value in arrays.items()}
    except (TypeError, ValueError, OverflowError):
        return result(False, "INVALID_SELECTED_ORDER_OR_PATH")
    cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
    if confirmation >= cutoff:
        return result(False,"NO_EXECUTION_WINDOW")
    details["selected_order_identity"] = dict(side=side,trigger=trigger,setup_id=setup_id,
        tradingsymbol=symbol,confirmation_ts=str(confirmation))
    stamps = arrays["timestamp_ns"]
    if (np.diff(stamps) <= 0).any() or (stamps % execution.MINUTE_NS).any():
        return result(False, "AMBIGUOUS_OR_MISTIMED_MINUTE")
    if ((stamps <= confirmation.value) | (stamps > cutoff.value)).any():
        return result(False, "MINUTE_OUTSIDE_EXECUTION_WINDOW")
    prices = np.column_stack([arrays[name] for name in FIELDS[1:]])
    if len(prices):
        op, hi, lo, cl = prices.T
        if not (np.isfinite(prices).all() and (prices > 0).all()
                and (hi >= np.maximum(op,cl)).all() and (lo <= np.minimum(op,cl)).all()):
            return result(False, "INVALID_OBSERVED_OHLC")
    required = {"ts","open","high","low","close","volume","symbol","source",
                "independent_interval","candle_minutes","source_hash","price_scale_compatible"}
    if not isinstance(envelopes,pd.DataFrame) or required.difference(envelopes):
        return result(False, "MISSING_INDEPENDENT_ENVELOPE_PROVENANCE")
    coarse = envelopes.copy()
    coarse["ts"] = pd.to_datetime(coarse.ts,utc=True,errors="coerce").dt.tz_convert(baseline.IST)
    groups = {stamp.value: group for stamp,group in coarse.dropna(subset=["ts"]).groupby("ts",sort=False)}
    config = baseline.frozen_config()
    expiry = int(config["entry_expiry_minutes"])
    expiry_end = min(confirmation+pd.Timedelta(minutes=expiry),cutoff)
    entry_window = {key: value[stamps <= expiry_end.value] for key,value in arrays.items()}
    entry = execution._entry(SimpleNamespace(side=side,trigger=trigger),entry_window,
        delay_bars=0,trigger_buffer_pct=0.,worse_fill_bps=0.,max_entry_delay_minutes=expiry)
    target_pct = float(config["exit"]["setups"].get(setup_id,config["exit"]["default"])["target_pct"])
    details.update(confirmation_ts=str(confirmation),square_off_ts=str(cutoff),
        entry_expiry_minutes=expiry,target_pct=target_pct,target_authority="FROZEN_SETUP_CONFIGURATION")
    if entry is None:
        terminal_end = expiry_end.value
        terminal = dict(filled=False,exit_reason="UNFILLED",resolution_ts=str(expiry_end))
        real = entry_window
        entry_ns = None
    else:
        entry_index,entry_price,effective_trigger,gap,overshoot = entry
        entry_ns = int(stamps[entry_index])
        if entry_ns > expiry_end.value:
            return result(False,"ENTRY_AFTER_CLOCK_EXPIRY")
        exit_index,exit_price,exit_reason,active_stop,event = execution.staged_exit(
            arrays,entry_index,entry_price,side=="LONG",target_pct)
        terminal_end = int(stamps[exit_index])
        if exit_reason not in TERMINAL_REASONS and terminal_end != cutoff.value:
            return result(False,"SQUARE_OFF_NOT_OBSERVED")
        real = {key:value[:exit_index+1] for key,value in arrays.items()}
        execution_ns = terminal_end-execution.MINUTE_NS if event=="OPEN" else terminal_end
        terminal = dict(filled=True,exit_reason=exit_reason,entry_index=int(entry_index),entry_price=float(entry_price),
            exit_index=int(exit_index),exit_price=float(exit_price),active_stop_pct=float(active_stop),event=event,
            exit_bar_end_ts=_stamp(terminal_end),resolution_ts=_stamp(execution_ns))
        details["entry"] = dict(index=int(entry_index),price=float(entry_price),trigger=float(effective_trigger),
            gap_through=bool(gap),overshoot_bps=float(overshoot),ts=_stamp(entry_ns))
    expected = pd.date_range(confirmation+pd.Timedelta(minutes=1),pd.Timestamp(terminal_end,tz="UTC").tz_convert(baseline.IST),freq="min").as_unit("ns").asi8
    missing = expected[~np.isin(expected,stamps)]
    proof_ends = [terminal_end]
    for stamp in missing:
        minute = pd.Timestamp(int(stamp),tz="UTC").tz_convert(baseline.IST)
        end = minute.ceil("5min")
        group = groups.get(end.value)
        if group is None or len(group)!=1:
            return result(False,"MISSING_OR_AMBIGUOUS_5M_ENVELOPE")
        bar = group.iloc[0]
        payload = str(bar.source_hash)
        independent=bar.independent_interval
        compatible=bar.price_scale_compatible
        try:
            candle_minutes=float(bar.candle_minutes)
        except (TypeError,ValueError,OverflowError):
            candle_minutes=np.nan
        if (str(bar.source)!=NATIVE_SOURCE or not isinstance(independent,(bool,np.bool_)) or not independent
                or not isinstance(compatible,(bool,np.bool_)) or not compatible
                or candle_minutes!=5 or str(bar.symbol)!=symbol
                or len(payload)!=64 or any(c not in "0123456789abcdefABCDEF" for c in payload)
                or pd.notna(bar.get("source_1m_count",np.nan))):
            return result(False,"UNTRUSTED_OR_DERIVED_5M_ENVELOPE")
        for flag in ("gap_filled","opening_snapshot","provisional_stale"):
            value=bar.get(flag,False)
            if pd.notna(value) and str(value).strip().lower() not in {"false","0","0.0","","no","off"}:
                return result(False,"SYNTHETIC_OR_PROVISIONAL_5M_ENVELOPE")
        try:
            opening,high,low,close,volume = (float(bar[name]) for name in ("open","high","low","close","volume"))
        except (TypeError,ValueError,OverflowError):
            return result(False,"INVALID_5M_ENVELOPE")
        if (not np.isfinite([opening,high,low,close,volume]).all() or min(opening,high,low,close)<=0 or volume<0
                or high<max(opening,close) or low>min(opening,close)):
            return result(False,"INVALID_5M_ENVELOPE")
        # Widen the range to cover float32 storage roundoff, never narrow it.
        rounding = max(abs(high),abs(low))*1e-6+1e-8
        bound_low,bound_high = low-rounding,high+rounding
        observed=(stamps>end.value-5*execution.MINUTE_NS)&(stamps<=end.value)
        if observed.any() and (arrays["low"][observed].min()<bound_low or arrays["high"][observed].max()>bound_high):
            return result(False,"5M_ENVELOPE_CONFLICTS_WITH_OBSERVED_1M")
        if entry_ns is None or stamp<entry_ns:
            safe = bound_high<trigger if side=="LONG" else bound_low>trigger
            barrier = dict(stage="BEFORE_ENTRY",trigger=trigger)
        else:
            sign = 1 if side=="LONG" else -1
            stop_pct = execution.TIGHTENED_STOP_PCT if stamp-execution.MINUTE_NS>=entry_ns+execution.TIGHTEN_AFTER_MINUTES*execution.MINUTE_NS else execution.INITIAL_STOP_PCT
            stop = entry_price*(1-sign*stop_pct/100)
            target = entry_price*(1+sign*target_pct/100)
            safe = (bound_low>stop and bound_high<target) if side=="LONG" else (bound_high<stop and bound_low>target)
            barrier = dict(stage="POSITION_OPEN",stop=float(stop),target=float(target),active_stop_pct=float(stop_pct))
        if not safe:
            return result(False,"5M_ENVELOPE_PERMITS_UNKNOWN_TOUCH")
        details["envelope_proofs"].append(dict(missing_minute_end=str(minute),envelope_end=str(end),
            source_hash=payload,lower_bound=bound_low,upper_bound=bound_high,**barrier))
        proof_ends.append(end.value)
    details.update(outcome=terminal,missing_minutes_certified=len(missing),
        mfe_mae_exact=bool(entry_ns is None or not (missing>=entry_ns).any()),
        proof_available_ts=_stamp(max(proof_ends)),full_session_observed=False)
    return result(True,"RESOLVED_INDEPENDENT_5M_"+terminal["exit_reason"],real)


def annotate_ledger(ledger: pd.DataFrame, proofs: dict) -> pd.DataFrame:
    """Mark sparse diagnostics honestly after unchanged native simulation."""
    out=ledger.copy()
    out["mfe_mae_exact"]=True
    out["execution_evidence_policy"]="OBSERVED_MINUTE_PREFIX"
    for index,row in out.iterrows():
        proof=proofs.get(int(row.sid),proofs.get(str(int(row.sid))))
        if proof is None:
            continue
        evidence=proof.get("evidence",proof)
        if evidence.get("version")!=EVIDENCE_VERSION:
            continue
        out.at[index,"execution_evidence_policy"]=EVIDENCE_VERSION
        out.at[index,"mfe_mae_exact"]=evidence["mfe_mae_exact"]
        if not evidence["mfe_mae_exact"]:
            for column in ("mfe_pct","mae_pct"):
                if column in out:
                    out.at[index,column]=np.nan
    return out
