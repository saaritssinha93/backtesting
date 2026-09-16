"""Causal two-completed-bar continuation candidates for V10-G.

The frozen V9 observation pool supplies features; newly selected identities get
separate verified raw one-minute paths. No forward prices enter selection.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_f_backtest as f
import fno_v13_v9_data as data

v9 = f.v9
KEYS = ["tradingsymbol", "futures_tradingsymbol", "confirmation_ts", "side"]
TWO_COLUMNS = ["v10_g_two_bar_change_pct", "v10_g_two_bar_valid",
               "v10_g_latest_body_directional", "v10_g_two_bar_previous_ts",
               "v10_g_two_bar_base_ts"]


def _identities(frame):
    if frame[KEYS].isna().any().any() or frame.duplicated(KEYS).any():
        raise ValueError("Missing or duplicate two-bar candidate identity")
    return pd.MultiIndex.from_frame(frame[KEYS])


def _two_bar_features(pool):
    """Require t, t-5 and t-10 to be observed in the identical session/contract."""
    groups = ["tradingsymbol", "futures_tradingsymbol", "day"]
    out = pool.sort_values([*groups, "signal_ts"], kind="stable").reset_index(drop=True).copy()
    if out.duplicated([*groups, "signal_ts"]).any():
        raise ValueError("Duplicate observed five-minute bar")
    stamp = pd.to_datetime(out.signal_ts, utc=True, errors="coerce")
    feature = pd.to_datetime(out.v9_5m_feature_ts, utc=True, errors="coerce")
    if (stamp.isna() | feature.isna() | feature.gt(stamp)).any():
        raise ValueError("Noncausal five-minute feature timestamp")
    grouped = out.groupby(groups, sort=False)
    first, second = grouped.signal_ts.shift(1), grouped.signal_ts.shift(2)
    base_close = pd.to_numeric(grouped.close.shift(2), errors="coerce").astype(float)
    latest = pd.to_numeric(out.close, errors="coerce").astype(float)
    valid = ((out.signal_ts - first).eq(pd.Timedelta(minutes=5)) &
             (out.signal_ts - second).eq(pd.Timedelta(minutes=10)) &
             np.isfinite(latest) & latest.gt(0) & np.isfinite(base_close) & base_close.gt(0))
    out["v10_g_two_bar_change_pct"] = ((latest / base_close - 1) * 100).where(valid)
    out["v10_g_two_bar_valid"] = valid
    out["v10_g_two_bar_previous_ts"] = first
    out["v10_g_two_bar_base_ts"] = second
    return out


def _strict_pool(pool, annotated):
    """Reproduce strict V9 candidates, except the single-bar price-size floor."""
    out = _two_bar_features(pool)
    bull = out.ema9.gt(out.ema20) & out.ema20.gt(out.ema50)
    bear = out.ema9.lt(out.ema20) & out.ema20.lt(out.ema50)
    out["side"] = np.where(bull, "LONG", np.where(bear, "SHORT", "NONE"))
    direction = np.where(out.side.eq("LONG"), 1., -1.)
    context_columns = ["contract_month", "day", "nifty_first_bar_return_pct",
                       "nifty_first_bar_alignment_pct"]
    context = annotated[context_columns].drop_duplicates()
    if context.duplicated(["contract_month", "day"]).any():
        raise ValueError("Conflicting frozen Nifty context")
    out = v9.v5.v13_v3.annotate_nifty_gate(out, context)
    exact_confirmation = (pd.to_datetime(out.v9_1m_feature_ts, utc=True, errors="coerce") ==
                          pd.to_datetime(out.confirmation_ts, utc=True, errors="coerce"))
    cap = v9.v5.v13_v2.POLICIES[v9.v5.BASE_POLICY_NAME].max_oi_change_pct
    strict = ((bull | bear) & out.oi.gt(out.prev_oi) & out.oi_change_pct.ge(.05) &
              out.oi_change_pct.le(cap) & out.volume_ratio.ge(.8) &
              out.v9_exact_confirmation_present & exact_confirmation &
              out.confirmation_high.gt(out.confirmation_low) &
              (direction * (out.confirmation_close - out.confirmation_open)).gt(0) &
              (direction * (out.confirmation_close - out.signal_close)).gt(0) &
              out.hhmm_int.between(925, 1500) & out.nifty_first_bar_gate_pass)
    out["wick_ratio"] = np.where(out.side.eq("LONG"), out.v9_1m_upper_wick_ratio,
                                  out.v9_1m_lower_wick_ratio)
    out["trigger"] = np.where(out.side.eq("LONG"), out.confirmation_high, out.confirmation_low)
    out["v10_g_latest_body_directional"] = (direction * (out.close - out.open)).gt(0)
    out["v13_v2_policy"] = v9.v5.BASE_POLICY_NAME
    out["v13_v2_oi_cap_pct"] = cap
    out = out.loc[strict.fillna(False)].copy()
    data.assert_feature_chronology(out)
    return out


def _augment_frames(original, pool, annotated):
    """Pure feature construction/identity assignment; convenient for regression tests."""
    original = original.reset_index(drop=True).copy()
    original_input = original.copy()
    original_keys = _identities(original)
    annotated_keys = _identities(annotated)
    if original.sid.duplicated().any() or annotated.sid.duplicated().any():
        raise ValueError("Duplicate frozen signal ID")
    ceiling = int(annotated.sid.max())
    annotated_ids = pd.Series(annotated.sid.to_numpy(), index=annotated_keys)
    expected_ids = annotated_ids.reindex(original_keys)
    if expected_ids.isna().any() or not np.array_equal(expected_ids.to_numpy(), original.sid.to_numpy()):
        raise ValueError("Frozen signal ID does not match annotated identity")
    reconstructed = _strict_pool(pool, annotated)
    direction = np.where(reconstructed.side.eq("LONG"), 1., -1.)
    native_floor = (direction * reconstructed.price_change_pct).ge(.10)
    reconstructed_keys = _identities(reconstructed)
    if set(reconstructed_keys[native_floor]) != set(original_keys):
        raise ValueError("Frozen native strict reconstruction parity failed")
    feature_values = reconstructed.set_index(KEYS)[TWO_COLUMNS].reindex(original_keys)
    for column in TWO_COLUMNS:
        original[column] = feature_values[column].array
    original["v10_g_two_bar_new_sid"] = False
    new_mask = (~reconstructed_keys.isin(original_keys) &
                (direction * reconstructed.price_change_pct).gt(0) &
                reconstructed.v10_g_two_bar_valid & reconstructed.v10_g_latest_body_directional &
                (direction * reconstructed.v10_g_two_bar_change_pct).gt(0))
    additions = reconstructed.loc[new_mask].sort_values(KEYS, kind="stable").copy()
    known = annotated_ids.reindex(_identities(additions))
    missing = known.isna().to_numpy()
    values = known.to_numpy().copy()
    values[missing] = np.arange(ceiling + 1, ceiling + 1 + int(missing.sum()))
    additions["sid"] = values.astype(np.int64)
    additions["v10_g_two_bar_new_sid"] = additions.sid.gt(ceiling)
    missing_columns = set(original.columns) - set(additions.columns)
    if missing_columns:
        raise ValueError(f"Reconstructed candidate columns missing: {sorted(missing_columns)}")
    result = pd.concat([original, additions[original.columns]], ignore_index=True)
    if result.sid.duplicated().any():
        raise ValueError("Augmented signal IDs collide")
    _identities(result)
    pd.testing.assert_frame_equal(result.iloc[:len(original_input)][original_input.columns],
                                  original_input, check_exact=True)
    proof = dict(native_signal_rows=len(original), reconstructed_native_rows=int(native_floor.sum()),
                 native_identity_parity=True, native_values_and_dtypes_exact=True,
                 added_candidate_rows=len(additions),
                 native_sid_ceiling=ceiling, assigned_new_ids=int(missing.sum()),
                 formula="100*(close_t/close_t_minus_10min-1); exact t,t-5,t-10 same session and contract",
                 latest_direction="signed close-to-close>0 AND signed(close-open)>0",
                 first_possible_alternate_signal="09:30", outcomes_used=False)
    return result, proof


def augment_source(source):
    """Extend a source already verified by G's frozen-provenance loader."""
    if not source.get("source_verification", {}).get("all_frozen_artifacts_verified"):
        raise ValueError("Two-bar augmentation requires verified frozen source")
    if source.get("two_bar_reconstruction_proof"):
        return source
    folder = Path(source["source"]) / "dataset"
    names = ["all_5m_features.parquet", "annotated.parquet"]
    for name in names:
        if f.v10.sha(folder / name) != source["manifest"]["output_sha256"][name]:
            raise RuntimeError(f"Frozen two-bar source artifact drift: {name}")
    signals, proof = _augment_frames(source["signals"], pd.read_parquet(folder / names[0]),
                                    pd.read_parquet(folder / names[1]))
    for name in names:
        if f.v10.sha(folder / name) != source["manifest"]["output_sha256"][name]:
            raise RuntimeError(f"Frozen two-bar source artifact changed during read: {name}")
    return {**source, "signals": signals, "two_bar_reconstruction_proof": proof}


def eligible(signals, setup):
    """Alternate only the price-size test; actual latest price values are retained."""
    native = v9.v5.replay._eligible(signals, setup)
    if signals.empty:
        return native
    # All callers operate on the verified augmented candidate pool. Root G
    # applies its immutable confirmation-volume filter before this function.
    other = v9.v5.replay._eligible(signals, replace(setup, price_change_pct=0.))
    direction = 1. if setup.side == "LONG" else -1.
    valid = (other.v10_g_two_bar_valid.fillna(False) &
             other.v10_g_latest_body_directional.fillna(False) &
             (direction * other.price_change_pct).gt(0) &
             (direction * other.v10_g_two_bar_change_pct).ge(setup.price_change_pct))
    alternate = other.loc[valid]
    if not alternate.empty:
        stamp = pd.to_datetime(alternate.signal_ts, utc=True, errors="coerce")
        first = pd.to_datetime(alternate.v10_g_two_bar_previous_ts, utc=True, errors="coerce")
        second = pd.to_datetime(alternate.v10_g_two_bar_base_ts, utc=True, errors="coerce")
        if not ((stamp - first).eq(pd.Timedelta(minutes=5)) &
                (stamp - second).eq(pd.Timedelta(minutes=10))).all():
            raise ValueError("Invalid two-bar feature chronology")
    ids = set(native.sid) | set(alternate.sid)
    return signals.loc[signals.sid.isin(ids)].copy()


def load_paths(source, orders):
    """Resolve by verified SID identity, rebuilding only genuinely new raw paths."""
    if not source.get("source_verification", {}).get("all_frozen_artifacts_verified"):
        raise ValueError("Two-bar paths require verified frozen source")
    orders = orders.drop_duplicates(["sid", *KEYS]).copy()
    known = source["signals"].set_index("sid")
    if known.index.duplicated().any() or orders.sid.duplicated().any():
        raise ValueError("Conflicting selected signal IDs")
    for row in orders.itertuples(index=False):
        if row.sid not in known.index or any(getattr(row, key) != known.loc[row.sid, key] for key in KEYS):
            raise ValueError("Selected SID does not match verified source identity")
    ceiling = source.get("two_bar_reconstruction_proof", {}).get("native_sid_ceiling")
    folder = Path(source["source"]) / "dataset"
    path_file = folder / "paths.npz"
    expected_cache = source["manifest"]["output_sha256"]["paths.npz"]
    if f.v10.sha(path_file) != expected_cache:
        raise RuntimeError("Frozen path cache drift")
    result, new_indices = {}, []
    with np.load(path_file, allow_pickle=False) as archive:
        for index, row in orders.iterrows():
            sid = int(row.sid)
            keys = {field: f"{sid}_{field}" for field in ("timestamp_ns", "open", "high", "low", "close")}
            if all(value in archive for value in keys.values()):
                if ceiling is not None and sid > ceiling:
                    raise ValueError("New signal ID collides with frozen path cache")
                result[sid] = {field: archive[value].copy() for field, value in keys.items()}
            else:
                if ceiling is None or sid <= ceiling:
                    raise RuntimeError(f"Missing frozen execution path for native sid={sid}")
                new_indices.append(index)
    selected_new = orders.loc[new_indices]
    proof = []
    if not selected_new.empty:
        records = {str(Path(record["path"]).resolve()).lower(): record for record in source["manifest"]["sources"]}
        for symbol in sorted(selected_new.tradingsymbol.unique()):
            path = data.hybrid.equity_one_minute_path(str(symbol), data.hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR)
            record = records.get(str(path.resolve()).lower())
            if record is None or not record["exists"] or not path.is_file() or f.v10.sha(path) != record["sha256"]:
                raise RuntimeError(f"Unverified two-bar raw minute source: {path}")
            proof.append(dict(path=str(path.resolve()), sha256=record["sha256"]))
        rebuilt, _ = v9.v5.materialize_raw_paths(selected_new, cutoff=v9.v5.OFFICIAL_CUTOFF)
        result.update(rebuilt)
        for record in proof:
            if f.v10.sha(Path(record["path"])) != record["sha256"]:
                raise RuntimeError(f"Raw minute source changed during path build: {record['path']}")
    if f.v10.sha(path_file) != expected_cache:
        raise RuntimeError("Frozen path cache changed during read")
    v9.validate_paths(orders, result)
    source["two_bar_path_proof"] = dict(cached_paths=len(result) - len(selected_new),
        rebuilt_paths=len(selected_new), raw_sources=proof, paths_validated=True)
    return result
