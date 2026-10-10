"""Source-hashed, causal V13-v9 selection and rejection research dataset.

The native V13-v5 signal builder is the identity authority. Its SID, strict
confirmation, 14 setup book and selection results are retained unchanged. A
separate audit covers every observed complete equity 5m bar, including bars
without matching futures OI, and every original setup-time/side opportunity.
Forward OHLC paths are stored separately and never enter feature construction.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sweep
import fno_oi_hybrid_data as hybrid
import fno_v13_corrected_v5_backtest as v5


SCHEMA_VERSION = "FNO_V13_V9_CAUSAL_CANDIDATE_DATA_V1"
KEYS = ["tradingsymbol", "futures_tradingsymbol", "confirmation_ts"]
FEATURE_VALUE_SUFFIXES = (
    "ema9", "ema20", "ema50", "ema_bull", "ema_bear", "ema_spread_pct",
    "ema9_slope_pct", "price_change_pct", "momentum_3bars_pct", "volume_ratio",
    "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "range_pct",
    "vwap", "distance_vwap_pct", "session_range_pct", "gap_pct",
)
TABLE_NAMES = ("signals", "annotated", "all_5m_features", "setup_audit", "path_quality", "eligibility")
DATASET_AUDIT_FILES = ("source_manifest.csv", "source_session_eligibility.csv")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def eligibility_through_day(
    eligibility: pd.DataFrame, through_day: str | date
) -> pd.DataFrame:
    """Return eligibility audit rows inside the declared dataset window.

    ``v5.load_market`` deliberately returns its full raw eligibility audit even
    though signals are built only through ``through_day``. A dataset bundle
    must not publish that wider audit as if it shared the dataset cutoff.
    """
    cutoff = date.fromisoformat(through_day) if isinstance(through_day, str) else through_day
    if "day" not in eligibility:
        raise ValueError("Eligibility audit is missing day")
    result = eligibility.copy()
    parsed = pd.to_datetime(result["day"], errors="coerce")
    if parsed.isna().any():
        raise ValueError("Eligibility audit contains an invalid day")
    result["day"] = parsed.dt.date
    result = result.loc[result["day"].le(cutoff)].copy()
    return result.sort_values("day", kind="stable").reset_index(drop=True)


def dataset_output_checksums(output_dir: Path | str) -> dict[str, str]:
    """Hash every reusable dataset output, including its CSV audit ledgers."""
    output_dir = Path(output_dir)
    names = [
        *(f"{name}.parquet" for name in TABLE_NAMES),
        "paths.npz",
        *DATASET_AUDIT_FILES,
    ]
    return {name: sha256(output_dir / name) for name in names}


def causal_bar_features(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Features of completed end-labelled bars, never using later bar values."""
    out = frame.sort_values("ts", kind="stable").drop_duplicates("ts", keep="last").reset_index(drop=True).copy()
    if out.empty:
        return out
    stamps = hybrid._to_ist(out["ts"])
    out["ts"] = stamps
    days = stamps.dt.date
    prices = {key: pd.to_numeric(out[key], errors="coerce") for key in ("open", "high", "low", "close", "volume")}
    close, volume = prices["close"], prices["volume"]
    for span in (9, 20, 50):
        out[f"{prefix}ema{span}"] = close.ewm(span=span, adjust=False).mean()
    e9, e20, e50 = [out[f"{prefix}ema{span}"] for span in (9, 20, 50)]
    out[f"{prefix}ema_bull"] = (e9.gt(e20) & e20.gt(e50)).astype("boolean")
    out[f"{prefix}ema_bear"] = (e9.lt(e20) & e20.lt(e50)).astype("boolean")
    out[f"{prefix}ema_spread_pct"] = (e9 - e50).div(close.where(close.gt(0))) * 100
    out[f"{prefix}ema9_slope_pct"] = e9.pct_change(fill_method=None) * 100
    out[f"{prefix}price_change_pct"] = close.pct_change(fill_method=None) * 100
    out[f"{prefix}momentum_3bars_pct"] = close.pct_change(3, fill_method=None) * 100
    prior_volume = volume.shift(1).rolling(20, min_periods=5).mean()
    out[f"{prefix}volume_ratio"] = volume.div(prior_volume.where(prior_volume.gt(0)))
    # Native confirmation arithmetic explicitly promotes stored float32 OHLC
    # to float64 before dividing; retain that precision at threshold edges.
    ohlc64 = {key: prices[key].astype(float) for key in ("open", "high", "low", "close")}
    bar_range = ohlc64["high"] - ohlc64["low"]
    denominator = bar_range.where(bar_range.gt(0))
    out[f"{prefix}body_ratio"] = (ohlc64["close"] - ohlc64["open"]).abs().div(denominator)
    out[f"{prefix}upper_wick_ratio"] = (ohlc64["high"] - pd.concat([ohlc64["open"], ohlc64["close"]], axis=1).max(axis=1)).div(denominator)
    out[f"{prefix}lower_wick_ratio"] = (pd.concat([ohlc64["open"], ohlc64["close"]], axis=1).min(axis=1) - ohlc64["low"]).div(denominator)
    out[f"{prefix}range_pct"] = bar_range.div(close.where(close.gt(0))) * 100
    typical = (prices["high"] + prices["low"] + close) / 3.0
    cumulative_volume = volume.groupby(days).cumsum()
    vwap = (typical * volume).groupby(days).cumsum().div(cumulative_volume.where(cumulative_volume.gt(0)))
    out[f"{prefix}vwap"] = vwap
    out[f"{prefix}distance_vwap_pct"] = (close.div(vwap.where(vwap.gt(0))) - 1) * 100
    running_high = prices["high"].groupby(days).cummax()
    running_low = prices["low"].groupby(days).cummin()
    out[f"{prefix}session_range_pct"] = (running_high.div(running_low.where(running_low.gt(0))) - 1) * 100
    # Only the preceding session's final close is used. Current session final
    # close is never mapped to that session's feature rows.
    previous_day_close = close.groupby(days, sort=True).last().shift(1)
    session_open = prices["open"].groupby(days).transform("first")
    out[f"{prefix}gap_pct"] = (session_open.div(days.map(previous_day_close)) - 1) * 100
    out[f"{prefix}feature_ts"] = stamps
    return out


def feature_columns(frame: pd.DataFrame) -> list[str]:
    return [name for name in frame if name.startswith(("v9_5m_", "v9_1m_"))]


def assert_feature_chronology(frame: pd.DataFrame) -> None:
    for column, boundary in (("v9_5m_feature_ts", "signal_ts"), ("v9_1m_feature_ts", "confirmation_ts")):
        if column not in frame:
            raise ValueError(f"Missing chronology column {column}")
        feature_ts = hybrid._to_ist(frame[column])
        cutoff = hybrid._to_ist(frame[boundary])
        if (feature_ts.notna() & (cutoff.isna() | feature_ts.gt(cutoff))).any():
            raise AssertionError(f"Feature chronology violation: {column} > {boundary}")
    forbidden = ("net_profit", "net_return", "exit_price", "exit_ts", "mfe", "mae", "pnl", "forward_return")
    if any(any(token in name.lower() for token in forbidden) for name in feature_columns(frame)):
        raise AssertionError("Outcome column in causal feature namespace")


def observed_pool(minute: pd.DataFrame, futures: pd.DataFrame, *, days: set[date], equity_symbol: str, futures_symbol: str, contract_month: str) -> pd.DataFrame:
    """All complete observed cash 5m bars, with exact future OI and next 1m."""
    five = hybrid.aggregate_equity_one_minute_to_five_minute(minute)
    if five.empty:
        return pd.DataFrame()
    five = causal_bar_features(hybrid.add_equity_five_minute_features(five), "v9_5m_")
    if futures.empty:
        for name in ("oi", "prev_oi", "oi_change_pct"):
            five[name] = np.nan
    else:
        fut = futures.sort_values("ts").copy()
        fut["oi"] = pd.to_numeric(fut["oi"], errors="coerce")
        fut["prev_oi"] = fut["oi"].shift(1)
        valid = fut["oi"].gt(0) & fut["prev_oi"].gt(0) & np.isfinite(fut["oi"]) & np.isfinite(fut["prev_oi"])
        fut["oi_change_pct"] = np.where(valid, (fut["oi"] / fut["prev_oi"] - 1) * 100, np.nan)
        five = five.merge(fut[["ts", "oi", "prev_oi", "oi_change_pct"]].drop_duplicates("ts", keep="last"), on="ts", how="left", validate="one_to_one")
    five["day"] = five["ts"].dt.date
    five = five.loc[five["day"].isin(days) & five["ts"].dt.strftime("%H%M").between("0920", "1515")].copy()
    if five.empty:
        return five
    five["signal_ts"] = five["ts"]
    five["confirmation_ts"] = five["signal_ts"] + pd.Timedelta(minutes=1)
    five["hhmm"] = five["signal_ts"].dt.strftime("%H%M")
    five["hhmm_int"] = five["hhmm"].astype(int)
    five["tradingsymbol"] = equity_symbol
    five["futures_tradingsymbol"] = futures_symbol
    five["contract_month"] = contract_month
    five["signal_close"] = five["close"]
    five["price_source"] = hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION
    five["oi_source"] = "NFO_FUTURE"
    five["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    minute_features = causal_bar_features(minute, "v9_1m_")
    minute_features["confirmation_source_flagged"] = False
    for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if column in minute_features:
            values = minute_features[column]
            minute_features["confirmation_source_flagged"] |= pd.to_numeric(values, errors="coerce").fillna(0).ne(0) | values.astype(str).str.strip().str.lower().isin({"true", "yes", "on"})
    selected = ["ts", "open", "high", "low", "close", "volume", "confirmation_source_flagged", *feature_columns(minute_features)]
    confirmation = minute_features[selected].rename(columns={"ts": "confirmation_ts", **{key: f"confirmation_{key}" for key in ("open", "high", "low", "close", "volume")}})
    five = five.merge(confirmation, on="confirmation_ts", how="left", validate="one_to_one")
    five["v9_exact_confirmation_present"] = five["v9_1m_feature_ts"].notna()
    five["v9_feature_available_ts"] = five["v9_1m_feature_ts"]
    five["confirmation_source_flagged"] = five["confirmation_source_flagged"].astype("boolean").fillna(False).astype(bool)
    five["body_ratio"] = five["v9_1m_body_ratio"]
    assert_feature_chronology(five)
    return five.drop(columns=["date", "ts", "gap_filled", "opening_snapshot", "provisional_stale"], errors="ignore")


def make_setup_audit(pool: pd.DataFrame, annotated: pd.DataFrame, gated: pd.DataFrame) -> pd.DataFrame:
    """Expose all causal filter failures before original per-setup top-N pick."""
    setups = v5.profile_setups(v5.PROFILES["higher_frequency"])
    parts = []
    for setup in setups:
        rows = pool.loc[pool["hhmm_int"].eq(int(setup.signal_end.replace(":", "")))].copy()
        rows["side"] = setup.side
        rows["setup_id"] = setup.setup_id
        rows["picker"] = setup.picker
        rows["max_entries"] = setup.max_entries
        long_side = setup.side == "LONG"
        sign = 1 if long_side else -1
        rows["wick_ratio"] = rows[f"v9_1m_{'upper' if long_side else 'lower'}_wick_ratio"]
        rows["trigger"] = rows[f"confirmation_{'high' if long_side else 'low'}"]
        rows["native_stop_pct"] = setup.stop_pct
        rows["native_target_pct"] = setup.target_pct
        rows["configured_confirmation_end"] = setup.confirmation_end
        checks = {
            "check_5m_ema_stack": rows[f"v9_5m_ema_{'bull' if long_side else 'bear'}"],
            "check_oi_increasing": rows["oi"].gt(rows["prev_oi"]),
            "check_loose_price": (sign * rows["price_change_pct"]).ge(sweep.LOOSE["price_change_pct"]),
            "check_loose_oi": rows["oi_change_pct"].ge(sweep.LOOSE["oi_change_pct"]),
            "check_loose_volume": rows["volume_ratio"].ge(sweep.LOOSE["volume_ratio"]),
            "check_exact_1m_confirmation": rows["v9_exact_confirmation_present"],
            "check_confirmation_range": rows["confirmation_high"].gt(rows["confirmation_low"]),
            "check_confirmation_direction": (sign * (rows["confirmation_close"] - rows["confirmation_open"])).gt(0),
            "check_confirmation_displacement": (sign * (rows["confirmation_close"] - rows["signal_close"])).gt(0),
            "check_policy_oi_cap": rows["oi_change_pct"].le(v5.v13_v2.POLICIES[v5.BASE_POLICY_NAME].max_oi_change_pct),
            "check_setup_price": (sign * rows["price_change_pct"]).ge(setup.price_change_pct),
            "check_setup_oi": rows["oi_change_pct"].ge(setup.oi_change_pct),
            "check_setup_volume": rows["volume_ratio"].ge(setup.volume_ratio),
            "check_setup_body": rows["body_ratio"].ge(setup.body_ratio),
            "check_setup_wick": rows["wick_ratio"].le(setup.max_wick_ratio),
            "check_setup_liquidity": rows["traded_value"].ge(setup.min_traded_value),
        }
        for column, values in checks.items():
            rows[column] = values.fillna(False).astype(bool)
        parts.append(rows)
    audit = pd.concat(parts, ignore_index=True)
    audit["candidate_id"] = (audit["futures_tradingsymbol"].astype(str) + "|" + audit["signal_ts"].astype(str) + "|" + audit["setup_id"].astype(str))
    if audit["candidate_id"].duplicated().any():
        raise AssertionError("Duplicate candidate identity in setup audit")
    context = v5.v13_v3.load_nifty_first_bar_context(audit["contract_month"].unique())
    audit = v5.v13_v3.annotate_nifty_gate(audit, context)
    audit["check_nifty_gate"] = audit["nifty_first_bar_gate_pass"]
    sid_columns = [*KEYS, "side", "sid"]
    audit = audit.merge(annotated[sid_columns], on=[*KEYS, "side"], how="left", validate="many_to_one")
    audit["native_strict_candidate"] = audit["sid"].notna()
    audit["sid"] = audit["sid"].astype("Int64")
    audit["native_policy_eligible"] = audit["sid"].isin(set(gated["sid"].astype(int)))
    check_columns = [name for name in audit if name.startswith("check_")]
    audit["all_causal_filters_pass"] = audit[check_columns].all(axis=1)
    audit["setup_thresholds_pass"] = audit[[name for name in check_columns if name.startswith("check_setup_")]].all(axis=1)
    audit["baseline_setup_eligible"] = audit["all_causal_filters_pass"] & audit["native_strict_candidate"]
    native_selected = v5.select_orders(gated, setups)
    selected_keys = set(zip(native_selected["sid"].astype(int), native_selected["setup_id"]))
    audit["baseline_selected"] = [(int(sid), setup_id) in selected_keys if pd.notna(sid) else False for sid, setup_id in zip(audit["sid"], audit["setup_id"])]
    audit["causal_rejection_reasons"] = audit[check_columns].apply(lambda row: "|".join(name.removeprefix("check_").upper() for name, passed in row.items() if not passed), axis=1)
    audit["selection_status"] = np.select([audit["baseline_selected"], audit["baseline_setup_eligible"], audit["all_causal_filters_pass"]], ["SELECTED", "RANKED_OUT", "NATIVE_PATH_UNAVAILABLE"], default="FILTER_REJECTED")
    actual = set(zip(audit.loc[audit["baseline_selected"], "sid"].astype(int), audit.loc[audit["baseline_selected"], "setup_id"]))
    if actual != selected_keys:
        raise AssertionError("Original selection not fully represented in setup audit")
    # Replay eligibility independently, proving the audit did not reinterpret
    # an original gate or omit a eligible ranked-out symbol.
    expected_eligible = set()
    for setup in setups:
        rows = v5.replay._eligible(gated, setup)
        expected_eligible.update((int(sid), setup.setup_id) for sid in rows["sid"])
    observed_eligible = set(zip(audit.loc[audit["baseline_setup_eligible"], "sid"].astype(int), audit.loc[audit["baseline_setup_eligible"], "setup_id"]))
    if observed_eligible != expected_eligible:
        raise AssertionError(f"Native setup eligibility mismatch: missing={len(expected_eligible-observed_eligible)}, extra={len(observed_eligible-expected_eligible)}")
    return audit


def attach_features(signals: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    columns = [*KEYS, "signal_ts", "v9_feature_available_ts", "v9_exact_confirmation_present", "confirmation_source_flagged", *feature_columns(pool)]
    out = signals.merge(pool[columns], on=KEYS, how="left", validate="many_to_one")
    if out["v9_5m_feature_ts"].isna().any() or out["v9_1m_feature_ts"].isna().any():
        raise AssertionError("Native V13 candidate missing its exact raw causal features")
    # Shared values catch stale native cache or feature reconstruction drift.
    for source, feature in (("price_change_pct", "v9_5m_price_change_pct"), ("volume_ratio", "v9_5m_volume_ratio"), ("body_ratio", "v9_1m_body_ratio")):
        if not np.allclose(out[source], out[feature], equal_nan=True, rtol=1e-10, atol=1e-10):
            raise AssertionError(f"Native feature parity failed: {source}")
    assert_feature_chronology(out)
    return out


def _manifest_sources(through_day: date) -> tuple[list[dict], dict[str, pd.DataFrame], dict[str, set[date]]]:
    # The legacy V6 eligibility CSV can stop before current completed sessions.
    # Scan raw contract coverage without writing any original V13 artifact.
    eligibility, _, _, regimes, _ = v5.v13_v3._load_eligibility(True, v5.MIN_CONTRACT_COVERAGE)
    eligible = eligibility.loc[pd.to_numeric(eligibility["coverage"], errors="coerce").ge(v5.MIN_CONTRACT_COVERAGE) & pd.to_numeric(eligibility["contracts_with_data"], errors="coerce").gt(0) & eligibility["day"].le(through_day) & eligibility["required_contract"].isin(regimes)]
    mapped, by_month = {}, {}
    paths: set[Path] = {Path(__file__).resolve(), Path(v5.__file__).resolve(), hybrid.TOKEN_CACHE_PATH, v5.v6.ELIGIBILITY_PATH, common.UNIVERSE_DIR / "contract_registry.parquet"}
    paths.update(Path(module.__file__).resolve() for module in (hybrid, sweep, provenance, v5.v6, v5.replay, v5.v13_v2, v5.v13_v3, common))
    paths.update(common.MASTER_DIR.glob("instrument_master_*.parquet"))
    for month, group in eligible.groupby("required_contract", sort=True):
        by_month[str(month)] = set(group["day"])
        mapped[str(month)], _ = provenance.load_backtest_universe(universe_path=regimes[month], contract_month_contains=month)
        paths.add(regimes[month])
        paths.add(v5.v13_v3.NIFTY_ROOT / f"NIFTY{month}FUT_5minute.parquet")
        for row in mapped[str(month)].to_dict("records"):
            symbol = hybrid.resolve_backtest_equity_symbol(row["equity_symbol"])
            paths.add(hybrid.equity_one_minute_path(symbol, hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR))
            paths.add(v5.v13_v3.NIFTY_ROOT / f"{common.safe_contract_stem(row['futures_tradingsymbol'])}_5minute.parquet")
    sources = [{"path": str(path.resolve()), "exists": path.is_file(), "sha256": sha256(path) if path.is_file() else None} for path in sorted(paths, key=str)]
    return sources, mapped, by_month


def _read_dataset(output_dir: Path, manifest: dict) -> dict[str, Any]:
    result: dict[str, Any] = {name: pd.read_parquet(output_dir / f"{name}.parquet") for name in TABLE_NAMES}
    for frame in result.values():
        if "day" in frame:
            frame["day"] = pd.to_datetime(frame["day"]).dt.date
    paths: dict[int, dict] = {}
    with np.load(output_dir / "paths.npz", allow_pickle=False) as blob:
        for key in blob.files:
            sid, field = key.split("_", 1)
            paths.setdefault(int(sid), {})[field] = blob[key]
    result.update(paths=paths, days=[date.fromisoformat(day) for day in manifest["days"]], calendar={month: date.fromisoformat(day) for month, day in manifest["calendar"].items()}, manifest=manifest, output_dir=output_dir)
    return result


def build_dataset(output_dir: Path | str, through_day: str | date = "2026-09-11", rebuild: bool = False) -> dict[str, Any]:
    """Build once, then reuse only after raw source and output hash validation."""
    output_dir = Path(output_dir).resolve()
    through_day = date.fromisoformat(through_day) if isinstance(through_day, str) else through_day
    sources, mapped, by_month = _manifest_sources(through_day)
    source_fingerprint = common.canonical_json_sha256(sources)
    manifest_path = output_dir / "dataset_manifest.json"
    if manifest_path.is_file() and not rebuild:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("schema") == SCHEMA_VERSION and manifest.get("through_day") == str(through_day) and manifest.get("source_fingerprint") == source_fingerprint:
            if all((output_dir / name).is_file() and sha256(output_dir / name) == checksum for name, checksum in manifest["output_sha256"].items()):
                return _read_dataset(output_dir, manifest)
    output_dir.mkdir(parents=True, exist_ok=True)
    native_sources = [record for record in sources if record["path"] != str(Path(__file__).resolve())]
    native_fingerprint = common.canonical_json_sha256(native_sources)
    native_manifest_path = output_dir / "native_source_manifest.json"
    native_unchanged = False
    if native_manifest_path.is_file() and not rebuild:
        native_manifest = json.loads(native_manifest_path.read_text(encoding="utf-8"))
        native_unchanged = native_manifest.get("complete") is True and native_manifest.get("source_fingerprint") == native_fingerprint
    original_cache = v5.CACHE_DIR
    original_cache_loader = v5._load_verified_v5_cache
    original_seed_loader = v5._load_verified_v3_seed

    def verified_native_cache(stem: Path, payload: dict):
        path = stem.with_suffix(".json")
        if not path.is_file():
            return None
        try:
            proof = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if proof.get("v9_native_source_fingerprint") != native_fingerprint:
            return None
        return original_cache_loader(stem, payload)

    try:
        v5.CACHE_DIR = output_dir / "native_v13_cache"
        v5._load_verified_v5_cache = verified_native_cache
        # An old V3 seed has no raw-input proof for this V9 dataset. Cache
        # misses rebuild from raw data even if a historical seed is available.
        v5._load_verified_v3_seed = lambda *args, **kwargs: None
        # Native V9-owned caches are reused only when their raw and original
        # code input hashes match. Historical V13 files are never rewritten.
        gated, _, _, calendar, cache_records, annotated, eligibility = v5.load_market(through_day, rebuild_cache=not native_unchanged, refresh_eligibility=True)
        eligibility = eligibility_through_day(eligibility, through_day)
    finally:
        v5.CACHE_DIR = original_cache
        v5._load_verified_v5_cache = original_cache_loader
        v5._load_verified_v3_seed = original_seed_loader
    for source in native_sources:
        path = Path(source["path"])
        if path.is_file() != source["exists"] or (source["exists"] and sha256(path) != source["sha256"]):
            raise RuntimeError(f"Native source changed during signal build: {path}")
    for record in cache_records:
        path = Path(record["v5_cache_manifest"])
        proof = json.loads(path.read_text(encoding="utf-8"))
        proof["v9_native_source_fingerprint"] = native_fingerprint
        common.atomic_write_json(path, proof)
        record["manifest"] = proof
        record["v5_cache_manifest_sha256"] = sha256(path)
    # Commit only after every regime succeeds. A failed refresh cannot make
    # old or partly rebuilt caches appear verified under a new fingerprint.
    common.atomic_write_json(native_manifest_path, {"complete": True, "source_fingerprint": native_fingerprint, "sources": native_sources})
    records = []
    for month, universe in mapped.items():
        for contract in universe.to_dict("records"):
            records.append({**contract, "contract_month": month})
    contracts = pd.DataFrame(records)
    contracts["resolved_equity_symbol"] = contracts["equity_symbol"].map(hybrid.resolve_backtest_equity_symbol)
    pools, path_quality, paths = [], [], {}
    total = contracts["resolved_equity_symbol"].nunique()
    for number, (symbol, group) in enumerate(contracts.groupby("resolved_equity_symbol", sort=True), 1):
        minute = hybrid.load_equity_one_minute(symbol)
        if minute.empty:
            continue
        minute = minute.sort_values("ts", kind="stable").drop_duplicates("ts", keep="last").reset_index(drop=True)
        for contract in group.to_dict("records"):
            month = contract["contract_month"]
            futures = sweep.load_five_minute_history(contract["futures_tradingsymbol"])
            pool = observed_pool(minute, futures, days=by_month[month], equity_symbol=symbol, futures_symbol=contract["futures_tradingsymbol"], contract_month=month)
            if not pool.empty:
                pool["instrument_token"] = int(contract["equity_instrument_token"])
                pool["futures_instrument_token"] = int(contract["futures_instrument_token"])
                pool["exchange"] = "NSE"
                pools.append(pool)
        ns = minute["ts"].astype("int64").to_numpy()
        for row in annotated.loc[annotated["tradingsymbol"].eq(symbol)].itertuples(index=False):
            confirmation = v5._to_ist_timestamp(row.confirmation_ts)
            idx = int(np.searchsorted(ns, confirmation.value))
            if idx >= len(ns) or ns[idx] != confirmation.value:
                raise AssertionError(f"Exact native confirmation missing: {symbol}/{confirmation}")
            path = minute.iloc[idx+1:idx+1+v5.MAX_FORWARD_BARS].copy()
            path = path.loc[path["ts"].dt.date.eq(confirmation.date()) & path["ts"].dt.strftime("%H%M").le(v5.OFFICIAL_CUTOFF)]
            exact_first = bool(len(path) and path.iloc[0]["ts"] == confirmation + pd.Timedelta(minutes=1))
            exact_terminal = bool(len(path) and path.iloc[-1]["ts"].strftime("%H%M") == v5.OFFICIAL_CUTOFF)
            continuous = bool(len(path) and path["ts"].diff().dropna().eq(pd.Timedelta(minutes=1)).all())
            paths[int(row.sid)] = {"timestamp_ns": path["ts"].astype("int64").to_numpy(), **{key: path[key].to_numpy(float) for key in ("open", "high", "low", "close")}}
            path_quality.append({"sid": int(row.sid), "tradingsymbol": symbol, "day": row.day, "confirmation_ts": confirmation, "path_rows": len(path), "first_forward_minute_present": exact_first, "exact_cutoff_present": exact_terminal, "continuous_one_minute_path": continuous, "path_complete": exact_first and exact_terminal and continuous})
        if number % 25 == 0 or number == total:
            print(f"[V13-v9 DATA] {number}/{total} stocks; {sum(len(pool) for pool in pools):,} observed 5m rows", flush=True)
    all_five = pd.concat(pools, ignore_index=True)
    signals = attach_features(gated, all_five)
    annotated = attach_features(annotated, all_five)
    setup_audit = make_setup_audit(all_five, annotated, signals)
    days = sorted(set().union(*by_month.values()))
    for record in sources:
        path = Path(record["path"])
        if path.is_file() != record["exists"] or (record["exists"] and sha256(path) != record["sha256"]):
            raise RuntimeError(f"Source changed during dataset build: {path}")
    result = {"signals": signals, "annotated": annotated, "all_5m_features": all_five, "setup_audit": setup_audit, "path_quality": pd.DataFrame(path_quality), "eligibility": eligibility}
    for name, frame in result.items():
        common.atomic_write_parquet(frame, output_dir / f"{name}.parquet")
    flat = {f"{sid}_{field}": value for sid, path in paths.items() for field, value in path.items()}
    np.savez_compressed(output_dir / "paths.npz", **flat)
    common.atomic_write_csv(pd.DataFrame(sources), output_dir / "source_manifest.csv")
    common.atomic_write_csv(eligibility, output_dir / "source_session_eligibility.csv")
    manifest = {"schema": SCHEMA_VERSION, "through_day": str(through_day), "source_fingerprint": source_fingerprint, "sources": sources, "days": [str(day) for day in days], "calendar": {month: str(day) for month, day in calendar.items()}, "native_cache_records": cache_records, "rows": {name: len(frame) for name, frame in result.items()}, "eligibility_scope": {"through_day": str(through_day), "rows": len(eligibility), "max_day": str(eligibility["day"].max()) if not eligibility.empty else None, "post_cutoff_rows": 0}, "feature_columns": feature_columns(all_five), "chronology": "5m features end <= signal_ts; exact next1m features end <= confirmation_ts; entry starts strictly after confirmation. Forward OHLC is a separate outcome-only artifact.", "selection_contract": "Exact original V13-v5 higher_frequency strict signal SID and 14 setup eligibility/ranking; V13-v6 portfolio acceptance applied downstream.", "all_5m_scope": "Every complete observed equity 5m bar 09:20-15:15 on source-eligible required-contract sessions. Missing equity bars are not imputed. Missing exact futures OI and 1m confirmation remain explicit NaNs/rejections.", "output_sha256": dataset_output_checksums(output_dir)}
    common.atomic_write_json(manifest_path, manifest)
    result.update(paths=paths, days=days, calendar=calendar, manifest=manifest, output_dir=output_dir)
    return result


def load_dataset(output_dir: Path | str) -> dict[str, Any]:
    """Load an existing dataset only after checking raw and artifact hashes."""
    output_dir = Path(output_dir).resolve()
    manifest = json.loads((output_dir / "dataset_manifest.json").read_text(encoding="utf-8"))
    if manifest["schema"] != SCHEMA_VERSION:
        raise ValueError("Unsupported V13-v9 dataset schema")
    for source in manifest["sources"]:
        path = Path(source["path"])
        if path.is_file() != source["exists"] or (source["exists"] and sha256(path) != source["sha256"]):
            raise ValueError(f"Dataset raw/source hash changed: {path}")
    for name, checksum in manifest["output_sha256"].items():
        if sha256(output_dir / name) != checksum:
            raise ValueError(f"Dataset artifact hash changed: {name}")
    return _read_dataset(output_dir, manifest)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--through-day", default="2026-09-11")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args(argv)
    result = build_dataset(args.output_dir, args.through_day, args.rebuild)
    print(json.dumps({"output_dir": str(args.output_dir), "sessions": len(result["days"]), **result["manifest"]["rows"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
