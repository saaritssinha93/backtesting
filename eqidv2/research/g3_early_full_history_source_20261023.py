"""Verified 57-session prefilter inputs for the isolated early-entry study.

Retains the historical study's symbol cohorts. The eight earliest sessions use
an after-dated monthly universe, explicitly disclosed; no current universe is
silently substituted. Historical minute execution is reconciled to every
consumed archived five-minute and confirmation candle before use.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from functools import lru_cache
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from research import g3_early_entry_study_20261023 as study
import fno_v13_replay_source as source

OUTPUT = ROOT / "outputs/g3_early_entry_20261023/full_history_source"
RAW = Path(r"C:\TradingData\eqidv2")
ARCHIVE_ROOT = RAW / "fno_oi/strategy_research/v13_v10_g_full_history"
ARCHIVES = (
    (ARCHIVE_ROOT / "run_20260925_cutoff_corrected_through_20260923/dataset", "2026-07-29", "2026-09-23"),
    (ARCHIVE_ROOT / "run_20261008_through_20261008_refresh/dataset", "2026-09-24", "2026-10-08"),
)
LEGACY_FIELDS = ["open", "high", "low", "close", "volume", "source_1m_count", "ema9", "ema20", "ema50",
    "prev_close", "price_change_pct", "volume_ratio", "traded_value", "oi", "prev_oi", "oi_change_pct",
    "day", "signal_ts", "confirmation_ts", "hhmm", "hhmm_int", "tradingsymbol", "futures_tradingsymbol",
    "contract_month", "signal_close", "price_source", "oi_source", "data_contract", "confirmation_open",
    "confirmation_high", "confirmation_low", "confirmation_close", "confirmation_volume",
    "confirmation_source_flagged", "v9_1m_ema20", "v9_1m_volume_ratio", "v9_1m_upper_wick_ratio",
    "v9_1m_lower_wick_ratio", "v9_1m_feature_ts", "v9_exact_confirmation_present", "v9_feature_available_ts",
    "body_ratio", "instrument_token", "futures_instrument_token", "exchange", "v9_5m_feature_ts",
    "v9_5m_ema9", "v9_5m_ema20", "v9_5m_ema50", "v9_5m_ema_bull", "v9_5m_ema_bear"]


def _json(path, obj):
    Path(path).write_text(json.dumps(source._json_ready(obj), indent=2), encoding="utf-8")


def _sha(path):
    return source._sha(Path(path))


def _assert_close(left, right, label):
    a = pd.to_numeric(pd.Series(left), errors="coerce").to_numpy(float)
    b = pd.to_numeric(pd.Series(right), errors="coerce").to_numpy(float)
    good = np.isclose(a, b, rtol=1e-7, atol=1e-7, equal_nan=True)
    if not good.all():
        examples = [(float(x), float(y)) for x, y in zip(a[~good][:5], b[~good][:5])]
        raise ValueError(f"Historical input mismatch {label}: {int((~good).sum())} rows, examples={examples}")


def _legacy_frames(days):
    frames, provenance = {}, {}
    for folder, first, last in ARCHIVES:
        manifest_path = folder / "dataset_manifest.json"
        manifest = json.loads(manifest_path.read_text())
        path = folder / "all_5m_features.parquet"
        observed = _sha(path)
        if observed != manifest["output_sha256"][path.name]:
            raise ValueError(f"Historical feature seal mismatch: {path}")
        fields = [c for c in LEGACY_FIELDS if c in pq.ParquetFile(path).schema.names]
        f = pd.read_parquet(path, columns=fields)
        f["day"] = pd.to_datetime(f.day).dt.date
        f = f.loc[f.day.isin(days) & f.day.astype(str).between(first, last) & f.hhmm_int.le(1330)].copy()
        for day, group in f.groupby("day", sort=True):
            frames[day] = group.reset_index(drop=True)
            provenance[day] = dict(feature_source=str(path), feature_source_sha256=observed,
                dataset_manifest=str(manifest_path), dataset_manifest_sha256=_sha(manifest_path),
                source_scope=manifest.get("all_5m_scope"), candidate_pool="ALL_OBSERVED_5M_BARS_BEFORE_OI_AND_SELECTION_FILTERS")
    return frames, provenance


def _price_inventory(summary):
    latest = max(summary["daily_path_evidence"], key=lambda r: r["run_dir"])
    latest_manifest = Path(latest["snapshot_manifest"])
    manifest = json.loads(latest_manifest.read_text())
    if _sha(latest_manifest) != latest["snapshot_manifest_sha256"]:
        raise ValueError("Latest sealed cash-history manifest drift")
    inventory = {}
    for row in manifest["sources"]:
        if row["role"] == "EQUITY_ONE_MINUTE" and row.get("captured"):
            symbol = Path(row["snapshot_relative_path"]).name.removesuffix("_stocks_indicators_1min.parquet")
            inventory[symbol] = (latest_manifest.parent / row["snapshot_relative_path"], row["sha256"], "SEALED_OCT23_FULL_CASH_HISTORY")
    # A historical symbol must retain its own price series, including renamed/removed symbols.
    for older_day in ("2026-10-12", "2026-09-24"):
        old_meta = RAW / f"backtesting_result_v13_v10_g_4/source_cache/v1/{older_day}/manifest.json"
        old = json.loads(old_meta.read_text()); old_path = Path(old["snapshot_manifest"])
        if _sha(old_path) != old["snapshot_manifest_sha256"]:
            raise ValueError("Older cash-history snapshot manifest drift")
        for row in json.loads(old_path.read_text())["sources"]:
            if row["role"] == "EQUITY_ONE_MINUTE" and row.get("captured"):
                symbol = Path(row["snapshot_relative_path"]).name.removesuffix("_stocks_indicators_1min.parquet")
                inventory.setdefault(symbol, (old_path.parent / row["snapshot_relative_path"], row["sha256"], f"SEALED_{older_day}_FULL_CASH_HISTORY"))
    return inventory


def _prepare_legacy_minutes(frames, summary):
    """Read each symbol once, compute causal 1m indicators, and cache day slices."""
    output = OUTPUT / "execution_minutes"
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    code_hash = _sha(__file__)
    wanted = {str(d): sorted(f.tradingsymbol.unique()) for d, f in frames.items()}
    if manifest_path.exists():
        meta = json.loads(manifest_path.read_text())
        if meta.get("loader_sha256") == code_hash and meta.get("day_symbols") == wanted:
            if all((output / f"{d}.parquet").exists() and _sha(output / f"{d}.parquet") == h for d, h in meta["outputs"].items()):
                return output, meta
    inventory = _price_inventory(summary)
    symbol_days = defaultdict(set)
    for day, f in frames.items():
        for symbol in f.tradingsymbol.unique():
            symbol_days[symbol].add(day)
    pieces = defaultdict(list)
    records = []
    for number, (symbol, dates) in enumerate(sorted(symbol_days.items()), 1):
        if symbol in inventory:
            path, expected, policy = inventory[symbol]
        else:
            path = RAW / f"stocks_indicators_1min_eq/{symbol}_stocks_indicators_1min.parquet"
            expected = None
            policy = "RETIRED_SYMBOL_RAW_HISTORY_HASHED_NOW_AND_FULL_OHLC_RECONCILED"
        observed = _sha(path)
        if expected and observed != expected:
            raise ValueError(f"Cash-history source seal mismatch: {symbol}")
        names = pq.ParquetFile(path).schema.names
        cols = ["date", "open", "high", "low", "close", "volume"]
        cols += [x for x in ("gap_filled", "opening_snapshot", "provisional_stale") if x in names]
        m = pd.read_parquet(path, columns=cols)
        m["ts"] = source.hybrid._to_ist(m.date).dt.as_unit("ns")
        m = m.sort_values("ts").reset_index(drop=True)
        if m.ts.duplicated().any():
            raise ValueError(f"Duplicate cash-history timestamp: {symbol}")
        m["v9_1m_ema20_rebuilt"] = pd.to_numeric(m.close).ewm(span=20, adjust=False).mean()
        prev = m.close.shift()
        tr = pd.concat([m.high-m.low, (m.high-prev).abs(), (m.low-prev).abs()], axis=1).max(axis=1)
        m["v9_1m_atr14"] = tr.rolling(14, min_periods=14).mean()
        m["v9_1m_volume_ratio_rebuilt"] = m.volume / m.volume.shift().rolling(20, min_periods=5).mean()
        m["day"] = m.ts.dt.date
        relevant = m.loc[m.day.isin(dates)]
        for day, part in relevant.groupby("day", sort=False):
            expected_times = pd.date_range(f"{day} 09:16", f"{day} 15:15", freq="min", tz="Asia/Kolkata")
            part = part.loc[part.ts.isin(expected_times)].copy()
            if len(part) != 360 or not np.array_equal(expected_times.asi8, pd.DatetimeIndex(part.ts).asi8):
                raise ValueError(f"Incomplete execution OHLC {symbol} {day}: {len(part)}/360")
            for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
                if flag in part and part[flag].astype(str).str.lower().isin(["true", "1", "1.0", "yes", "on"]).any():
                    raise ValueError(f"Flagged historical execution data {symbol} {day}: {flag}")
            values = part[["open", "high", "low", "close", "volume"]].to_numpy(float)
            if not np.isfinite(values).all() or (values[:, :4] <= 0).any():
                raise ValueError(f"Invalid execution OHLC {symbol} {day}")
            part["tradingsymbol"] = symbol
            pieces[day].append(part.drop(columns=["date"], errors="ignore"))
        records.append(dict(symbol=symbol, path=str(path), sha256=observed, policy=policy,
            first_date=str(min(dates)), last_date=str(max(dates))))
        if number % 40 == 0:
            print(f"[FULL SOURCE] cash history {number}/{len(symbol_days)}", flush=True)
    outputs = {}
    for day, requested in frames.items():
        minute = pd.concat(pieces[day], ignore_index=True)
        if set(minute.tradingsymbol) != set(requested.tradingsymbol):
            raise ValueError(f"Missing cash symbols for {day}")
        path = output / f"{day}.parquet"
        minute.to_parquet(path, index=False)
        outputs[str(day)] = _sha(path)
    meta = dict(loader_sha256=code_hash, day_symbols=wanted, outputs=outputs, sources=records,
        chronology="Indicators computed with causal pandas ewm/shift/rolling on completed observations only; execution day slices never influence past features.")
    _json(manifest_path, meta)
    return output, meta


@lru_cache(maxsize=700)
def _future(contract):
    path = RAW / f"fno_oi/raw_contracts_5m/{source.common.safe_contract_stem(contract)}_5minute.parquet"
    frame = pd.read_parquet(path)
    frame["ts"] = source.hybrid._to_ist(frame.timestamp).dt.as_unit("ns")
    return frame, dict(path=str(path), sha256=_sha(path))


def _membership(day, features):
    folder = RAW / "fno_oi/universe"
    exact = folder / f"near_month_{day}.parquet"
    policy = "EXACT_DATE_UNIVERSE_VALIDATION"
    chosen = exact
    if not exact.exists():
        earlier = sorted(p for p in folder.glob("near_month_*.parquet") if p.stem.removeprefix("near_month_") <= str(day))
        if earlier:
            chosen = earlier[-1]
            policy = "LAST_OBSERVED_UNIVERSE_BEFORE_SESSION_VALIDATION"
        else:
            chosen = folder / "near_month_2026-08-21.parquet"
            policy = "LEGACY_AFTER_DATED_MONTHLY_UNIVERSE_NOT_POINT_IN_TIME"
    u = pd.read_parquet(chosen)
    expected = set(u.loc[~u.is_index_future, "underlying"].astype(str))
    observed = set(features.tradingsymbol.astype(str))
    return dict(policy=policy, path=str(chosen), sha256=_sha(chosen),
        feature_symbols=len(observed), universe_symbols=len(expected),
        missing_from_features=sorted(expected-observed), extra_in_features=sorted(observed-expected),
        historical_cohort_preserved=True, strict_point_in_time_universe=policy=="EXACT_DATE_UNIVERSE_VALIDATION")


def _baseline_parity(day, features, history_orders):
    computed = study.contract.select_orders(features)
    expected = history_orders.loc[history_orders.day.eq(day)]
    keys = ["setup_id", "tradingsymbol"]
    a = computed.set_index(keys) if len(computed) else pd.DataFrame()
    b = expected.set_index(keys) if len(expected) else pd.DataFrame()
    ak = set(a.index) if len(a) else set(); bk = set(b.index) if len(b) else set()
    differences = []
    for key in sorted(ak & bk):
        for field in ("trigger", "native_target_pct"):
            av = float(a.loc[key, field]); bv = float(b.loc[key, field])
            if not np.isclose(av, bv, rtol=1e-7, atol=1e-7):
                differences.append(dict(key=list(key), field=field, reconstructed=av, legacy=bv))
    return dict(reconstructed_orders=len(computed), legacy_orders=len(expected),
        matching=ak==bk and not differences,
        added=[list(k) for k in sorted(ak-bk)], missing=[list(k) for k in sorted(bk-ak)],
        field_differences=differences)


def _finish_legacy(day, features, minute, proof, history_orders):
    f = features.copy().reset_index(drop=True)
    for key in ("signal_ts", "confirmation_ts", "v9_5m_feature_ts", "v9_1m_feature_ts", "v9_feature_available_ts"):
        if key in f:
            f[key] = pd.to_datetime(f[key], utc=True).dt.tz_convert("Asia/Kolkata")
    if f.duplicated(["tradingsymbol", "signal_ts"]).any():
        raise ValueError(f"Duplicate historical feature row {day}")
    counts = f.groupby("tradingsymbol").size()
    if not counts.eq(51).all() or not f.source_1m_count.eq(5).all() or not f.v9_exact_confirmation_present.all():
        raise ValueError(f"Incomplete unfiltered 51-clock pool {day}")
    m = minute.copy()
    m["five_ts"] = m.ts.dt.ceil("5min")
    bars = m.groupby(["tradingsymbol", "five_ts"], sort=False).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum")).reset_index()
    checked = f.merge(bars, left_on=["tradingsymbol", "signal_ts"], right_on=["tradingsymbol", "five_ts"], suffixes=("", "_raw"), validate="one_to_one")
    if len(checked) != len(f):
        raise ValueError(f"Missing five-minute price reconciliation {day}")
    for field in ("open", "high", "low", "close", "volume"):
        _assert_close(checked[field], checked[field+"_raw"], f"{day} 5m {field}")
    conf = m.rename(columns={c: "raw_"+c for c in ("open", "high", "low", "close", "volume")})
    checked = f.merge(conf, left_on=["tradingsymbol", "confirmation_ts"], right_on=["tradingsymbol", "ts"], suffixes=("", "_raw"), validate="one_to_one")
    if len(checked) != len(f):
        raise ValueError(f"Missing confirmation price reconciliation {day}")
    for field in ("open", "high", "low", "close", "volume"):
        _assert_close(checked["confirmation_"+field], checked["raw_"+field], f"{day} 1m {field}")
    _assert_close(checked.v9_1m_volume_ratio, checked.v9_1m_volume_ratio_rebuilt, f"{day} confirmation rolling-volume")
    # ATR is not present in the old all-bar archive but is consumed by current 10:36 setup guards.
    f["v9_1m_atr14"] = checked.v9_1m_atr14.to_numpy()
    if "v9_1m_ema20" not in f:
        f["v9_1m_ema20"] = checked.v9_1m_ema20_rebuilt.to_numpy()
    opening_proofs = []; opening_invalid = []
    for ix in f.index[f.hhmm_int.eq(920)]:
        row = f.loc[ix]
        future, fp = _future(str(row.futures_tradingsymbol))
        opening = source.opening_inputs.build_opening_features(row, future, day=day,
            symbol=row.tradingsymbol, contract=row.futures_tradingsymbol,
            expiry=future.expiry.iloc[-1], root=RAW / "dashboard_flow_observer")
        if opening.get("opening_0920_status") != "VALID":
            # Missing OI must fail closed in baseline, but must not hide a real
            # cash-price candidate from experiments that explicitly ignore OI.
            opening_invalid.append(str(row.tradingsymbol))
            opening.update(price_change_pct=(float(row.close)/float(row.open)-1)*100,
                price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE")
        for key, value in opening.items():
            if key not in f:
                f[key] = None
            if key in ("price_change_pct", "oi_change_pct", "prev_oi"):
                f[key] = pd.to_numeric(f[key], errors="coerce").astype(float)
            f.at[ix, key] = value
        ep = RAW / f"dashboard_flow_observer/historical/{day}/{row.futures_tradingsymbol}.json"
        opening_proofs.append(dict(symbol=row.tradingsymbol, observer_path=str(ep),
            observer_sha256=_sha(ep) if ep.exists() else None,
            opening_status=opening.get("opening_0920_status"), futures=fp))
    month = str(f.contract_month.mode().iloc[0])
    nf, nf_proof = _future(f"NIFTY{month}FUT")
    nr = nf.loc[nf.ts.eq(pd.Timestamp(f"{day} 09:20", tz="Asia/Kolkata"))]
    if len(nr) != 1:
        raise ValueError(f"Missing unique Nifty opening bar {day}")
    f["nifty_first_bar_return_pct"] = (float(nr.close.iloc[0])/float(nr.open.iloc[0])-1)*100
    proof = dict(proof, day=str(day), loader_sha256=_sha(__file__),
        observed_rows=len(f), stocks=f.tradingsymbol.nunique(),
        missing_oi_rows=int(f.oi_change_pct.isna().sum()),
        nonpositive_oi_rows=int(f.oi_change_pct.le(0).sum()),
        minute_ohlcv_reconciled=True, universe=_membership(day, f),
        opening_policy="Native cash09:15open-to09:20close and futures09:16-to09:20 OI, retrospective historical observer; replaces legacy overnight opening comparisons.",
        opening_provenance=opening_proofs, invalid_opening_oi_symbols=opening_invalid,
        nifty_provenance=nf_proof,
        baseline_selection_parity=_baseline_parity(day, f, history_orders))
    return f, proof


def _later_rebuild(day, history_orders):
    """Oct9/12: raw snapshots and last earlier archive EMA seed, never a later seed."""
    meta_path = RAW / f"backtesting_result_v13_v10_g_4/source_cache/v1/{day}/manifest.json"
    meta = json.loads(meta_path.read_text())
    manifest = Path(meta["snapshot_manifest"])
    if _sha(manifest) != meta["snapshot_manifest_sha256"]:
        raise ValueError(f"Late snapshot seal mismatch {day}")
    snap = manifest.parent
    # These older seals predate the current fingerprint serialization policy.
    # Their manifest bytes are pinned above; verify every captured member by
    # its pinned SHA instead of comparing against a newer schema's fingerprint.
    manifest_payload = json.loads(manifest.read_text())
    for record in manifest_payload["sources"]:
        member = (snap / record["snapshot_relative_path"]).resolve()
        member.relative_to(snap.resolve())
        if record.get("captured") and _sha(member) != record["sha256"]:
            raise ValueError(f"Older snapshot member seal mismatch: {member}")
    run = snap.parent.parent
    old_path = run / "feature_ledger.csv"
    old_manifest_path = run / "feature_ledger.csv.manifest.json"
    if old_manifest_path.exists():
        expected_seed_hash = json.loads(old_manifest_path.read_text())["artifact_sha256"]
    else:
        expected_seed_hash = json.loads((run / "run_manifest.json").read_text())["artifacts"]["feature_ledger"]["sha256"]
    if _sha(old_path) != expected_seed_hash:
        raise ValueError(f"Older complete gate-ledger seal mismatch {day}")
    old = pd.read_csv(old_path)
    old["hhmm_int"] = pd.to_datetime(old.signal_ts).dt.strftime("%H%M").astype(int)
    seed_archive = ARCHIVES[1][0] / "all_5m_features.parquet"
    seeds = pd.read_parquet(seed_archive, columns=["day", "tradingsymbol", "signal_ts", "close", "ema9", "ema20", "ema50"])
    seeds["day"] = pd.to_datetime(seeds.day).dt.date
    seeds = seeds.loc[seeds.day.lt(day)].sort_values("signal_ts").groupby("tradingsymbol",sort=False).tail(1).set_index("tradingsymbol")
    universe_record = next(r for r in json.loads(manifest.read_text())["sources"] if r["role"] == "DATED_UNIVERSE")
    universe = pd.read_parquet(snap / universe_record["snapshot_relative_path"])
    expected_symbols = set(universe.loc[~universe.is_index_future,"underlying"])
    missing_symbols = expected_symbols-set(old.tradingsymbol)
    historical_exclusions = []
    if (run/"run_manifest.json").exists():
        historical_exclusions = json.loads((run/"run_manifest.json").read_text()).get("coverage",{}).get("excluded_stocks",[])
    documented = {r["symbol"] for r in historical_exclusions}
    if missing_symbols-documented:
        raise ValueError(f"Undocumented omitted historical universe stocks {day}: {sorted(missing_symbols-documented)}")
    frames = []; minutes = []; problems=[]
    for number, (symbol, seed) in enumerate(old.groupby("tradingsymbol", sort=True), 1):
        first = seed.sort_values("signal_ts").iloc[0]
        earlier = seeds.loc[symbol]
        prior_clock = pd.Timestamp(earlier.signal_ts)
        if prior_clock.date() >= day:
            raise ValueError(f"Noncausal EMA seed {day} {symbol}")
        before = len(problems)
        mp = snap / f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
        start = pd.Timestamp(day,tz="Asia/Kolkata")-pd.Timedelta(days=10)
        cutoff = pd.Timestamp(f"{day} 15:15",tz="Asia/Kolkata")
        names = pq.ParquetFile(mp).schema.names
        fields = ["date","open","high","low","close","volume"] + [x for x in ("gap_filled","opening_snapshot","provisional_stale") if x in names]
        minute = pd.read_parquet(mp,columns=fields,filters=[("date",">=",start),("date","<=",cutoff)])
        minute["ts"] = source.hybrid._to_ist(minute.date).dt.as_unit("ns")
        minute = minute.sort_values("ts").reset_index(drop=True)
        if minute.ts.duplicated().any():
            raise ValueError(f"Duplicate minute {day} {symbol}")
        future_symbol = str(first.futures_tradingsymbol)
        fp = snap / f"futures_5m/{source.common.safe_contract_stem(future_symbol)}_5minute.parquet"
        future = source._load_future(fp, day, problems, symbol)
        source._coverage(minute, future, day, symbol, problems, signal_clocks=study.CLOCKS)
        if len(problems) > before:
            raise ValueError(f"Incomplete late-source data {day} {symbol}: {problems[before:]}")
        frame = source._observed_pool(minute, future, day=day, symbol=symbol,
            future_symbol=future_symbol, month=future_symbol[-8:-3], signal_clocks=study.CLOCKS)
        following = source.hybrid.aggregate_equity_one_minute_to_five_minute(minute)
        following = following.loc[following.ts.gt(prior_clock)].sort_values("ts")
        for span in (9,20,50):
            value = float(earlier[f"ema{span}"]);alpha=2/(span+1);values={}
            for bar in following.itertuples(index=False):
                value=(1-alpha)*value+alpha*float(bar.close);values[bar.ts]=value
            frame[f"ema{span}"]=frame.signal_ts.map(values)
            frame[f"v9_5m_ema{span}"]=frame[f"ema{span}"]
            overlap=frame.merge(seed[["signal_ts",f"ema{span}"]].assign(signal_ts=lambda x:pd.to_datetime(x.signal_ts)),on="signal_ts",suffixes=("","_sealed"))
            _assert_close(overlap[f"ema{span}"],overlap[f"ema{span}_sealed"],f"{day} {symbol} causalEMA{span}")
        frame["v9_5m_ema_bull"]=(frame.ema9>frame.ema20)&(frame.ema20>frame.ema50)
        frame["v9_5m_ema_bear"]=(frame.ema9<frame.ema20)&(frame.ema20<frame.ema50)
        ur = universe.loc[universe.underlying.eq(symbol)].iloc[0]
        frame["instrument_token"]=ur.get("equity_instrument_token",np.nan)
        frame["futures_instrument_token"]=ur.get("futures_instrument_token",ur.instrument_token)
        frame["exchange"]="NSE"
        frames.append(frame)
        m = minute.loc[minute.ts.dt.date.eq(day)].copy()
        m["tradingsymbol"] = symbol
        # _finish_legacy will compare and reuse the exact rebuilt1m rolling values.
        oneclose = pd.to_numeric(minute.close)
        tr = pd.concat([minute.high-minute.low,(minute.high-oneclose.shift()).abs(),(minute.low-oneclose.shift()).abs()],axis=1).max(axis=1)
        minute["v9_1m_atr14"] = tr.rolling(14,min_periods=14).mean()
        minute["v9_1m_ema20_rebuilt"] = oneclose.ewm(span=20,adjust=False).mean()
        minute["v9_1m_volume_ratio_rebuilt"] = minute.volume/minute.volume.shift().rolling(20,min_periods=5).mean()
        m = minute.loc[minute.ts.dt.date.eq(day)].copy();m["tradingsymbol"]=symbol
        minutes.append(m)
        if number % 50 == 0:
            print(f"[FULL SOURCE] later day {day}: {number} symbols", flush=True)
    f=pd.concat(frames,ignore_index=True);m=pd.concat(minutes,ignore_index=True)
    proof=dict(feature_source="FULL_RAW_RECONSTRUCTION", snapshot_manifest=str(manifest),
        snapshot_manifest_sha256=_sha(manifest), opening_seed_source=str(old_path),
        opening_seed_sha256=_sha(old_path), coverage_problems=problems,
        ema_seed_policy="Last earlier archived completed5mEMA then exact recurrence on subsequent raw completed bars; validated against all overlapping sealed gate-ledger EMA rows.",
        historical_stock_day_exclusions=historical_exclusions,
        historical_exclusion_note="Source's after-close stock-day eligibility is inherited for baseline comparability, not claimed as a live causal filter. All omissions are explicitly reported.",
        candidate_pool="ALL51_CLOCKS_BEFORE_SELECTION_FOR_SOURCE_ELIGIBLE_DATED_UNIVERSE_WITH_DISCLOSED_HISTORICAL_EXCLUSIONS")
    f,proof=_finish_legacy(day,f,m,proof,history_orders)
    return f,m,proof


def iter_days(days=None):
    """Yield(day,features,minute_frame,provenance), with no live side effects."""
    OUTPUT.mkdir(parents=True,exist_ok=True)
    summary=json.loads((study.HISTORY/"summary.json").read_text())
    requested=[date.fromisoformat(d) for d in summary["days"]]
    if days is not None:
        chosen={date.fromisoformat(str(d)) for d in days};requested=[d for d in requested if d in chosen]
    history_orders=pd.read_csv(study.HISTORY/"selected_orders.csv")
    history_orders["day"]=pd.to_datetime(history_orders.day).dt.date
    old,proofs=_legacy_frames(requested)
    minute_folder,minute_meta=_prepare_legacy_minutes(old,summary) if old else (None,None)
    daily={date.fromisoformat(Path(e["run_dir"]).parent.name):e for e in summary["daily_path_evidence"]}
    all_proof=[]
    for day in requested:
        folder=OUTPUT/"cache"/str(day);folder.mkdir(parents=True,exist_ok=True)
        fp=folder/"features.parquet";mp=folder/"minutes.parquet";jp=folder/"provenance.json"
        if jp.exists() and fp.exists() and mp.exists():
            saved=json.loads(jp.read_text())
            if saved.get("loader_sha256")==_sha(__file__) and saved.get("features_sha256")==_sha(fp) and saved.get("minutes_sha256")==_sha(mp):
                f=pd.read_parquet(fp);m=pd.read_parquet(mp);all_proof.append(saved)
                yield day,f,m,saved
                continue
        if day in old:
            m=pd.read_parquet(minute_folder/f"{day}.parquet")
            proof=dict(proofs[day],execution_source_manifest=str(minute_folder/"manifest.json"),
                execution_source_manifest_sha256=_sha(minute_folder/"manifest.json"))
            f,proof=_finish_legacy(day,old[day],m,proof,history_orders)
        elif day in daily:
            _,f,m,proof=study.build_day(daily[day],study.OUTPUT)
            proof=dict(proof,loader_sha256=_sha(__file__),day=str(day),universe=_membership(day,f),
                baseline_selection_parity=_baseline_parity(day,f,history_orders))
        else:
            f,m,proof=_later_rebuild(day,history_orders)
        f.to_parquet(fp,index=False);m.to_parquet(mp,index=False)
        proof.update(features_sha256=_sha(fp),minutes_sha256=_sha(mp))
        _json(jp,proof);all_proof.append(proof)
        print(f"[FULL SOURCE] ready {day}: {len(f)} rows; baseline keys match={proof['baseline_selection_parity']['matching']}",flush=True)
        _json(OUTPUT/"provenance.json",all_proof)
        yield day,f,m,proof
    _json(OUTPUT/"provenance.json",all_proof)


if __name__=="__main__":
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument("--days",nargs="*")
    args=parser.parse_args()
    for day,features,minute,proof in iter_days(args.days):
        print(day,len(features),len(minute),json.dumps(proof["baseline_selection_parity"]),flush=True)
