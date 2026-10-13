"""Rebuild G-4's six late morning clocks from G-3's sealed market inputs.

This is an offline, backtest-only source builder. It does not change the G-3
contract, production configuration, dashboard, or any latest pointer.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
import fno_v13_v10_g_3_daily_replay as replay
from research.g3_0950_long_backtest import (
    DEFAULT_HISTORY_PROVENANCE,
    DEFAULT_RECENT_PROVENANCE,
    normalize_signals,
)
from fno_v13_v10_g_3_backtest import OCTOBER12_SOURCE


LATE_CLOCKS = ("10:05", "10:10", "10:15", "10:20", "10:25", "10:30")
LATE_INTS = {int(clock.replace(":", "")) for clock in LATE_CLOCKS}
BASE_G3_V2_CLOCKS = ("09:25", "09:30", "09:35", "09:40", "09:45",
                     "09:50", "09:55", "10:00", "11:20")
CLOCK_POLICY = "G3_V2_PLUS_SIX_LATE_V1"
DEFAULT_CACHE = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\source_cache\v1")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def expanded_source_clocks(clocks=LATE_CLOCKS):
    """Use the pinned G3 V2 clocks plus G4's six clocks in this process only."""
    original = source.config.SIGNAL_TO_CONFIRMATION
    source.config.SIGNAL_TO_CONFIRMATION = {
        clock: f"{clock[:3]}{int(clock[3:]) + 1:02d}"
        for clock in (*BASE_G3_V2_CLOCKS, *clocks)
    }
    try:
        yield
    finally:
        source.config.SIGNAL_TO_CONFIRMATION = original


def _archive_paths(archive: Path, sids: set[int]) -> dict[int, dict]:
    paths: dict[int, dict] = {}
    with np.load(archive, allow_pickle=False) as packed:
        for name in packed.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in sids:
                paths.setdefault(sid, {})[field] = packed[name]
    if set(paths) != sids:
        raise RuntimeError(f"Historical source lacks {len(sids - set(paths))} execution paths")
    return paths


def _write_paths(path: Path, paths: dict[int, dict]) -> None:
    arrays = {f"{sid}_{field}": value for sid, fields in paths.items()
              for field, value in fields.items()}
    np.savez_compressed(path, **arrays)


def _read_paths(path: Path) -> dict[int, dict]:
    paths: dict[int, dict] = {}
    with np.load(path, allow_pickle=False) as packed:
        for name in packed.files:
            sid_text, field = name.split("_", 1)
            paths.setdefault(int(sid_text), {})[field] = packed[name]
    return paths


def _oi_can_pass(path: Path, day: date, clocks=LATE_CLOCKS) -> bool:
    """Safe necessary gate: no qualifying OI at any new clock means no setup."""
    frame = pd.read_parquet(path, columns=["timestamp", "oi"])
    frame["ts"] = source.hybrid._to_ist(frame["timestamp"])
    frame = frame.loc[frame.ts.le(pd.Timestamp(source.slot_datetime(day, "15:15")))]
    frame = frame.sort_values("ts", kind="stable").drop_duplicates("ts", keep="last")
    frame["previous_oi"] = pd.to_numeric(frame.oi, errors="coerce").shift(1)
    requested = frame.loc[frame.ts.dt.date.eq(day) & frame.ts.dt.strftime("%H:%M").isin(clocks)]
    if requested.empty:
        return False
    oi = pd.to_numeric(requested.oi, errors="coerce")
    previous = pd.to_numeric(requested.previous_oi, errors="coerce")
    change = (oi / previous - 1) * 100
    return bool((oi.gt(0) & previous.gt(0) & change.between(.05, 1.)).any())


def _rebuild_day(day: date, roots: source.DataRoots, clocks=LATE_CLOCKS) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict]:
    """Use raw inputs, skipping stock-days that cannot meet the common OI gate."""
    universe = pd.read_parquet(roots.universe / f"near_month_{day}.parquet")
    stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)]
    expiries = pd.to_datetime(stocks.expiry, errors="coerce").dropna().unique()
    if len(expiries) != 1:
        raise RuntimeError(f"G4 {day} dated universe expiry is ambiguous")
    month = pd.Timestamp(expiries[0]).strftime("%y%b").upper()
    mapped, _ = source.provenance.load_backtest_universe(
        universe_path=roots.universe / f"near_month_{day}.parquet", universe_date=day,
        contract_month_contains=month, require_persisted_mapping=True)
    if mapped.equity_symbol.duplicated().any():
        raise RuntimeError(f"G4 {day} dated universe has duplicate equity mapping")
    observed = []
    minutes: dict[str, pd.DataFrame] = {}
    excluded = []
    shortlisted = 0
    for number, contract_row in enumerate(mapped.to_dict("records"), 1):
        symbol = source.hybrid.resolve_backtest_equity_symbol(
            str(contract_row["equity_symbol"]), root=roots.equity_1m)
        future_symbol = str(contract_row["futures_tradingsymbol"])
        future_path = roots.futures_5m / f"{source.common.safe_contract_stem(future_symbol)}_5minute.parquet"
        if not _oi_can_pass(future_path, day, clocks):
            continue
        shortlisted += 1
        problems: list[dict] = []
        minute = source._load_minute(source.hybrid.equity_one_minute_path(symbol, roots.equity_1m),
                                      day, problems, symbol)
        future = source._load_future(future_path, day, problems, symbol)
        full_minute, full_future = minute, future
        # Ninety calendar days contain far more than the 50-bar EMA and 20-bar
        # volume warmups. Earlier history has a negligible EMA weight, while
        # native source quality for these days was already checked by G3.
        warmup_start = pd.Timestamp(source.slot_datetime(day - timedelta(days=90), "09:16"))
        minute = minute.loc[minute.ts.ge(warmup_start)].reset_index(drop=True)
        future = future.loc[future.ts.ge(warmup_start)].reset_index(drop=True)
        with expanded_source_clocks(clocks):
            source._coverage(minute, future, day, symbol, problems)
        reasons = {item["reason"] for item in problems}
        if reasons & source.EXCLUDABLE_COVERAGE_REASONS:
            excluded.append(dict(symbol=symbol, reasons=sorted(reasons & source.EXCLUDABLE_COVERAGE_REASONS)))
            continue
        if problems:
            raise RuntimeError(f"G4 {day} invalid source for {symbol}: {problems[:3]}")
        original = source.config.SIGNAL_TO_CONFIRMATION
        source.config.SIGNAL_TO_CONFIRMATION = {clock: _confirmation for clock, _confirmation in
            ((clock, f"{clock[:3]}{int(clock[3:]) + 1:02d}") for clock in clocks)}
        try:
            pool = source._observed_pool(minute, future, day=day, symbol=symbol,
                                         future_symbol=future_symbol, month=month)
        finally:
            source.config.SIGNAL_TO_CONFIRMATION = original
        if len(pool) != len(clocks):
            raise RuntimeError(f"G4 {day} late-clock construction incomplete for {symbol}")
        if shortlisted == 1:
            source.config.SIGNAL_TO_CONFIRMATION = {
                clock: f"{clock[:3]}{int(clock[3:]) + 1:02d}" for clock in clocks}
            try:
                untrimmed = source._observed_pool(full_minute, full_future, day=day,
                                                  symbol=symbol, future_symbol=future_symbol,
                                                  month=month)
            finally:
                source.config.SIGNAL_TO_CONFIRMATION = original
            for field in ("ema9", "ema20", "ema50", "price_change_pct", "volume_ratio",
                          "oi_change_pct", "v9_1m_volume_ratio"):
                if not np.allclose(pool[field].to_numpy(float), untrimmed[field].to_numpy(float),
                                   rtol=0, atol=1e-9, equal_nan=True):
                    raise RuntimeError(f"G4 {day} 90-day warmup parity failed: {field}")
        pool["instrument_token"] = int(contract_row["equity_instrument_token"])
        pool["futures_instrument_token"] = int(contract_row["futures_instrument_token"])
        pool["exchange"] = "NSE"
        strict = source._strict_signals(pool, 0.)
        if len(strict):
            observed.append(strict)
            minutes[symbol] = minute.loc[minute.ts.dt.date.eq(day)].copy()
        if number % 50 == 0:
            print(f"[G4 raw source] {day}: inspected {number}/{len(mapped)}, OI shortlist {shortlisted}", flush=True)
    candidates = pd.concat(observed, ignore_index=True, sort=False) if observed else source._empty_signals()
    return candidates, minutes, dict(universe_count=len(mapped), oi_shortlist=shortlisted,
                                      excluded_stocks=excluded)


def _day_cache(day: date, snapshot: Path, cache: Path, ordinal: int,
               clocks=LATE_CLOCKS, sid_base=4_000_000,
               verify_snapshot=False) -> tuple[pd.DataFrame, dict[int, dict]]:
    policy = CLOCK_POLICY if tuple(clocks) == LATE_CLOCKS and sid_base == 4_000_000 else (
        "G4_CLOCKS_" + "_".join(clocks) + f"_SID_{sid_base}_V1")
    folder = cache / str(day)
    candidate_path = folder / "candidates.parquet"
    paths_path = folder / "paths.npz"
    manifest_path = folder / "manifest.json"
    snapshot_manifest = snapshot / "snapshot_manifest.json"
    if verify_snapshot:
        # The enclosing loader has already verified the archived manifest hash.
        # Old snapshot fingerprints used an earlier schema; verify the actual
        # archived members instead of recomputing them with the current schema.
        proof = json.loads(snapshot_manifest.read_text(encoding="utf-8"))
        for item in proof["sources"]:
            if not item.get("captured"):
                continue
            member = (snapshot / item["snapshot_relative_path"]).resolve()
            if (not member.is_relative_to(snapshot.resolve()) or not member.is_file()
                    or sha256(member) != item["sha256"]):
                raise ValueError(f"G4 {day} archived snapshot member changed: {member.name}")
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest.get("day") == str(day)
                and manifest.get("clock_policy") == policy
                and manifest.get("complete") is True
                and manifest.get("snapshot_manifest_sha256") == sha256(snapshot_manifest)
                and manifest.get("candidate_sha256") == sha256(candidate_path)
                and manifest.get("paths_sha256") == sha256(paths_path)):
            candidates = pd.read_parquet(candidate_path)
            paths = _read_paths(paths_path)
            if set(candidates.sid.astype(int)) != set(paths):
                raise RuntimeError(f"G4 cached candidate/path mismatch for {day}")
            return candidates, paths
    universe_root = snapshot / "universe"
    if day == date(2026, 9, 24) and not (universe_root / f"near_month_{day}.parquet").is_file():
        history = json.loads(DEFAULT_HISTORY_PROVENANCE.read_text(encoding="utf-8"))
        entry = next(item for item in history["extensions"] if item["day"] == str(day))
        original = json.loads((Path(entry["run"]) / "source_manifest.json").read_text(encoding="utf-8"))
        record = next(item for item in original["sources"] if item["role"] == "DATED_UNIVERSE")
        universe_path = Path(record["path"])
        if sha256(universe_path) != record["sha256"]:
            raise RuntimeError("G4 September 24 dated-universe source drift")
        universe_root = universe_path.parent
    roots = source.DataRoots(universe=universe_root,
                             equity_1m=snapshot / "equity_1m",
                             futures_5m=snapshot / "futures_5m")
    rebuilt, minutes, coverage = _rebuild_day(day, roots, clocks)
    if coverage["universe_count"] - len(coverage["excluded_stocks"]) <= 0:
        raise RuntimeError(f"G4 {day} raw source has no eligible stocks")
    candidates = normalize_signals(rebuilt)
    candidates = candidates.loc[candidates.hhmm_int.isin({int(c.replace(':', '')) for c in clocks})].copy().reset_index(drop=True)
    candidates["sid"] = np.arange(sid_base + ordinal * 10_000,
                                  sid_base + ordinal * 10_000 + len(candidates))
    paths = source.selected_paths(candidates, minutes, day)
    if set(candidates.sid.astype(int)) != set(paths):
        raise RuntimeError(f"G4 reconstructed candidate/path mismatch for {day}")
    folder.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(candidate_path, index=False)
    _write_paths(paths_path, paths)
    manifest = dict(day=str(day), clock_policy=policy,
                    snapshot_manifest=str(snapshot_manifest),
                    snapshot_manifest_sha256=sha256(snapshot_manifest),
                    candidate_sha256=sha256(candidate_path), paths_sha256=sha256(paths_path),
                    candidate_count=len(candidates), excluded_stocks=len(coverage["excluded_stocks"]),
                    universe_count=coverage["universe_count"], oi_shortlist=coverage["oi_shortlist"], complete=True,
                    scope="BACKTEST_ONLY")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return candidates, paths


def _source_days_and_snapshots() -> tuple[list[date], dict[date, Path], dict]:
    history = json.loads(DEFAULT_HISTORY_PROVENANCE.read_text(encoding="utf-8"))
    recent = json.loads(DEFAULT_RECENT_PROVENANCE.read_text(encoding="utf-8"))
    sealed_days = json.loads((replay.DEFAULT_SEALED_SOURCE / "dataset_manifest.json")
                             .read_text(encoding="utf-8"))["days"]
    if history["days"][:len(sealed_days)] != sealed_days or len(history["days"]) != 46:
        raise RuntimeError("G4 historical source session list changed")
    days = [date.fromisoformat(day) for day in history["days"]]
    days.extend(date.fromisoformat(day) for day in ("2026-10-08", "2026-10-09", "2026-10-12"))
    if len(days) != 49 or days != sorted(set(days)):
        raise RuntimeError("G4 requires 49 ordered complete sessions")
    snapshots: dict[date, Path] = {}
    for item in history["extensions"]:
        day, snapshot = date.fromisoformat(item["day"]), Path(item["snapshot"])
        manifest = snapshot / "snapshot_manifest.json"
        if sha256(manifest) != history["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"G4 historical raw snapshot drift: {day}")
        snapshots[day] = snapshot
    for day_text in ("2026-10-08", "2026-10-09"):
        item = recent["recent_sources"][day_text]
        manifest = Path(item["snapshot_manifest"])
        if sha256(manifest) != item["snapshot_manifest_sha256"]:
            raise RuntimeError(f"G4 recent raw snapshot drift: {day_text}")
        snapshots[date.fromisoformat(day_text)] = manifest.parent
    oct12_source = json.loads((OCTOBER12_SOURCE / "source_manifest.json")
                              .read_text(encoding="utf-8"))
    oct12_manifest = Path(oct12_source["snapshot_manifest"])
    if sha256(oct12_manifest) != oct12_source["snapshot_manifest_sha256"]:
        raise RuntimeError("G4 October 12 raw snapshot drift")
    snapshots[date(2026, 10, 12)] = oct12_manifest.parent
    if set(snapshots) != set(days[len(sealed_days):]):
        raise RuntimeError("G4 recent source snapshot inventory incomplete")
    evidence = dict(history_provenance=str(DEFAULT_HISTORY_PROVENANCE),
                    history_provenance_sha256=sha256(DEFAULT_HISTORY_PROVENANCE),
                    recent_provenance=str(DEFAULT_RECENT_PROVENANCE),
                    recent_provenance_sha256=sha256(DEFAULT_RECENT_PROVENANCE),
                    october12_source=str(OCTOBER12_SOURCE),
                    october12_source_manifest_sha256=sha256(OCTOBER12_SOURCE / "source_manifest.json"))
    return days, snapshots, evidence


def load_late_source(cache: Path = DEFAULT_CACHE, *, clocks=LATE_CLOCKS,
                     sid_base=4_000_000, verify_snapshots=False) -> tuple[pd.DataFrame, dict[int, dict], list[date], dict, dict[date, Path]]:
    """Return complete, verified 49-session late-clock candidate/path inputs."""
    days, snapshots, provenance = _source_days_and_snapshots()
    sealed_root = replay.DEFAULT_SEALED_SOURCE
    manifest = json.loads((sealed_root / "dataset_manifest.json").read_text(encoding="utf-8"))
    for name in ("signals.parquet", "paths.npz"):
        if sha256(sealed_root / name) != manifest["output_sha256"][name]:
            raise RuntimeError(f"Sealed G4 source drift: {name}")
    historical = pd.read_parquet(sealed_root / "signals.parquet")
    historical = historical.loc[historical.hhmm_int.isin({int(c.replace(':', '')) for c in clocks})].copy()
    historical = normalize_signals(historical)
    historical_paths = _archive_paths(sealed_root / "paths.npz", set(historical.sid.astype(int)))
    sealed_days = [date.fromisoformat(day) for day in manifest["days"]]
    if set(pd.to_datetime(historical.day).dt.date) - set(sealed_days):
        raise RuntimeError("G4 sealed candidates escaped historical sessions")
    print(f"G4 sealed late-clock candidates: {len(historical)} / {len(sealed_days)} sessions", flush=True)
    frames = [historical]
    paths = historical_paths
    recent = days[len(sealed_days):]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        pending = {executor.submit(_day_cache, day, snapshots[day], Path(cache), ordinal, tuple(clocks), sid_base, verify_snapshots): day
                   for ordinal, day in enumerate(recent, 1)}
        for task in as_completed(pending):
            day = pending[task]
            frame, day_paths = task.result()
            results[day] = (frame, day_paths)
            print(f"G4 rebuilt {day}: {len(frame)} late-clock candidates", flush=True)
    for day in recent:
        frame, day_paths = results[day]
        frames.append(frame)
        if set(paths) & set(day_paths):
            raise RuntimeError("G4 source ID overlap")
        paths.update(day_paths)
    candidates = pd.concat(frames, ignore_index=True, sort=False)
    if candidates.sid.duplicated().any() or set(candidates.sid.astype(int)) != set(paths):
        raise RuntimeError("G4 late-clock source IDs/paths invalid")
    if set(pd.to_datetime(candidates.day).dt.date) - set(days):
        raise RuntimeError("G4 late-clock candidate escaped source sessions")
    evidence = dict(sealed_dataset_manifest=str(sealed_root / "dataset_manifest.json"),
                    sealed_dataset_manifest_sha256=sha256(sealed_root / "dataset_manifest.json"),
                    g3_source_evidence=provenance, late_cache=str(Path(cache).resolve()),
                    snapshot_members_hash_verified=verify_snapshots,
                    recent_day_manifests={str(day): sha256(Path(cache) / str(day) / "manifest.json")
                                          for day in days[len(sealed_days):]})
    return candidates, paths, days, evidence, snapshots


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--clocks", nargs="+", default=list(LATE_CLOCKS))
    parser.add_argument("--sid-base", type=int, default=4_000_000)
    args = parser.parse_args()
    candidates, _, days, _, _ = load_late_source(args.cache, clocks=tuple(args.clocks), sid_base=args.sid_base)
    print(f"G4 late-clock source ready: {len(candidates)} candidates, {len(days)} sessions")
    print(candidates.groupby(["hhmm_int", "side"]).size().to_string())


if __name__ == "__main__":
    main()
