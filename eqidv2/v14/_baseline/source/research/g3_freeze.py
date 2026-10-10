"""Seal and verify the accepted G-3 1-minute/1.10x historical replay.

This packages archived evidence, not a new full-universe signal recomputation.
Only the recorded immutable source snapshots are permitted as minute inputs.
"""
from __future__ import annotations

import ast
import hashlib
import json
import shutil
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STUDY = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_3\run_20261008_six_confirmation_variants")
DEFAULT_FROZEN = DEFAULT_STUDY.parent / "frozen_20261008_long110_nextminute_v1"
VARIANT = "G3_W1_V1p1"
IST = "Asia/Kolkata"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False), encoding="utf-8")


def _bool(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.lower().isin(["true", "1", "1.0", "yes", "on"])


def _source_closure() -> list[Path]:
    """Archive repository-local Python dependencies, including function imports."""
    seeds = ["research/g3_freeze.py", "fno_v13_v10_g_3_backtest.py",
             "research/g3_six_confirmation_variants.py", "research/g3_results_report.py"]
    pending = [ROOT / x for x in seeds]
    found: set[Path] = set()
    while pending:
        source = pending.pop().resolve()
        if source in found or not source.is_file() or not source.is_relative_to(ROOT):
            continue
        found.add(source)
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        modules = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.extend(x.name for x in node.names)
            elif isinstance(node, ast.ImportFrom):
                prefix = node.module or ""
                if node.level:
                    parts = list(source.relative_to(ROOT).parts[:-1])
                    if node.level > 1:
                        parts = parts[:-(node.level-1)]
                    prefix = ".".join(parts + ([prefix] if prefix else []))
                modules.append(prefix)
                modules.extend(prefix + "." + x.name for x in node.names if x.name != "*")
        for module in modules:
            candidate = ROOT.joinpath(*module.split("."))
            for dependency in (candidate.with_suffix(".py"), candidate / "__init__.py"):
                if dependency.is_file():
                    pending.append(dependency)
    return sorted(found)


def _minute_input(path: Path, day: str) -> pd.DataFrame:
    import fno_v13_v10_g_daily_replay as replay
    problems: list = []
    frame = replay._load_minute(path, pd.Timestamp(day).date(), problems, path.stem)
    if problems:
        raise RuntimeError(f"Invalid minute source {path}: {problems}")
    clock = frame.ts.dt.hour * 60 + frame.ts.dt.minute
    return frame.loc[clock.between(9*60+16, 15*60+30)].copy()


def _context(frame: pd.DataFrame, day: str, symbol: str) -> tuple[pd.DataFrame, dict]:
    start = pd.Timestamp(day + " 09:16", tz=IST)
    end = pd.Timestamp(day + " 15:15", tz=IST)
    prior = frame.loc[frame.ts.lt(start)].tail(200)
    session = frame.loc[frame.ts.between(start, end)]
    expected = pd.date_range(start, end, freq="min")
    if len(prior) < 200:
        raise RuntimeError(f"Insufficient 200-bar warmup: {symbol} {day}: {len(prior)}")
    if session.ts.duplicated().any() or not np.array_equal(pd.DatetimeIndex(session.ts).as_unit("ns").asi8,
                                                           expected.as_unit("ns").asi8):
        raise RuntimeError(f"Incomplete regular session: {symbol} {day}")
    use = pd.concat([prior, session], ignore_index=True)
    numeric = use[["open", "high", "low", "close", "volume"]].to_numpy(float)
    bad = (~np.isfinite(numeric).all(axis=1) | (numeric[:, :4] <= 0).any(axis=1)
           | (numeric[:, 4] < 0) | (numeric[:, 1] < numeric[:, 2])
           | (numeric[:, 1] < np.maximum(numeric[:, 0], numeric[:, 3]))
           | (numeric[:, 2] > np.minimum(numeric[:, 0], numeric[:, 3])))
    if bad.any():
        raise RuntimeError(f"Invalid OHLCV: {symbol} {day}, {int(bad.sum())} rows")
    same_date = use.ts.dt.date.eq(use.ts.shift().dt.date)
    if (same_date & use.ts.diff().ne(pd.Timedelta(minutes=1))).any():
        raise RuntimeError(f"Interrupted warmup/session minutes: {symbol} {day}")
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if flag in use:
            numeric_flag = pd.to_numeric(use[flag], errors="coerce").fillna(0).ne(0)
            if (numeric_flag | _bool(use[flag])).any():
                raise RuntimeError(f"Flagged minute context: {symbol} {day} {flag}")
        use[flag] = False
    use["day"] = day
    use["tradingsymbol"] = symbol
    use["context_role"] = np.where(use.ts.lt(start), "PRIOR_SESSION_WARMUP", "TRADE_SESSION")
    columns = ["day", "tradingsymbol", "ts", "open", "high", "low", "close", "volume",
               "gap_filled", "opening_snapshot", "provisional_stale", "context_role"]
    return use[columns], dict(day=day, tradingsymbol=symbol, session_bars=len(session),
                             warmup_bars=len(prior), first_context_ts=str(use.ts.iloc[0]),
                             last_context_ts=str(use.ts.iloc[-1]), quality_pass=True)


def build_frozen(path: Path = DEFAULT_FROZEN, study: Path = DEFAULT_STUDY) -> dict:
    import fno_oi_hybrid_data as hybrid
    import fno_v13_v10_g_2_backtest as g2
    from research.g3_six_confirmation_variants import _paths_from_archive, _simulate

    path, study = Path(path).resolve(), Path(study).resolve()
    if path.exists() and any(path.iterdir()):
        return load_frozen(path)
    source_provenance = _json(study / "provenance.json")
    source = Path(source_provenance["source_bundle"])
    source_config = Path(source_provenance["frozen_g_config"])
    for file, expected in [(source / "bundle_manifest.json", source_provenance["source_bundle_manifest_sha256"]),
                           (source_config, source_provenance["source_config_sha256"])]:
        if sha256(file) != expected:
            raise RuntimeError(f"Sealed source hash drift: {file}")
    g2.verify_bundle(source)
    source_g = _json(source_config)
    g2.g.checked_settings(source_g)
    config = g2.config(source_g)
    config.update(version="V13-V10-G-3-FROZEN-LONG110-NEXTMINUTE-V1", source_version="V13-V10-G-2",
                  accepted_variant=VARIANT, confirmation_window_minutes=1,
                  minimum_confirmation_1m_volume_ratio={"LONG": 1.10, "SHORT": 1.20},
                  evidence="ARCHIVED_46_SESSION_REUSED_HISTORY_REPLAY", live_configuration_changed=False,
                  execution_authority=False, state="FROZEN_RESEARCH_BASELINE",
                  freeze_semantics="Hash-sealed accepted archived ledger and exact monitoring inputs; no live authority")
    config["setup_rules"] = []
    change = g2.g.SelectionChange(**source_g["selection_change"])
    for setup in g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES["higher_frequency"]):
        core, expanded = g2.g.setup_pair(setup, change)
        config["setup_rules"].append(dict(original=asdict(setup), core=asdict(core), expanded=asdict(expanded)))

    trades = pd.read_csv(study / f"trades_{VARIANT}.csv")
    executed = trades.loc[_bool(trades.portfolio_executed)].copy()
    if len(trades) != 102 or len(executed) != 93 or int(_bool(trades.filled).sum()) != 93:
        raise RuntimeError("Accepted G-3 archived counts drifted")
    days = source_provenance["days"]
    if len(days) != 46 or max(days) != "2026-10-07":
        raise RuntimeError("Accepted G-3 session set drifted")
    daily = pd.read_csv(study / "daywise_comparison.csv")
    daily = daily.loc[daily.variant.eq(VARIANT)].copy().rename(columns={"date": "day"})
    if list(daily.day) != days or int(daily.trades.sum()) != 93:
        raise RuntimeError("Daily accepted ledger differs from trades")
    if not np.isclose(daily.net_pnl.sum(), executed.portfolio_net_profit_rupees.sum(), atol=1e-7):
        raise RuntimeError("Daily P&L differs from accepted trade ledger")

    snapshot_by_day = {x["day"]: Path(x["snapshot"]) for x in source_provenance["extensions"]}
    historic = Path(source_provenance["historical_snapshot"])
    used_snapshots = {snapshot_by_day.get(day, historic) for day in executed.day.unique()}
    snapshot_inventory = {}
    for snapshot in sorted(used_snapshots):
        manifest_path = snapshot / "snapshot_manifest.json"
        expected = source_provenance["verified_snapshot_manifest_hashes"][str(manifest_path)]
        if sha256(manifest_path) != expected:
            raise RuntimeError(f"Snapshot manifest hash drift: {manifest_path}")
        manifest = _json(manifest_path)
        if not manifest.get("complete"):
            raise RuntimeError(f"Incomplete snapshot: {snapshot}")
        snapshot_inventory[snapshot] = {x["snapshot_relative_path"]: x for x in manifest["sources"]
                                        if x.get("captured") and x.get("snapshot_relative_path")}

    parts, coverage, inputs = [], [], []
    contexts: dict[tuple[str, str], pd.DataFrame] = {}
    verified_inputs: dict[Path, str] = {}
    pairs = executed[["day", "tradingsymbol"]].drop_duplicates().sort_values(["day", "tradingsymbol"])
    for n, row in enumerate(pairs.itertuples(index=False), 1):
        snapshot = snapshot_by_day.get(row.day, historic)
        minute_path = hybrid.equity_one_minute_path(row.tradingsymbol, snapshot / "equity_1m")
        relative = minute_path.relative_to(snapshot).as_posix()
        expected = snapshot_inventory[snapshot][relative]["sha256"]
        if minute_path not in verified_inputs:
            observed = sha256(minute_path)
            if observed != expected:
                raise RuntimeError(f"Minute snapshot hash drift: {minute_path}")
            verified_inputs[minute_path] = observed
        minute = _minute_input(minute_path, row.day)
        context, record = _context(minute, row.day, row.tradingsymbol)
        contexts[(row.day, row.tradingsymbol)] = context
        parts.append(context)
        coverage.append(record)
        inputs.append(dict(day=row.day, tradingsymbol=row.tradingsymbol, snapshot=str(snapshot),
                           minute_path=str(minute_path), minute_sha256=expected))
        if n % 10 == 0 or n == len(pairs):
            print(f"G3 freeze minute context: {n}/{len(pairs)} symbol-days", flush=True)
    minutes = pd.concat(parts, ignore_index=True)
    historical_orders = trades.loc[trades.day.le("2026-09-23")]
    sealed_paths = _paths_from_archive(source, set(historical_orders.sid.astype(int)))
    path_checks, replay_paths = [], {}
    for trade in executed.itertuples(index=False):
        sid = int(trade.sid)
        context = contexts[(trade.day, trade.tradingsymbol)].set_index("ts")
        if trade.day <= "2026-09-23":
            sealed = sealed_paths[sid]
            stamps = pd.to_datetime(sealed["timestamp_ns"], utc=True).tz_convert(IST)
            match = context.reindex(stamps)
            for field in ("open", "high", "low", "close"):
                if not np.array_equal(match[field].to_numpy(float), sealed[field]):
                    raise RuntimeError(f"Historical execution path differs from immutable minute input: {sid} {field}")
            replay_paths[sid] = sealed
            path_checks.append(dict(sid=sid, day=trade.day, tradingsymbol=trade.tradingsymbol,
                                    check="EXACT_OHLC_TIMESTAMP_PARITY_WITH_SEALED_PATH", bars=len(stamps), passed=True))
        else:
            confirmation = pd.Timestamp(trade.confirmation_ts)
            segment = context.loc[context.index > confirmation]
            replay_paths[sid] = {k: segment[k].to_numpy(float) for k in ("open", "high", "low", "close")}
            replay_paths[sid]["timestamp_ns"] = segment.index.as_unit("ns").asi8
            path_checks.append(dict(sid=sid, day=trade.day, tradingsymbol=trade.tradingsymbol,
                                    check="HASH_VERIFIED_RECORDED_EXTENSION_SNAPSHOT", bars=len(segment), passed=True))
    # Recompute each filled trade's exit and sizing using archived candles. This
    # checks minute alignment, actual fills, staged stops and target preservation.
    repeated = _simulate(executed.copy(), replay_paths, source_g)
    old = executed.set_index("sid").sort_index()
    new = repeated.set_index("sid").sort_index()
    for column in ("entry_price", "exit_price", "gross_return_pct", "portfolio_net_profit_rupees"):
        if not np.allclose(old[column].astype(float), new[column].astype(float), rtol=0, atol=1e-7):
            raise RuntimeError(f"Archived execution recomputation drift: {column}")
    for column in ("entry_ts", "exit_ts"):
        if not np.array_equal(pd.to_datetime(old[column], utc=True).astype("int64"),
                              pd.to_datetime(new[column], utc=True).astype("int64")):
            raise RuntimeError(f"Archived execution timestamp drift: {column}")
    if not old.exit_reason.equals(new.exit_reason):
        raise RuntimeError("Archived exit reason drift")

    path.mkdir(parents=True, exist_ok=True)
    _write_json(path / "frozen_config.json", config)
    shutil.copy2(study / f"trades_{VARIANT}.csv", path / "trades.csv")
    daily.to_csv(path / "daily_results.csv", index=False)
    minutes.to_parquet(path / "minute_context.parquet", index=False)
    pd.DataFrame(coverage).to_csv(path / "minute_coverage.csv", index=False)
    pd.DataFrame(path_checks).to_csv(path / "execution_path_validation.csv", index=False)
    shutil.copy2(source_config, path / "source_g_config.json")
    shutil.copy2(study / "provenance.json", path / "source_study_provenance.json")
    shutil.copy2(study / "validation.json", path / "source_study_validation.json")
    source_files = []
    for origin in _source_closure():
        destination = path / "source" / origin.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, destination)
        source_files.append(dict(original=str(origin), frozen=str(destination.relative_to(path)), sha256=sha256(origin)))
    validation = dict(passed=True, selected_orders=len(trades), filled_trades=len(executed), sessions=len(days),
                      symbol_days=len(pairs), minute_rows=len(minutes), historical_path_parity_trades=int(executed.day.le("2026-09-23").sum()),
                      execution_replay_parity_trades=len(executed), full_session_bars=360, prior_session_warmup_bars=200,
                      net_profit_rupees=float(executed.portfolio_net_profit_rupees.sum()),
                      unchanged_g_g2_source_hashes={str(x): sha256(x) for x in [ROOT / "fno_v13_v10_g_backtest.py", ROOT / "fno_v13_v10_g_2_backtest.py"]})
    _write_json(path / "validation.json", validation)
    _write_json(path / "provenance.json", dict(study=str(study), accepted_variant=VARIANT, days=days,
                source_study_file_hashes={x.name: sha256(x) for x in study.iterdir() if x.is_file()},
                source_inputs=inputs, source_files=source_files,
                snapshot_verification="Recorded manifest hashes and every used minute artifact verified; unrelated snapshot files not rehashed",
                interpretation="Frozen archived accepted replay, with full executed-position monitoring context; October 1 excluded"))
    artifacts = {x.relative_to(path).as_posix(): dict(sha256=sha256(x), size_bytes=x.stat().st_size)
                 for x in sorted(path.rglob("*")) if x.is_file()}
    _write_json(path / "manifest.json", dict(schema_version="g3_frozen_research_v1", state="COMPLETE",
                created_at_utc=datetime.now(timezone.utc).isoformat(), accepted_variant=VARIANT, days=days,
                artifacts=artifacts, verification=validation))
    return load_frozen(path)


def load_frozen(path: Path = DEFAULT_FROZEN) -> dict:
    """Hash-verify and return config, all selected trades, context, days and manifest."""
    path = Path(path).resolve()
    manifest = _json(path / "manifest.json")
    if manifest.get("state") != "COMPLETE" or manifest.get("accepted_variant") != VARIANT:
        raise RuntimeError("Not a complete accepted G-3 freeze")
    for relative, record in manifest["artifacts"].items():
        artifact = (path / relative).resolve()
        if not artifact.is_relative_to(path) or not artifact.is_file() or sha256(artifact) != record["sha256"]:
            raise RuntimeError(f"Frozen G-3 artifact drift: {relative}")
    trades = pd.read_csv(path / "trades.csv")
    for column in ("filled", "portfolio_executed"):
        trades[column] = _bool(trades[column])
    for column in ("entry_ts", "exit_ts", "confirmation_ts", "signal_ts", "exit_bar_end_ts", "exit_execution_ts"):
        if column in trades:
            trades[column] = pd.to_datetime(trades[column], utc=True).dt.tz_convert(IST)
    minutes = pd.read_parquet(path / "minute_context.parquet")
    if len(trades) != 102 or int(trades.portfolio_executed.sum()) != 93:
        raise RuntimeError("Frozen ledger cardinality drift")
    return dict(path=path, config=_json(path / "frozen_config.json"), trades=trades,
                minutes=minutes, days=manifest["days"], manifest=manifest)


def export_archived_replay(output: Path, frozen: Path = DEFAULT_FROZEN) -> dict:
    """Verify and export the frozen replay, without selecting or tuning new trades."""
    data = load_frozen(frozen)
    output = Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("Archived replay export requires a new or empty directory")
    output.mkdir(parents=True, exist_ok=True)
    for name in ("trades.csv", "daily_results.csv", "frozen_config.json", "validation.json"):
        shutil.copy2(data["path"] / name, output / name)
    _write_json(output / "replay_provenance.json", dict(mode="VERIFIED_ARCHIVED_FROZEN_REPLAY_EXPORT",
                frozen_path=str(data["path"]), frozen_manifest_sha256=sha256(data["path"] / "manifest.json"),
                new_signals_recomputed=False, live_orders_enabled=False))
    return data
