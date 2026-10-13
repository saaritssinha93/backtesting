"""After-close, backtest-only 09:20 LONG sidecar for the scheduled G-3 day.

The production daily result remains untouched. Incomplete opening evidence
fails closed, and a verified sidecar can be read separately in Dashboard Flow.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import secrets
import tempfile

import pandas as pd

import backtesting_result_v13_v10_g_3_daily as production
import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research import g3_0920_feature_export as exporter
from research import g3_0920_long_current as current
from research.g3_0920_strategy import selection_audit_0920

ROOT = Path(RUNTIME_ROOT) / "backtesting_result_v13_v10_g_3"
POINTER_NAME = "current_0920_long_daily.json"


def _production_day(day: date, root: Path):
    verified = production._read_verified_pointer(root, "current_run.json", "production_daily", allow_legacy=True)
    if verified is None:
        raise ValueError("No verified G-3 production daily result")
    directory, manifest = verified
    if (manifest["session_date"] != str(day)
            or manifest["strategy_version"] != current.contract.STRATEGY_VERSION):
        raise ValueError("Sidecar requires the original V2 daily result; G3 V3 already includes 09:20 LONG natively")
    path = directory / manifest["artifacts"]["portfolio_trades"]["path"]
    baseline = pd.read_csv(path)
    if baseline.sid.duplicated().any():
        raise ValueError("Duplicate production daily order IDs")
    replayed = paired.allocate(baseline)
    paired._same_replayed_baseline(baseline, replayed)
    return replayed, directory, manifest


def publish(root: Path, output: Path) -> dict:
    root, output = root.resolve(), output.resolve()
    relative = output.relative_to(root)
    if relative.parts[:3] != ("backtests", "daily", "runs") or len(relative.parts) != 5:
        raise ValueError("Daily sidecar output must be under backtests/daily/runs/<day>/<run>")
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    if (manifest.get("variant_id") != current.VARIANT_ID or manifest.get("execution_authority") is not False
            or summary.get("variant_id") != current.VARIANT_ID or summary.get("sessions") != 1
            or summary.get("period_end") != relative.parts[3]
            or summary.get("existing_g3_trades_unchanged") is not True
            or json.loads((output / "config.json").read_text(encoding="utf-8")) != current._config()
            or any(Path(name).name != name or not (output / name).is_file()
                   or paired.sha256(output / name) != digest for name, digest in manifest["files"].items())):
        raise ValueError("09:20 daily sidecar publication verification failed")
    pointer = dict(schema_version="fno_g3_0920_long_daily_v1", role="backtest_daily",
        run_dir=relative.as_posix(), manifest_sha256=paired.sha256(manifest_path),
        variant_id=current.VARIANT_ID, session_date=summary["period_end"],
        execution_authority=False, created_at_ist=summary["created_at_ist"])
    latest = root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    previous_path = latest / POINTER_NAME
    if previous_path.is_file():
        previous = json.loads(previous_path.read_text(encoding="utf-8"))
        if (previous.get("schema_version") != pointer["schema_version"]
                or previous.get("variant_id") != pointer["variant_id"]
                or previous.get("session_date", "") > pointer["session_date"]):
            raise ValueError("Cannot replace a newer or different 09:20 daily pointer")
    fd, temporary = tempfile.mkstemp(prefix=".0920_daily.", suffix=".tmp", dir=latest)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(pointer, stream, indent=2)
            stream.write("\n")
        os.replace(temporary, latest / POINTER_NAME)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return pointer


def run(day: date, output: Path, *, root: Path = ROOT, publish_result: bool = True) -> dict:
    root, output = Path(root).resolve(), Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("09:20 daily output must be new or empty")
    config = current._config()
    baseline, source_dir, source_manifest = _production_day(day, root)
    features, coverage = exporter.build_day(day, data_root=RUNTIME_ROOT)
    if coverage["stock_rows"] != len(features):
        raise RuntimeError("09:20 daily feature export is incomplete")
    _, profile = current._reference()
    audit = selection_audit_0920(features, profile=profile)
    selected = audit.loc[audit.v9_selected & audit.side.eq("LONG")].copy().reset_index(drop=True)
    if set(selected.sid.astype(int)) & set(baseline.sid.astype(int)):
        raise RuntimeError("09:20 daily order IDs overlap production")
    selected["native_target_pct"] = config["target_pct"]
    selected["target_pct"] = config["target_pct"]
    selected["backtest_variant_id"] = current.VARIANT_ID
    paths, path_evidence = paired.new_execution_paths(selected, RUNTIME_ROOT)
    if len(selected):
        new_trades = simulator.simulate_staged(selected, paths,
            cost_bps=contract.ROUND_TRIP_COST_BPS,
            max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    else:
        new_trades = selected.copy()
        new_trades["filled"] = pd.Series(dtype=bool)
    variant = paired.allocate(pd.concat((baseline, new_trades), ignore_index=True, sort=False))
    comparison, _ = paired.compare_ledgers(baseline, variant, [day], [day],
                                            set(selected.sid.astype(int)))
    old = comparison["old_trade_effect"]
    if old["displaced_count"] or old["newly_accepted_count"] or abs(old["net_pnl_change_rs"]) > 1e-6:
        raise RuntimeError("09:20 daily sidecar changed an existing G-3 trade")
    added = variant.loc[variant.sid.isin(selected.sid)].copy()
    daily = current._daily(baseline, variant, added, [day])
    summary = dict(variant_id=current.VARIANT_ID, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), sessions=1,
        period_start=str(day), period_end=str(day), short_0920_enabled=False, target_pct=3.0,
        retrospective_opening_oi=True, live_tradability_verified=False,
        existing_g3_trades_unchanged=True, baseline=comparison["baseline"],
        with_0920_long=comparison["with_0920"],
        added_0920_long=comparison["added_0920_long"],
        production_source=dict(run_dir=str(source_dir),
            manifest_sha256=paired.sha256(source_dir / "run_manifest.json"),
            strategy_fingerprint=source_manifest["strategy_fingerprint"]),
        opening_feature_coverage=coverage, selected_path_provenance=path_evidence)
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in dict(selection_audit_0920=audit, added_0920_orders=selected,
            baseline_trades=baseline, added_0920_trades=added, variant_trades=variant,
            daily_results=daily).items():
        frame.to_csv(output / (name + ".csv"), index=False)
    (output / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    code = (Path(__file__).resolve(), Path(current.__file__).resolve(),
            Path(paired.__file__).resolve(), Path(exporter.__file__).resolve(),
            Path(__file__).with_name("g3_0920_strategy.py"), current.CONFIG_PATH)
    manifest = dict(variant_id=current.VARIANT_ID, execution_authority=False,
        code_sha256={path.name: paired.sha256(path) for path in code},
        files={path.name: paired.sha256(path) for path in output.iterdir() if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if publish_result:
        publish(root, output)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--no-publish", action="store_true")
    args = parser.parse_args()
    now = datetime.now(contract.IST)
    output = args.output_dir or ROOT / "backtests" / "daily" / "runs" / str(args.date) / f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_0920_long_t300"
    result = run(args.date, output, publish_result=not args.no_publish)
    print(json.dumps({key: result[key] for key in ("period_end", "baseline", "with_0920_long", "added_0920_long")}, indent=2))
    print(output.resolve())


if __name__ == "__main__":
    main()
