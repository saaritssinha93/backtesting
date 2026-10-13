"""Publish the selected 09:20 LONG as a separate, backtest-only G-3 history.

The sealed 09:50 historical backtest is the baseline. This runner never changes
the shared G-3 live/paper contract or the scheduled production daily replay.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
import secrets
import tempfile

import pandas as pd

import fno_v13_v10_g_2_backtest as simulator
import fno_v13_v10_g_3_contract as contract
from eqidv2_runtime_paths import RUNTIME_ROOT
from research import g3_0920_backtest as paired
from research import g3_0920_target_sweep as sweep
from research.g3_0920_strategy import selection_audit_0920

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs/strategies/v13_v10_g_3_backtest_0920_long_target300_v1.json"
ROOT = Path(RUNTIME_ROOT) / "backtesting_result_v13_v10_g_3"
POINTER_NAME = "current_0920_long_backtest.json"
VARIANT_ID = "V13-V10-G-3-BT-0920-LONG-T300-V1"


def _config() -> dict:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    expected = dict(variant_id=VARIANT_ID, scope="BACKTEST_ONLY",
        base_strategy=contract.STRATEGY_VERSION, signal_end="09:20",
        confirmation_end="09:21", side="LONG", short_0920_enabled=False,
        entry_profile="5m_reference_5m__1m_volume_180", target_pct=3.0,
        initial_stop_pct=contract.INITIAL_STOP_PCT,
        tightened_stop_pct=contract.TIGHTENED_STOP_PCT,
        tighten_after_minutes=contract.TIGHTEN_AFTER_MINUTES,
        square_off=contract.SQUARE_OFF, preserve_existing_g3_trades=True,
        execution_authority=False)
    if config != expected:
        raise ValueError("The fixed G-3 09:20 LONG backtest configuration changed")
    return config


def _reference():
    summary, profile = sweep.read_verified_reference(sweep.DEFAULT_REFERENCE)
    if summary["winners"]["best_observed_70"]["profile_id"] != _config()["entry_profile"]:
        raise ValueError("Sealed 09:20 entry profile changed")
    return summary, profile


def _daily(baseline: pd.DataFrame, variant: pd.DataFrame, added: pd.DataFrame, days) -> pd.DataFrame:
    def sums(frame):
        return frame.groupby(frame.day.astype(str)).portfolio_net_profit_rupees.sum()
    def counts(frame, key):
        work = frame.loc[frame.portfolio_executed.astype(bool)]
        if key == "wins":
            work = work.loc[work.portfolio_net_profit_rupees.gt(0)]
        return work.groupby(work.day.astype(str)).size()
    labels = [str(day) for day in days]
    result = pd.DataFrame({"day": labels})
    for name, frame in (("baseline", baseline), ("variant", variant), ("added_0920_long", added)):
        result[name + "_selected"] = frame.groupby(frame.day.astype(str)).size().reindex(labels, fill_value=0).to_numpy(int)
        result[name + "_fills"] = counts(frame, "fills").reindex(labels, fill_value=0).to_numpy(int)
        result[name + "_wins"] = counts(frame, "wins").reindex(labels, fill_value=0).to_numpy(int)
        result[name + "_net_pnl_rs"] = sums(frame).reindex(labels, fill_value=0).to_numpy(float)
    result["delta_net_pnl_rs"] = result.variant_net_pnl_rs - result.baseline_net_pnl_rs
    return result


def publish(root: Path, output: Path) -> dict:
    root, output = root.resolve(), output.resolve()
    relative = output.relative_to(root)
    if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4:
        raise ValueError("09:20 backtest output must be under backtests/runs/<day>/<run>")
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    if (manifest.get("variant_id") != VARIANT_ID or manifest.get("execution_authority") is not False
            or summary.get("variant_id") != VARIANT_ID or summary.get("execution_authority") is not False
            or summary.get("sessions") != 49 or summary.get("period_end") != relative.parts[2]
            or summary.get("existing_g3_trades_unchanged") is not True
            or json.loads((output / "config.json").read_text(encoding="utf-8")) != _config()
            or any(Path(name).name != name or not (output / name).is_file()
                   or paired.sha256(output / name) != digest for name, digest in manifest["files"].items())):
        raise ValueError("09:20 backtest publication verification failed")
    pointer = dict(schema_version="fno_g3_0920_long_backtest_v1", role="backtest_history",
        run_dir=relative.as_posix(), manifest_sha256=paired.sha256(manifest_path),
        variant_id=VARIANT_ID, session_date=summary["period_end"],
        execution_authority=False, created_at_ist=summary["created_at_ist"])
    latest = root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".0920_long.", suffix=".tmp", dir=latest)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(pointer, stream, indent=2)
            stream.write("\n")
        os.replace(temporary, latest / POINTER_NAME)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return pointer


def run(output: Path, *, publish_result: bool = True) -> dict:
    config = _config()
    output = Path(output).resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("09:20 backtest output must be new or empty")
    reference, profile = _reference()
    published, days, baseline_evidence = paired.load_published()
    if baseline_evidence["pointer_sha256"] != reference["baseline_evidence"]["pointer_sha256"]:
        raise RuntimeError("Current G-3 backtest baseline differs from the sealed 09:20 research")
    features_path = Path(reference["feature_path"])
    if paired.sha256(features_path) != reference["feature_sha256"]:
        raise RuntimeError("Sealed 09:20 feature export changed")
    all_features, coverage, feature_evidence = paired.load_feature_evidence(features_path)
    universe = paired.validate_feature_universe(all_features, RUNTIME_ROOT, coverage)
    features = all_features.loc[all_features.session_date.astype(str).isin({str(day) for day in days})]
    if paired._feature_days(features) != days:
        raise RuntimeError("09:20 features do not cover every baseline session")
    audit = selection_audit_0920(features, profile=profile)
    selected = audit.loc[audit.v9_selected & audit.side.eq("LONG")].copy().reset_index(drop=True)
    sealed_orders = pd.read_csv(sweep.DEFAULT_REFERENCE / "best_observed_70_orders.csv")
    expected = sealed_orders.loc[sealed_orders.side.eq("LONG"), ["sid", "signal_id"]].reset_index(drop=True)
    pd.testing.assert_frame_equal(selected[["sid", "signal_id"]], expected, check_dtype=False)
    selected["native_target_pct"] = config["target_pct"]
    selected["target_pct"] = config["target_pct"]
    selected["backtest_variant_id"] = VARIANT_ID
    paths, path_evidence = paired.new_execution_paths(selected, RUNTIME_ROOT)
    new_trades = simulator.simulate_staged(selected, paths,
        cost_bps=contract.ROUND_TRIP_COST_BPS,
        max_entry_delay_minutes=contract.ENTRY_EXPIRY_MINUTES)
    new_trades["scope"] = "BACKTEST_ONLY"
    new_trades["execution_authority"] = False
    baseline = paired.allocate(published)
    variant = paired.allocate(pd.concat((published, new_trades), ignore_index=True, sort=False))
    comparison, _ = paired.compare_ledgers(baseline, variant, days, days, set(selected.sid.astype(int)))
    old = comparison["old_trade_effect"]
    if old["displaced_count"] or old["newly_accepted_count"] or abs(old["net_pnl_change_rs"]) > 1e-6:
        raise RuntimeError("09:20 additions changed existing G-3 trade acceptance")
    added = variant.loc[variant.sid.isin(selected.sid)].copy()
    daily = _daily(baseline, variant, added, days)
    summary = dict(variant_id=VARIANT_ID, scope="BACKTEST_ONLY", execution_authority=False,
        created_at_ist=datetime.now(contract.IST).isoformat(), sessions=len(days),
        period_start=str(days[0]), period_end=str(days[-1]),
        baseline=comparison["baseline"], with_0920_long=comparison["with_0920"],
        added_0920_long=comparison["added_0920_long"],
        existing_g3_trades_unchanged=True, short_0920_enabled=False,
        target_pct=3.0, retrospective_opening_oi=True, live_tradability_verified=False,
        baseline_provenance=baseline_evidence, feature_export_provenance=feature_evidence,
        dated_universe_provenance=universe, selected_path_provenance=path_evidence)
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in dict(selection_audit_0920=audit, added_0920_orders=selected,
            baseline_trades=baseline, added_0920_trades=added, variant_trades=variant,
            daily_results=daily).items():
        frame.to_csv(output / (name + ".csv"), index=False)
    (output / "config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False, default=str) + "\n", encoding="utf-8")
    code = (Path(__file__).resolve(), Path(paired.__file__).resolve(),
            Path(__file__).with_name("g3_0920_strategy.py"), CONFIG_PATH)
    manifest = dict(variant_id=VARIANT_ID, execution_authority=False,
        code_sha256={path.name: paired.sha256(path) for path in code},
        files={path.name: paired.sha256(path) for path in output.iterdir() if path.is_file()})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if publish_result:
        publish(ROOT, output)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--no-publish", action="store_true")
    args = parser.parse_args()
    now = datetime.now(contract.IST)
    output = args.output_dir or ROOT / "backtests" / "runs" / "2026-10-12" / f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_0920_long_t300"
    result = run(output, publish_result=not args.no_publish)
    print(json.dumps({key: result[key] for key in ("sessions", "period_start", "period_end", "baseline", "with_0920_long", "added_0920_long")}, indent=2))
    print(output.resolve())


if __name__ == "__main__":
    main()
