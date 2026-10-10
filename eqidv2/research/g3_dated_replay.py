"""Research-only dated replay of the accepted, frozen G-3 rule.

Unlike ``fno_v13_v10_g_3_backtest.py`` (which exports a historical archive),
this consumes one completed daily G feature ledger and its sealed minute paths.
It never edits the frozen G/G-2/G-3 configurations or sends orders.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
from research import g2_session_forensic_audit as audit
from research import g3_freeze
from research import g3_six_confirmation_variants as variants

IST = ZoneInfo("Asia/Kolkata")
def _require_closed(day: date, now: datetime) -> None:
    local = now.astimezone(IST)
    if day > local.date() or (day == local.date() and (local.hour, local.minute) < (15, 35)):
        raise RuntimeError(f"{day} has not reached the 15:35 IST post-close data boundary")


def _verified_source(day: date) -> tuple[Path, dict, Path, Path]:
    run, result = ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT, day)
    manifest_value = result.get("artifacts", {}).get("input_snapshot_manifest")
    if not manifest_value:
        raise RuntimeError("A same-day sealed input snapshot is required")
    manifest_path = Path(manifest_value).resolve()
    snapshot = manifest_path.parent
    manifest = g2.read_json(manifest_path)
    if manifest.get("complete") is not True:
        raise RuntimeError("Input snapshot is incomplete")
    replay._verify_input_snapshot(snapshot, manifest)
    ledger = run / "feature_ledger.csv"
    ledger_meta = g2.read_json(run / "feature_ledger.csv.manifest.json")
    if g2.sha256(ledger) != ledger_meta["artifact_sha256"]:
        raise RuntimeError("Feature ledger hash differs from its source manifest")
    return run, result, snapshot, ledger


def run(day: date, output: Path, *, now: datetime | None = None) -> dict:
    _require_closed(day, now or datetime.now(IST))
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"Refusing to overwrite an existing run: {output}")
    source_run, source_result, snapshot, ledger_path = _verified_source(day)
    frozen = g3_freeze.load_frozen()
    bundle = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE, g2.DEFAULT_G_CONFIG)
    config = frozen["config"]
    if config.get("selection_change") != bundle["source_g"]["selection_change"]:
        raise RuntimeError("Frozen G-3 selection differs from the sealed G-2 source")
    frozen_exit = config["exit"]
    retained_exit = bundle["source_g"]["exit"]
    if (frozen_exit["default"]["target_pct"] != retained_exit["default"]["target_pct"]
        or any(frozen_exit["setups"][key]["target_pct"] != value["target_pct"]
               for key, value in retained_exit["setups"].items())
        or frozen_exit["default"]["stop_pct"] != g2.INITIAL_STOP_PCT
        or frozen_exit["scheduled_tightening"]["stop_pct"] != g2.TIGHTENED_STOP_PCT
        or frozen_exit["scheduled_tightening"]["after_minutes"] != g2.TIGHTEN_AFTER_MINUTES):
        raise RuntimeError("Frozen G-3 targets or staged stop differ from the G-2 simulator")
    raw = variants._extension_pool(day, source_run, snapshot)
    if raw.empty or set(raw.day) != {day}:
        raise RuntimeError("Feature ledger does not contain exactly the requested session")
    nifty = raw.nifty_first_bar_return_pct.dropna()
    if nifty.empty:
        raise RuntimeError("Missing first NIFTY bar return for the dated strategy gates")
    strict = replay._strict_signals(raw, float(nifty.iloc[0]))
    if not strict.empty and not (strict.confirmation_ts - strict.signal_ts).eq(pd.Timedelta(minutes=1)).all():
        raise RuntimeError("A strategy signal has no exact +1-minute confirmation")
    # W1 uses only the source's exact +1-minute confirmation. The accepted
    # change is LONG volume >=1.10x, while SHORT remains >=1.20x.
    selected = {
        "G2": variants._select(strict, bundle, 1.2),
        "G3_W1_V1p1": variants._select(strict, bundle, 1.1),
    }
    for orders in selected.values():
        orders["g3_confirmation_offset_minutes"] = 1
    union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
    paths = ext._selected_paths(union, day, snapshot) if not union.empty else {}
    trades = {name: variants._simulate(orders, paths, bundle["source_g"])
              for name, orders in selected.items()}
    base, improved = (audit.metrics(trades[name]) for name in ("G2", "G3_W1_V1p1"))
    summary = {
        "status": "COMPLETE_RESEARCH_REPLAY", "session_date": day.isoformat(),
        "strategy": config["version"], "execution_authority": False,
        "universe_stocks": source_result.get("coverage", {}).get("universe_stocks"),
        "included_stocks": source_result.get("coverage", {}).get("included_stocks"),
        "source_excluded_stocks": source_result.get("coverage", {}).get("excluded_stocks", []),
        "raw_features": len(raw), "strict_signals": len(strict),
        "G2": {"selected_orders": len(selected["G2"]), **base},
        "G3_W1_V1p1": {"selected_orders": len(selected["G3_W1_V1p1"]), **improved},
        "g3_net_delta_vs_g2": improved["net_pnl"] - base["net_pnl"],
        "provenance": {
            "source_g_daily_run": str(source_run),
            "source_g_daily_result_sha256": g2.sha256(source_run / "replay_result.json"),
            "feature_ledger": str(ledger_path), "feature_ledger_sha256": g2.sha256(ledger_path),
            "snapshot_manifest": str(snapshot / "snapshot_manifest.json"),
            "snapshot_manifest_sha256": g2.sha256(snapshot / "snapshot_manifest.json"),
            "frozen_g3_manifest": str(frozen["path"] / "manifest.json"),
            "frozen_g3_manifest_sha256": g2.sha256(frozen["path"] / "manifest.json"),
            "g2_source_bundle": str(bundle["source"]),
            "g2_source_bundle_manifest_sha256": g2.sha256(bundle["source"] / "bundle_manifest.json"),
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    for name, orders in selected.items():
        orders.to_csv(output / f"selected_{name}.csv", index=False)
        trades[name].to_csv(output / f"trades_{name}.csv", index=False)
    (output / "summary.json").write_text(json.dumps(summary, indent=2, default=str, allow_nan=False), encoding="utf-8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.date, args.output_dir), indent=2, default=str))
