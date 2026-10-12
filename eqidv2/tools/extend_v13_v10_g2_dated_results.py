"""Append a verified dated G trade to the frozen staged-stop G-2 history."""
from __future__ import annotations

import argparse
from datetime import date, datetime
from io import StringIO
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
from tools.extend_v13_v10_g_dated_results import verified_dataset_lineage


def _historical_rows_equal(old: pd.DataFrame, current: pd.DataFrame) -> None:
    if len(current) < len(old):
        raise ValueError("Extended ledger has fewer historical rows")
    normalized = pd.read_csv(StringIO(current.iloc[:len(old)][old.columns].to_csv(index=False)))
    pd.testing.assert_frame_equal(
        old.reset_index(drop=True), normalized.reset_index(drop=True),
        check_dtype=False, check_exact=False, rtol=0, atol=1e-7,
    )


def _historical_control_parity(old: pd.DataFrame, current: pd.DataFrame) -> None:
    """Match selection and execution despite older feature-column/ID schemas."""
    identity = ["day", "setup_id", "tradingsymbol", "side", "signal_ts"]
    outcome = [name for name in old.columns if name in current.columns and (
        name.startswith("portfolio_") or name.startswith("entry_")
        or name.startswith("exit_") or name.endswith("_profit_rupees")
        or name in {"filled", "native_stop_pct", "native_target_pct",
                    "gross_return_pct", "cost_pct", "net_return_pct", "stop_hit", "target_hit"}
    )]
    fields = identity + [name for name in outcome if name not in identity]
    prior = current.loc[current.day.isin(old.day)]
    if len(prior) != len(old) or old.duplicated(identity).any() or prior.duplicated(identity).any():
        raise ValueError("G and G-2 historical control selection counts differ")
    expected = old[fields].sort_values(identity).reset_index(drop=True)
    observed = prior[fields].sort_values(identity).reset_index(drop=True)
    _historical_rows_equal(expected, observed)


def build(base_run: Path, g_archive: Path, output: Path, day: date) -> dict:
    base_run, g_archive, output = (Path(path).resolve() for path in (base_run, g_archive, output))
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite output: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    base_provenance = ext._verify_output(base_run)
    sealed = Path(base_provenance["source_bundle"])
    g2.verify_bundle(sealed)
    source_g_path = Path(base_provenance["source_g_config"])
    if g2.sha256(source_g_path) != base_provenance["source_g_config_sha256"]:
        raise ValueError("Frozen G settings changed")
    source_g = g2.g.checked_settings(g2.read_json(source_g_path))
    settings = g2.checked_settings(g2.read_json(base_run / "frozen_config.json"), source_g)
    base = ext._portfolio_config(source_g)

    g_run = g_archive / "g_backtest"
    g_meta = g2.read_json(g_run / "run_metadata.json")
    _, _, earlier_extensions, _ = verified_dataset_lineage(g_archive, str(day))
    if (g_meta["last_session"] != str(day)
            or g_meta["frozen_g_config_sha256"] != g2.sha256(source_g_path)
            or str(day) not in earlier_extensions):
        raise ValueError("G archive is not a completed frozen dated extension")
    proof_path = g_run / "dated_extension_proof.json"
    proof = g2.read_json(proof_path)
    if (proof.get("schema_version") != "RETAINED_G_DATED_SESSION_V2"
            or proof.get("complete") is not True or proof.get("session_date") != str(day)
            or int(proof.get("selected_orders", 0)) <= 0
            or proof.get("historical_rows_unchanged") is not True
            or proof.get("frozen_g_config_sha256") != g2.sha256(source_g_path)):
        raise ValueError("G dated trade proof is incomplete or uses different settings")
    for name, expected in proof["input_hashes"].items():
        if g2.sha256(Path(name)) != expected:
            raise ValueError(f"G dated source changed: {name}")
    source_run = Path(proof["source_g_daily_run"])
    source_result = g2.read_json(source_run / "replay_result.json")
    if (source_result.get("state") != "SUCCESS" or source_result.get("complete") is not True
            or source_result.get("session_date") != str(day)
            or source_result.get("coverage") != proof.get("coverage")):
        raise ValueError("G daily replay is not a complete match for the dated proof")
    snapshot_manifest = Path(proof["snapshot_manifest"])
    snapshot = g2.read_json(snapshot_manifest)
    if snapshot.get("complete") is not True or snapshot.get("session_date") != str(day):
        raise ValueError("Dated execution snapshot is incomplete")
    replay._verify_input_snapshot(snapshot_manifest.parent, snapshot)

    old_trades = pd.read_csv(base_run / "selected_trades.csv")
    old_ledger = pd.read_csv(base_run / "portfolio_trades.csv")
    old_control = pd.read_csv(base_run / "g_control_portfolio_trades.csv")
    old_daily = pd.read_csv(base_run / "daily_results.csv")
    cutoff = date.fromisoformat(str(old_daily.day.max()))
    if day <= cutoff:
        raise ValueError("Dated extension must follow the frozen G-2 history")
    calendar = pd.read_csv(g_archive / "dataset/source_session_eligibility.csv")
    eligible = sorted(calendar.loc[calendar.eligible.astype(str).str.lower().eq("true"), "day"].tolist())
    days = [date.fromisoformat(value) for value in old_daily.day] + [day]
    if eligible != [str(value) for value in days]:
        raise ValueError("G archive calendar differs from the G-2 extension calendar")

    g_control = pd.read_csv(g_run / "portfolio_trades.csv")
    _historical_control_parity(old_control, g_control)
    dated_control = g_control.loc[g_control.day.eq(str(day))]
    if (len(dated_control) != int(proof["selected_orders"])
            or int(dated_control.portfolio_executed.sum()) != int(proof["trades"])
            or not np.isclose(dated_control.portfolio_net_profit_rupees.sum(),
                              proof["net_profit_rupees"], rtol=0, atol=1e-7)):
        raise ValueError("G dated trade ledger does not match its proof")
    orders = pd.read_csv(g_run / "dated_selected_orders.csv")
    identity = ["day", "sid", "setup_id", "tradingsymbol", "side"]
    expected = dated_control[identity].astype(str).sort_values(identity).reset_index(drop=True)
    observed = orders[identity].astype(str).sort_values(identity).reset_index(drop=True)
    if len(orders) != int(proof["selected_orders"]) or not expected.equals(observed):
        raise ValueError("G dated selected orders differ from the trade ledger")
    control = pd.concat([old_control, dated_control], ignore_index=True, sort=False)
    _historical_rows_equal(old_control, control)
    paths = ext._selected_paths(orders, day, snapshot_manifest.parent)
    dated_trades, _, _ = g2.evaluate(
        dict(source_g=source_g, orders=orders, paths=paths, v9_config=base), settings
    )
    dated_trades["selection_evidence"] = "HASH_VERIFIED_FROZEN_G_DATED_EXTENSION"
    dated_trades["execution_path_evidence"] = "HASH_VERIFIED_DAILY_INPUT_SNAPSHOT"
    trades = pd.concat([old_trades, dated_trades], ignore_index=True, sort=False)
    ledger, combined = g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    _historical_rows_equal(old_ledger, ledger)
    control_identity = control[identity].astype(str).sort_values(identity).reset_index(drop=True)
    ledger_identity = ledger[identity].astype(str).sort_values(identity).reset_index(drop=True)
    if not control_identity.equals(ledger_identity):
        raise ValueError("G-2 selection differs from frozen G")
    summary = g2.read_json(base_run / "summary.json")
    summary.update(combined)
    summary.update(selection_count=len(trades), first_session=str(min(days)),
                   last_session=str(max(days)), through_day=str(day), session_count=len(days),
                   historical_overlap_parity=True, historical_overlap_cutoff=str(cutoff))
    control_summary = g2.read_json(g_run / "summary.json")
    dataset = dict(
        source=sealed, g_config_path=source_g_path, source_g=source_g,
        days=days, control_ledger=control, control_summary=control_summary,
        extension_evidence=[*base_provenance["complete_daily_extensions"], dict(
            source_kind="VERIFIED_DATED_TRADE_EXTENSION", session_date=str(day),
            g_archive=str(g_archive), g_extension_proof_sha256=g2.sha256(proof_path),
            source_g_daily_run=str(source_run),
            source_g_daily_result_sha256=g2.sha256(source_run / "replay_result.json"),
            snapshot_manifest=str(snapshot_manifest),
            snapshot_manifest_sha256=g2.sha256(snapshot_manifest),
            selected_orders=len(orders), excluded_stocks=proof["coverage"]["excluded_stocks"],
            historical_rows_unchanged=True,
        )],
    )
    staging = output.with_name(".staging_" + output.name)
    if staging.exists():
        raise FileExistsError(f"Refusing to overwrite staging: {staging}")
    g2.save_run(staging, dataset, settings, trades, ledger, summary)
    control.to_csv(staging / "g_control_portfolio_trades.csv", index=False)
    (staging / "frozen_config.json").write_bytes((base_run / "frozen_config.json").read_bytes())
    check = dict(state="COMPLETE", strategy=g2.VERSION, through_day=str(day),
                 sessions=len(days), selected_orders=len(trades),
                 trades=summary["portfolio_executed_trades"],
                 net_profit_rupees=summary["net_profit_rupees"],
                 old_results_unchanged=True, old_sessions=len(old_daily),
                 base_run=str(base_run), g_archive=str(g_archive),
                 dated_extension_proof=str(proof_path),
                 added_daily_results=g2.build_breakdowns(ledger, days)["daily"].tail(1).to_dict("records"))
    g2.dump_json(staging / "extension_validation.json", check)
    provenance = g2.read_json(staging / "provenance.json")
    provenance.update(base_run=str(base_run), dated_g_archive=str(g_archive),
                      dated_g_proof_sha256=g2.sha256(proof_path),
                      extension_helper=str(Path(__file__).resolve()),
                      extension_helper_sha256=g2.sha256(Path(__file__)),
                      historical_overlap_parity=True, historical_overlap_cutoff=str(cutoff))
    provenance["artifacts"] = {path.name: {"bytes": path.stat().st_size, "sha256": g2.sha256(path)}
                               for path in staging.iterdir() if path.is_file() and path.name != "provenance.json"}
    g2.dump_json(staging / "provenance.json", provenance)
    ext._verify_output(staging)
    _historical_rows_equal(old_ledger, pd.read_csv(staging / "portfolio_trades.csv"))
    if not np.isclose(pd.read_csv(staging / "daily_results.csv").net_pnl_rupees.sum(),
                      summary["net_profit_rupees"], rtol=0, atol=0.01):
        raise ValueError("G-2 daily and summary P&L disagree")
    os.replace(staging, output)
    return check


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-run", required=True, type=Path)
    parser.add_argument("--g-archive", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    print(json.dumps(build(args.base_run, args.g_archive, args.output_dir, args.date), indent=2, default=str))
