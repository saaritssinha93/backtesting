"""Append retained staged-stop G-2 research sessions without changing old results.

Reads a completed, checksum-verified causal dataset and its frozen G control.
Never fetches data, changes strategy settings, or writes into previous runs.
"""
from __future__ import annotations

import argparse
from datetime import date
from io import StringIO
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext


def check_parity(old: pd.DataFrame, current: pd.DataFrame, keys: list[str]) -> None:
    # Compare persisted CSV values, normalizing in-memory timestamps and the
    # empty-string/null equivalence produced by the CSV reader.
    old = pd.read_csv(StringIO(old.to_csv(index=False)))
    current = pd.read_csv(StringIO(current.to_csv(index=False)))
    for column in keys:
        old[column] = old[column].astype(str)
        current[column] = current[column].astype(str)
    old = old.sort_values(keys).reset_index(drop=True)
    current = current.sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(
        old, current[old.columns], check_dtype=False, check_exact=False,
        rtol=1e-10, atol=1e-7,
    )


def verified_zero_session(proof_path: Path, day: date, source_g: dict,
                          source_g_path: Path, sealed: Path, base) -> dict:
    """Re-select frozen G from complete dated raw evidence; zero is not assumed."""
    from research import g2_session_forensic_audit as audit

    proof = g2.read_json(proof_path)
    if (proof.get("status") != "COMPLETE_RESEARCH_REPLAY"
            or proof.get("session_date") != str(day)
            or proof.get("G2", {}).get("selected_orders") != 0):
        raise ValueError("Dated proof is not a complete zero-order G-2 session")
    evidence = proof["provenance"]
    if (Path(evidence["g2_source_bundle"]).resolve() != sealed.resolve()
            or evidence["g2_source_bundle_manifest_sha256"] != g2.sha256(sealed / "bundle_manifest.json")):
        raise ValueError("Dated proof uses a different retained G source")
    run = Path(evidence["source_g_daily_run"])
    snapshot_manifest = Path(evidence["snapshot_manifest"])
    ledger_path = Path(evidence["feature_ledger"])
    for path, key in ((run / "replay_result.json", "source_g_daily_result_sha256"),
                      (snapshot_manifest, "snapshot_manifest_sha256"),
                      (ledger_path, "feature_ledger_sha256")):
        if g2.sha256(path) != evidence[key]:
            raise ValueError(f"Dated proof source hash mismatch: {path}")
    result = g2.read_json(run / "replay_result.json")
    coverage = result.get("coverage", {})
    if (result.get("state") != "SUCCESS" or result.get("complete") is not True
            or result.get("session_date") != str(day)
            or any(coverage.get(field) != 213 for field in
                   ("universe_stocks", "included_stocks", "checked_stocks"))
            or coverage.get("excluded_stocks") or coverage.get("problems")):
        raise ValueError("Dated proof lacks complete 213-stock session coverage")
    snapshot = g2.read_json(snapshot_manifest)
    if snapshot.get("complete") is not True or snapshot.get("session_date") != str(day):
        raise ValueError("Dated input snapshot is incomplete or misdated")
    ext.daily_replay._verify_input_snapshot(snapshot_manifest.parent, snapshot)
    ledger_meta = g2.read_json(run / "feature_ledger.csv.manifest.json")
    if ledger_meta["artifact_sha256"] != g2.sha256(ledger_path):
        raise ValueError("Dated raw feature ledger hash mismatch")
    raw = audit.normalize(pd.read_csv(ledger_path))
    if (raw.empty or set(raw.day) != {day} or raw.tradingsymbol.nunique() != 213
            or raw.duplicated(["tradingsymbol", "signal_ts"]).any()
            or not raw.confirmation_ts.sub(raw.signal_ts).eq(pd.Timedelta(minutes=1)).all()):
        raise ValueError("Dated raw feature scope or confirmation chronology is invalid")
    slots = {int(setup.signal_end.replace(":", "")) for setup in
             g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES["higher_frequency"])}
    if any(set(group.hhmm_int) != slots for _, group in raw.groupby("tradingsymbol")):
        raise ValueError("Dated feature ledger is missing a retained G setup slot")
    nifty = raw.nifty_first_bar_return_pct.dropna()
    if nifty.empty or nifty.nunique() != 1:
        raise ValueError("Dated NIFTY context is missing or inconsistent")
    strict = ext.daily_replay._strict_signals(raw, float(nifty.iloc[0]))
    selected = g2.g.select_orders(
        strict, base, g2.g.SelectionChange(**source_g["selection_change"]),
        core_first=source_g["core_first"], morning_slots=source_g.get("morning_slots", False),
        two_bar_continuation=source_g.get("two_bar_continuation", False))
    if not selected.empty:
        raise ValueError("Frozen G selects orders on the purported zero-order date")
    return dict(
        source_kind="VERIFIED_DATED_ZERO_ORDER_EXTENSION", session_date=str(day), complete=True,
        proof=str(proof_path.resolve()), proof_sha256=g2.sha256(proof_path),
        source_g_config=str(source_g_path), source_g_config_sha256=g2.sha256(source_g_path),
        source_g_daily_run=str(run), source_g_daily_result_sha256=g2.sha256(run / "replay_result.json"),
        feature_ledger=str(ledger_path), feature_ledger_sha256=g2.sha256(ledger_path),
        snapshot_manifest=str(snapshot_manifest), snapshot_manifest_sha256=g2.sha256(snapshot_manifest),
        coverage=coverage, raw_features=len(raw), strict_signals=len(strict),
        retained_g_selection_recomputed=True, selected_orders=0, trades=0, net_pnl_rupees=0.0,
    )


def build(base_run: Path, source: Path, output: Path, through_day: date,
          zero_order_proof: Path | None = None) -> dict:
    if output.exists():
        raise FileExistsError(f"Refusing to overwrite output: {output}")
    base_provenance = ext._verify_output(base_run)
    sealed = Path(base_provenance["source_bundle"])
    g2.verify_bundle(sealed)
    source_g_path = Path(base_provenance["source_g_config"])
    if g2.sha256(source_g_path) != base_provenance["source_g_config_sha256"]:
        raise ValueError("Retained G settings changed")
    source_g = g2.g.checked_settings(g2.read_json(source_g_path))
    settings = g2.checked_settings(g2.read_json(base_run / "frozen_config.json"), source_g)
    base = ext._portfolio_config(source_g)
    old_trades = pd.read_csv(base_run / "selected_trades.csv")
    old_ledger = pd.read_csv(base_run / "portfolio_trades.csv")
    old_daily = pd.read_csv(base_run / "daily_results.csv")
    cutoff = date.fromisoformat(str(old_daily.day.max()))

    folder = source / "dataset"
    manifest = g2.read_json(folder / "dataset_manifest.json")
    for name, expected in manifest["output_sha256"].items():
        if g2.sha256(folder / name) != expected:
            raise ValueError(f"Extension dataset artifact hash mismatch: {name}")
    source_metadata = g2.read_json(source / "g_backtest/run_metadata.json")
    if source_metadata["frozen_g_config_sha256"] != g2.sha256(source_g_path):
        raise ValueError("Extension G control uses different frozen settings")
    if source_metadata.get("historical_overlap_parity") is not True:
        raise ValueError("Extension source did not pass historical parity")
    dataset_days = [date.fromisoformat(day) for day in manifest["days"]
                    if cutoff < date.fromisoformat(day) <= through_day]
    new_days = dataset_days.copy()
    zero_evidence = []
    if zero_order_proof is not None:
        if through_day <= cutoff or through_day in new_days:
            raise ValueError("Zero-order proof must extend beyond existing and dataset sessions")
        zero_evidence.append(verified_zero_session(
            zero_order_proof, through_day, source_g, source_g_path, sealed, base))
        new_days.append(through_day)
    if not new_days or max(new_days) != through_day:
        raise ValueError("Extension source does not reach the requested cutoff")
    days = [date.fromisoformat(day) for day in old_daily.day] + new_days
    if len(set(days)) != len(days):
        raise ValueError("Duplicate session dates")

    signals = pd.read_parquet(folder / "signals.parquet")
    signals = signals.loc[pd.to_datetime(signals.day).dt.date.isin(new_days)].copy()
    orders = g2.g.select_orders(
        signals, base, g2.g.SelectionChange(**source_g["selection_change"]),
        core_first=source_g["core_first"],
        morning_slots=source_g.get("morning_slots", False),
        two_bar_continuation=source_g.get("two_bar_continuation", False),
    )
    with np.load(folder / "paths.npz", allow_pickle=False) as archive:
        paths = {int(sid): {key: archive[f"{int(sid)}_{key}"].copy()
                           for key in ("timestamp_ns", "open", "high", "low", "close")}
                 for sid in orders.sid}
    dataset = dict(source_g=source_g, orders=orders, paths=paths, v9_config=base)
    if orders.empty:
        # A completed zero-order interval is still a valid calendar extension.
        extension = old_trades.iloc[:0].copy()
        summary = g2.read_json(base_run / "summary.json")
    else:
        extension, _, summary = g2.evaluate(dataset, settings)
    extension["selection_evidence"] = "HASH_VERIFIED_FROZEN_G_RESEARCH_EXTENSION"
    extension["execution_path_evidence"] = "HASH_VERIFIED_CAUSAL_DATASET_PATHS"
    trades = pd.concat([old_trades, extension], ignore_index=True, sort=False)
    ledger, combined = g2.g.v9.v6.apply_portfolio_constraints(trades, base.portfolio_config())
    summary.update(combined)
    summary.update(selection_count=len(trades), first_session=str(min(days)),
                   last_session=str(max(days)), through_day=str(through_day),
                   session_count=len(days), historical_overlap_parity=True,
                   historical_overlap_cutoff=str(cutoff))

    if "g_control_portfolio_trades.csv" in base_provenance["artifacts"]:
        control_frames = [pd.read_csv(base_run / "g_control_portfolio_trades.csv")]
    else:
        control_frames = [pd.read_csv(sealed / "g_backtest/selected_trades.csv")]
        for evidence in base_provenance["complete_daily_extensions"]:
            run = Path(evidence["run"])
            for filename, key in (("replay_result.json", "replay_result_sha256"),
                                  ("source_manifest.json", "source_manifest_sha256"),
                                  ("selected_orders.csv", "selected_orders_sha256")):
                if g2.sha256(run / filename) != evidence[key]:
                    raise ValueError(f"Historical daily G source changed: {run / filename}")
            control = pd.read_csv(run / "portfolio_trades.csv")
            official = g2.read_json(run / "replay_result.json")["metrics"]
            observed = g2.g.r.metric(control, [evidence["session_date"]])
            for key in ("selected_orders", "trades", "wins", "losses", "net_profit_rupees"):
                if not np.isclose(observed[key], official[key], rtol=1e-10, atol=1e-7):
                    raise ValueError(f"Historical daily G control changed: {run}, {key}")
            control_frames.append(control)
    new_control = pd.read_csv(source / "g_backtest/portfolio_trades.csv")
    new_control = new_control.loc[pd.to_datetime(new_control.day).dt.date.isin(new_days)].copy()
    identity = ["day", "sid", "setup_id", "tradingsymbol", "side"]
    check_parity(orders[identity], new_control[identity], identity)
    control_trades = pd.concat([*control_frames, new_control], ignore_index=True, sort=False)
    control_ledger, control_summary = g2.g.v9.v6.apply_portfolio_constraints(
        control_trades, base.portfolio_config())

    overlap = ledger.loc[pd.to_datetime(ledger.day).dt.date.le(cutoff)]
    check_parity(old_ledger, overlap, identity)
    breakdowns = g2.build_breakdowns(ledger, days)
    new_daily = breakdowns["daily"].copy()
    new_daily["day"] = new_daily.day.astype(str)
    check_parity(old_daily, new_daily.loc[new_daily.day.le(str(cutoff))], ["day"])
    if not np.isclose(new_daily.net_pnl_rupees.sum(), summary["net_profit_rupees"]):
        raise ValueError("Daily and summary P&L disagree")
    extension_evidence = {
        "source": str(source.resolve()),
        "dataset_manifest_sha256": g2.sha256(folder / "dataset_manifest.json"),
        "g_control_sha256": g2.sha256(source / "g_backtest/portfolio_trades.csv"),
        "g_run_metadata_sha256": g2.sha256(source / "g_backtest/run_metadata.json"),
        "dataset_through_day": manifest["through_day"],
        "first_session": str(min(dataset_days)) if dataset_days else None,
        "last_session": str(max(dataset_days)) if dataset_days else None,
        "days": [str(day) for day in dataset_days], "selection_count": len(extension),
        "g_selection_identity_verified": True,
    }
    dataset.update(source=sealed, g_config_path=source_g_path, days=days,
                   control_ledger=control_ledger, control_summary=control_summary,
                   extension_evidence=[*base_provenance["complete_daily_extensions"], extension_evidence, *zero_evidence])
    g2.save_run(output, dataset, settings, trades, ledger, summary)
    control_ledger.to_csv(output / "g_control_portfolio_trades.csv", index=False)
    # Keep the frozen strategy file byte-for-byte identical to the published base.
    (output / "frozen_config.json").write_bytes((base_run / "frozen_config.json").read_bytes())
    validation = dict(
        state="COMPLETE", strategy=g2.VERSION, through_day=str(through_day),
        sessions=len(days), selected_orders=len(trades), trades=summary["portfolio_executed_trades"],
        net_profit_rupees=summary["net_profit_rupees"], old_results_unchanged=True,
        old_sessions=len(old_daily), old_selected_orders=len(old_trades),
        base_run=str(base_run.resolve()), base_provenance_sha256=g2.sha256(base_run / "provenance.json"),
        frozen_config_unchanged=True, extension=extension_evidence, dated_zero_order_extensions=zero_evidence,
        added_daily_results=new_daily.loc[new_daily.day.gt(str(cutoff))].to_dict(orient="records"),
    )
    g2.dump_json(output / "extension_validation.json", validation)
    provenance = g2.read_json(output / "provenance.json")
    provenance.update(base_run=str(base_run.resolve()), extension_helper=str(Path(__file__).resolve()),
                      extension_helper_sha256=g2.sha256(Path(__file__)),
                      historical_overlap_parity=True, historical_overlap_cutoff=str(cutoff))
    provenance["artifacts"] = {path.name: {"bytes": path.stat().st_size, "sha256": g2.sha256(path)}
                               for path in output.iterdir() if path.is_file() and path.name != "provenance.json"}
    g2.dump_json(output / "provenance.json", provenance)
    ext._verify_output(output)
    return validation


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-run", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--through-day", type=date.fromisoformat, required=True)
    parser.add_argument("--zero-order-proof", type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.base_run, args.source, args.output_dir, args.through_day,
                           args.zero_order_proof), indent=2))
