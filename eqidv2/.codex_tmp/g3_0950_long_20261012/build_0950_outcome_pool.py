"""Immutable input/outcome pool for joint 5m + 1m 09:50 LONG research."""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd

import strict_1pct_study as study


OUT = study.OUT / "joint_0950_5m_1m"
OUT.mkdir(parents=True, exist_ok=True)
CANDIDATES = OUT / "candidate_outcomes.parquet"
PROVENANCE = OUT / "candidate_outcomes_provenance.json"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    signals, bundle, provenance = study.previous.load_signals()
    days = [date.fromisoformat(value) for value in provenance["days"]]
    frozen = pd.read_csv(study.DEFAULT_FROZEN / "trades.csv")
    if set(study.contract.select_orders(signals).sid.astype(int)) != set(frozen.sid.astype(int)):
        raise RuntimeError("Frozen G3 baseline selection parity failed")
    for item in provenance["extensions"]:
        manifest = Path(item["snapshot"]) / "snapshot_manifest.json"
        if sha(manifest) != provenance["verified_snapshot_manifest_hashes"][str(manifest)]:
            raise RuntimeError(f"Historical extension source drift: {manifest}")
    hist = signals.loc[signals.signal_end.eq("09:50") & signals.side.eq("LONG")].copy()
    recent, recent_sources = study.recent_replay.candidates()
    donor = study.contract._ACTIVE_BY_ID["0956_LONG"]
    setup = replace(donor, signal_end="09:50", confirmation_end="09:51",
        price_change_pct=.10, oi_change_pct=.05, volume_ratio=.80,
        body_ratio=0., max_wick_ratio=1., stop_pct=1.25,target_pct=1.)
    key = setup.setup_id
    study.contract.CORE_SETUPS = (*study.contract.CORE_SETUPS, setup)
    study.contract.ACTIVE_SETUPS = (*study.contract.ACTIVE_SETUPS, setup)
    study.contract._CORE_BY_ID[key] = setup
    study.contract._ACTIVE_BY_ID[key] = setup
    study.contract.MIN_CONFIRMATION_VOLUME_BY_SIDE["LONG"] = 0.
    hist_audit = study.contract.selection_audit(hist)
    recent_audit = study.contract.selection_audit(recent)
    valid_hist = hist_audit.loc[hist_audit.setup_id.eq(key) & hist_audit.v9_filter_pass.eq(True)].copy()
    valid_recent = recent_audit.loc[recent_audit.setup_id.eq(key) & recent_audit.v9_filter_pass.eq(True)].copy()
    print("Valid candidates",len(valid_hist),"historical",len(valid_recent),"recent",flush=True)
    hist_paths = study.previous.paths_for(valid_hist,bundle,provenance)
    paths = dict(hist_paths)
    for day in study.recent_replay.RUNS:
        selection = valid_recent.loc[pd.to_datetime(valid_recent.day).dt.date.eq(day)].copy()
        if len(selection):
            snapshot = Path(recent_sources[str(day)]["snapshot_manifest"]).parent
            paths.update(study.recent_replay.ext._selected_paths(selection,day,snapshot))
    oct12_manifest = json.loads((study.OUT / "oct12_0950_pool_minute_sources.json").read_text())
    oct12_sources = {row["symbol"]: row for row in oct12_manifest["entries"]}
    for row in valid_recent.loc[pd.to_datetime(valid_recent.day).dt.date.eq(date(2026,10,12))].to_dict("records"):
        source = oct12_sources[row["tradingsymbol"]]
        path = Path(source["path"])
        if sha(path) != source["sha256"]:
            raise RuntimeError(f"October 12 minute source drift: {path}")
        minutes = pd.read_parquet(path)
        bars = minutes.loc[minutes.ts.gt(pd.Timestamp(row["confirmation_ts"])) &
            minutes.ts.le(pd.Timestamp("2026-10-12 15:15",tz=study.contract.IST))].copy()
        expected = pd.date_range("2026-10-12 09:52","2026-10-12 15:15",freq="min",tz=study.contract.IST)
        if len(bars) != len(expected) or not bars.ts.reset_index(drop=True).eq(pd.Series(expected)).all():
            raise RuntimeError(f"Incomplete October 12 execution path: {row['tradingsymbol']}")
        paths[int(row["sid"])] = dict(timestamp_ns=bars.ts.astype("int64").to_numpy(),
            **{name:bars[name].to_numpy(float) for name in ("open","high","low","close")})
    orders = pd.concat((valid_hist,valid_recent),ignore_index=True)
    if orders.sid.duplicated().any() or len(paths) != len(orders):
        raise RuntimeError("Duplicate or missing 09:50 candidate paths")
    orders["native_stop_pct"] = 1.25
    orders["native_target_pct"] = 1.0
    study.g2.g.v9.validate_paths(orders,paths)
    simulated = study.g2.simulate_staged(orders,paths,cost_bps=5.,max_entry_delay_minutes=10)
    simulated = study.g2.g.v9.v5.apply_fixed_capital_model(simulated,100_000.,5.)
    fields = ["sid","day","tradingsymbol","traded_value","price_change_pct","oi_change_pct",
        "volume_ratio","body_ratio","wick_ratio","v9_1m_volume_ratio","filled","entry_ts",
        "exit_ts","exit_reason","net_profit_rupees","signal_ts","confirmation_ts",
        "v9_filter_pass","v9_exact_confirmation_present"]
    missing = set(fields)-set(simulated)
    if missing:
        raise RuntimeError(f"Outcome columns missing: {missing}")
    table = simulated[fields].copy()
    table["source_group"] = ["historical"]*len(valid_hist)+["recent"]*len(valid_recent)
    table["day"] = pd.to_datetime(table.day).dt.date.astype(str)
    table.to_parquet(CANDIDATES,index=False)
    manifest = dict(scope="09:50 LONG signal candidates with valid exact 09:51 confirmation",
        historical_sessions=46,historical_candidate_count=len(valid_hist),
        recent_dates=[str(x) for x in study.recent_replay.DAYS],
        recent_candidate_count=len(valid_recent),
        exit=dict(initial_stop_pct=1.25,tightened_stop_pct=1.,tighten_after_minutes=120,
            target_pct=1.,cost_bps=5.,capital_per_entry_rs=100000.,leverage=5.),
        historic_source_manifest=str(bundle["root"] / "dataset_manifest.json"),
        historic_source_manifest_sha256=sha(bundle["root"] / "dataset_manifest.json"),
        frozen_trades_sha256=sha(study.DEFAULT_FROZEN / "trades.csv"),
        recent_sources=recent_sources,oct12_minute_manifest=str(study.OUT / "oct12_0950_pool_minute_sources.json"),
        oct12_minute_manifest_sha256=sha(study.OUT / "oct12_0950_pool_minute_sources.json"),
        outcome_file=str(CANDIDATES),outcome_sha256=sha(CANDIDATES))
    PROVENANCE.write_text(json.dumps(manifest,indent=2)+"\n",encoding="utf-8")
    print("Filled",int(table.filled.sum()),"of",len(table),flush=True)
    print(CANDIDATES,flush=True)


if __name__ == "__main__":
    main()
