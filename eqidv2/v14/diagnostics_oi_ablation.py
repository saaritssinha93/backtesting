"""Same-universe OI-on/off diagnostic from sealed broad FNO candidate inputs.

The broad all_5m_features table precedes the OI gates. No strategy files or
profiles are changed. Results are conditional historical diagnostics, not a
new validation result or recommendation to disable OI.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import baseline, features, path_evidence
from .diagnostics_baseline import ARCHIVE, OUTPUT, V14_RUN, profit_metrics, sha


def _verified(path, expected):
    if sha(path) != expected:
        raise ValueError(f"Sealed source changed: {path}")


def select_arms(broad: pd.DataFrame):
    """Keep pre-OI candidates for the control; share every non-OI decision."""
    strict = features.strict_signals(broad)
    oi_gate = strict.oi.gt(strict.prev_oi) & strict.oi_change_pct.between(.05, 1.)
    selected = {
        "OI_ON": baseline.select_orders(strict.loc[oi_gate].copy(),
            lambda rows, setup: rows.oi_change_pct.ge(setup["oi_change_pct"])),
        "OI_OFF": baseline.select_orders(strict),
    }
    return strict, oi_gate, selected


def run(output=OUTPUT, *, v14_run=V14_RUN):
    output = Path(output) / "fno_oi_ablation"
    output.mkdir(parents=True, exist_ok=True)
    provenance = json.loads((ARCHIVE / "source_study_provenance.json").read_text())
    bundle = Path(provenance["source_bundle"])
    _verified(bundle / "bundle_manifest.json", provenance["source_bundle_manifest_sha256"])
    manifest = json.loads((bundle / "bundle_manifest.json").read_text())
    broad_path = bundle / "dataset/all_5m_features.parquet"
    annotated_path = bundle / "dataset/annotated.parquet"
    for path in (broad_path, annotated_path):
        _verified(path, manifest["artifacts"][path.relative_to(bundle).as_posix()]["sha256"])
    broad = pd.read_parquet(broad_path)
    annotated = pd.read_parquet(annotated_path)
    days = json.loads((Path(v14_run) / "evaluation_coverage.json").read_text())["evaluated_days"]
    if not set(days).issubset(set(broad.day.astype(str))):
        raise ValueError("A requested date is absent from the broad original FNO dataset")
    breadth = dict(total_observed_bars=len(broad), outside_base_oi_gate=int((~broad.oi_change_pct.between(.05, 1.)).sum()),
                   missing_oi_bars=int(broad.oi_change_pct.isna().sum()),
                   annotated_rows=len(annotated), annotated_min_oi_change=float(annotated.oi_change_pct.min()))
    broad = broad.loc[broad.day.astype(str).isin(days) & broad.hhmm_int.isin(features.SIGNAL_TIMES)].copy()
    context = annotated.groupby(annotated.day.astype(str)).nifty_first_bar_return_pct.agg(["min", "max"])
    if not np.isclose(context["min"], context["max"], atol=1e-10, rtol=0, equal_nan=True).all():
        raise ValueError("Conflicting original NIFTY context")
    broad["day"] = broad.day.astype(str)
    broad["nifty_first_bar_return_pct"] = broad.day.map(context["min"])
    broad["sid"] = np.arange(len(broad), dtype=np.int64)
    strict, oi_gate, selected = select_arms(broad)
    archived = pd.read_csv(ARCHIVE / "trades.csv", float_precision="round_trip")
    archived = archived.loc[archived.day.isin(days)].copy()
    identity = ["day", "tradingsymbol", "setup_id"]
    original_keys = set(map(tuple, archived[identity].to_numpy()))
    on_keys = set(map(tuple, selected["OI_ON"][identity].to_numpy()))
    if original_keys != on_keys:
        (output / "selection_mismatch.json").write_text(json.dumps(dict(missing=sorted(original_keys - on_keys), additional=sorted(on_keys - original_keys)), indent=2))
        raise ValueError("OI-on selections do not reproduce archived G3; ablation stopped")
    snapshot = Path(provenance["historical_snapshot"])
    _verified(snapshot / "snapshot_manifest.json", provenance["verified_snapshot_manifest_hashes"][str(snapshot / "snapshot_manifest.json")])
    records = json.loads((snapshot / "snapshot_manifest.json").read_text())["sources"]
    records = {record["snapshot_relative_path"]: record for record in records if record.get("captured")}
    union = pd.concat(selected.values(), ignore_index=True).drop_duplicates("sid")
    paths, evidence, sources, unknown_days = {}, [], [], set()
    for symbol, rows in union.groupby("tradingsymbol", sort=True):
        relative = f"equity_1m/{symbol}_stocks_indicators_1min.parquet"
        path = snapshot / relative
        if relative not in records or not path.is_file():
            raise ValueError(f"Missing sealed cash input for selected symbol {symbol}")
        _verified(path, records[relative]["sha256"])
        minute = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume"])
        minute["ts"] = pd.to_datetime(minute.date, utc=True).dt.tz_convert(features.IST)
        sources.append(dict(symbol=symbol, source=str(path), sha256=sha(path)))
        for row in rows.itertuples(index=False):
            observed = baseline.native_paths(minute, row.confirmation_ts)
            proof = path_evidence.assess(row, observed)
            evidence.append(dict(sid=int(row.sid), day=row.day, symbol=symbol, resolved=proof["resolved"],
                                 reason=proof["reason"], **proof["evidence"]))
            if proof["resolved"]:
                paths[int(row.sid)] = observed if proof["evidence"]["full_session_observed"] else proof["path"]
            else:
                unknown_days.add(row.day)
    matched_days = [day for day in days if day not in unknown_days]
    full_paths = all(record["full_session_observed"] for record in evidence if record["day"] in matched_days)
    pd.DataFrame(evidence).to_csv(output / "path_evidence.csv", index=False)
    pd.DataFrame(sources).to_csv(output / "sealed_minute_sources.csv", index=False)
    summaries, ledgers = [], {}
    for label, orders in selected.items():
        orders.to_csv(output / f"selected_{label}.csv", index=False)
        retained = orders.loc[orders.day.isin(matched_days)].copy()
        ledger = baseline.simulate(retained, paths, allow_observed_prefix=not full_paths)
        ledger.to_csv(output / f"trades_{label}.csv", index=False)
        ledgers[label] = ledger
        summaries.append(dict(variant=label, **profit_metrics(ledger, matched_days)))
    # The OI-on reference must also reproduce actual archived numerical fills.
    original = archived.loc[archived.day.isin(matched_days)].copy()
    same = ledgers["OI_ON"].merge(original, on=identity, suffixes=("_replay", "_archive"), validate="one_to_one")
    for column in ("entry_price", "exit_price", "net_return_pct", "portfolio_net_profit_rupees"):
        if not np.allclose(same[column + "_replay"], same[column + "_archive"], atol=1e-8, rtol=1e-10, equal_nan=True):
            raise ValueError(f"OI-on execution differs from archived G3: {column}")
    for column in ("portfolio_executed", "exit_reason"):
        if not same[column + "_replay"].fillna("").astype(str).eq(same[column + "_archive"].fillna("").astype(str)).all():
            raise ValueError(f"OI-on execution differs from archived G3: {column}")
    pd.DataFrame(summaries).to_csv(output / "comparison.csv", index=False)
    detail = []
    for label, ledger in ledgers.items():
        executed = ledger.loc[ledger.portfolio_executed]
        for column in ("side", "setup_id", "exit_reason"):
            for key, rows in executed.groupby(column):
                detail.append(dict(variant=label, grouping=column, category=key, trades=len(rows),
                                   net_pnl=float(rows.portfolio_net_profit_rupees.sum()), wins=int(rows.portfolio_net_profit_rupees.gt(0).sum())))
    pd.DataFrame(detail).to_csv(output / "side_setup_exits.csv", index=False)
    summary = dict(status="COMPLETE_DIAGNOSTIC", changed_only="Base and per-setup stock futures OI gates disabled before native selection",
                   requested_days=days, evaluated_days=matched_days, excluded_unknown_days=sorted(unknown_days),
                   original_universe_symbols=int(broad.tradingsymbol.nunique()), raw_signal_slot_rows=len(broad),
                   cash_strict_candidates=len(strict), oi_on_strict_candidates=int(oi_gate.sum()),
                   candidate_breadth=breadth, oi_on_archived_selected_parity=True, oi_on_archived_execution_parity=True,
                   path_policy="STRICT_FULL_SESSION" if full_paths else path_evidence.EVIDENCE_VERSION,
                   selected_unique_paths=len(evidence), all_selected_paths_full_session=full_paths, results=summaries,
                   source_broad_features=str(broad_path), source_sha256=sha(broad_path),
                   limitations=["Conditional same-universe ablation on 18 V14-evaluable historical dates, not independent validation.",
                                "Removing OI changes candidate ranking competition and order/exposure count as well as eligibility.",
                                "Whole dates with any unresolved selected outcome are excluded from both arms; no replacement selections."])
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
