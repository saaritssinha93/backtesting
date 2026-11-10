"""Read-only parity audit for G7 V5 candidate reconstruction.

Rebuilds the G3 selection inputs before the stock OI gate, then checks that
unchanged G7 slots reproduce the frozen V4 opportunity identities. No orders
or live settings are changed by this research helper.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

import pandas as pd

import fno_v13_replay_source as source
from research import g3_target_cutoff_contract as contract


ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "outputs/g3_early_entry_20261023/full_history_source/cache"
G3_PRODUCTION = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/production/runs")
G7_NATIVE = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_research/runs/2026-10-27/20261027T165517683392_v4_extremes/inputs/native.csv")
UNAFFECTED = ("0931_LONG", "0951_LONG", "1046_LONG")


def load_day(day: str) -> tuple[pd.DataFrame, dict]:
    folder = CACHE / day
    parquet = folder / "features.parquet"
    proof = folder / "provenance.json"
    if parquet.is_file() and proof.is_file():
        metadata = json.loads(proof.read_text(encoding="utf-8"))
        if source._sha(parquet) != metadata["features_sha256"]:
            raise ValueError(f"Prefilter source hash changed: {day}")
        return pd.read_parquet(parquet), {"path": str(parquet), "sha256": source._sha(parquet),
                                          "source": metadata.get("candidate_pool")}
    production = G3_PRODUCTION / day
    runs = sorted(production.glob("*/run_manifest.json"))
    if len(runs) != 1:
        raise ValueError(f"Expected one sealed G3 feature ledger for {day}, found {len(runs)}")
    manifest_path = runs[0]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    path = manifest_path.parent / manifest["artifacts"]["feature_ledger"]["path"]
    if source._sha(path) != manifest["artifacts"]["feature_ledger"]["sha256"]:
        raise ValueError(f"Production feature ledger hash changed: {day}")
    return pd.read_csv(path, low_memory=False), {"path": str(path), "sha256": source._sha(path),
                                                 "source": "SEALED_G3_FEATURE_LEDGER_BEFORE_SELECTION"}


def run(output: Path, *, days: list[str] | None = None,
        setups: tuple[str, ...] = UNAFFECTED) -> dict:
    if output.exists():
        raise FileExistsError(output)
    native = pd.read_csv(G7_NATIVE, low_memory=False)
    if native.sid.duplicated().any():
        raise ValueError("V4 native opportunity identities are not unique")
    calendar = sorted(native.day.astype(str).str[:10].unique().tolist())
    if days is not None:
        if not set(days).issubset(calendar):
            raise ValueError("Requested day is outside frozen V4 source")
        calendar = [day for day in calendar if day in set(days)]
    records, selected, proofs = [], [], []
    for day in calendar:
        raw, proof = load_day(day)
        if raw.day.astype(str).str[:10].nunique() != 1 or str(raw.day.iloc[0])[:10] != day:
            raise ValueError(f"Prefilter day mismatch: {day}")
        context = pd.to_numeric(raw.nifty_first_bar_return_pct, errors="coerce").dropna().unique()
        if len(context) != 1:
            raise ValueError(f"Nifty opening context ambiguous: {day}")
        strict = source._strict_signals(raw, float(context[0]))
        strict = strict.copy()
        strict["sid"] = range(len(strict))
        audit = contract.selection_audit(strict)
        chosen = audit.loc[audit.v9_selected.eq(True) & audit.setup_id.isin(setups)].copy()
        frozen = native.loc[native.day.eq(day) & native.setup_id.isin(setups)]
        ours = set(zip(chosen.setup_id, chosen.tradingsymbol))
        theirs = set(zip(frozen.setup_id, frozen.tradingsymbol))
        records.append(dict(day=day, raw_rows=len(raw), strict_candidates=len(strict),
                            reconstructed_selected=len(ours), frozen_selected=len(theirs),
                            matching=ours == theirs, missing=sorted(theirs - ours),
                            extra=sorted(ours - theirs)))
        selected.append(chosen)
        proofs.append(dict(day=day, **proof))
        print("PARITY", day, "raw", len(raw), "strict", len(strict),
              "reconstructed", len(ours), "frozen", len(theirs), "matching", ours == theirs, flush=True)
    result = dict(first_day=calendar[0], last_day=calendar[-1], sessions=len(calendar),
                  all_matching=all(row["matching"] for row in records), checks=records,
                  source_proofs=proofs, setups=setups,
                  v4_native=str(G7_NATIVE), v4_native_sha256=source._sha(G7_NATIVE),
                  scope="BACKTEST_ONLY", execution_authority=False)
    output.mkdir(parents=True)
    (output / "parity.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    pd.concat(selected, ignore_index=True, sort=False).to_pickle(output / "reconstructed_unchanged_slots.pkl")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--days", nargs="*")
    parser.add_argument("--setups", nargs="*", default=list(UNAFFECTED))
    args = parser.parse_args()
    result = run(args.output, days=args.days, setups=tuple(args.setups))
    print("AUDIT", result["sessions"], "sessions all_matching", result["all_matching"], flush=True)


if __name__ == "__main__":
    main()
