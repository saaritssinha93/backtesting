"""Calculate fixed advanced CVD features from the verified minute capture."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v18_20261102.run import dump, guard_writes, sha
from .features import calculate_features

PACKAGE = Path(__file__).resolve().parent


def run(source: Path, output: Path):
    if output.resolve() == PACKAGE or not output.resolve().is_relative_to(PACKAGE):
        raise ValueError("Output must be a fresh child of this advanced experiment")
    output.mkdir(parents=True, exist_ok=False)
    guard_writes(PACKAGE)
    manifest_bytes = (source / "fetch_manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    raw_bytes = (source / "minutes_1m.parquet").read_bytes()
    input_hash = hashlib.sha256(raw_bytes).hexdigest()
    if input_hash != manifest["minutes_sha256"]:
        raise ValueError("Minute capture hash mismatch")
    cutoff = pd.Timestamp(manifest["completed_minute_cutoff_ist"]).tz_convert("Asia/Kolkata")
    config = dict(methodology=json.loads((PACKAGE / "methodology.json").read_text(encoding="utf-8")),
        methodology_sha256=sha(PACKAGE / "methodology.json"), input_sha256=input_hash,
        feature_code_sha256=sha(PACKAGE / "features.py"), as_of_ist=str(cutoff),
        source_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(), execution_authority=False)
    dump(output / "configuration.json", config)
    minutes = pd.read_parquet(io.BytesIO(raw_bytes))
    del raw_bytes
    print(f"Calculating advanced CVD from {len(minutes):,} one-minute candles / {minutes.symbol.nunique()} symbols", flush=True)
    result = calculate_features(minutes, as_of=cutoff)
    del minutes
    if result.duplicated(["symbol", "bar_end"]).any() or result.bar_end.gt(cutoff).any():
        raise ValueError("Feature keys or availability failed")
    # Independently produced data-adapter aggregation must agree with feature bars.
    expected_bytes = (source / "aggregate_5m.parquet").read_bytes()
    if hashlib.sha256(expected_bytes).hexdigest() != manifest["bars_sha256"]:
        raise ValueError("Five-minute aggregate capture hash mismatch")
    expected = pd.read_parquet(io.BytesIO(expected_bytes))
    del expected_bytes
    actual = result.sort_values(["symbol", "bar_end"]).reset_index(drop=True)
    expected = expected.sort_values(["symbol", "bar_end"]).reset_index(drop=True)
    if len(actual) != len(expected) or actual.symbol.tolist() != expected.symbol.tolist():
        raise ValueError("Feature aggregation identities differ from data-adapter aggregation")
    if not np.array_equal(pd.DatetimeIndex(actual.bar_end).as_unit("ns").asi8,
                          pd.DatetimeIndex(expected.bar_end).as_unit("ns").asi8):
        raise ValueError("Feature aggregation clocks differ from data-adapter aggregation")
    for field in ("open", "high", "low", "close", "volume"):
        if not np.allclose(actual[field], expected[field], rtol=0, atol=1e-8, equal_nan=True):
            raise ValueError(f"Feature/data-adapter aggregate mismatch: {field}")
    for field in ("source_1m_count", "bar_contiguous"):
        if not np.array_equal(actual[field].to_numpy(), expected[field].to_numpy()):
            raise ValueError(f"Feature/data-adapter completeness mismatch: {field}")
    del expected, actual
    if sha(PACKAGE / "features.py") != config["feature_code_sha256"]:
        raise ValueError("Feature code changed during calculation")
    result.to_parquet(output / "features.parquet", index=False)
    summary = dict(execution_authority=False, actual_aggressor_delta=False, rows=len(result),
        symbols=int(result.symbol.nunique()), acceleration_ready_bars=int(result.adv_ready.sum()),
        rvol_ready_bars=int(result.adv_rvol_ready.sum()), first_bar_ist=str(result.bar_end.min()),
        last_bar_ist=str(result.bar_end.max()), requested_cutoff_ist=str(cutoff),
        reason_counts=result.adv_reason.value_counts().to_dict(), aggregate_ohlcv_reconciliation_passed=True,
        source_all_requests_succeeded=manifest["all_requests_succeeded"])
    dump(output / "summary.json", summary)
    dump(output / "manifest.json", dict(files={p.name: sha(p) for p in output.iterdir() if p.is_file()}))
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    run(args.data, args.output)
