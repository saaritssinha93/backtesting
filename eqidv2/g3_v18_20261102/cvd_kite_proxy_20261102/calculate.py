"""Calculate the fixed estimated CVD features from a verified Kite data capture."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path

import pandas as pd

from g3_v18_20261102.run import dump, guard_writes, sha
from .features import calculate_features, METHOD

PACKAGE = Path(__file__).resolve().parent


def run(data: Path, output: Path):
    if output.resolve() == PACKAGE or not output.resolve().is_relative_to(PACKAGE):
        raise ValueError("Output must be a new child of this CVD experiment")
    output.mkdir(parents=True, exist_ok=False)
    guard_writes(PACKAGE)
    manifest_bytes = (data / "fetch_manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    blob = (data / "bars_5m.parquet").read_bytes()
    digest = hashlib.sha256(blob).hexdigest()
    if digest != manifest["bars_sha256"]:
        raise ValueError("Kite candle capture hash mismatch")
    cutoff = pd.Timestamp(manifest["completed_bar_cutoff_ist"])
    dump(output / "configuration.json", dict(method=METHOD, source_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        input_sha256=digest, as_of_ist=str(cutoff), execution_authority=False,
        feature_code_sha256=sha(PACKAGE / "features.py"), methodology=json.loads((PACKAGE / "methodology.json").read_text())))
    bars = pd.read_parquet(io.BytesIO(blob))
    print(f"Calculating fixed proxy for {len(bars):,} five-minute bars / {bars.symbol.nunique()} symbols", flush=True)
    result = calculate_features(bars, as_of=cutoff)
    result.to_parquet(output / "features.parquet", index=False)
    summary = dict(method=METHOD, execution_authority=False, actual_aggressor_delta=False,
        rows=len(result), symbols=int(result.symbol.nunique()), ready_bars=int(result.cvd_proxy_ready.sum()),
        first_bar_ist=str(result.bar_end.min()), last_bar_ist=str(result.bar_end.max()),
        reason_counts=result.cvd_proxy_reason.value_counts().to_dict(), input_sha256=digest,
        as_of_ist=str(cutoff), source_fetch_complete=manifest["complete"])
    dump(output / "summary.json", summary)
    dump(output / "manifest.json", {"files": {p.name: sha(p) for p in output.iterdir() if p.is_file()}})
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    run(args.data, args.output)
