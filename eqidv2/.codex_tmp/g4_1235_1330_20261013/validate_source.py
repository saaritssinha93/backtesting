"""Independent schema, timestamp, OHLC and provenance checks of afternoon source."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent


def sha(path):
    d = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            d.update(chunk)
    return d.hexdigest()


def main():
    summary = json.loads((HERE / "source_summary.json").read_text())
    frame = pd.read_parquet(HERE / "candidates.parquet")
    checks = []

    def check(name, good):
        checks.append({"name": name, "passed": bool(good)})

    for name, digest in summary["output_sha256"].items():
        check("output_hash_" + name, sha(HERE / name) == digest)
    check("unique_sids", not frame.sid.duplicated().any())
    check("declared_row_count", len(frame) == summary["candidate_count"])
    check("coverage_49_sessions", len(summary["days"]) == 49 and summary["days"] == sorted(set(summary["days"])))
    check("expected_last_session", summary["end_day"] == "2026-10-12")
    signal = pd.to_datetime(frame.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    conf = pd.to_datetime(frame.confirmation_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    one = pd.to_datetime(frame.v9_1m_feature_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    five = pd.to_datetime(frame.v9_5m_feature_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    check("source_session_membership", signal.dt.strftime("%Y-%m-%d").isin(summary["days"]).all())
    check("requested_clocks_only", signal.dt.strftime("%H:%M").isin(summary["clocks"]).all())
    check("all_24_pairs_present", len(frame.groupby(["hhmm_int", "side"]).size()) == 24)
    check("exact_next_minute_confirmation", conf.eq(signal + pd.Timedelta(minutes=1)).all())
    check("exact_one_minute_feature", one.eq(conf).all())
    check("five_minute_feature_causality", five.le(signal).all())
    check("one_minute_feature_causality", one.le(conf).all())
    check("raw_oi_envelope", frame.oi_change_pct.between(.05, 1.).all())
    check("raw_price_envelope", frame.price_change_pct.abs().ge(.1).all())
    check("raw_five_minute_volume_envelope", frame.volume_ratio.ge(.8).all())
    sign = frame.side.map({"LONG": 1., "SHORT": -1.})
    check("confirmation_direction", ((frame.confirmation_close-frame.confirmation_open)*sign).gt(0).all())
    check("confirmation_signal_close", ((frame.confirmation_close-frame.signal_close)*sign).gt(0).all())
    with np.load(HERE / "paths.npz", allow_pickle=False) as packed:
        path_sids = {int(name.split("_", 1)[0]) for name in packed.files}
        check("all_sids_have_paths", path_sids == set(frame.sid.astype(int)))
        for row in frame.itertuples(index=False):
            sid = int(row.sid)
            timestamp = packed[f"{sid}_timestamp_ns"]
            ohlc = [packed[f"{sid}_{name}"] for name in ("open", "high", "low", "close")]
            confirmation = pd.Timestamp(row.confirmation_ts)
            day = confirmation.tz_convert("Asia/Kolkata").date()
            cutoff = pd.Timestamp(f"{day} 15:15", tz="Asia/Kolkata").value
            good = (len(timestamp) > 0 and np.all(np.diff(timestamp) > 0)
                    and timestamp[0] > confirmation.value and timestamp[-1] <= cutoff
                    and all(len(values) == len(timestamp) for values in ohlc)
                    and all(np.isfinite(values).all() and (values > 0).all() for values in ohlc)
                    and np.all(ohlc[1] >= np.maximum(ohlc[0], ohlc[3]))
                    and np.all(ohlc[2] <= np.minimum(ohlc[0], ohlc[3])))
            check(f"minute_path_{sid}", good)
    cache = Path(summary["cache"])
    for day, digest in summary["evidence"]["recent_day_manifests"].items():
        path = cache / day / "manifest.json"
        check(f"cache_manifest_{day}", sha(path) == digest)
        manifest = json.loads(path.read_text())
        check(f"snapshot_manifest_{day}", sha(manifest["snapshot_manifest"]) == manifest["snapshot_manifest_sha256"])
        check(f"candidate_hash_{day}", sha(path.parent / "candidates.parquet") == manifest["candidate_sha256"])
        check(f"path_hash_{day}", sha(path.parent / "paths.npz") == manifest["paths_sha256"])
    failed = [item for item in checks if not item["passed"]]
    report = {"status": "FAIL" if failed else "PASS", "check_count": len(checks),
              "candidate_count": len(frame), "path_count": len(path_sids),
              "source_summary_sha256": sha(HERE / "source_summary.json"), "failed": failed,
              "negative_or_nonfinite_liquidity": int((~np.isfinite(frame.traded_value) | frame.traded_value.lt(0)).sum()),
              "checks": checks}
    (HERE / "source_validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "checks"}, indent=2))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
