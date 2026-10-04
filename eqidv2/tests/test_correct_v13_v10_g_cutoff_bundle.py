import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from tools import correct_v13_v10_g_cutoff_bundle as subject


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def _source_bundle(root: Path, *, signal_day: str = "2026-09-23") -> Path:
    source = root / "run_source_through_20260923"
    dataset = source / "dataset"
    backtest = source / "g_backtest"
    dataset.mkdir(parents=True)
    backtest.mkdir(parents=True)

    eligibility = pd.DataFrame(
        {
            "day": ["2026-09-22", "2026-09-23", "2026-09-24"],
            "expiry": [date(2026, 9, 29)] * 3,
            "eligible": [True, False, True],
            "coverage": [1.0, 0.5, 1.0],
        }
    )
    eligibility.to_parquet(dataset / "eligibility.parquet", index=False)
    eligibility.to_csv(dataset / "source_session_eligibility.csv", index=False)
    (dataset / "source_manifest.csv").write_text(
        "path,exists,sha256\nsource,True,abc\n", encoding="utf-8"
    )
    pd.DataFrame({"day": [signal_day], "sid": [1]}).to_parquet(
        dataset / "signals.parquet", index=False
    )
    dataset_manifest = {
        "schema": "TEST",
        "through_day": "2026-09-23",
        "rows": {"eligibility": 3, "signals": 1},
        "output_sha256": {
            "eligibility.parquet": subject.sha256_file(
                dataset / "eligibility.parquet"
            ),
            "signals.parquet": subject.sha256_file(dataset / "signals.parquet"),
        },
    }
    _write_json(dataset / "dataset_manifest.json", dataset_manifest)

    trades = pd.DataFrame(
        {
            "day": ["2026-09-23"],
            "sid": [1],
            "portfolio_net_profit_rupees": [123.45],
        }
    )
    trades.to_csv(backtest / "selected_trades.csv", index=False)
    trades.to_csv(backtest / "portfolio_trades.csv", index=False)
    _write_json(backtest / "summary.json", {"net_profit_rupees": 123.45})
    _write_json(
        backtest / "run_metadata.json",
        {
            "through_day": "2026-09-23",
            "source_dataset": str(dataset),
            "source_dataset_manifest_sha256": subject.sha256_file(
                dataset / "dataset_manifest.json"
            ),
            "metrics": {"full_history": {"net_profit_rupees": 123.45}},
        },
    )
    return source


def test_correction_publishes_new_complete_bundle_without_changing_parent(tmp_path):
    source = _source_bundle(tmp_path)
    output = tmp_path / "run_corrected_through_20260923"
    parent_before = subject.artifact_inventory(source)

    result = subject.correct_bundle(
        source,
        output,
        through_day="2026-09-23",
        now=datetime(2026, 9, 25, 7, 30, tzinfo=timezone.utc),
    )

    assert result["state"] == "COMPLETE"
    assert result["rows_before"] == 3
    assert result["rows_after"] == 2
    assert result["removed_rows"] == 1
    assert subject.artifact_inventory(source) == parent_before

    corrected_parquet = pd.read_parquet(output / subject.ELIGIBILITY_PARQUET)
    corrected_csv = pd.read_csv(output / subject.ELIGIBILITY_CSV)
    assert corrected_parquet["day"].astype(str).tolist() == [
        "2026-09-22",
        "2026-09-23",
    ]
    assert corrected_csv["day"].tolist() == ["2026-09-22", "2026-09-23"]

    dataset_manifest = subject.read_json(output / subject.DATASET_MANIFEST)
    assert dataset_manifest["rows"]["eligibility"] == 2
    assert dataset_manifest["eligibility_scope"] == {
        "max_day": "2026-09-23",
        "post_cutoff_rows": 0,
        "rows": 2,
        "through_day": "2026-09-23",
    }
    for name in (
        "eligibility.parquet",
        "source_manifest.csv",
        "source_session_eligibility.csv",
    ):
        assert dataset_manifest["output_sha256"][name] == subject.sha256_file(
            output / "dataset" / name
        )

    run_metadata = subject.read_json(output / subject.RUN_METADATA)
    assert run_metadata["metrics"] == {
        "full_history": {"net_profit_rupees": 123.45}
    }
    assert run_metadata["source_dataset"] == str(output / "dataset")
    assert run_metadata["source_dataset_manifest_sha256"] == subject.sha256_file(
        output / subject.DATASET_MANIFEST
    )
    assert run_metadata["cutoff_metadata_correction"]["removed_rows"] == 1
    assert not run_metadata["cutoff_metadata_correction"][
        "strategy_outputs_changed"
    ]

    for relative in subject.STRATEGY_OUTPUTS:
        assert subject.sha256_file(output / relative) == subject.sha256_file(
            source / relative
        )
    bundle_manifest = subject.read_json(output / "bundle_manifest.json")
    assert bundle_manifest["state"] == "COMPLETE"
    assert bundle_manifest["execution_authority"] is False
    assert bundle_manifest["promotion_eligible"] is False
    assert bundle_manifest["transform"]["strategy_outputs_unchanged"] is True
    assert bundle_manifest["parent"]["artifacts"] == parent_before
    assert bundle_manifest["artifacts"] == subject.artifact_inventory(
        output, exclude={"bundle_manifest.json"}
    )

    with pytest.raises(FileExistsError):
        subject.correct_bundle(source, output)


def test_correction_refuses_post_cutoff_strategy_rows_and_leaves_no_output(tmp_path):
    source = _source_bundle(tmp_path, signal_day="2026-09-24")
    output = tmp_path / "must_not_publish"

    with pytest.raises(ValueError, match="Non-eligibility dataset rows exceed cutoff"):
        subject.correct_bundle(source, output)

    assert not output.exists()
    assert not list(tmp_path.glob(".cutoff-correction-building-*"))
