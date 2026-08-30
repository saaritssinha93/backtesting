from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import fno_v10_repaired_snapshot_rerun as repaired


def test_metric_drift_is_keyed_and_directional() -> None:
    old = pd.DataFrame(
        [{"dataset": "H", "variant": "A", "period": "FULL", "fills": 2, "net": 1.0}]
    )
    new = pd.DataFrame(
        [{"dataset": "H", "variant": "A", "period": "FULL", "fills": 3, "net": 0.5}]
    )
    result = repaired.metric_drift(
        old,
        new,
        keys=("dataset", "variant", "period"),
        fields=("fills", "net"),
    ).iloc[0]
    assert result["_merge"] == "both"
    assert result["delta_fills"] == 1
    assert result["delta_net"] == -0.5


def test_validate_source_contracts_rejects_aug_today(tmp_path: Path, monkeypatch) -> None:
    def write(name: str, month: str, mapped: int, master: str, captures: int) -> Path:
        path = tmp_path / name
        payload = {
            "schema_version": "fno_backtest_source_snapshot_v1",
            "complete": True,
            "physical_copy": True,
            "snapshot_fingerprint": name,
            "universe": {
                "contract_month_filter": month,
                "mapped_stock_futures": mapped,
                "master_date": master,
            },
            "captures": [
                {"role": "NFO_FUTURES_5M" if index < captures // 2 else "NSE_EQUITY_1M"}
                for index in range(captures)
            ],
        }
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    historical = write("historical.json", "26AUG", 208, "2026-08-11", 416)
    today = write("today.json", "26SEP", 210, "2026-08-27", 420)
    rejected = write("rejected.json", "26AUG", 208, "2026-08-11", 416)
    result = repaired.validate_source_contracts(historical, today, rejected)
    assert result["today_sep_accepted"]["universe"]["contract_month_filter"] == "26SEP"
    assert result["today_aug_rejected"]["accepted"] is False
    assert result["today_aug_rejected"]["rejection_code"] == "WRONG_CONTRACT_MONTH_FOR_REPAIRED_TODAY"


def test_individual_registry_is_complete() -> None:
    assert [spec.variant for spec in repaired.filters.SPECS] == [
        "STAGE7_CONTROL",
        "0935_LONG_MOVE_MAX_040",
        "0935_LONG_MOVE_MAX_050",
        "0935_LONG_MOVE_MAX_060",
        "0925_LONG_BODY_MIN_050",
        "PREV10_VOLUME_RATIO_MIN_100",
        "PREV10_VOLUME_RATIO_MIN_125",
        "PREV10_RANGE_RATIO_MIN_100",
        "PREV10_RANGE_RATIO_MIN_125",
    ]


def test_combo_profile_registry_is_bounded() -> None:
    assert {profile.profile_id for profile in repaired.combo.PROFILES} == {
        "STAGE7",
        "STAGE7_GAP2",
        "MAX050",
        "MAX050_GAP0",
        "MAX050_GAP2",
        "MAX050_REJECT_ALL",
    }
