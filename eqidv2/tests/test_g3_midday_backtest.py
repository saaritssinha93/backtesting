"""Frozen-source promotion rejects drift and remains importable in CLI order."""
import json
import subprocess
import sys

import pytest

from research.archived_g3_v6 import backtest_profile as profile
from research.archived_g3_v6 import midday_backtest as history
from research.archived_g3_v5 import contract as old_contract


def test_exact_twelve_and_prior_v5_identity_are_preserved():
    value = profile.configuration()
    assert old_contract.strategy_fingerprint() == "fcb7c981b518b8c9e427155cdfb06e3cc34c6ed16525e78514e6ecf3afab2aea"
    assert len(old_contract.ACTIVE_SETUPS) == 32
    assert len(value["setups"]) == 12
    assert set(profile.contract._ACTIVE_BY_ID) - set(old_contract._ACTIVE_BY_ID) == profile.ADDED_IDS
    assert len(profile.contract.ACTIVE_SETUPS) == 44


@pytest.mark.parametrize("mutation", ["target", "threshold", "missing", "authority", "baseline"])
def test_historical_source_config_drift_is_rejected(tmp_path, monkeypatch, mutation):
    value = profile.configuration()
    if mutation == "target":
        value["setups"][0]["target_pct"] = 7.14
    elif mutation == "threshold":
        value["setups"][0]["thresholds"]["one_minute_volume_ratio_min"] = .1
    elif mutation == "missing":
        value["setups"].pop()
    elif mutation == "authority":
        value["execution_authority"] = True
    else:
        value["base_strategy_fingerprint"] = "unrecognized"
    path = tmp_path / "changed.json"
    path.write_text(json.dumps(value))
    monkeypatch.setattr(profile, "HISTORY_CONFIG", path)
    with pytest.raises(ValueError):
        profile.configuration()


def test_midday_package_rejects_changed_artifact_before_accounting(tmp_path):
    manifest = dict(variant_id=profile.VERSION, execution_authority=False,
        files={name: "wrong" for name in history.FILES})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    for name in history.FILES:
        (tmp_path / name).write_text("changed")
    with pytest.raises(ValueError, match="artifact changed"):
        history.verified_package(tmp_path)


@pytest.mark.parametrize("entry", ["fno_v13_v10_g_3_backtest", "backtesting_result_v13_v10_g_3_daily"])
def test_normal_cli_import_order_has_no_cycle(entry):
    result = subprocess.run([sys.executable, "-m", entry, "--help"], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
