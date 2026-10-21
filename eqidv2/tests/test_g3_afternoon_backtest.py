"""Afternoon promotion keeps frozen targets and independent G3 allocation."""
from dataclasses import asdict
from datetime import date
import json

import pandas as pd
import pytest

from research import g3_backtest_profile as profile
from research import g3_afternoon_backtest as history
from research.archived_g3_v6 import contract as previous


def test_exact_fifteen_preserves_forty_four_previous_rules():
    value = profile.production_configuration()
    assert previous.strategy_fingerprint() == "7d01e80f60cc874e7c7b94a894785d134b70df71b27be98fdf3c302a96f44b4a"
    assert len(previous.ACTIVE_SETUPS) == 44
    assert len(value["setups"]) == 15
    assert len(profile.production.ACTIVE_SETUPS) == 61
    assert set(profile.production._ACTIVE_BY_ID) - set(previous._ACTIVE_BY_ID) == (profile.production.afternoon_setups.SETUP_IDS | profile.production.ADDED_SETUP_IDS)
    for spec in previous.ACTIVE_SETUPS:
        old, current = asdict(spec), asdict(profile.production._ACTIVE_BY_ID[spec.setup_id])
        old.pop("source_version")
        current.pop("source_version")
        assert old == current


@pytest.mark.parametrize("mutation", ["target", "threshold", "missing", "authority", "baseline", "clock", "entry_source"])
def test_historical_config_cannot_drift_from_approved_research(tmp_path, monkeypatch, mutation):
    value = profile.production_configuration()
    if mutation == "target":
        value["setups"][0]["target_pct"] += .01
    elif mutation == "threshold":
        value["setups"][0]["thresholds"]["one_minute_volume_ratio_min"] += .1
    elif mutation == "missing":
        value["setups"].pop()
    elif mutation == "authority":
        value["execution_authority"] = True
    elif mutation == "baseline":
        value["base_strategy_fingerprint"] = "unrecognized"
    elif mutation == "clock":
        value["confirmation_offset_minutes"] = 2
    else:
        value["entry_source_manifest_sha256"] = "unrecognized"
    changed = tmp_path / "config.json"
    changed.write_text(json.dumps(value))
    monkeypatch.setattr(profile, "HISTORY_CONFIG", changed)
    with pytest.raises(ValueError):
        profile.production_configuration()


def test_new_package_rejects_artifact_drift_before_accounting(tmp_path):
    manifest = dict(variant_id=history.VERSION, execution_authority=False,
        files={name: "wrong" for name in history.FILES})
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    for name in history.FILES:
        (tmp_path / name).write_text("changed")
    with pytest.raises(ValueError, match="artifact changed"):
        history.verified_package(tmp_path)


def test_legacy_midday_entry_cannot_mix_current_runtime_identity(tmp_path):
    from research import g3_midday_backtest as archived_entry
    with pytest.raises(ValueError, match="verification-only"):
        archived_entry.run(tmp_path / "unexpected")
    assert not (tmp_path / "unexpected").exists()


def test_g3_allocation_qualification_is_reported_independently(monkeypatch):
    setup_id = "1236_LONG"
    monkeypatch.setattr(history, "ADDED_IDS", {setup_id})
    raw = pd.DataFrame(dict(setup_id=[setup_id] * 5, filled=True,
        net_return_pct=[.1, .1, .1, .1, -.1]))
    def ledger(executed):
        return raw.assign(day="2026-10-12", portfolio_executed=executed,
            portfolio_net_profit_rupees=[500, 500, 500, 500, -500],
            portfolio_gross_profit_rupees=[750, 750, 750, 750, -250],
            portfolio_cost_rupees=250)
    result = history.setup_comparison(raw, ledger([True, True, True, False, True]),
        ledger([True] * 5), [date(2026, 10, 12)]).iloc[0]
    assert result.native_qualified70 and result.g4_qualified70
    assert not result.g3_qualified70  # only four actual fills, despite 75% wins
    assert result.native_win_rate_pct == 80
    assert result.g3_win_rate_pct == 75
    assert result.g3_executed_trades == 4
