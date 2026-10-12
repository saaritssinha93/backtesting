"""Read-only consumers bind LIVE sizing without changing G3/PAPER decisions."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ai_platform.adapters.common import ArtifactError
from ai_platform.adapters.equity_orders import read_equity_orders
from ai_platform.api.registry import SourceRegistry
from ai_platform.api.repository import PlatformRepository
from fno_v13_v10_g_3_contract import STRATEGY_VERSION, strategy_fingerprint
from fno_v13_v10_g_3_live_execution_policy import execution_metadata, validate_execution_metadata

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 10, 12)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def repository(tmp_path):
    sources = SourceRegistry(ROOT / "docs/ai_platform/source_registry.json", tmp_path, profile="V13_V10_G_3")
    return PlatformRepository(sources, ROOT / "docs/ai_platform/profile_registry.json", cache_ttl_seconds=0)


def status_evidence(repo):
    for name, schema in (("equity_live_status", "status"), ("equity_live_heartbeat", "heartbeat")):
        write(repo.sources.path(name), dict(schema_version=f"fno_v6_live_kite_qty1_{schema}_v1",
              session_date=str(DAY), strategy_version=STRATEGY_VERSION,
              strategy_fingerprint=strategy_fingerprint(), state="DONE", execution_mode="LIVE",
              execution_profile="live_kite_qty1", updated_at_ist=f"{DAY}T15:32:00+05:30",
              **execution_metadata()))


def test_g3_api_readiness_exposes_bound_policy_with_unchanged_decision_identity(tmp_path):
    repo = repository(tmp_path)
    status_evidence(repo)
    result = repo.readiness()
    assert result["status"]["state"] == result["heartbeat"]["state"] == "COMPLETE"
    assert result["coherent_snapshot_id"]
    assert result["status"]["payload"]["strategy_fingerprint"] == strategy_fingerprint()
    assert result["status"]["payload"]["target_exposure_rs"] == 50000


@pytest.mark.parametrize("source", ["equity_live_status", "equity_live_heartbeat"])
@pytest.mark.parametrize("field,value", [("live_execution_policy_fingerprint", None),
    ("quantity_policy", "FIXED_ONE_SHARE"), ("capital_per_trade_rs", 100000),
    ("target_exposure_rs", 500000), ("leverage", 1)])
def test_g3_api_readiness_rejects_unchanged_strategy_with_wrong_sizing(tmp_path, source, field, value):
    repo = repository(tmp_path)
    status_evidence(repo)
    path = repo.sources.path(source)
    row = json.loads(path.read_text())
    if value is None:
        row.pop(field)
    else:
        row[field] = value
    write(path, row)
    result = repo.readiness()
    key = "status" if source == "equity_live_status" else "heartbeat"
    assert result[key]["state"] == "INTEGRITY_ERROR"
    assert result["coherent_snapshot_id"] is None


def order(**changes):
    return dict(schema_version="fno_v6_equity_order_state_v2", mode="LIVE", session_date=str(DAY),
        strategy_version=STRATEGY_VERSION, strategy_fingerprint=strategy_fingerprint(),
        signal_id="g3-example", tradingsymbol="EXAMPLE", side="LONG", status="OPEN",
        quantity=50, requested_quantity=50, strategy_sized_quantity=50, lot_size=1,
        sizing_reference_price=1000., trigger_price=1000., entry_price=1001.,
        capital_rs=10000., execution_profile="live_kite_qty1", **execution_metadata()) | changes


def read_order(tmp_path, row):
    write(tmp_path / row["mode"] / str(DAY) / "one.json", row)
    return read_equity_orders(tmp_path / "PAPER", tmp_path / "LIVE", DAY,
        expected_strategy_version=STRATEGY_VERSION, expected_strategy_fingerprint=strategy_fingerprint())[0]


def test_g3_order_adapter_accepts_variable_and_partial_fills_at_actual_slippage_price(tmp_path):
    assert read_order(tmp_path / "full", order()).quantity == 50
    assert read_order(tmp_path / "partial", order(quantity=17)).quantity == 17
    assert read_order(tmp_path / "reduced", order(quantity=10, requested_quantity=10,
                      execution_quantity_override=10)).quantity == 10


@pytest.mark.parametrize("changes", [
    dict(live_execution_policy_fingerprint="old-qty1-fingerprint"),
    dict(quantity_policy="FIXED_ONE_SHARE"), dict(capital_rs=100000),
    dict(sizing_reference_price=1001.), dict(requested_quantity=51),
    dict(quantity=51), dict(quantity=1.5), dict(quantity=0),
    dict(strategy_sized_quantity=500), dict(lot_size=50),
])
def test_g3_order_adapter_rejects_policy_and_sizing_drift(tmp_path, changes):
    with pytest.raises(ArtifactError, match="G3 LIVE"):
        read_order(tmp_path, order(**changes))


def test_paper_adapter_keeps_original_sizing_and_needs_no_live_policy(tmp_path):
    row = order(mode="PAPER", quantity=500, capital_rs=100000.)
    for key in execution_metadata():
        row.pop(key, None)
    assert read_order(tmp_path, row).quantity == 500


def test_registered_g3_live_policy_is_separate_from_unchanged_paper_and_legacy_g():
    profiles = json.loads((ROOT / "docs/ai_platform/profile_registry.json").read_text())["profiles"]
    live_name = "V13_V10_G_3_EQUITY_LIVE_10K_5X"
    assert live_name in profiles["V13_V10_G_3"]["execution_profiles"]
    validate_execution_metadata(profiles[live_name])
    assert profiles[live_name]["strategy_fingerprint"] == strategy_fingerprint()
    assert profiles["V13_V10_G_3_EQUITY_PAPER_EXPOSURE"]["capital_per_entry_rs"] == 100000
    assert profiles["V13_V10_G_EQUITY_LIVE_QTY1"]["quantity_policy"] == "FIXED_ONE_SHARE"

