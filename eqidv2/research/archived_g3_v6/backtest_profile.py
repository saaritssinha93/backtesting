"""Normal historical/daily backtests share the 44-setup G3 V6 contract."""
from dataclasses import asdict
import json
from pathlib import Path

from research.archived_g3_v6 import contract
from research.archived_g3_v5 import contract as previous

production = contract
ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_3_production_v6.json"
HISTORY_CONFIG = ROOT / "configs/strategies/v13_v10_g_3_backtest_selected_midday_v6.json"
VERSION = contract.STRATEGY_VERSION
ADDED_IDS = contract.midday_setups.SETUP_IDS


def configuration():
    value = json.loads(HISTORY_CONFIG.read_text(encoding="utf-8"))
    donor = json.loads((ROOT / "configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json").read_text(encoding="utf-8"))
    if (value.get("variant_id") != VERSION or value.get("scope") != "BACKTEST_ONLY"
            or value.get("execution_authority") is not False
            or value.get("preserve_existing_g3_setups") is not True
            or value.get("allow_unlisted_additions") is not False
            or value.get("base_strategy_version") != previous.STRATEGY_VERSION
            or value.get("base_strategy_fingerprint") != previous.strategy_fingerprint()):
        raise ValueError("G3 midday profile changed its authority or pinned V5 identity")
    for key in ("setups", "initial_stop_pct", "tightened_stop_pct", "tighten_after_minutes",
                "confirmation_offset_minutes", "entry_expiry_minutes", "square_off",
                "round_trip_cost_bps", "capital_per_entry_rs", "leverage", "portfolio_capital_rs",
                "target_source_run_relative", "target_source_manifest_sha256",
                "entry_source_run_relative", "entry_source_manifest_sha256"):
        if value[key] != donor[key]:
            raise ValueError(f"G3 changed the approved twelve midday rules: {key}")
    for name in ("CORE_SETUPS", "ACTIVE_SETUPS"):
        old = {s.setup_id: asdict(s) for s in getattr(previous, name)}
        current = {s.setup_id: asdict(s) for s in getattr(contract, name)}
        if set(current) != set(old) | ADDED_IDS:
            raise ValueError("G3 must retain its original32 plus exactly twelve midday setups")
        for setup_id, row in old.items():
            expected, actual = dict(row), dict(current[setup_id])
            expected.pop("source_version")
            actual.pop("source_version")
            if actual != expected:
                raise ValueError(f"G3 existing setup changed: {setup_id}")
        for row in value["setups"]:
            spec, threshold = current[row["setup_id"]], row["thresholds"]
            expected = dict(signal_end=row["signal_end"], confirmation_end=row["confirmation_end"],
                side=row["side"], max_entries=1, picker="max_liquidity", target_pct=row["target_pct"],
                stop_pct=1.25, price_change_pct=threshold["price_change_pct"],
                oi_change_pct=threshold["oi_change_pct"], volume_ratio=threshold["five_minute_volume_ratio_min"],
                body_ratio=threshold["one_minute_body_ratio_min"], max_wick_ratio=threshold["one_minute_wick_ratio_max"])
            if any(spec[key] != val for key, val in expected.items()):
                raise ValueError(f"Promoted G3 rule differs from approved midday selection: {row['setup_id']}")
    expected_volume = {**previous.MIN_CONFIRMATION_VOLUME_BY_SETUP,
        **{s["setup_id"]: s["thresholds"]["one_minute_volume_ratio_min"] for s in value["setups"]}}
    if contract.MIN_CONFIRMATION_VOLUME_BY_SETUP != expected_volume:
        raise ValueError("Promoted G3 confirmation volumes differ from approved selections")
    contract.validate_contract()
    return value
