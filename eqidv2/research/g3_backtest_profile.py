"""G3 historical/daily backtests use 51 setups including three restored and two new slots."""
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from eqidv2_runtime_paths import runtime_dir

import fno_v13_v10_g_3_contract as production
from research import g3_target_cutoff_contract as contract
from research.archived_g3_v6 import contract as previous

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_3_production_v11.json"
CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_3_backtest_target_gt075_restored3_v13.json"
HISTORY_CONFIG = ROOT / "configs/strategies/v13_v10_g_3_backtest_selected_afternoon_v7.json"
VERSION = contract.STRATEGY_VERSION
ADDED_IDS = production.afternoon_setups.SETUP_IDS - contract.DISABLED_IDS
DONOR_ROOT = runtime_dir("backtesting_result_v13_v10_g_4")
DONOR_RELATIVE = "target_sweeps/2026-10-13/20261013_afternoon_dual70_targets_v5"
DONOR_CONFIG_SHA256 = "5f244666ad68fd2d2d6ac6243f328a324b9363f676b30d7ae5a177246843b434"


def configuration():
    return contract.load_contract(CONFIG_PATH)


def production_configuration():
    """Verify the original immutable 59-setup promotion evidence."""
    value = json.loads(HISTORY_CONFIG.read_text(encoding="utf-8"))
    donor_path = DONOR_ROOT / DONOR_RELATIVE / "refined_config.json"
    if hashlib.sha256(donor_path.read_bytes()).hexdigest() != DONOR_CONFIG_SHA256:
        raise ValueError("G3 approved afternoon donor configuration changed")
    donor = json.loads(donor_path.read_text(encoding="utf-8"))
    if (value.get("variant_id") != production.afternoon_setups.VERSION or value.get("scope") != "BACKTEST_ONLY"
            or value.get("execution_authority") is not False
            or value.get("preserve_existing_g3_setups") is not True
            or value.get("allow_unlisted_additions") is not False
            or value.get("base_strategy_version") != previous.STRATEGY_VERSION
            or value.get("base_strategy_fingerprint") != previous.strategy_fingerprint()
            or value.get("target_source_run_relative") != DONOR_RELATIVE
            or donor.get("require_native70") is not True):
        raise ValueError("G3 afternoon profile changed its authority or pinned V6 identity")
    approved = [dict(enabled=True, **{k: v for k, v in row.items() if not k.startswith("prior_entry_")})
                for row in donor["setups"]]
    if value["setups"] != approved or len(approved) != 15:
        raise ValueError("G3 changed the approved fifteen afternoon rules")
    for key in ("initial_stop_pct", "tightened_stop_pct", "tighten_after_minutes",
                "entry_expiry_minutes", "square_off", "round_trip_cost_bps",
                "capital_per_entry_rs", "leverage", "portfolio_capital_rs"):
        if value[key] != donor["fixed_policy"][key]:
            raise ValueError(f"G3 changed the approved afternoon execution policy: {key}")
    if (value.get("confirmation_offset_minutes") != 1
            or value.get("entry_source_run_relative") != donor["fixed_policy"]["reference_run_relative"]
            or value.get("entry_source_manifest_sha256") != donor["fixed_policy"]["reference_manifest_sha256"]):
        raise ValueError("G3 afternoon entry provenance or clock changed")
    for name in ("CORE_SETUPS", "ACTIVE_SETUPS"):
        old = {s.setup_id: asdict(s) for s in getattr(previous, name)}
        current = {s.setup_id: asdict(s) for s in getattr(production, name)}
        if set(current) != set(old) | production.afternoon_setups.SETUP_IDS | production.ADDED_SETUP_IDS:
            raise ValueError("G3 must retain its original44 plus fifteen afternoon and two user-enabled setups")
        for setup_id, row in old.items():
            expected, actual = dict(row), dict(current[setup_id])
            expected.pop("source_version")
            actual.pop("source_version")
            if setup_id == "0926_LONG":
                expected["volume_ratio"] = 1.2
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
                raise ValueError(f"Promoted G3 rule differs from approved afternoon selection: {row['setup_id']}")
    expected_volume = {**previous.MIN_CONFIRMATION_VOLUME_BY_SETUP, "0941_LONG": .8, "0921_SHORT": 1.2, "1011_LONG": .6,
        **{s["setup_id"]: s["thresholds"]["one_minute_volume_ratio_min"] for s in value["setups"]}}
    if production.MIN_CONFIRMATION_VOLUME_BY_SETUP != expected_volume:
        raise ValueError("Promoted G3 confirmation volumes differ from approved selections")
    production.validate_contract()
    return value
