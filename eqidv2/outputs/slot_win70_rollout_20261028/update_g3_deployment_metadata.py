"""Update selected configuration metadata without changing execution authority."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
import fno_v13_v10_g_3_live_config as config
import strategy_slot_selection as slots

path = ROOT / "configs/strategies/active_profile.json"
value = json.loads(path.read_text(encoding="utf-8"))
if value["profile"] != "V13_V10_G_3" or value["live_activation_authorized"] is not False:
    raise ValueError("Unexpected active profile or activation state; no authority mutation allowed")
from research import archived_g3_production_v14 as prior_selection
value["superseded_same_day_identity"] = {
    "through_session": "2026-10-28", "strategy_version": prior_selection.STRATEGY_VERSION,
    "strategy_fingerprint": prior_selection.strategy_fingerprint(),
    "reason": "Recorded V14 decisions retain their identity; all-slot V15 governs new entries."}
value.update(strategy_version=config.STRATEGY_VERSION, strategy_fingerprint=config.strategy_fingerprint(),
    accepted_variant=config.contract.ACCEPTED_VARIANT,
    production_config="configs/strategies/v13_v10_g_3_production_v15.json",
    migration_date="2026-10-28", next_worker_session="2026-10-29",
    migration_plan="configs/strategies/slot_win70_20261028.json",
    slot_selection_fingerprint=slots.policy_fingerprint("G3"))
path.write_bytes((json.dumps(value, indent=2)+"\n").encode())
path = ROOT / "docs/ai_platform/profile_registry.json"
registry = json.loads(path.read_text(encoding="utf-8"))
parent = registry["profiles"]["V13_V10_G_3"]
for key in ["V13_V10_G_3", *parent.get("execution_profiles", [])]:
    entry = registry["profiles"][key]
    entry.update(strategy_version=config.STRATEGY_VERSION,
        strategy_fingerprint=config.strategy_fingerprint(),
        slot_selection_fingerprint=slots.policy_fingerprint("G3"))
    if "equity_strategy_version" in entry:
        entry["equity_strategy_version"] = config.STRATEGY_VERSION
        entry["strategy_version"] = "FNO_V13_V10_G_3_OPTIONS_ONE_LOT_ATM_SL30_T40P4_20261011"
parent.update(active_setup_count=len(config.ACTIVE_SETUPS),
    production_config_relative_path="configs/strategies/v13_v10_g_3_production_v15.json",
    signal_to_confirmation=config.SIGNAL_TO_CONFIRMATION, pipeline_deadline=config.PIPELINE_DEADLINE,
    five_minute_volume_ratio_min_by_setup={s.setup_id:s.volume_ratio for s in config.ACTIVE_SETUPS})
parent["confirmation_policy"]["minimum_volume_by_setup"] = config.MIN_CONFIRMATION_VOLUME_BY_SETUP
registry["profiles"]["V13_V10_G_7_V4_LOCK2"] = dict(label="G7 V4 two-loss lock: >=70% review of all slots",
    active_setup_count=len(slots.enabled_setup_ids("G7_V4_LOCK2")),
    slot_selection_fingerprint=slots.policy_fingerprint("G7_V4_LOCK2"),
    evidence_class="RETROSPECTIVE_BACKTEST", execution_authority=False,
    live_support="NOT_SUPPORTED", paper_support="NOT_SUPPORTED",
    runtime_config_relative_path="configs/strategies/v13_v10_g_7_v4_lock_runtime_v2.json")
path.write_bytes((json.dumps(registry, indent=2)+"\n").encode())
from fno_v13_v10_g_3_preflight import inspect_deployment
r = inspect_deployment(verify_frozen=False)
print(json.dumps({k:r[k] for k in ["ready", "deployment_identity_matches", "api_registry_matches", "historical_daily_paper_live_contract_matches", "backtest_variant_valid", "active_setup_count", "live_activation_authorized"]}))
