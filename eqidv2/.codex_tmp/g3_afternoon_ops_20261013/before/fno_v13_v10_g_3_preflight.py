"""Local-only G3 deployment preflight; no worker, broker, task or arm mutation."""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path


def inspect_deployment(*, verify_frozen: bool = True) -> dict:
    import fno_v13_v10_g_3_live_config as config
    from fno_strategy_registry import deployment_state
    config.validate_strategy()
    evidence = config.attest_selected_backtest() if verify_frozen else {"verified": False}
    workspace = Path(__file__).resolve().parent
    roles = ("scanner_5min", "equity_1min_feed", "confirmation_1min", "live_long", "live_short",
             "trade_logger", "net_result", "live_kite_qty1", "options_live_long", "options_live_short",
             "options_trade_logger", "options_net_result")
    paths = [workspace / "bat" / f"run_fno_v13_v10_g_3_{role}.bat" for role in roles]
    paths += [workspace / "bat/run_backtesting_result_v13_v10_g_3_1620.bat",
              workspace / "backtesting_result_v13_v10_g_3_daily.py"]
    missing = [str(path.relative_to(workspace)) for path in paths if not path.is_file()]
    state = deployment_state()
    registry = json.loads((workspace / "docs/ai_platform/profile_registry.json").read_text(encoding="utf-8"))["profiles"]
    current = registry[config.STRATEGY_PROFILE]
    registry_matches = (current.get("strategy_version") == config.STRATEGY_VERSION
        and current.get("strategy_fingerprint") == config.strategy_fingerprint()
        and current.get("signal_to_confirmation") == config.SIGNAL_TO_CONFIRMATION
        and current.get("active_setup_count") == len(config.ACTIVE_SETUPS)
        and current.get("confirmation_policy", {}).get("minimum_volume_by_setup") == config.MIN_CONFIRMATION_VOLUME_BY_SETUP
        and all(registry[name].get("strategy_fingerprint") == config.strategy_fingerprint()
                for name in current.get("execution_profiles", [])))
    from research.g3_backtest_profile import contract as backtest_contract, CONFIG_PATH as backtest_path, configuration
    configuration()
    backtest_matches = (backtest_contract is config.contract and backtest_path == config.CONFIG_PATH)
    promoted = config.setup_for("09:50", "LONG")
    opening = config.setup_for("09:20", "LONG")
    deployment_matches = (state.get("strategy_version") == config.STRATEGY_VERSION
        and state.get("strategy_fingerprint") == config.strategy_fingerprint())
    return {"schema_version": "g3_local_preflight_v1", "ready": not missing and deployment_matches and registry_matches and backtest_matches,
            "deployment_identity_matches": deployment_matches,
            "api_registry_matches": registry_matches, "historical_daily_paper_live_contract_matches": backtest_matches,
            "strategy_profile": config.STRATEGY_PROFILE, "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(), "missing_files": missing,
            "strategy_config_file": str(config.CONFIG_PATH), "active_setup_count": len(config.ACTIVE_SETUPS),
            "pipeline_deadline": config.PIPELINE_DEADLINE,
            "last_confirmation": max(config.SIGNAL_TO_CONFIRMATION.values()),
            "promoted_midday_twelve": [{"setup_id": s.setup_id, **asdict(s), "minimum_confirmation_volume_ratio":
                config.confirmation_volume_minimum(s.side, setup_id=s.setup_id)}
                for s in config.ACTIVE_SETUPS if s.setup_id in config.contract.midday_setups.SETUP_IDS],
            "promoted_selected_ten": [{**asdict(s), "minimum_confirmation_volume_ratio":
                config.confirmation_volume_minimum(s.side, setup_id=s.setup_id)}
                for s in config.ACTIVE_SETUPS if s.setup_id in config.contract.selected_setups.SETUP_IDS],
            "promoted_late_setups": [{**asdict(s), "minimum_confirmation_volume_ratio":
                config.confirmation_volume_minimum(s.side, setup_id=s.setup_id)}
                for s in config.ACTIVE_SETUPS if s.setup_id in config.contract.late_setups.SETUP_IDS],
            "promoted_0920_long_setup": {**asdict(opening), "minimum_confirmation_volume_ratio":
                config.confirmation_volume_minimum("LONG", signal_end="09:20")},
            "opening_oi_policy": config.strategy_payload()["opening_0920"],
            "promoted_0950_long_setup": {**asdict(promoted), "minimum_confirmation_volume_ratio":
                config.confirmation_volume_minimum(promoted.side, signal_end=promoted.signal_end)},
            **config.live_execution_policy.execution_metadata(),
            "configured_profile": state.get("profile"), "deployment_stage": state.get("deployment_stage"),
            "live_activation_authorized": state.get("live_activation_authorized") is True,
            "workers_started": False, "broker_requests": False, "arm_files_written": False,
            "task_changes": False, "historical_attestation": evidence,
            "remaining_live_gates": ["paper session evidence", "broker-flat/manual-review reconciliation",
                "explicit LIVE_APPROVED deployment", "G3 acknowledgement", "dated strategy and LIVE execution policy fingerprint arm",
                "legacy/global kill switches clear", "shared active-profile entry ownership"]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    result = inspect_deployment()
    print(json.dumps(result, indent=2))
    return 0 if result["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
