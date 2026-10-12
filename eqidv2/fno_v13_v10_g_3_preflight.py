"""Local-only G3 deployment preflight; no worker, broker, task or arm mutation."""
from __future__ import annotations

import argparse
import json
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
    return {"schema_version": "g3_local_preflight_v1", "ready": not missing,
            "strategy_profile": config.STRATEGY_PROFILE, "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(), "missing_files": missing,
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
