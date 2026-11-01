"""Read the exact FnO dashboard status adapters without changing worker state."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import log_dashboard_server as dashboard

CARDS = (
    "authentication_v2",
    "fno_oi_fetch_5min_fast_production",
    "fno_oi_feature_ranker",
    "fno_v13_v10_g_scanner_5min",
    "fno_v13_v10_g_equity_1min_feed",
    "fno_v13_v10_g_confirmation_1min",
    "fno_v13_v10_g_live_long",
    "fno_v13_v10_g_live_short",
    "fno_v13_v10_g_options_live_long",
    "fno_v13_v10_g_options_live_short",
    "kite_trade_fno_id_v13_v10_g",
)
FIELDS = (
    "status", "state", "phase", "reason", "message", "session_date", "ts",
    "updated_at_ist", "heartbeat_ist", "execution_mode", "signals", "errors",
    "processed_slots", "blocked_slots", "slot_end", "last_slot_end", "next_slot",
    "apps_available", "pid", "worker_pid", "task_state", "scheduler_state",
    "scheduler_last_result", "derived_status", "runtime_status", "arm_reason",
)

if __name__ == "__main__":
    tasks = dashboard.load_task_scheduler_snapshot(force=True)
    result = []
    for card in CARDS:
        path, _ = dashboard.resolve_log_target(card)
        status = dashboard._v7_monitor_status_for(card, tasks)
        status = dashboard.infer_scanner_runtime_status(card, path, status)
        status = dashboard.reconcile_fno_worker_recovery(card, status)
        if card == "authentication_v2":
            status = dashboard.reconcile_authentication_status(status)
        result.append({"card": card, "status_file": str(dashboard._runtime_status_path_for_card(card)),
                       **{name: status[name] for name in FIELDS if name in status}})
    print(json.dumps(result, indent=2, default=str))
