"""Record the all-slot rollout and its fresh full-opportunity replay."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RUN = HERE / "replay/all_slots_final_verified"
sys.path.insert(0, str(ROOT))

import strategy_slot_selection as slots
from fno_v13_v10_g_3_preflight import inspect_deployment
from fno_v13_v10_g_7_v4_lock_live_config import readiness


def rows(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def main():
    policy = slots.policy_payload()
    summary = json.loads((RUN / "summary.json").read_text(encoding="utf-8"))
    if (policy["selection_scope"] != "ALL_REPRESENTED_SLOTS"
            or summary["slot_policy_fingerprint"] != slots.policy_fingerprint()
            or not summary["baseline_reproduction_passed"]):
        raise ValueError("Current all-slot replay does not match policy or baseline")
    replay = {(r["strategy"], r["setup_id"]): r for r in rows(RUN / "selected_slot_results.csv")}
    decisions = []
    for strategy, spec in policy["strategies"].items():
        for row in spec["slot_statistics"]:
            selected = row["setup_id"] in spec["enabled_setup_ids"]
            item = replay[(strategy, row["setup_id"])]
            decisions.append(dict(strategy=strategy, setup_id=row["setup_id"],
                slot=row["slot"], side=row["side"],
                source_wins=row["wins"], source_trades=row["trades"],
                source_win_rate_pct=(100*row["wins"]/row["trades"] if row["trades"] else ""),
                enabled=selected, reason=("SOURCE_WIN_RATE_GE70" if selected else
                    "NO_EXECUTED_TRADES" if row["trades"] == 0 else "SOURCE_WIN_RATE_BELOW70"),
                replay_wins=item["wins"], replay_trades=item["trades"],
                replay_win_rate_pct=item["win_rate_pct"], replay_net_pnl_rs=item["net_pnl_rs"]))
    with (HERE / "all_slot_decisions.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(decisions[0]))
        writer.writeheader()
        writer.writerows(decisions)
    preflight = inspect_deployment(verify_frozen=False)
    status = dict(selection_scope=policy["selection_scope"],
        enabled_slots={s: len(slots.enabled_setup_ids(s)) for s in slots.STRATEGIES},
        disabled_slots={s: len(policy["strategies"][s]["disabled_setup_ids"]) for s in slots.STRATEGIES},
        source_period=policy["source_period"], policy_fingerprint=slots.policy_fingerprint(),
        current_replay=str(RUN), baseline_reproduction_passed=True,
        g3_configuration_preflight=preflight,
        g7_live_readiness=readiness("LIVE"), g7_paper_readiness=readiness("PAPER"),
        trading_activation_changed=False, broker_actions_performed=False,
        limitations=["Source outcomes and replay use the same 59 sessions; there is no independent holdout.",
            "A source >=70% slot may replay below 70% after freed capital and loss-lock allocation.",
            "G7 live and paper transports remain NOT_SUPPORTED."])
    (HERE / "rollout_status.json").write_bytes((json.dumps(status, indent=2, default=str)+"\n").encode())
    print(json.dumps({k: status[k] for k in
        ("selection_scope", "enabled_slots", "disabled_slots", "trading_activation_changed")}))


if __name__ == "__main__":
    main()
