"""Summarize the corrected ten-pair rollout without rewriting replay artifacts."""
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import strategy_slot_selection as slots
from fno_v13_v10_g_3_preflight import inspect_deployment
from fno_v13_v10_g_7_v4_lock_live_config import readiness

HERE = Path(__file__).resolve().parent
RUN = HERE / "replay/requested_ten_cli_verified"
policy = slots.policy_payload()
with (RUN / "selected_slot_results.csv").open(newline="", encoding="utf-8") as stream:
    replay = {(r["strategy"],r["setup_id"]):r for r in csv.DictReader(stream)}
rows = []
for strategy, spec in policy["strategies"].items():
    for row in spec["slot_statistics"]:
        if row["setup_id"] not in slots.REVIEWED_SETUP_IDS:
            continue
        result = replay[(strategy,row["setup_id"])]
        rows.append(dict(strategy=strategy, setup_id=row["setup_id"],
            selected=row["setup_id"] in spec["enabled_setup_ids"],
            source_wins=row["wins"],source_trades=row["trades"],
            source_win_rate_pct=100*row["wins"]/row["trades"] if row["trades"] else None,
            replay_wins=result["wins"],replay_trades=result["trades"],
            replay_win_rate_pct=result["win_rate_pct"],replay_net_pnl_rs=result["net_pnl_rs"]))
with (HERE / "requested_ten_decisions.csv").open("w",newline="",encoding="utf-8") as stream:
    writer=csv.DictWriter(stream,fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
preflight=inspect_deployment(verify_frozen=False)
status=dict(selection_scope="ONLY_REQUESTED_TEN_PAIRS", unchanged_slots_per_strategy=41,
    enabled_slots={s:len(slots.enabled_setup_ids(s)) for s in slots.STRATEGIES},
    policy_fingerprint=slots.policy_fingerprint(), data_through="2026-10-27",
    g3_configuration_preflight=preflight,
    g7_live_readiness=readiness("LIVE"),g7_paper_readiness=readiness("PAPER"),
    trading_activation_changed=False, broker_actions_performed=False,
    dashboard_authenticated_http_verified=True,
    current_replay=str(RUN),
    superseded_research="Earlier all-slot runs are archived; they are not the current selection.",
    normal_g3_full_minute_command="Superseded all-slot run interrupted after user clarified scope; corrected selection verified by fresh full-opportunity allocation and targeted normal/daily pipeline tests.",
    limitations=["Selection is retrospective; no forward win-rate guarantee.",
        "G3 13:01 SHORT was 18/25 before selection and is 18/26 after a new capital admission. Original selection remains fixed.",
        "Existing production 09:26 confirmation rules are preserved; its backtest-only minimum-wick rule is not promoted by this ten-pair change.",
        "G7 LIVE/PAPER execution adapters are absent. Readiness entrypoints explicitly fail closed."])
(HERE / "rollout_status.json").write_text(json.dumps(status,indent=2,default=str)+"\n",encoding="utf-8")
print(json.dumps({k:status[k] for k in ("selection_scope","enabled_slots","trading_activation_changed","broker_actions_performed")}))
