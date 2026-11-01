"""Materialize the all-slot >=70% allowlist from verified source counts."""
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
source = json.loads((HERE / "analysis/selection.json").read_text())
with (HERE / "analysis/all_slot_selection.csv").open(newline="", encoding="utf-8") as stream:
    rows = list(csv.DictReader(stream))
reviewed = {row["setup_id"] for row in rows}
policy = dict(schema_version="strategy_slot_win_rate_policy_v1",
    selection_id="G3_G7_V4_LOCK_ALL_SLOTS_WIN70_20261028", threshold_percent=70,
    selection_scope="ALL_REPRESENTED_SLOTS", reviewed_setup_ids=sorted(reviewed),
    selection_basis="ALL_REPRESENTED_SLOTS_NET_EXECUTED_WIN_RATE",
    selection_uses_outcomes=True, independent_holdout=False, execution_authority=False,
    source_period=dict(first_session=source["period_start"], last_session=source["period_end"],
        sessions=source["sessions"], cost_bps=5),
    zero_sample_policy="EXCLUDED", minimum_sample_rule="AT_LEAST_ONE_EXECUTED_TRADE",
    application="NEW_ENTRIES_ONLY_EXISTING_POSITIONS_REMAIN_MANAGED",
    limitations=["Retrospective selection on reused history; no future >=70% claim.",
        "Small samples are retained when they meet the requested cutoff.",
        "Portfolio allocation and daily loss locks must be recomputed after selection."], strategies={})
for evidence in source["source_hashes"]:
    if hashlib.sha256(Path(evidence["path"]).read_bytes()).hexdigest() != evidence["sha256"]:
        raise ValueError("Selection source changed")
    strategy = evidence["strategy"]
    stats = []
    for row in rows:
        if row["strategy"] != strategy:
            continue
        item = {k: row[k] for k in ("setup_id", "slot", "side")}
        item.update({k: int(row[k]) for k in ("opportunities", "trades", "wins", "losses", "breakeven")})
        item["net_pnl_rs"] = float(row["net_pnl_rs"])
        stats.append(item)
    enabled = sorted(r["setup_id"] for r in stats if r["trades"] > 0
        and 100*r["wins"] >= 70*r["trades"])
    policy["strategies"][strategy] = dict(source_ledger=evidence,
        enabled_setup_ids=enabled, disabled_setup_ids=sorted(set(r["setup_id"] for r in stats)-set(enabled)),
        slot_statistics=stats)
path = ROOT / "configs/strategies/slot_win70_20261028.json"
path.write_bytes((json.dumps(policy, indent=2, sort_keys=True, allow_nan=False)+"\n").encode("utf-8"))
print(path)
