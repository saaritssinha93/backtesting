"""Read-only validation of the V6 daily publication and retained V5 evidence."""
from pathlib import Path
import hashlib
import json
import sys

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
import fno_v13_v10_g_3_contract as contract
import fno_v13_v10_g_3_daily_replay as replay
import backtesting_result_v13_v10_g_3_daily as publisher

DATA = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3")
checks = []


def check(name, condition, **detail):
    checks.append(dict(name=name, passed=bool(condition), **detail))
    if not condition:
        raise AssertionError(name)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha(path):
    return replay.sha256(path)


directory, manifest = publisher._read_verified_pointer(DATA, "current_run.json", "production_daily")
check("daily_V6_session", manifest["days"] == ["2026-10-13"] and manifest["strategy_version"] == contract.STRATEGY_VERSION)
check("daily_44_shared_rules", len(contract.ACTIVE_SETUPS) == 44)
source = read(directory / manifest["artifacts"]["source_manifest"]["path"])
for item in source["code"]:
    check("current_code_hash", sha(Path(item["path"])) == item["sha256"], path=item["path"])
check("shared_contract", source["strategy_contract"] == contract.strategy_payload())
check("raw_source_only", source["completed_trade_outputs_consumed"] is False)
snapshot_path = Path(source["snapshot_manifest"])
check("snapshot_manifest_hash", sha(snapshot_path) == source["snapshot_manifest_sha256"])
snapshot = read(snapshot_path)
for item in snapshot["sources"]:
    if item.get("captured"):
        path = snapshot_path.parent / item["snapshot_relative_path"]
        check("immutable_input_hash", sha(path) == item["sha256"], path=item["snapshot_relative_path"])

old_pointer = read(HERE / "before/daily_publication/latest/current_run.json")
old_directory = DATA / old_pointer["run_dir"]
check("prior_daily_manifest_unchanged", sha(old_directory / "run_manifest.json") == old_pointer["manifest_sha256"])
old_manifest = read(old_directory / "run_manifest.json")
for item in old_manifest["artifacts"].values():
    check("prior_daily_artifact_unchanged", sha(old_directory / item["path"]) == item["sha256"], path=item["path"])
for pointer_name in ("current_run.json", "current_history.json"):
    old = read(HERE / "before/daily_publication/latest" / pointer_name)
    archived = read(DATA / "latest/archived" / old["strategy_version"] / pointer_name)
    check("previous_pointer_archived", old == archived, pointer=pointer_name)

ledger = pd.read_csv(directory / "portfolio_trades.csv", float_precision="round_trip")
old_ledger = pd.read_csv(old_directory / "portfolio_trades.csv", float_precision="round_trip")
check("unique_signal_ids", not ledger.signal_id.duplicated().any())
check("one_session_only", set(ledger.day) == {"2026-10-13"})
check("exact_signal_allowlist", set(ledger.setup_id).issubset({x.setup_id for x in contract.ACTIVE_SETUPS}))
check("cost_accounting", np.allclose(ledger.portfolio_gross_profit_rupees-ledger.portfolio_cost_rupees,
    ledger.portfolio_net_profit_rupees, atol=1e-6, rtol=0))
keys = ["setup_id", "tradingsymbol", "signal_ts"]
current_old = ledger.loc[~ledger.setup_id.isin(contract.midday_setups.SETUP_IDS)].set_index(keys).sort_index()
previous = old_ledger.set_index(keys).sort_index()
check("previous_daily_selected_orders_retained", current_old.index.equals(previous.index))
fields = ["filled", "portfolio_executed", "entry_ts", "exit_ts", "native_stop_pct", "native_target_pct",
    "portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees"]
for column in fields:
    left, right = current_old[column], previous[column]
    match = (np.allclose(left, right, atol=1e-6, rtol=0, equal_nan=True)
             if pd.api.types.is_numeric_dtype(left) else left.fillna("NULL").equals(right.fillna("NULL")))
    check("previous_daily_execution_unchanged", match, field=column)
added = ledger.loc[ledger.setup_id.isin(contract.midday_setups.SETUP_IDS)]
for row in added.itertuples(index=False):
    setup = contract._ACTIVE_BY_ID[row.setup_id]
    check("added_target_stop_matches", row.native_target_pct == setup.target_pct and row.native_stop_pct == 1.25,
        setup_id=row.setup_id)
    check("exact_confirmation_clock", pd.Timestamp(row.confirmation_ts) == pd.Timestamp(row.signal_ts)+pd.Timedelta(minutes=1),
        setup_id=row.setup_id)
actual = added.loc[added.portfolio_executed.eq(True)]
history_directory, history = publisher._read_verified_pointer(DATA, "current_history.json", "cumulative_history")
check("new_revision_history_isolation", history["days"] == ["2026-10-13"] and history["strategy_version"] == contract.STRATEGY_VERSION)
result = dict(status="PASS", checks=checks, check_count=len(checks), run_dir=str(directory),
    manifest_sha256=sha(directory / "run_manifest.json"), daily_metrics=manifest["metrics"],
    added_metrics=dict(selected_orders=len(added), trades=len(actual), wins=int(actual.portfolio_net_profit_rupees.gt(0).sum()),
        net_pnl_rs=float(actual.portfolio_net_profit_rupees.sum())),
    added_trades=added[["setup_id", "tradingsymbol", "native_target_pct", "portfolio_executed", "portfolio_net_profit_rupees"]].to_dict("records"),
    coverage=manifest["coverage"], history_run_dir=str(history_directory))
(HERE / "daily_validation.json").write_text(json.dumps(result, indent=2))
print(json.dumps({key:result[key] for key in ("status", "check_count", "run_dir", "manifest_sha256", "daily_metrics", "added_metrics")}, indent=2))
