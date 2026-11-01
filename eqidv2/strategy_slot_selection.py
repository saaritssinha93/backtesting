"""Frozen, user-selected slot eligibility shared by all strategy surfaces.

Selection uses net outcomes through 2026-10-27. It is retrospective and does
not assert an out-of-sample win rate. Loading it never arms an executor.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "configs/strategies/slot_win70_20261028.json"
STRATEGIES = ("G3", "G7_V4_LOCK2")
REVIEWED_SETUP_IDS = frozenset({row["setup_id"] for row in
    json.loads(CONFIG_PATH.read_text(encoding="utf-8"))["strategies"]["G3"]["slot_statistics"]})
EXPECTED_INVENTORY_SHA256 = "82d97bc71abc41d263f1507512dfb4950a5da942b30e8c533854b2fe50f6423a"


def qualifies(wins: int, trades: int) -> bool:
    if isinstance(wins, bool) or isinstance(trades, bool) or not isinstance(wins, int) or not isinstance(trades, int):
        raise ValueError("Win and trade counts must be integers")
    if trades < 0 or not 0 <= wins <= trades:
        raise ValueError("Invalid win/trade counts")
    return trades > 0 and 100 * wins >= 70 * trades


def load_policy(path=CONFIG_PATH):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    inventory_digest = hashlib.sha256(json.dumps(sorted(REVIEWED_SETUP_IDS),
        separators=(",", ":")).encode()).hexdigest()
    if (value.get("schema_version") != "strategy_slot_win_rate_policy_v1"
            or inventory_digest != EXPECTED_INVENTORY_SHA256
            or value.get("threshold_percent") != 70
            or value.get("execution_authority") is not False
            or value.get("selection_uses_outcomes") is not True
            or value.get("selection_scope") != "ALL_REPRESENTED_SLOTS"
            or set(value.get("reviewed_setup_ids", [])) != REVIEWED_SETUP_IDS
            or set(value.get("strategies", {})) != set(STRATEGIES)):
        raise ValueError("Invalid slot selection policy identity or authority")
    for strategy in STRATEGIES:
        spec = value["strategies"][strategy]
        rows = spec["slot_statistics"]
        ids = [r["setup_id"] for r in rows]
        if len(ids) != len(set(ids)) or not rows:
            raise ValueError("Duplicate or empty setup inventory")
        if set(ids) != REVIEWED_SETUP_IDS:
            raise ValueError("Strategy slot inventory differs from reviewed inventory")
        expected = sorted(r["setup_id"] for r in rows if qualifies(r["wins"], r["trades"]))
        disabled = sorted(set(ids) - set(expected))
        if spec["enabled_setup_ids"] != expected or spec["disabled_setup_ids"] != disabled:
            raise ValueError(f"{strategy} enabled slots do not match exact >=70% selection")
        for row in rows:
            if row["wins"] + row["losses"] + row["breakeven"] != row["trades"]:
                raise ValueError("Outcome counts do not reconcile")
    return value


def policy_payload(strategy=None):
    value = load_policy()
    if strategy is None:
        return copy.deepcopy(value)
    if strategy not in STRATEGIES:
        raise ValueError(f"Unsupported slot strategy: {strategy}")
    return {k: copy.deepcopy(v) for k, v in value.items() if k != "strategies"} | {
        "strategy": strategy, **copy.deepcopy(value["strategies"][strategy])}


def policy_fingerprint(strategy=None):
    return hashlib.sha256(json.dumps(policy_payload(strategy), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def enabled_setup_ids(strategy):
    return frozenset(policy_payload(strategy)["enabled_setup_ids"])


def slot_allowed(strategy, setup_id):
    return str(setup_id) in enabled_setup_ids(strategy)


def dashboard_summary():
    value = policy_payload()
    return {"selection_id": value["selection_id"], "threshold_percent": 70,
        "selection_scope": value["selection_scope"], "reviewed_setup_ids": sorted(REVIEWED_SETUP_IDS),
        "source_period": value["source_period"], "selection_uses_outcomes": True,
        "independent_holdout": False, "policy_fingerprint": policy_fingerprint(),
        "strategies": {s: {"enabled_setup_ids": v["enabled_setup_ids"],
            "disabled_setup_ids": v["disabled_setup_ids"],
            "enabled_count": len(v["enabled_setup_ids"]),
            "represented_count": len(v["slot_statistics"]),
            "small_sample_setup_ids": [r["setup_id"] for r in v["slot_statistics"]
                if r["setup_id"] in v["enabled_setup_ids"] and r["trades"] < 8]}
            for s, v in value["strategies"].items()}}
