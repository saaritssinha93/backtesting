"""Named G7 V4 lock configuration; execution transport is not yet supported.

The selected slot policy is shared with its backtest/dashboard.  This is not
a G3 alias and cannot start workers or grant broker authority.  Pure causal
rule primitives live in g7_v4_lock_runtime_rules for future adapter validation.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from strategy_slot_selection import enabled_setup_ids, policy_payload, policy_fingerprint

STRATEGY_PROFILE = "V13_V10_G_7_V4_LOCK"
STRATEGY_VERSION = "V13-V10-G-7-V4-LOCK-SLOT-WIN70-RUNTIME-V2"
DISPLAY_LABEL = "G7 V4 lock · 10-slot review"
SESSION_PREFIX = "fno_v13_v10_g_7_v4_lock"
LIVE_ROOT_NAME = "v13_v10_g_7_v4_lock_live"
CONTROL_ROOT_NAME = LIVE_ROOT_NAME
REPORT_PREFIX = SESSION_PREFIX
ORDER_TAG_PREFIX = "FVG7V4"
EXECUTION_AUTHORITY = False
ARMED = False
RUNTIME_SUPPORTED = False
ROOT = Path(__file__).resolve().parent
BASE_CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_7_extreme_entries_v4.json"
BASE_CONFIG_SHA256 = "cbccb667f819c59d2365261bfd47df9769e80230efbb643eed5fb563364590bb"
CONFIG_PATH = ROOT / "configs/strategies/v13_v10_g_7_v4_lock_runtime_v2.json"
NOT_SUPPORTED = (
    "CAUSAL_STOCK_AND_DATED_NIFTY_FEED_ADAPTER",
    "NATIVE_G7_CANDIDATE_RANKING_AND_POST_QUOTA_FILTER_ADAPTER",
    "DURABLE_PAPER_EXECUTION_AND_RESTART_RECONCILIATION_ADAPTER",
    "LIVE_BROKER_ENTRY_PROTECTION_AND_PARTIAL_FILL_ADAPTER",
)


def configuration():
    raw = BASE_CONFIG_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest() != BASE_CONFIG_SHA256:
        raise ValueError("Pinned G7 V2 native setup contract changed")
    base = json.loads(raw)["base_parameters"]
    selected = enabled_setup_ids("G7_V4_LOCK2")
    inventory = {row["setup_id"] for row in base["active_setups"]}
    if not selected or selected - inventory:
        raise ValueError("Selected G7 slots do not match the native setup inventory")
    return dict(
        strategy_profile=STRATEGY_PROFILE, strategy_version=STRATEGY_VERSION,
        scope="CONFIGURED_RUNTIME_NOT_SUPPORTED", runtime_supported=False,
        execution_authority=False, default_armed=False,
        state_root_name=LIVE_ROOT_NAME, control_root_name=CONTROL_ROOT_NAME,
        source_native_config_sha256=BASE_CONFIG_SHA256,
        slot_policy=policy_payload("G7_V4_LOCK2"),
        slot_policy_fingerprint=policy_fingerprint("G7_V4_LOCK2"),
        active_setups=[r for r in base["active_setups"] if r["setup_id"] in selected],
        native_entry_contract=base,
        additional_entry_rules={
            "0926_SHORT_minimum_directional_wick": {
                "minimum": .20, "application": "BEFORE_RANKING_AND_QUOTA"},
            "low_efficiency_extreme_rsi": {
                "efficiency_below": .30, "extreme": "VWAP_Z_GT_2P5_OR_PRIOR_SAME_CLOCK_95",
                "directional_rsi_delta3_below": 0., "minimum_session_bars": 30,
                "insufficient_warmup": "PRESERVE_ENTRY"}},
        profit_protection={"completed_post_entry_close_gross_pct": .6,
            "protected_gross_pct": .1, "effective": "NEXT_MINUTE_OPEN",
            "entry_bar_cannot_arm": True, "never_loosen_stop": True},
        admission={"one_active_position_per_symbol": True,
            "daily_realized_net_loss_limit": 2, "loss_cost_basis_bps": 5.,
            "entry_rule": "BLOCK_STRICTLY_AFTER_SECOND_LOSS_EXIT",
            "open_positions": "CONTINUE_NORMAL_MANAGEMENT"},
        unsupported_adapters=list(NOT_SUPPORTED))


def strategy_payload():
    return configuration()


def strategy_fingerprint():
    return hashlib.sha256(json.dumps(configuration(), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def validate_strategy():
    expected = configuration()
    if json.loads(CONFIG_PATH.read_text(encoding="utf-8")) != expected:
        raise ValueError("G7 runtime configuration differs from its selected-slot contract")
    return expected


def readiness(mode="PAPER"):
    mode = str(mode).upper()
    if mode not in {"PAPER", "LIVE"}:
        raise ValueError("Expected PAPER or LIVE mode")
    value = validate_strategy()
    return dict(status="NOT_SUPPORTED", requested_mode=mode,
        strategy_profile=STRATEGY_PROFILE, strategy_version=STRATEGY_VERSION,
        selected_setup_ids=[r["setup_id"] for r in value["active_setups"]],
        selected_setup_count=len(value["active_setups"]),
        slot_policy_fingerprint=value["slot_policy_fingerprint"],
        unsupported_adapters=list(NOT_SUPPORTED),
        message="Slot configuration is updated; a validated G7 live/paper transport is not available.",
        execution_authority=False, workers_started=False, broker_calls=0)
