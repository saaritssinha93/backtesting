"""Source-level parity assertions for the combined PAPER profiles.

These assertions prove that the frozen numeric contracts were transcribed
from the canonical backtest sources.  They are not a substitute for a full
historical event-for-event replay, so the exported certification label says so
explicitly.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

import fno_multi_paper_profiles as paper


PARITY_STATUS = "SOURCE_ASSERTED_AND_INVARIANT_TESTED_NOT_FULL_HISTORY_CERTIFIED"


def validate_canonical_profiles() -> dict[str, Any]:
    import fno_v10_backtest as v10_backtest
    import fno_v10_backtest_config as v10_locked
    import fno_v10_followup_challenger_research as v10_selection
    import fno_v10_gap_guard_research as v10_gap
    import fno_v10_unified_5m_1m_backtest as v10_base
    import fno_v11_backtest as v11_backtest
    import fno_v12_backtest as v12_backtest

    if paper.V10_PROFILE.profile_id != v10_backtest.MAX050_GAP2_PROFILE_ID:
        raise AssertionError("V10 PAPER profile ID differs from canonical backtest")
    if paper.V11_PROFILE.profile_id != v11_backtest.PROFILE_ID:
        raise AssertionError("V11 PAPER profile ID differs from canonical backtest")
    if paper.V12_PROFILE.profile_id != v12_backtest.PROFILE_ID:
        raise AssertionError("V12 PAPER profile ID differs from canonical backtest")
    if v10_locked.ACTIVE_VARIANT != "0940_LONG_MOVE_040":
        raise AssertionError("canonical V10 Stage 7 variant changed")
    if v10_selection.SPEC_BY_NAME[
        v10_backtest.MAX050_GAP2_SELECTION_VARIANT
    ].move_0935_long_max != 0.50:
        raise AssertionError("canonical V10 09:35 LONG ceiling changed")
    gap = next(
        item
        for item in v10_gap.GAP_GUARDS
        if item.variant == v10_backtest.MAX050_GAP2_GAP_VARIANT
    )
    if gap.max_adverse_gap_bps != 2.0 or gap.reject_all_gap_fills:
        raise AssertionError("canonical V10 Gap2 contract changed")

    base_by_id = {item.setup_id: item for item in v10_base.ACTIVE_SETUPS}
    paper_by_id = paper.V10_PROFILE.setup_by_id
    if set(base_by_id) != set(paper_by_id):
        raise AssertionError("PAPER setup IDs differ from canonical V10")
    for setup_id, observed in paper_by_id.items():
        expected = asdict(base_by_id[setup_id])
        if setup_id == "09:40_LONG":
            expected["price_change_pct"] = 0.40
        if asdict(observed) != expected:
            raise AssertionError(f"PAPER setup differs from V10 contract: {setup_id}")
    max050 = paper.V10_PROFILE.selection_constraint_by_id["09:35_LONG"]
    if max050.max_directional_move_pct != 0.50:
        raise AssertionError("PAPER V10 max050 overlay changed")

    runtime = v11_backtest.FIXED_RUNTIME_SPEC
    if (
        runtime.entry_setup_id != "09:30_SHORT"
        or runtime.entry_not_before_minute != 3
        or runtime.exit_rule is not None
        or runtime.same_side_symbol_limit != 2
    ):
        raise AssertionError("canonical V11 Stage10 runtime changed")
    if paper.V11_PROFILE.execution.entry_not_before != (("09:30_SHORT", 3),):
        raise AssertionError("PAPER V11 delayed-entry rule changed")
    if paper.V11_PROFILE.execution.same_side_symbol_limit != 2:
        raise AssertionError("PAPER V11 same-side limit changed")

    fixed = v12_backtest.FIXED_CONFIG
    if (
        fixed.selection.move_0935_long_max_pct != 0.50
        or fixed.selection.move_0940_long_min_pct != 0.40
        or fixed.selection.volume_0940_short_min != 1.50
        or fixed.selection.volume_0945_short_min != 1.50
        or fixed.gap.max_adverse_gap_bps != 2.0
        or fixed.runtime.m2_short_mode is not None
        or fixed.runtime.long_entry_expiry_minute is not None
    ):
        raise AssertionError("canonical V12 selected contract changed")
    for setup_id in ("09:40_SHORT", "09:45_SHORT"):
        if paper.V12_PROFILE.setup_by_id[setup_id].volume_ratio != 1.50:
            raise AssertionError(f"PAPER V12 volume gate changed: {setup_id}")

    return {
        "status": PARITY_STATUS,
        "full_history_event_parity_certified": False,
        "source_contract_assertions_passed": True,
        "profiles": {
            profile.key: {
                "profile_id": profile.profile_id,
                "profile_fingerprint": profile.fingerprint,
            }
            for profile in paper.PROFILES
        },
    }


__all__ = ["PARITY_STATUS", "validate_canonical_profiles"]
