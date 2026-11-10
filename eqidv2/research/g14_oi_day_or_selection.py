"""Isolated V14 morning selector for the OI-day OR and body/wick ablations.

This starts from the frozen complete prefilter stock pool.  It deliberately
does not use the six-field V14 V2 ``integrity_ok`` flag: official PDC is not an
input to V14 V1 or to the requested OI rule.  Existing V14 setup thresholds,
pickers, core preference and quotas are verified against the shared G3
selection contract before using its causal gate evaluator.

The four named arms isolate removal of minimum body/maximum wick, replacement
of the old minimum OI-delta threshold, and their combination.  Nifty-futures
permission and G7 low-efficiency veto are later post-selection decisions and
must be handled explicitly by the runner, not silently approximated here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_indicator_lab_20261103 import selection as lab
from research import g3_target_cutoff_contract as donor


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/strategies/v13_v10_g_14_oi_day_or_v3.json"
BASE_CONFIG = ROOT / "configs/strategies/v13_v10_g_14_backtest.json"
ARMS = ("baseline_reconstructed", "remove_body_wick", "oi_day_or", "combined")
SETUP_FIELDS = ("price_change_pct", "oi_change_pct", "volume_ratio",
                "body_ratio", "max_wick_ratio", "min_traded_value",
                "picker", "max_entries", "confirmation_end")


def _settings() -> tuple[dict, dict, dict[str, dict]]:
    overlay = json.loads(CONFIG.read_text(encoding="utf-8"))
    baseline_bytes = BASE_CONFIG.read_bytes()
    if hashlib.sha256(baseline_bytes).hexdigest() != overlay["baseline_config_sha256"]:
        raise ValueError("V14 V1 configuration changed after V3 overlay was frozen")
    baseline = json.loads(baseline_bytes)
    specs = {spec["setup_id"]: spec for spec in baseline["active_setups"]}
    if len(specs) != 12 or set(overlay["entry_gate_arms"]) != set(ARMS):
        raise ValueError("V14 V3 requires exactly twelve morning setups and four ablation arms")
    inherited = {spec.setup_id: spec for spec in donor.ACTIVE_SETUPS}
    if not set(specs) <= set(inherited):
        raise ValueError("V14 setup not present in frozen source selector")
    for setup_id, spec in specs.items():
        parent = inherited[setup_id]
        for field in SETUP_FIELDS:
            if spec[field] != getattr(parent, field):
                raise ValueError(f"V14 {setup_id} {field} differs from the full-pool gate evaluator")
        if spec["stop_pct"] != 1.2:
            raise ValueError("V14 V3 must preserve the 1.2% initial stop")
    if overlay["oi_rule"] != {
        "replaces_old_per_slot_oi_delta_minimum": True,
        "definition": "same-contract OI day change strictly greater than 1.00% OR latest completed-bar OI change strictly positive",
        "oi_day_pct_strictly_gt": 1.0,
        "oi_delta_5m_pct_strictly_gt": 0.0,
        "opening_0920_delta_window": "09:16_TO_COMPLETED_09:20_FOUR_MINUTES",
        "opening_0920_oi_day_pct": 0.0,
        "preserve_existing_oi_upper_cap": True,
        "missing_required_oi_evidence": "FAIL_CLOSED",
    }:
        raise ValueError("V14 V3 OI rule differs from frozen requested semantics")
    return overlay, baseline, specs


def _number(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame.get(column, pd.Series(np.nan, index=frame.index)), errors="coerce")


def _normalized_slot(frame: pd.DataFrame) -> pd.Series:
    slot = frame.get("slot", frame.get("hhmm", pd.Series("", index=frame.index)))
    return slot.astype(str).str.replace(":", "", regex=False).str.zfill(4)


def oi_day_or_branches(frame: pd.DataFrame) -> pd.DataFrame:
    """Evaluate strict branch conditions with independent, same-contract proof.

    For 09:25 onwards, the rising branch requires the source's previous OI to
    match the *immediately preceding* completed five-minute slot.  A stale
    comparison to an older available OI bar is not called "increasing".  The
    day branch instead compares current OI with the 09:20 OI of the same dated
    contract.  Missing day evidence cannot invalidate an otherwise exact
    rising branch, and vice versa.

    The separate inherited OI upper cap is applied by the caller's selection
    evaluator.  Thus a branch can be true even when upper-cap evidence is
    absent; the final admission then fails closed on the cap.  No observed
    H>1% candidate in the 63-day frozen pool lacks raw upper-cap evidence.
    """
    required = {"sid", "day", "tradingsymbol", "oi", "prev_oi", "oi_day_pct",
                "oi_delta_5m_pct", "opening_oi_0920", "contract_month",
                "opening_contract_month", "futures_tradingsymbol", "oi_source",
                "opening_futures_symbol"}
    missing = required - set(frame)
    if missing:
        raise ValueError(f"OI rule missing source fields: {sorted(missing)}")
    if frame.sid.isna().any() or frame.sid.duplicated().any():
        raise ValueError("OI rule requires unique source SID")
    work = frame.copy()
    slot = _normalized_slot(work)
    work["_slot"] = slot
    work = work.sort_values(["day", "tradingsymbol", "_slot"], kind="stable")
    group = work.groupby(["day", "tradingsymbol"], sort=False)
    preceding_oi = group.oi.shift(1)
    preceding_contract = group.futures_tradingsymbol.shift(1)
    preceding_slot = group._slot.shift(1)
    current_oi = _number(work, "oi")
    previous_oi = _number(work, "prev_oi")
    opening_oi = _number(work, "opening_oi_0920")
    delta = _number(work, "oi_delta_5m_pct")
    day_change = _number(work, "oi_day_pct")
    opening = slot.loc[work.index].eq("0920")
    current_contract = work.futures_tradingsymbol.astype("string")
    source_oi = work.oi_source.eq("NFO_FUTURE")

    day_branch = (
        ~opening & np.isfinite(current_oi) & current_oi.gt(0)
        & np.isfinite(opening_oi) & opening_oi.gt(0)
        & np.isfinite(day_change) & (100.0 * current_oi).gt(101.0 * opening_oi)
        & work.contract_month.eq(work.opening_contract_month)
        & current_contract.eq(work.opening_futures_symbol.astype("string"))
        & source_oi
    )

    # The opening comparison is explicitly 09:16->09:20, not five minutes.
    opening_proof = (
        work.get("opening_0920_status", pd.Series("", index=work.index)).eq("VALID")
        & work.get("oi_change_source", pd.Series("", index=work.index)).isin(
            ("FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", "PROVISIONAL_FULL_QUOTE"))
    )
    clock_int = pd.to_numeric(work._slot, errors="coerce")
    previous_clock_int = pd.to_numeric(preceding_slot, errors="coerce")
    consecutive = previous_clock_int.eq(clock_int - 5)
    prior_match = np.isclose(previous_oi.to_numpy(float),
                             pd.to_numeric(preceding_oi, errors="coerce").to_numpy(float),
                             rtol=0, atol=1e-6, equal_nan=False)
    exact_previous = (consecutive & prior_match &
                      current_contract.eq(preceding_contract.astype("string")))
    rising_branch = (
        np.isfinite(current_oi) & current_oi.gt(0)
        & np.isfinite(previous_oi) & previous_oi.gt(0)
        & np.isfinite(delta) & delta.gt(0.0)
        & current_oi.gt(previous_oi)
        & (opening & opening_proof | ~opening & exact_previous)
        & source_oi
    )
    result = pd.DataFrame({
        "sid": work.sid.astype("int64"),
        "oi_day_gt_1": day_branch.fillna(False).astype(bool),
        "oi_latest_bar_increasing": rising_branch.fillna(False).astype(bool),
        "oi_latest_bar_exact": (opening & opening_proof | ~opening & exact_previous).fillna(False).astype(bool),
    }, index=work.index)
    result["oi_day_or_pass"] = result.oi_day_gt_1 | result.oi_latest_bar_increasing
    return result.set_index("sid").reindex(frame.sid.astype("int64")).reset_index()


def oi_day_or_mask(frame: pd.DataFrame) -> pd.Series:
    """Boolean OR by the caller's row order; useful for boundary testing."""
    return pd.Series(oi_day_or_branches(frame).oi_day_or_pass.to_numpy(bool), index=frame.index)


def select_day(candidates: pd.DataFrame, arm: str = "combined") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Select one day from the full pool and return selected orders plus audit.

    This function forms the OI rule before inspecting any outcome.  Selection
    uses V14's other pre-quota gates, original picker/core priority and quotas;
    it does not use V14 V2's PDC-based integrity flag.  A later backtest runner
    must apply and audit G7's Nifty-futures and low-efficiency post-selection
    permissions separately if it claims an exact V14 V1 control.
    """
    overlay, _, specs = _settings()
    if arm not in ARMS:
        raise ValueError(f"Unknown V14 V3 arm {arm!r}")
    if candidates.empty:
        return candidates.copy(), candidates.copy()
    if len(set(candidates.day.astype(str).str[:10])) != 1:
        raise ValueError("Call select_day once per session")
    branches = oi_day_or_branches(candidates)
    by_sid = branches.set_index("sid")
    prepared = lab.prepare(candidates)
    # The source V14 thresholds/pickers were verified against lab.prepare's
    # frozen G3 contract above.  Targets differ for four setups and are reset
    # to V14 V1 after selection, without affecting eligibility or ranking.
    branch = prepared.sid.map(by_sid.oi_day_or_pass).fillna(False).astype(bool)
    choice = overlay["entry_gate_arms"][arm]
    removed = tuple(choice["remove"])
    if choice["replace_oi_lower_with_or"] != ("oi_lower" in removed):
        raise ValueError("V14 V3 OI lower removal must coincide with OR replacement")
    selected, audit = lab.select(prepared, variant=f"g14_{arm}",
                                 removed_gates=removed,
                                 extra_mask=branch if choice["replace_oi_lower_with_or"] else None,
                                 all_slots=True, use_core=True)
    for output in (selected, audit):
        for field in ("oi_day_gt_1", "oi_latest_bar_increasing", "oi_latest_bar_exact", "oi_day_or_pass"):
            output[field] = output.sid.map(by_sid[field]).fillna(False).astype(bool)
        output["v14_v3_arm"] = arm
        output["v14_v3_postselection_guards"] = "REQUIRE_SEPARATE_NIFTY_AND_LOW_EFFICIENCY_AUDIT"
        output["native_stop_pct"] = output.setup_id.map({k: v["stop_pct"] for k, v in specs.items()})
        output["native_target_pct"] = output.setup_id.map({k: v["target_pct"] for k, v in specs.items()})
        output["stop_pct"] = output.native_stop_pct
        output["target_pct"] = output.native_target_pct
    if selected.sid.duplicated().any() or selected.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("Duplicate V14 selected order")
    return selected, audit


__all__ = ["select_day", "oi_day_or_mask", "oi_day_or_branches", "ARMS", "CONFIG"]
