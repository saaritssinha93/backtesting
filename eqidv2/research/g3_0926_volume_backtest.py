"""Reselect both G3 09:26 sides from sealed pre-quota evidence.

LONG uses the accepted 1.2x five-minute volume threshold.  SHORT additionally
uses the V15 0.20 minimum directional (lower) wick rule.  Both rules run before
ranking and quota so a valid lower-ranked stock can replace a rejected one.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from eqidv2_runtime_paths import runtime_dir
from research import g3_target_cutoff_contract as contract
from research.g4_extended_target_sweep import read_frozen_csv
from research.g4_late_slot_source import sha256

SOURCE_RELATIVE = "runs/2026-10-15/20261015T141247213906_eaeed9b5_late_slots"
MANIFEST_SHA256 = "976a48a524ac0f970c509e37dc6279cf881f3837390c39c82c9f31204519d9fa"
POOL_SHA256 = "e9ac4cea2629ddcc899fd7885181872ceb2a24d3b7c8748309dda5d186d71375"
SETUP_IDS = ("0926_LONG", "0926_SHORT")
EXPECTED_CANDIDATES = {"0926_LONG": 169, "0926_SHORT": 163}


def reselect(original_orders, days):
    """Return selected orders, exact minute paths, source proof and full decision audit."""
    from research.g4_g3_mirror_backtest import Prices, read, verify

    directory = (runtime_dir("backtesting_result_v13_v10_g_4") / SOURCE_RELATIVE).resolve()
    manifest_path = directory / "manifest.json"
    pool_path = directory / "candidate_extension_audit.csv"
    if sha256(manifest_path) != MANIFEST_SHA256 or sha256(pool_path) != POOL_SHA256:
        raise ValueError("Pinned pre-quota 09:26 candidate evidence changed")
    summary = verify(directory)
    if (summary.get("sessions") != len(days) + 1 or summary.get("first_date") != str(days[0])
            or summary.get("last_date") <= str(days[-1])):
        raise ValueError("09:26 candidate calendar differs from G3 historical replay")
    pool = read_frozen_csv(pool_path)
    pool = pool.loc[pool.setup_id.isin(SETUP_IDS) & pool.day.astype(str).isin({str(day) for day in days})].copy()
    counts = pool.groupby("setup_id").size().to_dict()
    if counts != EXPECTED_CANDIDATES or pool.sid.duplicated().any():
        raise ValueError("09:26 pre-quota candidate inventory changed")

    def keys(frame):
        return set(zip(frame.day.astype(str).str[:10], frame.tradingsymbol, frame.sid.astype(int)))

    original = original_orders.loc[original_orders.setup_id.isin(SETUP_IDS)]
    if keys(pool.loc[pool.v9_selected.eq(True)]) != keys(original):
        raise ValueError("Pinned 09:26 selections differ from original G3 baseline")
    audit = contract.selection_audit(pool)
    selected = audit.loc[audit.v9_selected.eq(True)].copy()
    if selected.duplicated(["day", "setup_id", "tradingsymbol"]).any():
        raise ValueError("09:26 replay selected duplicate stocks")
    if selected.groupby([selected.day.astype(str).str[:10], "setup_id"]).size().gt(2).any():
        raise ValueError("09:26 replay exceeded the per-side two-entry quota")
    baseline_dir = Path(summary["baseline_g3_run"]).resolve()
    if sha256(baseline_dir / "manifest.json") != summary["baseline_g3_manifest_sha256"]:
        raise ValueError("Pinned 09:26 price-source baseline changed")
    prices = Prices(read(baseline_dir / "summary.json"))
    paths = prices.paths(selected)
    proof = dict(source_run=SOURCE_RELATIVE, source_manifest_sha256=MANIFEST_SHA256,
        source_pool_sha256=POOL_SHA256, candidate_rows=len(audit), original_selected=len(original),
        revised_selected=len(selected), candidate_rows_by_setup=counts,
        original_selected_by_setup=original.groupby("setup_id").size().to_dict(),
        revised_selected_by_setup=selected.groupby("setup_id").size().to_dict(),
        threshold_5m_volume_ratio={"0926_LONG": 1.2},
        minimum_directional_wick_ratio={"0926_SHORT": .20},
        filter_application="BEFORE_RANKING_AND_QUOTA_WITH_REPLACEMENT",
        outcomes_used_for_selection=False, price_sources=prices.proof)
    return selected, paths, proof, audit
