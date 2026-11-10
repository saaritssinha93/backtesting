"""Build the sealed 4 November V21 expanded native book for isolated research.

The function reuses V21's verified daily inputs, selection, execution paths,
and native simulator. It does not change any saved V21 policy or live rule.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from fno_v13_v10_g_3_daily_replay import simulate
from g3_v18_20261102 import data
from g3_v21_expanded_20261104 import selection, source


DAY = "2026-11-04"
DAY_INDEX = 64


def build_expanded_day(day: str = DAY) -> tuple[pd.DataFrame, dict]:
    """Return one sealed V21 cap-three native book and its source/selection proof.

    SIDs use the next daily block after the 64 immutable V21 checkpoints.
    Saved native fills are retained so the caller can apply isolated entry
    vetoes and the existing unlimited-capital replay consistently.
    """
    if day != DAY:
        raise ValueError(f"This isolated latest-day builder is pinned to {DAY}")
    raw, minutes, source_proof = source.load_day(day)
    if (source_proof.get("loader_source") !=
            "HASH_VERIFIED_SEALED_DAILY_FEATURE_LEDGER_AND_RAW_SNAPSHOT"):
        raise ValueError("Latest-day inputs must come from the sealed daily snapshot")
    for field in ("run_manifest", "source_manifest", "snapshot_manifest",
                  "features_sha256", "snapshot_manifest_sha256"):
        if not source_proof.get(field):
            raise ValueError(f"Latest-day source proof is missing {field}")
    expansion = source_proof.get("v21_expansion", {})
    if (expansion.get("missing_append_rows") != 0 or
            expansion.get("through_clock") != "14:00"):
        raise ValueError("Latest-day all-slot expansion is incomplete")
    if len(selection.SPECS) != 73:
        raise ValueError("V21 setup inventory changed")

    raw = raw.reset_index(drop=True).copy()
    raw["sid"] = DAY_INDEX * 1_000_000 + np.arange(len(raw), dtype=np.int64)
    orders, audit, _ = selection.select(raw)
    if orders.empty or not orders.sid.is_unique:
        raise ValueError("Missing or duplicate latest-day V21 selected orders")
    if not orders.day.astype(str).eq(day).all():
        raise ValueError("Latest-day selection crossed session boundaries")
    if (orders.sid.min() < DAY_INDEX * 1_000_000 or
            orders.sid.max() >= (DAY_INDEX + 1) * 1_000_000):
        raise ValueError("Latest-day SID block changed")
    if orders.groupby(["day", "setup_id"]).size().gt(selection.CAP).any():
        raise ValueError("Latest-day V21 cap-three selection changed")
    if not orders.setup_id.isin(selection.SPECS).all():
        raise ValueError("Unknown latest-day V21 setup")
    selected_audit = audit.loc[audit.v9_selected.eq(True)]
    if set(selected_audit.sid) != set(orders.sid):
        raise ValueError("Latest-day selected audit and native input differ")

    paths = data.execution_paths(orders, minutes)
    book, _ = simulate(orders, paths, [date.fromisoformat(day)])
    if set(book.sid) != set(orders.sid) or not book.sid.is_unique:
        raise ValueError("Latest-day native book changed selected identities")
    book["research_arm"] = "v21_expanded_cap3_g7_confirmation_source"
    book["execution_authority"] = False
    proof = dict(day=day, scope="BACKTEST_ONLY", execution_authority=False,
                 source=source_proof,
                 selection_version=selection.VERSION,
                 selection_fingerprint=selection.fingerprint(),
                 setup_inventory=len(selection.SPECS),
                 source_feature_rows=len(raw),
                 source_minute_rows=len(minutes),
                 selected_orders=len(orders),
                 native_fills=int(book.filled.astype(bool).sum()),
                 selected_setup_pairs=int(orders.setup_id.nunique()),
                 sid_first=int(book.sid.min()), sid_last=int(book.sid.max()))
    return book, proof
