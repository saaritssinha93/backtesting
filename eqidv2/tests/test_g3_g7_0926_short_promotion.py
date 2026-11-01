import json
from pathlib import Path

import pandas as pd

from research import archived_g3_production_v13 as paper_contract
from research import g3_target_cutoff_contract as g3


ROOT = Path(__file__).resolve().parents[1]


def test_g3_minimum_wick_runs_before_ranking_and_preserves_rejected_audit(monkeypatch):
    signals = pd.DataFrame([
        dict(sid=1, setup_id="0926_SHORT", day="2026-10-01", side="SHORT",
             tradingsymbol="LOW_WICK_TOP_VOLUME", wick_ratio=.10, volume_ratio=9.),
        dict(sid=2, setup_id="0926_SHORT", day="2026-10-01", side="SHORT",
             tradingsymbol="VALID_ONE", wick_ratio=.25, volume_ratio=8.),
        dict(sid=3, setup_id="0926_SHORT", day="2026-10-01", side="SHORT",
             tradingsymbol="VALID_REPLACEMENT", wick_ratio=.30, volume_ratio=7.),
    ])

    def fake_donor_audit(source):
        out = source.copy().sort_values("volume_ratio", ascending=False)
        out["v9_selected"] = False
        out.loc[out.index[:2], "v9_selected"] = True
        out["v9_filter_pass"] = True
        out["v9_rank_in_setup_day"] = range(1, len(out) + 1)
        out["v9_decision"] = out.v9_selected.map({True: "SELECTED", False: "RANKED_OUT"})
        out["hhmm_int"] = 925
        return out.reset_index(drop=True)

    monkeypatch.setattr(g3.production, "selection_audit", fake_donor_audit)
    monkeypatch.setattr(g3, "identify_orders", lambda frame: frame)
    audit = g3.selection_audit(signals)

    assert set(audit.loc[audit.v9_selected, "sid"]) == {2, 3}
    rejected = audit.set_index("sid").loc[1]
    assert rejected.v9_decision == "FILTER_REJECTED_MIN_DIRECTIONAL_WICK"
    assert not rejected.minimum_directional_wick_pass
    assert rejected.minimum_directional_wick_ratio == .20


def test_backtest_promotion_does_not_mutate_paper_or_live_identity():
    payload = g3.strategy_payload()
    assert payload["minimum_directional_wick_ratio_by_setup"] == {"0926_SHORT": .20}
    assert payload["minimum_directional_wick_policy"]["replacement_candidates"] is True
    assert payload["scope"] == "BACKTEST_ONLY"
    assert payload["execution_authority"] is False
    assert paper_contract.STRATEGY_VERSION == "V13-V10-G-3-PRODUCTION-V13"
    assert paper_contract.strategy_fingerprint() == (
        "74ff6bf6a2c1a68a08a324d8090809dbfe10a3ccacf289ab1953fb03636ca615")

    g7 = json.loads((ROOT / "configs/strategies/v13_v10_g_7_v4_lock_backtest.json").read_text())
    assert g7["selected_variant"] == "wick_ge_0p20"
    assert g7["entry_rule"]["minimum"] == .20
    assert g7["entry_rule"]["application"] == "BEFORE_MAX_VOLUME_RANKING_AND_TWO_ENTRY_QUOTA"
    assert g7["daily_loss_lock"]["maximum_realized_net_losing_exits"] == 2
    assert g7["scope"] == "BACKTEST_ONLY" and g7["execution_authority"] is False
