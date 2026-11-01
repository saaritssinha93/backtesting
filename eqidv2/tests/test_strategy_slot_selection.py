import json
from dataclasses import asdict

import pytest

import strategy_slot_selection as policy
from research import g3_slot_win70_contract as backtest
import fno_v13_v10_g_3_cutoff_production as production
import fno_v13_v10_g_3_live_config as live


@pytest.mark.parametrize("wins,trades,expected", [(7,10,True),(69,100,False),(1,1,True),(0,0,False),(2,3,False)])
def test_inclusive_exact_ratio_and_empty_sample(wins,trades,expected):
    assert policy.qualifies(wins,trades) is expected


def test_resealed_policy_cannot_enable_below_threshold_slot(tmp_path):
    value = policy.policy_payload()
    value["strategies"]["G3"]["enabled_setup_ids"].append("0931_LONG")
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="exact >=70"):
        policy.load_policy(path)


def test_each_strategy_uses_own_evidence_and_no_empty_sample():
    assert "1226_SHORT" not in policy.enabled_setup_ids("G3")
    assert "1226_SHORT" in policy.enabled_setup_ids("G7_V4_LOCK2")
    assert "0936_SHORT" in policy.enabled_setup_ids("G3")
    assert "0936_SHORT" not in policy.enabled_setup_ids("G7_V4_LOCK2")  # Zero executed trades.
    assert len(policy.enabled_setup_ids("G3")) == 34
    assert len(policy.enabled_setup_ids("G7_V4_LOCK2")) == 42
    for strategy in policy.STRATEGIES:
        assert len(policy.REVIEWED_SETUP_IDS) == 51
        assert not policy.enabled_setup_ids(strategy) - policy.REVIEWED_SETUP_IDS
        rows = policy.policy_payload(strategy)["slot_statistics"]
        assert policy.enabled_setup_ids(strategy) == {r["setup_id"] for r in rows
            if policy.qualifies(r["wins"], r["trades"])}


def test_current_g3_all_surfaces_share_selection_and_confirmation_rules():
    backtest.validate_contract()
    production.validate_contract()
    live.validate_strategy()
    expected = policy.enabled_setup_ids("G3")
    assert set(backtest._ACTIVE_BY_ID) == set(production._ACTIVE_BY_ID) == expected
    assert {s.setup_id for s in live.ACTIVE_SETUPS} == expected
    for setup_id in expected:
        a,b = asdict(backtest._ACTIVE_BY_ID[setup_id]),asdict(production._ACTIVE_BY_ID[setup_id])
        a.pop("source_version"); b.pop("source_version")
        assert a == b
    assert backtest.MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP == {"0926_SHORT": .2}
    assert production.MIN_DIRECTIONAL_WICK_RATIO_BY_SETUP == {}  # Preserve prior production rule differences.
    assert not production.EXECUTION_AUTHORITY and not live.ARMED


def test_disabled_slots_rejected_before_ranking_and_original_identity_preserved():
    setup = backtest.previous._ACTIVE_BY_ID["0931_LONG"]
    assert production.setup_for("09:30", "LONG") is None
    assert not production.passes_selected_filters({},setup)
    assert production.rank_candidates([{}],setup) == []
    old = production.previous
    assert old.STRATEGY_VERSION == "V13-V10-G-3-PRODUCTION-V13"
    assert old.strategy_fingerprint() == "74ff6bf6a2c1a68a08a324d8090809dbfe10a3ccacf289ab1953fb03636ca615"
    assert production.supported_identity(old.STRATEGY_VERSION,old.strategy_fingerprint(),allow_legacy=True)
    assert not production.supported_identity(old.STRATEGY_VERSION,old.strategy_fingerprint())
    prior = production.previous_selected
    assert prior.strategy_fingerprint() == prior.EXPECTED_FINGERPRINT
    assert production.supported_identity(prior.STRATEGY_VERSION,prior.strategy_fingerprint(),allow_legacy=True)
