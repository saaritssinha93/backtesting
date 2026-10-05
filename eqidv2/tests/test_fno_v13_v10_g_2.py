"""Fixed-contract tests for the V13-v10-G-2 1.00% stop replay."""
import copy

import pytest

import fno_v13_v10_g_2_backtest as g2


def source_config():
    return {
        "version": "V13-v10-G-NEW",
        "source_version": "V13-v10-F",
        "selection_change": {
            "price_multiplier": 0.65,
            "oi_multiplier": 1.0,
            "body_reduction": 0.0,
            "wick_increase": 0.0,
            "extra_setup_entries": 0,
            "expansion_side": "SHORT",
        },
        "core_first": True,
        "exit": {
            "version": "V13-v10-B",
            "default": {"stop_pct": 0.82, "target_pct": 1.23},
            "setups": {
                "0926_LONG": {"stop_pct": 0.60, "target_pct": 0.97},
                "0931_SHORT": {"stop_pct": 0.89, "target_pct": 2.00},
            },
            "partial_exits": False,
            "breakeven_stop": False,
        },
        "minimum_confirmation_1m_volume_ratio": 1.2,
        "entry_expiry_minutes": 10,
        "portfolio_capital_rupees": 1_000_000.0,
        "capital_per_entry_rupees": 100_000.0,
        "leverage_factor": 5.0,
        "max_positions": None,
        "cost_bps": 5.0,
        "partial_exits": False,
        "breakeven_stop": False,
        "source_oi_floor_pct": 0.05,
        "evidence": "SOURCE",
    }


def test_every_stop_is_one_percent_and_targets_are_unchanged():
    source = source_config()
    result = g2.config(source)
    assert result["version"] == "V13-v10-G-2"
    assert result["exit"]["default"]["stop_pct"] == 1.0
    assert result["exit"]["default"]["target_pct"] == source["exit"]["default"]["target_pct"]
    for setup_id, pair in result["exit"]["setups"].items():
        assert pair["stop_pct"] == 1.0
        assert pair["target_pct"] == source["exit"]["setups"][setup_id]["target_pct"]
    assert source == source_config(), "the source G configuration must not be mutated"


@pytest.mark.parametrize("mutation", ["stop", "target", "selection", "partial"])
def test_fixed_g2_contract_rejects_drift(mutation):
    source = source_config()
    candidate = g2.config(source)
    if mutation == "stop":
        candidate["exit"]["setups"]["0926_LONG"]["stop_pct"] = 0.99
    elif mutation == "target":
        candidate["exit"]["setups"]["0926_LONG"]["target_pct"] = 1.00
    elif mutation == "selection":
        candidate["selection_change"]["price_multiplier"] = 0.75
    else:
        candidate["partial_exits"] = True
    with pytest.raises(ValueError, match="fixed 1.00% stop experiment"):
        g2.checked_settings(candidate, source)


def test_exit_validator_rejects_setup_key_changes():
    source = source_config()["exit"]
    candidate = g2.transformed_exit(source)
    del candidate["setups"]["0931_SHORT"]
    with pytest.raises(ValueError, match="setup keys"):
        g2.validate_exit(candidate, source)


def test_configuration_has_no_execution_authority():
    result = g2.config(copy.deepcopy(source_config()))
    assert result["live_configuration_changed"] is False
    assert result["execution_authority"] is False
    assert result["evidence"] == g2.EVIDENCE
