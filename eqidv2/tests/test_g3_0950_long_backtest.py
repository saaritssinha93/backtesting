"""Guard the backtesting-only 09:50 G3 profile from changing production rules."""
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as contract
from research import g3_0950_long_backtest as backtest
from research.g3_0950_long_backtest import DEFAULT_CONFIG, PREFERRED_CONFIG, backtest_setup, configuration, normalize_signals


def test_extra_setup_is_scoped_and_original_g3_setup_is_preserved():
    config = configuration(DEFAULT_CONFIG)
    before_payload = contract.strategy_payload()
    before_0955 = contract.setup_for("09:55", "LONG")
    production_setup = contract.setup_for("09:50", "LONG")
    assert production_setup.target_pct == .75
    with backtest_setup(config) as setup:
        assert contract.setup_for("09:50", "LONG") == setup
        assert contract.setup_for("09:55", "LONG") == before_0955
        assert contract.confirmation_volume_minimum("LONG", signal_end="09:50") == .6
        assert contract.confirmation_volume_minimum("LONG") == 1.1
        assert len(contract.ACTIVE_SETUPS) == 15
        assert (setup.price_change_pct, setup.oi_change_pct, setup.volume_ratio) == (.1, .05, 1.3)
        assert (setup.body_ratio, setup.max_wick_ratio, setup.target_pct) == (.5, .5, 1.)
    assert contract.setup_for("09:50", "LONG") == production_setup
    assert contract.strategy_payload() == before_payload


def test_researched_target_changes_only_the_isolated_setup():
    production_fingerprint = contract.strategy_fingerprint()
    old = configuration(DEFAULT_CONFIG)
    new = configuration(PREFERRED_CONFIG)
    assert new["new_setup"] == {**old["new_setup"], "target_pct": .75}
    assert new["execution_authority"] is False
    with backtest_setup(new) as setup:
        assert setup.target_pct == .75
        assert setup.stop_pct == 1.25
        assert contract.strategy_fingerprint() == production_fingerprint
    assert contract.setup_for("09:50", "LONG").target_pct == .75


def test_extension_with_only_canonical_ema_columns_normalizes():
    frame = pd.DataFrame([dict(signal_ts="2026-09-24 09:50:00+05:30",
        confirmation_ts="2026-09-24 09:51:00+05:30",
        ema9=103.,ema20=102.,ema50=101.)])
    normalized = normalize_signals(frame)
    assert normalized.loc[0,["ema9","ema20","ema50"]].tolist() == [103.,102.,101.]
    assert normalized.loc[0,"signal_end"] == "09:50"
    assert normalized.loc[0,"source_1m_count"] == 5


def test_extension_preserves_native_signal_id_offset(monkeypatch):
    strict = pd.DataFrame([dict(sid=10,signal_ts="2026-09-24 09:50:00+05:30",
        confirmation_ts="2026-09-24 09:51:00+05:30",ema9=103.,ema20=102.,ema50=101.),
        dict(sid=27,signal_ts="2026-09-24 09:50:00+05:30",
        confirmation_ts="2026-09-24 09:51:00+05:30",ema9=103.,ema20=102.,ema50=101.)])
    monkeypatch.setattr(backtest,"_extension_pool",lambda *_: pd.DataFrame({"nifty_first_bar_return_pct":[.1]}))
    monkeypatch.setattr(backtest.source,"_strict_signals",lambda *_: strict.copy())
    actual = backtest._strict_extension(date(2026,9,24),Path("run"),Path("snapshot"),1_000_000)
    assert actual.sid.tolist() == [1_000_010,1_000_027]


@pytest.mark.parametrize("change", [
    {"execution_authority": True},
    {"new_setup": {"signal_end": "09:55"}},
    {"new_setup": {"target_pct": .9}},
])
def test_non_backtest_identity_or_exit_is_rejected(tmp_path, change):
    config = configuration(DEFAULT_CONFIG)
    for key, value in change.items():
        if key == "new_setup":
            config[key].update(value)
        else:
            config[key] = value
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        configuration(path)
