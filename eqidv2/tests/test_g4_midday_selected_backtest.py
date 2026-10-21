"""Selection and provenance guards for the twelve requested midday additions."""
import hashlib
import json
import sys

import pandas as pd
import pytest

import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_4_backtest as cli
from research import g4_midday_selected_backtest as study
from research import g4_selected_backtest as earlier


REQUESTED = {
    "1136_LONG": 7.15, "1136_SHORT": .87,
    "1141_LONG": 4.03, "1141_SHORT": 1.19,
    "1146_SHORT": 4.95, "1206_LONG": 1.40,
    "1211_SHORT": .79, "1216_LONG": 1.24, "1216_SHORT": 3.49,
    "1226_SHORT": 2.13, "1231_LONG": 2.53, "1231_SHORT": 1.85,
}


def candidate(clock, side, sid, **changes):
    stamp = pd.Timestamp("2026-10-09 " + clock, tz="Asia/Kolkata")
    row = dict(sid=sid, tradingsymbol=f"STOCK{sid}", side=side,
        signal_timestamp=stamp, confirmation_timestamp=stamp + pd.Timedelta(minutes=1),
        v9_1m_feature_ts=stamp + pd.Timedelta(minutes=1), signal_close=100.,
        price_change_pct=.5 if side == "LONG" else -.5, oi_change_pct=.5,
        oi=100500., prev_oi=100000., volume_ratio=10.,
        ema9=102. if side == "LONG" else 98., ema20=101. if side == "LONG" else 99., ema50=100.,
        body_ratio=.9, wick_ratio=.05, traded_value=1000., v9_1m_volume_ratio=10., confirmed=True)
    return dict(row, **changes)


def test_default_config_contains_exactly_user_requested_targets():
    config = study.configuration()
    assert config["variant_id"] == "V13-V10-G-4-SELECTED-MIDDAY-TWELVE-V5"
    assert study.SELECTED_TARGETS == REQUESTED
    assert {s["setup_id"]: s["target_pct"] for s in config["setups"]} == REQUESTED
    assert len(config["setups"]) == 12
    assert config["preserve_existing_g4_setups"] is True
    assert config["allow_unlisted_additions"] is False


@pytest.mark.parametrize("mutation", [
    "extra", "missing", "duplicate", "target", "nonfinite_target", "nonfinite_threshold",
    "clock", "confirmation", "side", "disabled", "stop", "quota", "picker",
    "authority", "allow_unlisted", "preserve_base", "cost", "portfolio_capital",
])
def test_configuration_rejects_subset_target_timing_and_policy_drift(tmp_path, mutation):
    config = study.configuration()
    row = config["setups"][0]
    if mutation == "extra":
        config["setups"].append(dict(row, setup_id="1151_LONG", signal_end="11:50", confirmation_end="11:51"))
    elif mutation == "missing":
        config["setups"].pop()
    elif mutation == "duplicate":
        config["setups"][-1] = row.copy()
    elif mutation == "target":
        row["target_pct"] = 1.
    elif mutation == "nonfinite_target":
        row["target_pct"] = float("nan")
    elif mutation == "nonfinite_threshold":
        row["thresholds"]["one_minute_volume_ratio_min"] = float("inf")
    elif mutation == "clock":
        row["signal_end"] = "11:34"
    elif mutation == "confirmation":
        row["confirmation_end"] = "11:40"
    elif mutation == "side":
        row["side"] = "SHORT"
    elif mutation == "disabled":
        row["enabled"] = False
    elif mutation == "stop":
        row["tightened_stop_pct"] = .5
    elif mutation == "quota":
        row["max_entries"] = 2
    elif mutation == "picker":
        row["picker"] = "max_volume"
    else:
        field, value = {
            "authority": ("execution_authority", True),
            "allow_unlisted": ("allow_unlisted_additions", True),
            "preserve_base": ("preserve_existing_g4_setups", False),
            "cost": ("round_trip_cost_bps", 0.),
            "portfolio_capital": ("portfolio_capital_rs", 2_000_000.),
        }[mutation]
        config[field] = value
    path = tmp_path / "changed.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    with pytest.raises(ValueError):
        study.configuration(path)


def test_selected_context_excludes_all_other_midday_pairs_and_restores_on_error():
    archived_before = earlier.contract.strategy_payload()
    production_before = production.strategy_payload()
    clocks = ("11:35", "11:40", "11:45", "11:50", "11:55", "12:00",
              "12:05", "12:10", "12:15", "12:20", "12:25", "12:30")
    with pytest.raises(RuntimeError, match="interrupted test"):
        with study.selected_setups(study.configuration()):
            assert earlier.contract.STRATEGY_VERSION == study.VERSION
            assert production.strategy_payload() == production_before
            assert {s.setup_id for s in earlier.contract.ACTIVE_SETUPS} == set(REQUESTED)
            for clock in clocks:
                confirmation = (pd.Timestamp("2000-01-01 " + clock) + pd.Timedelta(minutes=1)).strftime("%H%M")
                for side in ("LONG", "SHORT"):
                    setup = earlier.contract.setup_for(clock, side)
                    key = confirmation + "_" + side
                    if key not in REQUESTED:
                        assert setup is None
                    else:
                        assert setup.target_pct == REQUESTED[key]
                        assert setup.source_version == study.VERSION
                        assert setup.stop_pct == 1.25
            raise RuntimeError("interrupted test")
    assert earlier.contract.strategy_payload() == archived_before
    assert production.strategy_payload() == production_before


def test_selection_chooses_highest_liquidity_only_among_requested_qualifying_pairs():
    rows = [candidate("11:45", "SHORT", 1), candidate("11:45", "SHORT", 2, traded_value=2000.),
            candidate("11:45", "LONG", 3, traded_value=1_000_000.),
            candidate("11:50", "SHORT", 4, traded_value=1_000_000.)]
    delayed = candidate("11:45", "SHORT", 5, traded_value=3000.)
    delayed["confirmation_timestamp"] += pd.Timedelta(minutes=1)
    delayed["v9_1m_feature_ts"] = delayed["confirmation_timestamp"]
    with study.selected_setups(study.configuration()):
        audit = earlier.contract.selection_audit(pd.DataFrame(rows + [delayed]))
    selected = audit.loc[audit.v9_selected.eq(True)]
    assert selected.sid.tolist() == [2]
    assert selected.native_target_pct.tolist() == [4.95]
    assert selected.native_stop_pct.tolist() == [1.25]
    assert not set(audit.sid) & {3, 4}


@pytest.mark.parametrize("clock,side", [("11:35", "LONG"), ("11:40", "SHORT"), ("12:30", "LONG")])
def test_confirmation_must_be_exact_next_minute_even_for_relaxed_filters(clock, side):
    row = candidate(clock, side, 1)
    with study.selected_setups(study.configuration()):
        spec = earlier.contract.setup_for(clock, side)
        assert earlier.contract.passes_selected_filters(row, spec)
        for offset in (0, 2, 5):
            wrong = row["signal_timestamp"] + pd.Timedelta(minutes=offset)
            assert not earlier.contract.passes_selected_filters(
                dict(row, confirmation_timestamp=wrong, v9_1m_feature_ts=wrong), spec)


def test_explicit_legacy_cli_runs_exact_midday_subset_without_reoptimizing(monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(cli, "BACKTEST_ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["g4", "--config", str(study.DEFAULT_CONFIG)])

    def runner(config, output, *, cache):
        called.append((config, output, cache))
        return {key: 0 for key in ("sessions", "added_setups", "total_setups", "baseline_g4",
            "selected_additions", "combined_g4", "incremental_net_pnl_rs", "baseline_trades_changed")}

    def forbidden(*args, **kwargs):
        pytest.fail("The explicit legacy profile must replay the requested subset without a new sweep")

    monkeypatch.setattr(cli, "run_midday_selected", runner)
    monkeypatch.setattr(cli, "run_selected", forbidden)
    monkeypatch.setattr(cli, "run_extended", forbidden)
    monkeypatch.setattr(cli, "run_original", forbidden)
    monkeypatch.setattr(cli, "run_mirror", forbidden)
    cli.main()
    assert len(called) == 1
    assert called[0][0] == study.DEFAULT_CONFIG
    assert called[0][2] == cli.MIDDAY_CACHE
    assert called[0][1].resolve().is_relative_to(tmp_path.resolve())


def write_manifest(folder, *, files):
    folder.mkdir(parents=True)
    (folder / "manifest.json").write_text(json.dumps({
        "variant_id": "TEST", "execution_authority": False, "files": files,
    }), encoding="utf-8")
    return hashlib.sha256((folder / "manifest.json").read_bytes()).hexdigest()


def test_reference_requires_consumed_artifact_to_be_declared_in_pinned_manifest(monkeypatch, tmp_path):
    monkeypatch.setattr(study, "BACKTEST_ROOT", tmp_path)
    monkeypatch.setattr(earlier, "BACKTEST_ROOT", tmp_path)
    folder = tmp_path / "run"
    digest = write_manifest(folder, files={})
    (folder / "orders.csv").write_text("sid\n1\n", encoding="utf-8")
    with pytest.raises(ValueError):
        study.verify_reference("run", digest, "TEST", required_artifacts=("orders.csv",))
    artifact_digest = hashlib.sha256((folder / "orders.csv").read_bytes()).hexdigest()
    (folder / "manifest.json").write_text(json.dumps({
        "variant_id": "TEST", "execution_authority": False, "files": {"orders.csv": artifact_digest},
    }), encoding="utf-8")
    digest = hashlib.sha256((folder / "manifest.json").read_bytes()).hexdigest()
    actual, proof = study.verify_reference("run", digest, "TEST", required_artifacts=("orders.csv",))
    assert actual == folder.resolve()
    assert proof["manifest_sha256"] == digest


def test_reference_cannot_escape_g4_root_even_with_valid_manifest(monkeypatch, tmp_path):
    root = tmp_path / "g4"
    root.mkdir()
    digest = write_manifest(tmp_path / "outside", files={})
    monkeypatch.setattr(study, "BACKTEST_ROOT", root)
    monkeypatch.setattr(earlier, "BACKTEST_ROOT", root)
    with pytest.raises(ValueError):
        study.verify_reference("../outside", digest, "TEST")


def test_reference_rejects_modified_artifact_while_manifest_stays_pinned(monkeypatch, tmp_path):
    monkeypatch.setattr(study, "BACKTEST_ROOT", tmp_path)
    monkeypatch.setattr(earlier, "BACKTEST_ROOT", tmp_path)
    digest = write_manifest(tmp_path / "run", files={"orders.csv": hashlib.sha256(b"original").hexdigest()})
    (tmp_path / "run/orders.csv").write_bytes(b"modified")
    with pytest.raises(ValueError):
        study.verify_reference("run", digest, "TEST", required_artifacts=("orders.csv",))
