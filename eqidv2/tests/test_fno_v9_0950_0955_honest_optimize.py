from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v9_0950_0955_honest_optimize as opt
import fno_v8_windowed_1m_entry_backtest as v8


def _leg(slot: str = "09:50", side: str = "LONG") -> opt.LegConfig:
    return opt.LegConfig(slot, side, "BROAD", "S1_STRICT", "TIGHT")


def _book() -> opt.BookConfig:
    return opt.BookConfig(
        tuple(
            _leg(slot, side)
            for slot in opt.SLOTS
            for side in opt.SIDES
        )
    )


def _metric(
    *,
    fills: int = 50,
    active: int = 20,
    pf: float = 1.6,
    robust: float = 1.2,
    net: float = 5.0,
    top: float = 0.25,
    blocks: int = 3,
    incomplete: int = 0,
    unresolved: int = 0,
) -> dict[str, object]:
    return {
        "closed_fills": fills,
        "active_days": active,
        "profit_factor": pf,
        "robust_profit_factor_ex_best_day": robust,
        "net_return_percentage_points": net,
        "top_day_share": top,
        "positive_contiguous_blocks": blocks,
        "data_incomplete_candidates": incomplete,
        "unresolved_filled_trades": unresolved,
        "trades_per_session": fills / 30,
    }


def _bundle(metric: dict[str, object] | None = None) -> dict[str, object]:
    values = dict(metric or _metric())
    return {
        "combined": dict(values),
        "sides": {side: dict(values) for side in opt.SIDES},
        "legs": {leg: dict(values) for leg in opt.LEG_KEYS},
    }


def test_grid_is_exactly_48_per_independent_leg_and_deterministic() -> None:
    assert opt.LEG_KEYS == (
        "09:50_LONG",
        "09:50_SHORT",
        "09:55_LONG",
        "09:55_SHORT",
    )
    for key in opt.LEG_KEYS:
        grid = opt.LEG_GRIDS[key]
        assert len(grid) == 48
        assert len({value.config_hash for value in grid}) == 48
        assert {value.setup_id for value in grid} == {key}
        assert {value.max_entries for value in grid} == {2}
    assert opt.GRID_FAMILY_SHA256 == opt.common.canonical_json_sha256(
        {
            "schema_version": opt.GRID_SCHEMA_VERSION,
            "legs": {
                key: [value.config_hash for value in opt.LEG_GRIDS[key]]
                for key in opt.LEG_KEYS
            },
        }
    )
    opt.validate_preregistered_contract()


def test_book_accepts_any_nonempty_canonical_subset_of_independent_legs() -> None:
    for keys in (
        ("09:50_LONG",),
        ("09:50_LONG", "09:55_LONG"),
        ("09:50_SHORT", "09:55_LONG", "09:55_SHORT"),
        opt.LEG_KEYS,
    ):
        book = opt.BookConfig(
            tuple(_leg(*key.rsplit("_", 1)) for key in keys)
        )
        book.validate()
        assert tuple(config.setup_id for config in book.legs) == keys
        assert opt.BookConfig.from_payload(book.payload()) == book

    with pytest.raises(ValueError, match="cannot be empty"):
        opt.BookConfig(()).validate()
    with pytest.raises(ValueError, match="canonical"):
        opt.BookConfig((_leg("09:55", "LONG"), _leg("09:50", "LONG"))).validate()
    with pytest.raises(ValueError, match="unique"):
        opt.BookConfig((_leg(), _leg())).validate()


def test_default_split_and_sample_guards_match_exact_eod_contract() -> None:
    contract = opt.default_split_contract()
    sessions = opt.split_sessions(contract)
    assert {key: len(value) for key, value in sessions.items()} == {
        "TRAIN": 30,
        "VALIDATION": 10,
        "TEST": 6,
    }
    assert contract.train_through == date(2026, 7, 9)
    assert contract.validation_through == date(2026, 7, 23)
    assert contract.test_from == date(2026, 7, 24)
    assert contract.test_through == date(2026, 7, 31)
    guards = opt.Guards()
    assert guards.train_min_fills_per_leg == 40
    assert guards.validation_min_fills_per_leg == 15
    assert guards.test_min_fills_per_leg == 10
    assert guards.train_min_active_days == 15
    assert guards.validation_min_active_days == 8
    assert guards.test_min_active_days == 6
    assert guards.train_min_leg_pf == 1.50
    assert guards.train_min_leg_robust_pf == 1.20
    assert guards.train_max_leg_top_day_share == 0.25
    assert guards.validation_min_leg_pf == 1.50
    assert guards.validation_max_leg_top_day_share == 0.35
    assert guards.test_min_leg_pf == 1.50


def test_parser_defaults_full_universe_and_sequential_commands() -> None:
    parser = opt.build_parser()
    args = parser.parse_args(["search", "--source-snapshot", "snapshot.json"])
    assert args.coverage_mode == "full-universe"
    assert not args.allow_diagnostic_research
    assert args.symbols is None
    assert args.train_from == opt.DEFAULT_TRAIN_FROM
    assert args.train_through == opt.DEFAULT_TRAIN_THROUGH
    assert args.validation_from == opt.DEFAULT_VALIDATION_FROM
    assert args.validation_through == opt.DEFAULT_VALIDATION_THROUGH
    assert args.test_from == opt.DEFAULT_TEST_FROM
    assert args.test_through == "2026-07-31"
    assert parser.parse_args(
        ["evaluate-validation", "--search-run", "run"]
    ).command == "evaluate-validation"
    assert parser.parse_args(
        ["evaluate-test", "--validation-run", "run"]
    ).command == "evaluate-test"


@pytest.mark.parametrize(
    "extra",
    [
        ["--coverage-mode", "rectangular-panel"],
        ["--symbols", "ABC"],
        ["--train-from", "2026-05-28"],
    ],
)
def test_any_nonprimary_research_requires_opt_in_before_cache_access(
    monkeypatch, extra: list[str]
) -> None:
    accessed = False

    def forbidden(**kwargs):
        nonlocal accessed
        accessed = True
        raise AssertionError("cache should not be accessed")

    monkeypatch.setattr(opt, "load_or_build_cache", forbidden)
    args = opt.build_parser().parse_args(
        ["search", "--source-snapshot", "snapshot.json", *extra]
    )
    with pytest.raises(opt.DataEligibilityError, match="allow-diagnostic-research"):
        opt.execute_search(args)
    assert not accessed


@pytest.mark.parametrize(
    "extra",
    [
        ["--coverage-mode", "rectangular-panel"],
        ["--symbols", "ABC"],
        ["--train-from", "2026-05-28"],
    ],
)
def test_explicit_diagnostic_opt_in_is_parsed_for_every_nonprimary_mode(
    extra: list[str],
) -> None:
    args = opt.build_parser().parse_args(
        [
            "search",
            "--source-snapshot",
            "snapshot.json",
            "--allow-diagnostic-research",
            *extra,
        ]
    )
    assert args.allow_diagnostic_research


def test_only_exact_default_full_universe_contract_is_primary() -> None:
    default = opt.default_split_contract()
    assert opt.diagnostic_contract_reasons(
        contract=default,
        coverage_mode="full-universe",
        requested_symbols=None,
    ) == []
    assert opt.diagnostic_contract_reasons(
        contract=default,
        coverage_mode="full-universe",
        requested_symbols=["ABC"],
    ) == ["SYMBOL_SUBSET"]
    assert opt.diagnostic_contract_reasons(
        contract=default,
        coverage_mode="rectangular-panel",
        requested_symbols=None,
    ) == ["TRAIN_DERIVED_RECTANGULAR_PANEL"]

    custom = opt.SplitContract(
        date(2026, 5, 28),
        default.train_through,
        default.validation_from,
        default.validation_through,
        default.test_from,
        default.test_through,
    )
    assert opt.diagnostic_contract_reasons(
        contract=custom,
        coverage_mode="full-universe",
        requested_symbols=None,
    ) == ["CUSTOM_SPLIT_CONTRACT"]


def test_required_futures_grid_is_exact_predecessor_plus_signal() -> None:
    assert opt.REQUIRED_FUTURES_TIMES == ("09:45", "09:50", "09:55")
    opt.validate_preregistered_contract()


def test_cache_authority_is_looser_than_every_searched_gate() -> None:
    assert (
        opt.CACHE_FLOOR_PRICE_CHANGE_PCT,
        opt.CACHE_FLOOR_OI_CHANGE_PCT,
        opt.CACHE_FLOOR_VOLUME_RATIO,
    ) == (0.10, 0.05, 0.80)
    assert min(value.price_change_pct for value in opt.GATE_PROFILES) >= 0.20
    assert min(value.oi_change_pct for value in opt.GATE_PROFILES) >= 0.10
    assert min(value.volume_ratio for value in opt.GATE_PROFILES) >= 1.00
    baseline = opt._baseline_setup("09:50", "LONG")
    assert baseline.price_change_pct == 0.10
    assert baseline.oi_change_pct == 0.05
    assert baseline.volume_ratio == 0.80


def test_train_panel_membership_never_uses_later_coverage() -> None:
    train = [date(2026, 7, 8), date(2026, 7, 9)]
    coverage = pd.DataFrame(
        [
            {
                "symbol": "A",
                "source_complete_session_dates_json": json.dumps(
                    ["2026-07-08", "2026-07-09"]
                ),
                "unexpected_session_dates_json": "[]",
            },
            {
                "symbol": "B",
                "source_complete_session_dates_json": json.dumps(
                    ["2026-07-08", "2026-07-09", "2026-07-10"]
                ),
                "unexpected_session_dates_json": "[]",
            },
        ]
    )
    candidates = pd.DataFrame(
        {"candidate_id": ["A", "B"], "symbol": ["A", "B"]}
    )
    paths = pd.DataFrame({"candidate_id": ["A", "B"]})
    _, _, panel_coverage, metadata = opt.derive_train_panel(
        candidates, paths, coverage, train_sessions=train
    )
    assert metadata["panel_symbols"] == ["A", "B"]
    assert metadata["watermark"] == opt.DIAGNOSTIC_WATERMARK
    later = opt.derive_coverage(
        panel_coverage,
        symbols=metadata["panel_symbols"],
        sessions=[date(2026, 7, 10)],
    )
    assert not later["pass"]
    assert later["incomplete_symbols"] == ["A"]


def test_exact_grid_with_unknown_lineage_is_diagnostic_not_qualifying() -> None:
    session = date(2026, 7, 1)
    coverage = pd.DataFrame(
        [
            {
                "symbol": "CERTIFIED",
                "source_complete_session_dates_json": json.dumps(
                    [session.isoformat()]
                ),
                "unexpected_session_dates_json": "[]",
                "legacy_lineage_flags_absent": False,
            },
            {
                "symbol": "UNKNOWN",
                "source_complete_session_dates_json": json.dumps(
                    [session.isoformat()]
                ),
                "unexpected_session_dates_json": "[]",
                "legacy_lineage_flags_absent": True,
            },
        ]
    )
    gate = opt.derive_coverage(
        coverage,
        symbols=["CERTIFIED", "UNKNOWN"],
        sessions=[session],
    )
    assert gate["exact_grid_pass"]
    assert gate["pass"]
    assert not gate["lineage_certified"]
    assert not gate["qualifying_pass"]
    assert gate["legacy_lineage_flags_absent_symbols"] == ["UNKNOWN"]

    certified = opt.derive_coverage(
        coverage,
        symbols=["CERTIFIED"],
        sessions=[session],
    )
    assert certified["qualifying_pass"]
    assert certified["lineage_certified"]


def test_score_active_days_and_positive_gross_concentration() -> None:
    sessions = [date(2026, 7, day) for day in (1, 2, 3)]
    audit = pd.DataFrame(
        [
            {"session_date": sessions[0], "filled": True, "net_return_pct": 1.0, "net_pnl_rs": 1.0},
            {"session_date": sessions[0], "filled": True, "net_return_pct": -1.0, "net_pnl_rs": -1.0},
            {"session_date": sessions[1], "filled": True, "net_return_pct": 3.0, "net_pnl_rs": 3.0},
            {"session_date": sessions[2], "filled": True, "net_return_pct": 1.0, "net_pnl_rs": 1.0},
        ]
    )
    metrics = opt.score_audit(audit, sessions)
    assert metrics["active_days"] == 3
    assert metrics["flat_days"] == 1
    assert metrics["top_day_share"] == pytest.approx(3 / 4)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("closed_fills", 39),
        ("active_days", 14),
        ("profit_factor", 1.499999),
        ("robust_profit_factor_ex_best_day", 1.199999),
        ("top_day_share", 0.250001),
        ("positive_contiguous_blocks", 1),
        ("data_incomplete_candidates", 1),
    ],
)
def test_leg_train_guard_rejects_each_required_leg_weakness(
    field: str, value: object
) -> None:
    base = _metric()
    stress = _metric(pf=1.1, robust=1.1, net=1.0)
    assert opt.leg_train_guard(base, stress, opt.Guards())
    base[field] = value
    assert not opt.leg_train_guard(base, stress, opt.Guards())


def test_leg_frontier_uses_only_passing_behavior_representatives() -> None:
    configs = opt.LEG_GRIDS["09:50_LONG"][:4]
    trials = pd.DataFrame(
        [
            {
                "config_hash": configs[index].config_hash,
                "config": configs[index].payload(),
                "guard_pass": passed,
                "behavior_signature": signature,
                "trades_per_session": 2.0 - index / 10,
                "profit_factor": 1.5,
                "robust_pf": 1.2,
                "stress_pf": 1.1,
                "complexity": index,
            }
            for index, (passed, signature) in enumerate(
                ((True, "A"), (True, "A"), (False, "B"), (True, "C"))
            )
        ]
    )
    selected, ranked = opt.select_leg_frontier(trials, top_n=2)
    assert [value.config_hash for value in selected] == [
        configs[0].config_hash,
        configs[3].config_hash,
    ]
    alias = ranked.loc[ranked["config_hash"].eq(configs[1].config_hash)].iloc[0]
    assert alias["behavior_alias_of"] == configs[0].config_hash
    trials["guard_pass"] = False
    assert opt.select_leg_frontier(trials, top_n=2)[0] == []


def test_train_selects_each_leg_independently_and_disables_only_empty_legs() -> None:
    frontiers = {
        "09:50_LONG": list(opt.LEG_GRIDS["09:50_LONG"][:2]),
        "09:50_SHORT": [],
        "09:55_LONG": list(opt.LEG_GRIDS["09:55_LONG"][:1]),
        "09:55_SHORT": list(opt.LEG_GRIDS["09:55_SHORT"][:2]),
    }
    winners, disabled = opt.select_independent_leg_winners(frontiers)

    assert tuple(winners) == (
        "09:50_LONG",
        "09:55_LONG",
        "09:55_SHORT",
    )
    assert winners["09:50_LONG"] == frontiers["09:50_LONG"][0]
    assert winners["09:55_LONG"] == frontiers["09:55_LONG"][0]
    assert winners["09:55_SHORT"] == frontiers["09:55_SHORT"][0]
    assert set(disabled) == {"09:50_SHORT"}
    assert disabled["09:50_SHORT"] == {
        "stage": "TRAIN",
        "reason": "NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS",
        "permanently_disabled_for_run": True,
    }

    subset = opt.BookConfig(tuple(winners.values()))
    subset.validate()
    assert tuple(config.setup_id for config in subset.legs) == tuple(winners)


def test_no_train_leg_winner_fails_closed_without_constructing_empty_book() -> None:
    winners, disabled = opt.select_independent_leg_winners(
        {key: [] for key in opt.LEG_KEYS}
    )
    assert winners == {}
    assert tuple(disabled) == opt.LEG_KEYS
    with pytest.raises(ValueError, match="cannot be empty"):
        opt.BookConfig(tuple(winners.values())).validate()


def test_train_leg_guard_accepts_exact_pf_robust_and_concentration_boundaries() -> None:
    base = _metric(pf=1.50, robust=1.20, top=0.25)
    stress = _metric(pf=1.00, robust=1.20, net=1.0, top=0.25)
    assert opt.leg_train_guard(base, stress, opt.Guards())
    stress["profit_factor"] = 0.999999
    assert not opt.leg_train_guard(base, stress, opt.Guards())


def test_later_stage_guard_is_a_single_leg_decision() -> None:
    guards = opt.Guards()
    validation_base = _metric(fills=15, active=8, pf=1.50, top=0.35)
    validation_stress = _metric(fills=15, active=8, pf=1.00, net=1.0)
    assert opt.leg_stage_guard(
        validation_base,
        validation_stress,
        stage="VALIDATION",
        guards=guards,
    )["pass"]
    validation_base["closed_fills"] = 14
    assert not opt.leg_stage_guard(
        validation_base,
        validation_stress,
        stage="VALIDATION",
        guards=guards,
    )["pass"]

    test_base = _metric(fills=10, active=6, pf=1.50, top=0.45)
    test_stress = _metric(fills=10, active=6, pf=1.00, net=1.0)
    assert opt.leg_stage_guard(
        test_base, test_stress, stage="TEST", guards=guards
    )["pass"]
    test_base["active_days"] = 5
    assert not opt.leg_stage_guard(
        test_base, test_stress, stage="TEST", guards=guards
    )["pass"]


def test_validation_and_test_advance_only_passing_leg_subsets(monkeypatch) -> None:
    failing_leg = "09:50_SHORT"
    failing_fills = 14

    def fake_run_leg(
        prepared: opt.PreparedDataset,
        config: opt.LegConfig,
        *,
        stress: bool,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {"leg_key": [config.setup_id], "stress": [stress]}
        )

    def fake_score_audit(
        audit: pd.DataFrame, sessions: object
    ) -> dict[str, object]:
        leg = str(audit.iloc[0]["leg_key"])
        stress = bool(audit.iloc[0]["stress"])
        fills = failing_fills if leg == failing_leg else 15
        return _metric(
            fills=fills,
            active=8,
            pf=1.10 if stress else 1.60,
            net=1.0,
            top=0.20,
        )

    def fake_constrain(parts: object) -> pd.DataFrame:
        frames = list(parts)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # This intentionally terrible pooled diagnostic must not disable any leg.
    pooled = _bundle(
        _metric(fills=1, active=1, pf=0.10, robust=0.10, net=-10.0, top=1.0, blocks=0)
    )
    monkeypatch.setattr(opt, "run_leg_preportfolio", fake_run_leg)
    monkeypatch.setattr(opt, "score_audit", fake_score_audit)
    monkeypatch.setattr(opt, "constrain_book", fake_constrain)
    monkeypatch.setattr(opt, "score_book", lambda *args, **kwargs: pooled)

    prepared = opt.PreparedDataset(pd.DataFrame(), {}, (date(2026, 7, 10),))
    validation = opt._evaluate_independent_legs(
        prepared,
        _book(),
        stage="VALIDATION",
        guards=opt.Guards(),
    )
    assert tuple(validation.leg_results) == opt.LEG_KEYS
    assert tuple(config.setup_id for config in validation.advancing_book.legs) == (
        "09:50_LONG",
        "09:55_LONG",
        "09:55_SHORT",
    )
    assert set(validation.disabled_legs) == {"09:50_SHORT"}
    assert validation.disabled_legs["09:50_SHORT"][
        "permanently_disabled_for_run"
    ]
    assert not validation.leg_results["09:50_SHORT"]["passed"]
    assert validation.leg_results["09:50_SHORT"]["claimed_profit_factor"] is None
    assert validation.leg_results["09:50_LONG"]["pf_claim_eligible"]
    assert validation.leg_results["09:50_LONG"]["claimed_profit_factor"] == 1.6
    assert validation.portfolio_base_metrics["combined"]["profit_factor"] == 0.1

    # TEST receives exactly the surviving subset, then may shrink it again.
    failing_leg = "09:55_LONG"
    failing_fills = 9
    test = opt._evaluate_independent_legs(
        prepared,
        validation.advancing_book,
        stage="TEST",
        guards=opt.Guards(),
    )
    assert tuple(test.leg_results) == (
        "09:50_LONG",
        "09:55_LONG",
        "09:55_SHORT",
    )
    assert tuple(config.setup_id for config in test.advancing_book.legs) == (
        "09:50_LONG",
        "09:55_SHORT",
    )
    assert "09:50_SHORT" not in test.leg_results
    assert set(test.disabled_legs) == {"09:55_LONG"}


def test_leg_guard_breakdown_reports_every_leg_and_exact_thresholds() -> None:
    base = _bundle(_metric(fills=15, active=8, pf=1.5, top=0.35))
    stress = _bundle(_metric(fills=15, active=8, pf=1.0, net=1.0))
    breakdown = opt.leg_guard_breakdown(
        base, stress, stage="VALIDATION", guards=opt.Guards()
    )
    assert tuple(breakdown) == opt.LEG_KEYS
    assert all(value["pass"] for value in breakdown.values())
    assert breakdown["09:50_LONG"]["thresholds"] == {
        "min_fills": 15,
        "min_active_days": 8,
        "min_profit_factor": 1.5,
        "min_robust_profit_factor": None,
        "max_top_day_share": 0.35,
        "min_stress_profit_factor": 1.0,
        "min_positive_blocks": 2,
    }


def test_run_leg_uses_v8_same_session_state_machine_and_realistic_costs() -> None:
    config = _leg()
    signal = pd.Timestamp("2026-07-01 09:50", tz=opt.common.IST)
    candidate_id = "2026-07-01|09:50_LONG|ABC"
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "session_date": signal.date(),
                "signal_time": signal,
                "setup_id": "09:50_LONG",
                "side": "LONG",
                "symbol": "ABC",
                "futures_symbol": "ABCFUT",
                "equity_instrument_token": 1,
                "futures_instrument_token": 2,
                "tick_size": 0.05,
                "lot_size": 1,
                "five_min_open": 99.0,
                "five_min_high": 100.2,
                "five_min_low": 98.8,
                "five_min_close": 100.0,
                "five_min_volume": 1000.0,
                "ema9": 101.0,
                "ema20": 100.0,
                "ema50": 99.0,
                "price_change_pct": 0.5,
                "oi": 110.0,
                "prev_oi": 100.0,
                "oi_change_pct": 10.0,
                "volume_ratio": 2.0,
                "traded_value": 50_000_000.0,
            }
        ]
    )
    bars = tuple(
        v8.MinuteBar(
            signal + pd.Timedelta(minutes=minute),
            *ohlc,
            volume=100.0,
        )
        for minute, ohlc in (
            (1, (100.0, 101.1, 99.9, 101.0)),
            (2, (101.15, 101.3, 101.0, 101.2)),
            (3, (101.2, 103.0, 101.1, 102.8)),
        )
    )
    prepared = opt.PreparedDataset(candidates, {candidate_id: bars}, (signal.date(),))
    audit = opt.run_leg_preportfolio(prepared, config, stress=False)
    assert len(audit) == 1
    row = audit.iloc[0]
    assert row["status"] == v8.SignalState.TARGETED.value
    assert bool(row["filled"])
    assert row["cost_bps"] == 15.0
    assert row["slippage_bps"] == 1.0
    assert np.isfinite(row["net_pnl_rs"])


def test_global_portfolio_ledger_is_called_once_for_combined_book(monkeypatch) -> None:
    calls = 0

    def fake(frame: pd.DataFrame, policy: v8.PortfolioPolicy) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return frame

    monkeypatch.setattr(v8, "apply_global_portfolio_constraints", fake)
    frame = pd.DataFrame(
        {
            "session_date": [date(2026, 7, 1)],
            "signal_time": [pd.Timestamp("2026-07-01 09:50", tz=opt.common.IST)],
            "side": ["LONG"],
            "frozen_rank": [1],
            "symbol": ["ABC"],
        }
    )
    result = opt.constrain_book([frame])
    assert len(result) == 1
    assert calls == 1


def test_selection_authentication_rejects_same_file_tamper(tmp_path: Path) -> None:
    book = _book()
    selection = {
        "run_fingerprint": "a" * 64,
        "selected_book": book.payload(),
        "selected_leg_keys": list(opt.LEG_KEYS),
        "selected_legs": {
            config.setup_id: config.payload() for config in book.legs
        },
    }
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(selection) + "\n", encoding="utf-8")
    frozen = {
        "run_fingerprint": selection["run_fingerprint"],
        "results": {
            "selected_book_hash": book.config_hash,
            "selected_leg_keys": list(opt.LEG_KEYS),
        },
        "outputs": {"selection": opt.provenance.artifact_record(selection_path)},
    }
    (tmp_path / "provenance.json").write_text(
        json.dumps(frozen) + "\n", encoding="utf-8"
    )
    assert opt.load_authenticated_selection(tmp_path) == selection
    selection_path.write_text(json.dumps({**selection, "tampered": True}) + "\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="selection"):
        opt.load_authenticated_selection(tmp_path)


def test_stage_claim_is_create_once_in_local_and_global_registry(
    tmp_path: Path, monkeypatch
) -> None:
    search_run = tmp_path / "search"
    search_run.mkdir()
    monkeypatch.setattr(opt, "CLAIM_REGISTRY_ROOT", tmp_path / "registry")
    local, registry, payload = opt._claim_once(
        search_run=search_run,
        search_run_fingerprint="a" * 64,
        selection_sha256="b" * 64,
        stage="VALIDATION",
        evaluation_id="validation-fixed-id",
        input_book_hash="c" * 64,
        prior_stage_binding=None,
    )
    original = local.read_bytes()
    assert registry.read_bytes() == original
    assert payload["stage"] == "VALIDATION"
    assert payload["evaluation_id"] == "validation-fixed-id"
    assert payload["policy_evaluation_count"] == 1
    assert payload["claim_id"]

    with pytest.raises(opt.StageAccessError, match="already claimed"):
        opt._claim_once(
            search_run=search_run,
            search_run_fingerprint="a" * 64,
            selection_sha256="b" * 64,
            stage="VALIDATION",
            evaluation_id="attempted-overwrite",
            input_book_hash="c" * 64,
            prior_stage_binding=None,
        )
    assert local.read_bytes() == original
    assert registry.read_bytes() == original

    # Removing one claim copy cannot reopen the stage because the independent
    # registry is still an immutable consumption record.
    local.unlink()
    with pytest.raises(opt.StageAccessError, match="already claimed"):
        opt._claim_once(
            search_run=search_run,
            search_run_fingerprint="a" * 64,
            selection_sha256="b" * 64,
            stage="VALIDATION",
            evaluation_id="replay-after-local-delete",
            input_book_hash="c" * 64,
            prior_stage_binding=None,
        )
    assert registry.read_bytes() == original

    # Even loss of both claim copies fails closed once an evaluation directory
    # exists; incomplete/corrupt evaluations consume the one-shot policy too.
    registry.unlink()
    (search_run / "validation_incomplete_evidence").mkdir()
    with pytest.raises(opt.StageAccessError, match="already claimed"):
        opt._claim_once(
            search_run=search_run,
            search_run_fingerprint="a" * 64,
            selection_sha256="b" * 64,
            stage="VALIDATION",
            evaluation_id="replay-after-both-delete",
            input_book_hash="c" * 64,
            prior_stage_binding=None,
        )


def test_stage_authentication_detects_local_or_global_claim_tamper(
    tmp_path: Path, monkeypatch
) -> None:
    search_run = tmp_path / "search"
    search_run.mkdir()
    monkeypatch.setattr(opt, "CLAIM_REGISTRY_ROOT", tmp_path / "registry")
    evaluation_id = "validation_fixed"
    local, registry, claim = opt._claim_once(
        search_run=search_run,
        search_run_fingerprint="d" * 64,
        selection_sha256="e" * 64,
        stage="VALIDATION",
        evaluation_id=evaluation_id,
        input_book_hash="f" * 64,
        prior_stage_binding=None,
    )
    validation_run = search_run / evaluation_id
    validation_run.mkdir()
    result = {
        "run_fingerprint": "1" * 64,
        "claim_id": claim["claim_id"],
        "evaluation_id": evaluation_id,
        "stage": "VALIDATION",
        "status": "VALIDATION_NO_QUALIFYING_LEGS",
        "search_run_dir": str(search_run.resolve()),
        "search_run_fingerprint": "d" * 64,
        "selection_sha256": "e" * 64,
        "book_hash": "f" * 64,
        "advancing_book_hash": None,
        "advancing_leg_keys": [],
        "disabled_legs": {},
        "claimed_leg_profit_factors": {},
        "eligible_for_test": False,
        "prior_stage_binding": {},
    }
    result_path = validation_run / "result.json"
    result_path.write_text(json.dumps(result) + "\n", encoding="utf-8")
    frozen = {
        "run_fingerprint": result["run_fingerprint"],
        "stage_claim_binding": claim,
        "results": {
            key: result[key]
            for key in (
                "status",
                "book_hash",
                "advancing_book_hash",
                "advancing_leg_keys",
                "disabled_legs",
                "claimed_leg_profit_factors",
                "eligible_for_test",
            )
        },
        "outputs": {
            "result": opt.provenance.artifact_record(result_path),
            "claim": opt.provenance.artifact_record(local),
            "stage_registry": opt.provenance.artifact_record(registry),
        },
    }
    (validation_run / "provenance.json").write_text(
        json.dumps(frozen) + "\n", encoding="utf-8"
    )
    assert opt.load_authenticated_stage_result(validation_run) == result

    registry_original = registry.read_bytes()
    registry.write_bytes(registry_original + b"tamper")
    with pytest.raises(AssertionError, match="stage_registry"):
        opt.load_authenticated_stage_result(validation_run)
    registry.write_bytes(registry_original)

    local.write_bytes(local.read_bytes() + b"tamper")
    with pytest.raises(AssertionError, match="claim"):
        opt.load_authenticated_stage_result(validation_run)


def test_frozen_cache_hash_and_size_are_rechecked_before_later_stage(
    tmp_path: Path,
) -> None:
    cache_paths = {}
    for name, value in (
        ("candidates", b"candidate-cache"),
        ("paths", b"minute-path-cache"),
        ("coverage", b"coverage-cache"),
    ):
        path = tmp_path / f"{name}.bin"
        path.write_bytes(value)
        cache_paths[name] = path
    manifest = {"input_fingerprint": "cache-fingerprint"}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    selection = {
        "optimizer_source_sha256": opt.provenance.sha256_file(Path(opt.__file__)),
        "v8_source_sha256": opt.provenance.sha256_file(Path(v8.__file__)),
        "cache_manifest_path": str(manifest_path),
        "cache_manifest_size": manifest_path.stat().st_size,
        "cache_manifest_sha256": opt.provenance.sha256_file(manifest_path),
        "cache_input_fingerprint": "cache-fingerprint",
    }
    for name, path in cache_paths.items():
        selection[f"{name}_cache_path"] = str(path)
        selection[f"{name}_cache_size"] = path.stat().st_size
        selection[f"{name}_cache_sha256"] = opt.provenance.sha256_file(path)
    observed, paths = opt._verify_frozen_inputs(selection)
    assert observed == manifest
    assert paths["coverage"] == cache_paths["coverage"].resolve()

    tampered = bytearray(cache_paths["paths"].read_bytes())
    tampered[0] ^= 0x01
    cache_paths["paths"].write_bytes(tampered)
    with pytest.raises(AssertionError, match="paths cache changed"):
        opt._verify_frozen_inputs(selection)


def test_stage_report_claims_pf_per_passing_leg_only() -> None:
    result = {
        "stage": "VALIDATION",
        "status": "VALIDATION_INDEPENDENT_LEG_SUBSET_ADVANCED",
        "book_hash": "BOOK",
        "input_leg_keys": ["09:50_LONG", "09:50_SHORT"],
        "advancing_leg_keys": ["09:50_LONG"],
        "pf_claim_eligible": False,
        "claimed_profit_factor": None,
        "leg_results": {
            "09:50_LONG": {
                "passed": True,
                "pf_claim_eligible": True,
                "claimed_profit_factor": 1.6,
                "guard": {
                    "observed": {
                        "closed_fills": 15,
                        "active_days": 8,
                        "robust_profit_factor_ex_best_day": 1.2,
                        "stress_profit_factor": 1.1,
                    },
                    "checks": {"profit_factor": True},
                },
            },
            "09:50_SHORT": {
                "passed": False,
                "pf_claim_eligible": False,
                "claimed_profit_factor": None,
                "base_metrics": _metric(pf=9.87654321),
                "guard": {
                    "observed": {
                        "closed_fills": 14,
                        "active_days": 8,
                        "profit_factor": 9.87654321,
                    },
                    "checks": {"fills": False},
                },
            },
        },
        "portfolio_diagnostic": {
            "cannot_qualify_or_disqualify_any_leg": True,
            "base_metrics": _bundle(_metric(pf=8.7654321)),
        },
    }
    report = opt._stage_report(result)
    assert "Pooled portfolio PF claim eligible: False" in report
    assert "eligible_VALIDATION_PF=1.6" in report
    assert "PF=WITHHELD_INELIGIBLE_LEG" in report
    assert "9.87654321" not in report
    assert "8.7654321" not in report


def test_validation_is_locked_when_train_has_no_eligible_winner(
    tmp_path: Path,
) -> None:
    selection = {
        "run_fingerprint": "b" * 64,
        "selected_book": None,
        "eligible_for_validation": False,
        "diagnostic_only": False,
    }
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(selection) + "\n", encoding="utf-8")
    frozen = {
        "run_fingerprint": selection["run_fingerprint"],
        "results": {"selected_book_hash": None},
        "outputs": {"selection": opt.provenance.artifact_record(selection_path)},
    }
    (tmp_path / "provenance.json").write_text(json.dumps(frozen) + "\n", encoding="utf-8")
    args = argparse.Namespace(search_run=str(tmp_path))
    with pytest.raises(opt.StageAccessError, match="VALIDATION remains locked"):
        opt.execute_validation(args)


@pytest.mark.parametrize(
    ("selection_updates", "message"),
    [
        ({"diagnostic_only": True, "lineage_certified": True}, "diagnostic research"),
        (
            {"diagnostic_only": False, "lineage_certified": False},
            "uncertified historical lineage",
        ),
    ],
)
def test_diagnostic_or_lineage_unknown_train_selection_cannot_reach_validation(
    tmp_path: Path,
    monkeypatch,
    selection_updates: dict[str, bool],
    message: str,
) -> None:
    selection = {
        "eligible_for_validation": True,
        "selected_book": _book().payload(),
        **selection_updates,
    }
    monkeypatch.setattr(opt, "load_authenticated_selection", lambda path: selection)
    monkeypatch.setattr(
        opt,
        "_evaluate_frozen_stage",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("locked selection must not evaluate outcomes")
        ),
    )
    with pytest.raises(opt.StageAccessError, match=message):
        opt.execute_validation(argparse.Namespace(search_run=str(tmp_path)))


def test_test_is_locked_when_authenticated_validation_is_ineligible(
    tmp_path: Path, monkeypatch
) -> None:
    result = {
        "run_fingerprint": "c" * 64,
        "stage": "VALIDATION",
        "eligible_for_test": False,
    }
    monkeypatch.setattr(opt, "load_authenticated_stage_result", lambda path: result)
    with pytest.raises(opt.StageAccessError, match="TEST remains locked"):
        opt.execute_test(argparse.Namespace(validation_run=str(tmp_path)))


def test_test_consumes_only_validation_advancing_subset_and_binds_prior_run(
    tmp_path: Path, monkeypatch
) -> None:
    search_run = tmp_path / "search"
    validation_run = search_run / "validation_fixed"
    validation_run.mkdir(parents=True)
    train_book = _book()
    advancing_book = opt.BookConfig(
        (_leg("09:50", "LONG"), _leg("09:55", "SHORT"))
    )
    validation = {
        "stage": "VALIDATION",
        "eligible_for_test": True,
        "search_run_dir": str(search_run.resolve()),
        "search_run_fingerprint": "2" * 64,
        "book_hash": train_book.config_hash,
        "advancing_book": advancing_book.payload(),
        "advancing_book_hash": advancing_book.config_hash,
        "advancing_leg_keys": ["09:50_LONG", "09:55_SHORT"],
        "disabled_legs": {
            "09:50_SHORT": {"stage": "VALIDATION", "reason": "failed"}
        },
        "evaluation_id": "validation_fixed",
        "claim_id": "3" * 64,
        "run_fingerprint": "4" * 64,
    }
    result_path = validation_run / "result.json"
    result_path.write_text(json.dumps(validation) + "\n", encoding="utf-8")
    validation_provenance = {
        "search_run_fingerprint": validation["search_run_fingerprint"],
        "results": {
            "book_hash": validation["book_hash"],
            "advancing_book_hash": validation["advancing_book_hash"],
            "advancing_leg_keys": validation["advancing_leg_keys"],
            "eligible_for_test": True,
        },
    }
    provenance_path = validation_run / "provenance.json"
    provenance_path.write_text(
        json.dumps(validation_provenance) + "\n", encoding="utf-8"
    )
    selection = {
        "run_fingerprint": validation["search_run_fingerprint"],
        "selected_book": train_book.payload(),
    }
    monkeypatch.setattr(
        opt, "load_authenticated_stage_result", lambda path: validation
    )
    monkeypatch.setattr(opt, "load_authenticated_selection", lambda path: selection)
    captured: dict[str, object] = {}

    def fake_evaluate(**kwargs):
        captured.update(kwargs)
        return tmp_path / "test-output"

    monkeypatch.setattr(opt, "_evaluate_frozen_stage", fake_evaluate)
    output = opt.execute_test(
        argparse.Namespace(validation_run=str(validation_run))
    )
    assert output == tmp_path / "test-output"
    test_input = captured["input_book"]
    assert isinstance(test_input, opt.BookConfig)
    assert tuple(config.setup_id for config in test_input.legs) == (
        "09:50_LONG",
        "09:55_SHORT",
    )
    assert captured["inherited_disabled_legs"] == validation["disabled_legs"]
    prior = captured["prior_stage_binding"]
    assert isinstance(prior, dict)
    assert prior["run_dir"] == str(validation_run.resolve())
    assert prior["advancing_book_hash"] == advancing_book.config_hash
    assert prior["advancing_leg_keys"] == ["09:50_LONG", "09:55_SHORT"]
    assert prior["result_sha256"] == opt.provenance.sha256_file(result_path)
    assert prior["provenance_sha256"] == opt.provenance.sha256_file(
        provenance_path
    )
