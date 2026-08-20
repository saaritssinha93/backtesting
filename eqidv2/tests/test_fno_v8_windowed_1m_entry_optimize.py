from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import fno_oi_common as common
import fno_v8_windowed_1m_entry_backtest as v8
import fno_v8_windowed_1m_entry_optimize as opt


def _b0_config() -> opt.SideEntryConfig:
    return opt.SideEntryConfig(1, 0.0, False, None, "STRICT")


def _candidate_frame() -> pd.DataFrame:
    signal = pd.Timestamp("2026-07-13 09:25", tz=common.IST)
    return pd.DataFrame(
        [
            {
                "candidate_id": "2026-07-13|09:25_LONG|TEST",
                "session_date": date(2026, 7, 13),
                "signal_time": signal,
                "signal_end": "09:25",
                "setup_id": "09:25_LONG",
                "side": "LONG",
                "symbol": "TEST",
                "futures_symbol": "TEST26AUGFUT",
                "equity_instrument_token": 1,
                "futures_instrument_token": 2,
                "tick_size": 0.05,
                "lot_size": 1,
                "five_min_open": 99.5,
                "five_min_high": 100.2,
                "five_min_low": 99.4,
                "five_min_close": 100.0,
                "five_min_volume": 100_000.0,
                "ema9": 100.0,
                "ema20": 99.0,
                "ema50": 98.0,
                "price_change_pct": 0.50,
                "oi": 120.0,
                "prev_oi": 100.0,
                "oi_change_pct": 20.0,
                "volume_ratio": 4.0,
                "traded_value": 10_000_000.0,
                "picker": "max_liquidity",
                "picker_value": 10_000_000.0,
                "frozen_rank": 1,
                "schema_version": v8.CACHE_SCHEMA_VERSION,
            }
        ]
    )


def _path_frame() -> pd.DataFrame:
    signal = pd.Timestamp("2026-07-13 09:25", tz=common.IST)
    rows = [
        (1, 100.0, 101.0, 100.0, 100.9),
        (2, 101.0, 104.5, 101.0, 104.0),
    ]
    return pd.DataFrame(
        [
            {
                "candidate_id": "2026-07-13|09:25_LONG|TEST",
                "session_date": date(2026, 7, 13),
                "signal_time": signal,
                "setup_id": "09:25_LONG",
                "side": "LONG",
                "symbol": "TEST",
                "bar_ts": signal + pd.Timedelta(minutes=minute),
                "minute_index": minute,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": 10_000.0,
                "gap_filled": False,
                "opening_snapshot": False,
                "provisional_stale": False,
                "legacy_lineage_flags_absent": False,
                "path_policy_version": v8.PATH_POLICY_VERSION,
            }
            for minute, opening, high, low, close in rows
        ]
    )


def _side_trial_row(
    config: opt.SideEntryConfig,
    *,
    behavior: str,
    guard_pass: bool,
    trades_per_session: float,
) -> dict[str, object]:
    return {
        "side": "LONG",
        "config_hash": config.config_hash,
        "config": config.payload(),
        "complexity": config.complexity,
        "behavior_signature": behavior,
        "guard_pass": guard_pass,
        "trades_per_session": trades_per_session,
        "profit_factor": 2.0,
        "robust_profit_factor_ex_best_day": 1.2,
    }


def _pair_metric(
    *,
    closed_fills: int,
    profit_factor: float = 1.75,
    net: float = 5.0,
) -> dict[str, object]:
    return {
        "closed_fills": closed_fills,
        "profit_factor": profit_factor,
        "robust_profit_factor_ex_best_day": 1.10,
        "net_return_percentage_points": net,
        "top_day_share": 0.40,
        "positive_contiguous_blocks": 2,
        "data_incomplete_candidates": 0,
        "unresolved_filled_trades": 0,
    }


def _qualifying_pair_inputs() -> tuple[dict, dict, dict, dict]:
    train = {
        "combined": _pair_metric(closed_fills=40),
        "sides": {
            "LONG": _pair_metric(closed_fills=20, profit_factor=1.20),
            "SHORT": _pair_metric(closed_fills=20, profit_factor=1.20),
        },
    }
    validation = {
        "combined": _pair_metric(closed_fills=16, profit_factor=1.60),
        "sides": {
            "LONG": _pair_metric(closed_fills=8, profit_factor=1.10),
            "SHORT": _pair_metric(closed_fills=8, profit_factor=1.10),
        },
    }
    stress_train = {
        "combined": _pair_metric(closed_fills=40, profit_factor=1.05, net=1.0),
        "sides": {
            "LONG": _pair_metric(closed_fills=20, profit_factor=1.05, net=0.5),
            "SHORT": _pair_metric(closed_fills=20, profit_factor=1.05, net=0.5),
        },
    }
    stress_validation = {
        "combined": _pair_metric(closed_fills=16, profit_factor=1.05, net=1.0),
        "sides": {
            "LONG": _pair_metric(closed_fills=8, profit_factor=1.05, net=0.5),
            "SHORT": _pair_metric(closed_fills=8, profit_factor=1.05, net=0.5),
        },
    }
    return train, validation, stress_train, stress_validation


def test_grid_is_exactly_192_unique_deterministic_configs_per_side() -> None:
    first = opt.generate_side_grid()
    second = opt.generate_side_grid()
    assert len(first) == 192
    assert [item.config_hash for item in first] == [
        item.config_hash for item in second
    ]
    assert len({item.config_hash for item in first}) == 192


def test_grid_contains_exact_b0_through_b5_policy_points() -> None:
    observed = {
        (
            item.max_confirmation_minute,
            item.buffer_bps,
            item.midpoint_invalidation,
            item.close_location_min,
            item.morphology,
        )
        for item in opt.SIDE_GRID
    }
    expected = {
        (1, 0.0, False, None, "STRICT"),
        (4, 0.0, False, None, "STRICT"),
        (4, 2.0, False, None, "STRICT"),
        (4, 5.0, False, None, "STRICT"),
        (4, 2.0, True, None, "STRICT"),
        (4, 2.0, True, 0.75, "STRICT"),
    }
    assert expected.issubset(observed)


def test_default_split_is_disjoint_and_avoids_august_missing_close() -> None:
    split = opt.default_split_contract()
    split.validate()
    assert split.payload() == {
        "train_from": "2026-07-13",
        "train_through": "2026-07-22",
        "validation_from": "2026-07-23",
        "validation_through": "2026-07-27",
        "test_from": "2026-07-28",
        "test_through": "2026-07-31",
    }
    assert split.validation_from > split.train_through
    assert split.test_from > split.validation_through


def test_parser_defaults_to_full_universe_and_diagnostics_require_opt_in() -> None:
    args = opt.build_parser().parse_args(
        ["search", "--source-snapshot", "frozen-snapshot.json"]
    )
    assert args.coverage_mode == "full-universe"
    assert not args.allow_conditional_diagnostic

    for mode in ("RECTANGULAR_PANEL", "CONDITIONAL_STREAM"):
        with pytest.raises(opt.DataEligibilityError, match="allow-conditional-diagnostic"):
            opt._require_diagnostic_opt_in(mode, False)
        opt._require_diagnostic_opt_in(mode, True)
    opt._require_diagnostic_opt_in("FULL_UNIVERSE", False)


def test_selection_guard_defaults_are_preregistered_for_short_split() -> None:
    assert opt.SelectionGuards() == opt.SelectionGuards(
        min_side_train_fills=15,
        min_side_active_days=4,
        min_side_train_pf=1.10,
        min_side_robust_pf=1.0,
        min_pair_train_fills=40,
        min_pair_train_pf=1.50,
        min_validation_fills=16,
        min_validation_pf=1.50,
        max_top_day_share=0.50,
    )


def test_morphology_uses_setup_copy_and_never_mutates_frozen_book() -> None:
    setup = next(item for item in v8.ACTIVE_SETUPS if item.setup_id == "09:25_LONG")
    before = common.canonical_json_sha256([v8.asdict(item) for item in v8.ACTIVE_SETUPS])
    relaxed = opt._configured_setup(
        setup, opt.SideEntryConfig(4, 2.0, True, None, "RELAXED")
    )
    direction_only = opt._configured_setup(
        setup, opt.SideEntryConfig(4, 2.0, True, None, "DIRECTIONAL_ONLY")
    )
    assert relaxed.body_ratio == pytest.approx(setup.body_ratio * 0.5)
    assert relaxed.max_wick_ratio == pytest.approx(setup.max_wick_ratio + 0.2)
    assert direction_only.body_ratio == 0.0
    assert direction_only.max_wick_ratio == 1.0
    after = common.canonical_json_sha256([v8.asdict(item) for item in v8.ACTIVE_SETUPS])
    assert after == before


def test_side_preportfolio_reuses_v8_state_machine_and_economics() -> None:
    candidates = _candidate_frame()
    paths = _path_frame()
    prepared = opt.prepare_side_dataset(
        candidates, paths, side="LONG", session_dates=[date(2026, 7, 13)]
    )
    audit = opt.run_side_preportfolio(prepared, _b0_config())
    assert len(audit) == 1
    row = audit.iloc[0]
    assert bool(row["filled"])
    assert row["status"] == v8.SignalState.TARGETED.value
    assert int(row["confirmation_minute"]) == 1
    assert int(row["entry_minute"]) == 2
    assert float(row["net_return_pct"]) > 0
    assert float(row["net_pnl_rs"]) > 0


def test_rectangular_panel_filters_by_coverage_and_recomputes_rank() -> None:
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "A",
                "session_date": date(2026, 7, 13),
                "setup_id": "09:25_LONG",
                "symbol": "A",
                "picker_value": 20.0,
                "traded_value": 20.0,
                "frozen_rank": 1,
            },
            {
                "candidate_id": "B",
                "session_date": date(2026, 7, 13),
                "setup_id": "09:25_LONG",
                "symbol": "B",
                "picker_value": 10.0,
                "traded_value": 10.0,
                "frozen_rank": 2,
            },
        ]
    )
    paths = pd.DataFrame([{"candidate_id": "A"}, {"candidate_id": "B"}])
    coverage = pd.DataFrame(
        [
            {
                "symbol": "A",
                "source_complete_session_dates_json": json.dumps(["2026-07-13"]),
                "unexpected_session_count": 0,
            },
            {
                "symbol": "B",
                "source_complete_session_dates_json": json.dumps(
                    ["2026-07-13", "2026-07-14"]
                ),
                "unexpected_session_count": 0,
            },
        ]
    )
    panel_candidates, panel_paths, _, metadata = opt.derive_rectangular_panel(
        candidates,
        paths,
        coverage,
        session_dates=[date(2026, 7, 13), date(2026, 7, 14)],
    )
    assert panel_candidates["symbol"].tolist() == ["B"]
    assert panel_candidates["frozen_rank"].tolist() == [1]
    assert panel_paths["candidate_id"].tolist() == ["B"]
    assert metadata["panel_symbol_count"] == 1
    assert metadata["headline_source_complete"]


def test_panel_membership_is_train_only_and_later_coverage_is_a_gate() -> None:
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": symbol,
                "session_date": date(2026, 7, 13),
                "setup_id": "09:25_LONG",
                "symbol": symbol,
                "picker_value": picker,
                "traded_value": picker,
                "frozen_rank": rank,
            }
            for rank, (symbol, picker) in enumerate(
                (("A", 20.0), ("B", 10.0)), start=1
            )
        ]
    )
    paths = pd.DataFrame([{"candidate_id": "A"}, {"candidate_id": "B"}])
    coverage = pd.DataFrame(
        [
            {
                "symbol": "A",
                "source_complete_session_dates_json": json.dumps(["2026-07-13"]),
                "unexpected_session_count": 0,
            },
            {
                "symbol": "B",
                "source_complete_session_dates_json": json.dumps(
                    ["2026-07-13", "2026-07-23"]
                ),
                "unexpected_session_count": 0,
            },
        ]
    )

    _, _, _, metadata = opt.derive_rectangular_panel(
        candidates,
        paths,
        coverage,
        session_dates=[date(2026, 7, 13)],
    )

    assert metadata["panel_symbols"] == ["A", "B"]
    assert metadata["panel_derivation_split"] == "TRAIN_ONLY"
    later = opt.panel_split_coverage(
        coverage, metadata["panel_symbols"], [date(2026, 7, 23)]
    )
    assert not later["pass"]
    assert later["source_incomplete_symbol_sessions"] == 1
    assert later["incomplete_symbols"] == ["A"]


def test_score_uses_official_zero_trade_days_in_denominator() -> None:
    audit = pd.DataFrame(
        [
            {
                "session_date": date(2026, 7, 13),
                "side": "LONG",
                "filled": True,
                "net_return_pct": 1.0,
                "net_pnl_rs": 500.0,
                "status": v8.SignalState.TARGETED.value,
            }
        ]
    )
    metrics = opt.score_audit(
        audit,
        [date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15)],
    )
    assert metrics["closed_fills"] == 1
    assert metrics["trades_per_session"] == pytest.approx(1 / 3)
    assert metrics["flat_days"] == 2


def test_score_active_days_use_closed_counts_and_top_share_uses_positive_gross() -> None:
    sessions = [date(2026, 7, day) for day in range(13, 18)]
    audit = pd.DataFrame(
        [
            # These two closed trades offset exactly, but July 13 is active.
            (sessions[0], 2.0, 200.0),
            (sessions[0], -2.0, -200.0),
            (sessions[1], 3.0, 300.0),
            (sessions[2], 1.0, 100.0),
            (sessions[3], -2.0, -200.0),
        ],
        columns=["session_date", "net_return_pct", "net_pnl_rs"],
    )
    audit["filled"] = True
    audit["status"] = v8.SignalState.TARGETED.value

    metrics = opt.score_audit(audit, sessions)

    assert metrics["active_days"] == 4
    assert metrics["flat_days"] == 2  # one offsetting active day + one no-trade day
    assert metrics["top_day_share"] == pytest.approx(3.0 / 4.0)


def test_combine_calls_global_portfolio_ledger_once(monkeypatch) -> None:
    calls: list[int] = []

    def fake_ledger(frame: pd.DataFrame, policy: v8.PortfolioPolicy) -> pd.DataFrame:
        calls.append(len(frame))
        return frame.copy()

    monkeypatch.setattr(v8, "apply_global_portfolio_constraints", fake_ledger)
    long_audit = pd.DataFrame(
        [{"session_date": date(2026, 7, 13), "signal_time": "09:25", "side": "LONG", "frozen_rank": 1, "symbol": "X"}]
    )
    short_audit = pd.DataFrame(
        [{"session_date": date(2026, 7, 13), "signal_time": "09:25", "side": "SHORT", "frozen_rank": 1, "symbol": "Y"}]
    )
    result = opt.combine_and_constrain(long_audit, short_audit)
    assert len(result) == 2
    assert calls == [2]


def test_side_selection_uses_only_passing_behavioral_representatives() -> None:
    configs = list(opt.SIDE_GRID[:4])
    trials = pd.DataFrame(
        [
            _side_trial_row(
                configs[0], behavior="same", guard_pass=True, trades_per_session=2.0
            ),
            _side_trial_row(
                configs[1], behavior="same", guard_pass=True, trades_per_session=1.0
            ),
            _side_trial_row(
                configs[2], behavior="unique", guard_pass=True, trades_per_session=0.5
            ),
            _side_trial_row(
                configs[3], behavior="failed", guard_pass=False, trades_per_session=99.0
            ),
        ]
    )

    selected, ranked = opt.select_top_side_configs(trials, top_n=8)

    assert [item.config_hash for item in selected] == [
        configs[0].config_hash,
        configs[2].config_hash,
    ]
    alias = ranked.loc[ranked["config_hash"].eq(configs[1].config_hash)].iloc[0]
    assert alias["behavior_alias_of"] == configs[0].config_hash
    assert configs[3].config_hash not in {item.config_hash for item in selected}

    none, _ = opt.select_top_side_configs(
        trials.assign(guard_pass=False), top_n=8
    )
    assert none == []


def test_pair_frontier_supports_smaller_sets_and_empty_side() -> None:
    configs = list(opt.SIDE_GRID[:2])
    days = (date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15))
    long_data = opt.PreparedSideDataset("LONG", tuple(), days)
    short_data = opt.PreparedSideDataset("SHORT", tuple(), days)
    frame, audits = opt.evaluate_pair_frontier(
        long_configs=configs,
        short_configs=configs,
        train_long=long_data,
        train_short=short_data,
        validation_long=long_data,
        validation_short=short_data,
        guards=opt.SelectionGuards(),
    )
    assert len(frame) == 4
    assert len(audits) == 4

    empty_frame, empty_audits = opt.evaluate_pair_frontier(
        long_configs=[],
        short_configs=configs,
        train_long=long_data,
        train_short=short_data,
        validation_long=long_data,
        validation_short=short_data,
        guards=opt.SelectionGuards(),
    )
    assert empty_frame.empty
    assert empty_audits == {}


@pytest.mark.parametrize(
    ("bucket", "path", "value"),
    [
        ("train", ("combined", "robust_profit_factor_ex_best_day"), 0.99),
        ("validation", ("combined", "robust_profit_factor_ex_best_day"), 0.99),
        ("train", ("combined", "top_day_share"), 0.51),
        ("validation", ("combined", "top_day_share"), 0.51),
        ("train", ("combined", "positive_contiguous_blocks"), 1),
        ("validation", ("combined", "positive_contiguous_blocks"), 1),
        ("stress_train", ("combined", "data_incomplete_candidates"), 1),
        ("stress_validation", ("combined", "unresolved_filled_trades"), 1),
        ("stress_train", ("combined", "profit_factor"), 0.99),
        ("stress_validation", ("combined", "net_return_percentage_points"), 0.0),
        ("train", ("sides", "LONG", "profit_factor"), 0.99),
        ("train", ("sides", "SHORT", "net_return_percentage_points"), 0.0),
        ("validation", ("sides", "LONG", "net_return_percentage_points"), 0.0),
        ("validation", ("sides", "SHORT", "profit_factor"), 0.99),
    ],
)
def test_pair_guard_rejects_each_weak_or_incomplete_leg(
    bucket: str, path: tuple[str, ...], value: object
) -> None:
    train, validation, stress_train, stress_validation = _qualifying_pair_inputs()
    guards = opt.SelectionGuards()
    assert opt._pair_guard_pass(
        train, validation, stress_train, stress_validation, guards
    )
    groups = {
        "train": train,
        "validation": validation,
        "stress_train": stress_train,
        "stress_validation": stress_validation,
    }
    altered = deepcopy(groups)
    target = altered[bucket]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    assert not opt._pair_guard_pass(
        altered["train"],
        altered["validation"],
        altered["stress_train"],
        altered["stress_validation"],
        guards,
    )


def test_worker_count_one_and_two_are_deterministic(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidates.parquet"
    minute_path = tmp_path / "paths.parquet"
    _candidate_frame().to_parquet(candidate_path, index=False)
    _path_frame().to_parquet(minute_path, index=False)
    configs = list(opt.SIDE_GRID[:2])
    kwargs = dict(
        candidate_path=candidate_path,
        minute_path=minute_path,
        side="LONG",
        session_dates=[date(2026, 7, 13)],
        guards=opt.SelectionGuards(
            min_side_train_fills=0,
            min_side_active_days=0,
            min_side_train_pf=0,
            min_side_robust_pf=0,
        ),
        configs=configs,
    )
    serial = opt.search_side_grid(workers=1, **kwargs)
    parallel = opt.search_side_grid(workers=2, **kwargs)
    columns = ["config_hash", "behavior_signature", "closed_fills", "profit_factor"]
    pd.testing.assert_frame_equal(
        serial[columns].reset_index(drop=True), parallel[columns].reset_index(drop=True)
    )


def test_selection_is_authenticated_by_provenance_before_use(tmp_path: Path) -> None:
    selection_path = tmp_path / "selection.json"
    selection = {
        "schema_version": opt.OPTIMIZER_SCHEMA_VERSION,
        "search_fingerprint": "f" * 64,
        "selected_config": {"config_hash": "PAIR"},
    }
    selection_path.write_text(
        json.dumps(selection, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    provenance_payload = {
        "search_fingerprint": selection["search_fingerprint"],
        "results": {"selected_config_hash": "PAIR"},
        "outputs": {
            "selection": opt.provenance.artifact_record(selection_path),
        },
    }
    (tmp_path / "provenance.json").write_text(
        json.dumps(provenance_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert opt.load_authenticated_selection(tmp_path) == selection

    selection_path.write_text(
        json.dumps({**selection, "tampered": True}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="selection"):
        opt.load_authenticated_selection(tmp_path)


@pytest.mark.parametrize("tamper_mode", ["size", "same_size_hash"])
def test_derived_worker_cache_rejects_size_or_hash_tamper(
    tmp_path: Path, monkeypatch, tamper_mode: str
) -> None:
    monkeypatch.setattr(v8, "V8_ROOT", tmp_path)
    candidates = pd.DataFrame({"candidate_id": ["A"], "value": [1]})
    paths = pd.DataFrame({"candidate_id": ["A"], "minute_index": [1]})
    fingerprint = ("a" if tamper_mode == "size" else "b") * 64
    candidate_path, _ = opt._materialize_worker_inputs(
        candidates, paths, fingerprint=fingerprint
    )
    original = bytearray(candidate_path.read_bytes())
    if tamper_mode == "size":
        candidate_path.write_bytes(original + b"tamper")
    else:
        original[len(original) // 2] ^= 0x01
        candidate_path.write_bytes(original)

    with pytest.raises(AssertionError, match="derived cache"):
        opt._materialize_worker_inputs(candidates, paths, fingerprint=fingerprint)


def test_search_report_uses_actual_watermark_config_and_metrics() -> None:
    long_config = opt.SideEntryConfig(4, 5.0, True, 0.75, "RELAXED")
    short_config = opt.SideEntryConfig(3, 2.0, False, None, "MODERATE")
    train_combined = {
        **_pair_metric(closed_fills=40, profit_factor=1.75, net=8.25),
        "trades_per_session": 5.0,
        "max_drawdown_percentage_points": 1.25,
    }
    validation_combined = {
        **_pair_metric(closed_fills=17, profit_factor=1.625, net=3.5),
        "trades_per_session": 17 / 3,
        "max_drawdown_percentage_points": 0.75,
    }
    selection = {
        "selection_status": "DIAGNOSTIC_ONLY_SELECTION",
        "diagnostic_only": True,
        "coverage_mode": "RECTANGULAR_PANEL",
        "watermark": "UNIQUE-ACTUAL-WATERMARK-123",
        "selected_pair_rank": 2,
        "selected_qualifies": True,
        "pair_count": 6,
        "selected_config": {
            "config_hash": "PAIR-UNIQUE-456",
            "long": long_config.payload(),
            "short": short_config.payload(),
        },
        "selected_train_metrics": {"combined": train_combined},
        "selected_validation_metrics": {"combined": validation_combined},
        "selected_train_stress_metrics": {
            "combined": _pair_metric(closed_fills=40, profit_factor=1.10, net=1.25)
        },
        "selected_validation_stress_metrics": {
            "combined": _pair_metric(closed_fills=17, profit_factor=1.05, net=0.5)
        },
    }

    report = opt._search_report(selection)

    assert "UNIQUE-ACTUAL-WATERMARK-123" in report
    assert "PAIR-UNIQUE-456" in report
    assert "RELAXED" in report and "MODERATE" in report
    assert "max_confirmation_minute" in report
    assert "close_location_min" in report and "0.75" in report
    assert "PF=1.75" in report
    assert "fills=40" in report


def test_immutable_text_writer_refuses_replacement(tmp_path: Path) -> None:
    path = tmp_path / "artifact.txt"
    opt._write_new_text(path, "first")
    with pytest.raises(FileExistsError):
        opt._write_new_text(path, "second")
