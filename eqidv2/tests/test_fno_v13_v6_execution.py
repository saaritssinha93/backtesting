from __future__ import annotations

import json
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_oi_common as common
import fno_v13_v5_options_backtest as v5_options
import fno_v13_v6_option_liquidity_selector as selector
import fno_v13_v6_options_backtest as v6_options
import fno_v13_v6_portfolio_backtest as portfolio
import fno_v13_v6_research as research
import fno_v13_v7_exit_shadow as v7_exit
import fno_v13_v8_feature_shadow as v8_features
import fno_v13_run_vintage as vintage


def _stock_trade(
    sid: int,
    *,
    entry: str,
    exit_: str,
    pnl: float,
    symbol: str = "ABC",
    picker: str = "max_move",
    picker_value: float = 1.0,
) -> dict:
    return {
        "sid": sid,
        "day": "2026-09-01",
        "tradingsymbol": symbol,
        "filled": True,
        "confirmation_ts": pd.Timestamp(entry, tz=common.IST) - pd.Timedelta(minutes=1),
        "entry_ts": pd.Timestamp(entry, tz=common.IST),
        "exit_ts": pd.Timestamp(exit_, tz=common.IST),
        "setup_id": "0926_LONG",
        "picker": picker,
        "abs_price_change_pct": picker_value,
        "traded_value": 1_000_000.0,
        "volume_ratio": 2.0,
        "capital_per_entry_rupees": 100_000.0,
        "exposure_per_entry_rupees": 500_000.0,
        "initial_stop_pct": 1.5,
        "pre_cost_profit_rupees": pnl + 250.0,
        "cost_rupees": 250.0,
        "net_profit_rupees": pnl,
    }


def test_portfolio_reserves_and_releases_capital() -> None:
    source = pd.DataFrame(
        [
            _stock_trade(1, entry="2026-09-01 09:30", exit_="2026-09-01 10:00", pnl=1000),
            _stock_trade(2, entry="2026-09-01 09:31", exit_="2026-09-01 11:00", pnl=-500, symbol="DEF"),
            _stock_trade(3, entry="2026-09-01 09:32", exit_="2026-09-01 10:30", pnl=2000, symbol="GHI"),
            _stock_trade(4, entry="2026-09-01 10:01", exit_="2026-09-01 11:30", pnl=3000, symbol="JKL"),
        ]
    )
    ledger, summary = portfolio.apply_portfolio_constraints(
        source, portfolio.PortfolioConfig(portfolio_capital_rupees=200_000.0)
    )

    assert ledger.set_index("sid")["portfolio_status"].to_dict() == {
        1: "EXECUTED",
        2: "EXECUTED",
        3: "REJECTED",
        4: "EXECUTED",
    }
    assert ledger.set_index("sid").loc[3, "portfolio_reject_reason"] == (
        "INSUFFICIENT_PORTFOLIO_CAPITAL"
    )
    assert summary["portfolio_executed_trades"] == 3
    assert summary["peak_concurrent_positions"] == 2
    assert summary["peak_reserved_capital_rupees"] == pytest.approx(200_000.0)
    assert summary["net_profit_rupees"] == pytest.approx(3500.0)


def test_portfolio_same_timestamp_uses_picker_priority() -> None:
    source = pd.DataFrame(
        [
            _stock_trade(1, entry="2026-09-01 09:30", exit_="2026-09-01 10:00", pnl=1000, picker_value=0.5),
            _stock_trade(2, entry="2026-09-01 09:30", exit_="2026-09-01 10:00", pnl=2000, symbol="DEF", picker_value=1.0),
        ]
    )
    ledger, _ = portfolio.apply_portfolio_constraints(
        source, portfolio.PortfolioConfig(portfolio_capital_rupees=100_000.0)
    )
    status = ledger.set_index("sid")["portfolio_status"].to_dict()
    assert status == {1: "REJECTED", 2: "EXECUTED"}


def test_portfolio_open_risk_limit_is_causal() -> None:
    source = pd.DataFrame(
        [
            _stock_trade(1, entry="2026-09-01 09:30", exit_="2026-09-01 10:00", pnl=1000),
            _stock_trade(2, entry="2026-09-01 09:31", exit_="2026-09-01 10:00", pnl=1000, symbol="DEF"),
        ]
    )
    ledger, summary = portfolio.apply_portfolio_constraints(
        source,
        portfolio.PortfolioConfig(
            portfolio_capital_rupees=500_000.0,
            max_open_risk_rupees=8_000.0,
        ),
    )
    assert ledger.set_index("sid").loc[2, "portfolio_reject_reason"] == (
        "MAX_OPEN_RISK_REACHED"
    )
    assert summary["peak_open_initial_risk_rupees"] == pytest.approx(7750.0)


def test_capacity_research_compares_unconstrained_and_slots() -> None:
    source = pd.DataFrame(
        [
            _stock_trade(1, entry="2026-09-01 09:30", exit_="2026-09-01 10:00", pnl=1000),
            _stock_trade(2, entry="2026-09-01 09:31", exit_="2026-09-01 10:30", pnl=-500, symbol="DEF"),
        ]
    )
    comparison, scenarios = research.build_comparison(source, [1, 2], 100_000.0)
    september = comparison.loc[comparison["period"].eq("2026-09")].set_index("scenario")

    assert set(scenarios) == {
        "V13_V5_UNCONSTRAINED",
        "V13_V6_1_SLOT",
        "V13_V6_2_SLOT",
    }
    assert september.loc["V13_V5_UNCONSTRAINED", "executed_trades"] == 2
    assert september.loc["V13_V5_UNCONSTRAINED", "peak_concurrent_positions"] == 2
    assert september.loc["V13_V6_1_SLOT", "executed_trades"] == 1
    assert september.loc["V13_V6_2_SLOT", "net_profit_rupees"] == pytest.approx(500.0)


def _coverage_row(**overrides) -> pd.Series:
    base = {
        "trade_id": "2026-09-01|1|ABC|2026-09-01T09:30:00+05:30",
        "sid": 1,
        "day": "2026-09-01",
        "profile": "higher_frequency",
        "strategy_version": "V13V5",
        "equity_symbol": "ABC",
        "equity_side": "LONG",
        "option_tradingsymbol": "ABC26SEP100CE",
        "required_option_type": "CE",
        "option_strike": 100.0,
        "lot_size": 10,
        "quantity": 10,
        "coverage_state": "READY",
        "mapping_status": "MAPPED_ATM",
        "reference_entry_bar": pd.Timestamp("2026-09-01 09:30", tz=common.IST),
        "reference_entry_premium": 100.0,
        "entry_bar_volume": 100,
    }
    base.update(overrides)
    return pd.Series(base)


def _candles(records: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return v5_options.normalize_option_candles(frame)


def test_option_loader_uses_explicit_data_root(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    symbol = "ABC26SEP100CE"
    frame = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2026-09-01 09:30", tz=common.IST),
                "open": 11.0,
                "high": 11.0,
                "low": 11.0,
                "close": 11.0,
                "volume": 100,
            }
        ]
    )
    frame.to_parquet(v6_options.option_path(symbol, second), index=False)

    loaded = v6_options.load_option_candles(symbol, second, {})

    assert loaded.iloc[0]["open"] == pytest.approx(11.0)
    assert not v6_options.option_path(symbol, first).exists()


def test_option_coverage_roots_merge_and_later_root_wins(tmp_path: Path) -> None:
    roots = [tmp_path / "base", tmp_path / "daily"]
    for root in roots:
        (root / "audit").mkdir(parents=True)
        (root / "raw_options_1m").mkdir()
    pd.DataFrame(
        [
            {"trade_id": "A", "coverage_state": "OLD"},
            {"trade_id": "B", "coverage_state": "BASE_ONLY"},
        ]
    ).to_csv(roots[0] / "audit" / "option_trade_coverage_and_capital.csv", index=False)
    pd.DataFrame(
        [
            {"trade_id": "A", "coverage_state": "NEW"},
            {"trade_id": "C", "coverage_state": "DAILY_ONLY"},
        ]
    ).to_csv(roots[1] / "audit" / "option_trade_coverage_and_capital.csv", index=False)

    combined, sources = v6_options.load_coverage_roots(roots)

    assert combined["trade_id"].tolist() == ["B", "A", "C"]
    assert combined.set_index("trade_id").loc["A", "coverage_state"] == "NEW"
    assert len(sources) == 2
    assert all(record["coverage_sha256"] for record in sources)


def test_option_explicit_shadow_coverage_requires_raw_path(tmp_path: Path) -> None:
    path = tmp_path / "selected.csv"
    pd.DataFrame(
        [
            {
                "trade_id": "A",
                "coverage_state": "READY",
                "option_tradingsymbol": "ABC26SEP100CE",
                "_raw_options_dir": str(tmp_path / "raw_options_1m"),
            }
        ]
    ).to_csv(path, index=False)

    loaded, sources = v6_options.load_explicit_coverage(path)
    assert loaded.iloc[0]["trade_id"] == "A"
    assert sources[0]["coverage_sha256"]


def test_option_gap_stop_uses_adverse_open_and_two_sided_costs() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 100,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 70.0,
                "high": 70.0,
                "low": 70.0,
                "close": 70.0,
                "volume": 100,
            },
        ]
    )
    config = v6_options.OptionExecutionConfig(entry_cost_bps=5, exit_cost_bps=5)

    result = v6_options.simulate_option_native_trade(
        _coverage_row(), candles, v5_options.OPTION_VARIANTS[0], config
    )

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_reason"] == "FULL_STOP"
    assert result["exit_gap_through"]
    assert result["exit_premium"] == pytest.approx(70.0)
    assert result["gross_pnl_rupees"] == pytest.approx(-900.0)
    assert result["entry_cost_rupees"] == pytest.approx(1.5)
    assert result["exit_cost_rupees"] == pytest.approx(1.05)
    assert result["net_pnl_rupees"] == pytest.approx(-902.55)


def test_option_entry_capacity_can_reject_or_resize() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.9,
                "close": 100.0,
                "volume": 20,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 122.5,
                "high": 123.0,
                "low": 110.0,
                "close": 122.5,
                "volume": 100,
            },
        ]
    )
    rejected = v6_options.simulate_option_native_trade(
        _coverage_row(entry_bar_volume=20),
        candles,
        v5_options.OPTION_VARIANTS[0],
        v6_options.OptionExecutionConfig(entry_capacity_policy="reject"),
    )
    resized = v6_options.simulate_option_native_trade(
        _coverage_row(entry_bar_volume=20),
        candles,
        v5_options.OPTION_VARIANTS[0],
        v6_options.OptionExecutionConfig(entry_capacity_policy="resize"),
    )

    assert rejected["execution_status"] == "SKIPPED_ENTRY_SIZING_OR_CAPACITY"
    assert rejected["skip_reason"] == "ENTRY_VOLUME_CAPACITY_INSUFFICIENT"
    assert resized["execution_status"] == "EXECUTED"
    assert resized["option_lots"] == 2
    assert resized["quantity"] == 20


def test_option_same_bar_partial_fill_consumes_entry_capacity_first() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 123.0,
                "low": 99.9,
                "close": 122.5,
                "volume": 50,
            }
        ]
    )
    result = v6_options.simulate_option_native_trade(
        _coverage_row(entry_bar_volume=50),
        candles,
        v5_options.OPTION_VARIANTS[0],
        v6_options.OptionExecutionConfig(),
    )
    assert result["execution_status"] == "PARTIALLY_EXECUTED_OPEN_REMAINDER"
    assert result["exit_filled_quantity"] == 20
    assert result["unfilled_exit_quantity"] == 10
    fills = pd.DataFrame.from_records(json.loads(result["execution_fills_json"]))
    assert fills.loc[fills["side"].eq("SELL"), "capacity_consumed_before"].iloc[0] == 30


def test_option_missing_exact_entry_candle_fails_closed() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 100,
            }
        ]
    )
    result = v6_options.simulate_option_native_trade(
        _coverage_row(),
        candles,
        v5_options.OPTION_VARIANTS[0],
        v6_options.OptionExecutionConfig(),
    )
    assert result["execution_status"] == "SKIPPED_EXECUTION_INPUT"
    assert result["skip_reason"] == "MISSING_EXACT_ENTRY_CANDLE"


def test_option_exit_capacity_records_partial_open_remainder() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.9,
                "close": 100.0,
                "volume": 100,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 122.5,
                "high": 123.0,
                "low": 110.0,
                "close": 122.5,
                "volume": 20,
            },
        ]
    )
    result = v6_options.simulate_option_native_trade(
        _coverage_row(),
        candles,
        v5_options.OPTION_VARIANTS[0],
        v6_options.OptionExecutionConfig(),
    )
    assert result["execution_status"] == "PARTIALLY_EXECUTED_OPEN_REMAINDER"
    assert result["exit_filled_quantity"] == 20
    assert result["unfilled_exit_quantity"] == 10
    assert result["realized_net_pnl_rupees"] > 0
    assert pd.isna(result["net_pnl_rupees"])


def test_option_target_partial_fills_complete_across_bars() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.9,
                "close": 100.0,
                "volume": 100,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 122.5,
                "high": 123.0,
                "low": 110.0,
                "close": 122.5,
                "volume": 20,
            },
            {
                "timestamp": "2026-09-01 09:32:00+05:30",
                "open": 122.5,
                "high": 123.0,
                "low": 110.0,
                "close": 122.5,
                "volume": 10,
            },
        ]
    )
    result = v6_options.simulate_option_native_trade(
        _coverage_row(), candles, v5_options.OPTION_VARIANTS[0], v6_options.OptionExecutionConfig()
    )

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_fill_count"] == 2
    assert result["exit_filled_quantity"] == 30
    assert result["unfilled_exit_quantity"] == 0
    assert result["partial_exit"]
    assert result["exit_premium"] == pytest.approx(122.5)


def test_option_stop_residual_uses_later_adverse_open() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 101.0,
                "low": 99.9,
                "close": 100.0,
                "volume": 100,
            },
            {
                "timestamp": "2026-09-01 09:31:00+05:30",
                "open": 80.0,
                "high": 80.0,
                "low": 79.0,
                "close": 79.5,
                "volume": 10,
            },
            {
                "timestamp": "2026-09-01 09:32:00+05:30",
                "open": 60.0,
                "high": 62.0,
                "low": 59.0,
                "close": 61.0,
                "volume": 20,
            },
        ]
    )
    result = v6_options.simulate_option_native_trade(
        _coverage_row(), candles, v5_options.OPTION_VARIANTS[0], v6_options.OptionExecutionConfig()
    )

    assert result["execution_status"] == "EXECUTED"
    assert result["exit_fill_count"] == 2
    assert result["exit_premium"] == pytest.approx((80.0 + 2 * 60.0) / 3.0)
    assert "FULL_STOP_RESIDUAL" in result["exit_reason"]


def test_liquidity_selector_is_causal_and_can_override_illiquid_atm(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw_options_1m"
    raw.mkdir()
    entry = pd.Timestamp("2026-09-01 09:30", tz=common.IST)

    def write_contract(symbol: str, strike: float, pre_volume: int, post_volume: int) -> None:
        frame = _candles(
            [
                {
                    "timestamp": entry - pd.Timedelta(minutes=offset),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": pre_volume,
                    "oi": 1000,
                }
                for offset in (3, 2, 1)
            ]
            + [
                {
                    "timestamp": entry,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": post_volume,
                    "oi": 1000,
                },
                {
                    "timestamp": pd.Timestamp("2026-09-01 10:01", tz=common.IST),
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.0,
                    "volume": 100,
                    "oi": 1000,
                },
            ]
        )
        frame["strike"] = strike
        frame.to_parquet(v6_options.option_path(symbol, raw), index=False)

    write_contract("ABC26SEP100CE", 100.0, pre_volume=2, post_volume=100_000)
    write_contract("ABC26SEP105CE", 105.0, pre_volume=100, post_volume=10)
    coverage = pd.DataFrame(
        [
            _coverage_row(
                equity_entry_ts=pd.Timestamp("2026-09-01 09:29", tz=common.IST),
                equity_exit_ts=pd.Timestamp("2026-09-01 10:00", tz=common.IST),
                required_option_expiry="2026-09-29",
            )
        ]
    )
    plans = pd.DataFrame(
        [
            {
                "underlying": "ABC",
                "tradingsymbol": symbol,
                "instrument_token": token,
                "expiry": "2026-09-29",
                "strike": strike,
                "instrument_type": "CE",
                "lot_size": 10,
                "tick_size": 0.05,
                "required_sessions": "2026-09-01",
                "_source_priority": 0,
                "_raw_options_dir": str(raw),
            }
            for symbol, token, strike in (
                ("ABC26SEP100CE", 1, 100.0),
                ("ABC26SEP105CE", 2, 105.0),
            )
        ]
    )

    selected, audit = selector.build_liquidity_shadow(
        coverage, plans, selector.LiquiditySelectorConfig()
    )

    assert selected.iloc[0]["option_tradingsymbol"] == "ABC26SEP105CE"
    assert selected.iloc[0]["selector_reason"] == "NEAR_ATM_LIQUIDITY_OVERRIDE"
    atm = audit.loc[audit["candidate_tradingsymbol"].eq("ABC26SEP100CE")].iloc[0]
    assert atm["pre_entry_volume"] == pytest.approx(6.0)
    assert atm["pre_entry_volume"] < 100_000


def _write_manifest_run(root: Path, run_id: str, day: str, *, valid_hash: bool = True) -> Path:
    run_dir = root / run_id
    run_dir.mkdir(parents=True)
    output = run_dir / "result.csv"
    output.write_text("pnl\n100\n", encoding="utf-8")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    manifest = {
        "schema_version": "TEST_V1",
        "complete": True,
        "run_id": run_id,
        "generated_at_ist": "2026-09-09T17:00:00+05:30",
        "data_through_date": day,
        "outputs": {
            "result": {
                "path": str(output),
                "sha256": digest if valid_hash else "0" * 64,
            }
        },
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return run_dir / "manifest.json"


def test_run_vintage_validates_hashes_and_rejects_tampering(tmp_path: Path) -> None:
    manifest_path = _write_manifest_run(tmp_path, "run_a", "2026-09-08")
    assert vintage.validate_manifest(manifest_path)["valid"]

    (manifest_path.parent / "result.csv").write_text("pnl\n-999\n", encoding="utf-8")
    checked = vintage.validate_manifest(manifest_path)
    assert not checked["valid"]
    assert "OUTPUT_HASH_MISMATCH" in checked["errors"]


def test_run_vintage_never_falls_back_from_invalid_newest(tmp_path: Path) -> None:
    root = tmp_path / "family"
    old = _write_manifest_run(root, "run_old", "2026-09-08")
    new = _write_manifest_run(root, "run_new", "2026-09-08", valid_hash=False)
    old.touch()
    new.touch()
    checked = vintage.inspect_family(root)

    assert checked["run_id"] == "run_new"
    assert checked["status"] == "BLOCKED_INVALID_LATEST_RUN"


def test_run_vintage_blocks_mixed_family_dates(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write_manifest_run(first, "run_first", "2026-09-07")
    _write_manifest_run(second, "run_second", "2026-09-08")

    checked = vintage.build_vintage_status({"first": first, "second": second})
    assert checked["status"] == "BLOCKED_MIXED_DATA_VINTAGES"
    assert not checked["ready"]


def test_v8_time_of_day_features_use_only_prior_sessions_and_pre_entry_bars() -> None:
    rows = []
    for day, volume in (
        ("2026-08-27", 10),
        ("2026-08-28", 10),
        ("2026-08-31", 10),
        ("2026-09-01", 20),
    ):
        for minute in range(25, 30):
            rows.append(
                {
                    "date": f"{day} 09:{minute:02d}:00+05:30",
                    "open": 101.0,
                    "high": 101.2,
                    "low": 100.8,
                    "close": 101.0 + (minute - 25) * 0.1,
                    "volume": volume,
                    "oi": 1000,
                }
            )
    rows.append(
        {
            "date": "2026-09-01 09:30:00+05:30",
            "open": 999.0,
            "high": 999.0,
            "low": 999.0,
            "close": 999.0,
            "volume": 1_000_000,
            "oi": 999999,
        }
    )
    features = v8_features.causal_futures_features(
        pd.DataFrame(rows),
        entry_ts=pd.Timestamp("2026-09-01 09:30", tz=common.IST),
        equity_price=100.0,
        side="LONG",
        config=v8_features.FuturesFeatureConfig(),
    )

    assert features["v8_feature_status"] == "READY"
    assert features["v8_prior_session_count"] == 3
    assert features["fut_volume_tod_median_prior"] == pytest.approx(50.0)
    assert features["fut_volume_window"] == pytest.approx(100.0)
    assert features["fut_volume_tod_ratio"] == pytest.approx(2.0)
    assert features["v8_feature_max_timestamp"] < features["v8_feature_cutoff_exclusive"]
    assert features["v8_baseline_max_day"] == "2026-08-31"


def test_option_risk_budget_produces_integer_lots() -> None:
    candles = _candles(
        [
            {
                "timestamp": "2026-09-01 09:30:00+05:30",
                "open": 100.0,
                "high": 123.0,
                "low": 99.9,
                "close": 122.5,
                "volume": 100,
            }
        ]
    )
    result = v6_options.simulate_option_native_trade(
        _coverage_row(),
        candles,
        v5_options.OPTION_VARIANTS[0],
        v6_options.OptionExecutionConfig(risk_budget_rupees=400.0),
    )
    assert result["execution_status"] == "EXECUTED"
    assert result["option_lots"] == 2
    assert result["quantity"] == 20


def test_v7_exit_shadow_changes_only_the_time_cap() -> None:
    orders = pd.DataFrame(
        [
            {
                "sid": 1,
                "day": "2026-09-01",
                "tradingsymbol": "ABC",
                "side": "LONG",
                "trigger": 100.0,
            }
        ]
    )
    timestamps = pd.date_range(
        "2026-09-01 09:30", periods=250, freq="min", tz=common.IST
    )
    path = {
        "timestamp_ns": timestamps.astype("int64").to_numpy(),
        "open": np.full(250, 100.0),
        "high": np.full(250, 100.5),
        "low": np.full(250, 99.5),
        "close": np.full(250, 100.0),
    }
    path["close"][180] = 99.0
    path["high"][181] = 103.0
    path["low"][181] = 100.1

    arms = v7_exit.simulate_exit_arms(
        orders,
        {1: path},
        cost_bps=5.0,
        capital_per_entry_rupees=100_000.0,
        leverage_factor=5.0,
    )
    paired = v7_exit.paired_trade_delta(arms).iloc[0]

    assert arms["V13_V5_CONTROL_EOD"].iloc[0]["exit_reason"] == "RUNNER_TARGET"
    assert arms["V13_V7_TIME180_SHADOW"].iloc[0]["exit_reason"] == "MAX_HOLD_NO_T1"
    assert paired["holding_minutes_time180"] == pytest.approx(180.0)
    assert paired["pnl_delta_time180_minus_control_rupees"] < 0


def test_v7_control_parity_fails_on_pnl_drift() -> None:
    source = pd.DataFrame(
        [
            {
                "sid": 1,
                "filled": True,
                "net_profit_rupees": 100.0,
                "exit_reason": "RUNNER_TARGET",
            }
        ]
    )
    control = source.copy()
    assert v7_exit.validate_control_parity(source, control)["passed"]
    control.loc[0, "net_profit_rupees"] = 100.01
    with pytest.raises(RuntimeError, match="P&L delta"):
        v7_exit.validate_control_parity(source, control)
