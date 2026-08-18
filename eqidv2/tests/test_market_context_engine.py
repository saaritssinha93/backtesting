from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from market_context_engine import (
    FEATURE_VERSION,
    FORBIDDEN_OUTPUT_COLUMNS,
    MarketContextConfig,
    MarketContextEngine,
    attach_context_asof,
)


TZ = "Asia/Kolkata"


def _row(
    timestamp: str | pd.Timestamp,
    ticker: str,
    close: float,
    *,
    high: float | None = None,
    low: float | None = None,
    volume: float = 100.0,
    sector: str = "S1",
    ema20: float | None = None,
    ema50: float | None = None,
    opening_snapshot: bool = False,
    vol_ratio: float | None = 1.0,
) -> dict:
    result = {
        "date": pd.Timestamp(timestamp, tz=TZ) if not isinstance(timestamp, pd.Timestamp) else timestamp,
        "ticker": ticker,
        "open": close,
        "high": close if high is None else high,
        "low": close if low is None else low,
        "close": close,
        "volume": volume,
        "sector": sector,
        "opening_snapshot": opening_snapshot,
    }
    if ema20 is not None:
        result["EMA_20"] = ema20
    if ema50 is not None:
        result["EMA_50"] = ema50
    if vol_ratio is not None:
        result["vol_ratio"] = vol_ratio
    return result


def _three_index_bars(stock_frame: pd.DataFrame, source_ticker: str = "A") -> pd.DataFrame:
    base = stock_frame.loc[stock_frame["ticker"].eq(source_ticker), [
        "date", "open", "high", "low", "close", "volume"
    ]]
    pieces = []
    for ticker, multiplier in (
        ("NIFTYBEES", 1.0),
        ("BANKNIFTY", 1.2),
        ("NIFTYMIDCAP150", 0.8),
    ):
        one = base.copy()
        for column in ("open", "high", "low", "close"):
            one[column] *= multiplier
        one["ticker"] = ticker
        pieces.append(one)
    return pd.concat(pieces, ignore_index=True)


def _engine(**overrides) -> MarketContextEngine:
    defaults = {
        "min_sector_members": 1,
        "min_sector_coverage": 0.0,
        "min_market_coverage": 0.0,
        "regime_min_sessions": 2,
        "relative_volume_min_sessions": 2,
    }
    defaults.update(overrides)
    return MarketContextEngine(MarketContextConfig(**defaults))


def test_exact_advance_decline_and_breadth_formula() -> None:
    rows = []
    for ticker, sector in zip("ABCD", ("S1", "S1", "S2", "S2")):
        rows.append(_row("2026-01-05 15:30", ticker, 100.0, sector=sector, ema20=100, ema50=100))
    rows += [
        _row("2026-01-06 09:20", "A", 111, high=112, low=108, sector="S1", ema20=100, ema50=100),
        _row("2026-01-06 09:20", "B", 86, high=91, low=85, sector="S1", ema20=95, ema50=95),
        _row("2026-01-06 09:20", "C", 101, high=102, low=99, sector="S2", ema20=99, ema50=99),
        _row("2026-01-06 09:20", "D", 100, high=107, low=99, sector="S2", ema20=106, ema50=106),
    ]
    result = _engine().compute(pd.DataFrame(rows))
    got = result.market.loc[result.market["timestamp"].dt.strftime("%Y-%m-%d %H:%M").eq(
        "2026-01-06 09:20"
    )].iloc[0]

    assert got["advance_count"] == 2
    assert got["decline_count"] == 1
    assert got["unchanged_count"] == 1
    assert got["advance_decline_ratio"] == pytest.approx(2.5 / 1.5)
    assert got["advance_decline_net"] == pytest.approx(0.25)
    assert got["pct_above_vwap"] == pytest.approx(50.0)
    assert got["pct_above_ema20"] == pytest.approx(50.0)
    assert got["pct_above_ema50"] == pytest.approx(50.0)
    # First completed bar has no prior intraday high/low, so the four remaining
    # breadth components are reweighted over 90% of the configured weights.
    assert got["market_breadth"] == pytest.approx((0.30 * 0.25) / 0.90)


def test_gap_filled_rows_do_not_vote_in_breadth_or_mapping_participation() -> None:
    rows = [
        _row("2026-01-05 15:30", "A", 100, sector="S1", ema20=100, ema50=100),
        _row("2026-01-05 15:30", "B", 100, sector="S2", ema20=100, ema50=100),
        _row(
            "2026-01-06 09:20", "A", 100, high=101, low=99,
            sector="S1", ema20=100, ema50=100,
        ),
        _row(
            "2026-01-06 09:20", "B", 100, high=101, low=99,
            sector="S2", ema20=100, ema50=100,
        ),
        _row(
            "2026-01-06 09:25", "A", 99, high=100, low=98,
            sector="S1", ema20=100, ema50=100,
        ),
        _row(
            "2026-01-06 09:25", "B", 200, high=201, low=199, volume=0,
            sector="S2", ema20=100, ema50=100,
        ),
    ]
    rows[-1]["gap_filled"] = 1

    result = _engine().compute(pd.DataFrame(rows))
    got = result.market.loc[
        result.market["timestamp"].dt.strftime("%Y-%m-%d %H:%M").eq(
            "2026-01-06 09:25"
        )
    ].iloc[0]

    # The carried-forward row remains a valid LTP observation, but only A is a
    # fresh completed bar and therefore only A participates in breadth.
    assert got["price_eligible_count"] == 2
    assert got["fresh_price_eligible_count"] == 1
    assert got["previous_close_valid_count"] == 1
    assert got["advance_count"] == 0
    assert got["decline_count"] == 1
    assert got["unchanged_count"] == 0
    assert got["advance_decline_net"] == pytest.approx(-1.0)

    assert got["above_vwap_valid_count"] == 1
    assert got["above_ema20_valid_count"] == 1
    assert got["above_ema50_valid_count"] == 1
    assert got["pct_above_vwap"] == pytest.approx(0.0)
    assert got["pct_above_ema20"] == pytest.approx(0.0)
    assert got["pct_above_ema50"] == pytest.approx(0.0)
    assert got["new_high_low_valid_count"] == 1
    assert got["pct_new_intraday_highs"] == pytest.approx(0.0)
    assert got["pct_new_intraday_lows"] == pytest.approx(100.0)
    assert got["market_breadth"] == pytest.approx(-1.0)

    assert got["sector_mapped_count"] == 1
    assert got["sector_mapping_coverage"] == pytest.approx(1.0)
    stale_sector = result.sectors.loc[
        result.sectors["timestamp"].eq(got["timestamp"])
        & result.sectors["sector"].eq("S2")
    ].iloc[0]
    assert stale_sector["sector_fresh_price_eligible_count"] == 0
    assert pd.isna(stale_sector["sector_pct_above_vwap"])


def test_opening_snapshot_is_not_a_completed_context_bar() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:15", "A", 999, opening_snapshot=True, ema20=1, ema50=1),
            _row("2026-01-05 09:20", "A", 100, high=101, low=98, ema20=99, ema50=99),
        ]
    )
    result = _engine().compute(frame)

    assert result.market["timestamp"].dt.strftime("%H:%M").tolist() == ["09:20"]
    assert result.market.iloc[0]["pct_above_vwap"] == pytest.approx(100.0)


def test_rows_after_nse_session_close_are_excluded() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 15:30", "A", 100, ema20=99, ema50=98),
            _row("2026-01-05 15:35", "A", 999, ema20=99, ema50=98),
            _row("2026-01-06 09:20", "A", 101, ema20=99, ema50=98),
        ]
    )

    result = _engine().compute(frame)

    assert result.market["timestamp"].dt.strftime("%H:%M").tolist() == [
        "15:30",
        "09:20",
    ]
    next_session = result.market.iloc[-1]
    assert next_session["advance_count"] == 1
    assert next_session["decline_count"] == 0


def test_absurd_opening_snapshot_cannot_contaminate_later_features() -> None:
    ordinary = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, high=101, low=99, ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, high=102, low=100, ema20=100, ema50=99),
        ]
    )
    with_snapshot = pd.concat(
        [
            pd.DataFrame([
                _row(
                    "2026-01-05 09:15", "A", 1_000_000, high=2_000_000,
                    low=1, volume=99_000_000, opening_snapshot=True,
                )
            ]),
            ordinary,
        ],
        ignore_index=True,
    )
    engine = _engine()

    pd.testing.assert_frame_equal(
        engine.compute(ordinary).market,
        engine.compute(with_snapshot).market,
        check_dtype=False,
    )


def test_new_intraday_high_percentage_uses_only_prior_bars() -> None:
    rows = []
    for ticker in "ABCD":
        rows.append(_row("2026-01-05 09:20", ticker, 100, high=101, low=99, ema20=99, ema50=99))
    rows += [
        _row("2026-01-05 09:25", "A", 102, high=103, low=100, ema20=99, ema50=99),
        _row("2026-01-05 09:25", "B", 100, high=101, low=99, ema20=99, ema50=99),
        _row("2026-01-05 09:25", "C", 99, high=100, low=98, ema20=99, ema50=99),
        _row("2026-01-05 09:25", "D", 103, high=104, low=101, ema20=99, ema50=99),
    ]
    result = _engine().compute(pd.DataFrame(rows))

    first, second = result.market.sort_values("timestamp").iloc[[0, 1]].to_dict("records")
    assert np.isnan(first["pct_new_intraday_highs"])
    assert second["pct_new_intraday_highs"] == pytest.approx(50.0)
    assert second["pct_new_intraday_lows"] == pytest.approx(25.0)


def test_sector_strength_rank_orders_positive_flat_and_negative_momentum() -> None:
    rows = []
    slopes = {"LEAD": 1.0, "FLAT": 0.0, "LAG": -1.0}
    tickers = {"LEAD": ("A", "B"), "FLAT": ("C", "D"), "LAG": ("E", "F")}
    for bar, timestamp in enumerate(pd.date_range("2026-01-05 09:20", periods=8, freq="5min", tz=TZ)):
        for sector, members in tickers.items():
            for offset, ticker in enumerate(members):
                price = 100.0 + offset + slopes[sector] * bar
                rows.append(_row(timestamp, ticker, price, high=price + 0.2, low=price - 0.2, sector=sector, ema20=100, ema50=100))
    frame = pd.DataFrame(rows)
    result = _engine(min_sector_members=2).compute(frame)
    latest = result.sectors.loc[result.sectors["timestamp"].eq(result.sectors["timestamp"].max())]
    ranks = latest.set_index("sector")["sector_strength_rank"].to_dict()

    assert ranks == {"FLAT": 2.0, "LAG": 3.0, "LEAD": 1.0}
    assert latest.set_index("sector").loc["LEAD", "sector_momentum_30m_pct"] > 0
    assert latest.set_index("sector").loc["LAG", "sector_momentum_30m_pct"] < 0


def test_sector_relative_volume_uses_prior_same_slot_sessions_only() -> None:
    rows = []
    days = pd.bdate_range("2026-01-05", periods=4)
    for day_number, day in enumerate(days):
        timestamp = pd.Timestamp(f"{day.date()} 09:20", tz=TZ)
        rows.append(_row(timestamp, "A", 100, volume=200 if day_number == 3 else 100, sector="HIGH", vol_ratio=None))
        rows.append(_row(timestamp, "B", 100, volume=50 if day_number == 3 else 100, sector="LOW", vol_ratio=None))
    result = _engine(
        relative_volume_sessions=3,
        relative_volume_min_sessions=2,
    ).compute(pd.DataFrame(rows))
    latest = result.sectors.loc[result.sectors["timestamp"].eq(result.sectors["timestamp"].max())]
    rvol = latest.set_index("sector")["sector_relative_volume"].to_dict()

    assert rvol["HIGH"] == pytest.approx(2.0)
    assert rvol["LOW"] == pytest.approx(0.5)


def test_invalid_flow_bar_is_not_used_in_future_relative_volume_baseline() -> None:
    rows = []
    days = pd.bdate_range("2026-01-05", periods=4)
    volumes = [100.0, 99_000_000.0, 100.0, 200.0]
    for day_number, (day, volume) in enumerate(zip(days, volumes)):
        row = _row(
            pd.Timestamp(f"{day.date()} 09:20", tz=TZ),
            "A",
            100,
            volume=volume,
            sector="S1",
            vol_ratio=None,
        )
        row["gap_filled"] = int(day_number == 1)
        rows.append(row)
    result = _engine(
        relative_volume_sessions=3,
        relative_volume_min_sessions=2,
    ).compute(pd.DataFrame(rows))
    latest = result.sectors.iloc[-1]

    assert latest["sector_relative_volume"] == pytest.approx(2.0)


def test_rising_indexes_produce_positive_scores_and_uptrend_regime() -> None:
    rows = []
    for bar, timestamp in enumerate(pd.date_range("2026-01-05 09:20", periods=18, freq="5min", tz=TZ)):
        for ticker, sector in (("A", "S1"), ("B", "S2"), ("C", "S3")):
            price = 100.0 + bar + (ord(ticker) - ord("A")) * 0.01
            rows.append(_row(timestamp, ticker, price, high=price + 0.2, low=price - 0.2, sector=sector, ema20=price - 1, ema50=price - 2))
    stocks = pd.DataFrame(rows)
    result = _engine().compute(stocks, _three_index_bars(stocks))
    latest = result.market.iloc[-1]

    assert latest["nifty_trend_score"] > 20
    assert latest["bank_nifty_trend_score"] > 20
    assert latest["midcap_trend_score"] > 20
    assert latest["combined_index_trend_score"] > 20
    assert latest["trend_regime"] == "UPTREND"
    assert latest["risk_on_off_score"] > 0


def test_realized_volatility_requires_full_sixty_minute_window() -> None:
    rows = []
    for bar, timestamp in enumerate(
        pd.date_range("2026-01-05 09:20", periods=13, freq="5min", tz=TZ)
    ):
        price = 100.0 + bar
        rows.append(
            _row(timestamp, "A", price, high=price + 0.2, low=price - 0.2, ema20=99, ema50=98)
        )
    stocks = pd.DataFrame(rows)
    market = _engine().compute(stocks, _three_index_bars(stocks)).market

    assert market["nifty_realized_volatility_60m_bps"].iloc[:12].isna().all()
    assert np.isfinite(market["nifty_realized_volatility_60m_bps"].iloc[12])
    assert market["intraday_volatility_z"].isna().all()


def test_rotation_score_reweights_missing_warmup_components() -> None:
    stocks = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 101, sector="S1", ema20=100, ema50=99),
            _row("2026-01-05 09:20", "B", 100, sector="S2", ema20=100, ema50=99),
            _row("2026-01-05 09:20", "C", 99, sector="S3", ema20=100, ema50=99),
        ]
    )
    row = _engine().compute(stocks).market.iloc[0]

    assert pd.isna(row["sector_dispersion_z"])
    assert row["sector_rank_turnover_mean"] != row["sector_rank_turnover_mean"]
    assert row["rotation_score"] == pytest.approx(100.0 * (1.0 - abs(row["market_breadth"])))


def test_zero_variance_regime_baselines_remain_unavailable() -> None:
    rows = []
    for day in (5, 6, 7):
        timestamps = pd.date_range(
            f"2026-01-{day:02d} 09:20", periods=13, freq="5min", tz=TZ
        )
        for bar, timestamp in enumerate(timestamps):
            for ticker, sector, slope in (
                ("A", "S1", 1.0),
                ("B", "S2", 0.5),
                ("C", "S3", -0.2),
            ):
                price = 100.0 + slope * bar
                rows.append(
                    _row(
                        timestamp,
                        ticker,
                        price,
                        high=price + 0.2,
                        low=price - 0.2,
                        sector=sector,
                        ema20=99,
                        ema50=98,
                    )
                )
    stocks = pd.DataFrame(rows)
    latest = _engine().compute(stocks, _three_index_bars(stocks)).market.iloc[-1]

    assert latest["volatility_baseline_observations"] == 2
    assert pd.isna(latest["intraday_volatility_z"])
    assert not latest["volatility_baseline_ready"]
    assert pd.isna(latest["intraday_volatility_regime_code"])
    assert latest["intraday_volatility_regime"] == "WARMUP"
    assert latest["rotation_baseline_observations"] == 2
    assert pd.isna(latest["sector_dispersion_z"])
    assert not latest["rotation_baseline_ready"]
    assert pd.isna(latest["rotation_regime_code"])
    assert latest["rotation_regime"] == "WARMUP"


def test_future_bar_perturbation_cannot_change_earlier_context() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=10, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        for ticker, sector, slope in (("A", "S1", 1.0), ("B", "S2", -0.3), ("C", "S3", 0.2)):
            price = 100 + slope * bar
            rows.append(_row(timestamp, ticker, price, high=price + 1, low=price - 1, sector=sector, ema20=100, ema50=100))
    stocks = pd.DataFrame(rows)
    indexes = _three_index_bars(stocks)
    engine = _engine()
    original = engine.compute(stocks, indexes)

    changed_stocks = stocks.copy()
    changed_stocks.loc[changed_stocks["date"].eq(timestamps[-1]), ["high", "close", "volume"]] = [9999, 9000, 9_000_000]
    changed_indexes = indexes.copy()
    changed_indexes.loc[changed_indexes["date"].eq(timestamps[-1]), ["high", "close", "volume"]] = [9999, 9000, 9_000_000]
    changed = engine.compute(changed_stocks, changed_indexes)

    cutoff = timestamps[-2]
    pd.testing.assert_frame_equal(
        original.market.loc[original.market["timestamp"].le(cutoff)].reset_index(drop=True),
        changed.market.loc[changed.market["timestamp"].le(cutoff)].reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        original.sectors.loc[original.sectors["timestamp"].le(cutoff)].reset_index(drop=True),
        changed.sectors.loc[changed.sectors["timestamp"].le(cutoff)].reset_index(drop=True),
        check_dtype=False,
    )


def test_future_preferred_index_alias_cannot_erase_older_alias_history() -> None:
    stocks = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, ema20=99, ema50=98),
            _row("2026-01-05 09:30", "A", 102, ema20=99, ema50=98),
        ]
    )
    index_base = stocks[["date", "open", "high", "low", "close", "volume"]].copy()
    index_base["ticker"] = "NIFTY50"
    future_preferred = index_base.iloc[[-1]].copy()
    future_preferred[["open", "high", "low", "close"]] *= 2.8
    future_preferred["ticker"] = "NIFTYBEES"
    engine = _engine()

    baseline = engine.compute(stocks, index_base).market.iloc[:2]
    expanded = engine.compute(stocks, pd.concat([index_base, future_preferred])).market.iloc[:2]
    pd.testing.assert_series_equal(
        baseline["nifty_trend_score"].reset_index(drop=True),
        expanded["nifty_trend_score"].reset_index(drop=True),
        check_names=False,
    )
    assert expanded["nifty_source"].eq("NIFTY50").all()


def test_synonymous_index_aliases_are_never_mixed_into_one_price_series() -> None:
    rows = []
    for bar, timestamp in enumerate(pd.date_range("2026-01-05 09:20", periods=15, freq="5min", tz=TZ)):
        rows.append(_row(timestamp, "A", 100 + bar, ema20=99, ema50=98))
    stocks = pd.DataFrame(rows)
    first = stocks[["date", "open", "high", "low", "close", "volume"]].copy()
    first["ticker"] = "NIFTY50"
    synonym = first.copy()
    synonym[["open", "high", "low", "close"]] *= 2.0
    synonym["ticker"] = "NIFTY_50"
    engine = _engine()

    baseline = engine.compute(stocks, first).market
    combined = engine.compute(stocks, pd.concat([first, synonym])).market
    pd.testing.assert_series_equal(
        baseline["nifty_trend_score"], combined["nifty_trend_score"], check_names=False
    )
    pd.testing.assert_series_equal(
        baseline["nifty_realized_volatility_60m_bps"],
        combined["nifty_realized_volatility_60m_bps"],
        check_names=False,
    )


def test_explicitly_invalid_row_cannot_poison_later_intraday_high_state() -> None:
    base = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, high=101, low=99, ema20=99, ema50=98),
            _row("2026-01-05 09:30", "A", 102, high=103, low=101, ema20=100, ema50=99),
        ]
    )
    bad = _row(
        "2026-01-05 09:25", "A", 1_000_000, high=1_000_001, low=1,
        volume=99_000_000, ema20=None, ema50=None,
    )
    bad["is_eligible"] = False
    base["is_eligible"] = True
    with_bad = pd.concat([base, pd.DataFrame([bad])], ignore_index=True)
    engine = _engine()

    ordinary = engine.compute(base).market.iloc[-1]
    contaminated = engine.compute(with_bad).market.iloc[-1]
    assert ordinary["pct_new_intraday_highs"] == 100.0
    assert contaminated["pct_new_intraday_highs"] == ordinary["pct_new_intraday_highs"]


def test_future_only_ticker_cannot_revise_past_coverage() -> None:
    base = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, ema20=99, ema50=98),
        ]
    )
    expanded = pd.concat(
        [base, pd.DataFrame([_row("2026-01-05 09:25", "NEW", 50, sector="S2", ema20=49, ema50=48)])],
        ignore_index=True,
    )
    engine = _engine()
    before = engine.compute(base).market.iloc[0]
    after = engine.compute(expanded).market.iloc[0]

    assert before["universe_expected"] == after["universe_expected"] == 1
    assert before["market_coverage"] == after["market_coverage"] == 1.0


def test_asof_join_is_backward_prefixed_and_never_filters_candidates() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=3, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        rows.append(_row(timestamp, "A", 100 + bar, sector="S1", ema20=99, ema50=98))
    result = _engine().compute(pd.DataFrame(rows))
    candidates = pd.DataFrame(
        {
            "ticker": ["A", "A", "A"],
            "sector": ["S1", "S1", "S1"],
            "side": ["LONG", "LONG", "LONG"],
            "signal_time_ist": [
                pd.Timestamp("2026-01-05 09:19", tz=TZ),
                pd.Timestamp("2026-01-05 09:22", tz=TZ),
                pd.Timestamp("2026-01-05 09:27", tz=TZ),
            ],
        }
    )
    joined = attach_context_asof(candidates, result)

    assert len(joined) == len(candidates)
    assert joined["side"].tolist() == candidates["side"].tolist()
    assert pd.isna(joined.loc[0, "mce_timestamp"])
    assert joined.loc[1, "mce_timestamp"] == timestamps[0]
    assert joined.loc[2, "mce_timestamp"] == timestamps[1]
    assert joined.loc[1, "mce_age_seconds"] == pytest.approx(120.0)
    assert all(column.startswith("mce_") for column in joined.columns if column not in candidates.columns)


def test_asof_join_preserves_invalid_timestamps_and_has_unique_columns() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, sector="S1", ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, sector="S1", ema20=99, ema50=98),
        ]
    )
    result = _engine().compute(frame)
    candidates = pd.DataFrame(
        {"ticker": ["A", "A"], "sector": ["S1", "S1"], "signal_time_ist": [None, "bad"]}
    )
    joined = attach_context_asof(candidates, result)

    assert len(joined) == 2
    assert joined.columns.is_unique
    assert joined["mce_timestamp"].isna().all()


def test_publication_delay_and_staleness_boundary_are_enforced() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, sector="S1", ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, sector="S1", ema20=99, ema50=98),
        ]
    )
    result = _engine(publish_delay_seconds=60).compute(frame)
    candidates = pd.DataFrame(
        {
            "ticker": ["A"] * 4,
            "sector": ["S1"] * 4,
            # Deliberately unsorted: result order must remain unchanged.
            "signal_time_ist": [
                pd.Timestamp("2026-01-05 09:33:01", tz=TZ),
                pd.Timestamp("2026-01-05 09:26:00", tz=TZ),
                pd.Timestamp("2026-01-05 09:25:30", tz=TZ),
                pd.Timestamp("2026-01-05 09:33:00", tz=TZ),
            ],
        }
    )
    joined = attach_context_asof(candidates, result, max_staleness_minutes=7)

    assert pd.isna(joined.loc[0, "mce_timestamp"])
    assert joined.loc[1, "mce_timestamp"] == pd.Timestamp("2026-01-05 09:25", tz=TZ)
    assert joined.loc[2, "mce_timestamp"] == pd.Timestamp("2026-01-05 09:20", tz=TZ)
    assert joined.loc[3, "mce_timestamp"] == pd.Timestamp("2026-01-05 09:25", tz=TZ)
    valid = joined["mce_available_at"].notna()
    decision = pd.to_datetime(joined.loc[valid, "signal_time_ist"])
    assert (joined.loc[valid, "mce_available_at"].array <= decision.array).all()


def test_unmapped_panel_and_ema_warmup_are_missing_not_false() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, sector="", ema20=None, ema50=None),
            _row("2026-01-05 09:25", "A", 101, sector="", ema20=None, ema50=None),
        ]
    ).drop(columns=["vol_ratio"])
    result = _engine().compute(frame)

    assert result.sectors.empty
    assert result.market["pct_above_ema20"].isna().all()
    assert result.market["pct_above_ema50"].isna().all()
    assert result.market["rotation_regime"].eq("UNKNOWN").all()
    assert result.market["trend_regime"].eq("UNKNOWN").all()
    assert result.market["rotation_regime_code"].isna().all()
    assert result.market["trend_regime_code"].isna().all()


def test_missing_labels_do_not_create_fake_ticker_or_sector() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, sector=pd.NA, ema20=99, ema50=98),
            _row("2026-01-05 09:20", "B", 101, sector=pd.NA, ema20=99, ema50=98),
            _row("2026-01-05 09:20", "C", 102, sector=pd.NA, ema20=99, ema50=98),
            _row("2026-01-05 09:20", pd.NA, 999, sector=pd.NA, ema20=99, ema50=98),
        ]
    )
    config = _engine().config
    result = MarketContextEngine(
        config,
        sector_map={"A": "REAL", "B": "REAL", "C": "REAL"},
    ).compute(frame)

    assert set(result.sectors["sector"]) == {"REAL"}
    assert "<NA>" not in set(result.sectors["sector"])
    assert result.market.iloc[0]["price_eligible_count"] == 3
    assert result.market.iloc[0]["sector_mapping_coverage"] == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"publish_delay_seconds": -1}, "publish_delay_seconds"),
        ({"publish_delay_seconds": -0.5}, "publish_delay_seconds"),
        ({"rotation_lookback_bars": -1}, "rotation_lookback_bars"),
    ],
)
def test_causal_configuration_rejects_negative_time_offsets(overrides, match) -> None:
    with pytest.raises(ValueError, match=match):
        MarketContextConfig(**overrides)


def test_outputs_are_feature_only_and_versioned() -> None:
    frame = pd.DataFrame([_row("2026-01-05 09:20", "A", 100, sector="S1", ema20=99, ema50=98)])
    result = _engine().compute(frame)

    independent_forbidden = {
        "side", "signal", "trade", "entry", "entry_price", "exit", "exit_price",
        "stop", "stop_loss", "sl_price", "target", "target_price", "quantity",
        "position", "order", "order_type",
    }
    assert FORBIDDEN_OUTPUT_COLUMNS.issuperset(independent_forbidden)
    for output in (result.market, result.sectors):
        assert not (independent_forbidden & {str(column).lower() for column in output.columns})
        assert output["feature_version"].eq(FEATURE_VERSION).all()
