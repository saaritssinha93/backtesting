from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sector_intelligence import (
    FORBIDDEN_FEATURE_TOKENS,
    IDENTIFIER_COLUMNS,
    QUALITY_FEATURE_COLUMNS,
    SECTOR_INTELLIGENCE_VERSION,
    SECTOR_FEATURE_COLUMNS,
    STOCK_FEATURE_COLUMNS,
    SectorIntelligenceConfig,
    SectorIntelligenceEngine,
    attach_sector_intelligence_asof,
    sector_intelligence_feature_columns,
)


TZ = "Asia/Kolkata"


def _row(
    timestamp: str | pd.Timestamp,
    ticker: str,
    close: float,
    *,
    sector: str | None = "S1",
    volume: float = 100.0,
    ema20: float | None = None,
    ema50: float | None = None,
    vol_ratio: float | None = 1.0,
) -> dict:
    timestamp = (
        pd.Timestamp(timestamp, tz=TZ)
        if not isinstance(timestamp, pd.Timestamp)
        else timestamp
    )
    result = {
        "date": timestamp,
        "ticker": ticker,
        "sector": sector,
        "open": close,
        "high": close + 0.1,
        "low": close - 0.1,
        "close": close,
        "volume": volume,
    }
    if ema20 is not None:
        result["EMA_20"] = ema20
    if ema50 is not None:
        result["EMA_50"] = ema50
    if vol_ratio is not None:
        result["vol_ratio"] = vol_ratio
    return result


def _engine(**overrides) -> SectorIntelligenceEngine:
    defaults = {
        "min_sector_members": 2,
        "min_sector_data_coverage": 0.0,
        "relative_volume_min_sessions": 2,
        "regime_min_sessions": 2,
    }
    defaults.update(overrides)
    return SectorIntelligenceEngine(SectorIntelligenceConfig(**defaults))


def _linear_panel(
    *,
    periods: int = 13,
    sectors: tuple[str, ...] = ("S1", "S2", "S3"),
    members: int = 3,
) -> pd.DataFrame:
    rows = []
    timestamps = pd.date_range("2026-01-06 09:20", periods=periods, freq="5min", tz=TZ)
    slopes = {sector: float(index - 1) for index, sector in enumerate(sectors)}
    for sector in sectors:
        for member in range(members):
            rows.append(
                _row(
                    "2026-01-05 15:30",
                    f"{sector}_{member}",
                    100.0,
                    sector=sector,
                    ema20=100.0,
                    ema50=100.0,
                )
            )
    for bar, timestamp in enumerate(timestamps):
        for sector in sectors:
            for member in range(members):
                price = 100.0 + slopes[sector] * bar + 0.01 * member
                rows.append(
                    _row(
                        timestamp,
                        f"{sector}_{member}",
                        price,
                        sector=sector,
                        volume=100.0 * (member + 1),
                        ema20=99.5,
                        ema50=99.0,
                    )
                )
    return pd.DataFrame(rows)


def test_stock_leave_one_out_math_and_tie_neutral_percentile() -> None:
    returns = {"A": 0.03, "B": 0.02, "C": 0.02, "D": 0.01}
    rows = [
        _row("2026-01-05 15:30", ticker, 100.0, ema20=99, ema50=98)
        for ticker in returns
    ]
    timestamps = pd.date_range("2026-01-06 09:20", periods=7, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        for ticker, total_log_return in returns.items():
            price = 100.0 * np.exp(total_log_return * bar / 6.0)
            rows.append(
                _row(timestamp, ticker, price, ema20=99, ema50=98, volume=100)
            )

    result = _engine(min_sector_members=4).compute(pd.DataFrame(rows))
    latest = result.stocks.loc[
        result.stocks["timestamp"].eq(timestamps[-1])
    ].set_index("ticker")

    for ticker, own_return in returns.items():
        peers = [value for other, value in returns.items() if other != ticker]
        expected_relative = 100.0 * (own_return - np.mean(peers))
        expected_day_distance = 100.0 * (
            (np.exp(own_return) - 1.0)
            - np.mean([np.exp(value) - 1.0 for value in peers])
        )
        assert latest.loc[ticker, "stock_relative_momentum_30m_pct"] == pytest.approx(
            expected_relative
        )
        assert latest.loc[
            ticker, "stock_distance_from_sector_average_pct"
        ] == pytest.approx(expected_day_distance)

    assert latest["stock_sector_percentile"].to_dict() == pytest.approx(
        {"A": 100.0, "B": 50.0, "C": 50.0, "D": 0.0}
    )


def test_sector_volatility_requires_twelve_complete_intraday_returns() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=13, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        for member in range(2):
            price = 100.0 * np.exp(0.001 * bar)
            rows.append(
                _row(
                    timestamp,
                    f"A{member}",
                    price,
                    ema20=99,
                    ema50=98,
                )
            )
    sector = _engine().compute(pd.DataFrame(rows)).sectors.sort_values("timestamp")

    assert sector["sector_volatility_60m_bps"].iloc[:12].isna().all()
    expected = 10_000.0 * np.sqrt(12.0 * 0.001**2)
    assert sector["sector_volatility_60m_bps"].iloc[12] == pytest.approx(expected)


def test_sector_direction_participation_relative_strength_and_breadth() -> None:
    result = _engine().compute(_linear_panel())
    latest = result.sectors.loc[
        result.sectors["timestamp"].eq(result.sectors["timestamp"].max())
    ].set_index("sector")

    assert latest.loc["S3", "sector_relative_strength_score"] > 0
    assert latest.loc["S1", "sector_relative_strength_score"] < 0
    assert latest.loc["S3", "sector_participation_pct"] == pytest.approx(100.0)
    assert latest.loc["S1", "sector_signed_participation_pct"] == pytest.approx(-100.0)
    assert latest.loc["S3", "sector_breadth"] > latest.loc["S1", "sector_breadth"]
    assert latest.loc["S3", "sector_trend_score"] > latest.loc["S1", "sector_trend_score"]


def test_participation_can_detect_narrow_alignment_with_mean_sector_direction() -> None:
    rows = []
    total_returns = [-0.01] * 6 + [0.10] * 4
    timestamps = pd.date_range("2026-01-05 09:20", periods=7, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        for member, total_return in enumerate(total_returns):
            price = 100.0 * np.exp(total_return * bar / 6.0)
            rows.append(
                _row(timestamp, f"A{member}", price, ema20=99, ema50=98)
            )

    sector = _engine(min_sector_members=5).compute(pd.DataFrame(rows)).sectors.iloc[-1]

    assert sector["sector_momentum_30m_pct"] == pytest.approx(-1.0)
    assert sector["sector_participation_pct"] == pytest.approx(40.0)
    assert sector["sector_signed_participation_pct"] == pytest.approx(40.0)


def test_participation_is_missing_during_momentum_warmup_not_false_zero() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, ema20=99, ema50=98),
            _row("2026-01-05 09:20", "B", 101, ema20=99, ema50=98),
        ]
    )
    sector = _engine().compute(frame).sectors.iloc[0]

    assert pd.isna(sector["sector_participation_pct"])
    assert pd.isna(sector["sector_signed_participation_pct"])


def test_explicit_expected_members_are_aligned_by_timestamp_and_sector() -> None:
    rows = []
    timestamp = pd.Timestamp("2026-01-05 09:20", tz=TZ)
    for sector, observed, expected in (("A", 2, 4), ("B", 3, 3)):
        for member in range(observed):
            row = _row(
                timestamp,
                f"{sector}{member}",
                100 + member,
                sector=sector,
                ema20=99,
                ema50=98,
            )
            row["sector_expected_members"] = expected
            rows.append(row)
    latest = _engine(min_sector_data_coverage=0.70).compute(
        pd.DataFrame(rows)
    ).sectors.set_index("sector")

    assert latest.loc["A", "sector_expected_member_count"] == 4
    assert latest.loc["A", "sector_data_coverage_pct"] == pytest.approx(50.0)
    assert latest.loc["A", "sector_reliable_flag"] == 0
    assert latest.loc["B", "sector_expected_member_count"] == 3
    assert latest.loc["B", "sector_data_coverage_pct"] == pytest.approx(100.0)
    assert latest.loc["B", "sector_reliable_flag"] == 1


def test_invalid_expected_member_manifest_disables_sector_features() -> None:
    timestamp = pd.Timestamp("2026-01-05 09:20", tz=TZ)
    rows = []
    for member, expected in enumerate((1, 3)):
        row = _row(
            timestamp,
            f"A{member}",
            100.0 + member,
            ema20=99,
            ema50=98,
        )
        row["sector_expected_members"] = expected
        rows.append(row)

    sector = _engine().compute(pd.DataFrame(rows)).sectors.iloc[0]

    assert sector["sector_expected_member_source_code"] == -1
    assert sector["sector_expected_members_valid_flag"] == 0
    assert sector["sector_reliable_flag"] == 0
    assert pd.isna(sector["sector_trend_score"])


def test_stock_components_require_component_specific_peer_coverage() -> None:
    rows = []
    # Only half the sector has a previous close and usable flow history, while
    # all ten members have fresh prices and a valid 30-minute momentum path.
    for member in range(5):
        row = _row(
            "2026-01-05 15:30",
            f"A{member}",
            100.0,
            volume=100.0,
            ema20=99,
            ema50=98,
        )
        row["sector_expected_members"] = 10
        rows.append(row)
    for bar, timestamp in enumerate(
        pd.date_range("2026-01-06 09:20", periods=7, freq="5min", tz=TZ)
    ):
        for member in range(10):
            row = _row(
                timestamp,
                f"A{member}",
                100.0 + bar + 0.01 * member,
                volume=100.0 if member < 5 else np.nan,
                ema20=99,
                ema50=98,
            )
            row["sector_expected_members"] = 10
            rows.append(row)

    result = _engine(
        min_sector_members=5,
        min_sector_data_coverage=0.70,
    ).compute(pd.DataFrame(rows))
    latest_time = result.stocks["timestamp"].max()
    sector = result.sectors.loc[result.sectors["timestamp"].eq(latest_time)].iloc[0]
    stocks = result.stocks.loc[result.stocks["timestamp"].eq(latest_time)]

    assert sector["sector_reliable_flag"] == 1
    assert sector["sector_momentum_coverage_pct"] == pytest.approx(100.0)
    assert sector["sector_previous_close_coverage_pct"] == pytest.approx(50.0)
    assert sector["sector_above_vwap_coverage_pct"] == pytest.approx(50.0)
    assert sector["sector_rvol_coverage_pct"] == pytest.approx(50.0)
    assert pd.isna(sector["sector_intraday_return_pct"])
    assert stocks["stock_outperformance_component_count"].eq(1).all()
    assert stocks["stock_outperformance_score"].isna().all()
    assert stocks["stock_sector_leadership_score"].isna().all()


def test_peer_counts_and_coverage_exclude_the_subject_stock() -> None:
    rows = []
    for member in range(3):
        row = _row(
            "2026-01-05 15:30",
            f"A{member}",
            100.0,
            volume=100.0,
            ema20=99,
            ema50=98,
        )
        row["sector_expected_members"] = 4
        rows.append(row)
    for bar, timestamp in enumerate(
        pd.date_range("2026-01-06 09:20", periods=7, freq="5min", tz=TZ)
    ):
        for member in range(4):
            row = _row(
                timestamp,
                f"A{member}",
                100.0 + bar + 0.01 * member,
                volume=100.0 if member < 3 else np.nan,
                ema20=99,
                ema50=98,
            )
            row["sector_expected_members"] = 4
            rows.append(row)

    result = _engine(
        min_sector_members=3,
        min_sector_data_coverage=0.75,
    ).compute(pd.DataFrame(rows))
    latest = result.stocks.loc[
        result.stocks["timestamp"].eq(result.stocks["timestamp"].max())
    ].set_index("ticker")

    assert latest.loc["A0", "stock_peer_momentum_valid_count"] == 3
    assert latest.loc["A0", "stock_peer_intraday_valid_count"] == 2
    assert latest.loc["A3", "stock_peer_intraday_valid_count"] == 3
    assert latest["stock_outperformance_component_count"].eq(1).all()
    assert latest["stock_outperformance_score"].isna().all()


def test_outperformance_rank_requires_enough_jointly_comparable_stocks() -> None:
    rows = []
    # Previous closes exist for A4..A9; a 30-minute path exists for A0..A5.
    # Only A4/A5 therefore have two outperformance components even though each
    # primitive component independently satisfies the 55% coverage threshold.
    for member in range(4, 10):
        row = _row(
            "2026-01-05 15:30",
            f"A{member}",
            100.0,
            volume=100.0,
            ema20=99,
            ema50=98,
        )
        row["sector_expected_members"] = 10
        rows.append(row)
    timestamps = pd.date_range("2026-01-06 09:20", periods=7, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        for member in range(6):
            row = _row(
                timestamp,
                f"A{member}",
                100.0 + bar + 0.01 * member,
                volume=np.nan,
                ema20=99,
                ema50=98,
            )
            row["sector_expected_members"] = 10
            rows.append(row)
    for member in range(6, 10):
        row = _row(
            timestamps[-1],
            f"A{member}",
            106.0 + 0.01 * member,
            volume=np.nan,
            ema20=99,
            ema50=98,
        )
        row["sector_expected_members"] = 10
        rows.append(row)

    result = _engine(
        min_sector_members=5,
        min_sector_data_coverage=0.55,
    ).compute(pd.DataFrame(rows))
    latest = result.stocks.loc[result.stocks["timestamp"].eq(timestamps[-1])]

    assert latest["stock_outperformance_component_count"].eq(2).sum() == 2
    assert latest["stock_outperformance_score"].isna().all()
    assert latest["stock_sector_percentile"].isna().all()
    assert latest["stock_is_sector_leader"].isna().all()


def test_gap_filled_rows_do_not_enter_current_sector_breadth() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=2, freq="5min", tz=TZ)
    for timestamp in timestamps:
        for member in range(3):
            row = _row(
                timestamp,
                f"A{member}",
                100.0 + member,
                volume=100.0,
                ema20=99,
                ema50=98,
            )
            row["gap_filled"] = int(timestamp == timestamps[-1] and member == 2)
            rows.append(row)

    latest = _engine(min_sector_data_coverage=0.0).compute(
        pd.DataFrame(rows)
    ).sectors.iloc[-1]

    assert latest["sector_fresh_price_eligible_count"] == 2
    assert latest["sector_above_vwap_valid_count"] == 2
    assert latest["sector_above_ema20_valid_count"] == 2
    assert latest["sector_above_ema50_valid_count"] == 2


def test_nonfinite_or_out_of_domain_supplied_indicators_are_unavailable() -> None:
    ema_values = [np.inf, -np.inf, 0.0, -1.0, 99.0]
    rvol_values = [np.inf, -np.inf, -1.0, 1.0, 2.0]
    rows = [
        _row(
            "2026-01-05 09:20",
            f"A{member}",
            100.0 + member,
            ema20=ema_values[member],
            ema50=ema_values[member],
            vol_ratio=rvol_values[member],
        )
        for member in range(5)
    ]

    result = _engine(
        min_sector_members=3,
        min_sector_data_coverage=0.70,
    ).compute(pd.DataFrame(rows))
    sector = result.sectors.iloc[0]

    assert sector["sector_above_ema20_valid_count"] == 1
    assert sector["sector_above_ema50_valid_count"] == 1
    assert sector["sector_rvol_valid_count"] == 2
    assert pd.isna(sector["sector_pct_above_ema20"])
    assert pd.isna(sector["sector_relative_volume"])
    for output in (result.sectors, result.stocks):
        numeric = output.select_dtypes(include=[np.number]).to_numpy()
        assert not np.isinf(numeric).any()


def test_sector_acceleration_compares_non_overlapping_thirty_minute_windows() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=13, freq="5min", tz=TZ)
    log_price = 0.0
    for bar, timestamp in enumerate(timestamps):
        if bar > 0:
            log_price += 0.001 if bar <= 6 else 0.003
        for member in range(2):
            price = 100.0 * np.exp(log_price)
            rows.append(
                _row(timestamp, f"A{member}", price, ema20=99, ema50=98)
            )
    latest = _engine().compute(pd.DataFrame(rows)).sectors.iloc[-1]

    assert latest["sector_momentum_30m_pct"] == pytest.approx(1.8)
    assert latest["sector_acceleration_30m_pct"] == pytest.approx(1.2)


def test_market_relative_features_require_market_wide_momentum_coverage() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=7, freq="5min", tz=TZ)
    for bar, timestamp in enumerate(timestamps):
        for sector in ("S1", "S2"):
            for member in range(2):
                rows.append(
                    _row(
                        timestamp,
                        f"{sector}_{member}",
                        100.0 + bar + member,
                        sector=sector,
                        ema20=99,
                        ema50=98,
                    )
                )
    for sector in ("S1", "S2"):
        for member in range(2, 5):
            rows.append(
                _row(
                    timestamps[-1],
                    f"{sector}_{member}",
                    106.0 + member,
                    sector=sector,
                    ema20=99,
                    ema50=98,
                )
            )

    result = _engine(
        min_sector_members=2,
        min_sector_data_coverage=0.0,
        expected_universe_size=10,
        min_market_coverage=0.70,
    ).compute(pd.DataFrame(rows))
    latest = result.sectors.loc[
        result.sectors["timestamp"].eq(timestamps[-1])
    ]

    assert latest["market_expected_member_count"].eq(10).all()
    assert latest["market_momentum_valid_count"].eq(4).all()
    assert latest["market_momentum_coverage_pct"].eq(40.0).all()
    assert latest["market_momentum_ready_flag"].eq(0).all()
    assert latest["sector_relative_momentum_pct"].isna().all()
    assert latest["sector_relative_strength_score"].isna().all()


def test_cross_sector_scores_require_expected_sector_coverage() -> None:
    result = _engine(
        expected_sector_count=5,
        min_cross_sector_coverage=0.70,
    ).compute(_linear_panel(periods=13, sectors=("S1", "S2"), members=3))
    latest = result.sectors.loc[
        result.sectors["timestamp"].eq(result.sectors["timestamp"].max())
    ]

    assert latest["cross_sector_expected_count"].eq(5).all()
    assert latest["cross_sector_reliable_count"].eq(2).all()
    assert latest["cross_sector_reliable_coverage_pct"].eq(40.0).all()
    assert latest["cross_sector_ready_flag"].eq(0).all()
    assert latest["sector_relative_strength_score"].isna().all()
    assert latest["sector_acceleration_score"].isna().all()
    assert latest["sector_liquidity_score"].isna().all()
    assert latest["sector_strength_score"].isna().all()


def test_causal_expected_sector_count_detects_later_whole_sector_dropout() -> None:
    frame = _linear_panel(periods=13, sectors=("S1", "S2", "S3"), members=3)
    last_time = frame["date"].max()
    frame = frame.loc[~(frame["date"].eq(last_time) & frame["sector"].eq("S3"))]

    result = _engine(min_cross_sector_coverage=0.70).compute(frame)
    latest = result.sectors.loc[result.sectors["timestamp"].eq(last_time)]

    assert latest["cross_sector_expected_count"].eq(3).all()
    assert latest["cross_sector_reliable_count"].eq(2).all()
    assert np.allclose(
        latest["cross_sector_reliable_coverage_pct"], 200.0 / 3.0
    )
    assert latest["cross_sector_ready_flag"].eq(0).all()
    assert latest["sector_relative_strength_score"].isna().all()
    assert latest["sector_liquidity_score"].isna().all()
    assert latest["sector_strength_score"].isna().all()


def test_sector_liquidity_orders_absolute_rupee_turnover() -> None:
    rows = []
    for bar, timestamp in enumerate(
        pd.date_range("2026-01-05 09:20", periods=7, freq="5min", tz=TZ)
    ):
        for sector, volume in (("LOW", 100.0), ("MID", 1_000.0), ("HIGH", 10_000.0)):
            for member in range(2):
                rows.append(
                    _row(
                        timestamp,
                        f"{sector}_{member}",
                        100.0 + bar,
                        sector=sector,
                        volume=volume,
                        ema20=99,
                        ema50=98,
                    )
                )
    latest = _engine().compute(pd.DataFrame(rows)).sectors
    latest = latest.loc[latest["timestamp"].eq(latest["timestamp"].max())].set_index(
        "sector"
    )

    assert latest.loc["HIGH", "sector_liquidity_score"] > latest.loc[
        "MID", "sector_liquidity_score"
    ] > latest.loc["LOW", "sector_liquidity_score"]
    assert latest.loc["HIGH", "sector_total_turnover_crore"] == pytest.approx(0.212)


def test_leaders_and_weakest_are_numeric_not_ticker_valued_features() -> None:
    rows = []
    for bar, timestamp in enumerate(
        pd.date_range("2026-01-05 09:20", periods=7, freq="5min", tz=TZ)
    ):
        for member in range(10):
            price = 100.0 * np.exp((member + 1) * 0.001 * bar)
            rows.append(
                _row(
                    timestamp,
                    f"A{member}",
                    price,
                    volume=100.0 + member,
                    ema20=99,
                    ema50=98,
                )
            )
    result = _engine(min_sector_members=5).compute(pd.DataFrame(rows))
    latest_time = result.sectors["timestamp"].max()
    sector = result.sectors.loc[result.sectors["timestamp"].eq(latest_time)].iloc[0]
    stocks = result.stocks.loc[result.stocks["timestamp"].eq(latest_time)]

    assert sector["sector_leader_stock_count"] == 1
    assert sector["sector_weakest_stock_count"] == 1
    assert stocks["stock_is_sector_leader"].sum() == 1
    assert stocks["stock_is_sector_weakest"].sum() == 1
    assert stocks["stock_sector_leadership_score"].between(0.0, 100.0).all()


def test_all_tied_stock_cross_section_is_neutral_and_has_no_false_leaders() -> None:
    rows = []
    for bar, timestamp in enumerate(
        pd.date_range("2026-01-05 09:20", periods=7, freq="5min", tz=TZ)
    ):
        for member in range(10):
            rows.append(
                _row(
                    timestamp,
                    f"A{member}",
                    100.0 + bar,
                    volume=100.0,
                    ema20=99,
                    ema50=98,
                )
            )
    result = _engine(min_sector_members=5).compute(pd.DataFrame(rows))
    latest = result.stocks.loc[
        result.stocks["timestamp"].eq(result.stocks["timestamp"].max())
    ]

    assert latest["stock_sector_percentile"].eq(50.0).all()
    assert latest["stock_sector_leadership_percentile"].eq(50.0).all()
    assert latest["stock_is_sector_leader"].sum() == 0
    assert latest["stock_is_sector_weakest"].sum() == 0


def test_same_slot_relative_turnover_uses_prior_sessions_only() -> None:
    rows = []
    volumes = [100.0, 100.0, 100.0, 200.0]
    for day, volume in zip(pd.bdate_range("2026-01-05", periods=4), volumes):
        timestamp = pd.Timestamp(f"{day.date()} 09:20", tz=TZ)
        for member in range(2):
            rows.append(
                _row(
                    timestamp,
                    f"A{member}",
                    100.0,
                    volume=volume,
                    ema20=99,
                    ema50=98,
                    vol_ratio=None,
                )
            )
    latest = _engine(
        relative_volume_sessions=3,
        relative_volume_min_sessions=2,
    ).compute(pd.DataFrame(rows)).sectors.iloc[-1]

    assert latest["sector_relative_turnover"] == pytest.approx(2.0)


def test_future_price_and_membership_cannot_change_earlier_outputs() -> None:
    base = _linear_panel(periods=8, sectors=("S1", "S2"), members=2)
    engine = _engine()
    original = engine.compute(base)
    last_time = base["date"].max()
    changed = base.copy()
    changed.loc[changed["date"].eq(last_time), ["close", "high", "volume"]] = [
        9_000.0,
        9_001.0,
        9_000_000.0,
    ]
    changed = pd.concat(
        [
            changed,
            pd.DataFrame(
                [
                    _row(
                        last_time,
                        "FUTURE_NEW",
                        500.0,
                        sector="S1",
                        ema20=400,
                        ema50=300,
                    )
                ]
            ),
        ],
        ignore_index=True,
    )
    perturbed = engine.compute(changed)
    cutoff = last_time - pd.Timedelta(minutes=5)

    pd.testing.assert_frame_equal(
        original.sectors.loc[original.sectors["timestamp"].le(cutoff)].reset_index(
            drop=True
        ),
        perturbed.sectors.loc[perturbed.sectors["timestamp"].le(cutoff)].reset_index(
            drop=True
        ),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        original.stocks.loc[original.stocks["timestamp"].le(cutoff)].reset_index(
            drop=True
        ),
        perturbed.stocks.loc[perturbed.stocks["timestamp"].le(cutoff)].reset_index(
            drop=True
        ),
        check_dtype=False,
    )


def test_shuffled_input_order_cannot_change_sorted_outputs() -> None:
    frame = _linear_panel(periods=8, sectors=("S1", "S2"), members=3)
    engine = _engine()
    ordered = engine.compute(frame)
    shuffled = engine.compute(frame.sample(frac=1.0, random_state=17))

    pd.testing.assert_frame_equal(ordered.sectors, shuffled.sectors, check_dtype=False)
    pd.testing.assert_frame_equal(ordered.stocks, shuffled.stocks, check_dtype=False)


def test_unmapped_stock_is_retained_without_fake_sector_features() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, sector=None, ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, sector=None, ema20=99, ema50=98),
        ]
    )
    result = _engine().compute(frame)

    assert result.sectors.empty
    assert len(result.stocks) == 2
    assert result.stocks["stock_sector_mapped_flag"].eq(0).all()
    assert result.stocks["sector"].isna().all()
    assert result.stocks["stock_outperformance_score"].isna().all()


def test_latest_handles_an_all_unmapped_universe() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, sector=None, ema20=99, ema50=98),
            _row("2026-01-05 09:25", "A", 101, sector=None, ema20=99, ema50=98),
        ]
    )
    latest = _engine().latest(frame, asof=pd.Timestamp("2026-01-05 09:22", tz=TZ))

    assert latest.sectors.empty
    assert len(latest.stocks) == 1
    assert latest.stocks.iloc[0]["timestamp"] == pd.Timestamp(
        "2026-01-05 09:20", tz=TZ
    )


def test_latest_uses_stock_clock_when_ticker_becomes_unmapped() -> None:
    timestamps = pd.date_range("2026-01-05 09:20", periods=2, freq="5min", tz=TZ)
    frame = pd.DataFrame(
        [
            _row(timestamps[0], "A", 100, sector="S1", ema20=99, ema50=98),
            _row(timestamps[1], "A", 101, sector=None, ema20=99, ema50=98),
        ]
    )

    latest = _engine().latest(frame, asof=timestamps[1])

    assert latest.sectors.empty
    assert len(latest.stocks) == 1
    assert latest.stocks.iloc[0]["timestamp"] == timestamps[1]
    assert latest.stocks.iloc[0]["stock_sector_mapped_flag"] == 0


def test_point_in_time_reclassification_drives_sector_attachment() -> None:
    timestamps = pd.date_range("2026-01-05 09:20", periods=2, freq="5min", tz=TZ)
    rows = []
    memberships = (
        {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        {"A": "S2", "B": "S1", "C": "S2", "D": "S2"},
    )
    for timestamp, membership in zip(timestamps, memberships):
        for member, sector in membership.items():
            rows.append(
                _row(timestamp, member, 100.0, sector=sector, ema20=99, ema50=98)
            )
    result = _engine().compute(pd.DataFrame(rows))
    a_rows = result.stocks.loc[result.stocks["ticker"].eq("A")]
    assert a_rows["sector"].tolist() == ["S1", "S2"]

    candidates = pd.DataFrame(
        {"ticker": ["A", "A"], "signal_time_ist": timestamps}
    )
    joined = attach_sector_intelligence_asof(candidates, result)
    assert joined["si_sector_member_count"].tolist() == [2.0, 3.0]


def test_delayed_reclassification_join_uses_published_stock_sector_not_candidate() -> None:
    timestamps = pd.date_range("2026-01-05 09:20", periods=2, freq="5min", tz=TZ)
    memberships = (
        {"A": "S1", "B": "S1", "C": "S2", "D": "S2"},
        {"A": "S2", "B": "S1", "C": "S2", "D": "S2"},
    )
    rows = [
        _row(timestamp, ticker, 100.0, sector=sector, ema20=99, ema50=98)
        for timestamp, membership in zip(timestamps, memberships)
        for ticker, sector in membership.items()
    ]
    result = _engine(publish_delay_seconds=60).compute(pd.DataFrame(rows))
    decision_times = [
        pd.Timestamp("2026-01-05 09:25:30", tz=TZ),
        pd.Timestamp("2026-01-05 09:26:00", tz=TZ),
    ]
    candidates = pd.DataFrame(
        {
            "ticker": ["A", "A"],
            "sector": ["WRONG", "WRONG"],
            "signal_time_ist": decision_times,
        }
    )

    joined = attach_sector_intelligence_asof(candidates, result)

    assert joined["si_stock_timestamp"].tolist() == list(timestamps)
    assert joined["si_sector_timestamp"].tolist() == list(timestamps)
    assert joined["si_sector_member_count"].tolist() == [2.0, 3.0]
    assert (
        joined["si_stock_available_at"] <= pd.Series(decision_times)
    ).all()
    assert (
        joined["si_sector_available_at"] <= pd.Series(decision_times)
    ).all()


def test_asof_attachment_is_backward_delayed_and_preserves_candidates() -> None:
    rows = []
    timestamps = pd.date_range("2026-01-05 09:20", periods=2, freq="5min", tz=TZ)
    for timestamp in timestamps:
        for member in range(2):
            rows.append(
                _row(timestamp, f"A{member}", 100 + member, ema20=99, ema50=98)
            )
    result = _engine(publish_delay_seconds=60).compute(pd.DataFrame(rows))
    candidates = pd.DataFrame(
        {
            "candidate_id": [1, 2, 3],
            "ticker": ["A0", "A0", "A1"],
            "signal_time_ist": [
                pd.Timestamp("2026-01-05 09:20:30", tz=TZ),
                pd.Timestamp("2026-01-05 09:21:00", tz=TZ),
                pd.Timestamp("2026-01-05 09:26:00", tz=TZ),
            ],
        },
        index=[7, 2, 9],
    )
    joined = attach_sector_intelligence_asof(candidates, result)

    assert joined.index.tolist() == candidates.index.tolist()
    assert joined.index.name == candidates.index.name
    assert joined["candidate_id"].tolist() == [1, 2, 3]
    assert pd.isna(joined.loc[7, "si_stock_timestamp"])
    assert joined.loc[2, "si_stock_timestamp"] == timestamps[0]
    assert joined.loc[9, "si_stock_timestamp"] == timestamps[1]
    assert joined.loc[2, "si_stock_age_seconds"] == pytest.approx(0.0)
    assert joined.loc[9, "si_sector_timestamp"] == timestamps[1]
    assert joined.columns.is_unique
    assert not any(column.endswith(("_x", "_y")) for column in joined.columns)
    assert set(sector_intelligence_feature_columns(result)).issubset(joined.columns)


def test_asof_staleness_boundary_and_reserved_columns_are_enforced() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, ema20=99, ema50=98),
            _row("2026-01-05 09:20", "B", 101, ema20=99, ema50=98),
        ]
    )
    result = _engine().compute(frame)
    candidates = pd.DataFrame(
        {
            "ticker": ["A", "A"],
            "signal_time_ist": [
                pd.Timestamp("2026-01-05 09:27:00", tz=TZ),
                pd.Timestamp("2026-01-05 09:27:01", tz=TZ),
            ],
        }
    )
    joined = attach_sector_intelligence_asof(
        candidates, result, max_staleness_minutes=7
    )

    assert joined.loc[0, "si_stock_timestamp"] == pd.Timestamp(
        "2026-01-05 09:20", tz=TZ
    )
    assert pd.isna(joined.loc[1, "si_stock_timestamp"])
    with pytest.raises(ValueError, match="reserved"):
        attach_sector_intelligence_asof(
            candidates.assign(_si_row_order=0), result
        )


def test_asof_attachment_preserves_duplicate_multiindex_exactly() -> None:
    frame = pd.DataFrame(
        [
            _row("2026-01-05 09:20", "A", 100, ema20=99, ema50=98),
            _row("2026-01-05 09:20", "B", 101, ema20=99, ema50=98),
        ]
    )
    result = _engine().compute(frame)
    candidate_index = pd.MultiIndex.from_tuples(
        [("book", 1), ("book", 1), ("other", 2)],
        names=["source", "row_id"],
    )
    candidates = pd.DataFrame(
        {
            "ticker": ["A", "B", "A"],
            "signal_time_ist": [pd.Timestamp("2026-01-05 09:20", tz=TZ)] * 3,
        },
        index=candidate_index,
    )

    joined = attach_sector_intelligence_asof(candidates, result)

    assert joined.index.equals(candidate_index)
    assert joined.index.names == ["source", "row_id"]
    assert joined["ticker"].tolist() == candidates["ticker"].tolist()


def test_empty_candidate_attachment_still_has_the_frozen_schema() -> None:
    result = _engine().compute(
        pd.DataFrame(
            columns=["date", "ticker", "open", "high", "low", "close", "volume"]
        )
    )
    empty_index = pd.MultiIndex.from_arrays(
        [pd.Series(dtype="string"), pd.Series(dtype="int64")],
        names=["source", "row_id"],
    )
    candidates = pd.DataFrame(
        {
            "ticker": pd.Series(index=empty_index, dtype="string"),
            "signal_time_ist": pd.Series(
                index=empty_index, dtype=f"datetime64[ns, {TZ}]"
            ),
        },
        index=empty_index,
    )

    joined = attach_sector_intelligence_asof(candidates, result)

    assert joined.index.equals(empty_index)
    assert set(sector_intelligence_feature_columns(result)).issubset(joined.columns)


def test_schema_is_numeric_feature_only_versioned_and_ml_columns_prefixed() -> None:
    result = _engine().compute(_linear_panel(periods=7, sectors=("S1",), members=3))

    for frame in (result.sectors, result.stocks):
        assert frame["feature_version"].eq(SECTOR_INTELLIGENCE_VERSION).all()
        for column in frame.columns:
            if column in IDENTIFIER_COLUMNS:
                continue
            assert pd.api.types.is_numeric_dtype(frame[column]), column
            assert not any(token in column.lower() for token in FORBIDDEN_FEATURE_TOKENS)
    features = sector_intelligence_feature_columns(result)
    assert features
    assert all(column.startswith("si_") for column in features)
    assert "si_stock_outperformance_score" in features
    assert "si_sector_trend_score" in features


def test_empty_schema_and_feature_selection_are_immutable() -> None:
    empty_bars = pd.DataFrame(
        columns=["date", "ticker", "open", "high", "low", "close", "volume"]
    )
    result = _engine().compute(empty_bars)

    assert tuple(result.sectors.columns[2:-2]) == SECTOR_FEATURE_COLUMNS
    assert tuple(result.stocks.columns[3:-2]) == STOCK_FEATURE_COLUMNS
    all_features = sector_intelligence_feature_columns(
        result, include_quality_metadata=True
    )
    assert all_features == [
        *(f"si_{column}" for column in SECTOR_FEATURE_COLUMNS),
        *(f"si_{column}" for column in STOCK_FEATURE_COLUMNS),
    ]
    default_features = sector_intelligence_feature_columns(result)
    assert not {
        f"si_{column}" for column in QUALITY_FEATURE_COLUMNS
    }.intersection(default_features)

    candidates = pd.DataFrame(
        {
            "ticker": ["A"],
            "signal_time_ist": [pd.Timestamp("2026-01-05 09:20", tz=TZ)],
        }
    )
    joined = attach_sector_intelligence_asof(candidates, result)
    assert set(all_features).issubset(joined.columns)
    assert joined[all_features].isna().all().all()


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"sector_volatility_bars": 0}, "sector_volatility_bars"),
        ({"sector_acceleration_bars": -1}, "sector_acceleration_bars"),
        ({"min_sector_data_coverage": 1.1}, "min_sector_data_coverage"),
        ({"expected_sector_count": 0}, "expected_sector_count"),
        ({"min_cross_sector_coverage": 1.1}, "min_cross_sector_coverage"),
        ({"leader_percentile_threshold": 0.5}, "leader_percentile_threshold"),
    ],
)
def test_invalid_sector_configuration_is_rejected(overrides, match) -> None:
    with pytest.raises(ValueError, match=match):
        SectorIntelligenceConfig(**overrides)
