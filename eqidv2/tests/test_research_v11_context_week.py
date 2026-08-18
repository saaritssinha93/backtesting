from __future__ import annotations

import numpy as np
import pandas as pd

from market_context_engine import MarketContextResult
from research_v11_context_week import (
    _candidate_checksum,
    chronological_shadow_audit,
    enrich_candidates,
    feature_audit,
    trade_metrics,
)
from sector_intelligence import SectorIntelligenceResult


TZ = "Asia/Kolkata"


def _trades() -> pd.DataFrame:
    rows = []
    for day_index, day in enumerate(
        pd.bdate_range("2026-07-31", periods=5)
    ):
        for rank, pnl in ((0.0, -100.0), (1.0, 200.0)):
            timestamp = pd.Timestamp(f"{day.date()} 10:00", tz=TZ)
            rows.append(
                {
                    "candidate_id": f"{day.date()}-{rank}",
                    "signal_time_ist": timestamp,
                    "ticker": f"A{int(rank)}",
                    "side": "LONG",
                    "setup": "A_TEST",
                    "feature": rank + 0.01 * day_index,
                    "v6_net_pnl_rs": pnl,
                }
            )
    return pd.DataFrame(rows)


def test_trade_metrics_and_checksum_are_population_stable() -> None:
    trades = _trades()
    metrics = trade_metrics(trades, "v6_net_pnl_rs")

    assert metrics["trades"] == 10
    assert metrics["net_pnl_rs"] == 500.0
    assert metrics["profit_factor"] == 2.0
    assert metrics["win_rate_pct"] == 50.0
    assert _candidate_checksum(trades) == _candidate_checksum(trades.copy())
    assert _candidate_checksum(trades.iloc[::-1]) != _candidate_checksum(trades)


def test_feature_audit_is_descriptive_and_does_not_filter_rows() -> None:
    trades = _trades()
    audit = feature_audit(
        trades,
        ["feature"],
        pnl_col="v6_net_pnl_rs",
        group_columns=("side", "setup"),
    )

    overall = audit.loc[audit["group"].eq("ALL")].iloc[0]
    assert overall["group_rows"] == len(trades)
    assert overall["valid_rows"] == len(trades)
    assert overall["coverage_pct"] == 100.0
    assert overall["spearman_pnl"] > 0
    assert overall["high_average_pnl_rs"] > overall["low_average_pnl_rs"]


def test_chronological_shadow_freezes_first_three_days_for_last_two() -> None:
    trades = _trades()
    audit, manifest = chronological_shadow_audit(
        trades,
        ["feature"],
        pnl_col="v6_net_pnl_rs",
        minimum_train_rows=6,
        minimum_holdout_rows=2,
    )

    assert manifest["discovery_sessions"] == [
        "2026-07-31",
        "2026-08-03",
        "2026-08-04",
    ]
    assert manifest["holdout_sessions"] == ["2026-08-05", "2026-08-06"]
    overall = audit.loc[
        audit["side"].eq("LONG") & audit["feature"].eq("feature")
    ].iloc[0]
    assert overall["direction"] == "HIGH"
    assert overall["train_rows"] == 6
    assert overall["holdout_feature_valid_rows"] == 4
    assert overall["holdout_selected_rows"] == 2
    assert np.isclose(overall["holdout_selected_average_pnl_rs"], 200.0)


def test_enrichment_uses_published_snapshot_without_changing_population() -> None:
    timestamp = pd.Timestamp("2026-08-06 10:00", tz=TZ)
    available_at = timestamp + pd.Timedelta(seconds=60)
    market = MarketContextResult(
        market=pd.DataFrame(
            {
                "timestamp": [timestamp],
                "available_at": [available_at],
                "feature_version": ["test"],
                "market_breadth": [0.25],
            }
        ),
        sectors=pd.DataFrame(),
    )
    sector = SectorIntelligenceResult(
        stocks=pd.DataFrame(
            {
                "timestamp": [timestamp],
                "available_at": [available_at],
                "ticker": ["ABC"],
                "sector": ["IT"],
                "feature_version": ["test"],
                "stock_sector_mapped_flag": [1.0],
            }
        ),
        sectors=pd.DataFrame(
            {
                "timestamp": [timestamp],
                "available_at": [available_at],
                "sector": ["IT"],
                "feature_version": ["test"],
                "sector_trend_score": [0.4],
            }
        ),
    )
    candidate = pd.DataFrame(
        {
            "candidate_id": ["c1"],
            "ticker": ["ABC"],
            "side": ["LONG"],
            "setup": ["TEST"],
            "signal_time_ist": [timestamp],
        }
    )

    enriched = enrich_candidates(
        candidate,
        market_context=market,
        sector_context=sector,
        sector_map={"ABC": "IT"},
        fallback_decision_delay_seconds=60,
    )

    assert len(enriched) == len(candidate)
    assert _candidate_checksum(enriched) == _candidate_checksum(candidate)
    assert enriched.loc[0, "context_decision_time_ist"] == available_at
    assert enriched.loc[0, "mce_available_at"] <= available_at
    assert enriched.loc[0, "si_stock_available_at"] <= available_at
    assert enriched.loc[0, "mce_market_breadth"] == 0.25
