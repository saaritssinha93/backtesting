from __future__ import annotations

import unittest
from datetime import timedelta, timezone

import numpy as np
import pandas as pd

import fno_oi_rank_history as rh
from fno_oi_feature_ranker import build_contract_features, rank_feature_snapshot


IST_OFFSET = timezone(timedelta(hours=5, minutes=30))


def _raw_contract(
    symbol: str,
    underlying: str,
    *,
    expiry: str = "2026-08-25",
    contract_month: str = "26AUG",
    bars: int = 40,
    oi_step: float = 100.0,
    price_step: float = 1.0,
    start: str = "2026-08-10 09:20",
) -> pd.DataFrame:
    stamps = [
        pd.Timestamp(start).tz_localize(IST_OFFSET) + timedelta(minutes=5 * i)
        for i in range(bars)
    ]
    return pd.DataFrame(
        {
            "timestamp": stamps,
            "candle_start": [s - timedelta(minutes=5) for s in stamps],
            "underlying": underlying,
            "tradingsymbol": symbol,
            "instrument_token": abs(hash(symbol)) % 100000,
            "exchange_token": 1,
            "expiry": pd.Timestamp(expiry),
            "contract_month": contract_month,
            "days_to_expiry": 10,
            "lot_size": 250,
            "tick_size": 0.05,
            "is_index_future": False,
            "open": [100.0 + i * price_step for i in range(bars)],
            "high": [101.0 + i * price_step for i in range(bars)],
            "low": [99.0 + i * price_step for i in range(bars)],
            "close": [100.5 + i * price_step for i in range(bars)],
            "volume": [1000 + i * 10 for i in range(bars)],
            "oi": [10000.0 + i * oi_step for i in range(bars)],
            "quality_state": "VALID",
            "fetch_timestamp": stamps,
            "source": "kite_historical",
            "data_version": "fno_oi_raw_v1",
        }
    )


def _panel(frames: list[pd.DataFrame]) -> pd.DataFrame:
    built = [build_contract_features(f, **rh.LIVE_FEATURE_OPTIONS) for f in frames]
    keep = [c for c in rh.IDENTITY_COLUMNS + rh.FEATURE_COLUMNS]
    stacked = pd.concat(
        [b.loc[:, [c for c in keep if c in b.columns]] for b in built],
        ignore_index=True,
        sort=False,
    )
    return stacked


class LiveParityTests(unittest.TestCase):
    """The whole point of this module is that history == live."""

    def setUp(self):
        self.frames = [
            _raw_contract("AAA26AUGFUT", "AAA", oi_step=100.0, price_step=1.0),
            _raw_contract("BBB26AUGFUT", "BBB", oi_step=-60.0, price_step=-0.8),
            _raw_contract("CCC26AUGFUT", "CCC", oi_step=25.0, price_step=0.3),
            _raw_contract("DDD26AUGFUT", "DDD", oi_step=-5.0, price_step=1.5),
        ]
        self.panel = rh.annotate_front_month(_panel(self.frames))
        self.ranked = rh.rank_history(self.panel, cohort="month")

    def _live_slot(self, slot):
        snapshot = self.panel.loc[self.panel["timestamp"].eq(slot)].reset_index(drop=True)
        return rank_feature_snapshot(snapshot, slot.to_pydatetime())

    def test_ranks_match_live_for_every_slot(self):
        slots = sorted(self.panel["timestamp"].unique())
        compared = 0
        for slot in slots:
            live = self._live_slot(pd.Timestamp(slot))
            hist = self.ranked.loc[self.ranked["timestamp"].eq(slot)]
            if live.empty or hist.empty:
                continue
            merged = live[["tradingsymbol", "oi_rank_5m"]].merge(
                hist[["tradingsymbol", "oi_rank_5m"]],
                on="tradingsymbol",
                suffixes=("_live", "_hist"),
            )
            self.assertEqual(len(merged), len(live))
            self.assertTrue(
                np.isclose(
                    merged["oi_rank_5m_live"].astype(float),
                    merged["oi_rank_5m_hist"].astype(float),
                    equal_nan=True,
                ).all(),
                f"rank mismatch at {slot}",
            )
            compared += 1
        self.assertGreater(compared, 5)

    def test_activity_score_matches_live(self):
        slot = pd.Timestamp(sorted(self.panel["timestamp"].unique())[-1])
        live = self._live_slot(slot)
        hist = self.ranked.loc[self.ranked["timestamp"].eq(slot)]
        merged = live[["tradingsymbol", "activity_score"]].merge(
            hist[["tradingsymbol", "activity_score"]],
            on="tradingsymbol",
            suffixes=("_live", "_hist"),
        )
        self.assertTrue(
            np.isclose(
                merged["activity_score_live"],
                merged["activity_score_hist"],
                rtol=1e-9,
                equal_nan=True,
            ).all()
        )

    def test_percentiles_match_live(self):
        slot = pd.Timestamp(sorted(self.panel["timestamp"].unique())[-1])
        live = self._live_slot(slot)
        hist = self.ranked.loc[self.ranked["timestamp"].eq(slot)]
        for column in ("oi_percentile_5m", "volume_percentile", "oi_activity_percentile_5m"):
            merged = live[["tradingsymbol", column]].merge(
                hist[["tradingsymbol", column]], on="tradingsymbol", suffixes=("_l", "_h")
            )
            self.assertTrue(
                np.isclose(merged[column + "_l"], merged[column + "_h"], rtol=1e-9, equal_nan=True).all(),
                f"{column} mismatch",
            )

    def test_feature_options_mirror_the_live_bat(self):
        self.assertEqual(rh.LIVE_FEATURE_OPTIONS["min_price_move_pct"], 0.10)
        self.assertEqual(rh.LIVE_FEATURE_OPTIONS["min_oi_move_pct"], 0.25)
        self.assertEqual(rh.LIVE_TOP_N, 20)


class RankMechanicsTests(unittest.TestCase):
    def setUp(self):
        self.frames = [
            _raw_contract("AAA26AUGFUT", "AAA", oi_step=100.0),
            _raw_contract("BBB26AUGFUT", "BBB", oi_step=-60.0),
            _raw_contract("CCC26AUGFUT", "CCC", oi_step=25.0),
        ]
        self.panel = rh.annotate_front_month(_panel(self.frames))

    def test_ineligible_rows_get_no_rank(self):
        ranked = rh.rank_history(self.panel, cohort="month")
        ineligible = ranked.loc[~ranked["eligible_for_rank"].fillna(False)]
        self.assertTrue(ineligible["oi_rank_5m"].isna().all())

    def test_rank_change_is_prior_minus_current(self):
        ranked = rh.rank_history(self.panel, cohort="month").sort_values("timestamp")
        sample = ranked.loc[ranked["tradingsymbol"].eq("AAA26AUGFUT")].reset_index(drop=True)
        row = sample.loc[sample["oi_rank_change_5m"].notna()].iloc[0]
        prior_ts = row["timestamp"] - pd.Timedelta(minutes=5)
        prior = sample.loc[sample["timestamp"].eq(prior_ts), "oi_rank_5m"].iloc[0]
        self.assertAlmostEqual(
            float(row["oi_rank_change_5m"]), float(prior) - float(row["oi_rank_5m"])
        )

    def test_cohort_month_ranks_within_each_month(self):
        aug = self.frames
        sep = [
            _raw_contract("AAA26SEPFUT", "AAA", expiry="2026-09-29",
                          contract_month="26SEP", oi_step=90.0),
            _raw_contract("BBB26SEPFUT", "BBB", expiry="2026-09-29",
                          contract_month="26SEP", oi_step=-40.0),
        ]
        panel = rh.annotate_front_month(_panel(aug + sep))
        ranked = rh.rank_history(panel, cohort="month")
        slot = ranked["timestamp"].max()
        chunk = ranked.loc[ranked["timestamp"].eq(slot) & ranked["eligible_for_rank"]]
        for month, group in chunk.groupby("contract_month"):
            ranks = group["oi_rank_5m"].dropna()
            if len(ranks):
                self.assertEqual(ranks.min(), 1.0, f"{month} should start at rank 1")

    def test_cohort_all_pools_every_contract(self):
        sep = [
            _raw_contract("AAA26SEPFUT", "AAA", expiry="2026-09-29",
                          contract_month="26SEP", oi_step=90.0),
        ]
        panel = rh.annotate_front_month(_panel(self.frames + sep))
        ranked = rh.rank_history(panel, cohort="all")
        slot = ranked["timestamp"].max()
        sizes = ranked.loc[ranked["timestamp"].eq(slot), "cohort_size"].unique()
        self.assertEqual(list(sizes), [4])

    def test_cohort_size_recorded(self):
        ranked = rh.rank_history(self.panel, cohort="month")
        slot = ranked["timestamp"].max()
        self.assertEqual(
            int(ranked.loc[ranked["timestamp"].eq(slot), "cohort_size"].iloc[0]), 3
        )


class FrontMonthTests(unittest.TestCase):
    def _three_month_panel(self):
        frames = [
            _raw_contract("AAA26AUGFUT", "AAA", expiry="2026-08-25",
                          contract_month="26AUG", start="2026-08-10 09:20"),
            _raw_contract("AAA26SEPFUT", "AAA", expiry="2026-09-29",
                          contract_month="26SEP", start="2026-08-10 09:20"),
            _raw_contract("AAA26OCTFUT", "AAA", expiry="2026-10-27",
                          contract_month="26OCT", start="2026-08-10 09:20"),
        ]
        return _panel(frames)

    def test_front_month_is_the_nearest_unexpired(self):
        annotated = rh.annotate_front_month(self._three_month_panel())
        on_day = annotated.loc[annotated["contract_month"].eq("26AUG")]
        self.assertTrue(on_day["is_front_month"].all())
        later = annotated.loc[annotated["contract_month"].eq("26SEP")]
        self.assertFalse(later["is_front_month"].any())

    def test_contract_rank_on_date_orders_the_curve(self):
        annotated = rh.annotate_front_month(self._three_month_panel())
        ranks = (
            annotated.groupby("contract_month")["contract_rank_on_date"].first().to_dict()
        )
        self.assertEqual(ranks["26AUG"], 1)
        self.assertEqual(ranks["26SEP"], 2)
        self.assertEqual(ranks["26OCT"], 3)

    def test_cohort_front_keeps_only_front_rows(self):
        annotated = rh.annotate_front_month(self._three_month_panel())
        ranked = rh.rank_history(annotated, cohort="front")
        self.assertTrue(ranked["is_front_month"].all())
        self.assertEqual(set(ranked["contract_month"]), {"26AUG"})


class DiscoveryTests(unittest.TestCase):
    def test_contract_month_parsed_from_filename(self):
        from pathlib import Path

        self.assertEqual(rh.contract_month_of(Path("RELIANCE26AUGFUT_5minute.parquet")), "26AUG")
        self.assertEqual(rh.contract_month_of(Path("360ONE26SEPFUT_5minute.parquet")), "26SEP")

    def test_unparseable_name_is_flagged(self):
        from pathlib import Path

        self.assertEqual(rh.contract_month_of(Path("weird_name.parquet")), "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
