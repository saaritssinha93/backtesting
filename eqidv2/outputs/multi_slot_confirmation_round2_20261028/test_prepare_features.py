"""Synthetic causal/semantic checks without reading any backtest outcomes."""
import importlib.util
import json
from pathlib import Path
import unittest

import numpy as np
import pandas as pd

DIRECTORY = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("round2_features", DIRECTORY / "prepare_features.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def fixture(closes=None):
    closes = np.arange(100, 116, dtype=float) if closes is None else np.asarray(closes, dtype=float)
    return pd.DataFrame({"ts": pd.date_range("2026-10-27 09:16", periods=16, freq="min", tz=MODULE.IST),
        "open": closes-.2, "high": closes+.1, "low": closes-.3,
        "close": closes, "volume": 100.0})


def row_for(raw, side="LONG"):
    current = raw.iloc[15]
    return {"day": "2026-10-27", "setup_id": "0931_"+side, "side": side,
            "confirmation_ts": current.ts,
            **{"confirmation_"+field: float(current[field]) for field in MODULE.FIELDS[:4]}}


def mirrored(raw):
    out = raw.copy()
    for field in ("open", "close"):
        out[field] = 300 - raw[field]
    out["high"], out["low"] = 300-raw.low, 300-raw.high
    return out


class ConfirmationFeatures(unittest.TestCase):
    def test_direction_mirror(self):
        raw = fixture(np.linspace(100, 100.3, 16))
        left = MODULE.feature_row(raw, row_for(raw))
        reflected = mirrored(raw)
        right = MODULE.feature_row(reflected, row_for(reflected, "SHORT"))
        for name in ("r2_directional_ema5_13_spread", "r2_directional_ema5_change1",
                     "r2_directional_close_change10", "r2_directional_close_location",
                     "r2_efficiency10"):
            self.assertAlmostEqual(left[name], right[name], places=10)
        for name in MODULE.RULES:
            self.assertEqual(left[name], right[name], name)

    def test_exact_volume_boundaries_and_current_excluded(self):
        for ratio in (1., 1.5):
            raw = fixture()
            raw.loc[15, "volume"] = 100*ratio
            result = MODULE.feature_row(raw, row_for(raw))
            self.assertEqual(result["r2_confirmation_volume_ratio10"], ratio)
            self.assertEqual(result["r2_prior10_volume_mean"], 100.)
            self.assertTrue(result["r2_volume_ge_1p0"])
            self.assertEqual(result["r2_volume_ge_1p5"], ratio == 1.5)
        raw.loc[15, "volume"] = 149.999
        self.assertFalse(MODULE.feature_row(raw, row_for(raw))["r2_volume_ge_1p5"])

    def test_session_reset_ema_and_one_minute_slope(self):
        raw = fixture()
        result = MODULE.feature_row(raw, row_for(raw))
        self.assertTrue(result["r2_ema5_13_aligned"])
        self.assertAlmostEqual(result["r2_ema13"], raw.close.ewm(span=13, adjust=False, min_periods=13).mean().iloc[-1])
        prior_day = raw.copy()
        prior_day.ts -= pd.Timedelta(days=1)
        for field in MODULE.FIELDS[:4]:
            prior_day[field] *= 100
        self.assertEqual(result, MODULE.feature_row(pd.concat([prior_day, raw]), row_for(raw)))
        flat = fixture(np.repeat(100., 16))
        self.assertFalse(MODULE.feature_row(flat, row_for(flat))["r2_ema5_13_aligned"])

    def test_vwap_inclusive_zero_and_upper_boundaries(self):
        for distance in (0., .5, 1.):
            raw = fixture(np.repeat(100., 16))
            raw[["open", "high", "low", "close"]] = [100., 101., 99., 100.]
            # Every HLC3 is exactly100 and all volumes are100. Last HLC3 is
            # held constant while the confirmation close is on the boundary.
            raw.loc[15, ["open", "high", "low", "close"]] = [100., 102., 98.-distance, 100.+distance]
            result = MODULE.feature_row(raw, row_for(raw))
            self.assertEqual(result["r2_session_vwap"], 100.)
            self.assertEqual(result["r2_directional_vwap_distance_pct"], distance)
            self.assertEqual(result["r2_vwap_band_0p5"], distance <= .5)
            self.assertTrue(result["r2_vwap_band_1p0"])
        raw.loc[15, ["low", "close"]] = [96.999, 101.001]
        self.assertFalse(MODULE.feature_row(raw, row_for(raw))["r2_vwap_band_1p0"])

    def test_efficiency_ten_changes_boundary_and_flat(self):
        changes = [1., 1., 1., 1., 1., 1., .5, -1., -1., -1.5]
        closes = np.r_[np.repeat(100., 6), 100.+np.cumsum(changes)]
        raw = fixture(closes)
        result = MODULE.feature_row(raw, row_for(raw))
        self.assertEqual(result["r2_total_absolute_close_change10"], 10.)
        self.assertEqual(result["r2_efficiency10"], .3)
        self.assertTrue(result["r2_efficiency10_ge_0p3"])
        raw.loc[15, list(MODULE.FIELDS[:4])] -= .001
        self.assertFalse(MODULE.feature_row(raw, row_for(raw))["r2_efficiency10_ge_0p3"])
        flat = fixture(np.repeat(100.,16))
        result = MODULE.feature_row(flat, row_for(flat))
        self.assertEqual(result["r2_efficiency10"], 0.)
        self.assertFalse(result["r2_efficiency10_ge_0p3"])

    def test_strict_breakout_and_close_location_boundary(self):
        raw = fixture(np.repeat(100.,16))
        raw.loc[12:14, "high"] = 108.
        raw.loc[15, ["open", "high", "low", "close"]] = [107.,110.,100.,108.]
        result = MODULE.feature_row(raw, row_for(raw))
        self.assertEqual(result["r2_directional_close_location"], .8)
        self.assertTrue(result["r2_close_location_ge_0p8"])
        self.assertFalse(result["r2_breakout3"])
        raw.loc[15, "close"] = 108.001
        self.assertTrue(MODULE.feature_row(raw, row_for(raw))["r2_breakout3"])
        raw.loc[15, "close"] = 107.999
        self.assertFalse(MODULE.feature_row(raw, row_for(raw))["r2_close_location_ge_0p8"])

    def test_mutated_post_confirmation_is_ignored(self):
        raw = fixture()
        row = row_for(raw)
        expected = MODULE.feature_row(raw, row)
        future = raw.iloc[-1:].copy()
        future["ts"] += pd.Timedelta(minutes=1)
        future["volume"] = 1e18
        future["gap_filled"] = True
        # Even malformed future OHLC must have no effect on an earlier feature.
        future[["open", "high", "low", "close"]] = [1e9, -1., 1e20, np.nan]
        self.assertEqual(expected, MODULE.feature_row(pd.concat([raw,future]),row))

    def test_corrupt_required_prefix_fails(self):
        raw = fixture()
        row = row_for(raw)
        with self.assertRaises(ValueError):
            MODULE.feature_row(raw.drop(index=5), row)
        with self.assertRaises(ValueError):
            MODULE.feature_row(pd.concat([raw, raw.iloc[[5]]]), row)
        flagged = raw.copy()
        flagged["provisional_stale"] = False
        flagged.loc[5,"provisional_stale"] = True
        with self.assertRaises(ValueError):
            MODULE.feature_row(flagged, row)
        zero = raw.copy()
        zero.loc[5:14,"volume"] = 0.
        with self.assertRaises(ValueError):
            MODULE.feature_row(zero, row_for(zero))
        bad_row = dict(row, confirmation_close=row["confirmation_close"]+.002)
        with self.assertRaises(ValueError):
            MODULE.feature_row(raw, bad_row)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConfirmationFeatures)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    payload = {"tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
               "passed": result.wasSuccessful(), "outcomes_read": False,
               "source_code_sha256": MODULE.sha256(DIRECTORY/"prepare_features.py")}
    (DIRECTORY / "feature_qa.json").write_text(json.dumps(payload, indent=2)+"\n", encoding="utf-8")
    raise SystemExit(0 if result.wasSuccessful() else 1)
