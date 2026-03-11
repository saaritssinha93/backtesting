from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import avwap_combined_runner_v11 as runner


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _patch_long_config() -> None:
    original = runner.default_long_config

    def patched_default_long_config(**kwargs):
        cfg = original(**kwargs)
        cfg.enable_ema200_filter = _env_bool("V11_TMP_LONG_ENABLE_EMA200", cfg.enable_ema200_filter)
        cfg.require_vwap_side_persistence = _env_bool(
            "V11_TMP_LONG_REQUIRE_VWAP_PERSIST",
            cfg.require_vwap_side_persistence,
        )
        cfg.require_structure_filter = _env_bool(
            "V11_TMP_LONG_REQUIRE_STRUCTURE",
            cfg.require_structure_filter,
        )
        cfg.enable_setup_a_pullback_c2_break = _env_bool(
            "V11_TMP_LONG_ENABLE_A_PULLBACK",
            cfg.enable_setup_a_pullback_c2_break,
        )
        cfg.enable_setup_a_close_continuation_break = _env_bool(
            "V11_TMP_LONG_ENABLE_A_CLOSE_CONT",
            cfg.enable_setup_a_close_continuation_break,
        )
        cfg.enable_setup_b_huge_c1_close_reclaim_break = _env_bool(
            "V11_TMP_LONG_ENABLE_B_RECLAIM",
            cfg.enable_setup_b_huge_c1_close_reclaim_break,
        )
        cfg.adx_min = _env_float("V11_TMP_LONG_ADX_MIN", cfg.adx_min)
        cfg.adx_slope_min = _env_float("V11_TMP_LONG_ADX_SLOPE_MIN", cfg.adx_slope_min)
        cfg.volume_min_ratio = _env_float("V11_TMP_LONG_VOLUME_MIN_RATIO", cfg.volume_min_ratio)
        cfg.rsi_min_long = _env_float("V11_TMP_LONG_RSI_MIN", cfg.rsi_min_long)
        cfg.stochk_min = _env_float("V11_TMP_LONG_STOCHK_MIN", cfg.stochk_min)
        cfg.stochk_max = _env_float("V11_TMP_LONG_STOCHK_MAX", cfg.stochk_max)
        cfg.stop_pct = _env_float("V11_TMP_LONG_STOP_PCT", cfg.stop_pct)
        cfg.target_pct = _env_float("V11_TMP_LONG_TARGET_PCT", cfg.target_pct)
        cfg.be_trigger_pct = _env_float("V11_TMP_LONG_BE_TRIGGER_PCT", cfg.be_trigger_pct)
        cfg.trail_pct = _env_float("V11_TMP_LONG_TRAIL_PCT", cfg.trail_pct)
        return cfg

    runner.default_long_config = patched_default_long_config


def _load_temp_long_module():
    here = Path(__file__).resolve().parent
    mod_path = here / "avwap_long_strategy_v11_antichase.py"
    spec = importlib.util.spec_from_file_location("avwap_long_strategy_v11_antichase_tmp", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load temporary long strategy from {mod_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    temp_long = _load_temp_long_module()
    _patch_long_config()
    runner.MAX_WORKERS = int(os.getenv("V11_TMP_MAX_WORKERS", "1"))
    runner.TEST_TARGET_OVERRIDE = False
    runner.POSITION_SIZE_RS_SHORT = 50_000
    runner.POSITION_SIZE_RS_LONG = 100_000
    runner.FINAL_SIGNAL_WINDOW_OVERRIDE = True
    runner.FINAL_SHORT_USE_TIME_WINDOWS = True
    runner.FINAL_LONG_USE_TIME_WINDOWS = True
    runner.FINAL_SHORT_SIGNAL_WINDOWS = [(runner.dtime(9, 15, 0), runner.dtime(14, 30, 0))]
    runner.FINAL_LONG_SIGNAL_WINDOWS = [(runner.dtime(9, 15, 0), runner.dtime(14, 30, 0))]
    runner.scan_long = temp_long.scan_all_days_for_ticker
    print(
        "[TMP] Long anti-chase sandbox active | "
        f"variant={os.getenv('V11_TMP_VARIANT_NAME', 'unnamed')} | "
        f"max_workers={runner.MAX_WORKERS} | "
        f"trigger_atr={temp_long.LONG_ANTICHASE_MAX_TRIGGER_ATR:.2f} | "
        f"overshoot_atr={temp_long.LONG_ANTICHASE_MAX_ENTRY_OVERSHOOT_ATR:.2f} | "
        f"avwap_gap_atr={temp_long.LONG_ANTICHASE_MAX_AVWAP_DIST_ATR:.2f} | "
        f"ema20_gap_atr={temp_long.LONG_ANTICHASE_MAX_EMA20_GAP_ATR:.2f} | "
        f"huge_pullback_lookahead={temp_long.LONG_ANTICHASE_MAX_HUGE_PULLBACK_LOOKAHEAD_BARS} | "
        f"reclaim_lookahead={temp_long.LONG_ANTICHASE_MAX_HUGE_RECLAIM_LOOKAHEAD_BARS}"
    )
    runner.main()


if __name__ == "__main__":
    main()
