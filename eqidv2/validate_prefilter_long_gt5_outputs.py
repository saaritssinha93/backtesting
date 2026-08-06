"""Independent deterministic checks for the six-month LONG setup artifacts."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd


BASE = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\prefilter_long_5m_gt5pct_20260205_20260804"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def timestamps(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=True).dt.tz_convert(
            "Asia/Kolkata"
        )


def boolean(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values.dtype):
        return values.fillna(False).astype(bool)
    return values.astype(str).str.lower().eq("true")


def row_keys(frame: pd.DataFrame) -> set[tuple[str, str, str, str]]:
    return set(
        zip(
            frame["trade_date"].astype(str),
            frame["ticker"].astype(str),
            frame["membership_slot_ist"].astype(str),
            frame["signal_time_ist"].astype(str),
        )
    )


def main() -> int:
    summary = json.loads((BASE / "summary.json").read_text(encoding="utf-8"))
    cache = pd.read_parquet(BASE / "causal_entry_opportunities_v2.parquet")
    full = pd.read_csv(BASE / "all_long_prefilter_entries_with_daily_max.csv", low_memory=False)
    setup = pd.read_csv(BASE / "setup_entries_with_daily_max.csv", low_memory=False)
    movers = pd.read_csv(BASE / "gt5pct_movers_full_list.csv", low_memory=False)
    setup_movers = pd.read_csv(BASE / "setup_gt5pct_movers.csv", low_memory=False)
    compact_setup = pd.read_csv(BASE / "setup_entry_and_peak_times_compact.csv", low_memory=False)
    compact_setup_movers = pd.read_csv(
        BASE / "setup_gt5pct_movers_entry_and_peak_times.csv", low_memory=False
    )
    compact_full_movers = pd.read_csv(
        BASE / "all_long_gt5pct_movers_entry_and_peak_times.csv", low_memory=False
    )

    time_columns = [
        "membership_slot_ist",
        "signal_time_ist",
        "entry_execution_time_ist",
        "entry_price_source_bar_end_ist",
        "daily_max_bar_end_ist",
        "daily_max_interval_start_ist",
        "daily_max_interval_end_ist",
        "daily_max_time_ist",
        "first_hit_5pct_time_ist",
        "first_hit_5pct_bar_end_ist",
        "first_hit_5pct_interval_start_ist",
        "first_hit_5pct_interval_end_ist",
    ]
    timestamps(full, time_columns)
    timestamps(setup, time_columns)
    for frame in (cache,):
        for column in (
            "membership_slot_ist",
            "signal_time_ist",
            "entry_execution_time_ist",
            "entry_price_source_bar_end_ist",
        ):
            if frame[column].dt.tz is None:
                frame[column] = frame[column].dt.tz_localize("Asia/Kolkata")
            else:
                frame[column] = frame[column].dt.tz_convert("Asia/Kolkata")

    checks: dict[str, object] = {}
    failures: list[str] = []

    def require(name: str, condition: bool, detail: str = "") -> None:
        checks[name] = bool(condition)
        if not condition:
            failures.append(f"{name}: {detail}")

    require("cache_rows", len(cache) == 440_837, f"observed {len(cache)}")
    complete = cache.loc[boolean(cache["max_window_complete"])].copy()
    require("complete_cache_rows", len(complete) == 431_476, f"observed {len(complete)}")
    require("full_reference_rows", len(full) == 36_633, f"observed {len(full)}")
    require("setup_rows", len(setup) == 5_639, f"observed {len(setup)}")
    require("full_mover_rows", len(movers) == 1_007, f"observed {len(movers)}")
    require("setup_mover_rows", len(setup_movers) == 472, f"observed {len(setup_movers)}")
    require("compact_setup_rows", len(compact_setup) == len(setup))
    require("compact_setup_mover_rows", len(compact_setup_movers) == len(setup_movers))
    require("compact_full_mover_rows", len(compact_full_movers) == len(movers))

    for name, frame in (("full", full), ("setup", setup)):
        require(f"{name}_long_only", frame["primary_side"].astype(str).eq("LONG").all())
        require(
            f"{name}_rank_200_300",
            pd.to_numeric(frame["selection_rank"], errors="coerce")
            .between(200, 300, inclusive="both")
            .all(),
        )
        require(
            f"{name}_one_entry_ticker_day",
            not frame.duplicated(["trade_date", "ticker"]).any(),
        )
        signal_offset = (
            frame["signal_time_ist"] - frame["membership_slot_ist"]
        ) / pd.Timedelta(minutes=1)
        require(
            f"{name}_active_pool_schedule",
            signal_offset.between(5, 60, inclusive="both").all()
            and np.isclose(signal_offset % 5, 0).all(),
        )
        require(
            f"{name}_execution_boundary",
            frame["entry_execution_time_ist"].eq(frame["signal_time_ist"]).all(),
        )
        require(
            f"{name}_following_5m_open_label",
            frame["entry_price_source_bar_end_ist"]
            .sub(frame["entry_execution_time_ist"])
            .eq(pd.Timedelta(minutes=5))
            .all(),
        )
        require(
            f"{name}_same_day_peak",
            frame["daily_max_bar_end_ist"].dt.strftime("%Y-%m-%d").eq(frame["trade_date"]).all(),
        )
        require(
            f"{name}_peak_after_entry",
            frame["daily_max_bar_end_ist"].gt(frame["entry_execution_time_ist"]).all(),
        )
        require(
            f"{name}_peak_by_1530",
            frame["daily_max_bar_end_ist"].dt.strftime("%H:%M").le("15:30").all(),
        )
        interval_minutes = (
            frame["daily_max_interval_end_ist"] - frame["daily_max_interval_start_ist"]
        ) / pd.Timedelta(minutes=1)
        expected_minutes = frame["daily_max_time_source"].map(
            {"1min": 1.0, "5min_fallback": 5.0}
        )
        require(
            f"{name}_peak_interval_resolution",
            np.isclose(interval_minutes, expected_minutes, equal_nan=False).all(),
        )
        require(
            f"{name}_peak_time_is_interval_start",
            frame["daily_max_time_ist"].eq(frame["daily_max_interval_start_ist"]).all(),
        )
        recomputed_return = (
            pd.to_numeric(frame["daily_max_price"], errors="coerce")
            / pd.to_numeric(frame["entry_price"], errors="coerce")
            - 1.0
        ) * 100.0
        require(
            f"{name}_max_return_reconciles",
            np.allclose(
                recomputed_return,
                pd.to_numeric(frame["max_forward_return_pct"], errors="coerce"),
                atol=1e-9,
                rtol=0.0,
            ),
        )
        expected_hit = recomputed_return.ge(5.0 - 1e-9)
        require(f"{name}_hit_label_reconciles", expected_hit.eq(boolean(frame["hit_5pct"])).all())
        hit_rows = frame.loc[boolean(frame["hit_5pct"])]
        miss_rows = frame.loc[~boolean(frame["hit_5pct"])]
        require(f"{name}_hit_time_present", hit_rows["first_hit_5pct_bar_end_ist"].notna().all())
        require(f"{name}_miss_hit_time_absent", miss_rows["first_hit_5pct_bar_end_ist"].isna().all())

    complete = complete.sort_values(
        ["trade_date", "ticker", "entry_execution_time_ist", "membership_slot_ist"],
        kind="mergesort",
    )
    expected_full = complete.drop_duplicates(["trade_date", "ticker"], keep="first")
    require("full_is_earliest_complete_reference", row_keys(full) == row_keys(expected_full))

    threshold = float(summary["filters"][0]["value"])
    setup_mask = pd.to_numeric(complete["atr_pct"], errors="coerce").ge(threshold)
    expected_setup = complete.loc[setup_mask].drop_duplicates(
        ["trade_date", "ticker"], keep="first"
    )
    require("setup_is_earliest_frozen_rule_match", row_keys(setup) == row_keys(expected_setup))
    require(
        "setup_filter_applied",
        pd.to_numeric(setup["atr_pct"], errors="coerce").ge(threshold).all(),
    )
    require("full_movers_exact_subset", row_keys(movers) == row_keys(full.loc[boolean(full["hit_5pct"])]))
    require(
        "setup_movers_exact_subset",
        row_keys(setup_movers) == row_keys(setup.loc[boolean(setup["hit_5pct"])]),
    )

    for split, expected in summary["metrics_by_split"].items():
        frame = setup.loc[setup["split"].eq(split)]
        require(f"{split}_entries", len(frame) == int(expected["entries"]))
        require(
            f"{split}_hits",
            int(boolean(frame["hit_5pct"]).sum()) == int(expected["hits_5pct"]),
        )

    config_path = BASE / "prefilter_long_5m_gt5pct_conf.py"
    spec = importlib.util.spec_from_file_location("frozen_long_conf", config_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    require("config_not_production_approved", module.PRODUCTION_APPROVED is False)
    require("config_research_only", module.RESEARCH_ONLY is True)
    require("config_acceptance_passed", module.ACCEPTANCE["passed"] is True)
    require("config_filter_matches_threshold", module.matches({"atr_pct": threshold}))
    require("config_filter_rejects_below", not module.matches({"atr_pct": threshold - 1e-9}))

    output_files = [
        "summary.json",
        "prefilter_long_5m_gt5pct_conf.py",
        "all_long_prefilter_entries_with_daily_max.csv",
        "gt5pct_movers_full_list.csv",
        "setup_entries_with_daily_max.csv",
        "setup_gt5pct_movers.csv",
        "setup_entry_and_peak_times_compact.csv",
        "setup_gt5pct_movers_entry_and_peak_times.csv",
        "all_long_gt5pct_movers_entry_and_peak_times.csv",
        "indicator_ranges.csv",
        "hourly_summary.csv",
        "daily_summary.csv",
        "monthly_summary.csv",
    ]
    report = {
        "status": "PASS" if not failures else "FAIL",
        "checks_run": len(checks),
        "checks_passed": sum(bool(value) for value in checks.values()),
        "failures": failures,
        "checks": checks,
        "artifact_sha256": {name: sha256(BASE / name) for name in output_files},
    }
    (BASE / "integrity_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
