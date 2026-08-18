from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import avwap_5min_ID_v11_backtesting as v11


@pytest.mark.parametrize(
    ("start_date", "end_date", "expected_ids", "expected_dates"),
    [
        ("2026-06-02", "2026-06-05", ["inside"], ["2026-06-03"]),
        (
            "2026-06-03",
            "",
            ["inside", "after"],
            ["2026-06-03", "2026-06-06"],
        ),
        (
            "",
            "2026-06-03",
            ["before", "inside"],
            ["2026-06-01", "2026-06-03"],
        ),
        (
            "",
            "",
            ["before", "inside", "after"],
            ["2026-06-01", "2026-06-03", "2026-06-06"],
        ),
    ],
)
def test_cached_replay_filters_before_profile_and_exit_resolution(
    tmp_path,
    monkeypatch,
    start_date: str,
    end_date: str,
    expected_ids: list[str],
    expected_dates: list[str],
) -> None:
    source_dir = tmp_path / "cached"
    source_dir.mkdir()
    pd.DataFrame(
        {
            "candidate_id": ["before", "inside", "after"],
            "signal_time_ist": [
                "2026-06-01 09:35:00+05:30",
                "2026-06-03 10:05:00+05:30",
                "2026-06-06 11:25:00+05:30",
            ],
            "ticker": ["AAA", "BBB", "CCC"],
            "side": ["LONG", "LONG", "LONG"],
            "setup": ["TEST", "TEST", "TEST"],
        }
    ).to_csv(source_dir / "all_setups_entry_engine_signals.csv", index=False)

    seen: dict[str, list[str]] = {}

    def fake_profile(frame: pd.DataFrame, _profile: str):
        seen["profile"] = frame["candidate_id"].tolist()
        return frame.copy(), frame.iloc[0:0].copy(), {}

    def fake_resolve(frame: pd.DataFrame, **_kwargs) -> pd.DataFrame:
        seen["resolve"] = frame["candidate_id"].tolist()
        return pd.DataFrame()

    def fake_addon(**kwargs):
        seen["tier123_dates"] = list(kwargs["dates"])
        return kwargs["base_final"], pd.DataFrame()

    monkeypatch.setattr(v11, "_apply_selected_strategy_profile", fake_profile)
    monkeypatch.setattr(v11, "_resolve_v7_entry_engine_signals", fake_resolve)
    monkeypatch.setattr(v11, "_apply_tier123_balanced_addon", fake_addon)
    monkeypatch.setattr(v11, "_write_empty_outputs", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        v11,
        "_available_historical_dates_with_roots",
        lambda *_args, **_kwargs: ([], {}),
    )

    args = SimpleNamespace(
        out=str(tmp_path / "output"),
        cached_all_setups_dir=str(source_dir),
        candidate_5m_dir=str(tmp_path / "candidate_5m"),
        fallback_candidate_5m_dir="",
        start_date=start_date,
        end_date=end_date,
        selected_strategy_profile="none",
        entry_fill_model="next_1m_open",
    )

    assert v11._run_historical_cached_all_setups(args) == 0
    assert seen["profile"] == expected_ids
    assert seen["resolve"] == expected_ids
    assert seen["tier123_dates"] == expected_dates
