from __future__ import annotations

from pathlib import Path

import pandas as pd

import fno_equity_1m_backfill as backfill


def _base_rows(stamps: list[str]) -> pd.DataFrame:
    dates = pd.to_datetime(stamps).tz_localize("Asia/Kolkata")
    return pd.DataFrame(
        {
            "date": dates,
            "open": [100.0 + index for index in range(len(dates))],
            "high": [101.0 + index for index in range(len(dates))],
            "low": [99.0 + index for index in range(len(dates))],
            "close": [100.5 + index for index in range(len(dates))],
            "volume": [1000 + index for index in range(len(dates))],
        }
    )


def test_force_window_fetches_requested_start_instead_of_incremental_tail(
    tmp_path: Path, monkeypatch
) -> None:
    universe_path = tmp_path / "frozen_universe.parquet"
    pd.DataFrame(
        {"equity_symbol": ["ABC"], "equity_instrument_token": [12345]}
    ).to_parquet(universe_path, index=False)

    output_dir = tmp_path / "equity"
    output_dir.mkdir()
    output_path = output_dir / "ABC_stocks_indicators_1min.parquet"
    _base_rows(["2026-08-11 15:30"]).to_parquet(output_path, index=False)

    observed: dict[str, object] = {}
    fetched = _base_rows(["2026-08-12 09:16", "2026-08-13 15:30"])
    monkeypatch.setattr(backfill.stock_1m, "setup_kite_session", lambda: object())
    monkeypatch.setattr(backfill.stock_1m, "_read_holidays", lambda _path: set())
    monkeypatch.setattr(backfill.stock_1m, "get_kite_client_rr", lambda: object())
    monkeypatch.setattr(
        backfill.stock_1m,
        "process_ticker",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("force-window must not use the incremental pipeline")
        ),
    )

    def fake_fetch(client, token, start, end, logger, intraday_ts):
        observed.update(
            {
                "token": token,
                "start": start,
                "end": end,
                "intraday_ts": intraday_ts,
            }
        )
        return fetched

    monkeypatch.setattr(backfill.stock_1m, "fetch_historical_1min_df", fake_fetch)

    outcomes = backfill.run(
        ["ABC"],
        start_date="2026-08-12",
        end_date="2026-08-13",
        output_dir=output_dir,
        backup_root=tmp_path / "backup",
        universe_path=universe_path,
        force_window=True,
    )

    assert observed["token"] == 12345
    assert observed["start"].strftime("%Y-%m-%d %H:%M") == "2026-08-12 00:00"
    assert observed["end"].strftime("%Y-%m-%d %H:%M") == "2026-08-13 15:30"
    assert observed["intraday_ts"] == "end"
    assert outcomes[0]["status"] == "updated"
    assert outcomes[0]["force_window"] is True

    stored = pd.read_parquet(output_path, columns=["date"])
    assert set(pd.to_datetime(stored["date"])) == set(
        pd.to_datetime(
            [
                "2026-08-11 15:30+05:30",
                "2026-08-12 09:16+05:30",
                "2026-08-13 15:30+05:30",
            ],
            utc=True,
        ).tz_convert("Asia/Kolkata")
    )
    assert (tmp_path / "backup" / output_path.name).exists()
