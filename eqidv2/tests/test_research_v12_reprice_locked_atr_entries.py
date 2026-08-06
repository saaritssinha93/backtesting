from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

import research_v12_reprice_locked_atr_entries as reprice


def _raw_row(
    row_id: int = 1,
    *,
    signal_time: str = "2026-05-01 10:00:00+05:30",
    entry_time: str = "2026-05-01 10:01:00+05:30",
    ticker: str = "ABC",
) -> dict[str, object]:
    return {
        "_optimizer_row_id": row_id,
        "ticker": ticker,
        "setup": reprice.SETUP,
        "side": "LONG",
        "trade_date": signal_time[:10],
        "slot_ist": "2026-05-01 09:20:00+05:30",
        "selection_rank": 1,
        "context_score": 0,
        "signal_time_ist": signal_time,
        "bar_time_ist": signal_time,
        "v7_signal_entry_time_ist": entry_time,
        "v7_signal_entry_price": 100.10,
        "v7_signal_stop_price": 99.10,
        "v7_signal_target_price": 101.60,
        "v7_signal_sl_pct": 1.0,
        "v7_signal_target_pct": 1.5,
        "v7_signal_notional_rs": 100_100.0,
        "quantity": 1_000,
        "score": 250.0,
        "signal_low": 99.0,
        "signal_high": 100.5,
        "signal_close": 100.0,
        "signal_volume": 100_000.0,
        "signal_atr": 1.0,
        "range_atr": 1.5,
        "previous_return_5m_close_pct": 1.0,
        "return_5m_close_pct": 1.0,
        "impulse_atr_ratio": 2.5,
        "vwap_dist_atr": 0.0,
        "traded_value_rs": 5_000_000.0,
        "preregistered_signal": True,
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_cli_has_only_data_window_inputs_and_discovery_stays_locked() -> None:
    args = reprice.parse_args(
        [
            "--raw-entries",
            "X:/source/entry_engine_raw_entries.csv",
            "--start-date",
            "2025-06-16",
            "--end-date",
            "2026-08-04",
            "--one-minute-dir",
            "X:/one-minute",
            "--out",
            "X:/repriced",
        ]
    )

    assert args.start_date == "2025-06-16"
    assert args.end_date == "2026-08-04"
    assert not hasattr(args, "discovery_start")
    assert reprice.locked.DISCOVERY_START == "2026-06-05"
    assert reprice.locked.DISCOVERY_END == "2026-08-04"


def test_locked_input_validation_rejects_wrong_setup_and_gate_drift() -> None:
    wrong_setup = pd.DataFrame([_raw_row()])
    wrong_setup.loc[0, "setup"] = "NOT_THE_LOCKED_SETUP"
    with pytest.raises(ValueError, match="unexpected setup"):
        reprice.validate_locked_raw_entries(wrong_setup)

    gate_drift = pd.DataFrame([_raw_row()])
    gate_drift.loc[0, "impulse_atr_ratio"] = 2.49999
    with pytest.raises(ValueError, match="locked ATR signal/entry contract"):
        reprice.validate_locked_raw_entries(gate_drift)


def test_restore_uses_preserved_engine_values_and_prevents_double_slippage() -> None:
    frame = pd.DataFrame([_raw_row()])
    frame["entry_engine_raw_entry_price"] = 100.10
    frame["entry_engine_placeholder_quantity"] = 1_000
    frame["v7_signal_entry_price"] = 100.15
    frame["quantity"] = 400

    restored, fields = reprice.restore_entry_engine_fields(frame)
    repriced = reprice.base.add_execution_guards(restored)

    assert float(restored.loc[0, "v7_signal_entry_price"]) == 100.10
    assert int(restored.loc[0, "quantity"]) == 1_000
    assert "v7_signal_entry_price" in fields
    assert float(repriced.loc[0, "entry_engine_raw_entry_price"]) == 100.10
    assert float(repriced.loc[0, "entry_price_with_slippage"]) == 100.15


def test_source_manifest_hash_mismatch_is_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "entry_engine_raw_entries.csv"
    pd.DataFrame([_raw_row()]).to_csv(raw, index=False)
    (tmp_path / "integrity_manifest.json").write_text(
        json.dumps(
            {"artifacts": [{"file": raw.name, "sha256": "0" * 64}]}
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="hash differs"):
        reprice.source_provenance(raw)


def test_run_reprices_without_signal_regeneration_and_hashes_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_dir = tmp_path / "source"
    one_minute_dir = tmp_path / "one_minute"
    out = tmp_path / "out"
    source_dir.mkdir()
    one_minute_dir.mkdir()
    raw = source_dir / "entry_engine_raw_entries.csv"
    rows = pd.DataFrame(
        [
            _raw_row(1, signal_time="2026-05-01 10:00:00+05:30", entry_time="2026-05-01 10:01:00+05:30"),
            _raw_row(2, signal_time="2026-05-01 10:05:00+05:30", entry_time="2026-05-01 10:06:00+05:30"),
        ]
    )
    rows.to_csv(raw, index=False)
    source_summary = source_dir / "summary.json"
    source_summary.write_text(
        json.dumps(
            {"membership_audit": {"session_rows": [{"trade_date": "2026-05-01"}]}}
        ),
        encoding="utf-8",
    )
    (source_dir / "integrity_manifest.json").write_text(
        json.dumps(
            {
                "inputs": {"prefilter": "frozen.csv"},
                "artifacts": [
                    {"file": raw.name, "sha256": _sha256(raw)},
                    {"file": source_summary.name, "sha256": _sha256(source_summary)},
                ],
            }
        ),
        encoding="utf-8",
    )
    source_hash_before = _sha256(raw)

    monkeypatch.setattr(
        reprice.optimizer,
        "install_windowed_1m_loader",
        lambda *_args, **_kwargs: (lambda _ticker: pd.DataFrame({"close": [100.0]})),
    )
    monkeypatch.setattr(
        reprice.optimizer,
        "prewarm_windowed_1m_loader",
        lambda _loader, tickers, workers: {
            "requested": len(set(tickers)),
            "loaded": len(set(tickers)),
            "missing": 0,
            "failed": 0,
            "workers": workers,
        },
    )
    monkeypatch.setattr(
        reprice.optimizer, "install_day_1m_adapter", lambda _v12, _loader: object()
    )

    def fake_resolve(selected: pd.DataFrame, policy: object, label: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "_optimizer_row_id": int(row["_optimizer_row_id"]),
                    "ticker": row["ticker"],
                    "setup": label,
                    "trade_date": row["trade_date"],
                    "gross_pnl_rs": 100.0,
                    "cost_rs": 10.0,
                    "net_pnl_rs": 90.0,
                    "net_r": 0.9,
                    "exit_policy": policy.name,
                }
                for _, row in selected.iterrows()
            ]
        )

    monkeypatch.setattr(reprice.base, "resolve_policy", fake_resolve)
    args = reprice.parse_args(
        [
            "--raw-entries",
            str(raw),
            "--start-date",
            "2026-05-01",
            "--end-date",
            "2026-05-01",
            "--one-minute-dir",
            str(one_minute_dir),
            "--out",
            str(out),
        ]
    )

    summary = reprice.run(args)

    assert _sha256(raw) == source_hash_before
    assert summary["production_approved"] is False
    assert summary["signal_regeneration_performed"] is False
    assert summary["entry_engine_regeneration_performed"] is False
    assert summary["source_artifact"]["verification"] == (
        "verified_against_sibling_integrity_manifest"
    )
    assert summary["current_execution"]["repriced_guard_pass"] == 2
    assert summary["current_execution"]["selected_one_ticker_per_day"] == 1
    assert summary["current_execution"]["resolved_trades"] == 1
    assert summary["discovery_window"] == {
        "start": "2026-06-05",
        "end": "2026-08-04",
        "locked": True,
        "eligible_for_backward_validation": False,
    }

    repriced = pd.read_csv(out / reprice.OUTPUT_FILES["raw"])
    selected = pd.read_csv(out / reprice.OUTPUT_FILES["selected"])
    trades = pd.read_csv(out / reprice.OUTPUT_FILES["trades"])
    assert set(repriced["research_window"]) == {"backward_pre_discovery"}
    assert len(selected) == len(trades) == 1
    assert set(trades["setup"]) == {reprice.SETUP}

    manifest = json.loads(
        (out / reprice.OUTPUT_FILES["manifest"]).read_text(encoding="utf-8")
    )
    assert manifest["production_approved"] is False
    for artifact in manifest["artifacts"]:
        path = out / artifact["file"]
        assert artifact["sha256"] == _sha256(path)
