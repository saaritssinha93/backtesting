from datetime import date
from pathlib import Path
import json

import pandas as pd
import pytest

from tools.extend_v13_v10_g_dated_results import (
    publish, require_trade_source, require_zero_complete, verified_dataset_lineage,
)
import fno_v13_v10_g_2_backtest as g2


def inputs():
    day = date(2026, 10, 9)
    source = dict(state="SUCCESS", complete=True, session_date=str(day),
        coverage=dict(universe_stocks=1, included_stocks=1, checked_stocks=1, excluded_stocks=[], problems=[]))
    pool = pd.DataFrame([dict(day=day, tradingsymbol="STOCK", signal_ts=pd.Timestamp("2026-10-09T04:00:00Z"))])
    return day, source, pool, pool.iloc[:0].copy()


def test_completed_zero_session_requires_real_complete_source():
    day, source, pool, orders = inputs()
    require_zero_complete(source, pool, orders, day)
    source["complete"] = False
    with pytest.raises(ValueError, match="completed same-day"):
        require_zero_complete(source, pool, orders, day)


def test_nonzero_selection_cannot_be_published_as_zero():
    day, source, pool, _ = inputs()
    with pytest.raises(ValueError, match="zero-order"):
        require_zero_complete(source, pool, pool, day)


@pytest.mark.parametrize("field", ["included_stocks", "checked_stocks"])
def test_partial_coverage_cannot_be_published_as_zero(field):
    day, source, pool, orders = inputs()
    source["coverage"][field] = 0
    with pytest.raises(ValueError, match="complete universe"):
        require_zero_complete(source, pool, orders, day)


def test_duplicated_features_are_rejected():
    day, source, pool, orders = inputs()
    with pytest.raises(ValueError, match="Duplicate"):
        require_zero_complete(source, pd.concat([pool, pool]), orders, day)


def test_dated_trade_accepts_only_disclosed_missing_oi_exclusion():
    day, source, pool, _ = inputs()
    source["coverage"] = dict(
        universe_stocks=2, included_stocks=1, checked_stocks=2, problems=[],
        excluded_stocks=[dict(symbol="OTHER", reasons=["MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"])],
        ignored_problems=[dict(symbol="OTHER", reason="MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR")],
    )
    require_trade_source(source, pool, pool, day)
    source["coverage"]["ignored_problems"][0]["reason"] = "MISSING_REQUIRED_EQUITY_MINUTES"
    with pytest.raises(ValueError, match="only disclosed OI exclusions"):
        require_trade_source(source, pool, pool, day)


def test_publication_rejects_drift_before_creating_output(tmp_path: Path):
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "extension_manifest.json").write_text(json.dumps(dict(complete=True, artifacts={"proof.json": "wrong"})))
    (staging / "proof.json").write_text("{}")
    target = tmp_path / "output"
    with pytest.raises(ValueError, match="hash drift"):
        publish(staging, target)
    assert not target.exists()


def test_prior_zero_day_extension_can_use_its_frozen_dataset_lineage(tmp_path: Path):
    original = tmp_path / "full"
    extension = tmp_path / "extended"
    (original / "dataset").mkdir(parents=True)
    (original / "g_backtest").mkdir()
    (extension / "g_backtest").mkdir(parents=True)
    (original / "dataset/dataset_manifest.json").write_text(
        json.dumps({"through_day": "2026-10-08", "days": ["2026-10-08"]}))
    (original / "g_backtest/run_metadata.json").write_text(json.dumps({"last_session": "2026-10-08"}))
    (extension / "g_backtest/run_metadata.json").write_text(json.dumps({
        "last_session": "2026-10-09", "source_archive": str(original),
    }))
    source = original / "frozen_input.txt"
    source.write_text("unchanged")
    proof_path = extension / "g_backtest/dated_extension_proof.json"
    proof_path.write_text(json.dumps({
        "complete": True, "session_date": "2026-10-09", "selected_orders": 0,
        "trades": 0, "historical_ledger_unchanged": True,
        "source_archive": str(original), "input_hashes": {str(source): g2.sha256(source)},
    }))
    (extension / "extension_manifest.json").write_text(json.dumps({
        "complete": True, "through_day": "2026-10-09",
        "artifacts": {"g_backtest/dated_extension_proof.json": g2.sha256(proof_path)},
    }))
    dataset, manifest, days, evidence = verified_dataset_lineage(extension, "2026-10-09")
    assert dataset == original / "dataset"
    assert manifest["through_day"] == "2026-10-08"
    assert days == ["2026-10-09"]
    assert len(evidence) == 2
    traded = json.loads(proof_path.read_text())
    traded.update(selected_orders=1, trades=1, historical_ledger_unchanged=False,
                  historical_rows_unchanged=True)
    proof_path.write_text(json.dumps(traded))
    (extension / "extension_manifest.json").write_text(json.dumps({
        "complete": True, "through_day": "2026-10-09",
        "artifacts": {"g_backtest/dated_extension_proof.json": g2.sha256(proof_path)},
    }))
    assert verified_dataset_lineage(extension, "2026-10-09")[2] == ["2026-10-09"]
    source.write_text("changed")
    with pytest.raises(ValueError, match="input drift"):
        verified_dataset_lineage(extension, "2026-10-09")
