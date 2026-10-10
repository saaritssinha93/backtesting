from datetime import date
from pathlib import Path
import json

import pandas as pd
import pytest

from tools.extend_v13_v10_g_dated_results import publish, require_zero_complete


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


def test_publication_rejects_drift_before_creating_output(tmp_path: Path):
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "extension_manifest.json").write_text(json.dumps(dict(complete=True, artifacts={"proof.json": "wrong"})))
    (staging / "proof.json").write_text("{}")
    target = tmp_path / "output"
    with pytest.raises(ValueError, match="hash drift"):
        publish(staging, target)
    assert not target.exists()
