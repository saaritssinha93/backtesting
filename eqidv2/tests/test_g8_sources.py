"""Input integrity and accounting guards for the isolated G7 V2 source cache."""
import json

import pandas as pd
import pytest

from research import g8_sources as sources


def _manifest(cache, files):
    (cache / "manifest.json").write_text(json.dumps(dict(
        schema_version=sources.SCHEMA, through=sources.THROUGH, files=files)), encoding="utf-8")


def test_modified_cached_member_is_rejected_before_use(tmp_path):
    member = tmp_path / "orders.csv"
    member.write_text("sid,day\n1,2026-10-22\n", encoding="utf-8")
    _manifest(tmp_path, {member.name: sources.g7.mirror.sha256(member)})
    member.write_text("sid,day\n1,2026-10-23\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Frozen G8 source cache changed: orders.csv"):
        sources.load_sources(tmp_path)


def test_cache_manifest_cannot_reference_a_parent_file(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("sid\n1\n", encoding="utf-8")
    _manifest(cache, {"../outside.csv": sources.g7.mirror.sha256(outside)})
    with pytest.raises(ValueError, match="Frozen G8 source cache changed"):
        sources.load_sources(cache)


def test_a_later_cutoff_cannot_silently_reuse_october22_cache(tmp_path):
    with pytest.raises(ValueError, match="ends at 2026-10-22"):
        sources.load_sources(tmp_path, through="2026-10-23")


def test_parity_includes_native_fills_rejected_by_portfolio():
    row = dict(sid=1, filled=True, portfolio_executed=False, exit_reason="TARGET",
               entry_ts="2026-10-22 10:12:00+05:30", exit_ts="2026-10-22 10:20:00+05:30",
               entry_path_index=0, exit_path_index=8, entry_price=100., exit_price=101.,
               gross_return_pct=1., net_return_pct=.95, cost_pct=.05)
    expected = pd.DataFrame([row])
    changed = expected.copy()
    changed.loc[0, "exit_price"] = 100.9
    with pytest.raises(ValueError, match="Native replay value differs: exit_price"):
        sources._same_native(changed, expected)


@pytest.mark.skipif(not (sources.CACHE / "manifest.json").exists(), reason="Pinned research input cache is local")
def test_verified_local_cache_retains_exact_v2_baseline_and_unique_ids():
    result = sources.load_sources()
    stats = sources.g7.bounded.stats(result["baseline"], result["days"])
    assert len(result["days"]) == 56
    assert result["days"][-1] == "2026-10-22"
    assert (stats["trades"], stats["wins"], stats["losses"], stats["losing_days"]) == (428, 325, 103, 7)
    assert stats["net_pnl_rs"] == pytest.approx(1_069_336.820819679, abs=1e-6)
    assert stats["daily_close_drawdown_rs"] == pytest.approx(26_992.820771255414, abs=1e-6)
    assert result["config"]["variant_id"] == sources.g7.V2_VERSION
    ids = set(result["orders"].sid)
    assert len(ids) == 571
    assert set(result["paths"]) == ids
    for name in ("orders", "unfiltered", "baseline", "audit"):
        assert result[name].sid.is_unique
        assert set(result[name].sid) == ids
    assert not result["orders"].duplicated(["g8_source_package", "g8_source_sid"]).any()
    assert result["provenance"]["exact_original_price_bytes"] is True
    assert result["provenance"]["native_parity_all_orders"] is True
    assert result["provenance"]["execution_authority"] is False
