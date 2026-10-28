"""G9 input pinning, all-candidate preservation and exact source parity."""
import json

import pandas as pd
import pytest

from research import g9_sources as sources


def _package(directory, members):
    path = directory / "manifest.json"
    path.write_text(json.dumps({"files":members}), encoding="utf-8")
    return sources.sha(path)


def _book():
    return pd.DataFrame([dict(sid=1, day="2026-10-23", setup_id="1011_SHORT",
        tradingsymbol="EXAMPLE", side="SHORT", filled="True", g8_baseline_filled="True",
        g8_direction_allowed="True", g8_quantity_scale=1., portfolio_executed="False",
        confirmation_ts="2026-10-23 10:11:00+05:30", entry_ts="2026-10-23 10:12:00+05:30",
        exit_ts="2026-10-23 10:20:00+05:30")])


def test_changed_package_member_fails(tmp_path):
    member = tmp_path / "book.csv"
    member.write_text("original", encoding="utf-8")
    digest = _package(tmp_path, {member.name:sources.sha(member)})
    member.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="input member changed"):
        sources._verify_package(tmp_path, digest)


def test_package_cannot_escape_its_directory(tmp_path):
    directory = tmp_path / "inside"
    directory.mkdir()
    member = tmp_path / "outside.csv"
    member.write_text("outside", encoding="utf-8")
    digest = _package(directory, {"../outside.csv":sources.sha(member)})
    with pytest.raises(ValueError, match="input member changed"):
        sources._verify_package(directory, digest)


def test_changed_manifest_fails(tmp_path):
    _package(tmp_path, {"member":"hash"})
    with pytest.raises(ValueError, match="input manifest changed"):
        sources._verify_package(tmp_path, "wrong")


def test_later_date_does_not_silently_reuse_source():
    with pytest.raises(ValueError, match="ends at 2026-10-23"):
        sources.load_sources(through="2026-10-26")


def test_native_fill_rejected_by_capital_is_retained_and_bool_parsed():
    result = sources._validate_book(_book(), ["2026-10-23"])
    assert len(result) == 1
    assert bool(result.loc[0,"g8_baseline_filled"])
    assert not bool(result.loc[0,"portfolio_executed"])


def test_duplicate_candidate_identity_fails():
    book = pd.concat([_book(),_book().assign(sid=2)], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate G8 candidate identity"):
        sources._validate_book(book, ["2026-10-23"])


def test_lost_native_fill_or_changed_base_size_fails():
    with pytest.raises(ValueError, match="preserve native fills"):
        sources._validate_book(_book().assign(g8_baseline_filled=False), ["2026-10-23"])
    with pytest.raises(ValueError, match="full-size"):
        sources._validate_book(_book().assign(g8_quantity_scale=.5), ["2026-10-23"])


@pytest.mark.skipif(not (sources.DAILY / "manifest.json").is_file(), reason="Pinned G8 data is local")
def test_local_pinned_g8_all_candidate_baseline_reproduces():
    result = sources.load_sources()
    assert len(result["days"]) == 57
    assert len(result["book"]) == 582
    assert result["book"].g8_baseline_filled.sum() == 514
    assert result["book"].portfolio_executed.sum() == 432
    assert result["baseline_metrics"]["net_pnl_rs"] == pytest.approx(1083789.4152852027, abs=1e-6)
    assert result["config"].get("max_positions", 10) == 10
    assert result["provenance"]["execution_authority"] is False
    assert result["vix_path"].is_file()
    result["book"].loc[0,"g8_quantity_scale"] = .5
    assert result["expected_baseline"].loc[0,"g8_quantity_scale"] == 1.
