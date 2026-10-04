from __future__ import annotations

import json
import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd

import ai_platform.observability.cli as observability_cli
from ai_platform.observability.cli import BUNDLE_SCHEMA_VERSION, main
from ai_platform.observability.data_quality import AppendOnlyObservationLedger
from ai_platform.observability.journal import AppendOnlyEventJournal
from ai_platform.observability.reconciliation import compare_stage, load_stage_bundle


def _stdout_json(capsys):
    return json.loads(capsys.readouterr().out)


def test_cli_module_import_does_not_require_pandas(tmp_path: Path) -> None:
    script = """
import importlib.abc
import sys

class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pandas' or fullname.startswith('pandas.'):
            raise ImportError('pandas deliberately blocked')
        return None

sys.meta_path.insert(0, Block())
import ai_platform.observability.cli
print('ok')
""".strip()
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_doctor_reports_optional_capabilities(capsys) -> None:
    assert main(["doctor"]) == 0
    payload = _stdout_json(capsys)
    assert payload["status"].startswith("READY")
    assert payload["capabilities"]["core"]["available"] is True
    assert "data_quality" in payload["capabilities"]


def test_reconcile_writes_report_and_infers_date(tmp_path: Path, capsys) -> None:
    row = {
        "session_date": "2026-09-24",
        "slot": "0925",
        "symbol": "AAA",
        "close": 100.0,
    }
    paths = []
    for name in ("live", "observed", "finalized"):
        path = tmp_path / f"{name}.json"
        path.write_text(
            json.dumps(
                {"session_date": "2026-09-24", "stages": {"raw_equity": [row]}}
            ),
            encoding="utf-8",
        )
        paths.append(path)
    output = tmp_path / "report.json"
    code = main(
        [
            "reconcile",
            "--live",
            str(paths[0]),
            "--observed",
            str(paths[1]),
            "--finalized",
            str(paths[2]),
            "--output",
            str(output),
        ]
    )
    assert code == 0
    result = _stdout_json(capsys)
    assert result["status"] == "COMPLETE"
    assert json.loads(output.read_text(encoding="utf-8"))["diagnosis"]["actionable"]


def test_bundle_accepts_csv_and_json_and_has_verifiable_hash(
    tmp_path: Path, capsys
) -> None:
    raw = tmp_path / "raw.csv"
    raw.write_text(
        "session_date,slot,symbol,close\n2026-09-24,0925,AAA,100.0\n",
        encoding="utf-8",
    )
    features = tmp_path / "features.json"
    features.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "session_date": "2026-09-24",
                        "slot": "0925",
                        "symbol": "AAA",
                        "ema9": 99.5,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "nested" / "live.json"
    code = main(
        [
            "bundle",
            "--session-date",
            "2026-09-24",
            "--kind",
            "live",
            "--output",
            str(output),
            "--stage",
            f"raw_equity={raw}",
            "--stage",
            f"feature={features}",
        ]
    )
    assert code == 0
    result = _stdout_json(capsys)
    assert result["stage_count"] == 2
    assert result["row_count"] == 2

    payload = json.loads(output.read_text(encoding="utf-8"))
    claimed = payload.pop("content_sha256")
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    assert claimed == hashlib.sha256(canonical).hexdigest()
    assert payload["schema_version"] == BUNDLE_SCHEMA_VERSION
    assert list(payload["stages"]) == ["feature", "raw_equity"]


def test_bundle_rejects_unknown_stage_and_duplicate_stage_file(
    tmp_path: Path, capsys
) -> None:
    rows = tmp_path / "rows.json"
    rows.write_text("[]", encoding="utf-8")
    output = tmp_path / "bundle.json"

    assert (
        main(
            [
                "bundle",
                "--session-date",
                "2026-09-24",
                "--kind",
                "observed",
                "--output",
                str(output),
                "--stage",
                f"not_a_stage={rows}",
            ]
        )
        == 2
    )
    assert "unknown stage" in capsys.readouterr().err
    assert not output.exists()

    assert (
        main(
            [
                "bundle",
                "--session-date",
                "2026-09-24",
                "--kind",
                "finalized",
                "--output",
                str(output),
                "--stage",
                f"feature={rows}",
                "--stage",
                f"feature={rows}",
            ]
        )
        == 2
    )
    assert "selected more than once" in capsys.readouterr().err
    assert not output.exists()


def test_bundle_extracts_native_scanner_and_confirmation_snapshots(
    tmp_path: Path, capsys
) -> None:
    feature_rows = [
        {
            "session_date": "2026-09-24",
            "signal_ts": "2026-09-24T09:25:00+05:30",
            "tradingsymbol": "AAA",
            "ema9": 101.0,
        }
    ]
    scanner = tmp_path / "scanner.json"
    scanner.write_text(
        json.dumps(
            {
                "schema_version": "fno_v6_scanner_5m_hybrid_v3",
                "session_date": "2026-09-24",
                "signal_end": "09:25",
                "confirmation_end": "09:26",
                "feature_evaluation_count": 1,
                "feature_evaluations": feature_rows,
                "feature_evaluations_sha256": observability_cli._canonical_sha256(
                    feature_rows
                ),
                "candidates": [
                    {
                        "tradingsymbol": "AAA",
                        "signal_timestamp": "2026-09-24T09:25:00+05:30",
                        "side": "LONG",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    confirmation = tmp_path / "confirmation.json"
    confirmation.write_text(
        json.dumps(
            {
                "schema_version": "fno_v6_equity_confirmation_1m_v3",
                "session_date": "2026-09-24",
                "signal_end": "09:25",
                "confirmation_end": "09:26",
                "feature_evaluation_count": 1,
                "feature_evaluations": feature_rows,
                "feature_evaluations_sha256": observability_cli._canonical_sha256(
                    feature_rows
                ),
                "selected_signal_ids": ["20260924_0925_LONG_AAA_abc123"],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "bundle.json"

    assert (
        main(
            [
                "bundle",
                "--session-date",
                "2026-09-24",
                "--kind",
                "live",
                "--output",
                str(output),
                "--stage",
                f"feature={scanner}",
                "--stage",
                f"base_gate={scanner}",
                "--stage",
                f"ranking={scanner}",
                "--stage",
                f"confirmation={confirmation}",
                "--stage",
                f"setup_gate={confirmation}",
                "--stage",
                f"selection={confirmation}",
            ]
        )
        == 0
    )
    result = _stdout_json(capsys)
    assert result["native_selectors"] == {
        "base_gate": {"feature_evaluations": 1},
        "confirmation": {"feature_evaluations": 1},
        "feature": {"feature_evaluations": 1},
        "ranking": {"candidates": 1},
        "selection": {"selected_signal_ids": 1},
        "setup_gate": {"feature_evaluations": 1},
    }
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["stages"]["selection"] == [
        {
            "confirmation_end": "09:26",
            "session_date": "2026-09-24",
            "signal_end": "09:25",
            "signal_id": "20260924_0925_LONG_AAA_abc123",
        }
    ]
    assert payload["stages"]["base_gate"][0]["ema9"] == 101.0
    assert payload["stages"]["ranking"] == [
        {
            "confirmation_end": "09:26",
            "session_date": "2026-09-24",
            "side": "LONG",
            "signal_end": "09:25",
            "signal_timestamp": "2026-09-24T09:25:00+05:30",
            "signal_ts": "2026-09-24T09:25:00+05:30",
            "tradingsymbol": "AAA",
        }
    ]


def test_bundle_repeated_stage_and_directory_are_deterministic(
    tmp_path: Path, capsys
) -> None:
    source = tmp_path / "snapshots"
    nested = source / "nested"
    nested.mkdir(parents=True)
    first = source / "z.json"
    second = nested / "a.json"
    for path, symbol in ((first, "BBB"), (second, "AAA")):
        rows = [
            {
                "session_date": "2026-09-24",
                "slot": "0925",
                "symbol": symbol,
                "value": 1,
            }
        ]
        path.write_text(json.dumps({"rows": rows}), encoding="utf-8")
    (source / "README.txt").write_text("ignored", encoding="utf-8")

    directory_bundle = tmp_path / "directory.json"
    repeated_bundle = tmp_path / "repeated.json"
    common = [
        "bundle",
        "--session-date",
        "2026-09-24",
        "--kind",
        "live",
    ]
    assert main([*common, "--output", str(directory_bundle), "--stage", f"feature={source}"]) == 0
    directory_result = _stdout_json(capsys)
    assert directory_result["source_file_count"] == 2
    assert (
        main(
            [
                *common,
                "--output",
                str(repeated_bundle),
                "--stage",
                f"feature={second}",
                "--stage",
                f"feature={first}",
            ]
        )
        == 0
    )
    _stdout_json(capsys)
    directory_payload = json.loads(directory_bundle.read_text(encoding="utf-8"))
    repeated_payload = json.loads(repeated_bundle.read_text(encoding="utf-8"))
    assert directory_payload == repeated_payload
    assert [row["symbol"] for row in directory_payload["stages"]["feature"]] == [
        "AAA",
        "BBB",
    ]


def test_bundle_retains_duplicate_keys_and_marks_stage_indeterminate(
    tmp_path: Path, capsys
) -> None:
    paths = []
    for index, selected in enumerate((True, False)):
        path = tmp_path / f"selection-{index}.json"
        path.write_text(
            json.dumps(
                {
                    "rows": [
                        {
                            "session_date": "2026-09-24",
                            "signal_id": "signal-1",
                            "selected": selected,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        paths.append(path)
    output = tmp_path / "bundle.json"
    assert (
        main(
            [
                "bundle",
                "--session-date",
                "2026-09-24",
                "--kind",
                "live",
                "--output",
                str(output),
                "--stage",
                f"selection={paths[0]}",
                "--stage",
                f"selection={paths[1]}",
            ]
        )
        == 0
    )
    result = _stdout_json(capsys)
    assert result["indeterminate_stages"] == ["selection"]
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert len(payload["stages"]["selection"]) == 2
    assert payload["stage_diagnostics"]["selection"] == {
        "comparison_state": "INDETERMINATE",
        "duplicate_keys": [["signal-1"]],
        "reason": "duplicate_comparison_keys",
        "record_count": 2,
    }
    rows = load_stage_bundle(output)["selection"]
    assert compare_stage("selection", rows, rows).reason == "duplicate_comparison_keys"


def test_bundle_validates_native_envelope_and_embedded_hashes(
    tmp_path: Path, capsys
) -> None:
    feature_rows = [
        {
            "session_date": "2026-09-24",
            "slot": "0925",
            "symbol": "AAA",
            "value": 1,
        }
    ]
    snapshot = {
        "session_date": "2026-09-24",
        "feature_evaluations": feature_rows,
        "feature_evaluation_count": 1,
        "feature_evaluations_sha256": observability_cli._canonical_sha256(feature_rows),
    }
    envelope = {
        "schema_version": "fno_live_evidence_v1",
        "session_date": "2026-09-24",
        "payload_sha256": observability_cli._canonical_sha256(snapshot),
        "payload": snapshot,
    }
    source = tmp_path / "envelope.json"
    source.write_text(json.dumps(envelope), encoding="utf-8")
    output = tmp_path / "bundle.json"
    args = [
        "bundle",
        "--session-date",
        "2026-09-24",
        "--kind",
        "live",
        "--output",
        str(output),
        "--stage",
        f"feature={source}",
    ]
    assert main(args) == 0
    _stdout_json(capsys)

    envelope["payload"]["feature_evaluations"][0]["value"] = 2
    source.write_text(json.dumps(envelope), encoding="utf-8")
    assert main(args) == 2
    assert "evidence payload_sha256 digest mismatch" in capsys.readouterr().err

    envelope["payload_sha256"] = observability_cli._canonical_sha256(envelope["payload"])
    source.write_text(json.dumps(envelope), encoding="utf-8")
    assert main(args) == 2
    assert "feature_evaluations_sha256 digest mismatch" in capsys.readouterr().err


def test_bundle_bounds_session_and_output_location(tmp_path: Path, capsys, monkeypatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    row = source / "one.json"
    row.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "session_date": "2026-09-23",
                        "slot": "0925",
                        "symbol": "AAA",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    nested_output = source / "bundle.json"
    base = [
        "bundle",
        "--session-date",
        "2026-09-24",
        "--kind",
        "live",
        "--output",
        str(nested_output),
        "--stage",
        f"feature={source}",
    ]
    assert main(base) == 2
    assert "output must be outside" in capsys.readouterr().err

    outside = tmp_path / "outside.json"
    base[base.index(str(nested_output))] = str(outside)
    assert main(base) == 2
    assert "outside 2026-09-24" in capsys.readouterr().err

    row.write_text(json.dumps({"rows": []}), encoding="utf-8")
    (source / "two.json").write_text(json.dumps({"rows": []}), encoding="utf-8")
    monkeypatch.setattr(observability_cli, "MAX_STAGE_FILES", 1)
    assert main(base) == 2
    assert "maximum is 1" in capsys.readouterr().err


def test_bundle_help_documents_native_and_repeat_inputs(capsys) -> None:
    try:
        main(["bundle", "--help"])
    except SystemExit as exc:
        assert exc.code == 0
    help_text = capsys.readouterr().out
    assert "repeat the same STAGE for multiple inputs" in help_text
    assert "selected_signal_ids" in help_text
    assert "Duplicate comparison keys are retained" in help_text


def test_bundle_outputs_round_trip_into_reconcile(tmp_path: Path, capsys) -> None:
    rows = tmp_path / "rows.json"
    rows.write_text(
        json.dumps(
            [
                {
                    "session_date": "2026-09-24",
                    "slot": "0925",
                    "symbol": "AAA",
                    "selected": True,
                }
            ]
        ),
        encoding="utf-8",
    )
    bundles = []
    for kind in ("live", "observed", "finalized"):
        output = tmp_path / f"{kind}.json"
        assert (
            main(
                [
                    "bundle",
                    "--session-date",
                    "2026-09-24",
                    "--kind",
                    kind,
                    "--output",
                    str(output),
                    "--stage",
                    f"selection={rows}",
                ]
            )
            == 0
        )
        _stdout_json(capsys)
        bundles.append(output)

    report = tmp_path / "reconciliation.json"
    assert (
        main(
            [
                "reconcile",
                "--live",
                str(bundles[0]),
                "--observed",
                str(bundles[1]),
                "--finalized",
                str(bundles[2]),
                "--output",
                str(report),
            ]
        )
        == 0
    )
    assert _stdout_json(capsys)["status"] == "COMPLETE"


def test_data_quality_detects_expected_interval_gap(tmp_path: Path, capsys) -> None:
    data = pd.DataFrame(
        {
            "ts": ["2026-09-24T09:15:00+05:30", "2026-09-24T09:17:00+05:30"],
            "open": [100, 101],
            "high": [102, 103],
            "low": [99, 100],
            "close": [101, 102],
            "volume": [10, 11],
            "oi": [1000, 0],
        }
    )
    path = tmp_path / "bars.csv"
    data.to_csv(path, index=False)
    code = main(
        [
            "data-quality",
            "--input",
            str(path),
            "--expected-interval",
            "1min",
            "--source",
            "NFO",
            "--symbol",
            "TESTFUT",
        ]
    )
    assert code == 1
    payload = _stdout_json(capsys)
    assert payload["status"] == "BLOCKED"
    assert payload["missing_timestamp_count"] == 1
    assert payload["zero_oi_count"] == 1


def test_verifiers_and_profitability_commands(tmp_path: Path, capsys) -> None:
    observations = AppendOnlyObservationLedger(tmp_path / "observations")
    observations.append(
        "equity",
        {"rows": 1},
        observed_at="2026-09-24T04:00:00+00:00",
    )
    assert main(["verify-observations", "--root", str(observations.root)]) == 0
    assert _stdout_json(capsys)["status"] == "VALID"

    journal_path = tmp_path / "events.jsonl"
    AppendOnlyEventJournal(journal_path, service="test", strict=True).append(
        "order.submitted", {"order_id": "one"}
    )
    assert (
        main(
            [
                "verify-events",
                "--journal",
                str(journal_path),
                "--service",
                "test",
            ]
        )
        == 0
    )
    assert _stdout_json(capsys)["entries"] == 1

    trades = tmp_path / "trades.csv"
    trades.write_text(
        "status,net_pnl_rs,setup\nCLOSED,10,A\nCLOSED,-5,B\n",
        encoding="utf-8",
    )
    assert (
        main(
            [
                "profitability",
                "--input",
                str(trades),
                "--group-by",
                "setup",
                "--min-trades",
                "2",
            ]
        )
        == 0
    )
    payload = _stdout_json(capsys)
    assert payload["summary"]["net_pnl_rs"] == 5.0
    assert payload["summary"]["evidence_state"] == "SUFFICIENT"
    assert [group["dimensions"]["setup"] for group in payload["groups"]] == ["A", "B"]
