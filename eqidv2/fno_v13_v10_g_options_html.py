"""Add the frozen options model to a local stock report without rewriting it.

The injector is deliberately separate from the stock report builder. It strips
only its own tagged additions before rebuilding and proves that the original
document bytes, including every stock data/script/style byte, are unchanged.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path


ASSETS = Path(__file__).resolve().parent / "docs" / "v13_v10_g_options"
PREFIX = "V13_V10_G_OPTIONS"
BLOCK = re.compile(
    rb"<!-- V13_V10_G_OPTIONS:([A-Z]+):START -->.*?"
    rb"<!-- V13_V10_G_OPTIONS:\1:END -->", re.S
)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def strip_options(data: bytes) -> bytes:
    return BLOCK.sub(b"", data)


def marked(name: str, content: str) -> bytes:
    return (f"<!-- {PREFIX}:{name}:START -->" + content +
            f"<!-- {PREFIX}:{name}:END -->").encode("utf-8")


def insert_before(data: bytes, anchor: bytes, addition: bytes) -> bytes:
    if data.count(anchor) != 1:
        raise ValueError(f"Expected one anchor {anchor!r}, found {data.count(anchor)}")
    return data.replace(anchor, addition + anchor, 1)


def validate_payload(payload: dict) -> None:
    required = {"meta", "scenarios", "history", "curves", "monthly", "annual"}
    if not required <= payload.keys():
        raise ValueError(f"Missing payload keys: {required - payload.keys()}")
    meta = payload["meta"]
    if (meta["lots"], meta["stop_pct"], meta["target_pct"]) != (3, 12.5, 25):
        raise ValueError("This report requires fixed 3 lots, 12.5% SL and 25% target")
    if abs(meta["projection_start_equity"] - meta["initial_capital"] -
           meta["history_net_pnl"]) > 1e-6:
        raise ValueError("Projection opening does not reconcile with historical net profit")
    ids = {row["id"] for row in payload["scenarios"]}
    if ids != {"reference", "retain_75", "retain_50", "retain_20"}:
        raise ValueError("Expected the four saved payoff retention scenarios")
    pairs = {(scenario, sizing) for scenario in ids for sizing in ("fixed", "monthly", "stepup")}
    if {(r["scenario"], r["sizing"]) for r in payload["annual"]} != pairs or len(payload["annual"]) != 12:
        raise ValueError("Expected exactly one annual record for all twelve scenario/sizing pairs")
    if len(payload["curves"]) != 12 * (meta["horizon"] + 1) or len(payload["monthly"]) != 144:
        raise ValueError("Unexpected extra or missing curve/monthly rows")
    for scenario, sizing in pairs:
        curves = [r for r in payload["curves"] if (r["scenario"], r["sizing"]) == (scenario, sizing)]
        if [row["session"] for row in curves] != list(range(meta["horizon"] + 1)):
            raise ValueError(f"Missing, duplicate or unordered daily curve for {scenario}/{sizing}")
        months = [r for r in payload["monthly"] if (r["scenario"], r["sizing"]) == (scenario, sizing)]
        if [row["model_month"] for row in months] != list(range(1, 13)):
            raise ValueError(f"Missing, duplicate or unordered model month for {scenario}/{sizing}")


def build_html(original: bytes, payload: dict) -> bytes:
    validate_payload(payload)
    base = strip_options(original)
    additions = {
        name: (ASSETS / filename).read_text(encoding="utf-8")
        for name, filename in [("STYLE", "options.css"), ("SECTIONS", "options.html"),
                               ("SCRIPT", "options.js")]
    }
    data = insert_before(base, b"</head>", marked("STYLE", "<style>" + additions["STYLE"] + "</style>"))
    nav_anchor = re.search(rb'<a href="#monthly"[^>]*>.*?</a>', data, re.S)
    if nav_anchor is None:
        raise ValueError("Stock monthly navigation anchor not found")
    pos = nav_anchor.end()
    data = data[:pos] + marked("NAV", '\n<a href="#options-history"><span>O1</span> Options results</a>\n<a href="#options-projections"><span>O2</span> Options projections</a>') + data[pos:]
    stock_intro = re.search(rb'<section id="projections">.*?<p class="intro">.*?</p>', data, re.S)
    if stock_intro is None:
        raise ValueError("Stock projection introduction not found")
    pos = stock_intro.end()
    data = data[:pos] + marked("JUMP", '<p class="notice">Options model: <a href="#options-projections">compare monthly resizing and fixed 3-lot ATM CE / PE projections with 12.5% SL and 25% target</a>.</p>') + data[pos:]
    data = insert_before(data, b'<section id="architecture">', marked("SECTIONS", additions["SECTIONS"]))
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    encoded = encoded.replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    data = insert_before(data, b"</body>", marked("DATA", '<script type="application/json" id="optModelData">' + encoded + "</script>") + marked("SCRIPT", "<script>" + additions["SCRIPT"] + "</script>"))
    if strip_options(data) != base:
        raise AssertionError("Stock report bytes changed during options injection")
    # Dynamic JS/JSON snippets may contain repeated IDs as source strings; the
    # HTML-only check excludes all scripts before inspecting actual elements.
    html_only = re.sub(rb"<script\b[^>]*>.*?</script>", b"", data, flags=re.S)
    ids = re.findall(rb'\bid="([^\"]+)"', html_only)
    if len(ids) != len(set(ids)):
        raise AssertionError("Duplicate static HTML IDs after injection")
    return data


def inject(html_file: Path, payload_file: Path, output: Path | None = None) -> dict:
    html_file, payload_file = html_file.resolve(), payload_file.resolve()
    output = (output or html_file).resolve()
    original = html_file.read_bytes()
    payload_bytes = payload_file.read_bytes()
    payload = json.loads(payload_bytes.decode("utf-8"))
    result = build_html(original, payload)
    base = strip_options(original)
    backup_dir = payload_file.parent / "html_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    input_backup = backup_dir / f"{html_file.stem}_before_options_{sha(original)[:12]}.html"
    if input_backup.exists() and input_backup.read_bytes() != original:
        raise ValueError("Existing input backup has unexpected bytes")
    if not input_backup.exists():
        input_backup.write_bytes(original)
    backup = backup_dir / f"{html_file.stem}_stock_{sha(base)[:12]}.html"
    if backup.exists() and backup.read_bytes() != base:
        raise ValueError("Existing stock backup has unexpected bytes")
    if not backup.exists():
        backup.write_bytes(base)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".options.tmp")
    temporary.write_bytes(result)
    temporary.replace(output)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "html_file": str(output), "payload_file": str(payload_file),
        "input_backup": str(input_backup), "input_sha256": sha(original),
        "stock_backup": str(backup), "stock_original_sha256": sha(base),
        "stock_after_stripping_options_sha256": sha(strip_options(result)),
        "stock_bytes_unchanged": strip_options(result) == base,
        "payload_sha256": sha(payload_bytes), "html_sha256": sha(result),
        "generator_sha256": sha(Path(__file__).read_bytes()),
        "template_sha256": {path.name: sha(path.read_bytes()) for path in sorted(ASSETS.glob("options.*"))},
        "options_lots": payload["meta"]["lots"],
        "idempotent": build_html(result, payload) == result,
    }
    (payload_file.parent / "html_integration_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--html-file", type=Path, required=True)
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--output", type=Path, help="Optional separate preview; default edits the named HTML")
    args = parser.parse_args()
    print(json.dumps(inject(args.html_file, args.payload, args.output), indent=2))


if __name__ == "__main__":
    main()
