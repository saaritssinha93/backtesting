from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen

from eqidv2_runtime_paths import RUNTIME_ROOT


IPV4_PATTERN = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b")
DEFAULT_SOURCES: Tuple[str, ...] = (
    "https://api.ipify.org",
    "https://ipv4.icanhazip.com",
    "https://ifconfig.me/ip",
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monitor the public IPv4 address of this machine.")
    p.add_argument("--interval-sec", type=int, default=300, help="Polling interval in seconds (default: 300)")
    p.add_argument("--timeout-sec", type=int, default=10, help="Per-request timeout in seconds (default: 10)")
    p.add_argument("--once", action="store_true", help="Fetch once, record it, and exit")
    p.add_argument(
        "--sources",
        nargs="+",
        default=list(DEFAULT_SOURCES),
        help="HTTP endpoints that return the current public IP",
    )
    p.add_argument(
        "--output-dir",
        default=str(RUNTIME_ROOT / "logs" / "public_ip_monitor"),
        help="Directory for CSV/JSONL/state outputs",
    )
    return p.parse_args()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fetch_public_ipv4(sources: Sequence[str], timeout_sec: int) -> Tuple[str, str]:
    errors = []
    for source in sources:
        try:
            req = Request(source, headers={"User-Agent": "eqidv2-public-ip-monitor/1.0"})
            with urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="ignore").strip()
            match = IPV4_PATTERN.search(raw)
            if match:
                return match.group(0), source
            errors.append(f"{source}: no IPv4 in response")
        except URLError as exc:
            errors.append(f"{source}: {exc}")
        except Exception as exc:
            errors.append(f"{source}: {exc}")
    raise RuntimeError(" | ".join(errors))


def _load_last_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _ensure_csv(csv_path: Path) -> None:
    if csv_path.exists():
        return
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "ts_utc",
                "public_ipv4",
                "changed",
                "source",
                "previous_ipv4",
                "error",
            ],
        )
        writer.writeheader()


def _append_csv(csv_path: Path, row: dict) -> None:
    _ensure_csv(csv_path)
    with csv_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "ts_utc",
                "public_ipv4",
                "changed",
                "source",
                "previous_ipv4",
                "error",
            ],
        )
        writer.writerow(row)


def _append_jsonl(jsonl_path: Path, payload: dict) -> None:
    with jsonl_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _write_state(state_path: Path, payload: dict) -> None:
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _record_success(output_dir: Path, public_ip: str, source: str) -> dict:
    csv_path = output_dir / "public_ip_history.csv"
    jsonl_path = output_dir / "public_ip_history.jsonl"
    state_path = output_dir / "latest_public_ip_state.json"
    last = _load_last_state(state_path)
    previous_ip = str(last.get("public_ipv4", "") or "")
    changed = bool(previous_ip and previous_ip != public_ip) or (not previous_ip)
    payload = {
        "ts_utc": _utc_now_iso(),
        "public_ipv4": public_ip,
        "changed": changed,
        "source": source,
        "previous_ipv4": previous_ip,
        "error": "",
    }
    _append_csv(csv_path, payload)
    _append_jsonl(jsonl_path, payload)
    state_payload = {
        "public_ipv4": public_ip,
        "last_seen_utc": payload["ts_utc"],
        "last_source": source,
        "previous_ipv4": previous_ip,
        "changed_on_last_check": changed,
    }
    _write_state(state_path, state_payload)
    return payload


def _record_error(output_dir: Path, error_text: str) -> dict:
    csv_path = output_dir / "public_ip_history.csv"
    jsonl_path = output_dir / "public_ip_history.jsonl"
    payload = {
        "ts_utc": _utc_now_iso(),
        "public_ipv4": "",
        "changed": False,
        "source": "",
        "previous_ipv4": "",
        "error": error_text,
    }
    _append_csv(csv_path, payload)
    _append_jsonl(jsonl_path, payload)
    return payload


def _run_once(args: argparse.Namespace, output_dir: Path) -> int:
    try:
        public_ip, source = _fetch_public_ipv4(args.sources, args.timeout_sec)
        payload = _record_success(output_dir, public_ip, source)
        status = "CHANGED" if payload["changed"] else "UNCHANGED"
        print(f"[{payload['ts_utc']}] {status} public IPv4={public_ip} via {source}")
        return 0
    except Exception as exc:
        payload = _record_error(output_dir, str(exc))
        print(f"[{payload['ts_utc']}] ERROR {payload['error']}", file=sys.stderr)
        return 1


def main() -> int:
    args = _parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.once:
        return _run_once(args, output_dir)

    print(
        f"Monitoring public IPv4 every {args.interval_sec}s. "
        f"Writing to {output_dir}",
        flush=True,
    )
    while True:
        _run_once(args, output_dir)
        time.sleep(max(int(args.interval_sec), 1))


if __name__ == "__main__":
    raise SystemExit(main())
