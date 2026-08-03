"""Frozen runtime metadata for the V7/V11 parity pipeline.

Every long-running component writes one immutable startup manifest plus an
atomically replaced ``latest`` pointer.  The manifest is deliberately small:
it records the exact Python/config source hashes, resolved EQIDV2 environment,
launcher identity, command line, PID, and final-setup-conf contract.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import platform
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytz


IST = pytz.timezone("Asia/Kolkata")
MANIFEST_SCHEMA_VERSION = "eqidv2_runtime_manifest_v1"


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {
        "1", "true", "yes", "on", "enable", "enabled",
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_record(path_like: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(path_like).resolve()
    rec: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
    }
    if not path.exists() or not path.is_file():
        return rec
    stat = path.stat()
    rec.update(
        {
            "size": int(stat.st_size),
            "mtime_ist": datetime.fromtimestamp(stat.st_mtime, tz=IST).isoformat(),
            "sha256": _sha256(path),
        }
    )
    return rec


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(tmp, path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def assert_final_setup_conf_contract() -> dict[str, Any]:
    """Fail closed when a conf launcher does not resolve the intended book."""

    use_conf = _truthy(os.getenv("EQIDV2_USE_FINAL_SETUP_CONF", "0"))
    module_name = os.getenv("EQIDV2_FINAL_SETUP_CONF_MODULE", "").strip()
    expected = os.getenv("EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE", "").strip()
    if not use_conf and not expected:
        return {
            "enabled": False,
            "module": module_name,
            "expected_module": expected,
            "setup_count": 0,
        }
    if not use_conf:
        raise RuntimeError(
            "final setup conf contract failed: "
            "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE is set but "
            "EQIDV2_USE_FINAL_SETUP_CONF is not enabled"
        )
    if not module_name:
        raise RuntimeError(
            "final setup conf contract failed: "
            "EQIDV2_FINAL_SETUP_CONF_MODULE is empty"
        )
    if expected and module_name != expected:
        raise RuntimeError(
            "final setup conf contract failed: resolved module "
            f"{module_name!r} != expected {expected!r}"
        )

    module = importlib.import_module(module_name)
    conf = getattr(module, "FINAL_SETUP_CONF", None)
    if not isinstance(conf, dict) or not conf:
        raise RuntimeError(
            f"final setup conf contract failed: {module_name}.FINAL_SETUP_CONF "
            "is missing or empty"
        )
    module_path = Path(getattr(module, "__file__", "")).resolve()
    return {
        "enabled": True,
        "module": module_name,
        "expected_module": expected,
        "setup_count": int(len(conf)),
        "setup_keys": sorted(str(key) for key in conf),
        "source": _source_record(module_path),
    }


def freeze_runtime_manifest(
    component: str,
    *,
    runtime_root: str | os.PathLike[str],
    source_files: Iterable[str | os.PathLike[str]] = (),
    resolved_config: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Validate the conf contract and write an immutable startup manifest."""

    component_slug = "".join(
        ch if ch.isalnum() or ch in {"-", "_"} else "_"
        for ch in str(component).strip()
    ).strip("_") or "unknown_component"
    now = datetime.now(tz=IST)
    conf_contract = assert_final_setup_conf_contract()
    env = {
        key: value
        for key, value in sorted(os.environ.items())
        if key.startswith("EQIDV2_")
    }
    sources: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_path in source_files:
        path = Path(raw_path).resolve()
        key = str(path).casefold()
        if key in seen:
            continue
        seen.add(key)
        sources.append(_source_record(path))

    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "component": component_slug,
        "created_at_ist": now.isoformat(),
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "launcher_name": os.getenv("EQIDV2_LAUNCHER_NAME", "").strip(),
        "argv": list(sys.argv),
        "cwd": str(Path.cwd().resolve()),
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "final_setup_conf_contract": conf_contract,
        "resolved_eqidv2_env": env,
        "resolved_config": dict(resolved_config or {}),
        "source_files": sources,
        **dict(extra or {}),
    }
    manifest_dir = Path(runtime_root) / "runtime_manifests" / component_slug
    stamp = now.strftime("%Y%m%d_%H%M%S_%f")
    immutable_path = manifest_dir / f"{stamp}_pid{os.getpid()}.json"
    latest_path = manifest_dir / "latest.json"
    _atomic_write_json(immutable_path, payload)
    _atomic_write_json(latest_path, payload)
    return immutable_path, payload

