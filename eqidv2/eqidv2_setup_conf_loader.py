"""Shared setup-book loader for the V7 live path and V11 backtester.

Both runtimes must resolve the same module target or entry parity is impossible.
The neutral environment variable is authoritative; the older V11-specific name
remains a compatibility fallback for existing launchers and research scripts.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import os
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Iterable


FINAL_SETUP_CONF_MODULE_ENV = "EQIDV2_FINAL_SETUP_CONF_MODULE"
LEGACY_V11_FINAL_SETUP_CONF_MODULE_ENV = "EQIDV2_V11_FINAL_SETUP_CONF_MODULE"
DEFAULT_FINAL_SETUP_CONF_MODULE = "final_setup_conf"


def configured_target(
    env_names: Iterable[str] = (
        FINAL_SETUP_CONF_MODULE_ENV,
        LEGACY_V11_FINAL_SETUP_CONF_MODULE_ENV,
    ),
    default: str = DEFAULT_FINAL_SETUP_CONF_MODULE,
) -> str:
    """Return the first non-empty configured setup-book target."""
    for env_name in env_names:
        value = str(os.getenv(env_name, "")).strip()
        if value:
            return value
    return default


@lru_cache(maxsize=16)
def _load_target(target: str, base_dir: str) -> ModuleType:
    looks_like_path = target.lower().endswith(".py") or any(
        sep in target for sep in ("/", "\\")
    )
    if looks_like_path:
        path = Path(target)
        if not path.is_absolute():
            path = Path(base_dir) / path
        path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(str(path))
        digest = hashlib.md5(str(path).encode("utf-8")).hexdigest()[:10]
        module_name = f"_eqidv2_final_setup_conf_{digest}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"could not create module spec for {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    else:
        module = importlib.import_module(target)

    conf = getattr(module, "FINAL_SETUP_CONF", None)
    if not isinstance(conf, dict) or not conf:
        raise ValueError(
            f"{target!r} does not expose a non-empty FINAL_SETUP_CONF dict"
        )
    return module


def load_setup_conf_module(
    *,
    env_names: Iterable[str] = (
        FINAL_SETUP_CONF_MODULE_ENV,
        LEGACY_V11_FINAL_SETUP_CONF_MODULE_ENV,
    ),
    default: str = DEFAULT_FINAL_SETUP_CONF_MODULE,
    base_dir: str | Path | None = None,
) -> ModuleType:
    """Load and validate the setup-book module selected by the environment."""
    target = configured_target(env_names=env_names, default=default)
    resolved_base = Path(base_dir or Path(__file__).resolve().parent).resolve()
    return _load_target(target, str(resolved_base))

