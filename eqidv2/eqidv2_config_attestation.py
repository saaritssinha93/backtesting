"""
Audit #1 (2026-04-22) — Startup config attestation.

Why this exists:
  Production incident: bat set EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900 but the
  running executor used 300. Causes (any of which produce the same symptom):
    - bat edited after executor started; supervisor restart didn't re-source
    - wrong bat launched manually (v15 vs v16)
    - process inherited env from a stale parent shell
    - env-var name typo in bat
  In every case the bat looks fine, the process disagrees, nothing screams.

What this module provides:
  Bat-side (called from a tiny `python -c` in the bat):
    write_claim(process_name, claim_dict, claim_dir=None) -> Path
        Atomically writes runtime_config_claim_<process_name>_<date>.json
        listing the env values the bat just set. Bat dumps env values
        through this so quoting/escaping stays Python-side.

  Process-side (called once at executor startup, before any side-effects):
    verify_claim(process_name, whitelist, strict=False, claim_dir=None,
                 logger=None) -> AttestationResult
        Reads the claim file written by the bat, compares each whitelisted
        EQIDV2_* var against os.environ, and either:
          - logs [STARTUP.CONFIG] OK summary on full match
          - logs [STARTUP.CONFIG] DRIFT diff per mismatched key
            (always, both modes)
          - returns matched=False so the caller can sys.exit(2) when strict
        A bypass env (EQIDV2_CONFIG_CHECK_BYPASS=1) skips the gate
        entirely (logs [STARTUP.CONFIG] BYPASS) for emergency boots.

Design choices:
  * Whitelist-only comparison. We never compare every EQIDV2_* var because
    an operator-injected debug var would falsely fail every boot. The
    caller provides the keys it cares about (e.g. concurrency caps,
    timeouts, lag limits, gate toggles).
  * Soft-by-default. Strict mode (sys.exit on drift) is opt-in via
    EQIDV2_CONFIG_CHECK_STRICT=1. Two-week soak protects against
    whitelist-omission bugs blocking production starts.
  * No PID / active-producer flag here. That is a separate concern with
    its own failure modes (stale flag bricking restarts) and ships in a
    later iteration.

File layout:
  Claim path: <RUNTIME_ROOT>/live_signals/runtime_config_claim_<process>_<date>.json
  TradingData runtime path is OUTSIDE OneDrive, so atomic os.replace is
  not blocked by sync agents.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")


def _default_claim_dir() -> Path:
    runtime_root = os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")
    override = os.getenv("EQIDV2_CONFIG_CLAIM_DIR")
    if override:
        return Path(override)
    return Path(runtime_root) / "live_signals"


def _claim_path(process_name: str, claim_dir: Optional[Path] = None,
                date_str: Optional[str] = None) -> Path:
    base = claim_dir if claim_dir is not None else _default_claim_dir()
    if date_str is None:
        date_str = datetime.now(IST).strftime("%Y-%m-%d")
    safe_name = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in process_name)
    return base / f"runtime_config_claim_{safe_name}_{date_str}.json"


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except Exception:
            pass
    os.replace(tmp, path)


def write_claim(process_name: str,
                claim: Mapping[str, str],
                claim_dir: Optional[Path] = None) -> Path:
    """Bat-side: atomically write the per-day claim file."""
    now_ist = datetime.now(IST)
    payload = {
        "process_name":     process_name,
        "date":             now_ist.strftime("%Y-%m-%d"),
        "written_at_ist":   now_ist.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "writer_pid":       os.getpid(),
        "claim":            {str(k): str(v) for k, v in claim.items()},
    }
    path = _claim_path(process_name, claim_dir=claim_dir,
                       date_str=payload["date"])
    _atomic_write_json(path, payload)
    return path


@dataclass
class AttestationResult:
    matched: bool
    bypassed: bool = False
    strict: bool = False
    claim_path: Optional[str] = None
    missing_claim: bool = False
    diffs: list = field(default_factory=list)  # [(key, claim_value, env_value), ...]
    effective: dict = field(default_factory=dict)


def _emit(logger, msg: str) -> None:
    if logger is not None:
        try:
            logger.info(msg)
            return
        except Exception:
            pass
    print(msg, flush=True)


def _is_truthy(raw: Optional[str]) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def verify_claim(process_name: str,
                 whitelist: Iterable[str],
                 strict: Optional[bool] = None,
                 claim_dir: Optional[Path] = None,
                 logger=None) -> AttestationResult:
    """
    Process-side: read the claim file and compare against os.environ for
    each whitelisted key. Logs a [STARTUP.CONFIG] summary regardless of
    outcome. Returns AttestationResult; caller decides whether to exit on
    a mismatch when strict mode is on.

    Two off-by-default env knobs control behavior at boot:
      EQIDV2_CONFIG_CHECK_BYPASS=1  -> skip the check entirely (logs BYPASS)
      EQIDV2_CONFIG_CHECK_STRICT=1  -> sys.exit(2) on any mismatch
                                       (overridable via the `strict` arg)
    """
    keys = list(whitelist)
    if strict is None:
        strict = _is_truthy(os.environ.get("EQIDV2_CONFIG_CHECK_STRICT"))
    bypass = _is_truthy(os.environ.get("EQIDV2_CONFIG_CHECK_BYPASS"))

    result = AttestationResult(matched=True, strict=strict, bypassed=bypass)

    effective = {k: os.environ.get(k, "") for k in keys}
    result.effective = dict(effective)

    if bypass:
        _emit(logger,
              f"[STARTUP.CONFIG] BYPASS process={process_name} "
              f"reason=EQIDV2_CONFIG_CHECK_BYPASS=1 effective_keys={len(keys)}")
        return result

    path = _claim_path(process_name, claim_dir=claim_dir)
    result.claim_path = str(path)

    if not path.exists():
        result.missing_claim = True
        result.matched = False
        _emit(logger,
              f"[STARTUP.CONFIG] MISSING_CLAIM process={process_name} "
              f"expected={path} strict={strict}")
        if strict:
            _emit(logger, "[STARTUP.CONFIG] EXIT strict=true claim_missing")
            sys.exit(2)
        return result

    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        claimed = raw.get("claim", {}) if isinstance(raw, dict) else {}
    except Exception as exc:
        result.matched = False
        _emit(logger,
              f"[STARTUP.CONFIG] CLAIM_READ_FAILED path={path} err={exc!r} "
              f"strict={strict}")
        if strict:
            _emit(logger, "[STARTUP.CONFIG] EXIT strict=true claim_unreadable")
            sys.exit(2)
        return result

    diffs = []
    for key in keys:
        claim_val = str(claimed.get(key, "")) if key in claimed else None
        env_val = effective.get(key, "")
        if claim_val is None:
            # Bat did not include this key in its claim. Treat as a soft
            # warning — most likely a whitelist evolved beyond the bat,
            # not a config drift. Logged so the discrepancy is visible.
            diffs.append((key, "<not_claimed>", env_val))
            continue
        if claim_val != env_val:
            diffs.append((key, claim_val, env_val))

    result.diffs = diffs

    if not diffs:
        eff_compact = " ".join(f"{k}={effective[k]!r}" for k in keys)
        _emit(logger,
              f"[STARTUP.CONFIG] OK process={process_name} "
              f"keys={len(keys)} claim={path.name} "
              f"effective: {eff_compact}")
        return result

    result.matched = False
    _emit(logger,
          f"[STARTUP.CONFIG] DRIFT process={process_name} "
          f"mismatches={len(diffs)} claim={path.name} strict={strict}")
    for key, claim_val, env_val in diffs:
        _emit(logger,
              f"[STARTUP.CONFIG] DRIFT.KEY {key} "
              f"claim={claim_val!r} env={env_val!r}")
    if strict:
        _emit(logger,
              "[STARTUP.CONFIG] EXIT strict=true "
              f"mismatches={len(diffs)} — fix the env or run with "
              "EQIDV2_CONFIG_CHECK_BYPASS=1 to override")
        sys.exit(2)
    return result


# CLI for bat-side use:
#   python -m eqidv2_config_attestation write \
#       --process avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min \
#       --kv EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900 \
#       --kv EQIDV2_MAX_CONCURRENT_TRADES=10
def _cli_write(args) -> int:
    claim = {}
    for kv in args.kv or []:
        if "=" not in kv:
            print(f"[CONFIG_ATTESTATION] bad --kv (missing '='): {kv}",
                  file=sys.stderr)
            return 2
        k, v = kv.split("=", 1)
        claim[k.strip()] = v
    if args.from_env:
        for key in args.from_env:
            claim[key] = os.environ.get(key, "")
    path = write_claim(args.process, claim)
    print(f"[CONFIG_ATTESTATION] wrote {path}")
    return 0


def _main(argv=None) -> int:
    import argparse
    p = argparse.ArgumentParser(description="EQIDV2 startup config attestation")
    sub = p.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write", help="Write claim file (bat-side)")
    w.add_argument("--process", required=True, help="Process logical name")
    w.add_argument("--kv", action="append",
                   help="KEY=VALUE pair (repeatable)")
    w.add_argument("--from-env", action="append",
                   help="Env var name to capture from current environment")
    args = p.parse_args(argv)
    if args.cmd == "write":
        return _cli_write(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(_main())
