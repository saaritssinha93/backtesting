"""Warm study feature caches from completed cash audits while download runs.

This command is local-only. It never logs in, requests market data, or edits
cash inputs. A worker computes in a private directory and publishes only if
the raw source still matches the completed audit. Finish this command before
starting a study in the same output directory (``precompute.lock`` is held).
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any

import pandas as pd

from . import data, research


def _stamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Missing audit or cutoff timestamp")
    return stamp.tz_localize("Asia/Kolkata") if stamp.tzinfo is None else stamp.tz_convert("Asia/Kolkata")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _task_key(task: tuple) -> str:
    _, _, _, days, context, source_hash, code_hash = task
    return hashlib.sha256(json.dumps(dict(source=source_hash, code=code_hash,
        days=days, index=context), sort_keys=True, allow_nan=False).encode()).hexdigest()


@contextlib.contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError(f"Precompute writer lock exists; inspect the recorded PID before removing a stale lock: {path}") from None
    os.write(descriptor, str(os.getpid()).encode("ascii"))
    os.close(descriptor)
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


def _cache_matches(cache: Path, symbol: str, key: str) -> bool:
    metadata, target = cache / f"{symbol}.json", cache / f"{symbol}.parquet"
    if not metadata.is_file() or not target.is_file():
        return False
    try:
        record = _read_json(metadata)
        return record.get("key") == key and record.get("sha256") == research.sha256(target)
    except (OSError, ValueError, KeyError):
        return False


def _run_task(task: tuple) -> dict:
    """Guard one symbol and atomically publish a research-compatible cache."""
    root_text, cache_text, symbol, days, context, source_hash, code_hash = task
    root, cache = Path(root_text), Path(cache_text)
    source = root / "data/1m" / f"{symbol}.parquet"
    key = _task_key(task)
    if research.feature_code_signature() != code_hash:
        raise RuntimeError("Feature construction code changed; restart precompute")
    if research.sha256(source) != source_hash:
        return {"symbol": symbol, "key": key, "status": "SOURCE_CHANGED"}
    with _exclusive_lock(cache / ".precompute_locks" / f"{symbol}.lock"):
        if _cache_matches(cache, symbol, key):
            return {"symbol": symbol, "key": key, "status": "CACHED"}
        work = cache / ".precompute_work" / f"{symbol}_{uuid.uuid4().hex}"
        work.mkdir(parents=True)
        try:
            private_task = (root_text, str(work), symbol, days, context, source_hash, code_hash)
            try:
                _, audit = research._feature_worker(private_task)
            except RuntimeError as exc:
                # An atomic downloader replacement can happen between this
                # worker's initial check and research's own load/check pair.
                # Only that explicit race is retried; computation errors fail.
                if str(exc) in {
                    f"Cash input changed during feature build: {symbol}",
                    f"Cash input changed while loading features: {symbol}",
                }:
                    return {"symbol": symbol, "key": key, "status": "SOURCE_CHANGED"}
                raise
            if research.sha256(source) != source_hash:
                return {"symbol": symbol, "key": key, "status": "SOURCE_CHANGED"}
            if research.feature_code_signature() != code_hash:
                raise RuntimeError("Feature construction code changed; restart precompute")
            metadata = _read_json(work / f"{symbol}.json")
            if metadata.get("key") != key or metadata.get("sha256") != research.sha256(work / f"{symbol}.parquet"):
                raise RuntimeError(f"Feature worker produced an incompatible cache: {symbol}")
            # Metadata is the commit marker. A crash between replacements
            # leaves a hash mismatch, which the study's cache reader rejects.
            os.replace(work / f"{symbol}.parquet", cache / f"{symbol}.parquet")
            os.replace(work / f"{symbol}.json", cache / f"{symbol}.json")
            return {"symbol": symbol, "key": key, "status": "BUILT", "audit": audit}
        finally:
            # Only remove the exact worker-owned flat files; no recursive
            # deletion against a computed Windows path is needed.
            for path in work.iterdir():
                if path.is_file():
                    path.unlink()
            work.rmdir()


def _inputs(root: Path) -> tuple[list[str], dict, dict]:
    # During download this manifest can be the previous completed generation;
    # its study dates are usable, but its raw-file hashes are deliberately not
    # used. Completed per-symbol audits provide the new generation's hashes.
    manifest = _read_json(root / "data/manifest.json")
    universe = _read_json(root / "data/universe_manifest.json")
    days = list(manifest["study_sessions"])
    memberships = {day: set(universe["sessions"][day]["symbols"]) for day in days}
    index = research.load_nifty_context(days, root=root)
    context = {day: float(value) if pd.notna(value) else None
        for day, value in index.set_index("day")["nifty_first_bar_return_pct"].items()}
    return days, memberships, context


def _ready_tasks(root: Path, cache: Path, cutoff: pd.Timestamp, days: list[str],
                 memberships: dict, context: dict, code_hash: str, *,
                 audit_cache: dict | None = None) -> dict[str, tuple]:
    tasks = {}
    memo = audit_cache if audit_cache is not None else {}
    all_symbols = set().union(*memberships.values()) if memberships else set()
    for symbol in sorted(all_symbols):
        path = root / "data/symbol_audits" / f"{symbol}.json"
        try:
            stat = path.stat()
            fingerprint = (stat.st_mtime_ns, stat.st_size)
            cached = memo.get(symbol)
            if cached is not None and cached[:2] == fingerprint:
                if cached[2] is not None:
                    tasks[symbol] = cached[2]
                continue
            # Both ready and pre-cutoff audits can be memoized. The downloader
            # publishes each audit atomically, changing its file fingerprint;
            # raw content is still hashed before submitting and inside worker.
            memo[symbol] = (*fingerprint, None)
            audit = _read_json(path)
            if _stamp(audit.get("updated_at")) < cutoff or audit.get("symbol") != symbol:
                continue
            source_hash = str(audit["sha256"])
            if len(source_hash) != 64 or not (root / "data/1m" / f"{symbol}.parquet").is_file():
                continue
            allowed = [day for day in days if symbol in memberships[day]]
            task = (str(root), str(cache), symbol, allowed, context, source_hash, code_hash)
            tasks[symbol] = task
            memo[symbol] = (*fingerprint, task)
        except (OSError, KeyError, ValueError, TypeError):
            # The writer uses atomic audit publication. A malformed external
            # or incomplete audit is ineligible rather than guessed complete.
            continue
    return tasks


def _download_complete(root: Path, cutoff: pd.Timestamp) -> bool:
    if (root / "data/fetch.lock").exists():
        return False
    try:
        progress = _read_json(root / "data/progress.json")
        manifest = _read_json(root / "data/manifest.json")
        if progress.get("status") != "COMPLETE" or _stamp(progress["updated_at"]) < cutoff:
            return False
        if _stamp(manifest["created_at"]) < cutoff:
            return False
        if manifest["universe_sha256"] != research.sha256(root / "data/universe_manifest.json"):
            return False
        for record in manifest["files"]:
            audit = _read_json(root / "data/symbol_audits" / f"{record['symbol']}.json")
            if audit.get("sha256") != record.get("sha256"):
                return False
        return True
    except (OSError, KeyError, ValueError, TypeError):
        return False


def run_precompute(root: Path, output: Path, updated_after: str, *, workers: int = 2,
                   once: bool = False, poll_seconds: float = 3.,
                   executor_factory=ProcessPoolExecutor) -> dict:
    root, output = Path(root).resolve(), Path(output).resolve()
    cutoff = _stamp(updated_after)
    if workers < 1:
        raise ValueError("workers must be at least one")
    if (output / "summary.json").exists():
        raise FileExistsError("Completed study output is immutable; use a new output")
    if not hasattr(research, "feature_code_signature"):
        raise RuntimeError("research.feature_code_signature() is required before running precompute")
    output.mkdir(parents=True, exist_ok=True)
    cache = output / "feature_cache"
    cache.mkdir(exist_ok=True)
    done: dict[str, str] = {}
    attempted: set[tuple[str, str]] = set()
    built = cached = stale = 0
    with _exclusive_lock(output / "precompute.lock"):
        days, memberships, context = _inputs(root)
        code_hash = research.feature_code_signature()
        universe_hash = research.sha256(root / "data/universe_manifest.json")
        audit_cache = {}
        once_tasks = _ready_tasks(root, cache, cutoff, days, memberships, context, code_hash,
            audit_cache=audit_cache) if once else None
        futures = {}
        with executor_factory(max_workers=workers) as executor:
            try:
                while True:
                    if research.feature_code_signature() != code_hash:
                        raise RuntimeError("Feature code changed during precompute; restart it")
                    if research.sha256(root / "data/universe_manifest.json") != universe_hash:
                        raise RuntimeError("Universe changed during precompute; restart it")
                    for future in list(futures):
                        if not future.done():
                            continue
                        symbol = futures.pop(future)
                        result = future.result()
                        if result["status"] in {"BUILT", "CACHED"}:
                            done[symbol] = result["key"]
                            built += result["status"] == "BUILT"
                            cached += result["status"] == "CACHED"
                        else:
                            stale += 1
                        if len(done) == 1 or len(done) % 25 == 0:
                            print(f"PRECOMPUTE completed={len(done)} built={built} cached={cached} stale={stale} symbol={symbol}", flush=True)
                    ready = once_tasks if once else _ready_tasks(root, cache, cutoff, days, memberships, context, code_hash,
                        audit_cache=audit_cache)
                    active_symbols = set(futures.values())
                    for symbol, task in ready.items():
                        key = _task_key(task)
                        if symbol in active_symbols or done.get(symbol) == key or (once and (symbol, key) in attempted):
                            continue
                        if len(futures) >= workers:
                            break
                        # This can race with a subsequent writer, so _run_task
                        # verifies again before reading and before publication.
                        if research.sha256(root / "data/1m" / f"{symbol}.parquet") != task[5]:
                            if once:
                                attempted.add((symbol, key))
                                stale += 1
                            continue
                        attempted.add((symbol, key))
                        futures[executor.submit(_run_task, task)] = symbol
                    pending = [symbol for symbol, task in ready.items()
                        if done.get(symbol) != _task_key(task) and not (once and (symbol, _task_key(task)) in attempted)]
                    complete = not futures and not pending and (once or _download_complete(root, cutoff))
                    status = dict(status="COMPLETE" if complete else "RUNNING", pid=os.getpid(),
                        root=str(root), output=str(output), updated_after=cutoff.isoformat(),
                        updated_at=pd.Timestamp.now(tz="Asia/Kolkata").isoformat(), workers=workers,
                        eligible_audits=len(ready), completed_symbols=len(done), built=built,
                        cached=cached, stale_attempts=stale, active=len(futures),
                        feature_code_signature=code_hash, universe_sha256=universe_hash, once=once)
                    research.write_json(output / "precompute_status.json", status)
                    if complete:
                        research.write_json(output / "precompute_manifest.json", {**status, "cache_keys": done})
                        return status
                    if futures:
                        wait(futures, timeout=max(.05, min(10., poll_seconds)), return_when=FIRST_COMPLETED)
                    else:
                        time.sleep(max(.05, min(10., poll_seconds)))
            except BaseException:
                for future in futures:
                    future.cancel()
                raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=data.DEFAULT_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--updated-after", required=True, help="ISO cutoff; a naive value is interpreted as Asia/Kolkata")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--once", action="store_true", help="Process one snapshot of currently completed audits, then exit")
    parser.add_argument("--poll-seconds", type=float, default=3.)
    args = parser.parse_args()
    result = run_precompute(args.root, args.output, args.updated_after, workers=args.workers,
        once=args.once, poll_seconds=args.poll_seconds)
    print(f"PRECOMPUTE {result['status']} symbols={result['completed_symbols']} output={args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
