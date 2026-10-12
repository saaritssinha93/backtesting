"""Read-only metadata inventory. Never moves, deletes, opens secrets or runs strategy code."""
from __future__ import annotations
import json
import os
import stat
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
OUTPUT = Path(__file__).resolve().parent
ROOTS = {"P": PROJECT, "D": Path("C:/TradingData/eqidv2")}

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def main():
    result = {"started_utc": now(), "roots": {k: str(v) for k,v in ROOTS.items()}, "files": [], "directories": [], "errors": [], "excluded": []}
    for label, root in ROOTS.items():
        stack = [root]
        while stack:
            directory = stack.pop()
            try:
                entries = list(os.scandir(directory))
            except OSError as error:
                result["errors"].append({"root": label, "path": str(directory.relative_to(root)), "error": type(error).__name__})
                continue
            for entry in entries:
                path = Path(entry.path)
                rel = path.relative_to(root).as_posix()
                if path == OUTPUT or entry.name == ".git":
                    result["excluded"].append({"root": label, "path": rel, "reason": "Inventory work directory or repository metadata; retained in place."})
                    continue
                try:
                    info = entry.stat(follow_symlinks=False)
                    reparse = bool(getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT)
                    record = {"root": label, "path": rel, "bytes": info.st_size, "modified_utc": datetime.fromtimestamp(info.st_mtime, timezone.utc).isoformat(timespec="seconds")}
                    if entry.is_symlink() or (reparse and entry.is_dir(follow_symlinks=False)):
                        record["reason"] = "Link/reparse directory; not traversed or proposed for archiving."
                        result["excluded"].append(record)
                    elif entry.is_dir(follow_symlinks=False):
                        result["directories"].append({"root": label, "path": rel})
                        stack.append(path)
                    else:
                        result["files"].append(record)
                except OSError as error:
                    result["errors"].append({"root": label, "path": rel, "error": type(error).__name__})
        print(json.dumps({"root": label, "files": sum(x["root"]==label for x in result["files"]), "directories": sum(x["root"]==label for x in result["directories"])}), flush=True)
    result["finished_utc"] = now()
    (OUTPUT / "metadata.json").write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(json.dumps({"errors": len(result["errors"]), "excluded": len(result["excluded"]), "suffixes_project": Counter(Path(x["path"]).suffix.lower() for x in result["files"] if x["root"]=="P").most_common(12)}), flush=True)

if __name__ == "__main__":
    main()
