"""Publish verified corrected research reports without changing strategy settings."""
from pathlib import Path
import json
import shutil
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from research import g3_trade_monitor_backtest as fixed
from research import g3_trade_monitor_parameter_search as search


for module, isolated in ((fixed, "v13_v10_g_3_trade_monitor_backtest_causal_v2"),
                         (search, "v13_v10_g_3_trade_monitor_parameter_search_causal_v2")):
    isolated_root = module.DEFAULT_OUTPUT_ROOT.parent / isolated
    record = module.load_latest(isolated_root)
    source = Path(record["run_dir"]).resolve()
    pointer = json.loads((isolated_root / "latest/current_run.json").read_text(encoding="utf-8"))
    target_root = module.DEFAULT_OUTPUT_ROOT.resolve()
    target = (target_root / pointer["run_dir"]).resolve()
    if not target.is_relative_to(target_root) or target == target_root:
        raise ValueError("Invalid research publication destination")
    existing_pointer = target_root / "latest/current_run.json"
    backup = Path(__file__).parent / (target_root.name + "_previous_pointer.json")
    if existing_pointer.is_file() and not backup.exists():
        shutil.copy2(existing_pointer, backup)
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, target)
    module.verified_package(target, target_root)
    if fixed.history.sha256(target / "manifest.json") != pointer["manifest_sha256"]:
        raise ValueError("Copied research manifest mismatch")
    fixed.history.replay.atomic_json(existing_pointer, pointer)
    confirmed = module.load_latest(target_root)
    print(json.dumps(dict(root=str(target_root), run_dir=confirmed["run_dir"],
        execution_authority=False, strategy_configuration_changed=False)))
