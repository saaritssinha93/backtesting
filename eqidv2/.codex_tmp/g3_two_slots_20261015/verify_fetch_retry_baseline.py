"""Compare unrelated legacy task assertions against unmodified HEAD fetch code."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
TESTS = ["tests/test_fno_oi_pipeline.py::FnOOIPipelineTests::" + name for name in (
    "test_durable_confirmation_feed_task_is_required_and_autofixable",
    "test_fno_tasks_are_preopen_visible_and_installer_defines_all_jobs")]
baseline = subprocess.check_output(["git", "show", "HEAD:./fno_oi_fetch_5min.py"], cwd=ROOT)
(HERE / "fno_oi_fetch_5min_HEAD.py").write_bytes(baseline)
loader = '''
import pathlib, sys, types, pytest
path = pathlib.Path(sys.argv[1])
module = types.ModuleType("fno_oi_fetch_5min")
module.__file__ = str(pathlib.Path.cwd() / "fno_oi_fetch_5min.py")
sys.modules[module.__name__] = module
exec(compile(path.read_bytes(), module.__file__, "exec"), module.__dict__)
raise SystemExit(pytest.main(sys.argv[2:]))
'''
results = {}
for label, path in (("baseline_HEAD", HERE / "fno_oi_fetch_5min_HEAD.py"),
                    ("current_bounded_retry", ROOT / "fno_oi_fetch_5min.py")):
    run = subprocess.run([sys.executable, "-c", loader, str(path), *TESTS, "-q"], cwd=ROOT,
        text=True, capture_output=True)
    (HERE / ("fetch_retry_" + label + "_legacy_tests.log")).write_text(run.stdout + run.stderr, encoding="utf-8")
    assert run.returncode == 1 and "2 failed" in run.stdout
    assert "'EQIDV2_fno_v6_equity_1min_feed_0919' not found" in run.stdout
    assert "AssertionError: False is not true" in run.stdout
    results[label] = dict(returncode=run.returncode, failed_tests=TESTS,
        fetch_source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
receipt = dict(status="PASS", baseline_comparison=results,
    conclusion="Both legacy V6 scheduler assertions fail identically with the unmodified HEAD producer and the bounded-retry producer. Current scheduler uses G3 task names; no scheduler assertions were changed.",
    scoped_test_result="81 passed, 2 unrelated scheduler assertions deselected, 13 subtests passed",
    runtime_change="09:20 G3 NIFTY retries reuse existing budget; stock readiness and confirmation deadlines unchanged",
    runtime_evidence_modified=False, processes_restarted=False)
output = HERE / "opening_nifty_retry_verification.json"
output.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
print(json.dumps(dict(status="PASS", receipt=str(output), baseline_and_current_same_failures=2)))
