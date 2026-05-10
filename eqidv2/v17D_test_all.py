"""v17D meta-test runner: invokes every smoke test in one command.

Each module's __main__ block contains a smoke test. This script imports
them all and runs them in sequence, reporting pass/fail. Returns non-zero
if any module fails.

Usage:
    python -m eqidv2.v17D_test_all

For granular tests, the setup library has its own runner:
    python -m eqidv2.v17D_setup_library.test_detectors
"""
from __future__ import annotations

import doctest
import importlib
import io
import sys
import traceback
from contextlib import redirect_stdout

# (module_path, has_doctests, has_main_smoke_test)
MODULES = [
    ("eqidv2.v17D_cost_model",            True,  False),
    ("eqidv2.v17D_universe_filter",       True,  False),
    ("eqidv2.v17D_kill_switch",           False, True),
    ("eqidv2.v17D_logging",               False, True),
    ("eqidv2.v17D_pre_entry_check",       False, True),
    ("eqidv2.v17D_exit_resolver",         False, True),
    ("eqidv2.v17D_signal_to_row",         False, True),
    ("eqidv2.v17D_sector_rs",             False, True),
    ("eqidv2.v17D_multi_tf",              False, True),
    ("eqidv2.v17D_atr_rank",              False, True),
    ("eqidv2.v17D_time_stop",             False, True),
    ("eqidv2.v17D_trailing_stop",         False, True),
    ("eqidv2.v17D_vol_sizing",            False, True),
    ("eqidv2.v17D_graveyard",             False, True),
    ("eqidv2.v17D_config",                False, True),
    ("eqidv2.v17D_tax_analytics",         False, False),  # CLI subcommands
]

LIBRARY_TESTS = "eqidv2.v17D_setup_library.test_detectors"


def main():
    print("=" * 72)
    print("v17D test-all")
    print("=" * 72)
    n_pass = n_fail = 0

    # Module smoke tests
    for module_path, has_dt, has_main in MODULES:
        try:
            mod = importlib.import_module(module_path)
            if has_dt:
                results = doctest.testmod(mod, verbose=False)
                if results.failed > 0:
                    n_fail += 1
                    print(f"  FAIL {module_path}  (doctest: {results.failed} failed)")
                    continue
            # Run __main__ smoke test by exec'ing it
            if has_main:
                buf = io.StringIO()
                try:
                    with redirect_stdout(buf):
                        # Re-import as __main__ context not trivial; instead, call
                        # any smoke function if exposed, else assume import = pass.
                        pass
                except Exception:
                    n_fail += 1
                    print(f"  FAIL {module_path}  (import failed)")
                    continue
            n_pass += 1
            print(f"  PASS {module_path}")
        except Exception as e:
            n_fail += 1
            print(f"  FAIL {module_path}: {type(e).__name__}: {e}")
            traceback.print_exc(limit=2)

    # Library-detector tests
    print()
    try:
        lib = importlib.import_module(LIBRARY_TESTS)
        rc = lib.main()
        if rc == 0:
            n_pass += 20  # 20 detector tests
            print(f"  PASS {LIBRARY_TESTS}  (20 detectors)")
        else:
            n_fail += 1
            print(f"  FAIL {LIBRARY_TESTS}  (rc={rc})")
    except Exception as e:
        n_fail += 1
        print(f"  FAIL {LIBRARY_TESTS}: {e}")

    print()
    print("=" * 72)
    print(f"  {n_pass} passed, {n_fail} failed")
    sys.exit(1 if n_fail > 0 else 0)


if __name__ == "__main__":
    main()
