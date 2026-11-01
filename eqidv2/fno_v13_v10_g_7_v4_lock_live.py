"""G7 V4 lock readiness entrypoint. It cannot activate a broker or workers."""
from __future__ import annotations

import argparse
import json


def main(argv=None, *, mode="LIVE"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readiness-only", action="store_true")
    parser.add_argument("--print-config", action="store_true")
    args = parser.parse_args(argv)
    from fno_v13_v10_g_7_v4_lock_live_config import readiness, validate_strategy
    result = validate_strategy() if args.print_config else readiness(mode)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 2  # Explicit NOT_SUPPORTED, never a successful trading activation.


if __name__ == "__main__":
    raise SystemExit(main())
