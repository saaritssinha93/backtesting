"""G3 INR 10,000 x 5 LIVE coordinator; default invocation is readiness only."""
from __future__ import annotations

import os
import sys


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if "--auto-arm" in args:
        raise ValueError("G3 never auto-arms; use separately reviewed activation controls")
    if "--activate-live" not in args and "--readiness-only" not in args:
        args.append("--readiness-only")
    os.environ["FNO_LIVE_GENERATION"] = "v6"
    os.environ["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G_3"
    import fno_v6_live_kite_session as coordinator
    if coordinator.config.STRATEGY_PROFILE != "V13_V10_G_3":
        raise RuntimeError("A different coordinator is already loaded; use a fresh process")
    return coordinator.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
