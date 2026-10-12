"""Explicit G3 scanner, confirmation and execution entry point; import is inert."""
from __future__ import annotations

import os


def main(argv: list[str] | None = None) -> int:
    os.environ["FNO_LIVE_GENERATION"] = "v6"
    os.environ["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G_3"
    import fno_v5_live as runtime
    if runtime.config.STRATEGY_PROFILE != "V13_V10_G_3":
        raise RuntimeError("A different strategy runtime is already loaded; use a fresh process")
    return runtime.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
