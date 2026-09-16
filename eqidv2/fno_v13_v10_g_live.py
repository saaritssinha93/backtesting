"""Canonical V13-V10-G entry point for paper and isolated live workers."""
from __future__ import annotations

import os

os.environ["FNO_LIVE_GENERATION"] = "v6"  # Stable market-data/schema transport.
os.environ["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G"

import fno_v5_live as runtime  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    return runtime.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
