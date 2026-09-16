"""V13-V10-G quantity-one LIVE coordinator entry point.

The compatibility transport remains v6 so existing order state and arm/kill
controls remain authoritative. Importing this wrapper never starts workers.
"""

from __future__ import annotations

import os

os.environ["FNO_LIVE_GENERATION"] = "v6"
os.environ["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G"

import fno_v6_live_kite_session as coordinator  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    return coordinator.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
