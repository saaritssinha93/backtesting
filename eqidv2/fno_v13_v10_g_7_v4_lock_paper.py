"""G7 V4 lock PAPER readiness; unsupported execution never falls back to G3."""
from __future__ import annotations


def main(argv=None):
    from fno_v13_v10_g_7_v4_lock_live import main as readiness_main
    return readiness_main(argv, mode="PAPER")


if __name__ == "__main__":
    raise SystemExit(main())
