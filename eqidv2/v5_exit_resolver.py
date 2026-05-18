"""Entry point for the AVWAP ID v5 1-minute exit sweep.

This keeps the requested filename stable while the implementation lives in
`avwap_5min_ID_v5_exit_sweep.py`.

Run:
    python v5_exit_resolver.py
"""

from __future__ import annotations

from avwap_5min_ID_v5_exit_sweep import main


if __name__ == "__main__":
    raise SystemExit(main())
