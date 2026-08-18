"""V6 entry point for the shared cash-equity FnO live/paper runtime."""

from __future__ import annotations

import os

os.environ["FNO_LIVE_GENERATION"] = "v6"

import fno_v5_live as runtime  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    return runtime.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
