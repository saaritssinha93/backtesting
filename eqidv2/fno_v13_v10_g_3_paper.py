"""G3 PAPER-only convenience entry point."""
from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if any(arg == "--execution-mode" or arg.startswith("--execution-mode=")
           or arg == "--live-quantity" or arg.startswith("--live-quantity=") for arg in args):
        raise ValueError("G3 paper entry point fixes execution mode and rejects LIVE quantity overrides")
    from fno_v13_v10_g_3_live import main as run
    return run([*args, "--execution-mode", "PAPER"])


if __name__ == "__main__":
    raise SystemExit(main())
