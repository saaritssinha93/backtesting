"""G3 equity-fill source adapter for the unchanged one-lot PAPER options engine."""
from __future__ import annotations

import os


def main(argv: list[str] | None = None) -> int:
    os.environ["FNO_OPTIONS_EQUITY_PROFILE"] = "V13_V10_G_3"
    os.environ["FNO_V6_STRATEGY_PROFILE"] = "V13_V10_G_3"
    import fno_v13_v10_g_options_paper as runtime
    if runtime.equity_config.STRATEGY_PROFILE != "V13_V10_G_3":
        raise RuntimeError("A different options source is already loaded; use a fresh process")
    return runtime.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
