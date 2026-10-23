"""G8 backtest: user-adopted 30-minute VIX baseline; --research runs the old sweep."""
import sys

from research.g8_main_baseline import main

if __name__ == "__main__":
    if "--research" in sys.argv:
        sys.argv.remove("--research")
        from research.g8_nifty_vix_backtest import main as research_main
        research_main()
    else:
        main()
