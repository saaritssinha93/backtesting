# -*- coding: utf-8 -*-
"""Quick exit re-resolve for v19a2. Run: python quick_reresolve_v19a2.py

Optional target override (e.g. test 0.60% target on both sides):
    python quick_reresolve_v19a2.py --short-tgt 0.006 --long-tgt 0.006
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from quick_reresolve import run, main

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Quick exit re-resolve for v19a2")
    parser.add_argument("--short-tgt", type=float, default=None,
                        help="Override SHORT target pct (e.g. 0.006 for 0.6%%)")
    parser.add_argument("--long-tgt",  type=float, default=None,
                        help="Override LONG target pct  (e.g. 0.006 for 0.6%%)")
    parser.add_argument("--short-sl",  type=float, default=None,
                        help="Override SHORT stop-loss pct")
    parser.add_argument("--long-sl",   type=float, default=None,
                        help="Override LONG stop-loss pct")
    args = parser.parse_args()
    run("v19a2",
        short_tgt=args.short_tgt, long_tgt=args.long_tgt,
        short_sl=args.short_sl,   long_sl=args.long_sl)
