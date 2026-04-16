# -*- coding: utf-8 -*-
"""Quick exit re-resolve for v19a3. Run: python quick_reresolve_v19a3.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from quick_reresolve import run

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Quick exit re-resolve for v19a3")
    parser.add_argument("--short-tgt", type=float, default=None)
    parser.add_argument("--long-tgt",  type=float, default=None)
    parser.add_argument("--short-sl",  type=float, default=None)
    parser.add_argument("--long-sl",   type=float, default=None)
    args = parser.parse_args()
    run("v19a3",
        short_tgt=args.short_tgt, long_tgt=args.long_tgt,
        short_sl=args.short_sl,   long_sl=args.long_sl)
