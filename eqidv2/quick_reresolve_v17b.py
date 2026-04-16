# -*- coding: utf-8 -*-
"""Quick exit re-resolve for v17b. Run: python quick_reresolve_v17b.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quick_reresolve import run
run("v17b")
