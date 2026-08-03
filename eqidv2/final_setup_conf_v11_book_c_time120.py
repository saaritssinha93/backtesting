"""Book C120: reduced core with a 120-minute maximum hold."""
from final_setup_conf_v11_book_c_common import *
BOOK_ID = "V11_BOOK_C_TIME120_20260722"
BOOK_STATUS = "RESEARCH_ONLY"
FINAL_SETUP_CONF = build_book({"max_hold_minutes": 120}, "TIME120")

