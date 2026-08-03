"""Book C-BE: reduced core; arm break-even after reaching +1R."""
from final_setup_conf_v11_book_c_common import *
BOOK_ID = "V11_BOOK_C_BREAKEVEN_20260722"
BOOK_STATUS = "RESEARCH_ONLY"
FINAL_SETUP_CONF = build_book({"breakeven_trigger_r": 1.0}, "BREAKEVEN")

