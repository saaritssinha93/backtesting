"""Book C-Trail: trail by 0.5R after reaching +1R."""
from final_setup_conf_v11_book_c_common import *
BOOK_ID = "V11_BOOK_C_TRAILING_20260722"
BOOK_STATUS = "RESEARCH_ONLY"
FINAL_SETUP_CONF = build_book(
    {"trailing_trigger_r": 1.0, "trailing_distance_r": 0.5}, "TRAILING"
)
