"""Reviewed NSE regular-session closures, shared by runtime and monitoring.

2026 source: https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf
The Jan 15 closure and Feb 1 regular special session match the existing
frozen V8/shared-paper calendar. Nov 8 Muhurat hours are not regular hours.
Keep the fallback so a missing local CSV cannot reopen a known holiday.
"""
import csv
from datetime import date
from pathlib import Path

HOLIDAY_CSV = Path(__file__).resolve().parent / "nse_holidays.csv"
HOLIDAYS_2026 = {
    date.fromisoformat(day): name
    for day, name in (
        ("2026-01-15", "Municipal Corporation Election"),
        ("2026-01-26", "Republic Day"),
        ("2026-03-03", "Holi"),
        ("2026-03-26", "Shri Ram Navami"),
        ("2026-03-31", "Shri Mahavir Jayanti"),
        ("2026-04-03", "Good Friday"),
        ("2026-04-14", "Dr. Baba Saheb Ambedkar Jayanti"),
        ("2026-05-01", "Maharashtra Day"),
        ("2026-05-28", "Bakri Id"),
        ("2026-06-26", "Muharram"),
        ("2026-09-14", "Ganesh Chaturthi"),
        ("2026-10-02", "Mahatma Gandhi Jayanti"),
        ("2026-10-20", "Dussehra"),
        ("2026-11-10", "Diwali Balipratipada"),
        ("2026-11-24", "Prakash Gurpurb Sri Guru Nanak Dev"),
        ("2026-12-25", "Christmas"),
    )
}
REGULAR_SPECIAL_SESSIONS = frozenset({date(2026, 2, 1)})


def trading_holidays() -> set[date]:
    days = set(HOLIDAYS_2026)
    if HOLIDAY_CSV.is_file():
        with HOLIDAY_CSV.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                value = row.get("date", "").strip()
                if value:
                    days.add(date.fromisoformat(value))
    return days


def market_closed_reason(day: date) -> str:
    if day in trading_holidays():
        return f"NSE market closed: {HOLIDAYS_2026.get(day, 'trading holiday')} ({day.isoformat()})"
    if day == date(2026, 11, 8):
        return "NSE regular session closed: Muhurat trading uses separate hours (2026-11-08)"
    if day.weekday() >= 5 and day not in REGULAR_SPECIAL_SESSIONS:
        return f"NSE market closed: weekend ({day.isoformat()})"
    return ""
