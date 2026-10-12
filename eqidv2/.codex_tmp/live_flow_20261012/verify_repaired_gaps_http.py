"""Authenticated reporting-only checks; credentials never enter output."""
import base64
import gzip
import json
import math
import os
from pathlib import Path
from urllib.request import Request, urlopen

authorization = "Basic " + base64.b64encode((os.environ["LOG_DASH_USER"] + ":" + os.environ["LOG_DASH_PASS"]).encode()).decode()


def verify(request):
    port, day = request
    url = f"http://127.0.0.1:{port}/api/dashboard-flow/live?date={day}"
    with urlopen(Request(url, headers={"Authorization": authorization, "Accept-Encoding": "gzip"}), timeout=240) as response:
        raw = response.read()
        payload = json.loads(gzip.decompress(raw) if response.headers.get("Content-Encoding") == "gzip" else raw)
    assert "2026-09-14" not in payload["available_dates"]
    slots = {slot["time"]: slot for slot in payload["slots"]}
    selected = slots["09:30"]
    coverage = selected["coverage"]
    assert coverage["previous_close"] == coverage["prices"]
    for row in selected["rows"]:
        if row["price"] is not None:
            assert row["gap_open_pct"] is not None, row["symbol"]
            assert row["previous_close_source"] == "BROKER_DAILY_CASH_CLOSE"
            assert math.isclose(row["gap_open_pct"], (row["session_open"] / row["previous_close"] - 1) * 100, abs_tol=1e-9)
    assert slots["09:15"]["coverage"]["opening_gap"] == coverage["universe"]
    if day == "2026-10-12":
        assert coverage["previous_close"] == coverage["opening_gap"] == coverage["prices"] == 213
    sample = next(row for row in selected["rows"] if row["symbol"] == "RELIANCE")
    result = {"port": port, "date": day, "generated_at": payload["generated_at"], "coverage": coverage,
              "example": {key: sample.get(key) for key in ("symbol", "session_open", "previous_close", "previous_close_date", "previous_close_source", "gap_open_pct")},
              "warnings": payload.get("warnings", [])}
    print(json.dumps(result), flush=True)
    return result


# Historical reconstruction is CPU intensive; avoid competing cold requests in
# the same server. Reference files for every retained session are audited apart.
results = [verify(item) for item in [(8787, "2026-10-12"), (8790, "2026-10-12"), (8787, "2026-10-09")]]
Path(__file__).with_name("repaired_gap_http_checks.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
