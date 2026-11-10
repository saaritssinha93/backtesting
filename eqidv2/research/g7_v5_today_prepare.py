"""Join verified October 30 history and today's fetched partial cash minutes."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


FIELDS = ["date", "open", "high", "low", "close", "volume"]


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(today: Path, prior: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(output)
    current = json.loads((today / "retrieval_provenance.json").read_text(encoding="utf-8"))
    older = json.loads((prior / "retrieval_provenance.json").read_text(encoding="utf-8"))
    if current["day"] != "2026-11-02" or current["cash_accepted"] != current["universe_stocks"]:
        raise ValueError("Today's full cash universe was not retrieved")
    previous = {item["symbol"]: item for item in older["records"]}
    folder = output / "equity_1m"
    folder.mkdir(parents=True)
    receipts = []
    for index, item in enumerate(current["records"], 1):
        symbol = item["symbol"]
        prior_row = previous[symbol]
        if item["cash_status"] != "accepted" or not str(prior_row["status"]).startswith("accepted"):
            raise ValueError(f"No verified today/prior cash source: {symbol}")
        today_path = Path(item["cash_path"])
        prior_path = Path(prior_row["output_path"])
        if sha(today_path) != item["cash_sha256"] or sha(prior_path) != prior_row["output_sha256"]:
            raise ValueError(f"Cash source changed: {symbol}")
        old = pd.read_parquet(prior_path, columns=FIELDS)
        new = pd.read_parquet(today_path, columns=FIELDS)
        for frame in (old, new):
            frame["date"] = pd.to_datetime(frame.date, utc=True).dt.tz_convert("Asia/Kolkata")
        if old.date.max().strftime("%Y-%m-%d") != "2026-10-30":
            raise ValueError(f"Prior cash archive does not end on October 30: {symbol}")
        if new.date.min().strftime("%Y-%m-%d") != "2026-11-02":
            raise ValueError(f"New cash bars are not November 2: {symbol}")
        combined = pd.concat((old, new), ignore_index=True).sort_values("date")
        if combined.date.duplicated().any() or not np.isfinite(combined[FIELDS[1:]].to_numpy(float)).all():
            raise ValueError(f"Invalid merged cash series: {symbol}")
        path = folder / f"{symbol}_stocks_indicators_1min.parquet"
        combined.to_parquet(path, index=False)
        receipts.append(dict(symbol=symbol, prior_path=str(prior_path), prior_sha256=prior_row["output_sha256"],
                             today_path=str(today_path), today_sha256=item["cash_sha256"],
                             merged_path=str(path), merged_sha256=sha(path),
                             today_minutes=len(new), cash_live_5m_price_conflict=bool(item.get("live_5m_max_price_difference", 0) > .001)))
        if index % 25 == 0 or index == len(current["records"]):
            print(f"MERGE {index}/{len(current['records'])}", flush=True)
    result = dict(scope="BACKTEST_ONLY", execution_authority=False,
                  day=current["day"], cutoff=current["cutoff"], stocks=len(receipts),
                  today_retrieval=str(today / "retrieval_provenance.json"),
                  prior_retrieval=str(prior / "retrieval_provenance.json"),
                  live_5m_price_conflict_stocks=sum(x["cash_live_5m_price_conflict"] for x in receipts),
                  receipts=receipts)
    (output / "merge_provenance.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--today", type=Path, required=True)
    parser.add_argument("--prior", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps({k: v for k, v in run(args.today, args.prior, args.output).items() if k != "receipts"}, indent=2))


if __name__ == "__main__":
    main()
