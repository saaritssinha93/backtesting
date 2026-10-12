import json

import pandas as pd
import pytest

from v14 import data, gap_repair


def bars(stamps, close=100.):
    return pd.DataFrame(dict(ts=pd.to_datetime(stamps).tz_localize("Asia/Kolkata"),
        open=close, high=close+1, low=close-1, close=close, volume=10,
        symbol="ABC", source="real"))


def test_missing_minutes_does_not_create_bars():
    minute = bars(["2026-08-05 09:16", "2026-08-05 09:18"])
    missing = gap_repair.missing_minutes(minute, "2026-08-05")
    assert len(missing) == 358
    assert missing[0] == pd.Timestamp("2026-08-05 09:17", tz="Asia/Kolkata")
    assert len(minute) == 2


def test_append_preserves_existing_observations_and_gaps():
    original = bars(["2026-08-05 09:16", "2026-08-05 09:19"])
    incoming = bars(["2026-08-05 09:16", "2026-08-05 09:17"], close=110.)
    result = gap_repair.append_real_missing(original, incoming)
    assert list(result.close) == [100., 110., 100.]
    assert len(result) == 3
    assert pd.Timestamp("2026-08-05 09:18", tz="Asia/Kolkata") not in set(result.ts)


def test_append_rejects_ambiguous_duplicate_bars():
    repeated = bars(["2026-08-05 09:16", "2026-08-05 09:16"])
    with pytest.raises(ValueError, match="duplicate"):
        gap_repair.append_real_missing(repeated.iloc[:1], repeated)


def test_cash_identity_never_aliases_be_or_derivative():
    master = pd.DataFrame([
        dict(tradingsymbol="ABC-BE", exchange="NSE", instrument_type="EQ", instrument_token=1),
        dict(tradingsymbol="ABC", exchange="NFO", instrument_type="FUT", instrument_token=2),
        dict(tradingsymbol="XYZ", exchange="NSE", instrument_type="EQ", instrument_token=3),
    ])
    result = gap_repair.exact_cash_tokens(master, ["ABC", "XYZ"])
    assert list(result) == ["XYZ"] and result["XYZ"]["instrument_token"] == 3


def pair():
    return dict(symbol="ABC", day="2026-08-05",
        cash_identity=dict(instrument_token=123, tradingsymbol="ABC", exchange="NSE", instrument_type="EQ", name="ABC"),
        missing_execution_minutes=["2026-08-05 09:17:00+05:30"])


def test_fetch_end_labels_real_zero_volume_and_resume(tmp_path):
    class Client:
        calls = 0
        def history(self, token, start, end):
            self.calls += 1
            assert token == 123 and start == end == "2026-08-05"
            return [dict(date="2026-08-05 09:16:00+05:30", open=100, high=100, low=100, close=100, volume=0)]
    client = Client()
    first = gap_repair.fetch_pair(client, pair(), tmp_path)
    second = gap_repair.fetch_pair(client, pair(), tmp_path)
    assert first == second and client.calls == 1
    assert first["recovered_missing_minutes"] == pair()["missing_execution_minutes"]
    frame = pd.read_parquet(tmp_path / "broker_1m/ABC_2026-08-05.parquet")
    assert frame.ts.iloc[0] == pd.Timestamp("2026-08-05 09:17", tz="Asia/Kolkata")
    assert frame.volume.iloc[0] == 0


def test_corrupt_cached_bars_are_not_trusted(tmp_path):
    class Client:
        calls = 0
        def history(self, *args):
            self.calls += 1
            return []
    client = Client()
    gap_repair.fetch_pair(client, pair(), tmp_path)
    (tmp_path / "broker_1m/ABC_2026-08-05.parquet").write_bytes(b"corrupt")
    gap_repair.fetch_pair(client, pair(), tmp_path)
    assert client.calls == 2


def test_five_minute_payload_is_independent_and_end_labelled(tmp_path):
    class Client:
        def _call(self, method, token, start, end, interval, **kwargs):
            assert method == "historical_data" and token == 123 and interval == "5minute"
            assert kwargs == dict(continuous=False, oi=False)
            return [dict(date="2026-08-05 09:15:00+05:30", open=100, high=105, low=95, close=102, volume=900)]
    audit = gap_repair.fetch_pair(Client(), pair(), tmp_path, interval="5minute")
    frame = pd.read_parquet(tmp_path / "broker_5m/ABC_2026-08-05.parquet")
    raw = pd.read_parquet(tmp_path / "broker_raw_5m/ABC_2026-08-05.parquet")
    assert frame.ts.iloc[0] == pd.Timestamp("2026-08-05 09:20", tz="Asia/Kolkata")
    assert raw.date.iloc[0] == "2026-08-05 09:15:00+05:30"
    assert frame.independent_interval.iloc[0]
    assert frame.candle_minutes.iloc[0] == 5
    assert frame.source.iloc[0] == "KITE_HISTORICAL_5MINUTE"
    assert frame.source_hash.iloc[0] == audit["raw_payload_sha256"]
    assert not {"gap_filled", "source_1m_count"}.intersection(frame)


def test_errors_never_persist_broker_exception_text(tmp_path):
    class Client:
        def history(self, *args):
            raise ValueError("secret_token_and_private_url")
    result = gap_repair.fetch_pair(Client(), pair(), tmp_path)
    assert result["status"] == "FETCH_ERROR" and result["error_type"] == "ValueError"
    raw = (tmp_path / "broker_1m/ABC_2026-08-05.json").read_text()
    assert "secret" not in raw and "private_url" not in raw


def test_flagged_native_bar_remains_flagged():
    native = bars(["2026-08-05 09:17"])
    native["gap_filled"] = True
    status = gap_repair._native_status(native, native.ts.iloc[0])
    assert status["present"] and status["flags"] == ["gap_filled"]


def test_output_cannot_be_canonical_data(tmp_path):
    with pytest.raises(ValueError, match="separate"):
        gap_repair.run(root=tmp_path, source_run=tmp_path / "runs/x", output=tmp_path / "data/1m")


@pytest.fixture
def verified_fixture(tmp_path):
    root, run, output = [tmp_path / name for name in ["runtime", "source_run", "repair"]]
    for folder in [root / "data/1m", root / "data/instruments", run, output]:
        folder.mkdir(parents=True)
    source = root / "data/1m/ABC.parquet"
    bars(["2026-08-05 09:16"]).to_parquet(source, index=False)
    master = root / "data/instruments/nse_latest.parquet"
    pd.DataFrame([pair()["cash_identity"]]).to_parquet(master, index=False)
    unknown, evidence = run / "unknown_outcomes.csv", run / "path_evidence.json"
    unknown.write_text("sid,symbol,day\n1,ABC,2026-08-05\n")
    evidence.write_text("{}")
    inv = dict(source_run=str(run), canonical_root=str(root), pairs=[pair()],
        canonical_sha256={str(source): data._sha(source)},
        source_sha256={str(f): data._sha(f) for f in [master, unknown, evidence]})
    data._json(output / "inventory.json", inv)
    class Client:
        def history(self, *args):
            return [dict(date=f"2026-08-05 09:{minute}:00+05:30", open=110.1, high=111.1, low=109.1, close=110.1, volume=10) for minute in [15, 16]]
        def _call(self, *args, **kwargs):
            return [dict(date="2026-08-05 09:15:00+05:30", open=100, high=115, low=95, close=110, volume=900)]
    gap_repair.fetch_pair(Client(), pair(), output)
    gap_repair.fetch_pair(Client(), pair(), output, interval="5minute")
    return root, run, output


def test_verified_loader_withholds_entire_day_on_price_scale_mismatch(verified_fixture):
    root, run, output = verified_fixture
    before = data._sha(root / "data/1m/ABC.parquet")
    additions, five, audit = gap_repair.load_verified_repair("ABC", "2026-08-05", output, root=root, source_run=run)
    assert len(additions) == len(five) == 0
    assert audit["overlap_minutes"] == audit["overlap_exact_differences"] == 1
    assert not audit["price_scale_compatible"]
    assert audit["withheld_minute_additions"] == audit["withheld_5m_rows"] == 1
    assert data._sha(root / "data/1m/ABC.parquet") == before


def test_verified_loader_allows_only_native_float32_roundoff(verified_fixture):
    import numpy as np
    root, run, output = verified_fixture
    source = root / "data/1m/ABC.parquet"
    original = pd.read_parquet(source)
    original.loc[0, ["open", "high", "low", "close"]] = np.asarray([110.1, 111.1, 109.1, 110.1], dtype=np.float32).astype(float)
    original.to_parquet(source, index=False)
    inv_path = output / "inventory.json"
    inv = json.loads(inv_path.read_text()); inv["canonical_sha256"][str(source)] = data._sha(source)
    data._json(inv_path, inv)
    one, five, audit = gap_repair.load_verified_repair("ABC", "2026-08-05", output)
    assert len(one) == len(five) == 1
    assert one.ts.iloc[0].minute == 17
    assert five.price_scale_compatible.iloc[0] and audit["price_scale_compatible"]


def test_verified_loader_rejects_altered_raw_payload(verified_fixture):
    root, run, output = verified_fixture
    raw = output / "broker_raw_5m/ABC_2026-08-05.parquet"
    frame = pd.read_parquet(raw)
    frame.loc[0, "high"] = 200.
    frame.to_parquet(raw, index=False)
    with pytest.raises(AssertionError, match="Raw repair payload hash"):
        gap_repair.load_verified_repair("ABC", "2026-08-05", output)


def test_verified_loader_rejects_changed_canonical(verified_fixture):
    root, run, output = verified_fixture
    bars(["2026-08-05 09:16"], close=200.).to_parquet(root / "data/1m/ABC.parquet", index=False)
    with pytest.raises(AssertionError, match="Canonical source hash"):
        gap_repair.load_verified_repair("ABC", "2026-08-05", output)


def test_verified_loader_rejects_changed_identity(verified_fixture):
    root, run, output = verified_fixture
    audit = output / "broker_1m/ABC_2026-08-05.json"
    record = json.loads(audit.read_text())
    record["cash_identity"]["instrument_token"] = 999
    data._json(audit, record)
    with pytest.raises(AssertionError, match="Cash identity|Untrusted repair source"):
        gap_repair.load_verified_repair("ABC", "2026-08-05", output)
