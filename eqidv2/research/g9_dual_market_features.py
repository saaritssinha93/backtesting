"""Pure, fail-closed features for the G-9 dual-market research experiment.

These primitives consume genuine ordered quote updates and individual trade
prints. They do not acquire feeds, reconstruct a book, infer CVD from candles,
simulate fills, or assert that a provider supplies sequence-complete data.
The adapter must explicitly attest observation/sequence/trade-tape quality.
All timestamps must be timezone-aware; windows are half-open [start, end).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any, Iterable, Mapping, Sequence

IST = timezone(timedelta(hours=5, minutes=30), "Asia/Kolkata")
VERSION = "g9_dual_market_features_v1"
SCORE_WEIGHTS = {
    "oi_delta_percent": 25.0,
    "futures_ofi": 20.0,
    "futures_cvd": 15.0,
    "spot_ofi": 20.0,
    "spot_cvd": 20.0,
}


@dataclass(frozen=True)
class QuoteUpdate:
    instrument: str
    timestamp: datetime
    received_at: datetime
    sequence: int
    bid: float
    bid_size: float
    ask: float
    ask_size: float


@dataclass(frozen=True)
class TradePrint:
    instrument: str
    timestamp: datetime
    received_at: datetime
    trade_id: str
    price: float
    quantity: float
    # Reliable provider flag or causally classified side; 0/None is unknown.
    aggressor: int | None


@dataclass(frozen=True)
class MarketFlow:
    instrument: str
    window_start: datetime
    window_end: datetime
    observed_at: datetime
    ofi: float | None = None
    ofi_depth: float | None = None
    cvd_delta: float | None = None
    cvd_ratio: float | None = None
    recent_cvd_delta: float | None = None
    total_volume: float = 0.0
    classified_volume: float = 0.0
    unknown_volume: float = 0.0
    classification_coverage: float | None = None
    recent_classification_coverage: float | None = None
    quote_age_seconds: float | None = None
    spread_bps: float | None = None
    quote_updates: int = 0
    trade_count: int = 0
    reasons: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.reasons


@dataclass(frozen=True)
class GateConfig:
    # Uncalibrated operational values deliberately default to rejection.
    max_oi_age_seconds: float | None = None
    max_quote_age_seconds: float | None = None
    max_spread_bps: float | None = None
    min_trade_side_coverage: float = 0.95
    min_history_sessions: int = 20


@dataclass(frozen=True)
class ScoredCandidate:
    symbol: str
    future_contract: str
    window_end: datetime | None
    score: float | None
    percentiles: Mapping[str, float]
    history_counts: Mapping[str, int]
    reasons: tuple[str, ...]
    rank: int | None = None

    @property
    def eligible(self) -> bool:
        return self.score is not None and not self.reasons


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def _aware(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _book_valid(quote: QuoteUpdate) -> bool:
    return (all(_finite(x) and x > 0 for x in
                (quote.bid, quote.ask, quote.bid_size, quote.ask_size))
            and quote.bid < quote.ask)


def canonical_ofi(previous: QuoteUpdate, current: QuoteUpdate) -> float:
    """Canonical best-level OFI increment, including BOTH equality terms.

    Sequences here are adapter-assigned contiguous *book-update* sequences,
    not an exchange sequence that also counts unrelated message types.
    Invalid, duplicate, reversed, or skipped updates raise ValueError.
    """
    if previous.instrument != current.instrument or not previous.instrument:
        raise ValueError("OFI requires one exact instrument")
    if not _book_valid(previous) or not _book_valid(current):
        raise ValueError("OFI requires valid unlocked, uncrossed books")
    if not all(_aware(x) for x in (previous.timestamp, current.timestamp,
                                  previous.received_at, current.received_at)):
        raise ValueError("OFI timestamps must be timezone-aware")
    if (not all(isinstance(q.sequence, int) and not isinstance(q.sequence, bool)
                for q in (previous, current))
            or current.sequence != previous.sequence + 1
            or current.timestamp < previous.timestamp
            or current.received_at < previous.received_at
            or previous.received_at < previous.timestamp
            or current.received_at < current.timestamp):
        raise ValueError("OFI requires consecutive ordered observed updates")
    return float(
        (current.bid >= previous.bid) * current.bid_size
        - (current.bid <= previous.bid) * previous.bid_size
        - (current.ask <= previous.ask) * current.ask_size
        + (current.ask >= previous.ask) * previous.ask_size
    )


def build_market_flow(
    quotes: Iterable[QuoteUpdate], trades: Iterable[TradePrint], *,
    window_start: datetime, window_end: datetime, observed_at: datetime,
    subscription_started_at: datetime | None, sequence_complete: bool,
    real_trade_prints: bool, max_quote_age_seconds: float | None,
) -> MarketFlow:
    """Aggregate only events received by decision time for a completed window.

    Supply one seed quote strictly before the window plus all subsequent
    updates, in observed order. Future/unreceived events cannot affect the
    calculation. Missing data yields explicit reasons and unavailable values.
    ``sequence_complete`` is the adapter's attestation of feed continuity and
    finalized window coverage; sequence arithmetic alone cannot establish it.
    Trade signs must already be validated; this function never invents them.
    """
    if not all(_aware(t) for t in (window_start, window_end, observed_at)):
        raise ValueError("Window timestamps must be timezone-aware")
    if window_start >= window_end or observed_at < window_end:
        raise ValueError("Only completed positive-length windows can be observed")
    reasons: list[str] = []
    if not _aware(subscription_started_at) or subscription_started_at > window_start:
        reasons.append("incomplete_subscription_window")
    if sequence_complete is not True:
        reasons.append("incomplete_book_sequence")
    if real_trade_prints is not True:
        reasons.append("real_trade_prints_unavailable")
    if not _finite(max_quote_age_seconds) or max_quote_age_seconds <= 0:
        reasons.append("quote_freshness_uncalibrated")

    quote_rows: list[QuoteUpdate] = []
    for quote in quotes:
        if not _aware(quote.timestamp) or not _aware(quote.received_at):
            reasons.append("invalid_quote_timestamp")
            continue
        if quote.timestamp < window_end and quote.received_at <= observed_at:
            quote_rows.append(quote)
    # Validate observed ordering before discarding older warm-up quotes. A
    # delayed pre-window quote must not erase intervening in-window updates.
    if any(b.timestamp < a.timestamp or b.received_at < a.received_at
           for a, b in zip(quote_rows, quote_rows[1:])):
        reasons.append("invalid_book_sequence")
    seed_indices = [i for i, q in enumerate(quote_rows) if q.timestamp < window_start]
    if seed_indices:
        quote_rows = quote_rows[seed_indices[-1]:]
    else:
        reasons.append("missing_pre_window_book")
    instrument = quote_rows[0].instrument if quote_rows else ""
    ofi = ofi_depth = quote_age = spread = None
    updates = [q for q in quote_rows if window_start <= q.timestamp < window_end]
    quote_bad = any(not _book_valid(q) or not q.instrument or q.received_at < q.timestamp
                    for q in quote_rows)
    if quote_bad:
        reasons.append("invalid_book")
    if quote_rows and not quote_bad:
        try:
            increments = [canonical_ofi(a, b) for a, b in zip(quote_rows, quote_rows[1:])]
            if seed_indices:
                ofi = sum(increments)
                depths = [(q.bid_size + q.ask_size) / 2 for q in updates or quote_rows[-1:]]
                ofi_depth = ofi / (sum(depths) / len(depths))
            last_quote = quote_rows[-1]
            quote_age = (observed_at - last_quote.timestamp).total_seconds()
            spread = (last_quote.ask - last_quote.bid) / ((last_quote.ask + last_quote.bid) / 2) * 10000
            if _finite(max_quote_age_seconds) and quote_age > max_quote_age_seconds:
                reasons.append("stale_book")
        except ValueError:
            reasons.append("invalid_book_sequence")
    if not quote_rows:
        reasons.append("missing_book_updates")

    prints: list[TradePrint] = []
    for trade in trades:
        if not _aware(trade.timestamp) or not _aware(trade.received_at):
            reasons.append("invalid_trade_timestamp")
            continue
        if window_start <= trade.timestamp < window_end and trade.received_at <= observed_at:
            prints.append(trade)
    seen: set[str] = set()
    trade_bad = False
    last_time = last_receive = None
    for trade in prints:
        if (not _finite(trade.quantity) or trade.quantity <= 0
                or not _finite(trade.price) or trade.price <= 0
                or trade.aggressor not in (-1, 0, 1, None)
                or isinstance(trade.aggressor, bool)
                or not trade.trade_id or trade.trade_id in seen
                or trade.instrument != instrument or trade.received_at < trade.timestamp
                or (last_time is not None and trade.timestamp < last_time)
                or (last_receive is not None and trade.received_at < last_receive)):
            trade_bad = True
        seen.add(trade.trade_id)
        last_time, last_receive = trade.timestamp, trade.received_at
    if trade_bad:
        reasons.append("invalid_trade_prints")
    if not prints:
        reasons.append("missing_trade_prints")
    total = classified = unknown = 0.0
    delta = ratio = recent_delta = coverage = recent_coverage = None
    if prints and not trade_bad and real_trade_prints is True:
        total = sum(t.quantity for t in prints)
        classified = sum(t.quantity for t in prints if t.aggressor in (-1, 1))
        unknown = total - classified
        coverage = classified / total
        # All-unknown flow remains missing, rather than fabricated neutral flow.
        if classified > 0:
            delta = sum(t.quantity * (t.aggressor or 0) for t in prints)
            ratio = delta / total
        recent = [t for t in prints if t.timestamp >= window_end - timedelta(minutes=1)]
        if recent:
            recent_total = sum(t.quantity for t in recent)
            recent_known = sum(t.quantity for t in recent if t.aggressor in (-1, 1))
            recent_coverage = recent_known / recent_total
            if recent_known > 0:
                recent_delta = sum(t.quantity * (t.aggressor or 0) for t in recent)
        if classified == 0:
            reasons.append("unclassified_trade_flow")
    return MarketFlow(
        instrument=instrument, window_start=window_start, window_end=window_end,
        observed_at=observed_at, ofi=ofi, ofi_depth=ofi_depth, cvd_delta=delta,
        cvd_ratio=ratio, recent_cvd_delta=recent_delta, total_volume=total,
        classified_volume=classified, unknown_volume=unknown,
        classification_coverage=coverage, recent_classification_coverage=recent_coverage,
        quote_age_seconds=quote_age, spread_bps=spread, quote_updates=len(updates),
        trade_count=len(prints), reasons=tuple(dict.fromkeys(reasons)),
    )


def candidate_rejection_reasons(row: Mapping[str, Any], config: GateConfig) -> tuple[str, ...]:
    """Validate a complete paired candidate, rejecting absent evidence.

    Required scalar keys: symbol, future_contract, oi_start_contract,
    oi_end_contract, window_start, window_end, available_at,
    futures_return_5m, spot_return_5m, oi_delta_5m, oi_delta_percent,
    oi_age_seconds. futures_flow and spot_flow must be MarketFlow objects.
    Calibration thresholds must be explicit positive numbers. The recent-CVD
    gate is a conservative research default, not a validated trading rule.
    """
    reasons: list[str] = []
    for name in ("max_oi_age_seconds", "max_quote_age_seconds", "max_spread_bps"):
        value = getattr(config, name)
        if not _finite(value) or value <= 0:
            reasons.append(f"uncalibrated_{name}")
    if not _finite(config.min_trade_side_coverage) or not 0 < config.min_trade_side_coverage <= 1:
        reasons.append("invalid_min_trade_side_coverage")
    if not row.get("symbol") or not row.get("future_contract"):
        reasons.append("missing_instrument_mapping")
    if not row.get("future_contract") or not (
        row.get("future_contract") == row.get("oi_start_contract") == row.get("oi_end_contract")
    ):
        reasons.append("oi_contract_mismatch")
    times_valid = all(_aware(row.get(k)) for k in ("window_start", "window_end", "available_at"))
    if not times_valid:
        reasons.append("missing_candidate_timestamps")
    elif (row["window_end"] - row["window_start"] != timedelta(minutes=5)
          or row["available_at"] < row["window_end"]
          or row["window_end"].astimezone(IST).minute % 5 != 0
          or row["window_end"].second != 0 or row["window_end"].microsecond != 0):
        reasons.append("invalid_completed_five_minute_window")
    for key in ("futures_return_5m", "spot_return_5m", "oi_delta_5m", "oi_delta_percent"):
        value = row.get(key)
        if not _finite(value) or value <= 0:
            reasons.append(f"nonpositive_or_missing_{key}")
    oi_age = row.get("oi_age_seconds")
    if not _finite(oi_age) or oi_age < 0:
        reasons.append("missing_or_invalid_oi_age")
    elif _finite(config.max_oi_age_seconds) and oi_age > config.max_oi_age_seconds:
        reasons.append("stale_oi")
    for market, instrument_key in (("futures", "future_contract"), ("spot", "symbol")):
        flow = row.get(f"{market}_flow")
        if not isinstance(flow, MarketFlow):
            reasons.append(f"missing_{market}_flow")
            continue
        reasons.extend(f"{market}_{reason}" for reason in flow.reasons)
        if flow.instrument != row.get(instrument_key):
            reasons.append(f"{market}_instrument_mismatch")
        if times_valid and (flow.window_start != row["window_start"]
                            or flow.window_end != row["window_end"]
                            or flow.observed_at > row["available_at"]):
            reasons.append(f"{market}_window_or_observability_mismatch")
        for key in ("ofi_depth", "cvd_ratio", "cvd_delta", "recent_cvd_delta"):
            if not _finite(getattr(flow, key)):
                reasons.append(f"missing_{market}_{key}")
        if _finite(flow.recent_cvd_delta) and flow.recent_cvd_delta <= 0:
            reasons.append(f"nonpositive_{market}_recent_cvd")
        for key in ("classification_coverage", "recent_classification_coverage"):
            value = getattr(flow, key)
            if (not _finite(value) or value > 1 or value < 0
                    or not _finite(config.min_trade_side_coverage)
                    or value < config.min_trade_side_coverage):
                reasons.append(f"insufficient_{market}_{key}")
        age = flow.quote_age_seconds
        # A once-fresh flow cannot be reused later with its old age value.
        if times_valid and _aware(flow.observed_at) and _finite(age):
            age += (row["available_at"] - flow.observed_at).total_seconds()
        if not _finite(age) or age < 0 or (_finite(config.max_quote_age_seconds)
                                           and age > config.max_quote_age_seconds):
            reasons.append(f"stale_or_missing_{market}_book")
        if (not _finite(flow.spread_bps) or flow.spread_bps < 0
                or (_finite(config.max_spread_bps) and flow.spread_bps > config.max_spread_bps)):
            reasons.append(f"wide_or_missing_{market}_spread")
        if flow.trade_count <= 0 or flow.total_volume <= 0:
            reasons.append(f"missing_{market}_trade_volume")
    return tuple(dict.fromkeys(reasons))


def _feature_values(row: Mapping[str, Any]) -> dict[str, float | None]:
    values = {"oi_delta_percent": row.get("oi_delta_percent")}
    for market in ("futures", "spot"):
        flow = row.get(f"{market}_flow")
        values[f"{market}_ofi"] = flow.ofi_depth if isinstance(flow, MarketFlow) else row.get(f"{market}_ofi")
        values[f"{market}_cvd"] = flow.cvd_ratio if isinstance(flow, MarketFlow) else row.get(f"{market}_cvd")
    return values


def score_candidate(row: Mapping[str, Any], history: Sequence[Mapping[str, Any]], *,
                    decision_at: datetime, config: GateConfig) -> ScoredCandidate:
    """Score with prior-session, same-symbol, same-IST-slot empirical CDFs.

    History rows must include symbol, window_end, available_at and five raw
    SCORE_WEIGHTS feature keys (or the corresponding MarketFlow objects).
    They are already quality-approved training observations; never pass
    rejected/missing feed rows. At most one observation per prior session is
    permitted. Current/future sessions and not-yet-received rows are excluded.
    Percentile is count(history <= current)/n, a deterministic empirical CDF.
    """
    if not _aware(decision_at):
        raise ValueError("Decision timestamp must be timezone-aware")
    end, available = row.get("window_end"), row.get("available_at")
    # Ages are supplied as of available_at, and must advance to the actual
    # ranking decision; an old row is not a fresh executable book.
    evaluated_row = dict(row)
    if _aware(available) and available <= decision_at:
        evaluated_row["available_at"] = decision_at
        if _finite(row.get("oi_age_seconds")):
            evaluated_row["oi_age_seconds"] += (decision_at - available).total_seconds()
    reasons = list(candidate_rejection_reasons(evaluated_row, config))
    if not _aware(available) or available > decision_at:
        reasons.append("candidate_not_observed_at_decision")
    if not isinstance(config.min_history_sessions, int) or isinstance(config.min_history_sessions, bool) or config.min_history_sessions < 1:
        reasons.append("invalid_min_history_sessions")
    percentiles: dict[str, float] = {}
    counts: dict[str, int] = {}
    if not reasons:
        local_end = end.astimezone(IST)
        usable: list[Mapping[str, Any]] = []
        days = set()
        for old in history:
            old_end, old_available = old.get("window_end"), old.get("available_at")
            if (old.get("symbol") != row.get("symbol") or not _aware(old_end)
                    or not _aware(old_available) or old_available > decision_at
                    or old_available < old_end):
                continue
            local_old = old_end.astimezone(IST)
            if local_old.date() >= local_end.date() or local_old.time() != local_end.time():
                continue
            if local_old.date() in days:
                reasons.append("duplicate_training_session_slot")
                continue
            days.add(local_old.date())
            usable.append(old)
        current_values = _feature_values(row)
        for key in SCORE_WEIGHTS:
            values = [_feature_values(old)[key] for old in usable]
            values = [value for value in values if _finite(value)]
            counts[key] = len(values)
            if len(values) < config.min_history_sessions:
                reasons.append(f"insufficient_prior_history_{key}")
            else:
                percentiles[key] = sum(value <= current_values[key] for value in values) / len(values)
    score = sum(SCORE_WEIGHTS[k] * percentiles[k] for k in SCORE_WEIGHTS) if not reasons else None
    return ScoredCandidate(str(row.get("symbol", "")), str(row.get("future_contract", "")),
                           end, score, percentiles, counts, tuple(dict.fromkeys(reasons)))


def rank_candidates(rows: Sequence[Mapping[str, Any]], history: Sequence[Mapping[str, Any]], *,
                    decision_at: datetime, config: GateConfig, top_k: int = 3) -> list[ScoredCandidate]:
    """Return deterministic top K eligible candidates at ONE completed slot.

    Ties break by symbol then exact contract. Mixing completed slots or
    duplicate symbols raises rather than retroactively comparing unlike times.
    Rejections can be inspected separately with score_candidate.
    """
    if not isinstance(top_k, int) or isinstance(top_k, bool) or top_k < 1:
        raise ValueError("top_k must be a positive integer")
    ends = {r.get("window_end") for r in rows if _aware(r.get("window_end"))}
    if len(ends) > 1:
        raise ValueError("Rank one completed decision slot at a time")
    symbols = [r.get("symbol") for r in rows]
    if len(symbols) != len(set(symbols)):
        raise ValueError("Rank each underlying once per decision slot")
    scored = [score_candidate(r, history, decision_at=decision_at, config=config) for r in rows]
    eligible = sorted((s for s in scored if s.eligible), key=lambda s: (-s.score, s.symbol, s.future_contract))
    return [ScoredCandidate(s.symbol, s.future_contract, s.window_end, s.score,
                            s.percentiles, s.history_counts, s.reasons, rank)
            for rank, s in enumerate(eligible[:top_k], start=1)]
