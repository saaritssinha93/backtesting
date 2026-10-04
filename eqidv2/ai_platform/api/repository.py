"""Deterministic API queries over the Stage 1 adapters."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

from ai_platform.adapters.common import ArtifactError, file_sha256, read_json
from ai_platform.adapters.daily_replay import read_daily_replay
from ai_platform.adapters.equity_orders import preferred_filled_equity, read_equity_orders
from ai_platform.adapters.evidence import list_evidence
from ai_platform.adapters.options_orders import read_option_orders
from ai_platform.adapters.snapshot import read_coherent_snapshot
from ai_platform.adapters.status import inspect_status
from ai_platform.services.accounting import summarize_options_by_run_kind

from .cache import TTLCache
from .registry import SourceRegistry
from .serialization import public_value


EQUITY_PROFILE = "V13_V10_G"
OPTION_PROFILE = "V13_V10_G_OPTIONS_ONE_LOT_ATM_SL30_T40P4"


class PlatformRepository:
    def __init__(
        self,
        source_registry: SourceRegistry,
        profile_registry_path: Path,
        *,
        cache_ttl_seconds: float = 2.0,
        heartbeat_max_age_seconds: float = 120.0,
    ):
        self.sources = source_registry
        self.profile_registry_path = profile_registry_path.resolve()
        self.cache = TTLCache(cache_ttl_seconds)
        self.heartbeat_max_age = timedelta(seconds=heartbeat_max_age_seconds)

    def _profiles(self) -> dict[str, Any]:
        def load() -> dict[str, Any]:
            payload, _, _ = read_json(self.profile_registry_path)
            profiles = payload.get("profiles")
            if not isinstance(profiles, dict):
                raise ArtifactError("Profile registry has no profiles object")
            return profiles

        return self.cache.get_or_create("profiles", load)

    def _equity_identity(self) -> tuple[str, str]:
        profile = self._profiles()[EQUITY_PROFILE]
        return str(profile["strategy_version"]), str(profile["strategy_fingerprint"])

    def _option_identity(self) -> tuple[str, str, str]:
        profile = self._profiles()[OPTION_PROFILE]
        return (
            str(profile["strategy_version"]),
            str(profile["equity_strategy_version"]),
            str(profile["strategy_fingerprint"]),
        )

    def strategy(self) -> dict[str, Any]:
        profiles = self._profiles()
        base = profiles[EQUITY_PROFILE]
        frozen_path = self.sources.path("frozen_strategy_config")
        observed_sha = file_sha256(frozen_path)
        expected_sha = str(base["frozen_config_sha256"]).lower()
        if observed_sha != expected_sha:
            raise ArtifactError("Frozen strategy configuration failed its registered hash")
        settings, _, _ = read_json(frozen_path)
        if settings.get("morning_slots", False) or settings.get("two_bar_continuation", False):
            raise ArtifactError("Rejected strategy expansions are enabled")
        return {
            "profile": EQUITY_PROFILE,
            "identity": {
                "label": base["label"],
                "strategy_version": base["strategy_version"],
                "strategy_fingerprint": base["strategy_fingerprint"],
                "live_generation": base["live_generation"],
                "frozen_config_sha256": observed_sha,
            },
            "confirmation_policy": base["confirmation_policy"],
            "execution_profiles": {
                key: profiles[key] for key in base["execution_profiles"]
            },
            "evidence_class": base["evidence_class"],
        }

    def ready(self) -> dict[str, Any]:
        strategy = self.strategy()
        required = (
            "daily_replay_latest_json",
            "strategy_manifest",
            "equity_paper_orders",
            "equity_live_orders",
            "option_paper_orders",
            "immutable_evidence",
        )
        availability = {
            source_id: self.sources.path(source_id).exists() for source_id in required
        }
        return {
            "ready": all(availability.values()),
            "strategy_version": strategy["identity"]["strategy_version"],
            "sources": availability,
        }

    def readiness(self) -> dict[str, Any]:
        version, fingerprint = self._equity_identity()
        status_path = self.sources.path("equity_live_status")
        heartbeat_path = self.sources.path("equity_live_heartbeat")
        status = inspect_status(
            status_path,
            expected_schema="fno_v6_live_kite_qty1_status_v1",
            expected_strategy_version=version,
            expected_strategy_fingerprint=fingerprint,
            required_fields=("session_date", "state", "execution_mode"),
        )
        heartbeat = inspect_status(
            heartbeat_path,
            expected_schema="fno_v6_live_kite_qty1_heartbeat_v1",
            expected_strategy_version=version,
            expected_strategy_fingerprint=fingerprint,
            required_fields=("session_date", "state"),
        )
        # Match the dashboard policy: freshness is operationally required only
        # while the worker declares itself RUNNING.  Completed sessions remain
        # historical evidence rather than becoming false overnight alarms.
        if (
            heartbeat.payload
            and str(heartbeat.payload.get("state", "")).upper() == "RUNNING"
        ):
            heartbeat = inspect_status(
                heartbeat_path,
                expected_schema="fno_v6_live_kite_qty1_heartbeat_v1",
                expected_strategy_version=version,
                expected_strategy_fingerprint=fingerprint,
                required_fields=("session_date", "state"),
                max_age=self.heartbeat_max_age,
            )
        coherent = None
        if status_path.is_file() and heartbeat_path.is_file():
            coherent = read_coherent_snapshot(
                {"status": status_path, "heartbeat": heartbeat_path},
                equal_fields=("session_date", "strategy_version", "strategy_fingerprint", "state"),
            ).snapshot_id
        return {
            "status": public_value(status),
            "heartbeat": public_value(heartbeat),
            "coherent_snapshot_id": coherent,
            "freshness_policy": {
                "running_heartbeat_max_age_seconds": self.heartbeat_max_age.total_seconds(),
                "applies_when_state": "RUNNING",
            },
        }

    def equity_orders(self, session_date: date):
        version, fingerprint = self._equity_identity()
        return read_equity_orders(
            self.sources.path("equity_paper_orders"),
            self.sources.path("equity_live_orders"),
            session_date,
            expected_strategy_version=version,
            expected_strategy_fingerprint=fingerprint,
        )

    def option_orders(self, session_date: date):
        option_version, equity_version, fingerprint = self._option_identity()
        return read_option_orders(
            self.sources.path("option_paper_orders"),
            session_date,
            expected_strategy_version=option_version,
            expected_equity_strategy_version=equity_version,
            expected_strategy_fingerprint=fingerprint,
        )

    @staticmethod
    def _source_ref(source) -> dict[str, Any]:
        return {
            "source_id": source.source_id,
            "sha256": source.sha256,
            "modified_at_ist": public_value(source.modified_at_ist),
        }

    def _equity_view(self, row) -> dict[str, Any]:
        return {
            "asset": "EQUITY",
            "signal_id": row.signal_id,
            "session_date": row.session_date,
            "mode": row.mode,
            "status": row.status,
            "strategy_version": row.strategy_version,
            "strategy_fingerprint": row.strategy_fingerprint,
            "symbol": row.symbol,
            "side": row.side,
            "quantity": row.quantity,
            "entry_price": row.entry_price,
            "exit_price": row.exit_price,
            "realized_net_pnl_rs": row.realized_net_pnl_rs,
            "event_at_ist": row.event_at_ist,
            "source": self._source_ref(row.source),
        }

    def _option_view(self, row) -> dict[str, Any]:
        return {
            "asset": "OPTIONS",
            "trade_id": row.trade_id,
            "signal_id": row.signal_id,
            "session_date": row.session_date,
            "mode": row.mode,
            "status": row.status,
            "strategy_version": row.strategy_version,
            "equity_strategy_version": row.equity_strategy_version,
            "strategy_fingerprint": row.strategy_fingerprint,
            "source_equity_mode": row.source_equity_mode,
            "execution_source": row.execution_source,
            "run_kind": row.run_kind,
            "option_symbol": row.option_symbol,
            "quantity": row.quantity,
            "entry_price": row.entry_price,
            "exit_price": row.exit_price,
            "realized_net_pnl_rs": row.realized_net_pnl_rs,
            "open_mark_net_pnl_rs": row.open_mark_net_pnl_rs,
            "event_at_ist": row.event_at_ist,
            "source": self._source_ref(row.source),
        }

    @staticmethod
    def _page(rows: list[dict[str, Any]], page: int, page_size: int) -> dict[str, Any]:
        total = len(rows)
        start = (page - 1) * page_size
        return {
            "items": rows[start : start + page_size],
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_more": start + page_size < total,
        }

    def signals(
        self,
        session_date: date,
        *,
        mode: str | None,
        symbol: str | None,
        preferred_only: bool,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        orders = self.equity_orders(session_date)
        if preferred_only:
            orders = list(preferred_filled_equity(orders).values())
        if mode:
            orders = [row for row in orders if row.mode == mode]
        if symbol:
            orders = [row for row in orders if row.symbol == symbol]
        rows = [self._equity_view(row) for row in orders]
        rows.sort(key=lambda row: (str(row["signal_id"]), str(row["mode"])))
        return self._page(rows, page, page_size)

    def trades(
        self,
        session_date: date,
        *,
        asset: str,
        mode: str | None,
        status: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        if asset in {"ALL", "EQUITY"}:
            rows.extend(self._equity_view(row) for row in self.equity_orders(session_date))
        if asset in {"ALL", "OPTIONS"}:
            rows.extend(self._option_view(row) for row in self.option_orders(session_date))
        if mode:
            rows = [row for row in rows if row["mode"] == mode]
        if status:
            rows = [row for row in rows if row["status"] == status]
        rows.sort(key=lambda row: (str(row.get("event_at_ist") or ""), str(row.get("signal_id") or "")))
        return self._page(rows, page, page_size)

    def signal_trace(self, signal_id: str, session_date: date) -> dict[str, Any] | None:
        equity = [row for row in self.equity_orders(session_date) if row.signal_id == signal_id]
        options = [row for row in self.option_orders(session_date) if row.signal_id == signal_id]
        if not equity and not options:
            return None
        evidence = []
        slot = None
        generation = str(self._profiles()[EQUITY_PROFILE]["live_generation"])
        if equity:
            signal_end = str(equity[0].raw.get("signal_end", ""))
            slot = signal_end.replace(":", "") if signal_end else None
        if slot:
            for kind in ("scanner_snapshot", "confirmation_snapshot"):
                records = list_evidence(
                    self.sources.path("immutable_evidence"),
                    session_date=session_date,
                    slot=slot,
                    artifact_kind=kind,
                    generation=generation,
                    strict=False,
                )
                evidence.extend(
                    {
                        "artifact_kind": row.artifact_kind,
                        "slot": row.slot,
                        "observed_at_ist": row.observed_at_ist,
                        "payload_sha256": row.payload_sha256,
                        "payload": row.payload,
                    }
                    for row in records
                )
        return {
            "signal_id": signal_id,
            "session_date": session_date,
            "equity_states": [self._equity_view(row) for row in equity],
            "option_states": [self._option_view(row) for row in options],
            "evidence": evidence,
        }

    def result_summary(self, session_date: date | None) -> dict[str, Any]:
        version, fingerprint = self._equity_identity()
        replay = read_daily_replay(
            self.sources.path("daily_replay_latest_json"),
            expected_strategy_version=version,
            expected_strategy_fingerprint=fingerprint,
        )
        if session_date is not None and replay.session_date != session_date:
            raise KeyError("result_date")
        day = replay.session_date
        equity = self.equity_orders(day)
        options = self.option_orders(day)
        option_summaries = summarize_options_by_run_kind(options)
        equity_closed = [row for row in equity if row.status == "CLOSED"]
        equity_realized = sum(
            (row.realized_net_pnl_rs for row in equity_closed if row.realized_net_pnl_rs is not None),
            start=0,
        )
        return {
            "session_date": day,
            "replay": replay,
            "equity": {
                "records": len(equity),
                "closed": len(equity_closed),
                "realized_net_pnl_rs": equity_realized,
                "modes": sorted({row.mode for row in equity}),
            },
            "options_by_run_kind": option_summaries,
        }

    def evidence(
        self,
        source_id: str,
        *,
        session_date: date | None,
        slot: str | None,
        artifact_kind: str | None,
        mode: str,
    ) -> dict[str, Any]:
        record = self.sources.record(source_id)
        path = self.sources.path(source_id)
        if source_id == "immutable_evidence":
            if session_date is None or slot is None or artifact_kind is None:
                raise ValueError("date, slot and artifact_kind are required for immutable evidence")
            records = list_evidence(
                path,
                session_date=session_date,
                slot=slot,
                artifact_kind=artifact_kind,
                strict=True,
            )
            selected = records[:1] if mode == "observed" else records[-1:]
            return {
                "source_id": source_id,
                "selection_mode": mode,
                "items": selected,
                "available_revisions": len(records),
            }
        if path.suffix.lower() != ".json":
            raise ValueError("This registered source is not available as JSON evidence")
        payload, _, sha = read_json(path)
        expected_sha = record.get("sha256")
        if expected_sha and str(expected_sha).lower() != sha:
            raise ArtifactError("Registered evidence hash mismatch")
        return {"source_id": source_id, "sha256": sha, "payload": payload}
