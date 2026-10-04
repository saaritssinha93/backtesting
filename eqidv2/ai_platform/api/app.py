"""FastAPI application factory. Importing this module starts no workers."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path as FilePath
from typing import Literal

from fastapi import Depends, FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, ConfigDict

from ai_platform.adapters.common import ArtifactError, IST, now_ist
from ai_platform.observability.runtime import create_observability
from ai_platform.observability.reconciliation import canonical_sha256
from ai_platform.observability.runtime_collector import (
    RuntimeMetricsCollector,
    merge_prometheus_samples,
)

from .auth import Principal, require_capability
from .config import ApiSettings
from .errors import ApiError
from .registry import SourceRegistry
from .repository import PlatformRepository
from .serialization import bounded_payload, public_value


class AssistantSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_date: date | None = None
    asset: Literal["EQUITY", "OPTIONS", "ALL"] = "ALL"
    mode: Literal["PAPER", "LIVE", "ALL"] = "ALL"
    profile: Literal["V13_V10_G"] = "V13_V10_G"


def _error_response(request: Request, status: int, code: str, message: str) -> JSONResponse:
    headers = {"X-Request-ID": request.state.request_id}
    trace_id = getattr(request.state, "trace_id", None)
    if trace_id:
        headers["X-Trace-ID"] = trace_id
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return JSONResponse(
        status_code=status,
        content={
            "ok": False,
            "error": {"code": code, "message": message},
            "meta": {"request_id": request.state.request_id, "trace_id": trace_id},
        },
        headers=headers,
    )


def _success(request: Request, data, *, status_code: int = 200) -> JSONResponse:
    try:
        cleaned = bounded_payload(data, request.app.state.settings.max_response_bytes)
    except ValueError as exc:
        raise ApiError(413, "RESPONSE_TOO_LARGE", str(exc)) from exc
    trace_id = getattr(request.state, "trace_id", None)
    headers = {"X-Request-ID": request.state.request_id}
    if trace_id:
        headers["X-Trace-ID"] = trace_id
    return JSONResponse(
        status_code=status_code,
        content={
            "ok": True,
            "data": cleaned,
            "meta": {
                "request_id": request.state.request_id,
                "trace_id": trace_id,
                "as_of_ist": now_ist().isoformat(),
            },
        },
        headers=headers,
    )


def _validate_day(value: date) -> date:
    today = datetime.now(tz=IST).date()
    if value < date(2020, 1, 1) or value > today:
        raise ApiError(422, "DATE_OUT_OF_RANGE", "Date must be between 2020-01-01 and today")
    return value


def _observability_file_status(path: FilePath) -> dict[str, object]:
    try:
        stat = path.stat()
        return {
            "file": str(path),
            "exists": path.is_file(),
            "size_bytes": stat.st_size if path.is_file() else None,
            "modified_at_utc": datetime.fromtimestamp(
                stat.st_mtime, tz=timezone.utc
            ).isoformat(),
        }
    except OSError:
        return {"file": str(path), "exists": False, "size_bytes": None}


def _configured_observability_path(environment_name: str, default: FilePath) -> FilePath:
    configured = os.environ.get(environment_name)
    try:
        return FilePath(configured).expanduser().resolve() if configured else default.resolve()
    except (OSError, RuntimeError, ValueError):
        # A malformed telemetry override must not prevent the API from starting.
        return default.resolve()


def _read_reconciliation_report(
    observability_root: FilePath,
    session_date: date,
    *,
    max_bytes: int,
) -> dict:
    directory = (observability_root / "reconciliation").resolve()
    candidate = (directory / f"{session_date.isoformat()}.json").resolve()
    try:
        candidate.relative_to(directory)
    except ValueError as exc:
        raise ApiError(422, "INVALID_RECONCILIATION_DATE", "Invalid session date") from exc
    if not candidate.is_file():
        raise ApiError(
            404,
            "RECONCILIATION_NOT_FOUND",
            "No reconciliation report exists for that session date",
        )
    try:
        if candidate.stat().st_size > max_bytes:
            raise ApiError(
                413,
                "RECONCILIATION_TOO_LARGE",
                "The reconciliation report exceeds the configured response limit",
            )
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except ApiError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ApiError(
            503,
            "RECONCILIATION_INVALID",
            "The reconciliation report is unavailable or invalid",
        ) from exc
    if not isinstance(payload, dict):
        raise ApiError(
            503,
            "RECONCILIATION_INVALID",
            "The reconciliation report must be a JSON object",
        )
    claimed_digest = payload.get("report_sha256")
    unsigned = {key: value for key, value in payload.items() if key != "report_sha256"}
    if (
        not isinstance(claimed_digest, str)
        or claimed_digest != canonical_sha256(unsigned)
    ):
        raise ApiError(
            503,
            "RECONCILIATION_DIGEST_MISMATCH",
            "The reconciliation report failed its integrity check",
        )
    report_date = payload.get("session_date")
    if report_date is not None and str(report_date) != session_date.isoformat():
        raise ApiError(
            503,
            "RECONCILIATION_DATE_MISMATCH",
            "The reconciliation report contains a different session date",
        )
    return payload


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    resolved = settings or ApiSettings.from_env()
    source_registry = SourceRegistry(resolved.source_registry_path, resolved.runtime_root)
    repository = PlatformRepository(
        source_registry,
        resolved.profile_registry_path,
        cache_ttl_seconds=resolved.cache_ttl_seconds,
        heartbeat_max_age_seconds=resolved.heartbeat_max_age_seconds,
    )
    app = FastAPI(
        title="V13-V10-G Read-only API",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = resolved
    app.state.repository = repository
    observability_root = FilePath(resolved.observability_root).resolve()
    log_path = _configured_observability_path(
        "AI_PLATFORM_OBS_LOG_PATH",
        observability_root / "logs" / "ai_platform_api.jsonl",
    )
    journal_path = _configured_observability_path(
        "AI_PLATFORM_OBS_JOURNAL_PATH",
        observability_root / "journals" / "ai_platform_api_events.jsonl",
    )
    observability = create_observability(
        "ai_platform_api", log_path=log_path, journal_path=journal_path
    )
    app.state.observability = observability
    runtime_metrics = RuntimeMetricsCollector(
        resolved.runtime_root,
        observability_root=observability_root,
        profile_registry_path=resolved.profile_registry_path,
        cache_ttl_seconds=min(5.0, max(0.0, resolved.cache_ttl_seconds)),
    )
    app.state.runtime_metrics_collector = runtime_metrics
    app.state.observability_paths = {
        "root": observability_root,
        "log": log_path,
        "journal": journal_path,
        "reconciliation": observability_root / "reconciliation",
    }
    # Probe the configured durable sink without making API construction depend
    # on telemetry storage. This is a configuration attestation, not proof that
    # an ASGI server has started accepting requests.
    observability.event(
        "api.runtime.configured",
        durable=True,
        api_version=app.version,
        process_id=os.getpid(),
        read_only=True,
        execution_authority=False,
    )
    http_requests = observability.metrics.counter(
        "http_requests_total",
        "HTTP requests completed by the read-only API.",
        label_names=("method", "route", "status_code"),
    )
    http_duration = observability.metrics.histogram(
        "http_request_duration_seconds",
        "HTTP request duration in seconds.",
        label_names=("method", "route", "status_class"),
    )
    http_in_flight = observability.metrics.gauge(
        "http_requests_in_flight",
        "HTTP requests currently executing.",
        label_names=("method",),
    )
    app.state.sessions: dict[str, dict] = {}
    app.state.sessions_lock = threading.RLock()
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["127.0.0.1", "localhost", "testserver", "host.docker.internal"],
    )

    @app.middleware("http")
    async def request_boundary(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))[:128]
        method = request.method.upper()[:16]
        started = time.perf_counter()
        status_code = 500
        error_type: str | None = None
        http_in_flight.inc(method=method)
        with observability.bind(request_id=request.state.request_id):
            with observability.span(
                "http.server.request",
                kind="server",
                carrier=request.headers,
                attributes={"http.method": method, "http.route": request.url.path},
            ) as span:
                request.state.trace_id = span.trace_id
                try:
                    length = request.headers.get("content-length")
                    if length:
                        try:
                            too_large = int(length) > resolved.max_request_bytes
                        except ValueError:
                            too_large = True
                        if too_large:
                            response = _error_response(
                                request,
                                413,
                                "REQUEST_TOO_LARGE",
                                "Request exceeds the configured limit",
                            )
                        else:
                            response = await call_next(request)
                    else:
                        response = await call_next(request)
                    status_code = response.status_code
                    span.set_attribute("http.status_code", status_code)
                    response.headers.setdefault("X-Request-ID", request.state.request_id)
                    response.headers.setdefault("X-Trace-ID", span.trace_id)
                    response.headers.setdefault("Cache-Control", "no-store")
                    response.headers.setdefault("X-Content-Type-Options", "nosniff")
                    return response
                except BaseException as exc:
                    error_type = type(exc).__name__
                    span.set_attribute("error.type", error_type)
                    raise
                finally:
                    duration = time.perf_counter() - started
                    route_object = request.scope.get("route")
                    route = getattr(route_object, "path", None) or "unmatched"
                    status_text = str(status_code)
                    status_class = f"{status_text[0]}xx" if status_text else "unknown"
                    http_requests.inc(
                        method=method, route=route, status_code=status_text
                    )
                    http_duration.observe(
                        duration,
                        method=method,
                        route=route,
                        status_class=status_class,
                    )
                    http_in_flight.dec(method=method)
                    observability.event(
                        "http.request.completed",
                        severity="ERROR" if status_code >= 500 else "INFO",
                        method=method,
                        route=route,
                        status_code=status_code,
                        duration_ms=round(duration * 1000, 3),
                        error_type=error_type,
                    )

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError):
        return _error_response(request, exc.status_code, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        return _error_response(request, 422, "INVALID_REQUEST", "Request parameters are invalid")

    @app.exception_handler(ArtifactError)
    async def artifact_error(request: Request, exc: ArtifactError):
        return _error_response(
            request,
            503,
            "SOURCE_INVALID",
            "A registered source is unavailable or failed validation",
        )

    @app.get("/api/v1/health/live")
    def live(request: Request):
        return _success(request, {"state": "LIVE", "service": "v13-v10-g-read-api"})

    @app.get("/api/v1/observability/metrics", response_class=PlainTextResponse)
    def metrics(
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        registry_exposition = observability.metrics.render_prometheus()
        runtime_samples = runtime_metrics.render_prometheus_samples()
        return PlainTextResponse(
            merge_prometheus_samples(registry_exposition, runtime_samples),
            media_type="text/plain; version=0.0.4",
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/api/v1/observability/status")
    def observability_status(
        request: Request,
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        journal = observability.event_journal
        return _success(
            request,
            {
                "service": observability.service,
                "root_directory": str(observability_root),
                "metrics": {
                    "endpoint": "/api/v1/observability/metrics",
                    "format": "prometheus-0.0.4",
                    "dropped_updates": observability.metrics.dropped_updates,
                },
                "logs": _observability_file_status(log_path),
                "journal": {
                    **_observability_file_status(journal_path),
                    "dropped_events": journal.dropped_events if journal else None,
                    "last_error": journal.last_error if journal else "not_configured",
                },
                "traces": {
                    "otlp_enabled": observability.tracer.otlp_enabled,
                    "endpoint": observability.tracer.otel_endpoint,
                    "setup_error": observability.tracer.otel_error,
                    "dropped_spans": observability.tracer.dropped_spans,
                },
                "reconciliation": {
                    "directory": str(observability_root / "reconciliation"),
                    "endpoint_template": (
                        "/api/v1/observability/reconciliation/{session_date}"
                    ),
                },
            },
        )

    @app.get("/api/v1/observability/reconciliation/{session_date}")
    def reconciliation_report(
        request: Request,
        session_date: date,
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        day = _validate_day(session_date)
        return _success(
            request,
            _read_reconciliation_report(
                observability_root,
                day,
                max_bytes=resolved.max_response_bytes,
            ),
        )

    @app.get("/api/v1/health/ready")
    def ready(
        request: Request,
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        data = repository.ready()
        if not data["ready"]:
            raise ApiError(503, "NOT_READY", "One or more registered sources are unavailable")
        return _success(request, data)

    @app.get("/api/v1/readiness")
    def readiness(
        request: Request,
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        return _success(request, repository.readiness())

    @app.get("/api/v1/strategies/v13-v10-g")
    def strategy(
        request: Request,
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        return _success(request, repository.strategy())

    @app.get("/api/v1/signals")
    def signals(
        request: Request,
        session_date: date = Query(alias="date"),
        mode: Literal["PAPER", "LIVE"] | None = None,
        symbol: str | None = Query(default=None, min_length=1, max_length=32, pattern=r"^[A-Z0-9&.-]+$"),
        preferred_only: bool = False,
        page: int = Query(default=1, ge=1, le=10000),
        page_size: int = Query(default=50, ge=1, le=100),
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        day = _validate_day(session_date)
        return _success(
            request,
            repository.signals(
                day,
                mode=mode,
                symbol=symbol,
                preferred_only=preferred_only,
                page=page,
                page_size=page_size,
            ),
        )

    @app.get("/api/v1/signals/{signal_id}/trace")
    def signal_trace(
        request: Request,
        signal_id: str = Path(min_length=1, max_length=160, pattern=r"^[A-Za-z0-9_.-]+$"),
        session_date: date = Query(alias="date"),
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        trace = repository.signal_trace(signal_id, _validate_day(session_date))
        if trace is None:
            raise ApiError(404, "SIGNAL_NOT_FOUND", "No signal exists for that date and identity")
        return _success(request, trace)

    @app.get("/api/v1/trades")
    def trades(
        request: Request,
        session_date: date = Query(alias="date"),
        asset: Literal["ALL", "EQUITY", "OPTIONS"] = "ALL",
        mode: Literal["PAPER", "LIVE"] | None = None,
        status: str | None = Query(default=None, min_length=1, max_length=32, pattern=r"^[A-Z_]+$"),
        page: int = Query(default=1, ge=1, le=10000),
        page_size: int = Query(default=50, ge=1, le=100),
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        return _success(
            request,
            repository.trades(
                _validate_day(session_date),
                asset=asset,
                mode=mode,
                status=status,
                page=page,
                page_size=page_size,
            ),
        )

    @app.get("/api/v1/results/summary")
    def results_summary(
        request: Request,
        session_date: date | None = Query(default=None, alias="date"),
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        if session_date is not None:
            _validate_day(session_date)
        try:
            data = repository.result_summary(session_date)
        except KeyError as exc:
            raise ApiError(404, "RESULT_NOT_FOUND", "The latest result is for another date") from exc
        return _success(request, data)

    @app.get("/api/v1/evidence/{source_id}")
    def evidence(
        request: Request,
        source_id: str = Path(min_length=1, max_length=80, pattern=r"^[a-z0-9_]+$"),
        session_date: date | None = Query(default=None, alias="date"),
        slot: str | None = Query(default=None, pattern=r"^[0-2][0-9][0-5][0-9]$"),
        artifact_kind: str | None = Query(
            default=None, min_length=1, max_length=80, pattern=r"^[a-z0-9_]+$"
        ),
        mode: Literal["observed", "counterfactual"] = "observed",
        principal: Principal = Depends(require_capability("read:evidence")),
    ):
        if session_date is not None:
            _validate_day(session_date)
        try:
            data = repository.evidence(
                source_id,
                session_date=session_date,
                slot=slot,
                artifact_kind=artifact_kind,
                mode=mode,
            )
        except KeyError as exc:
            raise ApiError(404, "SOURCE_NOT_REGISTERED", "The source ID is not registered") from exc
        except ValueError as exc:
            raise ApiError(422, "INVALID_EVIDENCE_REQUEST", str(exc)) from exc
        return _success(request, data)

    @app.post("/api/v1/assistant/sessions")
    def create_session(
        request: Request,
        body: AssistantSessionRequest,
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        if body.session_date is not None:
            _validate_day(body.session_date)
        session_id = str(uuid.uuid4())
        created = now_ist()
        record = {
            "session_id": session_id,
            "state": "DETERMINISTIC_READY",
            "ai_enabled": False,
            "context": body.model_dump(mode="json"),
            "created_at_ist": created,
            "expires_at_ist": created + timedelta(hours=1),
            "principal": principal.name,
        }
        with app.state.sessions_lock:
            app.state.sessions[session_id] = record
        return _success(request, record, status_code=201)

    @app.get("/api/v1/assistant/sessions/{session_id}")
    def get_session(
        request: Request,
        session_id: str = Path(pattern=r"^[0-9a-f-]{36}$"),
        principal: Principal = Depends(require_capability("read:platform")),
    ):
        with app.state.sessions_lock:
            record = app.state.sessions.get(session_id)
        if record is None or record["principal"] != principal.name:
            raise ApiError(404, "SESSION_NOT_FOUND", "Assistant session was not found")
        return _success(request, record)

    return app
