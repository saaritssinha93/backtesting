# Stage 2: authenticated read-only API

The API is a separate loopback service. Importing its package or creating the application does not start scanners, fetchers, trading roles, schedulers, or broker sessions. All data paths come from the source registry and must remain inside the configured runtime root.

## Pinned environment

The repository-local `.venv-ai-platform` is created with Python 3.12 and [requirements.txt](../../ai_platform/requirements.txt). Recreate it with:

```powershell
python -m venv .venv-ai-platform
.\.venv-ai-platform\Scripts\python.exe -m pip install --requirement ai_platform\requirements.lock
```

`requirements.txt` records the direct dependencies. `requirements.lock` pins the complete resolved environment, including FastAPI 0.141.1, Uvicorn 0.53.0, HTTPX2 2.13.0, OpenTelemetry API/SDK and OTLP/HTTP exporter 1.44.0, pytest 9.1.1 and tzdata 2026.4. The environment is local build output and is not committed.

This venv is the API/CLI environment, not the interpreter used by the live and
replay launchers. At the 2026-09-25 check it imported the OTel SDK/exporter but
`doctor` reported `READY_WITH_OPTIONAL_GAPS` because pandas/numpy were not
installed, so the optional `data-quality` command is unavailable there. The
separately pinned system Python used by trading workers has pandas/numpy and the
pinned OpenTelemetry 1.44 SDK/exporter installed. Package installs in either
environment do not affect the other.

## Authentication and startup

Generate a fresh token for each deployment context and keep it only in the process environment:

```powershell
$env:AI_PLATFORM_API_TOKEN = python -c "import secrets; print(secrets.token_urlsafe(48))"
$env:EQIDV2_RUNTIME_ROOT = "C:\TradingData\eqidv2"
$env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://127.0.0.1:4318/v1/traces"
$env:OTEL_EXPORTER_OTLP_TRACES_PROTOCOL = "http/protobuf"
.\.venv-ai-platform\Scripts\python.exe -m ai_platform.api
```

The service binds only to `127.0.0.1:8788`. It refuses to start without a token of at least 32 characters. Interactive OpenAPI/Redoc routes are disabled. Do not add port 8788 to the existing Cloudflare launcher during this stage.

Example authenticated request:

```powershell
$headers = @{ Authorization = "Bearer $env:AI_PLATFORM_API_TOKEN" }
Invoke-RestMethod -Uri "http://127.0.0.1:8788/api/v1/results/summary?date=2026-09-17" -Headers $headers
```

Stop the foreground process with `Ctrl+C`. No scheduled task or automatic startup definition is created in Stage 2.

## Route boundary

- `GET /api/v1/health/live` is the only unauthenticated route and performs no source reads.
- `GET /api/v1/health/ready`, `/readiness`, `/strategies/v13-v10-g`, `/signals`, `/signals/{id}/trace`, `/trades`, and `/results/summary` require `read:platform`.
- `GET /api/v1/observability/metrics` requires `read:platform` and returns the in-process registry merged with the cached, read-only runtime filesystem projection in Prometheus 0.0.4 format.
- `GET /api/v1/observability/status` requires `read:platform` and reports the configured logs, journal, metrics and OTLP trace state.
- `GET /api/v1/observability/reconciliation/{session_date}` requires `read:platform` and returns only a bounded report already published below the configured observability root. It requires and recomputes `report_sha256`, verifies the report date, and returns HTTP 503 `RECONCILIATION_DIGEST_MISMATCH` rather than serving missing/tampered digest evidence.
- `GET /api/v1/evidence/{source_id}` requires `read:evidence`. The source ID must exist in the registry; clients cannot provide a filesystem path.
- `POST/GET /api/v1/assistant/sessions` creates bounded in-memory deterministic sessions. `ai_enabled=false` remains explicit until Stage 3 provides and validates a backend.

Requests and responses are bounded, list routes are paginated, inputs use strict formats, credential-like fields are redacted, and operational source failures return stable generic codes. Trading/evidence payloads omit local source paths; the authenticated observability status route intentionally reports its configured local sink paths to the operator.

OTLP trace export is opt-in and fail-open. `run_ai_platform_api.bat` supplies the
local Alloy endpoint unless the caller already set one. Set
`OTEL_SDK_DISABLED=true` for local-only spans. The API continues to write
redacted JSON logs and a hash-chained event journal under
`%EQIDV2_RUNTIME_ROOT%\observability` whether or not an OTLP collector is
available. Operational usage, dashboards and acceptance status are documented
in [OBSERVABILITY_OPERATIONS.md](../OBSERVABILITY_OPERATIONS.md).

Confirm capabilities with the same interpreter that will run the process:

```powershell
.\.venv-ai-platform\Scripts\python.exe -m ai_platform.observability doctor
& "C:\Users\Saarit\AppData\Local\Programs\Python\Python312\python.exe" `
  -c "import opentelemetry.sdk; import opentelemetry.exporter.otlp.proto.http.trace_exporter; print('worker OTLP ready')"
```

Both checks pass on the 2026-09-25 activated workstation. If either environment
is rebuilt, rerun them before relying on OTLP export; local logs, journals,
metrics and fallback spans remain fail-open if export is unavailable.

## Rollback

Stop the API process and remove `.venv-ai-platform` if the isolated environment is no longer required. Existing dashboard and trading processes do not depend on this service.
