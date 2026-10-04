"""Structured, redacted JSON logging without a mandatory third-party package."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Mapping

from .context import CorrelationContext, current_context
from .redaction import redact, redact_text, safe_json_dumps


class JsonFormatter(logging.Formatter):
    """Format a log record as one stable JSON object per line."""

    def __init__(
        self,
        *,
        service: str | None = None,
        static_fields: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__()
        self.service = service
        self.static_fields = dict(static_fields or {})

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc)
        context = current_context().as_dict()
        if self.service and "service" not in context:
            context["service"] = self.service
        event = getattr(record, "obs_event", None) or record.getMessage()
        payload: dict[str, Any] = {
            "schema_version": "eqidv2_observability_log_v1",
            "timestamp_utc": timestamp.isoformat(timespec="milliseconds").replace(
                "+00:00", "Z"
            ),
            "severity": record.levelname,
            "logger": record.name,
            "event": redact_text(str(event)),
            "context": context,
        }
        message = getattr(record, "obs_message", None)
        if message:
            payload["message"] = redact_text(str(message))
        fields = getattr(record, "obs_fields", None)
        if fields:
            payload["fields"] = fields
        if self.static_fields:
            payload["resource"] = self.static_fields
        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else "Exception",
                "message": redact_text(str(exc_value)) if exc_value else "",
                # Tracebacks are useful operationally and are redacted as text.
                "stacktrace": redact_text(self.formatException(record.exc_info)),
            }
        return safe_json_dumps(payload)


class EventLogger:
    """Fail-open facade that keeps event names separate from field values."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        static_fields: Mapping[str, object] | None = None,
    ) -> None:
        self.logger = logger
        self.static_fields = dict(static_fields or {})
        self.dropped_events = 0

    def bind(self, **fields: object) -> "EventLogger":
        return EventLogger(self.logger, static_fields={**self.static_fields, **fields})

    def emit(
        self,
        level: int | str,
        event: str,
        *,
        message: str | None = None,
        exc_info: bool | BaseException | tuple | None = None,
        **fields: object,
    ) -> bool:
        try:
            level_number = (
                logging._nameToLevel.get(level.upper(), logging.INFO)
                if isinstance(level, str)
                else int(level)
            )
            exception_info: bool | tuple
            if isinstance(exc_info, BaseException):
                exception_info = (type(exc_info), exc_info, exc_info.__traceback__)
            else:
                exception_info = exc_info or False
            self.logger.log(
                level_number,
                event,
                exc_info=exception_info,
                extra={
                    "obs_event": event,
                    "obs_message": message,
                    "obs_fields": redact({**self.static_fields, **fields}),
                },
            )
            return True
        except Exception:
            # Telemetry must never change the outcome of a trading operation.
            self.dropped_events += 1
            return False

    def debug(self, event: str, **fields: object) -> bool:
        return self.emit(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: object) -> bool:
        return self.emit(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: object) -> bool:
        return self.emit(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: object) -> bool:
        return self.emit(logging.ERROR, event, **fields)

    def exception(self, event: str, **fields: object) -> bool:
        return self.emit(logging.ERROR, event, exc_info=True, **fields)


def configure_json_logger(
    name: str,
    *,
    service: str | None = None,
    level: int | str = logging.INFO,
    stream: IO[str] | None = None,
    path: Path | str | None = None,
    max_bytes: int = 25 * 1024 * 1024,
    backup_count: int = 10,
    propagate: bool = False,
    static_fields: Mapping[str, object] | None = None,
) -> EventLogger:
    """Create an isolated JSON event logger.

    Reconfiguring the same name replaces only handlers installed by this
    function.  File logging is explicitly opt-in and rotates by size.
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = propagate
    for handler in list(logger.handlers):
        if getattr(handler, "_eqidv2_observability", False):
            logger.removeHandler(handler)
            handler.close()

    formatter = JsonFormatter(service=service, static_fields=static_fields)
    handlers: list[logging.Handler] = []
    if path is not None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            logging.handlers.RotatingFileHandler(
                output_path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding="utf-8",
            )
        )
    if stream is not None:
        handlers.append(logging.StreamHandler(stream))
    if not handlers:
        handlers.append(logging.NullHandler())
    for handler in handlers:
        handler.setFormatter(formatter)
        handler._eqidv2_observability = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
    return EventLogger(logger)


def console_json_logger(name: str, *, service: str | None = None) -> EventLogger:
    return configure_json_logger(name, service=service, stream=sys.stderr)


__all__ = ["EventLogger", "JsonFormatter", "configure_json_logger", "console_json_logger"]
