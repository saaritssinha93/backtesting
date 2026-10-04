"""Constant-time bearer authentication and server-owned capabilities."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Callable

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .errors import ApiError


bearer = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class Principal:
    name: str
    capabilities: frozenset[str]


def authenticated_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> Principal:
    settings = request.app.state.settings
    if (
        credentials is None
        or credentials.scheme.lower() != "bearer"
        or not hmac.compare_digest(credentials.credentials, settings.api_token)
    ):
        raise ApiError(401, "AUTH_REQUIRED", "A valid bearer token is required")
    return Principal(settings.principal, settings.capabilities)


def require_capability(capability: str) -> Callable:
    def dependency(principal: Principal = Depends(authenticated_principal)) -> Principal:
        if capability not in principal.capabilities:
            raise ApiError(403, "CAPABILITY_DENIED", "The principal lacks this capability")
        return principal

    return dependency
