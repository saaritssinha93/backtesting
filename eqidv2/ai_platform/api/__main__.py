"""Run the loopback API after explicit environment configuration."""

from __future__ import annotations

import os

import uvicorn

from .app import create_app
from .config import ApiSettings


def main() -> None:
    settings = ApiSettings.from_env()
    port = int(os.environ.get("AI_PLATFORM_API_PORT", "8788"))
    if port < 1024 or port > 65535:
        raise ValueError("AI_PLATFORM_API_PORT must be between 1024 and 65535")
    uvicorn.run(create_app(settings), host="127.0.0.1", port=port, access_log=False)


if __name__ == "__main__":
    main()
