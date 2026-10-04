"""Run the observability diagnostics CLI."""

from .cli import main


if __name__ == "__main__":  # pragma: no cover - exercised by subprocess use
    raise SystemExit(main())

