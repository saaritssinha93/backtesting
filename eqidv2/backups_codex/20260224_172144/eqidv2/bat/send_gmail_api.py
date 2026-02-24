#!/usr/bin/env python
"""
Send a plain-text email via Gmail API using OAuth tokens.

Usage examples:
  One-time token bootstrap:
    python send_gmail_api.py --credentials gmail_client_secret.json --token gmail_token.json --allow-interactive-auth --bootstrap-only

  Send email:
    python send_gmail_api.py --credentials gmail_client_secret.json --token gmail_token.json --to you@example.com --subject "Test" --body "Hello"
"""

from __future__ import annotations

import argparse
import base64
import sys
from email.message import EmailMessage
from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _import_google_deps():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except Exception as exc:  # pragma: no cover
        print(
            "ERROR missing_google_deps "
            "install: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib",
            file=sys.stderr,
        )
        print(f"DETAILS {exc}", file=sys.stderr)
        sys.exit(2)
    return Request, Credentials, InstalledAppFlow, build


def _load_credentials(
    credentials_path: Path,
    token_path: Path,
    allow_interactive_auth: bool,
):
    Request, Credentials, InstalledAppFlow, _ = _import_google_deps()
    creds = None

    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        except Exception:
            creds = None

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    if not creds or not creds.valid:
        if not allow_interactive_auth:
            raise RuntimeError(
                "Token missing/invalid and interactive auth disabled. "
                "Run once with --allow-interactive-auth --bootstrap-only."
            )
        if not credentials_path.exists():
            raise RuntimeError(f"Credentials file not found: {credentials_path}")

        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        creds = flow.run_local_server(
            port=0,
            open_browser=True,
            authorization_prompt_message=(
                "Authorize Gmail send access in your browser.\n"
                "If browser does not open, copy this URL into browser: {url}\n"
            ),
            success_message="Authorization complete. You may close this tab.",
        )

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def _send_email(
    creds,
    to_email: str,
    from_email: str | None,
    subject: str,
    body: str,
):
    _, _, _, build = _import_google_deps()
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)

    msg = EmailMessage()
    msg["To"] = to_email
    if from_email:
        msg["From"] = from_email
    msg["Subject"] = subject
    msg.set_content(body)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return str(result.get("id", ""))


def _normalize_recipients(raw_to: str) -> str:
    text = (raw_to or "").replace(";", ",")
    parts = [p.strip() for p in text.split(",")]
    clean = [p for p in parts if p]
    if not clean:
        return ""
    return ", ".join(clean)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Gmail API sender for EQIDV2 dashboard URL")
    p.add_argument("--credentials", required=True, help="Path to OAuth client credentials JSON")
    p.add_argument("--token", required=True, help="Path to OAuth token JSON")
    p.add_argument("--to", help="Recipient email")
    p.add_argument("--from", dest="from_email", help="Optional From email")
    p.add_argument("--subject", default="EQIDV2 Dashboard URL", help="Email subject")
    p.add_argument("--body", default="", help="Email body")
    p.add_argument("--allow-interactive-auth", action="store_true", help="Allow browser OAuth flow if token is missing/expired")
    p.add_argument("--bootstrap-only", action="store_true", help="Only create/refresh token; do not send email")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    credentials_path = Path(args.credentials).expanduser().resolve()
    token_path = Path(args.token).expanduser().resolve()

    try:
        creds = _load_credentials(
            credentials_path=credentials_path,
            token_path=token_path,
            allow_interactive_auth=args.allow_interactive_auth,
        )
    except Exception as exc:
        print(f"ERROR auth_failed {exc}", file=sys.stderr)
        return 3

    if args.bootstrap_only:
        print(f"OK token_ready token={token_path}")
        return 0

    to_value = _normalize_recipients(args.to or "")
    if not to_value:
        print("ERROR missing_to --to is required unless --bootstrap-only is used", file=sys.stderr)
        return 4

    try:
        message_id = _send_email(
            creds=creds,
            to_email=to_value,
            from_email=args.from_email,
            subject=args.subject,
            body=args.body,
        )
    except Exception as exc:
        print(f"ERROR send_failed {exc}", file=sys.stderr)
        return 5

    print(f"OK sent message_id={message_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
