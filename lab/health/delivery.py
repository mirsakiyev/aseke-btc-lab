from dataclasses import dataclass
import importlib.util
import logging
from pathlib import Path

import httpx


@dataclass(frozen=True)
class Delivery:
    state: str
    detail: str
    retry_after: int = 0
    message_id: int | None = None


def credentials():
    helper = Path.home() / ".codex" / "telegram-notify" / "telegram_notify.py"
    if not helper.is_file():
        raise ValueError("Existing Codex Telegram credential helper is unavailable to this Windows account")
    try:
        spec = importlib.util.spec_from_file_location("aseke_existing_telegram_credentials", helper)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.load_credentials()
    except Exception:
        raise ValueError("Existing user-scoped DPAPI credentials could not be read by this Windows account") from None


def send(message, *, transport=None):
    try:
        token, chat = credentials()
    except ValueError as exc:
        return Delivery("RETRY", str(exc), 900)
    try:
        # The Bot API authenticates in the URL. Suppress request logging in
        # this standalone process even if a caller enabled debug logging.
        logging.getLogger("httpx").disabled = True
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        with httpx.Client(timeout=httpx.Timeout(5, connect=3), trust_env=False, follow_redirects=False, transport=transport) as client:
            response = client.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id":chat,
                                   "text":message.replace(token,"[redacted]")[:4000], "link_preview_options":{"is_disabled":True}})
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        return Delivery("RETRY", "Telegram connection failed before delivery")
    except httpx.HTTPError:
        # Telegram sendMessage has no idempotency key. A connection interrupted
        # after sending may have posted the message. Retain it for review.
        return Delivery("UNCERTAIN", "Telegram response unavailable after transmission may have begun")
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if response.status_code == 429:
        parameters = payload.get("parameters") if isinstance(payload, dict) else None
        retry = parameters.get("retry_after") if isinstance(parameters, dict) else None
        header = response.headers.get("Retry-After", "")
        delay = max(60, retry if type(retry) is int else 0, int(header) if header.isdecimal() and len(header)<9 else 0)
        return Delivery("RETRY", "Telegram rate limit", delay)
    if isinstance(payload, dict) and payload.get("ok") is True and response.status_code == 200:
        result = payload.get("result")
        if isinstance(result, dict) and type(result.get("message_id")) is int:
            return Delivery("DELIVERED", "Telegram accepted the message", message_id=result["message_id"])
    if isinstance(payload, dict) and payload.get("ok") is False or 400 <= response.status_code < 500:
        return Delivery("RETRY", f"Telegram rejected request (HTTP {response.status_code})",
                        900 if response.status_code in (401, 403, 404) else 0)
    return Delivery("UNCERTAIN", f"Telegram acceptance unverified (HTTP {response.status_code})")
