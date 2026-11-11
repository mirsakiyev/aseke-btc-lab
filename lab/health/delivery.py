from dataclasses import dataclass, replace
import importlib.util
import logging
from pathlib import Path
from time import time as wall_time

import httpx
from lab.health.state import local


@dataclass(frozen=True)
class Delivery:
    state: str
    detail: str
    retry_after: int = 0
    message_id: int | None = None
    request_started: float | None = None
    request_completed: float | None = None
    api_accepted_at: float | None = None
    telegram_date: int | None = None


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
    started = None
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
            started = wall_time()
            message = f"{message}\nTelegram request started {local(started)}"
            response = client.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id":chat,
                                   "text":message.replace(token,"[redacted]")[:4000], "link_preview_options":{"is_disabled":True}})
            completed = wall_time()
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        return Delivery("RETRY", "Telegram connection failed before delivery",
                        request_started=started, request_completed=wall_time() if started is not None else None)
    except httpx.HTTPError:
        # Telegram sendMessage has no idempotency key. A connection interrupted
        # after sending may have posted the message. Retain it for review.
        return Delivery("UNCERTAIN", "Telegram response unavailable after transmission may have begun",
                        request_started=started, request_completed=wall_time() if started is not None else None)
    result = classify_response(response)
    return replace(result, request_started=started, request_completed=completed,
                   api_accepted_at=completed if result.state == "DELIVERED" else None)


def classify_response(response):
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
            sent_at = result.get("date")
            return Delivery("DELIVERED", "Telegram accepted the message", message_id=result["message_id"],
                            telegram_date=sent_at if type(sent_at) is int and sent_at > 0 else None)
    if isinstance(payload, dict) and payload.get("ok") is False or 400 <= response.status_code < 500:
        return Delivery("RETRY", f"Telegram rejected request (HTTP {response.status_code})",
                        900 if response.status_code in (401, 403, 404) else 0)
    return Delivery("UNCERTAIN", f"Telegram acceptance unverified (HTTP {response.status_code})")
