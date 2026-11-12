"""Verify the existing private Telegram route and send authorized summaries."""

from dataclasses import dataclass, field, replace
from hashlib import sha256
import logging
import re
from time import time

import httpx

from lab.health import delivery


CONNECTIVITY_TEXT = "ASEKE BTC Lab — Telegram connectivity test.\nNo trade or market data included."
TOKEN = re.compile(r"[0-9]+:[A-Za-z0-9_-]+\Z")


@dataclass(frozen=True, slots=True)
class Route:
    identifier: str
    chat: int = field(repr=False)
    token: str = field(repr=False)


def _client(transport):
    logging.getLogger("httpx").disabled = True
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return httpx.Client(timeout=httpx.Timeout(5, connect=3), trust_env=False, follow_redirects=False, transport=transport)


def _matches(result, chat):
    return isinstance(result, dict) and type(result.get("id")) is int and result["id"] == chat and result.get("type") == "private"


def destination(*, transport=None):
    """Bind existing DPAPI credentials to a provider-verified private chat."""
    token, chat = delivery.credentials()
    if not isinstance(token, str) or not TOKEN.fullmatch(token) or not re.fullmatch(r"[1-9][0-9]*", str(chat)):
        raise PermissionError("The existing Telegram destination is not a numeric private user route")
    chat = int(chat)
    try:
        with _client(transport) as client:
            response = client.post(f"https://api.telegram.org/bot{token}/getChat", json={"chat_id": chat})
            payload = response.json()
    except (httpx.HTTPError, ValueError):
        raise PermissionError("Telegram private destination could not be verified") from None
    if (response.status_code != 200 or not isinstance(payload, dict) or payload.get("ok") is not True
            or not _matches(payload.get("result"), chat)):
        raise PermissionError("Telegram did not confirm the existing route as the exact private chat")
    identity = sha256(f"aseke-ema-private-v1\0{token}\0{chat}".encode()).hexdigest()
    return Route(identity, chat, token)


def send(route, message, *, transport=None):
    """Use the exact verified credentials, with the watchdog's delivery semantics."""
    if not isinstance(route, Route) or type(message) is not str or not 1 <= len(message) <= 1000:
        raise ValueError("A verified route and bounded private message are required")
    started = time()
    try:
        with _client(transport) as client:
            response = client.post(f"https://api.telegram.org/bot{route.token}/sendMessage", json={
                "chat_id": route.chat, "text": message, "protect_content": False,
                "link_preview_options": {"is_disabled": True},
            })
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        return delivery.Delivery("RETRY", "Telegram connection failed before delivery", request_started=started,
                                 request_completed=time())
    except httpx.HTTPError:
        return delivery.Delivery("UNCERTAIN", "Telegram response unavailable after transmission may have begun",
                                 request_started=started, request_completed=time())
    finished = time()
    result = delivery.classify_response(response)
    if result.state == "DELIVERED" and not _matches(response.json()["result"].get("chat"), route.chat):
        result = delivery.Delivery("UNCERTAIN", "Telegram did not confirm the verified private route in its acceptance")
    return replace(result, request_started=started, request_completed=finished,
                   api_accepted_at=finished if result.state == "DELIVERED" else None)
