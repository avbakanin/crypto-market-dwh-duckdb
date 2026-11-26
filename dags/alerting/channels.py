"""Sending alerts with the standard library only: no package in Airflow's environment.

The Telegram provider is not in the image, and its client goes through httpx, which logs
every request URL at INFO; the bot token is part of that URL (/bot<token>/sendMessage).
Here the token is used only to build the request, and no error carries the URL.

Test the channel from the scheduler or the DAG processor (both have the variables):

    docker compose exec -w /opt/project/dags airflow-scheduler python -m alerting.channels
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

TELEGRAM_API = "https://api.telegram.org"
# The DuckDB port shares the bot with the Snowflake original, and both DAGs are called
# crypto_pipeline: every message says which pipeline sent it.
MESSAGE_PREFIX = "[duckdb] "
# Telegram's limit is 4096 characters; leave room for how it counts some emoji.
MAX_TEXT = 4000
TIMEOUT_S = 10.0
PING_ATTEMPTS = 3
PING_PAUSE_S = 5.0

Opener = Callable[..., Any]


class AlertDeliveryError(RuntimeError):
    """An alert did not reach its channel. The message never contains the secret."""


def _required(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "")
    if not value:
        raise AlertDeliveryError(f"{name} is not set")
    return value


def truncate(text: str, limit: int = MAX_TEXT) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def send_telegram(
    text: str,
    env: Mapping[str, str] = os.environ,
    urlopen: Opener = urllib.request.urlopen,
) -> None:
    """Send plain text after MESSAGE_PREFIX (no parse mode: underscores in test names would
    break Markdown)."""
    token = _required(env, "TELEGRAM_BOT_TOKEN")
    chat_id = _required(env, "TELEGRAM_CHAT_ID")
    api = env.get("TELEGRAM_API_URL") or TELEGRAM_API
    message = truncate(MESSAGE_PREFIX + text)
    body = {"chat_id": chat_id, "text": message, "disable_web_page_preview": True}
    request = urllib.request.Request(
        f"{api}/bot{token}/sendMessage",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=TIMEOUT_S) as response:
            reply = json.load(response)
    except urllib.error.HTTPError as error:
        # The body of a refusal names the reason ("chat not found"), never the token.
        try:
            description = json.load(error).get("description", "")
        except (ValueError, AttributeError, OSError):
            description = ""
        reason = f"HTTP {error.code} {description}".strip().replace(token, "***")
        raise AlertDeliveryError(f"Telegram refused the message: {reason}") from None
    except (OSError, ValueError) as error:
        # URLError is an OSError; its reason names the host at most, but stay safe.
        detail = f"{type(error).__name__}: {error}".replace(token, "***")
        raise AlertDeliveryError(f"Telegram is unreachable: {detail}") from None
    if not reply.get("ok"):
        raise AlertDeliveryError("Telegram did not accept the message")


def ping(
    url: str,
    urlopen: Opener = urllib.request.urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """GET the watchdog URL, a few attempts apart: a missed ping is a false alarm later."""
    for attempt in range(1, PING_ATTEMPTS + 1):
        try:
            with urlopen(url, timeout=TIMEOUT_S):
                return
        except (OSError, ValueError) as error:
            if attempt == PING_ATTEMPTS:
                raise AlertDeliveryError(
                    f"heartbeat ping failed {PING_ATTEMPTS} times: {type(error).__name__}"
                ) from None
            sleep(PING_PAUSE_S)


def main(argv: list[str] | None = None) -> int:
    words = argv if argv is not None else sys.argv[1:]
    text = " ".join(words) or "Test alert from crypto-market-dwh-duckdb"
    try:
        send_telegram(text)
    except AlertDeliveryError as error:
        print(f"ERROR: {error}")
        return 1
    print("OK: message sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
