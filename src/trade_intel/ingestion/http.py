"""Sesión HTTP con reintentos exponenciales y rate limiting simple (token bucket por minuto)."""

from __future__ import annotations

import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session(total_retries: int = 5, backoff: float = 2.0) -> requests.Session:
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"User-Agent": "trade-intel-platform/0.1"})
    return session


class RateLimiter:
    def __init__(self, per_minute: int):
        self.interval = 60.0 / max(per_minute, 1)
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            sleep_for = self._last + self.interval - now
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last = time.monotonic()
