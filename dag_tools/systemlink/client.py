"""Read-only HTTP client for the SystemLink Test Monitor query routes. Pure.

Request discipline (spec 5.2):

* The only operation is ``POST <version_path>/query-{results,steps,products}``. Nothing else can be
  sent, so nothing can write to SystemLink.
* 401/403 raise (:class:`AuthFailed`); a 400 raises :class:`SettingRejected` naming the setting that
  most likely spelled the request wrong (never a silent empty page); 504 / timeout raise
  :class:`GatewayTimeout` when the caller can split the work, and back off like a throttle when it
  cannot; 429 / 503 back off with full jitter and honour ``Retry-After`` (seconds or HTTP date).
* Every HTTP request carries a fresh uuid4 ``x-request-id`` and leaves one JSON log line holding the
  body's sha256 -- never the body, never a header.
* The API key is held only in the httpx client's headers.

``sleep``, ``now`` and ``rng`` are injected so a test never really waits.
"""
import email.utils
import hashlib
import json
import random
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional, Tuple

import httpx

from . import events as ev
from .queries import ROUTES

API_KEY_HEADER = "x-ni-api-key"


class SystemLinkError(Exception):
    """Base class; messages never carry a credential or a request body."""


class AuthFailed(SystemLinkError):
    pass


class GatewayTimeout(SystemLinkError):
    pass


class Throttled(SystemLinkError):
    pass


class SettingRejected(SystemLinkError):
    pass


class RequestFailed(SystemLinkError):
    pass


def parse_retry_after(value: Optional[str], now: Callable[[], float]) -> Optional[float]:
    """Seconds to wait from a ``Retry-After`` header: delta-seconds or an HTTP date."""
    if value is None:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    return max(0.0, when.timestamp() - now())


class SystemLinkClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        api_version_path: str = "/nitestmonitor/v2",
        verify_tls: bool = True,
        timeout_s: float = 30.0,
        max_retries: int = 5,
        max_concurrency: int = 1,
        events: Optional[ev.EventLog] = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], float] = time.time,
        rng: Callable[[], float] = random.random,
        backoff_base_s: float = 1.0,
        backoff_cap_s: float = 30.0,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> None:
        if not api_key:
            raise ValueError("an empty API key cannot authenticate")
        self._vp = api_version_path.rstrip("/")
        self._max_retries = max_retries
        self._events = events or ev.EventLog()
        self._sleep, self._now, self._rng = sleep, now, rng
        self._base, self._cap = backoff_base_s, backoff_cap_s
        self._sem = threading.BoundedSemaphore(max(1, max_concurrency))
        self._http = httpx.Client(
            base_url=base_url,
            headers={API_KEY_HEADER: api_key},
            verify=verify_tls,
            timeout=timeout_s,
            transport=transport,
        )

    def __repr__(self) -> str:  # never shows the header
        return "SystemLinkClient()"

    @property
    def events(self) -> ev.EventLog:
        return self._events

    def close(self) -> None:
        self._http.close()

    def _delay(self, attempt: int, retry_after: Optional[float]) -> float:
        jitter = self._rng() * min(self._cap, self._base * (2 ** attempt))  # full jitter
        return max(retry_after or 0.0, jitter)

    def post(
        self,
        route: str,
        body: Dict[str, Any],
        *,
        setting: str,
        splittable: bool = False,
    ) -> Tuple[Dict[str, Any], str]:
        """POST one query. Returns ``(json, request_id)`` of the successful attempt."""
        if route not in ROUTES:
            raise ValueError("route is not on the read-only allowlist")
        path = f"{self._vp}/{route}"
        digest = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        for attempt in range(self._max_retries + 1):
            request_id = str(uuid.uuid4())
            self._events.note_request()
            self._events.info(
                "request", request_id=request_id, route=route, attempt=attempt, body_sha256=digest
            )
            retry_after: Optional[float] = None
            timed_out = False
            with self._sem:
                try:
                    resp = self._http.post(path, json=body, headers={"x-request-id": request_id})
                except httpx.TimeoutException:
                    resp, timed_out = None, True
            if resp is not None:
                status = resp.status_code
                if status == 200:
                    try:
                        data = resp.json()
                    except ValueError as exc:
                        raise RequestFailed(f"{route}: 200 with a non-JSON body") from exc
                    if not isinstance(data, dict):
                        raise RequestFailed(f"{route}: 200 with a non-object body")
                    return data, request_id
                if status in (401, 403):
                    self._events.emit(ev.AUTH_FAILED, route=route, status=status, request_id=request_id)
                    raise AuthFailed(f"{route}: HTTP {status}; the API key was refused")
                if status == 400:
                    raise SettingRejected(
                        f"{route}: HTTP 400; the server rejected the request. "
                        f"Check the setting {setting!r} against this server's spelling."
                    )
                if status == 504:
                    timed_out = True
                elif status in (429, 503):
                    retry_after = parse_retry_after(resp.headers.get("retry-after"), self._now)
                    self._events.emit(
                        ev.THROTTLED, route=route, status=status, attempt=attempt, request_id=request_id
                    )
                else:
                    raise RequestFailed(f"{route}: HTTP {status}")
            if timed_out:
                if splittable:
                    raise GatewayTimeout(f"{route}: gateway timeout")
                self._events.emit(ev.THROTTLED, route=route, status=504, attempt=attempt)
            if attempt < self._max_retries:
                self._sleep(self._delay(attempt, retry_after))
        raise Throttled(f"{route}: still throttled after {self._max_retries} retries")
