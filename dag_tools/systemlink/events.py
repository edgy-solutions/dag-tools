"""The ingest event taxonomy as typed JSON log events (spec 5.3). Pure.

Every event is one JSON line on the ``dag_tools.systemlink`` logger and a count on
the run's :class:`EventLog`. Events carry identifiers and counts, never a request
body, a header or a credential.
"""
import json
import logging
import threading
from collections import Counter
from typing import Any, Dict

logger = logging.getLogger("dag_tools.systemlink")

AUTH_FAILED = "auth_failed"
NOT_VISIBLE = "not_visible"
GATEWAY_TIMEOUT_SPLIT = "gateway_timeout_split"
THROTTLED = "throttled"
SCHEMA_DRIFT = "schema_drift"
PARSE_FAILED = "parse_failed"

TAXONOMY = (AUTH_FAILED, NOT_VISIBLE, GATEWAY_TIMEOUT_SPLIT, THROTTLED, SCHEMA_DRIFT, PARSE_FAILED)

#: Classification of a fetched result against what is already stored.
NEW_TERMINAL = "new_terminal"
CHANGED_TERMINAL = "changed_terminal"
IN_PROGRESS = "in_progress"
KNOWN_UNCHANGED = "known_unchanged"


class EventLog:
    """Thread-safe counter that also writes each event as one JSON line."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counts: Counter = Counter()
        self.max_step_batch_sent = 0
        self.requests = 0

    def emit(self, event: str, **fields: Any) -> None:
        with self._lock:
            if event in TAXONOMY:
                self.counts[event] += 1
        logger.warning(json.dumps({"event": event, **fields}, sort_keys=True, default=str))

    def info(self, event: str, **fields: Any) -> None:
        logger.info(json.dumps({"event": event, **fields}, sort_keys=True, default=str))

    def note_request(self) -> None:
        with self._lock:
            self.requests += 1

    def note_step_batch(self, size: int) -> None:
        with self._lock:
            self.max_step_batch_sent = max(self.max_step_batch_sent, size)

    def summary(self) -> Dict[str, int]:
        with self._lock:
            return {e: int(self.counts.get(e, 0)) for e in TAXONOMY}
