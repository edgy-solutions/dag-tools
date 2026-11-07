"""Live regression: the concurrency cap against a real Restate server.

Opt-in (``DAGTOOLS_E2E_RESTATE=1``; needs Docker, and pulls
``restatedev/restate:1.6.2`` and ``python:3.12-slim`` on first run), like
the dbt end-to-end test. The unit tests cannot see this: the failure is
Restate cancelling an action's task, or aborting an attempt, while a request
is in flight, and only a real server does either.

Timeouts are shortened to 3 s / 6 s through the worker env vars, and the
cap is 1, so waits genuinely cross the permit deadline (3 + 6 - 1 - 5 = 3 s)
and the abort. Thirty groups at 0.5 s each queue for ~15 s behind one permit.
"""
import os
import time

import pytest

from dag_tools_tests import restate_e2e_harness as h

pytestmark = [
    pytest.mark.skipif(
        os.environ.get("DAGTOOLS_E2E_RESTATE") != "1",
        reason="set DAGTOOLS_E2E_RESTATE=1 (needs Docker) to run the live Restate test",
    ),
    pytest.mark.skipif(
        os.environ.get("DAGTOOLS_E2E_RESTATE") == "1" and not h.docker_available(),
        reason="docker is not available",
    ),
]

GROUPS = 30
CAP = 1
STILL_RETRYING = {"backing-off", "running", "suspended", "ready", "pending", "scheduled"}


def test_cap_holds_and_nothing_is_sent_twice_across_the_timeouts():
    api = h.FakeApi(lambda in_flight: 0.5)
    try:
        env = {
            "API_CALL_PLAN_INACTIVITY_TIMEOUT_SECONDS": "3",
            "API_CALL_PLAN_ABORT_TIMEOUT_SECONDS": "6",
        }
        with h.Stack(env) as stack:
            plans = [
                h.plan_for(f"g{i}", api.port, max_concurrency=CAP, timeout_seconds=1)
                for i in range(GROUPS)
            ]
            h.dispatch_all(stack, plans)

            deadline = time.time() + 150
            counts = {}
            while time.time() < deadline:
                counts = stack.status_counts()
                if counts.get("completed") == GROUPS:
                    break
                time.sleep(2)

            statuses = {row["status"] for row in stack.invocations()}
    finally:
        api.close()

    # No request key reached the API twice.
    assert api.duplicates() == {}
    # The cap held at the API, not just in the worker.
    assert api.max_in_flight <= CAP
    # Every group is done or still retrying; none was paused or failed.
    assert statuses <= (STILL_RETRYING | {"completed"}), statuses
    assert set(api.keys) <= {f"g{i}" for i in range(GROUPS)}
