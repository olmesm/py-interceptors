"""Where each chain of a parent/child pair runs for every policy combination.

``"caller"`` is the thread that entered the runtime; every other label is the
thread-name prefix of the lane, pool or portal that the policy names.
"""

import asyncio
import threading
from collections.abc import Callable

import pytest
from doubles import AsyncIdentity, Identity, Work, apply_policy

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionEvent,
    Interceptor,
    Policy,
    Runtime,
    ThreadPolicy,
    ThreadPoolPolicy,
)

LANE_A = ThreadPolicy("lane-a")
LANE_B = ThreadPolicy("lane-b")
POOL = ThreadPoolPolicy("pool", workers=2)
PORTAL = AsyncPolicy("io", isolated=True)

MATRIX = [
    pytest.param(None, None, Identity, "caller", "caller", id="none-inherit-sync"),
    pytest.param(
        None, None, AsyncIdentity, "caller", "caller", id="none-inherit-async"
    ),
    pytest.param(LANE_A, None, Identity, "lane-a", "lane-a", id="lane-inherit-sync"),
    pytest.param(
        LANE_A, None, AsyncIdentity, "lane-a", "lane-a", id="lane-inherit-async"
    ),
    pytest.param(
        LANE_A, AsyncPolicy(), AsyncIdentity, "lane-a", "caller", id="lane-async"
    ),
    pytest.param(LANE_A, PORTAL, AsyncIdentity, "lane-a", "io", id="lane-portal"),
    pytest.param(
        LANE_A, LANE_B, AsyncIdentity, "lane-a", "lane-b", id="lane-other-lane"
    ),
    pytest.param(LANE_A, POOL, Identity, "lane-a", "pool", id="lane-pool"),
    pytest.param(POOL, None, Identity, "pool", "pool", id="pool-inherit-sync"),
    pytest.param(AsyncPolicy(), None, Identity, "caller", "caller", id="async-inherit"),
    pytest.param(AsyncPolicy(), POOL, Identity, "caller", "pool", id="async-pool"),
    pytest.param(PORTAL, None, Identity, "io", "io", id="portal-inherit-sync"),
    pytest.param(PORTAL, PORTAL, AsyncIdentity, "io", "io", id="portal-same-portal"),
    pytest.param(
        PORTAL, AsyncPolicy(), AsyncIdentity, "io", "caller", id="portal-async"
    ),
]


def _runs_on(thread: str, where: str, caller: str) -> bool:
    return thread == caller if where == "caller" else thread.startswith(where)


@pytest.mark.parametrize(
    ("parent_policy", "child_policy", "step", "parent_where", "child_where"),
    MATRIX,
)
def test_policy_matrix_places_parent_and_child(
    parent_policy: Policy | None,
    child_policy: Policy | None,
    step: type[Interceptor[Work, Work]],
    parent_where: str,
    child_where: str,
) -> None:
    caller = threading.current_thread().name
    events: list[ExecutionEvent] = []
    child = apply_policy(Chain[Work, Work]("child").use(step), child_policy)
    workflow = apply_policy(
        Chain[Work, Work]("parent").use(Identity).use(child), parent_policy
    )
    expected = [
        ("parent", "enter", parent_where),
        ("child", "enter", child_where),
        ("child", "leave", child_where),
        ("parent", "leave", parent_where),
    ]

    async def run_async() -> Work:
        return await asyncio.wait_for(runtime.run_async(workflow, Work()), timeout=1)

    with Runtime().add_observer(events.append) as runtime:
        entries: list[Callable[[], object]] = [lambda: asyncio.run(run_async())]
        if not runtime.compile(workflow, initial=Work).is_async:
            entries.append(lambda: runtime.run_sync(workflow, Work()))

        for entry in entries:
            events.clear()
            entry()

            assert [(e.chain, e.stage) for e in events] == [
                (chain, stage) for chain, stage, _ in expected
            ]
            for event, (_, _, where) in zip(events, expected, strict=True):
                assert _runs_on(event.thread, where, caller), (event, where)
