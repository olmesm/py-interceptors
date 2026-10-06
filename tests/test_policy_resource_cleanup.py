"""Runtime-owned lanes, pools and portals are released by shutdown.

Every policy name here starts with ``cleanup-`` so a leaked thread can be
found by prefix in ``threading.enumerate()``.
"""

import asyncio
import threading
from collections.abc import Iterable
from contextlib import AbstractContextManager, nullcontext

import pytest
from doubles import Fan, Identity, RaiseEnter, Rendezvous, Work, apply_policy

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionEvent,
    Interceptor,
    Policy,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPolicy,
    ThreadPoolPolicy,
)

LANE = ThreadPolicy("cleanup-lane")
POOL = ThreadPoolPolicy("cleanup-pool", workers=2)
PORTAL = AsyncPolicy("cleanup-io", isolated=True)
OPENER_LANE = ThreadPolicy("cleanup-opener")
MAP_POOL = ThreadPoolPolicy("cleanup-map", workers=2)


class FanStreamFails(Fan):
    def stream(self, ctx: Work) -> Iterable[Work]:
        raise RuntimeError("stream failed")


class FanCollectFails(Fan):
    def collect(self, ctx: Work, items: Iterable[Work]) -> Work:
        raise RuntimeError("collect failed")


def _assert_released(runtime: Runtime) -> None:
    assert runtime._executors == {}
    assert runtime._async_portals == {}
    assert runtime._compiled_plans == {}
    leaked = [t.name for t in threading.enumerate() if t.name.startswith("cleanup-")]
    assert leaked == []


def _fan_out(
    opener: type[StreamInterceptor[Work, Work, Work, Work]],
    child: Chain[Work, Work],
    stream_policy: Policy,
) -> Chain[Work, Work]:
    stage = (
        StreamChain[Work, Work, Work, Work]("cleanup-stream")
        .stream(opener)
        .map(child)
        .on(stream_policy)
    )
    return Chain[Work, Work]("cleanup-outer").use(stage)


@pytest.mark.parametrize(
    ("parent_policy", "child_policy", "step"),
    [
        pytest.param(LANE, None, Identity, id="lane"),
        pytest.param(POOL, None, Identity, id="pool"),
        pytest.param(PORTAL, None, Identity, id="portal"),
        pytest.param(LANE, PORTAL, RaiseEnter, id="portal-child-raises-under-lane"),
        pytest.param(POOL, LANE, RaiseEnter, id="lane-child-raises-under-pool"),
    ],
)
def test_shutdown_releases_resources_after_run(
    parent_policy: Policy,
    child_policy: Policy | None,
    step: type[Interceptor[Work, Work]],
) -> None:
    child = apply_policy(Chain[Work, Work]("cleanup-child").use(step), child_policy)
    workflow = (
        Chain[Work, Work]("cleanup-parent").use(Identity).use(child).on(parent_policy)
    )
    expectation: AbstractContextManager[object] = (
        pytest.raises(ValueError, match="boom") if step is RaiseEnter else nullcontext()
    )

    with Runtime() as runtime, expectation:
        asyncio.run(runtime.run_async(workflow, Work()))

    _assert_released(runtime)


@pytest.mark.parametrize(
    ("stream_policy", "child_policy"),
    [
        pytest.param(OPENER_LANE, MAP_POOL, id="pool-on-map-child"),
        pytest.param(POOL, None, id="pool-on-stream-chain"),
    ],
)
def test_pooled_fan_out_runs_items_in_parallel_and_releases_the_pool(
    stream_policy: ThreadPolicy | ThreadPoolPolicy,
    child_policy: Policy | None,
) -> None:
    events: list[ExecutionEvent] = []
    child = apply_policy(
        Chain[Work, Work]("cleanup-item").use(
            Rendezvous, barrier=threading.Barrier(2, timeout=1)
        ),
        child_policy,
    )
    workflow = _fan_out(Fan, child, stream_policy)

    with Runtime().add_observer(events.append) as runtime:
        result = runtime.run_sync(workflow, Work(4))

    assert result == Work(6)
    item_threads = {e.thread for e in events if e.step == "Rendezvous"}
    assert len(item_threads) == 2
    assert all(thread.startswith("cleanup-") for thread in item_threads)
    assert [(e.step, e.stage) for e in events if e.step == "Fan"] == [
        ("Fan", "stream"),
        ("Fan", "collect"),
    ]
    assert all(
        e.thread.startswith(stream_policy.name) for e in events if e.step == "Fan"
    )
    _assert_released(runtime)


@pytest.mark.parametrize(
    ("opener", "step", "opener_stages", "item_counts"),
    [
        pytest.param(
            Fan,
            RaiseEnter,
            [("stream", False), ("error", True)],
            {1, 2, 3, 4},
            id="map-child-raises",
        ),
        pytest.param(
            FanCollectFails,
            Identity,
            [("stream", False), ("collect", True), ("error", True)],
            {4},
            id="collect-raises",
        ),
        pytest.param(
            FanStreamFails,
            Identity,
            [("stream", True), ("error", True)],
            {0},
            id="stream-raises",
        ),
    ],
)
def test_fan_out_failure_at_each_point_unwinds_and_releases_resources(
    opener: type[StreamInterceptor[Work, Work, Work, Work]],
    step: type[Interceptor[Work, Work]],
    opener_stages: list[tuple[str, bool]],
    item_counts: set[int],
) -> None:
    events: list[ExecutionEvent] = []
    child = Chain[Work, Work]("cleanup-item").use(step).on(MAP_POOL)
    workflow = _fan_out(opener, child, OPENER_LANE)

    with Runtime().add_observer(events.append) as runtime:
        with pytest.raises((ValueError, RuntimeError)):
            runtime.run_sync(workflow, Work(4))

    opener_events = [e for e in events if e.step == opener.__name__]
    assert [(e.stage, e.error is not None) for e in opener_events] == opener_stages
    assert all(e.thread.startswith("cleanup-opener") for e in opener_events)
    item_enters = [e for e in events if e.step == step.__name__ and e.stage == "enter"]
    assert len(item_enters) in item_counts
    _assert_released(runtime)
