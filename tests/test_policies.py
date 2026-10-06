import asyncio
import threading
from collections import Counter
from collections.abc import AsyncIterator, Iterable

import pytest
from doubles import (
    AsyncIdentity,
    AsyncRaiseEnter,
    Fan,
    Identity,
    RaiseEnter,
    Rendezvous,
    Work,
)

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionError,
    ExecutionEvent,
    Interceptor,
    Policy,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPolicy,
    ThreadPoolPolicy,
    ValidationError,
)


class AsyncFan(StreamInterceptor[Work, Work, Work, Work]):
    input_type = Work
    emit_type = Work
    collect_type = Work
    output_type = Work

    async def stream(self, ctx: Work) -> AsyncIterator[Work]:
        for value in range(ctx.value):
            await asyncio.sleep(0)
            yield Work(value)

    async def collect(self, ctx: Work, items: Iterable[Work]) -> Work:
        await asyncio.sleep(0)
        return Work(sum(item.value for item in items))


class FailFirstHoldSecond(Interceptor[Work, Work]):
    """Item 0 raises once item 1 is running; item 1 waits to be released."""

    input_type = Work
    output_type = Work

    started: threading.Barrier
    release: threading.Barrier

    def enter(self, ctx: Work) -> Work:
        if ctx.value == 0:
            self.started.wait()
            raise ValueError("boom")
        if ctx.value == 1:
            self.started.wait()
            self.release.wait()
        return ctx


class FailFirstHoldRest(Interceptor[Work, Work]):
    """Item 0 raises after the others have started; the others wait forever."""

    input_type = Work
    output_type = Work

    cancelled: list[int]

    async def enter(self, ctx: Work) -> Work:
        if ctx.value == 0:
            await asyncio.sleep(0)
            raise ValueError("boom")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.append(ctx.value)
            raise
        return ctx


def _portal_chain(name: str) -> Chain[Work, Work]:
    return (
        Chain[Work, Work](name).use(AsyncIdentity).on(AsyncPolicy("io", isolated=True))
    )


def test_sync_context_manager_shuts_down_lanes() -> None:
    workflow: Chain[Work, Work] = Chain("lane").use(Identity).on(ThreadPolicy("lane"))

    with Runtime() as runtime:
        assert runtime.run_sync(workflow, Work(1)) == Work(1)
        assert (ThreadPolicy, "lane") in runtime._executors

    assert runtime._executors == {}


def test_sync_context_manager_shuts_down_on_exception() -> None:
    workflow: Chain[Work, Work] = Chain("lane").use(Identity).on(ThreadPolicy("lane"))

    with pytest.raises(RuntimeError, match="fail"), Runtime() as runtime:
        runtime.run_sync(workflow, Work())
        raise RuntimeError("fail")

    assert runtime._executors == {}


def test_async_context_manager_shuts_down_portals() -> None:
    workflow = _portal_chain("portal")

    async def run() -> tuple[Runtime, threading.Thread]:
        async with Runtime() as runtime:
            await runtime.run_async(workflow, Work())
            portal_thread = runtime._async_portals["io"]._thread
            assert portal_thread.is_alive()
            return runtime, portal_thread

    runtime, portal_thread = asyncio.run(run())

    assert runtime._async_portals == {}
    assert not portal_thread.is_alive()


def test_async_context_manager_shuts_down_on_exception() -> None:
    workflow = _portal_chain("portal")
    threads: list[threading.Thread] = []

    async def run() -> None:
        async with Runtime() as runtime:
            await runtime.run_async(workflow, Work())
            threads.append(runtime._async_portals["io"]._thread)
            raise RuntimeError("fail")

    with pytest.raises(RuntimeError, match="fail"):
        asyncio.run(run())

    assert threads and not threads[0].is_alive()


def test_thread_policy_runs_enter_and_error_on_its_lane() -> None:
    events: list[ExecutionEvent] = []
    workflow = (
        Chain[Work, Work]("lane").use(Identity).use(RaiseEnter).on(ThreadPolicy("lane"))
    )

    with Runtime().add_observer(events.append) as runtime:
        with pytest.raises(ValueError, match="boom"):
            runtime.run_sync(workflow, Work())

    assert [(e.step, e.stage) for e in events] == [
        ("Identity", "enter"),
        ("RaiseEnter", "enter"),
        ("Identity", "error"),
    ]
    assert all(e.thread.startswith("lane") for e in events)


@pytest.mark.parametrize(
    "policy",
    [ThreadPolicy("lane"), ThreadPoolPolicy("pool", workers=1)],
    ids=["lane", "single-worker-pool"],
)
def test_nested_same_thread_policy_runs_inline(
    policy: ThreadPolicy | ThreadPoolPolicy,
) -> None:
    """A deadlock fails on the timeout instead of hanging the test run."""
    events: list[ExecutionEvent] = []
    inner: Chain[Work, Work] = Chain("inner").use(Identity).on(policy)
    workflow = Chain[Work, Work]("outer").use(Identity).use(inner).on(policy)
    runtime = Runtime().add_observer(events.append)

    async def run() -> Work:
        return await asyncio.wait_for(runtime.run_async(workflow, Work()), timeout=1)

    done = False
    try:
        asyncio.run(run())
        done = True
    finally:
        if not done:
            # A deadlocked worker blocks on a queued future forever; cancelling
            # it lets the worker exit so the interpreter can shut down.
            runtime.get_executor(policy).shutdown(wait=False, cancel_futures=True)
        runtime.shutdown(wait=done)

    assert len({e.thread for e in events}) == 1
    assert [(e.chain, e.stage) for e in events] == [
        ("outer", "enter"),
        ("inner", "enter"),
        ("inner", "leave"),
        ("outer", "leave"),
    ]


@pytest.mark.parametrize(
    ("inner_policy", "outer_policy"),
    [
        pytest.param(
            ThreadPoolPolicy("shared", workers=2),
            ThreadPoolPolicy("shared", workers=3),
            id="pool-workers",
        ),
        pytest.param(
            ThreadPolicy("shared"),
            ThreadPoolPolicy("shared", workers=2),
            id="policy-kind",
        ),
        pytest.param(
            AsyncPolicy("shared"),
            AsyncPolicy("shared", isolated=True),
            id="async-isolation",
        ),
    ],
)
def test_compile_rejects_conflicting_declarations_of_one_name(
    inner_policy: Policy,
    outer_policy: Policy,
) -> None:
    inner: Chain[Work, Work] = Chain("inner").use(Identity).on(inner_policy)
    workflow = Chain[Work, Work]("outer").use(inner).on(outer_policy)

    with pytest.raises(ValidationError, match="conflicting declarations"):
        Runtime().compile(workflow, initial=Work)


def test_async_policy_isolated_requires_name() -> None:
    with pytest.raises(ValueError, match="requires a name"):
        AsyncPolicy(isolated=True)


def test_pool_name_reused_with_different_workers_raises() -> None:
    first: Chain[Work, Work] = (
        Chain("first").use(Identity).on(ThreadPoolPolicy("pool", workers=2))
    )
    second: Chain[Work, Work] = (
        Chain("second").use(Identity).on(ThreadPoolPolicy("pool", workers=4))
    )

    with Runtime() as runtime:
        runtime.run_sync(first, Work())
        with pytest.raises(ExecutionError, match="workers=2; got workers=4"):
            runtime.run_sync(second, Work())


def test_isolated_async_policy_runs_on_its_own_loop_thread() -> None:
    events: list[ExecutionEvent] = []
    workflow = (
        Chain[Work, Work]("portal")
        .use(AsyncIdentity)
        .use(AsyncIdentity)
        .on(AsyncPolicy("io", isolated=True))
    )
    runtime = Runtime().add_observer(events.append)

    try:
        asyncio.run(runtime.run_async(workflow, Work()))
        portal_thread = runtime._async_portals["io"]._thread
    finally:
        runtime.shutdown()
        runtime.shutdown()  # idempotent

    assert {e.thread for e in events} == {"io"}
    assert runtime._async_portals == {}
    assert not portal_thread.is_alive()


def test_named_non_isolated_async_policy_uses_caller_loop_without_portal() -> None:
    events: list[ExecutionEvent] = []
    workflow: Chain[Work, Work] = (
        Chain("named").use(AsyncIdentity).on(AsyncPolicy("named"))
    )

    with Runtime().add_observer(events.append) as runtime:
        asyncio.run(runtime.run_async(workflow, Work()))
        assert runtime._async_portals == {}

    assert {e.thread for e in events} == {threading.current_thread().name}


def test_isolated_async_policy_exception_propagates_and_portal_remains_reusable() -> (
    None
):
    events: list[ExecutionEvent] = []
    failing: Chain[Work, Work] = (
        Chain("failing").use(AsyncRaiseEnter).on(AsyncPolicy("io", isolated=True))
    )
    working = _portal_chain("working")

    async def run() -> Work:
        with pytest.raises(ValueError, match="boom"):
            await asyncio.wait_for(runtime.run_async(failing, Work()), timeout=1)
        return await asyncio.wait_for(runtime.run_async(working, Work(2)), timeout=1)

    with Runtime().add_observer(events.append) as runtime:
        assert asyncio.run(run()) == Work(2)

    assert [(e.chain, e.thread) for e in events] == [
        ("failing", "io"),
        ("working", "io"),
        ("working", "io"),
    ]


def test_isolated_async_policy_serves_concurrent_runs_on_one_portal() -> None:
    events: list[ExecutionEvent] = []
    workflow = _portal_chain("portal")

    async def run_many() -> list[Work]:
        runs = (runtime.run_async(workflow, Work(i)) for i in range(4))
        return list(await asyncio.wait_for(asyncio.gather(*runs), timeout=1))

    with Runtime().add_observer(events.append) as runtime:
        results = asyncio.run(run_many())
        assert list(runtime._async_portals) == ["io"]

    assert [r.value for r in results] == [0, 1, 2, 3]
    assert {e.thread for e in events} == {"io"}


def test_isolated_async_policy_shutdown_allows_portal_recreation() -> None:
    workflow = _portal_chain("portal")
    threads: list[threading.Thread] = []

    with Runtime() as runtime:
        for _ in range(2):
            asyncio.run(runtime.run_async(workflow, Work()))
            threads.append(runtime._async_portals["io"]._thread)
            runtime.shutdown()

    first, second = threads
    assert second is not first
    assert not first.is_alive() and not second.is_alive()


def test_isolated_async_policy_materializes_async_streams_on_the_portal() -> None:
    events: list[ExecutionEvent] = []
    per_item: Chain[Work, Work] = Chain("square").use(AsyncIdentity)
    stage = StreamChain[Work, Work, Work, Work]("fan").stream(AsyncFan).map(per_item)
    workflow = (
        Chain[Work, Work]("portal-stream")
        .use(stage)
        .on(AsyncPolicy("stream-portal", isolated=True))
    )

    with Runtime().add_observer(events.append) as runtime:
        result = asyncio.run(runtime.run_async(workflow, Work(3)))

    assert result == Work(3)
    assert Counter((e.step, e.stage) for e in events) == {
        ("AsyncFan", "stream"): 1,
        ("AsyncIdentity", "enter"): 3,
        ("AsyncIdentity", "leave"): 3,
        ("AsyncFan", "collect"): 1,
    }
    assert {e.thread for e in events} == {"stream-portal"}


def test_thread_pool_caps_concurrent_runs_at_workers() -> None:
    events: list[ExecutionEvent] = []
    workflow = (
        Chain[Work, Work]("pooled")
        .use(Rendezvous, barrier=threading.Barrier(2, timeout=1))
        .on(ThreadPoolPolicy("pool", workers=2))
    )

    async def run_many() -> list[Work]:
        runs = (runtime.run_async(workflow, Work(i)) for i in range(4))
        return list(await asyncio.wait_for(asyncio.gather(*runs), timeout=2))

    with Runtime().add_observer(events.append) as runtime:
        results = asyncio.run(run_many())

    assert [r.value for r in results] == [0, 1, 2, 3]
    enter_threads = [e.thread for e in events if e.stage == "enter"]
    assert len(enter_threads) == 4
    assert len(set(enter_threads)) == 2
    assert all(thread.startswith("pool") for thread in enter_threads)


def test_concurrent_run_blocking_on_cold_runtime_shares_one_portal() -> None:
    events: list[ExecutionEvent] = []
    workflow: Chain[Work, Work] = Chain("blocking").use(AsyncIdentity)
    runtime = Runtime().add_observer(events.append)
    start = threading.Barrier(8, timeout=1)
    results: list[Work] = []

    def call(value: int) -> None:
        start.wait()
        results.append(runtime.run_blocking(workflow, Work(value)))

    callers = [threading.Thread(target=call, args=(i,)) for i in range(8)]
    for caller in callers:
        caller.start()
    for caller in callers:
        caller.join(timeout=2)
    portal_threads = [portal._thread for portal in runtime._async_portals.values()]
    runtime.shutdown()

    assert sorted(r.value for r in results) == list(range(8))
    assert len(portal_threads) == 1
    portal_name = portal_threads[0].name
    assert {e.thread for e in events} == {portal_name}
    assert [t for t in threading.enumerate() if t.name == portal_name] == []
    assert runtime._async_portals == {}


def test_sync_fan_out_stops_claiming_items_after_the_first_failure() -> None:
    events: list[ExecutionEvent] = []
    pool = ThreadPoolPolicy("pool", workers=2)
    started = threading.Barrier(3, timeout=1)
    release = threading.Barrier(2, timeout=1)
    child = (
        Chain[Work, Work]("item")
        .use(FailFirstHoldSecond, started=started, release=release)
        .on(pool)
    )
    stage = StreamChain[Work, Work, Work, Work]("fan").stream(Fan).map(child)
    workflow = Chain[Work, Work]("outer").use(stage)
    runtime = Runtime().add_observer(events.append)
    failure_recorded = threading.Event()
    outcome: list[Exception] = []

    def run() -> None:
        try:
            runtime.run_sync(workflow, Work(4))
        except Exception as err:
            outcome.append(err)

    caller = threading.Thread(target=run)
    caller.start()
    started.wait()
    # Both pool workers are busy, so this runs only once item 0's worker has
    # recorded the failure and returned to the pool.
    runtime.get_executor(pool).submit(failure_recorded.set)
    assert failure_recorded.wait(timeout=1)
    release.wait()
    caller.join(timeout=1)
    runtime.shutdown(wait=not caller.is_alive())

    assert not caller.is_alive()
    assert [type(err) for err in outcome] == [ValueError]
    item_events = [(e.stage, e.error is None) for e in events if e.chain == "item"]
    assert sorted(item_events) == [("enter", False), ("enter", True), ("leave", True)]


def test_async_fan_out_cancels_running_siblings_on_the_first_failure() -> None:
    events: list[ExecutionEvent] = []
    cancelled: list[int] = []
    child: Chain[Work, Work] = Chain("item").use(FailFirstHoldRest, cancelled=cancelled)
    stage = StreamChain[Work, Work, Work, Work]("fan").stream(Fan).map(child)
    workflow = Chain[Work, Work]("outer").use(stage)

    async def run() -> None:
        with pytest.raises(ValueError, match="boom"):
            await asyncio.wait_for(runtime.run_async(workflow, Work(4)), timeout=1)
        assert sorted(cancelled) == [1, 2, 3]

    with Runtime().add_observer(events.append) as runtime:
        asyncio.run(run())

    assert [(e.step, e.stage, type(e.error)) for e in events] == [
        ("Fan", "stream", type(None)),
        ("FailFirstHoldRest", "enter", ValueError),
        ("Fan", "error", ValueError),
    ]
