import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor as WorkerExecutor
from dataclasses import dataclass
from typing import ClassVar

import pytest

from py_interceptors import (
    AsyncPolicy,
    Chain,
    Interceptor,
    Portal,
    Runtime,
    ThreadPolicy,
    ThreadPoolPolicy,
    ValidationError,
)


@dataclass
class Work:
    value: int
    thread_names: list[str]
    loop_ids: list[int]


@dataclass
class PolicyTrace:
    events: list[tuple[str, str]]


class Identity(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work


class RecordLifecycle(Interceptor[PolicyTrace, PolicyTrace]):
    input_type = PolicyTrace
    output_type = PolicyTrace

    def enter(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("enter", threading.current_thread().name))
        return ctx

    def leave(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("leave", threading.current_thread().name))
        return ctx

    def error(self, ctx: PolicyTrace, err: Exception) -> PolicyTrace:
        ctx.events.append((f"error:{type(err).__name__}", threading.current_thread().name))
        return ctx


class FailEnter(Interceptor[PolicyTrace, PolicyTrace]):
    input_type = PolicyTrace
    output_type = PolicyTrace

    def enter(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("fail-enter", threading.current_thread().name))
        raise ValueError("boom")


class OuterLaneRecord(Interceptor[PolicyTrace, PolicyTrace]):
    input_type = PolicyTrace
    output_type = PolicyTrace

    def enter(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("outer-enter", threading.current_thread().name))
        return ctx

    def leave(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("outer-leave", threading.current_thread().name))
        return ctx


class InnerLaneRecord(Interceptor[PolicyTrace, PolicyTrace]):
    input_type = PolicyTrace
    output_type = PolicyTrace

    def enter(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("inner-enter", threading.current_thread().name))
        return ctx

    def leave(self, ctx: PolicyTrace) -> PolicyTrace:
        ctx.events.append(("inner-leave", threading.current_thread().name))
        return ctx


class AsyncDefaultRecord(Interceptor[PolicyTrace, PolicyTrace]):
    input_type = PolicyTrace
    output_type = PolicyTrace

    async def enter(self, ctx: PolicyTrace) -> PolicyTrace:
        await asyncio.sleep(0)
        ctx.events.append(("async-enter", threading.current_thread().name))
        return ctx


class RecordAsyncPortal(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work

    async def enter(self, ctx: Work) -> Work:
        ctx.thread_names.append(threading.current_thread().name)
        ctx.loop_ids.append(id(asyncio.get_running_loop()))
        return ctx


class SlowPoolStep(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work

    active: ClassVar[int] = 0
    max_active: ClassVar[int] = 0
    lock: ClassVar[threading.Lock] = threading.Lock()

    def enter(self, ctx: Work) -> Work:
        with self.lock:
            type(self).active += 1
            type(self).max_active = max(type(self).max_active, type(self).active)

        try:
            time.sleep(0.03)
            return ctx
        finally:
            with self.lock:
                type(self).active -= 1


class FailingAsyncPortal(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work

    async def enter(self, ctx: Work) -> Work:
        ctx.thread_names.append(threading.current_thread().name)
        raise ValueError("portal failed")


def test_policies_accept_portal_identity() -> None:
    portal = Portal("shared")
    inner: Chain[Work, Work] = Chain("inner").use(Identity).on(
        ThreadPoolPolicy(portal, workers=2)
    )
    workflow: Chain[Work, Work] = Chain[Work, Work]("outer").use(inner).on(
        ThreadPoolPolicy("shared", workers=2)
    )

    compiled = Runtime().compile(workflow, initial=Work)

    assert compiled.input_spec is Work
    assert compiled.output_spec is Work


def test_runtime_sync_context_manager_shuts_down_thread_resources() -> None:
    workflow: Chain[Work, Work] = Chain("identity").use(Identity).on(
        ThreadPolicy("lane")
    )

    with Runtime() as runtime:
        result = runtime.run_sync(workflow, Work(1, [], []))
        assert result.value == 1
        assert runtime._thread_lanes["lane"] is not None

    assert runtime._thread_lanes == {}


def test_runtime_sync_context_manager_shuts_down_on_exception() -> None:
    workflow: Chain[Work, Work] = Chain("identity").use(Identity).on(
        ThreadPolicy("lane")
    )
    runtime: Runtime | None = None

    with pytest.raises(RuntimeError, match="fail"):
        with Runtime() as active_runtime:
            runtime = active_runtime
            runtime.run_sync(workflow, Work(1, [], []))
            raise RuntimeError("fail")

    assert runtime is not None
    assert runtime._thread_lanes == {}


def test_runtime_async_context_manager_shuts_down_isolated_portals() -> None:
    workflow: Chain[Work, Work] = (
        Chain("isolated").use(RecordAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )

    async def run() -> tuple[Runtime, threading.Thread]:
        async with Runtime() as runtime:
            result = await runtime.run_async(workflow, Work(1, [], []))
            portal_thread = runtime._async_portals["io"]._thread
            assert result.thread_names == ["io"]
            assert portal_thread.is_alive()
            return runtime, portal_thread

    runtime, portal_thread = asyncio.run(run())

    assert runtime._async_portals == {}
    assert not portal_thread.is_alive()


def test_runtime_async_context_manager_shuts_down_on_exception() -> None:
    workflow: Chain[Work, Work] = (
        Chain("isolated").use(RecordAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )
    runtime: Runtime | None = None
    portal_thread: threading.Thread | None = None

    async def run() -> None:
        nonlocal runtime, portal_thread
        async with Runtime() as active_runtime:
            runtime = active_runtime
            await runtime.run_async(workflow, Work(1, [], []))
            portal_thread = runtime._async_portals["io"]._thread
            raise RuntimeError("fail")

    with pytest.raises(RuntimeError, match="fail"):
        asyncio.run(run())

    assert runtime is not None
    assert portal_thread is not None
    assert runtime._async_portals == {}
    assert not portal_thread.is_alive()


def test_runtime_async_lifecycle_methods_shutdown_isolated_portals() -> None:
    workflow: Chain[Work, Work] = (
        Chain("isolated").use(RecordAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )

    async def run() -> tuple[Runtime, threading.Thread]:
        runtime = Runtime()
        await runtime.startup()
        result = await runtime.run_async(workflow, Work(1, [], []))
        portal_thread = runtime._async_portals["io"]._thread
        assert result.thread_names == ["io"]
        assert portal_thread.is_alive()
        await runtime.shutdown_async()
        return runtime, portal_thread

    runtime, portal_thread = asyncio.run(run())

    assert runtime._async_portals == {}
    assert not portal_thread.is_alive()


def test_thread_policy_runs_sync_enter_leave_and_error_on_named_lane() -> None:
    runtime = Runtime()
    happy: Chain[PolicyTrace, PolicyTrace] = Chain("happy").use(
        RecordLifecycle
    ).on(
        ThreadPolicy("lane")
    )
    failing: Chain[PolicyTrace, PolicyTrace] = Chain("failing").use(
        RecordLifecycle
    ).use(
        FailEnter
    ).on(
        ThreadPolicy("lane")
    )

    try:
        happy_result = runtime.run_sync(happy, PolicyTrace(events=[]))
        failing_result = runtime.run_sync(failing, PolicyTrace(events=[]))
    finally:
        runtime.shutdown()

    assert happy_result.events == [("enter", "lane_0"), ("leave", "lane_0")]
    assert failing_result.events == [
        ("enter", "lane_0"),
        ("fail-enter", "lane_0"),
        ("error:ValueError", "lane_0"),
    ]


def test_thread_policy_nested_same_lane_does_not_deadlock_with_timeout() -> None:
    runtime = Runtime()
    inner: Chain[PolicyTrace, PolicyTrace] = Chain("inner").use(
        InnerLaneRecord
    ).on(
        ThreadPolicy("lane")
    )
    workflow: Chain[PolicyTrace, PolicyTrace] = (
        Chain[PolicyTrace, PolicyTrace]("outer")
        .use(OuterLaneRecord)
        .use(inner)
        .on(ThreadPolicy("lane"))
    )

    try:
        with WorkerExecutor(max_workers=1) as executor:
            future = executor.submit(runtime.run_sync, workflow, PolicyTrace(events=[]))
            result = future.result(timeout=1)
    finally:
        runtime.shutdown()

    assert result.events == [
        ("outer-enter", "lane_0"),
        ("inner-enter", "lane_0"),
        ("inner-leave", "lane_0"),
        ("outer-leave", "lane_0"),
    ]


def test_thread_policy_nested_different_lanes_switches_threads() -> None:
    runtime = Runtime()
    inner: Chain[PolicyTrace, PolicyTrace] = Chain("inner").use(
        InnerLaneRecord
    ).on(
        ThreadPolicy("B")
    )
    workflow: Chain[PolicyTrace, PolicyTrace] = (
        Chain[PolicyTrace, PolicyTrace]("outer")
        .use(OuterLaneRecord)
        .use(inner)
        .on(ThreadPolicy("A"))
    )

    try:
        result = runtime.run_sync(workflow, PolicyTrace(events=[]))
    finally:
        runtime.shutdown()

    assert result.events == [
        ("outer-enter", "A_0"),
        ("inner-enter", "B_0"),
        ("inner-leave", "B_0"),
        ("outer-leave", "A_0"),
    ]


def test_thread_policy_resumes_after_default_async_chain() -> None:
    runtime = Runtime()
    async_inner: Chain[PolicyTrace, PolicyTrace] = Chain("async").use(
        AsyncDefaultRecord
    ).on(
        AsyncPolicy()
    )
    workflow: Chain[PolicyTrace, PolicyTrace] = (
        Chain[PolicyTrace, PolicyTrace]("outer")
        .use(OuterLaneRecord)
        .use(async_inner)
        .on(ThreadPolicy("A"))
    )

    async def run() -> PolicyTrace:
        return await asyncio.wait_for(
            runtime.run_async(workflow, PolicyTrace(events=[])),
            timeout=1,
        )

    try:
        result = asyncio.run(run())
    finally:
        runtime.shutdown()

    assert result.events[0] == ("outer-enter", "A_0")
    assert result.events[1][0] == "async-enter"
    assert result.events[1][1] != "A_0"
    assert result.events[2] == ("outer-leave", "A_0")


def test_compile_rejects_thread_pool_worker_conflict() -> None:
    first: Chain[Work, Work] = Chain("first").use(Identity).on(
        ThreadPoolPolicy("shared", workers=2)
    )
    second: Chain[Work, Work] = Chain("second").use(Identity).on(
        ThreadPoolPolicy("shared", workers=3)
    )
    workflow: Chain[Work, Work] = Chain[Work, Work]("workflow").use(first).use(second)

    with pytest.raises(ValidationError, match="conflicting declarations"):
        Runtime().compile(workflow, initial=Work)


def test_compile_rejects_policy_kind_conflict() -> None:
    inner: Chain[Work, Work] = Chain("inner").use(Identity).on(
        ThreadPolicy("shared")
    )
    workflow: Chain[Work, Work] = Chain[Work, Work]("outer").use(inner).on(
        ThreadPoolPolicy("shared", workers=2)
    )

    with pytest.raises(ValidationError, match="conflicting declarations"):
        Runtime().compile(workflow, initial=Work)


def test_compile_rejects_async_policy_isolation_conflict() -> None:
    inner: Chain[Work, Work] = Chain("inner").use(Identity).on(AsyncPolicy("io"))
    workflow: Chain[Work, Work] = Chain[Work, Work]("outer").use(inner).on(
        AsyncPolicy("io", isolated=True)
    )

    with pytest.raises(ValidationError, match="conflicting declarations"):
        Runtime().compile(workflow, initial=Work)


def test_async_policy_isolated_requires_name() -> None:
    with pytest.raises(ValueError, match="requires a name"):
        AsyncPolicy(isolated=True)


def test_runtime_shutdown_stops_and_clears_isolated_async_portal() -> None:
    runtime = Runtime()
    portal = runtime.get_async_portal(AsyncPolicy("io", isolated=True))
    portal_thread = portal._thread

    async def probe() -> str:
        await asyncio.sleep(0)
        return threading.current_thread().name

    assert portal_thread.is_alive()
    assert portal.submit(probe()).result(timeout=1) == "io"

    runtime.shutdown()
    runtime.shutdown()

    assert runtime._async_portals == {}
    assert not portal_thread.is_alive()


def test_isolated_async_policy_runs_on_dedicated_loop_thread() -> None:
    runtime = Runtime()
    workflow: Chain[Work, Work] = (
        Chain("isolated")
        .use(RecordAsyncPortal)
        .use(RecordAsyncPortal)
        .on(AsyncPolicy(Portal("async-io"), isolated=True))
    )
    portal_thread: threading.Thread | None = None

    try:
        result = asyncio.run(runtime.run_async(workflow, Work(1, [], [])))
        portal_thread = runtime._async_portals["async-io"]._thread
    finally:
        runtime.shutdown()

    assert result.thread_names == ["async-io", "async-io"]
    assert len(set(result.loop_ids)) == 1
    assert portal_thread is not None
    assert not portal_thread.is_alive()


def test_named_non_isolated_async_policy_uses_caller_loop_without_portal() -> None:
    runtime = Runtime()
    workflow: Chain[Work, Work] = (
        Chain("named-async").use(RecordAsyncPortal).on(AsyncPolicy("named"))
    )

    async def run() -> tuple[str, int, Work]:
        caller_thread = threading.current_thread().name
        caller_loop_id = id(asyncio.get_running_loop())
        result = await runtime.run_async(workflow, Work(1, [], []))
        return caller_thread, caller_loop_id, result

    try:
        caller_thread, caller_loop_id, result = asyncio.run(run())
    finally:
        runtime.shutdown()

    assert result.thread_names == [caller_thread]
    assert result.loop_ids == [caller_loop_id]
    assert runtime._async_portals == {}


def test_isolated_async_policy_exception_propagates_and_portal_remains_reusable() -> None:
    runtime = Runtime()
    failing: Chain[Work, Work] = (
        Chain("failing").use(FailingAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )
    working: Chain[Work, Work] = (
        Chain("working").use(RecordAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )

    async def run() -> Work:
        with pytest.raises(ValueError, match="portal failed"):
            await asyncio.wait_for(runtime.run_async(failing, Work(1, [], [])), timeout=1)
        return await asyncio.wait_for(runtime.run_async(working, Work(2, [], [])), timeout=1)

    try:
        result = asyncio.run(run())
    finally:
        runtime.shutdown()

    assert result.thread_names == ["io"]


def test_isolated_async_policy_handles_concurrent_submissions_on_one_portal() -> None:
    runtime = Runtime()
    workflow: Chain[Work, Work] = (
        Chain("isolated").use(RecordAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )

    async def run_many() -> list[Work]:
        return list(
            await asyncio.wait_for(
                asyncio.gather(
                    *(runtime.run_async(workflow, Work(i, [], [])) for i in range(4))
                ),
                timeout=1,
            )
        )

    try:
        results = asyncio.run(run_many())
    finally:
        runtime.shutdown()

    assert [result.value for result in results] == [0, 1, 2, 3]
    assert {tuple(result.thread_names) for result in results} == {("io",)}
    assert len({result.loop_ids[0] for result in results}) == 1


def test_nested_same_isolated_async_policy_does_not_deadlock() -> None:
    runtime = Runtime()
    inner: Chain[Work, Work] = Chain("inner").use(RecordAsyncPortal).on(
        AsyncPolicy("io", isolated=True)
    )
    workflow: Chain[Work, Work] = Chain[Work, Work]("outer").use(inner).on(
        AsyncPolicy("io", isolated=True)
    )

    async def run() -> Work:
        return await asyncio.wait_for(runtime.run_async(workflow, Work(1, [], [])), 1)

    try:
        result = asyncio.run(run())
    finally:
        runtime.shutdown()

    assert result.thread_names == ["io"]


def test_isolated_async_policy_shutdown_allows_portal_recreation() -> None:
    runtime = Runtime()
    workflow: Chain[Work, Work] = (
        Chain("isolated").use(RecordAsyncPortal).on(AsyncPolicy("io", isolated=True))
    )

    first = asyncio.run(runtime.run_async(workflow, Work(1, [], [])))
    first_thread = runtime._async_portals["io"]._thread
    runtime.shutdown()
    second = asyncio.run(runtime.run_async(workflow, Work(2, [], [])))
    second_thread = runtime._async_portals["io"]._thread
    runtime.shutdown()

    assert first.thread_names == ["io"]
    assert second.thread_names == ["io"]
    assert not first_thread.is_alive()
    assert second_thread is not first_thread
    assert not second_thread.is_alive()


def test_thread_pool_policy_keeps_single_workflow_steps_sequential() -> None:
    SlowPoolStep.active = 0
    SlowPoolStep.max_active = 0

    runtime = Runtime()
    workflow: Chain[Work, Work] = (
        Chain[Work, Work]("sequential")
        .use(SlowPoolStep)
        .use(SlowPoolStep)
        .on(ThreadPoolPolicy("pool", workers=2))
    )

    try:
        result = runtime.run_sync(workflow, Work(1, [], []))
    finally:
        runtime.shutdown()

    assert result.value == 1
    assert SlowPoolStep.max_active == 1


def test_thread_pool_caps_concurrent_workflow_invocations() -> None:
    SlowPoolStep.active = 0
    SlowPoolStep.max_active = 0

    runtime = Runtime()
    workflow: Chain[Work, Work] = Chain("slow").use(SlowPoolStep).on(
        ThreadPoolPolicy("pool", workers=2)
    )

    async def run_many() -> list[Work]:
        return list(
            await asyncio.gather(
                *(runtime.run_async(workflow, Work(i, [], [])) for i in range(4))
            )
        )

    try:
        results = asyncio.run(run_many())
    finally:
        runtime.shutdown()

    assert [result.value for result in results] == [0, 1, 2, 3]
    assert SlowPoolStep.max_active == 2


def test_nested_same_thread_pool_policy_does_not_deadlock() -> None:
    runtime = Runtime()
    inner: Chain[Work, Work] = Chain("inner").use(Identity).on(
        ThreadPoolPolicy("pool", workers=1)
    )
    workflow: Chain[Work, Work] = Chain[Work, Work]("outer").use(inner).on(
        ThreadPoolPolicy("pool", workers=1)
    )

    try:
        result = runtime.run_sync(workflow, Work(1, [], []))
    finally:
        runtime.shutdown()

    assert result.value == 1
