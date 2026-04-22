import asyncio
import threading
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import ClassVar, Literal

import pytest

from py_interceptors import (
    AsyncPolicy,
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPolicy,
    ThreadPoolPolicy,
)
from py_interceptors.policies import Policy

type ParentPolicyKind = Literal["none", "thread", "pool", "async", "isolated"]
type ChildPolicyKind = Literal["inherit", "thread", "pool", "async", "isolated"]

PARENT_POLICY_KINDS: tuple[ParentPolicyKind, ...] = (
    "none",
    "thread",
    "pool",
    "async",
    "isolated",
)
CHILD_POLICY_KINDS: tuple[ChildPolicyKind, ...] = (
    "inherit",
    "thread",
    "pool",
    "async",
    "isolated",
)


@dataclass
class CleanupContext:
    events: list[str]


@dataclass
class CleanupItem:
    events: list[str]


class CleanupParent(Interceptor[CleanupContext, CleanupContext]):
    input_type = CleanupContext
    output_type = CleanupContext

    def enter(self, ctx: CleanupContext) -> CleanupContext:
        ctx.events.append(f"parent enter:{threading.current_thread().name}")
        return ctx

    def leave(self, ctx: CleanupContext) -> CleanupContext:
        ctx.events.append(f"parent leave:{threading.current_thread().name}")
        return ctx


class CleanupChild(Interceptor[CleanupContext, CleanupContext]):
    input_type = CleanupContext
    output_type = CleanupContext

    async def enter(self, ctx: CleanupContext) -> CleanupContext:
        await asyncio.sleep(0)
        ctx.events.append(f"child enter:{threading.current_thread().name}")
        return ctx


class FailingCleanupChild(Interceptor[CleanupContext, CleanupContext]):
    input_type = CleanupContext
    output_type = CleanupContext

    async def enter(self, ctx: CleanupContext) -> CleanupContext:
        await asyncio.sleep(0)
        ctx.events.append(f"child fail:{threading.current_thread().name}")
        raise RuntimeError("child failed")


class SplitCleanup(StreamInterceptor[CleanupContext, CleanupItem, CleanupItem, CleanupContext]):
    input_type = CleanupContext
    emit_type = CleanupItem
    collect_type = CleanupItem
    output_type = CleanupContext

    def stream(self, ctx: CleanupContext) -> Iterable[CleanupItem]:
        ctx.events.append(f"stream:{threading.current_thread().name}")
        yield CleanupItem(events=ctx.events)

    def collect(
        self,
        ctx: CleanupContext,
        items: Iterable[CleanupItem],
    ) -> CleanupContext:
        list(items)
        ctx.events.append(f"collect:{threading.current_thread().name}")
        return ctx


class CleanupItemChild(Interceptor[CleanupItem, CleanupItem]):
    input_type = CleanupItem
    output_type = CleanupItem

    async def enter(self, ctx: CleanupItem) -> CleanupItem:
        await asyncio.sleep(0)
        ctx.events.append(f"item enter:{threading.current_thread().name}")
        return ctx


class FailingCleanupItemChild(Interceptor[CleanupItem, CleanupItem]):
    input_type = CleanupItem
    output_type = CleanupItem

    async def enter(self, ctx: CleanupItem) -> CleanupItem:
        await asyncio.sleep(0)
        ctx.events.append(f"item fail:{threading.current_thread().name}")
        raise RuntimeError("child failed")


@dataclass(frozen=True)
class ResourceSnapshot:
    executors: tuple[ThreadPoolExecutor, ...]
    threads: tuple[threading.Thread, ...]


@dataclass
class ParallelContext:
    events: list[str]
    values: list[int]


@dataclass
class ParallelItem:
    value: int
    events: list[str]


class ParallelParent(Interceptor[ParallelContext, ParallelContext]):
    input_type = ParallelContext
    output_type = ParallelContext

    def enter(self, ctx: ParallelContext) -> ParallelContext:
        ctx.events.append(f"parent enter:{threading.current_thread().name}")
        return ctx

    def leave(self, ctx: ParallelContext) -> ParallelContext:
        ctx.events.append(f"parent leave:{threading.current_thread().name}")
        return ctx


class ParallelSplit(
    StreamInterceptor[ParallelContext, ParallelItem, ParallelItem, ParallelContext]
):
    input_type = ParallelContext
    emit_type = ParallelItem
    collect_type = ParallelItem
    output_type = ParallelContext

    def stream(self, ctx: ParallelContext) -> Iterable[ParallelItem]:
        ctx.events.append(f"stream:{threading.current_thread().name}")
        for value in range(4):
            yield ParallelItem(value=value, events=ctx.events)

    def collect(
        self,
        ctx: ParallelContext,
        items: Iterable[ParallelItem],
    ) -> ParallelContext:
        collected = list(items)
        ctx.events.append(f"collect:{threading.current_thread().name}")
        ctx.values = [item.value for item in collected]
        return ctx

    def error(self, ctx: ParallelContext, err: Exception) -> ParallelContext:
        ctx.events.append(f"error:{type(err).__name__}:{threading.current_thread().name}")
        return ctx


class StreamFailsBeforeMap(ParallelSplit):
    input_type = ParallelContext
    emit_type = ParallelItem
    collect_type = ParallelItem
    output_type = ParallelContext

    def stream(self, ctx: ParallelContext) -> Iterable[ParallelItem]:
        ctx.events.append(f"stream-fail:{threading.current_thread().name}")
        raise RuntimeError("stream failed")


class CollectFailsAfterMap(ParallelSplit):
    input_type = ParallelContext
    emit_type = ParallelItem
    collect_type = ParallelItem
    output_type = ParallelContext

    def collect(
        self,
        ctx: ParallelContext,
        items: Iterable[ParallelItem],
    ) -> ParallelContext:
        list(items)
        ctx.events.append(f"collect-fail:{threading.current_thread().name}")
        raise RuntimeError("collect failed")


class ParallelItemChild(Interceptor[ParallelItem, ParallelItem]):
    input_type = ParallelItem
    output_type = ParallelItem

    active: ClassVar[int] = 0
    max_active: ClassVar[int] = 0
    lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def reset(cls) -> None:
        cls.active = 0
        cls.max_active = 0

    def enter(self, ctx: ParallelItem) -> ParallelItem:
        with type(self).lock:
            type(self).active += 1
            type(self).max_active = max(type(self).max_active, type(self).active)

        try:
            time.sleep(0.02)
            ctx.events.append(f"item:{ctx.value}:{threading.current_thread().name}")
            return ctx
        finally:
            with type(self).lock:
                type(self).active -= 1


class FailingParallelItemChild(Interceptor[ParallelItem, ParallelItem]):
    input_type = ParallelItem
    output_type = ParallelItem

    active: ClassVar[int] = 0
    max_active: ClassVar[int] = 0
    lock: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def reset(cls) -> None:
        cls.active = 0
        cls.max_active = 0

    def enter(self, ctx: ParallelItem) -> ParallelItem:
        with type(self).lock:
            type(self).active += 1
            type(self).max_active = max(type(self).max_active, type(self).active)

        try:
            time.sleep(0.02)
            ctx.events.append(f"item:{ctx.value}:{threading.current_thread().name}")
            if ctx.value == 2:
                raise RuntimeError("item failed")
            return ctx
        finally:
            with type(self).lock:
                type(self).active -= 1


def _policy(kind: ParentPolicyKind | ChildPolicyKind, case: str) -> Policy | None:
    if kind in ("none", "inherit"):
        return None
    if kind == "thread":
        return ThreadPolicy(f"{case}-lane")
    if kind == "pool":
        return ThreadPoolPolicy(f"{case}-pool", workers=2)
    if kind == "async":
        return AsyncPolicy()
    return AsyncPolicy(f"{case}-io", isolated=True)


def _apply_policy(
    chain: Chain[CleanupContext, CleanupContext],
    policy: Policy | None,
) -> Chain[CleanupContext, CleanupContext]:
    if policy is None:
        return chain
    return chain.on(policy)


def _chain_workflow(
    case: str,
    *,
    parent_policy_kind: ParentPolicyKind,
    child_policy_kind: ChildPolicyKind,
    failing: bool,
) -> Chain[CleanupContext, CleanupContext]:
    child_step = FailingCleanupChild if failing else CleanupChild
    child: Chain[CleanupContext, CleanupContext] = Chain[CleanupContext, CleanupContext](
        f"{case}-child"
    ).use(child_step)
    child = _apply_policy(child, _policy(child_policy_kind, f"{case}-child"))

    workflow: Chain[CleanupContext, CleanupContext] = (
        Chain[CleanupContext, CleanupContext](f"{case}-parent")
        .use(CleanupParent)
        .use(child)
    )
    return _apply_policy(workflow, _policy(parent_policy_kind, f"{case}-parent"))


def _stream_workflow(
    case: str,
    *,
    parent_policy_kind: ParentPolicyKind,
    child_policy_kind: ChildPolicyKind,
    failing: bool,
) -> Chain[CleanupContext, CleanupContext]:
    child_step = FailingCleanupItemChild if failing else CleanupItemChild
    child: Chain[CleanupItem, CleanupItem] = Chain[CleanupItem, CleanupItem](
        f"{case}-item"
    ).use(child_step)
    child_policy = _policy(child_policy_kind, f"{case}-child")
    if child_policy is not None:
        child = child.on(child_policy)

    stream_stage = (
        StreamChain[CleanupContext, CleanupItem, CleanupItem, CleanupContext](
            f"{case}-stream"
        )
        .stream(SplitCleanup)
        .map(child)
    )

    workflow: Chain[CleanupContext, CleanupContext] = (
        Chain[CleanupContext, CleanupContext](f"{case}-parent")
        .use(CleanupParent)
        .use(stream_stage)
    )
    return _apply_policy(workflow, _policy(parent_policy_kind, f"{case}-parent"))


def _parallel_stream_workflow(
    case: str,
    *,
    opener: type[
        StreamInterceptor[ParallelContext, ParallelItem, ParallelItem, ParallelContext]
    ],
    child_step: type[Interceptor[ParallelItem, ParallelItem]],
) -> Chain[ParallelContext, ParallelContext]:
    child: Chain[ParallelItem, ParallelItem] = (
        Chain[ParallelItem, ParallelItem](f"{case}-items")
        .use(child_step)
        .on(ThreadPoolPolicy(f"{case}-map", workers=2))
    )
    stream_stage = (
        StreamChain[ParallelContext, ParallelItem, ParallelItem, ParallelContext](
            f"{case}-stream"
        )
        .stream(opener)
        .map(child)
        .on(ThreadPolicy(f"{case}-opener"))
    )
    return (
        Chain[ParallelContext, ParallelContext](f"{case}-workflow")
        .use(ParallelParent)
        .use(stream_stage)
        .on(ThreadPolicy(f"{case}-parent"))
    )


async def _run_and_shutdown(
    workflow: Chain[CleanupContext, CleanupContext],
    *,
    failing: bool,
) -> tuple[Runtime, ResourceSnapshot]:
    runtime = Runtime()
    try:
        if failing:
            with pytest.raises(RuntimeError, match="child failed"):
                await runtime.run_async(workflow, CleanupContext(events=[]))
        else:
            result = await runtime.run_async(workflow, CleanupContext(events=[]))
            assert result.events

        snapshot = _snapshot_resources(runtime)
    finally:
        await runtime.shutdown_async()

    return runtime, snapshot


async def _run_parallel_and_shutdown(
    workflow: Chain[ParallelContext, ParallelContext],
) -> tuple[Runtime, ResourceSnapshot, ParallelContext]:
    runtime = Runtime()
    try:
        result = await runtime.run_async(workflow, ParallelContext(events=[], values=[]))
        snapshot = _snapshot_resources(runtime)
    finally:
        await runtime.shutdown_async()

    return runtime, snapshot, result


def _snapshot_resources(runtime: Runtime) -> ResourceSnapshot:
    executors = (
        *runtime._thread_lanes.values(),
        *runtime._thread_pools.values(),
    )
    threads: list[threading.Thread] = []

    for executor in executors:
        threads.extend(_executor_threads(executor))

    threads.extend(portal._thread for portal in runtime._async_portals.values())

    return ResourceSnapshot(executors=executors, threads=tuple(threads))


def _executor_threads(executor: ThreadPoolExecutor) -> list[threading.Thread]:
    raw_threads: object = getattr(executor, "_threads", set())
    if not isinstance(raw_threads, set):
        return []
    return [
        thread
        for thread in raw_threads
        if isinstance(thread, threading.Thread)
    ]


def _assert_runtime_resources_released(
    runtime: Runtime,
    snapshot: ResourceSnapshot,
    case: str,
) -> None:
    assert runtime._thread_lanes == {}
    assert runtime._thread_pools == {}
    assert runtime._async_portals == {}
    assert runtime._compiled_plans == {}

    for executor in snapshot.executors:
        with pytest.raises(RuntimeError):
            executor.submit(_noop)

    for thread in snapshot.threads:
        thread.join(timeout=1)
        assert not thread.is_alive()

    leaked_threads = [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith(case)
    ]
    assert leaked_threads == []


def _noop() -> None:
    return None


@pytest.mark.parametrize("parent_policy_kind", PARENT_POLICY_KINDS)
@pytest.mark.parametrize("child_policy_kind", CHILD_POLICY_KINDS)
@pytest.mark.parametrize("failing", (False, True), ids=("success", "child-throws"))
def test_chain_policy_matrix_releases_runtime_owned_resources(
    parent_policy_kind: ParentPolicyKind,
    child_policy_kind: ChildPolicyKind,
    failing: bool,
) -> None:
    case = (
        "cleanup-chain-"
        f"{parent_policy_kind}-{child_policy_kind}-"
        f"{'fail' if failing else 'success'}"
    )
    workflow = _chain_workflow(
        case,
        parent_policy_kind=parent_policy_kind,
        child_policy_kind=child_policy_kind,
        failing=failing,
    )

    runtime, snapshot = asyncio.run(_run_and_shutdown(workflow, failing=failing))

    _assert_runtime_resources_released(runtime, snapshot, case)


@pytest.mark.parametrize("parent_policy_kind", PARENT_POLICY_KINDS)
@pytest.mark.parametrize("child_policy_kind", CHILD_POLICY_KINDS)
@pytest.mark.parametrize("failing", (False, True), ids=("success", "child-throws"))
def test_stream_policy_matrix_releases_runtime_owned_resources(
    parent_policy_kind: ParentPolicyKind,
    child_policy_kind: ChildPolicyKind,
    failing: bool,
) -> None:
    case = (
        "cleanup-stream-"
        f"{parent_policy_kind}-{child_policy_kind}-"
        f"{'fail' if failing else 'success'}"
    )
    workflow = _stream_workflow(
        case,
        parent_policy_kind=parent_policy_kind,
        child_policy_kind=child_policy_kind,
        failing=failing,
    )

    runtime, snapshot = asyncio.run(_run_and_shutdown(workflow, failing=failing))

    _assert_runtime_resources_released(runtime, snapshot, case)


def test_stream_explicit_policy_parallel_collect_path_releases_resources() -> None:
    case = "focused-stream-success"
    ParallelItemChild.reset()
    workflow = _parallel_stream_workflow(
        case,
        opener=ParallelSplit,
        child_step=ParallelItemChild,
    )

    runtime, snapshot, result = asyncio.run(_run_parallel_and_shutdown(workflow))

    assert ParallelItemChild.max_active == 2
    assert result.values == [0, 1, 2, 3]
    assert f"parent enter:{case}-parent_0" in result.events
    assert f"stream:{case}-opener_0" in result.events
    assert f"collect:{case}-opener_0" in result.events
    assert f"parent leave:{case}-parent_0" in result.events
    assert any(event.startswith(f"item:0:{case}-map_") for event in result.events)
    _assert_runtime_resources_released(runtime, snapshot, case)


def test_stream_explicit_policy_parallel_child_failure_releases_resources() -> None:
    case = "focused-stream-child-failure"
    FailingParallelItemChild.reset()
    workflow = _parallel_stream_workflow(
        case,
        opener=ParallelSplit,
        child_step=FailingParallelItemChild,
    )

    runtime, snapshot, result = asyncio.run(_run_parallel_and_shutdown(workflow))

    assert FailingParallelItemChild.max_active == 2
    assert result.values == []
    assert any(
        event == f"error:RuntimeError:{case}-opener_0"
        for event in result.events
    )
    assert not any(event.startswith("collect:") for event in result.events)
    _assert_runtime_resources_released(runtime, snapshot, case)


def test_stream_explicit_policy_collect_failure_releases_resources() -> None:
    case = "focused-stream-collect-failure"
    ParallelItemChild.reset()
    workflow = _parallel_stream_workflow(
        case,
        opener=CollectFailsAfterMap,
        child_step=ParallelItemChild,
    )

    runtime, snapshot, result = asyncio.run(_run_parallel_and_shutdown(workflow))

    assert ParallelItemChild.max_active == 2
    assert result.values == []
    assert f"collect-fail:{case}-opener_0" in result.events
    assert any(
        event == f"error:RuntimeError:{case}-opener_0"
        for event in result.events
    )
    _assert_runtime_resources_released(runtime, snapshot, case)


def test_stream_explicit_policy_stream_failure_releases_resources() -> None:
    case = "focused-stream-opener-failure"
    ParallelItemChild.reset()
    workflow = _parallel_stream_workflow(
        case,
        opener=StreamFailsBeforeMap,
        child_step=ParallelItemChild,
    )

    runtime, snapshot, result = asyncio.run(_run_parallel_and_shutdown(workflow))

    assert ParallelItemChild.max_active == 0
    assert result.values == []
    assert f"stream-fail:{case}-opener_0" in result.events
    assert any(
        event == f"error:RuntimeError:{case}-opener_0"
        for event in result.events
    )
    assert not any(event.startswith("item:") for event in result.events)
    _assert_runtime_resources_released(runtime, snapshot, case)
