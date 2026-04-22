import asyncio
import threading
from dataclasses import dataclass

import pytest

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionError,
    Interceptor,
    Runtime,
    ThreadPolicy,
    ThreadPoolPolicy,
)
from py_interceptors.policies import Policy


@dataclass
class MatrixContext:
    events: list[tuple[str, str]]


class ParentRecord(Interceptor[MatrixContext, MatrixContext]):
    input_type = MatrixContext
    output_type = MatrixContext

    def enter(self, ctx: MatrixContext) -> MatrixContext:
        ctx.events.append(("parent enter", threading.current_thread().name))
        return ctx

    def leave(self, ctx: MatrixContext) -> MatrixContext:
        ctx.events.append(("parent leave", threading.current_thread().name))
        return ctx


class SyncRecord(Interceptor[MatrixContext, MatrixContext]):
    input_type = MatrixContext
    output_type = MatrixContext

    def enter(self, ctx: MatrixContext) -> MatrixContext:
        ctx.events.append(("child", threading.current_thread().name))
        return ctx


class AsyncRecord(Interceptor[MatrixContext, MatrixContext]):
    input_type = MatrixContext
    output_type = MatrixContext

    async def enter(self, ctx: MatrixContext) -> MatrixContext:
        await asyncio.sleep(0)
        ctx.events.append(("child", threading.current_thread().name))
        return ctx


def _apply_policy(
    workflow: Chain[MatrixContext, MatrixContext],
    policy: Policy | None,
) -> Chain[MatrixContext, MatrixContext]:
    if policy is None:
        return workflow
    return workflow.on(policy)


def _workflow(
    step: type[Interceptor[MatrixContext, MatrixContext]],
    *,
    parent_policy: Policy | None = None,
    child_policy: Policy | None = None,
) -> Chain[MatrixContext, MatrixContext]:
    child: Chain[MatrixContext, MatrixContext] = Chain[MatrixContext, MatrixContext](
        "child"
    ).use(step)
    child = _apply_policy(child, child_policy)

    parent: Chain[MatrixContext, MatrixContext] = (
        Chain[MatrixContext, MatrixContext]("parent").use(ParentRecord).use(child)
    )
    return _apply_policy(parent, parent_policy)


def _run_sync(
    workflow: Chain[MatrixContext, MatrixContext],
) -> MatrixContext:
    runtime = Runtime()
    try:
        return runtime.run_sync(workflow, MatrixContext(events=[]))
    finally:
        runtime.shutdown()


async def _run_async(
    workflow: Chain[MatrixContext, MatrixContext],
) -> MatrixContext:
    runtime = Runtime()
    try:
        return await runtime.run_async(workflow, MatrixContext(events=[]))
    finally:
        await runtime.shutdown_async()


def test_policy_matrix_no_policy_sync_steps_run_on_caller_thread() -> None:
    workflow: Chain[MatrixContext, MatrixContext] = Chain("sync").use(SyncRecord)

    result = _run_sync(workflow)

    assert result.events == [("child", "MainThread")]


def test_policy_matrix_no_policy_async_steps_use_current_async_runtime() -> None:
    workflow: Chain[MatrixContext, MatrixContext] = Chain("async").use(AsyncRecord)

    result = asyncio.run(_run_async(workflow))

    assert result.events == [("child", "MainThread")]
    with pytest.raises(ExecutionError, match="use run_async"):
        Runtime().compile(workflow, initial=MatrixContext).run_sync(
            MatrixContext(events=[])
        )


def test_policy_matrix_thread_parent_inherits_sync_child_on_lane() -> None:
    workflow = _workflow(SyncRecord, parent_policy=ThreadPolicy("A"))

    result = _run_sync(workflow)

    assert result.events == [
        ("parent enter", "A_0"),
        ("child", "A_0"),
        ("parent leave", "A_0"),
    ]


def test_policy_matrix_thread_parent_inherits_async_child_on_lane() -> None:
    workflow = _workflow(AsyncRecord, parent_policy=ThreadPolicy("A"))

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "A_0"),
        ("child", "A_0"),
        ("parent leave", "A_0"),
    ]


def test_policy_matrix_thread_parent_async_child_uses_current_runtime() -> None:
    workflow = _workflow(
        AsyncRecord,
        parent_policy=ThreadPolicy("A"),
        child_policy=AsyncPolicy(),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "A_0"),
        ("child", "MainThread"),
        ("parent leave", "A_0"),
    ]


def test_policy_matrix_thread_parent_isolated_async_child_uses_portal() -> None:
    workflow = _workflow(
        AsyncRecord,
        parent_policy=ThreadPolicy("A"),
        child_policy=AsyncPolicy("io", isolated=True),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "A_0"),
        ("child", "io"),
        ("parent leave", "A_0"),
    ]


def test_policy_matrix_thread_parent_child_thread_policy_overrides_lane() -> None:
    workflow = _workflow(
        AsyncRecord,
        parent_policy=ThreadPolicy("A"),
        child_policy=ThreadPolicy("B"),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "A_0"),
        ("child", "B_0"),
        ("parent leave", "A_0"),
    ]


def test_policy_matrix_thread_parent_child_thread_pool_overrides_lane() -> None:
    workflow = _workflow(
        SyncRecord,
        parent_policy=ThreadPolicy("A"),
        child_policy=ThreadPoolPolicy("pool", workers=2),
    )

    result = _run_sync(workflow)

    assert result.events == [
        ("parent enter", "A_0"),
        ("child", "pool_0"),
        ("parent leave", "A_0"),
    ]


def test_policy_matrix_thread_pool_parent_inherits_sync_child_on_pool() -> None:
    workflow = _workflow(SyncRecord, parent_policy=ThreadPoolPolicy("pool", workers=2))

    result = _run_sync(workflow)

    assert result.events == [
        ("parent enter", "pool_0"),
        ("child", "pool_0"),
        ("parent leave", "pool_0"),
    ]


def test_policy_matrix_thread_pool_parent_inherits_async_child_on_pool() -> None:
    workflow = _workflow(AsyncRecord, parent_policy=ThreadPoolPolicy("pool", workers=2))

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "pool_0"),
        ("child", "pool_0"),
        ("parent leave", "pool_0"),
    ]


def test_policy_matrix_thread_pool_parent_async_child_uses_current_runtime() -> None:
    workflow = _workflow(
        AsyncRecord,
        parent_policy=ThreadPoolPolicy("pool", workers=2),
        child_policy=AsyncPolicy(),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "pool_0"),
        ("child", "MainThread"),
        ("parent leave", "pool_0"),
    ]


def test_policy_matrix_async_parent_inherits_sync_child_on_event_loop() -> None:
    workflow = _workflow(SyncRecord, parent_policy=AsyncPolicy())

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "MainThread"),
        ("child", "MainThread"),
        ("parent leave", "MainThread"),
    ]


def test_policy_matrix_async_parent_inherits_async_child_on_event_loop() -> None:
    workflow = _workflow(AsyncRecord, parent_policy=AsyncPolicy())

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "MainThread"),
        ("child", "MainThread"),
        ("parent leave", "MainThread"),
    ]


def test_policy_matrix_async_parent_thread_pool_child_offloads_sync_work() -> None:
    workflow = _workflow(
        SyncRecord,
        parent_policy=AsyncPolicy(),
        child_policy=ThreadPoolPolicy("pool", workers=2),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "MainThread"),
        ("child", "pool_0"),
        ("parent leave", "MainThread"),
    ]


def test_policy_matrix_isolated_parent_inherits_sync_child_on_portal() -> None:
    workflow = _workflow(SyncRecord, parent_policy=AsyncPolicy("io", isolated=True))

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "io"),
        ("child", "io"),
        ("parent leave", "io"),
    ]


def test_policy_matrix_isolated_parent_inherits_async_child_on_portal() -> None:
    workflow = _workflow(AsyncRecord, parent_policy=AsyncPolicy("io", isolated=True))

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "io"),
        ("child", "io"),
        ("parent leave", "io"),
    ]


def test_policy_matrix_isolated_parent_same_isolated_child_runs_inline() -> None:
    workflow = _workflow(
        AsyncRecord,
        parent_policy=AsyncPolicy("io", isolated=True),
        child_policy=AsyncPolicy("io", isolated=True),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "io"),
        ("child", "io"),
        ("parent leave", "io"),
    ]


def test_policy_matrix_isolated_parent_default_async_child_uses_current_runtime() -> None:
    workflow = _workflow(
        AsyncRecord,
        parent_policy=AsyncPolicy("io", isolated=True),
        child_policy=AsyncPolicy(),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "io"),
        ("child", "MainThread"),
        ("parent leave", "io"),
    ]


def test_policy_matrix_isolated_parent_thread_pool_child_offloads_sync_work() -> None:
    workflow = _workflow(
        SyncRecord,
        parent_policy=AsyncPolicy("io", isolated=True),
        child_policy=ThreadPoolPolicy("pool", workers=2),
    )

    result = asyncio.run(_run_async(workflow))

    assert result.events == [
        ("parent enter", "io"),
        ("child", "pool_0"),
        ("parent leave", "io"),
    ]
