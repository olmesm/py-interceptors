import asyncio
import threading
from dataclasses import dataclass
from typing import Any

import pytest

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionError,
    Interceptor,
    Runtime,
    ThreadPolicy,
    ValidationError,
)


@dataclass
class Start:
    value: int


@dataclass
class Added:
    value: int
    added: int


@dataclass
class Doubled:
    value: int
    added: int
    doubled: int


@dataclass
class Trace:
    events: list[str]


class AddOne(Interceptor[Start, Added]):
    input_type = Start
    output_type = Added

    def enter(self, ctx: Start) -> Added:
        return Added(value=ctx.value, added=ctx.value + 1)


class DoubleAdded(Interceptor[Added, Doubled]):
    input_type = Added
    output_type = Doubled

    def enter(self, ctx: Added) -> Doubled:
        return Doubled(
            value=ctx.value,
            added=ctx.added,
            doubled=ctx.added * 2,
        )


class AsyncAddOne(Interceptor[Start, Added]):
    input_type = Start
    output_type = Added

    async def enter(self, ctx: Start) -> Added:
        return Added(value=ctx.value, added=ctx.value + 1)


class OuterTrace(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.events.append("outer enter")
        return ctx

    def leave(self, ctx: Trace) -> Trace:
        ctx.events.append("outer leave")
        return ctx

    def error(self, ctx: Trace, err: Exception) -> Trace:
        ctx.events.append(f"outer error:{type(err).__name__}")
        return ctx


class InnerTrace(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.events.append("inner enter")
        return ctx

    def leave(self, ctx: Trace) -> Trace:
        ctx.events.append("inner leave")
        return ctx


class FailingTrace(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.events.append("failing enter")
        raise ValueError("boom")

    def error(self, ctx: Trace, err: Exception) -> Trace:
        ctx.events.append("failing error")
        return ctx


class InnerErrorHandler(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.events.append("handler enter")
        return ctx

    def error(self, ctx: Trace, err: Exception) -> Trace:
        ctx.events.append(f"handler error:{type(err).__name__}")
        return ctx


class ErrorReraiser(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.events.append("reraiser enter")
        return ctx

    def error(self, ctx: Trace, err: Exception) -> Trace:
        ctx.events.append(f"reraiser error:{type(err).__name__}")
        raise err


class LeaveFailer(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.events.append("leave-failer enter")
        return ctx

    def leave(self, ctx: Trace) -> Trace:
        ctx.events.append("leave-failer leave")
        raise RuntimeError("leave failed")


class AsyncThreadTrace(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    async def enter(self, ctx: Trace) -> Trace:
        await asyncio.sleep(0)
        ctx.events.append(threading.current_thread().name)
        return ctx


def test_chain_validates_and_runs_sync() -> None:
    workflow: Chain[Start, Doubled] = (
        Chain("math").use(AddOne).use(DoubleAdded).on(ThreadPolicy("A"))
    )

    runtime = Runtime()

    compiled = runtime.compile(workflow, initial=Start)
    assert compiled.input_spec is Start
    assert compiled.output_spec is Doubled
    assert compiled.is_async is False

    result = compiled.run_sync(Start(value=3))

    assert isinstance(result, Doubled)
    assert result.value == 3
    assert result.added == 4
    assert result.doubled == 8

    runtime.shutdown()


def test_runtime_runs_sync_directly() -> None:
    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    runtime = Runtime()

    result = runtime.run_sync(workflow, Start(value=2))

    assert result == Doubled(value=2, added=3, doubled=6)


def test_runtime_compile_caches_plan_per_workflow_and_initial() -> None:
    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    runtime = Runtime()

    first = runtime.compile(workflow, initial=Start)
    second = runtime.compile(workflow, initial=Start)
    object_plan = runtime.compile(workflow, initial=object)

    assert second is first
    assert object_plan is not first
    assert len(runtime._compiled_plans) == 2


def test_runtime_run_async_reuses_precompiled_plan() -> None:
    workflow: Chain[Start, Added] = Chain("async math").use(AsyncAddOne).on(
        AsyncPolicy()
    )
    runtime = Runtime()

    compiled = runtime.compile(workflow, initial=Start)
    result = asyncio.run(runtime.run_async(workflow, Start(value=3)))

    assert result == Added(value=3, added=4)
    assert len(runtime._compiled_plans) == 1
    assert next(iter(runtime._compiled_plans.values())) is compiled


def test_runtime_shutdown_clears_compiled_plan_cache() -> None:
    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    runtime = Runtime()

    runtime.compile(workflow, initial=Start)
    assert runtime._compiled_plans

    runtime.shutdown()

    assert runtime._compiled_plans == {}


def test_chain_validation_fails_on_wrong_order() -> None:
    workflow: Chain[Any, Any] = Chain("broken").use(DoubleAdded)
    workflow = workflow.use(AddOne).on(ThreadPolicy("A"))

    runtime = Runtime()

    with pytest.raises(ValidationError):
        runtime.compile(workflow, initial=Start)

    runtime.shutdown()


def test_async_step_requires_async_execution() -> None:
    workflow: Chain[Start, Added] = Chain("async math").use(AsyncAddOne).on(
        AsyncPolicy()
    )
    runtime = Runtime()

    compiled = runtime.compile(workflow, initial=Start)

    assert compiled.is_async is True
    with pytest.raises(ExecutionError):
        compiled.run_sync(Start(value=3))


def test_chain_runs_async() -> None:
    workflow: Chain[Start, Added] = Chain("async math").use(AsyncAddOne).on(
        AsyncPolicy()
    )
    runtime = Runtime()

    result = asyncio.run(runtime.run_async(workflow, Start(value=3)))

    assert result == Added(value=3, added=4)


def test_interceptor_leave_unwinds_after_all_enters() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(OuterTrace).use(InnerTrace)
    runtime = Runtime()

    result = runtime.run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "inner enter",
        "inner leave",
        "outer leave",
    ]


def test_interceptor_error_unwinds_entered_stack() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(OuterTrace).use(FailingTrace)
    runtime = Runtime()

    result = runtime.run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "failing enter",
        "outer error:ValueError",
    ]


def test_handled_error_resumes_remaining_leave_stack() -> None:
    workflow: Chain[Trace, Trace] = (
        Chain("trace").use(OuterTrace).use(InnerErrorHandler).use(FailingTrace)
    )
    runtime = Runtime()

    result = runtime.run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "handler enter",
        "failing enter",
        "handler error:ValueError",
        "outer leave",
    ]


def test_unhandled_error_continues_error_unwind_and_raises() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(ErrorReraiser).use(
        FailingTrace
    )
    runtime = Runtime()
    ctx = Trace(events=[])

    with pytest.raises(ValueError):
        runtime.run_sync(workflow, ctx)

    assert ctx.events == [
        "reraiser enter",
        "failing enter",
        "reraiser error:ValueError",
    ]


def test_leave_error_enters_error_unwind() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(OuterTrace).use(LeaveFailer)
    runtime = Runtime()

    result = runtime.run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "leave-failer enter",
        "leave-failer leave",
        "outer error:RuntimeError",
    ]


def test_async_step_can_run_on_thread_policy_via_async_runtime() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(AsyncThreadTrace).on(
        ThreadPolicy("async-lane")
    )
    runtime = Runtime()

    result = asyncio.run(runtime.run_async(workflow, Trace(events=[])))

    assert result.events
    assert result.events[0].startswith("async-lane")


def test_chain_rejects_constructor_items() -> None:
    with pytest.raises(TypeError):
        Chain("math", AddOne)  # type: ignore[misc, arg-type]
