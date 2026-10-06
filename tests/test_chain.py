import asyncio
from dataclasses import dataclass

import pytest
from doubles import Added, AddOne, AsyncAddOne, DoubleAdded, Doubled, Start

from py_interceptors import (
    Chain,
    ExecutionError,
    Interceptor,
    Runtime,
    ValidationError,
)


@dataclass
class Trace:
    events: list[str]


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


def test_chain_validates_and_runs_sync() -> None:
    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)

    with Runtime() as runtime:
        compiled = runtime.compile(workflow, initial=Start)
        assert compiled.input_spec is Start
        assert compiled.output_spec is Doubled
        assert compiled.is_async is False

        assert compiled.run_sync(Start(value=3)) == Doubled(value=3, added=4, doubled=8)


def test_runtime_compile_caches_plan_per_chain_identity_and_initial() -> None:
    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    twin: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    runtime = Runtime()

    first = runtime.compile(workflow, initial=Start)

    assert runtime.compile(workflow, initial=Start) is first
    assert runtime.compile(workflow, initial=object) is not first
    assert twin == workflow
    assert runtime.compile(twin, initial=Start) is not first


def test_plan_rejects_a_payload_of_the_wrong_type() -> None:
    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    plan = Runtime().compile(workflow, initial=Start)

    with pytest.raises(ExecutionError, match="expects Start but received Added"):
        plan.run_sync(Added(value=1, added=2))  # type: ignore[arg-type]


def test_chain_validation_fails_on_wrong_order() -> None:
    workflow = Chain[Added, Doubled]("broken").use(DoubleAdded).use(AddOne)  # type: ignore[arg-type]

    with pytest.raises(ValidationError, match="expects Start but received Doubled"):
        Runtime().compile(workflow, initial=Added)


def test_async_step_compiles_async_and_runs_only_via_run_async() -> None:
    workflow: Chain[Start, Added] = Chain("async math").use(AsyncAddOne)
    runtime = Runtime()

    compiled = runtime.compile(workflow, initial=Start)

    assert compiled.is_async is True
    with pytest.raises(ExecutionError, match="use run_async"):
        compiled.run_sync(Start(value=3))

    result = asyncio.run(runtime.run_async(workflow, Start(value=3)))

    assert result == Added(value=3, added=4)
    assert runtime.compile(workflow, initial=Start) is compiled


def test_interceptor_leave_unwinds_after_all_enters() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(OuterTrace).use(InnerTrace)

    result = Runtime().run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "inner enter",
        "inner leave",
        "outer leave",
    ]


def test_interceptor_error_unwinds_entered_stack() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(OuterTrace).use(FailingTrace)

    result = Runtime().run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "failing enter",
        "outer error:ValueError",
    ]


def test_handled_error_resumes_remaining_leave_stack() -> None:
    workflow: Chain[Trace, Trace] = (
        Chain("trace").use(OuterTrace).use(InnerErrorHandler).use(FailingTrace)
    )

    result = Runtime().run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "handler enter",
        "failing enter",
        "handler error:ValueError",
        "outer leave",
    ]


def test_unhandled_error_continues_error_unwind_and_raises() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(ErrorReraiser).use(FailingTrace)
    ctx = Trace(events=[])

    with pytest.raises(ValueError, match="boom"):
        Runtime().run_sync(workflow, ctx)

    assert ctx.events == [
        "reraiser enter",
        "failing enter",
        "reraiser error:ValueError",
    ]


def test_leave_error_enters_error_unwind() -> None:
    workflow: Chain[Trace, Trace] = Chain("trace").use(OuterTrace).use(LeaveFailer)

    result = Runtime().run_sync(workflow, Trace(events=[]))

    assert result.events == [
        "outer enter",
        "leave-failer enter",
        "leave-failer leave",
        "outer error:RuntimeError",
    ]
