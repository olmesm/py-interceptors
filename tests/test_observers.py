import asyncio
from collections.abc import Iterable
from dataclasses import dataclass

import pytest

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionEvent,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPolicy,
)


@dataclass
class Start:
    value: int


@dataclass
class Added:
    value: int


@dataclass
class Doubled:
    value: int


class AddOne(Interceptor[Start, Added]):
    name = "add-one"
    input_type = Start
    output_type = Added

    def enter(self, ctx: Start) -> Added:
        return Added(value=ctx.value + 1)


class Double(Interceptor[Added, Doubled]):
    input_type = Added
    output_type = Doubled

    def enter(self, ctx: Added) -> Doubled:
        return Doubled(value=ctx.value * 2)


class Failing(Interceptor[Start, Start]):
    input_type = Start
    output_type = Start

    def enter(self, ctx: Start) -> Start:
        raise ValueError("boom")


class AsyncAddOne(Interceptor[Start, Added]):
    name = "async-add"
    input_type = Start
    output_type = Added

    async def enter(self, ctx: Start) -> Added:
        await asyncio.sleep(0)
        return Added(value=ctx.value + 1)


@dataclass
class Numbers:
    values: list[int]


@dataclass
class Number:
    value: int


@dataclass
class Squared:
    value: int


@dataclass
class Total:
    value: int


class SplitNumbers(StreamInterceptor[Numbers, Number, Squared, Total]):
    name = "splitter"
    input_type = Numbers
    emit_type = Number
    collect_type = Squared
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[Number]:
        for value in ctx.values:
            yield Number(value=value)

    def collect(self, ctx: Numbers, items: Iterable[Squared]) -> Total:
        return Total(value=sum(item.value for item in items))


class Square(Interceptor[Number, Squared]):
    name = "square"
    input_type = Number
    output_type = Squared

    def enter(self, ctx: Number) -> Squared:
        return Squared(value=ctx.value * ctx.value)


def test_observer_records_sync_chain_names_paths_and_stages() -> None:
    events: list[ExecutionEvent] = []
    runtime = Runtime()

    assert runtime.add_observer(events.append) is runtime

    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(Double)
    result = runtime.run_sync(workflow, Start(value=2))

    assert result == Doubled(value=6)
    assert [(event.step, event.stage) for event in events] == [
        ("add-one", "enter"),
        ("Double", "enter"),
        ("Double", "leave"),
        ("add-one", "leave"),
    ]
    assert {event.execution_id for event in events} == {1}
    assert {event.chain for event in events} == {"math"}
    assert {event.path for event in events} == {("math",)}
    assert all(event.error is None for event in events)
    assert all(event.elapsed_ms >= 0 for event in events)
    assert not hasattr(events[0], "context")


def test_observer_records_stream_child_path_and_thread_policy() -> None:
    events: list[ExecutionEvent] = []
    runtime = Runtime().add_observer(events.append)
    per_item: Chain[Number, Squared] = (
        Chain[Number, Number]("square-chain")
        .use(Square)
        .on(ThreadPolicy("worker"))
    )
    stream_stage: StreamChain[Numbers, Number, Squared, Total] = (
        StreamChain[Numbers, Number, Squared, Total]("numbers")
        .stream(SplitNumbers)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    try:
        result = runtime.run_sync(workflow, Numbers(values=[1, 2]))
    finally:
        runtime.shutdown()

    assert result == Total(value=5)
    assert ("splitter", "stream", ("workflow", "numbers")) in {
        (event.step, event.stage, event.path) for event in events
    }
    assert ("splitter", "collect", ("workflow", "numbers")) in {
        (event.step, event.stage, event.path) for event in events
    }

    square_enters = [
        event
        for event in events
        if event.step == "square" and event.stage == "enter"
    ]
    assert len(square_enters) == 2
    assert {event.path for event in square_enters} == {
        ("workflow", "numbers", "square-chain")
    }
    assert {event.chain for event in square_enters} == {"square-chain"}
    assert {event.policy for event in square_enters} == {"ThreadPolicy('worker')"}
    assert all(event.thread.startswith("worker") for event in square_enters)


def test_observer_records_isolated_async_policy_thread() -> None:
    events: list[ExecutionEvent] = []
    runtime = Runtime().add_observer(events.append)
    workflow: Chain[Start, Added] = (
        Chain[Start, Start]("async-root")
        .use(AsyncAddOne)
        .on(AsyncPolicy("async-portal", isolated=True))
    )

    try:
        result = asyncio.run(runtime.run_async(workflow, Start(value=1)))
    finally:
        runtime.shutdown()

    assert result == Added(value=2)
    enter_events = [
        event
        for event in events
        if event.step == "async-add" and event.stage == "enter"
    ]
    assert len(enter_events) == 1
    assert enter_events[0].path == ("async-root",)
    assert enter_events[0].policy == "AsyncPolicy('async-portal', isolated=True)"
    assert enter_events[0].thread == "async-portal"


def test_error_event_and_exception_note_include_debug_metadata() -> None:
    events: list[ExecutionEvent] = []
    runtime = Runtime().add_observer(events.append)
    workflow: Chain[Start, Start] = Chain("failure").use(Failing)

    with pytest.raises(ValueError) as exc_info:
        runtime.run_sync(workflow, Start(value=1))

    assert [(event.step, event.stage, type(event.error)) for event in events] == [
        ("Failing", "enter", ValueError)
    ]
    notes = getattr(exc_info.value, "__notes__", [])
    assert any("chain='failure'" in note for note in notes)
    assert any("step='Failing'" in note for note in notes)
    assert any("stage='enter'" in note for note in notes)


def test_observer_exceptions_propagate() -> None:
    def fail_observer(event: ExecutionEvent) -> None:
        raise RuntimeError(f"observer failed: {event.step}")

    runtime = Runtime().add_observer(fail_observer)
    workflow: Chain[Start, Added] = Chain("math").use(AddOne)

    with pytest.raises(RuntimeError, match="observer failed: add-one"):
        runtime.run_sync(workflow, Start(value=1))
