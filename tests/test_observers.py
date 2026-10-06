import asyncio

import pytest
from doubles import (
    Added,
    AddOne,
    AsyncAddOne,
    DoubleAdded,
    Doubled,
    NumberItem,
    Numbers,
    RaiseEnter,
    Square,
    Start,
    Total,
    Work,
    split_square,
)

from py_interceptors import (
    AsyncPolicy,
    Chain,
    ExecutionEvent,
    Runtime,
    ThreadPolicy,
)


def test_observer_records_sync_chain_names_paths_and_stages() -> None:
    events: list[ExecutionEvent] = []
    runtime = Runtime()

    assert runtime.add_observer(events.append) is runtime

    workflow: Chain[Start, Doubled] = Chain("math").use(AddOne).use(DoubleAdded)
    result = runtime.run_sync(workflow, Start(value=2))

    assert result == Doubled(value=2, added=3, doubled=6)
    assert [(event.step, event.stage) for event in events] == [
        ("add-one", "enter"),
        ("DoubleAdded", "enter"),
        ("DoubleAdded", "leave"),
        ("add-one", "leave"),
    ]
    assert {event.execution_id for event in events} == {1}
    assert {event.chain for event in events} == {"math"}
    assert {event.path for event in events} == {("math",)}
    assert {event.policy for event in events} == {None}
    assert all(event.error is None for event in events)
    assert all(event.elapsed_ms >= 0 for event in events)


def test_observer_records_stream_child_path_and_thread_policy() -> None:
    events: list[ExecutionEvent] = []
    policy = ThreadPolicy("worker")
    per_item = Chain[NumberItem, NumberItem]("square-chain").use(Square).on(policy)

    with Runtime().add_observer(events.append) as runtime:
        result = runtime.run_sync(
            split_square(per_item=per_item), Numbers(items=[1, 2])
        )

    assert result == Total(total=5)
    stages = {(event.step, event.stage, event.path) for event in events}
    assert ("SplitNumbers", "stream", ("workflow", "split")) in stages
    assert ("SplitNumbers", "collect", ("workflow", "split")) in stages

    square_enters = [
        event for event in events if event.step == "Square" and event.stage == "enter"
    ]
    assert len(square_enters) == 2
    assert {event.path for event in square_enters} == {
        ("workflow", "split", "square-chain")
    }
    assert {event.chain for event in square_enters} == {"square-chain"}
    assert {event.policy for event in square_enters} == {"ThreadPolicy('worker')"}
    assert all(event.thread.startswith("worker") for event in square_enters)


def test_observer_records_isolated_async_policy_thread() -> None:
    events: list[ExecutionEvent] = []
    policy = AsyncPolicy("async-portal", isolated=True)
    label = "AsyncPolicy('async-portal', isolated=True)"
    workflow: Chain[Start, Added] = Chain("async-root").use(AsyncAddOne).on(policy)

    with Runtime().add_observer(events.append) as runtime:
        result = asyncio.run(runtime.run_async(workflow, Start(value=1)))

    assert result == Added(value=1, added=2)
    assert [(e.step, e.stage, e.path, e.policy, e.thread) for e in events] == [
        ("AsyncAddOne", stage, ("async-root",), label, "async-portal")
        for stage in ("enter", "leave")
    ]


def test_error_event_and_exception_note_include_debug_metadata() -> None:
    events: list[ExecutionEvent] = []
    runtime = Runtime().add_observer(events.append)
    workflow: Chain[Work, Work] = Chain("failure").use(RaiseEnter)

    with pytest.raises(ValueError, match="boom") as exc_info:
        runtime.run_sync(workflow, Work())

    assert [(event.step, event.stage, type(event.error)) for event in events] == [
        ("RaiseEnter", "enter", ValueError)
    ]
    notes = getattr(exc_info.value, "__notes__", [])
    assert any("chain='failure'" in note for note in notes)
    assert any("step='RaiseEnter'" in note for note in notes)
    assert any("stage='enter'" in note for note in notes)


def test_observer_exceptions_propagate() -> None:
    def fail_observer(event: ExecutionEvent) -> None:
        raise RuntimeError(f"observer failed: {event.step}")

    runtime = Runtime().add_observer(fail_observer)
    workflow: Chain[Start, Added] = Chain("math").use(AddOne)

    with pytest.raises(RuntimeError, match="observer failed: add-one"):
        runtime.run_sync(workflow, Start(value=1))
