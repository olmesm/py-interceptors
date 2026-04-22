import asyncio
import threading
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass

from py_interceptors import (
    AsyncPolicy,
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
)


@dataclass
class PortalEvent:
    label: str
    thread: str
    loop_id: int


@dataclass
class PortalTrace:
    threads: list[str]
    loop_ids: list[int]


@dataclass
class PortalNumbers:
    items: list[int]
    events: list[PortalEvent]


@dataclass
class PortalNumber:
    value: int
    events: list[PortalEvent]


@dataclass
class PortalSquared:
    value: int
    squared: int
    events: list[PortalEvent]


@dataclass
class PortalTotal:
    total: int
    events: list[PortalEvent]


def _event(label: str) -> PortalEvent:
    return PortalEvent(
        label=label,
        thread=threading.current_thread().name,
        loop_id=id(asyncio.get_running_loop()),
    )


class RecordPortal(Interceptor[PortalTrace, PortalTrace]):
    input_type = PortalTrace
    output_type = PortalTrace

    async def enter(self, ctx: PortalTrace) -> PortalTrace:
        await asyncio.sleep(0)
        ctx.threads.append(threading.current_thread().name)
        ctx.loop_ids.append(id(asyncio.get_running_loop()))
        return ctx


class AsyncSplitNumbers(
    StreamInterceptor[PortalNumbers, PortalNumber, PortalSquared, PortalTotal]
):
    input_type = PortalNumbers
    emit_type = PortalNumber
    collect_type = PortalSquared
    output_type = PortalTotal

    async def stream(self, ctx: PortalNumbers) -> AsyncIterator[PortalNumber]:
        ctx.events.append(_event("stream"))
        for item in ctx.items:
            await asyncio.sleep(0)
            yield PortalNumber(value=item, events=ctx.events)

    async def collect(
        self,
        ctx: PortalNumbers,
        items: Iterable[PortalSquared],
    ) -> PortalTotal:
        ctx.events.append(_event("collect"))
        return PortalTotal(
            total=sum(item.squared for item in items),
            events=ctx.events,
        )


class AsyncSquare(Interceptor[PortalNumber, PortalSquared]):
    input_type = PortalNumber
    output_type = PortalSquared

    async def enter(self, ctx: PortalNumber) -> PortalSquared:
        await asyncio.sleep(0)
        ctx.events.append(_event(f"square:{ctx.value}"))
        return PortalSquared(
            value=ctx.value,
            squared=ctx.value * ctx.value,
            events=ctx.events,
        )


def test_isolated_async_policy_runs_on_named_runtime_portal() -> None:
    workflow: Chain[PortalTrace, PortalTrace] = (
        Chain[PortalTrace, PortalTrace]("portal-record")
        .use(RecordPortal)
        .on(AsyncPolicy("isolated-io", isolated=True))
    )
    runtime = Runtime()

    async def run_twice() -> tuple[str, int, PortalTrace]:
        caller_thread = threading.current_thread().name
        caller_loop_id = id(asyncio.get_running_loop())
        first = await runtime.run_async(workflow, PortalTrace(threads=[], loop_ids=[]))
        second = await runtime.run_async(workflow, first)
        return caller_thread, caller_loop_id, second

    try:
        caller_thread, caller_loop_id, result = asyncio.run(run_twice())
    finally:
        runtime.shutdown()

    assert result.threads == ["isolated-io", "isolated-io"]
    assert all(thread != caller_thread for thread in result.threads)
    assert len(set(result.loop_ids)) == 1
    assert result.loop_ids[0] != caller_loop_id


def test_isolated_async_policy_materializes_streams_on_portal() -> None:
    per_item: Chain[PortalNumber, PortalSquared] = (
        Chain[PortalNumber, PortalNumber]("square").use(AsyncSquare)
    )
    stream_stage: StreamChain[
        PortalNumbers,
        PortalNumber,
        PortalSquared,
        PortalTotal,
    ] = (
        StreamChain[PortalNumbers, PortalNumber, PortalSquared, PortalTotal](
            "split-square"
        )
        .stream(AsyncSplitNumbers)
        .map(per_item)
    )
    workflow: Chain[PortalNumbers, PortalTotal] = (
        Chain[PortalNumbers, PortalNumbers]("portal-stream")
        .use(stream_stage)
        .on(AsyncPolicy("stream-portal", isolated=True))
    )
    runtime = Runtime()

    async def run_stream() -> tuple[int, PortalTotal]:
        caller_loop_id = id(asyncio.get_running_loop())
        result = await runtime.run_async(
            workflow,
            PortalNumbers(items=[1, 2, 3], events=[]),
        )
        return caller_loop_id, result

    try:
        caller_loop_id, result = asyncio.run(run_stream())
    finally:
        runtime.shutdown()

    assert result.total == 14
    assert {event.label for event in result.events} == {
        "stream",
        "square:1",
        "square:2",
        "square:3",
        "collect",
    }
    assert {event.thread for event in result.events} == {"stream-portal"}
    assert len({event.loop_id for event in result.events}) == 1
    assert result.events[0].loop_id != caller_loop_id
