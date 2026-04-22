import asyncio
import threading
import time
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass

import pytest

from py_interceptors import (
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPolicy,
    ThreadPoolPolicy,
)


@dataclass
class Numbers:
    items: list[int]


@dataclass
class NumberItem:
    value: int


@dataclass
class SquaredItem:
    value: int
    squared: int


@dataclass
class Total:
    total: int


@dataclass
class OrderedValues:
    values: list[int]


@dataclass
class TraceNumbers:
    items: list[int]
    events: list[str]


@dataclass
class TraceItem:
    value: int
    events: list[str]


@dataclass
class TraceResult:
    values: list[int]
    events: list[str]


class SplitNumbers(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        for item in ctx.items:
            yield NumberItem(value=item)

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=sum(item.squared for item in items))


class Square(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    def enter(self, ctx: NumberItem) -> SquaredItem:
        return SquaredItem(value=ctx.value, squared=ctx.value * ctx.value)


class SlowSquare(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    active = 0
    max_active = 0
    lock = threading.Lock()

    def enter(self, ctx: NumberItem) -> SquaredItem:
        with self.lock:
            type(self).active += 1
            type(self).max_active = max(type(self).max_active, type(self).active)

        try:
            time.sleep(0.02)
            return SquaredItem(value=ctx.value, squared=ctx.value * ctx.value)
        finally:
            with self.lock:
                type(self).active -= 1


class SlowOutOfOrderSquare(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    def enter(self, ctx: NumberItem) -> SquaredItem:
        time.sleep((5 - ctx.value) * 0.005)
        return SquaredItem(value=ctx.value, squared=ctx.value * ctx.value)


class StreamFails(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        raise ValueError("stream failed")

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)

    def error(self, ctx: Numbers, err: Exception) -> Total:
        return Total(total=-1)


class AsyncStreamFails(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    async def stream(self, ctx: Numbers) -> AsyncIterator[NumberItem]:
        yield NumberItem(value=1)
        await asyncio.sleep(0)
        raise ValueError("async stream failed")

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)

    async def error(self, ctx: Numbers, err: Exception) -> Total:
        await asyncio.sleep(0)
        return Total(total=-4)


class StreamErrorRaises(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        raise ValueError("stream failed")

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)

    def error(self, ctx: Numbers, err: Exception) -> Total:
        raise RuntimeError("replacement")


class FailingSquare(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    def enter(self, ctx: NumberItem) -> SquaredItem:
        raise ValueError("map failed")


class MapFails(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        yield NumberItem(value=1)

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)

    def error(self, ctx: Numbers, err: Exception) -> Total:
        return Total(total=-2)


class CollectFails(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        yield NumberItem(value=1)

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        raise ValueError("collect failed")

    def error(self, ctx: Numbers, err: Exception) -> Total:
        return Total(total=-3)


class PoolMapFails(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        for item in ctx.items:
            yield NumberItem(value=item)

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)

    def error(self, ctx: Numbers, err: Exception) -> Total:
        return Total(total=-5)


class AsyncCollectNumbers(
    StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]
):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        for item in ctx.items:
            yield NumberItem(value=item)

    async def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        await asyncio.sleep(0)
        return Total(total=sum(item.squared for item in items))


class OrderedSplit(
    StreamInterceptor[Numbers, NumberItem, SquaredItem, OrderedValues]
):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = OrderedValues

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        for item in ctx.items:
            yield NumberItem(value=item)

    def collect(
        self,
        ctx: Numbers,
        items: Iterable[SquaredItem],
    ) -> OrderedValues:
        return OrderedValues(values=[item.value for item in items])


class TraceSplit(
    StreamInterceptor[TraceNumbers, TraceItem, TraceItem, TraceResult]
):
    input_type = TraceNumbers
    emit_type = TraceItem
    collect_type = TraceItem
    output_type = TraceResult

    def stream(self, ctx: TraceNumbers) -> Iterable[TraceItem]:
        for item in ctx.items:
            yield TraceItem(value=item, events=ctx.events)

    def collect(
        self,
        ctx: TraceNumbers,
        items: Iterable[TraceItem],
    ) -> TraceResult:
        collected = list(items)
        ctx.events.append("collect")
        return TraceResult(
            values=[item.value for item in collected],
            events=ctx.events,
        )


class TraceLifecycle(Interceptor[TraceItem, TraceItem]):
    input_type = TraceItem
    output_type = TraceItem

    def enter(self, ctx: TraceItem) -> TraceItem:
        ctx.events.append(f"enter:{ctx.value}")
        return ctx

    def leave(self, ctx: TraceItem) -> TraceItem:
        ctx.events.append(f"leave:{ctx.value}")
        return ctx

    def error(self, ctx: TraceItem, err: Exception) -> TraceItem:
        ctx.events.append(f"error:{type(err).__name__}:{ctx.value}")
        return ctx


class TraceHandler(Interceptor[TraceItem, TraceItem]):
    input_type = TraceItem
    output_type = TraceItem

    def enter(self, ctx: TraceItem) -> TraceItem:
        ctx.events.append(f"handler-enter:{ctx.value}")
        return ctx

    def error(self, ctx: TraceItem, err: Exception) -> TraceItem:
        ctx.events.append(f"handler-error:{type(err).__name__}:{ctx.value}")
        return ctx


class TraceFail(Interceptor[TraceItem, TraceItem]):
    input_type = TraceItem
    output_type = TraceItem

    def enter(self, ctx: TraceItem) -> TraceItem:
        ctx.events.append(f"fail-enter:{ctx.value}")
        raise ValueError("item failed")


def test_stream_stage_runs_sync() -> None:
    per_item: Chain[NumberItem, SquaredItem] = (
        Chain("square").use(Square).on(ThreadPolicy("worker"))
    )
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("split-square")
        .stream(SplitNumbers)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = (
        Chain[Numbers, Numbers]("sum of squares")
        .use(stream_stage)
        .on(ThreadPolicy("main"))
    )

    runtime = Runtime()
    compiled = runtime.compile(workflow, initial=Numbers)

    result = compiled.run_sync(Numbers(items=[1, 2, 3]))

    assert result.total == 14

    runtime.shutdown()


def test_stream_empty_items_calls_collect_with_empty_results() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("empty")
        .stream(SplitNumbers)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    result = Runtime().run_sync(workflow, Numbers(items=[]))

    assert result.total == 0


def test_stream_error_handles_stream_failure() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("stream-fails")
        .stream(StreamFails)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    result = Runtime().run_sync(workflow, Numbers(items=[1]))

    assert result.total == -1


def test_stream_error_raises_replacement_error() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("error-raises")
        .stream(StreamErrorRaises)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    with pytest.raises(RuntimeError, match="replacement"):
        Runtime().run_sync(workflow, Numbers(items=[1]))


def test_stream_error_handles_mapped_child_failure() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("fail").use(FailingSquare)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("map-fails")
        .stream(MapFails)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    result = Runtime().run_sync(workflow, Numbers(items=[1]))

    assert result.total == -2


def test_stream_error_handles_thread_pool_mapped_child_failure() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("fail").use(
        FailingSquare
    ).on(
        ThreadPoolPolicy("pool", workers=2)
    )
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("pool-map-fails")
        .stream(PoolMapFails)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )
    runtime = Runtime()

    try:
        result = runtime.run_sync(workflow, Numbers(items=[1, 2]))
    finally:
        runtime.shutdown()

    assert result.total == -5


def test_stream_error_handles_collect_failure() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("collect-fails")
        .stream(CollectFails)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    result = Runtime().run_sync(workflow, Numbers(items=[1]))

    assert result.total == -3


def test_async_stream_iterator_failure_uses_async_error_handler() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("async-stream-fails")
        .stream(AsyncStreamFails)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    result = asyncio.run(Runtime().run_async(workflow, Numbers(items=[1])))

    assert result.total == -4


def test_stream_stage_runs_async_collect() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("async-collect")
        .stream(AsyncCollectNumbers)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("workflow").use(
        stream_stage
    )

    result = asyncio.run(Runtime().run_async(workflow, Numbers(items=[1, 2, 3])))

    assert result.total == 14


def test_stream_child_chain_leave_runs_before_collect() -> None:
    per_item: Chain[TraceItem, TraceItem] = Chain("trace").use(TraceLifecycle)
    stream_stage: StreamChain[TraceNumbers, TraceItem, TraceItem, TraceResult] = (
        StreamChain[TraceNumbers, TraceItem, TraceItem, TraceResult]("trace")
        .stream(TraceSplit)
        .map(per_item)
    )
    workflow: Chain[TraceNumbers, TraceResult] = (
        Chain[TraceNumbers, TraceNumbers]("workflow").use(stream_stage)
    )

    result = Runtime().run_sync(workflow, TraceNumbers(items=[1, 2], events=[]))

    assert result.values == [1, 2]
    assert result.events == [
        "enter:1",
        "leave:1",
        "enter:2",
        "leave:2",
        "collect",
    ]


def test_stream_child_chain_error_handling_runs_inside_map() -> None:
    per_item: Chain[TraceItem, TraceItem] = (
        Chain[TraceItem, TraceItem]("trace")
        .use(TraceLifecycle)
        .use(TraceHandler)
        .use(TraceFail)
    )
    stream_stage: StreamChain[TraceNumbers, TraceItem, TraceItem, TraceResult] = (
        StreamChain[TraceNumbers, TraceItem, TraceItem, TraceResult]("trace")
        .stream(TraceSplit)
        .map(per_item)
    )
    workflow: Chain[TraceNumbers, TraceResult] = (
        Chain[TraceNumbers, TraceNumbers]("workflow").use(stream_stage)
    )

    result = Runtime().run_sync(workflow, TraceNumbers(items=[1], events=[]))

    assert result.values == [1]
    assert result.events == [
        "enter:1",
        "handler-enter:1",
        "fail-enter:1",
        "handler-error:ValueError:1",
        "leave:1",
        "collect",
    ]


def test_stream_map_uses_thread_pool_parallelism() -> None:
    SlowSquare.active = 0
    SlowSquare.max_active = 0

    per_item: Chain[NumberItem, SquaredItem] = Chain("slow-square").use(
        SlowSquare
    ).on(
        ThreadPoolPolicy("pool", workers=2)
    )
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, Total] = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("split-square")
        .stream(SplitNumbers)
        .map(per_item)
    )
    workflow: Chain[Numbers, Total] = (
        Chain[Numbers, Numbers]("sum of squares")
        .use(stream_stage)
        .on(ThreadPolicy("main"))
    )

    runtime = Runtime()
    result = runtime.run_sync(workflow, Numbers(items=[1, 2, 3, 4]))

    assert result.total == 30
    assert SlowSquare.max_active == 2

    runtime.shutdown()


def test_thread_pool_stream_map_preserves_input_order_when_items_finish_out_of_order() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("slow-square").use(
        SlowOutOfOrderSquare
    ).on(
        ThreadPoolPolicy("pool", workers=4)
    )
    stream_stage: StreamChain[Numbers, NumberItem, SquaredItem, OrderedValues] = (
        StreamChain[Numbers, NumberItem, SquaredItem, OrderedValues]("ordered")
        .stream(OrderedSplit)
        .map(per_item)
    )
    workflow: Chain[Numbers, OrderedValues] = (
        Chain[Numbers, Numbers]("workflow").use(stream_stage)
    )
    runtime = Runtime()

    try:
        result = runtime.run_sync(workflow, Numbers(items=[1, 2, 3, 4]))
    finally:
        runtime.shutdown()

    assert result.values == [1, 2, 3, 4]
