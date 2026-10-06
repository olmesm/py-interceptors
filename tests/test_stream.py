import asyncio
import threading
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass

import pytest
from doubles import (
    SQUARE,
    NumberItem,
    Numbers,
    SquaredItem,
    Total,
    split_square,
)

from py_interceptors import (
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPoolPolicy,
)


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


class MapFails(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        return [NumberItem(value=item) for item in ctx.items]

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
        return [NumberItem(value=item) for item in ctx.items]

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        raise ValueError("collect failed")

    def error(self, ctx: Numbers, err: Exception) -> Total:
        return Total(total=-3)


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


class AsyncCollectNumbers(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        return [NumberItem(value=item) for item in ctx.items]

    async def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        await asyncio.sleep(0)
        return Total(total=sum(item.squared for item in items))


class OrderedSplit(StreamInterceptor[Numbers, NumberItem, SquaredItem, Numbers]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Numbers

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        return [NumberItem(value=item) for item in ctx.items]

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Numbers:
        return Numbers(items=[item.value for item in items])


class FailingSquare(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    def enter(self, ctx: NumberItem) -> SquaredItem:
        raise ValueError("map failed")


class GatedSquare(Interceptor[NumberItem, SquaredItem]):
    """Item 1 finishes only after another item has set ``gate``; ``done`` records the order."""

    input_type = NumberItem
    output_type = SquaredItem

    gate: threading.Event
    done: list[int]

    def enter(self, ctx: NumberItem) -> SquaredItem:
        if ctx.value == 1:
            self.gate.wait(timeout=1)
        else:
            self.gate.set()
        self.done.append(ctx.value)
        return SquaredItem(value=ctx.value, squared=ctx.value * ctx.value)


class TraceSplit(StreamInterceptor[TraceNumbers, TraceItem, TraceItem, TraceResult]):
    input_type = TraceNumbers
    emit_type = TraceItem
    collect_type = TraceItem
    output_type = TraceResult

    def stream(self, ctx: TraceNumbers) -> Iterable[TraceItem]:
        return [TraceItem(value=item, events=ctx.events) for item in ctx.items]

    def collect(self, ctx: TraceNumbers, items: Iterable[TraceItem]) -> TraceResult:
        collected = list(items)
        ctx.events.append("collect")
        return TraceResult(values=[item.value for item in collected], events=ctx.events)


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


def _trace_workflow(
    per_item: Chain[TraceItem, TraceItem],
) -> Chain[TraceNumbers, TraceResult]:
    stage = (
        StreamChain[TraceNumbers, TraceItem, TraceItem, TraceResult]("trace")
        .stream(TraceSplit)
        .map(per_item)
    )
    return Chain[TraceNumbers, TraceNumbers]("workflow").use(stage)


def test_stream_stage_runs_sync() -> None:
    workflow = split_square()

    with Runtime() as runtime:
        compiled = runtime.compile(workflow, initial=Numbers)
        assert compiled.is_async is False
        assert compiled.run_sync(Numbers(items=[1, 2, 3])) == Total(total=14)


def test_stream_empty_items_calls_collect_with_empty_results() -> None:
    assert Runtime().run_sync(split_square(), Numbers(items=[])) == Total(total=0)


FAIL: Chain[NumberItem, SquaredItem] = Chain("fail").use(FailingSquare)


@pytest.mark.parametrize(
    ("opener", "per_item", "total"),
    [
        pytest.param(StreamFails, SQUARE, -1, id="stream"),
        pytest.param(MapFails, FAIL, -2, id="map"),
        pytest.param(
            MapFails, FAIL.on(ThreadPoolPolicy("pool", workers=2)), -2, id="pooled-map"
        ),
        pytest.param(CollectFails, SQUARE, -3, id="collect"),
    ],
)
def test_stream_error_handles_failure_at_each_point(
    opener: type[StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]],
    per_item: Chain[NumberItem, SquaredItem],
    total: int,
) -> None:
    with Runtime() as runtime:
        result = runtime.run_sync(split_square(opener, per_item), Numbers(items=[1, 2]))

    assert result == Total(total=total)


def test_async_stream_iterator_failure_uses_async_error_handler() -> None:
    workflow = split_square(AsyncStreamFails)

    result = asyncio.run(Runtime().run_async(workflow, Numbers(items=[1])))

    assert result == Total(total=-4)


def test_stream_error_raises_replacement_error() -> None:
    with pytest.raises(RuntimeError, match="replacement"):
        Runtime().run_sync(split_square(StreamErrorRaises), Numbers(items=[1]))


def test_stream_stage_runs_async_collect() -> None:
    workflow = split_square(AsyncCollectNumbers)

    result = asyncio.run(Runtime().run_async(workflow, Numbers(items=[1, 2, 3])))

    assert result == Total(total=14)


def test_stream_child_chain_leave_runs_before_collect() -> None:
    workflow = _trace_workflow(Chain("trace").use(TraceLifecycle))

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
    per_item = (
        Chain[TraceItem, TraceItem]("trace")
        .use(TraceLifecycle)
        .use(TraceHandler)
        .use(TraceFail)
    )

    result = Runtime().run_sync(
        _trace_workflow(per_item), TraceNumbers(items=[1], events=[])
    )

    assert result.values == [1]
    assert result.events == [
        "enter:1",
        "handler-enter:1",
        "fail-enter:1",
        "handler-error:ValueError:1",
        "leave:1",
        "collect",
    ]


def test_thread_pool_stream_map_preserves_input_order_when_items_finish_out_of_order() -> (
    None
):
    done: list[int] = []
    per_item: Chain[NumberItem, SquaredItem] = (
        Chain("gated")
        .use(GatedSquare, gate=threading.Event(), done=done)
        .on(ThreadPoolPolicy("pool", workers=4))
    )
    stage = (
        StreamChain[Numbers, NumberItem, SquaredItem, Numbers]("ordered")
        .stream(OrderedSplit)
        .map(per_item)
    )
    workflow = Chain[Numbers, Numbers]("workflow").use(stage)

    with Runtime() as runtime:
        result = runtime.run_sync(workflow, Numbers(items=[1, 2, 3, 4]))

    assert done[0] != 1
    assert result == Numbers(items=[1, 2, 3, 4])
