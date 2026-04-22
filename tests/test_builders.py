from collections.abc import Iterable
from dataclasses import dataclass
from typing import assert_type

import pytest

from py_interceptors import (
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ThreadPolicy,
    chain,
    stream_chain,
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


def test_chain_builder_infers_types_and_runs() -> None:
    workflow = (
        chain("math")
        .use(AddOne)
        .use(DoubleAdded)
        .on(ThreadPolicy("builder-main"))
        .build()
    )
    assert_type(workflow, Chain[Start, Doubled])

    runtime = Runtime()
    try:
        result = runtime.run_sync(workflow, Start(value=3))
    finally:
        runtime.shutdown()

    assert result == Doubled(value=3, added=4, doubled=8)
    assert workflow.policy == ThreadPolicy("builder-main")


def test_stream_chain_builder_infers_types_and_runs() -> None:
    per_item = (
        chain("square")
        .use(Square)
        .on(ThreadPolicy("builder-worker"))
        .build()
    )
    assert_type(per_item, Chain[NumberItem, SquaredItem])

    stream_stage = (
        stream_chain("split-square")
        .on(ThreadPolicy("builder-stream"))
        .stream(SplitNumbers)
        .map(per_item)
        .build()
    )
    assert_type(stream_stage, StreamChain[Numbers, NumberItem, SquaredItem, Total])

    workflow = chain("sum of squares").use(stream_stage).build()
    assert_type(workflow, Chain[Numbers, Total])

    runtime = Runtime()
    try:
        result = runtime.run_sync(workflow, Numbers(items=[1, 2, 3]))
    finally:
        runtime.shutdown()

    assert result == Total(total=14)
    assert stream_stage.policy == ThreadPolicy("builder-stream")


def test_chains_require_names() -> None:
    with pytest.raises(TypeError):
        Chain()  # type: ignore[call-arg]

    with pytest.raises(TypeError):
        StreamChain()  # type: ignore[call-arg]

    with pytest.raises(TypeError):
        chain()  # type: ignore[call-arg]

    with pytest.raises(TypeError):
        stream_chain()  # type: ignore[call-arg]

    with pytest.raises(ValueError, match="non-empty name"):
        Chain("")

    with pytest.raises(ValueError, match="non-empty name"):
        StreamChain("")

    with pytest.raises(ValueError, match="non-empty name"):
        chain("")

    with pytest.raises(ValueError, match="non-empty name"):
        stream_chain("")
