from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pytest

from py_interceptors import (
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ValidationError,
)


@dataclass
class Start:
    value: int


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


class MissingInput(Interceptor[Start, Start]):
    output_type = Start


class MissingOutput(Interceptor[Start, Start]):
    input_type = Start


class LegacyMetadata(Interceptor[Start, Start]):
    Input = Start
    Output = Start
    input_type = Start
    output_type = Start


class EmptyName(Interceptor[Start, Start]):
    name = ""
    input_type = Start
    output_type = Start


class ExplicitObject(Interceptor[object, object]):
    input_type = object
    output_type = object


class SplitNumbers(StreamInterceptor[Start, NumberItem, SquaredItem, Total]):
    input_type = Start
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Start) -> Iterable[NumberItem]:
        for value in range(ctx.value):
            yield NumberItem(value=value)

    def collect(self, ctx: Start, items: Iterable[SquaredItem]) -> Total:
        return Total(total=sum(item.squared for item in items))


class Square(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    def enter(self, ctx: NumberItem) -> SquaredItem:
        return SquaredItem(value=ctx.value, squared=ctx.value * ctx.value)


class WrongMapOutput(Interceptor[NumberItem, NumberItem]):
    input_type = NumberItem
    output_type = NumberItem


class MissingStreamMetadata(
    StreamInterceptor[Start, NumberItem, SquaredItem, Total]
):
    input_type = Start
    emit_type = NumberItem
    output_type = Total

    def stream(self, ctx: Start) -> Iterable[NumberItem]:
        yield NumberItem(value=ctx.value)

    def collect(self, ctx: Start, items: Iterable[SquaredItem]) -> Total:
        return Total(total=sum(item.squared for item in items))


class MissingStreamMethod(
    StreamInterceptor[Start, NumberItem, SquaredItem, Total]
):
    input_type = Start
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def collect(self, ctx: Start, items: Iterable[SquaredItem]) -> Total:
        return Total(total=sum(item.squared for item in items))


class MissingCollectMethod(
    StreamInterceptor[Start, NumberItem, SquaredItem, Total]
):
    input_type = Start
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Start) -> Iterable[NumberItem]:
        yield NumberItem(value=ctx.value)


class EmptyStreamName(StreamInterceptor[Start, NumberItem, SquaredItem, Total]):
    name = ""
    input_type = Start
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Start) -> Iterable[NumberItem]:
        yield NumberItem(value=ctx.value)

    def collect(self, ctx: Start, items: Iterable[SquaredItem]) -> Total:
        return Total(total=sum(item.squared for item in items))


def test_compile_rejects_interceptor_missing_input_metadata() -> None:
    workflow: Chain[Start, Start] = Chain("bad").use(MissingInput)

    with pytest.raises(ValidationError, match="input_type"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_interceptor_missing_output_metadata() -> None:
    workflow: Chain[Start, Start] = Chain("bad").use(MissingOutput)

    with pytest.raises(ValidationError, match="output_type"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_legacy_uppercase_metadata() -> None:
    workflow: Chain[Start, Start] = Chain("bad").use(LegacyMetadata)

    with pytest.raises(ValidationError, match="legacy metadata"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_empty_interceptor_name() -> None:
    workflow: Chain[Start, Start] = Chain("bad").use(EmptyName)

    with pytest.raises(ValidationError, match="non-empty string"):
        Runtime().compile(workflow, initial=Start)


def test_compile_allows_explicit_object_metadata() -> None:
    workflow: Chain[object, object] = Chain("object").use(ExplicitObject)

    compiled = Runtime().compile(workflow, initial=object)

    assert compiled.input_spec is object
    assert compiled.output_spec is object


def test_compile_rejects_stream_chain_missing_stream() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stage: StreamChain[Start, NumberItem, SquaredItem, Total] = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad").map(per_item)
    )
    workflow: Chain[Start, Total] = Chain[Start, Start]("bad").use(stage)

    with pytest.raises(ValidationError, match="missing stream"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_stream_chain_missing_map() -> None:
    stage: StreamChain[Start, NumberItem, SquaredItem, Total] = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad").stream(SplitNumbers)
    )
    workflow: Chain[Start, Total] = Chain[Start, Start]("bad").use(stage)

    with pytest.raises(ValidationError, match="missing map"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_stream_missing_required_metadata() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stage: StreamChain[Start, NumberItem, SquaredItem, Total] = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad")
        .stream(MissingStreamMetadata)
        .map(per_item)
    )
    workflow: Chain[Start, Total] = Chain[Start, Start]("bad").use(stage)

    with pytest.raises(ValidationError, match="collect_type"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_empty_stream_interceptor_name() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    stage: StreamChain[Start, NumberItem, SquaredItem, Total] = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad")
        .stream(EmptyStreamName)
        .map(per_item)
    )
    workflow: Chain[Start, Total] = Chain[Start, Start]("bad").use(stage)

    with pytest.raises(ValidationError, match="non-empty string"):
        Runtime().compile(workflow, initial=Start)


def test_compile_rejects_stream_missing_required_methods() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)
    missing_stream = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad")
        .stream(MissingStreamMethod)
        .map(per_item)
    )
    missing_collect = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad")
        .stream(MissingCollectMethod)
        .map(per_item)
    )

    with pytest.raises(ValidationError, match="stream"):
        Runtime().compile(Chain[Start, Start]("bad").use(missing_stream), initial=Start)

    with pytest.raises(ValidationError, match="collect"):
        Runtime().compile(
            Chain[Start, Start]("bad").use(missing_collect),
            initial=Start,
        )


def test_compile_rejects_stream_child_output_incompatible_with_collect() -> None:
    per_item: Chain[Any, Any] = Chain("wrong").use(WrongMapOutput)
    stage: StreamChain[Start, NumberItem, SquaredItem, Total] = (
        StreamChain[Start, NumberItem, SquaredItem, Total]("bad")
        .stream(SplitNumbers)
        .map(per_item)
    )
    workflow: Chain[Start, Total] = Chain[Start, Start]("bad").use(stage)

    with pytest.raises(ValidationError, match="collector expects"):
        Runtime().compile(workflow, initial=Start)
