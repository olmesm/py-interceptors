from collections.abc import Iterable

import pytest
from doubles import (
    SQUARE,
    NumberItem,
    Numbers,
    SplitNumbers,
    SquaredItem,
    Start,
    Total,
    split_square,
)

from py_interceptors import (
    Chain,
    Interceptor,
    Runtime,
    StreamChain,
    StreamInterceptor,
    ValidationError,
)


class MissingInput(Interceptor[Start, Start]):
    output_type = Start


class MissingOutput(Interceptor[Start, Start]):
    input_type = Start


class EmptyName(Interceptor[Start, Start]):
    name = ""
    input_type = Start
    output_type = Start


class ExplicitObject(Interceptor[object, object]):
    input_type = object
    output_type = object


class Typed(Interceptor[Start, Start]):
    input_type = Start
    output_type = Start


class InheritsMetadata(Typed):
    pass


class InheritsStreamMetadata(SplitNumbers):
    pass


class WrongMapOutput(Interceptor[NumberItem, NumberItem]):
    input_type = NumberItem
    output_type = NumberItem


class MissingStreamMetadata(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        return []

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)


class MissingStreamMethod(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=0)


class MissingCollectMethod(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        return []


class EmptyStreamName(SplitNumbers):
    name = ""


@pytest.mark.parametrize(
    ("step", "message"),
    [
        pytest.param(MissingInput, "missing required metadata: input_type", id="input"),
        pytest.param(
            MissingOutput, "missing required metadata: output_type", id="output"
        ),
        pytest.param(EmptyName, "name must be a non-empty string", id="name"),
    ],
)
def test_compile_rejects_bad_interceptor_metadata(
    step: type[Interceptor[Start, Start]], message: str
) -> None:
    workflow: Chain[Start, Start] = Chain("bad").use(step)

    with pytest.raises(ValidationError, match=message):
        Runtime().compile(workflow, initial=Start)


@pytest.mark.parametrize(
    ("opener", "message"),
    [
        pytest.param(
            MissingStreamMetadata,
            "missing required metadata: collect_type",
            id="metadata",
        ),
        pytest.param(EmptyStreamName, "name must be a non-empty string", id="name"),
        pytest.param(
            MissingStreamMethod, "missing required methods: stream", id="stream-method"
        ),
        pytest.param(
            MissingCollectMethod,
            "missing required methods: collect",
            id="collect-method",
        ),
    ],
)
def test_compile_rejects_bad_stream_interceptor_metadata(
    opener: type[StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        Runtime().compile(split_square(opener), initial=Numbers)


def test_compile_accepts_inherited_metadata() -> None:
    workflow: Chain[Start, Start] = Chain("inherited").use(InheritsMetadata)
    runtime = Runtime()

    compiled = runtime.compile(workflow, initial=Start)

    assert (compiled.input_spec, compiled.output_spec) == (Start, Start)
    assert runtime.compile(split_square(InheritsStreamMetadata)).output_spec is Total


def test_compile_allows_explicit_object_metadata() -> None:
    workflow: Chain[object, object] = Chain("object").use(ExplicitObject)

    compiled = Runtime().compile(workflow, initial=object)

    assert compiled.input_spec is object
    assert compiled.output_spec is object


@pytest.mark.parametrize(
    ("stage", "message"),
    [
        pytest.param(
            StreamChain[Numbers, NumberItem, SquaredItem, Total]("bad").map(SQUARE),
            "'bad' is missing stream",
            id="stream",
        ),
        pytest.param(
            StreamChain[Numbers, NumberItem, SquaredItem, Total]("bad").stream(
                SplitNumbers
            ),
            "'bad' is missing map",
            id="map",
        ),
    ],
)
def test_compile_rejects_incomplete_stream_chain(
    stage: StreamChain[Numbers, NumberItem, SquaredItem, Total], message: str
) -> None:
    workflow: Chain[Numbers, Total] = Chain[Numbers, Numbers]("root").use(stage)

    with pytest.raises(ValidationError, match=message):
        Runtime().compile(workflow, initial=Numbers)


def test_compile_rejects_stream_child_output_incompatible_with_collect() -> None:
    per_item: Chain[NumberItem, SquaredItem] = Chain("wrong").use(WrongMapOutput)  # type: ignore[arg-type]

    with pytest.raises(
        ValidationError,
        match="collector expects SquaredItem but mapped pipeline returns NumberItem",
    ):
        Runtime().compile(split_square(per_item=per_item), initial=Numbers)
