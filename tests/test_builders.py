from collections.abc import Callable
from typing import assert_type

import pytest
from doubles import (
    AddOne,
    DoubleAdded,
    Doubled,
    NumberItem,
    Numbers,
    SplitNumbers,
    Square,
    SquaredItem,
    Start,
    Total,
)

from py_interceptors import (
    Chain,
    Runtime,
    StreamChain,
    ThreadPolicy,
    chain,
    stream_chain,
)


def test_chain_builder_infers_types_and_runs() -> None:
    workflow = (
        chain("math")
        .use(AddOne)
        .use(DoubleAdded)
        .on(ThreadPolicy("builder-main"))
        .build()
    )
    assert_type(workflow, Chain[Start, Doubled])

    with Runtime() as runtime:
        result = runtime.run_sync(workflow, Start(value=3))

    assert result == Doubled(value=3, added=4, doubled=8)
    assert workflow.policy == ThreadPolicy("builder-main")


def test_stream_chain_builder_infers_types_and_runs() -> None:
    per_item = chain("square").use(Square).build()
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

    with Runtime() as runtime:
        result = runtime.run_sync(workflow, Numbers(items=[1, 2, 3]))

    assert result == Total(total=14)
    assert stream_stage.policy == ThreadPolicy("builder-stream")


@pytest.mark.parametrize("make", [Chain, StreamChain, chain, stream_chain])
def test_chains_require_a_non_empty_name(make: Callable[[str], object]) -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        make("")


SPLIT = StreamChain[Numbers, NumberItem, SquaredItem, Total]("split")


@pytest.mark.parametrize(
    ("build", "message"),
    [
        pytest.param(
            lambda: Chain("math").use(42),  # type: ignore[call-overload]
            "Chain items must be Interceptor classes",
            id="chain-use-non-item",
        ),
        pytest.param(
            lambda: StreamChain("split").stream(Square),  # type: ignore[arg-type]
            "requires a StreamInterceptor class",
            id="stream-non-opener",
        ),
        pytest.param(
            lambda: SPLIT.stream(SplitNumbers).stream(SplitNumbers),
            "may only be called once",
            id="stream-twice",
        ),
        pytest.param(
            lambda: StreamChain("split").map(42),  # type: ignore[call-overload]
            "requires an Interceptor class or a Chain",
            id="map-non-item",
        ),
    ],
)
def test_chain_construction_rejects_wrong_items(
    build: Callable[[], object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        build()
