"""Shared test doubles; imported as ``from doubles import ...`` (tests/ is on sys.path)."""

import asyncio
import threading
from collections.abc import Iterable
from dataclasses import dataclass

from py_interceptors import Chain, Interceptor, Policy, StreamChain, StreamInterceptor


@dataclass
class Work:
    value: int = 0


class Identity(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work


class AsyncIdentity(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work

    async def enter(self, ctx: Work) -> Work:
        await asyncio.sleep(0)
        return ctx


class RaiseEnter(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work

    def enter(self, ctx: Work) -> Work:
        raise ValueError("boom")


class AsyncRaiseEnter(Interceptor[Work, Work]):
    input_type = Work
    output_type = Work

    async def enter(self, ctx: Work) -> Work:
        await asyncio.sleep(0)
        raise ValueError("boom")


class Rendezvous(Interceptor[Work, Work]):
    """Waits for ``barrier`` in enter: proves concurrency without sleeping."""

    input_type = Work
    output_type = Work

    barrier: threading.Barrier

    def enter(self, ctx: Work) -> Work:
        self.barrier.wait()
        return ctx


class Fan(StreamInterceptor[Work, Work, Work, Work]):
    """Emits ``Work(0)`` .. ``Work(value - 1)`` and collects the sum of the values."""

    input_type = Work
    emit_type = Work
    collect_type = Work
    output_type = Work

    def stream(self, ctx: Work) -> Iterable[Work]:
        return [Work(value) for value in range(ctx.value)]

    def collect(self, ctx: Work, items: Iterable[Work]) -> Work:
        return Work(sum(item.value for item in items))


def apply_policy[TIn, TOut](
    chain: Chain[TIn, TOut],
    policy: Policy | None,
) -> Chain[TIn, TOut]:
    return chain if policy is None else chain.on(policy)


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


class AddOne(Interceptor[Start, Added]):
    name = "add-one"
    input_type = Start
    output_type = Added

    def enter(self, ctx: Start) -> Added:
        return Added(value=ctx.value, added=ctx.value + 1)


class AsyncAddOne(Interceptor[Start, Added]):
    input_type = Start
    output_type = Added

    async def enter(self, ctx: Start) -> Added:
        await asyncio.sleep(0)
        return Added(value=ctx.value, added=ctx.value + 1)


class DoubleAdded(Interceptor[Added, Doubled]):
    input_type = Added
    output_type = Doubled

    def enter(self, ctx: Added) -> Doubled:
        return Doubled(value=ctx.value, added=ctx.added, doubled=ctx.added * 2)


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


class SplitNumbers(StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]):
    input_type = Numbers
    emit_type = NumberItem
    collect_type = SquaredItem
    output_type = Total

    def stream(self, ctx: Numbers) -> Iterable[NumberItem]:
        return [NumberItem(value=item) for item in ctx.items]

    def collect(self, ctx: Numbers, items: Iterable[SquaredItem]) -> Total:
        return Total(total=sum(item.squared for item in items))


class Square(Interceptor[NumberItem, SquaredItem]):
    input_type = NumberItem
    output_type = SquaredItem

    def enter(self, ctx: NumberItem) -> SquaredItem:
        return SquaredItem(value=ctx.value, squared=ctx.value * ctx.value)


SQUARE: Chain[NumberItem, SquaredItem] = Chain("square").use(Square)


def split_square(
    opener: type[
        StreamInterceptor[Numbers, NumberItem, SquaredItem, Total]
    ] = SplitNumbers,
    per_item: Chain[NumberItem, SquaredItem] = SQUARE,
) -> Chain[Numbers, Total]:
    """``Chain("workflow")`` holding one stream stage: ``opener`` mapped over ``per_item``."""

    stage = (
        StreamChain[Numbers, NumberItem, SquaredItem, Total]("split")
        .stream(opener)
        .map(per_item)
    )
    return Chain[Numbers, Numbers]("workflow").use(stage)
