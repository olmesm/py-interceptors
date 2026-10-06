"""Shared test doubles; imported as ``from doubles import ...`` (tests/ is on sys.path)."""

import asyncio
import threading
from collections.abc import Iterable
from dataclasses import dataclass

from py_interceptors import Chain, Interceptor, Policy, StreamInterceptor


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
