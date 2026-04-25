from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from py_interceptors.types import MaybeAsyncIterable, MaybeAwaitable, TypeSpec


class Context:
    """
    Optional marker base class for user payload/context objects.

    Users may subclass this, but it is not required.

    Example:
        >>> from dataclasses import dataclass
        >>> from py_interceptors import Context
        >>>
        >>> @dataclass
        ... class Request(Context):
        ...     user_id: int
        ...
        >>> isinstance(Request(user_id=42), Context)
        True
    """


class Interceptor[TIn, TOut]:
    """
    One-in, one-out workflow step.

    Declare the runtime types the step accepts and returns with
    ``input_type`` and ``output_type``. Override ``enter`` for forward
    execution, and optionally override ``leave`` and ``error`` for unwind
    behavior.

    Example:
        >>> from py_interceptors import Interceptor
        >>>
        >>> class ParseInt(Interceptor[str, int]):
        ...     input_type = str
        ...     output_type = int
        ...
        ...     def enter(self, ctx: str) -> int:
        ...         return int(ctx)
        ...
        >>> ParseInt().enter("42")
        42
    """

    name: ClassVar[str | None] = None
    input_type: ClassVar[TypeSpec] = object
    output_type: ClassVar[TypeSpec] = object

    def enter(self, ctx: TIn) -> MaybeAwaitable[TOut]:
        return ctx  # type: ignore[return-value]

    def leave(self, ctx: TOut) -> MaybeAwaitable[TOut]:
        return ctx

    def error(self, ctx: TIn | TOut, err: Exception) -> MaybeAwaitable[TOut]:
        raise err


class StreamInterceptor[TIn, TEmit, TCollect, TOut]:
    """
    Scoped stream stage.

    input_type -> stream(...) -> many emit_type items
    mapped child chain processes emit_type -> collect_type
    collect(...) merges collect_type items back into one output_type

    Example:
        >>> from collections.abc import Iterable
        >>> from dataclasses import dataclass
        >>> from py_interceptors import (
        ...     Interceptor,
        ...     Runtime,
        ...     StreamInterceptor,
        ...     chain,
        ...     stream_chain,
        ... )
        >>>
        >>> @dataclass
        ... class Numbers:
        ...     values: list[int]
        ...
        >>> @dataclass
        ... class Number:
        ...     value: int
        ...
        >>> @dataclass
        ... class Squared:
        ...     value: int
        ...
        >>> class Split(StreamInterceptor[Numbers, Number, Squared, int]):
        ...     input_type = Numbers
        ...     emit_type = Number
        ...     collect_type = Squared
        ...     output_type = int
        ...
        ...     def stream(self, ctx: Numbers) -> Iterable[Number]:
        ...         return [Number(value) for value in ctx.values]
        ...
        ...     def collect(self, ctx: Numbers, items: Iterable[Squared]) -> int:
        ...         return sum(item.value for item in items)
        ...
        >>> class Square(Interceptor[Number, Squared]):
        ...     input_type = Number
        ...     output_type = Squared
        ...
        ...     def enter(self, ctx: Number) -> Squared:
        ...         return Squared(ctx.value * ctx.value)
        ...
        >>> stage = stream_chain("split-square").stream(Split).map(Square).build()
        >>> workflow = chain("sum").use(stage).build()
        >>> with Runtime() as runtime:
        ...     result = runtime.run_sync(workflow, Numbers([1, 2, 3]))
        >>> result
        14
    """

    name: ClassVar[str | None] = None
    input_type: ClassVar[TypeSpec] = object
    emit_type: ClassVar[TypeSpec] = object
    collect_type: ClassVar[TypeSpec] = object
    output_type: ClassVar[TypeSpec] = object

    def stream(self, ctx: TIn) -> MaybeAsyncIterable[TEmit]:
        raise NotImplementedError

    def collect(self, ctx: TIn, items: Iterable[TCollect]) -> MaybeAwaitable[TOut]:
        raise NotImplementedError

    def error(self, ctx: TIn, err: Exception) -> MaybeAwaitable[TOut]:
        raise err


type InterceptorCls[TIn, TOut] = type[Interceptor[TIn, TOut]]
type StreamInterceptorCls[TIn, TEmit, TCollect, TOut] = type[
    StreamInterceptor[TIn, TEmit, TCollect, TOut]
]
