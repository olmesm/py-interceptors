from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from py_interceptors.types import MaybeAsyncIterable, MaybeAwaitable, TypeSpec


class Context:
    """
    Optional marker base class for user payload/context objects.

    Users may subclass this, but it is not required.
    """


class Interceptor[TIn, TOut]:
    """
    One-in, one-out step.
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
