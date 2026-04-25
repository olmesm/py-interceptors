from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import Any, Self, overload

from py_interceptors.interceptors import (
    Interceptor,
    InterceptorCls,
    StreamInterceptor,
    StreamInterceptorCls,
)
from py_interceptors.policies import Policy
from py_interceptors.types import TypeSpec


def _require_name(name: str, type_name: str) -> str:
    if not name:
        raise ValueError(f"{type_name} requires a non-empty name")
    return name


def _normalize_chain_item(item: object) -> object:
    if isinstance(item, Chain):
        return item
    if isinstance(item, StreamChain):
        return item
    if isinstance(item, type) and issubclass(item, Interceptor):
        return item
    raise TypeError(
        "Chain items must be Interceptor classes, Chain instances, or "
        "StreamChain instances"
    )


def _normalize_stream_item(item: object) -> Chain[Any, Any]:
    if isinstance(item, Chain):
        return item
    if isinstance(item, type) and issubclass(item, Interceptor):
        return Chain("stream-map").use(item)
    raise TypeError("StreamChain.map(...) requires an Interceptor class or a Chain")


@overload
def _item_input_spec[TIn](item: InterceptorCls[TIn, Any]) -> TypeSpec: ...


@overload
def _item_input_spec[TIn](item: Chain[TIn, Any]) -> TypeSpec: ...


@overload
def _item_input_spec[TIn](item: StreamChain[TIn, Any, Any, Any]) -> TypeSpec: ...


@overload
def _item_input_spec(item: object) -> TypeSpec: ...


def _item_input_spec(item: object) -> TypeSpec:
    if isinstance(item, Chain):
        return item.input_spec
    if isinstance(item, StreamChain):
        return item.input_spec
    if isinstance(item, type) and issubclass(item, Interceptor):
        return item.input_type
    raise TypeError(f"Unsupported chain item: {item!r}")


@overload
def _item_output_spec[TOut](item: InterceptorCls[Any, TOut]) -> TypeSpec: ...


@overload
def _item_output_spec[TOut](item: Chain[Any, TOut]) -> TypeSpec: ...


@overload
def _item_output_spec[TOut](item: StreamChain[Any, Any, Any, TOut]) -> TypeSpec: ...


@overload
def _item_output_spec(item: object) -> TypeSpec: ...


def _item_output_spec(item: object) -> TypeSpec:
    if isinstance(item, Chain):
        return item.output_spec
    if isinstance(item, StreamChain):
        return item.output_spec
    if isinstance(item, type) and issubclass(item, Interceptor):
        return item.output_type
    raise TypeError(f"Unsupported chain item: {item!r}")


@dataclass(frozen=True, slots=True)
class Chain[TIn, TOut]:
    """
    Immutable one-in, one-out workflow composition.

    Add interceptor classes, nested chains, or stream chains with ``use``.
    Each call returns a new chain and preserves the input/output type flow.

    Example:
        >>> from py_interceptors import Chain, Interceptor, Runtime
        >>>
        >>> class ParseInt(Interceptor[str, int]):
        ...     input_type = str
        ...     output_type = int
        ...
        ...     def enter(self, ctx: str) -> int:
        ...         return int(ctx)
        ...
        >>> workflow = Chain[str, int]("parse").use(ParseInt)
        >>> with Runtime() as runtime:
        ...     result = runtime.run_sync(workflow, "42")
        >>> result
        42
    """

    name: str
    _items: tuple[object, ...] = field(default_factory=tuple)
    policy: Policy | None = None

    def __init__(
        self,
        name: str,
        *,
        _items: tuple[object, ...] | None = None,
        policy: Policy | None = None,
    ) -> None:
        source_items = _items if _items is not None else ()
        normalized = tuple(_normalize_chain_item(item) for item in source_items)
        object.__setattr__(self, "name", _require_name(name, "Chain"))
        object.__setattr__(self, "_items", normalized)
        object.__setattr__(self, "policy", policy)

    @property
    def items(self) -> tuple[object, ...]:
        """Workflow items in execution order."""
        return self._items

    @property
    def input_spec(self) -> TypeSpec:
        """Input type required by the first item, or ``object`` when empty."""
        if not self._items:
            return object
        return _item_input_spec(self._items[0])

    @property
    def output_spec(self) -> TypeSpec:
        """Output type produced by the final item, or ``object`` when empty."""
        if not self._items:
            return object
        return _item_output_spec(self._items[-1])

    @overload
    def use[TNext](
        self: Chain[TIn, TOut],
        item: InterceptorCls[TOut, TNext],
    ) -> Chain[TIn, TNext]: ...

    @overload
    def use[TNext](
        self: Chain[TIn, TOut],
        item: Chain[TOut, TNext],
    ) -> Chain[TIn, TNext]: ...

    @overload
    def use[TNext](
        self: Chain[TIn, TOut],
        item: StreamChain[TOut, Any, Any, TNext],
    ) -> Chain[TIn, TNext]: ...

    def use(self, item: object) -> Chain[Any, Any]:
        """Return a new chain with ``item`` appended."""
        normalized = _normalize_chain_item(item)
        return replace(self, _items=(*self._items, normalized))

    def on(self, policy: Policy) -> Self:
        """Return a new chain that runs under ``policy`` unless overridden."""
        return replace(self, policy=policy)

    def __iter__(self) -> Iterable[object]:
        return iter(self._items)


@dataclass(frozen=True, slots=True)
class StreamChain[TIn, TEmit, TCollect, TOut]:
    """
    Immutable stream scope for split/map/collect workflow stages.

    ``stream`` sets the stream opener. ``map`` sets the child chain or
    interceptor used for each emitted item. A stream chain is executed by
    placing it inside a root ``Chain``.

    Example:
        >>> from collections.abc import Iterable
        >>> from py_interceptors import Chain, Interceptor, Runtime, StreamChain
        >>> from py_interceptors import StreamInterceptor
        >>>
        >>> class Words(StreamInterceptor[str, str, str, str]):
        ...     input_type = str
        ...     emit_type = str
        ...     collect_type = str
        ...     output_type = str
        ...
        ...     def stream(self, ctx: str) -> Iterable[str]:
        ...         return ctx.split()
        ...
        ...     def collect(self, ctx: str, items: Iterable[str]) -> str:
        ...         return " ".join(items)
        ...
        >>> class Upper(Interceptor[str, str]):
        ...     input_type = str
        ...     output_type = str
        ...
        ...     def enter(self, ctx: str) -> str:
        ...         return ctx.upper()
        ...
        >>> stage = StreamChain[str, str, str, str]("upper").stream(Words).map(Upper)
        >>> workflow = Chain[str, str]("headline").use(stage)
        >>> with Runtime() as runtime:
        ...     result = runtime.run_sync(workflow, "hello world")
        >>> result
        'HELLO WORLD'
    """

    name: str
    opener: StreamInterceptorCls[TIn, TEmit, TCollect, TOut] | None = None
    processor: Chain[TEmit, TCollect] | None = None
    policy: Policy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "StreamChain"))

    @property
    def input_spec(self) -> TypeSpec:
        """Input type required by the stream opener, or ``object`` when unset."""
        if self.opener is None:
            return object
        return self.opener.input_type

    @property
    def emit_spec(self) -> TypeSpec:
        """Type emitted by ``stream(...)``, or ``object`` when unset."""
        if self.opener is None:
            return object
        return self.opener.emit_type

    @property
    def collect_spec(self) -> TypeSpec:
        """Type expected by ``collect(...)``, or ``object`` when unset."""
        if self.opener is None:
            return object
        return self.opener.collect_type

    @property
    def output_spec(self) -> TypeSpec:
        """Output type produced by ``collect(...)``, or ``object`` when unset."""
        if self.opener is None:
            return object
        return self.opener.output_type

    def stream(
        self,
        opener: StreamInterceptorCls[TIn, TEmit, TCollect, TOut],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]:
        """Return a new stream chain with ``opener`` as the stream stage."""
        if self.opener is not None:
            raise ValueError("StreamChain.stream(...) may only be called once")
        if not isinstance(opener, type) or not issubclass(opener, StreamInterceptor):
            raise TypeError("StreamChain.stream(...) requires a StreamInterceptor class")
        return replace(self, opener=opener)

    @overload
    def map(
        self: StreamChain[TIn, TEmit, TCollect, TOut],
        item: InterceptorCls[TEmit, TCollect],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]: ...

    @overload
    def map(
        self: StreamChain[TIn, TEmit, TCollect, TOut],
        item: Chain[TEmit, TCollect],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]: ...

    def map(self, item: object) -> StreamChain[Any, Any, Any, Any]:
        """Return a new stream chain with ``item`` mapped over emitted values."""
        processor = _normalize_stream_item(item)
        return replace(self, processor=processor)

    def on(self, policy: Policy) -> Self:
        """Return a new stream chain that runs under ``policy``."""
        return replace(self, policy=policy)


@dataclass(frozen=True, slots=True)
class _EmptyChainBuilder:
    name: str
    policy: Policy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "chain"))

    @overload
    def use[TFirst, TNext](
        self,
        item: InterceptorCls[TFirst, TNext],
    ) -> _ChainBuilder[TFirst, TNext]: ...

    @overload
    def use[TFirst, TNext](
        self,
        item: Chain[TFirst, TNext],
    ) -> _ChainBuilder[TFirst, TNext]: ...

    @overload
    def use[TFirst, TNext](
        self,
        item: StreamChain[TFirst, Any, Any, TNext],
    ) -> _ChainBuilder[TFirst, TNext]: ...

    def use(self, item: object) -> _ChainBuilder[Any, Any]:
        normalized = _normalize_chain_item(item)
        return _ChainBuilder(
            name=self.name,
            _items=(normalized,),
            policy=self.policy,
        )

    def on(self, policy: Policy) -> _EmptyChainBuilder:
        return replace(self, policy=policy)


@dataclass(frozen=True, slots=True)
class _ChainBuilder[TIn, TOut]:
    name: str
    _items: tuple[object, ...] = field(default_factory=tuple)
    policy: Policy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "chain"))

    @overload
    def use[TNext](
        self,
        item: InterceptorCls[TOut, TNext],
    ) -> _ChainBuilder[TIn, TNext]: ...

    @overload
    def use[TNext](
        self,
        item: Chain[TOut, TNext],
    ) -> _ChainBuilder[TIn, TNext]: ...

    @overload
    def use[TNext](
        self,
        item: StreamChain[TOut, Any, Any, TNext],
    ) -> _ChainBuilder[TIn, TNext]: ...

    def use(self, item: object) -> _ChainBuilder[Any, Any]:
        normalized = _normalize_chain_item(item)
        return replace(self, _items=(*self._items, normalized))

    def on(self, policy: Policy) -> _ChainBuilder[TIn, TOut]:
        return replace(self, policy=policy)

    def build(self) -> Chain[TIn, TOut]:
        return Chain(self.name, _items=self._items, policy=self.policy)


@dataclass(frozen=True, slots=True)
class _EmptyStreamChainBuilder:
    name: str
    policy: Policy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "stream_chain"))

    def stream[TIn, TEmit, TCollect, TOut](
        self,
        opener: StreamInterceptorCls[TIn, TEmit, TCollect, TOut],
    ) -> _StreamMapBuilder[TIn, TEmit, TCollect, TOut]:
        if not isinstance(opener, type) or not issubclass(opener, StreamInterceptor):
            raise TypeError("StreamChain.stream(...) requires a StreamInterceptor class")
        return _StreamMapBuilder(
            name=self.name,
            opener=opener,
            policy=self.policy,
        )

    def on(self, policy: Policy) -> _EmptyStreamChainBuilder:
        return replace(self, policy=policy)


@dataclass(frozen=True, slots=True)
class _StreamMapBuilder[TIn, TEmit, TCollect, TOut]:
    opener: StreamInterceptorCls[TIn, TEmit, TCollect, TOut]
    name: str
    policy: Policy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "stream_chain"))

    @overload
    def map(
        self,
        item: InterceptorCls[TEmit, TCollect],
    ) -> _MappedStreamChainBuilder[TIn, TEmit, TCollect, TOut]: ...

    @overload
    def map(
        self,
        item: Chain[TEmit, TCollect],
    ) -> _MappedStreamChainBuilder[TIn, TEmit, TCollect, TOut]: ...

    def map(self, item: object) -> _MappedStreamChainBuilder[Any, Any, Any, Any]:
        processor = _normalize_stream_item(item)
        return _MappedStreamChainBuilder(
            name=self.name,
            opener=self.opener,
            processor=processor,
            policy=self.policy,
        )

    def on(self, policy: Policy) -> _StreamMapBuilder[TIn, TEmit, TCollect, TOut]:
        return replace(self, policy=policy)


@dataclass(frozen=True, slots=True)
class _MappedStreamChainBuilder[TIn, TEmit, TCollect, TOut]:
    opener: StreamInterceptorCls[TIn, TEmit, TCollect, TOut]
    processor: Chain[TEmit, TCollect]
    name: str
    policy: Policy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "stream_chain"))

    def on(
        self,
        policy: Policy,
    ) -> _MappedStreamChainBuilder[TIn, TEmit, TCollect, TOut]:
        return replace(self, policy=policy)

    def build(self) -> StreamChain[TIn, TEmit, TCollect, TOut]:
        return StreamChain(
            name=self.name,
            opener=self.opener,
            processor=self.processor,
            policy=self.policy,
        )


def chain(name: str) -> _EmptyChainBuilder:
    """
    Start a typed chain builder.

    The builder is the preferred way to compose workflows because type
    checkers can infer the chain input and output as each step is added.

    Example:
        >>> from py_interceptors import Interceptor, Runtime, chain
        >>>
        >>> class Strip(Interceptor[str, str]):
        ...     input_type = str
        ...     output_type = str
        ...
        ...     def enter(self, ctx: str) -> str:
        ...         return ctx.strip()
        ...
        >>> class ParseInt(Interceptor[str, int]):
        ...     input_type = str
        ...     output_type = int
        ...
        ...     def enter(self, ctx: str) -> int:
        ...         return int(ctx)
        ...
        >>> workflow = chain("parse").use(Strip).use(ParseInt).build()
        >>> with Runtime() as runtime:
        ...     result = runtime.run_sync(workflow, " 42 ")
        >>> result
        42
    """
    return _EmptyChainBuilder(name=name)


def stream_chain(name: str) -> _EmptyStreamChainBuilder:
    """
    Start a typed stream-chain builder.

    Use ``stream(...)`` with a ``StreamInterceptor`` and ``map(...)`` with an
    interceptor class or child ``Chain``. Place the built stream chain inside a
    root chain before running it.

    Example:
        >>> from collections.abc import Iterable
        >>> from py_interceptors import (
        ...     Interceptor,
        ...     Runtime,
        ...     StreamInterceptor,
        ...     chain,
        ...     stream_chain,
        ... )
        >>>
        >>> class Words(StreamInterceptor[str, str, str, str]):
        ...     input_type = str
        ...     emit_type = str
        ...     collect_type = str
        ...     output_type = str
        ...
        ...     def stream(self, ctx: str) -> Iterable[str]:
        ...         return ctx.split()
        ...
        ...     def collect(self, ctx: str, items: Iterable[str]) -> str:
        ...         return "-".join(items)
        ...
        >>> class Lower(Interceptor[str, str]):
        ...     input_type = str
        ...     output_type = str
        ...
        ...     def enter(self, ctx: str) -> str:
        ...         return ctx.lower()
        ...
        >>> stage = stream_chain("slug words").stream(Words).map(Lower).build()
        >>> workflow = chain("slug").use(stage).build()
        >>> with Runtime() as runtime:
        ...     result = runtime.run_sync(workflow, "Hello World")
        >>> result
        'hello-world'
    """
    return _EmptyStreamChainBuilder(name=name)
