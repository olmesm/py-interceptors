from __future__ import annotations

import types
import typing
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass, replace
from typing import Any, Self, get_args, get_origin, get_type_hints, overload

from py_interceptors.errors import (
    DependencyTypeError,
    UnknownDependencyError,
)
from py_interceptors.interceptors import (
    Interceptor,
    InterceptorCls,
    StreamInterceptor,
    StreamInterceptorCls,
)
from py_interceptors.policies import Policy
from py_interceptors.types import TypeSpec

_RESERVED_INTERCEPTOR_ATTRS: frozenset[str] = frozenset(
    {"name", "input_type", "output_type"}
)


def _require_name(name: str, type_name: str) -> str:
    if not name:
        raise ValueError(f"{type_name} requires a non-empty name")
    return name


def _dependency_hints(
    interceptor_cls: type[Interceptor[Any, Any]],
) -> dict[str, object]:
    """
    Public, non-``ClassVar`` class annotations other than the reserved
    metadata names. Annotations stay as strings when forward references
    cannot be evaluated; those are matched by name only.
    """

    try:
        hints = get_type_hints(interceptor_cls, include_extras=True)
    except Exception:
        hints = {}
        for cls in reversed(interceptor_cls.__mro__):
            hints.update(getattr(cls, "__annotations__", {}))

    return {
        attr: annotation
        for attr, annotation in hints.items()
        if attr not in _RESERVED_INTERCEPTOR_ATTRS
        and not attr.startswith("_")
        and get_origin(annotation) is not typing.ClassVar
    }


def _has_class_default(cls: type[object], attr: str) -> bool:
    """True when a class in the MRO below the framework bases defines ``attr``."""

    for klass in cls.__mro__:
        if klass in (Interceptor, StreamInterceptor, object):
            continue
        if attr in klass.__dict__:
            return True
    return False


def _type_name(spec: object) -> str:
    name = getattr(spec, "__name__", None)
    if isinstance(name, str):
        return name
    return repr(spec)


def _unwrap(spec: object) -> object:
    while get_origin(spec) in (typing.Annotated, typing.ClassVar):
        spec = get_args(spec)[0]
    return spec


def _is_assignable(provided: object, required: object) -> bool:
    """
    Whether a value of type ``provided`` satisfies the type spec ``required``.

    ``Any``/``object`` match everything, unions match any member, generics
    compare by origin only (``list[int]`` satisfies ``Sequence``), and plain
    classes compare with ``issubclass``.
    """

    provided = _unwrap(provided)
    required = _unwrap(required)
    if required in (Any, object) or provided in (Any, object):
        return True
    if provided == required:
        return True
    if get_origin(required) in (typing.Union, types.UnionType):
        return any(_is_assignable(provided, arg) for arg in get_args(required))
    if get_origin(provided) in (typing.Union, types.UnionType):
        return all(_is_assignable(arg, required) for arg in get_args(provided))
    provided_cls = get_origin(provided) or provided
    required_cls = get_origin(required) or required
    if isinstance(provided_cls, type) and isinstance(required_cls, type):
        try:
            return issubclass(provided_cls, required_cls)
        except TypeError:
            return False
    return False


@dataclass(frozen=True, slots=True)
class BoundInterceptor:
    """
    An interceptor class paired with kwargs supplied at ``.use(...)`` time.

    Stored inside ``Chain._items`` whenever ``.use(Cls, **kwargs)`` is called
    with at least one keyword argument. The runtime injects ``kwargs`` as
    attributes on a freshly-instantiated interceptor before ``enter`` is
    invoked. ``kwargs`` are stored as a tuple of ``(name, value)`` pairs so
    the dataclass remains hashable when the dependency values are.
    """

    interceptor_type: type[Interceptor[Any, Any]]
    kwargs: tuple[tuple[str, object], ...] = ()


def _check_direct_bindings(
    interceptor_cls: type[Interceptor[Any, Any]],
    kwargs: Mapping[str, object],
) -> None:
    """Validate ``.use(Cls, **kwargs)`` at the call site."""

    hints = _dependency_hints(interceptor_cls)
    for name, value in kwargs.items():
        if name not in hints:
            declared = ", ".join(sorted(hints)) or "(none)"
            raise UnknownDependencyError(
                f"{interceptor_cls.__name__} has no dependency {name!r}. "
                f"Declared dependencies: {declared}"
            )
        annotation = hints[name]
        if isinstance(annotation, str):
            continue
        if not _is_assignable(type(value), annotation):
            raise DependencyTypeError(
                f"{interceptor_cls.__name__}.{name} expected "
                f"{_type_name(annotation)}, got {type(value).__name__}"
            )


def _bind_item(item: object, kwargs: Mapping[str, object]) -> object:
    """Validate one ``use(...)`` argument and return the item to store."""

    if isinstance(item, type) and issubclass(item, Interceptor):
        if not kwargs:
            return item
        _check_direct_bindings(item, kwargs)
        return BoundInterceptor(item, tuple(kwargs.items()))
    if kwargs:
        raise TypeError(
            "use(...) only accepts keyword arguments for Interceptor "
            "classes. Use provide(...) on a Chain to supply dependencies."
        )
    if isinstance(item, (Chain, StreamChain, BoundInterceptor)):
        return item
    raise TypeError(
        "Chain items must be Interceptor classes, BoundInterceptor instances, "
        "Chain instances, or StreamChain instances"
    )


def _normalize_stream_item(item: object) -> Chain[Any, Any]:
    if isinstance(item, Chain):
        return item
    if isinstance(item, BoundInterceptor):
        return Chain("stream-map").use(item.interceptor_type, **dict(item.kwargs))
    if isinstance(item, type) and issubclass(item, Interceptor):
        return Chain("stream-map").use(item)
    raise TypeError("StreamChain.map(...) requires an Interceptor class or a Chain")


def _interceptor_cls_of(item: object) -> type[Interceptor[Any, Any]] | None:
    if isinstance(item, BoundInterceptor):
        return item.interceptor_type
    if isinstance(item, type) and issubclass(item, Interceptor):
        return item
    return None


def _item_input_spec(item: object) -> TypeSpec:
    if isinstance(item, (Chain, StreamChain)):
        return item.input_spec
    step_cls = _interceptor_cls_of(item)
    if step_cls is None:
        raise TypeError(f"Unsupported chain item: {item!r}")
    return step_cls.input_type


def _item_output_spec(item: object) -> TypeSpec:
    if isinstance(item, (Chain, StreamChain)):
        return item.output_spec
    step_cls = _interceptor_cls_of(item)
    if step_cls is None:
        raise TypeError(f"Unsupported chain item: {item!r}")
    return step_cls.output_type


@dataclass(frozen=True, slots=True)
class Chain[TIn, TOut]:
    """
    Immutable one-in, one-out workflow composition.

    Add interceptor classes, nested chains, or stream chains with ``use``.
    Each call returns a new chain and preserves the input/output type flow.
    Dependencies declared on interceptor classes may be bound directly with
    ``use(Cls, **kwargs)`` or supplied to descendants via ``provide(**kwargs)``.

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
    _: KW_ONLY
    _items: tuple[object, ...] = ()
    policy: Policy | None = None
    provides: tuple[tuple[str, object], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "Chain"))

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
        self,
        item: InterceptorCls[TOut, TNext],
        **kwargs: Any,
    ) -> Chain[TIn, TNext]: ...

    @overload
    def use[TNext](self, item: Chain[TOut, TNext]) -> Chain[TIn, TNext]: ...

    @overload
    def use[TNext](
        self,
        item: StreamChain[TOut, Any, Any, TNext],
    ) -> Chain[TIn, TNext]: ...

    def use(self, item: object, **kwargs: object) -> Chain[Any, Any]:
        """Return a new chain with ``item`` appended.

        When ``item`` is an ``Interceptor`` class and ``kwargs`` are supplied,
        the kwargs are bound to the interceptor and validated against its
        declared dependency annotations. ``Chain`` and ``StreamChain`` items
        do not accept kwargs at the ``use(...)`` site; use ``provide(...)``
        on the appropriate chain instead.
        """

        return replace(self, _items=(*self._items, _bind_item(item, kwargs)))

    def on(self, policy: Policy) -> Self:
        """Return a new chain that runs under ``policy`` unless overridden."""
        return replace(self, policy=policy)

    def provide(self, **kwargs: object) -> Self:
        """
        Return a new chain that exposes ``kwargs`` to descendants as injectable
        dependencies. Multiple calls merge with later calls overriding earlier
        bindings with the same name.
        """

        if not kwargs:
            return self
        return replace(self, provides=tuple({**dict(self.provides), **kwargs}.items()))

    def build(self) -> Self:
        """Return this chain; kept so builder-style call sites read the same."""
        return self


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
            raise TypeError(
                "StreamChain.stream(...) requires a StreamInterceptor class"
            )
        return replace(self, opener=opener)

    @overload
    def map(
        self,
        item: InterceptorCls[TEmit, TCollect],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]: ...

    @overload
    def map(
        self,
        item: Chain[TEmit, TCollect],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]: ...

    def map(self, item: object) -> StreamChain[Any, Any, Any, Any]:
        """Return a new stream chain with ``item`` mapped over emitted values."""
        return replace(self, processor=_normalize_stream_item(item))

    def on(self, policy: Policy) -> Self:
        """Return a new stream chain that runs under ``policy``."""
        return replace(self, policy=policy)

    def build(self) -> Self:
        """Return this stream chain; kept so builder-style call sites read the same."""
        return self


@dataclass(frozen=True, slots=True)
class _EmptyChainBuilder:
    """``chain(name)`` before its first ``use``: the first item fixes ``TIn``."""

    _chain: Chain[Any, Any]

    @overload
    def use[TFirst, TNext](
        self,
        item: InterceptorCls[TFirst, TNext],
        **kwargs: Any,
    ) -> Chain[TFirst, TNext]: ...

    @overload
    def use[TFirst, TNext](
        self, item: Chain[TFirst, TNext]
    ) -> Chain[TFirst, TNext]: ...

    @overload
    def use[TFirst, TNext](
        self,
        item: StreamChain[TFirst, Any, Any, TNext],
    ) -> Chain[TFirst, TNext]: ...

    def use(self, item: object, **kwargs: object) -> Chain[Any, Any]:
        return replace(self._chain, _items=(_bind_item(item, kwargs),))

    def on(self, policy: Policy) -> Self:
        return replace(self, _chain=self._chain.on(policy))

    def provide(self, **kwargs: object) -> Self:
        return replace(self, _chain=self._chain.provide(**kwargs))


@dataclass(frozen=True, slots=True)
class _EmptyStreamChainBuilder:
    """``stream_chain(name)`` before ``stream``: the opener fixes all four types."""

    _chain: StreamChain[Any, Any, Any, Any]

    def stream[TIn, TEmit, TCollect, TOut](
        self,
        opener: StreamInterceptorCls[TIn, TEmit, TCollect, TOut],
    ) -> _StreamMapBuilder[TIn, TEmit, TCollect, TOut]:
        return _StreamMapBuilder(self._chain.stream(opener))

    def on(self, policy: Policy) -> Self:
        return replace(self, _chain=self._chain.on(policy))


@dataclass(frozen=True, slots=True)
class _StreamMapBuilder[TIn, TEmit, TCollect, TOut]:
    """A stream chain with its opener set, waiting for ``map``."""

    _chain: StreamChain[TIn, TEmit, TCollect, TOut]

    @overload
    def map(
        self,
        item: InterceptorCls[TEmit, TCollect],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]: ...

    @overload
    def map(
        self,
        item: Chain[TEmit, TCollect],
    ) -> StreamChain[TIn, TEmit, TCollect, TOut]: ...

    def map(self, item: object) -> StreamChain[Any, Any, Any, Any]:
        return replace(self._chain, processor=_normalize_stream_item(item))

    def on(self, policy: Policy) -> Self:
        return replace(self, _chain=self._chain.on(policy))


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
    return _EmptyChainBuilder(Chain(name))


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
    return _EmptyStreamChainBuilder(StreamChain(name))
