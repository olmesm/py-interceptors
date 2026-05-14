from __future__ import annotations

import typing
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
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


def _interceptor_dependency_hints(
    interceptor_cls: type[Interceptor[Any, Any]],
) -> Mapping[str, object]:
    """
    Return the subset of class-level annotations that describe **required**
    injectable dependencies. An annotation is considered a required
    dependency when:

    - It is not a reserved framework attribute (``name``, ``input_type``,
      ``output_type``).
    - It is not private (does not start with an underscore).
    - It is not a ``ClassVar``.
    - The class does not provide a default value for that attribute on any
      class in its MRO. Annotations with a class-level default are treated
      as optional: descendants may still override them via ``provide(...)``,
      but missing them does not raise.
    """

    all_hints = _interceptor_all_dependency_hints(interceptor_cls)
    return MappingProxyType(
        {
            attr: annotation
            for attr, annotation in all_hints.items()
            if not _has_class_default(interceptor_cls, attr)
        }
    )


def _interceptor_all_dependency_hints(
    interceptor_cls: type[Interceptor[Any, Any]],
) -> Mapping[str, object]:
    """
    Like :func:`_interceptor_dependency_hints` but also includes annotations
    that have a class-level default value. Used by the resolver so that
    ``.provide(...)`` overrides can win over class defaults; the stricter
    "required" view from :func:`_interceptor_dependency_hints` is used for
    raising ``MissingDependencyError``.
    """

    try:
        hints = get_type_hints(interceptor_cls, include_extras=True)
    except Exception:
        # Fall back to raw __annotations__ across the MRO if PEP 563 evaluation
        # is unable to resolve forward references. The dependency feature is
        # best-effort in that case but still functional for resolved types.
        merged: dict[str, object] = {}
        for cls in reversed(interceptor_cls.__mro__):
            merged.update(getattr(cls, "__annotations__", {}))
        hints = merged

    deps: dict[str, object] = {}
    for attr, annotation in hints.items():
        if attr in _RESERVED_INTERCEPTOR_ATTRS:
            continue
        if attr.startswith("_"):
            continue
        if get_origin(annotation) is typing.ClassVar:
            continue
        deps[attr] = annotation
    return MappingProxyType(deps)


def _has_class_default(cls: type[object], attr: str) -> bool:
    """True when any class in ``cls``'s MRO defines ``attr`` as a real value.

    Walks the MRO and treats a presence in ``__dict__`` as a default. The
    base ``Interceptor`` class is skipped so its framework-managed metadata
    fields are not mistaken for user-supplied defaults.
    """

    for klass in cls.__mro__:
        if klass is Interceptor or klass is object:
            continue
        if attr in klass.__dict__:
            return True
    return False


def _value_matches_annotation(value: object, annotation: object) -> bool:
    """
    Conservative compatibility check between a concrete value and a type
    annotation. Mirrors the spirit of ``validation._is_assignable`` but works
    on values rather than type specs.
    """

    if annotation in (Any, object):
        return True

    origin = get_origin(annotation)
    if origin is typing.ClassVar:
        # Strip ClassVar wrapper, e.g. ClassVar[Logger] -> Logger
        args = get_args(annotation)
        if not args:
            return True
        return _value_matches_annotation(value, args[0])
    if origin is typing.Annotated:
        args = get_args(annotation)
        if not args:
            return True
        return _value_matches_annotation(value, args[0])

    if origin is typing.Union or origin is type(None) or origin is None:
        if origin is typing.Union:
            return any(
                _value_matches_annotation(value, arg) for arg in get_args(annotation)
            )

    if isinstance(annotation, type):
        try:
            return isinstance(value, annotation)
        except TypeError:
            return False

    if origin is not None and isinstance(origin, type):
        try:
            return isinstance(value, origin)
        except TypeError:
            return False

    # Anything we cannot reason about runtime-checks-wise is accepted; later
    # validation phases may still reject it.
    return True


@dataclass(frozen=True, slots=True)
class BoundInterceptor:
    """
    An interceptor class paired with kwargs supplied at ``.use(...)`` time.

    Stored inside ``Chain._items`` whenever ``.use(Cls, **kwargs)`` is called
    with at least one keyword argument. The runtime injects ``kwargs`` as
    attributes on a freshly-instantiated interceptor before ``enter`` is
    invoked.

    ``kwargs`` are stored as a tuple of ``(name, value)`` pairs so the
    dataclass remains hashable when the dependency values themselves are
    hashable. Use the ``bindings`` property for a read-only mapping view.
    """

    interceptor_type: type[Interceptor[Any, Any]]
    kwargs: tuple[tuple[str, object], ...] = ()

    @property
    def bindings(self) -> Mapping[str, object]:
        return MappingProxyType(dict(self.kwargs))

    @property
    def input_type(self) -> TypeSpec:
        return self.interceptor_type.input_type

    @property
    def output_type(self) -> TypeSpec:
        return self.interceptor_type.output_type


def _check_direct_bindings(
    interceptor_cls: type[Interceptor[Any, Any]],
    kwargs: Mapping[str, object],
) -> None:
    """Validate ``.use(Cls, **kwargs)`` at the call site."""

    if not kwargs:
        return
    hints = _interceptor_dependency_hints(interceptor_cls)
    for name, value in kwargs.items():
        if name not in hints:
            declared = ", ".join(sorted(hints)) or "(none)"
            raise UnknownDependencyError(
                f"{interceptor_cls.__name__} has no dependency {name!r}. "
                f"Declared dependencies: {declared}"
            )
        annotation = hints[name]
        if not _value_matches_annotation(value, annotation):
            raise DependencyTypeError(
                f"{interceptor_cls.__name__}.{name} expected "
                f"{_annotation_name(annotation)}, got {type(value).__name__}"
            )


def _annotation_name(annotation: object) -> str:
    name = getattr(annotation, "__name__", None)
    if isinstance(name, str):
        return name
    return repr(annotation)


def _normalize_chain_item(item: object) -> object:
    if isinstance(item, Chain):
        return item
    if isinstance(item, StreamChain):
        return item
    if isinstance(item, BoundInterceptor):
        return item
    if isinstance(item, type) and issubclass(item, Interceptor):
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
    if isinstance(item, BoundInterceptor):
        return item.interceptor_type.input_type
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
    if isinstance(item, BoundInterceptor):
        return item.interceptor_type.output_type
    if isinstance(item, type) and issubclass(item, Interceptor):
        return item.output_type
    raise TypeError(f"Unsupported chain item: {item!r}")


def _freeze_provides(
    provides: Mapping[str, object] | tuple[tuple[str, object], ...] | None,
) -> tuple[tuple[str, object], ...]:
    if not provides:
        return ()
    if isinstance(provides, tuple):
        return provides
    return tuple(provides.items())


def _provides_mapping(
    provides: tuple[tuple[str, object], ...],
) -> Mapping[str, object]:
    return MappingProxyType(dict(provides))


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
    _items: tuple[object, ...] = field(default_factory=tuple)
    policy: Policy | None = None
    provides: tuple[tuple[str, object], ...] = ()

    def __init__(
        self,
        name: str,
        *,
        _items: tuple[object, ...] | None = None,
        policy: Policy | None = None,
        provides: (
            Mapping[str, object] | tuple[tuple[str, object], ...] | None
        ) = None,
    ) -> None:
        source_items = _items if _items is not None else ()
        normalized = tuple(_normalize_chain_item(item) for item in source_items)
        object.__setattr__(self, "name", _require_name(name, "Chain"))
        object.__setattr__(self, "_items", normalized)
        object.__setattr__(self, "policy", policy)
        object.__setattr__(self, "provides", _freeze_provides(provides))

    @property
    def provided_bindings(self) -> Mapping[str, object]:
        """Read-only mapping view of dependencies this chain provides."""
        return _provides_mapping(self.provides)

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
        **kwargs: Any,
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

    def use(self, item: object, **kwargs: object) -> Chain[Any, Any]:
        """Return a new chain with ``item`` appended.

        When ``item`` is an ``Interceptor`` class and ``kwargs`` are supplied,
        the kwargs are bound to the interceptor and validated against its
        declared dependency annotations. ``Chain`` and ``StreamChain`` items
        do not accept kwargs at the ``use(...)`` site; use ``provide(...)``
        on the appropriate chain instead.
        """

        if kwargs and not (isinstance(item, type) and issubclass(item, Interceptor)):
            raise TypeError(
                "use(...) only accepts keyword arguments for Interceptor "
                "classes. Use provide(...) on a Chain to supply dependencies."
            )
        if isinstance(item, type) and issubclass(item, Interceptor) and kwargs:
            _check_direct_bindings(item, kwargs)
            normalized: object = BoundInterceptor(
                interceptor_type=item,
                kwargs=tuple(kwargs.items()),
            )
        else:
            normalized = _normalize_chain_item(item)
        return replace(self, _items=(*self._items, normalized))

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
        merged = dict(self.provides)
        merged.update(kwargs)
        return replace(self, provides=tuple(merged.items()))

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
            raise TypeError(
                "StreamChain.stream(...) requires a StreamInterceptor class"
            )
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
    provides: tuple[tuple[str, object], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "chain"))
        object.__setattr__(self, "provides", _freeze_provides(self.provides))

    @overload
    def use[TFirst, TNext](
        self,
        item: InterceptorCls[TFirst, TNext],
        **kwargs: Any,
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

    def use(self, item: object, **kwargs: object) -> _ChainBuilder[Any, Any]:
        if kwargs and not (isinstance(item, type) and issubclass(item, Interceptor)):
            raise TypeError(
                "use(...) only accepts keyword arguments for Interceptor "
                "classes. Use provide(...) on the chain builder to supply "
                "dependencies."
            )
        if isinstance(item, type) and issubclass(item, Interceptor) and kwargs:
            _check_direct_bindings(item, kwargs)
            normalized: object = BoundInterceptor(
                interceptor_type=item,
                kwargs=tuple(kwargs.items()),
            )
        else:
            normalized = _normalize_chain_item(item)
        return _ChainBuilder(
            name=self.name,
            _items=(normalized,),
            policy=self.policy,
            provides=self.provides,
        )

    def on(self, policy: Policy) -> _EmptyChainBuilder:
        return replace(self, policy=policy)

    def provide(self, **kwargs: object) -> _EmptyChainBuilder:
        if not kwargs:
            return self
        merged = dict(self.provides)
        merged.update(kwargs)
        return replace(self, provides=tuple(merged.items()))


@dataclass(frozen=True, slots=True)
class _ChainBuilder[TIn, TOut]:
    name: str
    _items: tuple[object, ...] = field(default_factory=tuple)
    policy: Policy | None = None
    provides: tuple[tuple[str, object], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _require_name(self.name, "chain"))
        object.__setattr__(self, "provides", _freeze_provides(self.provides))

    @overload
    def use[TNext](
        self,
        item: InterceptorCls[TOut, TNext],
        **kwargs: Any,
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

    def use(self, item: object, **kwargs: object) -> _ChainBuilder[Any, Any]:
        if kwargs and not (isinstance(item, type) and issubclass(item, Interceptor)):
            raise TypeError(
                "use(...) only accepts keyword arguments for Interceptor "
                "classes. Use provide(...) on the chain builder to supply "
                "dependencies."
            )
        if isinstance(item, type) and issubclass(item, Interceptor) and kwargs:
            _check_direct_bindings(item, kwargs)
            normalized: object = BoundInterceptor(
                interceptor_type=item,
                kwargs=tuple(kwargs.items()),
            )
        else:
            normalized = _normalize_chain_item(item)
        return replace(self, _items=(*self._items, normalized))

    def on(self, policy: Policy) -> _ChainBuilder[TIn, TOut]:
        return replace(self, policy=policy)

    def provide(self, **kwargs: object) -> _ChainBuilder[TIn, TOut]:
        if not kwargs:
            return self
        merged = dict(self.provides)
        merged.update(kwargs)
        return replace(self, provides=tuple(merged.items()))

    def build(self) -> Chain[TIn, TOut]:
        return Chain(
            self.name,
            _items=self._items,
            policy=self.policy,
            provides=self.provides,
        )


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
            raise TypeError(
                "StreamChain.stream(...) requires a StreamInterceptor class"
            )
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
