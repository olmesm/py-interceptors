from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any, get_args, get_origin

from py_interceptors.chains import (
    BoundInterceptor,
    Chain,
    StreamChain,
    _interceptor_all_dependency_hints,
    _interceptor_dependency_hints,
    _item_input_spec,
    _item_output_spec,
    _value_matches_annotation,
)
from py_interceptors.errors import (
    AmbiguousDependencyError,
    DependencyTypeError,
    MissingDependencyError,
    ValidationError,
)
from py_interceptors.interceptors import (
    Interceptor,
    InterceptorCls,
    StreamInterceptor,
)
from py_interceptors.policies import AsyncPolicy, Policy, ThreadPolicy, ThreadPoolPolicy
from py_interceptors.types import TypeSpec

type _ProvideScope = Mapping[str, object]
type _ResolutionMap = dict[int, Mapping[str, object]]


def _type_name(spec: object) -> str:
    name = getattr(spec, "__name__", None)
    if isinstance(name, str):
        return name
    return repr(spec)


def _is_assignable(provided: object, required: object) -> bool:
    """
    Conservative runtime compatibility check for validation.

    Supported:
    - normal classes via issubclass
    - exact typing alias equality, e.g. list[str] -> list[str]
    - object / Any as universal matches
    """

    if required in (Any, object):
        return True
    if provided in (Any, object):
        return True
    if provided == required:
        return True

    prov_origin = get_origin(provided)
    req_origin = get_origin(required)

    if isinstance(provided, type) and req_origin is provided:
        return True

    if prov_origin is not None or req_origin is not None:
        if provided == required:
            return True
        if prov_origin == req_origin and prov_origin is not None:
            return get_args(provided) == get_args(required)
        return False

    if isinstance(provided, type) and isinstance(required, type):
        try:
            return issubclass(provided, required)
        except TypeError:
            return False

    return False


def _is_async_callable(fn: object) -> bool:
    if fn is None:
        return False
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


type _PolicySignature = tuple[str, int | bool | None]


def _validate_chain(
    chain: Chain[Any, Any],
    initial: TypeSpec | None,
    policies: dict[str, _PolicySignature] | None = None,
    provide_stack: tuple[_ProvideScope, ...] = (),
    resolution: _ResolutionMap | None = None,
) -> TypeSpec:
    seen_policies = policies if policies is not None else {}
    _validate_policy(chain.policy, seen_policies)

    # Push this chain's provides onto the stack (nearest-first ordering).
    chain_scope: _ProvideScope = chain.provided_bindings
    next_stack: tuple[_ProvideScope, ...] = (
        (chain_scope, *provide_stack) if chain_scope else provide_stack
    )

    current: TypeSpec | None = initial

    if not chain.items:
        return current if current is not None else object

    for idx, item in enumerate(chain.items):
        if isinstance(item, type) and issubclass(item, Interceptor):
            _validate_interceptor_cls(item)
        elif isinstance(item, BoundInterceptor):
            _validate_interceptor_cls(item.interceptor_type)

        required = _item_input_spec(item)

        if current is None:
            current = required
        elif not _is_assignable(current, required):
            raise ValidationError(
                f"Chain '{chain.name}' item #{idx + 1} expects "
                f"{_type_name(required)} but received {_type_name(current)}"
            )

        if isinstance(item, Chain):
            current = _validate_chain(
                item, current, seen_policies, next_stack, resolution
            )
        elif isinstance(item, StreamChain):
            current = _validate_stream_chain(
                item, current, seen_policies, next_stack, resolution
            )
        else:
            current = _item_output_spec(item)
            if resolution is not None:
                _resolve_step_dependencies(item, next_stack, resolution)

    return current if current is not None else object


def _resolve_step_dependencies(
    item: object,
    provide_stack: tuple[_ProvideScope, ...],
    resolution: _ResolutionMap,
) -> None:
    """
    Resolve declared dependencies for a single chain step and record the
    fully-resolved attribute map in ``resolution`` keyed by ``id(item)``.
    """

    if isinstance(item, BoundInterceptor):
        interceptor_cls = item.interceptor_type
        direct = dict(item.kwargs)
    elif isinstance(item, type) and issubclass(item, Interceptor):
        interceptor_cls = item
        direct = {}
    else:
        return

    # Resolve against the BROADER hint set (including class-defaulted fields)
    # so .provide(...) can override class-level defaults. The required-check
    # pass uses the stricter set for MissingDependencyError reporting.
    hints = _interceptor_all_dependency_hints(interceptor_cls)
    if not hints and not direct:
        resolution[id(item)] = {}
        return

    resolved: dict[str, object] = dict(direct)

    for name, annotation in hints.items():
        if name in resolved:
            continue
        value, found = _resolve_from_provide_stack(
            interceptor_cls=interceptor_cls,
            attr_name=name,
            annotation=annotation,
            provide_stack=provide_stack,
        )
        if found:
            resolved[name] = value

    # Note: we do not raise MissingDependencyError for unresolved hints here.
    # Many interceptor attributes are not framework-managed dependencies
    # (e.g. ``state: dict[str, int] = {}`` on the class). A dependency is
    # only considered "missing" if the user explicitly tried to bind it via
    # .provide() but no descendant resolved it -- which is hard to detect
    # without explicit declarations. Today we silently leave unresolved
    # attributes alone; the user gets a clear AttributeError if their
    # interceptor tries to read one. See docs/dependencies.md.
    resolution[id(item)] = resolved


def _resolve_from_provide_stack(
    interceptor_cls: type[Interceptor[Any, Any]],
    attr_name: str,
    annotation: object,
    provide_stack: tuple[_ProvideScope, ...],
) -> tuple[object, bool]:
    """
    Walk providers from nearest to root. Prefer a name match (``attr_name``);
    fall back to a single type match at the same scope. Raise on type
    mismatch or same-scope ambiguity. Return ``(value, True)`` on success,
    ``(None, False)`` if no provider matched.
    """

    for scope in provide_stack:
        if attr_name in scope:
            value = scope[attr_name]
            if not _value_matches_annotation(value, annotation):
                raise DependencyTypeError(
                    f"{interceptor_cls.__name__}.{attr_name} expected "
                    f"{_annotation_name(annotation)}, got "
                    f"{type(value).__name__} from a parent provide(...)"
                )
            return value, True

        # Type-only fallback at this scope.
        candidates: list[tuple[str, object]] = []
        for provided_name, provided_value in scope.items():
            if _value_matches_annotation(provided_value, annotation):
                candidates.append((provided_name, provided_value))
        if len(candidates) == 1:
            return candidates[0][1], True
        if len(candidates) > 1:
            names = ", ".join(sorted(n for n, _ in candidates))
            raise AmbiguousDependencyError(
                f"{interceptor_cls.__name__}.{attr_name} "
                f"({_annotation_name(annotation)}) matches multiple values "
                f"in a single provide(...) scope: {names}. Bind explicitly "
                f"with .use({interceptor_cls.__name__}, {attr_name}=...) or "
                f"rename the provide kwarg."
            )

    return None, False


def _annotation_name(annotation: object) -> str:
    name = getattr(annotation, "__name__", None)
    if isinstance(name, str):
        return name
    return repr(annotation)


def build_resolution_map(chain: Chain[Any, Any]) -> _ResolutionMap:
    """
    Walk ``chain`` and return a map of ``id(step) -> {attr: value}`` for every
    bound or class-only interceptor step, applying nearest-wins resolution
    against the chain's ``.provide(...)`` scopes. Raises on missing required
    bindings only when a step is explicitly bound and a declared field cannot
    be resolved.
    """

    resolution: _ResolutionMap = {}
    _validate_chain(chain, None, {}, (), resolution)
    _check_required_bindings(chain, (), resolution)
    return resolution


def _check_required_bindings(
    chain: Chain[Any, Any],
    provide_stack: tuple[_ProvideScope, ...],
    resolution: _ResolutionMap,
) -> None:
    """
    Walk the chain tree and raise ``MissingDependencyError`` for any
    interceptor whose declared (no-default) dependency annotations could not
    be resolved from either a direct binding or any ancestor ``.provide``.
    """

    chain_scope = chain.provided_bindings
    next_stack: tuple[_ProvideScope, ...] = (
        (chain_scope, *provide_stack) if chain_scope else provide_stack
    )

    for item in chain.items:
        if isinstance(item, Chain):
            _check_required_bindings(item, next_stack, resolution)
            continue
        if isinstance(item, StreamChain):
            if item.processor is not None:
                _check_required_bindings(item.processor, next_stack, resolution)
            continue
        if isinstance(item, BoundInterceptor):
            interceptor_cls = item.interceptor_type
        elif isinstance(item, type) and issubclass(item, Interceptor):
            interceptor_cls = item
        else:
            continue

        hints = _interceptor_dependency_hints(interceptor_cls)
        if not hints:
            continue
        resolved = resolution.get(id(item), {})
        missing = sorted(name for name in hints if name not in resolved)
        if missing:
            raise MissingDependencyError(
                f"{interceptor_cls.__name__} is missing required "
                f"dependencies: {', '.join(missing)}. Bind them at "
                f".use({interceptor_cls.__name__}, ...) or supply them via "
                f".provide(...) on this chain or an ancestor."
            )


def _validate_stream_chain(
    stream_chain: StreamChain[Any, Any, Any, Any],
    initial: TypeSpec | None,
    policies: dict[str, _PolicySignature],
    provide_stack: tuple[_ProvideScope, ...] = (),
    resolution: _ResolutionMap | None = None,
) -> TypeSpec:
    _validate_policy(stream_chain.policy, policies)

    if stream_chain.opener is None:
        raise ValidationError(
            f"StreamChain '{stream_chain.name}' is missing stream(...)"
        )
    if stream_chain.processor is None:
        raise ValidationError(f"StreamChain '{stream_chain.name}' is missing map(...)")

    opener = stream_chain.opener
    processor = stream_chain.processor
    _validate_stream_interceptor_cls(opener)

    current = initial if initial is not None else opener.input_type
    if not _is_assignable(current, opener.input_type):
        raise ValidationError(
            f"StreamChain '{stream_chain.name}' expects "
            f"{_type_name(opener.input_type)} but received {_type_name(current)}"
        )

    processor_out = _validate_chain(
        processor, opener.emit_type, policies, provide_stack, resolution
    )

    if not _is_assignable(processor_out, opener.collect_type):
        raise ValidationError(
            f"StreamChain '{stream_chain.name}' collector expects "
            f"{_type_name(opener.collect_type)} but mapped pipeline returns "
            f"{_type_name(processor_out)}"
        )

    return opener.output_type


def _validate_interceptor_cls(step_cls: type[Interceptor[Any, Any]]) -> None:
    _validate_step_name(step_cls, "Interceptor")
    _reject_legacy_metadata(step_cls, ("Input", "Output"))
    missing = [
        attr for attr in ("input_type", "output_type") if attr not in step_cls.__dict__
    ]
    if missing:
        raise ValidationError(
            f"Interceptor '{step_cls.__name__}' is missing required metadata: "
            + ", ".join(missing)
        )


def _validate_stream_interceptor_cls(
    step_cls: type[StreamInterceptor[Any, Any, Any, Any]],
) -> None:
    _validate_step_name(step_cls, "StreamInterceptor")
    _reject_legacy_metadata(step_cls, ("Input", "Emit", "Collect", "Output"))
    missing_metadata = [
        attr
        for attr in ("input_type", "emit_type", "collect_type", "output_type")
        if attr not in step_cls.__dict__
    ]
    if missing_metadata:
        raise ValidationError(
            f"StreamInterceptor '{step_cls.__name__}' is missing required metadata: "
            + ", ".join(missing_metadata)
        )

    missing_methods = [
        name
        for name in ("stream", "collect")
        if getattr(step_cls, name) is getattr(StreamInterceptor, name)
    ]
    if missing_methods:
        raise ValidationError(
            f"StreamInterceptor '{step_cls.__name__}' is missing required methods: "
            + ", ".join(missing_methods)
        )


def _validate_step_name(step_cls: type[object], type_name: str) -> None:
    name = getattr(step_cls, "name", None)
    if name is None:
        return
    if not isinstance(name, str) or not name:
        raise ValidationError(
            f"{type_name} '{step_cls.__name__}' name must be a non-empty string"
        )


def _reject_legacy_metadata(
    step_cls: type[object],
    legacy_names: tuple[str, ...],
) -> None:
    found = [name for name in legacy_names if name in step_cls.__dict__]
    if found:
        raise ValidationError(
            f"{step_cls.__name__} uses legacy metadata: "
            + ", ".join(found)
            + ". Use lowercase *_type metadata."
        )


def _validate_policy(
    policy: Policy | None,
    policies: dict[str, _PolicySignature],
) -> None:
    if policy is None:
        return

    name = policy.name
    if name is None:
        return

    signature = _policy_signature(policy)
    existing = policies.get(name)
    if existing is None:
        policies[name] = signature
        return

    if existing != signature:
        raise ValidationError(
            f"Policy portal '{name}' has conflicting declarations: "
            f"{_policy_signature_name(existing)} and "
            f"{_policy_signature_name(signature)}"
        )


def _policy_signature(policy: Policy) -> _PolicySignature:
    if isinstance(policy, ThreadPolicy):
        return ("thread", None)
    if isinstance(policy, ThreadPoolPolicy):
        return ("thread-pool", policy.workers)
    return ("async", policy.isolated)


def _policy_signature_name(signature: _PolicySignature) -> str:
    kind, value = signature
    if kind == "thread-pool":
        return f"ThreadPoolPolicy(workers={value})"
    if kind == "async":
        return f"AsyncPolicy(isolated={value})"
    return "ThreadPolicy"


def _chain_is_async(chain: Chain[Any, Any]) -> bool:
    if isinstance(chain.policy, AsyncPolicy):
        return True
    return _chain_body_is_async(chain)


def _chain_body_is_async(chain: Chain[Any, Any]) -> bool:
    for item in chain.items:
        if isinstance(item, Chain):
            if _chain_is_async(item):
                return True
            continue

        if isinstance(item, StreamChain):
            if _stream_chain_is_async(item):
                return True
            continue

        if (
            isinstance(item, type)
            and issubclass(item, Interceptor)
            and _interceptor_cls_is_async(item)
        ):
            return True

    return False


def _stream_chain_is_async(stream_chain: StreamChain[Any, Any, Any, Any]) -> bool:
    if isinstance(stream_chain.policy, AsyncPolicy):
        return True
    return _stream_chain_body_is_async(stream_chain)


def _stream_chain_body_is_async(
    stream_chain: StreamChain[Any, Any, Any, Any],
) -> bool:
    if stream_chain.opener is None:
        return False
    if stream_chain.processor is None:
        return False

    opener = stream_chain.opener
    if _is_async_callable(opener.stream):
        return True
    if _is_async_callable(opener.collect):
        return True
    if _is_async_callable(opener.error):
        return True

    return _chain_is_async(stream_chain.processor)


def _interceptor_cls_is_async(step_cls: InterceptorCls[Any, Any]) -> bool:
    return (
        _is_async_callable(step_cls.enter)
        or _is_async_callable(step_cls.leave)
        or _is_async_callable(step_cls.error)
    )
