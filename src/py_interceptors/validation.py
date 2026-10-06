from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any

from py_interceptors.chains import (
    BoundInterceptor,
    Chain,
    StreamChain,
    _dependency_hints,
    _has_class_default,
    _interceptor_cls_of,
    _is_assignable,
    _item_input_spec,
    _type_name,
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
from py_interceptors.policies import AsyncPolicy, Policy
from py_interceptors.types import TypeSpec


def _is_async_callable(fn: object) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


def _validate_chain(
    chain: Chain[Any, Any],
    initial: TypeSpec | None,
    policies: dict[str, Policy] | None = None,
    provide_stack: tuple[Mapping[str, object], ...] = (),
) -> TypeSpec:
    seen_policies = policies if policies is not None else {}
    _validate_policy(chain.policy, seen_policies)

    chain_scope = dict(chain.provides)
    next_stack = (chain_scope, *provide_stack) if chain_scope else provide_stack

    current: TypeSpec | None = initial
    for idx, item in enumerate(chain.items):
        step_cls = _interceptor_cls_of(item)
        if step_cls is not None:
            _validate_interceptor_cls(step_cls)

        required = _item_input_spec(item)

        if current is None:
            current = required
        elif not _is_assignable(current, required):
            raise ValidationError(
                f"Chain '{chain.name}' item #{idx + 1} expects "
                f"{_type_name(required)} but received {_type_name(current)}"
            )

        if isinstance(item, Chain):
            current = _validate_chain(item, current, seen_policies, next_stack)
        elif isinstance(item, StreamChain):
            current = _validate_stream_chain(item, current, seen_policies, next_stack)
        elif step_cls is not None:
            direct = dict(item.kwargs) if isinstance(item, BoundInterceptor) else {}
            resolve_step_dependencies(step_cls, direct, next_stack)
            current = step_cls.output_type

    return current if current is not None else object


def resolve_step_dependencies(
    interceptor_cls: type[Interceptor[Any, Any]],
    direct: Mapping[str, object],
    provide_stack: tuple[Mapping[str, object], ...],
) -> dict[str, object]:
    """
    Return the attribute map to set on one instance of ``interceptor_cls``.

    Direct bindings win, then the nearest ``provide_stack`` scope (name
    match, else a single type match). A hint with a class default and no
    binding is left to the class; any other unresolved hint raises
    ``MissingDependencyError``.
    """

    resolved = dict(direct)
    missing: list[str] = []
    for name, annotation in _dependency_hints(interceptor_cls).items():
        if name in resolved:
            continue
        value, found = _resolve_from_provide_stack(
            interceptor_cls, name, annotation, provide_stack
        )
        if found:
            resolved[name] = value
        elif not _has_class_default(interceptor_cls, name):
            missing.append(name)
    if missing:
        raise MissingDependencyError(
            f"{interceptor_cls.__name__} is missing required "
            f"dependencies: {', '.join(sorted(missing))}. Bind them at "
            f".use({interceptor_cls.__name__}, ...) or supply them via "
            f".provide(...) on this chain or an ancestor."
        )
    return resolved


def _resolve_from_provide_stack(
    interceptor_cls: type[Interceptor[Any, Any]],
    attr_name: str,
    annotation: object,
    provide_stack: tuple[Mapping[str, object], ...],
) -> tuple[object, bool]:
    """
    Walk providers from nearest to root. Prefer a name match (``attr_name``);
    fall back to a single type match at the same scope. A string annotation
    (unresolved forward reference) matches by name only. Raise on type
    mismatch or same-scope ambiguity. Return ``(value, True)`` on success,
    ``(None, False)`` if no provider matched.
    """

    for scope in provide_stack:
        if attr_name in scope:
            value = scope[attr_name]
            if not isinstance(annotation, str) and not _is_assignable(
                type(value), annotation
            ):
                raise DependencyTypeError(
                    f"{interceptor_cls.__name__}.{attr_name} expected "
                    f"{_type_name(annotation)}, got "
                    f"{type(value).__name__} from a parent provide(...)"
                )
            return value, True
        if isinstance(annotation, str):
            continue

        candidates = [
            name
            for name, value in scope.items()
            if _is_assignable(type(value), annotation)
        ]
        if len(candidates) == 1:
            return scope[candidates[0]], True
        if len(candidates) > 1:
            raise AmbiguousDependencyError(
                f"{interceptor_cls.__name__}.{attr_name} "
                f"({_type_name(annotation)}) matches multiple values "
                f"in a single provide(...) scope: {', '.join(sorted(candidates))}. "
                f"Bind explicitly with .use({interceptor_cls.__name__}, "
                f"{attr_name}=...) or rename the provide kwarg."
            )

    return None, False


def _validate_stream_chain(
    stream_chain: StreamChain[Any, Any, Any, Any],
    initial: TypeSpec | None,
    policies: dict[str, Policy],
    provide_stack: tuple[Mapping[str, object], ...] = (),
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
        processor, opener.emit_type, policies, provide_stack
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
    missing = [
        attr
        for attr in ("input_type", "output_type")
        if not _has_class_default(step_cls, attr)
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
    missing_metadata = [
        attr
        for attr in ("input_type", "emit_type", "collect_type", "output_type")
        if not _has_class_default(step_cls, attr)
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


def _validate_policy(policy: Policy | None, policies: dict[str, Policy]) -> None:
    """Every use of one policy name in a workflow must declare the same policy."""

    if policy is None or policy.name is None:
        return
    existing = policies.setdefault(policy.name, policy)
    if existing != policy:
        raise ValidationError(
            f"Policy '{policy.name}' has conflicting declarations: "
            f"{existing!r} and {policy!r}"
        )


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
            if isinstance(item.policy, AsyncPolicy) or _stream_chain_body_is_async(
                item
            ):
                return True
            continue

        step_cls = _interceptor_cls_of(item)
        if step_cls is not None and _interceptor_cls_is_async(step_cls):
            return True

    return False


def _stream_chain_body_is_async(
    stream_chain: StreamChain[Any, Any, Any, Any],
) -> bool:
    opener = stream_chain.opener
    if opener is None or stream_chain.processor is None:
        return False
    if any(
        _is_async_callable(getattr(opener, method))
        for method in ("stream", "collect", "error")
    ):
        return True
    return _chain_is_async(stream_chain.processor)


def _interceptor_cls_is_async(step_cls: InterceptorCls[Any, Any]) -> bool:
    return (
        _is_async_callable(step_cls.enter)
        or _is_async_callable(step_cls.leave)
        or _is_async_callable(step_cls.error)
    )
