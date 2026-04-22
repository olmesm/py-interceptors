from __future__ import annotations

import inspect
from typing import Any, get_args, get_origin

from py_interceptors.chains import (
    Chain,
    StreamChain,
    _item_input_spec,
    _item_output_spec,
)
from py_interceptors.errors import ValidationError
from py_interceptors.interceptors import (
    Interceptor,
    InterceptorCls,
    StreamInterceptor,
)
from py_interceptors.policies import AsyncPolicy, Policy, ThreadPolicy, ThreadPoolPolicy
from py_interceptors.types import TypeSpec


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
) -> TypeSpec:
    seen_policies = policies if policies is not None else {}
    _validate_policy(chain.policy, seen_policies)

    current: TypeSpec | None = initial

    if not chain.items:
        return current if current is not None else object

    for idx, item in enumerate(chain.items):
        if isinstance(item, type) and issubclass(item, Interceptor):
            _validate_interceptor_cls(item)

        required = _item_input_spec(item)

        if current is None:
            current = required
        elif not _is_assignable(current, required):
            raise ValidationError(
                f"Chain '{chain.name}' item #{idx + 1} expects "
                f"{_type_name(required)} but received {_type_name(current)}"
            )

        if isinstance(item, Chain):
            current = _validate_chain(item, current, seen_policies)
        elif isinstance(item, StreamChain):
            current = _validate_stream_chain(item, current, seen_policies)
        else:
            current = _item_output_spec(item)

    return current if current is not None else object


def _validate_stream_chain(
    stream_chain: StreamChain[Any, Any, Any, Any],
    initial: TypeSpec | None,
    policies: dict[str, _PolicySignature],
) -> TypeSpec:
    _validate_policy(stream_chain.policy, policies)

    if stream_chain.opener is None:
        raise ValidationError(
            f"StreamChain '{stream_chain.name}' is missing stream(...)"
        )
    if stream_chain.processor is None:
        raise ValidationError(
            f"StreamChain '{stream_chain.name}' is missing map(...)"
        )

    opener = stream_chain.opener
    processor = stream_chain.processor
    _validate_stream_interceptor_cls(opener)

    current = initial if initial is not None else opener.input_type
    if not _is_assignable(current, opener.input_type):
        raise ValidationError(
            f"StreamChain '{stream_chain.name}' expects "
            f"{_type_name(opener.input_type)} but received {_type_name(current)}"
        )

    processor_out = _validate_chain(processor, opener.emit_type, policies)

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
        attr
        for attr in ("input_type", "output_type")
        if attr not in step_cls.__dict__
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
