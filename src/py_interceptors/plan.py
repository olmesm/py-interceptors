from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING

from py_interceptors.chains import Chain
from py_interceptors.errors import ExecutionError
from py_interceptors.types import TypeSpec
from py_interceptors.validation import _is_assignable, _type_name

if TYPE_CHECKING:
    from py_interceptors.runtime import Runtime


@dataclass(frozen=True, slots=True)
class CompiledPlan[TIn, TOut]:
    """
    Validated executable workflow plan.

    Plans are produced by ``Runtime.compile(...)`` and keep the validated root
    chain, input/output type specs, whether async execution is required, and
    the resolved dependency map for every interceptor in the chain tree.
    """

    runtime: Runtime
    root: Chain[TIn, TOut]
    input_spec: TypeSpec
    output_spec: TypeSpec
    is_async: bool
    resolution: Mapping[int, Mapping[str, object]] = field(
        default_factory=lambda: MappingProxyType({}),
    )

    def run_sync(self, payload: TIn) -> TOut:
        """Run this plan synchronously when it contains no async work."""
        if self.is_async:
            raise ExecutionError(
                "This plan contains async segments or async steps; use run_async(...)"
            )
        self._validate_payload(payload)
        with self.runtime._active_resolution(self.resolution):
            return self.runtime._run_chain_sync(self.root, payload, None)

    async def run_async(self, payload: TIn) -> TOut:
        """Run this plan asynchronously."""
        self._validate_payload(payload)
        with self.runtime._active_resolution(self.resolution):
            return await self.runtime._run_chain_async(self.root, payload, None)

    def _validate_payload(self, payload: object) -> None:
        if not _is_assignable(type(payload), self.input_spec):
            raise ExecutionError(
                f"Plan expects {_type_name(self.input_spec)} but received "
                f"{_type_name(type(payload))}"
            )
