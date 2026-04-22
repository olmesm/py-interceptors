from __future__ import annotations

import asyncio
import contextvars
import inspect
import threading
import time
from collections.abc import AsyncIterable, Awaitable, Callable, Coroutine, Iterable
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self, cast

from py_interceptors.chains import Chain, StreamChain
from py_interceptors.errors import CompilationError, ExecutionError, ValidationError
from py_interceptors.interceptors import Interceptor
from py_interceptors.plan import CompiledPlan
from py_interceptors.policies import (
    AsyncPolicy,
    Policy,
    ThreadPolicy,
    ThreadPoolPolicy,
)
from py_interceptors.types import TypeSpec
from py_interceptors.validation import (
    _chain_body_is_async,
    _chain_is_async,
    _stream_chain_body_is_async,
    _validate_chain,
)

type PolicyKey = tuple[type[object], str]
type _CompileCacheKey = tuple[Chain[Any, Any], TypeSpec | None]


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    execution_id: int
    chain: str
    path: tuple[str, ...]
    stage: str
    step: str
    policy: str | None
    thread: str
    elapsed_ms: float
    error: Exception | None = None


type Observer = Callable[[ExecutionEvent], None]


@dataclass(slots=True)
class _AsyncPortalRunner:
    name: str
    _ready: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread = field(init=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name=self.name,
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

    def submit[TResult](
        self,
        awaitable: Coroutine[Any, Any, TResult],
    ) -> Future[TResult]:
        self._ready.wait()
        if self._loop is None:
            raise ExecutionError(f"Async portal '{self.name}' is not running")
        return asyncio.run_coroutine_threadsafe(awaitable, self._loop)

    def shutdown(self, wait: bool) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if wait and threading.current_thread() is not self._thread:
            self._thread.join()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()


@dataclass(slots=True)
class Runtime:
    _thread_lanes: dict[str, ThreadPoolExecutor] = field(default_factory=dict)
    _thread_pools: dict[str, ThreadPoolExecutor] = field(default_factory=dict)
    _async_portals: dict[str, _AsyncPortalRunner] = field(default_factory=dict)
    _compiled_plans: dict[_CompileCacheKey, CompiledPlan[Any, Any]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _compile_lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )
    _observers: list[Observer] = field(default_factory=list, init=False, repr=False)
    _next_execution_id: int = field(default=0, init=False, repr=False)
    _execution_id_lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )
    _policy_key_var: contextvars.ContextVar[PolicyKey | None] = field(
        default_factory=lambda: contextvars.ContextVar(
            "py_interceptors_policy_key",
            default=None,
        ),
        init=False,
        repr=False,
    )
    _execution_id_var: contextvars.ContextVar[int | None] = field(
        default_factory=lambda: contextvars.ContextVar(
            "py_interceptors_execution_id",
            default=None,
        ),
        init=False,
        repr=False,
    )
    _path_var: contextvars.ContextVar[tuple[str, ...]] = field(
        default_factory=lambda: contextvars.ContextVar(
            "py_interceptors_path",
            default=(),
        ),
        init=False,
        repr=False,
    )

    def validate(
        self, chain: Chain[Any, Any], initial: TypeSpec | None = None
    ) -> TypeSpec:
        return _validate_chain(chain, initial)

    def add_observer(self, observer: Observer) -> Self:
        self._observers.append(observer)
        return self

    async def startup(self) -> None:
        """
        Application lifecycle hook.

        Runtime resources are still created lazily during execution. This hook
        exists so framework integrations can use one consistent startup/shutdown
        shape today, and so future eager resource preparation has a stable home.
        """

    async def shutdown_async(self, wait: bool = True) -> None:
        await asyncio.to_thread(self.shutdown, wait)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.shutdown()

    async def __aenter__(self) -> Self:
        await self.startup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.shutdown_async()

    def compile[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        *,
        initial: TypeSpec | None = None,
    ) -> CompiledPlan[TIn, TOut]:
        cache_key = self._compile_cache_key(chain, initial)
        if cache_key is None:
            return self._compile_uncached(chain, initial)

        with self._compile_lock:
            cached = self._compiled_plans.get(cache_key)
            if cached is not None:
                return cast(CompiledPlan[TIn, TOut], cached)

            compiled = self._compile_uncached(chain, initial)
            self._compiled_plans[cache_key] = compiled
            return compiled

    def _compile_uncached[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        initial: TypeSpec | None,
    ) -> CompiledPlan[TIn, TOut]:
        try:
            final_output = _validate_chain(chain, initial)
        except ValidationError:
            raise
        except Exception as err:
            raise CompilationError("Failed to compile chain") from err

        root_input = initial if initial is not None else chain.input_spec
        async_required = _chain_is_async(chain)
        return CompiledPlan(
            runtime=self,
            root=chain,
            input_spec=root_input,
            output_spec=final_output,
            is_async=async_required,
        )

    @staticmethod
    def _compile_cache_key(
        chain: Chain[Any, Any],
        initial: TypeSpec | None,
    ) -> _CompileCacheKey | None:
        key = (chain, initial)
        try:
            hash(key)
        except TypeError:
            return None
        return key

    async def run_async[TIn, TOut](
        self, chain: Chain[TIn, TOut], payload: TIn
    ) -> TOut:
        plan = self.compile(chain, initial=type(payload))
        return await plan.run_async(payload)

    def run_sync[TIn, TOut](self, chain: Chain[TIn, TOut], payload: TIn) -> TOut:
        plan = self.compile(chain, initial=type(payload))
        return plan.run_sync(payload)

    def get_executor(self, policy: ThreadPolicy | ThreadPoolPolicy) -> Executor:
        if isinstance(policy, ThreadPolicy):
            executor = self._thread_lanes.get(policy.name)
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix=policy.name
                )
                self._thread_lanes[policy.name] = executor
            return executor

        executor = self._thread_pools.get(policy.name)
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=policy.workers,
                thread_name_prefix=policy.name,
            )
            self._thread_pools[policy.name] = executor
        return executor

    def get_async_portal(self, policy: AsyncPolicy) -> _AsyncPortalRunner:
        if policy.name is None:
            raise ExecutionError("Isolated AsyncPolicy requires a named portal")

        portal = self._async_portals.get(policy.name)
        if portal is None:
            portal = _AsyncPortalRunner(policy.name)
            self._async_portals[policy.name] = portal
        return portal

    def shutdown(self, wait: bool = True) -> None:
        for portal in self._async_portals.values():
            portal.shutdown(wait=wait)
        for executor in self._thread_lanes.values():
            executor.shutdown(wait=wait)
        for executor in self._thread_pools.values():
            executor.shutdown(wait=wait)
        self._async_portals.clear()
        self._thread_lanes.clear()
        self._thread_pools.clear()
        with self._compile_lock:
            self._compiled_plans.clear()

    def _run_chain_sync[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
        inherited_policy: Policy | None,
        path: tuple[str, ...] | None = None,
    ) -> TOut:
        if self._execution_id_var.get() is None:
            root_path = (chain.name,)
            return self._run_root_sync(
                root_path,
                lambda: self._run_chain_sync(
                    chain,
                    payload,
                    inherited_policy,
                    root_path,
                ),
            )

        active_path = path if path is not None else (*self._path_var.get(), chain.name)
        path_token = self._path_var.set(active_path)
        try:
            return self._run_chain_sync_at_path(chain, payload, inherited_policy)
        finally:
            self._path_var.reset(path_token)

    def _run_chain_sync_at_path[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
        inherited_policy: Policy | None,
    ) -> TOut:
        effective_policy = chain.policy or inherited_policy

        if isinstance(effective_policy, AsyncPolicy):
            raise ExecutionError(
                f"Chain '{chain.name}' requires async execution"
            )

        if isinstance(effective_policy, ThreadPolicy | ThreadPoolPolicy):
            if not self._is_current_policy(effective_policy):
                return self._run_with_thread_policy_sync(
                    effective_policy,
                    lambda: self._run_chain_body_sync(
                        chain,
                        payload,
                        effective_policy,
                    ),
                )

        return self._run_chain_body_sync(chain, payload, effective_policy)

    def _run_chain_body_sync[TOut](
        self,
        chain: Chain[Any, TOut],
        payload: Any,
        effective_policy: Policy | None,
    ) -> TOut:
        current = payload
        entered: list[tuple[Interceptor[Any, Any], Policy | None]] = []
        pending_error: Exception | None = None

        for item in chain.items:
            if pending_error is not None:
                break

            if isinstance(item, type) and issubclass(item, Interceptor):
                interceptor = self._instantiate(item)
                try:
                    current = self._run_interceptor_stage_sync(
                        chain.name,
                        effective_policy,
                        interceptor.enter,
                        interceptor,
                        "enter",
                        current,
                    )
                    entered.append((interceptor, effective_policy))
                except Exception as err:
                    pending_error = err
                    break
                continue

            try:
                current = self._run_item_sync(item, current, effective_policy)
            except Exception as err:
                pending_error = err

        for interceptor, step_policy in reversed(entered):
            if pending_error is None:
                try:
                    current = self._run_interceptor_stage_sync(
                        chain.name,
                        step_policy,
                        interceptor.leave,
                        interceptor,
                        "leave",
                        current,
                    )
                except Exception as err:
                    pending_error = err
            else:
                try:
                    current = self._run_interceptor_stage_sync(
                        chain.name,
                        step_policy,
                        interceptor.error,
                        interceptor,
                        "error",
                        current,
                        pending_error,
                    )
                    pending_error = None
                except Exception as err:
                    pending_error = err

        if pending_error is not None:
            raise pending_error

        return cast(TOut, current)

    def _run_item_sync(
        self,
        item: object,
        payload: Any,
        inherited_policy: Policy | None,
    ) -> Any:
        if isinstance(item, Chain):
            return self._run_chain_sync(
                item,
                payload,
                inherited_policy,
                (*self._path_var.get(), item.name),
            )
        if isinstance(item, StreamChain):
            return self._run_stream_chain_sync(
                item,
                payload,
                inherited_policy,
                (*self._path_var.get(), item.name),
            )
        raise ExecutionError(f"Unsupported chain item: {item!r}")

    def _run_stream_chain_sync(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        inherited_policy: Policy | None,
        path: tuple[str, ...] | None = None,
    ) -> Any:
        if self._execution_id_var.get() is None:
            root_path = (stream_chain.name,)
            return self._run_root_sync(
                root_path,
                lambda: self._run_stream_chain_sync(
                    stream_chain,
                    payload,
                    inherited_policy,
                    root_path,
                ),
            )

        active_path = (
            path if path is not None else (*self._path_var.get(), stream_chain.name)
        )
        path_token = self._path_var.set(active_path)
        try:
            return self._run_stream_chain_sync_at_path(
                stream_chain,
                payload,
                inherited_policy,
            )
        finally:
            self._path_var.reset(path_token)

    def _run_stream_chain_sync_at_path(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        inherited_policy: Policy | None,
    ) -> Any:
        effective_policy = stream_chain.policy or inherited_policy

        if isinstance(effective_policy, AsyncPolicy):
            raise ExecutionError(
                f"StreamChain '{stream_chain.name}' requires async execution"
            )

        if isinstance(effective_policy, ThreadPolicy | ThreadPoolPolicy):
            if not self._is_current_policy(effective_policy):
                return self._run_with_thread_policy_sync(
                    effective_policy,
                    lambda: self._run_stream_chain_body_sync(
                        stream_chain,
                        payload,
                        effective_policy,
                    ),
                )

        return self._run_stream_chain_body_sync(
            stream_chain,
            payload,
            effective_policy,
        )

    def _run_stream_chain_body_sync(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        effective_policy: Policy | None,
    ) -> Any:
        if stream_chain.opener is None:
            raise ExecutionError("StreamChain is missing stream(...)")
        if stream_chain.processor is None:
            raise ExecutionError("StreamChain is missing map(...)")

        opener = self._instantiate(stream_chain.opener)

        try:
            emitted_items = self._run_stream_interceptor_stage_sync(
                stream_chain.name,
                effective_policy,
                opener.stream,
                opener,
                "stream",
                payload,
            )
        except Exception as err:
            return self._run_stream_interceptor_stage_sync(
                stream_chain.name,
                effective_policy,
                opener.error,
                opener,
                "error",
                payload,
                err,
            )

        try:
            collected = self._map_stream_items_sync(
                stream_chain.processor,
                emitted_items,
                effective_policy,
            )
            return self._run_stream_interceptor_stage_sync(
                stream_chain.name,
                effective_policy,
                opener.collect,
                opener,
                "collect",
                payload,
                collected,
            )
        except Exception as err:
            return self._run_stream_interceptor_stage_sync(
                stream_chain.name,
                effective_policy,
                opener.error,
                opener,
                "error",
                payload,
                err,
            )

    async def _run_chain_async[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
        inherited_policy: Policy | None,
        path: tuple[str, ...] | None = None,
    ) -> TOut:
        if self._execution_id_var.get() is None:
            root_path = (chain.name,)
            return await self._run_root_async(
                root_path,
                lambda: self._run_chain_async(
                    chain,
                    payload,
                    inherited_policy,
                    root_path,
                ),
            )

        active_path = path if path is not None else (*self._path_var.get(), chain.name)
        path_token = self._path_var.set(active_path)
        try:
            return await self._run_chain_async_at_path(
                chain,
                payload,
                inherited_policy,
            )
        finally:
            self._path_var.reset(path_token)

    async def _run_chain_async_at_path[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
        inherited_policy: Policy | None,
    ) -> TOut:
        effective_policy = chain.policy or inherited_policy

        if isinstance(effective_policy, ThreadPolicy | ThreadPoolPolicy):
            if _chain_body_is_async(chain):
                return await self._run_chain_body_async(
                    chain,
                    payload,
                    effective_policy,
                )
            return await self._run_sync_in_executor(
                effective_policy,
                lambda: self._run_chain_body_sync(
                    chain,
                    payload,
                    effective_policy,
                ),
            )

        return await self._run_chain_body_async(chain, payload, effective_policy)

    async def _run_chain_body_async[TOut](
        self,
        chain: Chain[Any, TOut],
        payload: Any,
        effective_policy: Policy | None,
    ) -> TOut:
        current: Any = payload
        entered: list[tuple[Interceptor[Any, Any], Policy | None]] = []
        pending_error: Exception | None = None

        for item in chain.items:
            if pending_error is not None:
                break

            if isinstance(item, type) and issubclass(item, Interceptor):
                interceptor = self._instantiate(item)
                try:
                    current = await self._run_interceptor_stage_async(
                        chain.name,
                        effective_policy,
                        interceptor.enter,
                        interceptor,
                        "enter",
                        current,
                    )
                    entered.append((interceptor, effective_policy))
                except Exception as err:
                    pending_error = err
                    break
                continue

            try:
                current = await self._run_item_async(item, current, effective_policy)
            except Exception as err:
                pending_error = err

        for interceptor, step_policy in reversed(entered):
            if pending_error is None:
                try:
                    current = await self._run_interceptor_stage_async(
                        chain.name,
                        step_policy,
                        interceptor.leave,
                        interceptor,
                        "leave",
                        current,
                    )
                except Exception as err:
                    pending_error = err
            else:
                try:
                    current = await self._run_interceptor_stage_async(
                        chain.name,
                        step_policy,
                        interceptor.error,
                        interceptor,
                        "error",
                        current,
                        pending_error,
                    )
                    pending_error = None
                except Exception as err:
                    pending_error = err

        if pending_error is not None:
            raise pending_error

        return cast(TOut, current)

    async def _run_item_async(
        self,
        item: object,
        payload: Any,
        inherited_policy: Policy | None,
    ) -> Any:
        if isinstance(item, Chain):
            return await self._run_chain_async(
                item,
                payload,
                inherited_policy,
                (*self._path_var.get(), item.name),
            )
        if isinstance(item, StreamChain):
            return await self._run_stream_chain_async(
                item,
                payload,
                inherited_policy,
                (*self._path_var.get(), item.name),
            )
        raise ExecutionError(f"Unsupported chain item: {item!r}")

    async def _run_stream_chain_async(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        inherited_policy: Policy | None,
        path: tuple[str, ...] | None = None,
    ) -> Any:
        if self._execution_id_var.get() is None:
            root_path = (stream_chain.name,)
            return await self._run_root_async(
                root_path,
                lambda: self._run_stream_chain_async(
                    stream_chain,
                    payload,
                    inherited_policy,
                    root_path,
                ),
            )

        active_path = (
            path if path is not None else (*self._path_var.get(), stream_chain.name)
        )
        path_token = self._path_var.set(active_path)
        try:
            return await self._run_stream_chain_async_at_path(
                stream_chain,
                payload,
                inherited_policy,
            )
        finally:
            self._path_var.reset(path_token)

    async def _run_stream_chain_async_at_path(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        inherited_policy: Policy | None,
    ) -> Any:
        effective_policy = stream_chain.policy or inherited_policy

        if isinstance(effective_policy, ThreadPolicy | ThreadPoolPolicy):
            if _stream_chain_body_is_async(stream_chain):
                return await self._run_stream_chain_body_async(
                    stream_chain,
                    payload,
                    effective_policy,
                )
            return await self._run_sync_in_executor(
                effective_policy,
                lambda: self._run_stream_chain_body_sync(
                    stream_chain,
                    payload,
                    effective_policy,
                ),
            )

        return await self._run_stream_chain_body_async(
            stream_chain,
            payload,
            effective_policy,
        )

    async def _run_stream_chain_body_async(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        effective_policy: Policy | None,
    ) -> Any:
        if stream_chain.opener is None:
            raise ExecutionError("StreamChain is missing stream(...)")
        if stream_chain.processor is None:
            raise ExecutionError("StreamChain is missing map(...)")

        opener = self._instantiate(stream_chain.opener)

        try:
            emitted_items = await self._run_stream_interceptor_stage_async(
                stream_chain.name,
                effective_policy,
                opener.stream,
                opener,
                "stream",
                payload,
            )
        except Exception as err:
            return await self._run_stream_interceptor_stage_async(
                stream_chain.name,
                effective_policy,
                opener.error,
                opener,
                "error",
                payload,
                err,
            )

        try:
            collected = await self._map_stream_items_async(
                stream_chain.processor,
                emitted_items,
                effective_policy,
            )
            return await self._run_stream_interceptor_stage_async(
                stream_chain.name,
                effective_policy,
                opener.collect,
                opener,
                "collect",
                payload,
                collected,
            )
        except Exception as err:
            return await self._run_stream_interceptor_stage_async(
                stream_chain.name,
                effective_policy,
                opener.error,
                opener,
                "error",
                payload,
                err,
            )

    def _map_stream_items_sync(
        self,
        processor: Chain[Any, Any],
        emitted_items: list[Any],
        inherited_policy: Policy | None,
    ) -> list[Any]:
        parallelism = self._map_parallelism_sync(processor, inherited_policy)
        processor_path = (*self._path_var.get(), processor.name)

        if parallelism <= 1:
            return [
                self._run_chain_sync(processor, item, inherited_policy, processor_path)
                for item in emitted_items
            ]

        policy = processor.policy or inherited_policy
        if not isinstance(policy, ThreadPoolPolicy):
            return [
                self._run_chain_sync(processor, item, inherited_policy, processor_path)
                for item in emitted_items
            ]

        executor = self.get_executor(policy)
        futures: list[Future[Any]] = [
            executor.submit(
                contextvars.copy_context().run,
                self._call_with_policy_key,
                self._policy_key(policy),
                lambda item=item: self._run_chain_sync(
                    processor,
                    item,
                    inherited_policy,
                    processor_path,
                ),
            )
            for item in emitted_items
        ]
        return [future.result() for future in futures]

    async def _map_stream_items_async(
        self,
        processor: Chain[Any, Any],
        emitted_items: list[Any],
        inherited_policy: Policy | None,
    ) -> list[Any]:
        parallelism = self._map_parallelism_async(processor, inherited_policy)
        processor_path = (*self._path_var.get(), processor.name)

        async def run_one(item: Any) -> Any:
            return await self._run_chain_async(
                processor,
                item,
                inherited_policy,
                processor_path,
            )

        if parallelism == 1:
            return [await run_one(item) for item in emitted_items]
        if parallelism is None:
            return list(
                await asyncio.gather(*(run_one(item) for item in emitted_items))
            )

        semaphore = asyncio.Semaphore(parallelism)

        async def bounded(item: Any) -> Any:
            async with semaphore:
                return await run_one(item)

        return list(await asyncio.gather(*(bounded(item) for item in emitted_items)))

    def _map_parallelism_sync(
        self,
        processor: Chain[Any, Any],
        inherited_policy: Policy | None,
    ) -> int:
        effective_policy = processor.policy or inherited_policy
        if isinstance(effective_policy, ThreadPoolPolicy):
            if self._is_current_policy(effective_policy):
                return 1
            return effective_policy.workers
        return 1

    def _map_parallelism_async(
        self,
        processor: Chain[Any, Any],
        inherited_policy: Policy | None,
    ) -> int | None:
        effective_policy = processor.policy or inherited_policy
        if isinstance(effective_policy, ThreadPolicy):
            return 1
        if isinstance(effective_policy, ThreadPoolPolicy):
            return effective_policy.workers
        if isinstance(effective_policy, AsyncPolicy):
            return None
        if _chain_is_async(processor):
            return None
        return 1

    def _run_interceptor_stage_sync[TResult](
        self,
        chain_name: str,
        policy: Policy | None,
        fn: Callable[..., TResult],
        interceptor: Interceptor[Any, Any],
        stage: str,
        *args: object,
    ) -> TResult:
        return self._run_observed_stage_sync(
            chain_name,
            self._step_name(type(interceptor)),
            stage,
            policy,
            lambda *inner_args: self._call_sync(fn, *inner_args),
            *args,
        )

    async def _run_interceptor_stage_async[TResult](
        self,
        chain_name: str,
        policy: Policy | None,
        fn: Callable[..., TResult],
        interceptor: Interceptor[Any, Any],
        stage: str,
        *args: object,
    ) -> TResult:
        return await self._run_observed_stage_async(
            chain_name,
            self._step_name(type(interceptor)),
            stage,
            policy,
            lambda *inner_args: self._call_async(fn, *inner_args),
            *args,
        )

    def _run_stream_interceptor_stage_sync[TResult](
        self,
        chain_name: str,
        policy: Policy | None,
        fn: Callable[..., TResult],
        interceptor: object,
        stage: str,
        *args: object,
    ) -> Any:
        if stage == "stream":
            def operation(*inner_args: object) -> Any:
                return self._materialize_stream_result_sync(fn, *inner_args)
        else:
            def operation(*inner_args: object) -> Any:
                return self._call_sync(fn, *inner_args)

        return self._run_observed_stage_sync(
            chain_name,
            self._step_name(type(interceptor)),
            stage,
            policy,
            operation,
            *args,
        )

    async def _run_stream_interceptor_stage_async[TResult](
        self,
        chain_name: str,
        policy: Policy | None,
        fn: Callable[..., TResult],
        interceptor: object,
        stage: str,
        *args: object,
    ) -> Any:
        if stage == "stream":
            async def operation(*inner_args: object) -> Any:
                return await self._materialize_stream_result_async(fn, *inner_args)
        else:
            async def operation(*inner_args: object) -> Any:
                return await self._call_async(fn, *inner_args)

        return await self._run_observed_stage_async(
            chain_name,
            self._step_name(type(interceptor)),
            stage,
            policy,
            operation,
            *args,
        )

    def _run_observed_stage_sync[TResult](
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
        operation: Callable[..., TResult],
        *args: object,
    ) -> TResult:
        def observed(*inner_args: object) -> TResult:
            start = time.perf_counter()
            try:
                result = operation(*inner_args)
            except Exception as err:
                self._emit_failed_stage(
                    chain_name,
                    step_name,
                    stage,
                    policy,
                    start,
                    err,
                )
                raise

            self._emit_completed_stage(
                chain_name,
                step_name,
                stage,
                policy,
                start,
            )
            return result

        return self._apply_policy_sync(policy, observed, *args)

    async def _run_observed_stage_async[TResult](
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
        operation: Callable[..., Awaitable[TResult]],
        *args: object,
    ) -> TResult:
        async def observed(*inner_args: object) -> TResult:
            start = time.perf_counter()
            try:
                result = await operation(*inner_args)
            except Exception as err:
                self._emit_failed_stage(
                    chain_name,
                    step_name,
                    stage,
                    policy,
                    start,
                    err,
                )
                raise

            self._emit_completed_stage(
                chain_name,
                step_name,
                stage,
                policy,
                start,
            )
            return result

        return await self._apply_policy_async(
            policy,
            cast(Callable[..., TResult], observed),
            *args,
        )

    def _emit_failed_stage(
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
        start: float,
        error: Exception,
    ) -> None:
        self._annotate_exception(chain_name, step_name, stage, policy, error)
        self._emit_event(chain_name, step_name, stage, policy, start, error)

    def _emit_completed_stage(
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
        start: float,
    ) -> None:
        self._emit_event(chain_name, step_name, stage, policy, start, None)

    def _emit_event(
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
        start: float,
        error: Exception | None,
    ) -> None:
        if not self._observers:
            return

        execution_id = self._execution_id_var.get()
        if execution_id is None:
            execution_id = self._next_id()

        event = ExecutionEvent(
            execution_id=execution_id,
            chain=chain_name,
            path=self._path_var.get(),
            stage=stage,
            step=step_name,
            policy=self._policy_label(policy),
            thread=threading.current_thread().name,
            elapsed_ms=(time.perf_counter() - start) * 1000,
            error=error,
        )
        for observer in tuple(self._observers):
            observer(event)

    def _annotate_exception(
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
        error: Exception,
    ) -> None:
        error.add_note(
            "py_interceptors "
            f"chain={chain_name!r} "
            f"path={self._path_var.get()!r} "
            f"step={step_name!r} "
            f"stage={stage!r} "
            f"policy={self._policy_label(policy)!r}"
        )

    def _run_root_sync[TResult](
        self,
        path: tuple[str, ...],
        call: Callable[[], TResult],
    ) -> TResult:
        execution_token = self._execution_id_var.set(self._next_id())
        path_token = self._path_var.set(path)
        try:
            return call()
        finally:
            self._path_var.reset(path_token)
            self._execution_id_var.reset(execution_token)

    async def _run_root_async[TResult](
        self,
        path: tuple[str, ...],
        call: Callable[[], Awaitable[TResult]],
    ) -> TResult:
        execution_token = self._execution_id_var.set(self._next_id())
        path_token = self._path_var.set(path)
        try:
            return await call()
        finally:
            self._path_var.reset(path_token)
            self._execution_id_var.reset(execution_token)

    def _next_id(self) -> int:
        with self._execution_id_lock:
            self._next_execution_id += 1
            return self._next_execution_id

    @staticmethod
    def _step_name(step_cls: type[object]) -> str:
        name = getattr(step_cls, "name", None)
        if isinstance(name, str) and name:
            return name
        return step_cls.__name__

    @staticmethod
    def _policy_label(policy: Policy | None) -> str | None:
        if policy is None:
            return None
        if isinstance(policy, ThreadPolicy):
            return f"ThreadPolicy({policy.name!r})"
        if isinstance(policy, ThreadPoolPolicy):
            return f"ThreadPoolPolicy({policy.name!r}, workers={policy.workers})"
        if policy.name is None:
            return "AsyncPolicy()"
        if policy.isolated:
            return f"AsyncPolicy({policy.name!r}, isolated=True)"
        return f"AsyncPolicy({policy.name!r})"

    def _apply_policy_sync[TResult](
        self,
        policy: Policy | None,
        fn: Callable[..., TResult],
        *args: object,
    ) -> TResult:
        if policy is None:
            return self._call_sync(fn, *args)
        if isinstance(policy, AsyncPolicy):
            raise ExecutionError("Cannot use AsyncPolicy in sync execution")
        if self._is_current_policy(policy):
            return self._call_sync(fn, *args)

        return self._run_with_thread_policy_sync(
            policy,
            lambda: self._call_sync(fn, *args),
        )

    async def _apply_policy_async[TResult](
        self,
        policy: Policy | None,
        fn: Callable[..., TResult],
        *args: object,
    ) -> TResult:
        if policy is None:
            return await self._call_async(fn, *args)
        if isinstance(policy, AsyncPolicy):
            if policy.isolated:
                return await self._run_in_async_policy(
                    policy,
                    lambda: self._call_async(fn, *args),
                )
            return await self._call_async(fn, *args)

        return await self._run_sync_in_executor(
            policy,
            lambda: self._invoke_in_sync_context(fn, *args),
        )

    def _materialize_stream_result_sync(
        self,
        fn: Callable[..., object],
        *args: object,
    ) -> list[Any]:
        result = self._call_sync(fn, *args)
        if isinstance(result, AsyncIterable):
            raise ExecutionError(
                "StreamInterceptor.stream returned an async iterable in sync mode"
            )
        return list(self._require_iterable(result))

    async def _materialize_stream_result_async(
        self,
        fn: Callable[..., object],
        *args: object,
    ) -> list[Any]:
        result = fn(*args)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, AsyncIterable):
            return [item async for item in result]
        return list(self._require_iterable(result))

    def _run_with_thread_policy_sync[TResult](
        self,
        policy: ThreadPolicy | ThreadPoolPolicy,
        call: Callable[[], TResult],
    ) -> TResult:
        key = self._policy_key(policy)
        if self._is_current_policy(policy):
            return call()

        executor = self.get_executor(policy)
        context = contextvars.copy_context()
        future = executor.submit(context.run, self._call_with_policy_key, key, call)
        return future.result()

    async def _run_sync_in_executor[TResult](
        self,
        policy: ThreadPolicy | ThreadPoolPolicy,
        call: Callable[[], TResult],
    ) -> TResult:
        key = self._policy_key(policy)

        if self._current_policy_key() == key:
            return call()

        loop = asyncio.get_running_loop()
        executor = self.get_executor(policy)
        context = contextvars.copy_context()
        return await loop.run_in_executor(
            executor,
            lambda: context.run(self._call_with_policy_key, key, call),
        )

    async def _run_in_async_policy[TResult](
        self,
        policy: AsyncPolicy,
        call: Callable[[], Awaitable[TResult]],
    ) -> TResult:
        key = self._policy_key(policy)
        if self._current_policy_key() == key:
            return await call()

        portal = self.get_async_portal(policy)
        future = portal.submit(
            self._call_async_with_policy_key(
                key,
                call,
                self._execution_id_var.get(),
                self._path_var.get(),
            )
        )
        return await asyncio.wrap_future(future)

    def _call_with_policy_key[TResult](
        self,
        key: PolicyKey,
        call: Callable[[], TResult],
    ) -> TResult:
        token = self._policy_key_var.set(key)
        try:
            return call()
        finally:
            self._policy_key_var.reset(token)

    async def _call_async_with_policy_key[TResult](
        self,
        key: PolicyKey,
        call: Callable[[], Awaitable[TResult]],
        execution_id: int | None,
        path: tuple[str, ...],
    ) -> TResult:
        policy_token = self._policy_key_var.set(key)
        execution_token = self._execution_id_var.set(execution_id)
        path_token = self._path_var.set(path)
        try:
            return await call()
        finally:
            self._path_var.reset(path_token)
            self._execution_id_var.reset(execution_token)
            self._policy_key_var.reset(policy_token)

    def _current_policy_key(self) -> PolicyKey | None:
        return self._policy_key_var.get()

    def _is_current_policy(
        self,
        policy: ThreadPolicy | ThreadPoolPolicy | AsyncPolicy,
    ) -> bool:
        return self._current_policy_key() == self._policy_key(policy)

    @staticmethod
    def _policy_key(
        policy: ThreadPolicy | ThreadPoolPolicy | AsyncPolicy,
    ) -> PolicyKey:
        if isinstance(policy, AsyncPolicy):
            if policy.name is None:
                raise ExecutionError("Unnamed AsyncPolicy has no portal key")
            return (type(policy), policy.name)
        return (type(policy), policy.name)

    @staticmethod
    def _call_sync[TResult](fn: Callable[..., TResult], *args: object) -> TResult:
        result = fn(*args)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise ExecutionError("Async result produced during sync execution")
        return result

    @staticmethod
    def _invoke_in_sync_context[TResult](
        fn: Callable[..., TResult],
        *args: object,
    ) -> TResult:
        result = fn(*args)
        if inspect.isawaitable(result):
            return cast(
                TResult,
                asyncio.run(Runtime._await_any(cast(Awaitable[Any], result))),
            )
        return result

    @staticmethod
    async def _await_any(awaitable: Awaitable[Any]) -> Any:
        return await awaitable

    @staticmethod
    async def _call_async[TResult](
        fn: Callable[..., TResult],
        *args: object,
    ) -> TResult:
        result = fn(*args)
        if inspect.isawaitable(result):
            return cast(TResult, await result)
        return result

    @staticmethod
    def _require_iterable(value: object) -> Iterable[Any]:
        if not isinstance(value, Iterable):
            raise ExecutionError("StreamInterceptor.stream must return an iterable")
        return value

    @staticmethod
    def _instantiate[TResult](cls: type[TResult]) -> TResult:
        try:
            return cls()
        except Exception as err:
            raise ExecutionError(f"Could not instantiate {cls.__name__}") from err
