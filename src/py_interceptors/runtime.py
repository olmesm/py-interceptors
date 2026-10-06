from __future__ import annotations

import asyncio
import contextvars
import inspect
import itertools
import threading
import time
from collections.abc import (
    AsyncIterable,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    Mapping,
)
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from types import TracebackType
from typing import Any, Self, cast

from py_interceptors.chains import BoundInterceptor, Chain, StreamChain
from py_interceptors.errors import ExecutionError
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
    resolve_step_dependencies,
)

type PolicyKey = tuple[type[object], str | None]
type _CompileCacheKey = tuple[int, TypeSpec | None]

_DEFAULT_PORTAL = "py-interceptors-default"


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """
    Observer payload emitted after each interceptor stage.

    Events include the execution id, current chain path, stage name, step name,
    effective policy label, thread name, elapsed time, and optional error.
    """

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
    _loop: asyncio.AbstractEventLoop = field(init=False)

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
        return asyncio.run_coroutine_threadsafe(awaitable, self._loop)

    def shutdown(self, wait: bool) -> None:
        if self._loop.is_running():
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
    """
    Validate, compile, and execute interceptor workflows.

    Use one ``Runtime`` per application boundary or test. The runtime owns
    compiled-plan caches, observer callbacks, thread pools, thread lanes, and
    isolated async portals.

    Example:
        >>> from py_interceptors import Interceptor, Runtime, chain
        >>>
        >>> class Increment(Interceptor[int, int]):
        ...     input_type = int
        ...     output_type = int
        ...
        ...     def enter(self, ctx: int) -> int:
        ...         return ctx + 1
        ...
        >>> workflow = chain("increment").use(Increment).build()
        >>> with Runtime() as runtime:
        ...     result = runtime.run_sync(workflow, 41)
        >>> result
        42
    """

    _executors: dict[PolicyKey, ThreadPoolExecutor] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _async_portals: dict[str, _AsyncPortalRunner] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _compiled_plans: dict[_CompileCacheKey, CompiledPlan[Any, Any]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    # Guards the plan cache and the lazy creation of executors and portals.
    _lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )
    _observers: list[Observer] = field(default_factory=list, init=False, repr=False)
    _execution_ids: Iterator[int] = field(
        default_factory=lambda: itertools.count(1),
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
    _execution_id_var: contextvars.ContextVar[int] = field(
        default_factory=lambda: contextvars.ContextVar(
            "py_interceptors_execution_id",
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
    _provide_var: contextvars.ContextVar[tuple[Mapping[str, object], ...]] = field(
        default_factory=lambda: contextvars.ContextVar(
            "py_interceptors_provides",
            default=(),
        ),
        init=False,
        repr=False,
    )

    def add_observer(self, observer: Observer) -> Self:
        """Register an observer callback and return this runtime."""
        self._observers.append(observer)
        return self

    async def shutdown_async(self, wait: bool = True) -> None:
        """Shutdown runtime-owned resources from async code."""
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
        """Validate and cache an executable plan for ``chain``."""
        cache_key: _CompileCacheKey = (id(chain), initial)
        with self._lock:
            cached = self._compiled_plans.get(cache_key)
            if cached is not None:
                return cast(CompiledPlan[TIn, TOut], cached)

            final_output = _validate_chain(chain, initial)
            compiled = CompiledPlan(
                runtime=self,
                root=chain,
                input_spec=initial if initial is not None else chain.input_spec,
                output_spec=final_output,
                is_async=_chain_is_async(chain),
            )
            self._compiled_plans[cache_key] = compiled
            return compiled

    async def run_async[TIn, TOut](self, chain: Chain[TIn, TOut], payload: TIn) -> TOut:
        """Compile if needed, then execute ``chain`` asynchronously."""
        plan = self.compile(chain, initial=type(payload))
        return await plan.run_async(payload)

    def run_sync[TIn, TOut](self, chain: Chain[TIn, TOut], payload: TIn) -> TOut:
        """Compile if needed, then execute a sync-only ``chain``."""
        plan = self.compile(chain, initial=type(payload))
        return plan.run_sync(payload)

    def run_blocking[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
    ) -> TOut:
        """
        Drive an async-capable ``chain`` from a sync caller that has no event loop.

        Intended for sync entry points that live on a worker thread, such as a
        synchronous FastAPI route or a sync framework worker. The chain runs on
        a long-lived runtime-owned event loop and the calling thread blocks on
        the result. Thread-policy segments still bounce out to their lanes via
        ``run_in_executor``; async segments run on the runtime portal loop.

        Raises ``ExecutionError`` if called from inside a running event loop.
        For async callers, use ``run_async`` instead.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise ExecutionError(
                "run_blocking cannot be called from inside a running event loop; "
                "use run_async instead"
            )

        plan = self.compile(chain, initial=type(payload))
        future = self._portal(_DEFAULT_PORTAL).submit(plan.run_async(payload))
        return future.result()

    def get_executor(self, policy: ThreadPolicy | ThreadPoolPolicy) -> Executor:
        """Return the runtime-owned executor for a thread policy."""
        key = self._policy_key(policy)
        workers = policy.workers if isinstance(policy, ThreadPoolPolicy) else 1
        with self._lock:
            executor = self._executors.get(key)
            if executor is None:
                executor = ThreadPoolExecutor(
                    max_workers=workers,
                    thread_name_prefix=policy.name,
                )
                self._executors[key] = executor
            elif executor._max_workers != workers:
                raise ExecutionError(
                    f"ThreadPoolPolicy {policy.name!r} already exists with "
                    f"workers={executor._max_workers}; got workers={workers}"
                )
            return executor

    def get_async_portal(self, policy: AsyncPolicy) -> _AsyncPortalRunner:
        """Return the runtime-owned isolated async portal for ``policy``."""
        if policy.name is None:
            raise ExecutionError("Isolated AsyncPolicy requires a named portal")
        return self._portal(policy.name)

    def _portal(self, name: str) -> _AsyncPortalRunner:
        with self._lock:
            portal = self._async_portals.get(name)
            if portal is None:
                portal = _AsyncPortalRunner(name)
                self._async_portals[name] = portal
            return portal

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown runtime-owned resources and clear compiled-plan caches."""
        for portal in self._async_portals.values():
            portal.shutdown(wait=wait)
        for executor in self._executors.values():
            executor.shutdown(wait=wait)
        self._async_portals.clear()
        self._executors.clear()
        with self._lock:
            self._compiled_plans.clear()

    @contextmanager
    def _frame(self, name: str) -> Iterator[None]:
        """Push ``name`` onto the current path; at the root, start an execution."""
        execution_token = None
        if self._execution_id_var.get(None) is None:
            execution_token = self._execution_id_var.set(next(self._execution_ids))
        path_token = self._path_var.set((*self._path_var.get(), name))
        try:
            yield
        finally:
            self._path_var.reset(path_token)
            if execution_token is not None:
                self._execution_id_var.reset(execution_token)

    def _run_chain_sync[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
        inherited_policy: Policy | None,
    ) -> TOut:
        with self._frame(chain.name), self._provide_scope(chain):
            policy = chain.policy or inherited_policy
            if isinstance(policy, AsyncPolicy):
                raise ExecutionError(f"Chain '{chain.name}' requires async execution")
            if isinstance(policy, ThreadPolicy | ThreadPoolPolicy):
                return self._run_with_thread_policy_sync(
                    policy,
                    lambda: self._run_chain_body_sync(chain, payload, policy),
                )
            return self._run_chain_body_sync(chain, payload, policy)

    def _run_chain_body_sync[TOut](
        self,
        chain: Chain[Any, TOut],
        payload: Any,
        policy: Policy | None,
    ) -> TOut:
        def stage(
            fn: Callable[..., object],
            interceptor: Interceptor[Any, Any],
            name: str,
            *args: object,
        ) -> Any:
            return self._run_stage_sync(
                chain.name, policy, fn, interceptor, name, *args
            )

        current = payload
        entered: list[Interceptor[Any, Any]] = []
        pending_error: Exception | None = None

        for item in chain.items:
            if (isinstance(item, type) and issubclass(item, Interceptor)) or (
                isinstance(item, BoundInterceptor)
            ):
                interceptor = self._instantiate_step(item)
                try:
                    current = stage(interceptor.enter, interceptor, "enter", current)
                    entered.append(interceptor)
                except Exception as err:
                    pending_error = err
                    break
                continue

            try:
                current = self._run_item_sync(item, current, policy)
            except Exception as err:
                pending_error = err
                break

        for interceptor in reversed(entered):
            if pending_error is None:
                try:
                    current = stage(interceptor.leave, interceptor, "leave", current)
                except Exception as err:
                    pending_error = err
            else:
                try:
                    current = stage(
                        interceptor.error, interceptor, "error", current, pending_error
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
            return self._run_chain_sync(item, payload, inherited_policy)
        if isinstance(item, StreamChain):
            return self._run_stream_chain_sync(item, payload, inherited_policy)
        raise ExecutionError(f"Unsupported chain item: {item!r}")

    def _run_stream_chain_sync(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        inherited_policy: Policy | None,
    ) -> Any:
        with self._frame(stream_chain.name):
            policy = stream_chain.policy or inherited_policy
            if isinstance(policy, AsyncPolicy):
                raise ExecutionError(
                    f"StreamChain '{stream_chain.name}' requires async execution"
                )
            if isinstance(policy, ThreadPolicy | ThreadPoolPolicy):
                return self._run_with_thread_policy_sync(
                    policy,
                    lambda: self._run_stream_chain_body_sync(
                        stream_chain, payload, policy
                    ),
                )
            return self._run_stream_chain_body_sync(stream_chain, payload, policy)

    def _run_stream_chain_body_sync(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        policy: Policy | None,
    ) -> Any:
        if stream_chain.opener is None:
            raise ExecutionError("StreamChain is missing stream(...)")
        if stream_chain.processor is None:
            raise ExecutionError("StreamChain is missing map(...)")

        opener = self._instantiate(stream_chain.opener)

        def stage(name: str, fn: Callable[..., object], *args: object) -> Any:
            return self._run_stage_sync(
                stream_chain.name, policy, fn, opener, name, *args
            )

        try:
            emitted_items = stage(
                "stream",
                partial(self._materialize_stream_result_sync, opener.stream),
                payload,
            )
        except Exception as err:
            return stage("error", opener.error, payload, err)

        try:
            collected = self._map_stream_items_sync(
                stream_chain.processor, emitted_items, policy
            )
            return stage("collect", opener.collect, payload, collected)
        except Exception as err:
            return stage("error", opener.error, payload, err)

    async def _run_chain_async[TIn, TOut](
        self,
        chain: Chain[TIn, TOut],
        payload: TIn,
        inherited_policy: Policy | None,
    ) -> TOut:
        with self._frame(chain.name), self._provide_scope(chain):
            policy = chain.policy or inherited_policy
            if isinstance(
                policy, ThreadPolicy | ThreadPoolPolicy
            ) and not _chain_body_is_async(chain):
                return await self._run_sync_in_executor(
                    policy,
                    lambda: self._run_chain_body_sync(chain, payload, policy),
                )
            return await self._run_chain_body_async(chain, payload, policy)

    async def _run_chain_body_async[TOut](
        self,
        chain: Chain[Any, TOut],
        payload: Any,
        policy: Policy | None,
    ) -> TOut:
        async def stage(
            fn: Callable[..., object],
            interceptor: Interceptor[Any, Any],
            name: str,
            *args: object,
        ) -> Any:
            return await self._run_stage_async(
                chain.name, policy, fn, interceptor, name, *args
            )

        current: Any = payload
        entered: list[Interceptor[Any, Any]] = []
        pending_error: Exception | None = None

        for item in chain.items:
            if (isinstance(item, type) and issubclass(item, Interceptor)) or (
                isinstance(item, BoundInterceptor)
            ):
                interceptor = self._instantiate_step(item)
                try:
                    current = await stage(
                        interceptor.enter, interceptor, "enter", current
                    )
                    entered.append(interceptor)
                except Exception as err:
                    pending_error = err
                    break
                continue

            try:
                current = await self._run_item_async(item, current, policy)
            except Exception as err:
                pending_error = err
                break

        for interceptor in reversed(entered):
            if pending_error is None:
                try:
                    current = await stage(
                        interceptor.leave, interceptor, "leave", current
                    )
                except Exception as err:
                    pending_error = err
            else:
                try:
                    current = await stage(
                        interceptor.error, interceptor, "error", current, pending_error
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
            return await self._run_chain_async(item, payload, inherited_policy)
        if isinstance(item, StreamChain):
            return await self._run_stream_chain_async(item, payload, inherited_policy)
        raise ExecutionError(f"Unsupported chain item: {item!r}")

    async def _run_stream_chain_async(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        inherited_policy: Policy | None,
    ) -> Any:
        with self._frame(stream_chain.name):
            policy = stream_chain.policy or inherited_policy
            if isinstance(
                policy, ThreadPolicy | ThreadPoolPolicy
            ) and not _stream_chain_body_is_async(stream_chain):
                return await self._run_sync_in_executor(
                    policy,
                    lambda: self._run_stream_chain_body_sync(
                        stream_chain, payload, policy
                    ),
                )
            return await self._run_stream_chain_body_async(
                stream_chain, payload, policy
            )

    async def _run_stream_chain_body_async(
        self,
        stream_chain: StreamChain[Any, Any, Any, Any],
        payload: Any,
        policy: Policy | None,
    ) -> Any:
        if stream_chain.opener is None:
            raise ExecutionError("StreamChain is missing stream(...)")
        if stream_chain.processor is None:
            raise ExecutionError("StreamChain is missing map(...)")

        opener = self._instantiate(stream_chain.opener)

        async def stage(name: str, fn: Callable[..., object], *args: object) -> Any:
            return await self._run_stage_async(
                stream_chain.name, policy, fn, opener, name, *args
            )

        try:
            emitted_items = await stage(
                "stream",
                partial(self._materialize_stream_result_async, opener.stream),
                payload,
            )
        except Exception as err:
            return await stage("error", opener.error, payload, err)

        try:
            collected = await self._map_stream_items_async(
                stream_chain.processor, emitted_items, policy
            )
            return await stage("collect", opener.collect, payload, collected)
        except Exception as err:
            return await stage("error", opener.error, payload, err)

    def _map_stream_items_sync(
        self,
        processor: Chain[Any, Any],
        emitted_items: list[Any],
        inherited_policy: Policy | None,
    ) -> list[Any]:
        def run_one(item: Any) -> Any:
            return self._run_chain_sync(processor, item, inherited_policy)

        policy = processor.policy or inherited_policy
        if not isinstance(policy, ThreadPoolPolicy):
            return [run_one(item) for item in emitted_items]

        results: list[Any] = [None] * len(emitted_items)
        pending = iter(enumerate(emitted_items))
        lock = threading.Lock()
        failures: list[Exception] = []

        def drain() -> None:
            while not failures:
                with lock:
                    index, item = next(pending, (None, None))
                if index is None:
                    return
                try:
                    results[index] = run_one(item)
                except Exception as err:
                    failures.append(err)
                    return

        holds_worker = self._is_current_policy(policy)
        helpers = min(policy.workers, len(emitted_items)) - int(holds_worker)
        executor = self.get_executor(policy)
        key = self._policy_key(policy)
        futures = [
            executor.submit(
                contextvars.copy_context().run,
                self._call_with_policy_key,
                key,
                drain,
            )
            for _ in range(helpers)
        ]
        if holds_worker:
            drain()
            for future in futures:
                future.cancel()
        for future in futures:
            if not future.cancelled():
                future.result()
        if failures:
            raise failures[0]
        return results

    async def _map_stream_items_async(
        self,
        processor: Chain[Any, Any],
        emitted_items: list[Any],
        inherited_policy: Policy | None,
    ) -> list[Any]:
        parallelism = self._map_parallelism_async(processor, inherited_policy)

        async def run_one(item: Any) -> Any:
            return await self._run_chain_async(processor, item, inherited_policy)

        if parallelism == 1:
            return [await run_one(item) for item in emitted_items]

        semaphore = asyncio.Semaphore(parallelism or len(emitted_items))

        async def bounded(item: Any) -> Any:
            async with semaphore:
                return await run_one(item)

        try:
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(bounded(item)) for item in emitted_items]
        except ExceptionGroup as err:
            if len(err.exceptions) == 1:
                raise err.exceptions[0] from None
            raise
        return [task.result() for task in tasks]

    def _map_parallelism_async(
        self,
        processor: Chain[Any, Any],
        inherited_policy: Policy | None,
    ) -> int | None:
        policy = processor.policy or inherited_policy
        if isinstance(policy, ThreadPolicy):
            return 1
        if isinstance(policy, ThreadPoolPolicy):
            return policy.workers
        if isinstance(policy, AsyncPolicy):
            return None
        if _chain_is_async(processor):
            return None
        return 1

    def _run_stage_sync(
        self,
        chain_name: str,
        policy: Policy | None,
        fn: Callable[..., object],
        interceptor: object,
        stage: str,
        *args: object,
    ) -> Any:
        with self._observe(
            chain_name, self._step_name(type(interceptor)), stage, policy
        ):
            return self._call_sync(fn, *args)

    async def _run_stage_async(
        self,
        chain_name: str,
        policy: Policy | None,
        fn: Callable[..., object],
        interceptor: object,
        stage: str,
        *args: object,
    ) -> Any:
        step_name = self._step_name(type(interceptor))

        async def observed(*inner_args: object) -> Any:
            with self._observe(chain_name, step_name, stage, policy):
                return await self._call_async(fn, *inner_args)

        return await self._apply_policy_async(policy, observed, *args)

    @contextmanager
    def _observe(
        self,
        chain_name: str,
        step_name: str,
        stage: str,
        policy: Policy | None,
    ) -> Iterator[None]:
        """Time one stage and report how it ended to the observers."""
        start = time.perf_counter()
        try:
            yield
        except Exception as err:
            err.add_note(
                f"py_interceptors chain={chain_name!r} path={self._path_var.get()!r} "
                f"step={step_name!r} stage={stage!r} "
                f"policy={self._policy_label(policy)!r}"
            )
            self._emit_event(chain_name, step_name, stage, policy, start, err)
            raise
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

        event = ExecutionEvent(
            execution_id=self._execution_id_var.get(),
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

    async def _apply_policy_async[TResult](
        self,
        policy: Policy | None,
        fn: Callable[..., Coroutine[Any, Any, TResult]],
        *args: object,
    ) -> TResult:
        if isinstance(policy, ThreadPolicy | ThreadPoolPolicy):
            # WHY asyncio.run: the stage must run on the policy's thread, which
            # has no event loop, so each async stage gets its own short-lived
            # loop. Loop-bound resources made in enter() are therefore not
            # usable in leave()/error() under a thread policy.
            return await self._run_sync_in_executor(
                policy, lambda: asyncio.run(fn(*args))
            )
        if isinstance(policy, AsyncPolicy) and policy.isolated:
            return await self._run_in_async_policy(policy, lambda: fn(*args))
        return await fn(*args)

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
        result = await self._call_async(fn, *args)
        if isinstance(result, AsyncIterable):
            return [item async for item in result]
        return list(self._require_iterable(result))

    def _run_with_thread_policy_sync[TResult](
        self,
        policy: ThreadPolicy | ThreadPoolPolicy,
        call: Callable[[], TResult],
    ) -> TResult:
        if self._is_current_policy(policy):
            return call()

        executor = self.get_executor(policy)
        context = contextvars.copy_context()
        future = executor.submit(
            context.run, self._call_with_policy_key, self._policy_key(policy), call
        )
        return future.result()

    async def _run_sync_in_executor[TResult](
        self,
        policy: ThreadPolicy | ThreadPoolPolicy,
        call: Callable[[], TResult],
    ) -> TResult:
        if self._is_current_policy(policy):
            return call()

        loop = asyncio.get_running_loop()
        executor = self.get_executor(policy)
        context = contextvars.copy_context()
        key = self._policy_key(policy)
        return await loop.run_in_executor(
            executor,
            lambda: context.run(self._call_with_policy_key, key, call),
        )

    async def _run_in_async_policy[TResult](
        self,
        policy: AsyncPolicy,
        call: Callable[[], Awaitable[TResult]],
    ) -> TResult:
        if self._is_current_policy(policy):
            return await call()

        portal = self.get_async_portal(policy)
        future = portal.submit(
            self._call_async_with_policy_key(self._policy_key(policy), call)
        )
        return await asyncio.wrap_future(future)

    def _call_with_policy_key[TResult](
        self,
        key: PolicyKey,
        call: Callable[[], TResult],
    ) -> TResult:
        # WHY: the copied context carries the submitter's policy key into the
        # executor; it is replaced with the executor's own key so that nested
        # segments on the same policy run inline (see _is_current_policy).
        token = self._policy_key_var.set(key)
        try:
            return call()
        finally:
            self._policy_key_var.reset(token)

    async def _call_async_with_policy_key[TResult](
        self,
        key: PolicyKey,
        call: Callable[[], Awaitable[TResult]],
    ) -> TResult:
        # WHY only the policy key: run_coroutine_threadsafe snapshots the
        # submitter's context into the portal task, so the execution id, path
        # and provide stack arrive on their own; the policy key alone must
        # change from the submitter's to the portal's.
        token = self._policy_key_var.set(key)
        try:
            return await call()
        finally:
            self._policy_key_var.reset(token)

    def _is_current_policy(self, policy: Policy) -> bool:
        # WHY: a segment already running on its policy's lane, pool or portal
        # runs inline. Submitting to the executor we are on and blocking on the
        # result would deadlock a single-worker lane or portal loop.
        return self._policy_key_var.get() == self._policy_key(policy)

    @staticmethod
    def _policy_key(policy: Policy) -> PolicyKey:
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

    def _instantiate_step(
        self,
        item: type[Interceptor[Any, Any]] | BoundInterceptor,
    ) -> Interceptor[Any, Any]:
        direct: Mapping[str, object] = {}
        if isinstance(item, BoundInterceptor):
            interceptor_cls = item.interceptor_type
            direct = dict(item.kwargs)
        else:
            interceptor_cls = item

        instance = self._instantiate(interceptor_cls)
        resolved = resolve_step_dependencies(
            interceptor_cls, direct, self._provide_var.get()
        )
        for attr, value in resolved.items():
            setattr(instance, attr, value)
        return instance

    @contextmanager
    def _provide_scope(self, chain: Chain[Any, Any]) -> Iterator[None]:
        if not chain.provides:
            yield
            return
        token = self._provide_var.set((dict(chain.provides), *self._provide_var.get()))
        try:
            yield
        finally:
            self._provide_var.reset(token)
