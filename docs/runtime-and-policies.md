# Runtime and policies

`Runtime` compiles workflows, runs them, owns the threads and event loops that
policies need, and sends execution events to observers.

## Running workflows

`run_sync(...)` runs a workflow with no async steps and no `AsyncPolicy` on
the calling thread. On a plan that contains either, it raises
`ExecutionError` and tells you to use `run_async`.

```python
with Runtime() as runtime:
    result = runtime.run_sync(workflow, payload)
```

`run_async(...)` runs any workflow from async code. Unplaced async steps run
on the caller's event loop.

```python
async with Runtime() as runtime:
    result = await runtime.run_async(workflow, payload)
```

`run_blocking(...)` runs any workflow from sync code that has no running
event loop, such as a sync `def` FastAPI route (Starlette calls those on a
worker thread), a CLI command, or a background worker thread. The workflow
runs on an event loop the runtime owns, in a thread named
`py-interceptors-default` that starts on the first call, and the calling
thread blocks until the result is ready. Stages under a `ThreadPolicy` or
`ThreadPoolPolicy` still run on that policy's threads.

```python
with Runtime() as runtime:
    result = runtime.run_blocking(workflow, payload)
```

Calling `run_blocking` from a thread that is already running an event loop
raises `ExecutionError`, because blocking that loop on its own work would
deadlock. Use `await runtime.run_async(...)` there.

## Lifecycle

Use `with Runtime() as runtime:` in sync code and
`async with Runtime() as runtime:` in async code. Leaving the block calls
`shutdown()` or `shutdown_async()`, which stops the runtime's event loop
threads, shuts down its thread lanes and pools, and clears the plan cache.
Both take `wait=True` by default; pass `wait=False` to return without joining
the threads.

There is no startup step. Thread lanes, pools and event loop threads are
created the first time a stage needs them. In a FastAPI app, open the runtime
in the lifespan and compile there so a broken workflow fails at startup:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with Runtime() as runtime:
        runtime.compile(workflow, initial=Payload)
        app.state.runtime = runtime
        yield
```

[fastapi_pipeline.py](../examples/fastapi_pipeline.py) uses this lifespan
with an `async def` route that calls `run_async` and a `def` route that calls
`run_blocking`.

## Compilation and the plan cache

`compile(...)` validates a workflow and returns a `CompiledPlan`:

```python
plan = runtime.compile(workflow, initial=PayloadType)
result = await plan.run_async(payload)
```

Compilation checks that each step's `input_type` accepts the previous step's
output, that every interceptor has its metadata, that stream stages are
complete, that each policy name means one policy, and that every declared
dependency resolves. Failures raise `ValidationError` or one of its
`DependencyError` subclasses. It does not instantiate interceptors or start
any threads or event loops.

A plan has `run_sync(payload)` and `run_async(payload)`, both of which raise
`ExecutionError` when the payload's type does not match `input_spec`. It also
exposes `input_spec`, `output_spec` and `is_async`.

Plans are cached per `Runtime`, keyed by the identity of the chain object and
the `initial` type. `run_sync`, `run_async` and `run_blocking` compile with
`initial=type(payload)`, so the first run of a chain compiles it and later
runs with the same chain object and payload type reuse the plan. Two chains
that compare equal but were built separately compile separately, so build a
workflow once (at module level or at startup) and reuse that object.
`shutdown()` empties the cache.

## Policies

| Policy | Where stages run |
| --- | --- |
| `ThreadPolicy("main")` | One runtime-owned thread, named `main_0`, shared by every chain in this runtime that uses the name `main`. Stages run one at a time. |
| `ThreadPoolPolicy("api", workers=8)` | A runtime-owned pool of up to 8 threads (`api_0`, `api_1`, ...). One chain's stages run one after another; the pool runs several chains or stream items at once. |
| `AsyncPolicy()` | The event loop that is already running the workflow: the caller's loop under `run_async`, the runtime's loop under `run_blocking`. |
| `AsyncPolicy("io", isolated=True)` | A runtime-owned event loop on its own thread named `io`, shared by every chain in this runtime that uses that name. |

`AsyncPolicy("io")` without `isolated=True` runs on the current loop, the
same as `AsyncPolicy()`; the name only takes part in the conflict check
below. A workflow containing any `AsyncPolicy` must run with `run_async` or
`run_blocking`.

The constructors raise `ValueError` for an empty name, for `workers` below 1,
and for `isolated=True` without a name.

Within one workflow a name must always mean the same policy. Using
`ThreadPoolPolicy("io", workers=4)` and `AsyncPolicy("io", isolated=True)` in
one workflow makes `compile` raise
`ValidationError: Policy 'io' has conflicting declarations: ...`. Pools are
shared across workflows in one runtime, so running a second workflow that
declares an existing pool name with a different `workers` count raises
`ExecutionError`.

Policy inheritance is lexical. A `Chain` or `StreamChain` without its own
`.on(...)` inherits the policy of the chain that contains it. A child that
declares a policy uses it for itself and everything nested in it, and the
parent continues on its own policy after the child returns.

## Placement examples

Thread names in the last column are what `ExecutionEvent.thread` reports for
these policies.

| Parent policy | Child policy | Child contains | Where child runs |
| --- | --- | --- | --- |
| none | none | sync steps | caller thread |
| none | none | async steps | caller's event loop; use `run_async(...)` |
| `ThreadPolicy("A")` | none | sync steps | thread lane `A` (`A_0`) |
| `ThreadPolicy("A")` | none | async steps | thread lane `A`, one `asyncio.run` per stage (see below) |
| `ThreadPolicy("A")` | `AsyncPolicy()` | async steps | caller's event loop, then the parent resumes on lane `A` |
| `ThreadPolicy("A")` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | pool `pool`, then the parent resumes on lane `A` |
| `ThreadPoolPolicy("pool", workers=4)` | none | sync steps | pool `pool` |
| `ThreadPoolPolicy("pool", workers=4)` | `AsyncPolicy()` | async steps | caller's event loop |
| `AsyncPolicy()` | none | sync steps | caller's event loop thread; blocking work here stalls the loop |
| `AsyncPolicy()` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | pool `pool` |
| `AsyncPolicy("io", isolated=True)` | none | async steps | the `io` event loop thread |
| `AsyncPolicy("io", isolated=True)` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | pool `pool` |

## Async steps under a thread policy

An async interceptor in a chain under `ThreadPolicy` or `ThreadPoolPolicy`
runs on that policy's thread, and each of its stages runs inside its own
`asyncio.run(...)` call. This keeps thread affinity, with two costs:

- The lane is blocked while the stage's coroutine runs, so a slow `await`
  holds up every other chain on that lane.
- `enter`, `leave` and `error` of one interceptor run on different event
  loops. An object bound to a loop, such as an `httpx.AsyncClient`, an
  `asyncio.Lock` or a database connection pool, created in `enter` cannot be
  used in `leave` or `error`.

For async IO inside a thread-affined workflow, put the async steps in a child
chain with `.on(AsyncPolicy())`. The child then runs on the caller's event
loop and the parent resumes on its lane afterwards:

```python
import asyncio
from collections import Counter

from py_interceptors import AsyncPolicy, Interceptor, Runtime, ThreadPolicy, chain

CONTINENTS = {"Paris": "Europe", "Lima": "South America", "Rome": "Europe"}


class SplitCities(Interceptor[str, list[str]]):
    input_type = str
    output_type = list[str]

    def enter(self, ctx: str) -> list[str]:
        return ctx.split(",")


class LookUpContinents(Interceptor[list[str], list[str]]):
    input_type = list[str]
    output_type = list[str]

    async def enter(self, ctx: list[str]) -> list[str]:
        await asyncio.sleep(0)  # stands in for an HTTP call
        return [CONTINENTS[city] for city in ctx]


class CountContinents(Interceptor[list[str], dict[str, int]]):
    input_type = list[str]
    output_type = dict[str, int]

    def enter(self, ctx: list[str]) -> dict[str, int]:
        return dict(Counter(ctx))


lookup = chain("lookup").use(LookUpContinents).on(AsyncPolicy()).build()

workflow = (
    chain("continents")
    .use(SplitCities)
    .use(lookup)
    .use(CountContinents)
    .on(ThreadPolicy("main"))
    .build()
)


async def main() -> None:
    async with Runtime() as runtime:
        print(await runtime.run_async(workflow, "Paris,Lima,Rome"))
        # {'Europe': 2, 'South America': 1}


asyncio.run(main())
```

`SplitCities` and `CountContinents` run on `main_0`; `LookUpContinents` runs
on the thread that called `asyncio.run`.
[cities_countries_continents.py](../examples/cities_countries_continents.py)
uses the same `ThreadPolicy("main") -> AsyncPolicy() -> ThreadPolicy("main")`
shape around a stream stage.

## Stream fan-out concurrency

A stream stage runs its mapped chain once per emitted item. How many items run
at the same time depends on the policy of the mapped chain, which is its own
`.on(...)` or the policy it inherits from the stream chain and its ancestors:

| Mapped chain policy | Items at a time |
| --- | --- |
| none, sync steps | one |
| none, async steps (`run_async`) | all of them |
| `ThreadPolicy` | one, on the lane |
| `ThreadPoolPolicy(workers=n)` | up to `n` |
| `AsyncPolicy()` or an isolated `AsyncPolicy` | all of them |

Results reach `collect` in the order `stream` emitted the items, whatever
order they finished in.

Under a pool, when the stream stage itself already runs on a worker of that
pool (for example because a parent chain is on the same pool), that worker
processes items too and the runtime submits at most `n - 1` helpers. Nested
stream stages on one pool therefore cannot deadlock waiting for a free worker.

When an item fails:

- Sync fan-out on a pool: workers stop taking new items, items already running
  finish, and the first exception is raised.
- Async fan-out: the remaining items are cancelled. A single failure is
  raised as it is; if several items fail before the cancellation reaches
  them, they are raised together as an `ExceptionGroup`.

In both cases the exception goes to the stream interceptor's `error` method;
see [Stream interceptors](stream-interceptors.md).
[external_api_fanout.py](../examples/external_api_fanout.py) fans out async
calls on an isolated `AsyncPolicy`.
