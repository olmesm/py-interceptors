# Runtime And Policies

`Runtime` validates workflows, executes them, owns execution resources, and
emits observer events.

## Running Workflows

Use `run_sync(...)` for sync-only workflows:

```python
with Runtime() as runtime:
    result = runtime.run_sync(workflow, payload)
```

Use `run_async(...)` for workflows containing async steps or async policies:

```python
async with Runtime() as runtime:
    result = await runtime.run_async(workflow, payload)
```

For a complete application lifecycle example, see
[fastapi_request_pipeline.py](../examples/fastapi_request_pipeline.py).

## Compilation

You can compile first:

```python
compiled = runtime.compile(workflow, initial=PayloadType)
result = await compiled.run_async(payload)
```

Compiled plans are cached per `Runtime`, keyed by workflow and initial type.
That means application startup can call `compile(...)` to validate and warm the
plan, while later `run_sync(...)` or `run_async(...)` calls with the same
workflow and payload type reuse it.

Compilation does not instantiate interceptors or create thread pools, thread
lanes, or isolated async portals. Those resources are created lazily during
execution.

For application frameworks with async lifecycles, `Runtime` exposes
`startup()` and `shutdown_async()`. `startup()` does not eagerly create
execution resources; it provides a stable lifecycle hook for integrations such
as FastAPI lifespan handlers.

```python
await runtime.startup()
runtime.compile(workflow, initial=PayloadType)
await runtime.shutdown_async()
```

## Policies

Policies define where code runs, not what it does.

| Policy | Meaning |
| --- | --- |
| `ThreadPolicy("main")` | named single-thread lane with affinity |
| `ThreadPoolPolicy("api", workers=8)` | named worker pool for blocking or parallel sync work |
| `AsyncPolicy()` | current/default async runtime |
| `AsyncPolicy("io", isolated=True)` | named runtime-owned async portal with a dedicated loop |

Thread lanes serialize work and preserve affinity. Thread pools allow parallel
sync work. Async policies are for non-blocking IO. A workflow can mix all three.

Policy inheritance is lexical. If a child `Chain` or `StreamChain` does not
declare its own policy with `.on(...)`, it inherits the parent policy. If a
child declares a policy, it overrides the parent for that child and its nested
work.

## Placement Examples

| Parent policy | Child policy | Child contains | Where child runs |
| --- | --- | --- | --- |
| none | none | sync steps | caller thread |
| none | none | async steps | current async runtime; use `run_async(...)` |
| `ThreadPolicy("A")` | none | sync steps | thread lane `A` |
| `ThreadPolicy("A")` | none | async steps | thread lane `A`; the lane is blocked while async work completes |
| `ThreadPolicy("A")` | `AsyncPolicy()` | async steps | current async runtime, then parent resumes on lane `A` |
| `ThreadPolicy("A")` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | named worker pool, then parent resumes on lane `A` |
| `ThreadPoolPolicy("pool", workers=4)` | none | sync steps | named worker pool |
| `ThreadPoolPolicy("pool", workers=4)` | `AsyncPolicy()` | async steps | current async runtime |
| `AsyncPolicy()` | none | sync steps | current async runtime thread; avoid blocking work here |
| `AsyncPolicy()` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | named worker pool |
| `AsyncPolicy("io", isolated=True)` | none | async steps | isolated `io` async portal |
| `AsyncPolicy("io", isolated=True)` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | named worker pool |

An async child chain without a policy also inherits a parent `ThreadPolicy`:

```python
child = chain("child").use(FetchCountries).build()

workflow = (
    chain("workflow")
    .use(CitiesToDataFrame)
    .use(child)
    .on(ThreadPolicy("main"))
    .build()
)
```

In this case `FetchCountries` runs on the `main` thread lane, even if its
methods are async. This preserves thread affinity, but it blocks that lane while
the async method is driven to completion.

For true async IO inside a thread-affined parent workflow, give the child chain
an explicit async policy. That creates the intended
`ThreadPolicy("main") -> AsyncPolicy() -> ThreadPolicy("main")` execution
shape. See
[cities_countries_continents.py](../examples/cities_countries_continents.py)
for the full workflow.

## Portals

`Portal` is an optional named execution identity that can be shared across
policies instead of repeating string names:

```python
from py_interceptors import AsyncPolicy, Portal, chain


io = Portal("io")

fetch = (
    chain("fetch")
    .use(FetchCountries)
    .on(AsyncPolicy(io, isolated=True))
    .build()
)
```

Within one workflow, the same portal name must refer to compatible policy
settings. For example, using `ThreadPoolPolicy("io", workers=4)` and
`AsyncPolicy("io", isolated=True)` together is rejected during validation.

For a runnable isolated-portal fanout, see
[external_api_fanout.py](../examples/external_api_fanout.py).
