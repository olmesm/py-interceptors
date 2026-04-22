# py-interceptors

py-interceptors provides type-safe workflow composition, scoped stream fanout, Pedestal-style enter/leave/error semantics, and explicit runtime execution policies for sync, async, threaded, and pooled work.

> py-interceptors lets teams define units of workflow logic independently from how those units are executed, so execution strategy can be deferred, experimented with, and tuned without rewriting the workflow itself.

## Features

- **Typed workflow composition**: declare input and output context types, compose chains, and validate that each step receives the context it expects.
- **Type-inferred builders**: use `chain(...)` and `stream_chain(...)` so mypy can infer workflow and stream-stage types without variable annotations.
- **Pedestal-style interceptor lifecycle**: model work with `enter`, `leave`, and `error`, where `leave` unwinds in reverse order after the forward path.
- **Scoped stream fanout**: split one context into many items, run a child chain per item, then collect results back into one context.
- **Explicit execution policies**: choose default async execution, isolated async portals, named thread lanes, or named worker pools independently from workflow logic.
- **Policy inheritance and override**: let child chains inherit parent placement, or override execution for specific sync, async, or blocking work.
- **Runtime-owned resources**: manage thread lanes, thread pools, and isolated async portals through a `Runtime` lifecycle.
- **Validation before execution**: catch incompatible step order, stream boundary mismatches, missing metadata, and policy conflicts before running the workflow.
- **Per-runtime compile cache**: compile once to validate and warm a reusable plan without eagerly creating execution resources.

## Install

```bash
uv add py-interceptors
# pip
pip install py-interceptors
```

## Mental Model

There are two execution modes.

Normal chains keep one context flowing through the pipeline:

```text
Context -> Context -> Context
```

Stream stages temporarily expand one context into many items, process each item
through a child chain, then collect the results back into one context:

```text
Context -> many items -> processed items -> Context
```

The outer workflow always remains one context in, one context out. Stream stages
are scoped expansions, not a permanent change in flow.

## Execution Flow

```text
Initial Context
        |
        v
+----------------------+
| CitiesToDataFrame    |  ThreadPolicy("main")
+----------------------+
        |
        v
+------------------------------+
| Stream Stage: ChunkCities    |
|                              |
| stream(ctx)                  |
|   item 1 ---+                |
|   item 2 ---+--> child chain |
|   item 3 ---+                |
|                              |
| collect(ctx, processed)      |
+------------------------------+
        |
        v
+----------------------+
| GroupByContinent     |  ThreadPolicy("main")
+----------------------+
        |
        v
Final Context
```

Key points:

- only one context flows through the outer `Chain`
- a `StreamChain` temporarily expands to many items
- mapped items are processed independently
- `StreamInterceptor.collect(...)` merges results back into one context

## Basic Usage

```python
from dataclasses import dataclass

import polars as pl

from py_interceptors import Interceptor, Runtime, chain


@dataclass
class CitiesContext:
    cities: list[str]


@dataclass
class DataFrameContext:
    df: pl.DataFrame


class CitiesToDataFrame(Interceptor[CitiesContext, DataFrameContext]):
    name = "cities-to-dataframe"
    input_type = CitiesContext
    output_type = DataFrameContext

    def enter(self, ctx: CitiesContext) -> DataFrameContext:
        return DataFrameContext(df=pl.DataFrame({"city": ctx.cities}))


workflow = chain("cities").use(CitiesToDataFrame).build()

runtime = Runtime()
result = runtime.run_sync(workflow, CitiesContext(cities=["London", "Paris"]))
runtime.shutdown()
```

## Interceptors

`Interceptor` is the basic one-in, one-out step.

```python
class AddCountry(Interceptor[DataFrameContext, DataFrameContext]):
    name = "add-country"
    input_type = DataFrameContext
    output_type = DataFrameContext

    def enter(self, ctx: DataFrameContext) -> DataFrameContext:
        ...

    def leave(self, ctx: DataFrameContext) -> DataFrameContext:
        ...

    def error(self, ctx: DataFrameContext, err: Exception) -> DataFrameContext:
        ...
```

Methods:

- `enter(ctx)` runs in chain order
- `leave(ctx)` runs in reverse order after successful forward execution
- `error(ctx, err)` runs during error unwind

`enter`, `leave`, and `error` may be sync or async.

### Enter And Leave Stack

Composed interceptors behave like a forward queue plus a return stack. `enter`
runs in chain order. Each interceptor that enters successfully is pushed onto
the stack. After the forward path completes, `leave` runs in reverse order.

```python
workflow = (
    chain("trace")
    .use(Outer)
    .use(Inner)
    .build()
)

# Execution order:
# outer enter
# inner enter
# inner leave
# outer leave
```

Each `leave` receives the current context from the forward path or the inner
`leave` that ran before it. It can update or replace that context before
returning it to the next outer interceptor.

This makes `leave` useful for wrapping behavior around inner work:

- timing and tracing
- response decoration
- cleanup after successful work
- final normalization before returning to the caller

Only successfully entered interceptors participate in the return path. If an
`enter` fails, the runtime starts error unwind over the already-entered stack.
If an `error` handler returns normally, the error is handled and the remaining
outer stack resumes through `leave`. If `leave` itself raises, execution switches
to error unwind for the remaining outer stack.

Interceptors may set an optional `name`. If omitted, observers use the class
name. If provided, the name must be a non-empty string.

## Stream Stages

Use `StreamInterceptor` when one parent context should emit many item contexts,
run a child chain per item, then merge results back into one parent context.

```python
from collections.abc import Iterable

from py_interceptors import AsyncPolicy, StreamInterceptor, chain, stream_chain


class ChunkCities(
    StreamInterceptor[DataFrameContext, list[str], list[str], DataFrameContext]
):
    name = "chunk-cities"
    input_type = DataFrameContext
    emit_type = list[str]
    collect_type = list[str]
    output_type = DataFrameContext

    def stream(self, ctx: DataFrameContext) -> Iterable[list[str]]:
        cities = ctx.df["city"].to_list()
        for i in range(0, len(cities), 100):
            yield cities[i : i + 100]

    def collect(
        self,
        ctx: DataFrameContext,
        items: Iterable[list[str]],
    ) -> DataFrameContext:
        countries = [country for chunk in items for country in chunk]
        return DataFrameContext(
            df=ctx.df.with_columns(pl.Series("country", countries))
        )


class FetchCountries(Interceptor[list[str], list[str]]):
    name = "fetch-countries"
    input_type = list[str]
    output_type = list[str]

    async def enter(self, cities: list[str]) -> list[str]:
        return await api.fetch_countries(cities)


enrich_countries = (
    chain("enrich countries")
    .use(FetchCountries)
    .on(AsyncPolicy())
    .build()
)

chunk_cities = (
    stream_chain("chunk cities")
    .stream(ChunkCities)
    .map(enrich_countries)
    .build()
)
```

There is no `StreamChain.collect()` builder method. The collection step is the
`collect(...)` method on the `StreamInterceptor`.

## Compose A Workflow

```python
from py_interceptors import ThreadPolicy, chain


class GroupByContinent(Interceptor[DataFrameContext, DataFrameContext]):
    name = "group-by-continent"
    input_type = DataFrameContext
    output_type = DataFrameContext

    def enter(self, ctx: DataFrameContext) -> DataFrameContext:
        ...


workflow = (
    chain("cities -> continents")
    .use(CitiesToDataFrame)
    .use(chunk_cities)
    .use(GroupByContinent)
    .on(ThreadPolicy("main"))
    .build()
)
```

`Chain(...)` and `StreamChain(...)` remain available when you want explicit
constructor-based composition. The lowercase builders are the preferred API when
you want mypy to infer the workflow type without a variable annotation.

All chains and stream chains require a non-empty name, whether built with
`chain(...)` / `stream_chain(...)` or constructed directly with `Chain(...)` /
`StreamChain(...)`.

## Observability

`Runtime.add_observer(...)` registers a callback that receives an
`ExecutionEvent` after every interceptor stage. There is no default logging;
observers are the hook for application logging, metrics, or debugging.

```python
from py_interceptors import ExecutionEvent, Runtime


def log_event(event: ExecutionEvent) -> None:
    logger.info(
        "interceptor",
        extra={
            "execution_id": event.execution_id,
            "chain": event.chain,
            "path": event.path,
            "step": event.step,
            "stage": event.stage,
            "policy": event.policy,
            "thread": event.thread,
            "elapsed_ms": event.elapsed_ms,
            "failed": event.error is not None,
        },
    )


runtime = Runtime().add_observer(log_event)
```

Events include chain and interceptor names, the nested path, policy label,
thread name, elapsed time, and an optional error. Context payloads are not
included. Observer exceptions propagate to the caller.

When a stage fails, the original exception is preserved and annotated with a
Python exception note containing the chain, path, step, stage, and policy.

## Run

Use `run_sync(...)` for sync-only workflows:

```python
runtime = Runtime()
try:
    result = runtime.run_sync(workflow, CitiesContext(cities=["London"]))
finally:
    runtime.shutdown()
```

Use `run_async(...)` for workflows containing async steps or async policies:

```python
runtime = Runtime()
try:
    result = await runtime.run_async(
        workflow,
        CitiesContext(cities=["London", "Paris", "Tokyo"]),
    )
finally:
    runtime.shutdown()
```

You can also compile first:

```python
compiled = runtime.compile(workflow, initial=CitiesContext)
result = await compiled.run_async(CitiesContext(cities=["Madrid"]))
```

Compiled plans are cached per `Runtime`, keyed by workflow and initial type. That
means application startup can call `compile(...)` to validate and warm the plan,
while later `run_sync(...)` or `run_async(...)` calls with the same workflow and
payload type reuse it. Compilation does not instantiate interceptors or create
thread pools, thread lanes, or isolated async portals; those resources are still
created lazily during execution.

For application frameworks with async lifecycles, `Runtime` also exposes
`startup()` and `shutdown_async()`:

```python
runtime = Runtime()
await runtime.startup()
runtime.compile(workflow, initial=CitiesContext)

try:
    result = await runtime.run_async(workflow, CitiesContext(cities=["Madrid"]))
finally:
    await runtime.shutdown_async()
```

`startup()` does not eagerly create execution resources. It provides a stable
lifecycle hook for integrations such as FastAPI lifespan handlers.

## Policies

Policies define where code runs, not what it does.

| Policy | Meaning |
| --- | --- |
| `ThreadPolicy("A")` | named single-thread lane with affinity |
| `ThreadPoolPolicy("api", workers=8)` | named worker pool for blocking or parallel sync work |
| `AsyncPolicy()` | current/default async runtime |
| `AsyncPolicy("io", isolated=True)` | named runtime-owned async portal with a dedicated loop |

Thread lanes serialize work and preserve affinity. Thread pools allow parallel
sync work. Async policies are for non-blocking IO. A workflow can mix all three.

Policy inheritance is lexical. If a child `Chain` or `StreamChain` does not
declare its own policy with `.on(...)`, it inherits the parent policy.

Common scenarios:

| Parent policy | Child policy | Child contains | Where child runs | Notes |
| --- | --- | --- | --- | --- |
| none | none | sync steps | caller thread | Direct sync execution. |
| none | none | async steps | current async runtime | Requires `run_async(...)`; `run_sync(...)` is rejected. |
| `ThreadPolicy("A")` | none | sync steps | thread lane `A` | Child inherits the parent lane. |
| `ThreadPolicy("A")` | none | async steps | thread lane `A` | Async methods are driven to completion on lane `A`; this preserves affinity but blocks the lane. |
| `ThreadPolicy("A")` | `AsyncPolicy()` | async steps | current async runtime | Recommended for true async IO inside a thread-affined parent; parent `leave/error` resumes on lane `A`. |
| `ThreadPolicy("A")` | `AsyncPolicy("io", isolated=True)` | async steps | isolated `io` async portal | Use when async IO needs a dedicated runtime-owned event loop. |
| `ThreadPolicy("A")` | `ThreadPolicy("B")` | sync or async steps | thread lane `B` | Child overrides the parent lane; parent resumes on lane `A`. |
| `ThreadPolicy("A")` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | named worker pool | Use for blocking work where affinity is not needed. |
| `ThreadPoolPolicy("pool", workers=4)` | none | sync steps | named worker pool | Child inherits the pool. |
| `ThreadPoolPolicy("pool", workers=4)` | none | async steps | named worker pool | Async methods are driven to completion in pool workers; this blocks those workers. |
| `ThreadPoolPolicy("pool", workers=4)` | `AsyncPolicy()` | async steps | current async runtime | Prefer this for non-blocking async IO from a blocking pool workflow. |
| `AsyncPolicy()` | none | sync steps | current async runtime thread | Sync methods run directly in the event loop thread; avoid blocking work here. |
| `AsyncPolicy()` | none | async steps | current async runtime | Child inherits default async execution. |
| `AsyncPolicy()` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | named worker pool | Use to offload blocking sync work from async workflows. |
| `AsyncPolicy("io", isolated=True)` | none | sync steps | isolated `io` async portal thread | Sync methods run directly on the isolated event loop thread; avoid blocking work here. |
| `AsyncPolicy("io", isolated=True)` | none | async steps | isolated `io` async portal | Child inherits the isolated portal. |
| `AsyncPolicy("io", isolated=True)` | `AsyncPolicy("io", isolated=True)` | async steps | same isolated `io` async portal | Same-portal nesting runs inline on the portal and does not resubmit. |
| `AsyncPolicy("io", isolated=True)` | `AsyncPolicy()` | async steps | current async runtime | Child overrides the isolated portal and returns to the caller's async runtime. |
| `AsyncPolicy("io", isolated=True)` | `ThreadPoolPolicy("pool", workers=4)` | sync steps | named worker pool | Use to offload blocking sync work from an isolated async portal. |

That means an async child chain without a policy also inherits a parent
`ThreadPolicy`:

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
an explicit async policy:

```python
child = chain("child").use(FetchCountries).on(AsyncPolicy()).build()

workflow = (
    chain("workflow")
    .use(CitiesToDataFrame)
    .use(child)
    .use(GroupByContinent)
    .on(ThreadPolicy("main"))
    .build()
)
```

This gives the intended `ThreadPolicy("main") -> AsyncPolicy() ->
ThreadPolicy("main")` execution shape.

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

## When To Use What

| Use | Tool |
| --- | --- |
| Transform one context | `Interceptor` |
| Split and merge data | `StreamInterceptor` |
| Compose workflow steps | `Chain` |
| Compose stream scope | `StreamChain` |
| Single-thread affinity | `ThreadPolicy` |
| Parallel blocking work | `ThreadPoolPolicy` |
| Async IO | `AsyncPolicy` |
| Dedicated async loop | `AsyncPolicy(name, isolated=True)` |

## Examples

- `examples/cities_countries_continents.py`: async stream enrichment over city chunks with Polars aggregation.
- `examples/fastapi_request_pipeline.py`: FastAPI order endpoint using `enter`, `error`, and `leave` to authenticate, validate, create a response, and map failures to HTTP responses.
- `examples/external_api_fanout.py`: fake external API fanout using a `StreamChain` and an isolated named `AsyncPolicy` portal.
- `examples/csv_import_pipeline.py`: sync CSV import that parses rows, validates them through a mapped stream pipeline, and collects accepted/rejected rows.

The example scenarios are covered by tests and can be run with `uv run pytest`.

## Philosophy

- typed transformations
- explicit execution placement
- context carries data
- streams are scoped, not global
