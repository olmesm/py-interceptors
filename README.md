# py-interceptors

> py-interceptors lets teams define units of workflow logic independently from how those units are executed, so execution strategy can be deferred, experimented with, and tuned without rewriting the workflow itself.

py-interceptors provides type-safe workflow composition, scoped stream fanout, Pedestal-style enter/leave/error semantics, and explicit runtime execution policies for sync, async, threaded, and pooled work.


## Install

```bash
uv add py-interceptors
# or
pip install py-interceptors
```

## Quick Example

```python
from dataclasses import dataclass

from py_interceptors import Interceptor, Runtime, chain


@dataclass
class GreetingRequest:
    name: str


@dataclass
class GreetingResponse:
    body: str


class NormalizeName(Interceptor[GreetingRequest, GreetingRequest]):
    input_type = GreetingRequest
    output_type = GreetingRequest

    def enter(self, ctx: GreetingRequest) -> GreetingRequest:
        return GreetingRequest(name=ctx.name.strip().title())


class RenderGreeting(Interceptor[GreetingRequest, GreetingResponse]):
    input_type = GreetingRequest
    output_type = GreetingResponse

    def enter(self, ctx: GreetingRequest) -> GreetingResponse:
        return GreetingResponse(body=f"Hello, {ctx.name}")


workflow = (
    chain("greeting")
    .use(NormalizeName)
    .use(RenderGreeting)
    .build()
)

with Runtime() as runtime:
    result = runtime.run_sync(workflow, GreetingRequest(" ada "))
```

`chain(...)` and `stream_chain(...)` are the preferred builders because mypy can
infer the input and output types as each step is added.

## Mental Model

Normal chains keep one context flowing through the pipeline:

```text
Context -> Interceptor -> Interceptor -> Context
```

Each interceptor may define:

- `enter(ctx)`: forward execution
- `leave(ctx)`: reverse-order unwind after success
- `error(ctx, err)`: reverse-order unwind after failure

Stream stages temporarily expand one context into many items, process each item
through a child chain, then collect the results back into one context:

```text
Context -> many items -> processed items -> Context
```

Policies say where code runs, not what it does. Add them with `.on(...)` when
a chain needs a named thread lane, a worker pool, or async execution.

## When To Use What

| Need | Use |
| --- | --- |
| One-in, one-out step | `Interceptor` |
| Split and merge data | `StreamInterceptor` |
| Compose workflow steps | `chain(...)` |
| Compose stream scope | `stream_chain(...)` |
| Run sync workflows | `Runtime.run_sync(...)` |
| Run async workflows | `Runtime.run_async(...)` |
| Control placement | `AsyncPolicy`, `ThreadPolicy`, `ThreadPoolPolicy` |

## Deeper Docs

- [Interceptor lifecycle](docs/interceptor-lifecycle.md): `enter`, `leave`,
  `error`, naming, and chain composition.
- [Runtime and policies](docs/runtime-and-policies.md): running, compiling,
  lifecycle hooks, policy inheritance, portals, and placement rules.
- [Observability](docs/observability.md): execution events, observers, and
  exception notes.
- [Organizing workflows](docs/organizing-workflows.md): suggested application
  structure for larger projects.

## Examples

- [csv_import_pipeline.py](examples/csv_import_pipeline.py): sync stream
  split/map/collect.
- [external_api_fanout.py](examples/external_api_fanout.py): async fanout
  through an isolated policy.
- [cities_countries_continents.py](examples/cities_countries_continents.py):
  stream composition inside a larger policy-shaped workflow.
- [fastapi_request_pipeline.py](examples/fastapi_request_pipeline.py): async
  request workflow with lifecycle startup, compilation, and error mapping.

The example scenarios are covered by tests and can be run with `uv run pytest`.
