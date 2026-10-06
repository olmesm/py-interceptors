# py-interceptors

> Compose typed chains of interceptors with `enter`, `leave` and `error`
> stages, then choose per chain with `.on(policy)` whether it runs on the
> caller's thread, a named thread, a worker pool or an event loop. The
> interceptor classes stay the same whichever policy you pick.

The enter/leave/error model follows the interceptors of Clojure's Pedestal
library. Each interceptor declares `input_type` and `output_type`, and
`Runtime.compile(...)` checks that every step accepts what the previous step
returns and that every declared dependency resolves before anything runs.
Stream stages split one value into many items, run each item through a child
chain, and collect the results.

## Install

py-interceptors requires Python 3.12 or newer and has no runtime
dependencies.

Releases are built and published by this repository's release workflow
(`.github/workflows/ci.yml`). Until the package appears on PyPI, install it
from GitHub:

```bash
uv add git+https://github.com/olmesm/py-interceptors
# or
pip install git+https://github.com/olmesm/py-interceptors
```

## Quick example

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

assert result == GreetingResponse(body="Hello, Ada")
```

Build workflows with `chain(...)` and `stream_chain(...)`: mypy infers the
input and output types as each step is added. `Chain[...]` and
`StreamChain[...]` can also be constructed directly when you write the type
parameters yourself.

## Mental model

A chain passes one value, `ctx`, through its interceptors:

```text
ctx -> Interceptor -> Interceptor -> result
```

Each interceptor may define:

- `enter(ctx)`: forward execution, in chain order
- `leave(ctx)`: unwind in reverse order after success
- `error(ctx, err)`: unwind in reverse order after a failure

A stream stage expands one value into many items, runs each item through a
child chain, then collects the results back into one value:

```text
ctx -> stream() -> items -> child chain per item -> collect() -> result
```

Add a policy with `.on(...)` when a chain needs a named thread lane, a worker
pool, or an event loop. A chain without a policy inherits its parent's.

## When to use what

| Need | Use |
| --- | --- |
| One-in, one-out step | `Interceptor` |
| Split and merge data | `StreamInterceptor` |
| Compose workflow steps | `chain(...)` |
| Compose a stream stage | `stream_chain(...)` |
| Run a sync-only workflow | `Runtime.run_sync(...)` |
| Run a workflow from async code | `await Runtime.run_async(...)` |
| Run an async workflow from sync code with no running event loop, such as a sync FastAPI route | `Runtime.run_blocking(...)` |
| Validate a workflow before the first request | `Runtime.compile(...)` |
| Control placement | `ThreadPolicy`, `ThreadPoolPolicy`, `AsyncPolicy` |

## Public API

Everything below is importable from `py_interceptors`.

| Name | What it is |
| --- | --- |
| `Interceptor`, `StreamInterceptor` | Base classes for steps and stream stages |
| `chain`, `stream_chain` | Typed builders |
| `Chain`, `StreamChain` | The immutable chain types the builders return |
| `Runtime` | Compiles and runs chains; owns threads, event loops and the plan cache |
| `CompiledPlan` | Returned by `Runtime.compile(...)`; has `run_sync` and `run_async` |
| `ThreadPolicy`, `ThreadPoolPolicy`, `AsyncPolicy` | Placement policies for `.on(...)` |
| `Policy` | Type alias for the union of the three policies |
| `ExecutionEvent`, `Observer` | Observer payload and callback type for `Runtime.add_observer(...)` |
| `ValidationError` | Raised by `compile` for type-flow, metadata, stream-shape and policy-name problems |
| `DependencyError` | Subclass of `ValidationError`; base of the four dependency errors |
| `UnknownDependencyError`, `DependencyTypeError`, `MissingDependencyError`, `AmbiguousDependencyError` | Dependency failures, described in [Dependencies](docs/dependencies.md) |
| `ExecutionError` | Raised when a valid plan cannot run as asked, for example `run_sync` on a plan with async steps |

## Deeper docs

- [Interceptor lifecycle](docs/interceptor-lifecycle.md): `enter`, `leave`,
  `error`, unwind order, metadata, and chain composition.
- [Stream interceptors](docs/stream-interceptors.md): `stream`, `collect`,
  `error`, and building stream stages with `stream_chain(...)`.
- [Runtime and policies](docs/runtime-and-policies.md): `run_sync`,
  `run_async`, `run_blocking`, compilation and caching, policies, placement
  rules, and stream fan-out concurrency.
- [Dependencies](docs/dependencies.md): declaring interceptor dependencies,
  binding them with `.use(Cls, **kwargs)`, and supplying them with
  `.provide(...)`.
- [Observability](docs/observability.md): execution events, observers, and
  exception notes.
- [Organizing workflows](docs/organizing-workflows.md): a suggested package
  layout for larger projects.

## Examples

- [csv_import_pipeline.py](examples/csv_import_pipeline.py): sync stream
  split/map/collect, with `AcceptedRow | RejectedRow` as the collect type.
- [external_api_fanout.py](examples/external_api_fanout.py): async fan-out
  through an isolated `AsyncPolicy`.
- [cities_countries_continents.py](examples/cities_countries_continents.py):
  a stream stage inside a
  `ThreadPolicy -> AsyncPolicy -> ThreadPolicy` workflow.
- [fastapi_pipeline.py](examples/fastapi_pipeline.py): one workflow behind an
  `async def` route that calls `run_async` and a `def` route that calls
  `run_blocking`, with the runtime opened in the app lifespan and an error
  boundary that maps known errors to HTTP responses and re-raises the rest.

Each example has a test under `tests/`, so `uv run pytest` runs all of them.
The FastAPI example needs the dev dependencies (`uv sync`).
