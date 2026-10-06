# Stream interceptors

A stream stage turns one value into many items, runs each item through a
child chain, and merges the results back into one value. The splitting and
merging live in a `StreamInterceptor`; the per-item work is an ordinary
`Interceptor` or `Chain`.

```text
ctx -> stream(ctx) -> items -> mapped chain per item -> collect(ctx, results) -> result
```

## Writing a stream interceptor

`StreamInterceptor[TIn, TEmit, TCollect, TOut]` has four type parameters and
four matching class attributes, all required:

| Attribute | Meaning |
| --- | --- |
| `input_type` | what the stage receives from the previous step |
| `emit_type` | what `stream` yields for each item; the mapped chain's input |
| `collect_type` | what the mapped chain returns per item; what `collect` receives |
| `output_type` | what `collect` (or `error`) returns to the next step |

It has three methods:

- `stream(ctx)` returns the items, as any iterable or, under `run_async` or
  `run_blocking`, an async iterable (an `async def` generator works). The
  runtime reads all of them into a list before the first item is mapped, so a
  generator is consumed completely up front.
- `collect(ctx, items)` receives the stage's input `ctx` and the list of
  mapped results, in the order `stream` produced the items, and returns the
  stage's output.
- `error(ctx, err)` runs when `stream`, any mapped item, or `collect` raises.
  `ctx` is the stage's input. Whatever `error` returns becomes the stage's
  result and the workflow continues with it; the default re-raises `err`.

`stream` and `collect` must be overridden. There is no `leave` stage. Any of
the three methods may be `async def`.

## Building a stream stage

`stream_chain(name)` starts the builder. `.stream(...)` takes the
`StreamInterceptor` class, `.map(...)` takes an `Interceptor` class or a
`Chain` to run per item, and `.build()` returns the `StreamChain`. `.on(...)`
can be added at any point to place the whole stage.

A stream chain does not run on its own: `Runtime` runs `Chain` objects, so
add the stream chain to a chain with `.use(...)`.

```python
from collections.abc import Iterable
from dataclasses import dataclass

from py_interceptors import Interceptor, Runtime, StreamInterceptor, chain, stream_chain


@dataclass
class Batch:
    lines: list[str]


@dataclass
class Report:
    total: int
    error: str | None = None


class SplitLines(StreamInterceptor[Batch, str, int, Report]):
    input_type = Batch
    emit_type = str
    collect_type = int
    output_type = Report

    def stream(self, ctx: Batch) -> Iterable[str]:
        return ctx.lines

    def collect(self, ctx: Batch, items: Iterable[int]) -> Report:
        return Report(total=sum(items))

    def error(self, ctx: Batch, err: Exception) -> Report:
        return Report(total=0, error=str(err))


class ParseLine(Interceptor[str, int]):
    input_type = str
    output_type = int

    def enter(self, ctx: str) -> int:
        return int(ctx)


parse_lines = stream_chain("parse lines").stream(SplitLines).map(ParseLine).build()
workflow = chain("import").use(parse_lines).build()

with Runtime() as runtime:
    print(runtime.run_sync(workflow, Batch(["1", "2", "4"])))
    # Report(total=7, error=None)
    print(runtime.run_sync(workflow, Batch(["1", "x"])))
    # Report(total=0, error="invalid literal for int() with base 10: 'x'")
```

In the second run `ParseLine` raises `ValueError` for `"x"`, the runtime
passes it to `SplitLines.error`, and the `Report` that `error` returns is the
workflow's result.

To run several steps per item, map a chain:
`.map(chain("per line").use(Strip).use(ParseLine).build())`. A bare
interceptor class passed to `.map(...)` is wrapped in a chain named
`stream-map`, which is the name observers see in `ExecutionEvent.path`.

`StreamChain` has no `.provide(...)`. Mapped steps resolve dependencies from
the chains around the stream stage, or from a mapped chain that calls
`.provide(...)` itself.

## Validation

`compile` checks the stage's types like any other step. The previous step's
output must match `input_type`, `emit_type` must match the mapped chain's
input, and the mapped chain's output must match `collect_type`:

```text
ValidationError: StreamChain 'g' collector expects int but mapped pipeline returns str
```

It also raises `ValidationError` when `.stream(...)` or `.map(...)` was never
called or when the stream interceptor does not override `stream` or
`collect`. Calling `.stream(...)` twice raises `ValueError`.

## Concurrency

Without a policy, a sync mapped chain processes one item at a time, and an
async mapped chain under `run_async` processes all items at once. A
`ThreadPoolPolicy` on the stream chain or the mapped chain runs up to
`workers` items at once, and a failing item stops or cancels the rest. The full rules are in
[Runtime and policies](runtime-and-policies.md#stream-fan-out-concurrency).
