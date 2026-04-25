# Interceptor Lifecycle

`Interceptor` is the basic one-in, one-out workflow step. Each interceptor
declares the type it expects and the type it returns.

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

- `enter(ctx)` runs in chain order.
- `leave(ctx)` runs in reverse order after successful forward execution.
- `error(ctx, err)` runs during error unwind.

`enter`, `leave`, and `error` may be sync or async.

## Enter And Leave Stack

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

Each `leave` receives the current context from the forward path or from the
inner `leave` that ran before it. It can update or replace that context before
returning it to the next outer interceptor.

This makes `leave` useful for wrapping behavior around inner work:

- timing and tracing
- response decoration
- cleanup after successful work
- final normalization before returning to the caller

## Error Handling

Only successfully entered interceptors participate in the return path.

If an `enter` fails, the runtime starts error unwind over the already-entered
stack. If an `error` handler returns normally, the error is handled and the
remaining outer stack resumes through `leave`. If `leave` itself raises,
execution switches to error unwind for the remaining outer stack.

If an `error` handler raises, the raised exception becomes the active error for
the next outer interceptor.

## Names And Metadata

Interceptors may set an optional `name`. If omitted, observers use the class
name. If provided, the name must be a non-empty string.

Every interceptor class used in a validated chain must declare:

- `input_type`
- `output_type`

Use lowercase metadata names. Legacy `Input` and `Output` metadata is rejected
during validation.

## Composition

Use the lowercase builders when possible:

```python
workflow = (
    chain("cities -> continents")
    .use(CitiesToDataFrame)
    .use(AddCountry)
    .use(GroupByContinent)
    .build()
)
```

`Chain(...)` and `StreamChain(...)` remain available when you want explicit
constructor-based composition. The lowercase builders are preferred when you
want mypy to infer the workflow type without a variable annotation.

All chains and stream chains require a non-empty name, whether built with
`chain(...)` / `stream_chain(...)` or constructed directly with `Chain(...)` /
`StreamChain(...)`.
