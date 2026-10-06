# Interceptor lifecycle

`Interceptor` is the one-in, one-out workflow step. Each interceptor declares
the type it accepts and the type it returns.

```python
class AddTax(Interceptor[Order, Order]):
    name = "add-tax"
    input_type = Order
    output_type = Order

    def enter(self, ctx: Order) -> Order:
        ...

    def leave(self, ctx: Order) -> Order:
        ...

    def error(self, ctx: Order, err: Exception) -> Order:
        ...
```

All three methods are optional. The defaults return `ctx` unchanged from
`enter` and `leave`, and re-raise `err` from `error`. Any of them may be
`async def`; a chain that contains an async method must run with
`run_async(...)` or `run_blocking(...)`.

The runtime creates a new instance of the class each time the chain runs, so
`enter`, `leave` and `error` of one run share `self`, and nothing on `self`
carries over to the next run. The class must be constructible with no
arguments; if `cls()` raises, the run fails with `ExecutionError`.
Collaborators are injected as attributes instead (see
[Dependencies](dependencies.md)).

## Enter and leave order

`enter` runs in chain order. Each interceptor whose `enter` returns is pushed
onto a stack. When the forward path completes, `leave` runs in reverse order:

```python
workflow = (
    chain("trace")
    .use(Outer)
    .use(Inner)
    .build()
)

# Execution order:
# Outer.enter
# Inner.enter
# Inner.leave
# Outer.leave
```

The innermost `leave` receives the value the forward path ended with. Each
`leave` returns the value for the next outer interceptor. That makes `leave` the place for work that wraps the inner steps:

- timing and tracing
- response decoration
- cleanup after successful work
- final normalization before returning to the caller

## Error handling

Only interceptors whose `enter` returned take part in the unwind. If an
`enter` raises, that interceptor's own `leave` and `error` do not run; the
runtime calls `error(ctx, err)` on the interceptors already on the stack, in
reverse order.

- If an `error` handler returns a value, the error is handled. The value
  becomes the context, and the remaining outer interceptors run `leave`.
- If an `error` handler raises, the raised exception becomes the error passed
  to the next outer interceptor's `error`.
- If a `leave` raises, the remaining outer interceptors switch to `error`.

If no handler recovers, the last exception propagates to the caller of
`run_sync`, `run_async` or `run_blocking`.

## Names and metadata

An interceptor may set `name`. Observers and exception notes use it, or the
class name when it is unset. A `name` that is set must be a non-empty string;
otherwise `compile` raises `ValidationError`.

`input_type` and `output_type` are required. They may be set on the class or
inherited from a base class of your own; the `object` defaults on
`Interceptor` itself do not count. `compile` raises `ValidationError` when one
is missing, and also when an item's `input_type` does not accept the previous
item's output.

## Composition

```python
workflow = (
    chain("orders")
    .use(ParseOrder)
    .use(AddTax)
    .use(SaveOrder)
    .build()
)
```

`.use(...)` accepts an interceptor class, another `Chain`, or a
`StreamChain`. A nested chain runs as one step: its interceptors enter and
leave inside the parent's position, and its own `.on(...)` and
`.provide(...)` apply to it and its descendants. Chains are immutable;
`.use`, `.on` and `.provide` return a new chain, and `.build()` returns the
chain unchanged.

`Chain[...]` and `StreamChain[...]` can be constructed directly when you
write the type parameters yourself:

```python
workflow = Chain[str, int]("parse").use(ParseInt)
```

Every chain and stream chain needs a non-empty name. An empty name raises
`ValueError` when the chain is constructed, whether through `chain(...)`,
`stream_chain(...)`, `Chain(...)` or `StreamChain(...)`.
