# Dependencies

Interceptors often need collaborators: a logger, an auth client, a database
connection, a feature flag service. Hard-coding those into the interceptor
makes it impossible to test in isolation and forces every chain to share the
same instance. The dependency system lets an interceptor *declare* what it
needs via type-annotated attributes, then *receive* those collaborators from
the chain that runs it.

## Two Ways to Supply a Dependency

### 1. Direct binding at `.use(Cls, **kwargs)`

Pass the value alongside the class. Bindings are resolved at chain compile
time and validated against the interceptor's annotations.

```python
class AuthCheck(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    auth: AuthClient  # required dependency

    def enter(self, ctx: Request) -> Request:
        self.auth.assert_valid(ctx.token)
        return ctx


workflow = (
    chain("api")
    .use(AuthCheck, auth=AuthClient(token="..."))
    .use(Render)
    .build()
)
```

Unknown kwargs raise `UnknownDependencyError` immediately at the `.use(...)`
call site, and a kwarg whose value does not match the annotated type raises
`DependencyTypeError`.

### 2. Ambient binding via `.provide(**kwargs)`

Sometimes the same collaborator is shared across many steps. Declare it once
on the chain (or any ancestor) and every descendant interceptor that
annotates a matching attribute will receive it.

```python
shared_logger = Logger(name="api")

workflow = (
    chain("api")
    .provide(logger=shared_logger)
    .use(AuthCheck, auth=AuthClient(token="..."))
    .use(AuditLog)         # declares `logger: Logger`
    .use(Render)            # declares `logger: Logger`
    .build()
)
```

`.provide(...)` does not bind to a specific step; it pushes a scope onto the
provide stack that every nested step can consult during resolution.

## Resolution Rules

For each declared dependency on each interceptor, the resolver checks, in
order:

1. **Direct binding** — a kwarg passed to `.use(Cls, **kwargs)` for this
   exact step always wins.
2. **Nearest `.provide(...)` ancestor** — walk from the innermost chain
   outward. At each scope:
   1. **Name match** — if a key in `.provide(...)` matches the attribute
      name, use it (and raise `DependencyTypeError` if the type is wrong).
   2. **Type-only fallback** — otherwise, if exactly one value in the same
      scope matches the annotation, use it. If two or more match, raise
      `AmbiguousDependencyError`.
3. **Class-level default** — if the annotation has a default value on the
   class (or any class in its MRO), the default is kept.
4. **Missing** — otherwise raise `MissingDependencyError` at compile time.

"Nearest wins" means an inner chain can shadow an outer one. If both supply
`logger=`, the inner value is the one each step inside that inner chain
sees.

## What Counts as a Dependency

An interceptor attribute is treated as an injectable dependency when:

- It has a class-level type annotation.
- The attribute name is not one of the reserved framework fields
  (`name`, `input_type`, `output_type`).
- The name does not start with an underscore.
- The annotation is not a `ClassVar`.

If the class also assigns a default value to that attribute (anywhere in the
MRO except the base `Interceptor`), the dependency is **optional**: missing
bindings do not raise, but `.provide(...)` can still override the default.

```python
class AuditLog(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    logger: Logger = Logger(name="fallback")  # optional, can be overridden

    def enter(self, ctx: Request) -> Request:
        self.logger.log(f"user={ctx.user_id}")
        return ctx
```

## Multiple Fields of the Same Type

When an interceptor declares two fields with the same annotation, the
resolver cannot use type-only matching to choose between them — it would
always be ambiguous. Bind by name explicitly:

```python
class TwoLoggers(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    primary: Logger
    secondary: Logger

    def enter(self, ctx: Request) -> Request:
        self.primary.log("p")
        self.secondary.log("s")
        return ctx


workflow = (
    chain("x")
    .provide(primary=Logger(name="p"), secondary=Logger(name="s"))
    .use(TwoLoggers)
    .build()
)
```

If you must inject same-typed collaborators without picking names, bind at
the call site instead:

```python
chain("x").use(TwoLoggers, primary=p, secondary=s)
```

There are no wireup-style qualifier annotations in this release; bind by
name and you are done.

## Testing Sub-chains with `.provide(...)`

The most useful side effect of `.provide(...)` is that any sub-chain whose
interceptors depend on collaborators can be exercised under a test by simply
wrapping it in a chain that supplies fakes:

```python
# production code
def order_pipeline() -> Chain[Request, Response]:
    return (
        chain("orders")
        .use(AuthCheck)       # needs `auth: AuthClient`
        .use(AuditLog)        # needs `logger: Logger`
        .use(SaveOrder)
        .build()
    )


# test code
def test_order_pipeline() -> None:
    fake_auth = FakeAuthClient()
    fake_logger = Logger(name="test")

    workflow = (
        chain("test")
        .provide(auth=fake_auth, logger=fake_logger)
        .use(order_pipeline())
        .build()
    )
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(...))

    assert fake_logger.events == [...]
```

No constructor patching, no mocks at import time — just a chain that
supplies the deps the inner chain expects.

## Footgun: Inner Shadowing

Because resolution is "nearest wins", `.provide(...)` inside a sub-chain
will shadow the same key from a parent. If a sub-chain pins its own
dependencies, callers can no longer override them from above:

```python
# This sub-chain is hard to test because it pins `logger` itself.
def pinned() -> Chain[Request, Response]:
    return (
        chain("pinned")
        .provide(logger=Logger(name="prod"))  # shadows everything above
        .use(AuditLog)
        .build()
    )
```

For sub-chains that you want callers to be able to override, leave their
dependencies declared but unbound and let an ancestor supply them.

## Errors

All errors below subclass `ValidationError`:

| Error | When |
|---|---|
| `UnknownDependencyError` | `.use(Cls, foo=...)` where `foo` is not annotated on `Cls` |
| `DependencyTypeError` | A bound or provided value's type does not match the annotation |
| `MissingDependencyError` | A required (no-default) annotation could not be resolved |
| `AmbiguousDependencyError` | Type-only fallback in a single `.provide(...)` scope matches two or more values |

All four are exported from `py_interceptors`.
