# Dependencies

Interceptors often need collaborators: a logger, an auth client, a database
connection, a feature flag service. Constructing those inside the interceptor
makes it hard to test in isolation and forces every chain to share one
instance. Instead, an interceptor declares what it needs as type-annotated
class attributes, and the chain that runs it supplies the values.

The runtime creates a new instance of each interceptor class for every run
and sets the resolved values as attributes on it before `enter` runs.

## Two ways to supply a dependency

### 1. Direct binding at `.use(Cls, **kwargs)`

Pass the value alongside the class. The binding applies to this one step.

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

`.use(...)` checks the keyword arguments immediately. A name that is not a
dependency of the class raises `UnknownDependencyError`, and a value whose
type does not match the annotation raises `DependencyTypeError`.

### 2. Ambient binding via `.provide(**kwargs)`

When several steps share a collaborator, declare it once on the chain or any
ancestor, and every interceptor nested under that chain can resolve it.

```python
shared_logger = Logger(name="api")

workflow = (
    chain("api")
    .provide(logger=shared_logger)
    .use(AuthCheck, auth=AuthClient(token="..."))
    .use(AuditLog)  # declares `logger: Logger`
    .use(Render)    # declares `logger: Logger`
    .build()
)
```

`.provide(...)` does not bind to a specific step. It adds a scope that every
step inside the chain consults during resolution. Calling it twice on one
chain merges the values, and the later call wins for a repeated name.

## Resolution rules

`Runtime.compile(...)` resolves every declared dependency of every step, so
dependency errors surface before the first run. For each dependency the
resolver checks, in order:

1. **Direct binding**: a keyword argument passed to `.use(Cls, **kwargs)` for
   this step always wins.
2. **Nearest `.provide(...)` scope**: walk from the innermost chain outward.
   At each scope:
   1. **Name match**: if a key matches the attribute name, use its value,
      raising `DependencyTypeError` if the type is wrong.
   2. **Type-only fallback**: otherwise, if exactly one value in this scope
      matches the annotation, use it. If two or more match, raise
      `AmbiguousDependencyError`.
3. **Class-level default**: if the class or a base class assigns a value to
   the attribute, keep that value.
4. **Missing**: otherwise `compile` raises `MissingDependencyError`.

Each scope is checked completely before the next one out, so a type-only
match in an inner chain wins over a name match in an outer chain, and a
value provided by an inner chain shadows the same name provided further out.

## What counts as a dependency

An attribute of an interceptor class is an injectable dependency when:

- It has a class-level type annotation, on the class or a base class.
- Its name is not one of the reserved fields `name`, `input_type` and
  `output_type`.
- Its name does not start with an underscore.
- The annotation is not a `ClassVar`.

If the class or a base class (other than `Interceptor` itself) also assigns a
value, the dependency is optional: nothing is raised when no binding is
found, and both `.use(Cls, attr=...)` and `.provide(...)` can replace the
default.

```python
class AuditLog(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    logger: Logger = Logger(name="fallback")  # optional, can be overridden

    def enter(self, ctx: Request) -> Request:
        self.logger.log(f"user={ctx.user_id}")
        return ctx
```

## Matching values to annotations

A value matches an annotation when its class is a subclass of the annotated
class. Unions match when the value matches any member, so `logger: Logger |
None` accepts a `Logger` or `None`. Such a dependency is still required unless
the class gives it a default, so bind or provide `None` explicitly when that
is the value you want.
`Any` and `object` accept every value. Generic annotations are checked by
their origin only: `items: list[int]` accepts any `list`, and
`items: Sequence[int]` accepts a `list` or a `tuple`.

Annotations are evaluated with `typing.get_type_hints`. If that fails, for
example because the annotation names a type imported only under
`if TYPE_CHECKING:`, the annotations stay strings. A dependency with a string
annotation is matched by name only, and its value's type is not checked.

## Several dependencies of the same type

When one `.provide(...)` scope holds a single `Logger`, type-only matching
gives it to every `Logger` attribute. When it holds two, type-only matching
raises `AmbiguousDependencyError`. Provide each value under the attribute's
name:

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

These values are visible to every step under `x`. To give them to this one
step only, bind them at the call site:

```python
chain("x").use(TwoLoggers, primary=p, secondary=s)
```

There are no qualifier annotations: matching is by attribute name or by type.

## Testing sub-chains with `.provide(...)`

A sub-chain whose interceptors declare their collaborators can be tested by
wrapping it in a chain that provides fakes:

```python
# production code
def order_pipeline() -> Chain[Request, Response]:
    return (
        chain("orders")
        .use(AuthCheck)  # needs `auth: AuthClient`
        .use(AuditLog)   # needs `logger: Logger`
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

## Sub-chains that pin their own values

A sub-chain that calls `.provide(...)` itself shadows anything its callers
provide under the same name, so a test cannot swap that value from outside:

```python
def pinned() -> Chain[Request, Response]:
    return (
        chain("pinned")
        .provide(logger=Logger(name="prod"))  # callers cannot override this
        .use(AuditLog)
        .build()
    )
```

For a sub-chain whose callers should choose the values, leave the
dependencies unbound in the sub-chain and provide them from an ancestor.

## Errors

All four errors subclass `DependencyError`, which subclasses
`ValidationError`. All are exported from `py_interceptors`.

| Error | Raised by | When |
|---|---|---|
| `UnknownDependencyError` | `.use(Cls, foo=...)` | `foo` is not a dependency of `Cls` (see [What counts as a dependency](#what-counts-as-a-dependency)) |
| `DependencyTypeError` | `.use(...)` or `compile` | A bound or provided value does not match the annotation |
| `MissingDependencyError` | `compile` | A dependency without a default has no binding |
| `AmbiguousDependencyError` | `compile` | Type-only matching finds two or more values in one `.provide(...)` scope |
