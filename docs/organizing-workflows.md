# Organizing Workflows

Interceptor-heavy applications can become hard to navigate if every step lands
in one global `interceptors.py` file. A better default is to organize by
workflow or use case, with reusable cross-cutting interceptors kept separately.

## Recommended Layout

```text
app/
  workflows/
    orders/
      __init__.py
      context.py
      workflow.py
      interceptors/
        auth.py
        validation.py
        persistence.py
        response.py
      streams/
        pricing.py
      policies.py
      tests/
        test_workflow.py
        test_validation.py

    imports/
      context.py
      workflow.py
      interceptors/
      streams/
      policies.py
```

For each workflow package:

- `context.py`: dataclasses that flow through the chain.
- `workflow.py`: chain composition only, with no business logic.
- `interceptors/`: units of work grouped by concern.
- `streams/`: stream interceptors and mapped child chains.
- `policies.py`: named policy choices for this workflow.
- `tests/`: tests next to the workflow, or mirrored under the project-level
  `tests/` directory.

## Workflow Composition

The composition file should read like the workflow. If it contains branching
logic, HTTP calls, SQL, dataframe manipulation, or other detailed work, it is
doing too much.

```python
# workflows/orders/workflow.py

from py_interceptors import chain

from .context import OrderContext
from .interceptors.auth import Authenticate
from .interceptors.validation import ValidateOrder
from .interceptors.persistence import CreateOrder
from .interceptors.response import ResponseBoundary
from .policies import order_policy


order_workflow = (
    chain("orders.create")
    .use(ResponseBoundary)
    .use(Authenticate)
    .use(ValidateOrder)
    .use(CreateOrder)
    .on(order_policy)
    .build()
)
```

## Boundary Guidance

Keep workflow-specific interceptors close to the workflow they serve. Keep
general-purpose interceptors in a shared package only when they are genuinely
reusable across workflows.

Good shared interceptors include:

- request or execution IDs
- tracing and metrics
- authentication boundaries
- error mapping boundaries
- body parsing or response rendering

Workflow-specific interceptors should usually stay with the workflow:

- `ValidateOrder`
- `CreateOrder`
- `LoadCustomer`
- `ReserveInventory`
- `FetchImportFile`
- `NormalizeRows`

## Domain Boundaries

Interceptors should usually orchestrate work rather than contain all business
logic. In larger applications, keep domain rules and external adapters outside
the interceptor classes:

```text
orders/
  domain/
    model.py
    services.py
    errors.py
  workflows/
    create/
      context.py
      workflow.py
      interceptors/
  adapters/
    repository.py
    payment_gateway.py
```

The workflow layer wires the steps together. Interceptors call domain services,
ports, repositories, or adapters through objects on the context. The domain
layer should not need to know about `Chain`, `Runtime`, policies, or
interceptor lifecycle methods.
