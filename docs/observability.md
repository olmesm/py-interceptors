# Observability

`Runtime.add_observer(callback)` registers a function that receives an
`ExecutionEvent` after every stage: `enter`, `leave` and `error` for
interceptors, and `stream`, `collect` and `error` for stream interceptors.
It returns the runtime, so calls can be chained. The library does no logging
of its own.

```python
import logging

from py_interceptors import ExecutionEvent, Runtime

logger = logging.getLogger("workflows")


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

Observers are called in the order they were added, on the thread that ran the
stage, before the next stage starts. An observer that is slow slows the
workflow.

## Event fields

| Field | Value |
| --- | --- |
| `execution_id` | `int`, numbered from 1 per `Runtime`; every stage of one `run_*` call shares it, including stages on other threads |
| `chain` | name of the chain or stream chain that owns the stage |
| `path` | tuple of chain names from the root to `chain`, e.g. `('orders', 'enrich')`. A stream stage mapped over a bare interceptor class adds a `'stream-map'` entry |
| `stage` | `'enter'`, `'leave'`, `'error'`, `'stream'` or `'collect'` |
| `step` | the interceptor's `name`, or its class name when `name` is unset |
| `policy` | the effective policy as a string, e.g. `"ThreadPolicy('main')"`, `"ThreadPoolPolicy('api', workers=8)"`, `"AsyncPolicy()"`, or `None` |
| `thread` | `threading.current_thread().name` where the stage ran |
| `elapsed_ms` | wall time of the stage in milliseconds |
| `error` | the exception the stage raised, or `None` |

Events carry no payload: the context values passing through the chain are
not included.

## Exception notes

When a stage raises an `Exception`, the runtime adds a note to it with
`add_note` and re-raises the same object. The note names the stage:

```text
py_interceptors chain='f' path=('f',) step='Boom' stage='enter' policy=None
```

Each stage the exception passes through adds its own note. If `Boom.enter`
raises and an outer interceptor's `error` re-raises it, the exception that
reaches the caller carries two notes, the second ending in
`step='Outer' stage='error'`. Python prints the notes under the traceback.

## Observer exceptions

Observers run inside the stage, so an exception raised by an observer changes
what the workflow does:

- After a failed stage, the observer's exception propagates in place of the
  stage's exception. The stage's exception, with its note, is available as
  the new exception's `__context__`. Outer `error` handlers receive the
  observer's exception.
- After a successful stage, the runtime treats the stage as having raised the
  observer's exception, and no failure event is emitted for it. For an
  `enter` stage this means the interceptor is not added to the unwind stack,
  so its own `leave` and `error` do not run, and outer `error` handlers
  receive the observer's exception.
- Observers added after the one that raised do not receive that event.

Catch exceptions inside any observer that must not affect the workflow.
