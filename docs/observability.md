# Observability

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

Events include:

- execution ID
- chain name
- nested path
- interceptor or stream interceptor name
- stage name
- policy label
- thread name
- elapsed time
- optional error

Context payloads are not included in observer events. Observer exceptions
propagate to the caller.

When a stage fails, the original exception is preserved and annotated with a
Python exception note containing the chain, path, step, stage, and policy.
