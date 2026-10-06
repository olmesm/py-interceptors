import asyncio
import threading
from dataclasses import dataclass, field

import pytest

from py_interceptors import ExecutionError, Interceptor, Runtime, ThreadPolicy, chain


@dataclass
class Trace:
    threads: list[str] = field(default_factory=list)


class LaneStep(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    def enter(self, ctx: Trace) -> Trace:
        ctx.threads.append(threading.current_thread().name)
        return ctx


class LoopStep(Interceptor[Trace, Trace]):
    input_type = Trace
    output_type = Trace

    async def enter(self, ctx: Trace) -> Trace:
        await asyncio.sleep(0)
        ctx.threads.append(threading.current_thread().name)
        return ctx


workflow = (
    chain("blocking")
    .use(chain("lane").use(LaneStep).on(ThreadPolicy("lane")).build())
    .use(LoopStep)
    .build()
)


def test_run_blocking_rejects_a_running_loop() -> None:
    with Runtime() as runtime:

        async def inside_loop() -> Trace:
            return runtime.run_blocking(workflow, Trace())

        with pytest.raises(ExecutionError, match="running event loop"):
            asyncio.run(inside_loop())


def test_run_blocking_blocks_the_caller_and_runs_the_chain_elsewhere() -> None:
    caller = threading.current_thread().name

    with Runtime() as runtime:
        result = runtime.run_blocking(workflow, Trace())

    lane, loop = result.threads
    assert lane.startswith("lane")
    assert caller not in (lane, loop)
    assert loop != lane
