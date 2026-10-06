import asyncio

from examples import external_api_fanout as example

from py_interceptors import Runtime


def test_external_api_fanout_example_builds_customer_report() -> None:
    with Runtime() as runtime:
        result = asyncio.run(runtime.run_async(example.workflow, [3, 1, 99]))

    assert [(p.customer_id, p.name) for p in result.profiles] == [
        (1, "Ada"),
        (3, "Grace"),
        (99, "Unknown"),
    ]
    assert result.premium_count == 2
