import asyncio

from _example_loader import load_example_module

from py_interceptors import Runtime


def test_external_api_fanout_example_builds_customer_report() -> None:
    example = load_example_module("external_api_fanout")
    runtime = Runtime()

    try:
        result = asyncio.run(
            runtime.run_async(example.workflow, example.CustomerIds(ids=[3, 1, 99]))
        )
    finally:
        runtime.shutdown()

    assert [profile.customer_id for profile in result.profiles] == [1, 3, 99]
    assert [profile.name for profile in result.profiles] == [
        "Ada",
        "Grace",
        "Unknown",
    ]
    assert result.premium_count == 2
