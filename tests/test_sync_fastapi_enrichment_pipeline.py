from __future__ import annotations

import threading

import pytest
from _example_loader import load_example_module
from fastapi.testclient import TestClient

from py_interceptors import Runtime
from py_interceptors.errors import ExecutionError


def _post_enrich(client: TestClient) -> dict[str, object]:
    response = client.post(
        "/enrich?library=alpha&library=beta",
        content="AAPL,MSFT,GOOG",
        headers={"content-type": "text/plain"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def test_sync_route_drives_async_chain_via_run_blocking() -> None:
    example = load_example_module("sync_fastapi_enrichment_pipeline")
    app = example.create_app()

    with TestClient(app) as client:
        runtime = example.get_runtime(app)

        # The plan was compiled at lifespan startup, but resources are lazy:
        # no portal exists before the first request.
        assert len(runtime._compiled_plans) == 1
        assert runtime._async_portals == {}

        body = _post_enrich(client)

        # The default portal is now alive and reusable across requests.
        portal_names = set(runtime._async_portals.keys())
        assert "py-interceptors-default" in portal_names
        default_portal = runtime._async_portals["py-interceptors-default"]
        assert default_portal._thread.is_alive()

        # A second request reuses the same portal — no per-request loop.
        body_again = _post_enrich(client)
        assert runtime._async_portals["py-interceptors-default"] is default_portal

        # Payload comes from dispatch_libraries and symbols.
        payload = body["payload"]
        assert isinstance(payload, str)
        for library in ("alpha", "beta"):
            for symbol in ("AAPL", "MSFT", "GOOG"):
                assert f"{library}:{symbol}={library}:{symbol}" in payload

        assert body == body_again

    # Lifespan shutdown stops the portal.
    assert not default_portal._thread.is_alive()


def test_thread_segments_share_one_frame_lane() -> None:
    example = load_example_module("sync_fastapi_enrichment_pipeline")

    with TestClient(example.create_app()) as client:
        body = _post_enrich(client)

    trace = body["trace"]
    assert isinstance(trace, list)

    # Pull the thread name out of each "label@thread" entry.
    labels_threads = [item.rsplit("@", 1) for item in trace]
    by_label = {label: thread for label, thread in labels_threads}

    frame_steps = [
        "deserialize",
        "validate",
        "convert-match",
        "match",
        "convert-full",
        "build-mapped",
        "serialize",
    ]
    frame_threads = {by_label[step] for step in frame_steps}

    # All thread-policy steps share the single "enrichment-frame" lane.
    # The ThreadPoolExecutor suffixes the worker name with an index, but
    # because ThreadPolicy is single-worker the suffix is always "_0".
    assert len(frame_threads) == 1
    only_frame_thread = next(iter(frame_threads))
    assert only_frame_thread.startswith("enrichment-frame")

    # Async steps run on the io portal, not on the frame lane and not on
    # the caller's worker thread.
    io_steps = ["authorize", "fetch-match", "fetch-full"]
    for step in io_steps:
        assert by_label[step] == "enrichment-io"
        assert by_label[step] != only_frame_thread


def test_validation_failures_surface_to_caller() -> None:
    example = load_example_module("sync_fastapi_enrichment_pipeline")

    with TestClient(example.create_app()) as client:
        # Missing libraries query parameter.
        response = client.post(
            "/enrich",
            content="AAPL",
            headers={"content-type": "text/plain"},
        )

    assert response.status_code == 422
    assert response.json() == {"error": "at least one library is required"}


def test_run_blocking_rejects_running_loop() -> None:
    example = load_example_module("sync_fastapi_enrichment_pipeline")
    workflow = example.workflow

    runtime = Runtime()
    try:
        import asyncio

        async def call_from_inside_loop() -> None:
            payload = example.EnrichmentRequest(raw_body=b"AAPL", libraries=["alpha"])
            runtime.run_blocking(workflow, payload)

        with pytest.raises(ExecutionError, match="running event loop"):
            asyncio.run(call_from_inside_loop())
    finally:
        runtime.shutdown()


def test_run_blocking_caller_thread_blocks_until_done() -> None:
    """The calling thread blocks; the work runs on the portal thread."""

    example = load_example_module("sync_fastapi_enrichment_pipeline")
    workflow = example.workflow

    runtime = Runtime()
    try:
        caller_thread = threading.current_thread().name
        payload = example.EnrichmentRequest(raw_body=b"AAPL,MSFT", libraries=["alpha"])

        result = runtime.run_blocking(workflow, payload)

        # The serialize step (last thread-policy step before return) ran on
        # the frame lane, not on the caller thread.
        labels_threads = dict(item.rsplit("@", 1) for item in result.trace)
        assert labels_threads["serialize"].startswith("enrichment-frame")
        assert labels_threads["serialize"] != caller_thread
        assert labels_threads["fetch-match"] == "enrichment-io"
    finally:
        runtime.shutdown()
