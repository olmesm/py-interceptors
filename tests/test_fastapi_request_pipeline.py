from _example_loader import load_example_module
from fastapi.testclient import TestClient


def test_fastapi_pipeline_creates_order() -> None:
    example = load_example_module("fastapi_request_pipeline")
    app = example.create_app()

    with TestClient(app) as client:
        runtime = example.get_runtime(app)
        assert len(runtime._compiled_plans) == 1
        assert runtime._async_portals == {}

        response = client.post(
            "/orders",
            headers={"Authorization": "Bearer secret-token"},
            json={"item_id": "sku-123", "quantity": 2},
        )
        portal_thread = runtime._async_portals["fastapi-order"]._thread

        assert portal_thread.name == "fastapi-order"
        assert portal_thread.is_alive()

    assert response.status_code == 201
    assert response.headers["x-workflow"] == "py-interceptors"
    assert response.json() == {
        "order_id": "ord-user-123-sku-123",
        "item_id": "sku-123",
        "quantity": 2,
    }
    assert not portal_thread.is_alive()


def test_fastapi_pipeline_maps_auth_error_to_401() -> None:
    example = load_example_module("fastapi_request_pipeline")

    with TestClient(example.create_app()) as client:
        response = client.post(
            "/orders",
            json={"item_id": "sku-123", "quantity": 2},
        )

    assert response.status_code == 401
    assert response.headers["x-workflow"] == "py-interceptors"
    assert response.json() == {"error": "unauthorized"}


def test_fastapi_pipeline_maps_validation_error_to_422() -> None:
    example = load_example_module("fastapi_request_pipeline")

    with TestClient(example.create_app()) as client:
        response = client.post(
            "/orders",
            headers={"Authorization": "Bearer secret-token"},
            json={"item_id": "sku-123", "quantity": 0},
        )

    assert response.status_code == 422
    assert response.headers["x-workflow"] == "py-interceptors"
    assert response.json() == {"error": "quantity must be a positive integer"}
