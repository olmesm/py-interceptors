import pytest
from examples import fastapi_pipeline as example
from fastapi.testclient import TestClient

AUTH = {"Authorization": "Bearer secret-token"}
ORDER = {"item_id": "sku-1", "quantity": 2}
ROUTES = ["/async/orders", "/sync/orders"]


def _threads(trace: list[str]) -> dict[str, str]:
    return dict(entry.rsplit("@", 1) for entry in trace)


@pytest.mark.parametrize("route", ROUTES)
def test_route_returns_a_receipt(route: str) -> None:
    with TestClient(example.app) as client:
        response = client.post(route, json=ORDER, headers=AUTH)

    assert response.status_code == 201
    body = response.json()
    assert (body["order_id"], body["total"]) == ("ord-user-123-sku-1", 10)


@pytest.mark.parametrize("route", ROUTES)
def test_sync_stages_share_one_lane_across_the_async_hop(route: str) -> None:
    with TestClient(example.app) as client:
        response = client.post(route, json=ORDER, headers=AUTH)

    threads = _threads(response.json()["trace"])
    assert threads["authenticate"] == threads["receipt"]
    assert threads["authenticate"].startswith("orders-cpu")
    assert threads["lookup-price"] != threads["authenticate"]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    ("headers", "order", "status", "detail"),
    [
        ({}, ORDER, 401, "unauthorized"),
        (AUTH, {"item_id": "sku-404", "quantity": 1}, 404, "unknown item sku-404"),
    ],
)
def test_boundary_maps_known_errors(
    route: str,
    headers: dict[str, str],
    order: dict[str, object],
    status: int,
    detail: str,
) -> None:
    with TestClient(example.app) as client:
        response = client.post(route, json=order, headers=headers)

    assert response.status_code == status
    assert response.json() == {"detail": detail}


def test_boundary_reraises_unknown_errors() -> None:
    boundary = example.ApiBoundary()
    with pytest.raises(RuntimeError, match="unexpected"):
        boundary.error(
            example.OrderRequest(None, "sku-1", 1), RuntimeError("unexpected")
        )
