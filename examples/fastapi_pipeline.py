"""
One order workflow behind two FastAPI routes.

``POST /async/orders`` is an ``async def`` route: it awaits ``run_async`` on
the request loop. ``POST /sync/orders`` is a plain ``def`` route, which
FastAPI runs on an anyio worker thread; it calls ``run_blocking``, which
drives the same chain on the runtime's own loop thread and blocks the worker
until the result is ready. The sync route stands in for any handler that
cannot be async, and it holds one of anyio's worker tokens (40 by default)
for the whole chain.

The receipt records the thread each stage ran on. The two sync stages share
one ``ThreadPolicy`` lane; the async stage runs on whichever loop drives the
chain, the request loop under ``run_async`` or the runtime loop under
``run_blocking``. ``ApiBoundary`` maps known errors to HTTP responses and
re-raises anything else, so unexpected failures still reach the server log.
"""

import asyncio
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Request

from py_interceptors import Interceptor, Runtime, ThreadPolicy, chain


@dataclass
class OrderRequest:
    authorization: str | None
    item_id: str
    quantity: int


@dataclass
class AuthorizedOrder:
    user_id: str
    item_id: str
    quantity: int
    trace: list[str]


@dataclass
class PricedOrder:
    user_id: str
    item_id: str
    quantity: int
    unit_price: int
    trace: list[str]


@dataclass
class Receipt:
    order_id: str
    total: int
    trace: list[str]


PRICES = {"sku-1": 5, "sku-2": 12}


def _here(label: str) -> str:
    return f"{label}@{threading.current_thread().name}"


class ApiBoundary(Interceptor[OrderRequest, OrderRequest]):
    input_type = OrderRequest
    output_type = OrderRequest

    def error(self, ctx: OrderRequest, err: Exception) -> OrderRequest:
        if isinstance(err, PermissionError):
            raise HTTPException(401, "unauthorized") from err
        if isinstance(err, LookupError):
            raise HTTPException(404, str(err)) from err
        raise err


class Authenticate(Interceptor[OrderRequest, AuthorizedOrder]):
    input_type = OrderRequest
    output_type = AuthorizedOrder

    def enter(self, ctx: OrderRequest) -> AuthorizedOrder:
        if ctx.authorization != "Bearer secret-token":
            raise PermissionError("invalid bearer token")
        return AuthorizedOrder(
            "user-123", ctx.item_id, ctx.quantity, [_here("authenticate")]
        )


class LookupPrice(Interceptor[AuthorizedOrder, PricedOrder]):
    input_type = AuthorizedOrder
    output_type = PricedOrder

    async def enter(self, ctx: AuthorizedOrder) -> PricedOrder:
        await asyncio.sleep(0)  # a pricing service call in production
        if ctx.item_id not in PRICES:
            raise LookupError(f"unknown item {ctx.item_id}")
        trace = [*ctx.trace, _here("lookup-price")]
        return PricedOrder(
            ctx.user_id, ctx.item_id, ctx.quantity, PRICES[ctx.item_id], trace
        )


class IssueReceipt(Interceptor[PricedOrder, Receipt]):
    input_type = PricedOrder
    output_type = Receipt

    def enter(self, ctx: PricedOrder) -> Receipt:
        order_id = f"ord-{ctx.user_id}-{ctx.item_id}"
        total = ctx.quantity * ctx.unit_price
        return Receipt(order_id, total, [*ctx.trace, _here("receipt")])


cpu = ThreadPolicy("orders-cpu")

workflow = (
    chain("orders")
    .use(ApiBoundary)
    .use(chain("authenticate").use(Authenticate).on(cpu).build())
    .use(LookupPrice)
    .use(chain("receipt").use(IssueReceipt).on(cpu).build())
    .build()
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with Runtime() as runtime:
        runtime.compile(workflow, initial=OrderRequest)
        app.state.runtime = runtime
        yield


app = FastAPI(lifespan=lifespan)


def order_request(
    item_id: Annotated[str, Body()],
    quantity: Annotated[int, Body(gt=0)],
    authorization: Annotated[str | None, Header()] = None,
) -> OrderRequest:
    return OrderRequest(authorization, item_id, quantity)


@app.post("/async/orders", status_code=201)
async def create_order(
    request: Request, order: Annotated[OrderRequest, Depends(order_request)]
) -> Receipt:
    runtime: Runtime = request.app.state.runtime
    return await runtime.run_async(workflow, order)


@app.post("/sync/orders", status_code=201)
def create_order_blocking(
    request: Request, order: Annotated[OrderRequest, Depends(order_request)]
) -> Receipt:
    runtime: Runtime = request.app.state.runtime
    return runtime.run_blocking(workflow, order)
