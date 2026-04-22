from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import cast

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from py_interceptors import AsyncPolicy, Interceptor, Runtime, chain


@dataclass
class ApiContext:
    headers: dict[str, str]
    body: dict[str, object]
    user_id: str | None = None
    item_id: str | None = None
    quantity: int | None = None
    response_status: int | None = None
    response_body: dict[str, object] | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)


class ApiBoundary(Interceptor[ApiContext, ApiContext]):
    input_type = ApiContext
    output_type = ApiContext

    def enter(self, ctx: ApiContext) -> ApiContext:
        ctx.events.append("boundary enter")
        return ctx

    def leave(self, ctx: ApiContext) -> ApiContext:
        ctx.events.append("boundary leave")
        ctx.response_headers["x-workflow"] = "py-interceptors"
        return ctx

    def error(self, ctx: ApiContext, err: Exception) -> ApiContext:
        ctx.events.append(f"boundary error:{type(err).__name__}")
        ctx.response_headers["x-workflow"] = "py-interceptors"

        if isinstance(err, PermissionError):
            ctx.response_status = 401
            ctx.response_body = {"error": "unauthorized"}
            return ctx

        if isinstance(err, ValueError):
            ctx.response_status = 422
            ctx.response_body = {"error": str(err)}
            return ctx

        ctx.response_status = 500
        ctx.response_body = {"error": "internal server error"}
        return ctx


class Authenticate(Interceptor[ApiContext, ApiContext]):
    input_type = ApiContext
    output_type = ApiContext

    def enter(self, ctx: ApiContext) -> ApiContext:
        ctx.events.append("authenticate enter")
        token = ctx.headers.get("authorization")
        if token != "Bearer secret-token":
            raise PermissionError("invalid bearer token")

        ctx.user_id = "user-123"
        return ctx


class ValidateOrder(Interceptor[ApiContext, ApiContext]):
    input_type = ApiContext
    output_type = ApiContext

    def enter(self, ctx: ApiContext) -> ApiContext:
        ctx.events.append("validate enter")
        item_id = ctx.body.get("item_id")
        quantity = ctx.body.get("quantity")

        if not isinstance(item_id, str) or not item_id:
            raise ValueError("item_id is required")
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("quantity must be a positive integer")

        ctx.item_id = item_id
        ctx.quantity = quantity
        return ctx


class CreateOrder(Interceptor[ApiContext, ApiContext]):
    input_type = ApiContext
    output_type = ApiContext

    async def enter(self, ctx: ApiContext) -> ApiContext:
        ctx.events.append("create enter")
        await asyncio.sleep(0)

        if ctx.user_id is None or ctx.item_id is None or ctx.quantity is None:
            raise RuntimeError("order context is incomplete")

        ctx.response_status = 201
        ctx.response_body = {
            "order_id": f"ord-{ctx.user_id}-{ctx.item_id}",
            "item_id": ctx.item_id,
            "quantity": ctx.quantity,
        }
        return ctx


fastapi_order_policy = AsyncPolicy("fastapi-order", isolated=True)

workflow = (
    chain("fastapi order")
    .use(ApiBoundary)
    .use(Authenticate)
    .use(ValidateOrder)
    .use(CreateOrder)
    .on(fastapi_order_policy)
    .build()
)


def _json_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def get_runtime(app: FastAPI) -> Runtime:
    value: object = getattr(app.state, "workflow_runtime", None)
    if not isinstance(value, Runtime):
        raise RuntimeError("Workflow runtime is only available during app lifespan")
    return value


def create_app(runtime: Runtime | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        workflow_runtime = runtime or Runtime()
        app.state.workflow_runtime = workflow_runtime
        try:
            yield
        finally:
            if runtime is None:
                workflow_runtime.shutdown()

    app = FastAPI(lifespan=lifespan)

    @app.post("/orders")
    async def create_order(request: Request) -> JSONResponse:
        raw_body = await request.json()
        ctx = ApiContext(
            headers={key.lower(): value for key, value in request.headers.items()},
            body=_json_object(raw_body),
        )

        workflow_runtime = get_runtime(cast(FastAPI, request.app))
        result = await workflow_runtime.run_async(workflow, ctx)
        return JSONResponse(
            status_code=result.response_status or 500,
            content=result.response_body or {"error": "missing response"},
            headers=result.response_headers,
        )

    return app


app = create_app()
