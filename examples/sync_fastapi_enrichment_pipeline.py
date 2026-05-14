"""
Sync-FastAPI enrichment pipeline.

Generalises a real-world shape:

  [thread] Deserialize             CPU-heavy decode of the request body
  [thread] ValidateAndNormalize    cheap fail-fast before any I/O
  [async]  Authorize               HTTP to an authz service
  [async]  FetchMatchFields        HTTP to an enrichment service (small payload)
  [thread] ConvertMatchData        CPU-heavy transform on a DataFrame-like object
  [thread] MatchInstruments        cheap join keeping the frame on the lane
  [async]  FetchFullFields         HTTP to the enrichment service for full rows
  [thread] ConvertFullData         CPU-heavy transform on the frame
  [thread] BuildMappedInstruments  cheap shaping
  [async]  DispatchLibraries       async fanout: one HTTP call per library, gathered
  [thread] Serialize               CPU-heavy encode of the response

The whole workflow contains async segments and so cannot be driven by
``runtime.run_sync``. The FastAPI route in this example is intentionally a
synchronous handler (``def``, not ``async def``) running on the threadpool
that FastAPI/anyio dispatches sync routes onto. The handler calls
``runtime.run_blocking(...)``, which drives the chain on a long-lived
runtime-owned event loop and blocks the calling worker thread on the result.

Why this matters in production:

* A single event loop lives for the lifetime of the process. Async HTTP
  clients (``httpx.AsyncClient`` or similar) can be created once and reused.
* No ``asyncio.run()`` is created per request, so there is no per-request loop
  setup/teardown cost and no TLS-handshake-per-request anti-pattern.
* Thread-policy segments still bounce out to their dedicated lanes; the
  DataFrame-like object keeps thread affinity across the convert + match
  steps because they share one ``ThreadPolicy`` lane.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import cast

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from py_interceptors import (
    AsyncPolicy,
    Interceptor,
    Runtime,
    StreamInterceptor,
    ThreadPolicy,
    chain,
    stream_chain,
)


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


@dataclass
class EnrichmentRequest:
    """Raw request from a sync FastAPI route."""

    raw_body: bytes
    libraries: list[str]


@dataclass
class NormalizedRequest:
    """Validated/normalized payload that downstream steps expect."""

    symbols: list[str]
    libraries: list[str]
    auth_token: str | None = None
    match_rows: list[dict[str, object]] = field(default_factory=list)
    full_rows: list[dict[str, object]] = field(default_factory=list)
    matched_instruments: list[dict[str, object]] = field(default_factory=list)
    mapped_instruments: list[dict[str, object]] = field(default_factory=list)
    library_results: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    trace: list[str] = field(default_factory=list)


@dataclass
class EnrichmentResponse:
    payload: bytes
    trace: list[str]


@dataclass
class LibraryRequest:
    """One unit of work for the library fanout stream."""

    library: str
    mapped_instruments: list[dict[str, object]]
    auth_token: str | None


@dataclass
class LibraryResult:
    library: str
    rows: list[dict[str, object]]


# ---------------------------------------------------------------------------
# Policies — one ThreadPolicy lane gives the frame thread affinity across the
# convert/match steps; AsyncPolicy is a runtime-owned isolated portal for the
# HTTP-bound steps.
# ---------------------------------------------------------------------------


frame_lane = ThreadPolicy("enrichment-frame")
io_portal = AsyncPolicy("enrichment-io", isolated=True)


# ---------------------------------------------------------------------------
# CPU-heavy / thread-bound interceptors
# ---------------------------------------------------------------------------


def _trace(ctx: NormalizedRequest, label: str) -> None:
    ctx.trace.append(f"{label}@{threading.current_thread().name}")


class Deserialize(Interceptor[EnrichmentRequest, NormalizedRequest]):
    """CPU-heavy decode of the request body into a normalized payload."""

    input_type = EnrichmentRequest
    output_type = NormalizedRequest

    def enter(self, ctx: EnrichmentRequest) -> NormalizedRequest:
        symbols = [
            symbol.strip()
            for symbol in ctx.raw_body.decode("utf-8").split(",")
            if symbol.strip()
        ]
        normalized = NormalizedRequest(symbols=symbols, libraries=list(ctx.libraries))
        _trace(normalized, "deserialize")
        return normalized


class ValidateAndNormalize(Interceptor[NormalizedRequest, NormalizedRequest]):
    """Cheap fail-fast checks before any network call is made."""

    input_type = NormalizedRequest
    output_type = NormalizedRequest

    def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "validate")
        if not ctx.symbols:
            raise ValueError("at least one symbol is required")
        if not ctx.libraries:
            raise ValueError("at least one library is required")
        return ctx


class ConvertMatchData(Interceptor[NormalizedRequest, NormalizedRequest]):
    """CPU-heavy convert of the match-fields response onto the frame lane."""

    input_type = NormalizedRequest
    output_type = NormalizedRequest

    def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "convert-match")
        ctx.matched_instruments = [
            {**row, "normalized_symbol": str(row.get("symbol", "")).upper()}
            for row in ctx.match_rows
        ]
        return ctx


class MatchInstruments(Interceptor[NormalizedRequest, NormalizedRequest]):
    """Cheap join that keeps the frame on the same lane as ConvertMatchData."""

    input_type = NormalizedRequest
    output_type = NormalizedRequest

    def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "match")
        for row in ctx.matched_instruments:
            row["matched"] = True
        return ctx


class ConvertFullData(Interceptor[NormalizedRequest, NormalizedRequest]):
    """CPU-heavy convert of the full-fields response back on the frame lane."""

    input_type = NormalizedRequest
    output_type = NormalizedRequest

    def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "convert-full")
        index = {row["normalized_symbol"]: row for row in ctx.matched_instruments}
        for full in ctx.full_rows:
            key = str(full.get("symbol", "")).upper()
            existing = index.get(key)
            if existing is not None:
                existing.update(
                    {k: v for k, v in full.items() if k != "symbol"}
                )
        return ctx


class BuildMappedInstruments(Interceptor[NormalizedRequest, NormalizedRequest]):
    """Cheap shaping for downstream dispatch — still on the frame lane."""

    input_type = NormalizedRequest
    output_type = NormalizedRequest

    def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "build-mapped")
        ctx.mapped_instruments = [
            {
                "symbol": row["normalized_symbol"],
                "name": row.get("name"),
                "price": row.get("price"),
            }
            for row in ctx.matched_instruments
        ]
        return ctx


class Serialize(Interceptor[NormalizedRequest, EnrichmentResponse]):
    """CPU-heavy encode of the final response."""

    input_type = NormalizedRequest
    output_type = EnrichmentResponse

    def enter(self, ctx: NormalizedRequest) -> EnrichmentResponse:
        _trace(ctx, "serialize")
        parts: list[str] = []
        for library, rows in sorted(ctx.library_results.items()):
            for row in rows:
                parts.append(
                    f"{library}:{row.get('symbol')}={row.get('value')}"
                )
        return EnrichmentResponse(
            payload="|".join(parts).encode("utf-8"),
            trace=list(ctx.trace),
        )


# ---------------------------------------------------------------------------
# Async / I/O-bound interceptors — these would call httpx.AsyncClient in
# production. The fake bodies just await once to demonstrate they run on the
# portal's loop, not the caller's thread.
# ---------------------------------------------------------------------------


class Authorize(Interceptor[NormalizedRequest, NormalizedRequest]):
    input_type = NormalizedRequest
    output_type = NormalizedRequest

    async def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "authorize")
        await asyncio.sleep(0)
        ctx.auth_token = "token-abc"
        return ctx


class FetchMatchFields(Interceptor[NormalizedRequest, NormalizedRequest]):
    input_type = NormalizedRequest
    output_type = NormalizedRequest

    async def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "fetch-match")
        await asyncio.sleep(0)
        ctx.match_rows = [
            {"symbol": symbol, "name": f"Name {symbol}"}
            for symbol in ctx.symbols
        ]
        return ctx


class FetchFullFields(Interceptor[NormalizedRequest, NormalizedRequest]):
    input_type = NormalizedRequest
    output_type = NormalizedRequest

    async def enter(self, ctx: NormalizedRequest) -> NormalizedRequest:
        _trace(ctx, "fetch-full")
        await asyncio.sleep(0)
        ctx.full_rows = [
            {"symbol": symbol, "price": 100 + index}
            for index, symbol in enumerate(ctx.symbols)
        ]
        return ctx


# ---------------------------------------------------------------------------
# Library dispatch — a stream chain with an async per-item HTTP call. The
# split/collect run on the portal loop (cheap work); only the per-library
# fetch awaits.
# ---------------------------------------------------------------------------


class SplitLibraries(
    StreamInterceptor[
        NormalizedRequest,
        LibraryRequest,
        LibraryResult,
        NormalizedRequest,
    ]
):
    input_type = NormalizedRequest
    emit_type = LibraryRequest
    collect_type = LibraryResult
    output_type = NormalizedRequest

    def stream(self, ctx: NormalizedRequest) -> Iterable[LibraryRequest]:
        _trace(ctx, "dispatch-split")
        for library in ctx.libraries:
            yield LibraryRequest(
                library=library,
                mapped_instruments=ctx.mapped_instruments,
                auth_token=ctx.auth_token,
            )

    def collect(
        self,
        ctx: NormalizedRequest,
        items: Iterable[LibraryResult],
    ) -> NormalizedRequest:
        _trace(ctx, "dispatch-collect")
        ctx.library_results = {item.library: item.rows for item in items}
        return ctx


class FetchLibraryRows(Interceptor[LibraryRequest, LibraryResult]):
    input_type = LibraryRequest
    output_type = LibraryResult

    async def enter(self, ctx: LibraryRequest) -> LibraryResult:
        await asyncio.sleep(0)
        return LibraryResult(
            library=ctx.library,
            rows=[
                {
                    "symbol": instrument["symbol"],
                    "value": f"{ctx.library}:{instrument['symbol']}",
                }
                for instrument in ctx.mapped_instruments
            ],
        )


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------


fetch_library_chain = (
    chain("fetch library").use(FetchLibraryRows).on(io_portal).build()
)

dispatch_libraries = (
    stream_chain("dispatch libraries")
    .stream(SplitLibraries)
    .map(fetch_library_chain)
    .on(io_portal)
    .build()
)

authorize_and_match = (
    chain("authorize and match")
    .use(Authorize)
    .use(FetchMatchFields)
    .on(io_portal)
    .build()
)

fetch_full = chain("fetch full").use(FetchFullFields).on(io_portal).build()

convert_match_block = (
    chain("convert match block")
    .use(ConvertMatchData)
    .use(MatchInstruments)
    .on(frame_lane)
    .build()
)

convert_full_block = (
    chain("convert full block")
    .use(ConvertFullData)
    .use(BuildMappedInstruments)
    .on(frame_lane)
    .build()
)

deserialize_block = (
    chain("deserialize block")
    .use(Deserialize)
    .use(ValidateAndNormalize)
    .on(frame_lane)
    .build()
)

# Serialize is CPU-heavy and needs the same lane as the other frame steps.
# Wrapping it in its own sub-chain pinned to ``frame_lane`` ensures it lands
# on the frame-affinity thread regardless of what ran immediately before it.
serialize_block = (
    chain("serialize block").use(Serialize).on(frame_lane).build()
)

workflow = (
    chain("sync enrichment")
    .use(deserialize_block)
    .use(authorize_and_match)
    .use(convert_match_block)
    .use(fetch_full)
    .use(convert_full_block)
    .use(dispatch_libraries)
    .use(serialize_block)
    .build()
)


# ---------------------------------------------------------------------------
# FastAPI integration — note the sync ``def`` route handler.
# ---------------------------------------------------------------------------


def get_runtime(app: FastAPI) -> Runtime:
    value: object = getattr(app.state, "enrichment_runtime", None)
    if not isinstance(value, Runtime):
        raise RuntimeError(
            "Enrichment runtime is only available during app lifespan"
        )
    return value


def create_app(runtime: Runtime | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owns_runtime = runtime is None
        enrichment_runtime = runtime or Runtime()
        if owns_runtime:
            await enrichment_runtime.startup()

        enrichment_runtime.compile(workflow, initial=EnrichmentRequest)
        app.state.enrichment_runtime = enrichment_runtime
        try:
            yield
        finally:
            if owns_runtime:
                await enrichment_runtime.shutdown_async()

    app = FastAPI(lifespan=lifespan)

    @app.post("/enrich")
    def enrich(request: Request) -> JSONResponse:
        # Sync route handler — FastAPI dispatches this onto its worker
        # threadpool. The handler must not touch ``await``.
        raw_body = _read_body_sync(request)
        libraries = [
            value
            for value in request.query_params.getlist("library")
            if value
        ]
        payload = EnrichmentRequest(raw_body=raw_body, libraries=libraries)

        enrichment_runtime = get_runtime(cast(FastAPI, request.app))
        try:
            result = enrichment_runtime.run_blocking(workflow, payload)
        except ValueError as err:
            return JSONResponse(status_code=422, content={"error": str(err)})

        return JSONResponse(
            status_code=200,
            content={
                "payload": result.payload.decode("utf-8"),
                "trace": result.trace,
            },
        )

    return app


def _read_body_sync(request: Request) -> bytes:
    """Drain the request body from a sync handler.

    FastAPI exposes ``request.body()`` as a coroutine. From a sync route we
    use ``anyio.from_thread.run`` if available; for the test harness we just
    schedule the coroutine on the running portal-less helper loop.
    """

    import anyio.from_thread

    return anyio.from_thread.run(request.body)


app = create_app()
