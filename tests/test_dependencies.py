"""Tests for chain/interceptor dependency injection via .use(Cls, **kwargs) and .provide(...)."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from py_interceptors import (
    AmbiguousDependencyError,
    Chain,
    DependencyTypeError,
    Interceptor,
    MissingDependencyError,
    Runtime,
    UnknownDependencyError,
    chain,
)

# --------------------------------------------------------------------------
# Test doubles
# --------------------------------------------------------------------------


@dataclass
class Logger:
    name: str = "default"
    events: list[str] = field(default_factory=list)

    def log(self, event: str) -> None:
        self.events.append(f"{self.name}:{event}")


@dataclass
class AuthClient:
    token: str


@dataclass
class Request:
    user_id: int
    authorized: bool = False
    logged: bool = False


@dataclass
class Response:
    user_id: int
    body: str


# --------------------------------------------------------------------------
# Interceptors
# --------------------------------------------------------------------------


class AuthInterceptor(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    auth_client: AuthClient

    def enter(self, ctx: Request) -> Request:
        ctx.authorized = self.auth_client.token == "ok"
        return ctx


class LoggingInterceptor(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    logger: Logger

    def enter(self, ctx: Request) -> Request:
        self.logger.log(f"enter user={ctx.user_id}")
        ctx.logged = True
        return ctx


class Render(Interceptor[Request, Response]):
    input_type = Request
    output_type = Response

    def enter(self, ctx: Request) -> Response:
        body = f"ok user={ctx.user_id} authorized={ctx.authorized}"
        return Response(user_id=ctx.user_id, body=body)


class NoDeps(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    def enter(self, ctx: Request) -> Request:
        return ctx


class OptionalDep(Interceptor[Request, Request]):
    """Has an annotation but a class-level default, so it is NOT required."""

    input_type = Request
    output_type = Request

    logger: Logger = Logger(name="fallback")

    def enter(self, ctx: Request) -> Request:
        self.logger.log(f"optional user={ctx.user_id}")
        return ctx


class TwoLoggers(Interceptor[Request, Request]):
    input_type = Request
    output_type = Request

    primary: Logger
    audit: Logger

    def enter(self, ctx: Request) -> Request:
        self.primary.log(f"primary user={ctx.user_id}")
        self.audit.log(f"audit user={ctx.user_id}")
        return ctx


# --------------------------------------------------------------------------
# Direct binding at .use(Cls, **kwargs)
# --------------------------------------------------------------------------


def test_direct_binding_injects_attribute() -> None:
    auth = AuthClient(token="ok")
    workflow = chain("auth").use(AuthInterceptor, auth_client=auth).use(Render).build()
    with Runtime() as runtime:
        result = runtime.run_sync(workflow, Request(user_id=42))
    assert result.body == "ok user=42 authorized=True"


def test_direct_binding_unknown_kwarg_raises_at_use() -> None:
    with pytest.raises(UnknownDependencyError):
        chain("x").use(AuthInterceptor, auth_clinet=AuthClient(token="ok"))


def test_direct_binding_type_mismatch_raises_at_use() -> None:
    with pytest.raises(DependencyTypeError):
        chain("x").use(AuthInterceptor, auth_client="not a client")


# --------------------------------------------------------------------------
# .provide(...) on a chain
# --------------------------------------------------------------------------


def test_provide_supplies_dependency_to_bare_class_step() -> None:
    logger = Logger(name="prod")
    workflow = (
        chain("root")
        .provide(logger=logger)
        .use(LoggingInterceptor)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(user_id=7))
    assert logger.events == ["prod:enter user=7"]


def test_provide_on_outer_chain_resolves_for_inner_chain() -> None:
    logger = Logger(name="outer")
    inner = chain("inner").use(LoggingInterceptor).use(Render).build()
    outer = chain("outer").provide(logger=logger).use(inner).build()
    with Runtime() as runtime:
        runtime.run_sync(outer, Request(user_id=1))
    assert logger.events == ["outer:enter user=1"]


def test_nearest_provide_wins_when_shadowed() -> None:
    outer_logger = Logger(name="outer")
    inner_logger = Logger(name="inner")
    inner = (
        chain("inner")
        .provide(logger=inner_logger)
        .use(LoggingInterceptor)
        .use(Render)
        .build()
    )
    outer = chain("outer").provide(logger=outer_logger).use(inner).build()
    with Runtime() as runtime:
        runtime.run_sync(outer, Request(user_id=3))
    assert inner_logger.events == ["inner:enter user=3"]
    assert outer_logger.events == []


def test_direct_bind_wins_over_ancestor_provide() -> None:
    direct_logger = Logger(name="direct")
    ancestor_logger = Logger(name="ancestor")
    workflow = (
        chain("root")
        .provide(logger=ancestor_logger)
        .use(LoggingInterceptor, logger=direct_logger)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(user_id=9))
    assert direct_logger.events == ["direct:enter user=9"]
    assert ancestor_logger.events == []


# --------------------------------------------------------------------------
# Missing / ambiguous / type mismatched ancestor resolution
# --------------------------------------------------------------------------


def test_missing_dependency_raises_at_compile() -> None:
    workflow = chain("x").use(LoggingInterceptor).use(Render).build()
    with Runtime() as runtime:
        with pytest.raises(MissingDependencyError):
            runtime.compile(workflow, initial=Request)


def test_provide_type_mismatch_for_named_kwarg_raises_at_compile() -> None:
    workflow = (
        chain("x")
        .provide(logger="not-a-logger")
        .use(LoggingInterceptor)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        with pytest.raises(DependencyTypeError):
            runtime.compile(workflow, initial=Request)


def test_ambiguous_type_only_resolution_raises_at_compile() -> None:
    """Two Logger values, neither named `primary` nor `audit`."""

    workflow = (
        chain("x")
        .provide(
            first=Logger(name="first"),
            second=Logger(name="second"),
        )
        .use(LoggingInterceptor)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        with pytest.raises(AmbiguousDependencyError):
            runtime.compile(workflow, initial=Request)


def test_name_match_over_type_match_disambiguates() -> None:
    """When primary and audit names match, both resolve and ambiguity is avoided."""

    primary = Logger(name="primary")
    audit = Logger(name="audit")
    workflow = (
        chain("x")
        .provide(primary=primary, audit=audit)
        .use(TwoLoggers)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(user_id=2))
    assert primary.events == ["primary:primary user=2"]
    assert audit.events == ["audit:audit user=2"]


# --------------------------------------------------------------------------
# Optional / no-deps / multiple provides
# --------------------------------------------------------------------------


def test_no_deps_class_works_without_provide() -> None:
    workflow = chain("x").use(NoDeps).use(Render).build()
    with Runtime() as runtime:
        result = runtime.run_sync(workflow, Request(user_id=11))
    assert result.user_id == 11


def test_optional_dep_uses_class_default_when_not_provided() -> None:
    workflow = chain("x").use(OptionalDep).use(Render).build()
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(user_id=5))


def test_optional_dep_is_overridden_by_provide() -> None:
    override = Logger(name="override")
    workflow = (
        chain("x")
        .provide(logger=override)
        .use(OptionalDep)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(user_id=6))
    assert override.events == ["override:optional user=6"]


def test_multiple_provide_calls_merge_and_override() -> None:
    first = Logger(name="first")
    second = Logger(name="second")
    workflow = (
        chain("x")
        .provide(logger=first)
        .provide(logger=second)  # overrides the first
        .use(LoggingInterceptor)
        .use(Render)
        .build()
    )
    with Runtime() as runtime:
        runtime.run_sync(workflow, Request(user_id=8))
    assert second.events == ["second:enter user=8"]
    assert first.events == []


# --------------------------------------------------------------------------
# Sub-chain testability (the whole point of the design)
# --------------------------------------------------------------------------


def production_subchain() -> Chain[Request, Response]:
    """Sub-chain that pulls a Logger from somewhere up the tree."""
    return chain("auth-and-log").use(LoggingInterceptor).use(Render).build()


def test_subchain_can_be_tested_in_isolation_with_provide() -> None:
    fake_logger = Logger(name="test")
    test_workflow = (
        chain("test")
        .provide(logger=fake_logger)
        .use(production_subchain())
        .build()
    )
    with Runtime() as runtime:
        result = runtime.run_sync(test_workflow, Request(user_id=99))
    assert fake_logger.events == ["test:enter user=99"]
    assert result.body.startswith("ok user=99")


# --------------------------------------------------------------------------
# .use(...) keyword guard rails
# --------------------------------------------------------------------------


def test_use_rejects_kwargs_for_subchain() -> None:
    inner = chain("inner").use(NoDeps).build()
    with pytest.raises(TypeError):
        chain("outer").use(inner, logger=Logger(name="x"))  # type: ignore[call-overload]
