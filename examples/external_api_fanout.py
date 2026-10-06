import asyncio
from collections.abc import Iterable
from dataclasses import dataclass

from py_interceptors import (
    AsyncPolicy,
    Interceptor,
    Runtime,
    StreamInterceptor,
    chain,
    stream_chain,
)


@dataclass
class CustomerProfile:
    customer_id: int
    name: str
    tier: str


@dataclass
class CustomerReport:
    profiles: list[CustomerProfile]
    premium_count: int


FAKE_API = {
    1: CustomerProfile(1, "Ada", "premium"),
    2: CustomerProfile(2, "Linus", "standard"),
    3: CustomerProfile(3, "Grace", "premium"),
}


class SplitCustomerIds(
    StreamInterceptor[list[int], int, CustomerProfile, CustomerReport]
):
    input_type = list[int]
    emit_type = int
    collect_type = CustomerProfile
    output_type = CustomerReport

    def stream(self, ctx: list[int]) -> Iterable[int]:
        return ctx

    def collect(
        self, ctx: list[int], items: Iterable[CustomerProfile]
    ) -> CustomerReport:
        # The async map preserves item order; sorting here shows that collect
        # owns the final ordering regardless.
        profiles = sorted(items, key=lambda profile: profile.customer_id)
        premium = sum(1 for profile in profiles if profile.tier == "premium")
        return CustomerReport(profiles, premium)


class FetchCustomerProfile(Interceptor[int, CustomerProfile]):
    input_type = int
    output_type = CustomerProfile

    async def enter(self, ctx: int) -> CustomerProfile:
        await asyncio.sleep(0)
        return FAKE_API.get(ctx, CustomerProfile(ctx, "Unknown", "standard"))


# An isolated policy runs every fetch on one runtime-owned loop thread, so the
# fan-out never blocks the caller's loop and a shared API client could live there.
customer_api = AsyncPolicy("customer-api", isolated=True)

fetch_customer = (
    chain("fetch customer profile").use(FetchCustomerProfile).on(customer_api).build()
)

fanout_stage = (
    stream_chain("customer fanout").stream(SplitCustomerIds).map(fetch_customer).build()
)

# Runtime.run_* take a Chain, so the lone stream stage is wrapped in one.
workflow = chain("customer report").use(fanout_stage).build()


async def run_example() -> CustomerReport:
    async with Runtime() as runtime:
        return await runtime.run_async(workflow, [1, 2, 3])


if __name__ == "__main__":
    print(asyncio.run(run_example()))
