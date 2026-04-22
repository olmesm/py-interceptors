from __future__ import annotations

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
class CustomerIds:
    ids: list[int]


@dataclass
class CustomerId:
    value: int


@dataclass
class CustomerProfile:
    customer_id: int
    name: str
    tier: str


@dataclass
class CustomerReport:
    profiles: list[CustomerProfile]
    premium_count: int


class SplitCustomerIds(
    StreamInterceptor[CustomerIds, CustomerId, CustomerProfile, CustomerReport]
):
    input_type = CustomerIds
    emit_type = CustomerId
    collect_type = CustomerProfile
    output_type = CustomerReport

    def stream(self, ctx: CustomerIds) -> Iterable[CustomerId]:
        for customer_id in ctx.ids:
            yield CustomerId(value=customer_id)

    def collect(
        self,
        ctx: CustomerIds,
        items: Iterable[CustomerProfile],
    ) -> CustomerReport:
        profiles = sorted(items, key=lambda profile: profile.customer_id)
        return CustomerReport(
            profiles=profiles,
            premium_count=sum(
                1 for profile in profiles if profile.tier == "premium"
            ),
        )


class FetchCustomerProfile(Interceptor[CustomerId, CustomerProfile]):
    input_type = CustomerId
    output_type = CustomerProfile

    async def enter(self, ctx: CustomerId) -> CustomerProfile:
        await asyncio.sleep(0)
        fake_api = {
            1: CustomerProfile(1, "Ada", "premium"),
            2: CustomerProfile(2, "Linus", "standard"),
            3: CustomerProfile(3, "Grace", "premium"),
        }
        return fake_api.get(
            ctx.value,
            CustomerProfile(ctx.value, "Unknown", "standard"),
        )


customer_api = AsyncPolicy("customer-api", isolated=True)

fetch_customer = (
    chain("fetch customer profile")
    .use(FetchCustomerProfile)
    .on(customer_api)
    .build()
)

fanout_stage = (
    stream_chain("customer fanout")
    .stream(SplitCustomerIds)
    .map(fetch_customer)
    .build()
)

workflow = chain("customer report").use(fanout_stage).build()


async def run_example() -> CustomerReport:
    runtime = Runtime()
    try:
        return await runtime.run_async(workflow, CustomerIds(ids=[1, 2, 3]))
    finally:
        runtime.shutdown()


def main() -> None:
    result = asyncio.run(run_example())
    print(result)


if __name__ == "__main__":
    main()
