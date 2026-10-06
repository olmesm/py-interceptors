"""
Stream composition inside a policy-shaped workflow.

The root chain runs on one thread lane. The chunked fetch inside it is an
async child chain, so each chunk hops back to the caller's event loop and the
grouping step returns to the lane: ThreadPolicy -> AsyncPolicy -> ThreadPolicy.
"""

import asyncio
from collections import Counter
from collections.abc import Iterable
from itertools import batched

from py_interceptors import (
    AsyncPolicy,
    Interceptor,
    Runtime,
    StreamInterceptor,
    ThreadPolicy,
    chain,
    stream_chain,
)

COUNTRY_OF = {
    "London": "UK",
    "Paris": "France",
    "Berlin": "Germany",
    "Madrid": "Spain",
    "Tokyo": "Japan",
    "Sydney": "Australia",
}

CONTINENT_OF = {
    "UK": "Europe",
    "France": "Europe",
    "Germany": "Europe",
    "Spain": "Europe",
    "Japan": "Asia",
    "Australia": "Oceania",
}


class ChunkCities(StreamInterceptor[list[str], tuple[str, ...], list[str], list[str]]):
    input_type = list[str]
    emit_type = tuple[str, ...]
    collect_type = list[str]
    output_type = list[str]

    def stream(self, ctx: list[str]) -> Iterable[tuple[str, ...]]:
        return batched(ctx, 3)

    def collect(self, ctx: list[str], items: Iterable[list[str]]) -> list[str]:
        return [country for chunk in items for country in chunk]


class FetchCountries(Interceptor[tuple[str, ...], list[str]]):
    input_type = tuple[str, ...]
    output_type = list[str]

    async def enter(self, ctx: tuple[str, ...]) -> list[str]:
        await asyncio.sleep(0.01)
        return [COUNTRY_OF.get(city, "Unknown") for city in ctx]


class GroupByContinent(Interceptor[list[str], Counter[str]]):
    input_type = list[str]
    output_type = Counter

    def enter(self, ctx: list[str]) -> Counter[str]:
        return Counter(CONTINENT_OF.get(country, "Other") for country in ctx)


enrich_countries = (
    chain("fetch countries").use(FetchCountries).on(AsyncPolicy()).build()
)

chunk_cities = (
    stream_chain("chunk cities").stream(ChunkCities).map(enrich_countries).build()
)

workflow = (
    chain("cities -> continents")
    .use(chunk_cities)
    .use(GroupByContinent)
    .on(ThreadPolicy("main"))
    .build()
)

EXAMPLE_CITIES = ["London", "Paris", "Berlin", "Madrid", "Tokyo", "Sydney"]


async def run_example() -> Counter[str]:
    async with Runtime() as runtime:
        return await runtime.run_async(workflow, EXAMPLE_CITIES)


if __name__ == "__main__":
    print(asyncio.run(run_example()))
