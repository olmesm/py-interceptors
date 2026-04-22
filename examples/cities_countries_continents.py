from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass

import polars as pl

from py_interceptors import (
    AsyncPolicy,
    Interceptor,
    Runtime,
    StreamInterceptor,
    ThreadPolicy,
    chain,
    stream_chain,
)


@dataclass
class CitiesContext:
    cities: list[str]


@dataclass
class DataFrameContext:
    df: pl.DataFrame


@dataclass
class FinalContext:
    df: pl.DataFrame


class CitiesToDataFrame(Interceptor[CitiesContext, DataFrameContext]):
    input_type = CitiesContext
    output_type = DataFrameContext

    def enter(self, ctx: CitiesContext) -> DataFrameContext:
        return DataFrameContext(df=pl.DataFrame({"city": ctx.cities}))


class ChunkCities(
    StreamInterceptor[DataFrameContext, list[str], list[str], DataFrameContext]
):
    input_type = DataFrameContext
    emit_type = list[str]
    collect_type = list[str]
    output_type = DataFrameContext

    chunk_size = 3

    def stream(self, ctx: DataFrameContext) -> Iterable[list[str]]:
        cities = ctx.df["city"].to_list()
        for i in range(0, len(cities), self.chunk_size):
            yield cities[i : i + self.chunk_size]

    def collect(
        self,
        ctx: DataFrameContext,
        items: Iterable[list[str]],
    ) -> DataFrameContext:
        countries = [country for chunk in items for country in chunk]

        return DataFrameContext(
            df=ctx.df.with_columns(pl.Series("country", countries))
        )


class FetchCountries(Interceptor[list[str], list[str]]):
    input_type = list[str]
    output_type = list[str]

    async def enter(self, ctx: list[str]) -> list[str]:
        lookup = {
            "London": "UK",
            "Paris": "France",
            "Berlin": "Germany",
            "Madrid": "Spain",
            "Tokyo": "Japan",
            "Sydney": "Australia",
        }

        await asyncio.sleep(0.01)

        return [lookup.get(city, "Unknown") for city in ctx]


class GroupByContinent(Interceptor[DataFrameContext, FinalContext]):
    input_type = DataFrameContext
    output_type = FinalContext

    def enter(self, ctx: DataFrameContext) -> FinalContext:
        country_to_continent = {
            "UK": "Europe",
            "France": "Europe",
            "Germany": "Europe",
            "Spain": "Europe",
            "Japan": "Asia",
            "Australia": "Oceania",
        }

        df = ctx.df.with_columns(
            pl.col("country")
            .map_elements(
                lambda country: country_to_continent.get(country, "Other"),
                return_dtype=pl.String,
            )
            .alias("continent")
        )

        grouped = (
            df.group_by("continent")
            .agg(pl.len().alias("city_count"))
            .sort("continent")
        )

        return FinalContext(df=grouped)


thread_main = ThreadPolicy("main")
async_io = AsyncPolicy()

enrich_countries = (
    chain("fetch countries").use(FetchCountries).on(async_io).build()
)

chunk_cities = (
    stream_chain("chunk cities")
    .stream(ChunkCities)
    .map(enrich_countries)
    .build()
)

workflow = (
    chain("cities -> continents")
    .use(CitiesToDataFrame)
    .use(chunk_cities)
    .use(GroupByContinent)
    .on(thread_main)
    .build()
)


def example_context() -> CitiesContext:
    return CitiesContext(
        cities=[
            "London",
            "Paris",
            "Berlin",
            "Madrid",
            "Tokyo",
            "Sydney",
        ]
    )


async def run_example() -> FinalContext:
    runtime = Runtime()
    try:
        return await runtime.run_async(workflow, example_context())
    finally:
        runtime.shutdown()


def main() -> None:
    result = asyncio.run(run_example())
    print(result.df)


if __name__ == "__main__":
    main()
