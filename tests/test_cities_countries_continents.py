import asyncio
from collections import Counter

from examples import cities_countries_continents as example


def test_cities_countries_continent_summary_example() -> None:
    result = asyncio.run(example.run_example())

    assert result == Counter({"Europe": 4, "Asia": 1, "Oceania": 1})
