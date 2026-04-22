import asyncio
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from py_interceptors import Runtime


def load_example_module() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / "examples"
        / "cities_countries_continents.py"
    )
    spec = importlib.util.spec_from_file_location("cities_countries_continents", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load example module from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_cities_countries_continent_summary_example() -> None:
    example = load_example_module()
    runtime = Runtime()

    try:
        compiled = runtime.compile(example.workflow, initial=example.CitiesContext)

        assert compiled.input_spec is example.CitiesContext
        assert compiled.output_spec is example.FinalContext
        assert compiled.is_async is True

        result = asyncio.run(compiled.run_async(example.example_context()))

        assert result.df.to_dict(as_series=False) == {
            "continent": ["Asia", "Europe", "Oceania"],
            "city_count": [1, 4, 1],
        }
    finally:
        runtime.shutdown()
