import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_example_module(module_name: str) -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "examples" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load example module from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module
