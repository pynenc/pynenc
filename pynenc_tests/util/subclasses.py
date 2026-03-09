from pynenc.runner.base_runner import BaseRunner
from pynenc.util.subclasses import get_all_subclasses


def get_runner_subclasses() -> list[type[BaseRunner]]:
    """Get all subclasses recursively."""
    subclasses: list[type[BaseRunner]] = []
    for c in get_all_subclasses(BaseRunner):  # type: ignore[type-abstract]
        if (
            "mock" in c.__name__.lower()
            or c.__name__.startswith("Dummy")
            or c.__name__.startswith("External")
            or c.__name__.startswith("Patched")
        ):
            continue
        subclasses.append(c)
    return subclasses
