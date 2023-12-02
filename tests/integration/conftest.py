import pytest


def get_defined_markers(config: pytest.Config) -> list[str]:
    """Return a list of marker names defined in the pytest configuration."""
    markers = config.getini("markers")
    return [marker.split(":")[0].strip() for marker in markers]


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    for marker in get_defined_markers(config):
        if marker in config.getoption("-m"):
            for item in items:
                if marker in item.callspec.id.lower():
                    marker_obj = getattr(pytest.mark, marker)
                    item.add_marker(marker_obj)
