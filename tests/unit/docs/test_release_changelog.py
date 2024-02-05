import tomllib
from pathlib import Path


def get_project_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[3] / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        pyproject_data = tomllib.load(f)
    return pyproject_data["tool"]["poetry"]["version"]


def test_release_changelog() -> None:
    """Test that the release has an entry in the changelog."""
    project_version = get_project_version()
    with open("docs/changelog.md") as f:
        changelog = f.read()
    assert (
        f"## Version {project_version}" in changelog
    ), f"Version {project_version} not found in changelog."
