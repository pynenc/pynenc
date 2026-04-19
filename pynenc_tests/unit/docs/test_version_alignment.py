import re
import tomllib
from pathlib import Path


def get_project_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[3] / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        pyproject_data = tomllib.load(f)
    return pyproject_data["project"]["version"]


def test_readme_whats_new_version_matches_project() -> None:
    """Test that the README 'What's New' section references the current project version."""
    project_version = get_project_version()
    readme_path = Path(__file__).resolve().parents[3] / "README.md"

    with readme_path.open() as f:
        readme = f.read()

    whats_new_pattern = rf"What's New in v{re.escape(project_version)}"
    assert re.search(whats_new_pattern, readme), (
        f"README.md 'What's New' section references a different version than "
        f"the current project version ({project_version}). "
        f'Expected to find: "What\'s New in v{project_version}"'
    )
