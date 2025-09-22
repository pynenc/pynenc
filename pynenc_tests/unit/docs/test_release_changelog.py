import re
import tomllib
from pathlib import Path


def get_project_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[3] / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        pyproject_data = tomllib.load(f)
    return pyproject_data["tool"]["poetry"]["version"]


def test_release_changelog() -> None:
    """Test that the release has an entry in the changelog with proper formatting."""
    project_version = get_project_version()
    changelog_path = Path(__file__).resolve().parents[3] / "docs" / "changelog.md"

    with changelog_path.open() as f:
        changelog = f.read()

    # Check version exists with proper date format
    version_pattern = rf"## \[{project_version}\] [-–] \d{{4}}-\d{{2}}-\d{{2}}"
    assert re.search(version_pattern, changelog), (
        f"Version {project_version} not found in changelog or wrong date format. "
        f"Expected format: ## [{project_version}] - YYYY-MM-DD"
    )

    # Get version content and remove the version line itself
    version_content = changelog.split(f"## [{project_version}]")[1].split("## [")[0]

    # Extract lines, removing empty lines and the date line
    content_lines = [
        line
        for line in version_content.strip().split("\n")
        if line and not re.match(r"^[-–] \d{4}-\d{2}-\d{2}$", line.strip())
    ]

    # Check that version has at least one valid section
    valid_sections = [
        "### Added",
        "### Changed",
        "### Deprecated",
        "### Removed",
        "### Fixed",
        "### Security",
    ]
    has_valid_section = any(section in version_content for section in valid_sections)
    assert has_valid_section, (
        f"Version {project_version} must include at least one standard section type. "
        f"Valid sections are: {', '.join(valid_sections)}"
    )

    # Check if there's content without a section (ignoring empty lines and date line)
    # Track which section we're currently in
    current_section = None
    for line in content_lines:
        if line.startswith("###"):
            current_section = line
            continue

        # Skip empty lines and properly indented list items
        if not line.strip() or line.startswith("  "):
            continue

        # Line is part of a list item but not indented
        if line.strip().startswith("-") and current_section:
            continue

        # Any other line should be in a section
        assert current_section is not None, (
            f"Found content without a section in version {project_version}: '{line}'. "
            f"All changes must be under a valid section type."
        )
