"""
Validate PR descriptions for release note quality.

This script ensures PRs have sufficient description quality to be used
in automated release notes. It checks title format, description length,
and content quality. Works in conjunction with Release Drafter to ensure
proper categorization and quality release notes.
"""

import os
import re
import sys

from github import Github


class PRValidator:
    """Validates PR descriptions for release readiness."""

    MIN_DESCRIPTION_LENGTH = 50
    MIN_TITLE_LENGTH = 10

    # Conventional commit types from your commit instructions
    VALID_COMMIT_TYPES = {"feat", "fix", "docs", "style", "refactor", "test", "chore"}

    # Labels used by Release Drafter for categorization
    RELEASE_DRAFTER_LABELS = {
        "feature",
        "enhancement",
        "fix",
        "bugfix",
        "bug",
        "documentation",
        "docs",
        "test",
        "tests",
        "chore",
        "refactor",
        "style",
        "major",
        "minor",
        "patch",
    }

    PLACEHOLDER_PATTERNS = [
        r"\bwip\b",
        r"\btodo\b",
        r"\bfixme\b",
        r"\btemporary\b",
        r"\btbd\b",
        r"\bcoming soon\b",
    ]

    def __init__(self, github_token: str, repository: str, pr_number: int):
        self.github = Github(github_token)
        self.repo = self.github.get_repo(repository)
        self.pr = self.repo.get_pull(pr_number)
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def validate(self) -> bool:
        """
        Run all validation checks on the PR.

        :return: True if all validations pass
        """
        self._validate_title()
        self._validate_conventional_commit_format()
        self._validate_description()
        self._validate_content_quality()
        self._validate_labels()

        if self.errors:
            self._write_results()
            return False

        if self.warnings:
            self._write_results()

        return True

    def _validate_title(self) -> None:
        """Validate PR title meets minimum standards."""
        title = self.pr.title.strip()

        if len(title) < self.MIN_TITLE_LENGTH:
            self.errors.append(
                f"❌ **Title too short**: Must be at least {self.MIN_TITLE_LENGTH} characters "
                f"(currently {len(title)})"
            )

    def _validate_conventional_commit_format(self) -> None:
        """Validate title follows conventional commit format."""
        title = self.pr.title.strip()

        # Pattern: type(scope): description or type: description
        pattern = r"^(feat|fix|docs|style|refactor|test|chore)(\([^)]+\))?: .+"

        if not re.match(pattern, title, re.IGNORECASE):
            self.errors.append(
                "❌ **Invalid title format**: Must follow conventional commit format:\n"
                "   - `type(scope): description` or `type: description`\n"
                f"   - Valid types: {', '.join(sorted(self.VALID_COMMIT_TYPES))}\n"
                f"   - Example: `feat(triggers): add cron expression support`\n"
                f"   - Current: `{title}`"
            )

    def _validate_description(self) -> None:
        """Validate PR description has sufficient content."""
        body = self.pr.body or ""

        # Remove markdown formatting for length calculation
        clean_body = re.sub(r"```[\s\S]*?```", "", body)
        clean_body = re.sub(r"##+\s+", "", clean_body)
        clean_body = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", clean_body)
        clean_body = clean_body.strip()

        if not clean_body:
            self.errors.append(
                "❌ **No description**: Please add a description explaining what this PR does."
            )
            return

        if len(clean_body) < self.MIN_DESCRIPTION_LENGTH:
            self.errors.append(
                f"❌ **Description too short**: Must be at least {self.MIN_DESCRIPTION_LENGTH} "
                f"characters (currently {len(clean_body)}). Please explain what changed and why."
            )

    def _validate_content_quality(self) -> None:
        """Validate content doesn't contain placeholder text."""
        combined_text = f"{self.pr.title}\n{self.pr.body or ''}".lower()

        for pattern in self.PLACEHOLDER_PATTERNS:
            if re.search(pattern, combined_text, re.IGNORECASE):
                self.errors.append(
                    f"❌ **Placeholder text detected**: Found '{pattern}' - "
                    "please complete the description before requesting review."
                )

    def _validate_labels(self) -> None:
        """Validate PR has appropriate labels for Release Drafter."""
        pr_labels = {label.name for label in self.pr.labels}

        if not pr_labels:
            self.warnings.append(
                "⚠️ **No labels**: Consider adding labels for better release notes categorization.\n"
                f"   Suggested labels: {', '.join(sorted(self.RELEASE_DRAFTER_LABELS))}"
            )
            return

        # Check if any label matches Release Drafter categories
        if not pr_labels & self.RELEASE_DRAFTER_LABELS:
            self.warnings.append(
                "⚠️ **Missing categorization labels**: Add labels for release notes:\n"
                "   - Feature: `feature`, `enhancement`\n"
                "   - Bug Fix: `fix`, `bugfix`, `bug`\n"
                "   - Documentation: `documentation`, `docs`\n"
                "   - Testing: `test`, `tests`\n"
                "   - Maintenance: `chore`, `refactor`, `style`\n"
                "   - Version bump: `major`, `minor`, `patch`"
            )

    def _write_results(self) -> None:
        """Write validation results to file for GitHub Actions."""
        error_file = "/tmp/pr_validation_errors.txt"
        with open(error_file, "w") as f:
            if self.errors:
                for error in self.errors:
                    f.write(f"{error}\n\n")
                    print(error, file=sys.stderr)

            if self.warnings:
                f.write("\n---\n\n")
                for warning in self.warnings:
                    f.write(f"{warning}\n\n")
                    print(warning)


def main() -> None:
    """Validate PR and exit with appropriate code."""
    github_token = os.environ["GITHUB_TOKEN"]
    repository = os.environ["REPOSITORY"]
    pr_number = os.environ["PR_NUMBER"]

    if not all([github_token, repository, pr_number]):
        print("Error: Missing required environment variables", file=sys.stderr)
        sys.exit(1)

    try:
        pr_number_int = int(pr_number)
    except ValueError:
        print(f"Error: Invalid PR number: {pr_number}", file=sys.stderr)
        sys.exit(1)

    validator = PRValidator(github_token, repository, pr_number_int)

    if validator.validate():
        print("✅ PR validation passed!")
        if validator.warnings:
            print("\nℹ️ Consider addressing warnings for better release notes.")
        sys.exit(0)
    else:
        print("\n❌ PR validation failed. See errors above.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
