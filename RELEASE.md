# Release Process

This document outlines the release process for our Python package. Our release workflow is automated through GitHub Actions and uses Poetry for package management.

## Release Types

We have two types of releases:

1. **Development Releases** - Published to TestPyPI
2. **Production Releases** - Published to PyPI

## How to Create Releases

### Development Releases (to TestPyPI)

To create a development release to TestPyPI:

1. Merge your changes into the `main` branch **without changing the version** in `pyproject.toml`
2. GitHub Actions will automatically:
   - Detect that no version change was made
   - Create a temporary development version (patch + timestamp)
   - Build the package
   - Publish it to TestPyPI

Development releases are intended for testing and will have version numbers like `0.1.2.dev.1677593784`.

### Production Releases (to PyPI)

To create a production release to PyPI:

1. Update the version in `pyproject.toml` following [Semantic Versioning](https://semver.org/) principles
2. Update the CHANGELOG.md with details about the new release
3. Merge these changes into the `main` branch
4. GitHub Actions will automatically:
   - Detect the version change
   - Create a git tag matching the new version
   - Build the package
   - Publish it to PyPI
   - Generate release notes

## Version Numbering

Our project follows semantic versioning (SemVer):

- **MAJOR** version for incompatible API changes
- **MINOR** version for backward-compatible functionality additions
- **PATCH** version for backward-compatible bug fixes

## Release Checklist

Before creating a production release:

1. Ensure all tests are passing
2. Update documentation if necessary
3. Update the version in `pyproject.toml`
4. Update CHANGELOG.md with:
   - New version number and release date
   - Summary of changes
   - List of new features, bug fixes, and breaking changes
5. Create a pull request with these changes
6. After review, merge the pull request into `main`

## Troubleshooting

If a release fails:

1. Check the GitHub Actions logs for error details
2. Common issues include:
   - PyPI/TestPyPI authentication failures
   - Duplicate version numbers
   - Build errors

## Manual Releases

If you need to create a release manually:

1. Follow the steps to update versions as described above
2. Run locally:

   ```bash
   poetry build
   poetry publish
   ```

## Additional Notes

- The release process uses the [salsify/action-detect-and-tag-new-version](https://github.com/salsify/action-detect-and-tag-new-version) action to detect version changes
- Release notes are automatically generated using [release-drafter](https://github.com/release-drafter/release-drafter)
