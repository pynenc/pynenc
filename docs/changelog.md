# Changelog

For detailed information on each version, please visit the [Pynenc GitHub Releases page](https://github.com/pynenc/pynenc/releases).

## Version 0.0.11

- Relax dependency version pins for redis and pyyaml to improve compatibility

## Version 0.0.10

- Refactor of conf module to use pynenc.cistell pypi package

## Version 0.0.9

- Specify available pptions in task decorator for improved code clarity and completion
- Adding unit test to ensure that ConfigTask and task decorator parameters are in sync

## Version 0.0.8

- Test for (this) changelog so it's sync with pynenc version
- Minimum changes in the docs

## Version 0.0.7

- Improve the docs and README.md

## Version 0.0.6

- Use autodoc2 for automatic documentation using Sphinx and Myst markdown formats
- Refactor all the docstrings in markdown and sphinx syntax
- Improve README.md adding detailed informatino
- Adding Pynenc logo to the docs
- Move all the docs files to md format
- Using absolute imports (required by autodoc2)
- Adding new tests for get_subclasses (requires imports in module `__init__`)

## Version 0.0.5

- **Implement exception handling for tasks in **main** module and enhance test robustness**:
  This update includes raising RuntimeError for task definitions in `__main__` modules,
  serialization/deserialization in test/dev environments, syntax changes in task configuration arguments,
  logging updates in `dist_invocation`, new tests for exception handling and task ID generation,
  and comprehensive documentation revisions.

## Version 0.0.4

- Add scripts section to pyproject.toml for cli executable

## Version 0.0.3

- Rename MemRunner to ThreadRunner
- Implement command line interface for starting runners
- Configuration options for specifying subclasses
- Adding automatic task retry

## Version 0.0.2

- Fixing github actions
- Bug on runners when running only one thread globaly
- Fix config inheritance and class/instance variables
- Add timeout to integration tests

The changelog documents the history of changes and version releases for Pynenc.

## Version 0.0.1

- Developing github actions for testing and building the package
