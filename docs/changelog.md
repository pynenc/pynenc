# Changelog

For detailed information on each version, please visit the [Pynenc GitHub Releases page](https://github.com/pynenc/pynenc/releases).

## Version 0.0.15

- **Development Infrastructure**:

  - Added comprehensive Makefile for development tasks
  - Simplified test execution and coverage reporting
  - Improved development workflow documentation

- **Bug Fixes**:
  - Fixed redundant orchestrator notification in `DistributedInvocationGroup.results` when all invocations are already resolved

## Version 0.0.14

- **Redis Connection Improvements**:
  - Added `redis_client` utility to centralize Redis connection handling
  - Support for Redis URL-based connections
  - Improved handling of Redis credentials (username/password)
  - Added comprehensive test coverage for Redis client creation
  - Empty credentials now properly handled as None values

## Version 0.0.13

- **New MultiThreadRunner Implementation**:
  - Added `MultiThreadRunner` class that manages multiple `ThreadRunner` instances across separate processes
  - Added process-level task distribution and management
  - Implemented dynamic process scaling based on pending invocations
  - Added process lifecycle management (spawn/terminate) based on workload
  - Added idle process detection and cleanup
  - Added shared status tracking between processes
  - Added configuration options:
    - `max_threads`: Override default threads per process (default: 4)
    - `min_processes`: Minimum number of processes (default: 1)
    - `max_processes`: Maximum processes, defaults to CPU count
    - `idle_timeout_process_sec`: Time before terminating idle processes (default: 4s)
    - `enforce_max_processes`: Flag to maintain max processes count (default: False)
- **Broker Enhancements**:
  - Added `count_invocations()` method to `BaseBroker` for efficient queue size checking
- **ThreadRunner Improvements**:
  - Added configurable `min_threads` and `max_threads` settings
  - Optimized thread management when running under `MultiThreadRunner`

## Version 0.0.12

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
