# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.17] - 2025-03-04

### Fixed

- Handle broken pipe errors during MultiThreadRunner shutdown
- Prevent crashes when cleaning up shared state after manager shutdown
- Add safe removal of process state from manager dictionary

### Added

- Argument caching system for large serialized arguments:

  - Configurable size threshold for caching (default 1KB)
  - Multiple caching strategies: identity, hash, fingerprint, and content
  - Storage implementations:
    - Redis-based for distributed caching
    - Memory-based for development/testing
    - Disabled option for bypassing caching
  - LRU cache with configurable size for local caches

- New task configuration options:

  - `call_result_cache`: Enable reuse of previous results
  - `disable_cache_args`: Specify arguments to exclude from caching

- Runner-level shared cache:

  - Process-safe managed dictionary for sharing data
  - Automatic fallback to local cache when runner isn't available
  - Shared storage across all invocations in same machine
  - Optimized for large serialized arguments

- Configurable argument string representation:
  - Multiple display modes (FULL, KEYS, TRUNCATED, HIDDEN)
  - Configurable truncation length
  - Security-focused option to hide argument values
  - Per-application configuration control

### Changed

- Enhanced argument serialization with caching
- Improved memory efficiency by sharing cache across processes
- Optimized large argument handling in distributed tasks
- Improved Redis broker efficiency using BLPOP with configurable timeout
- Reduced CPU usage in message queue polling by eliminating continuous polling

### Documentation

- Added Use Case 9: Argument Caching guide with:
  - Basic usage examples with numpy arrays
  - Configuration options in pyproject.toml
  - Cache control mechanisms per task
  - Backend selection guidelines
  - Performance optimization tips
  - Cache sharing explanations
  - LRU cache management details

## [0.0.16] - 2025-03-02

### Added

- Millisecond precision to log timestamps.
- Colored output for logs using ANSI color codes.
- Comprehensive performance testing framework:
  - Support for different runner types (Thread, Process, MultiThread).
  - Automated test configuration based on runner type.
  - Performance metrics collection and analysis.
  - Environment-aware test parameters.
- Automated PR test releases to TestPyPI:
  - Version format: `{version}rc{run_number}.pr{pr_number}`.
  - Automatic deployment on PR creation and updates.
  - PR comments with installation instructions.
  - Skip existing versions to handle multiple PR updates.
- Direct log testing with `capture_logs` utility in tests
  - Support for colored output in test assertions
  - No dependency on pytest's caplog fixture
  - Automatic cleanup with context manager

### Changed

- Improved logging format with colored level names and prefixes.
- Restructured performance tests for better maintainability.
- Enhanced test assertions with detailed performance data.

## [0.0.15] - 2025-02-28

### Added

- Comprehensive Makefile for development tasks.
  - Test execution, coverage reporting, and Docker Redis container management.
- Updated CONTRIBUTING.md with development workflow details.

### Fixed

- Redundant orchestrator notification in `DistributedInvocationGroup.results` to avoid unnecessary Redis calls.

## [0.0.14] - 2025-02-28

### Added

- Centralized `redis_client` utility for consistent Redis connection handling.
  - Support for both URL-based and parameter-based connections.
  - Comprehensive test coverage for Redis client creation.

### Fixed

- Handling of empty Redis credentials now properly treats them as `None`.

## [0.0.13] - 2025-02-28

### Added

- `MultiThreadRunner` class to manage multiple `ThreadRunner` instances across processes.
  - Process-level task distribution and dynamic scaling.
  - Process lifecycle management including idle detection and cleanup.
  - Configuration options:
    - `max_threads`: Override default threads per process (default: 4).
    - `min_processes`: Minimum number of processes (default: 1).
    - `max_processes`: Maximum processes (defaults to CPU count).
    - `idle_timeout_process_sec`: Time before terminating idle processes (default: 4s).
    - `enforce_max_processes`: Flag to maintain max processes count (default: False).
- Broker enhancements:
  - Added `count_invocations()` to `BaseBroker` for efficient queue size checking.
- ThreadRunner improvements:
  - Configurable `min_threads` and `max_threads` settings.
  - Optimized thread management when running under `MultiThreadRunner`.

## [0.0.12] - 2025-02-25

### Changed

- Relaxed dependency version pins for redis and pyyaml to improve compatibility.

## [0.0.11] - 2025-02-25

### Changed

- Relaxed dependency version pins (redis and pyyaml) and adjusted constraints in `pyproject.toml` for better integration.

## [0.0.10] - 2024-03-15

### Changed

- Refactored the configuration module to use the external package `pynenc.cistell`.

## [0.0.9] - 2024-02-07

### Changed

- Specified available options in the task decorator for improved code clarity.
- Added unit tests to ensure synchronization between `ConfigTask` and task decorator parameters.

## [0.0.8] - 2024-02-05

### Changed

- Added tests to verify changelog sync with the pynenc version.
- Made minimal changes in the documentation.

## [0.0.7] - 2024-02-04

### Changed

- Improved documentation and updated README.md.

## [0.0.6] - 2024-02-02

### Changed

- Use autodoc2 for automatic documentation using Sphinx and Myst markdown formats
- Refactor all the docstrings in markdown and sphinx syntax
- Improve README.md adding detailed informatino
- Adding Pynenc logo to the docs
- Move all the docs files to md format
- Using absolute imports (required by autodoc2)
- Adding new tests for get_subclasses (requires imports in module `__init__`)

## [0.0.5] - 2024-01-12

### Changed

- Enhanced exception handling for tasks in the `__main__` module.
  - Now raises a RuntimeError for tasks defined in `__main__`.
  - Updated serialization/deserialization in test/dev environments.
  - Revised task configuration arguments and logging in `dist_invocation`.

### Added

- New tests for exception handling and task ID generation.
- Documentation updates to reflect these changes.

## [0.0.4] - 2024-01-06

### Added

- Added a scripts section to `pyproject.toml` for the CLI executable.

## [0.0.3] - 2024-01-06

### Changed

- Renamed `MemRunner` to `ThreadRunner`.
- Implemented a command line interface for starting runners.

### Added

- Added configuration options for specifying subclasses.
- Introduced automatic task retry functionality.

## [0.0.2] - 2023-12-10

### Changed

- Fixed GitHub Actions configuration.
- Resolved bug in runners when only one thread was used globally.
- Fixed config inheritance issues and class/instance variables.

### Added

- Added timeouts to integration tests.

## [0.0.1] - 2023-12-10

### Added

- Initial development of GitHub Actions for testing and building the package.

The changelog documents the history of changes and version releases for Pynenc.
