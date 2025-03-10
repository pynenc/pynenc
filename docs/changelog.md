# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.18] - 2025-03-06

### Added

- **Full support for asynchronous task execution and result retrieval**:

  - Introduced `async_result()` for **awaiting individual task results**.
  - Introduced `async_results()` for **awaiting multiple task results in parallel**.
  - Enabled **async group invocations** using `parallelize().async_results()`.

- **File Path Support in `find_app_instance`**: Enhanced `pynenc.util.import_app.find_app_instance` to support loading a `Pynenc` instance from a file path (e.g., `path/to/app.py`) in addition to module paths. The function now detects file paths using `os.path.sep` or `.py` extension, loads the module with `spec_from_file_location`, and adjusts `sys.path` for relative imports.
- **Enhanced `sys.path` Handling in `find_app_instance`**: For file paths, now adds the inferred project root (three levels up from the file) to `sys.path`, enabling nested imports (e.g., `from core.params.config_helpers import load_settings`) to resolve correctly, aligning with tools like Uvicorn.
- **Comprehensive Tests for File Path Loading**: Added `tests/unit/util/test_import_app_filepath.py` with full test coverage for `find_app_instance` file path functionality, including:
  - Successful loading with and without `.py` extension.
  - Error handling for nonexistent files, invalid module specs, and missing `Pynenc` instances.
  - Support for imports from sibling files via `sys.path` modification.
  - Type hints for static type checking with `mypy`.
- **CLI Test for File Path Config**: Added `test_cli_show_config_with_file_path` to `tests/unit/cli/test_config_cli.py` to verify the CLI command `pynenc --app <file_path> show_config` loads and displays configuration from a file path.

### Tests

- **New unit tests for async result handling**:

  - Verified that `async_waiting_for_results()` correctly delegates to `_waiting_for_results()`.
  - Ensured that external invocations in `DummyRunner` use `time.sleep()` as expected.
  - Added tests for `async_result()` and `async_results()` behavior with **delayed and failing invocations**.

- **New integration tests for async execution**:
  - Full coverage of async task execution including:
    - **Simple async tasks**
    - **Async sleep-based tasks**
    - **Tasks raising exceptions**
    - **Tasks with async dependencies**
    - **Cycle detection in async tasks**
  - Group invocation tests verifying **async parallel execution**.

### Changed

- Optimized `async_result()` to **improve polling efficiency** and reduce unnecessary orchestrator calls.
- Updated `async_waiting_for_results()` to **respect configurable sleep time** for better performance in distributed runners.

- Improved ThreadRunner error handling:
  - Wrapped thread creation in `runner_loop_iteration()` in try/except; on failure (RuntimeError), the offending invocation is requeued via `reroute_invocations`.
  - Enhanced cleanup of finished threads in the `available_threads` property by joining and removing them.
  - Clarified the use of `daemon=True` for threads (daemon threads won’t block process exit).
  - Slight adjustments to `_waiting_for_results` to continue polling the local final cache for dependency resolution.

### Tests

- **Enhanced test isolation in `conftest.py`:**

  - **Before**: Mocks were defined at the class level, leading to **state sharing** across tests.
  - **Now**: Each mock method is encapsulated **within the instance**, preventing **unexpected state persistence** in parallel test runs.
  - **Improved MockPynenc instantiation** to ensure each test gets **a fresh, isolated instance**.

- **Renamed `SynchronousInvocation` to `ConcurrentInvocation`:**

  - **Why?** The term _Synchronous_ was misleading since the class is designed for **concurrent execution**, not strictly _blocking synchronous_ execution.
  - **What changed?**
    - All references to `SynchronousInvocation` have been **replaced** with `ConcurrentInvocation`.
    - Code, documentation, and tests **updated accordingly** to reflect the new naming.
    - Ensured backward compatibility by **aliasing** `SynchronousInvocation` to `ConcurrentInvocation` (this will be removed in a future version).

- Added a unit test (`test_thread_start_failure`) that forces thread creation to fail and verifies that the invocation is correctly rerouted.
- Updated integration tests to cover asynchronous task execution, waiting, failure, dependency, and parallel performance.

- **Fixed runner reference update in subprocesses:**

  - When a MultiThreadRunner spawns a ThreadRunner in a subprocess, the ThreadRunner now explicitly resets `app.runner` to itself.
  - This ensures that tasks executed within the subprocess use the correct runner instance, eliminating spurious warnings from `waiting_for_results`.

- **Optimized `ThreadRunner` to poll local cache instead of pausing threads**:
  - **Before**: Used `threading.Condition` to pause threads in `_waiting_for_results`, relying on Redis status checks and condition notifications.
  - **Now**: Polls a local `OrderedDict` (`final_invocations`) to check for completed dependencies, reducing Redis queries and eliminating thread suspension overhead.
  - **Details**:
    - Removed `wait_conditions` and simplified `wait_invocation` to a `set` of collective dependencies.
    - Introduced `final_invocations` with a max size of 10,000 entries, evicting oldest entries when full.
    - Centralized status checks in `runner_loop_iteration`, populating the local cache and removing finalized invocations from `wait_invocation`.
    - Updated `_waiting_for_results` to poll the cache with a configurable sleep time (`invocation_wait_results_sleep_time_sec`), resuming tasks when all dependencies are final.
  - **Impact**: Improves performance by avoiding context switches, though it may slightly increase CPU usage due to polling. Memory is bounded by the cache size limit.

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
