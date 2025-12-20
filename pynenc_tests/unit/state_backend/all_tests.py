# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the state_backend plugins.
# Plugins should import: from pynenc_tests.unit.state_backend.all_tests import *

from pynenc_tests.unit.state_backend.test_state_backend_all_instances import *

# Files in this folder that are NOT exported to plugins
# (implementation-specific tests or base class tests)
IGNORED_FILES = {
    "test_base_state_backend",  # Base class tests, not plugin-specific
    "test_mem_state_backend",  # Memory implementation tests
    "test_sqlite_state_backend",  # SQLite implementation tests
    "test_sub_invocations",  # Internal functionality tests
}
