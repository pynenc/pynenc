# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the arg_cache plugins.
# Plugins should import: from pynenc_tests.unit.arg_cache.all_tests import *

from pynenc_tests.unit.arg_cache.test_arg_cache_all_instances import *

# Files in this folder that are NOT exported to plugins
# (implementation-specific tests or base class tests)
IGNORED_FILES = {
    "test_arg_cache",  # Base/general arg cache tests
    "test_disable_arg_cache",  # Disabled cache tests
    "test_local_arg_caches",  # Local implementation tests
}
