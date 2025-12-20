# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the arg_cache integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.test_arg_cache.all_tests import *

from pynenc_tests.integration.test_arg_cache.test_all_arg_cache import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
