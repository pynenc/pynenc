# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the state_backend integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.state_backend.all_tests import *

from pynenc_tests.integration.state_backend.test_state_backend import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
