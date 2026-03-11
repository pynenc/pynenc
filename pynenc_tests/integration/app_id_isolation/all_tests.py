# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the app_id isolation integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.app_id_isolation.all_tests import *

from pynenc_tests.integration.app_id_isolation.test_app_id_isolation import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
