# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the app plugins.
# Plugins should import: from pynenc_tests.unit.app.all_tests import *

from pynenc_tests.unit.app.test_app_all_instances import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES = {
    "test_app_task_list",  # Core app tests, not plugin-specific
}
