# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the app_id isolation unit tests that should run in plugins.
# Plugins should import: from pynenc_tests.unit.app_id_isolation.all_tests import *

from pynenc_tests.unit.app_id_isolation.test_app_id_isolation_all_instances import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
