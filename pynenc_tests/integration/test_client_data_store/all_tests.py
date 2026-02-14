# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the client_data_store integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.test_client_data_store.all_tests import *

from pynenc_tests.integration.test_client_data_store.test_all_client_data_store import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
