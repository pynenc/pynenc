# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the client_data_store plugins.
# Plugins should import: from pynenc_tests.unit.client_data_store.all_tests import *

from pynenc_tests.unit.client_data_store.test_client_data_store_all_instances import *

# Files in this folder that are NOT exported to plugins
# (implementation-specific tests or base class tests)
IGNORED_FILES = {
    "test_client_data_store",  # Base/general client_data_store tests
    "test_disable_client_data_store",  # Disabled client_data_store tests
}
