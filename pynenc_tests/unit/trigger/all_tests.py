# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the trigger plugins.
# Plugins should import: from pynenc_tests.unit.trigger.all_tests import *

from pynenc_tests.unit.trigger.test_trigger_all_instances import *

# Files in this folder that are NOT exported to plugins
# (implementation-specific tests or base class tests)
IGNORED_FILES = {
    "test_trigger_builder",  # Builder-specific tests
}
