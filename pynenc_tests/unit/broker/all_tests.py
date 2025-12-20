# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the broker plugins.
# Plugins should import: from pynenc_tests.unit.broker.all_tests import *

from pynenc_tests.unit.broker.test_broker_all_instances import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
