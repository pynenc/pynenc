# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the broker integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.broker.all_tests import *

from pynenc_tests.integration.broker.test_subclasses_routing import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
