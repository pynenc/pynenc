# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the pynmon view tests that should run in plugins.
# Plugins should import: from pynenc_tests.unit.pynmon.views.all_tests import *

from pynenc_tests.unit.pynmon.views.test_broker import *
from pynenc_tests.unit.pynmon.views.test_home import *
from pynenc_tests.unit.pynmon.views.test_invocations import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES = {
    "test_pynmon_arg_cache",  # Arg cache specific views
    "test_pynmon_orchestrator",  # Orchestrator specific views
    "test_pynmon_state_backend",  # State backend specific views
}
