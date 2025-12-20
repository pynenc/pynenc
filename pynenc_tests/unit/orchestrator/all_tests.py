# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the tests that should run in the orchestrator plugins.
# Plugins should import: from pynenc_tests.unit.orchestrator.all_tests import *

from pynenc_tests.unit.orchestrator.test_all_orchestrator_instances import *
from pynenc_tests.unit.orchestrator.test_atomic_service_all_instances import *
from pynenc_tests.unit.orchestrator.test_blocking_control_all_instances import *
from pynenc_tests.unit.orchestrator.test_cycle_control_all_instances import *
from pynenc_tests.unit.orchestrator.test_recovery_service_all_instances import *
from pynenc_tests.unit.orchestrator.test_running_concurrency_all_instances import *

# Files in this folder that are NOT exported to plugins
# (implementation-specific tests or base class tests)
IGNORED_FILES = {
    "test_arg_pair",  # Internal utility tests
    "test_base_orchestrator",  # Base class tests
}
