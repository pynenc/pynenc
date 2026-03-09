# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the orchestrator integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.orchestrator.all_tests import *

from pynenc_tests.integration.orchestrator.test_blocking_control import *
from pynenc_tests.integration.orchestrator.test_config_propagation import *
from pynenc_tests.integration.orchestrator.test_get_invocations import *
from pynenc_tests.integration.orchestrator.test_invocation_registration_concurrency import *
from pynenc_tests.integration.orchestrator.test_invocation_running_concurrency import *
from pynenc_tests.integration.orchestrator.test_invocation_status import *
from pynenc_tests.integration.orchestrator.test_pending_status import *
from pynenc_tests.integration.orchestrator.test_retries import *
from pynenc_tests.integration.orchestrator.test_status_transitions_integrity import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
