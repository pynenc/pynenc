# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.combinations.all_tests import *

from pynenc_tests.integration.combinations.test_app_combinations import *
from pynenc_tests.integration.combinations.test_client_data_store_performance import *
from pynenc_tests.integration.combinations.test_async_combinations import *
from pynenc_tests.integration.combinations.test_parallelize_performance import *
from pynenc_tests.integration.combinations.test_performance import *
from pynenc_tests.integration.combinations.test_core_tasks_combinations import *
from pynenc_tests.integration.combinations.test_status_owner_coherence import *
from pynenc_tests.integration.orchestrator.test_status_transitions_integrity import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
