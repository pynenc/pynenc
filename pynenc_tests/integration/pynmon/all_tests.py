# ruff: noqa: F403
# mypy: ignore-errors
# This file contains the pynmon integration tests that should run in plugins.
# Plugins should import: from pynenc_tests.integration.pynmon.all_tests import *

from pynenc_tests.integration.pynmon.test_calls import *
from pynenc_tests.integration.pynmon.test_home_page import *
from pynenc_tests.integration.pynmon.test_family_tree_deep import *
from pynenc_tests.integration.pynmon.test_family_tree_massive import *
from pynenc_tests.integration.pynmon.test_invocation import *
from pynenc_tests.integration.pynmon.test_invocations_pagination import *
from pynenc_tests.integration.pynmon.test_invocations_timeline import *
from pynenc_tests.integration.pynmon.test_invocations_timeline_complex import *
from pynenc_tests.integration.pynmon.test_invocations_timeline_multi_runner import *
from pynenc_tests.integration.pynmon.test_log_explorer_formats import *
from pynenc_tests.integration.pynmon.test_sub_workflows import *
from pynenc_tests.integration.pynmon.test_task_details import *
from pynenc_tests.integration.pynmon.test_tasks_list import *
from pynenc_tests.integration.pynmon.test_workflow_runs import *
from pynenc_tests.integration.pynmon.test_workflows import *

# Files in this folder that are NOT exported to plugins
IGNORED_FILES: set[str] = set()
