"""
Config propagation test for process-based runners.

Verifies that configuration set via PynencBuilder in the parent process propagates
correctly to child processes spawned with the ``spawn`` start method. Without proper
propagation, child processes re-import modules and create new Pynenc app instances
with different configurations (e.g. different temp DB paths), which causes task
results to be written to the wrong database and the parent to hang forever.

The fix uses a class-level ``_subprocess_config`` registry on ``Pynenc``.  When the
parent's app is deserialized in the child (``__setstate__``), the parent's immutable
config snapshot is stored in the registry keyed by ``app_id``.  All Pynenc instances
with the same ``app_id`` resolve their ``config_values`` through the registry, so
lazily-created components (orchestrator, broker, …) use the parent's configuration.

Key components:
- test_config_propagation_persistent_process: Validates config in PersistentProcessRunner workers
"""

import tempfile
import threading

from pynenc import PynencBuilder

# Create temp database file at module load — in a child process (spawn)
# this entire module is re-imported, producing a DIFFERENT temp file path.
# Config propagation must ensure the child still uses the parent's path.
_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db_path = _temp_db.name
_temp_db.close()

app = (
    PynencBuilder()
    .sqlite(_temp_db_path)
    .persistent_process_runner(num_processes=2)
    .app_id("test-config-propagation")
    .build()
)


@app.task
def get_resolved_config() -> dict[str, str]:
    """Return resolved config values from the module-level app in this process.

    Accesses the module-level app's orchestrator config — the same code path
    every component follows.  Without config propagation the returned
    ``sqlite_db_path`` would point to a different temp file created when
    the child re-imported this module.
    """
    return {
        "sqlite_db_path": get_resolved_config.app.orchestrator.conf.sqlite_db_path,
        "app_id": app.app_id,
    }


def test_config_propagation_persistent_process() -> None:
    """Verify PersistentProcessRunner propagates config to child processes.

    Without the ``_subprocess_config`` registry, the child process re-imports
    this module, creates a new temp DB at a different path, and the task
    stores its result in the wrong database — making ``inv.result`` hang
    forever in the parent.

    With the registry, when the parent's app is deserialized in the child,
    ``__setstate__`` stores the parent's config snapshot.  The module-level
    app's ``config_values`` property resolves through the registry, so its
    lazily-created orchestrator connects to the parent's database.
    """
    app.purge()

    worker = threading.Thread(target=app.runner.run, daemon=True)
    worker.start()

    try:
        inv = get_resolved_config()
        result = inv.result

        assert result["sqlite_db_path"] == app.orchestrator.conf.sqlite_db_path
        assert result["app_id"] == app.app_id
    finally:
        app.runner.stop_runner_loop()
