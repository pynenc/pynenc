import sqlite3

import pytest

from pynenc import PynencBuilder
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.state_backend.sqlite_state_backend import SQLiteStateBackend
from pynenc.identifiers.task_id import TaskId
from pynenc.util.sqlite_utils import sanitize_table_prefix
from pynenc.workflow import WorkflowIdentity


@pytest.mark.usefixtures("temp_sqlite_db_path")
def test_workflow_purge(temp_sqlite_db_path: str) -> None:
    """
    Test that purge removes all data from SQLiteStateBackend tables.
    """
    app = (
        PynencBuilder()
        .app_id("test_workflow_purge")
        .sqlite(sqlite_db_path=temp_sqlite_db_path)
        .build()
    )
    # Force initialization of state backend
    # It lazily registers the app info on first time backend is instantiated
    state_backend = app.state_backend
    assert isinstance(state_backend, SQLiteStateBackend)
    # Check tables exist
    wf_data_table = state_backend.tables.WORKFLOW_DATA
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert wf_data_table in tables

    # Store workflow data
    wf_id = WorkflowIdentity(
        InvocationId("test_wf_id"), TaskId("test_module", "test_func")
    )
    # Use the app's backend for assertions
    app.state_backend.set_workflow_data(wf_id, "key0", "value0")
    assert app.state_backend.get_workflow_data(wf_id, "key0") == "value0"

    # Check data exists
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {wf_data_table}").fetchone()[0] == 1

    # Purge
    app.state_backend.purge()

    # Check all data is gone
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {wf_data_table}").fetchone()[0] == 0
    assert app.state_backend.get_workflow_data(wf_id, "key0") is None


@pytest.mark.usefixtures("temp_sqlite_db_path")
def test_all_tables_prefix(temp_sqlite_db_path: str) -> None:
    """
    Test that all tables in the SQLiteStateBackend have the correct app_id prefix.
    """
    app_id = "test-all-tables-prefix"
    app = (
        PynencBuilder()
        .app_id(app_id)
        .sqlite(sqlite_db_path=temp_sqlite_db_path)
        .build()
    )
    SQLiteStateBackend(app)

    expected_prefix = f"{sanitize_table_prefix(app_id)}__state_backend_"
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        assert len(tables) > 0
        assert all(name.startswith(expected_prefix) for name in tables)
