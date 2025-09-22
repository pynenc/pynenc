import sqlite3

import pytest

from pynenc import Pynenc, PynencBuilder
from pynenc.state_backend.sqlite_state_backend import SQLiteStateBackend
from pynenc.workflow import WorkflowIdentity


@pytest.mark.usefixtures("temp_sqlite_db_path")
def test_workflow_purge(temp_sqlite_db_path: str) -> None:
    """
    Test that purge removes all data from SQLiteStateBackend tables.
    """
    app = PynencBuilder().sqlite(sqlite_db_path=temp_sqlite_db_path).build()
    # Check tables exist
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "state_backend_workflow_data" in tables

    # Store workflow data
    wf_id = WorkflowIdentity("wf_task", "wf_id")
    # Use the app's backend for assertions
    app.state_backend.set_workflow_data(wf_id, "key0", "value0")
    assert app.state_backend.get_workflow_data(wf_id, "key0") == "value0"

    # Check data exists
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM state_backend_workflow_data").fetchone()[
                0
            ]
            == 1
        )

    # Purge
    app.state_backend.purge()

    # Check all data is gone
    with sqlite3.connect(temp_sqlite_db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM state_backend_workflow_data").fetchone()[
                0
            ]
            == 0
        )
    assert app.state_backend.get_workflow_data(wf_id, "key0") is None


@pytest.mark.usefixtures("temp_sqlite_db_path")
def test_all_tables_prefix(temp_sqlite_db_path: str) -> None:
    """
    Test that all tables in the SQLiteStateBackend have the correct prefix.
    """
    app = Pynenc(config_values={"sqlite_db_path": temp_sqlite_db_path})
    SQLiteStateBackend(app)

    with sqlite3.connect(temp_sqlite_db_path) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        assert len(tables) > 0
        assert all(name.startswith("state_backend_") for name in tables)
