"""
SQLite-based orchestrator for cross-process testing.

This module provides a SQLite-based orchestrator implementation that enables
true cross-process coordination for testing process runners. Unlike shared memory,
SQLite provides ACID transactions and handles concurrent access automatically.
"""

import sqlite3
from functools import cached_property
from time import time
from typing import TYPE_CHECKING, Callable, Iterator, Optional

from pynenc.conf.config_orchestrator import ConfigOrchestratorSQLite
from pynenc.invocation.status import InvocationStatus
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseCycleControl,
    BaseOrchestrator,
)
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task
    from pynenc.types import Params, Result


class Tables:
    INVOCATIONS = "orchestrator_invocations"
    INVOCATION_ARGS = "orchestrator_invocation_args"
    CYCLE_CALLS = "orchestrator_cycle_calls"
    CYCLE_EDGES = "orchestrator_cycle_edges"
    BLOCKING_EDGES = "orchestrator_blocking_edges"


class SQLiteCycleControl(BaseCycleControl):
    """
    Cycle control for SQLiteOrchestrator using SQLite for cross-process cycle detection.
    """

    def __init__(self, app: "Pynenc", sqlite_db_path: str) -> None:
        self.app = app
        self.sqlite_db_path = sqlite_db_path
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize SQLite tables for cycle tracking."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.CYCLE_CALLS} (
                    call_id TEXT PRIMARY KEY,
                    call_json TEXT NOT NULL
                )
            """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.CYCLE_EDGES} (
                    caller_id TEXT NOT NULL,
                    callee_id TEXT NOT NULL,
                    PRIMARY KEY (caller_id, callee_id)
                )
            """
            )
            conn.commit()

    def cleanup(self) -> None:
        """Cleanup method for BaseCycleControl compatibility."""
        try:
            with sqlite_conn(self.sqlite_db_path) as conn:
                conn.execute(f"DELETE FROM {Tables.CYCLE_CALLS}")
                conn.execute(f"DELETE FROM {Tables.CYCLE_EDGES}")
                conn.commit()
        except Exception:
            pass  # Ignore cleanup errors

    def add_call_and_check_cycles(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> None:
        """
        Add a call dependency and check for cycles using graph traversal.
        """
        from pynenc.exceptions import CycleDetectedError

        # Check for direct self-cycle first
        if caller.call_id == callee.call_id:
            raise CycleDetectedError.from_cycle([caller.call])

        caller_id = caller.call_id
        callee_id = callee.call_id

        with sqlite_conn(self.sqlite_db_path) as conn:
            # Add calls to tracking
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.CYCLE_CALLS} (call_id, call_json) VALUES (?, ?)",
                (caller_id, caller.call.to_json()),
            )
            conn.execute(
                f"INSERT OR REPLACE INTO {Tables.CYCLE_CALLS} (call_id, call_json) VALUES (?, ?)",
                (callee_id, callee.call.to_json()),
            )

            # Check for cycle before adding edge
            cycle = self._find_cycle_with_new_edge(conn, caller_id, callee_id)
            if cycle:
                # Convert call_ids back to Call objects for error message
                from pynenc.call import Call

                call_objects = []
                for call_id in cycle:
                    cursor = conn.execute(
                        f"SELECT call_json FROM {Tables.CYCLE_CALLS} WHERE call_id = ?",
                        (call_id,),
                    )
                    row = cursor.fetchone()
                    cursor.close()
                    if row:
                        try:
                            call_obj = Call.from_json(self.app, row[0])
                            call_objects.append(call_obj)
                        except Exception:
                            pass

                if call_objects:
                    raise CycleDetectedError.from_cycle(call_objects)

            # If no cycle, add the edge permanently
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {Tables.CYCLE_EDGES} (caller_id, callee_id) VALUES (?, ?)
            """,
                (caller_id, callee_id),
            )
            conn.commit()

    def get_callees(self, caller_call_id: str) -> Iterator[str]:
        """
        Returns an iterator of direct callee call_ids for the given caller_call_id.

        :param str caller_call_id: The call_id of the caller invocation.
        :return: Iterator of callee call_ids.
        :rtype: Iterator[str]
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT callee_id FROM {Tables.CYCLE_EDGES} WHERE caller_id = ?",
                (caller_call_id,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for row in cursor_rows:
                yield row[0]

    def _find_cycle_with_new_edge(
        self, conn: sqlite3.Connection, caller_id: str, callee_id: str
    ) -> list[str] | None:
        """
        Find cycle that would be caused by adding a new edge from caller to callee.
        """
        # Use DFS to detect cycles
        visited: set[str] = set()
        path: list[str] = []

        # Temporarily consider the new edge exists
        def get_edges(call_id: str) -> list[str]:
            cursor = conn.execute(
                f"SELECT callee_id FROM {Tables.CYCLE_EDGES} WHERE caller_id = ?",
                (call_id,),
            )
            edges = [row[0] for row in cursor.fetchall()]
            cursor.close()
            # Add the temporary edge if it's from this caller
            if call_id == caller_id and callee_id not in edges:
                edges.append(callee_id)
            return edges

        return self._find_cycle_dfs(caller_id, visited, path, get_edges)

    def _find_cycle_dfs(
        self,
        current_id: str,
        visited: set[str],
        path: list[str],
        get_edges: Callable[[str], list[str]],
    ) -> list[str] | None:
        """
        DFS utility to find cycles.
        """
        visited.add(current_id)
        path.append(current_id)

        for next_id in get_edges(current_id):
            if next_id not in visited:
                cycle = self._find_cycle_dfs(next_id, visited, path, get_edges)
                if cycle:
                    return cycle
            elif next_id in path:
                # Found cycle, return from cycle start to end
                cycle_start_idx = path.index(next_id)
                return path[cycle_start_idx:]

        path.remove(current_id)
        return None

    def clean_up_invocation_cycles(self, invocation_id: str) -> None:
        """Clean up cycle tracking data for a completed invocation."""
        call_id = self.app.orchestrator.get_invocation_call_id(invocation_id)
        if not self.app.orchestrator.any_non_final_invocations(call_id):
            with sqlite_conn(self.sqlite_db_path) as conn:
                # Remove from calls tracking
                conn.execute(
                    f"DELETE FROM {Tables.CYCLE_CALLS} WHERE call_id = ?", (call_id,)
                )
                # Remove from edges tracking
                conn.execute(
                    f"DELETE FROM {Tables.CYCLE_EDGES} WHERE caller_id = ? OR callee_id = ?",
                    (call_id, call_id),
                )
                conn.commit()


class SQLiteBlockingControl(BaseBlockingControl):
    """
    Blocking control for SQLiteOrchestrator using SQLite for cross-process invocation dependencies.

    This class manages dependencies between task invocations, ensuring that invocations waiting for others
    are properly tracked and released. Implements blocking control using persistent SQLite tables.

    Key components:
    - waiting_for: Tracks which invocations are waiting for results from others
    - waited_by: Tracks which invocations are being waited on by others
    """

    def __init__(self, app: "Pynenc", sqlite_db_path: str) -> None:
        self.app = app
        self.sqlite_db_path = sqlite_db_path
        self._init_tables()

    def _init_tables(self) -> None:
        """Initialize SQLite table for blocking control."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.BLOCKING_EDGES} (
                    waiter_id TEXT NOT NULL,
                    waited_id TEXT NOT NULL,
                    PRIMARY KEY (waiter_id, waited_id)
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_blocking_waited_id ON {Tables.BLOCKING_EDGES}(waited_id)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_blocking_waiter_id ON {Tables.BLOCKING_EDGES}(waiter_id)"
            )
            conn.commit()

    def waiting_for_results(
        self, caller_invocation_id: str, result_invocation_ids: list[str]
    ) -> None:
        """Notifies the system that an invocation is waiting for the results of other invocations."""
        waiter_id = caller_invocation_id
        with sqlite_conn(self.sqlite_db_path) as conn:
            for waited_id in result_invocation_ids:
                conn.execute(
                    f"INSERT OR IGNORE INTO {Tables.BLOCKING_EDGES} (waiter_id, waited_id) VALUES (?, ?)",
                    (waiter_id, waited_id),
                )
                conn.execute(
                    f"INSERT OR IGNORE INTO {Tables.BLOCKING_EDGES} (waiter_id, waited_id) VALUES (?, ?)",
                    (waiter_id, waited_id),
                )
            conn.commit()

    def release_waiters(self, waited_invocation_id: str) -> None:
        """Removes an invocation from the graph, along with any dependencies related to it."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"DELETE FROM {Tables.BLOCKING_EDGES} WHERE waited_id = ?",
                (waited_invocation_id,),
            )
            conn.commit()

    def get_blocking_invocations(self, max_num_invocations: int) -> Iterator[str]:
        """Retrieves invocations that are blocking others but are not themselves waiting for any results."""
        available_statuses = tuple(
            status.value for status in InvocationStatus.get_available_for_run_statuses()
        )
        placeholders = ",".join("?" for _ in available_statuses)
        query = f"""
            SELECT DISTINCT b.waited_id
            FROM {Tables.BLOCKING_EDGES} b
            JOIN {Tables.INVOCATIONS} i ON b.waited_id = i.invocation_id
            WHERE b.waited_id NOT IN (
                SELECT waiter_id FROM {Tables.BLOCKING_EDGES}
            )
            AND i.status IN ({placeholders})
            LIMIT ?
        """
        params = [*available_statuses, max_num_invocations]
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(query, params)
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (waited_id,) in cursor_rows:
                yield waited_id


class SQLiteOrchestrator(BaseOrchestrator):
    """
    A SQLite-based implementation of the orchestrator for cross-process testing.

    This orchestrator uses SQLite for cross-process coordination and implements
    all required abstract methods from BaseOrchestrator. It's designed specifically
    for testing process runners.

    ```{warning}
    The `SQLiteOrchestrator` class is designed for testing purposes only and should
    not be used in production systems. It uses temporary SQLite files for state.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)

        # Use database path from configuration with validation
        self.sqlite_db_path = get_sqlite_sqlite_db_path(self.conf.sqlite_db_path)

        # Initialize database tables
        self._init_tables()

        # Initialize control components
        self._cycle_control = SQLiteCycleControl(app, self.sqlite_db_path)
        self._blocking_control = SQLiteBlockingControl(app, self.sqlite_db_path)

    def _init_tables(self) -> None:
        """Initialize SQLite tables for orchestrator state."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.INVOCATIONS} (
                    invocation_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    call_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    pending_start_time REAL,
                    pre_pending_status TEXT,
                    auto_purge_timestamp REAL
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_orchestrator_task_id ON {Tables.INVOCATIONS}(task_id)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_orchestrator_call_id ON {Tables.INVOCATIONS}(call_id)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_orchestrator_status ON {Tables.INVOCATIONS}(status)"
            )

            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.INVOCATION_ARGS} (
                    invocation_id TEXT NOT NULL,
                    arg_key TEXT NOT NULL,
                    arg_value TEXT NOT NULL,
                    PRIMARY KEY (invocation_id, arg_key)
                )
            """
            )
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_orchestrator_args_key_value ON {Tables.INVOCATION_ARGS}(arg_key, arg_value)
            """
            )
            conn.commit()

    @cached_property
    def conf(self) -> ConfigOrchestratorSQLite:
        return ConfigOrchestratorSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def cycle_control(self) -> BaseCycleControl:
        """Return cycle control."""
        return self._cycle_control

    @property
    def blocking_control(self) -> BaseBlockingControl:
        """Return blocking control."""
        return self._blocking_control

    def _register_new_invocations(
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """Register new invocations with status Register if they don't exist yet."""
        # TRY TO INSERT, IF CONFLICT, DO NOTHING
        with sqlite_conn(self.sqlite_db_path) as conn:
            for invocation in invocations:
                conn.execute(
                    f"""
                    INSERT INTO {Tables.INVOCATIONS} (
                    invocation_id, task_id, call_id, status
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(invocation_id) DO NOTHING
                """,
                    (
                        invocation.invocation_id,
                        invocation.task.task_id,
                        invocation.call_id,
                        InvocationStatus.REGISTERED.value,
                    ),
                )
            conn.commit()

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> Iterator[str]:
        """
        Get existing invocation IDs for a task, optionally filtered by arguments and statuses.

        :param Task[Params, Result] task: The task for which to retrieve invocation IDs.
        :param dict[str, str] | None key_serialized_arguments: Serialized arguments to filter invocations.
        :param list[InvocationStatus] | None statuses: The statuses to filter invocations.
        :return: An iterator over matching invocation IDs.
        """
        query = f"SELECT i.invocation_id FROM {Tables.INVOCATIONS} i"
        joins = []
        params = []
        if key_serialized_arguments:
            idx = 0
            for k, v in key_serialized_arguments.items():
                alias = f"a{idx}"
                joins.append(
                    f"JOIN {Tables.INVOCATION_ARGS} {alias} ON i.invocation_id = {alias}.invocation_id AND {alias}.arg_key = ? AND {alias}.arg_value = ?"
                )
                params.extend([k, v])
                idx += 1
        wheres = ["i.task_id = ?"]
        params.append(task.task_id)
        if statuses:
            wheres.append(f"i.status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])
        sql = query + " " + " ".join(joins)
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, params)
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield invocation_id

    def get_task_invocation_ids(self, task_id: str) -> Iterator[str]:
        """Retrieves all invocation ids for a given task id."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {Tables.INVOCATIONS} WHERE task_id = ?",
                (task_id,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield invocation_id

    def get_call_invocation_ids(self, call_id: str) -> Iterator[str]:
        """Retrieves all invocation ids for a given call id."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {Tables.INVOCATIONS} WHERE call_id = ?",
                (call_id,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield invocation_id

    def get_invocation_call_id(self, invocation_id: str) -> str:
        """Retrieves the call ID associated with a specific invocation ID."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT call_id FROM {Tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            return cursor.fetchone()[0]

    def any_non_final_invocations(self, call_id: str) -> bool:
        """Checks if there are any non-final invocations for a specific call ID."""
        final_status = [s.value for s in InvocationStatus.get_final_statuses()]
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT 1 FROM {Tables.INVOCATIONS}
                WHERE call_id = ? AND status NOT IN ({','.join(['?' for _ in final_status])})
                LIMIT 1
                """,
                (call_id, *final_status),
            )
            return cursor.fetchone() is not None

    def _set_invocation_status(
        self,
        invocation_id: str,
        status: InvocationStatus,
    ) -> None:
        """Set the status of an invocation by ID."""
        self._set_invocations_status([invocation_id], status)

    def _set_invocations_status(
        self, invocation_ids: list[str], status: InvocationStatus
    ) -> None:
        """Set the status of multiple invocations at once by ID."""
        now = time() if status == InvocationStatus.PENDING else None
        # pre_pending = status.value if status == InvocationStatus.PENDING else None
        with sqlite_conn(self.sqlite_db_path) as conn:
            placeholders = ",".join(["?" for _ in invocation_ids])
            sql = f"""UPDATE {Tables.INVOCATIONS}
                        SET status = ?,
                            pre_pending_status = CASE WHEN ? = '{InvocationStatus.PENDING.value}' THEN status ELSE NULL END,
                            pending_start_time = ?
                        WHERE invocation_id IN ({placeholders})"""
            params = [status.value, status.value, now] + invocation_ids
            conn.execute(sql, params)
            conn.commit()

    def _set_invocation_pending_status(self, invocation_id: str) -> None:
        """
        Set an invocation to pending status with locking by ID.

        :param str invocation_id: The invocation ID
        :raises PendingInvocationLockError: If already pending
        """
        self._set_invocation_status(invocation_id, InvocationStatus.PENDING)

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            for key, value in invocation.serialized_arguments.items():
                conn.execute(
                    f"INSERT OR REPLACE INTO {Tables.INVOCATION_ARGS} (invocation_id, arg_key, arg_value) VALUES (?, ?, ?)",
                    (invocation.invocation_id, key, value),
                )
            conn.commit()

    def get_invocation_pending_timer(self, invocation_id: str) -> Optional[float]:
        """Retrieves the pending timer for a specific invocation."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT pending_start_time FROM {Tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def set_up_invocation_auto_purge(self, invocation_id: str) -> None:
        """
        Set up invocation for auto-purging by setting the auto_purge_timestamp.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"UPDATE {Tables.INVOCATIONS} SET auto_purge_timestamp = ? WHERE invocation_id = ?",
                (time(), invocation_id),
            )
            conn.commit()

    def auto_purge(self) -> None:
        """
        Auto-purge old invocations based on auto_purge_timestamp.
        """
        threshold = time() - self.conf.auto_final_invocation_purge_hours * 3600
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {Tables.INVOCATIONS} WHERE auto_purge_timestamp IS NOT NULL AND auto_purge_timestamp <= ?",
                (threshold,),
            )
            to_purge = [row[0] for row in cursor.fetchall()]
            cursor.close()
            for invocation_id in to_purge:
                self.clean_up_invocation_cycles(invocation_id)
                self.release_waiters(invocation_id)
                conn.execute(
                    f"DELETE FROM {Tables.INVOCATIONS} WHERE invocation_id = ?",
                    (invocation_id,),
                )
                conn.execute(
                    f"DELETE FROM {Tables.INVOCATION_ARGS} WHERE invocation_id = ?",
                    (invocation_id,),
                )
            conn.commit()

    def get_invocation_status(self, invocation_id: str) -> InvocationStatus:
        """
        Get the current status of an invocation by ID, handling pending timeouts.

        :param str invocation_id: The invocation ID
        :return: The current status
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""SELECT status, pending_start_time, pre_pending_status
                    FROM {Tables.INVOCATIONS}
                    WHERE invocation_id = ?""",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                raise KeyError(f"Invocation ID {invocation_id} not found")
            status, pending_start_time, pre_pending_status = row
            status_enum = InvocationStatus(status)
            if status_enum == InvocationStatus.PENDING:
                elapsed = time() - pending_start_time
                if elapsed > self.app.conf.max_pending_seconds and pre_pending_status:
                    # Fallback to pre-pending status and update in DB
                    conn.execute(
                        f"UPDATE {Tables.INVOCATIONS} SET status = ? WHERE invocation_id = ?",
                        (pre_pending_status, invocation_id),
                    )
                    conn.commit()
                    return InvocationStatus(pre_pending_status)
            return status_enum

    def increment_invocation_retries(self, invocation_id: str) -> None:
        """
        Increment the retry count for an invocation by ID.

        :param str invocation_id: The invocation ID
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"UPDATE {Tables.INVOCATIONS} SET retry_count = retry_count + 1 WHERE invocation_id = ?",
                (invocation_id,),
            )
            conn.commit()

    def get_invocation_retries(self, invocation_id: str) -> int:
        """
        Get the number of retries for an invocation by ID.

        :param str invocation_id: The invocation ID
        :return: The number of retries
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT retry_count FROM {Tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            return row[0] if row else 0

    def filter_by_status(
        self, invocation_ids: list[str], status_filter: set["InvocationStatus"]
    ) -> list[str]:
        """
        Filter invocations by status by ID.

        :param list[str] invocation_ids: The invocation IDs to filter
        :param set[InvocationStatus] | None status_filter: The statuses to filter by
        :return: List of invocation IDs matching the status filter
        """
        if not invocation_ids or status_filter is None:
            return []
        with sqlite_conn(self.sqlite_db_path) as conn:
            placeholders = ",".join(["?" for _ in invocation_ids])
            status_placeholders = ",".join(["?" for _ in status_filter])
            sql = f"""
                SELECT invocation_id FROM {Tables.INVOCATIONS}
                WHERE invocation_id IN ({placeholders}) AND status IN ({status_placeholders})
            """
            params = invocation_ids + [s.value for s in status_filter]
            cursor = conn.execute(sql, params)
            invocation_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return invocation_ids

    def purge(self) -> None:
        """
        Clear all orchestrator state.
        """
        delete_tables_with_prefix(self.sqlite_db_path, "orchestrator_")
        self._init_tables()
