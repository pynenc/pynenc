"""
SQLite-based orchestrator for cross-process testing.

This module provides a SQLite-based orchestrator implementation that enables
true cross-process coordination for testing process runners. Unlike shared memory,
SQLite provides ACID transactions and handles concurrent access automatically.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from functools import cached_property
from time import time
from typing import TYPE_CHECKING

from pynenc.conf.config_orchestrator import ConfigOrchestratorSQLite
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    status_record_transition,
)
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseOrchestrator,
)
from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
from pynenc.util.sqlite_utils import create_sqlite_connection as sqlite_conn
from pynenc.util.sqlite_utils import (
    delete_tables_with_prefix,
    get_sqlite_sqlite_db_path,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.call_id import CallId
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task, TaskId
    from pynenc.types import Params, Result


class Tables:
    INVOCATIONS = "orchestrator_invocations"
    INVOCATION_ARGS = "orchestrator_invocation_args"
    BLOCKING_EDGES = "orchestrator_blocking_edges"
    RUNNER_HEARTBEATS = "orchestrator_runner_heartbeats"


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
        self,
        caller_invocation_id: "InvocationId",
        result_invocation_ids: list["InvocationId"],
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

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["InvocationId"]:
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
            cursor = conn.execute(query, tuple(params))
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
        self._blocking_control = SQLiteBlockingControl(app, self.sqlite_db_path)

    def _init_tables(self) -> None:
        """Initialize SQLite tables for orchestrator state."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.INVOCATIONS} (
                    invocation_id TEXT PRIMARY KEY,
                    task_id_key TEXT NOT NULL,
                    call_id_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    status_runner_id TEXT,
                    status_timestamp REAL NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    auto_purge_timestamp REAL
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_orchestrator_task_id ON {Tables.INVOCATIONS}(task_id_key)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_orchestrator_call_id ON {Tables.INVOCATIONS}(call_id_key)"
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

            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Tables.RUNNER_HEARTBEATS} (
                    runner_id TEXT PRIMARY KEY,
                    creation_timestamp REAL NOT NULL,
                    allow_to_run_atomic_service INTEGER NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    last_service_start TEXT,
                    last_service_end TEXT
                )
            """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_runner_heartbeat ON {Tables.RUNNER_HEARTBEATS}(last_heartbeat)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_runner_creation ON {Tables.RUNNER_HEARTBEATS}(creation_timestamp)"
            )

            conn.commit()

    @cached_property
    def conf(self) -> ConfigOrchestratorSQLite:
        return ConfigOrchestratorSQLite(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def blocking_control(self) -> BaseBlockingControl:
        """Return blocking control."""
        return self._blocking_control

    def _register_new_invocations(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Register new invocations with status Register if they don't exist yet."""
        status_record = InvocationStatusRecord(InvocationStatus.REGISTERED, runner_id)
        with sqlite_conn(self.sqlite_db_path) as conn:
            for invocation in invocations:
                conn.execute(
                    f"""
                    INSERT INTO {Tables.INVOCATIONS} (
                    invocation_id, task_id_key, call_id_key, status, status_runner_id, status_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(invocation_id) DO NOTHING
                """,
                    (
                        invocation.invocation_id,
                        invocation.task.task_id.key,
                        invocation.call.call_id.key,
                        status_record.status.value,
                        status_record.runner_id,
                        status_record.timestamp.timestamp(),
                    ),
                )
            conn.commit()
        return status_record

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> Iterator["InvocationId"]:
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
        wheres = ["i.task_id_key = ?"]
        params.append(task.task_id.key)
        if statuses:
            wheres.append(f"i.status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])
        sql = query + " " + " ".join(joins)
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, tuple(params))
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def get_task_invocation_ids(self, task_id: "TaskId") -> Iterator["InvocationId"]:
        """Retrieves all invocation ids for a given task id."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {Tables.INVOCATIONS} WHERE task_id_key = ?",
                (task_id.key,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def get_invocation_ids_paginated(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["InvocationId"]:
        """
        Retrieves invocation IDs with pagination support.

        :param TaskId | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :param int limit: Maximum number of results to return.
        :param int offset: Number of results to skip.
        :return: List of matching invocation IDs.
        """
        query = f"SELECT invocation_id FROM {Tables.INVOCATIONS}"
        wheres = []
        params: list = []

        if task_id:
            wheres.append("task_id_key = ?")
            params.append(task_id.key)

        if statuses:
            wheres.append(f"status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])

        sql = query
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)

        # Order by timestamp descending (newest first) for consistent pagination
        sql += " ORDER BY status_timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, tuple(params))
            result = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return result

    def count_invocations(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> int:
        """
        Counts invocations matching the given filters.

        :param str | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :return: The total count of matching invocations.
        """
        query = f"SELECT COUNT(*) FROM {Tables.INVOCATIONS}"
        wheres = []
        params: list = []

        if task_id:
            wheres.append("task_id_key = ?")
            params.append(task_id.key)

        if statuses:
            wheres.append(f"status IN ({','.join(['?' for _ in statuses])})")
            params.extend([s.value for s in statuses])

        sql = query
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(sql, tuple(params))
            count = cursor.fetchone()[0]
            cursor.close()
            return count

    def get_call_invocation_ids(self, call_id: "CallId") -> Iterator["InvocationId"]:
        """Retrieves all invocation ids for a given call id."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"SELECT invocation_id FROM {Tables.INVOCATIONS} WHERE call_id_key = ?",
                (call_id.key,),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()
            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def _atomic_status_transition(
        self,
        invocation_id: "InvocationId",
        status: InvocationStatus,
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Atomically read, validate, and write invocation status.

        Uses ``BEGIN IMMEDIATE`` to acquire a write lock before reading so that
        no two processes can concurrently observe the same "from" status, compute
        independent (valid) transitions, and both commit — which would produce
        duplicate history records and could allow forbidden status regressions
        (e.g. RUNNING → PENDING) to slip through.
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            # Acquire write lock immediately so the read-validate-write is
            # truly atomic across all concurrent SQLite connections.
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                f"SELECT status, status_runner_id, status_timestamp FROM {Tables.INVOCATIONS} WHERE invocation_id = ?",
                (invocation_id,),
            )
            row = cursor.fetchone()
            if not row:
                # Raising here lets the context manager's __exit__ rollback the
                # IMMEDIATE transaction automatically.
                raise KeyError(f"Invocation ID {invocation_id} not found")
            prev_status_record = InvocationStatusRecord(
                InvocationStatus(row[0]), row[1], row[2]
            )
            new_record = status_record_transition(prev_status_record, status, runner_id)

            conn.execute(
                f"""UPDATE {Tables.INVOCATIONS}
                        SET status = ?,
                            status_runner_id = ?,
                            status_timestamp = ?
                        WHERE invocation_id = ?""",
                (
                    new_record.status.value,
                    new_record.runner_id,
                    new_record.timestamp.timestamp(),
                    invocation_id,
                ),
            )
            conn.commit()
        return new_record

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        with sqlite_conn(self.sqlite_db_path) as conn:
            for key, value in invocation.call.serialized_arguments.items():
                conn.execute(
                    f"INSERT OR REPLACE INTO {Tables.INVOCATION_ARGS} (invocation_id, arg_key, arg_value) VALUES (?, ?, ?)",
                    (invocation.invocation_id, key, value),
                )
            conn.commit()

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

    def get_invocation_status_record(
        self, invocation_id: str
    ) -> InvocationStatusRecord:
        """
        Get the current status of an invocation by ID, handling pending timeouts.

        :param str invocation_id: The invocation ID
        :return: The current status
        """
        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""SELECT status, status_timestamp, status_runner_id
                    FROM {Tables.INVOCATIONS}
                    WHERE invocation_id = ?""",
                (invocation_id,),
            )
            row = cursor.fetchone()
            cursor.close()
            if not row:
                raise KeyError(f"Invocation ID {invocation_id} not found")
            status_str, status_timestamp, status_runner_id = row
            status = InvocationStatus(status_str)
            timestamp = datetime.fromtimestamp(status_timestamp, tz=UTC)
            return InvocationStatusRecord(status, status_runner_id, timestamp)

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
        self,
        invocation_ids: list["InvocationId"],
        status_filter: frozenset["InvocationStatus"],
    ) -> list["InvocationId"]:
        """
        Filter invocations by status by ID.

        :param list["InvocationId"] invocation_ids: The invocation IDs to filter
        :param frozenset["InvocationStatus"] | None status_filter: The statuses to filter by
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
            cursor = conn.execute(sql, tuple(params))
            invocation_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return invocation_ids

    def register_runner_heartbeats(
        self, runner_ids: list[str], can_run_atomic_service: bool = False
    ) -> None:
        """Register or update heartbeat timestamps for one or more runners."""
        if not runner_ids:
            return
        current_time = time()
        with sqlite_conn(self.sqlite_db_path) as conn:
            for runner_id in runner_ids:
                conn.execute(
                    f"""
                    INSERT INTO {Tables.RUNNER_HEARTBEATS} (
                        runner_id, creation_timestamp, last_heartbeat, allow_to_run_atomic_service
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(runner_id) DO UPDATE SET
                        last_heartbeat = excluded.last_heartbeat,
                        allow_to_run_atomic_service = excluded.allow_to_run_atomic_service
                    """,
                    (
                        runner_id,
                        current_time,
                        current_time,
                        int(can_run_atomic_service),
                    ),
                )
            conn.commit()

    def _get_active_runners(
        self, timeout_seconds: float, can_run_atomic_service: bool | None = None
    ) -> list[ActiveRunnerInfo]:
        """Retrieve all active runners with heartbeat information and atomic service eligibility."""
        current_time = time()
        cutoff_time = current_time - timeout_seconds

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT runner_id, creation_timestamp, last_heartbeat,
                       allow_to_run_atomic_service, last_service_start, last_service_end
                FROM {Tables.RUNNER_HEARTBEATS}
                WHERE last_heartbeat >= ?
                AND (? IS NULL OR allow_to_run_atomic_service = ?)
                ORDER BY creation_timestamp ASC
                """,
                (cutoff_time, can_run_atomic_service, can_run_atomic_service),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()

            active_runners = []
            for (
                runner_id,
                creation_ts,
                last_hb,
                allow_to_run_atomic_service,
                service_start,
                service_end,
            ) in cursor_rows:
                active_runners.append(
                    ActiveRunnerInfo(
                        runner_id=runner_id,
                        creation_time=datetime.fromtimestamp(creation_ts, tz=UTC),
                        last_heartbeat=datetime.fromtimestamp(last_hb, tz=UTC),
                        allow_to_run_atomic_service=bool(allow_to_run_atomic_service),
                        last_service_start=datetime.fromisoformat(service_start)
                        if service_start
                        else None,
                        last_service_end=datetime.fromisoformat(service_end)
                        if service_end
                        else None,
                    )
                )

            return active_runners

    def record_atomic_service_execution(
        self, runner_id: str, start_time: datetime, end_time: datetime
    ) -> None:
        """Record the latest atomic service execution window for a runner."""
        with sqlite_conn(self.sqlite_db_path) as conn:
            conn.execute(
                f"""
                UPDATE {Tables.RUNNER_HEARTBEATS}
                SET last_service_start = ?, last_service_end = ?
                WHERE runner_id = ?
                """,
                (start_time.isoformat(), end_time.isoformat(), runner_id),
            )
            conn.commit()

    def get_pending_invocations_for_recovery(self) -> Iterator["InvocationId"]:
        """Retrieve invocation IDs stuck in PENDING status beyond the allowed time."""
        max_pending_seconds = self.app.conf.max_pending_seconds
        current_time = time()
        cutoff_time = current_time - max_pending_seconds

        with sqlite_conn(self.sqlite_db_path) as conn:
            cursor = conn.execute(
                f"""
                SELECT invocation_id
                FROM {Tables.INVOCATIONS}
                WHERE status = ? AND status_timestamp <= ?
                """,
                (InvocationStatus.PENDING.value, cutoff_time),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()

            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def _get_running_invocations_for_recovery(
        self, timeout_seconds: float
    ) -> Iterator["InvocationId"]:
        """Retrieve RUNNING invocation IDs owned by inactive runners."""
        current_time = time()
        cutoff_time = current_time - timeout_seconds

        with sqlite_conn(self.sqlite_db_path) as conn:
            # Find RUNNING invocations where the owner is not in active runners
            # A runner is active if it has a recent heartbeat
            cursor = conn.execute(
                f"""
                SELECT i.invocation_id
                FROM {Tables.INVOCATIONS} i
                LEFT JOIN {Tables.RUNNER_HEARTBEATS} r ON i.status_runner_id = r.runner_id
                WHERE i.status = ?
                  AND i.status_runner_id IS NOT NULL
                  AND (r.runner_id IS NULL OR r.last_heartbeat < ?)
                """,
                (InvocationStatus.RUNNING.value, cutoff_time),
            )
            cursor_rows = cursor.fetchall()
            cursor.close()

            for (invocation_id,) in cursor_rows:
                yield InvocationId(invocation_id)

    def purge(self) -> None:
        """
        Clear all orchestrator state.
        """
        delete_tables_with_prefix(self.sqlite_db_path, "orchestrator_")
        self._init_tables()
